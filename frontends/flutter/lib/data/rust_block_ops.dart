/// BlockOps implementation using Rust opaque Block types via FFI.
///
/// This class provides:
/// - Complete BlockOps interface implementation
/// - Caching layer for efficient block access
/// - Echo suppression to prevent duplicate UI updates from P2P sync
/// - Stream management for change notifications
/// - Lifecycle management for proper resource cleanup
/// - Collapsed state tracking (UI-only state)
library;

import 'dart:async';
import 'package:flutter/foundation.dart';
import 'package:outliner_view/outliner_view.dart' show BlockOps;
import '../src/rust/api/repository.dart' as repo;
import '../src/rust/api/types.dart' as rust;

// Block is generated in repository.dart by FRB (since it's used in method signatures there)
typedef RustBlock = repo.Block;

class RustBlockOps implements BlockOps<RustBlock> {
  /// The underlying Rust repository (FFI)
  final repo.RustDocumentRepository _backend;

  /// Cache of blocks by ID for efficient synchronous access
  final Map<String, RustBlock> _cache = {};

  /// Collapsed state per block (UI-only state, not persisted)
  final Map<String, bool> _collapsedState = {};

  /// IDs of blocks created/updated locally (for echo suppression)
  final Set<String> _localEditIds = {};

  /// Stream controller for change notifications
  final StreamController<RustBlock> _changeController =
      StreamController<RustBlock>.broadcast();

  /// Subscription to the Rust change stream
  StreamSubscription<rust.BlockChange>? _changeSubscription;

  /// Root block ID (special container for top-level blocks)
  String? _rootId;

  /// Whether this repository has been disposed
  bool _disposed = false;

  RustBlockOps._(this._backend);

  /// Create a new document and initialize it.
  static Future<RustBlockOps> createNew(String docId) async {
    final backend = await repo.RustDocumentRepository.createNew(docId: docId);
    final ops = RustBlockOps._(backend);
    await ops._initialize();
    return ops;
  }

  /// Open an existing document and initialize it.
  static Future<RustBlockOps> openExisting(String docId) async {
    final backend = await repo.RustDocumentRepository.openExisting(
      docId: docId,
    );
    final ops = RustBlockOps._(backend);
    await ops._initialize();
    return ops;
  }

  /// Initialize by streaming initial state and subscribing to changes.
  Future<void> _initialize() async {
    // Watch changes from the beginning (includes current state as Created events)
    final position = await repo.streamPositionBeginning();
    final changeStream = _backend.watchChangesSince(position: position);

    _changeSubscription = changeStream.listen(
      _handleChange,
      onError: (error) {
        debugPrint('Change stream error: $error');
      },
      onDone: () {
        debugPrint('Change stream closed');
      },
    );

    // Wait for initial state to load
    await Future.delayed(const Duration(milliseconds: 100));

    // Load all blocks including root (level 0) and top-level (level 1)
    final traversal = await repo.traversalNew(
      minLevel: BigInt.from(0),
      maxLevel: BigInt.from(1),
    );
    final blocks = await _backend.getAllBlocks(traversal: traversal);

    // Cache all blocks
    for (final block in blocks) {
      final blockId = repo.blockGetId(block: block);
      _cache[blockId] = block;

      // Identify the root block (has NO_PARENT_ID as parent)
      final parentId = repo.blockGetParentId(block: block);
      if (parentId == '__no_parent__') {
        _rootId = blockId;
      }
    }
  }

  /// Handle API errors from the Rust backend.
  void _handleApiError(rust.ApiError error) {
    error.when(
      blockNotFound: (id) => debugPrint('Block not found: $id'),
      documentNotFound: (docId) => debugPrint('Document not found: $docId'),
      cyclicMove: (id, target) => debugPrint('Cyclic move: $id -> $target'),
      invalidOperation: (msg) => debugPrint('Invalid operation: $msg'),
      networkError: (msg) => debugPrint('Network error: $msg'),
      internalError: (msg) => debugPrint('Internal error: $msg'),
    );
  }

  /// Handle incoming change events from the Rust backend.
  void _handleChange(rust.BlockChange change) {
    change.when(
      created: (id, parentId, content, children, origin) {
        // Echo suppression: skip if local and in _localEditIds
        if (origin == rust.ChangeOrigin.local && _localEditIds.contains(id)) {
          return;
        }

        // Fetch the full Block object from backend for cache
        _backend
            .getBlock(id: id)
            .then((block) {
              _cache[id] = block;
              _emitChange();
            })
            .catchError((e) {
              debugPrint('Error fetching created block $id: $e');
            });
      },
      updated: (id, content, origin) {
        // Echo suppression
        if (origin == rust.ChangeOrigin.local && _localEditIds.contains(id)) {
          return;
        }

        // Fetch updated Block object from backend
        _backend
            .getBlock(id: id)
            .then((block) {
              _cache[id] = block;
              _emitChange();
            })
            .catchError((e) {
              debugPrint('Error fetching updated block $id: $e');
            });
      },
      deleted: (id, origin) {
        // Echo suppression
        if (origin == rust.ChangeOrigin.local && _localEditIds.contains(id)) {
          return;
        }

        // Remove from cache and state
        _cache.remove(id);
        _collapsedState.remove(id);
        _emitChange();
      },
      moved: (id, newParent, after, origin) {
        // Echo suppression
        if (origin == rust.ChangeOrigin.local && _localEditIds.contains(id)) {
          return;
        }

        // Fetch moved Block object from backend
        _backend
            .getBlock(id: id)
            .then((block) {
              _cache[id] = block;
              _emitChange();
            })
            .catchError((e) {
              debugPrint('Error fetching moved block $id: $e');
            });
      },
    );
  }

  /// Emit a change event with the current root block.
  void _emitChange() {
    if (_rootId != null && _cache.containsKey(_rootId)) {
      _changeController.add(_cache[_rootId]!);
    }
  }

  // =========================================================================
  // BlockAccessOps implementation
  // =========================================================================

  @override
  String getId(RustBlock block) => repo.blockGetId(block: block);

  @override
  String getContent(RustBlock block) => repo.blockGetContent(block: block);

  @override
  List<RustBlock> getChildren(RustBlock block) {
    return repo
        .blockGetChildren(block: block)
        .map((id) => _cache[id])
        .where((b) => b != null)
        .cast<RustBlock>()
        .toList();
  }

  @override
  bool getIsCollapsed(RustBlock block) {
    return _collapsedState[repo.blockGetId(block: block)] ?? false;
  }

  @override
  DateTime getCreatedAt(RustBlock block) {
    // TODO: BlockMetadata is opaque and doesn't expose fields yet
    // Need to add getters in Rust or mirror the type with fields
    return DateTime.now();
  }

  @override
  DateTime getUpdatedAt(RustBlock block) {
    // TODO: BlockMetadata is opaque and doesn't expose fields yet
    // Need to add getters in Rust or mirror the type with fields
    return DateTime.now();
  }

  // =========================================================================
  // BlockTreeOps implementation
  // =========================================================================

  @override
  List<RustBlock> getTopLevelBlocks() {
    if (_rootId == null) return [];
    final root = _cache[_rootId];
    if (root == null) return [];
    return getChildren(root);
  }

  @override
  bool isDescendantOf(RustBlock potentialAncestor, RustBlock block) {
    final ancestorId = getId(potentialAncestor);
    RustBlock? current = block;

    while (current != null) {
      final parentId = repo.blockGetParentId(block: current);
      if (parentId == ancestorId) return true;
      if (parentId == _rootId) return false;
      current = _cache[parentId];
    }

    return false;
  }

  @override
  Future<RustBlock?> findNextVisibleBlock(RustBlock block) async {
    // If not collapsed and has children, return first child
    if (!getIsCollapsed(block)) {
      final children = getChildren(block);
      if (children.isNotEmpty) {
        return children.first;
      }
    }

    // Find next sibling or ancestor's next sibling
    RustBlock? current = block;
    while (current != null) {
      final parent = await findParent(current);
      if (parent == null) return null;

      final siblings = getChildren(parent);
      final currentIndex = siblings.indexWhere(
        (b) => getId(b) == getId(current!),
      );

      if (currentIndex != -1 && currentIndex + 1 < siblings.length) {
        return siblings[currentIndex + 1];
      }

      // Move up to parent and continue
      current = parent;
      if (getId(current) == _rootId) return null;
    }

    return null;
  }

  @override
  Future<RustBlock?> findPreviousVisibleBlock(RustBlock block) async {
    final parent = await findParent(block);
    if (parent == null) return null;

    final siblings = getChildren(parent);
    final currentIndex = siblings.indexWhere((b) => getId(b) == getId(block));

    if (currentIndex == -1) return null;

    // If has previous sibling, return its last visible descendant
    if (currentIndex > 0) {
      var prev = siblings[currentIndex - 1];
      while (!getIsCollapsed(prev)) {
        final children = getChildren(prev);
        if (children.isEmpty) break;
        prev = children.last;
      }
      return prev;
    }

    // Otherwise return parent (unless it's root)
    if (getId(parent) == _rootId) return null;
    return parent;
  }

  @override
  Future<RustBlock?> findParent(RustBlock block) async {
    return _cache[repo.blockGetParentId(block: block)];
  }

  @override
  Future<RustBlock?> findBlockById(String blockId) async {
    return _cache[blockId];
  }

  @override
  Future<RustBlock> getRootBlock() async {
    if (_rootId == null) {
      throw StateError('Root block not initialized');
    }
    final root = _cache[_rootId];
    if (root == null) {
      throw StateError('Root block not found in cache');
    }
    return root;
  }

  // =========================================================================
  // BlockMutationOps implementation
  // =========================================================================

  @override
  Future<void> updateBlock(RustBlock block, String content) async {
    final id = getId(block);
    try {
      // Mark as local edit for echo suppression
      _localEditIds.add(id);

      // Update in Rust
      await _backend.updateBlock(id: id, content: content);

      // Invalidate cache (will be refreshed via change stream)
      _cache.remove(id);

      // Clean up echo suppression marker after delay
      Future.delayed(const Duration(seconds: 2), () {
        _localEditIds.remove(id);
      });
    } on rust.ApiError catch (e) {
      _handleApiError(e);
      _localEditIds.remove(id);
      rethrow;
    } catch (e) {
      debugPrint('Unexpected error in updateBlock: $e');
      _localEditIds.remove(id);
      rethrow;
    }
  }

  @override
  Future<void> deleteBlock(RustBlock block) async {
    final id = getId(block);
    try {
      // Mark as local edit
      _localEditIds.add(id);

      // Delete in Rust
      await _backend.deleteBlock(id: id);

      // Remove from cache and state
      _cache.remove(id);
      _collapsedState.remove(id);

      // Clean up echo suppression marker
      Future.delayed(const Duration(seconds: 2), () {
        _localEditIds.remove(id);
      });
    } on rust.ApiError catch (e) {
      _handleApiError(e);
      _localEditIds.remove(id);
      rethrow;
    } catch (e) {
      debugPrint('Unexpected error in deleteBlock: $e');
      _localEditIds.remove(id);
      rethrow;
    }
  }

  @override
  Future<void> moveBlock(
    RustBlock block,
    RustBlock? newParent,
    int newIndex,
  ) async {
    final id = getId(block);
    final newParentId = newParent != null ? getId(newParent) : _rootId!;

    try {
      // Mark as local edit
      _localEditIds.add(id);

      // Calculate the "after" block based on newIndex
      String? after;
      if (newParent != null) {
        final siblings = getChildren(newParent);
        if (newIndex > 0 && newIndex <= siblings.length) {
          after = getId(siblings[newIndex - 1]);
        }
      }

      // Move in Rust
      await _backend.moveBlock(id: id, newParent: newParentId, after: after);

      // Invalidate cache
      _cache.remove(id);

      // Clean up echo suppression marker
      Future.delayed(const Duration(seconds: 2), () {
        _localEditIds.remove(id);
      });
    } on rust.ApiError catch (e) {
      _handleApiError(e);
      _localEditIds.remove(id);
      rethrow;
    } catch (e) {
      debugPrint('Unexpected error in moveBlock: $e');
      _localEditIds.remove(id);
      rethrow;
    }
  }

  @override
  Future<void> toggleCollapse(RustBlock block) async {
    final id = getId(block);
    _collapsedState[id] = !(_collapsedState[id] ?? false);
    _emitChange();
  }

  @override
  Future<void> addChildBlock(RustBlock parent, RustBlock child) async {
    // Move the child to be the last child of the parent
    final children = getChildren(parent);
    await moveBlock(child, parent, children.length);
  }

  @override
  Future<void> addTopLevelBlock(RustBlock block) async {
    // Special case: If the block is the root (from ops.create() placeholder),
    // create a new block instead of trying to move it
    if (getId(block) == _rootId) {
      await createBlockAsync(content: '', parentId: _rootId);
      return;
    }

    // Otherwise, move existing block to root level at the end
    final topLevel = getTopLevelBlocks();
    final root = await getRootBlock();
    await moveBlock(block, root, topLevel.length);
  }

  @override
  Future<String> splitBlock(RustBlock block, int cursorPosition) async {
    final content = getContent(block);
    final beforeCursor = content.substring(0, cursorPosition);
    final afterCursor = content.substring(cursorPosition);

    // Update current block with content before cursor
    await updateBlock(block, beforeCursor);

    // Create new block with content after cursor
    final parent = await findParent(block);
    final parentId = parent != null ? getId(parent) : _rootId!;

    final newBlock = await _backend.createBlock(
      parentId: parentId,
      content: afterCursor,
    );

    // Cache the new block (cast to RustBlock)
    final rustBlock = newBlock;
    final newBlockId = repo.blockGetId(block: rustBlock);
    _cache[newBlockId] = rustBlock;

    // Move new block to be right after current block
    final siblings = parent != null ? getChildren(parent) : getTopLevelBlocks();
    final currentIndex = siblings.indexWhere((b) => getId(b) == getId(block));
    if (currentIndex != -1) {
      await moveBlock(rustBlock, parent, currentIndex + 1);
    }

    return newBlockId;
  }

  @override
  Future<void> indentBlock(RustBlock block) async {
    final parent = await findParent(block);
    if (parent == null) return;

    final siblings = getChildren(parent);
    final currentIndex = siblings.indexWhere((b) => getId(b) == getId(block));

    // Can only indent if there's a previous sibling
    if (currentIndex <= 0) return;

    final newParent = siblings[currentIndex - 1];
    final newParentChildren = getChildren(newParent);

    // Move to be the last child of the previous sibling
    await moveBlock(block, newParent, newParentChildren.length);
  }

  @override
  Future<void> outdentBlock(RustBlock block) async {
    final parent = await findParent(block);
    if (parent == null) return;
    if (getId(parent) == _rootId) return; // Already at top level

    final grandparent = await findParent(parent);
    if (grandparent == null) return;

    final parentSiblings = getChildren(grandparent);
    final parentIndex = parentSiblings.indexWhere(
      (b) => getId(b) == getId(parent),
    );

    // Move to be right after the parent
    await moveBlock(block, grandparent, parentIndex + 1);
  }

  // =========================================================================
  // BlockCreationOps implementation
  // =========================================================================

  @override
  RustBlock copyWith(
    RustBlock block, {
    String? content,
    List<RustBlock>? children,
    bool? isCollapsed,
  }) {
    throw UnimplementedError(
      'RustOpaque blocks cannot be copied on Dart side. '
      'Use updateBlock() or create a new block via the Rust API instead.',
    );
  }

  @override
  RustBlock create({
    String? id,
    required String content,
    List<RustBlock>? children,
    bool? isCollapsed,
  }) {
    // Returns the root block as a placeholder for synchronous contexts.
    //
    // This method is called by:
    // 1. OutlinerNotifier._createInitialState - Creates initial state placeholder
    //    (immediately replaced by _loadInitialState with the real root)
    // 2. Empty state widget - Creates a placeholder block that gets detected
    //    and replaced in addTopLevelBlock()
    //
    // The root block is safe to return because:
    // - It's a valid RustBlock from the cache
    // - Callers either replace it immediately or we detect and handle it specially
    //
    // For actual block creation, this class provides createBlockAsync().
    if (_rootId == null || !_cache.containsKey(_rootId)) {
      throw StateError(
        'Cannot create block: Root block not yet initialized. '
        'Ensure RustBlockOps is fully initialized before use.',
      );
    }
    return _cache[_rootId]!;
  }

  // =========================================================================
  // Additional helper methods
  // =========================================================================

  /// Create a new block asynchronously via the Rust API.
  Future<RustBlock> createBlockAsync({
    required String content,
    String? parentId,
    String? id,
  }) async {
    try {
      final block = await _backend.createBlock(
        parentId: parentId ?? _rootId!,
        content: content,
        id: id,
      );

      final blockId = repo.blockGetId(block: block);
      _localEditIds.add(blockId);
      // Cast to RustBlock for cache
      final rustBlock = block;
      _cache[blockId] = rustBlock;

      Future.delayed(const Duration(seconds: 2), () {
        _localEditIds.remove(blockId);
      });

      return rustBlock;
    } on rust.ApiError catch (e) {
      _handleApiError(e);
      rethrow;
    }
  }

  // =========================================================================
  // BlockOps changeStream
  // =========================================================================

  @override
  Stream<RustBlock> get changeStream => _changeController.stream;

  // =========================================================================
  // P2P Operations (backend-specific, not part of BlockOps)
  // =========================================================================

  /// Get this node's P2P identifier.
  Future<String> getNodeId() async {
    return await _backend.getNodeId();
  }

  /// Connect to a peer for P2P synchronization.
  Future<void> connectToPeer(String peerNodeId) async {
    try {
      await _backend.connectToPeer(peerNodeId: peerNodeId);
    } on rust.ApiError catch (e) {
      _handleApiError(e);
      rethrow;
    }
  }

  /// Start accepting incoming P2P connections.
  Future<void> acceptConnections() async {
    try {
      await _backend.acceptConnections();
    } on rust.ApiError catch (e) {
      _handleApiError(e);
      rethrow;
    }
  }

  // =========================================================================
  // Lifecycle management
  // =========================================================================

  /// Dispose of resources and clean up.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;

    await _changeSubscription?.cancel();
    await _changeController.close();
    await _backend.dispose();

    _cache.clear();
    _collapsedState.clear();
    _localEditIds.clear();
  }
}
