import 'dart:async';
import 'package:flutter/foundation.dart';
import 'package:outliner_view/outliner_view.dart';

/// BlockOps implementation for Map<String, dynamic> row data.
///
/// This adapter bridges between flat row data (from SQL queries) and the
/// hierarchical OutlinerListView widget. It implements BlockOps<Map<String, dynamic>>
/// to work directly with row data without conversion overhead.
class RowDataBlockOps implements BlockOps<Map<String, dynamic>> {
  /// Row data cache: id -> row data
  final Map<String, Map<String, dynamic>> _rowCache;

  /// Column name for parent ID (e.g., "parent_id")
  final String _parentIdColumn;

  /// Column name for sort key (e.g., "sort_key")
  final String _sortKeyColumn;

  /// Stream controller for change notifications
  final StreamController<Map<String, dynamic>> _changeController =
      StreamController<Map<String, dynamic>>.broadcast();

  /// Callback for executing operations (indent, outdent, move, etc.)
  final Future<void> Function(
    String entityName,
    String operationName,
    Map<String, dynamic> params,
  )?
  _onOperation;

  /// Entity name for operations (e.g., "blocks", "todoist_tasks")
  final String _entityName;

  /// Synthetic root block ID
  static const String _rootId = '__root__';

  /// Collapsed state per block (UI-only state, not persisted)
  final Map<String, bool> _collapsedState = {};

  RowDataBlockOps({
    required Map<String, Map<String, dynamic>> rowCache,
    required String parentIdColumn,
    required String sortKeyColumn,
    required String entityName,
    Future<void> Function(String, String, Map<String, dynamic>)? onOperation,
  }) : _rowCache = rowCache,
       _parentIdColumn = parentIdColumn,
       _sortKeyColumn = sortKeyColumn,
       _entityName = entityName,
       _onOperation = onOperation;

  /// Compare two sort key values for ordering
  int _compareSortKeys(dynamic a, dynamic b) {
    // Handle null values
    if (a == null && b == null) return 0;
    if (a == null) return -1;
    if (b == null) return 1;

    // Try numeric comparison first
    if (a is num && b is num) {
      return a.compareTo(b);
    }

    // Fall back to string comparison
    return a.toString().compareTo(b.toString());
  }

  // =========================================================================
  // BlockAccessOps implementation
  // =========================================================================

  @override
  String getId(Map<String, dynamic> block) {
    if (block['id'] == _rootId) return _rootId;
    return block['id']?.toString() ?? '';
  }

  @override
  String getContent(Map<String, dynamic> block) {
    if (block['id'] == _rootId) return '';
    return block['content']?.toString() ?? '';
  }

  @override
  List<Map<String, dynamic>> getChildren(Map<String, dynamic> block) {
    final id = getId(block);
    if (id == _rootId) {
      // Return top-level blocks (those with null or empty parent_id)
      final topLevel = _rowCache.values.where((row) {
        final parentId = row[_parentIdColumn];
        return parentId == null ||
            parentId.toString().isEmpty ||
            parentId.toString() == 'null';
      }).toList();
      topLevel.sort(
        (a, b) => _compareSortKeys(a[_sortKeyColumn], b[_sortKeyColumn]),
      );
      return topLevel;
    }

    // Return children of this block
    final children = _rowCache.values
        .where((row) => row[_parentIdColumn]?.toString() == id)
        .toList();
    children.sort(
      (a, b) => _compareSortKeys(a[_sortKeyColumn], b[_sortKeyColumn]),
    );
    return children;
  }

  @override
  bool getIsCollapsed(Map<String, dynamic> block) {
    final id = getId(block);
    return _collapsedState[id] ?? false;
  }

  @override
  DateTime getCreatedAt(Map<String, dynamic> block) {
    if (block['id'] == _rootId) return DateTime.now();
    final createdAt = block['created_at'];
    if (createdAt is DateTime) return createdAt;
    if (createdAt is String) {
      try {
        return DateTime.parse(createdAt);
      } catch (_) {
        return DateTime.now();
      }
    }
    return DateTime.now();
  }

  @override
  DateTime getUpdatedAt(Map<String, dynamic> block) {
    if (block['id'] == _rootId) return DateTime.now();
    final updatedAt = block['updated_at'];
    if (updatedAt is DateTime) return updatedAt;
    if (updatedAt is String) {
      try {
        return DateTime.parse(updatedAt);
      } catch (_) {
        return DateTime.now();
      }
    }
    return DateTime.now();
  }

  // =========================================================================
  // BlockTreeOps implementation
  // =========================================================================

  @override
  List<Map<String, dynamic>> getTopLevelBlocks() {
    return getChildren(_getRootBlock());
  }

  @override
  bool isDescendantOf(
    Map<String, dynamic> potentialAncestor,
    Map<String, dynamic> block,
  ) {
    final ancestorId = getId(potentialAncestor);
    Map<String, dynamic>? current = block;

    while (current != null) {
      final parentId = current[_parentIdColumn]?.toString();
      if (parentId == null || parentId.isEmpty || parentId == 'null') {
        return false;
      }
      if (parentId == ancestorId) return true;
      current = _rowCache[parentId];
      if (current == null) return false;
    }

    return false;
  }

  @override
  Future<Map<String, dynamic>?> findNextVisibleBlock(
    Map<String, dynamic> block,
  ) async {
    // If not collapsed and has children, return first child
    if (!getIsCollapsed(block)) {
      final children = getChildren(block);
      if (children.isNotEmpty) {
        return children.first;
      }
    }

    // Find next sibling or ancestor's next sibling
    Map<String, dynamic>? current = block;
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
  Future<Map<String, dynamic>?> findPreviousVisibleBlock(
    Map<String, dynamic> block,
  ) async {
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
  Future<Map<String, dynamic>?> findParent(Map<String, dynamic> block) async {
    final id = getId(block);
    if (id == _rootId) return null;

    final parentId = block[_parentIdColumn]?.toString();
    if (parentId == null || parentId.isEmpty || parentId == 'null') {
      return _getRootBlock();
    }

    return _rowCache[parentId];
  }

  @override
  Future<Map<String, dynamic>?> findBlockById(String blockId) async {
    if (blockId == _rootId) return _getRootBlock();
    return _rowCache[blockId];
  }

  @override
  Future<Map<String, dynamic>> getRootBlock() async {
    return _getRootBlock();
  }

  /// Get synthetic root block
  Map<String, dynamic> _getRootBlock() {
    return {
      'id': _rootId,
      'content': '',
      _parentIdColumn: null,
      _sortKeyColumn: 0,
    };
  }

  // =========================================================================
  // BlockMutationOps implementation
  // =========================================================================

  @override
  Future<void> updateBlock(Map<String, dynamic> block, String content) async {
    final id = getId(block);
    if (id == _rootId) return;

    if (_onOperation != null) {
      await _onOperation(_entityName, 'set_field', {
        'id': id,
        'field': 'content',
        'value': content,
      });
    }
  }

  @override
  Future<void> deleteBlock(Map<String, dynamic> block) async {
    final id = getId(block);
    if (id == _rootId) return;

    if (_onOperation != null) {
      await _onOperation(_entityName, 'delete', {'id': id});
    }
  }

  @override
  Future<void> moveBlock(
    Map<String, dynamic> block,
    Map<String, dynamic>? newParent,
    int newIndex,
  ) async {
    final id = getId(block);
    if (id == _rootId) return;

    final newParentId = newParent != null ? getId(newParent) : null;
    final actualParentId = newParentId == _rootId ? null : newParentId;

    // Calculate new sort_key based on siblings
    final siblings = newParent != null
        ? getChildren(newParent)
        : getTopLevelBlocks();

    int newSortKey;
    if (siblings.isEmpty || newIndex >= siblings.length) {
      // Append to end
      if (siblings.isEmpty) {
        newSortKey = 0;
      } else {
        final lastSortKey = siblings.last[_sortKeyColumn];
        newSortKey = (lastSortKey is num ? lastSortKey.toInt() : 0) + 1;
      }
    } else if (newIndex == 0) {
      // Insert at beginning
      final firstSortKey = siblings.first[_sortKeyColumn];
      newSortKey = (firstSortKey is num ? firstSortKey.toInt() : 0) - 1;
    } else {
      // Insert between siblings
      final prevSortKey = siblings[newIndex - 1][_sortKeyColumn];
      final nextSortKey = siblings[newIndex][_sortKeyColumn];
      final prev = prevSortKey is num ? prevSortKey.toInt() : 0;
      final next = nextSortKey is num ? nextSortKey.toInt() : 0;
      newSortKey = (prev + next) ~/ 2;
    }

    if (_onOperation != null) {
      await _onOperation(_entityName, 'move', {
        'id': id,
        'parent_id': actualParentId,
        'sort_key': newSortKey,
      });
    }
  }

  @override
  Future<void> toggleCollapse(Map<String, dynamic> block) async {
    final id = getId(block);
    if (id == _rootId) return;

    _collapsedState[id] = !(_collapsedState[id] ?? false);
    _emitChange();
  }

  @override
  Future<void> addChildBlock(
    Map<String, dynamic> parent,
    Map<String, dynamic> child,
  ) async {
    final children = getChildren(parent);
    await moveBlock(child, parent, children.length);
  }

  @override
  Future<void> addTopLevelBlock(Map<String, dynamic> block) async {
    final topLevel = getTopLevelBlocks();
    await moveBlock(block, _getRootBlock(), topLevel.length);
  }

  @override
  Future<String> splitBlock(
    Map<String, dynamic> block,
    int cursorPosition,
  ) async {
    final id = getId(block);
    if (id == _rootId) return id;

    final content = getContent(block);
    final beforeCursor = content.substring(0, cursorPosition);
    final afterCursor = content.substring(cursorPosition);

    // Update current block with content before cursor
    await updateBlock(block, beforeCursor);

    // Create new block with content after cursor
    if (_onOperation != null) {
      final parentId = block[_parentIdColumn]?.toString();
      await _onOperation(_entityName, 'create', {
        'parent_id': parentId,
        'content': afterCursor,
      });
    }

    // Return a placeholder ID - the actual ID will come from CDC stream
    return '${id}_split';
  }

  @override
  Future<void> indentBlock(Map<String, dynamic> block) async {
    final parent = await findParent(block);
    if (parent == null || getId(parent) == _rootId) return;

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
  Future<void> outdentBlock(Map<String, dynamic> block) async {
    final parent = await findParent(block);
    if (parent == null || getId(parent) == _rootId) return;

    final grandparent = await findParent(parent);
    if (grandparent == null) return;

    final parentSiblings = getChildren(grandparent);
    final parentIndex = parentSiblings.indexWhere(
      (b) => getId(b) == getId(parent),
    );

    if (parentIndex == -1) return;

    // Move to be right after the parent
    await moveBlock(block, grandparent, parentIndex + 1);
  }

  // =========================================================================
  // BlockCreationOps implementation
  // =========================================================================

  @override
  Map<String, dynamic> copyWith(
    Map<String, dynamic> block, {
    String? content,
    List<Map<String, dynamic>>? children,
    bool? isCollapsed,
  }) {
    final newBlock = Map<String, dynamic>.from(block);
    if (content != null) newBlock['content'] = content;
    if (isCollapsed != null) {
      final id = getId(block);
      _collapsedState[id] = isCollapsed;
    }
    // Note: children are computed dynamically, so we don't store them
    return newBlock;
  }

  @override
  Map<String, dynamic> create({
    String? id,
    required String content,
    List<Map<String, dynamic>>? children,
    bool? isCollapsed,
  }) {
    // Return root block as placeholder for synchronous contexts
    return _getRootBlock();
  }

  @override
  Future<String> createTopLevelBlockAsync({
    required String content,
    String? id,
  }) async {
    if (_onOperation != null) {
      await _onOperation(_entityName, 'create', {
        'parent_id': null,
        'content': content,
        if (id != null) 'id': id,
      });
    }
    // Return placeholder - actual ID will come from CDC stream
    return id ?? 'new_${DateTime.now().millisecondsSinceEpoch}';
  }

  @override
  Future<String> createChildBlockAsync({
    required Map<String, dynamic> parent,
    required String content,
    String? id,
    int? index,
  }) async {
    final parentId = getId(parent);
    final actualParentId = parentId == _rootId ? null : parentId;

    if (_onOperation != null) {
      await _onOperation(_entityName, 'create', {
        'parent_id': actualParentId,
        'content': content,
        if (id != null) 'id': id,
      });
    }
    // Return placeholder - actual ID will come from CDC stream
    return id ?? 'new_${DateTime.now().millisecondsSinceEpoch}';
  }

  // =========================================================================
  // BlockOps changeStream
  // =========================================================================

  @override
  Stream<Map<String, dynamic>> get changeStream => _changeController.stream;

  /// Emit a change event with the current root block
  void _emitChange() {
    _changeController.add(_getRootBlock());
  }

  /// Update row cache (called from ReactiveQueryWidget when CDC events arrive)
  void updateRowCache(String rowId, Map<String, dynamic>? rowData) {
    if (rowData == null) {
      _rowCache.remove(rowId);
      _collapsedState.remove(rowId);
    } else {
      _rowCache[rowId] = rowData;
    }
    _emitChange();
  }

  /// Dispose resources
  void dispose() {
    _changeController.close();
  }
}
