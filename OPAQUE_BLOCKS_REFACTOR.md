# Opaque Blocks Architecture Refactoring

**Status:** In Progress (Phases 1-2 Complete)
**Started:** 2025-10-31
**Location:** `/Users/martin/Workspaces/pkm/holon/`

---

## Goal

**Eliminate 329 lines of FRB boilerplate** by using RustOpaque `Block` types directly in the UI, with zero conversion overhead.

### The Problem We're Solving

Currently, there's significant boilerplate converting between Rust blocks and Flutter UI:

```
Rust Backend (Flat Structure)
  Block { children: Vec<String> }  ← IDs only
  ↓ (329 lines of conversion in outliner_adapter.dart)
Dart UI (Hierarchical Structure)
  Block { children: List<Block> }  ← Full nested objects
```

**Issues:**
- 329 lines of conversion code in `outliner_adapter.dart`
- Recursive hierarchy construction on every read
- Double memory representation (flat Rust + hierarchical Dart)
- ID handling exposed throughout UI layer

### The Solution

Use RustOpaque blocks directly with a flat-structure architecture:

```
Rust Backend (Flat Structure)
  Block { children: Vec<String> }
  CoreOperations trait with block-based methods
  ↓ (FFI - RustOpaque, zero conversion)
Dart State Layer
  Map<String, rust.Block> (cache)
  RustBlockOps (thin adapter, ~30 lines)
  ↓ (opaque blocks)
Dart UI Layer
  Works with rust.Block directly
  NEVER sees or handles IDs
```

**Benefits:**
- ✅ **~180 line reduction** (329 → ~150 total)
- ✅ **Zero conversion overhead** - RustOpaque used directly
- ✅ **UI never handles IDs** - complete abstraction
- ✅ **Sync rendering** - cache enables synchronous lookups
- ✅ **Single source of truth** - Rust backend only

---

## Architecture Overview

### Key Insights

1. **Both sides are fundamentally flat**:
   - Rust: `Block { children: Vec<String> }` (IDs)
   - CoreOperations: flat structure with ID→Block lookups
   - Loro CRDT: fractional indexing (inherently flat)

2. **We control outliner_flutter**:
   - Can refactor it to work with flat structures
   - No need to force hierarchical assumptions

3. **RustOpaque eliminates serialization**:
   - Blocks stay in Rust memory
   - Dart holds opaque references
   - Zero-copy across FFI boundary

### Data Flow

```
┌─────────────────────────────────────────────┐
│ Rust: CoreOperations                        │
│ - get_all_blocks() → Vec<Block>             │
│ - update_block_by_ref(block, content) ✅    │
│ - move_block_by_ref(block, parent, after) ✅│
│ - delete_block_by_ref(block) ✅             │
│ - watch_changes_since() → Stream            │
└──────────────────┬──────────────────────────┘
                   │ FFI (RustOpaque)
                   ↓
┌─────────────────────────────────────────────┐
│ Dart: BlockState (Riverpod)                 │
│ - Map<String, rust.Block> _blockMap         │
│ - RustBlockOps(blockMap)                    │
│ - Synced via change stream                  │
│ - getRootBlocks() → List<rust.Block>        │
└──────────────────┬──────────────────────────┘
                   │ rust.Block (opaque)
                   ↓
┌─────────────────────────────────────────────┐
│ Dart: RustBlockOps (adapter)                │
│ - getChildren(block) → List<rust.Block>     │
│ - getContent(block) → String                │
│ - NO explicit ID handling in mutations      │
└──────────────────┬──────────────────────────┘
                   │ rust.Block (opaque)
                   ↓
┌─────────────────────────────────────────────┐
│ Dart: outliner_flutter Widgets              │
│ - BlockWidget(block: rust.Block)            │
│ - Uses ops.getChildren() for traversal      │
│ - NEVER sees IDs (except for widget keys)   │
└─────────────────────────────────────────────┘
```

---

## Completed Work

### ✅ Phase 1: Rust Backend Extensions

**Date:** 2025-10-31
**Files Modified:**
- `crates/holon/src/api/repository.rs`

**Changes:**

Added 3 block-based methods to `CoreOperations` trait:

```rust
// Before: ID-based only
async fn update_block(&self, id: &str, content: String) -> Result<(), ApiError>;

// After: Both ID-based AND block-based
async fn update_block(&self, id: &str, content: String) -> Result<(), ApiError>;

async fn update_block_by_ref(&self, block: &Block, content: String) -> Result<(), ApiError> {
    self.update_block(&block.id, content).await  // Default impl
}
```

**New Methods:**
1. `update_block_by_ref(&self, block: &Block, content: String)`
2. `delete_block_by_ref(&self, block: &Block)`
3. `move_block_by_ref(&self, block: &Block, new_parent: Option<&Block>, after: Option<&Block>)`

All methods have **default implementations** that extract IDs internally.

**Testing:**
- ✅ `cargo check` - compiles successfully
- ✅ `cargo test` - 127 tests passed, 0 failed
- ⚠️ 2 pre-existing stress test failures (unrelated)

**Location:** `crates/holon/src/api/repository.rs:223-291`

---

### ✅ Phase 2: Flutter-Rust-Bridge Layer

**Date:** 2025-10-31
**Files Modified:**
- `frontends/flutter/rust/src/api/repository.rs`
- Auto-generated FRB bindings

**Changes:**

Added block-based wrapper methods to `RustDocumentRepository`:

```rust
/// Update block content using a block reference.
pub async fn update_block_by_ref(&self, block: Block, content: String) -> Result<(), ApiError> {
    let backend = self.backend.write().await;
    backend.update_block_by_ref(&block, content).await.map_err(Into::into)
}

pub async fn delete_block_by_ref(&self, block: Block) -> Result<(), ApiError> {
    // ...
}

pub async fn move_block_by_ref(
    &self,
    block: Block,
    new_parent: Option<Block>,
    after: Option<Block>,
) -> Result<(), ApiError> {
    let backend = self.backend.write().await;
    backend
        .move_block_by_ref(&block, new_parent.as_ref(), after.as_ref())
        .await
        .map_err(Into::into)
}
```

**Generated Dart API:**

```dart
// lib/src/rust/api/repository.dart

abstract class RustDocumentRepository {
  // Original ID-based methods (unchanged)
  Future<void> updateBlock({required String id, required String content});

  // NEW: Block-based methods
  Future<void> updateBlockByRef({required Block block, required String content});
  Future<void> deleteBlockByRef({required Block block});
  Future<void> moveBlockByRef({
    required Block block,
    Block? newParent,
    Block? after,
  });
}
```

**FRB Codegen:**
- ✅ `flutter_rust_bridge_codegen generate` - successful
- ✅ Dart signatures correctly generated with `Block` type (RustOpaque)
- ✅ Optional parameters handled correctly

**Location:** `frontends/flutter/rust/src/api/repository.rs:136-179`

---

## Remaining Work

### 🔄 Phase 3: Refactor outliner_flutter Library

**Status:** Not Started
**Location:** `/Users/martin/Workspaces/pkm/outliner-flutter/`
**Estimated Effort:** 2-3 hours

#### Objectives

Make outliner_flutter work with **flat block structures** where children are IDs, not nested objects.

#### Changes Required

**3.1: Update `BlockOps` Interface**

File: `lib/core/block_ops.dart`

```dart
// CURRENT (hierarchical)
abstract class BlockOps<T> {
  List<T> getChildren(T block);  // Returns nested blocks
}

// NEW (flat-aware)
abstract class BlockOps<T> {
  List<T> getChildren(T block);  // Still returns blocks
  String getId(T block);          // For internal use (widget keys)
  T? getParent(T block);          // Navigate to parent
  // ... other methods unchanged
}
```

**Key Point:** `getChildren()` still returns `List<T>`, but the implementation will resolve IDs internally using a block map.

**3.2: Update `OutlinerRepository` Interface** (Optional)

File: `lib/repositories/outliner_repository.dart`

Consider whether repository methods should accept `T` blocks instead of IDs:

```dart
// CURRENT
Future<void> updateBlock(String id, String content);

// NEW (optional)
Future<void> updateBlock(T block, String content);
```

**3.3: Update Widgets to Accept `BlockOps`**

Files:
- `lib/widgets/outliner_list_view.dart`
- `lib/widgets/block_widget.dart`
- `lib/widgets/draggable_block_widget.dart`

```dart
// CURRENT
class BlockWidget extends HookConsumerWidget {
  final Block block;  // Freezed type
  // ...
}

// NEW (generic)
class BlockWidget<T> extends HookConsumerWidget {
  final T block;           // Any type
  final BlockOps<T> ops;   // For field access
  // ...

  Widget build(BuildContext context, WidgetRef ref) {
    final content = ops.getContent(block);
    final children = ops.getChildren(block);  // Resolved by ops
    // ...
  }
}
```

**3.4: Update `FreezedBlockOps` Implementation**

File: `lib/core/freezed_block_ops.dart`

Ensure backwards compatibility for existing users:

```dart
class FreezedBlockOps implements BlockOps<Block> {
  const FreezedBlockOps();

  @override
  String getId(Block block) => block.id;

  @override
  List<Block> getChildren(Block block) => block.children;

  @override
  Block? getParent(Block block) {
    // Need to implement parent lookup (may require repository reference)
    throw UnimplementedError('Requires repository context');
  }

  // ... other methods
}
```

**3.5: Update Tests and Examples**

- Run `flutter test` in outliner-flutter
- Update example app if needed
- Verify backwards compatibility

**3.6: Update Documentation**

- Update `README.md` with flat structure examples
- Update `core/README.md` with RustOpaque guidance

---

### 🔄 Phase 4: Create RustBlockOps Adapter

**Status:** Not Started
**Location:** `/Users/martin/Workspaces/pkm/holon/frontends/flutter/lib/data/`
**Estimated Effort:** 30 minutes

#### File to Create

`lib/data/rust_block_ops.dart` (~30 lines)

#### Implementation

```dart
import 'package:outliner_view/core/block_ops.dart';
import '../src/rust/api/types.dart' as rust;

/// BlockOps implementation for RustOpaque blocks.
///
/// This adapter allows outliner_flutter to work with Rust blocks
/// without any conversion. It maintains a reference to the block map
/// for resolving child IDs synchronously.
class RustBlockOps implements BlockOps<rust.Block> {
  /// Reference to the global block map for ID resolution
  final Map<String, rust.Block> _blockMap;

  /// Local UI state for collapsed blocks (not persisted)
  final Map<String, bool> _collapsedState = {};

  RustBlockOps(this._blockMap);

  @override
  String getId(rust.Block block) => block.id;

  @override
  String getContent(rust.Block block) => block.content;

  @override
  List<rust.Block> getChildren(rust.Block block) {
    // Resolve child IDs to blocks using internal map
    return block.children
        .map((id) => _blockMap[id])
        .whereType<rust.Block>()
        .toList();
  }

  @override
  rust.Block? getParent(rust.Block block) {
    final parentId = block.parentId;
    return parentId.isEmpty ? null : _blockMap[parentId];
  }

  @override
  bool getIsCollapsed(rust.Block block) {
    return _collapsedState[block.id] ?? false;
  }

  @override
  DateTime getCreatedAt(rust.Block block) {
    return DateTime.fromMillisecondsSinceEpoch(
      block.metadata.createdAt.toInt(),
    );
  }

  @override
  DateTime getUpdatedAt(rust.Block block) {
    return DateTime.fromMillisecondsSinceEpoch(
      block.metadata.updatedAt.toInt(),
    );
  }

  /// Toggle collapsed state (UI-only, not persisted)
  void toggleCollapse(rust.Block block) {
    _collapsedState[block.id] = !getIsCollapsed(block);
  }

  void setCollapsed(rust.Block block, bool collapsed) {
    _collapsedState[block.id] = collapsed;
  }
}
```

**Key Features:**
- **Simple field accessors** - no complex logic
- **Internal ID resolution** - UI never sees IDs
- **Collapsed state tracking** - local Map (not persisted)
- **Zero conversion** - just field access

---

### 🔄 Phase 5: Implement BlockState Management

**Status:** Not Started
**Location:** `/Users/martin/Workspaces/pkm/holon/frontends/flutter/lib/data/`
**Estimated Effort:** 1 hour

#### Files to Create

1. `lib/data/block_state.dart` (~30 lines)
2. `lib/data/block_state_notifier.dart` (~80 lines)

#### 5.1: BlockState Class

```dart
/// Immutable state containing all blocks and operations.
class BlockState {
  final Map<String, rust.Block> _blockMap;
  final RustBlockOps ops;

  BlockState(this._blockMap) : ops = RustBlockOps(_blockMap);

  /// Get all root-level blocks
  List<rust.Block> getRootBlocks() {
    return _blockMap.values
        .where((b) => b.parentId == rust.ROOT_PARENT_ID)
        .toList();
  }

  /// Internal: Get block by ID
  rust.Block? getBlock(String id) => _blockMap[id];
}
```

#### 5.2: BlockStateNotifier Class

```dart
/// State notifier that manages blocks and syncs with Rust backend.
class BlockStateNotifier extends StateNotifier<BlockState?> {
  final RustBlockRepository _repo;
  StreamSubscription? _changeSub;

  BlockStateNotifier(this._repo) : super(null) {
    _initialize();
  }

  Future<void> _initialize() async {
    try {
      // 1. Load all blocks at once
      final traversal = await rust.traversalAllButRoot();
      final allBlocks = await _repo.getAllBlocks(traversal);
      final map = {for (var b in allBlocks) b.id: b};
      state = BlockState(map);

      // 2. Subscribe to change stream
      _changeSub = _repo.changes.listen(_handleChange);
    } catch (e) {
      debugPrint('Error initializing block state: $e');
    }
  }

  void _handleChange(BlockChangeEvent change) {
    if (state == null) return;

    final currentMap = state!._blockMap;

    if (change is BlockCreatedEvent) {
      // Add new block to map
      state = BlockState({...currentMap, change.block.id: change.block});

    } else if (change is BlockUpdatedEvent) {
      // Refetch updated block
      _repo.getBlock(change.id).then((block) {
        if (block != null && mounted) {
          state = BlockState({...currentMap, change.id: block});
        }
      });

    } else if (change is BlockDeletedEvent) {
      // Remove from map
      final newMap = Map<String, rust.Block>.from(currentMap);
      newMap.remove(change.id);
      state = BlockState(newMap);

    } else if (change is BlockMovedEvent) {
      // Refetch affected blocks
      _refetchAffectedBlocks(change.id, change.newParent);
    }
  }

  Future<void> _refetchAffectedBlocks(String movedId, String? newParentId) async {
    // Fetch moved block and its parents (their child lists changed)
    final idsToFetch = [movedId];
    if (newParentId != null) idsToFetch.add(newParentId);

    final blocks = await _repo.getBlocks(idsToFetch);

    if (mounted && state != null) {
      final newMap = Map<String, rust.Block>.from(state!._blockMap);
      for (var block in blocks) {
        newMap[block.id] = block;
      }
      state = BlockState(newMap);
    }
  }

  // === Mutation Methods (use block-based API) ===

  Future<void> updateBlock(rust.Block block, String content) async {
    await _repo.backend.updateBlockByRef(block: block, content: content);
    // Change stream will update state
  }

  Future<void> deleteBlock(rust.Block block) async {
    await _repo.backend.deleteBlockByRef(block: block);
    // Change stream will update state
  }

  Future<void> moveBlock(
    rust.Block block,
    rust.Block? newParent,
    rust.Block? after,
  ) async {
    await _repo.backend.moveBlockByRef(
      block: block,
      newParent: newParent,
      after: after,
    );
    // Change stream will update state
  }

  @override
  void dispose() {
    _changeSub?.cancel();
    super.dispose();
  }
}
```

**Key Features:**
- **Single source of truth** - Rust backend
- **Cache kept in sync** - via change stream
- **Mutations use block-based API** - no ID extraction!
- **Simple state updates** - immutable map replacement

---

### 🔄 Phase 6: Update Main Application

**Status:** Not Started
**Location:** `/Users/martin/Workspaces/pkm/holon/frontends/flutter/`
**Estimated Effort:** 30 minutes

#### 6.1: Update Providers

File: `lib/main.dart`

```dart
// REMOVE: Old repository provider
// final outlinerRepositoryProvider = ...

// ADD: New state provider
final blockStateProvider = StateNotifierProvider<BlockStateNotifier, BlockState?>(
  (ref) {
    final rustRepo = ref.watch(rustBlockRepositoryProvider);
    return BlockStateNotifier(rustRepo);
  },
);
```

#### 6.2: Update OutlinerView Usage

File: `lib/ui/outliner_view.dart` or `lib/main.dart`

```dart
// CURRENT
const OutlinerListView()

// NEW
Consumer(
  builder: (context, ref, _) {
    final state = ref.watch(blockStateProvider);
    if (state == null) return CircularProgressIndicator();

    return OutlinerListView<rust.Block>(
      rootBlocks: state.getRootBlocks(),
      ops: state.ops,
      onUpdateBlock: (block, content) {
        ref.read(blockStateProvider.notifier).updateBlock(block, content);
      },
      onDeleteBlock: (block) {
        ref.read(blockStateProvider.notifier).deleteBlock(block);
      },
      onMoveBlock: (block, parent, after) {
        ref.read(blockStateProvider.notifier).moveBlock(block, parent, after);
      },
    );
  },
)
```

#### 6.3: Update Widget Builders

Files:
- `lib/ui/widgets/block_builder.dart`
- `lib/ui/widgets/bullet_builder.dart`

Change from `outliner.Block` to `rust.Block`:

```dart
// CURRENT
import 'package:outliner_view/outliner_view.dart' show Block;

Widget buildBlockContent(BuildContext context, Block block, bool isEditing) {
  // ...
}

// NEW
import '../src/rust/api/types.dart' as rust;

Widget buildBlockContent(BuildContext context, rust.Block block, bool isEditing) {
  final content = block.content;  // Direct field access
  // ...
}
```

#### 6.4: Delete Obsolete Files

**🗑️ DELETE:** `lib/data/outliner_adapter.dart` (329 lines)

This is the big win! All that conversion code is gone.

**Verify:** No remaining imports of `outliner_adapter.dart`

```bash
grep -r "outliner_adapter" lib/
# Should return nothing
```

---

### 🔄 Phase 7: Testing & Validation

**Status:** Not Started
**Estimated Effort:** 1-2 hours

#### 7.1: Static Analysis

```bash
flutter analyze
# Should show no errors
```

#### 7.2: Build and Run

```bash
flutter run -d macos
```

**Manual Test Checklist:**
- [ ] App launches without errors
- [ ] Initial blocks load correctly
- [ ] Create new block works
- [ ] Edit block content works
- [ ] Move block (drag-and-drop) works
- [ ] Delete block works
- [ ] Collapse/expand blocks works
- [ ] Keyboard shortcuts work (Tab, Enter, etc.)

#### 7.3: Change Stream Verification

If P2P is enabled:
- [ ] Open two instances
- [ ] Make changes in one instance
- [ ] Verify changes appear in other instance
- [ ] Verify no echo (local changes don't bounce back)

#### 7.4: Performance Testing

- [ ] Create document with 1000+ blocks
- [ ] Verify initial load time < 2 seconds
- [ ] Verify scrolling is smooth (60 FPS)
- [ ] Verify updates don't cause full tree rebuilds

#### 7.5: Memory Profiling

```bash
flutter run --profile
# Use DevTools to check memory usage
```

Verify:
- [ ] No memory leaks
- [ ] Block map size reasonable
- [ ] No duplicate block storage

---

## Code Metrics

### Before (Current)

```
outliner_adapter.dart:     329 lines  (all conversion code)
Total boilerplate:         329 lines
```

### After (Target)

```
rust_block_ops.dart:        30 lines  (thin adapter)
block_state.dart:           30 lines  (state container)
block_state_notifier.dart:  80 lines  (state management)
Total new code:            140 lines

Net change:  329 - 140 = -189 lines (-57% reduction!)
```

Plus:
- ✅ Zero conversion overhead
- ✅ UI never handles IDs
- ✅ Sync rendering
- ✅ Single source of truth

---

## Testing Strategy

### Unit Tests

**outliner_flutter:**
- Test `BlockOps` interface with mock implementations
- Test flat structure traversal
- Test widget rendering with generic types

**holon:**
- Test `RustBlockOps` field accessors
- Test `BlockStateNotifier` change handling
- Test state updates are immutable

### Integration Tests

- Test full create/read/update/delete cycle
- Test drag-and-drop operations
- Test change stream synchronization
- Test concurrent operations (if P2P enabled)

### Performance Tests

- Benchmark initial load time (1000+ blocks)
- Benchmark update latency
- Measure memory usage
- Profile UI rendering (DevTools)

---

## Rollback Plan

If critical issues arise:

### Quick Rollback (Git)

```bash
# Revert to before refactoring
git revert <commit-range>
```

### Feature Flag Approach

Keep both implementations temporarily:

```dart
// In main.dart
const USE_OPAQUE_BLOCKS = false;  // Toggle here

final provider = USE_OPAQUE_BLOCKS
  ? blockStateProvider
  : outlinerRepositoryProvider;
```

### Gradual Migration

1. Keep `outliner_adapter.dart` until fully validated
2. Run both implementations side-by-side
3. Compare results for consistency
4. Delete old code only after confidence

---

## Questions / Decisions

### Open Questions

1. **Collapsed state persistence**: Should it be saved to Rust backend?
   - Current: UI-only (Map in `RustBlockOps`)
   - Alternative: Store in Rust metadata

2. **Lazy loading**: Should we support for huge documents (10k+ blocks)?
   - Current: Load all blocks at once
   - Alternative: Load on-demand with `Traversal` filters

3. **BlockOps.getId()**: Should it be removed from public API?
   - Current: Public (needed for widget keys)
   - Alternative: Internal-only with extension method

4. **Parent tracking**: Should blocks have bidirectional links?
   - Current: Only child→parent (via `parentId`)
   - Alternative: Parent also tracks children (redundant but faster)

### Decisions Made

- ✅ Use block-based methods in `CoreOperations` (Phases 1-2)
- ✅ Keep ID-based methods for backwards compatibility
- ✅ RustOpaque blocks throughout (no Freezed conversion)
- ✅ Sync rendering via Dart-side cache
- ✅ Change stream for cache updates
- ✅ Collapsed state stays UI-only (for now)

---

## Context for AI Sessions

### To Resume Work

1. **Read this document**: Full context on goals and progress
2. **Check completed phases**: Phases 1-2 done, 3-7 remaining
3. **Current location**: `/Users/martin/Workspaces/pkm/holon/frontends/flutter/`
4. **Next task**: Phase 3 requires working in `/Users/martin/Workspaces/pkm/outliner-flutter/`
5. **Reference implementation**: See code templates in each phase section

### Key Files to Understand

**Rust Backend:**
- `crates/holon/src/api/repository.rs` - CoreOperations trait
- `crates/holon/src/api/types.rs` - Block structure
- `frontends/flutter/rust/src/api/repository.rs` - FFI layer

**Dart Frontend:**
- `lib/src/rust/api/repository.dart` - Generated FRB bindings
- `lib/data/rust_block_repository.dart` - Repository wrapper (to be replaced)
- `lib/data/outliner_adapter.dart` - **329 lines to DELETE** ✨

**outliner_flutter Library:**
- `lib/core/block_ops.dart` - Interface to refactor
- `lib/widgets/outliner_list_view.dart` - Main widget
- `lib/widgets/block_widget.dart` - Individual block rendering

### Architecture Principles

1. **Flat is fundamental** - both Rust and outliner_flutter should embrace flat structures
2. **IDs are internal** - UI works with opaque blocks, never extracts IDs
3. **Block-based mutations** - pass blocks, not IDs, for ergonomic FFI
4. **Cache for sync** - Dart-side Map enables synchronous lookups
5. **Stream for sync** - change stream keeps cache up-to-date

---

## Success Criteria

### Functional Requirements

- [ ] All existing features work (create, edit, delete, move, collapse)
- [ ] No performance regression
- [ ] No memory leaks
- [ ] Change stream synchronization works
- [ ] Keyboard shortcuts work

### Code Quality Requirements

- [ ] `flutter analyze` shows no errors
- [ ] All tests pass
- [ ] Code is well-documented
- [ ] No compiler warnings

### Quantitative Goals

- [ ] **Code reduction**: 329 → ~140 lines (-57%)
- [ ] **Initial load**: < 2 seconds for 1000 blocks
- [ ] **Update latency**: < 50ms
- [ ] **Memory**: No increase vs baseline
- [ ] **Test coverage**: > 80%

---

## Timeline

**Completed:**
- Phase 1: 1 hour (2025-10-31)
- Phase 2: 30 minutes (2025-10-31)

**Estimated Remaining:**
- Phase 3: 2-3 hours (outliner_flutter refactor)
- Phase 4: 30 minutes (RustBlockOps)
- Phase 5: 1 hour (BlockState)
- Phase 6: 30 minutes (main app)
- Phase 7: 1-2 hours (testing)

**Total Remaining: ~6-8 hours**

---

## Related Documentation

- `CLAUDE.md` - Project instructions for AI agents
- `README.md` - Project overview
- `outliner-flutter/README.md` - Library documentation
- `outliner-flutter/CHANGELOG.md` - Recent architectural changes

---

## Notes

### Why This Approach Works

1. **RustOpaque** eliminates serialization overhead
2. **Flat structures** match CRDT reality (Loro's fractional indexing)
3. **Block-based API** makes FFI ergonomic (no ID extraction in Dart)
4. **Sync cache** enables fast UI rendering (no async traversal)
5. **Change stream** keeps everything in sync automatically

### Lessons Learned

1. Don't force hierarchical assumptions on flat data structures
2. Leverage type classes (`BlockOps`) for clean abstraction
3. Use the platform's strengths (RustOpaque for zero-copy)
4. Keep mutations simple (pass blocks, not IDs)
5. Cache strategically (Dart-side map for sync access)

---

**Last Updated:** 2025-10-31
**Document Version:** 1.0
**Status:** Phases 1-2 Complete ✅
