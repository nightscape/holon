# Spec 0001: Flutter Frontend with outliner-flutter

## Status
**Updated with Agent Feedback** - GPT-5 and Gemini-2.5-Pro reviewed, recommendations incorporated

## Overview
Add a Flutter-based frontend to holon that provides a LogSeq-like block-based editing experience using the outliner-flutter library. This will run as a parallel frontend option alongside the existing Tauri implementation, using Flutter-Rust-Bridge for backend communication.

## Background

### Current State
- Rust backend with Loro CRDT for collaborative document editing
- Iroh P2P networking for peer-to-peer synchronization
- Tauri-based desktop frontend
- Architecture supports collaborative editing with eventual consistency

### Motivation
- **Set-based design**: Experiment with multiple frontend technologies (Tauri, Flutter, potential web stack) to determine optimal approach
- **Loose coupling**: Establish clean interface boundaries that work across different UI technologies
- **Mobile support**: Enable Android and desktop platforms with native performance
- **Specialized UI**: Leverage outliner-flutter's block-based editing optimized for hierarchical note-taking

## Goals

### Primary Goals
1. Create a Flutter frontend with LogSeq-like hierarchical block editing
2. Implement repository pattern calling Rust backend via Flutter-Rust-Bridge
3. Support block operations: create, read, update, delete, move, nest
4. Enable P2P collaboration control (start instance, connect to peer)
5. Build UI components: main outliner view, configuration, quick actions, page lists
6. Support Android and Desktop platforms initially

### Non-Goals
1. Replace or modify existing Tauri frontend
2. Implement P2P logic in Flutter (backend responsibility)
3. iOS or Web support in initial implementation
4. Real-time cursor presence (future enhancement)

## Design

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Flutter Frontend                         │
│                                                             │
│  ┌─────────────────┐  ┌──────────────────────────────┐    │
│  │ UI Layer        │  │ Repository Layer             │    │
│  │ - Outliner View │  │ - BlockRepository            │    │
│  │ - Config View   │──▶│ - Calls Rust via FRB        │    │
│  │ - Quick Actions │  │ - Listens to change stream   │    │
│  │ - Page Lists    │  │                              │    │
│  └─────────────────┘  └──────────────────────────────┘    │
│                              │                             │
└──────────────────────────────┼─────────────────────────────┘
                               │ Flutter-Rust-Bridge
┌──────────────────────────────┼─────────────────────────────┐
│                              ▼                             │
│  ┌──────────────────────────────────────────────────┐     │
│  │ Backend API Layer (New)                          │     │
│  │ - Block CRUD operations                          │     │
│  │ - Hierarchical structure management              │     │
│  │ - Change notification stream                     │     │
│  │ - P2P connection control                         │     │
│  └──────────────────────────────────────────────────┘     │
│                              │                             │
│  ┌──────────────────────────┼───────────────────────────┐ │
│  │ Existing Backend (Adapt)                            │ │
│  │ - CollaborativeDoc                                  │ │
│  │ - Loro CRDT with hierarchical list support          │ │
│  │ - Iroh P2P networking                               │ │
│  └─────────────────────────────────────────────────────┘ │
│                    Rust Backend                           │
└───────────────────────────────────────────────────────────┘
```

### Component Design

#### 1. Backend API Layer (Rust)

**Purpose**: Provide a clean, technology-agnostic interface for frontend communication.

**Location**: API module within main crate for cross-frontend consistency
- **Shared Types**: `crates/holon/src/api/types.rs` - Core data types used by all frontends
- **Domain Service**: `crates/holon/src/api/repository.rs` - `DocumentRepository` trait and implementations
- **Flutter Adapter**: `frontends/flutter/rust/src/bridge.rs` - FRB-specific bindings using shared types
- **Tauri Adapter**: `src-tauri/src/commands.rs` - Tauri commands using shared types

**Interface** (`crates/holon/src/api/types.rs`):
```rust
// Core data types
pub struct Block {
    pub id: String,  // URI format: "local://<uuid-v4>" or "todoist://task/12345"
    pub parent_id: String,  // Parent block ID (ROOT_PARENT_ID for the root block)
    pub content: String,
    pub children: Vec<String>,  // IDs of child blocks
    pub metadata: BlockMetadata,
}

// Sentinel value for the root block's parent
pub const ROOT_PARENT_ID: &str = "__root_parent__";

pub struct BlockMetadata {
    pub created_at: i64,
    pub updated_at: i64,
    // NOTE: UI state like 'collapsed' is stored per-client locally in Flutter,
    // not in the CRDT to avoid cross-user UI churn
}

// Structured error types
pub enum ApiError {
    BlockNotFound { id: String },
    DocumentNotFound { doc_id: String },
    CyclicMove { id: String, target_parent: String },
    InvalidOperation { message: String },
    NetworkError { message: String },
    InternalError { message: String },
}

// Repository-style API (technology-agnostic domain service)
pub struct DocumentRepository {
    doc: CollaborativeDoc,
    // Change notification system
}

pub struct SubscriptionHandle {
    // Opaque handle for managing subscription lifecycle
}

impl DocumentRepository {
    // Document lifecycle
    pub async fn create_new(doc_id: String) -> Result<Self, ApiError>;
    pub async fn open_existing(doc_id: String) -> Result<Self, ApiError>;
    pub async fn dispose(self) -> Result<(), ApiError>;

    // Single-block operations
    pub async fn get_block(&self, id: &str) -> Result<Block, ApiError>;
    pub async fn get_root_blocks(&self) -> Result<Vec<String>, ApiError>;
    pub async fn list_children(&self, parent_id: &str) -> Result<Vec<String>, ApiError>;
    pub async fn create_block(&self, parent_id: Option<String>, content: String, id: Option<String>) -> Result<Block, ApiError>;
    // id parameter: None = generate "local://<uuid-v4>", Some = use provided URI (e.g., "todoist://task/123")
    pub async fn update_block(&self, id: &str, content: String) -> Result<(), ApiError>;
    pub async fn delete_block(&self, id: &str) -> Result<(), ApiError>;

    // Anchor-based move (more CRDT-friendly than index-based)
    // after=None means insert at start, after=Some(id) means insert after that sibling
    pub async fn move_block(&self, id: &str, new_parent: Option<String>, after: Option<String>) -> Result<(), ApiError>;

    // Batch operations (critical for performance)
    pub async fn get_blocks(&self, ids: Vec<String>) -> Result<Vec<Block>, ApiError>;
    pub async fn create_blocks(&self, blocks: Vec<NewBlock>) -> Result<Vec<Block>, ApiError>;
    pub async fn delete_blocks(&self, ids: Vec<String>) -> Result<(), ApiError>;

    // Change notifications using StreamSink pattern (FRB best practice)
    // The stream immediately emits Created events for all existing blocks, then streams future changes
    // This prevents race conditions without needing a separate get_initial_state() call
    pub fn watch_changes(&self, sink: StreamSink<BlockChange>) -> Result<SubscriptionHandle, ApiError>;
    pub fn unsubscribe(&self, handle: SubscriptionHandle) -> Result<(), ApiError>;

    // P2P operations
    pub async fn get_node_id(&self) -> String;
    pub async fn connect_to_peer(&self, peer_node_id: String) -> Result<(), ApiError>;
    pub async fn accept_connections(&self) -> Result<(), ApiError>;
}

// For batch creation
pub struct NewBlock {
    pub parent_id: Option<String>,
    pub content: String,
    pub after: Option<String>,  // Insert position relative to sibling
    pub id: Option<String>,      // None = generate local URI, Some = use provided URI
}

// Change notification with origin tracking
pub enum ChangeOrigin {
    Local,   // Change initiated by this client
    Remote,  // Change from P2P sync
}

pub enum BlockChange {
    Created { block: Block, origin: ChangeOrigin },
    Updated { id: String, content: String, origin: ChangeOrigin },  // Character-level text changes
    Deleted { id: String, origin: ChangeOrigin },
    Moved { id: String, new_parent: Option<String>, after: Option<String>, origin: ChangeOrigin },
}

// Note: Updated events include full content for simplicity. Initial implementation streams
// character-level changes. If performance becomes an issue, we can optimize to delta-based
// updates using Loro's text diff capabilities.
```

**Design Rationale**:
- Repository pattern provides familiar abstraction for frontend developers
- Block-centric API hides Loro/CRDT implementation details
- **StreamSink pattern** for change notifications is FRB best practice (more robust than returning Stream)
- **Stream includes initial state**: `watch_changes()` emits Created events for all existing blocks first, then streams future changes (prevents race conditions without separate initialization call)
- **Anchor-based moves** (after sibling) instead of index-based for CRDT-friendliness
- **Batch operations** critical for performance with large documents and multi-select
- **Structured ApiError** enables type-safe error handling in Flutter
- **Origin tracking** prevents UI echo when local changes sync back via P2P
- **Explicit dispose()** for clean resource management and hot-restart support
- UI state (collapsed) kept local in Flutter, not in CRDT
- Async design matches both Rust/Flutter patterns

#### 2. Flutter Repository Layer

**Purpose**: Adapt backend API to outliner-flutter's repository interface.

**Implementation** (`frontends/flutter/lib/data/block_repository.dart`):
```dart
class RustBlockRepository implements BlockRepository {
  final DocumentRepository _rustRepo;
  final StreamController<BlockChange> _changeController = StreamController.broadcast();
  SubscriptionHandle? _subscriptionHandle;
  Map<String, Block> _cache = {};  // Block cache to minimize FFI calls
  Set<String> _localEditIds = {};   // Track local edits to suppress echo

  RustBlockRepository(this._rustRepo) {
    _initializeRepository();
  }

  Future<void> _initializeRepository() async {
    // Subscribe to changes - stream includes initial state as Created events
    final sink = _changeController.sink;
    _subscriptionHandle = await _rustRepo.watchChanges(sink);
  }

  @override
  Future<Block> createBlock(String? parentId, String content) async {
    final block = await _rustRepo.createBlock(parentId, content);
    _cache[block.id] = block;
    _localEditIds.add(block.id);  // Mark as local to suppress echo
    return block;
  }

  @override
  Future<void> updateBlock(String id, String content) async {
    await _rustRepo.updateBlock(id, content);
    _localEditIds.add(id);
    if (_cache.containsKey(id)) {
      _cache[id] = _cache[id]!.copyWith(content: content);
    }
  }

  @override
  Future<void> deleteBlock(String id) async {
    await _rustRepo.deleteBlock(id);
    _cache.remove(id);
    _localEditIds.add(id);
  }

  @override
  Future<void> moveBlock(String id, String? newParent, String? after) async {
    await _rustRepo.moveBlock(id, newParent, after);
    _localEditIds.add(id);
  }

  // Batch operations for performance
  Future<List<Block>> getBlocks(List<String> ids) async {
    // Check cache first
    final missing = ids.where((id) => !_cache.containsKey(id)).toList();

    if (missing.isNotEmpty) {
      final fetched = await _rustRepo.getBlocks(missing);
      for (var block in fetched) {
        _cache[block.id] = block;
      }
    }

    return ids.map((id) => _cache[id]!).toList();
  }

  @override
  Stream<BlockChange> get changes => _changeController.stream
      .map((change) {
        // Update cache from all changes
        _updateCacheFromChange(change);
        return change;
      })
      .where((change) {
        // Filter out echo from local changes that synced back via P2P
        if (change.origin == ChangeOrigin.local) {
          return true;
        }
        // For remote changes, check if we made this edit locally
        final id = change.id;
        if (_localEditIds.contains(id)) {
          _localEditIds.remove(id);
          return false;  // Suppress echo
        }
        return true;
      });

  void _updateCacheFromChange(BlockChange change) {
    switch (change) {
      case BlockChange.created:
        _cache[change.block.id] = change.block;
        break;
      case BlockChange.updated:
        if (_cache.containsKey(change.id)) {
          _cache[change.id] = _cache[change.id]!.copyWith(content: change.content);
        }
        break;
      case BlockChange.deleted:
        _cache.remove(change.id);
        break;
      // ... handle moved
    }
  }

  Future<void> dispose() async {
    if (_subscriptionHandle != null) {
      await _rustRepo.unsubscribe(_subscriptionHandle!);
    }
    await _changeController.close();
    await _rustRepo.dispose();
  }
}
```

**Design Rationale**:
- Implements outliner-flutter's BlockRepository interface
- Provides clean separation between Rust FFI and UI logic
- Change stream enables reactive updates from P2P sync
- Conversion layer isolates type differences

#### 3. UI Components

**Main Outliner View**:
- Uses outliner-flutter's hierarchical block editor
- Custom bullet builder for LogSeq-style bullets
- Custom block builder for content rendering
- Handles keyboard shortcuts (desktop) and gestures (mobile)
- Hamburger menu for page lists and configuration link

**Configuration View**:
- Document ID input/display
- P2P settings:
  - Display own Node ID
  - Input field for peer Node ID
  - Connect/Start listening buttons
- Connection status indicator

**Quick Actions Bar** (bottom):
- New block at root
- Indent
- Outdent
- Search (future)

**Page Lists** (expandable sidebar/drawer):
- Journal pages (chronological)
- Favorite pages
- Recently visited pages

### Data Model

**Loro Hierarchical Structure** (Normalized Adjacency-List Pattern):
```
Document (Loro::LoroDoc)
├─ "blocks_by_id": LoroMap<String, BlockData>
│   ├─ "block-123": LoroMap
│   │   ├─ "id": String
│   │   ├─ "content": LoroText
│   │   ├─ "parent_id": Option<String>
│   │   ├─ "created_at": i64
│   │   ├─ "updated_at": i64
│   │   └─ "deleted_at": Option<i64>  // Tombstone for safe concurrent deletion
│   └─ ...
│
├─ "root_order": LoroList<String>  // Ordered IDs of root-level blocks
│   ├─ "block-123"
│   ├─ "block-456"
│   └─ ...
│
└─ "children_by_parent": LoroMap<String, LoroList<String>>
    ├─ "block-123": LoroList<String>  // Children of block-123, ordered
    │   ├─ "block-789"
    │   └─ "block-101"
    └─ ...
```

**Design Decisions**:
- **Normalized structure** enables O(1) block lookup by ID via `blocks_by_id` map
- **Separate ordering lists** (`root_order`, `children_by_parent`) minimize conflicts during moves
- **LoroText for content** enables true collaborative character-level editing
- **Tombstones** (`deleted_at`) instead of hard deletion prevent orphans during concurrent operations
- **No UI state in CRDT** - `collapsed` and other view state kept locally in Flutter
- **Parent references** (`parent_id`) provide quick parent lookups without graph traversal

**CRDT Operation Guarantees**:
- **Move operations** use Loro transactions to atomically update:
  1. Remove from old parent's children list (or `root_order`)
  2. Insert into new parent's children list (or `root_order`)
  3. Update block's `parent_id` field
- **Deletion** marks `deleted_at` timestamp, actual cleanup happens during compaction
- **Concurrent edits** on different parts of hierarchy merge cleanly via CRDT properties

**Implementation Requirements**:

All compound operations MUST be wrapped in Loro transactions:
```rust
// Example: Moving a block
pub async fn move_block(&self, id: &str, new_parent: Option<String>, after: Option<String>) -> Result<(), ApiError> {
    let doc = &self.doc.loro_doc;
    let txn = doc.txn();  // Start transaction

    // 1. Remove from old location
    let old_parent = self.get_parent(id)?;
    if let Some(parent) = old_parent {
        let children = doc.get_list(&format!("children_by_parent/{}", parent))?;
        children.remove_by_value(&id)?;
    } else {
        let root_order = doc.get_list("root_order")?;
        root_order.remove_by_value(&id)?;
    }

    // 2. Insert at new location
    if let Some(parent) = new_parent {
        let children = doc.get_or_create_list(&format!("children_by_parent/{}", parent))?;
        let insert_idx = self.find_insert_position(&children, after)?;
        children.insert(insert_idx, id)?;
    } else {
        let root_order = doc.get_list("root_order")?;
        let insert_idx = self.find_insert_position(&root_order, after)?;
        root_order.insert(insert_idx, id)?;
    }

    // 3. Update parent reference
    let block = doc.get_map(&format!("blocks_by_id/{}", id))?;
    block.set("parent_id", new_parent)?;
    block.set("updated_at", now())?;

    txn.commit()?;  // Atomic commit
    Ok(())
}
```

### Platform Support

**Target Platforms** (Initial):
1. **Android**: Native mobile experience
   - Touch gestures for drag/drop
   - Mobile-optimized layout
   - System back button integration

2. **Desktop** (Windows, macOS, Linux):
   - Keyboard shortcuts
   - Wider layout with sidebar
   - Native window chrome

**Platform Abstractions**:
- Use Flutter's platform detection for conditional UI
- Separate widget implementations where needed
- Shared business logic via repository pattern

### Error Handling

**Backend Errors**:
- Network failures (P2P connection)
- Invalid block operations (e.g., cyclic parent relationships)
- CRDT merge conflicts (shouldn't occur but handle gracefully)

**Flutter Errors**:
- UI state validation
- Optimistic update rollback on backend failure
- User-friendly error messages

**Strategy**:
- Use Rust `Result` types across FFI boundary
- Flutter repository converts to exceptions
- UI layer shows snackbars/dialogs for user-facing errors

**Offline Behavior**:
- Display offline status indicator in UI when backend unreachable
- Queue operations locally until connection restored
- App functions normally - users can continue editing
- Changes sync automatically when backend becomes available
- No Flutter-side persistence required - Rust backend handles state

### Performance Considerations

**Large Documents**:
- Lazy loading of collapsed subtrees
- Virtual scrolling in outliner view
- Incremental updates via change stream

**FFI Overhead**:
- Batch operations where sensible
- Minimize boundary crossings
- Use streaming for continuous updates

**CRDT Performance**:
- Loro handles this internally
- Monitor document size growth
- Periodic compaction if needed

## Implementation Approach

### Phase 1: Foundation
1. Set up Flutter project structure in `frontends/flutter/`
2. Configure flutter_rust_bridge codegen
3. Define Rust API types and DocumentRepository interface
4. Implement basic FFI bridge with simple ping/pong test

### Phase 2: Backend API

**Development Approach**: Test-Driven Development with Real Loro
- Write tests first for each operation before implementation
- Use actual Loro instances in tests (no mocks)
- Test concurrent operations and conflict scenarios
- Verify transaction atomicity with real CRDT semantics

**Implementation Steps**:
1. Create shared API module (`crates/holon/src/api/`)
2. Adapt CollaborativeDoc to hierarchical block model using normalized adjacency-list
3. Implement CRUD operations with comprehensive test coverage
4. Create change notification system with character-level granularity
5. Add P2P control operations (connect, listen)
6. Test with 500+ block documents for performance validation

### Phase 3: Flutter Repository
1. Generate FRB bindings
2. Implement RustBlockRepository
3. Wire up change stream from Rust to Flutter
4. Add error handling and type conversions

### Phase 4: UI Implementation
1. Integrate outliner-flutter library
2. Build main outliner view with custom builders
3. Create configuration view
4. Implement quick actions bar
5. Add page lists (journal, favorites, recent)

### Phase 5: Integration & Testing
1. End-to-end testing of block operations using a Cucumber-like framework
2. Test P2P synchronization with external changes
3. Platform-specific testing (Android, Desktop)
4. Performance profiling with large documents

### Phase 6: Polish
1. Keyboard shortcuts (desktop)
2. Touch gestures (mobile)
3. Error handling and user feedback
4. Documentation and examples

## Dependencies

### New Dependencies

**Rust**:
- `flutter_rust_bridge` (codegen and runtime)
- `flutter_rust_bridge_macros` for simpler API definitions

**Flutter** (`pubspec.yaml`):
```yaml
dependencies:
  flutter:
    sdk: flutter
  flutter_riverpod: ^2.6.1
  hooks_riverpod: ^2.6.1
  flutter_hooks: ^0.20.5
  freezed_annotation: ^2.4.0
  json_annotation: ^4.9.0
  # Reference to outliner-flutter (git dependency or path)
  outliner:
    git:
      url: https://github.com/nightscape/outliner-flutter
      ref: main  # or specific commit

dev_dependencies:
  flutter_test:
    sdk: flutter
  build_runner: ^2.4.0
  freezed: ^2.4.0
  json_serializable: ^6.7.0
  flutter_rust_bridge_codegen: ^2.0.0
```

### Existing Dependencies
- holon core library
- Loro CRDT
- Iroh P2P networking
- All remain unchanged

### API Module Integration
**`crates/holon/src/api/`** (new module within main crate):
```toml
# Dependencies already in crates/holon/Cargo.toml:
serde = { version = "1", features = ["derive"] }
loro = "1.0"
anyhow = "1"
async-trait = "0.1"
thiserror = "2.0"

# Dev dependencies for property-based testing:
proptest = "1.6"
proptest-stateful = "0.1"
```

Purpose: Shared API types and traits for all frontends (Tauri, Flutter, future REST), integrated within the main holon crate for simplified architecture.

## Testing Strategy

### Unit Tests
- Rust: Test DocumentRepository operations in isolation using real Loro instances
- Rust: Test block model and hierarchical operations using real Loro instances
- Flutter: Test RustBlockRepository adapter logic
- Flutter: Test UI widget behavior

### Integration Tests
- Test FFI boundary with various data types
- Test change stream propagation from Rust to Flutter
- Test P2P sync with two Flutter instances

### End-to-End Tests
- Set up Cucumber-like tests using Flutter integration test framework
- Create, edit, move, delete blocks
- Start collaborative session and connect from second instance
- Verify changes sync correctly
- Test on Android and Desktop platforms

### Performance Tests
- Large document with 1000+ blocks
- Rapid block creation/deletion
- FFI call overhead measurement

## Security Considerations

1. **FFI Safety**: Ensure all FFI boundaries are memory-safe
2. **Input Validation**: Validate block content and IDs before Rust calls
3. **P2P Authentication**: Use existing Iroh cryptographic verification
4. **Data Sanitization**: Escape/sanitize user content for display

## Documentation Requirements

1. **Architecture Documentation**: Explain interface design and coupling strategy
2. **Setup Guide**: How to build and run Flutter frontend
3. **API Documentation**: Document Rust API and Flutter repository
4. **User Guide**: How to use the Flutter app
5. **Contributing Guide**: How to extend or modify the frontend

## Future Enhancements (Out of Scope)

### Multi-Document Architecture (Phase 2+)

**Concept**: Enable sharing and organization at page and folder levels

**Design Approach**:
- **Document Granularity**: Each page is a separate Loro document
- **Folder Structure**: Folders are collections of page document IDs
  - Could be stored as another Loro document (folder metadata + page list)
  - Or as local organizational structure with references
- **Selective Sharing**: Share individual pages or entire folders independently
  - Page: Share single document via P2P (one Node ID)
  - Folder: Share folder document, recipients sync referenced pages
- **Document Discovery**:
  - Local index of available documents
  - Recent/favorite pages tracked per-client
  - Folder trees maintained separately from page content

**Architecture Impact**:
- `DocumentRepository` already supports multiple instances (one per document)
- Need: Document manager layer to handle multiple repositories
- Need: Inter-document references (links between pages)
- Need: Document metadata (title, creation date, tags)

**Implementation Strategy**:
1. Phase 1: Single-document foundation with extensible architecture
2. Phase 2: Add document manager and multi-document UI
3. Phase 3: Folder structure and selective sharing
4. Phase 4: Cross-document search and references

### Other Enhancements

1. **iOS Support**: Add iOS as a target platform
2. **Web Support**: PWA via Flutter web
3. **Rich Text**: Support markdown rendering and editing
4. **Block References**: Link between blocks within and across documents
5. **Search**: Full-text search across blocks and documents
6. **Real-time Presence**: Show other users' cursors
7. **Conflict Resolution UI**: Manual resolution for rare CRDT conflicts
8. **Version History**: Browse and restore previous document versions
9. **Import/Export**: Support for Markdown, Org-mode, OPML formats

## Success Criteria

1. ✅ Flutter app runs on Android and Desktop
2. ✅ Can create, edit, delete, move blocks in hierarchical structure
3. ✅ Can start P2P instance or connect to peer
4. ✅ Changes from other peers propagate to UI reactively
5. ✅ UI remains responsive with 500+ block document
6. ✅ No memory leaks across FFI boundary
7. ✅ Clean interface boundaries that could support REST or other backends

## Architecture Decisions

The following key decisions have been made to guide implementation:

1. **CRDT Logic Location**: All CRDT logic remains in Rust
   - No Loro Dart bindings needed
   - Simplifies maintenance and ensures consistency
   - Leverages Rust's performance and safety guarantees

2. **Change Stream Granularity**: Character-level changes initially
   - Stream includes full content on text updates
   - Start with simple full-content updates
   - Optimize to Loro text deltas if performance requires it
   - Allows for fine-grained collaborative editing

3. **Offline Behavior**: Graceful degradation with queue
   - Display offline indicator in UI
   - App functions normally, queues operations
   - Auto-sync when connection restored
   - No manual user intervention required

4. **State Persistence**: Backend is source of truth
   - No Flutter-side persistence of document state
   - Always query Rust backend for data
   - Simplifies consistency model
   - Cache exists only for performance (can be cleared)

5. **Document Scope**: Single document for initial implementation
   - Phase 1 supports one document per session
   - Architecture designed for future multi-document support
   - See "Multi-Document Architecture" in Future Enhancements

6. **API Isolation**: Shared crate for cross-frontend consistency
   - New `crates/holon-api` crate
   - Ensures Tauri, Flutter, and future frontends use identical types
   - Prevents API divergence over time

## References

- [outliner-flutter](https://github.com/nightscape/outliner-flutter)
- [Flutter-Rust-Bridge](https://cjycode.com/flutter_rust_bridge/)
- [Loro Documentation](https://loro.dev/)
- [Iroh Documentation](https://iroh.computer/docs)
- Existing `holon` architecture in `README.md`

---

**Version**: 0.5
**Last Updated**: 2025-10-23
**Author**: AI-assisted specification
**Status**: Ready for implementation

## Changelog

### Version 0.5 (2025-10-23) - Architecture Simplification
**Integrated API into main crate**

**Architectural Change**:
- Changed from separate `crates/holon-api` workspace member to integrated `crates/holon/src/api/` module
- Simplifies build and dependency management
- Maintains same clean interface boundaries for cross-frontend consistency
- All references updated throughout specification

### Version 0.4 (2025-10-23) - URI-based IDs for External Integration
**Added support for external system integration**

**Block ID Format**:
- Changed from simple strings to URI format: `"local://<uuid-v4>"` or `"todoist://task/12345"`
- Enables blocks from different systems (Todoist, Logseq, etc.) under same parent
- `create_block` accepts optional `id` parameter for external IDs
- Default behavior generates local UUIDs when `id` is None

### Version 0.3 (2025-10-23) - User Feedback and Architecture Decisions
**Incorporated user comments and finalized key decisions**

**Architecture Refinements**:
1. **Shared API Module**: Added `crates/holon/src/api/` for cross-frontend consistency
   - Shared types, traits, and domain logic within main crate
   - Prevents API divergence between Tauri and Flutter
   - Enables future REST/Web frontends
   - Simplified architecture compared to separate crate

2. **TDD Approach**: Formalized test-first development for Phase 2
   - Use real Loro instances (no mocks)
   - Test concurrent operations and CRDT semantics
   - Performance validation with 500+ blocks

**Key Decisions Finalized**:
3. **Change Granularity**: Character-level initially, optimize if needed
   - Full content on updates for simplicity
   - Path to delta-based updates if performance requires

4. **Offline Behavior**: Graceful degradation with operation queuing
   - Offline indicator in UI
   - Automatic sync when backend available

5. **State Management**: Backend as single source of truth
   - No Flutter-side persistence
   - Cache for performance only

6. **Document Scope**: Single document in Phase 1
   - Multi-document architecture planned for Phase 2+
   - Pages and folders as separate documents

**Future Enhancements Added**:
7. **Multi-Document Architecture**: Detailed design for pages and folders
   - Each page as separate Loro document
   - Selective sharing at page/folder level
   - Document manager layer for Phase 2+

### Version 0.2 (2025-10-23) - Agent Feedback Integration
**Reviewed by**: GPT-5-Pro, Gemini-2.5-Pro

**Critical Architecture Changes**:
1. **Data Model**: Changed from flat LoroList to normalized adjacency-list pattern
   - Added `blocks_by_id` map for O(1) lookups
   - Added `root_order` and `children_by_parent` lists for hierarchy
   - Prevents O(n) scans and enables efficient moves

2. **Stream API**: Changed from returning Stream to StreamSink registration pattern
   - More robust with FRB lifecycle management
   - Explicit subscription handles for cleanup

3. **Move Operation**: Changed from index-based to anchor-based positioning
   - `after: Option<String>` instead of `position: usize`
   - More CRDT-friendly, fewer concurrent conflicts

4. **Stream Initialization**: Stream includes initial state as Created events
   - No separate `get_initial_state()` call needed
   - `watch_changes()` emits all existing blocks first, then future changes
   - Prevents race conditions without version vector complexity

**Feature Additions**:
5. **Batch Operations**: Added `get_blocks`, `create_blocks`, `delete_blocks`
   - Critical for performance with large documents
   - Supports paste, multi-select, lazy loading

6. **Structured Errors**: Replaced generic errors with `ApiError` enum
   - Type-safe error handling in Flutter
   - Enables specific error UI (e.g., "Block not found" vs "Network error")

7. **Origin Tracking**: Added `ChangeOrigin::Local/Remote` to `BlockChange`
   - Prevents UI echo when local edits sync back via P2P
   - Flutter can suppress redundant updates

8. **Lifecycle Management**:
   - Added explicit `dispose()` method
   - Added `SubscriptionHandle` for stream cleanup
   - Supports Flutter hot-restart

9. **UI State Separation**: Moved `collapsed` state from CRDT to Flutter local storage
   - Prevents cross-user UI churn in collaborative sessions
   - Per-client preferences stay local

10. **Implementation Guide**: Added transaction requirements and code examples
    - Documented atomic move operations
    - Shown proper Loro transaction usage

**Flutter Repository Enhancements**:
11. **Caching Layer**: Added block cache in `RustBlockRepository`
    - Minimizes FFI boundary crossings
    - Batch fetching for missing blocks

12. **Echo Suppression**: Implemented local edit tracking
    - Tracks locally-initiated changes by ID
    - Filters them when they echo back from P2P sync

### Version 0.1 (2025-10-23) - Initial Draft
- Initial specification based on user requirements
- Basic architecture and component design
- Identified need for agent consultation
