# TUI-R3BL Block Operations Integration

**Goal:** Integrate backend block operations (Indent, Outdent, MoveBlock) into the R3BL TUI frontend to create an interactive outliner with keyboard-driven block manipulation.

**Status:** Planning → Implementation

**Related:**
- Operations: `crates/holon/src/operations/block_ops.rs`
- Fractional Indexing: `crates/holon/src/storage/fractional_index.rs`
- Database: `crates/holon/src/storage/turso.rs`
- Implementation Plan: `codev/plans/0001-reactive-prql-rendering.md` (Phase 3.2)

---

## Phase 1: Database Integration (Foundation)

**Goal:** Replace in-memory sample data with live database queries using RenderEngine

### 1.1 Update State Structure
- [ ] Add `engine: Arc<RwLock<RenderEngine>>` to State struct
- [ ] Add `cdc_stream: Option<RowChangeStream>` to State struct (for future CDC)
- [ ] Keep existing fields: `data`, `selected_index`, `status_message`
- [ ] Remove `sql_query` field (unused)
- [ ] Update State::new() constructor to accept engine and initial data
- [ ] **No operations field needed!** Operations are now inside RenderEngine ✅

**Files:** `src/state.rs`

**Note:** RenderEngine now combines database + query compilation + operations + CDC streaming in one API.

### 1.2 Initialize RenderEngine in Launcher
- [ ] Add holon dependency to Cargo.toml
- [ ] Import RenderEngine (no need for OperationRegistry!)
- [ ] Initialize RenderEngine:
  - [ ] Use `RenderEngine::new_in_memory().await` for MVP
  - [ ] Or `RenderEngine::new(db_path).await` for persistent storage
  - [ ] **Operations are automatically registered!** (UpdateField, Indent, Outdent, MoveBlock) ✅
- [ ] Query initial blocks using `query_and_watch()`:
  - [ ] Define PRQL query (not SQL):
    ```prql
    from blocks
    select {id, parent_id, depth, sort_key, content, collapsed, completed, block_type}
    sort {parent_id, sort_key}
    ```
  - [ ] Call `engine.query_and_watch(prql, params).await`
  - [ ] Returns: `(RenderSpec, Vec<Entity>, RowChangeStream)`
  - [ ] Use `Vec<Entity>` for initial data
  - [ ] Store `RowChangeStream` in State (unused in MVP, ready for future)
- [ ] Pass engine and initial data to State
- [ ] Handle database errors gracefully

**Files:** `src/launcher.rs`

**Key Advantages:**
1. `query_and_watch()` combines PRQL compilation + query execution + CDC setup
2. Operations auto-registered - no manual setup needed
3. Single object to manage: just `Arc<RwLock<RenderEngine>>`

### 1.3 Implement execute_operation() in RenderEngine ✅ COMPLETE

**Decision:** **Option A** - Store OperationRegistry inside RenderEngine (cleaner architecture)

**Completed Implementation:**
- [x] Added `operations: Arc<OperationRegistry>` field to RenderEngine
- [x] Auto-register default operations in constructor (UpdateField, Indent, Outdent, MoveBlock)
- [x] Simplified `execute_operation()` signature - **no registry parameter!**
  ```rust
  pub async fn execute_operation(&self, op_name: &str, params: Entity) -> Result<()>
  ```
- [x] Added `register_operation()` for custom operations (optional)
- [x] Method locks backend internally and passes to operation
- [x] Added comprehensive tests (9 tests passing) ✅

**Files:** `crates/holon/src/api/render_engine.rs`

**Benefits:**
- ✅ Matches FFI bridge design (no registry parameter in API)
- ✅ RenderEngine is complete backend facade (data + operations + CDC)
- ✅ Cleaner TUI integration (one object instead of two)
- ✅ Better encapsulation (operations are backend concern)

### 1.4 Test Database Connection
- [ ] Create test database with sample hierarchical blocks
- [ ] Verify TUI launches and connects to database
- [ ] Verify blocks display from database (not hardcoded sample data)
- [ ] Verify arrow key navigation works
- [ ] Verify status bar shows connection status

**Acceptance Criteria:**
- ✅ TUI launches without panics
- ✅ Displays blocks from database
- ✅ Arrow keys navigate blocks
- ✅ Status bar shows "Ready" or error message

---

## Phase 2: Hierarchical Display

**Goal:** Render blocks as a tree with proper indentation

### 2.1 Update Rendering Logic
- [ ] Extract block fields in app_render():
  - [ ] `depth` (i64 → usize)
  - [ ] `content` (string)
  - [ ] `collapsed` (bool)
  - [ ] `completed` (bool)
  - [ ] `parent_id` (optional string)
- [ ] Build tree prefix for each block:
  - [ ] Calculate indentation: `"  ".repeat(depth)`
  - [ ] Add collapse icon: `▶` if collapsed, `▼` if expanded
  - [ ] Add checkbox: `[✓]` if completed, `[ ]` if not
- [ ] Combine: `{indent}{icon} {checkbox} {content}`
- [ ] Apply colors:
  - [ ] Selected block: different background
  - [ ] Completed blocks: strikethrough or dimmed color
  - [ ] Collapsed blocks: lighter icon color

**Files:** `src/app_main.rs` (app_render method)

### 2.2 Handle Collapsed Blocks (Optional for MVP)
- [ ] Filter out children of collapsed blocks in rendering
- [ ] Track collapsed state per block
- [ ] Toggle collapse on click/enter (future enhancement)

**Decision:** Skip for MVP, show all blocks regardless of collapsed state

### 2.3 Test Hierarchical Display
- [ ] Create test database with 3-level hierarchy
- [ ] Verify indentation increases with depth
- [ ] Verify collapse/expand icons display correctly
- [ ] Verify checkboxes reflect completion status
- [ ] Verify cursor highlights correct block

**Acceptance Criteria:**
- ✅ Blocks display with correct indentation
- ✅ Icons and checkboxes render correctly
- ✅ Cursor highlights current block
- ✅ Visual hierarchy is clear

---

## Phase 3: Keyboard Operations

**Goal:** Implement block manipulation via keyboard shortcuts

### 3.1 Add Keyboard Handlers
- [ ] Handle Tab key (Indent):
  - [ ] Detect `KeyPress::Plain { key: Key::SpecialKey(SpecialKey::Tab) }`
  - [ ] Call `execute_indent_operation()`
  - [ ] Mark event as consumed
- [ ] Handle Shift+Tab (Outdent):
  - [ ] Detect `KeyPress::WithModifiers` with Tab + Shift
  - [ ] Call `execute_outdent_operation()`
  - [ ] Mark event as consumed
- [ ] Handle Ctrl+↑ (Move Up):
  - [ ] Detect `KeyPress::WithModifiers` with Up + Ctrl
  - [ ] Call `execute_move_up_operation()`
  - [ ] Mark event as consumed
- [ ] Handle Ctrl+↓ (Move Down):
  - [ ] Detect `KeyPress::WithModifiers` with Down + Ctrl
  - [ ] Call `execute_move_down_operation()`
  - [ ] Mark event as consumed
- [ ] Update status message on success/failure

**Files:** `src/app_main.rs` (app_handle_input_event method)

### 3.2 Implement Indent Operation Helper
- [ ] Create `execute_indent_operation()` method in AppMain
- [ ] Extract selected block ID from state
- [ ] Build params Entity with `id` field
- [ ] Execute via `engine.execute_operation("indent", params).await` (no registry parameter!)
- [ ] Handle Result (show error in status bar on failure)
- [ ] Re-query blocks on success:
  - [ ] Call `engine.query_and_watch()` with same PRQL query
  - [ ] Extract `Vec<Entity>` from result (discard RenderSpec and CDC stream)
  - [ ] Update state.data with new blocks
- [ ] Maintain selected_index (or adjust if needed)
- [ ] Update status message: "Block indented" or error

**Files:** `src/app_main.rs`

**Note:**
- RenderEngine.execute_operation() handles all backend locking internally ✅
- Operations are pre-registered in RenderEngine - no manual setup ✅
- Re-query ensures UI shows latest database state after operation
- CDC streaming can replace re-query later (Phase 4+)

### 3.3 Implement Outdent Operation Helper
- [ ] Create `execute_outdent_operation()` method
- [ ] Similar to indent but call "outdent" operation
- [ ] Handle errors (e.g., already at root level)
- [ ] Re-query blocks using `query_and_watch()` on success
- [ ] Update state and status message

**Files:** `src/app_main.rs`

### 3.4 Implement Move Up Operation Helper
- [ ] Create `execute_move_up_operation()` method
- [ ] Extract selected block and its parent_id
- [ ] Find previous sibling in same parent:
  - [ ] Iterate backwards through state.data from current_idx
  - [ ] Match blocks with same parent_id
  - [ ] Get first match as prev_sibling
- [ ] If no previous sibling, show error: "Already at top"
- [ ] Build params Entity:
  - [ ] `id`: selected block ID
  - [ ] `new_parent_id`: current parent_id
  - [ ] `after_block_id`: prev_sibling's predecessor ID (or empty if moving to first)
- [ ] Get backend and execute via `OperationRegistry::execute("move_block", ...)`
- [ ] Re-query blocks using `query_and_watch()` on success
- [ ] Update state and status message

**Files:** `src/app_main.rs`

### 3.5 Implement Move Down Operation Helper
- [ ] Create `execute_move_down_operation()` method
- [ ] Similar to move up but find next sibling
- [ ] Move after next_sibling (set after_block_id to next_sibling's ID)
- [ ] Handle "Already at bottom" case
- [ ] Re-query blocks using `query_and_watch()` on success

**Files:** `src/app_main.rs`

### 3.6 Test Operations
- [ ] Test Tab (Indent):
  - [ ] Select middle child, press Tab → moves under previous sibling
  - [ ] Select first child, press Tab → shows error "Cannot indent first block"
- [ ] Test Shift+Tab (Outdent):
  - [ ] Select nested child, press Shift+Tab → moves to parent's level
  - [ ] Select root block, press Shift+Tab → shows error "Already at root"
- [ ] Test Ctrl+↑ (Move Up):
  - [ ] Select second sibling, press Ctrl+↑ → swaps with first sibling
  - [ ] Select first sibling, press Ctrl+↑ → shows "Already at top"
- [ ] Test Ctrl+↓ (Move Down):
  - [ ] Select first sibling, press Ctrl+↓ → swaps with second sibling
  - [ ] Select last sibling, press Ctrl+↓ → shows "Already at bottom"

**Acceptance Criteria:**
- ✅ All keyboard shortcuts work as expected
- ✅ Operations update database correctly
- ✅ UI refreshes after each operation
- ✅ Error messages display for invalid operations
- ✅ Block order maintains fractional indexing invariants

---

## Phase 4: Polish & Error Handling

**Goal:** Add edge case handling, user feedback, and performance optimizations

### 4.1 Edge Case Handling
- [ ] Indent edge cases:
  - [ ] First block at any level → show error
  - [ ] Block with no previous sibling → show error
- [ ] Outdent edge cases:
  - [ ] Root-level block → show error "Already at root"
  - [ ] Handle blocks with children (should children move too?)
- [ ] Move edge cases:
  - [ ] First sibling moving up → show message
  - [ ] Last sibling moving down → show message
  - [ ] Single child in parent → disable move operations
- [ ] Handle database lock failures gracefully
- [ ] Handle operation panics (wrap in catch_unwind if needed)

**Files:** `src/app_main.rs`

### 4.2 Visual Feedback Enhancements
- [ ] Add debug mode (press 'd' to toggle):
  - [ ] Show sort_key values next to each block
  - [ ] Show parent_id and depth
  - [ ] Show block IDs
- [ ] Color-code status bar:
  - [ ] Green for success messages
  - [ ] Red for error messages
  - [ ] Yellow for warnings
- [ ] Add operation history display (show last 5 operations)
- [ ] Show fractional index rebalancing notifications

**Files:** `src/app_main.rs`, `src/state.rs`

### 4.3 Performance Optimization
- [ ] Profile re-query overhead after operations
- [ ] Consider caching block tree structure:
  - [ ] Build tree once on query
  - [ ] Track dirty state
  - [ ] Only re-query on operation completion
- [ ] Debounce rapid operations (prevent double-presses)
- [ ] Add loading indicator for slow operations

**Files:** `src/app_main.rs`, `src/state.rs`

### 4.4 User Experience Improvements
- [ ] Add help screen (press '?' to show):
  - [ ] List all keyboard shortcuts
  - [ ] Explain operation behavior
  - [ ] Show example tree structure
- [ ] Improve status bar messages:
  - [ ] "Block indented under 'Parent Block Name'"
  - [ ] "Block moved to position 3 of 5"
- [ ] Add visual feedback during operation (spinner or "Working...")
- [ ] Preserve cursor position after operations (stay on same block ID)

**Files:** `src/app_main.rs`

### 4.5 Final Testing
- [ ] Create comprehensive test database with complex hierarchy
- [ ] Test all operations in sequence:
  - [ ] Indent → Outdent → Move Up → Move Down
- [ ] Test rapid operations (keyboard held down)
- [ ] Test with large dataset (100+ blocks)
- [ ] Verify fractional indexing doesn't overflow (check sort_key lengths)
- [ ] Test error recovery (database locked, operation fails)
- [ ] Verify no memory leaks (run for extended period)

**Acceptance Criteria:**
- ✅ All edge cases handled gracefully
- ✅ Visual feedback is clear and helpful
- ✅ Performance is acceptable for large datasets
- ✅ No crashes or panics during testing
- ✅ User experience feels polished

---

## Future Enhancements (Post-MVP)

### CDC Streaming (Reactive Updates)
- [ ] Use AppSignal::RefreshData for async updates
- [ ] Connect to `TursoBackend::row_changes()` stream
- [ ] Update UI when other clients modify blocks
- [ ] Handle concurrent modifications gracefully

### Advanced Operations
- [ ] Enter key: Create new block below current
- [ ] Backspace: Delete empty block
- [ ] Ctrl+D: Duplicate block
- [ ] Ctrl+C/V: Copy/paste blocks

### Display Enhancements
- [ ] Box-drawing characters for tree (├─ │ └─)
- [ ] Collapse/expand blocks on Enter
- [ ] Filter blocks by search term
- [ ] Sort blocks by priority/date/name

### Multi-Selection
- [ ] Shift+↑/↓: Select multiple blocks
- [ ] Tab: Indent all selected blocks
- [ ] Ctrl+↑/↓: Move block group

---

## Test Database Setup

Create sample hierarchy for testing:

```sql
-- test_blocks.sql
CREATE TABLE IF NOT EXISTS blocks (
    id TEXT PRIMARY KEY,
    parent_id TEXT,
    depth INTEGER NOT NULL,
    sort_key TEXT NOT NULL,
    content TEXT NOT NULL,
    collapsed BOOLEAN DEFAULT 0,
    completed BOOLEAN DEFAULT 0,
    block_type TEXT DEFAULT 'paragraph'
);

INSERT INTO blocks (id, parent_id, depth, sort_key, content, collapsed, completed) VALUES
('root-1', NULL, 0, '0500', 'Root Block 1', 0, 0),
('child-1-1', 'root-1', 1, '0500', 'Child 1.1', 0, 0),
('child-1-1-1', 'child-1-1', 2, '0500', 'Child 1.1.1', 0, 0),
('child-1-1-2', 'child-1-1', 2, '0600', 'Child 1.1.2', 0, 1),
('child-1-2', 'root-1', 1, '0600', 'Child 1.2', 0, 0),
('root-2', NULL, 0, '0600', 'Root Block 2', 0, 0),
('child-2-1', 'root-2', 1, '0500', 'Child 2.1', 1, 0),
('child-2-1-1', 'child-2-1', 2, '0500', 'Child 2.1.1 (hidden)', 0, 0),
('root-3', NULL, 0, '0700', 'Root Block 3', 0, 1);
```

---

## Notes

- **Architecture Decision:** Use RenderEngine with `query_and_watch()` for database access
- **Operation Execution:** RenderEngine has built-in OperationRegistry - just call `execute_operation(name, params)` ✅
- **Operation Registration:** Automatic! (UpdateField, Indent, Outdent, MoveBlock) - no manual setup needed ✅
- **Query Format:** Use PRQL (not SQL) with RenderEngine for automatic compilation
- **Re-query Strategy:** Call `query_and_watch()` after each operation to refresh UI (synchronous)
- **CDC Strategy:** MVP ignores CDC stream; add reactive updates via AppSignal in Phase 4+
- **Display Format:** Simple indentation for MVP; box-drawing as enhancement
- **Error Handling:** Show errors in status bar, don't crash
- **Testing:** Test with hierarchical database, verify fractional indexing works
- **No FFI:** TUI is Rust-native, calls operations directly via RenderEngine (no separate registry needed)

---

## Progress Tracking

**Phase 1:** ⬜️ Not Started
**Phase 2:** ⬜️ Not Started
**Phase 3:** ⬜️ Not Started
**Phase 4:** ⬜️ Not Started

**Last Updated:** 2025-01-05

---

## Architecture Changes

### 2025-01-05 (Evening): OperationRegistry Moved into RenderEngine ✅

**Refactored to store OperationRegistry inside RenderEngine:**

**What Changed:**
- ✅ Added `operations: Arc<OperationRegistry>` field to RenderEngine struct
- ✅ Auto-register default operations in RenderEngine::new() and new_in_memory()
- ✅ Simplified `execute_operation()` signature:
  - Before: `execute_operation(op_name, params, registry)`
  - After: `execute_operation(op_name, params)` ← **No registry parameter!**
- ✅ Added `register_operation()` method for custom operations (optional)
- ✅ Updated all tests to work with new API (9 tests passing)

**Why This is Better:**
1. **Matches FFI bridge design** - FFI function (line 554 of plan) doesn't pass registry
2. **Proper encapsulation** - Operations are backend behavior, belong with backend
3. **Simpler API** - Fewer parameters, cleaner code
4. **Single source of truth** - RenderEngine = complete backend facade
5. **Better DX** - TUI only needs `Arc<RwLock<RenderEngine>>`, not separate registry

**TUI Integration Impact:**
- State struct: No longer needs `operations` field ✅
- Launcher: No manual operation registration needed ✅
- Operation calls: Simpler - `engine.execute_operation("indent", params).await` ✅

**Commits:** Implemented in render_engine.rs (lines 41, 87-97, 202-240)

---

### 2025-01-05 (Morning): Initial RenderEngine Integration

**Updated to use RenderEngine instead of direct TursoBackend:**
- ✅ Phase 1.1: State now holds `Arc<RwLock<RenderEngine>>` instead of `TursoBackend`
- ✅ Phase 1.2: Use `query_and_watch()` method instead of `execute_sql()`
- ✅ Phase 1.2: Pass PRQL queries (not SQL) - automatic compilation
- ✅ Phase 1.2: CDC stream setup happens automatically (for future use)
- ✅ Phase 1.3: Added execute_operation() method to RenderEngine
- ✅ Phase 3.2-3.5: Updated operation helpers to use simplified API
- ✅ Notes: Updated architecture decisions to reflect RenderEngine integration

**Benefits:**
- Simpler database integration (one method call for query + CDC setup)
- PRQL compilation handled automatically
- Ready for future CDC streaming without major refactoring
- Consistent with Flutter FFI bridge architecture
