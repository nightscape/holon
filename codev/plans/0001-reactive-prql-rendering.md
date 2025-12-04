# Implementation Plan: Reactive PRQL Rendering System

**Status**: Plan
**Date**: 2025-01-03
**Spec**: `codev/specs/0001-reactive-prql-outliner-complete-spec.md`
**Related**: `0001-reactive-prql-rendering-primitives.md`, `0001-reactive-prql-schema.sql`, `0001-complete-outliner.prql`

---

## Overview

Implement a reactive PRQL rendering system that compiles declarative PRQL queries with embedded UI specifications into live Flutter applications. The system bridges SQL queries with UI rendering through a clean abstraction layer.

**Core Innovation**: Write UI logic in PRQL alongside queries, compile to Rust/Flutter.

---

## Architecture Summary

**Key Decision**: UI-agnostic backend. Rust parses PRQL into generic AST, UIs interpret for their capabilities.

```
┌─────────────────────────────────────────────────────────────┐
│  PRQL Query + render()                                      │
│  ├─ SQL query (blocks, joins, filters, derives)            │
│  └─ Render expression (function calls, args, column refs)  │
└─────────────────────────────────────────────────────────────┘
                          ↓ parse_query_render() (crates/query-render)
┌─────────────────────────────────────────────────────────────┐
│  Rust Query Engine (UI-AGNOSTIC)                           │
│  ├─ Compile PRQL → SQL (via prqlc)                         │
│  ├─ Parse render() → Generic AST (RenderExpr tree)         │
│  │   - FunctionCall, ColumnRef, BinaryOp, Literal          │
│  │   - NO UI semantics (no "ListView", "TextField")        │
│  ├─ Execute SQL → Vec<HashMap<String, Value>> (data)       │
│  └─ CDC: Stream row changes (Added/Updated/Removed)        │
│      ├─ Coalesce DELETE+INSERT → UPDATE (no flicker)       │
│      └─ Keyed caching (HashMap<BlockId, RowData>)          │
└─────────────────────────────────────────────────────────────┘
                          ↓ FFI boundary (flutter_rust_bridge)
┌─────────────────────────────────────────────────────────────┐
│  Flutter UI (interprets generic AST)                        │
│  ├─ Parse RenderExpr → Flutter widgets                     │
│  │   - FunctionCall("list") → ListView.builder             │
│  │   - FunctionCall("block") → Column                      │
│  │   - FunctionCall("editable_text") → TextField           │
│  ├─ StreamBuilder<List<RowEvent>> (CDC deltas)             │
│  ├─ Keyed widget cache (minimal rebuilds)                  │
│  └─ Nested queries: ReactiveTableWidget (own stream)       │
└─────────────────────────────────────────────────────────────┘
         OR
┌─────────────────────────────────────────────────────────────┐
│  TUI (interprets same AST differently)                      │
│  ├─ Parse RenderExpr → Ratatui widgets                     │
│  │   - FunctionCall("list") → List                         │
│  │   - FunctionCall("block") → Paragraph                   │
│  │   - FunctionCall("editable_text") → Input               │
│  └─ Same CDC stream, different rendering                   │
└─────────────────────────────────────────────────────────────┘
         OR
┌─────────────────────────────────────────────────────────────┐
│  Web UI (future: Svelte/React)                             │
│  ├─ Parse RenderExpr → HTML elements                       │
│  │   - FunctionCall("list") → <ul>                         │
│  │   - FunctionCall("editable_text") → <input>             │
│  └─ WebSocket CDC stream                                   │
└─────────────────────────────────────────────────────────────┘
```

---

## Design Decisions Implemented

Based on consensus review (GPT-5-Pro: 8/10, Gemini-2.5-Pro: 9/10) + architectural refinement (2025-01-04):

1. ✅ **UI-Agnostic Backend**: Rust parses PRQL into generic AST, no UI-specific primitives
   - Backend: `RenderExpr` (FunctionCall, ColumnRef, BinaryOp, Literal)
   - Flutter: Interprets `FunctionCall("list")` → `ListView.builder`
   - TUI: Interprets `FunctionCall("list")` → `List` widget
   - Web: Interprets `FunctionCall("list")` → `<ul>` element

2. ✅ **CDC Event Coalescing**: DELETE + INSERT on same row → UPDATE (prevents widget flicker)
   - Batch process CDC events, match by row ID
   - Flutter never sees intermediate delete

3. ✅ **Keyed Widget Caching**: `HashMap<BlockId, RowData>` + sorted view
   - Minimal UI rebuilds via stable keys
   - Works for Flutter (`ValueKey`) and TUI (line indices)

4. ✅ **Nested Reactive Queries**: Each `RenderExpr` can have optional `query` field
   - `ReactiveTableWidget` with own stream (lazy-loaded, auto-disposed)
   - No stream multiplexing needed (Flutter uses FFI, TUI is local)

5. ✅ **Offline-First Command Sourcing** (Event Sourcing / CQRS):
   - Commands append to log → apply to Turso (optimistic) → CDC → UI
   - Background sync worker replays commands to external systems
   - Idempotency via client-generated UUIDs
   - On rejection: Re-fetch canonical state, notify user

6. ✅ **Helper Functions Over Primitives**: `drop_zone` primitive + `drop_zones()` helper

7. ✅ **Generic Operations**: `HashMap<String, Value>` interface, not fixed structs

8. ✅ **Minimal Magic**: Single `$session_user_id` param + table joins (no `@` variables)

9. ✅ **Composable Patterns**: `standard_block_ops()`, `drop_zones()` as regular PRQL functions

---

## Phases

### Phase 1: Core Infrastructure
**Goal**: Establish foundation - PRQL parser, SQL execution, basic data flow

#### 1.1: PRQL Parser & AST ✅ COMPLETE
**Status**: Parser complete with function expansion, all primitives implemented

**Implemented** in `crates/query-render/`:
- ✅ PRQL AST types (`RenderNode`, `Expr`, `UISpec`) in `types.rs`
- ✅ Parse PRQL query section (via `prqlc::prql_to_pl`)
- ✅ Parse render section (`split_prql_at_render` in `parser.rs`)
- ✅ Convert PRQL AST → JSON intermediate format (`prql_ast_to_json`)

**Completed**:
- ✅ Added new primitives to RenderNode enum:
  - `DropZone` (position, on_drop, invalid_targets, visible)
  - `BlockOperations` (operations Vec, params HashMap)
  - `OperationDef` (name, default_key, icon, description)
- ✅ Implemented function expansion in render():
  - `expand_functions_in_expr()`: Recursive function call expansion
  - `find_function_in_module()`: Locates user-defined functions
  - `expand_function_call()`: Inlines function body with parameter substitution
  - `substitute_params()`: Replaces parameter references with arguments
- ✅ Helper functions now work:
  - `drop_zones(invalid_targets)` → expands to 3 drop_zone primitives
  - `standard_block_ops(params)` → expands to block_operations (PRQL record syntax limitation noted)
- ✅ Comprehensive test coverage (13 tests passing):
  - Unit tests: test_compile_drop_zone, test_compile_block_operations, test_function_expansion, test_function_expansion_with_params
  - Integration tests: test_drop_zone_primitive, test_helper_function_drop_zones_inline, test_helper_function_with_prql_function, test_standard_block_ops_helper

**Files**: `crates/query-render/src/parser.rs`, `crates/query-render/src/types.rs`, `crates/query-render/src/compiler.rs`

#### 1.2: SQL Compilation & Parameter Binding ✅ COMPLETE
**Status**: PRQL → SQL compilation complete, parameter binding fully implemented
**Completed**: 2025-01-05

**Implemented**:
- ✅ PRQL → SQL via `prqlc` (`pl_to_rq`, `rq_to_sql` in `query-render/lib.rs`)
- ✅ Handles all PRQL features: FROM, JOIN, FILTER, DERIVE, SORT, recursive CTEs
- ✅ **Runtime parameter binding** via `TursoBackend::execute_sql()`:
  - Named parameters: `$session_user_id`, `$min_depth`, etc.
  - Safe binding via prepared statements (SQL injection prevention)
  - `bind_parameters()` converts `$param_name` → `?` placeholders
  - Supports all Value types: String, Integer, Boolean, DateTime, JSON, Reference, Null
- ✅ **Integration complete**: PRQL compilation + SQL execution + parameter binding
- ✅ **Test coverage**: parameter binding tests in `render_engine_tests.rs`

**Files**:
- `crates/query-render/src/lib.rs` (PRQL compilation)
- `crates/holon/src/storage/turso.rs` (execute_sql, bind_parameters)
- `crates/holon/src/api/render_engine.rs` (execute_query wrapper)

#### 1.3: Database Layer with CDC ✅ MOSTLY COMPLETE
**Status**: Row change notifications via materialized views fully implemented and tested

**Critical Finding**: We must use the **`turso` crate** (not `libsql`) for CDC support!
- ✅ **CDC CONFIRMED**: `PRAGMA unstable_capture_data_changes_conn('full')` works
- ✅ **`turso_cdc` table**: Automatically captures INSERT/UPDATE/DELETE with change_type, before/after states
- ⚠️ **Materialized views**: Experimental - requires `--experimental-views` flag
- 📦 **Crate**: `turso = "0.3"` (Limbo - Rust rewrite of SQLite by Turso team)

**Investigation Results** (2025-01-03):
- ✅ Tested CDC PRAGMA in turso crate: **WORKS**
- ✅ Verified `turso_cdc` table exists and captures changes: **2 CDC records found** (1 CREATE TABLE + 1 INSERT)
- ✅ CDC provides JSON helpers: `bin_record_json_object()`, `table_columns_json_array()`
- ⚠️ Materialized views need experimental flag but ARE supported
- ⚠️ API differences from libsql: `Row.column_name()` doesn't exist, use `Statement.columns()` instead

**Already implemented** in `crates/holon/src/storage/turso.rs`:
- ✅ Turso/SQLite connection (`TursoBackend::new`, `new_in_memory`)
- ✅ CRUD operations (`get`, `query`, `insert`, `update`, `delete`)
- ✅ **SQL Injection Prevention**: All operations now use prepared statements ✅ **SECURITY FIX**
  - `insert()`: Uses parameterized placeholders (turso.rs:431-459)
  - `update()`: Uses parameterized SET clauses (turso.rs:473-507)
  - `delete()`: Uses parameterized WHERE clause (turso.rs:509-523)
  - `get_version()`, `set_version()`: Parameterized queries (turso.rs:526-570)
  - `build_where_clause()`: Builds SQL with parameter binding (turso.rs:267-303)
  - `query()`: Uses prepared statements with filter parameters (turso.rs:404-446)
  - New helper: `value_to_turso_param()` converts Value → turso::Value (turso.rs:236-246)
- ⚠️ Manual CDC column (`_dirty`) - **REMOVED** with built-in CDC via row_changes()
- ✅ **`_version` column**: KEPT for tracking upstream versions in Loro and external systems (not an auto-incrementing counter)
- ✅ Basic tests in `turso_tests.rs`
- ✅ Property-based tests in `turso_pbt_tests.rs` with bounded wait for stream delivery

**Implementation Status**:
- [x] **INVESTIGATE**: Test if Turso CDC works ✅ **CONFIRMED WORKING**
- [x] **MIGRATE**: Switch from `libsql` to `turso` crate in Cargo.toml ✅ **DONE**
- [x] Fix API differences: Replace `Row.column_name()` with `Statement.columns()` approach ✅ **DONE**
- [x] Implement row change notifications via `row_changes()` callback (not manual turso_cdc queries) ✅ **DONE**
  - Uses materialized view change callbacks, which is the correct approach for views
  - Exposed as `TursoBackend::row_changes() -> (Connection, RowChangeStream)`
  - Returns `RowChange` events (Insert/Update/Delete with ROWID and data)
- [x] **FILE-BASED STORAGE**: ✅ **CONFIRMED WORKING** (2025-01-05)
  - ✅ Verified file-based storage works via `UnixIO` on Unix-like systems
  - ✅ Supported platforms: macOS, Linux, BSD, iOS (via POSIX syscalls)
  - ⚠️ Windows: Falls back to in-memory with warning (turso_core doesn't export Windows IO)
  - ✅ Updated `TursoBackend::new()` to use `UnixIO` with conditional compilation
  - ✅ Added persistence test: `test_file_based_storage_persistence()`
  - ✅ Previous comment "not yet supported" was incorrect - file storage works!
- [x] **CDC Event Coalescing** (prevents materialized view flickering): ✅ **IMPLEMENTED**
  - ✅ Batch process CDC events per callback invocation
  - ✅ Track both pending DELETEs and pending INSERTs in HashMap
  - ✅ DELETE then INSERT → UPDATE (existing logic)
  - ✅ INSERT then DELETE → no-op (drop both events - NEW)
  - ✅ Bounded channel (1024 capacity) with backpressure handling
  - ✅ Result: Flutter never sees intermediate delete from view updates
  - **Implementation**: `CdcCoalescer` in `turso.rs:38-106`
  - **Key Changes**:
    - Added `pending_inserts` HashMap to track INSERT events
    - INSERT→DELETE pairs now coalesce to no-op (prevents flicker)
    - Bounded channel prevents memory exhaustion under bursty changes
- [x] **UI Keying Requirements Documentation**: ✅ **DOCUMENTED**
  - ✅ Added comprehensive documentation to `RowChange` and `ChangeData` types
  - ✅ **CRITICAL**: UI must key by entity ID from `data.get("id")`, NOT by ROWID
  - ✅ ROWIDs are:
    - Unique per view (not globally unique)
    - Can be reused after DELETE operations
    - Used for transport and coalescing only
  - ✅ Example code provided showing correct vs incorrect usage
  - **Location**: `turso.rs:17-45`
- [x] Enable experimental materialized views ✅ **DONE**
  - Enabled via `DatabaseOpts::default().with_views(true)` (turso.rs:159)
  - CREATE MATERIALIZED VIEW works with PRQL-generated SQL
  - Row change callbacks capture base table changes that affect views
- [x] Row-level diffing for CDC events ✅ **DONE via coalescer**
  - `CdcCoalescer` prevents UI flicker by coalescing DELETE+INSERT → UPDATE
  - Handles INSERT+DELETE → no-op to avoid transient rows
  - Tested via property-based tests in `turso_pbt_tests.rs`
- [x] Stream emitter ✅ **DONE**
  - `RowChangeStream = ReceiverStream<RowChange>` where `RowChange` contains Insert/Update/Delete
  - UI subscribes via `row_changes()` method
- [x] Test CDC coalescing ✅ **TESTED**
  - 9 unit tests for coalescer logic (all passing)
  - PBT tests verify view change notifications with coalescing
  - Removed redundant ignored tests that used wrong approach (turso_cdc table directly)

**Files**: `crates/holon/src/storage/turso.rs`, `crates/holon/src/storage/cdc.rs` (new), `crates/holon/src/storage/schema.sql`

#### 1.4: Fractional Indexing ✅ COMPLETE
**Status**: Fractional indexing library integrated and tested
**Completed**: 2025-01-05

**Implemented** in `crates/holon/src/storage/fractional_index.rs`:
- ✅ **Library chosen**: `loro_fractional_index` v1.0 (matches Loro integration)
- ✅ **Key generation functions**:
  - `gen_key_between(prev, next)` - Generate key between two optional keys
  - `gen_n_keys(count)` - Generate N evenly-spaced keys (for rebalancing)
- ✅ **Constants**: `MAX_SORT_KEY_LENGTH = 32` bytes (rebalancing trigger)
- ✅ **Comprehensive test coverage** (10 tests passing):
  - Basic insertion: beginning, middle, end, empty list
  - Ordering verification (10 sequential insertions)
  - Evenly-spaced generation (rebalancing test)
  - Deep nesting (50+ insertions remain under max length)
  - Edge cases: empty list, single item

**Key Findings**:
- loro_fractional_index uses efficient algorithm - keys don't grow excessively
- Keys remain well under 32 bytes even with 50+ sequential insertions
- Rebalancing via `gen_n_keys()` creates uniform spacing
- Library API: `FractionalIndex::new(lower, upper)` for generation
- Hex string format: parse with `from_hex_string()`, output via `to_string()`

**Integration Plan** (Phase 3.2):
- `move_block` operation will use `gen_key_between()` to calculate new sort_key
- If generated key length > MAX_SORT_KEY_LENGTH: trigger rebalancing
- Rebalancing: Query all siblings, use `gen_n_keys(sibling_count)`, update all
- Transaction isolation (snapshot) handles concurrent move operations

**Design Decision**: **Anchor-based approach** (Option A from discussion)
- UI sends: `move_block(block_id, new_parent_id, after_block_id)`
- Rust queries DB for predecessor/successor sort_keys (source of truth)
- Fractional index library generates new sort_key
- No UI knowledge of sort_keys required (clean separation)

**Files**: `crates/holon/src/storage/fractional_index.rs` (190 lines)


---

### Phase 2: Render Engine (UI-Agnostic)
**Goal**: Parse render() into generic AST that any UI can interpret

#### 2.1: Generic AST Parser ✅ COMPLETE
**Status**: Generic AST implementation complete with all expression types
**Completed**: 2025-01-04

**Implemented** in `crates/query-render/`:
- ✅ Generic AST types in `types.rs`:
  - `RenderExpr::FunctionCall { name, args }` - Generic function call
  - `RenderExpr::ColumnRef { name }` - Data access: `block_id`, `depth`, etc.
  - `RenderExpr::Literal { value }` - Literals: `"hello"`, `42`, `true`
  - `RenderExpr::BinaryOp { op, left, right }` - Math/logic: `depth * 24`, `completed and visible`
  - `RenderExpr::Array { items }` - Arrays: `[item1, item2, item3]`
  - `RenderExpr::Object { fields }` - Records: `{key1: value1, key2: value2}`
  - `Arg { name, value }` - Named and positional arguments
  - `BinaryOperator` - All operators including And/Or
- ✅ Compiler in `compiler.rs`:
  - `compile_render_spec()` - Compiles JSON AST to RenderSpec
  - `compile_render_expr()` - Recursive expression compiler
  - Handles function calls, column refs, literals, binary ops, arrays, objects
  - Helper function expansion already working (from Phase 1.1)
- ✅ Integration with parser:
  - Parser extracts render() and expands functions (Phase 1.1)
  - Converts PRQL AST to JSON intermediate format
  - Compiler converts JSON to generic RenderExpr
- ✅ **NO UI-specific knowledge**: Rust doesn't know about "ListView", "TextField", etc.
- ✅ All types ready for FRB serialization
- ✅ Comprehensive unit tests (17 tests passing):
  - `test_compile_simple_text`, `test_compile_with_column_ref`
  - `test_compile_nested_calls`, `test_compile_binary_op`
  - `test_compile_array`, `test_compile_object`
  - Integration tests: `test_simple_function_call`, `test_with_column_reference`
  - `test_nested_function_calls`, `test_named_arguments`
  - `test_helper_function_expansion`, `test_helper_function_with_params`
  - `test_sql_generation`

**Key Insight**: Backend is UI-agnostic. Flutter interprets `FunctionCall("list")` as `ListView`, TUI interprets it as `List`, Web as `<ul>`.

**Files**: `crates/query-render/src/types.rs`, `crates/query-render/src/compiler.rs`, `crates/query-render/src/lib.rs`

#### 2.2: FRB Type Definitions (Generic AST) ✅ COMPLETE
**Status**: Generic AST types successfully exposed to Flutter via FRB
**Completed**: 2025-01-04

**Implementation**:
- ✅ Added `query-render` dependency to `crates/holon/Cargo.toml`
- ✅ Re-exported query-render types from `crates/holon/src/lib.rs`:
  ```rust
  pub use query_render::types::{Arg, BinaryOperator, RenderExpr, RenderSpec};
  ```
- ✅ Configured `frontends/flutter/flutter_rust_bridge.yaml`:
  ```yaml
  rust_input: crate::api,query-render
  ```
- ✅ Added `/// flutter_rust_bridge:non_opaque` comments to all types in `crates/query-render/src/types.rs`:
  - This tells FRB to generate translatable types without requiring `#[frb]` attributes
  - Keeps query-render UI-agnostic (no FRB dependency needed)
  - Works with multi-input scanning feature
- ✅ Generated Dart bindings in `frontends/flutter/lib/src/rust/third_party/query_render/`:
  - `RenderSpec` - class with `root` and `nestedQueries`
  - `RenderExpr` - freezed sealed class with all variants (FunctionCall, ColumnRef, Literal, BinaryOp, Array, Object)
  - `Arg` - class with optional `name` and `value`
  - `BinaryOperator` - enum with all operators
  - `Value` - freezed sealed class (JSON value representation)
  - `Number`, `N` - supporting types for JSON numbers
- ⏳ `RowEvent` enum → Added, Updated, Removed (for CDC streaming) - **TODO in Phase 4.1**

**Key Insight**: Using doc comments `/// flutter_rust_bridge:non_opaque` is the cleanest way to make third-party crate types translatable without adding FRB as a dependency. This keeps the backend truly UI-agnostic while enabling full type generation for Flutter.

**Files**:
- `crates/query-render/src/types.rs` (source types with FRB comments)
- `crates/holon/src/lib.rs` (re-exports)
- `frontends/flutter/flutter_rust_bridge.yaml` (FRB config)
- `frontends/flutter/lib/src/rust/third_party/query_render/types.dart` (FRB-generated)

#### 2.3: UI-Side Interpreters (Flutter, TUI, Web) ✅ COMPLETE
**Status**: Flutter and TUI interpreters complete with comprehensive tests
**Completed**: 2025-01-04 (Flutter), 2025-01-05 (TUI)

- [x] **Flutter Interpreter** (`flutter/lib/render/render_interpreter.dart`):
  - Map FunctionCall names to Flutter widgets
  - `"list"` → `ListView.builder`, `"block"` → `Column`, `"editable_text"` → `TextField`
  - `"row"` → `Row`, `"text"` → `Text`, `"flexible"` → `Flexible`
  - `"collapse_button"` → `IconButton`, `"drop_zone"` → `Container`, `"block_operations"` → `IconButton`
  - Nested queries: `ReactiveTableWidget` with own stream (ready for Phase 4.1 CDC wiring)
  - Expression evaluation: literals, column refs, binary ops (arithmetic, comparison, logical)
  - `RenderContext` for passing row data and operation callbacks
- [x] **Unit tests**: 27 tests passing covering all expression types and widget mappings
- [x] **TUI Interpreter** (`frontends/tui/src/render_interpreter.rs`):
  - Map FunctionCall names to Ratatui widgets
  - `"list"` → `List`, `"block"` → `Paragraph`, `"editable_text"` → styled `Paragraph`
  - `"row"` → Horizontal layout, `"text"` → `Span`, `"checkbox"` → `[✓]`/`[ ]`
  - `"badge"` → colored `Span`, `"icon"` → icon character
  - Expression evaluation: literals, column refs, binary ops (same as Flutter)
  - Full integration with app.rs, ui.rs for reactive rendering
- [x] **Unit tests**: 7 tests passing (5 interpreter unit tests + 2 integration tests)
- [ ] **Web Interpreter** (future: `web/src/render_interpreter.ts`):
  - Map FunctionCall names to HTML elements
  - `"list"` → `<ul>`, `"editable_text"` → `<input>`

**Key Design Decisions**:
- **Option A**: Explicit `flexible()` wrapper for flex constraints (chosen)
  - Users write `row(button, flexible(textfield))` in PRQL
  - Makes constraints visible, works across all UIs
  - Flutter needs it due to unbounded constraints in Row
  - TUI doesn't need it (always bounded), but can ignore it
  - Alternative rejected: Auto-wrapping (too magical, context-dependent)

**Files**:
- `frontends/flutter/lib/render/render_interpreter.dart` (650 lines)
- `frontends/flutter/lib/render/reactive_query_widget.dart` (CDC streaming, keyed caching)
- `frontends/flutter/test/render/render_interpreter_test.dart` (27 tests)
- `frontends/tui/src/render_interpreter.rs` (540 lines, TUI interpreter)
- `frontends/tui/src/ui.rs` (updated for RenderSpec)
- `frontends/tui/src/app.rs` (updated for RenderSpec)
- `frontends/tui/src/test_integration.rs` (7 tests)

#### 2.4: Automatic Operation Inference ✅ COMPLETE
**Status**: Simplified approach complete, operations embedded in tree nodes
**Completed**: 2025-01-06 (initial complex approach), 2025-11-07 (final simplification)
**Goal**: Automatically wire operations based on column references - eliminate manual operation declarations

**Key Innovation**: 🎉 **Elegant simplicity** - No transformation, no aliases, no lineage on render expressions. Just walk the tree and check for direct column references!

**Final Approach** (2025-11-07):
1. ✅ **Extract table name** from main query's `from` clause
2. ✅ **Walk RenderExpr tree** recursively (isomorphic to what we're analyzing)
3. ✅ **Check for "this." prefix** on ColumnRef parameters
4. ✅ **Attach operations directly** to FunctionCall nodes where they apply

**Example**:
```prql
# Input:
from blocks
select {id, content, completed}
render (list item_template:(row (checkbox checked:this.completed) (text content:this.content)))

# Tree structure (after annotation):
FunctionCall {
    name: "checkbox",
    args: [
        Arg { name: Some("checked"), value: ColumnRef { name: "this.completed" } }
    ],
    operations: [
        OperationWiring {
            widget_type: "checkbox",
            modified_param: "checked",
            table: "blocks",
            id_column: "id",
            field: "completed"
        }
    ]
}
```

**Architecture Evolution**:
- ❌ **Old approach**: Complex 5-pass transformation with widget aliases, stub generation, expression flattening → lineage analysis
- ✅ **New approach**: Direct tree inspection with simple table name extraction (~50 lines vs ~300 lines)

**Key Insight**: The RenderExpr tree structure is isomorphic to what we're analyzing! No need for transformation - just walk it directly and check for "this." column references.

**Data Structure Changes**:
- ❌ **Old**: `RenderSpec.operations: HashMap<String, OperationWiring>` (widget aliases as keys)
- ✅ **New**: `RenderExpr::FunctionCall { operations: Vec<OperationWiring> }` (embedded in nodes)

**Implemented** in `crates/query-render/src/lib.rs`:
- [x] **`extract_table_name(module)`** - Parses `from TABLE` in main query (~15 lines)
  ```rust
  fn extract_table_name(module: &ModuleDef) -> Result<String> {
      // Find VarDef with kind Main
      // Look for 'from' transform in pipeline
      // Return table name
  }
  ```
- [x] **`annotate_tree_with_operations(expr, table_name)`** - Recursive tree walker (~50 lines)
  ```rust
  fn annotate_tree_with_operations(expr: &mut RenderExpr, table_name: &str) {
      match expr {
          FunctionCall { args, operations, .. } => {
              // Check each arg for ColumnRef with "this." prefix
              for arg in args {
                  if arg.value is ColumnRef with "this." {
                      operations.push(OperationWiring { ... });
                  }
              }
              // Recurse into nested expressions
          }
          // Handle other expression types...
      }
  }
  ```
- [x] **`parse_query_render_with_operations(prql)`** - Main entry point
  - Split query and render
  - Extract table name from query
  - Compile both to SQL + RenderSpec
  - Annotate tree with operations
  - Return results

**Test Coverage**:
- ✅ 17 query-render tests passing (lib.rs, parser.rs, compiler.rs)
- ✅ Integration test `test_operations_inference` passing (render_engine.rs:516)
- ✅ Verifies both checkbox and text widgets get auto-operations
- ⚠️ Lineage module tests disabled (old approach, not needed)

**Benefits**:
- ✅ **~300 lines removed** - Eliminated widget aliases, transformation pipeline, complex lineage integration
- ✅ **Zero transformation** - Render syntax stays compact, no explosion to derive statements
- ✅ **Embedded operations** - Data lives where it's used (in tree nodes, not separate HashMap)
- ✅ **Self-describing tree** - No need for aliases, tree nodes are the authority
- ✅ **One-line API** - `parse_query_render_with_operations(&prql)` does everything
- ✅ **80% less boilerplate** in PRQL render() specs (original goal achieved!)

**Current Limitations**:
- ⚠️ Primary key hardcoded to "id" (acceptable for now, can extend later)
- ⚠️ No computed column detection (lineage.rs kept for future use)
- ⚠️ No multi-table support yet (single table from `from` clause only)

**Future Enhancements** (deferred, not blocking):
- [ ] **Primary key detection** from schema metadata
- [ ] **Computed column validation** using lineage (warn if widget references non-updatable field)
- [ ] **Multi-table queries** with joins (track multiple source tables)
- [ ] **Validation & error reporting** (error if primary key not in query)
- [ ] **Opt-out mechanism** (explicit `on_edit: null` to disable auto-wiring)

**Files**:
- `crates/query-render/src/lib.rs` ✅ **UPDATED** - Simple table extraction + tree annotation
- `crates/query-render/src/types.rs` ✅ **UPDATED** - `RenderExpr::FunctionCall.operations` field
- `crates/query-render/src/compiler.rs` ✅ **UPDATED** - Initialize empty operations Vec
- `crates/query-render/src/lineage.rs` ⚠️ **MOSTLY UNUSED** - Complex approach kept for reference, tests disabled
- `crates/holon/src/api/render_engine.rs` ✅ **UPDATED** - Uses `parse_query_render_with_operations()`
- `examples/test-lineage/` ℹ️ **ARCHIVED** - POC for complex approach (not used in final implementation)

#### 2.5: Helper Function Resolution
- [ ] Store helper function definitions in registry
- [ ] Resolve function calls during render() compilation
- [ ] Implement built-in helpers as PRQL functions:
  - `drop_zones(invalid_targets, on_drop)` → returns array of 3 `drop_zone` widgets
  - `standard_block_ops(params)` → returns `block_operations` with param map
- [ ] Unit tests for helper function expansion

**Files**: `crates/query-render/src/helpers.rs` (new)

---

### Phase 3: Operation Registry
**Goal**: Generic operation system with HashMap-based parameters

#### 3.1: Operation Trait & Registry ✅ COMPLETE
**Status**: Operation trait, RowView, and Registry implementation complete with tests
**Completed**: 2025-01-05

**Implemented** in `crates/holon/src/operations/`:
- ✅ `Operation` trait (no return value - one-directional flow):
  ```rust
  pub trait Operation: Send + Sync {
      fn name(&self) -> &str;
      fn execute(
          &self,
          row_data: &Entity,
          ui_state: &UiState,
          db: &mut TursoBackend
      ) -> Result<()>;
  }
  ```
- ✅ `UiState` struct already existed in `api/render_engine.rs` (cursor_pos, focused_id)
- ✅ `OperationRegistry` for registration and execution by name
- ✅ `RowView` validation layer with typed accessors:
  - `id()`, `parent_id()`, `depth()`, `sort_key()`, `content()`
  - `is_collapsed()`, `is_completed()`, `block_type()`
  - `get()` for custom fields, `entity()` for raw access
- ✅ Comprehensive test coverage (10 tests passing):
  - RowView: required fields, optional fields, boolean fields, null handling, missing fields
  - Registry: registration, operation_names, execute, error handling

**Note**: Operations mutate DB directly. UI updates happen via CDC → query re-run → new render. This is the Elm/Redux unidirectional data flow: Action → Model → View.

**Files**: `crates/holon/src/operations/mod.rs`, `crates/holon/src/operations/registry.rs`, `crates/holon/src/operations/row_view.rs`

#### 3.2: Core Block Operations ⏳ IN PROGRESS
**Status**: UpdateField operation complete, remaining operations pending
**Completed**: 2025-01-05 (UpdateField)

**Design Decision**: **Hybrid approach** - generic operations for simple updates, specific operations for complex logic
- ✅ Generic: `UpdateField` - handles most field updates (completed, collapsed, content, priority, etc.)
- ⏳ Specific: `Delete`, `Indent`, `Outdent`, `Split`, `MoveBlock` - complex multi-field operations with validation

**Implemented**:
- ✅ **`UpdateField`** operation (`block_ops.rs:10-131`):
  - Generic field updater for all entity types (blocks, tasks, people, companies, etc.)
  - Intent-based (not toggle) - avoids distributed systems toggle problem
  - Field validation: type checking, constraints (depth >= 0), reserved fields (id)
  - Unknown fields allowed (with warning) for extension flexibility
  - Works for: completed, collapsed, content, priority, parent_id, metadata, tags, etc.
  - 8 unit tests passing (validation, execution, error handling)
- ✅ **Async trait support**: Operation trait uses `#[async_trait]` for database mutations
- ✅ **StorageBackend integration**: Uses `StorageBackend::update()` method

**Remaining**:
- [ ] `Delete`: Simple deletion (may add cascading later)
- [ ] `Indent`/`Outdent`: Multi-field atomic updates (parent_id, depth, sort_key)
- [ ] `Split`: Creates new block + updates content
- [ ] `MoveBlock`: Drag-drop with cycle prevention + tree validation
- [ ] Server-side validation for complex operations (especially drag-drop cycle prevention)
- [ ] Integration tests: Operation execution → DB mutation → CDC stream emits event

**Key Insight**: UI handles "toggle" logic by reading current state and sending explicit values to backend. This prevents distributed systems issues where stale UI state causes incorrect toggles.

**Files**: `crates/holon/src/operations/block_ops.rs`

#### 3.3: Database Mutation & CDC
- [ ] Operations mutate database directly via SQL UPDATE/INSERT/DELETE
- [ ] Use fractional indexing for move operations (update sort_key)
- [ ] **CDC Strategy** (based on Phase 1.3 findings):
  - ✅ **Built-in CDC works**: turso_cdc table captures all base table changes automatically
  - ✅ **No manual dirty tracking needed**: Remove _dirty/_version columns, use turso_cdc instead
  - [ ] Enable CDC per connection: `PRAGMA unstable_capture_data_changes_conn('full')`
  - [ ] Query turso_cdc for changes: `SELECT * FROM turso_cdc WHERE table_name = 'blocks' AND change_id > ?`
  - [ ] Test CDC with experimental materialized views (if using PRQL-generated views)
- [ ] Integration tests: Mutation → turso_cdc capture → Query changes → RowEvent stream

**Files**: `crates/holon/src/operations/` (operations mutate via turso.rs, no separate executor)

#### 3.4: Stream-Based External System Integration (Reactive Sync) ✅ COMPLETE
**Status**: Stream-based architecture fully implemented and compiling
**Completed**: 2025-01-09

**Design Decision**: Provider-centric stream architecture with fire-and-forget operations
- External changes happen outside app's control → need continuous sync
- Stream-based push model replaces pull-based `IncrementalSync` trait
- One `sync()` call → multiple typed streams → separate caches
- **QueryableCache as transparent proxy**: Wraps datasource, implements both read and write traits
- **Fake implements same DataSource trait, emits its own stream**: Simulates external API behavior for testing/offline mode
- Clean separation: Operations (business logic) → Cache (proxy) → DataSource (primitives) → External API

**Architecture Overview**:
```
┌─────────────────────────────────────────────────────────┐
│ UI Layer                                                 │
│  - Calls operations via trait methods on cache          │
│  - Subscribes to CDC stream for UI updates              │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ QueryableCache<T> (Transparent Proxy + Operations)      │
│  - datasource: Arc<dyn CrudOperationProvider<T>>            │
│  - db: TursoBackend (local cache)                       │
│                                                          │
│ Implements DataSource<T>:                               │
│  - get_all() → query local db (FAST)                    │
│  - get_by_id() → query local db (FAST)                  │
│                                                          │
│ Implements CrudOperationProvider<T>:                        │
│  - set_field() → delegate to datasource (pass-through)  │
│  - create() → delegate to datasource                    │
│  - delete() → delegate to datasource                    │
│                                                          │
│ Implements MutableBlockDataSource<T> (if T: BlockEntity):│
│  - indent_block() → read cache + write via primitives   │
│  - move_block() → read cache + write via primitives     │
│  - outdent_block() → read cache + write via primitives  │
│                                                          │
│ Implements MutableTaskDataSource<T> (if T: TaskEntity): │
│  - set_completion() → delegates to set_field()          │
│  - set_priority() → delegates to set_field()            │
│  - set_due_date() → delegates to set_field()            │
│  (both get default impls automatically via blanket impl)│
│                                                          │
│ Stream Ingestion:                                       │
│  - ingest_stream(rx) → updates local db                 │
└─────────────────────────────────────────────────────────┘
        ↓ (delegates writes)              ↑ (receives changes)
        ↓                                  ↑
┌──────────────────────┐         ┌──────────────────────┐
│ TodoistTaskDataSource│   OR    │ TodoistTaskFake      │
│ (STATELESS)          │         │ (STATELESS)          │
│  - client: HTTP      │         │  - db: TursoBackend  │
│  - set_field() →     │         │  - change_tx: Sender │
│    POST /tasks/:id   │         │  - set_field() →     │
│    returns ()        │         │    UPDATE db + emit  │
└──────────────────────┘         └──────────────────────┘
        ↓                                  ↓
   (HTTP call)                        (emits change)
        ↓                                  ↓
┌──────────────────────┐         ┌──────────────────────┐
│ External Todoist API │         │ Stream (fake's own)  │
└──────────────────────┘         └──────────────────────┘
        ↓
   (state changes)
        ↓
┌─────────────────────────────────────────┐
│ TodoistProvider (polls external API)    │
│  - sync() → GET /sync/v9/sync           │
│  - change_tx: Sender<Vec<Change<T>>>    │
└─────────────────────────────────────────┘
        ↓
   (emits changes on stream)
```

**Key Innovations**:
1. **Eliminates redundant API calls**: Todoist returns tasks + projects + labels in one call, emits on multiple streams
2. **Fire-and-forget operations**: Return `Result<()>` (or `Result<String>` for create), updates arrive asynchronously
3. **Cache isolation**: QueryableCache wraps datasource, not contained within it (production datasource stays stateless)
4. **Fake emits stream**: TodoistTaskFake has its own broadcast channel, simulates external API behavior
5. **Unified interface**: Real and fake both implement CrudOperationProvider<T>, identical from cache's perspective
6. **No contract-specs**: Hand-written fakes are simpler, type-safe, and easier to maintain
7. **Trait-based operations**: `#[operations_trait]` macro generates OperationDescriptors from trait methods automatically
8. **Blanket impl for complex operations**: QueryableCache gets task-specific methods automatically via trait bounds

**Implementation Tasks**:

- [x] **Core Traits** (`crates/holon/src/core/datasource.rs`): ✅ **COMPLETE**
  ```rust
  // Change representation for stream updates
  pub enum Change<T> {
      Upsert(T),       // Insert or update (can't distinguish from API)
      Delete(String),  // ID of deleted item
  }

  // Operation metadata (for UI generation)
  pub struct OperationDescriptor {
      pub name: String,
      pub description: String,
      pub params: Vec<ParamDescriptor>,
  }

  pub struct ParamDescriptor {
      pub name: String,
      pub param_type: String,  // "String", "bool", "i64", etc.
      pub required: bool,
      pub default: Option<Value>,
  }

  // Read-only data access (from cache)
  #[async_trait]
  pub trait DataSource<T>: Send + Sync
  where
      T: Send + Sync + 'static,
  {
      async fn get_all(&self) -> Result<Vec<T>>;
      async fn get_by_id(&self, id: &str) -> Result<Option<T>>;

      // Helper queries (default implementations)
      async fn get_children(&self, parent_id: &str) -> Result<Vec<T>> {
          Ok(self.get_all().await?
              .into_iter()
              .filter(|t| t.parent_id() == Some(parent_id))
              .collect())
      }
  }

  // Write-only primitives (fire-and-forget to external system)
  #[operations_trait]  // 🔑 Generates MUTABLE_DATA_SOURCE_OPERATIONS array
  #[async_trait]
  pub trait CrudOperationProvider<T: Entity>: Send + Sync {
      /// Set single field (returns () - update arrives via stream)
      async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<()>;

      /// Create new entity (returns new ID immediately, full data via stream)
      async fn create(&self, fields: HashMap<String, Value>) -> Result<String>;

      /// Delete entity (returns () - deletion confirmed via stream)
      async fn delete(&self, id: &str) -> Result<()>;
  }

  // Entity capability markers (composition via trait bounds)
  /// Entities that support hierarchical tree structure
  pub trait BlockEntity: Send + Sync {
      fn parent_id(&self) -> Option<&str>;
      fn sort_key(&self) -> &str;
      fn depth(&self) -> i64;
  }

  /// Entities that support task management (completion, priority, etc.)
  pub trait TaskEntity: Send + Sync {
      fn completed(&self) -> bool;
      fn priority(&self) -> Option<i64>;
      fn due_date(&self) -> Option<DateTime<Utc>>;
  }

  // Hierarchical structure operations (for any block-like entity)
  #[operations_trait]  // 🔑 Generates MUTABLE_BLOCK_DATA_SOURCE_OPERATIONS array
  #[async_trait]
  pub trait MutableBlockDataSource<T: BlockEntity>:
      CrudOperationProvider<T> + DataSource<T>  // 🔑 Requires both read and write
  {
      /// Move block under a new parent (increase indentation)
      async fn indent_block(&self, id: &str, new_parent_id: &str) -> Result<()> {
          // Query cache for current state (fast - no network)
          let parent = self.get_by_id(new_parent_id).await?
              .ok_or_else(|| anyhow!("Parent not found"))?;
          let siblings = self.get_children(new_parent_id).await?;

          // Calculate new position via fractional indexing
          let sort_key = gen_key_between(
              siblings.last().map(|s| s.sort_key()),
              None
          )?;

          // Execute primitives (delegates to self.set_field)
          self.set_field(id, "parent_id", json!(new_parent_id)).await?;
          self.set_field(id, "depth", json!(parent.depth() + 1)).await?;
          self.set_field(id, "sort_key", json!(sort_key)).await?;
          Ok(())
      }

      /// Move block to different position (reorder within same parent or different parent)
      async fn move_block(&self, id: &str, after_id: Option<&str>) -> Result<()> {
          // Calculate new sort_key based on neighbors
          let (prev_key, next_key) = if let Some(after) = after_id {
              let after_block = self.get_by_id(after).await?
                  .ok_or_else(|| anyhow!("Reference block not found"))?;
              let next = self.get_next_sibling(after).await?;
              (Some(after_block.sort_key().to_string()), next.map(|b| b.sort_key().to_string()))
          } else {
              (None, self.get_first_sibling().await?.map(|b| b.sort_key().to_string()))
          };

          let new_key = gen_key_between(prev_key.as_deref(), next_key.as_deref())?;
          self.set_field(id, "sort_key", json!(new_key)).await
      }

      /// Move block out to parent's level (decrease indentation)
      async fn outdent_block(&self, id: &str) -> Result<()> {
          let block = self.get_by_id(id).await?
              .ok_or_else(|| anyhow!("Block not found"))?;
          let parent_id = block.parent_id()
              .ok_or_else(|| anyhow!("Cannot outdent root block"))?;

          let grandparent = self.get_by_id(parent_id).await?
              .ok_or_else(|| anyhow!("Parent not found"))?;
          let grandparent_id = grandparent.parent_id();

          // Move to grandparent's children
          let new_depth = block.depth() - 1;
          self.set_field(id, "parent_id", json!(grandparent_id)).await?;
          self.set_field(id, "depth", json!(new_depth)).await?;
          Ok(())
      }
  }

  // Task management operations (for any task-like entity)
  #[operations_trait]  // 🔑 Generates MUTABLE_TASK_DATA_SOURCE_OPERATIONS array
  #[async_trait]
  pub trait MutableTaskDataSource<T: TaskEntity>:
      CrudOperationProvider<T> + DataSource<T>
  {
      /// Toggle or set task completion status
      async fn set_completion(&self, id: &str, completed: bool) -> Result<()> {
          self.set_field(id, "completed", json!(completed)).await
      }

      /// Set task priority (1=highest, 4=lowest in Todoist)
      async fn set_priority(&self, id: &str, priority: i64) -> Result<()> {
          self.set_field(id, "priority", json!(priority)).await
      }

      /// Set task due date
      async fn set_due_date(&self, id: &str, due_date: Option<DateTime<Utc>>) -> Result<()> {
          self.set_field(id, "due_date", json!(due_date.map(|d| d.to_rfc3339()))).await
      }
  }

  // Blanket implementations: Automatically provide operations for any compatible type
  impl<T: BlockEntity, D> MutableBlockDataSource<T> for D
  where
      D: DataSource<T> + CrudOperationProvider<T>
  {}

  impl<T: TaskEntity, D> MutableTaskDataSource<T> for D
  where
      D: DataSource<T> + CrudOperationProvider<T>
  {}
  ```

  **Usage Examples**:

  1. **Blocks only** (hierarchical outline, not tasks):
     ```rust
     struct OutlineBlock {
         id: String,
         parent_id: Option<String>,
         sort_key: String,
         depth: i64,
         content: String,
     }

     impl BlockEntity for OutlineBlock { /* ... */ }
     // Gets indent_block, move_block, outdent_block via MutableBlockDataSource
     ```

  2. **Tasks only** (flat task list, no hierarchy):
     ```rust
     struct FlatTask {
         id: String,
         completed: bool,
         priority: Option<i64>,
         due_date: Option<DateTime<Utc>>,
     }

     impl TaskEntity for FlatTask { /* ... */ }
     // Gets set_completion, set_priority, set_due_date via MutableTaskDataSource
     ```

  3. **Blocks AND Tasks** (Todoist tasks with sub-tasks):
     ```rust
     struct TodoistTask {
         id: String,
         parent_id: Option<String>,
         sort_key: String,
         depth: i64,
         completed: bool,
         priority: Option<i64>,
         // ... other fields
     }

     impl BlockEntity for TodoistTask { /* ... */ }
     impl TaskEntity for TodoistTask { /* ... */ }
     // Gets ALL operations from both MutableBlockDataSource AND MutableTaskDataSource!
     // Can call: indent_block, move_block, outdent_block, set_completion, set_priority, etc.
     ```
  ```

  **Implementation Hints**:

  1. **`#[operations_trait]` macro challenges**:
     - Must handle `#[async_trait]` compatibility (apply `#[async_trait]` first, then `#[operations_trait]`)
     - Extract method signatures from trait definition (not impl blocks)
     - Generate statics at module level (not inside trait)
     - Trait methods can't have bodies in non-default impls → use default trait methods

  2. **Blanket impl gotcha**:
     - `QueryableCache<T>` implements both `DataSource<T>` and `CrudOperationProvider<T>`
     - Therefore automatically implements `MutableTaskDataSource<T>` via blanket impl
     - NO explicit impl needed for QueryableCache! (Rust's orphan rules allow this)

  3. **Operations metadata aggregation**:
     ```rust
     // Generated by #[operations_trait] macro:
     static MUTABLE_DATA_SOURCE_OPERATIONS: &[OperationDescriptor] = &[
         SET_FIELD_OP, CREATE_OP, DELETE_OP
     ];

     static MUTABLE_BLOCK_DATA_SOURCE_OPERATIONS: &[OperationDescriptor] = &[
         INDENT_BLOCK_OP, MOVE_BLOCK_OP, OUTDENT_BLOCK_OP
     ];

     static MUTABLE_TASK_DATA_SOURCE_OPERATIONS: &[OperationDescriptor] = &[
         SET_COMPLETION_OP, SET_PRIORITY_OP, SET_DUE_DATE_OP
     ];

     // Entity types declare which operations they support
     trait OperationRegistry {
         fn all_operations() -> Vec<OperationDescriptor>;
     }

     impl OperationRegistry for TodoistTask {
         fn all_operations() -> Vec<OperationDescriptor> {
             MUTABLE_DATA_SOURCE_OPERATIONS.iter()
                 .chain(MUTABLE_BLOCK_DATA_SOURCE_OPERATIONS.iter())
                 .chain(MUTABLE_TASK_DATA_SOURCE_OPERATIONS.iter())
                 .cloned()
                 .collect()
         }
     }

     impl OperationRegistry for OutlineBlock {
         fn all_operations() -> Vec<OperationDescriptor> {
             MUTABLE_DATA_SOURCE_OPERATIONS.iter()
                 .chain(MUTABLE_BLOCK_DATA_SOURCE_OPERATIONS.iter())
                 .cloned()
                 .collect()
         }
     }

     // Cache discovers operations from entity type
     impl<T: Entity + OperationRegistry> QueryableCache<T> {
         pub fn operations(&self) -> Vec<OperationDescriptor> {
             T::all_operations()
         }
     }
     ```

  4. **Type parameter constraints**:
     - `TaskEntity` is a marker trait: `trait TaskEntity: Entity {}`
     - Allows restricting task-specific operations to task types only
     - Prevents calling `indent_task()` on non-task entities
  ```

- [x] **Real DataSource** (`crates/holon-todoist/src/stream_datasource.rs`): ✅ **COMPLETE**
  ```rust
  pub struct TodoistTaskDataSource {
      client: TodoistClient,  // NO cache - stateless!
  }

  #[async_trait]
  impl CrudOperationProvider<TodoistTask> for TodoistTaskDataSource {
      async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<()> {
          // HTTP call, returns immediately
          self.client
              .patch(&format!("/rest/v2/tasks/{}", id))
              .json(&json!({ field: value }))
              .send()
              .await?;

          Ok(())  // Fire-and-forget! Update arrives via Provider stream
      }

      async fn create(&self, fields: HashMap<String, Value>) -> Result<String> {
          let response: TodoistTask = self.client
              .post("/rest/v2/tasks")
              .json(&fields)
              .send()
              .await?
              .json()
              .await?;

          Ok(response.id)  // Return ID immediately
      }

      async fn delete(&self, id: &str) -> Result<()> {
          self.client
              .delete(&format!("/rest/v2/tasks/{}", id))
              .send()
              .await?;
          Ok(())
      }

      fn operations(&self) -> &[OperationDescriptor] {
          &TODOIST_TASK_OPERATIONS  // Static metadata
      }
  }
  ```

- [x] **Fake DataSource** (`crates/holon-todoist/src/fake.rs`): ✅ **COMPLETE**
  ```rust
  pub struct TodoistTaskFake {
      // Read access to cache (for getting current state)
      cache: Arc<dyn DataSource<TodoistTask>>,
      change_tx: broadcast::Sender<Vec<Change<TodoistTask>>>,  // 🔑 Emits own stream!
  }

  impl TodoistTaskFake {
      pub fn new(cache: Arc<dyn DataSource<TodoistTask>>)
          -> (Self, broadcast::Receiver<Vec<Change<TodoistTask>>>)
      {
          let (tx, rx) = broadcast::channel(100);
          let fake = Self { cache, change_tx: tx };
          (fake, rx)
      }
  }

  #[async_trait]
  impl CrudOperationProvider<TodoistTask> for TodoistTaskFake {
      async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<()> {
          // Read current state from cache (not from own DB!)
          let mut task = self.cache.get_by_id(id).await?
              .ok_or_else(|| anyhow!("Task not found"))?;

          // Apply field update
          match field {
              "content" => {
                  if let Value::String(s) = value {
                      task.content = s;
                  }
              }
              "description" => {
                  task.description = match value {
                      Value::String(s) => Some(s),
                      Value::Null => None,
                      _ => return Err(anyhow!("Invalid value type").into()),
                  };
              }
              // ... handle other fields ...
              _ => return Err(anyhow!("Unknown field: {}", field).into()),
          }

          // Emit change event - cache will receive this and update its database
          // (we don't write to cache directly!)
          let _ = self.change_tx.send(vec![Change::Upsert(task)]);
          Ok(())
      }

      async fn create(&self, mut fields: HashMap<String, Value>) -> Result<String> {
          let id = Uuid::new_v4().to_string();
          fields.insert("id".to_string(), json!(id));
          fields.insert("created_at".to_string(), json!(Utc::now().to_rfc3339()));

          // Build task from fields
          let new_task: TodoistTask = fields.try_into()?;

          // Emit change event - cache will receive this and update its database
          // (we don't write to cache directly!)
          let _ = self.change_tx.send(vec![Change::Upsert(new_task.clone())]);
          Ok(id)
      }

      async fn delete(&self, id: &str) -> Result<()> {
          // Verify task exists in cache (read-only check)
          if self.cache.get_by_id(id).await?.is_none() {
              return Err(anyhow!("Task not found").into());
          }

          // Emit change event - cache will receive this and update its database
          // (we don't write to cache directly!)
          let _ = self.change_tx.send(vec![Change::Delete(id.to_string())]);
          Ok(())
      }

      fn operations(&self) -> &[OperationDescriptor] {
          &TODOIST_TASK_OPERATIONS  // Same as real!
      }
  }
  ```

- [x] **QueryableCache as Transparent Proxy** (`crates/holon/src/core/stream_cache.rs`): ✅ **COMPLETE**
  ```rust
  pub struct QueryableCache<T> {
      datasource: Arc<dyn CrudOperationProvider<T>>,
      db: TursoBackend,
      table: String,
  }

  impl<T: Entity> QueryableCache<T> {
      pub fn new(
          datasource: Arc<dyn CrudOperationProvider<T>>,
          db: TursoBackend,
          table: String,
      ) -> Self {
          Self { datasource, db, table }
      }

      /// Wire up stream ingestion (spawns background task)
      pub fn ingest_stream(&self, mut rx: broadcast::Receiver<Vec<Change<T>>>) {
          let db = self.db.clone();
          let table = self.table.clone();

          tokio::spawn(async move {
              loop {
                  match rx.recv().await {
                      Ok(changes) => {
                          for change in changes {
                              match change {
                                  Change::Upsert(item) => {
                                      let _ = upsert_to_db(&db, &table, item).await;
                                  }
                                  Change::Delete(id) => {
                                      let _ = db.delete(&table, &id).await;
                                  }
                              }
                          }
                      }
                      Err(broadcast::error::RecvError::Lagged(n)) => {
                          warn!("Stream lagged by {}, triggering resync", n);
                          // TODO: Trigger full resync
                      }
                      Err(broadcast::error::RecvError::Closed) => break,
                  }
              }
          });
      }
  }

  // Implement DataSource (reads from cache)
  #[async_trait]
  impl<T: DeserializeOwned + Send + Sync + 'static> DataSource<T> for QueryableCache<T> {
      async fn get_all(&self) -> Result<Vec<T>> {
          let entities = self.db.query(&self.table, HashMap::new()).await?;
          entities.into_iter()
              .map(|e| Ok(serde_json::from_value(serde_json::to_value(e)?)?))
              .collect()
      }

      async fn get_by_id(&self, id: &str) -> Result<Option<T>> {
          match self.db.get(&self.table, id).await? {
              Some(entity) => Ok(Some(serde_json::from_value(
                  serde_json::to_value(entity)?
              )?)),
              None => Ok(None),
          }
      }
  }

  // Implement CrudOperationProvider (delegates to wrapped datasource)
  #[async_trait]
  impl<T: Send + Sync + 'static> CrudOperationProvider<T> for QueryableCache<T> {
      async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<()> {
          self.datasource.set_field(id, field, value).await
          // Update arrives via stream, ingested into local DB
      }

      async fn create(&self, fields: HashMap<String, Value>) -> Result<String> {
          self.datasource.create(fields).await
          // Full entity arrives via stream
      }

      async fn delete(&self, id: &str) -> Result<()> {
          self.datasource.delete(id).await
          // Deletion confirmed via stream
      }

      fn operations(&self) -> &[OperationDescriptor] {
          self.datasource.operations()
      }
  }
  ```

- [x] **Provider Layer with Builder Pattern** (`crates/holon-todoist/src/stream_provider.rs`): ✅ **COMPLETE**

  **Phase 1: Explicit Builder (Immediate Implementation)**
  ```rust
  pub struct TodoistProvider {
      client: TodoistClient,
      task_tx: broadcast::Sender<Vec<Change<TodoistTask>>>,
      project_tx: broadcast::Sender<Vec<Change<TodoistProject>>>,
      sync_token: Arc<RwLock<Option<String>>>,
  }

  pub struct TodoistProviderBuilder {
      provider: TodoistProvider,
      registrations: Vec<Box<dyn FnOnce(&TodoistProvider) + Send>>,
  }

  impl TodoistProvider {
      pub fn new(client: TodoistClient) -> TodoistProviderBuilder {
          let (task_tx, _) = broadcast::channel(100);
          let (project_tx, _) = broadcast::channel(100);

          TodoistProviderBuilder {
              provider: TodoistProvider {
                  client,
                  task_tx,
                  project_tx,
                  sync_token: Arc::new(RwLock::new(None)),
              },
              registrations: vec![],
          }
      }

      /// Trigger sync - ONE API call, emits on multiple streams
      pub async fn sync(&mut self) -> Result<()> {
          let token = self.sync_token.read().await.clone();
          let response = self.client.sync_items(token).await?;

          // Update sync token
          if let Some(new_token) = response.sync_token {
              *self.sync_token.write().await = Some(new_token);
          }

          // Split and emit on separate typed streams
          let _ = self.task_tx.send(compute_task_changes(&response));
          let _ = self.project_tx.send(compute_project_changes(&response));

          Ok(())
      }
  }

  impl TodoistProviderBuilder {
      /// Register a cache for tasks
      pub fn with_tasks(mut self, cache: Arc<QueryableCache<TodoistTask>>) -> Self {
          self.registrations.push(Box::new(move |provider: &TodoistProvider| {
              let rx = provider.task_tx.subscribe();
              cache.ingest_stream(rx);
          }));
          self
      }

      /// Register a cache for projects
      pub fn with_projects(mut self, cache: Arc<QueryableCache<TodoistProject>>) -> Self {
          self.registrations.push(Box::new(move |provider: &TodoistProvider| {
              let rx = provider.project_tx.subscribe();
              cache.ingest_stream(rx);
          }));
          self
      }

      /// Build the provider and execute all registrations
      pub fn build(self) -> TodoistProvider {
          for register in self.registrations {
              register(&self.provider);
          }
          self.provider
      }
  }

  // Type-independent sync trait
  trait SyncableProvider {
      async fn sync(&mut self) -> Result<()>;
  }

  impl SyncableProvider for TodoistProvider {
      async fn sync(&mut self) -> Result<()> {
          self.sync().await
      }
  }
  ```

  **Phase 2: CacheFactory Extension (Future Enhancement)**
  ```rust
  /// Factory that knows how to create caches for entity types
  pub trait CacheFactory {
      /// Try to create a cache for type T
      fn create_cache<T: Entity + 'static>(&self) -> Option<Arc<QueryableCache<T>>>;
  }

  impl TodoistProviderBuilder {
      /// Auto-register all entity types the factory knows about
      pub fn auto_register(mut self, factory: &impl CacheFactory) -> Self {
          // Try to create cache for each known entity type
          if let Some(cache) = factory.create_cache::<TodoistTask>() {
              self = self.with_tasks(cache);
          }
          if let Some(cache) = factory.create_cache::<TodoistProject>() {
              self = self.with_projects(cache);
          }
          // Future: Could use type registry to discover all types automatically
          self
      }
  }
  ```

  **Benefits**:
  - ✅ Type-safe - compiler ensures correct cache types
  - ✅ Explicit - clear what's being wired up
  - ✅ Flexible - can add/remove entity types easily
  - ✅ Future extensible - CacheFactory allows plug-and-play

- [ ] **NOTE: Operations Now Live on Traits** (No separate Operations struct needed):

  With the trait-based architecture, operations are called **directly on the cache**:

  ```rust
  // ✅ NEW: Direct trait method calls
  let cache = Arc::new(QueryableCache::new(...));

  // Primitive operations (CrudOperationProvider)
  cache.set_field(id, "content", json!("new content")).await?;
  cache.create(fields).await?;
  cache.delete(id).await?;

  // Block operations (MutableBlockDataSource - if T: BlockEntity)
  cache.indent_block(id, parent_id).await?;
  cache.move_block(id, Some(after_id)).await?;
  cache.outdent_block(id).await?;

  // Task operations (MutableTaskDataSource - if T: TaskEntity)
  cache.set_completion(id, true).await?;
  cache.set_priority(id, 1).await?;
  cache.set_due_date(id, Some(due_date)).await?;

  // Operations metadata (via OperationRegistry trait)
  let ops = cache.operations(); // Vec<OperationDescriptor>
  ```

  **Optional: Legacy TodoistTaskOperations Wrapper** (for backward compatibility):
  ```rust
  /// Optional wrapper for backward compatibility
  /// NOT NEEDED for new code - use cache directly
  pub struct TodoistTaskOperations {
      cache: Arc<QueryableCache<TodoistTask>>,
  }

  impl TodoistTaskOperations {
      pub fn new(cache: Arc<QueryableCache<TodoistTask>>) -> Self {
          Self { cache }
      }

      /// Delegate to cache.indent_block()
      pub async fn indent_block(&self, id: &str, new_parent_id: &str) -> Result<()> {
          self.cache.indent_block(id, new_parent_id).await
      }

      /// Delegate to cache.set_completion()
      pub async fn set_completion(&self, id: &str, completed: bool) -> Result<()> {
          self.cache.set_completion(id, completed).await
      }

      // ... other delegations
  }
  ```

- [ ] **`#[operations_trait]` Macro** (`crates/holon-macros/src/lib.rs`): ⏳ **DEFERRED**

  Generates operation descriptors for all methods in a trait.

  **Usage**:
  ```rust
  #[operations_trait]  // Must come BEFORE #[async_trait]
  #[async_trait]
  pub trait MutableBlockDataSource<T: BlockEntity>: ... { ... }
  ```

  **Generates**:
  - One `const OPERATION_NAME_OP: OperationDescriptor` per method
  - One `static TRAIT_NAME_OPERATIONS: &[OperationDescriptor]` array

  **Implementation**:
  ```rust
  #[proc_macro_attribute]
  pub fn operations_trait(_attr: TokenStream, item: TokenStream) -> TokenStream {
      let trait_def = parse_macro_input!(item as ItemTrait);

      let trait_name = &trait_def.ident;
      let trait_name_upper = trait_name.to_string().to_uppercase();
      let operations_array_name = format_ident!("{}_OPERATIONS", trait_name_upper);

      // 1. Extract all async fn methods (skip associated types, consts, etc.)
      let methods: Vec<_> = trait_def.items.iter()
          .filter_map(|item| match item {
              TraitItem::Method(method) if method.sig.asyncness.is_some() => Some(method),
              _ => None,
          })
          .collect();

      // 2. Generate OperationDescriptor const for each method
      let operation_consts: Vec<_> = methods.iter()
          .map(|method| {
              let method_name = &method.sig.ident;
              let const_name = format_ident!("{}_OP", method_name.to_string().to_uppercase());

              // Extract doc comments for description
              let description = extract_doc_comments(&method.attrs);

              // Extract parameters (skip &self)
              let params = method.sig.inputs.iter()
                  .skip(1)  // Skip &self
                  .filter_map(|arg| match arg {
                      FnArg::Typed(pat_type) => {
                          let param_name = extract_param_name(&pat_type.pat);
                          let (type_str, required) = infer_type(&pat_type.ty);
                          Some(quote! {
                              ParamDescriptor {
                                  name: #param_name,
                                  param_type: #type_str,
                                  required: #required,
                                  default: None,
                              }
                          })
                      }
                      _ => None,
                  })
                  .collect::<Vec<_>>();

              quote! {
                  const #const_name: OperationDescriptor = OperationDescriptor {
                      name: stringify!(#method_name),
                      description: #description,
                      params: &[ #(#params),* ],
                  };
              }
          })
          .collect();

      // 3. Generate static array of all operations
      let operation_refs: Vec<_> = methods.iter()
          .map(|method| {
              let method_name = &method.sig.ident;
              let const_name = format_ident!("{}_OP", method_name.to_string().to_uppercase());
              quote! { #const_name }
          })
          .collect();

      let expanded = quote! {
          // Original trait (unchanged)
          #trait_def

          // Generated operation descriptors (at module level)
          #(#operation_consts)*

          // Generated operations array
          static #operations_array_name: &[OperationDescriptor] = &[
              #(#operation_refs),*
          ];
      };

      expanded.into()
  }

  // Helper functions (same as in #[operation] macro)
  fn extract_doc_comments(attrs: &[Attribute]) -> String { ... }
  fn extract_param_name(pat: &Pat) -> String { ... }
  fn infer_type(ty: &Type) -> (String, bool) { ... }
  ```

  **Generated Output Example**:
  ```rust
  // Input:
  #[operations_trait]
  #[async_trait]
  pub trait MutableBlockDataSource<T: BlockEntity>: ... {
      /// Move block under a new parent
      async fn indent_block(&self, id: &str, new_parent_id: &str) -> Result<()> { ... }

      async fn move_block(&self, id: &str, after_id: Option<&str>) -> Result<()> { ... }

      async fn outdent_block(&self, id: &str) -> Result<()> { ... }
  }

  // Output:
  #[async_trait]
  pub trait MutableBlockDataSource<T: BlockEntity>: ... {
      async fn indent_block(&self, id: &str, new_parent_id: &str) -> Result<()> { ... }
      async fn move_block(&self, id: &str, after_id: Option<&str>) -> Result<()> { ... }
      async fn outdent_block(&self, id: &str) -> Result<()> { ... }
  }

  const INDENT_BLOCK_OP: OperationDescriptor = OperationDescriptor {
      name: "indent_block",
      description: "Move block under a new parent",
      params: &[
          ParamDescriptor { name: "id", param_type: "String", required: true, default: None },
          ParamDescriptor { name: "new_parent_id", param_type: "String", required: true, default: None },
      ],
  };

  const MOVE_BLOCK_OP: OperationDescriptor = OperationDescriptor {
      name: "move_block",
      description: "",
      params: &[
          ParamDescriptor { name: "id", param_type: "String", required: true, default: None },
          ParamDescriptor { name: "after_id", param_type: "String", required: false, default: None },
      ],
  };

  const OUTDENT_BLOCK_OP: OperationDescriptor = OperationDescriptor {
      name: "outdent_block",
      description: "",
      params: &[
          ParamDescriptor { name: "id", param_type: "String", required: true, default: None },
      ],
  };

  static MUTABLE_BLOCK_DATA_SOURCE_OPERATIONS: &[OperationDescriptor] = &[
      INDENT_BLOCK_OP,
      MOVE_BLOCK_OP,
      OUTDENT_BLOCK_OP,
  ];
  ```

- [ ] **`#[operation]` Macro** (for standalone functions - OPTIONAL): ⏳ **DEFERRED**

- [x] **Wiring Examples**: ✅ **COMPLETE** (implementation matches examples)

  **Production Setup with Builder Pattern** (`main.rs` or `app.rs`):
  ```rust
  async fn setup_production() -> Result<Arc<QueryableCache<TodoistTask>>> {
      let client = TodoistClient::new(api_key);
      let db = TursoBackend::new("app.db").await?;

      // Create stateless datasource
      let datasource = Arc::new(TodoistTaskDataSource::new(client.clone()));

      // Wrap in cache
      let task_cache = Arc::new(QueryableCache::new(
          datasource,
          db.clone(),
          "todoist_tasks".to_string(),
      ));

      let project_cache = Arc::new(QueryableCache::new(
          Arc::new(TodoistProjectDataSource::new(client.clone())),
          db.clone(),
          "todoist_projects".to_string(),
      ));

      // Build provider with builder pattern
      let provider = TodoistProvider::new(client)
          .with_tasks(task_cache.clone())
          .with_projects(project_cache)
          .build();

      // Start periodic sync
      tokio::spawn(async move {
          loop {
              if let Err(e) = provider.sync().await {
                  error!("Sync failed: {}", e);
              }
              tokio::time::sleep(Duration::from_secs(60)).await;
          }
      });

      // UI calls operations directly on cache:
      // task_cache.indent_block(id, parent_id).await?;
      // task_cache.set_completion(id, true).await?;

      Ok(task_cache)
  }
  ```

  **Testing/Offline Setup**:
  ```rust
  async fn setup_fake() -> Result<Arc<QueryableCache<TodoistTask>>> {
      let db = Arc::new(TursoBackend::new_in_memory().await?);

      // Create fake with its own stream
      let (fake, rx) = TodoistTaskFake::new(db.clone(), "todoist_tasks".to_string());

      // Wrap in cache
      let cache = Arc::new(QueryableCache::new(
          Arc::new(fake),
          (*db).clone(),
          "todoist_tasks".to_string(),
      ));

      // Wire up stream from FAKE (not external provider!)
      cache.ingest_stream(rx);

      // No periodic sync needed - fake is synchronous

      // UI calls operations directly on cache:
      // cache.indent_block(id, parent_id).await?;
      // cache.set_completion(id, true).await?;

      Ok(cache)
  }
  ```

  **Future: CacheFactory Pattern**:
  ```rust
  async fn setup_with_factory() -> Result<TodoistProvider> {
      let client = TodoistClient::new(api_key);
      let factory = MyCacheFactory::new(db);

      // Auto-discovers and wires up all supported types
      let provider = TodoistProvider::new(client)
          .auto_register(&factory)
          .build();

      Ok(provider)
  }
  ```

- [ ] **Optional: Anodized Pre/Post-Conditions** (`Cargo.toml` feature flag): ⏳ **DEFERRED**
  ```rust
  use anodized::{precondition, postcondition};

  #[async_trait]
  impl CrudOperationProvider<TodoistTask> for TodoistTaskFake {
      #[precondition(self.db.get(&self.table, id).await?.is_some(), "Task must exist")]
      #[precondition(!field.is_empty(), "Field name cannot be empty")]
      #[postcondition(
          self.db.get(&self.table, id).await?.unwrap().get(field) == Some(&value),
          "Field must be updated"
      )]
      async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<()> {
          // Implementation
      }
  }
  ```

- [x] **Integration Tests** (`crates/holon-todoist/src/stream_integration_test.rs`): ✅ **COMPLETE**
  - ✅ Verify fake datasource emits stream on operations
  - ✅ Test cache reads from local database
  - ✅ Test cache delegates writes to datasource
  - ✅ Test cache updates via stream ingestion
  - ✅ Test provider → cache stream flow
  - ✅ Test multiple caches receiving same stream
  - ✅ Test delete operation and stream propagation
  - ✅ Test get_all() returns all tasks from cache
  - 8 integration tests passing

**Implementation Summary**:

**Completed Components**:
1. ✅ **Core Traits** (`datasource.rs`):
   - `Change<T>` enum (Upsert/Delete)
   - `OperationDescriptor` and `ParamDescriptor` structs
   - `DataSource<T>` trait (read-only)
   - `CrudOperationProvider<T>` trait (write primitives)
   - `BlockEntity` and `TaskEntity` marker traits
   - `MutableBlockDataSource<T>` and `MutableTaskDataSource<T>` traits with default implementations
   - Blanket implementations for automatic trait composition

2. ✅ **QueryableCache** (`stream_cache.rs`):
   - Transparent proxy wrapping `CrudOperationProvider<T>`
   - Implements both `DataSource<T>` (reads from local DB) and `CrudOperationProvider<T>` (delegates writes)
   - Stream ingestion via `ingest_stream()` method (spawns background task)
   - Handles broadcast channel lag and closed errors
   - Exported as `StreamCache` to avoid naming conflicts

3. ✅ **TodoistTaskFake** (`fake.rs`):
   - Implements `DataSource<TodoistTask>` and `CrudOperationProvider<TodoistTask>`
   - Uses local `TursoBackend` for storage
   - Emits changes via `broadcast::Sender<Vec<Change<TodoistTask>>>`
   - Simulates external API behavior for testing/offline mode
   - All operations update DB and emit change events

4. ✅ **TodoistTaskDataSource** (`stream_datasource.rs`):
   - Real HTTP implementation of `CrudOperationProvider<TodoistTask>`
   - Stateless (no cache - cache is in QueryableCache)
   - Fire-and-forget operations (returns immediately)
   - Lifetime-safe string handling (clones strings to avoid lifetime issues)
   - Uses `TodoistClient` for HTTP calls

5. ✅ **TodoistProvider** (`stream_provider.rs`):
   - Builder pattern for cache registration (`with_tasks()`, `with_projects()`)
   - Single `sync()` call emits on multiple typed streams
   - Manages sync tokens and change propagation
   - Helper functions `compute_task_changes()` and `compute_project_changes()`
   - Type-safe per-entity-type broadcast channels

6. ✅ **Model Updates** (`models.rs`):
   - `TodoistTask` implements `BlockEntity` trait (parent_id, sort_key, depth)
   - `TodoistTask` implements `TaskEntity` trait (completed, priority, due_date)
   - Placeholder implementations for sort_key and depth (TODO: compute from order/created_at)

7. ✅ **Integration Tests** (`stream_integration_test.rs`):
   - 8 comprehensive tests covering stream propagation, cache behavior, and operation delegation
   - All tests passing ✅

**Compilation Status**:
- ✅ `holon` crate: compiles successfully (3 warnings)
- ✅ `holon-todoist` crate: compiles successfully (6 warnings)

**Key Fixes Applied**:
- Fixed API mismatches (StorageEntity methods, Value types)
- Fixed lifetime issues in stream_datasource (string cloning)
- Fixed Clone trait bounds for broadcast channels
- Removed missing `contracts` module reference
- Fixed broken `DataSource` implementation in `command_sourcing.rs`

**Architecture Flow**:
```
Provider.sync() → API call → Split into changes → Emit on streams
                                                         ↓
Cache.ingest_stream() ← Subscribe to stream ← Broadcast channel
                                                         ↓
Cache updates local DB ← Process changes ← Receive batches
                                                         ↓
UI reads from Cache → Fast local reads → No network calls
```

**Next Steps** (Future Enhancements):
- [ ] `#[operations_trait]` macro implementation (for automatic OperationDescriptor generation)
- [ ] `OperationRegistry` trait for operation metadata aggregation
- [ ] Fractional indexing integration for `indent_block`/`move_block` operations
- [ ] Lagged stream handling (full resync trigger)
- [ ] Periodic sync worker for production use

**Key Benefits**:
- ✅ **Simple architecture**: No contract-specs complexity, hand-written fakes easy to understand
- ✅ **Type-safe**: DataSource traits enforce consistency between real and fake
- ✅ **Testable**: Fake emits same stream interface as production
- ✅ **Efficient**: Stateless datasources, cache-wrapped for performance
- ✅ **Fire-and-forget**: Operations return immediately, UI updates asynchronously
- ✅ **Optional validation**: Add anodized contracts incrementally as bugs are found

**Files**:
- `crates/holon/src/core/datasource.rs` (DataSource, CrudOperationProvider, Change, OperationDescriptor, BlockEntity, TaskEntity, MutableBlockDataSource, MutableTaskDataSource)
- `crates/holon/src/core/stream_cache.rs` (QueryableCache transparent proxy, exported as StreamCache)
- `crates/holon/src/core/mod.rs` (module exports)
- `crates/holon-todoist/src/stream_datasource.rs` (real HTTP implementation)
- `crates/holon-todoist/src/fake.rs` (fake implementation with broadcast channel)
- `crates/holon-todoist/src/stream_provider.rs` (sync coordinator with builder pattern)
- `crates/holon-todoist/src/models.rs` (BlockEntity and TaskEntity implementations for TodoistTask)
- `crates/holon-todoist/src/stream_integration_test.rs` (integration tests)
- `crates/holon-todoist/src/lib.rs` (module exports)

---

### Phase 4: Flutter Integration
**Goal**: Bridge Rust engine to Flutter UI via FFI

#### 4.1: FFI Bridge ✅ COMPLETE
**Status**: All FFI functions implemented and tested
**Completed**: 2025-01-05

- [x] Set up `flutter_rust_bridge` codegen
- [x] Expose Rust API to Flutter:
  - `init_render_engine(db_path: String) -> Result<Arc<RwLock<RenderEngine>>>`
  - `compile_query(engine, prql: String) -> Result<(String, RenderSpec)>`
  - `execute_query(engine, sql: String, params: HashMap<String, Value>) -> Result<Vec<Entity>>`
  - `watch_query(engine, sql: String, params: HashMap<String, Value>) -> Result<RowChangeStream>`
  - `execute_operation(engine, op_name: String, params: HashMap<String, Value>) -> Result<()>` (stub for Phase 3.1)
  - `set_ui_state(engine, ui_state: UiState) -> Result<()>`
  - `get_ui_state(engine) -> Result<UiState>`
- [x] Define FRB types: `RenderSpec`, `RowEvent`, `UiState`, `CursorPosition`, `RowChange`, `ChangeData`
- [x] Handle FFI errors gracefully (anyhow::Error propagation across FFI boundary)
- [x] CDC streaming: `RowChangeStream` (tokio ReceiverStream) exposed to Flutter
- [x] **SQL Execution with Parameter Binding**:
  - `TursoBackend::execute_sql()` - Raw SQL execution with named parameters
  - `bind_parameters()` - Safe parameter binding (`$param_name` → `?`)
  - SQL injection prevention via prepared statements
- [x] **CDC Connection Lifecycle Management**:
  - CDC connection stored in RenderEngine (`_cdc_conn` field)
  - Connection kept alive for streaming duration
  - watch_query() properly manages connection lifecycle
- [x] **Comprehensive test coverage**:
  - 5 render_engine tests (creation, compile, UI state, SQL execution, parameter binding)
  - 3 ffi_bridge tests (init/compile, UI state, operation stub)
  - All tests passing ✅

**Note**: `execute_operation` doesn't return new IDs. One-directional flow: Operation → DB → CDC → Query → Render → UI (new IDs appear in next render).

**Files**:
- `crates/holon/src/api/render_engine.rs` (RenderEngine with SQL execution)
- `crates/holon/src/api/ffi_bridge.rs` (FFI exports)
- `crates/holon/src/storage/turso.rs` (execute_sql, bind_parameters)
- `crates/holon/src/api/mod.rs` (re-exports)
- `flutter/lib/src/rust/` (FRB-generated Dart bindings)

#### 4.2: Flutter Widget Mappings
- [ ] Create widget mappers for each primitive:
  - `list` → `ListView.builder` with virtualization
  - `block` → `Padding(left: indent) + Column[bullet, content]`
  - `row` → `Row(children: [...])`
  - `editable_text` → `TextField` with FocusNode
  - `drop_zone` → `DragTarget<String>` with validation
  - `collapse_button` → `IconButton` (expand/collapse indicator)
- [ ] Implement drag-drop:
  - `LongPressDraggable` for draggable blocks
  - `DragTarget` for drop zones (3 per block: before, after, as_child)
  - Client-side validation (check `invalid_targets` via HashSet membership)
- [ ] Handle operation triggers (keyboard shortcuts, button clicks)

**Files**: `flutter/lib/src/widgets/`, `flutter/lib/src/primitives/`

#### 4.3: State Management
- [ ] Set up `StreamBuilder` to consume query results
- [ ] Maintain UI state (cursor position, focused block)
- [ ] Sync UI state to Rust on operation execution
- [ ] Handle loading states, errors

**Files**: `flutter/lib/src/state/`, `flutter/lib/src/screens/outliner_screen.dart`

---

### Phase 5: Advanced Features
**Goal**: Ephemeral state sync, extensions, performance optimizations

#### 5.1: Loro Integration (Ephemeral State)
- [ ] Set up Loro CRDT document for real-time cursor sync
- [ ] Merge Loro ephemeral cursors with Turso persistent cursors
- [ ] Broadcast cursor updates via WebSocket/CRDT sync
- [ ] Periodic save to Turso (every 5s + on blur)

**Files**: `crates/holon/src/integrations/loro.rs` (new), `crates/holon/src/integrations/cursor_sync.rs` (new)

#### 5.2: Extension System
- [ ] Implement `extension_area` primitive in RenderNode enum
- [ ] Create extension registry (keyed by item_type)
- [ ] Allow plugins to register custom widgets for areas ("metadata", "actions")
- [ ] Example: JIRA extension (story points badge, "View in JIRA" button)

**Files**: `crates/holon/src/extensions/mod.rs` (new), `crates/holon/src/extensions/registry.rs` (new)

#### 5.3: Unified Block View via Materialized Views
**Goal**: Create a single `blocks` materialized view that merges data from multiple sources (Loro, Todoist, etc.)

- [ ] **TODO**: Specify detailed design for multi-source block aggregation
  - Materialized view approach: `CREATE MATERIALIZED VIEW blocks AS (SELECT * FROM loro_blocks UNION ALL SELECT * FROM todoist_blocks ...)`
  - Each source table has its own schema but projects to unified block schema
  - Fractional indexing: Determine if sort_key generation happens in DB (triggers) or Rust (application layer)
  - Schema updates needed in `0001-reactive-prql-schema.sql`:
    - Remove `_dirty` column references (replaced by row change callbacks)
    - Update `_version` column documentation (tracks upstream versions, not auto-increment)
    - Add source-specific tables: `loro_blocks`, `todoist_blocks`, etc.
    - Add materialized view definition for `blocks`
- [ ] Implement source table schemas
- [ ] Create materialized view with UNION ALL of all sources
- [ ] Set up row change callbacks to track changes across all sources
- [ ] Handle fractional indexing for sort_key (DB triggers vs Rust application logic)
- [ ] Test multi-source updates propagate correctly to unified view

**Design Questions**:
- Should each source table have its own sort_key, or is there a global sort order?
- How do we handle conflicts when same entity exists in multiple sources?
- Does fractional indexing happen at insert time (Rust) or via triggers (DB)?

**Files**: `crates/holon/src/storage/schema.sql`, `codev/specs/0002-multi-source-blocks.md` (new spec needed)

#### 5.4: Performance Optimizations
- [ ] Profile recursive CTE performance at scale (1k, 10k blocks)
- [ ] Implement caching for `ancestor_path` if needed
- [ ] Consider materialized ancestor columns for very deep trees
- [ ] Measure and optimize CDC polling overhead

**Files**: Performance tests, benchmarks

---

### Phase 6: Testing & Documentation
**Goal**: Comprehensive test coverage, usage documentation

#### 6.1: Integration Tests
- [ ] End-to-end test: PRQL → SQL → CDC → Flutter update
- [ ] Test all operations (indent, outdent, split, delete, move, collapse)
- [ ] Test drag-drop with cycle prevention
- [ ] Test multi-user scenarios (Loro cursor sync)
- [ ] Performance benchmarks (1000+ blocks, rapid CDC polling)

**Files**: `tests/integration/`, `flutter/test/integration/`

#### 6.2: Documentation
- [ ] API documentation (Rust crates, Flutter packages)
- [ ] Usage guide: "Building your first PRQL outliner"
- [ ] Extension development guide
- [ ] Performance tuning guide

**Files**: `docs/`, `README.md`

---

## Success Criteria

Each phase is "done" when:

1. ✅ All checkboxes completed
2. ✅ Unit tests pass
3. ✅ Integration tests pass (where applicable)
4. ✅ Code reviewed (self or peer)
5. ✅ Committed with descriptive message

**No time estimates** - focus on done/not done.

---

## Risk Mitigation

### High Priority (from consensus review)
<!-- Please make sure all of these are integrated/addressed in the implementation plan above -->

1. **Runtime Type Safety** ⚠️
   - **Risk**: HashMap lookups can fail at runtime
   - **Mitigation**: Implement `RowView<T>` typed accessors (Phase 3.1)
   - **Mitigation**: Add compile-time validation for operation params (future)

2. **Server-Side Validation** 🔴
   - **Risk**: Client could bypass cycle prevention
   - **Mitigation**: Always revalidate drag-drop in `move_block` operation (Phase 3.2)

3. **Performance at Scale** ℹ️
   - **Risk**: Recursive CTEs expensive at 10k+ blocks
   - **Mitigation**: Profile and add caching if needed (Phase 5.4)
   - **Defer**: Materialized ancestor columns (only if benchmarks show issues)

4. **Row-Level CDC Streaming** 🔄
   - **Risk**: Full result sets on every change → excessive rerendering
   - **Mitigation**: Implement row-level diffing (Phase 1.3)
   - **Optimization**: Stream `RowEvent` (Added/Updated/Removed) instead of full data

### Medium Priority

5. **Fractional Index Key Growth**
   - **Risk**: Keys grow with repeated insertions
   - **Mitigation**: Implement rebalancing (Phase 1.4)

6. **CDC Polling Overhead**
   - **Risk**: Polling turso_cdc table may have latency
   - **Mitigation**: Use change_id cursor to only fetch new changes since last poll
   - **Mitigation**: Measure overhead, tune polling interval (Phase 5.4)
   - **Note**: Built-in CDC confirmed working in turso crate v0.3

7. **Cache Consistency** ℹ️
   - **Note**: Turso acts as queryable cache for Loro/external systems
   - **No transactions needed**: Cache updates are eventually consistent
   - **Conflict resolution**: Loro CRDT handles conflicts, Turso reflects final state

---

## Dependencies

- **Rust Core**: prql-compiler, `turso` (Limbo SQLite), serde, tokio (async), anyhow
- **Contracts**: `contract-specs` v0.1 (declarative contract specification and validation)
- **Testing**: proptest (property-based testing), `contract-specs` with `laws` feature
- **Fractional Indexing**: `loro_fractional_index` v1.0
- **Flutter**: flutter_rust_bridge, provider (state management)
- **CRDT**: loro-rs (ephemeral state sync)
- **Database**: `turso` crate v0.3+ (Limbo - Rust SQLite with built-in CDC and experimental materialized views)
- **HTTP**: reqwest (for external API clients)

**Important Notes**:
- Use `turso` crate, NOT `libsql`! Only `turso` (Limbo) supports CDC features needed for reactive rendering.
- `contract-specs` is a local crate in `crates/contract-specs/` - provides generative contracts for simulation and validation.

---

## Error Handling Strategy

Errors propagate through the system with proper context at each layer:

```
┌──────────────────────────────────────────────────────────┐
│  PRQL Source (user-written query)                       │
└────────────────┬─────────────────────────────────────────┘
                 ↓ parse error
┌──────────────────────────────────────────────────────────┐
│  Parser (crates/query-render/parser.rs)                 │
│  → ParseError { line, column, expected, found }         │
└────────────────┬─────────────────────────────────────────┘
                 ↓ compile error
┌──────────────────────────────────────────────────────────┐
│  Compiler (prqlc → SQL, render → UISpec)                │
│  → CompileError { source, sql_error?, render_error? }   │
└────────────────┬─────────────────────────────────────────┘
                 ↓ runtime error
┌──────────────────────────────────────────────────────────┐
│  Database (query execution, CDC)                        │
│  → DatabaseError { query, sql_error, backtrace }       │
└────────────────┬─────────────────────────────────────────┘
                 ↓ operation error
┌──────────────────────────────────────────────────────────┐
│  Operations (execute_operation)                         │
│  → OperationError { op_name, validation_error }        │
└────────────────┬─────────────────────────────────────────┘
                 ↓ FFI boundary (Result → Dart exception)
┌──────────────────────────────────────────────────────────┐
│  Flutter UI (error display, user feedback)              │
│  → Show SnackBar, error dialog, or inline validation    │
└──────────────────────────────────────────────────────────┘
```

**Key Principles**:
1. **No silent failures**: All errors surface to UI with context
2. **User-friendly messages**: Parse/compile errors show line numbers, helpful suggestions
3. **Graceful degradation**: UI shows stale data + error banner if CDC stream fails
4. **FFI error mapping**: Rust `Result<T, E>` → Dart exceptions with proper stack traces
5. **Validation before execution**: Operations validate parameters before DB mutations

**Implementation** (Phase 4.1):
- Define `RenderError` enum covering all error types
- Implement `From<ParseError>`, `From<SqlError>`, etc. for `RenderError`
- FRB codegen handles `Result<T, RenderError>` → Dart exceptions
- Flutter catches exceptions and shows appropriate UI feedback

---

## Open Questions
<!-- Please ensure these are addressed in the implementation plan above -->

1. **PRQL Compiler Integration**: ✅ **RESOLVED**
   - **Decision**: Use hybrid approach (already implemented in `crates/query-render/`)
   - **Implementation**:
     - Official `prqlc` for query portion (via `prql_to_pl`, `pl_to_rq`, `rq_to_sql`)
     - Custom parser for `render()` section → generic AST (UI-agnostic)
   - **Status**: Parser exists and works, needs refactoring to generic AST

2. **FFI Type System**: ✅ **RESOLVED**
   - **Decision**: Use flutter_rust_bridge for generic AST types (not UI-specific)
   - **Rationale**: Compile-time safety, lower overhead than JSON/MessagePack
   - **FRB Types**: `RenderExpr`, `RowEvent`, `UiState`, `CommandRequest`

3. **Loro Integration**: ✅ **RESOLVED**
   - **Decision**: Full Loro integration for content (CRDT auto-merge) + cursor sync (Phase 5.1)
   - **Rationale**: Purpose-built for this use case, battle-tested CRDT

4. **Partial Batch Failure Handling**: ⏳ **OPEN**
   - **Scenario**: Syncing 10 commands for entity, command #5 fails - what happens to commands 6-10?
   - **Options**:
     - **Option A: Stop on First Failure** (recommended, implemented in Phase 3.4)
       - Pros: Simple, prevents cascading failures, entity in known state
       - Cons: Remaining commands stay pending until next sync
       - Implementation: `break` on first error, re-fetch entity, notify user
     - **Option B: Continue All, Mark Failures**
       - Pros: Maximizes successful commands
       - Cons: Commands 6-10 might depend on 5, complex failure state
       - Use case: Commands are known to be independent (rare)
     - **Option C: Abort, Refetch, Retry Remaining**
       - Pros: Ensures remaining commands apply to correct state
       - Cons: Complex, expensive, adjusted commands might still fail
       - Use case: Commands have complex dependencies
     - **Option D: Server-Side Transactional Batch**
       - Pros: All-or-nothing semantics (cleanest)
       - Cons: Requires server transaction support, one failure aborts all
       - Use case: Server supports transactions, batch is tightly coupled
   - **Decision needed**: Implement Option A initially. Add hooks for plugins to customize per command type.
   - **Testing**: Property-based tests with failure injection at random positions

---

## Notes

- This plan assumes single-platform (Flutter) initially. Web/desktop can be added later using the same Rust core.
- The render engine is UI-framework agnostic - Flutter is just the first client.
- Extension system enables future integrations (Todoist, JIRA, Google Calendar, etc.).

---

## Next Steps

1. User approval of this plan
2. Consultation with other AI agents (optional, already done in spec phase)
3. Begin Phase 1.1: PRQL Parser & AST

**Ready to implement?** ✅

---

## Plan Updates (Incorporating User Comments)

✅ **Phase 1.1**: Noted existing `crates/query-render/` parser, focused on testing PRQL functions in render()
✅ **Phase 1.2**: Removed redundant SQL compilation (prqlc handles it), focused on parameter binding
✅ **Phase 1.3**: Referenced existing `crates/holon/src/storage/turso.rs`, flagged CDC-for-views investigation
✅ **Phase 1.4**: Updated fractional indexing library links (loro_fractional_index, fractional_index)
✅ **Phase 2.1-2.2**: Clarified FRB **translatable** types (not opaque/JSON), auto-generated Dart classes
✅ **Phase 3.1**: Removed Action return type from Operation trait (one-directional flow: Op → DB → CDC → UI)
✅ **Phase 3.3**: Clarified CDC dirty marking depends on Turso IVM investigation (Phase 1.3)
✅ **All Phases**: Updated file paths to use existing structure:
  - `crates/query-render/src/` for PRQL parsing/compilation
  - `crates/holon/src/storage/` for DB layer
  - `crates/holon/src/operations/` for operations
  - `crates/holon/src/api/` for FFI exports
  - `crates/holon/src/integrations/` for Loro
  - `crates/holon/src/extensions/` for extension system
✅ **Risk Mitigations**: All addressed in phase checkboxes (type safety via RowView, server-side validation, CDC streaming, etc.)
✅ **Phase 2.4**: Added automatic operation inference from PRQL lineage (2025-01-06)
  - Validated feasibility via POC in `examples/test-lineage/`
  - Uses `pl_to_lineage` + AST parsing to auto-wire widget operations
  - Eliminates 80% of manual operation declarations
  - Maintains type safety (only updatable columns, primary keys required)

---

## Architectural Refinements (2025-01-04)

Based on deep architectural discussion with user + Gemini-2.5-Pro consultation:

### 1. UI-Agnostic Backend ✅
**Problem**: Original plan had Rust parsing `render()` into UI-specific `RenderNode` enum (Flutter widgets).
**Solution**: Rust parses into generic AST (`RenderExpr`):
- `FunctionCall { name, args }` - Generic function calls
- `ColumnRef`, `Literal`, `BinaryOp`, `Array`, `Object`
- NO UI semantics (no "ListView", "TextField")
- Each UI interprets: Flutter → widgets, TUI → Ratatui, Web → HTML

**Benefits**: Single backend supports multiple UIs, each with their own rendering capabilities.

### 2. CDC Event Coalescing ✅
**Problem**: Materialized view updates emit DELETE + INSERT, causing widget flicker.
**Solution**: Batch process CDC events, match DELETE + INSERT by row_id → emit UPDATE.
- Implementation: `CdcProcessor::process_batch()` in Phase 1.3
- Flutter never sees intermediate delete

**Benefits**: Smooth UI updates, no jank from view refreshes.

### 3. Nested Reactive Queries ✅
**Problem**: How to handle blocks containing live tables with their own queries?
**Solution**: Option A (Nested UISpec with Sub-Queries):
- Each `RenderExpr` can have optional `query` field
- UI creates nested `StreamBuilder` / `ReactiveTableWidget`
- Lazy loading: Query only initiated when scrolled into view
- Auto-disposed when widget removed from tree

**Benefits**: Efficient, aligns with Flutter lifecycle, no stream multiplexing needed.

### 4. Offline-First Command Sourcing ✅
**Problem**: Original design assumed online, immediate external sync with rollback.
**Solution**: Event Sourcing / CQRS pattern (Phase 3.4):
- Commands append to durable log (with client-generated UUIDs)
- Apply optimistically to Turso → CDC → UI updates immediately
- Background sync worker replays commands to external systems
- Idempotency via `Idempotency-Key` header
- On rejection: Re-fetch canonical state, notify user
- Command compaction for long offline periods

**Benefits**: Full offline support, optimistic UI, eventual consistency with external systems.

### 5. Partial Batch Failure Strategy ⏳
**Problem**: When syncing 10 commands, if #5 fails, what happens to 6-10?
**Solution**: Open question with 4 options documented (see Open Questions #4).
**Recommended**: Stop on first failure, re-fetch entity, leave remaining commands pending.

**Implementation**: Phase 3.4 implements Option A (stop on failure).

### 6. Automatic Operation Inference via Lineage ✅ (2025-01-06)
**Problem**: Manual operation wiring is verbose and error-prone:
```prql
content: (editable_text
  content: content,
  on_edit: (update id: id, fields: {content: $new_value})
)
```

**Solution**: Use PRQL's `pl_to_lineage` + AST parsing to automatically infer operations (Phase 2.4):
- **Pass 1**: Parse AST for widget calls (`ui_checkbox checked:this.completed`)
- **Pass 2**: Trace `this.completed` through lineage → `blocks.completed` (updatable)
- **Pass 3**: Validate primary key present (`blocks.id`)
- **Pass 4**: Auto-wire `UpdateField(table: "blocks", id: row["id"], field: "completed")`

**Result**:
```prql
# Widget automatically gets update handler!
content: (editable_text content: content)
```

**POC Validation** (`examples/test-lineage/`):
- ✅ `pl_to_lineage` distinguishes direct vs computed columns
- ✅ AST contains widget function calls with parameter mappings
- ✅ Can trace columns through joins, derives, transformations
- ✅ Primary key detection works
- ✅ 26/33 columns updatable, 7 computed (correctly marked read-only)

**Benefits**: 80% less boilerplate, type-safe (operations only enabled if data present), self-documenting (lineage shows data flow).

---

## Summary of Key Decisions

| Area | Decision | Impact |
|------|----------|--------|
| Backend AST | Generic `RenderExpr`, no UI semantics | Supports Flutter, TUI, Web from same backend |
| CDC Flicker | Coalesce DELETE + INSERT → UPDATE | Smooth UI updates for materialized views |
| Nested Queries | Each widget has optional query, own stream | Lazy loading, auto-disposal, efficient |
| Offline-First | Command sourcing with event log | Full offline support, optimistic UI |
| Batch Failure | Stop on first failure (Option A) | Simple, predictable, prevents cascading failures |
| Auto-Wiring | Lineage-based operation inference | 80% less boilerplate, type-safe, self-documenting |

**Status**: Architecture validated by Gemini-2.5-Pro (Thinkdeep analysis), ready for implementation.

### 7. Stream-Based External System Integration ✅ (2025-01-09)
**Problem**: Current `IncrementalSync<T>` trait causes redundant syncs for unified APIs like Todoist.
- Todoist returns tasks + projects + labels in **one API call**
- Calling `sync_incremental()` on `TodoistTaskDataSource` syncs everything
- Calling `sync_incremental()` on `TodoistProjectDataSource` **also** syncs everything
- Two redundant API calls waste bandwidth and complexity

**Solution**: Provider-centric stream architecture (Phase 3.4):
```rust
// Provider emits typed streams
impl TodoistProvider {
    pub fn subscribe_tasks(&self) -> broadcast::Receiver<Vec<Change<TodoistTask>>> { ... }
    pub fn subscribe_projects(&self) -> broadcast::Receiver<Vec<Change<TodoistProject>>> { ... }

    // ONE sync call, multiple stream emissions
    pub async fn sync(&mut self) -> Result<()> {
        let response = self.client.sync_items(token).await?; // One API call
        let _ = self.task_tx.send(compute_task_changes(&response));
        let _ = self.project_tx.send(compute_project_changes(&response));
        Ok(())
    }
}

// Caches ingest streams asynchronously
impl<T> QueryableCache<T> {
    async fn ingest_stream<S: Stream<Item = Vec<Change<T>>>>(&self, stream: S) {
        while let Some(changes) = stream.next().await {
            for change in changes {
                match change {
                    Change::Upsert(item) => self.upsert(item).await?,
                    Change::Delete(id) => self.delete(&id).await?,
                }
            }
            self.notify_ui_changed().await; // Trigger UI update via existing CDC
        }
    }
}
```

**Architecture**:
```
External API → Provider.sync() → Broadcast Streams → QueryableCache → UI
                 (one call)          (per-type)         (ingests)       ↓
                                                                   DataSource queries
```

**Key Benefits**:
- ✅ **No redundant syncs**: One `sync()` call updates all types
- ✅ **Real-time updates**: Changes flow automatically to UI
- ✅ **Type-safe streams**: Per-type broadcast channels prevent routing errors
- ✅ **Clean separation**: Provider (sync) → Streams (propagate) → Cache (store) → DataSource (query)
- ✅ **Works with DataSource**: QueryableCache implements both `DataSource<T>` (pull) and stream ingestion (push)

**Design Decisions**:

1. **Per-Type Streams** (not unified enum):
   ```rust
   // ✅ Chosen: Type-safe, can't route tasks to project cache
   provider.subscribe_tasks() -> Stream<Vec<Change<TodoistTask>>>
   provider.subscribe_projects() -> Stream<Vec<Change<TodoistProject>>>

   // ❌ Rejected: Type-unsafe, requires runtime matching
   enum TodoistChange { Task(Change<TodoistTask>), Project(...) }
   provider.subscribe() -> Stream<Vec<TodoistChange>>
   ```

2. **Broadcast Channels** (not mpsc):
   - Multiple consumers can subscribe to same stream
   - Acceptable to drop messages for slow consumers (UI wants latest state, not all intermediates)
   - Lagged consumers trigger full resync

3. **Centralized SyncManager**:
   - Manages provider lifecycle, periodic sync, graceful shutdown
   - Coordinates multiple providers (Todoist, GitHub, etc.)

4. **Two-Way Flow**:
   - **External → DB**: Streams update from sync
   - **DB → External**: DataSource writes go to external API
   - Next sync() reconciles any conflicts

**Challenges & Mitigations**:

| Challenge | Mitigation |
|-----------|------------|
| **Initial Data Load** | Load from Turso first (persisted), then trigger immediate sync, then periodic |
| **Backpressure** | Broadcast channel drops old messages (acceptable), lagged consumers trigger resync |
| **Error Handling** | Retry with exponential backoff, expose `health()` status API for UI monitoring |
| **Testing** | Mock providers with `tokio::time::pause()` for deterministic tests |
| **Debugging** | Structured logging (`tracing`), change history in DB, health checks |
| **State Consistency** | Version numbers, snapshot isolation, eventual consistency acceptable |

**Migration Path**:
1. **Phase 1**: Keep existing `IncrementalSync` working, add stream-based provider in parallel
2. **Phase 2**: Migrate consumers one by one (old DataSource → new stream ingestion)
3. **Phase 3**: Deprecate `IncrementalSync` trait
4. **Phase 4**: Remove old code

**Relationship with Contract-Based Simulation**:

The stream-based architecture handles **sync from external systems**, while contract-based simulation handles **writes to external systems**. They work together:

```rust
// Reading: Stream-based (Phase 3.4, Section 7)
External API → Provider.sync() → Streams → QueryableCache → UI

// Writing: Contract-based simulation (Phase 3.4, original section)
UI → Command → ContractFake (optimistic) → Turso → CDC → UI
                    ↓
              SyncWorker → Real API (background, uses same Provider)
```

Both use the **same Provider** but for different purposes:
- **Provider.sync()**: Pull changes from external API → emit on streams
- **Provider.apply_command()**: Push commands to external API (via ExternalSystem trait)

**Integration**:
1. **User creates task** → Contract fake generates optimistic response → Turso updated → CDC → UI shows new task
2. **SyncWorker** → Sends command to real API → Validates response against contract → Updates ID mappings
3. **Provider.sync()** → Pulls latest state from API → Emits on task stream → Cache ingests → Reconciles any differences
4. **External user modifies task** → Provider.sync() detects change → Stream emits update → Cache updates Turso → CDC → UI updates

**Implementation** (Phase 3.4):

**Stream-Based Sync** (new):
- Remove `IncrementalSync<T>` trait from DataSource
- Add `SyncableProvider` trait (type-independent `sync()`)
- Implement `TodoistProvider` with broadcast channels
- Add `QueryableCache::ingest_stream()` method
- Create `SyncManager` for lifecycle control
- Integration tests: sync → stream → cache → UI update

**Contract-Based Commands** (existing plan, still relevant):
- Keep all contract specifications, fake implementations, real clients
- ExternalSystem trait for commands (create, update, delete)
- CommandExecutor for optimistic updates
- SyncWorker for background sync to real API
- Contract validation on responses

**Files**:
- `crates/holon-todoist/src/provider.rs` (stream-based sync + command application)
- `crates/holon/src/core/queryable_cache.rs` (stream ingestion)
- `crates/holon/src/sync/manager.rs` (new - lifecycle management)
- `crates/holon/src/core/traits.rs` (`Change<T>` enum, `SyncableProvider` trait)
- `crates/holon/src/contracts/` (contract specs - unchanged)
- `crates/holon/src/integrations/todoist_fake.rs` (fake implementation - unchanged)
- `crates/holon/src/integrations/todoist_client.rs` (real HTTP client - unchanged)
- `crates/holon/src/commands/executor.rs` (command execution - unchanged)
- `crates/holon/src/commands/sync_worker.rs` (background sync - uses Provider.sync())
