# Per-Row UI Templates Design

## Problem Statement

Currently, operations are not properly wired for UNION queries because:
1. `BackendEngine::extract_table_name_from_prql()` only extracts the first table (`crates/holon/src/api/backend_engine.rs:91-102`)
2. `enhance_operations_with_dispatcher()` filters operations by single `entity_name` (`crates/holon/src/api/backend_engine.rs:112-232`)
3. Operations for other tables in the UNION are not found

More fundamentally, the current single `render` clause at query end doesn't support:
- Type-specific rendering per entity
- Type-specific operations per entity
- Extensible "just union and it works" data source integration

## Proposed Solution

Per-row UI templates via `derive { ui = (render ...) }`:

```prql
from todoist_tasks
derive { ui = (render (row (bullet) (checkbox checked:this.completed) (text this.content))) }
append (
  from todoist_projects
  derive { ui = (render (row (bullet) (folder_icon) (text this.content)))) }
)
render (tree parent_id:parent_id sortkey:sort_key item_template:this.ui)
```

### Key Insight

When the compiler sees `derive { ui = (render ...) }` after `from todoist_tasks`, it **knows the source table**. This enables:
1. Extracting the render expression
2. Wiring operations for that specific entity
3. Storing in an indexed registry
4. Replacing column value with index

### SQL Output

```sql
SELECT id, content, ..., 0 as ui FROM todoist_tasks
UNION ALL
SELECT id, content, ..., 1 as ui FROM todoist_projects
```

The `ui` column is an integer index into the template registry.

### RenderSpec Changes

```rust
// crates/holon-api/src/render_types.rs

struct RenderSpec {
    pub root: RenderExpr,
    pub row_templates: Vec<RowTemplate>,  // NEW: indexed per-row templates
}

struct RowTemplate {
    pub index: usize,
    pub entity_name: String,
    pub expr: RenderExpr,  // Operations wired in FunctionCall.operations
}
```

### Render-Time Behavior

1. Tree/list widget iterates rows
2. For each row, reads `row['ui']` as integer index
3. Looks up `renderSpec.rowTemplates[index]`
4. Renders that template with row data
5. Operations are already wired in the template

## Data Flow

```
PRQL Source
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ Parser                                                   │
│ - Recognizes derive { ui = (render ...) } after FROM    │
│ - Extracts render expression                            │
│ - Notes source table for this derive                    │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ Compiler                                                 │
│ - Assigns index to each extracted template              │
│ - Wires operations based on source table                │
│ - Replaces (render ...) with integer index in AST       │
│ - Generates SQL with index column                       │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ RenderSpec                                               │
│ - root: collection-level render (tree/list)             │
│ - row_templates: Vec<RowTemplate> with wired operations │
└─────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ Flutter Render                                           │
│ - Tree iterates rows                                    │
│ - Reads row['ui'] index                                 │
│ - Looks up template, renders with row data              │
│ - Operations available from template wirings            │
└─────────────────────────────────────────────────────────┘
```

## File References

### Rust - Parser
- `crates/query-render/src/parser.rs` - PRQL parsing, split at render
- `crates/query-render/src/lib.rs` - `parse_query_render_to_rq()` entry point

### Rust - Compiler
- `crates/query-render/src/compiler.rs` - `compile_render_spec()`
- `crates/query-render/src/lib.rs:180-259` - `annotate_tree_with_operations()` (legacy, to be replaced)

### Rust - Backend Engine
- `crates/holon/src/api/backend_engine.rs:63-89` - `compile_query()`
- `crates/holon/src/api/backend_engine.rs:112-232` - `enhance_operations_with_dispatcher()`

### Rust - Types
- `crates/holon-api/src/render_types.rs` - `RenderSpec`, `RenderExpr`, `OperationWiring`

### Flutter - Rendering
- `frontends/flutter/lib/render/render_interpreter.dart` - Widget building from RenderExpr
- `frontends/flutter/lib/render/tree_view_widget.dart` - Tree rendering (needs template lookup)
- `frontends/flutter/lib/render/render_context.dart` - `RenderContext` with `availableOperations`

### Flutter - Generated Types
- `frontends/flutter/lib/src/rust/third_party/holon_api/render_types.dart` - Dart types (auto-generated)

## Future Enhancements

### PRQL Functions for Reusability

```prql
let task_row = (row (bullet) (checkbox checked:this.completed) (text this.content))
let project_row = (row (bullet) (folder_icon) (text this.content))

from todoist_tasks
derive { ui = (render task_row) }
append (from todoist_projects derive { ui = (render project_row) })
render (tree parent_id:parent_id sortkey:sort_key item_template:this.ui)
```

### Entity Default Templates

Entities register default templates for zero-config integration:

```rust
impl EntityMetadata for TodoistTask {
    fn default_row_template() -> Option<&'static str> {
        Some("(row (bullet) (checkbox checked:this.completed) (text this.content))")
    }
}
```

Query becomes:
```prql
from todoist_tasks  -- implicitly gets ui = default template
append (from calendar_events)  -- implicitly gets ui = default template
render (tree parent_id:parent_id sortkey:sort_key)
```

Adding a new system:
1. Implement data source
2. Register default template
3. Users can union it - just works

### Tables

Tables remain homogeneous (single entity type). Use list/tree for heterogeneous data.

## Implementation TODOs

### Phase 1: Core Mechanism

- [ ] **Parser**: Recognize `derive { ui = (render ...) }` pattern
  - File: `crates/query-render/src/parser.rs`
  - Track source table context when parsing derives
  - Extract render expressions from derive clauses

- [ ] **Compiler**: Handle extracted templates
  - File: `crates/query-render/src/compiler.rs`
  - Assign indices to extracted templates
  - Replace `(render ...)` with integer literal in derive
  - Store templates in new `row_templates` field

- [ ] **Operation Wiring**: Wire operations per-template
  - File: `crates/holon/src/api/backend_engine.rs`
  - Modify `enhance_operations_with_dispatcher()` to handle `row_templates`
  - Wire operations based on each template's source entity

- [ ] **RenderSpec**: Add row_templates field
  - File: `crates/holon-api/src/render_types.rs`
  - Add `RowTemplate` struct
  - Add `row_templates: Vec<RowTemplate>` to `RenderSpec`
  - Run `flutter_rust_bridge_codegen generate` to update Dart types

- [ ] **Flutter**: Template lookup at render time
  - File: `frontends/flutter/lib/render/tree_view_widget.dart`
  - Read `row['ui']` as template index
  - Look up template from `renderSpec.rowTemplates`
  - Use template's operations for pie menu

### Phase 2: PRQL Reusability

- [ ] Support `let` bindings for render expressions
- [ ] Support imports across PRQL files

### Phase 3: Entity Defaults

- [ ] Define `EntityMetadata` trait with `default_row_template()`
- [ ] Compiler auto-inserts default template when no explicit `derive { ui = ... }`
- [ ] Registry for entity metadata lookup

## Testing Strategy

1. **Unit tests**: Parser correctly extracts templates with source table context
2. **Unit tests**: Compiler assigns indices and wires operations correctly
3. **Integration tests**: UNION query with two entity types renders correctly
4. **Integration tests**: Operations dispatch to correct entity
5. **Flutter tests**: Pie menu shows correct operations for each row type

## Migration

Existing queries with single `render` clause continue to work unchanged. The new `derive { ui = ... }` syntax is additive.

For UNION queries currently not working:
```prql
-- Before (operations broken):
from todoist_projects
append (from todoist_tasks)
render (tree ... item_template:(row ...))

-- After (operations work):
from todoist_projects
derive { ui = (render (row ...project template...)) }
append (
  from todoist_tasks
  derive { ui = (render (row ...task template...)) }
)
render (tree ... item_template:this.ui)
```
