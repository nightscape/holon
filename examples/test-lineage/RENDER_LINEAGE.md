# Lineage with Render Functions

This document explains how the test script handles the custom `render` function and UI elements with lineage tracking.

## Challenge

The original query uses a custom `render` function with UI-specific operations that aren't part of standard PRQL:

```prql
render (list hierarchical_sort:[parent_id, sort_key] item_template:(row (checkbox checked:completed) ...))
```

These functions (`render`, `list`, `row`, `checkbox`, `text`, `badge`) are UI-level operations, not SQL transformations.

## Solution: Function Stubs

To make lineage tracking work, we define stub functions that act as identity functions (return their input):

```prql
# Prefix with ui_ to avoid conflicts with PRQL standard library
let ui_checkbox = checked:null -> checked
let ui_text = text_content:null -> text_content
let ui_badge = badge_content:null color:null -> badge_content
let ui_row = items:null -> items
let ui_list = rel:null hierarchical_sort:null item_template:null -> rel
let ui_render = rel:null -> rel
```

### Key Design Decisions

1. **Prefixed names** (`ui_*`): Avoids conflicts with PRQL's standard library
2. **Renamed parameters**: Parameters like `content` → `text_content` and `relation` → `rel` to avoid ambiguity with column names
3. **Named parameters** (`param:null`): Allows using named arguments in function calls
4. **Identity functions**: Simply return their input to allow lineage tracking

## Query Structure

The query is broken into derive stages:

```prql
# Stage 1: Create UI elements from columns
derive {
    checkbox_el = (ui_checkbox checked:this.completed),
    id_el = (ui_text text_content:this.id),
    space_el = (ui_text text_content:" "),
    content_el = (ui_text text_content:this.content),
    parent_label_el = (ui_text text_content:" parent: "),
    parent_el = (ui_text text_content:this.parent_id),
    badge_el = (ui_badge badge_content:this.block_type color:"cyan")
}

# Stage 2: Compose UI elements into render output
derive {
    row_el = (ui_row items:[...]),
    list_output = (ui_list rel:this hierarchical_sort:[parent_id, sort_key] ...),
    render_output = (ui_render rel:list_output)
}

# Stage 3: Select final output
select render_output
```

## What Lineage Captures

With the render functions, lineage now tracks:

### 1. UI Element Creation
Each UI element (`checkbox_el`, `id_el`, etc.) shows its dependency on source columns:
- `checkbox_el` depends on `blocks.completed`
- `id_el` depends on `blocks.id`
- `content_el` depends on `blocks.content`
- etc.

### 2. Composition Chain
The lineage graph shows how UI elements compose:
```
blocks.completed → checkbox_el → row_el → list_output → render_output
blocks.id → id_el → row_el → list_output → render_output
...
```

### 3. Sort Dependencies
The `list_output` function call includes `hierarchical_sort:[parent_id, sort_key]`, showing that:
- The sorting operation depends on `blocks.parent_id` and `blocks.sort_key`
- These columns flow through to the final output

### 4. Complete Data Flow
From source table to rendered UI:
```
blocks table
  → select (id, parent_id, depth, sort_key, content, completed, block_type, collapsed)
  → derive UI elements (7 elements created)
  → derive composition (row_el, list_output, render_output)
  → select render_output
```

## Lineage Output Size

The full lineage JSON is **2027 lines** because it captures:
- All source columns (8)
- All derived UI elements (7)
- All composition stages (row_el, list_output, render_output)
- Complete AST of the query
- All nodes in the dependency graph
- All transformation frames

## Benefits

### 1. **UI Data Dependencies**
Track which database columns feed into which UI components.

### 2. **Impact Analysis**
Understand what UI elements are affected when a database column changes.

### 3. **Documentation**
Auto-generate documentation showing data flow from DB to UI.

### 4. **Debugging**
Trace issues from UI back to source data:
```
UI bug in badge → badge_el → blocks.block_type → source table
```

### 5. **Testing**
Identify which UI components need testing when source data changes.

## Example Analysis

For the query, lineage reveals:

**Direct dependencies**:
- `checkbox_el` ← `completed`
- `badge_el` ← `block_type`
- `content_el` ← `content`

**Sort dependencies**:
- `list_output` requires `parent_id` and `sort_key` for hierarchical sorting
- These columns must be present for the render to work correctly

**Full chain example**:
```
blocks.completed (source)
  → checkbox_el (UI element via ui_checkbox)
  → row_el (composition via ui_row)
  → list_output (list with sorting via ui_list)
  → render_output (final output via ui_render)
```

## Practical Use Cases

1. **Schema Changes**: Know which UI components break if `completed` column is removed
2. **Feature Development**: See all data dependencies for a new UI feature
3. **Code Reviews**: Verify all necessary columns are selected
4. **Performance**: Identify unnecessary column selections that aren't used in UI
5. **Refactoring**: Safely rename columns by understanding their UI impact

## Limitations

- Stubs are identity functions - they don't capture actual UI transformation logic
- Only tracks data flow, not UI behavior
- Sorting parameters are tracked but not validated
- Custom styling (like `color:"cyan"`) is in the AST but not the lineage graph

## Running

```bash
cd /Users/martin/Workspaces/pkm/holon/examples/test-lineage
cargo run > output.txt
```

The output includes complete lineage in JSON format showing all dependencies from database columns through UI rendering.
