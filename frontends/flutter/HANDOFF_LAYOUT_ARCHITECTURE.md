# Handoff: Document-Driven Layout via PRQL+Render

## Context

We're implementing a document-driven home screen layout for the Holon Flutter app. The layout should be defined by blocks with properties like `REGION: left_sidebar`, `REGION: main`, `REGION: right_sidebar`.

## Current State

### What exists:
1. **PRQL+render system**: Queries data and renders via `RenderInterpreter`
   - Supports: `list`, `tree`, `table`, `row`, `column`, etc.
   - `ReactiveQueryWidget` executes PRQL and renders results with CDC streaming
   - See: `lib/render/render_interpreter.dart`, `lib/render/reactive_query_widget.dart`

2. **Blocks table**: Contains blocks with properties
   ```sql
   SELECT id, properties FROM blocks WHERE properties LIKE '%REGION%'
   -- Returns blocks like:
   -- { id: "...", properties: { "REGION": "left_sidebar" } }
   -- { id: "...", properties: { "REGION": "main" } }
   ```

3. **Partially implemented IndexLayout system** (in progress, may be wrong approach):
   - `lib/providers/index_layout_provider.dart` - queries blocks, parses into IndexBlock
   - `lib/models/layout_region.dart` - data models
   - `lib/ui/widgets/sidebar_widgets.dart` - custom widgets
   - This feels like reinventing PRQL+render

### The Problem

We're building a parallel UI system when we already have PRQL+render. The user suggests expressing the layout entirely through PRQL:

```prql
from blocks
filter s"json_extract(properties, '$.REGION')" == "left_sidebar"
derive { ui = (render (column width:0.25 (list ???))) }
append (
  from blocks
  filter s"json_extract(properties, '$.REGION')" == "main"
  derive { ui = (render (column width:0.5 (list ???))) }
)
append (
  from blocks
  filter s"json_extract(properties, '$.REGION')" == "right_sidebar"
  derive { ui = (render (column width:0.25 (list ???))) }
)
render (row ???)
```

## Key Questions to Explore

1. **Can PRQL express layout composition?**
   - Using `append` to combine different region queries
   - Using `derive { ui = ... }` to assign render specs per region
   - Final `render (row ...)` to compose horizontally

2. **How should nested queries work?**
   - Some blocks have `VIEW: query` with a child PRQL source block
   - The render should execute that nested query and display results
   - Is this `render (query source:this.source_code)`?

3. **What render primitives are needed?**
   - `column width:0.25` - fixed-width column?
   - `row` - horizontal composition?
   - `sidebar` - semantic layout region?

4. **How does the RenderInterpreter need to evolve?**
   - Currently handles: list, tree, table, text, row, column
   - May need: layout regions, nested query execution

## Files to Review

- `lib/render/render_interpreter.dart` - current render spec handling
- `lib/render/reactive_query_widget.dart` - PRQL execution + CDC
- `crates/holon/src/prql/` - PRQL compiler/execution (Rust side)
- `lib/main.dart` - current home screen structure (search for `_buildBodyContent`)

## Desired Outcome

A single PRQL query that:
1. Defines the three-region layout (left sidebar, main, right sidebar)
2. Each region contains blocks filtered by REGION property
3. Blocks with VIEW=query execute their embedded PRQL
4. The entire home screen is data-driven via this query

## Constraints

- Don't hard-code org-file specific logic
- Blocks and Loro are the primary abstraction
- Use existing PRQL+render where possible
- Fail hard on errors (no defensive programming)
