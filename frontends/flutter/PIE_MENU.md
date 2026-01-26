# Pie Menu Integration Requirements

## Overview

The frontend uses a generic, data-driven approach where views (outliner, table, kanban, calendar, etc.) are defined by PRQL queries containing `render` blocks. These render expressions are interpreted by `RenderInterpreter` to build the visual hierarchy.

We want to integrate pie menus as a universal interaction mechanism across all view types, while maintaining the generic nature of the rendering system.

## Core Requirements

### 1. Universal Availability
- Pie menus should be available on items across **all view types** (outliner, table, kanban, calendar, etc.)
- The mechanism for triggering pie menus should be consistent but the available operations can vary by context

### 2. View-Specific Placement

#### Outliner View
- Primary pie menu location: **bullet point** (left of content)
- Should feel natural for hierarchical operations

#### Other Views (Table, Kanban, Calendar)
- Placement TBD, but should be discoverable and consistent within each view type
- Possibly: right-click/long-press on the entire item widget

### 3. Dynamic, Context-Aware Operations

Operations should be:
- **Dynamically generated** based on item metadata/state
- **Filtered by view type** to show only relevant operations
- **Filtered by widget fragment** when attached to specific parts

#### Example: View-Type Filtering
- **Outliner view**: Show `indent`, `outdent`, `toggle_collapse`
- **Table view**: Hide `indent`/`outdent` (doesn't make sense in flat table)
- **All views**: Show `select_for_multi_selection`, `delete`, `duplicate`

#### Example: Fragment-Specific Operations
Different parts of an item's widget could have different pie menus:

1. **Status icon fragment**:
   - Show available status transitions (e.g., "todo" → "in_progress", "done")
   - Visual: pie slices with status colors

2. **Text content fragment**:
   - Formatting: "Make Heading 1", "Make Heading 2", "Bold", "Italic"
   - Transform: "Convert to task", "Convert to note"

3. **Bullet point fragment** (outliner-specific):
   - Structural: `indent`, `outdent`, `move_up`, `move_down`
   - Visibility: `toggle_collapse`

4. **Date fragment** (if present):
   - "Set due date", "Clear date", "Postpone 1 day", "Postpone 1 week"

### 4. Multi-Selection Support
- One of the pie menu options should allow selecting items for subsequent bulk operations
- Selected items should have visual feedback (highlight/checkmark)

## Design Challenges

### Challenge 1: Maintaining Generic Architecture
The RenderInterpreter currently has no knowledge of "views" or "operations" - it just maps PRQL render expressions to widgets. How do we add pie menus without hard-coding view-specific logic?

**Question**: Should the PRQL `render` block be extended to declaratively specify pie menu configurations?
<!--
I would try not to do that.
We had something similar previously with explicit `onClick` options in the render call and it was super annoying.
-->

### Challenge 2: Operation Metadata Flow
The backend knows what operations are available for each item (based on data source capabilities). How does this metadata reach the RenderInterpreter?

**Current state**: `RenderContext` has an `onOperation` callback but no metadata about available operations.

**Question**: Should we extend `RenderContext` to include operation metadata? Or should operations be part of the row data?
<!--
This already exists. Please search for and read through the code related to OperationWiring
-->

### Challenge 3: Attaching Pie Menus to Widget Fragments
The RenderInterpreter builds a tree of widgets. How do we attach different pie menus to different parts of that tree?
<!--
We might attach pie menus to every widget that is able to mutate some underlying data.
-->

**Options**:
1. Wrap individual widgets in gesture detectors during render interpretation
2. Use a special `pie_menu()` render function
3. Use Flutter's `PieCanvas` to wrap entire sections
<!-- No idea -->

### Challenge 4: View Type Context
The RenderInterpreter doesn't currently know what "view type" it's rendering. Should it?
<!--
If possible I'd rather not have it know that.
-->

**Question**: Should view type be:
- Part of RenderContext?
- Inferred from the render expression structure?
- Explicitly passed from the query?
<!--
Each view type itself could/should know, where good places to put a pie menu are.
Outliners know it's on the bullet, tables maybe put an icon in front of every row,
for calendars it might be everywhere on the block except for the text.
-->

## Possible Implementation Approaches

### Approach 1: Declarative PRQL Extension

Extend the render DSL to include pie menu specifications:

```prql
render list(
  item: block(
    depth: depth,
    row(
      # Pie menu attached to bullet point
      pie_menu(
        trigger: bullet_point(),
        operations: [indent, outdent, toggle_collapse],
        filter_by_view: true
      ),
      pie_menu(
        trigger: status_icon(status),
        operations: status_transitions,
        filter_by_view: false
      ),
      editable_text(content: content)
    )
  )
)
```

**Pros**:
- Declarative and fits with existing PRQL approach
- Operations defined at query time (close to data)
- Easy to filter by view in the query itself

**Cons**:
- Complex PRQL syntax
- Tight coupling between data queries and UI interaction
- Hard to share common pie menu patterns across queries
<!-- I'll veto that one 😉 -->

### Approach 2: Wrapper Widget with Operation Metadata

Pass operation metadata through `RenderContext.rowData` and programmatically wrap widgets:

```dart
// In RenderContext.rowData:
{
  "block_id": "123",
  "content": "My task",
  "available_operations": [
    {"name": "indent", "view_filter": ["outliner"]},
    {"name": "outdent", "view_filter": ["outliner"]},
    {"name": "select", "view_filter": null}
  ],
  "fragment_operations": {
    "status": [
      {"name": "set_status_todo", "label": "Todo"},
      {"name": "set_status_done", "label": "Done"}
    ]
  }
}

// In RenderInterpreter:
Widget _wrapWithPieMenu(Widget child, String fragmentKey) {
  final ops = _getOperationsForFragment(fragmentKey, context);
  if (ops.isEmpty) return child;

  return PieCanvas(
    child: Builder(
      builder: (context) => PieMenu(
        actions: ops.map((op) => PieAction(...)).toList(),
        child: child,
      ),
    ),
  );
}
```

**Pros**:
- Keeps PRQL clean and focused on data/rendering
- Operation metadata comes from backend (single source of truth)
- Easy to filter operations programmatically in Dart

**Cons**:
- Less declarative about where pie menus appear
- Needs convention for fragment naming
- RenderInterpreter becomes more opinionated

### Approach 3: Hybrid - New `pie_menu()` Render Function

Add a `pie_menu()` function to the render DSL that wraps its child:

```prql
render list(
  item: block(
    row(
      pie_menu(
        bullet_point(),
        fragment: "bullet",
        view_filter: ["outliner"]
      ),
      pie_menu(
        status_icon(status),
        fragment: "status"
      ),
      editable_text(content)
    )
  )
)
```

RenderInterpreter maps this to:

```dart
Widget _buildPieMenu(
  Map<String, RenderExpr> namedArgs,
  List<RenderExpr> positionalArgs,
  RenderContext context
) {
  final child = build(positionalArgs[0], context);
  final fragment = namedArgs['fragment']?.toString() ?? 'default';
  final viewFilter = _extractViewFilter(namedArgs['view_filter']);

  final ops = context.getFragmentOperations(fragment)
    .where((op) => _matchesViewFilter(op, viewFilter))
    .toList();

  return PieCanvas(
    child: Builder(
      builder: (ctx) => PieMenu(
        actions: ops.map(_operationToPieAction).toList(),
        child: child,
      ),
    ),
  );
}
```

**Pros**:
- Balance between declarative and programmatic
- Clear where pie menus are attached
- Operation metadata still comes from backend
- PRQL stays relatively simple

**Cons**:
- Need to extend RenderContext with operation metadata
- Need to define fragment naming convention

<!-- I'll veto against that one, too -->

### Approach 4: Auto-Attachment by Widget Type

Automatically attach pie menus to certain widget types based on conventions:

```dart
Widget _buildBulletPoint(args, context) {
  final bullet = IconButton(...);

  // Auto-attach pie menu if operations available
  if (context.hasOperationsFor('bullet')) {
    return _wrapWithPieMenu(bullet, 'bullet', context);
  }
  return bullet;
}
```
<!-- Interesting, let's keep that one -->

**Pros**:
- No PRQL changes needed
- Convention over configuration
- Works across all queries

**Cons**:
- Less explicit/discoverable
- Hard to customize per-query
- Magic behavior might surprise users

## Recommended Approach

Based on feedback and code analysis, here's the field-based approach:

### Approach 5: Field-Based Auto-Attachment (No Abstract Domains Needed!)

**Core Principle**: Operations declare which fields they affect. Widgets declare which fields they care about. The intersection determines pie menu contents.

**Key Insights from Existing Code**:
1. `OperationDispatcher` already provides `available_operations(entity_name)` and operation metadata via `OperationDescriptor`
2. `RenderContext` already has `onOperation` callback for execution
3. Operation metadata already flows from backend through queries

**How It Works**:

1. **Operations declare affected fields** (backend/Rust):
   ```rust
   // In OperationDescriptor (holon-prql-render crate)
   pub struct OperationDescriptor {
       pub name: String,
       pub entity_name: String,
       pub required_params: Vec<OperationParam>,  // Already exists
       pub affected_fields: Vec<String>,          // NEW!
   }

   // Example: indent operation
   // Parameters: id, parent_id
   // Affects: parent_id, depth, sort_key

   // Example: toggle_collapse operation
   // Parameters: id
   // Affects: is_collapsed

   // Example: set_status_done operation
   // Parameters: id
   // Affects: status
   ```

2. **Operations annotated with affected fields**:
   ```rust
   #[operation(affects = ["is_collapsed"])]
   async fn toggle_collapse(&self, id: &str) -> Result<()> {
       self.set_field(id, "is_collapsed", ...).await
   }

   #[operation(affects = ["parent_id", "depth", "sort_key"])]
   async fn indent(&self, id: &str, parent_id: &str) -> Result<()> {
       self.set_field(id, "parent_id", ...).await?;
       self.set_field(id, "depth", ...).await?;
       self.set_field(id, "sort_key", ...).await?;
       Ok(())
   }
   ```

3. **RenderContext gets enriched** with available operations:
   ```dart
   class RenderContext {
     final Map<String, dynamic> rowData;
     final Future<void> Function(String, Map<String, dynamic>)? onOperation;
     final List<OperationDescriptor> availableOperations;  // NEW

     // Helper to filter operations by affected fields
     List<OperationDescriptor> operationsAffecting(List<String> fields) {
       return availableOperations.where((op) =>
         op.affectedFields.any((f) => fields.contains(f))
       ).toList();
     }
   }
   ```

4. **Widgets declare fields they care about** and auto-attach pie menus:
   ```dart
   Widget _buildCollapseButton(args, context) {
     final isCollapsed = _evaluateToBool(args['is_collapsed'], context);
     final button = IconButton(
       icon: Icon(isCollapsed ? Icons.chevron_right : Icons.expand_more),
     );

     // This widget cares about is_collapsed field
     return _autoAttachPieMenu(button, ['is_collapsed'], context);
   }

   Widget _buildBulletPoint(args, context) {
     final bullet = IconButton(icon: const Icon(Icons.circle));

     // This widget deals with hierarchy structure
     return _autoAttachPieMenu(bullet, ['parent_id', 'sort_key'], context);
   }

   Widget _buildStatusIcon(args, context) {
     final status = _evaluateToString(args['status'], context);
     final icon = Icon(_iconForStatus(status));

     // This widget deals with status
     return _autoAttachPieMenu(icon, ['status'], context);
   }

   Widget _autoAttachPieMenu(
     Widget child,
     List<String> fieldsOfInterest,
     RenderContext context,
   ) {
     // Find operations that affect any of these fields
     final relevantOps = context.operationsAffecting(fieldsOfInterest);
     if (relevantOps.isEmpty) return child;

     return PieCanvas(
       child: Builder(
         builder: (ctx) => PieMenu(
           actions: relevantOps.map((op) => PieAction(
             tooltip: Text(op.displayName),
             onSelect: () => context.onOperation?.call(op.name, {
               'id': context.rowData['id'],
             }),
             child: Icon(_iconForOperation(op)),
           )).toList(),
           child: child,
         ),
       ),
     );
   }
   ```

**Example: How It Works in Practice**

Outliner block with status:
- **Bullet point widget** declares interest in `['parent_id', 'sort_key']`
  → Gets pie menu with: `indent`, `outdent`, `move_up`, `move_down`

- **Status icon widget** declares interest in `['status']`
  → Gets pie menu with: `set_status_todo`, `set_status_done`, etc.

- **Collapse button widget** declares interest in `['is_collapsed']`
  → Gets pie menu with: `toggle_collapse`

**Pros**:
- **No abstract domains** - uses concrete field names
- **No PRQL changes** - purely backend + widget implementation
- **Automatic filtering** - operations naturally group by affected fields
- **Type-safe** - field names match actual data schema
- **Self-documenting** - clear what each widget/operation deals with
- **Flexible** - widgets can declare interest in multiple fields

**Cons**:
- Need to add `affected_fields` to OperationDescriptor (one-time backend change)
- Need macro support for `#[operation(affects = [...])]` annotation
- Widgets must explicitly declare field interests (but this is good for clarity!)

### Original Hybrid Approach (Approach 3) - VETOED

~~**Hybrid Approach (Approach 3)** seems most balanced:~~

1. Add `pie_menu()` as a render function in the DSL
2. Extend `RenderContext` to include operation metadata:
   ```dart
   class RenderContext {
     final Map<String, List<Operation>> fragmentOperations;
     final String? viewType;
     // ... existing fields
   }
   ```
3. Backend includes operation metadata in query results
4. RenderInterpreter filters operations based on view type and builds PieMenu widgets

### Implementation Steps

#### Phase 1: Foundation
1. Define `Operation` data structure for metadata
2. Extend `RenderContext` with `fragmentOperations` and `viewType`
3. Update backend to include operation metadata in query results
4. Add `pie_menu()` case to RenderInterpreter switch

#### Phase 2: Basic Integration
5. Implement `_buildPieMenu()` in RenderInterpreter
6. Create operation → PieAction mapping
7. Wire up `onOperation` callback to actually execute operations
8. Add visual feedback for operation execution

#### Phase 3: Refinement
9. Implement view-type filtering
10. Add fragment-specific operation filtering
11. Create reusable PRQL patterns for common pie menu configurations
12. Add multi-selection support

#### Phase 4: Polish
13. Add animations/transitions for pie menu appearance
14. Implement operation chaining (e.g., select multiple → bulk delete)
15. Add keyboard shortcuts for operations
16. Create documentation/examples for query authors

## Open Questions

1. **Operation Discovery**: How does a PRQL query author know what operations are available?
   - Should we generate documentation from backend capabilities?
   - Should there be a query to list available operations?

2. **Operation Composition**: Can operations be chained? (e.g., "indent 2 levels")
   - Single shot vs. repeatable operations

3. **Visual Customization**: Should pie menu appearance be customizable per view?
   - Colors, icons, layout
   - Could this be part of the render spec?

4. **Error Handling**: What happens when an operation fails?
   - Toast notification?
   - Inline error?
   - Undo support?

5. **Mobile vs. Desktop**: Different interaction patterns
   - Long-press on mobile, right-click on desktop
   - Should pie menu behavior differ by platform?

## Next Steps (Field-Based Approach)

### Phase 1: Backend Foundation
1. Add `affected_fields: Vec<String>` to `OperationDescriptor` in `holon-prql-render/src/types.rs`
2. Extend `#[operations_trait]` macro to support `#[operation(affects = [...])]` attribute
3. Annotate existing operations with affected fields:
   - `indent` → `["parent_id", "depth", "sort_key"]`
   - `toggle_collapse` → `["is_collapsed"]`
   - `set_status_*` → `["status"]`
   - etc.

### Phase 2: Flutter Integration
4. Extend `RenderContext` with `availableOperations: List<OperationDescriptor>`
5. Add helper method `operationsAffecting(List<String> fields)` to RenderContext
6. Implement `_autoAttachPieMenu(Widget, List<String>, RenderContext)` helper in RenderInterpreter
7. Update widget builders to declare field interests:
   - `_buildCollapseButton` → `['is_collapsed']`
   - `_buildBulletPoint` → `['parent_id', 'sort_key']`
   - `_buildBlockOperations` → appropriate fields

### Phase 3: Testing & Refinement
8. Test with outliner view and verify operations appear on correct widgets
9. Add icon/label mapping for operations
10. Implement operation execution flow
11. Add visual feedback for pie menu activation
12. Handle edge cases (no operations available, operation failures)

### Phase 4: Expansion
13. Add more operation annotations as needed
14. Implement multi-selection support
15. Add keyboard shortcuts for common operations
16. Document the field-based approach for future contributors
