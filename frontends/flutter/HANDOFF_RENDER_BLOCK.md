# Handoff: Implement `render_block` Primitive with Live Query Support

## Goal

Add a `render_block(this)` render primitive that automatically dispatches based on block type. For query blocks (PRQL source), it executes the embedded query with its own CDC stream and renders the results inline.

## Context

Currently, render specs explicitly specify how to render content:
```prql
render (list item_template:(row (editable_text content:content)))
```

We want a polymorphic `render_block` that does the right thing based on the block's `content_type` and `source_language`:
```prql
render (list item_template:(row (render_block this)))
```

This enables nested live queries: a journal page can contain a query block that shows live-updating task results.

## Dispatch Logic

```
render_block(this) dispatches based on content_type/source_language:
├── content_type: "source" AND source_language: "prql"
│   → LiveQueryWidget(prql: source_code)  # Executes query, renders with own CDC
├── content_type: "source" AND source_language: other
│   → source_editor(language: source_language, source: source_code)
├── content_type: "text" (or default)
│   → editable_text(content: content)
```

## What Exists

1. **RenderInterpreter** (`lib/render/render_interpreter.dart`)
   - Maps function names to widget builders
   - Switch statement at lines 175-259 handles all primitives
   - Add `case 'render_block':` here

2. **ReactiveQueryWidget** (`lib/render/reactive_query_widget.dart`)
   - Renders query results with CDC streaming
   - Takes `sql` (used only for keying), `params`, `renderSpec`, `changeStream`, `initialData`
   - `NestedQueryWidget` (line 650) is a trivial wrapper - **can be deleted**

3. **Query Providers** (`lib/providers/query_providers.dart`)
   - `queryResultByPrqlProvider(prql)` - family provider keyed on PRQL string
   - Returns `(renderSpec, initialData, changeStream)` - everything needed
   - Handles compilation and CDC setup internally

## What Needs to Be Implemented

### 1. `LiveQueryWidget` (new file: `lib/render/live_query_widget.dart`)

A widget that:
1. Takes PRQL source code as input
2. Uses `queryResultByPrqlProvider(prql)` to get everything
3. Creates a ReactiveQueryWidget with the results
4. Handles loading/error states

```dart
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../providers/query_providers.dart';
import '../utils/value_converter.dart';
import 'reactive_query_widget.dart';

class LiveQueryWidget extends ConsumerWidget {
  final String prql;

  const LiveQueryWidget({
    super.key,
    required this.prql,
  });

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    // Provider is keyed on PRQL string - handles compilation, execution, CDC setup
    final queryResult = ref.watch(queryResultByPrqlProvider(prql));

    return queryResult.when(
      loading: () => const Center(child: CircularProgressIndicator()),
      error: (error, _) => Text('Query error: $error'),
      data: (result) => ReactiveQueryWidget(
        sql: prql, // Used only for keying (could rename param to queryKey)
        params: const {},
        renderSpec: result.renderSpec,
        changeStream: result.changeStream,
        initialData: result.initialData
            .map((row) => valueMapToDynamic(row))
            .toList(),
      ),
    );
  }
}
```

**Key points:**
- `queryResultByPrqlProvider` is a family provider - each unique PRQL string gets its own instance
- The provider handles: PRQL → compile → execute → CDC stream setup
- ReactiveQueryWidget receives pre-computed results, just renders + handles updates
- Each LiveQueryWidget instance has independent CDC subscription

### 2. `_buildRenderBlock` in RenderInterpreter

Add to `render_interpreter.dart`:

```dart
case 'render_block':
  return _buildRenderBlock(namedArgs, positionalArgs, enrichedContext);
```

Implementation:

```dart
Widget _buildRenderBlock(
  Map<String, RenderExpr> namedArgs,
  List<RenderExpr> positionalArgs,
  RenderContext context,
) {
  // Get block metadata from context.rowData
  final contentType = context.rowData['content_type']?.toString();
  final sourceLanguage = context.rowData['source_language']?.toString()?.toLowerCase();

  // Case 1: PRQL query block - execute and render live
  // Note: For source blocks, the source code is stored in the 'content' column
  if (contentType == 'source' && sourceLanguage == 'prql') {
    final prqlSource = context.rowData['content']?.toString();
    assert(prqlSource != null && prqlSource.isNotEmpty,
      'Query block ${context.rowData['id']} has content_type=source, '
      'source_language=prql but empty content');

    return LiveQueryWidget(prql: prqlSource!);
  }

  // Case 2: Other source block - syntax highlighted editor
  if (contentType == 'source' && sourceLanguage != null) {
    return _buildSourceEditor({
      'language': RenderExpr.literal(Value.string(sourceLanguage)),
      'source': RenderExpr.literal(Value.string(
        context.rowData['content']?.toString() ?? ''
      )),
    }, context);
  }

  // Case 3: Default - editable text
  final contentExpr = namedArgs['content'] ??
    (positionalArgs.isNotEmpty ? positionalArgs.first : null);

  if (contentExpr != null) {
    return _buildEditableText({'content': contentExpr}, [], context);
  }

  // Fallback: render content column as text
  final content = context.rowData['content']?.toString() ?? '';
  return Text(content);
}
```

### 3. Cleanup: Delete `NestedQueryWidget`

`NestedQueryWidget` in `reactive_query_widget.dart` (line 650) is a trivial pass-through:

```dart
class NestedQueryWidget extends StatelessWidget {
  // Just returns ReactiveQueryWidget with same params
}
```

**Delete it.** If anything uses it, replace with direct `ReactiveQueryWidget` call or use `LiveQueryWidget` for PRQL-based queries.

## Files to Modify

1. **Create**: `lib/render/live_query_widget.dart`
2. **Modify**: `lib/render/render_interpreter.dart`
   - Add import for LiveQueryWidget
   - Add `case 'render_block':` in switch
   - Add `_buildRenderBlock` method
3. **Modify**: `lib/render/reactive_query_widget.dart`
   - Delete `NestedQueryWidget` class (lines 650-686)
   - Search codebase for usages and update if any

## Testing

1. Create a test block with:
   - `content_type: "source"`
   - `source_language: "prql"`
   - `content: "from blocks select { id, content } render (list item_template:(text content))"`

   Note: Source blocks store their code in the `content` column (not a separate `source_code` column).

2. Render it via a parent query using `render_block(this)`

3. Verify:
   - The nested query executes
   - Results appear inline
   - CDC updates flow through (modify a block, see it update)

## Constraints

- **Fail hard on errors**: Use assertions, don't program defensively
- **No try-catch**: Let errors bubble up
- **Keep it simple**: Don't over-engineer for hypothetical cases
- **Reuse existing infrastructure**: `queryResultByPrqlProvider` does all the heavy lifting

## Architecture Note

The key insight is that `queryResultByPrqlProvider` already does everything:
1. Takes PRQL string as family parameter (automatic keying)
2. Compiles PRQL to SQL + RenderSpec
3. Executes query and gets initial data
4. Sets up CDC stream

`LiveQueryWidget` just wraps this provider and passes results to `ReactiveQueryWidget`. No new compilation or streaming logic needed.

## Interaction with Multi-View Render

See `/docs/HANDOFF_MULTI_VIEW_RENDER.md` for the multi-view render feature.

**RenderSpec structure change:**
- `RenderSpec.root` is replaced by `RenderSpec.views: HashMap<String, ViewSpec>`
- Backward compat: `RenderSpec.root()` getter returns `views[defaultView].structure`
- `ReactiveQueryWidget` should use `renderSpec.root()` or handle views explicitly

**Nested queries with multiple views:**
- A PRQL block executed via `render_block` may itself define multiple views
- For now: use `renderSpec.defaultView` (simplest)
- Future enhancement: optionally show view switcher for nested queries

## Out of Scope (for now)

- Cycle detection (defer until needed)
- Query result caching across identical source_code
- Lazy loading of off-screen query blocks
- Layout regions (separate task)
- Renaming ReactiveQueryWidget's `sql` param to `queryKey` (nice-to-have cleanup)
