# Gesture-Scoped Parameter Providers

This document describes the architecture for dynamically wiring operation parameters from multiple UI widgets. It extends the existing Automatic Operation Discovery system to support operations like `move_block` that require parameters from different sources (e.g., `id` from the dragged item, `parent_id` and `after_block_id` from the drop target).

## Problem Statement

Current state:
- Operations have `required_params` (e.g., `move_block` needs `id`, `parent_id`, `after_block_id`)
- Lineage analysis wires single-widget operations (checkbox → `set_completion`)

Challenge:
- Some params come from the item itself (`id`)
- Some come from widget values (`completed` from checkbox, `content` from text field)
- Some come from **other widgets** (`parent_id`, `after_block_id` from drag-drop target or search-select)

Additional challenge:
- Operations like `delete` that only need `id` would incorrectly match during drag-drop
- We need a way to signal **user intent** so only relevant operations are considered

Goal: **Plug-and-play extensibility** — add a new widget that provides certain params, and operations automatically become available without explicit wiring, while filtering out irrelevant operations.

## Core Concepts

### 1. Intent-Carrying Parameters

Certain parameters signal user intent. When present, only operations that explicitly declare they use these params should be considered.

**Key insight**: The `#[triggered_by]` annotation serves two purposes:
1. **Parameter transformation**: Map contextual params (like `tree_position`) to required params (`parent_id`, `after_block_id`)
2. **Intent signaling**: Declare that this operation responds to the availability of this param

```rust
// In Rust - operations declare what triggers them
#[triggered_by(availability_of = "tree_position", providing = ["parent_id", "after_block_id"])]
async fn move_block(&self, id: &str, parent_id: &str, after_block_id: Option<&str>) -> Result<()>

// Identity mapping for direct params - signals intent without transformation
#[triggered_by(availability_of = "completed")]
async fn set_completion(&self, id: &str, completed: bool) -> Result<()>
```

### 2. Widgets as Parameter Providers

Widgets declare what parameters they can provide:

```dart
mixin ParamProvider {
  List<ParamSpec> get providedParams;
  Map<String, dynamic> currentParamValues();
}

// TreeView provides tree_position on drop
class OutlinerTreeView with ParamProvider {
  @override
  List<ParamSpec> get providedParams => [
    ParamSpec(name: 'tree_position', type: TreePosition),
  ];

  @override
  Map<String, dynamic> currentParamValues() {
    if (_currentDropPosition == null) return {};
    return {'tree_position': _currentDropPosition!.toMap()};
  }
}

// SearchNodeSelector provides selected_id on confirm
class SearchNodeSelector with ParamProvider {
  @override
  List<ParamSpec> get providedParams => [
    ParamSpec(name: 'selected_id', type: String),
  ];
}
```

### 3. Gesture Context

Gestures (drag-drop, search+select) accumulate parameters as they progress:

```dart
class GestureContext {
  final String? sourceItemId;
  final RenderContext? sourceRenderContext;  // Has available operations
  final Map<String, dynamic> _committedParams = {};
  final Map<String, dynamic> _previewParams = {};

  GestureContext({this.sourceItemId, this.sourceRenderContext}) {
    if (sourceItemId != null) {
      _committedParams['id'] = sourceItemId;
    }
  }

  /// Widget updates preview (e.g., during drag hover)
  void updatePreview(Map<String, dynamic> params) {
    _previewParams.addAll(params);
  }

  /// Widget commits params (e.g., on drop, on confirm)
  void commitParams(Map<String, dynamic> params) {
    _committedParams.addAll(params);
    for (final key in params.keys) {
      _previewParams.remove(key);
    }
  }

  /// Find operations satisfiable with committed params
  List<MatchedOperation> findSatisfiableOperations() {
    final candidates = sourceRenderContext?.availableOperations ?? [];
    return OperationMatcher.findSatisfiable(candidates, _committedParams);
  }
}
```

### 4. The `#[triggered_by]` Annotation

Operations declare what contextual params trigger them using the `#[triggered_by]` macro attribute:

```rust
// In Rust - OperationDescriptor has param_mappings field
pub struct OperationDescriptor {
    // ... existing fields ...
    pub param_mappings: Vec<ParamMapping>,
}

pub struct ParamMapping {
    pub from: String,           // Source param (e.g., "tree_position", "completed")
    pub provides: Vec<String>,  // Required params this satisfies
    pub defaults: HashMap<String, Value>,  // Default values for optional params
}
```

**Usage examples:**

```rust
// Transform case: tree_position provides parent_id and after_block_id
#[triggered_by(availability_of = "tree_position", providing = ["parent_id", "after_block_id"])]
async fn move_block(&self, id: &str, parent_id: &str, after_block_id: Option<&str>) -> Result<()>

// Identity case: completed triggers and provides itself (no transformation)
// When `providing` is omitted, it defaults to [availability_of]
#[triggered_by(availability_of = "completed")]
async fn set_completion(&self, id: &str, completed: bool) -> Result<()>

// Multiple triggers: operation can be triggered by different params
#[triggered_by(availability_of = "tree_position", providing = ["parent_id", "after_block_id"])]
#[triggered_by(availability_of = "selected_id", providing = ["parent_id"])]
async fn move_block(...)
```

### 5. Intent-Based Filtering

The `OperationMatcher` filters operations based on intent-carrying params:

```dart
class OperationMatcher {
  /// Filter operations based on intent-carrying params.
  ///
  /// "Intent params" are params that operations declare in param_mappings.from
  /// (via #[triggered_by]). If any of these are present in availableParams,
  /// only operations that use them are considered.
  static List<OperationDescriptor> _filterByIntentParams(
    List<OperationDescriptor> operations,
    Map<String, dynamic> availableParams,
  ) {
    // 1. Collect all "intent param sources" from param_mappings
    final intentParamSources = operations
        .expand((op) => op.paramMappings.map((m) => m.from))
        .toSet();

    // 2. Which intent params are actually present?
    final presentIntentParams = intentParamSources
        .where((p) => availableParams.containsKey(p))
        .toSet();

    // 3. If none present, return all operations
    if (presentIntentParams.isEmpty) return operations;

    // 4. Filter to operations that use at least one present intent param
    return operations.where((op) =>
        op.paramMappings.any((m) => presentIntentParams.contains(m.from))
    ).toList();
  }
}
```

**How this prevents unwanted matches:**

| Scenario | Committed Params | Intent Params Present | `delete` | `move_block` |
|----------|------------------|----------------------|----------|--------------|
| Drag-drop | `{id, tree_position}` | `tree_position` | ❌ Excluded (no triggered_by) | ✅ Included |
| Checkbox | `{id, completed}` | `completed` | ❌ Excluded | ❌ Excluded |
| Checkbox | `{id, completed}` | `completed` | ❌ Excluded | N/A |
| (set_completion has triggered_by) | | | | ✅ `set_completion` included |

### 6. Operation Matching (Flutter-side)

Matching logic runs entirely in Flutter to minimize FFI surface:

```dart
class MatchedOperation {
  final OperationDescriptor descriptor;
  final Map<String, dynamic> resolvedParams;
  final List<String> missingParams;

  bool get isFullySatisfied => missingParams.isEmpty;
}

class OperationMatcher {
  static List<MatchedOperation> findSatisfiable(
    List<OperationDescriptor> operations,
    Map<String, dynamic> availableParams,
  ) {
    // 1. Filter by intent params first
    final filteredOps = _filterByIntentParams(operations, availableParams);

    // 2. Try to match each operation
    final results = <MatchedOperation>[];
    for (final op in filteredOps) {
      final match = _tryMatch(op, availableParams);
      if (match != null) results.add(match);
    }

    // 3. Sort: fully satisfied first, then by resolved param count
    results.sort((a, b) {
      if (a.isFullySatisfied && !b.isFullySatisfied) return -1;
      if (!a.isFullySatisfied && b.isFullySatisfied) return 1;
      // Prefer operations that use more of the committed params
      return b.resolvedParams.length.compareTo(a.resolvedParams.length);
    });

    return results;
  }

  static dynamic _resolveParam(
    String paramName,
    OperationDescriptor op,
    Map<String, dynamic> available,
  ) {
    // Direct match
    if (available.containsKey(paramName)) {
      return available[paramName];
    }

    // Try param mappings (from #[triggered_by] annotations)
    for (final mapping in op.paramMappings) {
      if (!mapping.provides.contains(paramName)) continue;

      final sourceValue = available[mapping.from];
      if (sourceValue != null) {
        // Extract from structured source (e.g., tree_position['parent_id'])
        if (sourceValue is Map<String, dynamic> &&
            sourceValue.containsKey(paramName)) {
          return sourceValue[paramName];
        }
        // Use source directly if single-value mapping
        if (mapping.provides.length == 1) {
          return sourceValue;
        }
      }

      // Check defaults
      if (mapping.defaults.containsKey(paramName)) {
        return mapping.defaults[paramName];
      }
    }

    return null;
  }
}
```

## Data Flow

### Drag-Drop Flow

```
1. User starts drag on TreeItem[A]
   └─ GestureContext created: { id: "A" }

2. User hovers over drop position (after B, under P)
   └─ TreeView.updatePreview({ tree_position: {parent_id: P, after_block_id: B} })
   └─ UI can show: "Move A after B"

3. User drops
   └─ TreeView.commitParams({ tree_position: {...} })
   └─ GestureContext now: { id: "A", tree_position: {parent_id: P, after_block_id: B} }

4. Intent filtering
   └─ Intent params present: { tree_position }
   └─ move_block has #[triggered_by(availability_of = "tree_position")] → included
   └─ delete has no #[triggered_by] → excluded

5. Operation matching
   └─ move_block needs {id, parent_id, after_block_id}
   └─ tree_position provides {parent_id, after_block_id}
   └─ All params resolved ✓

6. Execute via existing onOperation callback
```

### Checkbox Click Flow

```
1. User clicks checkbox on Task[A]
   └─ GestureContext created: { id: "A", completed: true }

2. Intent filtering
   └─ Intent params present: { completed }
   └─ set_completion has #[triggered_by(availability_of = "completed")] → included
   └─ delete has no #[triggered_by] → excluded
   └─ move_block has no triggered_by for "completed" → excluded

3. Operation matching
   └─ set_completion needs {id, completed}
   └─ Both directly available ✓

4. Execute
```

### Search+Select Flow

```
1. User selects TreeItem[A], triggers "Move to..." from pie menu
   └─ GestureContext created: { id: "A" }

2. SearchNodeSelector opens
   └─ User types, filters nodes
   └─ User selects target node P
   └─ updatePreview({ selected_id: "P" })

3. User confirms (Enter or button)
   └─ commitParams({ selected_id: "P" })
   └─ GestureContext now: { id: "A", selected_id: "P" }

4. Intent filtering + Operation matching
   └─ move_block param_mapping: selected_id → {parent_id}, defaults: {after_block_id: null}
   └─ Resolved: { id: "A", parent_id: "P", after_block_id: null }

5. Execute
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ Flutter (UI)                                                    │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │ TreeItem     │    │ TreeView     │    │ SearchSelect │      │
│  │ (drag source)│    │ (drop target)│    │ (alternative)│      │
│  │              │    │              │    │              │      │
│  │ provides:    │    │ provides:    │    │ provides:    │      │
│  │   id         │    │   tree_pos   │    │   selected_id│      │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘      │
│         │                   │                   │               │
│         └───────────────────┼───────────────────┘               │
│                             ▼                                   │
│                   ┌──────────────────┐                          │
│                   │  GestureContext  │                          │
│                   │  (accumulates    │                          │
│                   │   params)        │                          │
│                   └────────┬─────────┘                          │
│                            │                                    │
│                            ▼                                    │
│                   ┌──────────────────┐                          │
│                   │ OperationMatcher │                          │
│                   │                  │                          │
│                   │ 1. Filter by     │                          │
│                   │    intent params │                          │
│                   │ 2. Match params  │                          │
│                   │ 3. Sort by fit   │                          │
│                   └────────┬─────────┘                          │
│                            │                                    │
│         ┌──────────────────┼──────────────────┐                 │
│         ▼                  ▼                  ▼                 │
│   ┌──────────┐      ┌──────────┐      ┌──────────┐             │
│   │ Execute  │      │ Prompt   │      │ Disambig │             │
│   │ (1 match,│      │ (1 match,│      │ (N match)│             │
│   │  complete)│     │  partial)│      │          │             │
│   └────┬─────┘      └────┬─────┘      └────┬─────┘             │
│        │                 │                 │                    │
│        └─────────────────┴─────────────────┘                    │
│                          │                                      │
│                          ▼                                      │
│              ┌─────────────────────┐                            │
│              │ RenderContext       │                            │
│              │ .onOperation(       │                            │
│              │   entityName,       │                            │
│              │   opName,           │                            │
│              │   params)           │                            │
│              └──────────┬──────────┘                            │
└─────────────────────────┼───────────────────────────────────────┘
                          │ FFI
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│ Rust (Backend)                                                  │
│                                                                 │
│  ┌─────────────────────┐    ┌─────────────────────┐            │
│  │ OperationDispatcher │───▶│ TodoistDataSource   │            │
│  │ (routes by entity)  │    │ .execute_operation()│            │
│  └─────────────────────┘    └─────────────────────┘            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### 1. No Global Availability Enum

Widgets don't use a global `Availability` enum. Each widget simply commits params when ready. This allows adding new widgets without modifying shared enums.

### 2. Intent Signaling via `#[triggered_by]`

Operations explicitly declare what params trigger them. This is self-documenting: adding `#[triggered_by(availability_of = "X")]` automatically makes `X` an intent-carrying param that filters out unrelated operations.

### 3. Matching Logic in Flutter

Operation matching runs in Flutter, not Rust. This:
- Minimizes FFI surface area
- Keeps data flow unidirectional (Rust → Flutter for descriptors, Flutter → Rust for execution)
- Allows Flutter to handle disambiguation UI directly

### 4. Operation-Side Param Mappings

Operations declare how to derive their params from alternative sources. This keeps domain knowledge with the operation, not scattered across widgets.

### 5. Gesture Context from Inherited Widget

`GestureContext` is provided via `InheritedWidget`, allowing any widget in the tree to contribute params without explicit prop drilling.

## Disambiguation

When multiple operations match the same params:

1. **Keyboard modifiers** (configurable): Default operation on drop, Shift+drop for copy, etc.
2. **Priority field**: Operations have priority; highest wins without modifier
3. **Disambiguation menu**: If still ambiguous, show quick picker

Configuration example (YAML):

```yaml
disambiguation:
  modifiers:
    none: highest_priority
    shift: second_priority
    alt: show_menu

  operation_priorities:
    move_block: 100
    copy_block: 50
    link_block: 25
```

## Implementation Status

### Phase 1: Core Infrastructure ✅

- [x] **Rust: Add `ParamMapping` to `OperationDescriptor`**
  - File: `crates/holon-api/src/render_types.rs`
  - Added `param_mappings: Vec<ParamMapping>` field
  - Added `ParamMapping` struct with `from`, `provides`, `defaults`

- [x] **Rust: Add `#[triggered_by]` macro attribute**
  - File: `crates/holon-macros/src/lib.rs`
  - Parses `availability_of` and `providing` arguments
  - Supports identity mappings (providing defaults to [availability_of])

- [x] **Rust: Update `move_block` with triggered_by**
  - File: `crates/holon/src/core/datasource.rs`
  - `#[triggered_by(availability_of = "tree_position", providing = ["parent_id", "after_block_id"])]`

- [x] **Rust: Update task operations with triggered_by**
  - `set_completion`: `#[triggered_by(availability_of = "completed")]`
  - `set_priority`: `#[triggered_by(availability_of = "priority")]`

- [x] **Rust: Update `find_operations` to consider param_mappings**
  - Operations with param_mappings are included even if not all required params are directly available

### Phase 2: Flutter Core ✅

- [x] **Create `ParamSpec` and `ParamProvider` mixin**
  - File: `frontends/flutter/lib/render/param_provider.dart`

- [x] **Create `GestureContext`**
  - File: `frontends/flutter/lib/render/gesture_context.dart`
  - Includes `TreePosition` class with `parent_id` and `after_block_id`

- [x] **Create `OperationMatcher`**
  - File: `frontends/flutter/lib/render/operation_matcher.dart`
  - Includes intent-based filtering via `_filterByIntentParams`

### Phase 3: Widget Integration ✅

- [x] **TreeView drag-drop integration**
  - File: `frontends/flutter/lib/render/tree_view_widget.dart`
  - Commits `tree_position` on drop
  - Uses `GestureContext` and `OperationMatcher`

### Phase 4: Search+Select ✅

- [x] **Create `SearchSelectOverlay` widget**
  - File: `frontends/flutter/lib/render/search_select_overlay.dart`
  - Implements `ParamProvider` mixin providing `selected_id`
  - DragTarget that accepts drops during drag operations
  - Expands to show filtered node list when dropped on
  - Commits `selected_id` on node selection

- [x] **Wire into drag-drop flow**
  - File: `frontends/flutter/lib/render/render_interpreter.dart`
  - Shows overlay when drag starts (positioned right of drag source)
  - Overlay acts as alternative drop target for search-based move
  - Uses `GestureContext` and `OperationMatcher` for operation execution

- [x] **Add `selected_id` triggered_by to move_block**
  - File: `crates/holon/src/core/datasource.rs`
  - `#[triggered_by(availability_of = "selected_id", providing = ["parent_id"])]`

### Phase 5: Polish

- [ ] **Add disambiguation UI**
  - Quick menu when multiple operations match

- [ ] **Add keyboard modifier support**
  - Read modifiers during drop
  - Filter operations by modifier-priority mapping

- [ ] **Configuration for priorities**
  - YAML or settings for operation priorities and modifier mappings

## Related Documents

- `ARCHITECTURE_PRINCIPLES.md` - Overall architecture, existing Operation Discovery
- `REACTIVE_PRQL_RENDERING.md` - Query/render system details
- `docs/architecture.md` - Technical architecture details

## Open Questions

1. **Should `GestureContext` be part of `RenderContext`?** Currently separate, but could be combined if every render needs gesture awareness.

2. **How to handle nested gestures?** (e.g., start drag, then trigger search+select for target) — Probably disallow or cancel outer gesture.

3. **Preview rendering**: How should "drop here" preview work? Currently `previewParams` exist but UI visualization not specified.
