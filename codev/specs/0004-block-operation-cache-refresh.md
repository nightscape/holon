# Spec 0004: Keep QueryableCache in sync for block operations

## Problem
Indent/outdent now succeed against Todoist, but the TUI continues to show the old hierarchy. Unlike basic `set_field` calls (which go through `QueryableCache::set_field`), block operations invoke the datasource directly, so the cache never refreshes its copy and therefore emits no CDC events for the UI.

## Requirements
1. After any operation that QueryableCache delegates to the underlying datasource (e.g., `indent`, `outdent`, `move_block`), refresh the affected entity inside the cache so downstream consumers receive CDC updates.
2. Do not rely on manual UI mutations; keep the existing CDC-based flow intact.
3. Keep the change localized to QueryableCache so all providers benefit automatically.

## Out of Scope
- Emitting custom change events from the Todoist datasource (handled separately by sync).
- Reordering logic inside the UI; that already reacts to CDC changes when they happen.

## Acceptance Criteria
- Executing `indent`/`outdent` triggers a cache refresh (and therefore a CDC row change), allowing the TUI to reorder blocks without manual intervention.
- Other operations (e.g., `move_block`) also benefit automatically if they include an `id` parameter.
