# Spec 0003: Provide indent/outdent parameters and Todoist support

## Problem
After fixing the dispatch layer (Spec 0002), indent/outdent now reach the Todoist datasource but still fail:
- `indent` reports `Missing or invalid parameter: parent_id` because the TUI forwards only the selected row as params, so the backend never learns which block should be the new parent.
- `outdent` reports `Field parent_id not supported` since the Todoist datasource refuses to update `parent_id`, and it also cannot handle subsequent `depth` / `sort_key` writes used by the generic block operations.

## Requirements
1. When the user presses the indent key, compute the appropriate `parent_id` (previous block) and include it in the operation payload so the backend has the context it needs.
2. Teach the Todoist datasource to accept `parent_id` updates (moving a task under a different parent) and tolerate local-only fields such as `depth` and `sort_key`.
3. Keep error reporting friendly: if there is no previous block to indent under, the UI should surface a clear message instead of sending a malformed request.
4. Leave the backend contract (operation descriptors, dispatch helpers) unchanged so other providers remain compatible.

## Scope / Out of Scope
- In scope: front-end parameter injection; Todoist `set_field` enhancements; updating request structs + client.
- Out of scope: improving fractional sort ordering or Todoist depth modelling; broader UI enhancements beyond this operation.
