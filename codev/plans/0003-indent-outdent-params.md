# Plan 0003: Provide indent/outdent parameters and Todoist support

## Phase 1 – UI parameter injection
- Update `State::execute_operation_on_selected` (frontends/tui) to clone the selected row into a mutable parameter map.
- For the `indent` operation, compute the new parent candidate (previous visual row) and inject its `id` as `parent_id`. Handle edge cases (no previous row, missing id) with user-friendly errors.

## Phase 2 – Todoist datasource support
- Extend `UpdateTaskRequest` and the Todoist client so we can send `parent_id` changes (including clearing to root).
- Enhance `TodoistTaskDataSource::set_field` to:
  * call the new client support when `parent_id` is set (string or null),
  * treat `depth` and `sort_key` writes as noop successes (local-only metadata).

## Phase 3 – Validation
- `cargo check -p tui-frontend` to cover the UI changes.
- `cargo check -p holon-todoist --lib` to cover datasource / client changes.
- Document the work & residual risks in a review note.
