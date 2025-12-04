# Review 0003: Provide indent/outdent parameters and Todoist support

## Outcomes
- The TUI now injects a `parent_id` before dispatching the `indent` operation, so the backend has the correct context instead of receiving a null value.
- `UpdateTaskRequest` plus the Todoist client can send parent changes (and explicit clears to root), and the datasource accepts `parent_id`, `depth`, and `sort_key` writes—treating the latter two as local-only no-ops.

## Validation
- `cargo check -p tui-frontend`
- `cargo check -p holon-todoist --lib`

## Follow-ups / Risks
- Only parent reassignment is sent to Todoist; we still ignore fractional `sort_key` / `depth` updates because the API has no equivalent. Items may land at Todoist's default insertion point instead of the requested fractional slot.
- QueryableCache still relies on Todoist sync events to update local order. If Todoist doesn't rebroadcast quickly, the UI may momentarily show the old structure.
