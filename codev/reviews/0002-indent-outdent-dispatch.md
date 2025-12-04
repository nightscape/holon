# Review 0002: Fix indent/outdent operation dispatch

## Outcomes
- Added `UnknownOperationError` plus helper so generated dispatchers can signal "operation not handled" without relying on string parsing.
- Updated the `#[operations_trait]` macro to return the structured error from every default arm, which keeps downstream crates in sync automatically.
- Patched `TodoistTaskDataSource`, `TodoistOperationProvider`, and `TodoistFakeOperationProvider` to stop as soon as a trait dispatcher either succeeds or fails with a *real* error; only genuine `UnknownOperationError`s now fall through to the next trait.

## Validation
- `cargo check -p holon-todoist --lib` (passes with pre-existing warnings).

## Follow-ups / Risks
- Block operations will now surface their real failure modes (e.g., unsupported `parent_id` updates). We still need to implement the actual indent/outdent semantics against the Todoist API to fully close the feature gap.
- UI still hides `indent` until `parent_id` is provided; addressing that remains out-of-scope for this change.
