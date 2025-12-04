# Plan 0004: Keep QueryableCache in sync for block operations

## Phase 1 – Cache refresh hook
- Update `QueryableCache::execute_operation` default branch to remember the `id` parameter before handing off to the datasource.
- After the delegated operation succeeds, fetch the latest entity via `get_by_id` and call `update_cache`, so CDC picks up the change.

## Phase 2 – Validation
- `cargo check -p holon-todoist --lib` (covers datasource + cache crate).
- Run `cargo check -p tui-frontend` to ensure the UI still compiles after the indirect change.
- Document the behavior in a short review note.
