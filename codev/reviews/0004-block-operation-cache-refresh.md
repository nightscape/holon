# Review 0004: Keep QueryableCache in sync for block operations

## Outcomes
- QueryableCache now refreshes its local copy after any delegated operation by grabbing the `id` parameter, executing the datasource call, and (on success) fetching the updated entity and upserting it into the cache. This causes Turso CDC to fire, so frontends like the TUI immediately see indent/outdent changes without manual patches.

## Validation
- `cargo check -p holon-todoist --lib`
- `cargo check -p tui-frontend`

## Notes
- Only operations that include an `id` benefit from the automatic refresh; others behave as before. That matches our current block operations (indent/outdent/move).
