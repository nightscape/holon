# Refactoring: Todoist Code Moved to Separate Crate

## What Was Done

Successfully separated Todoist-specific code into `holon-todoist` crate while keeping generic infrastructure in the main `holon` crate.

## New Crate: holon-todoist

**Location**: `crates/holon-todoist/`

**Contains**:
- `src/contracts.rs` - Todoist-specific contract specifications
  - `indent_block_contract()` - Contract for indent operation
- `src/fake.rs` - `TodoistFake` implementation for optimistic updates
- `src/client.rs` - `TodoistClient` HTTP client with contract validation
- `src/lib.rs` - Module exports

**Tests**: ✅ 5 tests passing
- 4 contract tests
- 1 fake implementation test

## Main Crate: holon

**Kept (Generic Infrastructure)**:
- `src/sync/external_system.rs`:
  - `ExternalSystem` trait (generic interface)
  - `json_to_value()` helper function
- `src/storage/command_sourcing.rs`:
  - Schema setup (`commands`, `id_mappings` tables)
  - `InMemoryStateAccess` implementation
- `src/contracts/mod.rs`:
  - Now just documentation pointing to system-specific crates

**Removed (Todoist-Specific)**:
- ❌ `src/contracts/todoist.rs` - Moved to holon-todoist
- ❌ `TodoistFake` implementation - Moved to holon-todoist
- ❌ `TodoistClient` implementation - Moved to holon-todoist

**Tests**: ✅ 1 test passing (schema initialization)

## Dependencies

The new crate depends on:
```toml
holon = { path = "../holon" }
```

This creates a clean dependency flow:
```
holon-todoist
    ├─> holon (generic infrastructure)
```

## Benefits

1. **Clean Separation**: Generic infrastructure vs system-specific code
2. **Extensibility**: Easy to add more system crates (holon-notion, etc.)
3. **Reusability**: `ExternalSystem` trait can be implemented by any system
4. **No Code Duplication**: Shared helpers like `json_to_value()` remain in main crate
5. **Better Organization**: Each external system gets its own crate

## Future External Systems

To add a new external system:

1. Create `crates/holon-{system}/`
2. Implement contracts in `src/contracts.rs`
3. Implement `ExternalSystem` for fake and real clients
4. Add to workspace `Cargo.toml`

Example:
```
crates/holon-notion/
    src/
        fake.rs         # NotionFake
        client.rs       # NotionClient
        lib.rs
```

## Test Results

```
✅ holon-todoist: 5 tests passing
✅ holon: 1 test passing (command_sourcing)
✅ All previous functionality preserved
```
