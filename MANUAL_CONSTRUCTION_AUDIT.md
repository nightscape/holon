# Manual Construction Audit

This document identifies places in the codebase where objects are constructed manually instead of using dependency injection (DI).

## Summary

The codebase uses `ferrous-di` for dependency injection, with services registered in:
- `crates/holon/src/di/mod.rs` - Core services (BackendEngine, TursoBackend, OperationDispatcher)
- `crates/holon-todoist/src/di.rs` - Todoist services

However, there are several places where manual construction still occurs.

## Critical Issues (Should Use DI)

### 1. FFI Bridge (`crates/holon/src/api/ffi_bridge.rs`)

**Location**: `init_render_engine()` function

**Current Code**:
```rust
pub async fn init_render_engine(db_path: String) -> Result<Arc<BackendEngine>> {
    let engine = BackendEngine::new(db_path.into()).await?;
    Ok(Arc::new(engine))
}
```

**Issue**: Creates `BackendEngine` manually instead of using DI. This bypasses the DI container and means:
- No access to registered `OperationProvider`s
- No access to registered `SyncableProvider`s
- OperationDispatcher is created with empty providers

**Impact**: HIGH - This is a production entry point (Flutter FFI)

**Recommendation**: Create a DI container in the FFI bridge and use `Resolver::get_required::<BackendEngine>()`

---

### 2. BackendEngine Constructors (`crates/holon/src/api/backend_engine.rs`)

**Location**: `BackendEngine::new()` and `BackendEngine::new_in_memory()`

**Current Code**:
```rust
pub async fn new(db_path: PathBuf) -> Result<Self> {
    let backend = Arc::new(RwLock::new(TursoBackend::new(db_path).await?));
    let dispatcher = OperationDispatcher::new(Vec::new(), HashMap::new());
    // ...
}
```

**Issue**: Manually creates `OperationDispatcher` with empty providers instead of getting it from DI.

**Impact**: HIGH - These constructors are used in tests and potentially other places

**Recommendation**:
- Deprecate `new()` and `new_in_memory()` in favor of `from_dependencies()`
- Or make them use DI internally

---

### 3. Tests Using `BackendEngine::new_in_memory()`

**Locations**:
- `crates/holon/src/api/backend_engine.rs` (multiple test functions)
- `crates/holon/src/api/ffi_bridge.rs` (test functions)
- `frontends/tui/src/tui_pbt_state_machine.rs` (PBT test setup)
- `frontends/tui/tests/navigation_test.rs` (unsaved)

**Issue**: Tests create `BackendEngine` manually, bypassing DI. This means:
- Tests don't exercise the DI setup
- Tests may not reflect production behavior
- Harder to test with different provider configurations

**Impact**: MEDIUM - Tests should ideally mirror production setup

**Recommendation**: Create test helpers that set up DI containers

---

### 4. PBT Infrastructure (`frontends/tui/src/tui_pbt_backend.rs`, `tui_pbt_state_machine.rs`)

**Location**: `TuiR3blBlockTreeTest::init_test()`

**Current Code**:
```rust
let engine = runtime
    .block_on(BackendEngine::new_in_memory())
    .expect("Failed to create BackendEngine");
```

**Issue**: Creates `BackendEngine` manually for property-based testing

**Impact**: MEDIUM - PBT should ideally use same setup as production

**Recommendation**: Use DI container setup in PBT initialization

---

## Moderate Issues (May Be Acceptable)

### 5. Launcher Manual Construction (`frontends/tui/src/launcher.rs`)

**Locations**:
- `AppMain::new_boxed()` - Line 11
- `KeyBindingConfig::load_from_file()` / `KeyBindingConfig::empty()` - Lines 82-95
- `State::new()` - Line 102

**Issue**: These are UI/framework-level objects that may not need DI:
- `AppMain` is a simple struct wrapper
- `KeyBindingConfig` is configuration loading
- `State` is application state that takes `BackendEngine` (which comes from DI)

**Impact**: LOW - These are likely fine as-is since they're not core services

**Recommendation**: Keep as-is unless they start needing more dependencies

---

### 6. OperationDispatcher Manual Creation

**Location**: `crates/holon/src/api/backend_engine.rs` lines 35, 51, 806, 863

**Issue**: `OperationDispatcher::new()` is called manually in several places:
- In `BackendEngine::new()` and `new_in_memory()` constructors
- In `BackendEngine` methods that rebuild the dispatcher

**Impact**: MEDIUM - Should use DI or get from existing DI container

**Recommendation**:
- When rebuilding dispatcher, get providers from DI
- Or refactor to avoid rebuilding dispatcher

---

## Historical/Removed Code

### 7. Todoist Manual Construction (Removed?)

**Note**: The attached selection shows manual construction of Todoist components in `launcher.rs` (lines 33-98), but this code doesn't exist in the current file. This suggests:
- The code was already refactored to use DI
- Or it's in a different branch/version

**If this code still exists elsewhere**: It should be migrated to use DI through `TodoistModule`

---

### 8. QueryableCache Manual Construction

**Locations**: Multiple files including:
- `crates/holon/src/core/queryable_cache.rs` (test)
- `crates/holon/src/examples/task_queries.rs`
- `crates/holon-todoist/src/stream_integration_test.rs`
- Documentation files (MVPs.md, STREAM_INTEGRATION_GUIDE.md)

**Issue**: `QueryableCache` instances are created manually using `new()`, `new_with_backend()`, or `with_database()`.

**Impact**: LOW-MEDIUM - This may be intentional since:
- QueryableCache instances are often created per-datasource
- They may not need to be singletons
- They're frequently created in tests/examples

**However**: If QueryableCache instances need to be shared or registered with BackendEngine, they should be created through DI.

**Recommendation**:
- If QueryableCache needs to be a service, register it in DI
- If it's meant to be created per-use-case, keep manual construction but ensure backend comes from DI
- Consider creating a factory method that takes backend from DI

---

## Recommendations Summary

### High Priority
1. **FFI Bridge**: Migrate `init_render_engine()` to use DI
2. **BackendEngine Constructors**: Refactor to use DI or deprecate in favor of `from_dependencies()`

### Medium Priority
3. **Tests**: Create test helpers that set up DI containers
4. **PBT Infrastructure**: Use DI in PBT setup
5. **OperationDispatcher**: Avoid manual creation, use DI

### Low Priority
6. **UI Components**: Keep as-is unless dependencies grow

---

## Migration Strategy

1. **Create DI Test Helpers**: Add helper functions in test modules that set up DI containers
2. **Refactor FFI Bridge**: Set up DI container in FFI initialization
3. **Deprecate Manual Constructors**: Mark `BackendEngine::new()` as deprecated, guide users to DI
4. **Update Tests**: Migrate tests to use DI helpers gradually

---

## Files to Review

- `crates/holon/src/api/ffi_bridge.rs` - FFI entry point
- `crates/holon/src/api/backend_engine.rs` - Core engine constructors
- `frontends/tui/src/tui_pbt_state_machine.rs` - PBT setup
- `frontends/tui/tests/navigation_test.rs` - Test setup
- `crates/holon/src/api/backend_engine.rs` (tests) - Multiple test functions

