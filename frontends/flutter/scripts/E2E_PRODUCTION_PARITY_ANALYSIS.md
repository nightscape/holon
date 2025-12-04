# E2E Test vs. Production Parity Analysis

**Date:** 2026-01-23
**Subtask:** 2-4 - Check E2E test for production parity
**Issue:** Grey window deadlock when running with `RUST_BACKTRACE=full`

## Executive Summary

**The E2E tests CANNOT reproduce the grey window deadlock** because they use a fundamentally different initialization path that bypasses OpenTelemetry and HTTP client creation entirely.

## Key Finding

| Aspect | Production (Flutter FFI) | E2E Tests |
|--------|--------------------------|-----------|
| OpenTelemetry | Initialized via `init_opentelemetry()` | **NOT INITIALIZED** |
| OTLP HTTP client | Created via `opentelemetry_otlp::SpanExporter::builder().with_http().build()` | **NOT CREATED** |
| Reqwest HTTP client | Used by OTLP exporter | **NOT USED** |
| Tracing subscriber | Full OpenTelemetry bridge | **NOT CONFIGURED** |
| Potential for deadlock | YES (HTTP + RUST_BACKTRACE) | **NO** |

## Detailed Analysis

### Production Initialization Path (Flutter FFI)

**File:** `frontends/flutter/rust/src/api/ffi_bridge.rs`

```
Flutter App Start
    └─► main.dart: frb.RustLib.init()
         └─► ffi.initRenderEngine(db_path, config)
              └─► init_opentelemetry()          <── CRITICAL DIFFERENCE
                   └─► SpanExporter::builder()
                        .with_http()           <── Creates reqwest HTTP client
                        .build()               <── POTENTIAL DEADLOCK POINT
                   └─► LogExporter::builder()
                        .with_http()           <── Creates reqwest HTTP client
                        .build()               <── POTENTIAL DEADLOCK POINT
              └─► holon::di::create_backend_engine()
```

When `RUST_BACKTRACE=full` is set, any error during HTTP client initialization triggers symbol resolution which can interact badly with async runtimes and native code, causing a deadlock.

### E2E Test Initialization Path

**Files:**
- `crates/holon/src/testing/e2e_test_helpers.rs`
- `crates/holon/src/di/test_helpers.rs`

```
E2E Test Start
    └─► E2ETestContext::new()
         └─► create_test_engine()
              └─► create_backend_engine(":memory:", |_| Ok(()))
                   └─► TursoBackend::new()      <── Direct SQLite, no HTTP
                   └─► OperationDispatcher      <── No network calls
                   └─► TransformPipeline        <── Pure computation

         NO init_opentelemetry()  <── SKIPPED ENTIRELY
         NO SpanExporter         <── NEVER CREATED
         NO HTTP clients          <── NEVER CREATED
```

The E2E tests call `create_backend_engine()` directly from the DI module, which creates:
1. `TursoBackend` - Local SQLite database
2. `OperationDispatcher` - Local operation handling
3. `TransformPipeline` - PRQL transformations

**It never calls `init_opentelemetry()`**, so:
- No OTLP exporters are created
- No HTTP clients are created
- No reqwest connections are established
- No symbol resolution interactions with async runtime

## Code Evidence

### Production Code (`ffi_bridge.rs` lines 324-341)
```rust
pub async fn init_render_engine(
    db_path: String,
    config: HashMap<String, String>,
) -> anyhow::Result<Arc<BackendEngine>> {
    // ...
    init_opentelemetry().await?;  // <-- THIS IS THE DIFFERENCE
    // ...
    let engine = holon::di::create_backend_engine(db_path.into(), |services| {
        // DI setup...
    }).await?;
```

### E2E Test Code (`test_helpers.rs` lines 31-33)
```rust
pub async fn create_test_engine() -> Result<Arc<BackendEngine>> {
    create_test_engine_with_path(":memory:".into()).await
}

pub async fn create_test_engine_with_path(db_path: PathBuf) -> Result<Arc<BackendEngine>> {
    create_backend_engine(db_path, |_| Ok(())).await  // <-- NO init_opentelemetry()
}
```

## Why E2E Tests Cannot Reproduce the Issue

1. **No OpenTelemetry Initialization**: E2E tests skip `init_opentelemetry()` entirely
2. **No HTTP Client Creation**: The OTLP HTTP client (reqwest-based) is never created
3. **No Symbol Resolution Pressure**: Without HTTP client errors, `RUST_BACKTRACE=full` has nothing to resolve
4. **No Async/Native Thread Interaction**: The code path that causes deadlock is never executed

## Verification Tests (Manual - Requires Rust Tooling)

Run these commands to verify E2E tests don't hang:

```bash
# Test 1: Run E2E tests with RUST_BACKTRACE=full
cd /Users/martin/Workspaces/pkm/holon
RUST_BACKTRACE=full cargo test -p holon e2e_backend_engine_test -- --nocapture

# Expected: Tests complete normally (no hang)

# Test 2: Run production Flutter app with RUST_BACKTRACE=full
cd frontends/flutter
RUST_BACKTRACE=full flutter run -d macos

# Expected: Grey window (hang at OpenTelemetry init)
```

## Recommendation to Achieve Production Parity

To reproduce the issue in E2E tests, you would need to:

1. **Option A: Add OpenTelemetry to E2E tests** (not recommended - adds complexity)
   ```rust
   pub async fn create_test_engine_with_otel() -> Result<Arc<BackendEngine>> {
       init_opentelemetry().await?;  // Add this
       create_backend_engine(":memory:".into(), |_| Ok(())).await
   }
   ```

2. **Option B: Create a specific FFI initialization test** (recommended)
   ```rust
   #[tokio::test]
   async fn test_ffi_init_with_rust_backtrace() {
       std::env::set_var("RUST_BACKTRACE", "full");
       std::env::set_var("OTEL_TRACES_EXPORTER", "stdout");  // Avoid HTTP

       let result = init_render_engine("test.db".into(), HashMap::new()).await;
       assert!(result.is_ok());
   }
   ```

3. **Option C: Test OpenTelemetry initialization in isolation** (quick to implement)
   ```rust
   #[tokio::test]
   async fn test_opentelemetry_init_with_rust_backtrace() {
       std::env::set_var("RUST_BACKTRACE", "full");
       std::env::set_var("OTEL_TRACES_EXPORTER", "stdout");  // Use stdout, not OTLP

       let result = init_opentelemetry().await;
       assert!(result.is_ok());
   }
   ```

## Conclusion

The `general_e2e_pbt.rs` file referenced in the spec does not exist. The actual E2E tests (`crates/holon/tests/e2e_backend_engine_test.rs`) use a different initialization path that:

1. **Skips OpenTelemetry entirely**
2. **Never creates HTTP clients**
3. **Cannot reproduce the RUST_BACKTRACE + OTLP deadlock**

This is a **fundamental architectural difference** - the E2E tests are designed to test the BackendEngine query/operation logic, not the FFI initialization code path.

To test the deadlock issue, the recommended approach is:
1. Create a separate integration test for FFI initialization
2. Or use the manual reproduction scripts in `frontends/flutter/scripts/`
