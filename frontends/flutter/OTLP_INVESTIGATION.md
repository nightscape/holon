# OpenTelemetry OTLP HTTP Investigation

**Subtask 2-3: Test if OpenTelemetry OTLP HTTP connection causes hang**

## Summary

This document describes the investigation into whether the OpenTelemetry OTLP HTTP exporter
causes the grey window deadlock when combined with `RUST_BACKTRACE=full`.

## Hypothesis

The grey window issue is caused by the OTLP HTTP client initialization blocking or deadlocking
when `RUST_BACKTRACE=full` triggers symbol resolution during HTTP client setup.

### Technical Details

In `ffi_bridge.rs`, the `init_opentelemetry()` function creates OTLP exporters:

```rust
// Line 114-117 - Suspected deadlock point
let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
    .with_http()
    .with_endpoint(traces_endpoint)
    .build()?;
```

The `with_http()` call:
1. Creates a `reqwest` HTTP client
2. `reqwest` uses `hyper` for async HTTP and `tokio` for async runtime
3. TLS certificate loading may occur
4. Any error during this setup triggers backtrace capture when `RUST_BACKTRACE=full`

### Why RUST_BACKTRACE=full Matters

When `RUST_BACKTRACE=full` is set:
- Rust captures full backtraces on any panic or error
- Backtrace capture requires symbol resolution (DWARF debug info parsing)
- Symbol resolution acquires global locks
- If the async runtime is in a specific state, this can cause deadlock

## Test Scripts

### 1. OTLP Bypass Test (`test_otlp_bypass.sh`)

**Purpose:** Test if bypassing OTLP HTTP entirely resolves the issue.

**How it works:**
- Keeps `RUST_BACKTRACE=full` (the suspected trigger)
- Sets `OTEL_TRACES_EXPORTER=stdout` (bypasses OTLP HTTP)

**Run:**
```bash
cd frontends/flutter
./scripts/test_otlp_bypass.sh
```

**Interpretation:**
- If app works: **CONFIRMS** OTLP HTTP client is the cause
- If app hangs: **CONTRADICTS** hypothesis, look elsewhere

### 2. Network Timeout Test (`test_otlp_network_timeout.sh`)

**Purpose:** Test behavior when OTLP endpoint is unreachable.

**Tests:**
1. Default endpoint (localhost:4318) - likely unreachable
2. Non-routable address (10.255.255.1:4318)
3. Short timeout (OTEL_EXPORTER_OTLP_TIMEOUT=1000)

**Run:**
```bash
cd frontends/flutter
./scripts/test_otlp_network_timeout.sh
```

**Interpretation:**
- If hang with unreachable: HTTP connection timeout contributes to issue
- If immediate fail still hangs: Issue is in HTTP client init, not connection
- If short timeout helps: Adding explicit timeout could mitigate

## Expected Diagnostic Output

When running tests, look for these diagnostic markers:

### Successful OTLP Bypass

```
[FFI-DIAG] init_opentelemetry() ENTER
[FFI-DIAG] RUST_BACKTRACE=full
[FFI-DIAG] service_name=holon-backend
[FFI-DIAG] exporter_type=stdout     <-- Key: shows stdout, not otlp
[FFI-DIAG] Creating OpenTelemetry Resource...
[FFI-DIAG] Using stdout exporters (no OTLP)   <-- OTLP bypassed
[FFI-DIAG] Creating stdout SpanExporter...
[FFI-DIAG] Creating stdout LogExporter...
... continues to completion ...
[DIAG 36] About to call runApp()
```

### Deadlock with OTLP (Expected Failure Case)

```
[FFI-DIAG] init_opentelemetry() ENTER
[FFI-DIAG] RUST_BACKTRACE=full
[FFI-DIAG] service_name=holon-backend
[FFI-DIAG] exporter_type=stdout,otlp  <-- Default includes otlp
[FFI-DIAG] Creating OTLP trace exporter...
<--- HANG - NO MORE OUTPUT --->
```

## Code Path Analysis

### With OTLP (Default)

```
init_render_engine()
  └── init_opentelemetry()
        └── exporter_type.contains("otlp") == true
              └── SpanExporter::builder().with_http().build()
                    └── reqwest::Client::new()
                          └── [POTENTIAL DEADLOCK]
                                └── TLS setup / DNS resolution
                                      └── Error? → backtrace::capture()
                                            └── Symbol resolution
                                                  └── DWARF parsing locks
```

### With Stdout (Bypass)

```
init_render_engine()
  └── init_opentelemetry()
        └── exporter_type.contains("otlp") == false
              └── opentelemetry_stdout::SpanExporter::default()
                    └── [No HTTP, no locks, no deadlock potential]
```

## Environment Variables

| Variable | Effect | Test Usage |
|----------|--------|------------|
| `RUST_BACKTRACE=full` | Captures full backtraces | Keep set to reproduce issue |
| `OTEL_TRACES_EXPORTER=stdout` | Bypasses OTLP HTTP | Set to test bypass hypothesis |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector URL | Test unreachable endpoints |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | Connection timeout in ms | Test if timeout helps |

## Recommended Fixes (If OTLP Confirmed)

### Option 1: Lazy OTLP Initialization (Recommended)

Move OTLP HTTP client creation to a background task after app starts:

```rust
// Start with stdout exporter
global::set_tracer_provider(stdout_provider);

// After runApp(), spawn OTLP setup
tokio::spawn(async move {
    if let Ok(otlp_exporter) = create_otlp_exporter().await {
        global::set_tracer_provider(otlp_provider);
    }
});
```

### Option 2: Disable OTLP in Debug Mode

```rust
let exporter_type = if env::var("RUST_BACKTRACE").is_ok() {
    // Avoid HTTP client when backtrace is enabled
    "stdout".to_string()
} else {
    env::var("OTEL_TRACES_EXPORTER").unwrap_or_else(|_| "stdout,otlp".to_string())
};
```

### Option 3: Add Explicit Timeout

Ensure HTTP client has reasonable connection timeout:

```rust
let http_client = reqwest::Client::builder()
    .connect_timeout(Duration::from_secs(5))
    .timeout(Duration::from_secs(10))
    .build()?;
```

### Option 4: Disable Backtrace During HTTP Init

Temporarily disable RUST_BACKTRACE during HTTP client creation:

```rust
let _guard = BacktraceDisabler::new();  // Clears RUST_BACKTRACE temporarily
let client = create_http_client();
drop(_guard);  // Re-enables RUST_BACKTRACE
```

## Verification Checklist

- [ ] Run `test_otlp_bypass.sh` - verify bypass resolves issue
- [ ] Run `test_otlp_network_timeout.sh` - verify network timeout behavior
- [ ] Document which test confirms/contradicts hypothesis
- [ ] If confirmed, document recommended fix approach

## Related Files

- `frontends/flutter/rust/src/api/ffi_bridge.rs` - OTLP initialization code
- `frontends/flutter/scripts/test_otlp_bypass.sh` - Bypass test script
- `frontends/flutter/scripts/test_otlp_network_timeout.sh` - Network timeout tests
- `frontends/flutter/lib/main.dart` - Flutter initialization sequence
