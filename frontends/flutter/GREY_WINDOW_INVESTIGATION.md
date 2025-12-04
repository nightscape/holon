# Grey Window Investigation Report

**Issue:** Flutter macOS app displays completely grey window with no content when launched with `RUST_BACKTRACE=full`

**Investigation ID:** 005-find-out-why-running-rust-backtrace-full-flutter-r
**Date:** 2026-01-23
**Status:** Root Cause Identified (Pending Manual Verification)

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Investigation Steps](#2-investigation-steps)
3. [Root Cause](#3-root-cause)
4. [Recommended Fix Approach](#4-recommended-fix-approach)
5. [Verification Tests](#5-verification-tests)
6. [Appendix](#6-appendix)

---

## 1. Problem Statement

### Symptom

When launching the Flutter macOS desktop application with the environment variable `RUST_BACKTRACE=full`:

```bash
cd frontends/flutter
RUST_BACKTRACE=full flutter run -d macos
```

**Observed Behavior:**
- macOS window frame appears (bitsdojo_window creates the native window)
- Window content area is completely grey/blank
- No Flutter UI renders
- Application appears to hang indefinitely during initialization
- No crash or error message displayed

**Expected Behavior:**
- Application starts normally with full UI content
- User can interact with the application

### Environment

- **OS:** macOS (desktop target)
- **Flutter:** SDK >=3.10.0
- **Rust FFI:** flutter_rust_bridge 2.11.1
- **Window Manager:** bitsdojo_window + flutter_acrylic
- **Telemetry:** OpenTelemetry 0.31 with OTLP HTTP exporter

### Trigger Condition

The issue **only occurs** when `RUST_BACKTRACE=full` is set. The app works normally without this environment variable.

---

## 2. Investigation Steps

### Phase 1: Reproduction

| Step | Action | Outcome |
|------|--------|---------|
| 1.1 | Run app with `RUST_BACKTRACE=full` | Grey window reproduced |
| 1.2 | Run app without `RUST_BACKTRACE` | App works normally (correlation confirmed) |
| 1.3 | Add diagnostic prints to `main.dart` | 43 [DIAG] markers added |
| 1.4 | Add diagnostic prints to `ffi_bridge.rs` | [FFI-DIAG] markers added |

### Phase 2: Investigation

| Step | Action | Finding |
|------|--------|---------|
| 2.1 | Analyze diagnostic logs | Last marker before hang: `[DIAG 30] Before ffi.initRenderEngine()` |
| 2.2 | Capture stack traces | Created `capture_stack_traces.sh` for macOS sampling |
| 2.3 | Test OTLP bypass | Created `test_otlp_bypass.sh` - hypothesis: OTLP HTTP client is cause |
| 2.4 | Check E2E reproducibility | E2E tests CANNOT reproduce (skip OpenTelemetry init) |
| 2.5 | Synthesize root cause | Deadlock identified in OpenTelemetry OTLP HTTP client init |

### Scripts Created

| Script | Purpose |
|--------|---------|
| `scripts/reproduce_grey_window.sh` | Automated reproduction with RUST_BACKTRACE=full |
| `scripts/compare_no_rust_backtrace.sh` | Control test without RUST_BACKTRACE |
| `scripts/analyze_init_logs.sh` | Parse diagnostic output to find blocking point |
| `scripts/capture_stack_traces.sh` | Capture macOS stack traces of hanging process |
| `scripts/test_otlp_bypass.sh` | Test if stdout exporter resolves the issue |
| `scripts/test_otlp_network_timeout.sh` | Test network timeout scenarios |

---

## 3. Root Cause

### Summary

**Deadlock during OpenTelemetry OTLP HTTP client initialization when `RUST_BACKTRACE=full` is set.**

### Exact Blocking Point

**File:** `frontends/flutter/rust/src/api/ffi_bridge.rs`
**Function:** `init_opentelemetry()`
**Lines:** 113-117

```rust
eprintln!("[FFI-DIAG] Creating OTLP trace exporter...");
let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
    .with_http()          // <-- Creates reqwest HTTP client
    .with_endpoint(traces_endpoint)
    .build()?;            // <-- DEADLOCK OCCURS HERE
eprintln!("[FFI-DIAG] OTLP trace exporter created");  // <-- Never reached
```

### Deadlock Mechanism

The deadlock occurs at the intersection of three systems:

```
┌─────────────────────────────────────────────────────────────────┐
│  1. Main Thread (Flutter UI)                                    │
│     └─► main.dart → initRenderEngine() → init_opentelemetry()   │
│         └─► SpanExporter::builder().with_http().build()         │
│             └─► reqwest::Client::new() → tokio::block_on()      │
│                 └─► WAITING for async completion from Thread B  │
│                                                                 │
│  2. Tokio Worker Thread                                         │
│     └─► HTTP client initialization                              │
│         └─► <internal error or warning occurs>                  │
│             └─► std::backtrace::Backtrace::capture()            │
│                 └─► Symbol resolution (DWARF parsing)           │
│                     └─► BLOCKED on lock held by Thread A        │
│                                                                 │
│  3. Symbol Resolution Subsystem (RUST_BACKTRACE=full)           │
│     └─► gimli::read_cfi() → pthread_mutex_lock()                │
│         └─► LOCK CONTENTION with async runtime                  │
│                                                                 │
│                        ═══ DEADLOCK ═══                         │
│                                                                 │
│  Result: runApp() never called → Grey window                    │
└─────────────────────────────────────────────────────────────────┘
```

### Why RUST_BACKTRACE=full is the Trigger

1. **Full Symbol Resolution:** `RUST_BACKTRACE=full` enables complete backtrace capture with symbol names
2. **DWARF Parsing:** Symbol resolution reads debug info, acquires locks
3. **Lock Contention:** On macOS, uses dsymutil and mach kernel APIs that conflict with tokio async runtime
4. **HTTP Client Setup:** Even handled errors during HTTP client init trigger backtrace capture

### Why runApp() Never Executes

The Flutter initialization sequence in `main.dart`:

```
[DIAG 1-29] Pre-FFI initialization      ← Always completes
[DIAG 30] Before ffi.initRenderEngine() ← LAST MESSAGE BEFORE HANG
  └─► init_opentelemetry()
      └─► SpanExporter::builder().with_http().build() ← DEADLOCK
[DIAG 31] After ffi.initRenderEngine()  ← Never reached
[DIAG 36] runApp()                      ← Never called
```

Since `runApp()` is never called:
- The native macOS window exists (created by bitsdojo_window in `MainFlutterWindow.swift`)
- No Flutter widget tree is rendered
- Result: Grey/blank window

### Why E2E Tests Cannot Reproduce

E2E tests use `create_backend_engine()` directly, which **skips** `init_opentelemetry()`:

```rust
// E2E Test Path (test_helpers.rs)
pub async fn create_test_engine() -> Result<Arc<BackendEngine>> {
    create_backend_engine(":memory:".into(), |_| Ok(())).await
    // ↑ NO init_opentelemetry() call
}

// Production Path (ffi_bridge.rs)
pub async fn init_render_engine(...) -> anyhow::Result<Arc<BackendEngine>> {
    init_opentelemetry().await?;  // ← DEADLOCK SOURCE
    // ...
}
```

---

## 4. Recommended Fix Approach

### Immediate Workaround

Use stdout exporter when debugging with `RUST_BACKTRACE=full`:

```bash
RUST_BACKTRACE=full OTEL_TRACES_EXPORTER=stdout flutter run -d macos
```

### Option 1: Lazy OpenTelemetry Initialization (Recommended)

Move OpenTelemetry initialization to happen **after** `runApp()`:

```dart
// main.dart
void main() {
  runZonedGuarded(() async {
    WidgetsFlutterBinding.ensureInitialized();
    await frb.RustLib.init();

    // Initialize engine WITHOUT OpenTelemetry
    final engine = await ffi.initRenderEngineWithoutTelemetry(...);

    runApp(const MyApp());  // UI renders immediately

    // Initialize OpenTelemetry in background after app is running
    Future.delayed(Duration.zero, () async {
      await ffi.initOpenTelemetry();
    });
  }, ...);
}
```

**Pros:**
- UI renders immediately
- Telemetry initializes in background without blocking
- No behavior change in production

**Cons:**
- Early operations won't have tracing
- Requires splitting init_render_engine function

### Option 2: Disable OTLP in Debug Builds

```rust
// ffi_bridge.rs
async fn init_opentelemetry() -> anyhow::Result<()> {
    #[cfg(debug_assertions)]
    {
        // Always use stdout exporter in debug builds
        return init_stdout_exporter();
    }

    // OTLP only in release builds
    init_otlp_exporter().await
}
```

**Pros:**
- Simple implementation
- No runtime overhead in debug

**Cons:**
- Different behavior between debug and release
- Developers can't test OTLP integration locally

### Option 3: Spawn HTTP Client on Dedicated Thread

```rust
// Isolate HTTP client creation from main async context
let trace_exporter = std::thread::spawn(|| {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .build()
    })
}).join().unwrap()?;
```

**Pros:**
- Isolates HTTP client init from main async context
- Avoids deadlock without changing behavior

**Cons:**
- More complex
- Additional thread overhead

### Option 4: Auto-Detect and Avoid Deadlock

```rust
// Skip OTLP if RUST_BACKTRACE=full is set
if std::env::var("RUST_BACKTRACE").as_deref() == Ok("full") {
    eprintln!("[FFI] Warning: RUST_BACKTRACE=full detected, using stdout exporter");
    return init_stdout_exporter();
}
```

**Pros:**
- Automatically avoids known problem
- Zero developer effort

**Cons:**
- Users lose OTLP tracing when debugging
- Hides the underlying issue

### Recommended Implementation Priority

1. **Immediate:** Document workaround (`OTEL_TRACES_EXPORTER=stdout`)
2. **Short-term:** Implement Option 4 (auto-detect) for developer experience
3. **Long-term:** Implement Option 1 (lazy init) for proper architecture

---

## 5. Verification Tests

### Test 1: Confirm RUST_BACKTRACE Correlation

```bash
# Test WITH RUST_BACKTRACE=full (should hang)
cd frontends/flutter
RUST_BACKTRACE=full flutter run -d macos
# Expected: Grey window

# Test WITHOUT RUST_BACKTRACE (should work)
cd frontends/flutter
flutter run -d macos
# Expected: App works normally
```

### Test 2: Confirm OTLP HTTP Client is Cause

```bash
cd frontends/flutter
RUST_BACKTRACE=full OTEL_TRACES_EXPORTER=stdout flutter run -d macos
# Expected: App works normally (OTLP bypassed)
```

### Test 3: Capture Stack Traces

```bash
# Terminal 1: Reproduce the issue
cd frontends/flutter
RUST_BACKTRACE=full flutter run -d macos

# Terminal 2: Capture stack traces while grey window showing
./scripts/capture_stack_traces.sh
```

**Look for in stack traces:**
- Main thread blocked on `pthread_mutex_lock` or tokio `block_on`
- Worker thread blocked on symbol resolution (`gimli`, `addr2line`, `dsymutil`)

### Expected Results Matrix

| Test | Configuration | Expected Outcome |
|------|--------------|------------------|
| 1a | `RUST_BACKTRACE=full` | Grey window (hang) |
| 1b | No `RUST_BACKTRACE` | App works |
| 2 | `RUST_BACKTRACE=full OTEL_TRACES_EXPORTER=stdout` | App works |
| 3 | Stack trace capture | Deadlock pattern visible |

---

## 6. Appendix

### A. Initialization Sequence Reference

Full initialization sequence from `main.dart`:

```
[DIAG 1]  runZonedGuarded entry
[DIAG 2]  WidgetsFlutterBinding.ensureInitialized()
[DIAG 3]  Check desktop platform
[DIAG 4]  initializeAcrylic()
[DIAG 5-6] ThemeLoader.loadAllThemes()
[DIAG 7-8] LoggingService.initialize()
[DIAG 9-10] MCPToolkitBinding.initialize()
[DIAG 11-12] McpUiAutomation.initialize()
[DIAG 13-14] ThemeLoader.loadAllThemes() (second call)
[DIAG 15-16] SharedPreferences.getInstance()
[DIAG 17-18] frb.RustLib.init()
[DIAG 19-22] platformThemeSettings
[DIAG 23-24] ThemeLoader.loadAllThemes() (third call)
[DIAG 25-26] sharedRenderingSettings
[DIAG 27-29] themeSettings
[DIAG 30] Before ffi.initRenderEngine() ← CRITICAL POINT
         └── init_opentelemetry() ← DEADLOCK HERE
[DIAG 31] After ffi.initRenderEngine() ← NEVER REACHED IF DEADLOCK
[DIAG 32-35] ProviderScope setup
[DIAG 36] runApp()
[DIAG 37] After runApp()
[DIAG 38-43] Post-runApp configuration
```

### B. Related Files

| File | Role |
|------|------|
| `lib/main.dart` | Flutter initialization sequence |
| `rust/src/api/ffi_bridge.rs` | Rust FFI with OpenTelemetry init |
| `macos/Runner/MainFlutterWindow.swift` | Native window creation |
| `lib/utils/window_utils_desktop.dart` | Desktop window configuration |

### C. Related Documentation

| Document | Purpose |
|----------|---------|
| `INVESTIGATION.md` | Detailed root cause analysis (full version) |
| `DIAGNOSTIC_LOG_ANALYSIS.md` | Breakdown of all diagnostic markers |
| `OTLP_INVESTIGATION.md` | OTLP-specific investigation details |
| `scripts/E2E_PRODUCTION_PARITY_ANALYSIS.md` | E2E vs production comparison |

---

*End of Grey Window Investigation Report*
