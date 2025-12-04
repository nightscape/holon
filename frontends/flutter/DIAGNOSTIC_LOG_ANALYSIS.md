# Diagnostic Log Analysis: Grey Window Investigation

**Subtask 2-1: Analyze diagnostic logs to identify exact blocking point**

Generated: 2026-01-23

## Executive Summary

Based on code analysis of the diagnostic markers in `main.dart` and `ffi_bridge.rs`, the **most likely blocking point** is:

```
[DIAG 30] Before ffi.initRenderEngine() - THIS IS THE CRITICAL RUST CALL
  └── [FFI-DIAG] Creating OTLP trace exporter...
      └── SpanExporter::builder().with_http().build()  ← SUSPECTED DEADLOCK
```

The `with_http().build()` call creates an HTTP client (reqwest/hyper) which involves:
1. Tokio async runtime interaction
2. TLS library initialization
3. Connection pool setup

When `RUST_BACKTRACE=full` is set, any error condition (even handled errors) triggers symbol resolution, which can deadlock with async runtime operations.

---

## Initialization Sequence Analysis

### Dart Side (main.dart)

| DIAG # | Operation | Risk Level |
|--------|-----------|------------|
| 1 | Starting runZonedGuarded async block | Low |
| 2-3 | WidgetsFlutterBinding.ensureInitialized() | Low |
| 4-7 | LoggingService.initialize() + flush | Low |
| 8-9 | MCPToolkitBinding initialization | Low |
| 10-11 | McpUiAutomation.initialize() | Low |
| 12-14 | SharedPreferences.getInstance() | Low |
| 15-17 | frb.RustLib.init() | Medium |
| 18-29 | Config/path resolution | Low |
| **30-31** | **ffi.initRenderEngine()** | **HIGH - CRITICAL** |
| 32 | Store engine in global | Low |
| 33-35 | ThemeLoader.loadAllThemes() | Low |
| **36-37** | **runApp()** | Milestone |
| 38-42 | doWhenWindowReady() | Low |
| 43 | INITIALIZATION COMPLETE | Success |

### Rust Side (ffi_bridge.rs - init_render_engine)

| FFI-DIAG Marker | Operation | Risk Level |
|-----------------|-----------|------------|
| init_render_engine() ENTER | Function entry | Low |
| Calling init_opentelemetry()... | Start OTEL setup | Medium |
| init_opentelemetry() ENTER | Function entry | Medium |
| RUST_BACKTRACE={value} | Env check (diagnostic) | - |
| Creating OpenTelemetry Resource... | Resource builder | Low |
| **Creating OTLP trace exporter...** | **HTTP client init** | **HIGH** |
| OTLP trace exporter created | Exporter ready | Success |
| **Creating OTLP log exporter...** | **HTTP client init** | **HIGH** |
| OTLP log exporter created | Exporter ready | Success |
| Creating SdkTracerProvider... | Provider setup | Medium |
| Setting global tracer provider... | Global state | Medium |
| Creating OpenTelemetryLayer... | Tracing bridge | Low |
| Setting global default subscriber... | Subscriber setup | Low |
| init_opentelemetry() EXIT | Function complete | Success |
| holon::di::create_backend_engine()... | DI + DB init | Medium |
| Engine stored in GLOBAL_ENGINE | Singleton setup | Success |
| init_render_engine() EXIT | Complete | Success |

---

## Blocking Point Analysis

### Most Likely Scenario: OTLP HTTP Client Initialization

**Code Path:**
```rust
// ffi_bridge.rs lines 113-118
eprintln!("[FFI-DIAG] Creating OTLP trace exporter...");
let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
    .with_http()  // Creates reqwest HTTP client
    .with_endpoint(traces_endpoint)
    .build()?;    // ← POTENTIAL DEADLOCK HERE
eprintln!("[FFI-DIAG] OTLP trace exporter created");
```

**Why This Blocks with RUST_BACKTRACE=full:**

1. **HTTP Client Creation (reqwest):**
   - `with_http()` internally creates a `reqwest::Client`
   - reqwest uses hyper + tokio for async HTTP
   - Creates TLS configuration (native-tls or rustls)

2. **Symbol Resolution Trigger:**
   - During HTTP client setup, various internal operations may emit tracing events
   - If any error occurs (e.g., TLS library warning, connection pool limit)
   - With `RUST_BACKTRACE=full`, Rust captures full backtraces

3. **Deadlock Mechanism:**
   - Backtrace capture requires symbol resolution (reading DWARF info)
   - On macOS, symbol resolution involves `dsymutil` and system libraries
   - This may acquire locks that conflict with tokio's async runtime
   - The async runtime may be blocked waiting for I/O completion
   - Result: Mutual deadlock

### Expected Log Pattern When Blocked

```
[DIAG 29] Config built with 2 keys: ["TODOIST_API_KEY", "ORGMODE_ROOT_DIRECTORY"]
[DIAG 30] Before ffi.initRenderEngine() - THIS IS THE CRITICAL RUST CALL
[FFI-DIAG] ========================================
[FFI-DIAG] init_render_engine() ENTER
[FFI-DIAG] db_path=/path/to/holon.db
[FFI-DIAG] config keys: ["TODOIST_API_KEY", "ORGMODE_ROOT_DIRECTORY"]
[FFI-DIAG] ========================================
[FFI-DIAG] Calling init_opentelemetry()...
[FFI-DIAG] init_opentelemetry() ENTER
[FFI-DIAG] RUST_BACKTRACE=full
[FFI-DIAG] service_name=holon-backend
[FFI-DIAG] exporter_type=stdout,otlp
[FFI-DIAG] Creating OpenTelemetry Resource...
[FFI-DIAG] OpenTelemetry Resource created
[FFI-DIAG] Creating OTLP trace exporter...
<--- HANG HERE - no more output --->
```

If the last message is `Creating OTLP trace exporter...` without `OTLP trace exporter created` following, this confirms the deadlock hypothesis.

---

## Alternative Blocking Scenarios

### Scenario 2: Log Exporter Creation
Same mechanism as trace exporter, but occurs later:
```
[FFI-DIAG] Creating OTLP log exporter...
<--- HANG HERE --->
```

### Scenario 3: Tracing Subscriber Setup
Less likely, but possible global state deadlock:
```
[FFI-DIAG] Setting global default subscriber...
<--- HANG HERE --->
```

### Scenario 4: Backend Engine DI Setup
If OpenTelemetry completes but engine fails:
```
[FFI-DIAG] init_opentelemetry() EXIT (success)
[FFI-DIAG] Calling holon::di::create_backend_engine()...
<--- HANG HERE --->
```

---

## Verification Tests

### Test 1: Bypass OTLP (Confirm HTTP Client is Issue)
```bash
RUST_BACKTRACE=full OTEL_TRACES_EXPORTER=stdout flutter run -d macos
```
If app works → Confirms OTLP HTTP client is the blocking point.

### Test 2: Disable Backtrace (Control Test)
```bash
flutter run -d macos  # No RUST_BACKTRACE
```
If app works → Confirms RUST_BACKTRACE=full is required to trigger the issue.

### Test 3: Different Backtrace Levels
```bash
RUST_BACKTRACE=1 flutter run -d macos      # Short backtraces
RUST_BACKTRACE=0 flutter run -d macos      # Explicitly disabled
```
May help isolate if `full` (symbol resolution) is the specific trigger.

---

## Analysis Tools

### Log Analysis Script
```bash
./frontends/flutter/scripts/analyze_init_logs.sh <logfile>
```

### Stack Trace Capture (While Hanging)
```bash
# Get process ID
ps aux | grep -i flutter | grep -v grep

# Capture stack traces
sample <pid> 5 -file stack_sample.txt

# Or use spindump for all processes
sudo spindump -file spindump_output.txt
```

---

## Next Steps (Subtask 2-2 and Beyond)

1. **Run reproduction test** with scripts in `frontends/flutter/scripts/`
2. **Capture stack traces** if app hangs to confirm thread state
3. **Test OTLP bypass** with `OTEL_TRACES_EXPORTER=stdout`
4. **Document root cause** once confirmed
5. **Check E2E test** (subtask-2-4) for production parity

---

## Appendix: Full Diagnostic Marker List

### main.dart Markers
```
[DIAG 1] Starting runZonedGuarded async block
[DIAG 2] Before WidgetsFlutterBinding.ensureInitialized()
[DIAG 3] After WidgetsFlutterBinding.ensureInitialized()
[DIAG 4] Before LoggingService.initialize()
[DIAG 5] After LoggingService.initialize()
[DIAG 6] Sending test log and flushing
[DIAG 7] After LoggingService.flush()
[DIAG 8] Before MCPToolkitBinding.instance.initialize()
[DIAG 9] After MCPToolkitBinding initialization
[DIAG 10] Before McpUiAutomation.initialize()
[DIAG 11] After McpUiAutomation.initialize()
[DIAG 12] Before SharedPreferences.getInstance()
[DIAG 13] After SharedPreferences.getInstance()
[DIAG 14] Theme mode loaded: {mode}
[DIAG 15] useMockBackend={value}
[DIAG 16] Before frb.RustLib.init()
[DIAG 17] After frb.RustLib.init()
[DIAG 18] Got todoist API key (length: {n})
[DIAG 19-24] macOS secure bookmarks handling
[DIAG 25-28] Database path resolution
[DIAG 29] Config built with {n} keys
[DIAG 30] Before ffi.initRenderEngine() - THIS IS THE CRITICAL RUST CALL
[DIAG 31] After ffi.initRenderEngine() - Rust engine created
[DIAG 32] Engine stored in global variable
[DIAG 33] Before ThemeLoader.loadAllThemes()
[DIAG 34] After ThemeLoader.loadAllThemes()
[DIAG 35] Initial colors resolved
[DIAG 36] ===== ABOUT TO CALL runApp() =====
[DIAG 37] runApp() returned - Flutter app is now running
[DIAG 38-42] Desktop window configuration
[DIAG 43] ===== INITIALIZATION COMPLETE =====
```

### ffi_bridge.rs Markers (init_opentelemetry)
```
[FFI-DIAG] init_opentelemetry() ENTER
[FFI-DIAG] RUST_BACKTRACE={value}
[FFI-DIAG] service_name={value}
[FFI-DIAG] exporter_type={value}
[FFI-DIAG] Creating OpenTelemetry Resource...
[FFI-DIAG] OpenTelemetry Resource created
[FFI-DIAG] Creating OTLP trace exporter...
[FFI-DIAG] OTLP trace exporter created
[FFI-DIAG] Creating OTLP log exporter...
[FFI-DIAG] OTLP log exporter created
[FFI-DIAG] Creating SdkTracerProvider...
[FFI-DIAG] SdkTracerProvider created
[FFI-DIAG] Setting global tracer provider...
[FFI-DIAG] Global tracer provider set
[FFI-DIAG] Creating SdkLoggerProvider...
[FFI-DIAG] SdkLoggerProvider created
[FFI-DIAG] Getting global tracer...
[FFI-DIAG] Global tracer obtained
[FFI-DIAG] Creating OpenTelemetryLayer...
[FFI-DIAG] OpenTelemetryLayer created
[FFI-DIAG] Creating log bridge...
[FFI-DIAG] Log bridge created
[FFI-DIAG] Building tracing subscriber...
[FFI-DIAG] Tracing subscriber built
[FFI-DIAG] Setting global default subscriber...
[FFI-DIAG] Global default subscriber set
[FFI-DIAG] init_opentelemetry() EXIT (success)
```

### ffi_bridge.rs Markers (init_render_engine)
```
[FFI-DIAG] ========================================
[FFI-DIAG] init_render_engine() ENTER
[FFI-DIAG] db_path={value}
[FFI-DIAG] config keys: {keys}
[FFI-DIAG] ========================================
[FFI-DIAG] Calling init_opentelemetry()...
[FFI-DIAG] init_opentelemetry() completed successfully
[FFI-DIAG] Calling holon::di::create_backend_engine()...
[FFI-DIAG] Inside DI configuration closure...
[FFI-DIAG] DI configuration closure complete
[FFI-DIAG] holon::di::create_backend_engine() completed successfully
[FFI-DIAG] Storing engine in GLOBAL_ENGINE singleton...
[FFI-DIAG] Engine stored in GLOBAL_ENGINE singleton
[FFI-DIAG] ========================================
[FFI-DIAG] init_render_engine() EXIT (success)
[FFI-DIAG] ========================================
```
