# Stack Trace Analysis Guide for Grey Window Investigation

**Subtask 2-2: Capture stack traces using macOS sample/spindump**

Generated: 2026-01-23

## Overview

This document explains how to capture and interpret stack traces from a hanging Flutter/Holon macOS application to identify the root cause of the grey window issue.

## Quick Start

```bash
# Terminal 1: Reproduce the issue
cd frontends/flutter
RUST_BACKTRACE=full flutter run -d macos

# Terminal 2: Capture stack traces (while grey window is showing)
./scripts/capture_stack_traces.sh
```

---

## Capture Methods

### Method 1: `sample` Command

The `sample` command captures periodic stack traces from a running process.

```bash
# Basic usage
sample <pid> 5 -file output.txt

# Find the process
pgrep -f 'holon|Flutter'
```

**Pros:**
- Captures multiple samples over time
- Shows call frequency (helps identify hot spots)
- No sudo required for own processes

**Output Format:**
```
Sampling process 12345 for 5 seconds with 1 millisecond of run time between samples
Thread_123
  100 main (in holon) + 0x1234
   100 flutter::xxx (in Flutter) + 0x5678
    75 tokio::runtime::park (in holon) + 0xabcd  ← Potential issue
     50 pthread_cond_wait (in libsystem_pthread.dylib)
```

### Method 2: `spindump` Command

The `spindump` command provides more detailed hang analysis.

```bash
# Basic usage (may require sudo)
sudo spindump <pid> -timeline 10 -file output.txt

# Alternative: Capture all processes
sudo spindump -file output.txt
```

**Pros:**
- Better for detecting hangs and freezes
- Timeline mode shows thread state over time
- Groups related threads

**Output Format:**
```
Process:         holon [12345]
Path:            /Users/.../holon.app/Contents/MacOS/holon
State:           Stalled

Thread 0x1234
  Priority: 47 (base 47, QoS: User Interactive)
  State: blocked on mutex 0xABCD held by thread 0x5678
```

---

## What to Look For

### 1. Main Thread Blocked on Mutex

**Symptom:** Grey window, no UI updates

**Stack Pattern:**
```
Thread 1 (main thread)
  pthread_mutex_lock + 0x10
  or
  __psynch_mutexwait + 0x14
```

**Interpretation:** Main thread is waiting for a lock. Find which thread holds it.

### 2. Tokio Async Runtime Parked

**Symptom:** Async operations not completing

**Stack Pattern:**
```
tokio::runtime::scheduler::current_thread::CoreGuard::park
tokio::park::thread::ParkThread::park
```

**Interpretation:** Tokio runtime is waiting for work. If another thread is supposed to provide work but is blocked, this indicates a deadlock.

### 3. HTTP/Network Operation Blocked

**Symptom:** Initialization hangs during network setup

**Stack Pattern:**
```
reqwest::blocking::client::Client::new
hyper::client::pool::Pool::checkout
tokio::net::TcpStream::connect
connect + 0x14
```

**Interpretation:** HTTP client creation is stuck. Could be:
- DNS resolution hanging
- TCP connection timing out
- TLS handshake deadlocked

### 4. Backtrace Symbol Resolution

**Symptom:** Hang when RUST_BACKTRACE=full only

**Stack Pattern:**
```
std::backtrace::Backtrace::capture
  gimli::read::cfi::...
  addr2line::...
  symbolic::...
  mach_vm_read_overwrite
```

**Interpretation:** Rust is capturing a backtrace and resolving symbols. On macOS, this involves:
1. Reading DWARF debug info
2. Using mach kernel APIs
3. Potentially acquiring locks that conflict with async runtime

### 5. OpenTelemetry Initialization

**Symptom:** Hang in OTLP exporter setup

**Stack Pattern:**
```
opentelemetry_otlp::exporter::http::HttpExporterBuilder::build
  reqwest::Client::new
    hyper::...
```

**Interpretation:** OpenTelemetry is creating an HTTP client for OTLP export. This is the primary hypothesis for the grey window issue.

---

## Expected Deadlock Pattern

Based on the investigation, we expect to see:

### Thread A: Main Flutter Thread
```
main
  Flutter::runApp
    dart::Invoke
      FFI::initRenderEngine  ← Rust call
        init_opentelemetry
          SpanExporter::builder().with_http().build()
            reqwest::Client::new
              tokio::runtime::Handle::block_on  ← Blocking on async
                <waiting for work from Thread B>
```

### Thread B: Tokio Async Worker
```
tokio::runtime::worker
  some_async_operation
    <error occurs>
      std::backtrace::Backtrace::capture  ← RUST_BACKTRACE=full
        gimli::read_cfi
          pthread_mutex_lock  ← Waiting for symbol resolution lock
            <deadlocked with Thread A>
```

---

## Analysis Checklist

Use this checklist when analyzing captured stack traces:

### Immediate Findings
- [ ] Total thread count
- [ ] Main thread state (running/blocked/waiting)
- [ ] Any threads explicitly marked as "blocked on mutex"

### Thread State Analysis
- [ ] Is main thread in `pthread_mutex_lock` or `__psynch_mutexwait`?
- [ ] Is there a tokio thread in `park` state?
- [ ] Are there threads in `mach_msg_trap` (waiting for kernel)?

### Keyword Search
- [ ] Search for `tokio` - async runtime frames
- [ ] Search for `backtrace` or `gimli` - symbol resolution
- [ ] Search for `reqwest` or `hyper` - HTTP client
- [ ] Search for `opentelemetry` or `otlp` - telemetry setup
- [ ] Search for `mutex` or `lock` - synchronization primitives

### Root Cause Indicators
- [ ] If `backtrace::capture` appears, RUST_BACKTRACE is relevant
- [ ] If `reqwest::Client::new` appears, HTTP client init is involved
- [ ] If multiple threads show `pthread_mutex_lock`, deadlock is likely

---

## Sample Analysis Commands

```bash
# Count threads
grep -c "Thread_" sample_output.txt

# Find mutex waits
grep -E "pthread_mutex|__psynch_" sample_output.txt

# Find tokio frames
grep -E "tokio::" sample_output.txt

# Find backtrace operations
grep -E "backtrace|gimli|addr2line" sample_output.txt

# Find HTTP/network operations
grep -E "reqwest|hyper|http|connect" sample_output.txt

# Find OpenTelemetry
grep -E "opentelemetry|otlp" sample_output.txt

# Find which thread is in which state
grep -B5 "pthread_mutex_lock\|tokio.*park\|mach_msg_trap" sample_output.txt
```

---

## Known macOS-Specific Issues

### dsymutil Lock Contention
On macOS, symbol resolution may use `dsymutil` which can acquire system-wide locks. When combined with async operations, this can cause deadlock.

### Mach VM Access
Backtrace capture on macOS uses `mach_vm_read_overwrite` to read memory. This can conflict with tokio's memory operations.

### SecureTransport vs OpenSSL
macOS native TLS (SecureTransport) has different threading behavior than OpenSSL/rustls. The HTTP client initialization may behave differently based on TLS backend.

---

## Reporting Findings

When documenting stack trace analysis:

1. **Capture timestamp and conditions**
   - When was trace captured
   - How long was app in grey window state
   - Environment variables (RUST_BACKTRACE, OTEL_* vars)

2. **Thread summary**
   - Total threads
   - Threads in blocked/waiting state
   - Main thread state

3. **Key stack frames**
   - Quote the specific frames showing the issue
   - Note any mutex/lock operations
   - Note async/await frames

4. **Conclusion**
   - Confirmed deadlock pattern (if any)
   - Which subsystem is involved
   - Recommended next steps

---

## Next Steps After Analysis

1. **If OTLP HTTP client is confirmed**
   → Test with `OTEL_TRACES_EXPORTER=stdout` to bypass OTLP

2. **If backtrace symbol resolution is confirmed**
   → Test with `RUST_BACKTRACE=0` or `RUST_BACKTRACE=1`

3. **If tokio runtime deadlock is confirmed**
   → Consider spawning HTTP client setup on separate thread

4. **If no clear deadlock found**
   → Use lldb to attach and inspect further
