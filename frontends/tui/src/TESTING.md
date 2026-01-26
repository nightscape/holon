# TUI Testing Guide

This document describes the testing infrastructure for TUI applications based on r3bl's approach.

## Overview

Testing TUI applications requires actual terminal interaction, which is challenging in automated test environments. The r3bl library provides a sophisticated PTY (pseudo-terminal) based testing infrastructure that enables automated testing of terminal operations.

## PTY-Based Integration Testing

### Architecture

The testing infrastructure uses a **master/slave process architecture** with PTY pairs:

```text
┌─────────────────────────────────────────────────────────────┐
│ Test Function (entry point)                                 │
│  - Macro detects role via environment variable              │
│  - Routes to master or slave function                       │
│  - Skips in CI environments automatically                   │
└────────────┬────────────────────────────────┬───────────────┘
             │                                │
      Master Path                      Slave Path
             │                                │
             ▼                                ▼
┌────────────────────────┐    ┌──────────────────────────────┐
│ Macro: PTY Setup       │    │ Slave Function               │
│ - Creates PTY pair     │    │ - Enable raw mode (if needed)│
│ - Spawns slave         │────▶ - Execute test logic         │
│ - Passes to master fn  │    │ - Report via stdout          │
└────────────┬───────────┘    └──────────────┬───────────────┘
             │                               │ PTY stdout
             ▼                               │
┌────────────────────────┐                   │
│ Master Function        │                   │
│ - Receives pty_pair    │                   │
│ - Receives child       │                   │
│ - Reads results        │◀───────────────── ┘
│ - Verifies assertions  │
│ - Waits for child      │
└────────────────────────┘
```

### The `generate_pty_test!` Macro

Located in: `r3bl_tui/src/core/test_fixtures/pty_test_fixtures/generate_pty_test.rs`

This macro handles the boilerplate for PTY-based integration tests:

1. **CI detection**: Automatically skips tests in CI environments
2. **Process routing**: Routes to master or slave code based on environment variable
3. **PTY setup**: Creates PTY pair and spawns slave process automatically
4. **Debug output**: Prints diagnostic messages for troubleshooting

### Key Features

- **Automatic CI detection**: Tests requiring interactive terminals are skipped in CI
- **Process isolation**: Uses `R3BL_PTY_TEST_SLAVE` environment variable to route execution
- **Standard terminal size**: Creates 24x80 PTY pairs
- **Dependency injection**: Macro passes PTY resources to test functions
- **Merged streams**: stdout and stderr are merged in PTY (use content-based filtering)

## Writing PTY Tests

### Basic Structure

```rust
use crate::{RawModeGuard, generate_pty_test};
use std::io::{BufRead, BufReader, Write};

generate_pty_test! {
    /// Test description
    test_fn: test_name,
    master: master_function,
    slave: slave_function
}

fn master_function(
    pty_pair: portable_pty::PtyPair,
    mut child: Box<dyn portable_pty::Child + Send + Sync>,
) {
    // 1. Get reader from PTY
    let reader = pty_pair.master.try_clone_reader().expect("Failed to get reader");
    let mut buf_reader = BufReader::new(reader);

    // 2. Read and verify slave output
    let mut line = String::new();
    buf_reader.read_line(&mut line).expect("Failed to read");

    // 3. Assert expectations
    assert!(line.contains("EXPECTED_OUTPUT"));

    // 4. Wait for slave to exit
    child.wait().expect("Failed to wait for child");
}

fn slave_function() -> ! {
    // 1. Print start marker
    println!("SLAVE_STARTING");
    std::io::stdout().flush().expect("Failed to flush");

    // 2. Perform terminal operations
    let _guard = RawModeGuard::new().expect("Failed to enable raw mode");

    // 3. Report results
    println!("SUCCESS: Test passed");
    std::io::stdout().flush().expect("Failed to flush");

    // 4. CRITICAL: Exit to prevent test recursion
    std::process::exit(0);
}
```

### Master Function Responsibilities

The master function receives:
- `pty_pair: portable_pty::PtyPair` - The PTY pair for communication
- `child: Box<dyn portable_pty::Child + Send + Sync>` - The spawned slave process

It should:
1. Create a reader: `pty_pair.master.try_clone_reader()`
2. Optionally create a writer: `pty_pair.master.take_writer()`
3. Read slave output and verify assertions
4. Wait for the child process: `child.wait()`

### Slave Function Requirements

The slave function **MUST**:
1. Print status messages to stdout (for master verification)
2. Flush stdout after each message
3. Call `std::process::exit(0)` before returning (prevents test recursion)
4. Never return normally (signature: `-> !`)

### PTY Stream Behavior

**Important**: In a PTY, stdout and stderr are **merged into a single stream**:

```rust
// Slave code (both go to the same stream)
println!("Protocol message");      // Use println!, not eprintln!
println!("Debug: Event occurred"); // Also println!

// Master code (filter by content, not stream)
if line.starts_with("Protocol:") {
    // Handle protocol message
} else if line.starts_with("Debug:") {
    // Skip debug message
}
```

## Test Coverage Areas

The r3bl library demonstrates comprehensive test coverage across:

### 1. Terminal Raw Mode
- `integration_tests/test_basic_enable_disable.rs` - Basic raw mode lifecycle
- `integration_tests/test_flag_verification.rs` - Terminal flag verification
- `integration_tests/test_input_behavior.rs` - Raw mode input behavior
- `integration_tests/test_multiple_cycles.rs` - Multiple enable/disable cycles

### 2. VT-100 ANSI Conformance (20+ test files)
- Character operations (text rendering)
- Control operations (escape sequences)
- Cursor operations (positioning, movement)
- DSR operations (device status reports)
- SGR operations (text styling)
- Scroll operations
- Margin operations
- Mode operations
- OSC operations

### 3. Input Parsing
- Keyboard modifiers (Ctrl, Alt, Shift combinations)
- Mouse events (clicks, drags, scrolls)
- Bracketed paste mode
- UTF-8 text input
- New keyboard protocol features
- Terminal events

### 4. Editor Components
- Text editing operations
- Cursor movement
- Scrolling behavior

### 5. Layout and Rendering
- Flexbox layout
- Surface composition
- 2-column layouts (simple and complex)

### 6. Markdown Parsing
- Custom parser tests
- Snapshot tests
- Benchmark tests

### 7. Performance Benchmarks
- Render operation benchmarks
- Pixel char performance
- Vec vs SmallVec comparisons

## Running Tests

### Run All Tests
```bash
cd /path/to/r3bl-open-core
cargo test -p r3bl_tui
```

### Run Specific Test
```bash
cargo test -p r3bl_tui --lib test_raw_mode_pty -- --nocapture
```

### Run with Logging
```bash
RUST_LOG=debug cargo test -p r3bl_tui -- --nocapture
```

### Watch Tests (if using cargo-watch)
```bash
cargo watch -x "test -p r3bl_tui"
```

## CI Considerations

PTY tests automatically skip in CI environments via `is_ci::cached()`:

```rust
if pty_slave_env_var.is_err() && is_ci::cached() {
    println!("⏭️  Skipped in CI (requires interactive terminal)");
    return;
}
```

This ensures tests don't fail in GitHub Actions or other CI systems that lack proper PTY support.

## Common Patterns

### Timeout Pattern
```rust
use std::time::{Duration, Instant};

let start = Instant::now();
while start.elapsed() < Duration::from_secs(5) {
    match buf_reader.read_line(&mut line) {
        Ok(0) => break, // EOF
        Ok(_) => {
            // Process line
            if line.contains("SUCCESS") {
                test_passed = true;
                break;
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            std::thread::sleep(Duration::from_millis(10));
        }
        Err(e) => panic!("Read error: {e}"),
    }
}
```

### State Verification Pattern
```rust
// Get state BEFORE operation
let before_state = get_terminal_state();

// Perform operation
let _guard = enable_feature();

// Get state AFTER operation
let after_state = get_terminal_state();

// Verify state changed
assert_ne!(before_state, after_state, "State should have changed");
```

## Debugging Tips

1. **Use `-- --nocapture`** to see all output:
   ```bash
   cargo test test_name -- --nocapture
   ```

2. **Check environment variable**:
   ```rust
   eprintln!("🔍 ENV: {:?}", std::env::var("R3BL_PTY_TEST_SLAVE"));
   ```

3. **Flush output immediately**:
   ```rust
   println!("Debug message");
   std::io::stdout().flush().expect("Failed to flush");
   ```

4. **Add timeouts** to prevent hanging tests

5. **Use descriptive markers** in output:
   ```rust
   println!("SUCCESS: Test passed");
   println!("FAILED: Error condition");
   ```

## References

- r3bl_tui source: `/Users/martin/Workspaces/rust/r3bl-open-core/tui`
- PTY test macro: `tui/src/core/test_fixtures/pty_test_fixtures/generate_pty_test.rs`
- Integration tests: `tui/src/core/ansi/terminal_raw_mode/integration_tests/`
- VT-100 tests: `tui/src/core/ansi/vt_100_pty_output_parser/vt_100_pty_output_conformance_tests/`

## Best Practices

1. **Always exit slave processes** with `std::process::exit(0)`
2. **Use println! not eprintln!** in slave (streams are merged)
3. **Flush stdout** after important messages
4. **Add timeouts** to prevent infinite waits
5. **Use descriptive markers** for master/slave communication
6. **Test both success and failure paths**
7. **Verify state changes**, don't just check no errors occurred
8. **Keep tests focused** on a single concern

## Limitations

- PTY tests require interactive terminal support (skipped in most CI)
- Terminal emulator behavior can vary across platforms
- Some terminal features may not be fully supported in PTYs
- Race conditions possible with timing-sensitive operations
- Wide emoji support is limited (display width vs string index mismatch)
