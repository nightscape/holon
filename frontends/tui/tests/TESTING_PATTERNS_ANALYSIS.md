# Testing Patterns Analysis: R3BL vs holon

## Executive Summary

R3BL has a **sophisticated, production-grade testing infrastructure** centered on PTY-based integration tests with extensive testing utilities. holon has adopted some of these patterns but is still developing its approach. Both projects prioritize integration testing over unit tests, recognizing that TUI components need real terminal interaction verification.

---

## 1. TEST FILE ORGANIZATION & STRUCTURE

### R3BL Approach
**Highly modular fixture organization:**
- Fixtures organized into categorical subdirectories:
  - `/test_fixtures/pty_test_fixtures/` - PTY infrastructure (4 files, 927 LOC)
  - `/test_fixtures/input_device_fixtures/` - Input mocking (4 files)
  - `/test_fixtures/output_device_fixtures/` - Output mocking (2 files)
  - `/test_fixtures/tcp_stream_fixtures/` - Network test support (2 files)

- Tests embedded in source files with `#[test]` and `#[tokio::test]` annotations
- Integration tests in `integration_tests/` subdirectories alongside implementation
- **18+ VT-100 conformance tests** organized by operation type (CSI, OSC, SGR, cursor, etc.)

**File structure pattern:**
```
src/component/
├── lib.rs
├── implementation.rs
├── tests.rs                      # Unit tests
└── integration_tests/
    ├── mod.rs
    ├── test_case_1.rs
    └── test_case_2.rs
```

### holon Approach
**Test isolation in separate directory:**
- All tests in `tests/` directory at crate root
- Tests separated from source code
- Supporting infrastructure in `tests/pty_support/` subdirectory:
  - `mod.rs` - PtySession wrapper
  - `page_objects.rs` - PageObject implementations

**File structure pattern:**
```
crate/
├── src/
│   └── ...
└── tests/
    ├── navigation_test.rs
    ├── pty_infrastructure_test.rs
    └── pty_support/
        ├── mod.rs
        └── page_objects.rs
```

**Recommendation:** Adopt R3BL's categorical fixture organization if testing scope expands. Currently, holon's centralized approach is manageable but could become unwieldy with many tests.

---

## 2. TESTING PATTERNS FOR TUI COMPONENTS

### R3BL Patterns

#### A. PTY-Based Integration Testing (Macro-Driven)
- Uses `generate_pty_test!` macro - a **sophisticated codegen utility** (198 LOC)
- Handles boilerplate: PTY setup, master/slave coordination, CI detection
- Automatically spawns test as subprocess in slave mode
- Key features:
  - **Environment variable routing** (`R3BL_PTY_TEST_SLAVE`)
  - **CI auto-skip** via `is_ci::cached()`
  - **Dependency injection** - macro creates PTY, user provides master function
  - **Flexible resource management** - master function controls what it needs

**Example structure:**
```rust
generate_pty_test! {
    test_fn: test_raw_mode_input_behavior,
    master: pty_master_entry_point,      // (pty_pair, child) -> ()
    slave: pty_slave_entry_point          // () -> !  (never returns)
}
```

#### B. Mock Device Pattern
- **MockInputDevice** - yields synthetic events from vector
- **StdoutMock** - Arc<Mutex<Vec<u8>>> for capturing output
- Provides convenience functions:
  - `get_copy_of_buffer_as_string()`
  - `get_copy_of_buffer_as_string_strip_ansi()`
- Used with trait-based dependency injection

**Observation:** R3BL tests both "real terminal" (PTY) and "mock device" paths.

### holon Patterns

#### A. Manual PTY Coordination
- Manual implementation instead of macro
- Duplicates PTY setup boilerplate across tests
- Same master/slave pattern but less abstracted
- Includes custom `PtySession` wrapper for convenience

#### B. PageObject Pattern
- Implemented `MainPage` and `Screen` abstractions
- Provides high-level actions: `navigate_down()`, `toggle_completion()`
- **Problem:** PageObject specific to one view; would need duplication for other screens
- **Strength:** Clear test intent vs low-level key sequences

---

## 3. USE OF TEST UTILITIES & HELPERS

### R3BL Infrastructure

**1. Deadline & Async Debounced Deadline**
- `Deadline` (233 LOC) - Simple timeout enforcement for sync operations
- `AsyncDebouncedDeadline` (285 LOC) - Event debouncing for async contexts
- **Key insight:** Different patterns for different scenarios (timeout vs debounce)

**2. Input Stream Generation**
- `gen_input_stream()` - Create async stream from event vector
- `gen_input_stream_with_delay()` - Add timing between events
- Enables realistic input simulation (e.g., typing delays)

**3. Output Capture**
- Strip ANSI escapes with `strip_ansi_escapes` crate
- Mock implements `std::io::Write` trait
- Arc<Mutex> allows cloning and sharing output buffer

**4. Test Isolation**
- `is_ci::cached()` - Detect CI environment
- Auto-skip PTY tests in CI (cannot allocate controlling terminal)
- Markers: `#[ignore]` on PTY tests, `#[test]` on unit tests

### holon Infrastructure

**1. PtySession Wrapper**
- Encapsulates reader/writer/child process
- Methods: `send()`, `send_key()`, `read_line()`, `wait_for()`, `drain_output()`
- Good progress but could benefit from timeout utilities

**2. Screen Parsing**
- Basic ANSI stripping implementation (custom, ~30 LOC)
- Simple `contains()`, `get_line()`, `find_text()`, `count_occurrences()`
- Functional but brittle for complex layouts

**3. Key Sequences**
- Enum-based key definitions (Key::Up, Key::Down, etc.)
- `to_sequence()` returns ANSI codes
- **Missing:** Support for timing/delays between keys

**Recommendations:**
1. **Import Deadline utilities** from R3BL pattern - standardize timeout handling
2. **Add input delay support** - Allow simulating realistic typing speed
3. **Enhance ANSI stripping** - Use `strip_ansi_escapes` crate for reliability
4. **Expand Screen parsing** - Add pattern matching for visual regression testing

---

## 4. INTEGRATION VS UNIT TEST APPROACHES

### R3BL Strategy
- **Hybrid approach:**
  - Unit tests embedded in source files test individual functions
  - Integration tests in `integration_tests/` subdirectories test interactions
  - Mock devices for mid-level testing (without full PTY)
  - PTY tests for end-to-end verification

**Example breakdown (terminal_raw_mode):**
1. Unit tests: Flag verification, immediate checks
2. Mock tests: Input/output device behavior without PTY
3. Integration tests: Full PTY with subprocess (3 test types)

- **Clear separation:** Each test type serves a purpose
- **Pyramid structure:** Many unit tests, fewer integration tests (inverted)
- **Actually runs:** Even integration tests marked `#[ignore]` CAN run locally

### holon Strategy
- **Mostly integration-focused:**
  - 2 main PTY integration tests (navigation_test, pty_infrastructure_test)
  - Both marked `#[ignore]` (not run by default)
  - Unit tests for utilities (`test_screen_parsing`, `test_key_sequences`)
  - **Dependency:** Full TUI app required for meaningful tests

**Issue:** Missing mid-level tests. You need either:
1. Unit tests for individual components (BlockList rendering, state management)
2. Mock integration tests (no PTY, but structured test scenarios)
3. Or extensive e2e tests (expensive, fragile)

**Recommendation:** Create a testing pyramid:
```
        /\              End-to-end PTY tests (1-2 tests)
       /  \             Integration tests with mocks (5-10 tests)
      /    \            Unit tests for components (20+ tests)
     /______\
```

---

## 5. PTY & TERMINAL-RELATED TESTING PATTERNS

### R3BL PTY Macro Architecture

**Key insight:** The `generate_pty_test!` macro solves the "test recursion problem":
- Normal test function would run in child process too
- Macro uses environment variable to detect role
- Master: Creates PTY, spawns self as slave
- Slave: Receives env var, runs test logic, exits with `std::process::exit(0)`
- **Non-recursive:** Slave never creates PTY (has env var set)

**Master receives:**
```rust
pty_pair: portable_pty::PtyPair
  - master: reader/writer for controlling terminal
child: Box<dyn portable_pty::Child>
  - wait() to wait for slave exit
  - stdout/stderr merged in PTY (not separate)
```

**Important:** PTY merges stdout/stderr - use content-based filtering, not stream type.

### holon PTY Implementation
- Manual environment variable checking (duplicated per test)
- PtySession wrapper over pty_pair
- **Strengths:**
  - Direct, understandable flow
  - No macro magic to debug
- **Weaknesses:**
  - Boilerplate duplication
  - No timeout utilities
  - No CDC (Change Data Capture) stream support built-in

### Terminal Output Verification

**R3BL approach:**
1. Master sends input via `writer.write_all()`
2. Slave reads byte-by-byte via `read_exact()`
3. Slave reports back via println!("RECEIVED: 0x{:02x}")
4. Master parses protocol messages

**holon approach:**
1. Master sends via `session.send_key(Key::Down)`
2. Master reads entire lines via `read_line()`
3. Uses `wait_for()` to find expected text
4. PageObject provides high-level assertions

**R3BL is more granular** (byte-level), **holon is more abstracted** (line-level).
- R3BL good for: Terminal protocol verification
- holon good for: Application workflow testing

---

## 6. MOCK/FIXTURE PATTERNS

### R3BL Fixtures

**1. StdoutMock (80 LOC)**
```rust
pub struct StdoutMock {
    pub buffer: Arc<StdMutex<InlineVec<u8>>>,
}
// Implements: Write, Clone (via Arc), Debug
// Methods: get_copy_of_buffer(), get_copy_of_buffer_as_string_strip_ansi()
```
- Thread-safe via Arc<Mutex>
- Integrated ANSI stripping
- Zero-copy cloning (Arc)
- Embedded tests in same file

**2. MockInputDevice (60 LOC)**
```rust
pub struct MockInputDevice {
    resource: PinnedInputStream<CrosstermEventResult>,
}
// Implements: InputDeviceExt { next_input_event() }
// Constructor: new() with event vector, new_with_delay() with timing
```
- Async-aware (returns `Option<InputEvent>`)
- Filters invalid events during iteration
- Optional delays simulate user speed

**3. Input Device Extension (30 LOC)**
```rust
pub trait InputDeviceExtMock {
    fn new_mock(vec) -> InputDevice;
}
```
- Backward compatible
- Trait-based vs concrete type

### holon Fixtures

**1. PtySession (120 LOC)**
- Wrapper around PTY pair and child process
- Methods: `send()`, `read_line()`, `wait_for()`, `drain_output()`
- Error types: Timeout, Eof, Io, NotFound
- **No timeout enforcement** - relies on `read_line()` timeout parameter

**2. Screen (80 LOC)**
- Parses output by stripping ANSI codes (custom impl)
- Methods: `contains()`, `get_line()`, `find_text()`, `count_occurrences()`
- **Not extensible** - fixed to line-based operations
- No visual layout representation (could add grid)

**3. MainPage (not fully shown)**
- PageObject pattern for main outliner view
- Methods: `navigate_down()`, `navigate_up()`, `toggle_completion()`, etc.
- **Problem:** Specific to one view, would duplicate for others

**4. Key Enum (40 LOC)**
- Maps Key -> ANSI sequence string
- Supports: Arrows, modifiers (Ctrl), special keys
- **Missing:** Timing between key presses

### Comparison

| Aspect | R3BL | holon |
|--------|------|-----------------|
| **Mock Types** | Device-level (Input/Output) | Session-level (PTY wrapper) |
| **Concurrency** | Arc<Mutex> for sharing | Single-threaded assumptions |
| **Error Handling** | Via event stream (Result<Event>) | Via enum (PtyError) |
| **Reusability** | Traits for extension | Concrete structs |
| **Test Utilities** | Deadline, DebouncedDeadline | Simple timeout params |
| **ANSI Handling** | `strip_ansi_escapes` crate | Custom impl (~30 LOC) |

---

## SUMMARY: GAPS & RECOMMENDATIONS

### Gaps in holon

1. **Timeout enforcement**
   - Currently using Duration params in methods
   - No reusable Deadline pattern
   - **Risk:** Tests can hang indefinitely if message never arrives

2. **Input timing**
   - Key sequences sent instantly
   - **Risk:** App may not process fast enough, false negatives
   - **Need:** Optional delays between key presses

3. **Test isolation**
   - Manual master/slave coordination
   - **Risk:** Boilerplate duplication as more tests added
   - **Need:** Consider macro-based approach or helper module

4. **Mid-level testing**
   - No mock device tests (only full PTY)
   - **Risk:** Can't test without full app setup
   - **Need:** Unit tests for components, mock tests for interactions

5. **Output parsing**
   - Custom ANSI stripping (fragile)
   - **Risk:** Breaks with unusual escape sequences
   - **Need:** Use battle-tested `strip_ansi_escapes` crate

6. **PageObject generalization**
   - Currently MainPage only
   - **Risk:** Will need to duplicate for other views
   - **Need:** Generic PageObject base, view-specific extensions

### Concrete Improvements (Priority Order)

#### HIGH PRIORITY
1. **Add Deadline utility** (can copy from R3BL)
   - Enable proper test timeout enforcement
   - Prevent hanging tests
   - Add to `tests/pty_support/mod.rs`

2. **Switch to `strip_ansi_escapes` crate**
   - Replace custom implementation
   - Add dependency: `strip_ansi_escapes = "0.2"`
   - Improves reliability

3. **Add key timing support**
   - Extend Key enum with timing variant
   - Add `send_key_with_delay(key, delay)`
   - Allows realistic typing simulation

#### MEDIUM PRIORITY
4. **Create test macro or helper**
   - Reduce PTY setup boilerplate
   - Similar to R3BL's `generate_pty_test!`
   - Or simpler: helper function that returns PtySession

5. **Add unit tests for components**
   - Test BlockList rendering in isolation
   - Mock the render engine
   - Test state transitions
   - Create test pyramid (3:2:1 ratio)

6. **Add mock integration tests**
   - Test without full PTY
   - Use mock OutputDevice
   - Faster, more reliable than PTY tests

#### LOW PRIORITY
7. **Generalize PageObject**
   - Create `PageObject<T>` base
   - Implement `MainPage(PageObject<MainPageBehavior>)`
   - Enable `ListPage`, `DialogPage`, etc.

8. **Add visual regression testing**
   - Screenshot comparison
   - Reference rendering at specific state
   - Detect layout changes

---

## CODE EXAMPLES

### Adopting Deadline Pattern
```rust
// In tests/pty_support/mod.rs

use std::time::{Duration, Instant};

pub struct Deadline {
    expires_at: Instant,
}

impl Deadline {
    pub fn new(timeout: Duration) -> Self {
        Self {
            expires_at: Instant::now() + timeout,
        }
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

// Usage:
let deadline = Deadline::new(Duration::from_secs(5));
while !deadline.is_expired() {
    // Read output, check for expected text
}
```

### Adopting Key Timing
```rust
// Extend Key enum
pub enum Key {
    Char(char),
    Down,
    // ... existing variants
    WithDelay(Box<Key>, Duration),  // NEW
}

// Usage:
page.send_key(Key::WithDelay(
    Box::new(Key::Char('a')),
    Duration::from_millis(50)
));
```

### Adding Strip ANSI Crate
```toml
# In Cargo.toml
[dev-dependencies]
strip_ansi_escapes = "0.2"

# In page_objects.rs
let cleaned = strip_ansi_escapes::strip(&output);
```

---

## CONCLUSION

R3BL's testing infrastructure is **comprehensive, well-organized, and production-ready**. holon has made good progress adopting PTY-based integration testing and PageObject pattern, but should:

1. **Immediate:** Add Deadline + improve ANSI stripping (HIGH impact, LOW effort)
2. **Short-term:** Create test pyramid with unit + mock tests (HIGH impact, MEDIUM effort)
3. **Long-term:** Generalize PageObject + consider macro for PTY setup (MEDIUM impact, MEDIUM effort)

The improvements will make tests **more reliable**, **faster to develop**, and **easier to maintain** as the project scales.
