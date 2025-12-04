# Testing Improvements - Actionable Checklist

Quick reference for implementing testing improvements identified in the analysis.

## HIGH PRIORITY (Do First)

### 1. Add Deadline Utility to PtySession
**Impact:** Medium
**Effort:** Low
**Time:** ~30 minutes

- [ ] Create `/tests/pty_support/deadline.rs`
- [ ] Copy/adapt Deadline from R3BL:
  - `new(timeout: Duration)`
  - `is_expired() -> bool`
  - `has_time_remaining() -> bool`
  - Default 5s timeout
- [ ] Add unit tests for Deadline
- [ ] Update `/tests/pty_support/mod.rs` to export it
- [ ] Update PtySession::read_line() to use Deadline internally (optional)

**Reference:** /Users/martin/Workspaces/rust/r3bl-open-core/tui/src/core/test_fixtures/pty_test_fixtures/deadline.rs (233 LOC)

---

### 2. Switch ANSI Stripping to Crate
**Impact:** High
**Effort:** Low
**Time:** ~15 minutes

- [ ] Add to `Cargo.toml` dev-dependencies:
  ```toml
  strip_ansi_escapes = "0.2"
  ```
- [ ] Update `/tests/pty_support/page_objects.rs`:
  - Replace custom `strip_ansi_codes()` function with crate
  - Delete ~30 LOC of custom implementation
  - Import: `use strip_ansi_escapes::strip;`
- [ ] Test that Screen parsing still works
- [ ] Verify ANSI handling for edge cases

**Before:**
```rust
fn strip_ansi_codes(text: &str) -> String {
    // 30 lines of custom logic
}
```

**After:**
```rust
let cleaned = String::from_utf8(strip(text.as_bytes())).unwrap();
```

---

### 3. Add Key Timing Support
**Impact:** Medium
**Effort:** Low
**Time:** ~20 minutes

- [ ] Update `/tests/pty_support/mod.rs` Key enum:
  - Add variant: `pub enum Key { ... WithDelay(Box<Key>, Duration), }`
- [ ] Implement `to_sequence()` for WithDelay:
  - Send inner key's sequence
  - Sleep for specified duration
  - Return immediately
- [ ] Add `send_key_with_delay()` helper method on PtySession
- [ ] Update tests that need delays:
  - Remove manual `std::thread::sleep()` calls after `send_key()`
  - Replace with `send_key_with_delay()`

**Before:**
```rust
main_page.navigate_down()?;
std::thread::sleep(Duration::from_millis(200));
```

**After:**
```rust
main_page.navigate_down_with_delay(Duration::from_millis(200))?;
```

---

## MEDIUM PRIORITY (Do Next)

### 4. Create PTY Test Helper Module
**Impact:** Medium
**Effort:** Medium
**Time:** ~1 hour

- [ ] Create `/tests/pty_support/test_helper.rs`
- [ ] Add function: `fn create_pty_session() -> (PtySession, ...)`
  - Encapsulates common PTY setup code
  - Handles environment variable checking
  - Spawns test binary as slave
  - Returns ready PtySession
- [ ] Extract common patterns from:
  - `navigation_test.rs` lines 36-61
  - `pty_infrastructure_test.rs` lines 45-65
- [ ] Update both tests to use helper
- [ ] Reduce boilerplate by ~40 LOC per test

**Pattern:**
```rust
fn setup_pty(slave_fn: impl Fn() -> !) -> PtySession {
    const PTY_SLAVE_ENV_VAR: &str = "TUI_TEST_SLAVE";

    if std::env::var(PTY_SLAVE_ENV_VAR).is_ok() {
        slave_fn();  // Never returns
    }

    // ... common setup ...
    PtySession::new(pty_pair, child)
}
```

---

### 5. Add Component Unit Tests
**Impact:** High
**Effort:** Medium
**Time:** ~2-3 hours

- [ ] Identify components to test:
  - BlockList rendering (grid layout, cursor position)
  - State transitions (navigation, toggle, collapse)
  - Event handling (key mapping, action dispatch)
- [ ] Create test module per component:
  - `/src/components/block_list/tests.rs`
  - `/src/state/tests.rs`
  - `/src/input/tests.rs`
- [ ] Write unit tests (no PTY, no full app):
  - Test grid bounds checking
  - Test cursor movement within bounds
  - Test state updates
  - ~20-30 tests total
- [ ] Use `#[test]` (not `#[ignore]`)
- [ ] Make tests run with `cargo test` (not `cargo test -- --ignored`)

**Example:**
```rust
#[test]
fn test_block_list_cursor_movement() {
    let mut list = BlockList::new(vec!["a", "b", "c"]);
    list.move_down();
    assert_eq!(list.cursor_index(), 1);
}
```

---

### 6. Add Mock Integration Tests
**Impact:** High
**Effort:** Medium
**Time:** ~2-3 hours

- [ ] Create `/tests/mock_integration_test.rs` (NOT PTY-based)
- [ ] Mock RenderEngine:
  - Return fixed data structures
  - No database, no CDC stream
  - Controllable behavior
- [ ] Test scenarios without full TUI app:
  - Application state initialization
  - User input -> state change
  - State change -> render call
  - CDC stream processing
- [ ] 5-10 tests covering major workflows
- [ ] Run in CI (no PTY requirement)
- [ ] Faster and more reliable than PTY tests

**Structure:**
```rust
#[test]
fn test_toggle_without_app() {
    let mut state = create_test_state();
    state.toggle_current_item();
    assert!(state.is_item_completed(0));
}
```

---

## LOW PRIORITY (Polish)

### 7. Generalize PageObject
**Impact:** Medium
**Effort:** Medium
**Time:** ~2 hours

- [ ] Create base trait: `PageObject`
  - Methods: `get_screen()`, `assert_contains()`, `quit()`
  - Generic over session
- [ ] Refactor MainPage to implement trait
- [ ] Plan future PageObjects:
  - DialogPage (for confirm dialogs)
  - ListPage (generic list operations)
  - SearchPage (for search functionality)
- [ ] Document PageObject extension pattern

---

### 8. Add Visual Regression Testing
**Impact:** Low
**Effort:** High
**Time:** ~4+ hours

- [ ] Capture terminal output at key points:
  - After app start
  - After navigation
  - After toggle
  - After error condition
- [ ] Store as "golden" reference files:
  - `/tests/golden/app_start.txt`
  - `/tests/golden/nav_down.txt`
  - Compare with `assert_eq!(actual, golden)`
- [ ] Or use image comparison:
  - Convert ANSI output to image
  - Use `pixdiff` or similar crate
  - Detect layout changes visually
- [ ] Setup CI to fail on visual regression

---

## IMPLEMENTATION ORDER

1. **Week 1:** Complete HIGH PRIORITY items (1-3)
   - Small wins, quick improvements
   - Foundation for future work
   - Estimated: 1-2 hours total

2. **Week 2:** Start MEDIUM PRIORITY items (4-6)
   - 4 (helper): ~1 hour
   - 5 (unit tests): ~2-3 hours
   - 6 (mock tests): ~2-3 hours
   - **Recommendation:** Do 4 first (unblocks efficiency), then pick one of 5 or 6

3. **Week 3+:** LOW PRIORITY items (7-8)
   - Polish and nice-to-haves
   - Only if time and motivation

---

## SUCCESS METRICS

After implementing HIGH PRIORITY:
- [ ] All tests have proper timeouts (no hanging)
- [ ] Tests more reliable (ANSI stripping works for edge cases)
- [ ] Tests faster (key delays more realistic)

After implementing MEDIUM PRIORITY:
- [ ] <50 LOC boilerplate per new PTY test
- [ ] 20+ unit tests passing in `cargo test` (not ignored)
- [ ] 5+ mock integration tests in CI
- [ ] Test pyramid ratio roughly 3:2:1 (unit:mock:PTY)

---

## REFERENCE MATERIALS

- **R3BL Deadline:** `/Users/martin/Workspaces/rust/r3bl-open-core/tui/src/core/test_fixtures/pty_test_fixtures/deadline.rs`
- **R3BL PTY Macro:** `/Users/martin/Workspaces/rust/r3bl-open-core/tui/src/core/test_fixtures/pty_test_fixtures/generate_pty_test.rs`
- **R3BL Mock Devices:** `/Users/martin/Workspaces/rust/r3bl-open-core/tui/src/core/test_fixtures/`
- **Current Implementation:** `/Users/martin/Workspaces/pkm/holon/frontends/tui/tests/`
- **Detailed Analysis:** See `TESTING_PATTERNS_ANALYSIS.md` in this project root

---

## NOTES

- All changes should be backward compatible
- Add tests for tests (unit test the Deadline utility, etc.)
- Keep helper functions generic and reusable
- Document new patterns in code comments
- Consider adding test utilities to dedicated module (e.g., `tests/fixtures/mod.rs`)
