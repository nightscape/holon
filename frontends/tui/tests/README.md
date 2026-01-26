# TUI Integration Tests

This directory contains integration tests for the TUI frontend using the PageObject pattern with PTY (pseudo-terminal) infrastructure.

## Overview

The tests use a master/slave architecture:
- **Master process**: Controls the test, sends input, verifies output
- **Slave process**: Runs the actual TUI application in a PTY
- **PageObject**: Provides high-level abstractions (MainPage) for test readability

## Running Tests

### Run all tests (excluding PTY tests by default)
```bash
cargo test
```

### Run integration tests with PTY support
```bash
cargo test --test navigation_test -- --ignored --nocapture
```

### Run specific test
```bash
cargo test test_navigation_and_toggle -- --ignored --nocapture
```

## Test Structure

```
tests/
├── README.md                      # This file
├── pty_support/                   # PTY infrastructure
│   ├── mod.rs                     # PtySession wrapper
│   └── page_objects.rs            # MainPage PageObject
└── navigation_test.rs             # Integration tests
```

## Writing Tests

### Basic Test Pattern

```rust
use pty_support::{PtySession, page_objects::MainPage};

#[test]
#[ignore] // PTY tests ignored by default
fn test_my_feature() {
    // 1. Set up PTY and spawn slave
    let session = create_pty_session();
    let mut page = MainPage::new(session);

    // 2. Wait for app to start
    page.wait_for_ready(Duration::from_secs(5))
        .expect("App did not start");

    // 3. Perform actions
    page.navigate_down().expect("Failed to navigate");
    page.toggle_completion().expect("Failed to toggle");

    // 4. Verify results
    assert!(page.count_checked() > 0, "Should have checked items");

    // 5. Clean up
    page.quit().expect("Failed to quit");
    page.wait_for_exit().expect("Failed to exit");
}
```

### Available PageObject Methods

```rust
// Navigation
page.navigate_down()
page.navigate_up()

// Actions
page.toggle_completion()
page.indent()
page.outdent()
page.move_up()
page.move_down()

// Verification
page.get_screen()              // Get current screen state
page.assert_contains("text")   // Assert screen contains text
page.count_checked()           // Count checked checkboxes
page.count_unchecked()         // Count unchecked checkboxes
page.status_contains("text")   // Check status message

// Lifecycle
page.quit()                    // Quit the app
page.wait_for_exit()           // Wait for clean exit
```

## CI Considerations

PTY tests are automatically skipped in CI environments (via `is_ci::cached()`). They must be run with `--ignored` flag locally:

```bash
cargo test -- --ignored --nocapture
```

## Debugging

### View all output
```bash
cargo test test_name -- --ignored --nocapture
```

### Common Issues

1. **Test hangs**: Check timeout values, ensure app exits cleanly
2. **Output not found**: Increase sleep delays after actions
3. **PTY errors**: Verify terminal emulator supports PTY operations
4. **CI failures**: Ensure test is marked with `#[ignore]`

## Implementation Notes

### Master/Slave Pattern

The test uses environment variable `TUI_TEST_SLAVE` to determine role:
- Not set: Master process (controls test)
- Set: Slave process (runs TUI app)

### Timing

Small delays are necessary between actions to allow:
- TUI to process input events
- Screen to update
- CDC changes to propagate

Typical delays:
- After key press: 50-200ms
- After screen update: 200-500ms
- App startup: 1-5 seconds

### Output Parsing

ANSI escape codes are stripped from output for easier text matching. The PageObject provides structured access to screen content.

## Future Enhancements

- [ ] Visual regression testing (screenshot comparison)
- [ ] Protocol-based state inspection (query app state directly)
- [ ] Async PageObject methods
- [ ] Record/playback test generation
- [ ] More component-specific PageObjects (DialogPage, ListPage)
