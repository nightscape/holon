# PageObject Pattern for TUI Testing

## Concept

The PageObject pattern from web testing (Selenium, Playwright) can be adapted for TUI applications. Instead of abstracting DOM elements and browser interactions, we abstract terminal components and PTY interactions.

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│ Test Code (High Level)                                      │
│   editor_page.type_text("Hello")                            │
│   editor_page.press_ctrl_s()                                │
│   assert_eq!(editor_page.get_line(0), "Hello")              │
└────────────────┬────────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────────┐
│ PageObject Layer (Abstraction)                              │
│   - Component-specific operations                           │
│   - Screen reading and parsing                              │
│   - Wait conditions                                         │
│   - Assertions                                              │
└────────────────┬────────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────────┐
│ PTY Interaction Layer                                       │
│   - Send input (keys, mouse)                                │
│   - Read output (screen content)                            │
│   - Parse ANSI sequences                                    │
│   - Handle timing/synchronization                           │
└────────────────┬────────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────────┐
│ PTY Master/Slave Infrastructure                             │
│   (generate_pty_test! macro)                                │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Approach

### 1. Base PTY Wrapper

Create a foundational wrapper for PTY interaction:

```rust
use std::io::{BufRead, BufReader, Write};
use std::time::{Duration, Instant};
use portable_pty::{PtyPair, Child};

pub struct PtySession {
    reader: BufReader<Box<dyn std::io::Read + Send>>,
    writer: Box<dyn std::io::Write + Send>,
    child: Option<Box<dyn Child + Send + Sync>>,
}

impl PtySession {
    pub fn new(pty_pair: PtyPair, child: Box<dyn Child + Send + Sync>) -> Self {
        let reader = BufReader::new(
            pty_pair.master.try_clone_reader().expect("Failed to get reader")
        );
        let writer = pty_pair.master.take_writer().expect("Failed to get writer");

        Self {
            reader,
            writer,
            child: Some(child),
        }
    }

    /// Send a string to the PTY
    pub fn send(&mut self, text: &str) -> Result<(), std::io::Error> {
        write!(self.writer, "{}", text)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Send a single key press
    pub fn send_key(&mut self, key: Key) -> Result<(), std::io::Error> {
        let sequence = key.to_ansi_sequence();
        self.send(&sequence)
    }

    /// Read a line from PTY with timeout
    pub fn read_line(&mut self, timeout: Duration) -> Result<String, PtyError> {
        let start = Instant::now();
        let mut line = String::new();

        loop {
            if start.elapsed() > timeout {
                return Err(PtyError::Timeout);
            }

            match self.reader.read_line(&mut line) {
                Ok(0) => return Err(PtyError::Eof),
                Ok(_) => return Ok(line),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => return Err(PtyError::Io(e)),
            }
        }
    }

    /// Wait for specific output to appear
    pub fn wait_for(&mut self, expected: &str, timeout: Duration) -> Result<(), PtyError> {
        let start = Instant::now();

        while start.elapsed() < timeout {
            let line = self.read_line(Duration::from_millis(100))?;
            if line.contains(expected) {
                return Ok(());
            }
        }

        Err(PtyError::Timeout)
    }

    /// Read all available output without blocking
    pub fn drain_output(&mut self) -> Vec<String> {
        let mut lines = Vec::new();

        while let Ok(line) = self.read_line(Duration::from_millis(50)) {
            lines.push(line);
        }

        lines
    }
}

#[derive(Debug)]
pub enum PtyError {
    Timeout,
    Eof,
    Io(std::io::Error),
    Unexpected(String),
}

pub enum Key {
    Char(char),
    Enter,
    Backspace,
    Delete,
    Tab,
    Escape,
    Up,
    Down,
    Left,
    Right,
    CtrlC,
    CtrlD,
    CtrlS,
    CtrlW,
    // ... more keys
}

impl Key {
    fn to_ansi_sequence(&self) -> String {
        match self {
            Key::Char(c) => c.to_string(),
            Key::Enter => "\r".to_string(),
            Key::Backspace => "\x7f".to_string(),
            Key::Delete => "\x1b[3~".to_string(),
            Key::Tab => "\t".to_string(),
            Key::Escape => "\x1b".to_string(),
            Key::Up => "\x1b[A".to_string(),
            Key::Down => "\x1b[B".to_string(),
            Key::Right => "\x1b[C".to_string(),
            Key::Left => "\x1b[D".to_string(),
            Key::CtrlC => "\x03".to_string(),
            Key::CtrlD => "\x04".to_string(),
            Key::CtrlS => "\x13".to_string(),
            Key::CtrlW => "\x17".to_string(),
        }
    }
}
```

### 2. Screen Parser

Parse ANSI output to extract screen state:

```rust
pub struct Screen {
    lines: Vec<String>,
    cursor_row: usize,
    cursor_col: usize,
}

impl Screen {
    /// Parse ANSI output into a screen representation
    pub fn parse(output: &str) -> Self {
        // Strip ANSI escape sequences and build screen state
        let stripped = strip_ansi_codes(output);
        let lines: Vec<String> = stripped.lines().map(|s| s.to_string()).collect();

        Self {
            lines,
            cursor_row: 0,
            cursor_col: 0,
        }
    }

    pub fn get_line(&self, row: usize) -> Option<&str> {
        self.lines.get(row).map(|s| s.as_str())
    }

    pub fn contains(&self, text: &str) -> bool {
        self.lines.iter().any(|line| line.contains(text))
    }

    pub fn get_cursor_position(&self) -> (usize, usize) {
        (self.cursor_row, self.cursor_col)
    }

    pub fn find_text(&self, text: &str) -> Option<(usize, usize)> {
        for (row, line) in self.lines.iter().enumerate() {
            if let Some(col) = line.find(text) {
                return Some((row, col));
            }
        }
        None
    }
}

fn strip_ansi_codes(text: &str) -> String {
    // Simple ANSI stripping - could use a crate like `strip-ansi-escapes`
    let re = regex::Regex::new(r"\x1b\[[0-9;]*[a-zA-Z]").unwrap();
    re.replace_all(text, "").to_string()
}
```

### 3. Component PageObjects

Create PageObjects for specific components:

```rust
/// PageObject for an Editor component
pub struct EditorPage {
    session: PtySession,
    component_id: String,
}

impl EditorPage {
    pub fn new(session: PtySession, component_id: impl Into<String>) -> Self {
        Self {
            session,
            component_id: component_id.into(),
        }
    }

    /// Type text into the editor
    pub fn type_text(&mut self, text: &str) -> Result<(), PtyError> {
        self.session.send(text)?;
        // Wait for echo or confirmation
        std::thread::sleep(Duration::from_millis(100));
        Ok(())
    }

    /// Press a key
    pub fn press_key(&mut self, key: Key) -> Result<(), PtyError> {
        self.session.send_key(key)?;
        std::thread::sleep(Duration::from_millis(50));
        Ok(())
    }

    /// Get the content of a specific line
    pub fn get_line(&mut self, line_num: usize) -> Result<String, PtyError> {
        // Request screen refresh or read current state
        let output = self.session.drain_output().join("\n");
        let screen = Screen::parse(&output);

        screen.get_line(line_num)
            .map(|s| s.to_string())
            .ok_or(PtyError::Unexpected(format!("Line {} not found", line_num)))
    }

    /// Get all editor content
    pub fn get_content(&mut self) -> Result<Vec<String>, PtyError> {
        let output = self.session.drain_output().join("\n");
        let screen = Screen::parse(&output);
        Ok(screen.lines.clone())
    }

    /// Save the file (Ctrl+S)
    pub fn save(&mut self) -> Result<(), PtyError> {
        self.press_key(Key::CtrlS)?;
        self.session.wait_for("Saved", Duration::from_secs(2))
    }

    /// Assert editor contains text
    pub fn assert_contains(&mut self, text: &str) -> Result<(), PtyError> {
        let output = self.session.drain_output().join("\n");
        let screen = Screen::parse(&output);

        if screen.contains(text) {
            Ok(())
        } else {
            Err(PtyError::Unexpected(
                format!("Screen does not contain '{}'", text)
            ))
        }
    }

    /// Move cursor to position
    pub fn move_to(&mut self, row: usize, col: usize) -> Result<(), PtyError> {
        // Send appropriate key sequences to position cursor
        // This depends on the component's behavior
        todo!()
    }

    /// Select text
    pub fn select(&mut self, from: (usize, usize), to: (usize, usize)) -> Result<(), PtyError> {
        // Move to start, shift+move to end
        todo!()
    }
}

/// PageObject for a Dialog component
pub struct DialogPage {
    session: PtySession,
}

impl DialogPage {
    pub fn new(session: PtySession) -> Self {
        Self { session }
    }

    pub fn is_visible(&mut self) -> Result<bool, PtyError> {
        let output = self.session.drain_output().join("\n");
        let screen = Screen::parse(&output);
        Ok(screen.contains("Dialog") || screen.contains("┌")) // Dialog border
    }

    pub fn type_input(&mut self, text: &str) -> Result<(), PtyError> {
        self.session.send(text)
    }

    pub fn confirm(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::Enter)
    }

    pub fn cancel(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::Escape)
    }

    pub fn wait_for_dialog(&mut self, timeout: Duration) -> Result<(), PtyError> {
        self.session.wait_for("Dialog", timeout)
    }
}

/// PageObject for a List/Menu component
pub struct ListPage {
    session: PtySession,
}

impl ListPage {
    pub fn new(session: PtySession) -> Self {
        Self { session }
    }

    pub fn select_item(&mut self, index: usize) -> Result<(), PtyError> {
        // Move down N times
        for _ in 0..index {
            self.session.send_key(Key::Down)?;
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }

    pub fn confirm_selection(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::Enter)
    }

    pub fn get_selected_item(&mut self) -> Result<String, PtyError> {
        let output = self.session.drain_output().join("\n");
        let screen = Screen::parse(&output);

        // Find line with selection indicator (>, ▶, highlighted, etc.)
        for line in &screen.lines {
            if line.starts_with(">") || line.starts_with("▶") {
                return Ok(line.trim_start_matches(|c| c == '>' || c == '▶' || c == ' ')
                    .to_string());
            }
        }

        Err(PtyError::Unexpected("No selected item found".to_string()))
    }

    pub fn get_items(&mut self) -> Result<Vec<String>, PtyError> {
        let output = self.session.drain_output().join("\n");
        let screen = Screen::parse(&output);
        Ok(screen.lines.clone())
    }
}
```

### 4. Application PageObject (Composition)

Combine component PageObjects into an application-level PageObject:

```rust
pub struct AppPage {
    session: PtySession,
}

impl AppPage {
    pub fn new(session: PtySession) -> Self {
        Self { session }
    }

    /// Get editor PageObject
    pub fn editor(&mut self, id: &str) -> EditorPage {
        EditorPage::new(self.session, id)
    }

    /// Get dialog PageObject
    pub fn dialog(&mut self) -> DialogPage {
        DialogPage::new(self.session)
    }

    /// Get list PageObject
    pub fn list(&mut self) -> ListPage {
        ListPage::new(self.session)
    }

    /// Open dialog with keyboard shortcut
    pub fn open_dialog(&mut self) -> Result<DialogPage, PtyError> {
        self.session.send_key(Key::CtrlD)?;
        let dialog = DialogPage::new(self.session);
        dialog.wait_for_dialog(Duration::from_secs(2))?;
        Ok(dialog)
    }

    /// Switch focus to component
    pub fn focus_component(&mut self, id: &str) -> Result<(), PtyError> {
        // Send Tab or specific keyboard shortcut to change focus
        todo!()
    }

    /// Quit the application
    pub fn quit(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::CtrlC)?;
        self.session.wait_for("Goodbye", Duration::from_secs(1))
    }
}
```

### 5. Test Example

Here's how tests would look using the PageObject pattern:

```rust
use crate::{generate_pty_test, AppPage, PtySession};

generate_pty_test! {
    test_fn: test_editor_workflow,
    master: test_editor_master,
    slave: test_editor_slave
}

fn test_editor_master(
    pty_pair: portable_pty::PtyPair,
    child: Box<dyn portable_pty::Child + Send + Sync>,
) {
    // Create PageObject
    let session = PtySession::new(pty_pair, child);
    let mut app = AppPage::new(session);

    // Wait for app to start
    std::thread::sleep(Duration::from_secs(1));

    // High-level test operations
    let mut editor = app.editor("main");

    editor.type_text("Hello, World!").expect("Failed to type");
    editor.press_key(Key::Enter).expect("Failed to press Enter");
    editor.type_text("Second line").expect("Failed to type");

    // Assertions
    editor.assert_contains("Hello, World!").expect("Text not found");

    let line0 = editor.get_line(0).expect("Failed to get line");
    assert_eq!(line0, "Hello, World!");

    let line1 = editor.get_line(1).expect("Failed to get line");
    assert_eq!(line1, "Second line");

    // Save
    editor.save().expect("Failed to save");

    // Open dialog
    let mut dialog = app.open_dialog().expect("Failed to open dialog");
    assert!(dialog.is_visible().expect("Failed to check visibility"));

    dialog.type_input("test.txt").expect("Failed to type");
    dialog.confirm().expect("Failed to confirm");

    // Quit
    app.quit().expect("Failed to quit");
}

fn test_editor_slave() -> ! {
    // Launch the actual TUI application
    println!("SLAVE_STARTING");
    std::io::stdout().flush().unwrap();

    // Run your TUI app here
    // my_tui_app::run();

    std::process::exit(0);
}
```

### 6. Advanced Features

#### Wait Conditions

```rust
pub trait WaitCondition {
    fn check(&self, screen: &Screen) -> bool;
}

pub struct TextAppears(&'static str);
impl WaitCondition for TextAppears {
    fn check(&self, screen: &Screen) -> bool {
        screen.contains(self.0)
    }
}

pub struct TextDisappears(&'static str);
impl WaitCondition for TextDisappears {
    fn check(&self, screen: &Screen) -> bool {
        !screen.contains(self.0)
    }
}

impl PtySession {
    pub fn wait_until<C: WaitCondition>(
        &mut self,
        condition: C,
        timeout: Duration,
    ) -> Result<(), PtyError> {
        let start = Instant::now();

        while start.elapsed() < timeout {
            let output = self.drain_output().join("\n");
            let screen = Screen::parse(&output);

            if condition.check(&screen) {
                return Ok(());
            }

            std::thread::sleep(Duration::from_millis(100));
        }

        Err(PtyError::Timeout)
    }
}

// Usage:
session.wait_until(TextAppears("Saved"), Duration::from_secs(2))?;
```

#### Screen Snapshots

```rust
impl Screen {
    pub fn snapshot(&self) -> String {
        self.lines.join("\n")
    }

    pub fn assert_snapshot_matches(&self, expected: &str) {
        let actual = self.snapshot();
        assert_eq!(actual, expected, "Screen snapshot mismatch");
    }
}
```

#### Fluent Interface

```rust
impl EditorPage {
    pub fn type_text(mut self, text: &str) -> Self {
        self.session.send(text).unwrap();
        self
    }

    pub fn press_enter(mut self) -> Self {
        self.session.send_key(Key::Enter).unwrap();
        self
    }

    pub fn save(mut self) -> Self {
        self.session.send_key(Key::CtrlS).unwrap();
        self
    }
}

// Fluent usage:
editor
    .type_text("Hello")
    .press_enter()
    .type_text("World")
    .save();
```

## Benefits

1. **Abstraction**: Hide PTY complexity from tests
2. **Reusability**: Component PageObjects reusable across tests
3. **Maintainability**: Changes to UI structure only affect PageObjects
4. **Readability**: Tests read like user workflows
5. **Type Safety**: Compile-time verification of interactions
6. **Composability**: Build complex scenarios from simple operations

## Challenges

1. **Screen State**: TUI doesn't have DOM-like introspection
   - Solution: Parse ANSI output or use protocol for state queries

2. **Timing**: Async rendering requires careful synchronization
   - Solution: Implement robust wait conditions

3. **Focus Management**: Tracking which component has focus
   - Solution: Add focus markers in output or protocol queries

4. **Platform Differences**: Terminal emulator variations
   - Solution: Test on multiple platforms, abstract differences

5. **ANSI Complexity**: Parsing escape sequences correctly
   - Solution: Use existing crates or build robust parser

## Comparison with Web PageObjects

| Aspect | Web (Selenium) | TUI (PTY-based) |
|--------|---------------|-----------------|
| Element location | CSS selectors | Screen coordinates, text patterns |
| State introspection | DOM API | ANSI parsing, protocol queries |
| Actions | Click, type | Send keys, ANSI sequences |
| Assertions | Element properties | Screen text, patterns |
| Wait conditions | Element visible | Text appears, state changes |
| Isolation | Browser instances | PTY pairs |

## Future Enhancements

1. **Visual Regression Testing**: Compare screen snapshots
2. **Protocol Extension**: Add TUI-specific query protocol
3. **Component Inspector**: Runtime introspection API
4. **Async Support**: Tokio-based async PageObjects
5. **Recording/Playback**: Record interactions for test generation
6. **Accessibility Testing**: Verify keyboard navigation, screen readers

## Conclusion

The PageObject pattern adapts well to TUI testing when combined with PTY infrastructure. It provides the same benefits as web testing: abstraction, reusability, and maintainability. The key is building robust screen parsing and synchronization mechanisms.
