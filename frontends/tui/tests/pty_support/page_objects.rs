/// PageObject pattern for TUI testing
///
/// Provides high-level abstractions over terminal interactions.
use super::{Key, PtyError, PtySession};
use std::time::Duration;

/// Parse terminal output into structured screen representation
#[derive(Debug)]
pub struct Screen {
    pub lines: Vec<String>,
}

impl Screen {
    /// Parse output into screen lines, stripping ANSI codes
    pub fn parse(output: &str) -> Self {
        let lines: Vec<String> = output.lines().map(|line| strip_ansi_codes(line)).collect();

        Self { lines }
    }

    /// Check if screen contains text
    pub fn contains(&self, text: &str) -> bool {
        self.lines.iter().any(|line| line.contains(text))
    }

    /// Get a specific line
    pub fn get_line(&self, index: usize) -> Option<&str> {
        self.lines.get(index).map(|s| s.as_str())
    }

    /// Find text and return (row, col) position
    pub fn find_text(&self, text: &str) -> Option<(usize, usize)> {
        for (row, line) in self.lines.iter().enumerate() {
            if let Some(col) = line.find(text) {
                return Some((row, col));
            }
        }
        None
    }

    /// Count occurrences of text
    pub fn count_occurrences(&self, text: &str) -> usize {
        self.lines.iter().filter(|line| line.contains(text)).count()
    }
}

/// Strip ANSI escape codes from text
fn strip_ansi_codes(text: &str) -> String {
    // Use the battle-tested strip-ansi-escapes crate for robust ANSI handling
    let bytes = strip_ansi_escapes::strip(text);
    String::from_utf8_lossy(&bytes).into_owned()
}

/// PageObject for the main outliner screen
pub struct MainPage {
    session: PtySession,
}

impl MainPage {
    pub fn new(session: PtySession) -> Self {
        Self { session }
    }

    /// Set the delay between key presses (for realistic typing simulation)
    pub fn set_key_delay(&mut self, delay: std::time::Duration) {
        self.session.set_key_delay(delay);
    }

    /// Type text with realistic timing between characters
    pub fn type_text(&mut self, text: &str) -> Result<(), PtyError> {
        self.session.type_text(text).map_err(PtyError::Io)
    }

    /// Type text instantly (no delay between characters)
    pub fn type_text_instant(&mut self, text: &str) -> Result<(), PtyError> {
        self.session.type_text_instant(text).map_err(PtyError::Io)
    }

    /// Wait for the app to fully start
    pub fn wait_for_ready(&mut self, timeout: Duration) -> Result<(), PtyError> {
        self.session.wait_for("Block Outliner", timeout)
    }

    /// Navigate down one item
    pub fn navigate_down(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::Down).map_err(PtyError::Io)
    }

    /// Navigate up one item
    pub fn navigate_up(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::Up).map_err(PtyError::Io)
    }

    /// Toggle completion checkbox (Space key)
    pub fn toggle_completion(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::Space).map_err(PtyError::Io)
    }

    /// Indent the selected block (Tab or ])
    pub fn indent(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::Tab).map_err(PtyError::Io)
    }

    /// Outdent the selected block (Shift+Tab or [)
    pub fn outdent(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::ShiftTab).map_err(PtyError::Io)
    }

    /// Move block up (Ctrl+Up)
    pub fn move_up(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::CtrlUp).map_err(PtyError::Io)
    }

    /// Move block down (Ctrl+Down)
    pub fn move_down(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::CtrlDown).map_err(PtyError::Io)
    }

    /// Indent using Ctrl+Right
    pub fn indent_ctrl(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::CtrlRight).map_err(PtyError::Io)
    }

    /// Outdent using Ctrl+Left
    pub fn outdent_ctrl(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::CtrlLeft).map_err(PtyError::Io)
    }

    /// Press a character key
    pub fn press_char(&mut self, ch: char) -> Result<(), PtyError> {
        self.session.send_key(Key::Char(ch)).map_err(PtyError::Io)
    }

    /// Quit the application (Ctrl+q key)
    pub fn quit(&mut self) -> Result<(), PtyError> {
        self.session.send_key(Key::CtrlQ).map_err(PtyError::Io)
    }

    /// Get current screen state
    pub fn get_screen(&mut self) -> Screen {
        // Collect recent output
        let lines = self.session.drain_output();
        let output = lines.join("\n");
        Screen::parse(&output)
    }

    /// Get all output collected so far
    pub fn get_all_output(&self) -> String {
        self.session.get_output_buffer().join("\n")
    }

    /// Check if status message contains text
    pub fn status_contains(&mut self, text: &str) -> bool {
        let screen = self.get_screen();
        // Status is typically on the last line
        screen
            .lines
            .last()
            .map(|line| line.contains(text))
            .unwrap_or(false)
    }

    /// Wait for status message to contain text
    pub fn wait_for_status(&mut self, text: &str, timeout: Duration) -> Result<(), PtyError> {
        self.session.wait_for(text, timeout)
    }

    /// Assert that screen contains text
    pub fn assert_contains(&mut self, text: &str) -> Result<(), PtyError> {
        let screen = self.get_screen();
        if screen.contains(text) {
            Ok(())
        } else {
            Err(PtyError::NotFound(format!(
                "Screen does not contain '{}'",
                text
            )))
        }
    }

    /// Assert that specific line contains text
    pub fn assert_line_contains(&mut self, line_index: usize, text: &str) -> Result<(), PtyError> {
        let screen = self.get_screen();
        if let Some(line) = screen.get_line(line_index) {
            if line.contains(text) {
                Ok(())
            } else {
                Err(PtyError::NotFound(format!(
                    "Line {} does not contain '{}': {}",
                    line_index, text, line
                )))
            }
        } else {
            Err(PtyError::NotFound(format!("Line {} not found", line_index)))
        }
    }

    /// Count checked checkboxes in output
    pub fn count_checked(&mut self) -> usize {
        let screen = self.get_screen();
        screen.count_occurrences("[✓]")
    }

    /// Count unchecked checkboxes in output
    pub fn count_unchecked(&mut self) -> usize {
        let screen = self.get_screen();
        screen.count_occurrences("[ ]")
    }

    /// Wait for child process to exit
    pub fn wait_for_exit(&mut self) -> Result<(), PtyError> {
        self.session.wait_for_child()
    }

    /// Unwrap the inner session for advanced operations
    pub fn into_session(self) -> PtySession {
        self.session
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_ansi_codes() {
        let input = "\x1b[1;31mHello\x1b[0m World";
        let output = strip_ansi_codes(input);
        assert_eq!(output, "Hello World");
    }

    #[test]
    fn test_screen_parse() {
        let output = "Line 1\nLine 2\nLine 3";
        let screen = Screen::parse(output);
        assert_eq!(screen.lines.len(), 3);
        assert_eq!(screen.get_line(0), Some("Line 1"));
        assert_eq!(screen.get_line(1), Some("Line 2"));
        assert_eq!(screen.get_line(2), Some("Line 3"));
    }

    #[test]
    fn test_screen_contains() {
        let output = "Hello World\nFoo Bar";
        let screen = Screen::parse(output);
        assert!(screen.contains("World"));
        assert!(screen.contains("Foo"));
        assert!(!screen.contains("Baz"));
    }

    #[test]
    fn test_screen_find_text() {
        let output = "Line 1\nLine 2 with text\nLine 3";
        let screen = Screen::parse(output);
        assert_eq!(screen.find_text("with text"), Some((1, 7)));
        assert_eq!(screen.find_text("nonexistent"), None);
    }
}
