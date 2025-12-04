use portable_pty::{Child, PtyPair};
/// PTY session infrastructure for TUI testing
///
/// Provides a wrapper around PTY pairs for easier testing.
use std::io::{BufRead, BufReader, Write};
use std::time::{Duration, Instant};

pub mod page_objects;

/// Deadline utility for enforcing timeouts on operations
#[derive(Debug, Clone)]
pub struct Deadline {
    start: Instant,
    timeout: Duration,
}

impl Deadline {
    /// Create a new deadline with the given timeout
    pub fn new(timeout: Duration) -> Self {
        Self {
            start: Instant::now(),
            timeout,
        }
    }

    /// Check if the deadline has been exceeded
    pub fn has_expired(&self) -> bool {
        self.start.elapsed() > self.timeout
    }

    /// Get the remaining time until the deadline
    pub fn remaining(&self) -> Duration {
        self.timeout.saturating_sub(self.start.elapsed())
    }

    /// Panic if deadline has expired (use in test assertions)
    pub fn assert_not_expired(&self, context: &str) {
        if self.has_expired() {
            panic!(
                "Deadline exceeded after {:?} (timeout: {:?}): {}",
                self.start.elapsed(),
                self.timeout,
                context
            );
        }
    }
}

pub struct PtySession {
    reader: BufReader<Box<dyn std::io::Read + Send>>,
    writer: Box<dyn std::io::Write + Send>,
    child: Option<Box<dyn Child + Send + Sync>>,
    output_buffer: Vec<String>,
    key_delay: Duration,
}

impl PtySession {
    pub fn new(pty_pair: PtyPair, child: Box<dyn Child + Send + Sync>) -> Self {
        let reader = BufReader::new(
            pty_pair
                .master
                .try_clone_reader()
                .expect("Failed to get reader"),
        );
        let writer = pty_pair.master.take_writer().expect("Failed to get writer");

        Self {
            reader,
            writer,
            child: Some(child),
            output_buffer: Vec::new(),
            key_delay: Duration::from_millis(50), // Default delay
        }
    }

    /// Set the delay between key presses (for realistic typing simulation)
    pub fn set_key_delay(&mut self, delay: Duration) {
        self.key_delay = delay;
    }

    /// Get the current key delay
    pub fn key_delay(&self) -> Duration {
        self.key_delay
    }

    /// Send a string to the PTY
    pub fn send(&mut self, text: &str) -> Result<(), std::io::Error> {
        write!(self.writer, "{}", text)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Send a key sequence with configured delay
    pub fn send_key(&mut self, key: Key) -> Result<(), std::io::Error> {
        let sequence = key.to_sequence();
        self.send(&sequence)?;
        // Use configured delay to allow TUI to process
        std::thread::sleep(self.key_delay);
        Ok(())
    }

    /// Send a key sequence without delay (instant)
    pub fn send_key_instant(&mut self, key: Key) -> Result<(), std::io::Error> {
        let sequence = key.to_sequence();
        self.send(&sequence)?;
        Ok(())
    }

    /// Send multiple characters with timing between each (realistic typing)
    pub fn type_text(&mut self, text: &str) -> Result<(), std::io::Error> {
        for ch in text.chars() {
            self.send_key(Key::Char(ch))?;
        }
        Ok(())
    }

    /// Send multiple characters instantly (no delay)
    pub fn type_text_instant(&mut self, text: &str) -> Result<(), std::io::Error> {
        self.send(text)?;
        Ok(())
    }

    /// Read a single line with timeout
    pub fn read_line(&mut self, timeout: Duration) -> Result<String, PtyError> {
        let deadline = Deadline::new(timeout);
        let mut line = String::new();

        loop {
            if deadline.has_expired() {
                return Err(PtyError::Timeout);
            }

            match self.reader.read_line(&mut line) {
                Ok(0) => return Err(PtyError::Eof),
                Ok(_) => {
                    self.output_buffer.push(line.clone());
                    return Ok(line);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => return Err(PtyError::Io(e)),
            }
        }
    }

    /// Wait for specific text to appear in output
    pub fn wait_for(&mut self, expected: &str, timeout: Duration) -> Result<(), PtyError> {
        let deadline = Deadline::new(timeout);

        while !deadline.has_expired() {
            match self.read_line(Duration::from_millis(100)) {
                Ok(line) => {
                    if line.contains(expected) {
                        return Ok(());
                    }
                }
                Err(PtyError::Timeout) => continue,
                Err(e) => return Err(e),
            }
        }

        Err(PtyError::Timeout)
    }

    /// Drain all available output without blocking
    pub fn drain_output(&mut self) -> Vec<String> {
        let mut lines = Vec::new();

        while let Ok(line) = self.read_line(Duration::from_millis(50)) {
            lines.push(line);
        }

        lines
    }

    /// Get all output collected so far
    pub fn get_output_buffer(&self) -> &[String] {
        &self.output_buffer
    }

    /// Clear the output buffer
    pub fn clear_output_buffer(&mut self) {
        self.output_buffer.clear();
    }

    /// Wait for child process to exit
    pub fn wait_for_child(&mut self) -> Result<(), PtyError> {
        if let Some(mut child) = self.child.take() {
            child.wait().map_err(|e| PtyError::Io(e))?;
        }
        Ok(())
    }
}

/// Key sequences for terminal input
#[derive(Debug, Clone, Copy)]
pub enum Key {
    Char(char),
    Enter,
    Tab,
    ShiftTab,
    Escape,
    Up,
    Down,
    Left,
    Right,
    CtrlUp,
    CtrlDown,
    CtrlLeft,
    CtrlRight,
    CtrlC,
    CtrlQ,
    Space,
}

impl Key {
    pub fn to_sequence(&self) -> String {
        match self {
            Key::Char(c) => c.to_string(),
            Key::Enter => "\r".to_string(),
            Key::Tab => "\t".to_string(),
            Key::ShiftTab => "\x1b[Z".to_string(),
            Key::Escape => "\x1b".to_string(),
            Key::Up => "\x1b[A".to_string(),
            Key::Down => "\x1b[B".to_string(),
            Key::Right => "\x1b[C".to_string(),
            Key::Left => "\x1b[D".to_string(),
            Key::CtrlUp => "\x1b[1;5A".to_string(),
            Key::CtrlDown => "\x1b[1;5B".to_string(),
            Key::CtrlRight => "\x1b[1;5C".to_string(),
            Key::CtrlLeft => "\x1b[1;5D".to_string(),
            Key::CtrlC => "\x03".to_string(),
            Key::CtrlQ => "\x11".to_string(), // Ctrl+Q is ASCII 17 (0x11)
            Key::Space => " ".to_string(),
        }
    }
}

#[derive(Debug)]
pub enum PtyError {
    Timeout,
    Eof,
    Io(std::io::Error),
    NotFound(String),
}

impl std::fmt::Display for PtyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PtyError::Timeout => write!(f, "Timeout"),
            PtyError::Eof => write!(f, "End of file"),
            PtyError::Io(e) => write!(f, "IO error: {}", e),
            PtyError::NotFound(s) => write!(f, "Not found: {}", s),
        }
    }
}

impl std::error::Error for PtyError {}
