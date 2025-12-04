/// Simple PTY infrastructure test
/// This test verifies the PTY session and PageObject infrastructure work correctly
/// without depending on the full TUI application.
mod pty_support;

use portable_pty::{CommandBuilder, NativePtySystem, PtySize, PtySystem};
use pty_support::{page_objects::Screen, PtySession};
use std::io::Write;
use std::time::Duration;

#[test]
#[ignore] // Requires terminal support
fn test_pty_session_basic() {
    const PTY_SLAVE_ENV_VAR: &str = "PTY_SESSION_TEST_SLAVE";

    // Check if we're the slave process
    if std::env::var(PTY_SLAVE_ENV_VAR).is_ok() {
        println!("SLAVE_STARTING");
        std::io::stdout().flush().unwrap();

        println!("Hello from PTY!");
        std::io::stdout().flush().unwrap();

        println!("Line 1: Test content");
        std::io::stdout().flush().unwrap();

        println!("Line 2: More content");
        std::io::stdout().flush().unwrap();

        println!("SUCCESS: PTY test passed");
        std::io::stdout().flush().unwrap();

        std::process::exit(0);
    }

    // Skip in CI
    if is_ci::cached() {
        println!("⏭️  Skipped in CI");
        return;
    }

    eprintln!("🚀 Master: Starting PTY infrastructure test");

    // Create PTY
    let pty_system = NativePtySystem::default();
    let pty_pair = pty_system
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("Failed to create PTY");

    // Spawn slave
    let test_binary = std::env::current_exe().expect("Failed to get exe");
    let mut cmd = CommandBuilder::new(&test_binary);
    cmd.env(PTY_SLAVE_ENV_VAR, "1");
    cmd.args(&[
        "--test-threads",
        "1",
        "--nocapture",
        "--ignored",
        "test_pty_session_basic",
    ]);

    let child = pty_pair.slave.spawn_command(cmd).expect("Failed to spawn");

    // Create session
    let mut session = PtySession::new(pty_pair, child);

    // Test 1: Wait for slave to start
    eprintln!("📝 Test 1: Waiting for slave start");
    session
        .wait_for("SLAVE_STARTING", Duration::from_secs(2))
        .expect("Slave did not start");
    eprintln!("  ✓ Slave started");

    // Test 2: Wait for specific message
    eprintln!("📝 Test 2: Waiting for hello message");
    session
        .wait_for("Hello from PTY", Duration::from_secs(2))
        .expect("Hello message not found");
    eprintln!("  ✓ Hello message received");

    // Test 3: Read multiple lines
    eprintln!("📝 Test 3: Reading multiple lines");
    let lines = session.drain_output();
    eprintln!("  Received {} lines", lines.len());

    // Test 4: Parse screen
    eprintln!("📝 Test 4: Parsing screen");
    let output = lines.join("\n");
    let screen = Screen::parse(&output);

    assert!(
        screen.contains("Test content"),
        "Should contain test content"
    );
    assert!(
        screen.contains("More content"),
        "Should contain more content"
    );
    eprintln!("  ✓ Screen parsed correctly");

    // Test 5: Wait for success message
    eprintln!("📝 Test 5: Waiting for success");
    session
        .wait_for("SUCCESS", Duration::from_secs(2))
        .expect("Success message not found");
    eprintln!("  ✓ Success message received");

    // Wait for clean exit
    session.wait_for_child().expect("Failed to wait for child");

    eprintln!("✅ PTY infrastructure test passed!");
}

#[test]
fn test_screen_parsing() {
    // Test ANSI stripping
    let output_with_ansi = "\x1b[1;31mHello\x1b[0m World\nLine 2\x1b[32m colored\x1b[0m";
    let screen = Screen::parse(output_with_ansi);

    assert_eq!(screen.lines.len(), 2);
    assert!(screen.contains("Hello"));
    assert!(screen.contains("World"));
    assert!(screen.contains("Line 2"));
    assert!(screen.contains("colored"));

    // Test line access
    assert!(screen.get_line(0).is_some());
    assert!(screen.get_line(0).unwrap().contains("Hello"));

    // Test find
    assert_eq!(screen.find_text("Line 2"), Some((1, 0)));

    // Test count
    assert_eq!(screen.count_occurrences("Line"), 1);
}

#[test]
fn test_key_sequences() {
    use pty_support::Key;

    // Verify key sequences are generated correctly
    assert_eq!(Key::Up.to_sequence(), "\x1b[A");
    assert_eq!(Key::Down.to_sequence(), "\x1b[B");
    assert_eq!(Key::Left.to_sequence(), "\x1b[D");
    assert_eq!(Key::Right.to_sequence(), "\x1b[C");
    assert_eq!(Key::Enter.to_sequence(), "\r");
    assert_eq!(Key::Tab.to_sequence(), "\t");
    assert_eq!(Key::Space.to_sequence(), " ");
    assert_eq!(Key::Char('a').to_sequence(), "a");
    assert_eq!(Key::CtrlC.to_sequence(), "\x03");
}
