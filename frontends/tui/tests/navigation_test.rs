/// Integration test for TUI navigation using PageObject pattern
///
/// This test uses PTY pairs to test actual terminal interaction.
/// Run with: cargo test navigation_test -- --nocapture
mod pty_support;

use portable_pty::{CommandBuilder, NativePtySystem, PtySize, PtySystem};
use pty_support::{page_objects::MainPage, PtySession};
use std::io::Write;
use std::time::Duration;
use tui_r3bl_frontend::config::KeyBindingConfig;

#[test]
#[ignore] // Ignore by default since it requires terminal support
fn test_navigation_and_toggle() {
    const PTY_SLAVE_ENV_VAR: &str = "TUI_TEST_SLAVE";

    // Check if we're running as the slave process
    if std::env::var(PTY_SLAVE_ENV_VAR).is_ok() {
        eprintln!("🔍 Slave: Running TUI app");
        println!("SLAVE_STARTING");
        std::io::stdout().flush().expect("Failed to flush");

        // Run the TUI app
        run_tui_app_slave();
    }

    // Skip in CI environments
    if is_ci::cached() {
        println!("⏭️  Skipped in CI (requires interactive terminal)");
        return;
    }

    eprintln!("🚀 Master: Starting navigation test");

    // Create PTY pair
    let pty_system = NativePtySystem::default();
    let pty_pair = pty_system
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("Failed to create PTY pair");

    // Spawn slave process
    let test_binary = std::env::current_exe().expect("Failed to get current executable");
    let mut cmd = CommandBuilder::new(&test_binary);
    cmd.env(PTY_SLAVE_ENV_VAR, "1");
    cmd.env("RUST_BACKTRACE", "1");
    cmd.args(&[
        "--test-threads",
        "1",
        "--nocapture",
        "--ignored",
        "test_navigation_and_toggle",
    ]);

    eprintln!("🚀 Master: Spawning slave process");
    let child = pty_pair
        .slave
        .spawn_command(cmd)
        .expect("Failed to spawn slave process");

    // Create PageObject
    let session = PtySession::new(pty_pair, child);
    let mut main_page = MainPage::new(session);

    // Run the test scenario
    eprintln!("📝 Master: Waiting for app to start");

    // Wait for app to be ready
    main_page
        .wait_for_ready(Duration::from_secs(5))
        .expect("App did not start");
    eprintln!("  ✓ App started");

    // Give it a moment to fully render
    std::thread::sleep(Duration::from_secs(1));

    // Test 1: Navigate down
    eprintln!("📝 Master: Test 1 - Navigate down");
    main_page.navigate_down().expect("Failed to navigate down");
    std::thread::sleep(Duration::from_millis(200));

    main_page.navigate_down().expect("Failed to navigate down");
    std::thread::sleep(Duration::from_millis(200));
    eprintln!("  ✓ Navigated down 2 times");

    // Test 2: Toggle completion
    eprintln!("📝 Master: Test 2 - Toggle completion");
    let before_checked = main_page.count_checked();
    eprintln!("  Before toggle: {} checked items", before_checked);

    main_page.toggle_completion().expect("Failed to toggle");
    std::thread::sleep(Duration::from_millis(300));

    let after_checked = main_page.count_checked();
    eprintln!("  After toggle: {} checked items", after_checked);

    // Verify toggle worked (count should change)
    assert_ne!(
        before_checked, after_checked,
        "Toggle should change checked count"
    );
    eprintln!("  ✓ Toggle changed completion status");

    // Test 3: Navigate up
    eprintln!("📝 Master: Test 3 - Navigate up");
    main_page.navigate_up().expect("Failed to navigate up");
    std::thread::sleep(Duration::from_millis(200));
    eprintln!("  ✓ Navigated up");

    // Test 4: Check status messages
    eprintln!("📝 Master: Test 4 - Status messages");
    main_page.press_char('r').expect("Failed to press 'r'");
    std::thread::sleep(Duration::from_millis(200));

    if main_page.status_contains("refresh") || main_page.status_contains("CDC") {
        eprintln!("  ✓ Status message updated");
    } else {
        eprintln!("  ⚠ Status message not found (may be normal)");
    }

    // Quit the app
    eprintln!("📝 Master: Quitting app");
    main_page.quit().expect("Failed to quit");

    // Wait for clean exit
    main_page.wait_for_exit().expect("Failed to wait for exit");

    eprintln!("✅ Master: Navigation test completed successfully");
}

/// Slave process - runs the actual TUI application
fn run_tui_app_slave() -> ! {
    use std::io::Write;

    // Set up a simple tokio runtime for the TUI
    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");

    rt.block_on(async {
        // Try to run the app with a timeout
        let result = tokio::time::timeout(Duration::from_secs(10), run_simple_tui()).await;

        match result {
            Ok(Ok(())) => {
                println!("TUI_EXITED_NORMALLY");
                std::io::stdout().flush().unwrap();
            }
            Ok(Err(e)) => {
                eprintln!("TUI error: {}", e);
                println!("TUI_ERROR");
                std::io::stdout().flush().unwrap();
            }
            Err(_) => {
                eprintln!("TUI timeout");
                println!("TUI_TIMEOUT");
                std::io::stdout().flush().unwrap();
            }
        }
    });

    std::process::exit(0);
}

/// Simple TUI runner for testing
async fn run_simple_tui() -> Result<(), Box<dyn std::error::Error>> {
    use holon::api::backend_engine::BackendEngine;
    use holon::storage::gen_key_between;
    use r3bl_tui::TerminalWindow;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tui_r3bl_frontend::{app_main::AppMain, state::State};

    let app = AppMain::new_boxed();

    // Initialize BackendEngine with in-memory database
    let mut engine = BackendEngine::new_in_memory().await?;

    // Create blocks table
    let create_table_sql = r#"
        CREATE TABLE IF NOT EXISTS blocks (
            id TEXT PRIMARY KEY,
            parent_id TEXT,
            depth INTEGER NOT NULL DEFAULT 0,
            sort_key TEXT NOT NULL,
            content TEXT NOT NULL,
            collapsed INTEGER NOT NULL DEFAULT 0,
            completed INTEGER NOT NULL DEFAULT 0,
            block_type TEXT NOT NULL DEFAULT 'text',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    "#;

    engine
        .execute_query(create_table_sql.to_string(), HashMap::new())
        .await?;

    // Insert minimal sample data for testing
    let key1 = gen_key_between(None, None)?;
    let key2 = gen_key_between(Some(&key1), None)?;
    let key3 = gen_key_between(Some(&key2), None)?;

    let sample_data_sql = format!(
        r#"
        INSERT OR IGNORE INTO blocks (id, parent_id, depth, sort_key, content, block_type, completed)
        VALUES
            ('test-1', NULL, 0, '{}', 'First task', 'text', 0),
            ('test-2', NULL, 0, '{}', 'Second task', 'text', 0),
            ('test-3', NULL, 0, '{}', 'Third task', 'text', 1)
    "#,
        key1, key2, key3
    );

    engine
        .execute_query(sample_data_sql.to_string(), HashMap::new())
        .await?;

    // PRQL query with render spec
    let prql_query = r#"
from blocks
select {
    id,
    parent_id,
    depth,
    sort_key,
    content,
    completed,
    block_type,
    collapsed
}
render (list hierarchical_sort:[parent_id, sort_key] item_template:(row (checkbox checked:this.completed) (editable_text content:this.content)))
"#.to_string();

    let (render_spec, initial_data, cdc_stream) =
        engine.query_and_watch(prql_query, HashMap::new()).await?;

    let engine = Arc::new(RwLock::new(engine));

    // Create CDC channel
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let cdc_receiver = Arc::new(std::sync::Mutex::new(rx));

    // Forward CDC stream
    tokio::spawn(async move {
        use tokio_stream::StreamExt;
        let mut stream = cdc_stream;
        while let Some(change) = stream.next().await {
            let _ = tx.send(change);
        }
    });

    let keybindings = Arc::new(KeyBindingConfig::empty());

    // Create initial state
    let state = State::new(engine, render_spec, initial_data, cdc_receiver, keybindings);

    // Run the terminal window with Ctrl+q as exit key
    use r3bl_tui::{InputEvent, Key, KeyPress, KeyState};
    let exit_keys = &[InputEvent::Keyboard(KeyPress::WithModifiers {
        key: Key::Character('q'),
        mask: r3bl_tui::ModifierKeysMask {
            ctrl_key_state: KeyState::Pressed,
            shift_key_state: KeyState::NotPressed,
            alt_key_state: KeyState::NotPressed,
        },
    })];

    let result = TerminalWindow::main_event_loop(app, exit_keys, state)?;
    result.await?;

    Ok(())
}
