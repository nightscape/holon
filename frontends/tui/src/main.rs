mod app_main;
mod components;
mod config;
mod launcher;
mod render_interpreter;
mod state;
mod stylesheet;
mod ui_element;

use launcher::run_app;
use r3bl_tui::{log::try_initialize_logging_global, CommonResult};
use std::fs::OpenOptions;
use std::path::PathBuf;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> CommonResult<()> {
    // Disable r3bl logging to prevent breaking TUI display
    try_initialize_logging_global(tracing_core::LevelFilter::OFF).ok();

    // Set up file-based logging for application logs (Todoist sync, etc.)
    // Logs go to ~/.config/tui/tui.log or ./tui.log
    let log_file_path = if let Some(home) = std::env::var_os("HOME") {
        let mut path = PathBuf::from(home);
        path.push(".config");
        path.push("tui");
        std::fs::create_dir_all(&path).ok();
        path.push("tui.log");
        path
    } else {
        PathBuf::from("tui.log")
    };

    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file_path)
        .unwrap_or_else(|_| {
            // Fallback to stderr if file can't be opened
            eprintln!(
                "Warning: Could not open log file {:?}, logging to stderr",
                log_file_path
            );
            std::fs::File::create("tui.log").unwrap()
        });

    // Initialize tracing subscriber that writes to file
    // Default to INFO level, can be overridden with RUST_LOG env var
    // Suppress Turso's verbose TRACE/DEBUG logs by default to prevent log spam
    // Users can override by setting RUST_LOG=turso_core=debug if needed
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
        // Add Turso filters after env filter so they can be overridden if needed
        .add_directive("turso_core=warn".parse().unwrap())
        .add_directive("turso_core::storage=warn".parse().unwrap())
        .add_directive("turso_core::vdbe=warn".parse().unwrap());

    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer().with_writer(log_file).with_ansi(false), // Disable ANSI colors for file output
        )
        .init();

    // Parse command-line arguments
    let mut args = std::env::args().skip(1);
    let mut db_path = PathBuf::from("blocks.db");
    let mut keybindings_path: Option<PathBuf> = None;

    // Simple argument parsing: --keybindings <path> or <db_path>
    while let Some(arg) = args.next() {
        if arg == "--keybindings" || arg == "-k" {
            if let Some(path) = args.next() {
                keybindings_path = Some(PathBuf::from(path));
            }
        } else if !arg.starts_with('-') {
            // Positional argument is database path
            db_path = PathBuf::from(arg);
        }
    }

    // Check environment variable if not provided via CLI
    if keybindings_path.is_none() {
        if let Ok(env_path) = std::env::var("TUI_R3BL_KEYBINDINGS") {
            keybindings_path = Some(PathBuf::from(env_path));
        }
    }

    // Default to ~/.config/tui/keybindings.yaml if still not set
    if keybindings_path.is_none() {
        if let Some(home) = std::env::var_os("HOME") {
            let mut default_path = PathBuf::from(home);
            default_path.push(".config");
            default_path.push("tui");
            default_path.push("keybindings.yaml");
            if default_path.exists() {
                keybindings_path = Some(default_path);
            }
        }
    }

    run_app(db_path, keybindings_path).await
}
