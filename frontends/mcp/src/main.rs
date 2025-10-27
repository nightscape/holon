use anyhow::Result;
use rmcp::ServiceExt;
use std::fs::OpenOptions;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_subscriber::{self, fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod resources;
mod server;
mod telemetry;
mod tools;
mod types;

use server::HolonMcpServer;

/// Create a default EnvFilter that suppresses noisy HTTP client and OpenTelemetry logs
fn default_env_filter() -> EnvFilter {
    // Some crates use dashes in target names, others use underscores - filter both variants
    EnvFilter::new(
        "info,\
         reqwest=warn,\
         hyper=warn,\
         hyper_util=warn,\
         h2=warn,\
         tower=warn,\
         opentelemetry=warn,\
         opentelemetry_sdk=warn,\
         opentelemetry_http=warn,\
         opentelemetry_otlp=warn,\
         opentelemetry-sdk=warn,\
         opentelemetry-http=warn,\
         opentelemetry-otlp=warn,\
         holon=debug",
    )
}

#[derive(Debug, Clone)]
enum TransportMode {
    Stdio,
    Http { bind_address: SocketAddr },
}

struct Config {
    db_path: PathBuf,
    transport_mode: TransportMode,
    orgmode_root: Option<PathBuf>,
    orgmode_loro_dir: Option<PathBuf>,
    loro_enabled: bool,
}

fn parse_args() -> Result<Config> {
    let mut args = std::env::args().skip(1);
    let mut db_path = PathBuf::from(":memory:");
    let mut transport_mode = TransportMode::Stdio;
    let mut orgmode_root: Option<PathBuf> = None;
    let mut orgmode_loro_dir: Option<PathBuf> = None;
    let mut loro_enabled = std::env::var("HOLON_LORO_ENABLED")
        .map(|v| !v.is_empty() && v != "0" && v.to_lowercase() != "false")
        .unwrap_or(false);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--http" | "-H" => {
                let addr_str = args.next().unwrap_or_else(|| "127.0.0.1:8000".to_string());
                let addr: SocketAddr = addr_str
                    .parse()
                    .map_err(|e| anyhow::anyhow!("Invalid bind address '{}': {}", addr_str, e))?;
                transport_mode = TransportMode::Http { bind_address: addr };
            }
            "--stdio" | "-S" => {
                transport_mode = TransportMode::Stdio;
            }
            "--orgmode-root" | "--orgmode-dir" => {
                let path_str = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--orgmode-root requires a path argument"))?;
                orgmode_root = Some(PathBuf::from(path_str));
            }
            "--orgmode-loro-dir" => {
                let path_str = args.next().ok_or_else(|| {
                    anyhow::anyhow!("--orgmode-loro-dir requires a path argument")
                })?;
                orgmode_loro_dir = Some(PathBuf::from(path_str));
            }
            "--loro" => {
                loro_enabled = true;
            }
            "--help" | "-h" => {
                // Write help to stderr to avoid interfering with stdout in stdio mode
                eprintln!("Usage: holon-mcp [OPTIONS] [DATABASE_PATH]");
                eprintln!();
                eprintln!("Options:");
                eprintln!(
                    "  --http, -H [ADDRESS]         Run HTTP server (default: 127.0.0.1:8000)"
                );
                eprintln!("  --stdio, -S                  Run stdio server (default)");
                eprintln!("  --orgmode-root PATH          OrgMode root directory (required for OrgMode features)");
                eprintln!("  --orgmode-loro-dir PATH      OrgMode Loro storage directory (default: {{orgmode-root}}/.loro)");
                eprintln!("  --help, -h                   Show this help message");
                eprintln!();
                eprintln!("Examples:");
                eprintln!(
                    "  holon-mcp                                    # stdio mode with in-memory DB"
                );
                eprintln!(
                    "  holon-mcp /path/to/db.db                      # stdio mode with file DB"
                );
                eprintln!(
                    "  holon-mcp --http                              # HTTP mode on 127.0.0.1:8000"
                );
                eprintln!("  holon-mcp --orgmode-root /path/to/org         # Enable OrgMode with root directory");
                eprintln!("  holon-mcp --orgmode-root /org --orgmode-loro-dir /custom/loro  # Custom Loro storage");
                std::process::exit(0);
            }
            _ => {
                // Treat as database path if it doesn't start with --
                if !arg.starts_with("--") {
                    db_path = PathBuf::from(arg);
                }
            }
        }
    }

    Ok(Config {
        db_path,
        transport_mode,
        orgmode_root,
        orgmode_loro_dir,
        loro_enabled,
    })
}

async fn run_stdio_server(
    engine: std::sync::Arc<holon::api::backend_engine::BackendEngine>,
) -> Result<()> {
    let server = HolonMcpServer::new(engine);
    use rmcp::transport::stdio;
    let running = server.serve(stdio()).await?;

    // Wait for the connection to close
    // This returns Result<QuitReason, JoinError>
    // QuitReason indicates why the server quit (e.g., connection closed, error, etc.)
    // Note: Connection closed errors are expected when stdin closes and should be handled gracefully
    if let Err(join_err) = running.waiting().await {
        // The background task errored
        // Check if it's a panic
        if join_err.is_panic() {
            return Err(anyhow::anyhow!("MCP server task panicked"));
        }
        // For JoinError, check if it's a connection closed error
        // Connection closed is expected when stdin closes, so we should exit cleanly
        let error_msg = format!("{}", join_err).to_lowercase();
        if error_msg.contains("connection closed")
            || error_msg.contains("connectionclosed")
            || error_msg.contains("closed")
        {
            // This is expected - stdin was closed, server should exit cleanly
            // Don't treat this as an error
            return Ok(());
        }
        // For other errors, convert to anyhow::Error
        return Err(anyhow::anyhow!("MCP server error: {}", join_err));
    }
    // Server quit normally (Ok(QuitReason))
    Ok(())
}

async fn run_http_server_standalone(
    engine: std::sync::Arc<holon::api::backend_engine::BackendEngine>,
    bind_address: SocketAddr,
) -> Result<()> {
    use tokio_util::sync::CancellationToken;

    // Create cancellation token that will be cancelled on Ctrl+C
    let cancellation_token = CancellationToken::new();
    let token_for_signal = cancellation_token.clone();

    // Spawn a task to handle Ctrl+C
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Received Ctrl+C, shutting down HTTP server...");
        token_for_signal.cancel();
    });

    tracing::info!("Holon MCP HTTP server starting on http://{}", bind_address);
    tracing::info!("MCP endpoint: http://{}/mcp", bind_address);

    // Use the shared run_http_server from di module
    holon_mcp::di::run_http_server(engine, bind_address, cancellation_token).await
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse arguments first to determine transport mode
    let config = parse_args()?;

    // Configure logging based on transport mode
    match config.transport_mode {
        TransportMode::Stdio => {
            // In stdio mode, write all logs to a file to avoid interfering with protocol communication
            // Determine log file path
            let log_file_path = std::env::var("HOLON_MCP_LOG_FILE")
                .map(PathBuf::from)
                .unwrap_or_else(|_| {
                    // Default to temp directory with timestamp
                    let mut path = std::env::temp_dir();
                    // Use system time for timestamp
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    path.push(format!("holon-mcp-{}.log", timestamp));
                    path
                });

            // Create log file
            let log_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_file_path)
                .map_err(|e| {
                    anyhow::anyhow!("Failed to create log file at {:?}: {}", log_file_path, e)
                })?;

            // Configure log level - use default filter if RUST_LOG not set
            let log_level =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| default_env_filter());

            // Build subscriber with all layers
            let registry = tracing_subscriber::registry();

            // Initialize OpenTelemetry providers if enabled
            let otel_enabled = std::env::var("OTEL_TRACES_EXPORTER").is_ok()
                || std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok();
            if otel_enabled {
                match telemetry::init_opentelemetry() {
                    Ok(()) => {
                        // Add OpenTelemetry layer
                        let telemetry_layer = telemetry::create_opentelemetry_layer();
                        registry
                            .with(telemetry_layer)
                            .with(log_level)
                            .with(
                                fmt::layer()
                                    .with_writer(log_file)
                                    .with_ansi(false)
                                    .with_target(true)
                                    .with_thread_ids(true)
                                    .with_file(true)
                                    .with_line_number(true),
                            )
                            .init();
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to initialize OpenTelemetry: {}", e);
                        eprintln!("Continuing without OpenTelemetry support");
                        registry
                            .with(log_level)
                            .with(
                                fmt::layer()
                                    .with_writer(log_file)
                                    .with_ansi(false)
                                    .with_target(true)
                                    .with_thread_ids(true)
                                    .with_file(true)
                                    .with_line_number(true),
                            )
                            .init();
                    }
                }
            } else {
                // Add EnvFilter and fmt layer (no OpenTelemetry)
                registry
                    .with(log_level)
                    .with(
                        fmt::layer()
                            .with_writer(log_file)
                            .with_ansi(false)
                            .with_target(true)
                            .with_thread_ids(true)
                            .with_file(true)
                            .with_line_number(true),
                    )
                    .init();
            }

            // Write log file location to stderr once (before protocol starts)
            eprintln!("Holon MCP server started in stdio mode");
            eprintln!("Logs are being written to: {}", log_file_path.display());
            eprintln!("Set HOLON_MCP_LOG_FILE to specify a custom log file location");
        }
        TransportMode::Http { .. } => {
            // In HTTP mode, normal stderr logging is fine
            let log_level =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| default_env_filter());

            // Build subscriber with all layers
            let registry = tracing_subscriber::registry();

            // Initialize OpenTelemetry providers if enabled
            let otel_enabled = std::env::var("OTEL_TRACES_EXPORTER").is_ok()
                || std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok();
            if otel_enabled {
                match telemetry::init_opentelemetry() {
                    Ok(()) => {
                        // Add OpenTelemetry layer
                        let telemetry_layer = telemetry::create_opentelemetry_layer();
                        registry
                            .with(telemetry_layer)
                            .with(log_level)
                            .with(fmt::layer().with_writer(std::io::stderr).with_ansi(false))
                            .init();
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to initialize OpenTelemetry: {}", e);
                        eprintln!("Continuing without OpenTelemetry support");
                        registry
                            .with(log_level)
                            .with(fmt::layer().with_writer(std::io::stderr).with_ansi(false))
                            .init();
                    }
                }
            } else {
                // Add EnvFilter and fmt layer (no OpenTelemetry)
                registry
                    .with(log_level)
                    .with(fmt::layer().with_writer(std::io::stderr).with_ansi(false))
                    .init();
            }
        }
    }

    // Build FrontendConfig from CLI config
    let mut frontend_config = holon_frontend::FrontendConfig::new()
        .with_db_path(config.db_path)
        .without_wait();

    if let Some(root) = config.orgmode_root {
        frontend_config = frontend_config.with_orgmode(root);
    }
    if config.loro_enabled {
        frontend_config = frontend_config.with_loro();
    }
    if let Some(loro_dir) = config.orgmode_loro_dir {
        frontend_config = frontend_config.with_loro_storage(loro_dir);
    }

    let session = holon_frontend::FrontendSession::new(frontend_config).await?;
    let engine = session.engine().clone();

    // Run server based on transport mode
    match config.transport_mode {
        TransportMode::Stdio => {
            // In stdio mode, don't log startup message to avoid interfering with protocol
            // The server will start silently and communicate via stdout
            run_stdio_server(engine).await?;
        }
        TransportMode::Http { bind_address } => {
            tracing::info!("Starting Holon MCP server in HTTP mode on {}", bind_address);
            run_http_server_standalone(engine, bind_address).await?;
        }
    }

    Ok(())
}
