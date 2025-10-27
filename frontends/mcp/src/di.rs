//! Dependency Injection module for MCP server
//!
//! This module provides DI integration for embedding the MCP server within
//! applications that use Ferrous DI. The MCP server shares the same BackendEngine
//! instance as the host application, enabling shared undo/redo, CDC streams, and operations.
//!
//! # Usage
//!
//! ```rust,ignore
//! use holon_mcp::di::McpServiceCollectionExt;
//!
//! let engine = holon::di::create_backend_engine(db_path, |services| {
//!     // Register MCP server on port 8000
//!     services.add_mcp_server(8000)?;
//!     Ok(())
//! }).await?;
//!
//! // Start the server
//! let mcp_handle = provider.get_required::<McpServerHandle>();
//! mcp_handle.start().await?;
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use ferrous_di::{DiResult, Resolver, ServiceCollection, ServiceModule};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use holon::api::backend_engine::BackendEngine;

use crate::server::HolonMcpServer;

/// Configuration for the MCP server
#[derive(Clone, Debug)]
pub struct McpServerConfig {
    /// Address to bind the HTTP server to
    pub bind_address: SocketAddr,
}

impl McpServerConfig {
    /// Create a new MCP server configuration
    pub fn new(port: u16) -> Self {
        Self {
            bind_address: ([127, 0, 0, 1], port).into(),
        }
    }

    /// Create configuration with a custom bind address
    pub fn with_address(bind_address: SocketAddr) -> Self {
        Self { bind_address }
    }
}

/// Handle for managing MCP server lifecycle
///
/// This struct provides methods to start and stop the MCP HTTP server.
/// The server shares the same BackendEngine as the host application.
pub struct McpServerHandle {
    config: McpServerConfig,
    engine: Arc<BackendEngine>,
    state: Mutex<ServerState>,
}

struct ServerState {
    task: Option<JoinHandle<()>>,
    cancellation_token: Option<CancellationToken>,
}

impl McpServerHandle {
    /// Create a new MCP server handle
    pub fn new(config: McpServerConfig, engine: Arc<BackendEngine>) -> Self {
        Self {
            config,
            engine,
            state: Mutex::new(ServerState {
                task: None,
                cancellation_token: None,
            }),
        }
    }

    /// Start the MCP HTTP server
    ///
    /// Returns an error if the server is already running.
    pub async fn start(&self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;

        if state.task.is_some() {
            anyhow::bail!("MCP server is already running");
        }

        let engine = self.engine.clone();
        let bind_address = self.config.bind_address;
        let cancellation_token = CancellationToken::new();
        let token_for_task = cancellation_token.clone();

        let task = tokio::spawn(async move {
            if let Err(e) = run_http_server(engine, bind_address, token_for_task).await {
                tracing::error!("MCP server error: {}", e);
            }
        });

        state.task = Some(task);
        state.cancellation_token = Some(cancellation_token);

        tracing::info!("MCP server started on http://{}", self.config.bind_address);
        Ok(())
    }

    /// Stop the MCP HTTP server
    ///
    /// Returns Ok(()) if the server was stopped or wasn't running.
    pub async fn stop(&self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;

        if let Some(token) = state.cancellation_token.take() {
            token.cancel();
        }

        if let Some(task) = state.task.take() {
            // Wait for the task to finish with a timeout
            match tokio::time::timeout(std::time::Duration::from_secs(5), task).await {
                Ok(Ok(())) => tracing::info!("MCP server stopped gracefully"),
                Ok(Err(e)) => tracing::warn!("MCP server task panicked: {}", e),
                Err(_) => tracing::warn!("MCP server stop timed out"),
            }
        }

        Ok(())
    }

    /// Check if the server is running
    pub async fn is_running(&self) -> bool {
        let state = self.state.lock().await;
        state.task.is_some()
    }

    /// Get the bind address
    pub fn bind_address(&self) -> SocketAddr {
        self.config.bind_address
    }
}

/// Run the MCP HTTP server
///
/// This is the core server loop, extracted for reuse by both the standalone binary
/// and the DI-managed server handle.
pub async fn run_http_server(
    engine: Arc<BackendEngine>,
    bind_address: SocketAddr,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    use axum::{response::Html, routing::get, Router};
    use rmcp::transport::{
        streamable_http_server::{
            session::local::LocalSessionManager, tower::StreamableHttpService,
        },
        StreamableHttpServerConfig,
    };

    let cancellation_token_for_service = cancellation_token.clone();

    // Create streamable HTTP service
    let mcp_service: StreamableHttpService<HolonMcpServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(HolonMcpServer::new(engine.clone())),
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig {
                sse_keep_alive: Some(std::time::Duration::from_secs(15)),
                stateful_mode: true,
                cancellation_token: cancellation_token_for_service,
            },
        );

    // Create index page
    const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html>
<head>
    <title>Holon MCP Server</title>
    <style>
        body { font-family: sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
        code { background: #f4f4f4; padding: 2px 6px; border-radius: 3px; }
        pre { background: #f4f4f4; padding: 15px; border-radius: 5px; overflow-x: auto; }
    </style>
</head>
<body>
    <h1>Holon MCP Server</h1>
    <p>Model Context Protocol server for Holon backend engine.</p>

    <h2>MCP Endpoint</h2>
    <p>The MCP endpoint is available at: <code>/mcp</code></p>

    <h2>Available Tools</h2>
    <ul>
        <li><code>create_table</code> - Create database tables</li>
        <li><code>insert_data</code> - Insert rows into tables</li>
        <li><code>drop_table</code> - Drop tables</li>
        <li><code>execute_prql</code> - Execute PRQL queries</li>
        <li><code>execute_sql</code> - Execute SQL queries</li>
        <li><code>watch_query</code> - Watch queries for changes</li>
        <li><code>poll_changes</code> - Poll for CDC changes</li>
        <li><code>stop_watch</code> - Stop watching a query</li>
        <li><code>execute_operation</code> - Execute entity operations</li>
        <li><code>list_operations</code> - List available operations</li>
        <li><code>undo</code> / <code>redo</code> - Undo/redo operations</li>
        <li><code>can_undo</code> / <code>can_redo</code> - Check undo/redo availability</li>
    </ul>
</body>
</html>"#;

    async fn index() -> Html<&'static str> {
        Html(INDEX_HTML)
    }

    async fn health_check() -> &'static str {
        "OK"
    }

    // Create router
    let app = Router::new()
        .route("/", get(index))
        .route("/health", get(health_check))
        .nest_service("/mcp", mcp_service);

    // Start HTTP server
    let listener = tokio::net::TcpListener::bind(bind_address).await?;
    tracing::info!("Holon MCP HTTP server listening on http://{}", bind_address);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancellation_token.cancelled().await;
            tracing::info!("MCP server shutting down...");
        })
        .await?;

    Ok(())
}

/// DI module for MCP server
///
/// This module registers the MCP server handle as a singleton service.
/// The handle receives the BackendEngine from DI, ensuring it shares
/// the same instance as the host application.
pub struct McpServerModule;

impl ServiceModule for McpServerModule {
    fn register_services(self, services: &mut ServiceCollection) -> DiResult<()> {
        services.add_singleton_factory::<McpServerHandle, _>(|resolver| {
            let config = resolver.get_required::<McpServerConfig>();
            let engine = resolver.get_required::<BackendEngine>();

            McpServerHandle::new((*config).clone(), engine)
        });

        Ok(())
    }
}

/// Extension trait for registering MCP server services in a [`ServiceCollection`]
///
/// This trait provides a convenient method to register the MCP server
/// with a single call, taking just the port as a parameter.
///
/// # Example
///
/// ```rust,ignore
/// use holon_mcp::di::McpServiceCollectionExt;
///
/// services.add_mcp_server(8000)?;
/// ```
pub trait McpServiceCollectionExt {
    /// Register MCP server services with the given port
    ///
    /// This registers:
    /// - `McpServerConfig` - Server configuration
    /// - `McpServerHandle` - Server lifecycle handle (via McpServerModule)
    ///
    /// After registration, resolve `McpServerHandle` and call `start()` to run the server.
    fn add_mcp_server(&mut self, port: u16) -> DiResult<()>;

    /// Register MCP server services with a custom configuration
    fn add_mcp_server_with_config(&mut self, config: McpServerConfig) -> DiResult<()>;
}

impl McpServiceCollectionExt for ServiceCollection {
    fn add_mcp_server(&mut self, port: u16) -> DiResult<()> {
        self.add_mcp_server_with_config(McpServerConfig::new(port))
    }

    fn add_mcp_server_with_config(&mut self, config: McpServerConfig) -> DiResult<()> {
        use ferrous_di::ServiceCollectionModuleExt;
        self.add_singleton(config);
        self.add_module_mut(McpServerModule)?;
        Ok(())
    }
}
