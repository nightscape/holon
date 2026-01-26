//! FFI bridge functions for Flutter
//!
//! This module provides a minimal FFI surface exposing only FrontendSession and essential types.
//! Low-level holon_prql_render types (Expr, ModuleDef, Lineage) are hidden as implementation details.
//!
//! ## Architecture: FrontendSession as the Primary API
//!
//! All query and operation methods are accessed through `FrontendSession`, not `BackendEngine`.
//! This design guarantees that:
//!
//! 1. **No initialization race conditions**: `FrontendSession::new()` completes all schema
//!    initialization (including materialized views like `blocks_with_paths`) before returning.
//!    Since all query methods are on `FrontendSession`, they can only be called after init.
//!
//! 2. **Identical code paths**: Flutter and E2E tests use the exact same API surface
//!    (`FrontendSession`), eliminating bugs that only appear in production.
//!
//! 3. **Type-safe ordering**: It's impossible to call `initial_widget()` before
//!    `init_render_engine()` completes - the compiler enforces this.

use crate::api::types::TraceContext;
use crate::frb_generated::StreamSink;
use flutter_rust_bridge::frb;
pub use holon_api::{BatchMapChange, BatchMapChangeWithMetadata, MapChange};
use holon_api::{OperationDescriptor, Value, WidgetSpec};
use holon_frontend::FrontendSession;
use once_cell::sync::OnceCell;
use opentelemetry::global;
use opentelemetry::trace::{Span, Tracer};
use opentelemetry::Context;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;

// Re-export types needed by generated code
pub use holon_api::Change;

// Global singleton to store the session (NOT the engine directly)
// This prevents Flutter Rust Bridge from disposing it during async operations
// and guarantees all methods are called after initialization completes.
static GLOBAL_SESSION: OnceCell<Arc<FrontendSession>> = OnceCell::new();

/// Create a default EnvFilter that suppresses noisy HTTP client and OpenTelemetry logs
fn default_env_filter() -> tracing_subscriber::EnvFilter {
    // Some crates use dashes in target names, others use underscores - filter both variants
    tracing_subscriber::EnvFilter::new(
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
         holon=debug,\
         rust_lib_holon=debug",
    )
}

/// Create an OpenTelemetry span from optional trace context
///
/// If trace_context is provided, creates a child span. Otherwise creates a new root span.
fn create_span_from_context(
    name: &'static str,
    trace_context: Option<TraceContext>,
) -> impl opentelemetry::trace::Span {
    // Use service name from env or default - convert to static string
    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "holon-backend".to_string());
    let service_name_static: &'static str = Box::leak(service_name.into_boxed_str());
    let tracer = global::tracer(service_name_static);

    if let Some(ctx) = trace_context {
        if let Some(span_ctx) = ctx.to_span_context() {
            // Create child span from provided context
            // Use Context::current() and attach span context
            use opentelemetry::trace::TraceContextExt;
            let parent_ctx = Context::current().with_remote_span_context(span_ctx);
            tracer.start_with_context(name, &parent_ctx)
        } else {
            // Invalid context, create new root span
            tracer.start(name)
        }
    } else {
        // No context provided, create new root span
        tracer.start(name)
    }
}

use holon::storage::turso::RowChangeStream;

/// Spawn a task to forward CDC stream events to a Flutter sink
///
/// This is shared between `query_and_watch` and `initial_widget`.
fn spawn_stream_forwarder(mut stream: RowChangeStream, sink: MapChangeSink) {
    tokio::spawn(async move {
        use tracing::{debug, info, warn, Instrument};

        let forwarding_span = tracing::span!(tracing::Level::INFO, "ffi.stream_forwarding");
        let _guard = forwarding_span.enter();

        info!("[FFI] Stream forwarding task started");
        while let Some(batch_with_metadata) = stream.next().await {
            let change_count = batch_with_metadata.inner.items.len();
            let relation_name = batch_with_metadata.metadata.relation_name.clone();
            let trace_context = batch_with_metadata.metadata.trace_context.clone();

            // Count change types
            let mut created_count = 0;
            let mut updated_count = 0;
            let mut deleted_count = 0;
            for row_change in &batch_with_metadata.inner.items {
                match &row_change.change {
                    MapChange::Created { .. } => created_count += 1,
                    MapChange::Updated { .. } => updated_count += 1,
                    MapChange::Deleted { .. } => deleted_count += 1,
                    MapChange::FieldsChanged { .. } => updated_count += 1,
                }
            }

            // Create span for batch forwarding, linked to the original trace if available
            let batch_span = tracing::span!(
                tracing::Level::INFO,
                "ffi.batch_forward",
                "batch.relation_name" = %relation_name,
                "batch.change_count" = change_count,
                "batch.created_count" = created_count,
                "batch.updated_count" = updated_count,
                "batch.deleted_count" = deleted_count,
            );

            // If we have trace context from the batch, set it as the parent context
            if let Some(ref trace_ctx) = trace_context {
                use opentelemetry::trace::{
                    SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
                };
                use tracing_opentelemetry::OpenTelemetrySpanExt;

                if let (Ok(trace_id_bytes), Ok(span_id_bytes)) = (
                    u128::from_str_radix(&trace_ctx.trace_id, 16),
                    u64::from_str_radix(&trace_ctx.span_id, 16),
                ) {
                    let parent_span_context = SpanContext::new(
                        TraceId::from(trace_id_bytes),
                        SpanId::from(span_id_bytes),
                        TraceFlags::new(trace_ctx.trace_flags),
                        true, // is_remote = true since this came from another span
                        TraceState::default(),
                    );

                    if parent_span_context.is_valid() {
                        let parent_context = opentelemetry::Context::new()
                            .with_remote_span_context(parent_span_context);
                        let _ = batch_span.set_parent(parent_context);
                    }
                }
            }

            let _batch_guard = batch_span.enter();

            info!(
                "[FFI] Received batch from stream: {} changes, relation={}, trace_context={:?}",
                change_count, relation_name, trace_context
            );

            // Extract metadata before converting batch
            let metadata = batch_with_metadata.metadata.clone();

            // Convert Batch<RowChange> to Batch<MapChange>
            let map_changes: Vec<MapChange> = batch_with_metadata
                .inner
                .items
                .into_iter()
                .map(|row_change| row_change.change)
                .collect();

            let batch_map_change = BatchMapChange { items: map_changes };

            let batch_map_change_with_metadata = BatchMapChangeWithMetadata {
                inner: batch_map_change,
                metadata,
            };

            if sink.sink.add(batch_map_change_with_metadata).is_err() {
                warn!("[FFI] Sink closed, stopping stream forwarding");
                break;
            }
            debug!("[FFI] Forwarded batch to sink");
        }
        warn!("[FFI] CDC stream ended — sink will be dropped, signaling stream end to Flutter");
        drop(sink);
    });
}

/// Initialize OpenTelemetry tracing and logging
///
/// Sets up OTLP and stdout exporters based on environment variables.
/// Bridges tracing to OpenTelemetry so existing tracing spans appear in traces.
/// Also bridges tracing logs to OpenTelemetry logs for log export.
async fn init_opentelemetry() -> anyhow::Result<()> {
    use opentelemetry::global;
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::Resource;
    use std::env;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Registry;

    // Get service name from env or use default
    let service_name =
        env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "holon-backend".to_string());

    // Determine which exporters to use
    let exporter_type =
        env::var("OTEL_TRACES_EXPORTER").unwrap_or_else(|_| "stdout,otlp".to_string());

    // Create resource with service name
    // In 0.31, Resource uses builder pattern: Resource::builder_empty().with_attributes().build()
    let resource = Resource::builder_empty()
        .with_attributes(vec![KeyValue::new("service.name", service_name.clone())])
        .build();

    // Set up trace provider and log provider
    if exporter_type.contains("otlp") {
        let otlp_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:4318".to_string());

        // Remove trailing slash if present
        let base_endpoint = otlp_endpoint.trim_end_matches('/').to_string();

        // Traces endpoint: /v1/traces
        let traces_endpoint = format!("{}/v1/traces", base_endpoint);
        // Logs endpoint: /v1/logs
        let logs_endpoint = format!("{}/v1/logs", base_endpoint);

        eprintln!("[FFI] Initializing OpenTelemetry OTLP exporters:");
        eprintln!("[FFI]   Traces endpoint: {}", traces_endpoint);
        eprintln!("[FFI]   Logs endpoint: {}", logs_endpoint);

        // Use OTLP exporter builder (0.31 API)
        use opentelemetry_otlp::WithExportConfig;

        // Create OTLP trace exporter using builder - use with_http() to set HTTP protocol
        let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(traces_endpoint)
            .build()?;

        // Create OTLP log exporter
        let log_exporter = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_endpoint(logs_endpoint)
            .build()?;

        // Set up trace provider
        use opentelemetry_sdk::trace::SdkTracerProvider;
        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(trace_exporter)
            .with_resource(resource.clone())
            .build();

        global::set_tracer_provider(tracer_provider);

        // Set up log provider
        use opentelemetry_sdk::logs::SdkLoggerProvider;
        let logger_provider = SdkLoggerProvider::builder()
            .with_batch_exporter(log_exporter)
            .with_resource(resource.clone())
            .build();

        // Convert service_name to static string for tracer
        let service_name_static: &'static str = Box::leak(service_name.clone().into_boxed_str());
        let tracer = global::tracer(service_name_static);

        // Bridge tracing spans to OpenTelemetry traces
        let telemetry_layer = tracing_opentelemetry::OpenTelemetryLayer::new(tracer);

        // Bridge tracing logs to OpenTelemetry logs
        // Filter to only include actual log events (info!, debug!, warn!, error!), not span lifecycle events
        use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
        use tracing_subscriber::filter::{FilterFn, Filtered};

        // Only process events (not spans) - span lifecycle events are handled by telemetry_layer
        let log_filter = FilterFn::new(|metadata| {
            // Only process events, not spans
            metadata.is_event()
        });

        let log_bridge = Filtered::new(
            OpenTelemetryTracingBridge::new(&logger_provider),
            log_filter,
        );

        // Combine with existing fmt layer
        let subscriber = Registry::default()
            .with(telemetry_layer)
            .with(log_bridge)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_ansi(false),
            )
            .with(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| default_env_filter()),
            );

        // Initialize subscriber (idempotent)
        let _ = tracing::subscriber::set_global_default(subscriber);

        eprintln!("[FFI] OpenTelemetry tracing and logging initialized with OTLP exporters");
    } else {
        // Use stdout exporters only
        use opentelemetry_stdout::{LogExporter, SpanExporter};
        let stdout_trace_exporter = SpanExporter::default();
        let stdout_log_exporter = LogExporter::default();

        eprintln!("[FFI] Initializing OpenTelemetry stdout exporters");

        // Set up trace provider
        use opentelemetry_sdk::trace::SdkTracerProvider;
        let tracer_provider = SdkTracerProvider::builder()
            .with_simple_exporter(stdout_trace_exporter)
            .with_resource(resource.clone())
            .build();

        global::set_tracer_provider(tracer_provider);

        // Set up log provider
        use opentelemetry_sdk::logs::SdkLoggerProvider;
        let logger_provider = SdkLoggerProvider::builder()
            .with_simple_exporter(stdout_log_exporter)
            .with_resource(resource)
            .build();

        // Bridge tracing spans to OpenTelemetry traces
        // Convert service_name to static string for tracer
        let service_name_static: &'static str = Box::leak(service_name.clone().into_boxed_str());
        let tracer = global::tracer(service_name_static);
        let telemetry_layer = tracing_opentelemetry::OpenTelemetryLayer::new(tracer);

        // Bridge tracing logs to OpenTelemetry logs
        // Filter to only include actual log events (info!, debug!, warn!, error!), not span lifecycle events
        use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
        use tracing_subscriber::filter::{FilterFn, Filtered};

        // Only process events (not spans) - span lifecycle events are handled by telemetry_layer
        let log_filter = FilterFn::new(|metadata| {
            // Only process events, not spans
            metadata.is_event()
        });

        let log_bridge = Filtered::new(
            OpenTelemetryTracingBridge::new(&logger_provider),
            log_filter,
        );

        // Combine with existing fmt layer
        let subscriber = Registry::default()
            .with(telemetry_layer)
            .with(log_bridge)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_ansi(false),
            )
            .with(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| default_env_filter()),
            );

        // Initialize subscriber (idempotent)
        let _ = tracing::subscriber::set_global_default(subscriber);

        eprintln!("[FFI] OpenTelemetry tracing and logging initialized with stdout exporters");
    }

    Ok(())
}

/// Initialize the render engine with a database at the given path
///
/// This is the main entry point for Flutter. It creates a `FrontendSession` which:
/// 1. Initializes the database and schema (including materialized views)
/// 2. Waits for file watcher readiness (if OrgMode is configured)
/// 3. Detects any startup errors
///
/// The session is stored globally so that subsequent FFI calls can access it.
/// All query methods go through `FrontendSession`, ensuring they can only be
/// called after initialization completes.
///
/// # Parameters
/// * `db_path` - Path to the database file
/// * `config` - Configuration map containing:
///   - `TODOIST_API_KEY` - Todoist API key for sync
///   - `ORGMODE_ROOT_DIRECTORY` - OrgMode files root directory
///   - `MCP_SERVER_PORT` - Port for MCP HTTP server (optional, starts server if provided)
///
/// # Returns
/// An opaque handle to the session. The actual `FrontendSession` is stored globally
/// and accessed by subsequent FFI calls. This return value is kept alive by Flutter
/// to prevent the Rust Bridge from disposing the underlying resources.
pub async fn init_render_engine(
    db_path: String,
    config: HashMap<String, String>,
) -> anyhow::Result<ArcFrontendSession> {
    use holon_frontend::FrontendConfig;
    use std::println;

    // Initialize OpenTelemetry (includes tracing subscriber with OpenTelemetry bridge)
    init_opentelemetry().await?;

    println!("[FFI] Tracing subscriber initialized - Rust logs will appear below");
    eprintln!("[FFI] Tracing subscriber initialized - Rust logs will appear below");

    // Parse MCP server port from config (only on non-WASM targets)
    #[cfg(not(target_arch = "wasm32"))]
    let mcp_port: Option<u16> = config
        .get("MCP_SERVER_PORT")
        .and_then(|s| s.parse().ok())
        .or(Some(8520));

    // Build FrontendConfig from HashMap
    let mut frontend_config = FrontendConfig::new().with_db_path(db_path.into());

    if let Some(root_dir) = config.get("ORGMODE_ROOT_DIRECTORY") {
        println!(
            "[FFI] Configuring OrgMode with root directory: {}",
            root_dir
        );
        frontend_config = frontend_config.with_orgmode(root_dir.into());
    } else {
        println!("[FFI] No ORGMODE_ROOT_DIRECTORY in config, skipping OrgMode integration");
    }

    // Enable Loro CRDT if configured
    let loro_enabled = config
        .get("LORO_ENABLED")
        .map(|v| !v.is_empty() && v != "0" && v.to_lowercase() != "false")
        .unwrap_or(false);
    if loro_enabled {
        println!("[FFI] Enabling Loro CRDT layer");
        frontend_config = frontend_config.with_loro();
    }

    if let Some(api_key) = config.get("TODOIST_API_KEY") {
        println!("[FFI] Configuring Todoist with API key");
        frontend_config = frontend_config.with_todoist(api_key.clone());
    } else {
        println!("[FFI] No TODOIST_API_KEY in config, skipping Todoist integration");
    }

    // Create session using FrontendSession (waits for readiness!)
    // This is the critical initialization step - all schema, matviews, etc. are ready after this
    println!("[FFI] Creating FrontendSession...");
    let session = Arc::new(FrontendSession::new(frontend_config).await?);

    // Check for startup errors (DDL/sync races)
    if session.has_startup_errors() {
        eprintln!(
            "[FFI] WARNING: {} startup errors detected during initialization",
            session.startup_error_count()
        );
    }

    println!("[FFI] FrontendSession created successfully");

    // Store in global singleton - this is what FFI functions use to access the session
    GLOBAL_SESSION
        .set(session.clone())
        .map_err(|_| anyhow::anyhow!("Session already initialized"))?;

    // Start MCP server if port is configured (only on non-WASM targets)
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(port) = mcp_port {
        println!("[FFI] Starting MCP server on port {}", port);
        let mcp_engine = session.engine().clone();
        tokio::spawn(async move {
            use tokio_util::sync::CancellationToken;
            let bind_address = std::net::SocketAddr::from(([127, 0, 0, 1], port));
            let cancellation_token = CancellationToken::new();
            if let Err(e) =
                holon_mcp::di::run_http_server(mcp_engine, bind_address, cancellation_token).await
            {
                eprintln!("[FFI] MCP server error: {}", e);
            }
        });
        println!("[FFI] MCP server started on http://127.0.0.1:{}", port);
    }

    Ok(ArcFrontendSession(session))
}

/// Opaque handle to the FrontendSession for Flutter
///
/// This is returned from `init_render_engine` and should be kept alive by Flutter
/// to prevent the Rust Bridge from disposing the underlying session.
/// All actual operations go through the global `GLOBAL_SESSION`.
#[frb(opaque)]
pub struct ArcFrontendSession(Arc<FrontendSession>);

/// Helper to get the global session, returning a clear error if not initialized
fn get_session() -> anyhow::Result<Arc<FrontendSession>> {
    GLOBAL_SESSION
        .get()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Session not initialized. Call init_render_engine first."))
}

/// flutter_rust_bridge:non_opaque
pub struct MapChangeSink {
    pub sink: StreamSink<BatchMapChangeWithMetadata>,
}

/// Compile a PRQL query, execute it, and set up CDC streaming
///
/// This combines query compilation, execution, and change watching into a single call.
/// Returns a WidgetSpec (render spec + data) and forwards CDC events to the sink.
///
/// # Returns
/// - `WidgetSpec`: Contains render_spec and data (actions empty for regular queries)
///
/// # UI Usage
/// The UI should:
/// 1. Subscribe to the MapChangeSink using StreamBuilder in Flutter
/// 2. Key widgets by entity ID from data.get("id"), NOT by rowid
/// 3. Handle Added/Updated/Removed events to update UI
///
pub async fn query_and_watch(
    prql: String,
    params: HashMap<String, Value>,
    sink: MapChangeSink,
    trace_context: Option<TraceContext>,
    // Optional block ID for context. When set, `from children` resolves to children of this block.
    context_block_id: Option<String>,
) -> anyhow::Result<WidgetSpec> {
    use holon_frontend::QueryContext;

    let mut span = create_span_from_context("ffi.query_and_watch", trace_context);
    span.set_attribute(opentelemetry::KeyValue::new("prql.query", prql.clone()));

    let session = get_session()?;

    // Create QueryContext if block_id is provided
    // Look up block path for descendants/grandchildren queries
    let context = if let Some(id) = context_block_id {
        let block_path = session.lookup_block_path(&id).await?;
        Some(QueryContext::for_block_with_path(id, None, block_path))
    } else {
        None
    };

    let (widget_spec, stream) = session.query_and_watch(prql, params, context).await?;

    span.set_attribute(opentelemetry::KeyValue::new(
        "query.result_count",
        widget_spec.data.len() as i64,
    ));

    // Forward CDC stream to Flutter sink
    spawn_stream_forwarder(stream, sink);

    span.end();
    Ok(widget_spec)
}

/// Get available operations for an entity
///
/// Returns a list of operation descriptors available for the given entity_name.
/// Use "*" as entity_name to get wildcard operations.
///
/// # FFI Function
/// This is exposed to Flutter via flutter_rust_bridge
pub async fn available_operations(entity_name: String) -> anyhow::Result<Vec<OperationDescriptor>> {
    let session = get_session()?;
    Ok(session.available_operations(&entity_name).await)
}

/// Execute an operation on the database
///
/// # FFI Function
/// This is exposed to Flutter via flutter_rust_bridge
///
/// Operations mutate the database directly. UI updates happen via CDC streams.
/// This follows the unidirectional data flow: Action → Model → View
///
/// # Note
/// This function does NOT return new data. Changes propagate through:
/// Operation → DB mutation → CDC event → watch_query stream → UI update
pub async fn execute_operation(
    entity_name: String,
    op_name: String,
    params: HashMap<String, Value>,
    trace_context: Option<TraceContext>,
) -> anyhow::Result<()> {
    use opentelemetry::trace::TraceContextExt;
    use tracing::info;
    use tracing::Instrument;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    // Create tracing span that will be bridged to OpenTelemetry
    let span = tracing::span!(
        tracing::Level::INFO,
        "ffi.execute_operation",
        "operation.entity" = %entity_name,
        "operation.name" = %op_name
    );

    // Build the parent context from Flutter's trace context
    let parent_ctx = if let Some(ref ctx) = trace_context {
        if let Some(span_ctx) = ctx.to_span_context() {
            Context::current().with_remote_span_context(span_ctx)
        } else {
            Context::current()
        }
    } else {
        Context::current()
    };

    let _ = span.set_parent(parent_ctx.clone());

    // Create BatchTraceContext for task-local propagation
    let batch_trace_ctx = trace_context
        .as_ref()
        .and_then(|ctx| ctx.to_span_context())
        .map(|span_ctx| holon_api::BatchTraceContext::from_span_context(&span_ctx));

    // Use task-local storage to propagate trace context through async call chain
    let result = if let Some(trace_ctx) = batch_trace_ctx {
        holon_api::CURRENT_TRACE_CONTEXT
            .scope(trace_ctx, async {
                let session = get_session()?;

                info!(
                    "[FFI] execute_operation called: entity={}, op={}, params={:?}",
                    entity_name, op_name, params
                );

                session
                    .execute_operation(&entity_name, &op_name, params.clone())
                    .await
            })
            .instrument(span)
            .await
    } else {
        async {
            let session = get_session()?;

            info!(
                "[FFI] execute_operation called: entity={}, op={}, params={:?}",
                entity_name, op_name, params
            );

            session
                .execute_operation(&entity_name, &op_name, params.clone())
                .await
        }
        .instrument(span)
        .await
    };

    match &result {
        Ok(_) => {
            info!(
                "[FFI] execute_operation succeeded: entity={}, op={}",
                entity_name, op_name
            );
        }
        Err(e) => {
            tracing::error!(
                "[FFI] execute_operation failed: entity={}, op={}, error={}",
                entity_name,
                op_name,
                e
            );
        }
    }

    result.map_err(|e| {
        anyhow::anyhow!(
            "Operation '{}' on entity '{}' failed: {}",
            op_name,
            entity_name,
            e
        )
    })
}

/// Check if an operation is available for an entity
///
/// # FFI Function
/// This is exposed to Flutter via flutter_rust_bridge
///
/// # Returns
/// `true` if the operation is available, `false` otherwise
pub async fn has_operation(entity_name: String, op_name: String) -> anyhow::Result<bool> {
    let session = get_session()?;
    Ok(session.has_operation(&entity_name, &op_name).await)
}

/// Undo the last operation
///
/// Executes the inverse operation from the undo stack and pushes it to the redo stack.
/// Returns true if an operation was undone, false if the undo stack is empty.
pub async fn undo() -> anyhow::Result<bool> {
    let session = get_session()?;
    session.undo().await
}

/// Redo the last undone operation
///
/// Executes the inverse of the last undone operation and pushes it back to the undo stack.
/// Returns true if an operation was redone, false if the redo stack is empty.
pub async fn redo() -> anyhow::Result<bool> {
    let session = get_session()?;
    session.redo().await
}

/// Check if undo is available
pub async fn can_undo() -> anyhow::Result<bool> {
    let session = get_session()?;
    Ok(session.can_undo().await)
}

/// Check if redo is available
pub async fn can_redo() -> anyhow::Result<bool> {
    let session = get_session()?;
    Ok(session.can_redo().await)
}

/// Get the initial widget for the application root
///
/// This is the main entry point for the backend-driven UI architecture.
/// Returns a WidgetSpec containing:
/// - render_spec: How to render the layout (e.g., columns with regions)
/// - data: The layout data (region blocks with widths, content, etc.)
/// - actions: Global actions available throughout the app
///
/// The frontend renders this using RenderInterpreter, just like any other widget.
/// CDC events are forwarded to the provided sink for live updates.
///
/// **This method can only be called after `init_render_engine` completes.**
/// The type system enforces this: `FrontendSession` (stored in `GLOBAL_SESSION`)
/// is only available after initialization, so `initial_widget` cannot be called
/// before the schema (including `blocks_with_paths`) is ready.
///
/// # Example usage in Flutter:
/// ```dart
/// final widgetSpec = await initialWidget(sink);
/// // Use RenderInterpreter to render the widget
/// final widget = RenderInterpreter().build(widgetSpec.renderSpec, context);
/// ```
pub async fn initial_widget(sink: MapChangeSink) -> anyhow::Result<WidgetSpec> {
    use tracing::info;

    let session = get_session()?;

    info!("[FFI] initial_widget called");

    let (widget_spec, stream) = session.initial_widget().await?;

    info!(
        "[FFI] initial_widget completed: {} data rows, {} actions",
        widget_spec.data.len(),
        widget_spec.actions.len()
    );

    // Forward CDC stream to Flutter sink
    spawn_stream_forwarder(stream, sink);

    Ok(widget_spec)
}
