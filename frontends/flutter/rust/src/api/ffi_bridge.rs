//! FFI bridge functions for Flutter
//!
//! This module provides a minimal FFI surface exposing only BackendEngine and essential types.
//! Low-level query_render types (Expr, ModuleDef, Lineage) are hidden as implementation details.

use crate::api::types::TraceContext;
use crate::frb_generated::StreamSink;
use ferrous_di::ServiceCollectionModuleExt;
use holon_api::{BatchMapChange, BatchMapChangeWithMetadata, MapChange};
use holon_api::{OperationDescriptor, RenderSpec, Value};
use once_cell::sync::OnceCell;
use opentelemetry::global;
use opentelemetry::trace::{Span, Tracer};
use opentelemetry::Context;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;

// Re-export types needed by generated code (also available for use in this module)
pub use super::BackendEngine;
pub use holon_api::Change;
// Global singleton to store the engine
// This prevents Flutter Rust Bridge from disposing the engine during async operations
static GLOBAL_ENGINE: OnceCell<Arc<BackendEngine>> = OnceCell::new();

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
                tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                    // Default filter: info level for our app, but suppress noisy HTTP client logs
                    let mut filter = tracing_subscriber::EnvFilter::new("info");
                    // Aggressively suppress HTTP client connection pooling logs (set to error to hide debug/info)
                    filter = filter.add_directive("reqwest=error".parse().unwrap());
                    filter = filter.add_directive("hyper=error".parse().unwrap());
                    filter = filter.add_directive("h2=error".parse().unwrap());
                    filter = filter.add_directive("http=error".parse().unwrap());
                    filter = filter.add_directive("tower=error".parse().unwrap());
                    filter = filter.add_directive("opentelemetry_http=error".parse().unwrap());
                    // Allow debug for our application code
                    filter = filter.add_directive("holon=debug".parse().unwrap());
                    filter = filter.add_directive("rust_lib_holon=debug".parse().unwrap());
                    filter
                }),
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
                tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                    // Default filter: info level for our app, but suppress noisy HTTP client logs
                    let mut filter = tracing_subscriber::EnvFilter::new("info");
                    // Aggressively suppress HTTP client connection pooling logs (set to error to hide debug/info)
                    filter = filter.add_directive("reqwest=error".parse().unwrap());
                    filter = filter.add_directive("hyper=error".parse().unwrap());
                    filter = filter.add_directive("h2=error".parse().unwrap());
                    filter = filter.add_directive("http=error".parse().unwrap());
                    filter = filter.add_directive("tower=error".parse().unwrap());
                    filter = filter.add_directive("opentelemetry_http=error".parse().unwrap());
                    // Allow debug for our application code
                    filter = filter.add_directive("holon=debug".parse().unwrap());
                    filter = filter.add_directive("rust_lib_holon=debug".parse().unwrap());
                    filter
                }),
            );

        // Initialize subscriber (idempotent)
        let _ = tracing::subscriber::set_global_default(subscriber);

        eprintln!("[FFI] OpenTelemetry tracing and logging initialized with stdout exporters");
    }

    Ok(())
}

/// Initialize a render engine with a database at the given path
///
/// Uses dependency injection to properly configure the engine with all registered providers.
/// The engine is stored in a global singleton to prevent premature disposal.
///
/// # Parameters
/// * `db_path` - Path to the database file
/// * `config` - Configuration map (e.g., API keys like "TODOIST_API_KEY", paths like "ORGMODE_ROOT_DIRECTORY")
pub async fn init_render_engine(
    db_path: String,
    config: HashMap<String, String>,
) -> anyhow::Result<Arc<BackendEngine>> {
    use holon_orgmode::di::{OrgModeConfig, OrgModeModule};
    use holon_todoist::di::{TodoistConfig, TodoistModule};
    use std::path::PathBuf;
    use std::println;

    // Initialize OpenTelemetry (includes tracing subscriber with OpenTelemetry bridge)
    init_opentelemetry().await?;

    // Also print a message to confirm logging is initialized
    println!("[FFI] Tracing subscriber initialized - Rust logs will appear below");
    eprintln!("[FFI] Tracing subscriber initialized - Rust logs will appear below");

    // Use shared DI setup function
    // Register modules based on config
    let engine = holon::di::create_backend_engine(db_path.into(), |services| {
        // Check for Todoist API key in config
        if let Some(api_key) = config.get("TODOIST_API_KEY") {
            println!("[FFI] Registering TodoistConfig with API key");
            services.add_singleton(TodoistConfig::new(Some(api_key.clone())));

            println!("[FFI] Registering TodoistModule");
            services.add_module_mut(TodoistModule).map_err(|e| {
                let msg = format!("Failed to register TodoistModule: {}", e);
                println!("[FFI] ERROR: {}", msg);
                eprintln!("[FFI] ERROR: {}", msg);
                anyhow::anyhow!("{}", msg)
            })?;
            println!("[FFI] TodoistModule registered successfully");
        } else {
            println!("[FFI] No TODOIST_API_KEY in config, skipping Todoist integration");
        }

        // Check for OrgMode root directory in config
        if let Some(root_dir) = config.get("ORGMODE_ROOT_DIRECTORY") {
            println!(
                "[FFI] Registering OrgModeConfig with root directory: {}",
                root_dir
            );
            services.add_singleton(OrgModeConfig::new(PathBuf::from(root_dir)));

            println!("[FFI] Registering OrgModeModule");
            services.add_module_mut(OrgModeModule).map_err(|e| {
                let msg = format!("Failed to register OrgModeModule: {}", e);
                println!("[FFI] ERROR: {}", msg);
                eprintln!("[FFI] ERROR: {}", msg);
                anyhow::anyhow!("{}", msg)
            })?;
            println!("[FFI] OrgModeModule registered successfully");
        } else {
            println!("[FFI] No ORGMODE_ROOT_DIRECTORY in config, skipping OrgMode integration");
        }

        Ok(())
    })
    .await?;

    // Store in global singleton to prevent Flutter Rust Bridge from disposing it
    GLOBAL_ENGINE
        .set(engine.clone())
        .map_err(|_| anyhow::anyhow!("Engine already initialized"))?;

    Ok(engine)
}

//pub type MapChangeSink = StreamSink<Change<HashMap<String, Value>>>;

/// flutter_rust_bridge:non_opaque
pub struct MapChangeSink {
    pub sink: StreamSink<BatchMapChangeWithMetadata>,
}

/// Compile a PRQL query, execute it, and set up CDC streaming
///
/// This combines query compilation, execution, and change watching into a single call.
/// Returns the render specification, current query results, and a stream of ongoing changes.
///
/// # Returns
/// A tuple containing:
/// - `RenderSpec`: UI rendering specification from the PRQL query
/// - `Vec<HashMap<String, Value>>`: Current query results
/// - `RowChangeStream`: Stream of ongoing changes to the query results
///
/// # UI Usage
/// The UI should:
/// 1. Subscribe to the RowChangeStream using StreamBuilder in Flutter
/// 2. Key widgets by entity ID from data.get("id"), NOT by rowid
/// 3. Handle Added/Updated/Removed events to update UI
///
pub async fn query_and_watch(
    prql: String,
    params: HashMap<String, Value>,
    sink: MapChangeSink,
    trace_context: Option<TraceContext>,
) -> anyhow::Result<(RenderSpec, Vec<HashMap<String, Value>>)> {
    let mut span = create_span_from_context("ffi.query_and_watch", trace_context);
    span.set_attribute(opentelemetry::KeyValue::new("prql.query", prql.clone()));

    let engine = GLOBAL_ENGINE
        .get()
        .ok_or_else(|| anyhow::anyhow!("Engine not initialized. Call init_render_engine first."))?
        .clone();

    let (render_spec, data, mut stream) = engine.query_and_watch(prql, params).await?;

    span.set_attribute(opentelemetry::KeyValue::new(
        "query.result_count",
        data.len() as i64,
    ));

    // Spawn a task to forward stream batches to the sink
    // Note: We can't use ContextGuard in spawned tasks as it's not Send
    // The span context propagation happens automatically through the tracing layer
    tokio::spawn(async move {
        use tracing::debug;
        use tracing::info;
        use tracing::warn;
        use tracing::Instrument;

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
            // This links this span to the original trace that created the changes
            if let Some(ref trace_ctx) = trace_context {
                use opentelemetry::trace::{
                    SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
                };
                use tracing_opentelemetry::OpenTelemetrySpanExt;

                // Parse trace_id and span_id from hex strings
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
            // StorageEntity is HashMap<String, Value>, so Change<StorageEntity> is already MapChange
            // Access inner.items directly since Deref doesn't allow moving
            let map_changes: Vec<MapChange> = batch_with_metadata
                .inner
                .items
                .into_iter()
                .map(|row_change| {
                    // RowChange.change is Change<StorageEntity> which is Change<HashMap<String, Value>> = MapChange
                    row_change.change
                })
                .collect();

            let batch_map_change = BatchMapChange { items: map_changes };

            // Wrap the converted batch with the preserved metadata
            let batch_map_change_with_metadata = BatchMapChangeWithMetadata {
                inner: batch_map_change,
                metadata,
            };

            if sink.sink.add(batch_map_change_with_metadata).is_err() {
                warn!("[FFI] Sink closed, stopping stream forwarding");
                break;
            }
            info!("[FFI] Forwarded batch to sink");
        }
        info!("[FFI] Stream forwarding task ended");
    });

    span.end();
    Ok((render_spec, data))
}

/// Get available operations for an entity
///
/// Returns a list of operation descriptors available for the given entity_name.
/// Use "*" as entity_name to get wildcard operations.
///
/// # FFI Function
/// This is exposed to Flutter via flutter_rust_bridge
pub async fn available_operations(entity_name: String) -> anyhow::Result<Vec<OperationDescriptor>> {
    let engine = GLOBAL_ENGINE
        .get()
        .ok_or_else(|| anyhow::anyhow!("Engine not initialized. Call init_render_engine first."))?
        .clone();

    Ok(engine.available_operations(&entity_name).await)
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

    // Note: Trace context propagation uses task-local storage (CURRENT_TRACE_CONTEXT)
    // which properly propagates through async call chains across .await boundaries.

    // Create tracing span that will be bridged to OpenTelemetry
    // Use .instrument() to maintain context across async boundaries
    let span = tracing::span!(
        tracing::Level::INFO,
        "ffi.execute_operation",
        "operation.entity" = %entity_name,
        "operation.name" = %op_name
    );

    // Build the parent context from Flutter's trace context
    let parent_ctx = if let Some(ref ctx) = trace_context {
        if let Some(span_ctx) = ctx.to_span_context() {
            // Create context with remote span context - this will be attached to current thread
            Context::current().with_remote_span_context(span_ctx)
        } else {
            Context::current()
        }
    } else {
        Context::current()
    };

    // Set the parent context on the tracing span (for OTel span creation)
    let _ = span.set_parent(parent_ctx.clone());

    // Create BatchTraceContext for task-local propagation
    let batch_trace_ctx = if let Some(ref ctx) = trace_context {
        if let Some(span_ctx) = ctx.to_span_context() {
            Some(holon_api::BatchTraceContext {
                trace_id: format!("{:032x}", span_ctx.trace_id()),
                span_id: format!("{:016x}", span_ctx.span_id()),
                trace_flags: if span_ctx.is_sampled() { 0x01 } else { 0x00 },
            })
        } else {
            None
        }
    } else {
        None
    };

    // Use task-local storage to propagate trace context through async call chain
    // This is the most reliable method as it works across .await boundaries
    let result = if let Some(trace_ctx) = batch_trace_ctx {
        holon_api::CURRENT_TRACE_CONTEXT
            .scope(trace_ctx, async {
                let engine = GLOBAL_ENGINE
                    .get()
                    .ok_or_else(|| {
                        anyhow::anyhow!("Engine not initialized. Call init_render_engine first.")
                    })?
                    .clone();

                info!(
                    "[FFI] execute_operation called: entity={}, op={}, params={:?}",
                    entity_name, op_name, params
                );

                engine
                    .execute_operation(&entity_name, &op_name, params.clone())
                    .await
            })
            .instrument(span)
            .await
    } else {
        async {
            let engine = GLOBAL_ENGINE
                .get()
                .ok_or_else(|| {
                    anyhow::anyhow!("Engine not initialized. Call init_render_engine first.")
                })?
                .clone();

            info!(
                "[FFI] execute_operation called: entity={}, op={}, params={:?}",
                entity_name, op_name, params
            );

            engine
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
    let engine = GLOBAL_ENGINE
        .get()
        .ok_or_else(|| anyhow::anyhow!("Engine not initialized. Call init_render_engine first."))?
        .clone();

    Ok(engine.has_operation(&entity_name, &op_name).await)
}

/// Undo the last operation
///
/// Executes the inverse operation from the undo stack and pushes it to the redo stack.
/// Returns true if an operation was undone, false if the undo stack is empty.
pub async fn undo() -> anyhow::Result<bool> {
    let engine = GLOBAL_ENGINE
        .get()
        .ok_or_else(|| anyhow::anyhow!("Engine not initialized. Call init_render_engine first."))?
        .clone();

    engine.undo().await
}

/// Redo the last undone operation
///
/// Executes the inverse of the last undone operation and pushes it back to the undo stack.
/// Returns true if an operation was redone, false if the redo stack is empty.
pub async fn redo() -> anyhow::Result<bool> {
    let engine = GLOBAL_ENGINE
        .get()
        .ok_or_else(|| anyhow::anyhow!("Engine not initialized. Call init_render_engine first."))?
        .clone();

    engine.redo().await
}

/// Check if undo is available
pub async fn can_undo() -> anyhow::Result<bool> {
    let engine = GLOBAL_ENGINE
        .get()
        .ok_or_else(|| anyhow::anyhow!("Engine not initialized. Call init_render_engine first."))?
        .clone();

    Ok(engine.can_undo().await)
}

/// Check if redo is available
pub async fn can_redo() -> anyhow::Result<bool> {
    let engine = GLOBAL_ENGINE
        .get()
        .ok_or_else(|| anyhow::anyhow!("Engine not initialized. Call init_render_engine first."))?
        .clone();

    Ok(engine.can_redo().await)
}
