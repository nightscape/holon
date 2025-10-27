//! OpenTelemetry initialization for MCP server
//!
//! This module sets up OpenTelemetry tracing and logging, similar to the main application.
//! It supports both OTLP (OpenTelemetry Protocol) and stdout exporters.

use anyhow::Result;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use std::env;

/// Initialize OpenTelemetry providers
///
/// Configures OpenTelemetry exporters based on environment variables:
/// - `OTEL_SERVICE_NAME`: Service name (default: "holon-mcp")
/// - `OTEL_TRACES_EXPORTER`: Exporters to use (default: "stdout,otlp")
/// - `OTEL_EXPORTER_OTLP_ENDPOINT`: OTLP endpoint (default: "http://localhost:4318")
///
/// This function only sets up the OpenTelemetry providers. The layers should be added
/// to the tracing subscriber in the calling code.
pub fn init_opentelemetry() -> Result<()> {
    // Get service name from env or use default
    let service_name = env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "holon-mcp".to_string());

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

        // Note: Can't use tracing::info! here because subscriber isn't initialized yet
        eprintln!("Initializing OpenTelemetry OTLP exporters:");
        eprintln!("  Traces endpoint: {}", traces_endpoint);
        eprintln!("  Logs endpoint: {}", logs_endpoint);

        // Use OTLP exporter builder (0.31 API)
        use opentelemetry_otlp::WithExportConfig;

        // Create OTLP trace exporter using builder - use with_http() to set HTTP protocol
        let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(traces_endpoint)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build trace exporter: {}", e))?;

        // Create OTLP log exporter
        let log_exporter = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_endpoint(logs_endpoint)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build log exporter: {}", e))?;

        // Set up trace provider
        use opentelemetry_sdk::trace::SdkTracerProvider;
        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(trace_exporter)
            .with_resource(resource.clone())
            .build();

        global::set_tracer_provider(tracer_provider);

        // Set up log provider
        // Note: logger_provider must be kept alive for the lifetime of the application
        use opentelemetry_sdk::logs::SdkLoggerProvider;
        let logger_provider = SdkLoggerProvider::builder()
            .with_batch_exporter(log_exporter)
            .with_resource(resource.clone())
            .build();
        // Leak the logger provider to keep it alive for the lifetime of the application
        Box::leak(Box::new(logger_provider));
    } else {
        // Use stdout exporters only
        use opentelemetry_stdout::{LogExporter, SpanExporter};
        let stdout_trace_exporter = SpanExporter::default();
        let stdout_log_exporter = LogExporter::default();

        // Note: Can't use tracing::info! here because subscriber isn't initialized yet
        eprintln!("Initializing OpenTelemetry stdout exporters");

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
        // Leak the logger provider to keep it alive for the lifetime of the application
        Box::leak(Box::new(logger_provider));
    }

    Ok(())
}

/// Create OpenTelemetry layers for a tracing subscriber
///
/// This function creates the tracing layers that bridge tracing to OpenTelemetry.
/// It should be called after `init_opentelemetry()`.
///
/// Returns only the telemetry layer for now. Log bridge requires storing logger provider reference.
pub fn create_opentelemetry_layer(
) -> impl tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync {
    // Get service name
    let service_name = env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "holon-mcp".to_string());
    let service_name_static: &'static str = Box::leak(service_name.into_boxed_str());
    let tracer = global::tracer(service_name_static);

    // Bridge tracing spans to OpenTelemetry traces
    tracing_opentelemetry::OpenTelemetryLayer::new(tracer)
}
