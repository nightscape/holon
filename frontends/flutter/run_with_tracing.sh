#!/bin/bash
# Script to run Flutter app with OpenTelemetry tracing configured
# Configured for SigNoz running via docker-compose

# Set OpenTelemetry environment variables
# SigNoz collector listens on localhost:4317 (gRPC) and localhost:4318 (HTTP)
# Using HTTP endpoint for Rust side (with_http() in code)
# Enable both stdout and otlp exporters to see traces locally and send to collector
# Note: Use HTTP (not HTTPS) with port 4318 for OTLP HTTP exporter
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.signoz.orb.local:4318
export OTEL_TRACES_EXPORTER=stdout,otlp
export OTEL_LOGS_EXPORTER=stdout,otlp
export OTEL_SERVICE_NAME=holon-backend

# Detect available device (prefer macOS desktop, fallback to first available)
DEVICE="macos"
if ! flutter devices | grep -q "macOS (desktop)"; then
  DEVICE=$(flutter devices | grep -E "^\w" | head -1 | awk '{print $1}' | tr -d '•')
  if [ -z "$DEVICE" ]; then
    echo "No devices found. Please connect a device or start an emulator."
    exit 1
  fi
fi

echo "Using device: $DEVICE"
echo "OpenTelemetry endpoint: $OTEL_EXPORTER_OTLP_ENDPOINT"
echo "Traces exporters: $OTEL_TRACES_EXPORTER"
echo "Logs exporters: $OTEL_LOGS_EXPORTER"
echo "Service name: $OTEL_SERVICE_NAME"
echo "Starting Flutter app with tracing and logging enabled..."

# Run Flutter app with dart-define flags (for Flutter-side configuration)
flutter run -d "$DEVICE" \
  --dart-define=OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.signoz.orb.local:4318 \
  --dart-define=OTEL_TRACES_EXPORTER=stdout,otlp \
  --dart-define=OTEL_LOGS_EXPORTER=stdout,otlp \
  --dart-define=OTEL_SERVICE_NAME=holon-flutter
