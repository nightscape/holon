# OpenTelemetry Tracing and Logging Configuration

This application supports OpenTelemetry tracing and logging for both Flutter (Dart) and Rust components.

## Quick Start

To run the app with tracing to your OpenTelemetry collector:

```bash
cd frontends/flutter
./run_with_tracing.sh
```

The script is configured to send traces and logs to both stdout (for debugging) and your OTLP collector.

Or manually:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_TRACES_EXPORTER=stdout,otlp
export OTEL_LOGS_EXPORTER=stdout,otlp
export OTEL_SERVICE_NAME=holon

flutter run \
  --dart-define=OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
  --dart-define=OTEL_TRACES_EXPORTER=stdout,otlp \
  --dart-define=OTEL_LOGS_EXPORTER=stdout,otlp \
  --dart-define=OTEL_SERVICE_NAME=holon
```

## Configuration

### Using Environment Variables

The Rust side reads environment variables directly. The Flutter side uses `--dart-define` flags.

**Important**: Set environment variables for Rust, and use `--dart-define` for Flutter configuration.

#### Quick Setup Script

Use the provided script:

```bash
cd frontends/flutter
./run_with_tracing.sh
```

#### Manual Setup

```bash
# Set environment variables (for Rust side)
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_TRACES_EXPORTER=stdout,otlp
export OTEL_LOGS_EXPORTER=stdout,otlp
export OTEL_SERVICE_NAME=holon

# Run with dart-define flags (for Flutter side)
flutter run \
  --dart-define=OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
  --dart-define=OTEL_TRACES_EXPORTER=stdout,otlp \
  --dart-define=OTEL_LOGS_EXPORTER=stdout,otlp \
  --dart-define=OTEL_SERVICE_NAME=holon
```

## Configuration Options

### Environment Variables

- `OTEL_EXPORTER_OTLP_ENDPOINT`: The OTLP collector endpoint (default: `http://localhost:4318`)
- `OTEL_TRACES_EXPORTER`: Comma-separated list of trace exporters to use:
  - `stdout` - Console exporter (for development)
  - `otlp` - OTLP exporter (for production/collector)
  - Default: `stdout,otlp` (both enabled)
- `OTEL_LOGS_EXPORTER`: Comma-separated list of log exporters to use:
  - `stdout` - Console exporter (for development)
  - `otlp` - OTLP HTTP exporter (for production/collector)
  - Default: `stdout,otlp` (both enabled)
- `OTEL_SERVICE_NAME`: Service name for traces and logs (default: `holon`)

### Example: Production Setup (SigNoz only)

Send traces and logs only to SigNoz collector (no console output):

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_TRACES_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
export OTEL_SERVICE_NAME=holon

flutter run \
  --dart-define=OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
  --dart-define=OTEL_TRACES_EXPORTER=otlp \
  --dart-define=OTEL_LOGS_EXPORTER=otlp \
  --dart-define=OTEL_SERVICE_NAME=holon
```

### Example: Development Setup (SigNoz + Console)

Use both console and SigNoz collector for traces and logs:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_TRACES_EXPORTER=stdout,otlp
export OTEL_LOGS_EXPORTER=stdout,otlp
export OTEL_SERVICE_NAME=holon

flutter run \
  --dart-define=OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
  --dart-define=OTEL_TRACES_EXPORTER=stdout,otlp \
  --dart-define=OTEL_LOGS_EXPORTER=stdout,otlp \
  --dart-define=OTEL_SERVICE_NAME=holon
```

### SigNoz Configuration

The script is configured for SigNoz running via docker-compose:
- **OTLP gRPC endpoint**: `http://localhost:4317` (used by Rust)
- **OTLP HTTP endpoint**: `http://localhost:4318` (alternative)
- **SigNoz UI**: `http://localhost:8080`

Make sure SigNoz is running before starting the app:
```bash
docker-compose up -d  # In your SigNoz directory
```

### Querying Logs via SigNoz API

You can query logs directly from SigNoz using the Query Builder v5 API endpoint. This is useful for programmatic access, debugging, or integration with other tools.

#### Authentication

You'll need two authentication tokens:
1. **Bearer Token**: JWT token from SigNoz (get from browser DevTools → Network → Authorization header)
2. **API Key**: SigNoz API key (get from browser DevTools → Network → signoz-api-key header)

#### Basic Query Example

Query all logs from the last hour:

```bash
curl 'http://localhost:8080/api/v5/query_range' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer YOUR_BEARER_TOKEN' \
  -H 'Content-Type: application/json' \
  -H 'signoz-api-key: YOUR_API_KEY' \
  --data-raw '{
    "schemaVersion": "v1",
    "start": 1764060111000,
    "end": 1764063711000,
    "requestType": "raw",
    "compositeQuery": {
      "queries": [{
        "type": "builder_query",
        "spec": {
          "name": "A",
          "signal": "logs",
          "stepInterval": null,
          "disabled": false,
          "filter": {"expression": ""},
          "limit": 100,
          "offset": 0,
          "order": [
            {"key": {"name": "timestamp"}, "direction": "desc"},
            {"key": {"name": "id"}, "direction": "desc"}
          ],
          "having": {"expression": ""}
        }
      }]
    },
    "formatOptions": {
      "formatTableResultForUI": false,
      "fillGaps": false
    },
    "variables": {}
  }'
```

#### Query by Service Name

Filter logs for specific services (`holon-backend` or `holon-flutter`):

```bash
curl 'http://localhost:8080/api/v5/query_range' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer YOUR_BEARER_TOKEN' \
  -H 'Content-Type: application/json' \
  -H 'signoz-api-key: YOUR_API_KEY' \
  --data-raw '{
    "schemaVersion": "v1",
    "start": 1764060111000,
    "end": 1764063711000,
    "requestType": "raw",
    "compositeQuery": {
      "queries": [{
        "type": "builder_query",
        "spec": {
          "name": "A",
          "signal": "logs",
          "disabled": false,
          "filter": {
            "expression": "service.name=\"holon-backend\" OR service.name=\"holon-flutter\""
          },
          "limit": 100,
          "offset": 0,
          "order": [{"key": {"name": "timestamp"}, "direction": "desc"}]
        }
      }]
    },
    "formatOptions": {"formatTableResultForUI": false, "fillGaps": false},
    "variables": {}
  }'
```

#### Query Error Logs Only

Get only ERROR and FATAL level logs:

```bash
curl 'http://localhost:8080/api/v5/query_range' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer YOUR_BEARER_TOKEN' \
  -H 'Content-Type: application/json' \
  -H 'signoz-api-key: YOUR_API_KEY' \
  --data-raw '{
    "schemaVersion": "v1",
    "start": 1764060111000,
    "end": 1764063711000,
    "requestType": "raw",
    "compositeQuery": {
      "queries": [{
        "type": "builder_query",
        "spec": {
          "name": "A",
          "signal": "logs",
          "disabled": false,
          "filter": {
            "expression": "severity_text=\"ERROR\" OR severity_text=\"FATAL\""
          },
          "limit": 50,
          "offset": 0,
          "order": [{"key": {"name": "timestamp"}, "direction": "desc"}]
        }
      }]
    },
    "formatOptions": {"formatTableResultForUI": false, "fillGaps": false},
    "variables": {}
  }'
```

#### Query Parameters

- **start/end**: Unix timestamps in milliseconds for the time range
- **limit**: Maximum number of log entries to return (default: 100)
- **offset**: Pagination offset for retrieving more results
- **filter.expression**: ClickHouse SQL WHERE clause for filtering logs
- **order**: Array of sort keys (timestamp, id, etc.) with direction (asc/desc)

#### Getting Authentication Tokens

1. Open SigNoz UI in your browser (`http://localhost:8080`)
2. Open browser DevTools (F12 or Cmd+Option+I)
3. Go to Network tab
4. Perform any action in SigNoz UI (e.g., view logs)
5. Find a request to `/api/v5/query_range`
6. Copy the `Authorization` header value (Bearer token)
7. Copy the `signoz-api-key` header value (API key)

#### Response Format

The API returns JSON with the following structure:

```json
{
  "status": "success",
  "data": {
    "type": "raw",
    "meta": {
      "rowsScanned": 541,
      "bytesScanned": 246945,
      "durationMs": 44
    },
    "data": {
      "results": [{
        "queryName": "A",
        "nextCursor": "...",
        "rows": [{
          "data": {
            "body": "Log message text",
            "severity_text": "INFO",
            "severity_number": 9,
            "resources_string": {
              "service.name": "holon-backend"
            },
            "scope_name": "holon::api::backend_engine",
            "timestamp": 1764061188643308000,
            "trace_id": "...",
            "span_id": "..."
          },
          "timestamp": "2025-11-25T08:59:48.643308Z"
        }]
      }]
    }
  }
}
```

## Logging Configuration

### Using the Log Utility

Use the `log` utility for structured logging that forwards to both console and OpenTelemetry:

```dart
import 'package:holon/utils/log.dart';

// Info level logs
log.info('Application started');

// Debug level logs
log.debug('Processing user input');

// Warning level logs
log.warn('Deprecated API used');

// Error level logs (with optional error and stack trace)
log.error('Operation failed', error: e, stackTrace: stackTrace);
```

### Log Level Mapping

- `log.info()` → INFO level logs (also prints to console)
- `log.debug()` → DEBUG level logs (also uses debugPrint)
- `log.warn()` → WARN level logs (also prints to console)
- `log.error()` → ERROR level logs (with stack traces, also uses debugPrint)

All logs are forwarded to the Otel collector via OTLP HTTP at `/v1/logs` endpoint.

### Log Batching

Logs are batched and sent to the collector:
- Batch size: 10 logs
- Flush interval: 5 seconds
- Automatic flush on app shutdown

### Disabling Log Export

To disable log export to collector (console only):

```bash
export OTEL_LOGS_EXPORTER=stdout

flutter run --dart-define=OTEL_LOGS_EXPORTER=stdout
```

## Architecture

- **Flutter Side**:
  - Creates root spans for user interactions and queries
  - Bridges `print`/`debugPrint` to OpenTelemetry logs
  - Sends logs to Otel collector via OTLP HTTP
- **Rust Side**:
  - Creates child spans for backend operations, sync operations, and CDC events
  - Bridges tracing logs to OpenTelemetry logs
  - Sends logs to Otel collector via OTLP HTTP
- **Trace Context**: Propagated across FFI boundary via `TraceContext` struct

## Notes

- Both Flutter and Rust sides handle OTLP export directly
- Flutter logs use OTLP HTTP endpoint at `/v1/logs` (same base URL as traces)
- Rust logs use OTLP HTTP endpoint at `/v1/logs` (same base URL as traces)
- Both sides share the same trace IDs when context is propagated
- Traces flow: Flutter → FFI → Rust Backend → Operations → CDC → UI Updates
- Logs flow: Flutter (print/debugPrint) → LoggingService → Otel Collector
