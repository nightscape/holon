import 'dart:async';
import 'package:flutter/foundation.dart' show debugPrint;
import 'package:opentelemetry_logging/opentelemetry_logging.dart';

/// Simple trace context holder for log correlation
class LogTraceContext {
  final String traceId;
  final String spanId;

  const LogTraceContext({required this.traceId, required this.spanId});
}

/// Service for managing OpenTelemetry logging
///
/// Bridges Dart logs to OpenTelemetry logs and forwards them to the Otel collector.
/// Uses the opentelemetry_logging package for OTLP protocol handling.
class LoggingService {
  static OpenTelemetryLogger? _logger;
  static bool _initialized = false;
  static bool _consoleEnabled = true;

  /// Current trace context for log correlation (set from CDC batches)
  static LogTraceContext? _currentTraceContext;

  /// Initialize OpenTelemetry logging
  ///
  /// Sets up log exporters based on environment variables or dart-define flags.
  /// Supports both console (stdout) and OTLP HTTP exporters.
  static Future<void> initialize() async {
    if (_initialized) {
      return; // Already initialized
    }

    // Get configuration from environment or dart-define
    final exporterType = const String.fromEnvironment(
      'OTEL_LOGS_EXPORTER',
      defaultValue: 'stdout,otlp',
    );
    final otlpEndpoint = const String.fromEnvironment(
      'OTEL_EXPORTER_OTLP_ENDPOINT',
      defaultValue: 'http://localhost:4318',
    );

    // Configure console exporter
    _consoleEnabled = exporterType.contains('stdout') || exporterType.isEmpty;

    // Configure OTLP exporter
    if (exporterType.contains('otlp')) {
      try {
        // Remove trailing slash if present
        final baseEndpoint = otlpEndpoint.trim().replaceAll(RegExp(r'/$'), '');
        final logsEndpoint = Uri.parse('$baseEndpoint/v1/logs');

        // Create HTTP backend with error handler for diagnostics
        final backend = OpenTelemetryHttpBackend(
          endpoint: logsEndpoint,
          onPostError: ({required int statusCode, required String body}) async {
            print(
              '[LoggingService] HTTP error sending logs: status=$statusCode, body=$body',
            );
            print('[LoggingService] Endpoint was: $logsEndpoint');
          },
        );

        print(
          '[LoggingService] Created HTTP backend for endpoint: $logsEndpoint',
        );

        // Get service name for use in attributes (not service.name to avoid ambiguity)
        final serviceName = const String.fromEnvironment(
          'OTEL_SERVICE_NAME',
          defaultValue: 'holon-flutter',
        );

        _logger = OpenTelemetryLogger(
          backend: backend,
          batchSize: 10,
          flushInterval: const Duration(seconds: 5),
          // Use 'app.component' instead of 'service.name' to avoid ambiguity
          // service.name should be a resource attribute, but opentelemetry_logging
          // doesn't support resource attributes (sends empty resource: {})
          attributes: {'app.component': serviceName},
        );

        // Send a test log to verify connection (will be batched and sent after flushInterval)
        _logger!.info('LoggingService initialized successfully');

        // Force flush after a short delay to ensure test log is sent
        Future.delayed(const Duration(milliseconds: 100), () async {
          print('[LoggingService] Flushing logs...');
          await _logger?.flush();
          print('[LoggingService] Flush completed');
        });
      } catch (e, stackTrace) {
        // Log error but don't throw (fail silently to avoid breaking app)
        print('[LoggingService] Failed to initialize OTLP logger: $e');
        print('[LoggingService] Stack trace: $stackTrace');
        _logger = null;
      }
    }

    _initialized = true;
    // Note: Use print here instead of log to avoid circular dependency during initialization
    print('[LoggingService] Initialized with exporters: $exporterType');
    if (_logger != null) {
      final serviceName = const String.fromEnvironment(
        'OTEL_SERVICE_NAME',
        defaultValue: 'holon-flutter',
      );
      print('[LoggingService] OTLP endpoint: $otlpEndpoint/v1/logs');
      print(
        '[LoggingService] Service name: $serviceName (note: not set as resource attribute due to package limitations)',
      );
      print('[LoggingService] Logger is ready - test log sent');
    } else {
      print(
        '[LoggingService] WARNING: OTLP logger is null - logs will only go to console',
      );
    }
  }

  /// Log a message at INFO level
  static void info(String message) {
    final formattedMessage = _formatWithTraceContext(message);

    // Console output
    if (_consoleEnabled) {
      print(formattedMessage);
    }

    // OTLP export with trace context
    if (_logger != null) {
      try {
        _logger!.info(message, traceId: _currentTraceContext?.traceId);
      } catch (e, stackTrace) {
        // Fail silently to avoid breaking app
        if (_consoleEnabled) {
          print('[LoggingService] Failed to send log: $e');
          print('[LoggingService] Stack trace: $stackTrace');
        }
      }
    }
  }

  /// Log a message at DEBUG level
  static void debug(String message) {
    final formattedMessage = _formatWithTraceContext(message);

    // Console output
    if (_consoleEnabled) {
      debugPrint(formattedMessage);
    }

    // OTLP export with trace context
    if (_logger != null) {
      try {
        _logger!.debug(message, traceId: _currentTraceContext?.traceId);
      } catch (e) {
        // Fail silently to avoid breaking app
        if (_consoleEnabled) {
          debugPrint('[LoggingService] Failed to send log: $e');
        }
      }
    }
  }

  /// Log a message at WARN level
  static void warn(String message) {
    final formattedMessage = _formatWithTraceContext(message);

    // Console output
    if (_consoleEnabled) {
      print('[WARN] $formattedMessage');
    }

    // OTLP export with trace context
    if (_logger != null) {
      try {
        _logger!.warn(message, traceId: _currentTraceContext?.traceId);
      } catch (e) {
        // Fail silently to avoid breaking app
        if (_consoleEnabled) {
          print('[LoggingService] Failed to send log: $e');
        }
      }
    }
  }

  /// Log a message at ERROR level
  static void error(String message, {Object? error, StackTrace? stackTrace}) {
    // Format error message
    final errorMessage = error != null
        ? '$message\nError: $error${stackTrace != null ? "\n$stackTrace" : ""}'
        : message;
    final formattedMessage = _formatWithTraceContext(errorMessage);

    // Console output
    if (_consoleEnabled) {
      debugPrint('[ERROR] $formattedMessage');
    }

    // OTLP export with trace context
    if (_logger != null) {
      try {
        _logger!.error(errorMessage, traceId: _currentTraceContext?.traceId);
      } catch (e) {
        // Fail silently to avoid breaking app
        if (_consoleEnabled) {
          debugPrint('[LoggingService] Failed to send log: $e');
        }
      }
    }
  }

  /// Force flush all buffered logs
  static Future<void> flush() async {
    if (_logger != null) {
      try {
        print('[LoggingService] Calling flush() on logger...');
        await _logger!.flush();
        print('[LoggingService] Flush completed successfully');
      } catch (e, stackTrace) {
        if (_consoleEnabled) {
          print('[LoggingService] Failed to flush logs: $e');
          print('[LoggingService] Stack trace: $stackTrace');
        }
      }
    } else {
      print('[LoggingService] Cannot flush - logger is null');
    }
  }

  /// Shutdown logging service
  static Future<void> shutdown() async {
    // The opentelemetry_logging package should handle cleanup automatically
    // This method is kept for API compatibility
    _logger = null;
    _initialized = false;
  }

  /// Check if logging is initialized
  static bool get isInitialized => _initialized;

  /// Set the current trace context for log correlation.
  /// Logs emitted after this call will include trace_id and span_id.
  static void setTraceContext(LogTraceContext? context) {
    _currentTraceContext = context;
  }

  /// Get the current trace context
  static LogTraceContext? get currentTraceContext => _currentTraceContext;

  /// Clear the current trace context
  static void clearTraceContext() {
    _currentTraceContext = null;
  }

  /// Format message with trace context suffix for log correlation
  static String _formatWithTraceContext(String message) {
    if (_currentTraceContext != null) {
      return '$message | trace_id=${_currentTraceContext!.traceId} | span_id=${_currentTraceContext!.spanId}';
    }
    return '$message | trace_id= | span_id=';
  }
}
