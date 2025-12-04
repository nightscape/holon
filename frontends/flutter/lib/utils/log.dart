import 'package:flutter/foundation.dart' show debugPrint;
import '../services/logging_service.dart';

/// Simple logging utility that forwards to both console and OpenTelemetry
///
/// Usage:
/// ```dart
/// log.info('This is an info message');
/// log.debug('This is a debug message');
/// log.warn('This is a warning');
/// log.error('This is an error', error: e, stackTrace: stackTrace);
/// ```
class Log {
  /// Log an info message
  void info(String message) {
    print(message);
    if (LoggingService.isInitialized) {
      LoggingService.info(message);
    }
  }

  /// Log a debug message
  void debug(String message) {
    debugPrint(message);
    if (LoggingService.isInitialized) {
      LoggingService.debug(message);
    }
  }

  /// Log a warning message
  void warn(String message) {
    print('[WARN] $message');
    if (LoggingService.isInitialized) {
      LoggingService.warn(message);
    }
  }

  /// Log an error message
  void error(String message, {Object? error, StackTrace? stackTrace}) {
    final errorMsg = error != null
        ? '$message\nError: $error${stackTrace != null ? "\n$stackTrace" : ""}'
        : message;
    debugPrint('[ERROR] $errorMsg');
    if (LoggingService.isInitialized) {
      LoggingService.error(message, error: error, stackTrace: stackTrace);
    }
  }
}

/// Global log instance for convenient access
final log = Log();
