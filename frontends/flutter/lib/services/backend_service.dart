import 'dart:async';
import '../src/rust/third_party/holon_api.dart' show Value;
import '../src/rust/third_party/holon_api/render_types.dart'
    show OperationDescriptor, RenderSpec;
import '../src/rust/api/ffi_bridge.dart' as ffi;
import '../src/rust/api/types.dart' show TraceContext;

/// Abstract interface for backend operations.
///
/// This abstraction allows swapping between real Rust FFI implementation
/// and mock implementations for testing.
abstract class BackendService {
  /// Compile a PRQL query, execute it, and set up CDC streaming.
  ///
  /// Returns the render specification and current query results.
  /// Change events are sent to the provided [sink].
  ///
  /// Parameters:
  /// - [prql]: PRQL query string
  /// - [params]: Query parameters
  /// - [sink]: Sink for receiving change events
  ///
  /// Returns:
  /// A tuple containing:
  /// - [RenderSpec]: UI rendering specification from the PRQL query
  /// - [List<Map<String, Value>>]: Current query results
  Future<(RenderSpec, List<Map<String, Value>>)> queryAndWatch({
    required String prql,
    required Map<String, Value> params,
    required ffi.MapChangeSink sink,
    TraceContext? traceContext,
  });

  /// Get available operations for an entity.
  ///
  /// Returns a list of operation descriptors available for the given entityName.
  /// Use "*" as entityName to get wildcard operations.
  Future<List<OperationDescriptor>> availableOperations({
    required String entityName,
  });

  /// Execute an operation on the database.
  ///
  /// Operations mutate the database directly. UI updates happen via CDC streams.
  /// This follows the unidirectional data flow: Action → Model → View
  ///
  /// Parameters:
  /// - [entityName]: Name of the entity (e.g., "blocks")
  /// - [opName]: Name of the operation (e.g., "indent", "outdent")
  /// - [params]: Operation parameters
  Future<void> executeOperation({
    required String entityName,
    required String opName,
    required Map<String, Value> params,
    TraceContext? traceContext,
  });

  /// Check if an operation is available for an entity.
  ///
  /// Returns:
  /// `true` if the operation is available, `false` otherwise
  Future<bool> hasOperation({
    required String entityName,
    required String opName,
  });

  /// Undo the last operation
  ///
  /// Returns:
  /// `true` if an operation was undone, `false` if nothing to undo
  Future<bool> undo();

  /// Redo the last undone operation
  ///
  /// Returns:
  /// `true` if an operation was redone, `false` if nothing to redo
  Future<bool> redo();

  /// Check if undo is available
  Future<bool> canUndo();

  /// Check if redo is available
  Future<bool> canRedo();
}

/// Rust FFI implementation of BackendService.
///
/// This wraps the actual Rust FFI calls from ffi_bridge.dart.
/// Note: This implementation requires the Rust library to be initialized.
class RustBackendService implements BackendService {
  RustBackendService();

  @override
  @override
  Future<(RenderSpec, List<Map<String, Value>>)> queryAndWatch({
    required String prql,
    required Map<String, Value> params,
    required ffi.MapChangeSink sink,
    TraceContext? traceContext,
  }) async {
    return await ffi.queryAndWatch(
      prql: prql,
      params: params,
      sink: sink,
      traceContext: traceContext,
    );
  }

  @override
  Future<List<OperationDescriptor>> availableOperations({
    required String entityName,
  }) async {
    return await ffi.availableOperations(entityName: entityName);
  }

  @override
  Future<void> executeOperation({
    required String entityName,
    required String opName,
    required Map<String, Value> params,
    TraceContext? traceContext,
  }) async {
    return await ffi.executeOperation(
      entityName: entityName,
      opName: opName,
      params: params,
      traceContext: traceContext,
    );
  }

  @override
  Future<bool> hasOperation({
    required String entityName,
    required String opName,
  }) async {
    return await ffi.hasOperation(entityName: entityName, opName: opName);
  }

  @override
  Future<bool> undo() async {
    return await ffi.undo();
  }

  @override
  Future<bool> redo() async {
    return await ffi.redo();
  }

  @override
  Future<bool> canUndo() async {
    return await ffi.canUndo();
  }

  @override
  Future<bool> canRedo() async {
    return await ffi.canRedo();
  }
}
