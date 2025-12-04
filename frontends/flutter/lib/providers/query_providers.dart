import 'dart:async';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart'
    show RustStreamSink;
import 'package:holon/src/rust/third_party/holon_api/streaming.dart'
    show
        BatchMapChangeWithMetadata,
        MapChange,
        MapChange_Created,
        MapChange_Updated,
        MapChange_Deleted;
import '../services/backend_service.dart';
import '../services/mock_backend_service.dart';
import '../services/mcp_backend_wrapper.dart';
import '../utils/log.dart';
import '../src/rust/api/ffi_bridge.dart' as ffi;
import '../src/rust/third_party/holon_api.dart' show Value;
import '../src/rust/third_party/holon_api/render_types.dart' show RenderSpec;
import '../providers/settings_provider.dart' show prqlQueryProvider;
import '../utils/value_converter.dart' show valueMapToDynamic;

/// Provider for BackendService.
///
/// This can be overridden in tests to use MockBackendService.
/// Default implementation uses RustBackendService wrapped with MCP tools.
/// The McpBackendWrapper registers MCP tools (in debug mode) that allow
/// external agents like Claude to interact with the app.
final backendServiceProvider = Provider<BackendService>((ref) {
  // Default to RustBackendService wrapped with MCP tools
  // In tests, this can be overridden with MockBackendService
  return McpBackendWrapper(RustBackendService());
});

/// Provider for BackendEngine (kept for backward compatibility).
///
/// This is still used in some places that need direct engine access.
/// In the future, this can be removed in favor of BackendService.
final backendEngineProvider = Provider<ffi.ArcBackendEngine>((ref) {
  // This provider is kept for places that still need direct engine access
  // It should be initialized before use (via _globalEngine in main.dart)
  throw UnimplementedError(
    'backendEngineProvider: Engine must be initialized via main.dart',
  );
});

/// Reactive provider that executes PRQL query and watches for changes.
///
/// Using regular FutureProvider (not autoDispose) to prevent engine disposal.
/// It will still re-execute when dependencies change (prqlQueryProvider).
///
/// Retry is disabled because:
/// 1. Creating materialized views is expensive
/// 2. Query errors (syntax, schema) won't resolve themselves
/// 3. User must fix the query in settings, which invalidates prqlQueryProvider
final queryResultProvider =
    FutureProvider<
      ({
        RenderSpec renderSpec,
        List<Map<String, Value>> initialData,
        Stream<BatchMapChangeWithMetadata> changeStream,
      })
    >.internal(
      (ref) async {
        // Log to detect re-executions
        log.warn(
          '[queryResultProvider] EXECUTING - this should only happen once per query change',
        );

        // Watch backendServiceProvider to ensure service is available
        final backendService = ref.watch(backendServiceProvider);

        // Watch the PRQL query - this will re-execute when query changes
        // Directly await the future - ref.watch ensures it's reactive
        final prqlQuery = await ref.watch(prqlQueryProvider.future);

        try {
          log.debug('Executing query: $prqlQuery');

          // Query and set up CDC streaming
          // Each execution creates a fresh sink and stream
          final params = <String, Value>{};

          // In mock mode, use a regular Dart StreamController instead of RustStreamSink
          // RustStreamSink requires Rust to initialize the stream, which doesn't happen in mock mode
          Stream<BatchMapChangeWithMetadata> batchStream;
          RenderSpec renderSpec;
          List<Map<String, Value>> initialData;

          if (backendService is MockBackendService) {
            // Mock mode: get mock data directly without using RustStreamSink
            // RustStreamSink requires Rust initialization which isn't available in mock mode
            final mockStreamController =
                StreamController<BatchMapChangeWithMetadata>.broadcast();
            batchStream = mockStreamController.stream;

            // Get mock result directly (queryAndWatch signature requires sink, but we bypass it)
            (renderSpec, initialData) = backendService.getMockQueryResult();
            log.debug('Mock mode: using mock query result');
          } else {
            // Real mode: use RustStreamSink for CDC
            final batchSink = RustStreamSink<BatchMapChangeWithMetadata>();
            (renderSpec, initialData) = await backendService.queryAndWatch(
              prql: prqlQuery,
              params: params,
              sink: ffi.MapChangeSink(sink: batchSink),
              traceContext: null,
            );
            batchStream = batchSink.stream.asBroadcastStream();
          }

          log.debug('Query result count: ${initialData.length}');

          // Add OpenTelemetry logging when batches are received
          final loggedStream = batchStream.map((batchWithMetadata) {
            // Log batch reception with metadata details
            final relationName = batchWithMetadata.metadata.relationName;
            final changeCount = batchWithMetadata.inner.items.length;

            // Count change types
            int createdCount = 0;
            int updatedCount = 0;
            int deletedCount = 0;
            for (final change in batchWithMetadata.inner.items) {
              if (change is MapChange_Created) {
                createdCount++;
              } else if (change is MapChange_Updated) {
                updatedCount++;
              } else if (change is MapChange_Deleted) {
                deletedCount++;
              }
            }

            // Log batch reception with trace context if available
            final traceCtx = batchWithMetadata.metadata.traceContext;
            final traceInfo = traceCtx != null
                ? ' | trace_id=${traceCtx.traceId} | span_id=${traceCtx.spanId}'
                : ' | trace_id= | span_id=';
            log.debug(
              'Batch received: relation=$relationName, changes=$changeCount (created=$createdCount, updated=$updatedCount, deleted=$deletedCount)$traceInfo',
            );

            return batchWithMetadata;
          });

          return (
            renderSpec: renderSpec,
            initialData: initialData,
            changeStream: loggedStream,
          );
        } catch (e, stackTrace) {
          log.error('Error executing query', error: e, stackTrace: stackTrace);
          rethrow;
        }
      },
      from: null,
      argument: null,
      isAutoDispose: false,
      name: 'queryResultProvider',
      dependencies: null,
      $allTransitiveDependencies: null,
      retry: null, // Disable automatic retry - errors won't resolve themselves
    );

/// Provider for RenderSpec extracted from query result.
final renderSpecProvider = Provider<RenderSpec>((ref) {
  final queryResult = ref.watch(queryResultProvider);
  return queryResult.when(
    data: (result) => result.renderSpec,
    loading: () =>
        throw UnimplementedError('renderSpecProvider must be overridden'),
    error: (_, __) =>
        throw UnimplementedError('renderSpecProvider must be overridden'),
  );
});

/// Provider for initial data extracted from query result.
final initialDataProvider = Provider<List<Map<String, Value>>>((ref) {
  final queryResult = ref.watch(queryResultProvider);
  return queryResult.when(
    data: (result) => result.initialData,
    loading: () =>
        throw UnimplementedError('initialDataProvider must be overridden'),
    error: (_, __) =>
        throw UnimplementedError('initialDataProvider must be overridden'),
  );
});

/// Provider for initial data converted to dynamic types.
final transformedInitialDataProvider = Provider<List<Map<String, dynamic>>>((
  ref,
) {
  final initialData = ref.watch(initialDataProvider);
  return initialData.map((row) => valueMapToDynamic(row)).toList();
});

/// Provider for change stream extracted from query result.
/// Returns Stream<BatchMapChangeWithMetadata> to preserve metadata through the pipeline.
final changeStreamProvider = Provider<Stream<BatchMapChangeWithMetadata>>((
  ref,
) {
  final queryResult = ref.watch(queryResultProvider);
  return queryResult.when(
    data: (result) => result.changeStream,
    loading: () =>
        throw UnimplementedError('changeStreamProvider must be overridden'),
    error: (_, __) =>
        throw UnimplementedError('changeStreamProvider must be overridden'),
  );
});
