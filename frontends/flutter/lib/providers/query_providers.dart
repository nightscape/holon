import 'dart:async';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart'
    show RustStreamSink;
import 'package:holon/src/rust/third_party/holon_api/streaming.dart'
    show
        BatchMapChangeWithMetadata,
        MapChange_Created,
        MapChange_Updated,
        MapChange_Deleted;
import '../services/backend_service.dart';
import '../services/mock_backend_service.dart';
import '../services/mcp_backend_wrapper.dart';
import '../utils/log.dart';
import '../src/rust/api/ffi_bridge.dart' as ffi;
import '../src/rust/third_party/holon_api.dart' show Value;
import '../src/rust/third_party/holon_api/render_types.dart'
    show RenderExpr, RenderSpec;
import '../src/rust/third_party/holon_api/widget_spec.dart' show WidgetSpec;
import '../utils/value_converter.dart' show valueMapToDynamic;
import '../utils/query_id.dart' show prqlToQueryId, validateQueryId;

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

/// Reverse lookup: map from queryId to PRQL source.
///
/// This is populated when queries are executed and allows lookup by queryId.
/// Since Riverpod caches by PRQL string in queryResultByPrqlProvider,
/// we maintain a reverse mapping here for queryId-based access.
final _prqlSourceCache = <String, String>{};

/// Get PRQL source for a queryId.
///
/// Returns null if queryId was never registered (query never executed).
String? _getPrqlSourceForQueryId(String queryId) {
  return _prqlSourceCache[queryId];
}

/// Register PRQL source for a queryId.
void _registerPrqlSourceForQueryId(String queryId, String prql) {
  _prqlSourceCache[queryId] = prql;
}

/// Type alias for query result.
typedef QueryResult = ({
  RenderSpec renderSpec,
  List<Map<String, Value>> initialData,
  Stream<BatchMapChangeWithMetadata> changeStream,
});

/// Family provider for executing a specific PRQL query.
///
/// Each unique PRQL query string creates its own instance with independent CDC stream.
/// The queryId is automatically computed from the PRQL (normalized hash).
///
/// Use this for all queries (embedded queries in blocks, main UI, etc.).
///
/// Example:
/// ```dart
/// final result = ref.watch(queryResultByPrqlProvider('''
///   from blocks
///   filter file_path == "index.org"
///   render (tree parent_id:parent_id ...)
/// '''));
/// ```
final queryResultByPrqlProvider = FutureProvider.family<QueryResult, String>((
  ref,
  prqlQuery,
) async {
  // Register the PRQL source for queryId lookup
  final queryId = prqlToQueryId(prqlQuery);
  _registerPrqlSourceForQueryId(queryId, prqlQuery);

  log.debug(
    '[queryResultByPrqlProvider] Executing query (id: $queryId): ${prqlQuery.substring(0, prqlQuery.length.clamp(0, 50))}...',
  );

  final backendService = ref.watch(backendServiceProvider);

  try {
    final params = <String, Value>{};
    Stream<BatchMapChangeWithMetadata> batchStream;
    RenderSpec renderSpec;
    List<Map<String, Value>> initialData;

    if (backendService is MockBackendService) {
      final mockStreamController =
          StreamController<BatchMapChangeWithMetadata>.broadcast();
      batchStream = mockStreamController.stream;
      final mockResult = backendService.getMockQueryResult();
      renderSpec = mockResult.$1;
      initialData = mockResult.$2;
    } else {
      final batchSink = RustStreamSink<BatchMapChangeWithMetadata>();
      final widgetSpec = await backendService.queryAndWatch(
        prql: prqlQuery,
        params: params,
        sink: ffi.MapChangeSink(sink: batchSink),
        traceContext: null,
      );
      renderSpec = widgetSpec.renderSpec;
      initialData = widgetSpec.data;
      batchStream = batchSink.stream.asBroadcastStream();
    }

    log.debug(
      '[queryResultByPrqlProvider] Result count: ${initialData.length}',
    );

    // Add OpenTelemetry logging when batches are received
    final loggedStream = batchStream.map((batchWithMetadata) {
      final relationName = batchWithMetadata.metadata.relationName;
      final changeCount = batchWithMetadata.inner.items.length;

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
    log.error(
      '[queryResultByPrqlProvider] Error executing query',
      error: e,
      stackTrace: stackTrace,
    );
    rethrow;
  }
});

/// Query parameters including PRQL and optional context.
typedef QueryParams = ({String prql, String? contextBlockId});

/// Family provider for executing a PRQL query with optional context.
///
/// Use this when you need to pass a context block ID for `from children` resolution.
/// The contextBlockId determines which block's children are returned.
///
/// Example:
/// ```dart
/// final result = ref.watch(queryResultWithContextProvider((
///   prql: 'from children select {id, content}',
///   contextBlockId: 'block-123',
/// )));
/// ```
final queryResultWithContextProvider = FutureProvider.family<QueryResult, QueryParams>((
  ref,
  params,
) async {
  final prqlQuery = params.prql;
  final contextBlockId = params.contextBlockId;

  final queryId = prqlToQueryId(prqlQuery);
  _registerPrqlSourceForQueryId(queryId, prqlQuery);

  log.debug(
    '[queryResultWithContextProvider] Executing query (id: $queryId, context: $contextBlockId): ${prqlQuery.substring(0, prqlQuery.length.clamp(0, 50))}...',
  );

  final backendService = ref.watch(backendServiceProvider);

  try {
    final queryParams = <String, Value>{};
    Stream<BatchMapChangeWithMetadata> batchStream;
    RenderSpec renderSpec;
    List<Map<String, Value>> initialData;

    if (backendService is MockBackendService) {
      final mockStreamController =
          StreamController<BatchMapChangeWithMetadata>.broadcast();
      batchStream = mockStreamController.stream;
      final mockResult = backendService.getMockQueryResult();
      renderSpec = mockResult.$1;
      initialData = mockResult.$2;
    } else {
      final batchSink = RustStreamSink<BatchMapChangeWithMetadata>();
      final widgetSpec = await backendService.queryAndWatch(
        prql: prqlQuery,
        params: queryParams,
        sink: ffi.MapChangeSink(sink: batchSink),
        traceContext: null,
        contextBlockId: contextBlockId,
      );
      renderSpec = widgetSpec.renderSpec;
      initialData = widgetSpec.data;
      batchStream = batchSink.stream.asBroadcastStream();
    }

    log.debug(
      '[queryResultWithContextProvider] Result count: ${initialData.length}',
    );

    final loggedStream = batchStream.map((batchWithMetadata) {
      final relationName = batchWithMetadata.metadata.relationName;
      final changeCount = batchWithMetadata.inner.items.length;
      log.debug(
        'Batch received: relation=$relationName, changes=$changeCount',
      );
      return batchWithMetadata;
    });

    return (
      renderSpec: renderSpec,
      initialData: initialData,
      changeStream: loggedStream,
    );
  } catch (e, stackTrace) {
    log.error(
      '[queryResultWithContextProvider] Error executing query',
      error: e,
      stackTrace: stackTrace,
    );
    rethrow;
  }
});

/// Family provider for query result by queryId (normalized PRQL hash).
///
/// This provider requires that the PRQL source was already executed via queryResultByPrqlProvider.
/// It looks up the cached result by queryId.
///
/// Note: This is a convenience provider. In practice, you should use queryResultByPrqlProvider
/// directly with the PRQL string, which automatically computes the queryId.
///
/// Fails hard if queryId is not found (PRQL was never executed).
///
/// Example:
/// ```dart
/// final queryId = prqlToQueryId(prql);
/// // First ensure the query is executed
/// ref.watch(queryResultByPrqlProvider(prql));
/// // Then you can look it up by queryId
/// final result = ref.watch(queryResultByQueryIdProvider(queryId));
/// ```
final queryResultByQueryIdProvider = FutureProvider.family<QueryResult, String>((
  ref,
  queryId,
) async {
  validateQueryId(queryId);

  // Look up PRQL source for this queryId
  final prqlSource = _getPrqlSourceForQueryId(queryId);
  if (prqlSource == null) {
    throw StateError(
      'QueryId $queryId not found. Execute the query via queryResultByPrqlProvider(prql) first, which will register the queryId automatically.',
    );
  }

  // Use the existing PRQL-based provider (Riverpod will use cached result)
  return ref.watch(queryResultByPrqlProvider(prqlSource).future);
});

/// Family provider for RenderSpec extracted from query result by queryId.
///
/// Fails hard if queryId is not found.
final renderSpecProvider = Provider.family<RenderSpec, String>((ref, queryId) {
  validateQueryId(queryId);
  final queryResult = ref.watch(queryResultByQueryIdProvider(queryId));
  return queryResult.when(
    data: (result) => result.renderSpec,
    loading: () => RenderSpec(
      views: {},
      defaultView: "main",
      nestedQueries: [],
      operations: {},
      root: RenderExpr.literal(value: Value.string("loading...")),
      rowTemplates: [],
    ),
    error: (error, stackTrace) =>
        throw StateError('QueryId $queryId failed: $error'),
  );
});

/// Family provider for initial data extracted from query result by queryId.
final initialDataProvider = Provider.family<List<Map<String, Value>>, String>((
  ref,
  queryId,
) {
  validateQueryId(queryId);
  final queryResult = ref.watch(queryResultByQueryIdProvider(queryId));
  return queryResult.when(
    data: (result) => result.initialData,
    loading: () => throw StateError('QueryId $queryId is loading'),
    error: (error, stackTrace) =>
        throw StateError('QueryId $queryId failed: $error'),
  );
});

/// Family provider for initial data converted to dynamic types by queryId.
final transformedInitialDataProvider =
    Provider.family<List<Map<String, dynamic>>, String>((ref, queryId) {
      final initialData = ref.watch(initialDataProvider(queryId));
      return initialData.map((row) => valueMapToDynamic(row)).toList();
    });

/// Family provider for change stream extracted from query result by queryId.
/// Returns Stream<BatchMapChangeWithMetadata> to preserve metadata through the pipeline.
final changeStreamProvider =
    Provider.family<Stream<BatchMapChangeWithMetadata>, String>((ref, queryId) {
      validateQueryId(queryId);
      final queryResult = ref.watch(queryResultByQueryIdProvider(queryId));
      return queryResult.when(
        data: (result) => result.changeStream,
        loading: () => throw StateError('QueryId $queryId is loading'),
        error: (error, stackTrace) =>
            throw StateError('QueryId $queryId failed: $error'),
      );
    });
