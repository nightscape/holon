import 'dart:async';
import 'package:flutter/services.dart' show rootBundle;
import 'package:yaml/yaml.dart';
import 'backend_service.dart';
import '../src/rust/third_party/holon_api/streaming.dart' show MapChange;
import '../src/rust/api/types.dart' show TraceContext;
import '../src/rust/third_party/holon_api.dart' show Value;
import '../src/rust/third_party/holon_api/render_types.dart'
    show OperationDescriptor, RenderSpec, RenderExpr, Arg, RowTemplate;
import '../src/rust/api/ffi_bridge.dart' as ffi show MapChangeSink;

/// Mock implementation of BackendService for testing.
///
/// This allows test code to:
/// - Control query results
/// - Inject change events programmatically
/// - Verify operation calls
/// - Test without Rust FFI dependencies
class MockBackendService implements BackendService {
  /// Current query results (can be set by tests)
  (RenderSpec, List<Map<String, Value>>)? _queryResult;

  /// Stream controller for change events (injected by tests)
  final StreamController<MapChange> _changeStreamController =
      StreamController<MapChange>.broadcast();

  /// List of operation calls made (for verification)
  final List<OperationCall> _operationCalls = [];

  /// Map of available operations (entityName -> List<OperationDescriptor>)
  final Map<String, List<OperationDescriptor>> _availableOperations = {};

  /// Whether sync should succeed or fail
  bool _syncShouldSucceed = true;

  /// Error to throw on sync (if _syncShouldSucceed is false)
  Exception? _syncError;

  MockBackendService();

  /// Set the query result that will be returned by queryAndWatch.
  void setQueryResult(
    RenderSpec renderSpec,
    List<Map<String, Value>> initialData,
  ) {
    _queryResult = (renderSpec, initialData);
  }

  /// Cached mock data loaded from YAML
  static (RenderSpec, List<Map<String, Value>>)? _cachedMockData;

  /// Get mock query result directly (bypasses sink creation for mock mode).
  ///
  /// This avoids the need to create RustStreamSink which requires Rust initialization.
  (RenderSpec, List<Map<String, Value>>) getMockQueryResult() {
    if (_queryResult != null) {
      return _queryResult!;
    }
    // Return cached mock data or fallback
    return _cachedMockData ?? _createFallbackData();
  }

  /// Load mock data from assets/mock_data.yaml
  static Future<void> loadMockData() async {
    try {
      final yamlString = await rootBundle.loadString('assets/mock_data.yaml');
      final yaml = loadYaml(yamlString);
      _cachedMockData = _parseMockData(yaml);
    } catch (e) {
      // Fall back to hardcoded data if YAML loading fails
      _cachedMockData = _createFallbackData();
    }
  }

  /// Parse YAML into RenderSpec and data
  static (RenderSpec, List<Map<String, Value>>) _parseMockData(YamlMap yaml) {
    // Parse row templates
    final rowTemplates = <RowTemplate>[];
    final yamlTemplates = yaml['row_templates'] as YamlList?;
    if (yamlTemplates != null) {
      for (final t in yamlTemplates) {
        rowTemplates.add(
          RowTemplate(
            index: BigInt.from(t['index'] as int),
            entityName: t['entity_name'] as String,
            entityShortName: t['entity_short_name'] as String,
            expr: _parseExpr(t['expr']),
          ),
        );
      }
    }

    // Parse tree configuration
    final tree = yaml['tree'] as YamlMap?;
    final parentIdColumn = tree?['parent_id_column'] as String? ?? 'parent_id';
    final sortKeyColumn = tree?['sort_key_column'] as String? ?? 'sort_key';

    // Build RenderSpec with tree() as root
    final renderSpec = RenderSpec(
      root: RenderExpr.functionCall(
        name: 'tree',
        args: [
          Arg(
            name: 'parent_id',
            value: RenderExpr.columnRef(name: parentIdColumn),
          ),
          Arg(
            name: 'sortkey',
            value: RenderExpr.columnRef(name: sortKeyColumn),
          ),
          const Arg(
            name: 'item_template',
            value: RenderExpr.columnRef(name: 'ui'),
          ),
        ],
        operations: const [],
      ),
      nestedQueries: const [],
      operations: const {},
      rowTemplates: rowTemplates,
    );

    // Parse data rows
    final data = <Map<String, Value>>[];
    final yamlData = yaml['data'] as YamlList?;
    if (yamlData != null) {
      for (final row in yamlData) {
        data.add(_parseRow(row as YamlMap));
      }
    }

    return (renderSpec, data);
  }

  /// Parse a YAML row into a Map<String, Value>
  static Map<String, Value> _parseRow(YamlMap row) {
    final result = <String, Value>{};
    for (final entry in row.entries) {
      final key = entry.key as String;
      result[key] = _parseValue(entry.value);
    }
    return result;
  }

  /// Parse a YAML value into a Value
  static Value _parseValue(dynamic v) {
    if (v == null) return const Value.null_();
    if (v is bool) return Value.boolean(v);
    if (v is int) return Value.integer(v);
    if (v is double) return Value.float(v);
    if (v is String) return Value.string(v);
    if (v is YamlList) return Value.array(v.map(_parseValue).toList());
    if (v is YamlMap) {
      final map = <String, Value>{};
      for (final entry in v.entries) {
        map[entry.key as String] = _parseValue(entry.value);
      }
      return Value.object(map);
    }
    return Value.string(v.toString());
  }

  /// Parse a YAML expression into RenderExpr
  static RenderExpr _parseExpr(dynamic expr) {
    if (expr is YamlMap) {
      // Check for column reference
      if (expr.containsKey('column')) {
        return RenderExpr.columnRef(name: expr['column'] as String);
      }
      // Check for function call
      if (expr.containsKey('function')) {
        final name = expr['function'] as String;
        final args = <Arg>[];

        // Parse positional args
        if (expr.containsKey('args')) {
          final yamlArgs = expr['args'] as YamlList;
          for (final arg in yamlArgs) {
            if (arg is YamlMap) {
              args.add(Arg(value: _parseExpr(arg)));
            } else {
              args.add(Arg(value: RenderExpr.literal(value: _parseValue(arg))));
            }
          }
        }

        // Parse named args
        if (expr.containsKey('named_args')) {
          final namedArgs = expr['named_args'] as YamlMap;
          for (final entry in namedArgs.entries) {
            args.add(
              Arg(name: entry.key as String, value: _parseExpr(entry.value)),
            );
          }
        }

        return RenderExpr.functionCall(
          name: name,
          args: args,
          operations: const [],
        );
      }
    }
    // Fallback to literal
    return RenderExpr.literal(value: _parseValue(expr));
  }

  /// Create fallback data if YAML loading fails
  static (RenderSpec, List<Map<String, Value>>) _createFallbackData() {
    final itemTemplate = RowTemplate(
      index: BigInt.zero,
      entityName: 'mock_items',
      entityShortName: 'item',
      expr: RenderExpr.functionCall(
        name: 'row',
        args: [
          Arg(
            value: RenderExpr.functionCall(
              name: 'text',
              args: [
                const Arg(
                  name: 'content',
                  value: RenderExpr.columnRef(name: 'content'),
                ),
              ],
              operations: const [],
            ),
          ),
        ],
        operations: const [],
      ),
    );

    final renderSpec = RenderSpec(
      root: RenderExpr.functionCall(
        name: 'tree',
        args: const [
          Arg(
            name: 'parent_id',
            value: RenderExpr.columnRef(name: 'parent_id'),
          ),
          Arg(
            name: 'sortkey',
            value: RenderExpr.columnRef(name: 'sort_key'),
          ),
          Arg(
            name: 'item_template',
            value: RenderExpr.columnRef(name: 'ui'),
          ),
        ],
        operations: const [],
      ),
      nestedQueries: const [],
      operations: const {},
      rowTemplates: [itemTemplate],
    );

    final data = <Map<String, Value>>[
      {
        'id': const Value.string('fallback-1'),
        'parent_id': const Value.null_(),
        'content': const Value.string('Mock data (YAML loading failed)'),
        'entity_name': const Value.string('mock_items'),
        'sort_key': const Value.string('01'),
        'ui': const Value.integer(0),
      },
    ];

    return (renderSpec, data);
  }

  /// Emit a change event to the stream.
  ///
  /// This allows tests to simulate change events from the backend.
  void emitChange(MapChange change) {
    _changeStreamController.add(change);
  }

  /// Emit multiple change events in sequence.
  void emitChanges(List<MapChange> changes) {
    for (final change in changes) {
      _changeStreamController.add(change);
    }
  }

  /// Get the list of operation calls made (for verification).
  List<OperationCall> get operationCalls => List.unmodifiable(_operationCalls);

  /// Clear the operation calls list.
  void clearOperationCalls() {
    _operationCalls.clear();
  }

  /// Set which operations are available.
  void setAvailableOperations(
    String entityName,
    List<OperationDescriptor> operations,
  ) {
    _availableOperations[entityName] = List.from(operations);
  }

  /// Set whether sync should succeed or fail.
  void setSyncBehavior({required bool shouldSucceed, Exception? error}) {
    _syncShouldSucceed = shouldSucceed;
    _syncError = error;
  }

  /// Get the change stream controller (for advanced test scenarios).
  StreamController<MapChange> get changeStreamController =>
      _changeStreamController;

  @override
  Future<(RenderSpec, List<Map<String, Value>>)> queryAndWatch({
    required String prql,
    required Map<String, Value> params,
    required ffi.MapChangeSink sink,
    TraceContext? traceContext,
  }) async {
    // Set up stream forwarding to the sink
    // Note: RustStreamSink doesn't expose add/addError/close directly
    // Instead, we'll forward events through the sink's stream mechanism
    // For testing, we'll use a different approach - the mock will manage
    // its own stream and tests can inject events via emitChange()

    // In a real implementation, the Rust side would add events to the sink
    // For the mock, we'll set up a listener that forwards to the sink's stream
    // However, since RustStreamSink is opaque, we need a different approach

    // For now, we'll just return the result. The test will need to handle
    // stream injection separately, or we can modify the interface to return
    // the stream directly. Let's keep it simple for now and return the result.

    // Return the configured query result or default
    if (_queryResult != null) {
      return _queryResult!;
    }

    // Default empty result
    return (
      RenderSpec(
        root: const RenderExpr.columnRef(name: 'id'),
        nestedQueries: const [],
        operations: const {},
        rowTemplates: const [],
      ),
      <Map<String, Value>>[],
    );
  }

  /// Get the change stream for testing purposes.
  /// This allows tests to listen to changes without going through the sink.
  Stream<MapChange> get changeStream => _changeStreamController.stream;

  @override
  Future<List<OperationDescriptor>> availableOperations({
    required String entityName,
  }) async {
    if (entityName == '*') {
      // Return mock wildcard operations
      return [
        OperationDescriptor(
          entityName: '*',
          entityShortName: 'all',
          idColumn: '',
          name: 'sync',
          displayName: 'Sync',
          description: 'Sync providers',
          requiredParams: const [],
          affectedFields: const [],
          paramMappings: const [],
        ),
      ];
    }
    // Return operations for the entity if configured
    return _availableOperations[entityName] ?? [];
  }

  @override
  Future<void> executeOperation({
    required String entityName,
    required String opName,
    required Map<String, Value> params,
    TraceContext? traceContext,
  }) async {
    // Record the operation call
    _operationCalls.add(
      OperationCall(
        entityName: entityName,
        opName: opName,
        params: Map.from(params),
      ),
    );

    // Simulate async operation
    await Future.delayed(const Duration(milliseconds: 10));
  }

  @override
  Future<bool> hasOperation({
    required String entityName,
    required String opName,
  }) async {
    final ops = _availableOperations[entityName] ?? [];
    return ops.any((op) => op.name == opName);
  }

  @override
  Future<bool> undo() async => false;

  @override
  Future<bool> redo() async => false;

  @override
  Future<bool> canUndo() async => false;

  @override
  Future<bool> canRedo() async => false;

  /// Dispose resources.
  void dispose() {
    _changeStreamController.close();
  }
}

/// Record of an operation call for verification.
class OperationCall {
  final String entityName;
  final String opName;
  final Map<String, Value> params;

  OperationCall({
    required this.entityName,
    required this.opName,
    required this.params,
  });

  @override
  String toString() {
    return 'OperationCall(entityName: $entityName, opName: $opName, params: $params)';
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is OperationCall &&
          runtimeType == other.runtimeType &&
          entityName == other.entityName &&
          opName == other.opName &&
          _mapEquals(params, other.params);

  @override
  int get hashCode => Object.hash(entityName, opName, _mapHash(params));
}

bool _mapEquals(Map<String, Value> a, Map<String, Value> b) {
  if (a.length != b.length) return false;
  for (final entry in a.entries) {
    if (b[entry.key] != entry.value) return false;
  }
  return true;
}

int _mapHash(Map<String, Value> map) {
  return Object.hashAll(map.entries.map((e) => Object.hash(e.key, e.value)));
}
