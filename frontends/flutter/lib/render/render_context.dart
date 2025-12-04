import '../src/rust/third_party/holon_api/render_types.dart';
import 'reactive_query_widget.dart';
import '../styles/app_styles.dart';

/// Context passed to widget builders during rendering.
/// Contains row data and configuration needed to build widgets.
class RenderContext {
  /// Current row data being rendered (from SQL query results).
  final Map<String, dynamic> rowData;

  /// Callback for executing operations (indent, outdent, etc.).
  /// Parameters: entityName, operationName, params
  final Future<void> Function(
    String entityName,
    String operationName,
    Map<String, dynamic> params,
  )?
  onOperation;

  /// Configuration for nested queries (if applicable).
  final Map<String, dynamic>? nestedQueryConfig;

  /// Available operations for this context (extracted from RenderExpr FunctionCall nodes).
  final List<OperationDescriptor> availableOperations;

  /// Entity name for this context (e.g., "blocks", "todoist_tasks").
  /// Extracted from operation descriptors or query metadata.
  final String? entityName;

  /// Row index in the list (for operations that need context from other rows).
  final int? rowIndex;

  /// Previous row data (for operations like indent that need parent_id).
  final Map<String, dynamic>? previousRowData;

  /// Row cache for outline widget (id -> row data).
  final Map<String, Map<String, dynamic>>? rowCache;

  /// Change stream for CDC updates (used by outline widget).
  final Stream<RowEvent>? changeStream;

  /// Parent ID column name for outline widget (e.g., "parent_id").
  final String? parentIdColumn;

  /// Sort key column name for outline widget (e.g., "sort_key").
  final String? sortKeyColumn;

  /// Row templates for heterogeneous UNION queries.
  /// Each template has an index matching the `ui` column value in rows.
  final List<RowTemplate> rowTemplates;

  /// Theme colors for rendering (optional, defaults to light theme).
  final AppColors colors;

  /// Current focus depth (0.0 = overview, 1.0 = deep flow).
  /// Widgets can adapt their rendering based on this value
  /// for progressive concealment.
  final double focusDepth;

  const RenderContext({
    required this.rowData,
    required this.rowTemplates,
    this.onOperation,
    this.nestedQueryConfig,
    this.availableOperations = const [],
    this.entityName,
    this.rowIndex,
    this.previousRowData,
    this.rowCache,
    this.changeStream,
    this.parentIdColumn,
    this.sortKeyColumn,
    this.colors = AppColors.light,
    this.focusDepth = 0.0,
  });

  /// Get a column value from row data, returns null if not found.
  dynamic getColumn(String name) => rowData[name];

  /// Get a column value with type casting, throws if type mismatch.
  T getTypedColumn<T>(String name) {
    final value = rowData[name];
    if (value is! T) {
      throw ArgumentError(
        'Column $name is not of type $T (got ${value.runtimeType})',
      );
    }
    return value;
  }

  /// Filter operations that affect any of the given fields.
  ///
  /// Returns operations where `affected_fields` intersects with the provided fields.
  List<OperationDescriptor> operationsAffecting(List<String> fields) {
    return availableOperations.where((op) {
      return op.affectedFields.any((field) => fields.contains(field));
    }).toList();
  }
}
