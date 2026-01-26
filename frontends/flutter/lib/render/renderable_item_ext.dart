import '../src/rust/third_party/holon_api/render_types.dart';

/// A unified object combining row data, template, and operations.
///
/// This enables uni-directional data flow where operations are always
/// available with the item, avoiding repeated lookups.
class RenderableItem {
  final Map<String, dynamic> rowData;
  final RowTemplate template;
  final List<OperationDescriptor> operations;

  RenderableItem({
    required this.rowData,
    required this.template,
    List<OperationDescriptor>? operations,
  }) : operations = operations ?? _extractOperations(template.expr);

  /// Get the row ID
  String get id => rowData['id']?.toString() ?? '';

  /// Get the entity name (from row data or template)
  String get entityName =>
      rowData['entity_name']?.toString() ?? template.entityName;

  /// Get the entity short name (e.g., "task", "project")
  String get entityShortName => template.entityShortName;

  /// Get the render expression
  RenderExpr get expr => template.expr;

  /// Extract operations from the root FunctionCall of a RenderExpr.
  static List<OperationDescriptor> _extractOperations(RenderExpr expr) {
    if (expr case RenderExpr_FunctionCall(:final operations)) {
      return operations.map((w) => w.descriptor).toList();
    }
    return const [];
  }
}
