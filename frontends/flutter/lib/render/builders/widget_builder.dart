import 'package:flutter/material.dart';
import 'package:pie_menu/pie_menu.dart';
import '../../src/rust/third_party/holon_api/render_types.dart';
import '../render_context.dart';

/// Pre-resolved arguments for widget builders.
///
/// This enables bottom-up construction where:
/// - Named args are pre-evaluated to dynamic values
/// - Positional function calls are pre-built into widgets (children)
/// - Positional values are pre-evaluated (positionalValues)
/// - Template args are kept as RenderExpr for deferred instantiation
class ResolvedArgs {
  /// Named args pre-evaluated to dynamic values.
  /// e.g., `width: 8` → `{'width': 8}`, `content: this.name` → `{'content': 'John'}`
  final Map<String, dynamic> named;

  /// Positional args that were function calls, pre-built into widgets.
  /// e.g., `row(text("a"), text("b"))` → `[Text("a"), Text("b")]`
  /// Used by composition builders like row, block, columns.
  final List<Widget> children;

  /// Positional args that were values (not function calls), pre-evaluated.
  /// e.g., `text("hello")` → `["hello"]`, `icon('todoist')` → `["todoist"]`
  /// Used by builders like text, icon that expect value args.
  final List<dynamic> positionalValues;

  /// Template args kept as RenderExpr (only for list/tree/outline).
  /// e.g., `item_template: row(...)` → `{'item_template': RenderExpr}`
  final Map<String, RenderExpr> templates;

  const ResolvedArgs({
    required this.named,
    required this.children,
    this.positionalValues = const [],
    this.templates = const {},
  });

  /// Get first positional value as string (for text(), icon(), etc.)
  String getPositionalString(int index, [String defaultValue = '']) {
    if (index >= positionalValues.length) return defaultValue;
    final value = positionalValues[index];
    return value?.toString() ?? defaultValue;
  }

  /// Get an integer value with default.
  int getInt(String key, [int defaultValue = 0]) {
    final value = named[key];
    if (value is int) return value;
    if (value is num) return value.toInt();
    return defaultValue;
  }

  /// Get a string value with default.
  String getString(String key, [String defaultValue = '']) {
    final value = named[key];
    if (value == null) return defaultValue;
    return value.toString();
  }

  /// Get a boolean value with default.
  bool getBool(String key, [bool defaultValue = false]) {
    final value = named[key];
    if (value is bool) return value;
    return defaultValue;
  }

  /// Get a double value with default.
  double getDouble(String key, [double defaultValue = 0.0]) {
    final value = named[key];
    if (value is double) return value;
    if (value is num) return value.toDouble();
    return defaultValue;
  }

  /// Get a num value with default.
  num getNum(String key, [num defaultValue = 0]) {
    final value = named[key];
    if (value is num) return value;
    return defaultValue;
  }

  /// Get a typed value, returning null if not present or wrong type.
  T? get<T>(String key) {
    final value = named[key];
    if (value is T) return value;
    return null;
  }

  /// Check if a named arg exists.
  bool has(String key) => named.containsKey(key);

  /// Get the field name for a named arg that came from a column ref.
  /// Returns null if the arg wasn't a column ref or doesn't exist.
  /// E.g., for `checked: this.completed`, `getFieldName('checked')` returns 'completed'.
  String? getFieldName(String key) => named['_${key}_field'] as String?;
}

/// Standard factory for widget builders (35 of 38 builders).
/// No external dependencies needed - receives pre-resolved args.
typedef WidgetBuilderFactory = Widget Function(
  ResolvedArgs args,
  RenderContext context,
);

/// Template factory for list/tree/outline builders (3 builders).
/// Receives function to instantiate templates per row.
typedef TemplateBuilderFactory = Widget Function(
  ResolvedArgs args,
  RenderContext context,
  Widget Function(RenderExpr template, RenderContext rowContext) buildTemplate,
);

/// Static helpers for operations and pie menu functionality.
/// These are stateless utilities used by interactive builders.
class OperationHelpers {
  OperationHelpers._();

  /// Find an operation to update a field value.
  /// Priority: field-specific ops → set_field for entity → any set_field
  static OperationDescriptor? findSetFieldOperation(
    String fieldName,
    RenderContext context,
  ) {
    // 1. Field-specific operations
    var ops = context.operationsAffecting([fieldName]);
    if (ops.isNotEmpty) return ops.first;

    // 2. set_field for current entity
    ops = context.availableOperations
        .where(
          (op) =>
              op.name == 'set_field' &&
              (context.entityName == null ||
                  op.entityName == context.entityName),
        )
        .toList();
    if (ops.isNotEmpty) return ops.first;

    // 3. Any set_field
    return context.availableOperations
        .where((op) => op.name == 'set_field')
        .firstOrNull;
  }

  /// Automatically attach pie menu to a widget based on field interests.
  ///
  /// If operations are available that affect any of the specified fields,
  /// wraps the child widget in a PieMenu (requires PieCanvas ancestor).
  static Widget autoAttachPieMenu(
    Widget child,
    List<String> fieldsOfInterest,
    RenderContext context,
  ) {
    // Find operations that affect any of these fields
    final relevantOps = context.operationsAffecting(fieldsOfInterest);
    if (relevantOps.isEmpty) return child;

    return attachTapPieMenu(child, relevantOps, context);
  }

  /// Attach pie menu triggered by tap with the given operations.
  static Widget attachTapPieMenu(
    Widget child,
    List<OperationDescriptor> operations,
    RenderContext context,
  ) {
    if (operations.isEmpty) return child;

    // Map operations to pie actions
    final actions = operations.map((op) {
      return PieAction(
        tooltip: Text(op.displayName.isNotEmpty ? op.displayName : op.name),
        onSelect: () {
          final params = <String, dynamic>{'id': context.rowData['id']};

          // Handle special cases (e.g., indent needs parent_id from previous row)
          if (op.name == 'indent' &&
              context.rowIndex != null &&
              context.rowIndex! > 0) {
            if (context.previousRowData != null) {
              final previousId = context.previousRowData!['id'];
              if (previousId != null) {
                params['parent_id'] = previousId.toString();
              }
            }
          }

          // Add other required parameters if available in row data
          for (final param in op.requiredParams) {
            if (context.rowData.containsKey(param.name)) {
              params[param.name] = context.rowData[param.name];
            }
          }

          // Use entity_name from row data (for UNION queries), then operation descriptor, then context
          final entityName =
              context.rowData['entity_name']?.toString() ??
              (op.entityName.isNotEmpty ? op.entityName : null) ??
              context.entityName;

          if (entityName == null) {
            throw StateError(
              'Cannot dispatch operation "${op.name}": no entity_name found in row data, '
              'operation descriptor, or context. This is a bug.',
            );
          }

          context.onOperation?.call(entityName, op.name, params);
        },
        child: Icon(iconForOperation(op)),
      );
    }).toList();

    return PieMenu(
      theme: createPieTheme(),
      actions: actions,
      child: child,
    );
  }

  /// Get icon for an operation based on its name.
  static IconData iconForOperation(OperationDescriptor op) {
    final name = op.name.toLowerCase();
    if (name.contains('indent')) {
      return Icons.subdirectory_arrow_right;
    } else if (name.contains('outdent')) {
      return Icons.subdirectory_arrow_left;
    } else if (name.contains('collapse') || name.contains('expand')) {
      return Icons.expand_more;
    } else if (name.contains('move_up') || name.contains('moveup')) {
      return Icons.arrow_upward;
    } else if (name.contains('move_down') || name.contains('movedown')) {
      return Icons.arrow_downward;
    } else if (name.contains('delete') || name.contains('remove')) {
      return Icons.delete;
    } else if (name.contains('status')) {
      return Icons.circle;
    } else if (name.contains('complete')) {
      return Icons.check_circle;
    } else if (name.contains('priority')) {
      return Icons.flag;
    } else if (name.contains('due') || name.contains('date')) {
      return Icons.calendar_today;
    } else if (name.contains('split')) {
      return Icons.content_cut;
    }
    return Icons.more_horiz;
  }

  /// Create a beautiful pie menu theme with glassy buttons.
  static PieTheme createPieTheme() {
    return PieTheme(
      regularPressShowsMenu: true,
      longPressShowsMenu: false,
      overlayColor: Colors.white.withOpacity(0.05),
      buttonTheme: PieButtonTheme(
        backgroundColor: Colors.transparent,
        iconColor: const Color(0xFF1F2937),
        decoration: BoxDecoration(
          gradient: LinearGradient(
            begin: Alignment.topLeft,
            end: Alignment.bottomRight,
            colors: [
              Colors.white.withOpacity(0.9),
              Colors.white.withOpacity(0.7),
            ],
          ),
          border: Border.all(color: Colors.white.withOpacity(0.5), width: 1.5),
          shape: BoxShape.circle,
          boxShadow: [
            BoxShadow(
              color: Colors.black.withOpacity(0.1),
              blurRadius: 8,
              offset: const Offset(0, 2),
            ),
            BoxShadow(
              color: Colors.white.withOpacity(0.5),
              blurRadius: 4,
              offset: const Offset(-1, -1),
            ),
          ],
        ),
      ),
      buttonThemeHovered: PieButtonTheme(
        backgroundColor: Colors.transparent,
        iconColor: const Color(0xFF3B82F6),
        decoration: BoxDecoration(
          gradient: LinearGradient(
            begin: Alignment.topLeft,
            end: Alignment.bottomRight,
            colors: [
              Colors.white.withOpacity(0.95),
              Colors.white.withOpacity(0.85),
            ],
          ),
          border: Border.all(
            color: const Color(0xFF3B82F6).withOpacity(0.3),
            width: 2,
          ),
          shape: BoxShape.circle,
          boxShadow: [
            BoxShadow(
              color: Colors.black.withOpacity(0.15),
              blurRadius: 12,
              offset: const Offset(0, 3),
            ),
            BoxShadow(
              color: const Color(0xFF3B82F6).withOpacity(0.2),
              blurRadius: 6,
              offset: const Offset(0, 0),
            ),
          ],
        ),
      ),
    );
  }
}
