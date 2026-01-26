import 'package:flutter/material.dart';
import '../src/rust/third_party/holon_api.dart';
import '../src/rust/third_party/holon_api/render_types.dart';
import 'render_context.dart';
export 'render_context.dart';
import 'builders/widget_builder.dart';
import 'builders/builder_registry.dart';

/// Interprets generic RenderExpr AST and builds Flutter widgets.
///
/// This interpreter maps function calls to Flutter widgets:
/// - `list(...)` → ListView.builder
/// - `block(...)` → Column with indentation
/// - `editable_text(...)` → TextField
/// - `row(...)` → Row
/// - Custom functions can be added via extensibility
///
/// Uses a registry-based dispatch for extensibility. Each widget builder
/// is a separate class with a static build method registered in BuilderRegistry.
class RenderInterpreter {
  /// Registry of widget builder factories.
  final BuilderRegistry _registry;

  /// Create interpreter with default builders.
  RenderInterpreter() : _registry = BuilderRegistry.createDefault();

  /// Create interpreter with custom registry (for testing/extension).
  RenderInterpreter.withRegistry(this._registry);

  /// Build a widget from a RenderExpr using the provided context.
  Widget build(RenderExpr expr, RenderContext context) {
    return expr.when(
      functionCall: (name, args, wirings) =>
          _buildFunctionCall(name, args, wirings, context),
      columnRef: (name) => _buildColumnRef(name, context),
      literal: (value) => _buildLiteral(value),
      binaryOp: (op, left, right) => _buildBinaryOp(op, left, right, context),
      array: (items) => _buildArray(items, context),
      object: (fields) => _buildObject(fields, context),
    );
  }

  /// Build widget for a specific named view
  ///
  /// Applies the view's filter to select rows and then builds the view's structure.
  ///
  /// [allRows] - All rows from the query result (before filtering)
  /// [spec] - The render specification containing view definitions
  /// [viewName] - Name of the view to build
  /// [baseContext] - Base context to use (will be updated with filtered rows)
  Widget buildView(
    List<Map<String, dynamic>> allRows,
    RenderSpec spec,
    String viewName,
    RenderContext baseContext,
  ) {
    // Get the view spec, falling back to default view if not found
    // Note: This will work once RenderSpec.views and RenderSpec.defaultView are added to Dart bindings
    final viewSpec = _getViewSpec(spec, viewName);
    if (viewSpec == null) {
      return const Text('No view available');
    }

    // Apply filter to get rows for this view
    final filteredRows = applyFilter(allRows, viewSpec.filter);

    // Build the view structure with filtered rows
    // For collection widgets (list, tree), they will iterate over filteredRows
    // For now, we'll build with the first filtered row as context
    // The actual collection widgets will handle iteration
    if (filteredRows.isEmpty) {
      return const SizedBox.shrink();
    }

    // Create a context with the first row (collection widgets handle their own iteration)
    final context = RenderContext(
      rowData: filteredRows.first,
      rowTemplates: baseContext.rowTemplates,
      onOperation: baseContext.onOperation,
      nestedQueryConfig: baseContext.nestedQueryConfig,
      availableOperations: baseContext.availableOperations,
      entityName: baseContext.entityName,
      rowIndex: baseContext.rowIndex,
      previousRowData: baseContext.previousRowData,
      rowCache: baseContext.rowCache,
      changeStream: baseContext.changeStream,
      parentIdColumn: baseContext.parentIdColumn,
      sortKeyColumn: baseContext.sortKeyColumn,
      colors: baseContext.colors,
      focusDepth: baseContext.focusDepth,
      queryParams: baseContext.queryParams,
    );

    return build(viewSpec.structure, context);
  }

  /// Get view spec for a view name, falling back to default view
  ViewSpec? _getViewSpec(RenderSpec spec, String viewName) {
    return spec.views[viewName] ?? spec.views[spec.defaultView];
  }

  /// Apply filter to rows, returning only rows that match the filter
  List<Map<String, dynamic>> applyFilter(
    List<Map<String, dynamic>> rows,
    FilterExpr? filter,
  ) {
    if (filter == null) return rows;

    return rows.where((row) => evaluateFilter(row, filter)).toList();
  }

  /// Evaluate a filter expression against a row
  bool evaluateFilter(Map<String, dynamic> row, FilterExpr filter) {
    return filter.when(
      eq: (column, value) {
        final rowValue = row[column];
        // Convert Value to comparable type
        return _valueEquals(rowValue, value);
      },
      ne: (column, value) {
        final rowValue = row[column];
        return !_valueEquals(rowValue, value);
      },
      and: (filters) => filters.every((f) => evaluateFilter(row, f)),
      or: (filters) => filters.any((f) => evaluateFilter(row, f)),
      all: () => true,
    );
  }

  /// Compare a row value with a Value enum
  bool _valueEquals(dynamic rowValue, Value filterValue) {
    // Handle Value enum comparison
    if (rowValue is Value) {
      return rowValue == filterValue;
    }
    // Handle primitive types - convert rowValue to match filterValue type
    return filterValue.when(
      string: (s) {
        if (rowValue is String) return rowValue == s;
        if (rowValue is Value) return rowValue == filterValue;
        return rowValue.toString() == s;
      },
      integer: (i) {
        if (rowValue is int) return rowValue == i;
        if (rowValue is Value) return rowValue == filterValue;
        return false;
      },
      float: (f) {
        if (rowValue is double) return rowValue == f;
        if (rowValue is int) return rowValue.toDouble() == f;
        if (rowValue is Value) return rowValue == filterValue;
        return false;
      },
      boolean: (b) {
        if (rowValue is bool) return rowValue == b;
        if (rowValue is Value) return rowValue == filterValue;
        return false;
      },
      null_: () => rowValue == null,
      array: (_) => false, // Complex comparison not implemented
      object: (_) => false, // Complex comparison not implemented
      json: (s) => rowValue.toString() == s,
      reference: (r) {
        if (rowValue is String) return rowValue == r;
        if (rowValue is Value) return rowValue == filterValue;
        return false;
      },
      dateTime: (dt) {
        if (rowValue is String) return rowValue == dt;
        if (rowValue is Value) return rowValue == filterValue;
        return false;
      },
    );
  }

  /// Build widget from function call (main widget mapping logic).
  ///
  /// Each FunctionCall node has its own operations attached based on the columns
  /// it references. These operations are passed directly via [wirings] - no aggregation needed.
  ///
  /// This method first tries to dispatch via the registry (bottom-up architecture).
  /// If no builder is registered, it falls back to the legacy switch statement.
  Widget _buildFunctionCall(
    String name,
    List<Arg> args,
    List<OperationWiring> wirings,
    RenderContext context,
  ) {
    // Extract operations from this node's wirings (no aggregation from children)
    final nodeOperations = wirings.map((w) => w.descriptor).toList();

    // Extract entity name from first operation (all operations should have same entity_name)
    final entityName = nodeOperations.isNotEmpty
        ? nodeOperations.first.entityName
        : context.entityName;

    // Create context with this node's operations
    // For pie_menu with fields:this.*, merge parent operations if nodeOperations is empty
    final finalOperations = nodeOperations.isNotEmpty
        ? nodeOperations
        : (name == 'pie_menu' ? context.availableOperations : nodeOperations);

    final enrichedContext = RenderContext(
      rowData: context.rowData,
      rowTemplates: context.rowTemplates,
      onOperation: context.onOperation,
      nestedQueryConfig: context.nestedQueryConfig,
      availableOperations: finalOperations,
      entityName: entityName,
      rowIndex: context.rowIndex,
      previousRowData: context.previousRowData,
      rowCache: context.rowCache,
      changeStream: context.changeStream,
      parentIdColumn: context.parentIdColumn,
      sortKeyColumn: context.sortKeyColumn,
      colors: context.colors,
      focusDepth: context.focusDepth,
      queryParams: context.queryParams,
      isScreenLayout: context.isScreenLayout,
      drawerState: context.drawerState,
      sidebarWidth: context.sidebarWidth,
    );

    // Try registry dispatch first (bottom-up architecture)
    final entry = _registry.get(name);
    if (entry != null) {
      // Resolve args bottom-up
      final resolved = _resolveArgs(args, enrichedContext, entry.templateArgNames);

      // Dispatch based on builder type
      if (entry.isTemplate) {
        return entry.template!(resolved, enrichedContext, build);
      } else {
        return entry.standard!(resolved, enrichedContext);
      }
    }

    // No builder found in registry - unknown function
    return _buildUnknownFunction(name, args);
  }

  /// Build placeholder for unknown functions.
  Widget _buildUnknownFunction(String name, List<Arg> args) {
    return Container(
      padding: const EdgeInsets.all(8),
      color: Colors.red.withValues(alpha: 0.1),
      child: Text(
        'Unknown function: $name',
        style: const TextStyle(color: Colors.red),
      ),
    );
  }

  /// Build widget from column reference (e.g., `block_id`, `content`).
  Widget _buildColumnRef(String name, RenderContext context) {
    debugPrint(
      '[DEBUG] _buildColumnRef: name=$name, available columns: ${context.rowData.keys.toList()}',
    );
    final value = context.getColumn(name);
    debugPrint(
      '[DEBUG] _buildColumnRef: value=$value (type: ${value.runtimeType})',
    );
    return Text(value?.toString() ?? '');
  }

  /// Build widget from literal value.
  Widget _buildLiteral(Value value) {
    return value.when(
      null_: () => const Text('null'),
      boolean: (b) => Text(b.toString()),
      integer: (i) => Text(i.toString()),
      float: (f) => Text(f.toString()),
      string: (s) => Text(s),
      dateTime: (s) => Text(s),
      json: (s) => Text(s),
      reference: (r) => Text(r),
      array: (items) => Text('[${items.length} items]'),
      object: (fields) => Text('{${fields.length} fields}'),
    );
  }

  /// Build widget from binary operation (e.g., `depth * 24`, `completed and visible`).
  Widget _buildBinaryOp(
    BinaryOperator op,
    RenderExpr left,
    RenderExpr right,
    RenderContext context,
  ) {
    // Evaluate binary operation to a value, then display
    final result = _evaluateBinaryOp(op, left, right, context);
    return Text(result.toString());
  }

  /// Build widget from array literal.
  Widget _buildArray(List<RenderExpr> items, RenderContext context) {
    final children = items.map((item) => build(item, context)).toList();
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      mainAxisSize: MainAxisSize.min,
      children: children,
    );
  }

  /// Build widget from object literal.
  Widget _buildObject(Map<String, RenderExpr> fields, RenderContext context) {
    // Objects are typically not rendered directly, but used as arguments
    return Text('{${fields.keys.join(', ')}}');
  }

  // --- Expression Evaluation Helpers ---


  /// Evaluate expression to boolean value.
  bool _evaluateToBool(RenderExpr expr, RenderContext context) {
    return expr.when(
      literal: (value) => value.when(
        boolean: (b) => b,
        null_: () => false,
        integer: (i) => i != 0,
        float: (f) => f != 0.0,
        string: (s) => s.isNotEmpty,
        dateTime: (s) => s.isNotEmpty,
        json: (s) => s.isNotEmpty,
        reference: (r) => r.isNotEmpty,
        array: (items) => items.isNotEmpty,
        object: (fields) => fields.isNotEmpty,
      ),
      columnRef: (name) {
        debugPrint('[DEBUG] _evaluateToBool columnRef: name=$name');
        final value = context.getColumn(name);
        debugPrint(
          '[DEBUG] _evaluateToBool columnRef: value=$value (type: ${value.runtimeType})',
        );
        if (value is bool) return value;
        if (value == null) return false;
        // Handle integer 0/1 as boolean
        if (value is int) return value != 0;
        throw ArgumentError(
          'Column $name is not boolean (got ${value.runtimeType})',
        );
      },
      binaryOp: (op, left, right) {
        final result = _evaluateBinaryOp(op, left, right, context);
        if (result is bool) return result;
        throw ArgumentError('Binary operation did not produce boolean result');
      },
      functionCall: (_, __, ___) =>
          throw ArgumentError('Cannot evaluate function call to bool'),
      array: (_) => throw ArgumentError('Cannot evaluate array to bool'),
      object: (_) => throw ArgumentError('Cannot evaluate object to bool'),
    );
  }

  /// Evaluate binary operation to a value.
  dynamic _evaluateBinaryOp(
    BinaryOperator op,
    RenderExpr left,
    RenderExpr right,
    RenderContext context,
  ) {
    switch (op) {
      // Comparison operators
      case BinaryOperator.eq:
        return _evaluateGeneric(left, context) ==
            _evaluateGeneric(right, context);
      case BinaryOperator.neq:
        return _evaluateGeneric(left, context) !=
            _evaluateGeneric(right, context);
      case BinaryOperator.gt:
        return _compareNumeric(left, right, context, (a, b) => a > b);
      case BinaryOperator.lt:
        return _compareNumeric(left, right, context, (a, b) => a < b);
      case BinaryOperator.gte:
        return _compareNumeric(left, right, context, (a, b) => a >= b);
      case BinaryOperator.lte:
        return _compareNumeric(left, right, context, (a, b) => a <= b);

      // Arithmetic operators
      case BinaryOperator.add:
        return _evaluateToNum(left, context) + _evaluateToNum(right, context);
      case BinaryOperator.sub:
        return _evaluateToNum(left, context) - _evaluateToNum(right, context);
      case BinaryOperator.mul:
        return _evaluateToNum(left, context) * _evaluateToNum(right, context);
      case BinaryOperator.div:
        return _evaluateToNum(left, context) / _evaluateToNum(right, context);

      // Logical operators
      case BinaryOperator.and:
        return _evaluateToBool(left, context) &&
            _evaluateToBool(right, context);
      case BinaryOperator.or:
        return _evaluateToBool(left, context) ||
            _evaluateToBool(right, context);
    }
  }

  /// Evaluate expression to num (int or double).
  num _evaluateToNum(RenderExpr expr, RenderContext context) {
    return expr.when(
      literal: (value) => value.when(
        integer: (i) => i.toInt(),
        float: (f) => f,
        null_: () => 0,
        boolean: (_) => throw ArgumentError('Cannot convert bool to num'),
        string: (_) => throw ArgumentError('Cannot convert string to num'),
        dateTime: (_) => throw ArgumentError('Cannot convert dateTime to num'),
        json: (_) => throw ArgumentError('Cannot convert json to num'),
        reference: (_) =>
            throw ArgumentError('Cannot convert reference to num'),
        array: (_) => throw ArgumentError('Cannot convert array to num'),
        object: (_) => throw ArgumentError('Cannot convert object to num'),
      ),
      columnRef: (name) {
        final value = context.getColumn(name);
        if (value is num) return value;
        throw ArgumentError('Column $name is not numeric');
      },
      binaryOp: (op, left, right) {
        final result = _evaluateBinaryOp(op, left, right, context);
        if (result is num) return result;
        throw ArgumentError('Binary operation did not produce numeric result');
      },
      functionCall: (_, __, ___) =>
          throw ArgumentError('Cannot evaluate function call to num'),
      array: (_) => throw ArgumentError('Cannot evaluate array to num'),
      object: (_) => throw ArgumentError('Cannot evaluate object to num'),
    );
  }

  /// Evaluate expression to generic dynamic value.
  dynamic _evaluateGeneric(RenderExpr expr, RenderContext context) {
    return expr.when(
      literal: (value) => _valueToNative(value),
      columnRef: (name) => context.getColumn(name),
      binaryOp: (op, left, right) =>
          _evaluateBinaryOp(op, left, right, context),
      functionCall: (_, __, ___) =>
          throw ArgumentError('Cannot evaluate function call generically'),
      array: (items) =>
          items.map((item) => _evaluateGeneric(item, context)).toList(),
      object: (fields) => fields.map(
        (key, value) => MapEntry(key, _evaluateGeneric(value, context)),
      ),
    );
  }

  /// Convert Value to native Dart type.
  dynamic _valueToNative(Value value) {
    return value.when(
      null_: () => null,
      boolean: (b) => b,
      integer: (i) => i.toInt(),
      float: (f) => f,
      string: (s) => s,
      dateTime: (s) => s,
      json: (s) => s,
      reference: (r) => r,
      array: (items) => items.map(_valueToNative).toList(),
      object: (fields) =>
          fields.map((key, value) => MapEntry(key, _valueToNative(value))),
    );
  }

  /// Compare two numeric expressions.
  bool _compareNumeric(
    RenderExpr left,
    RenderExpr right,
    RenderContext context,
    bool Function(num, num) compare,
  ) {
    final leftVal = _evaluateToNum(left, context);
    final rightVal = _evaluateToNum(right, context);
    return compare(leftVal, rightVal);
  }

  /// Resolve args bottom-up: evaluate values, build children, keep templates.
  ///
  /// This is the key to the bottom-up architecture:
  /// - Named args are pre-evaluated to dynamic values
  /// - Positional function calls are pre-built into widgets (children)
  /// - Positional values are pre-evaluated (positionalValues)
  /// - Template args (specified by the builder) are kept as RenderExpr
  ResolvedArgs _resolveArgs(
    List<Arg> args,
    RenderContext context,
    Set<String> templateArgNames,
  ) {
    final named = <String, dynamic>{};
    final children = <Widget>[];
    final positionalValues = <dynamic>[];
    final templates = <String, RenderExpr>{};

    for (final arg in args) {
      if (arg.name != null) {
        // Named arg
        if (templateArgNames.contains(arg.name)) {
          // Keep as template (RenderExpr)
          templates[arg.name!] = arg.value;
        } else {
          // Evaluate to value
          named[arg.name!] = _evaluateGeneric(arg.value, context);
          // Track field name for column refs (needed for interactive builders)
          if (arg.value is RenderExpr_ColumnRef) {
            named['_${arg.name}_field'] = (arg.value as RenderExpr_ColumnRef).name;
          }
        }
      } else {
        // Positional arg - check if it's a function call (widget) or value
        if (arg.value is RenderExpr_FunctionCall) {
          // Function call → build into widget (bottom-up)
          children.add(build(arg.value, context));
        } else {
          // Value (literal, column ref, etc.) → evaluate to dynamic
          positionalValues.add(_evaluateGeneric(arg.value, context));
        }
      }
    }

    return ResolvedArgs(
      named: named,
      children: children,
      positionalValues: positionalValues,
      templates: templates,
    );
  }
}
