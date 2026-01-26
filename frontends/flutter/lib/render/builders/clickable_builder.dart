import 'package:flutter/material.dart';
import '../../src/rust/third_party/holon_api.dart';
import '../../src/rust/third_party/holon_api/render_types.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds clickable() widget - triggers an operation on tap.
///
/// Usage: `clickable((row ...) action:(navigation.focus region:"main" block_id:this.id))`
///
/// The action parameter is a function call expression where:
/// - The function name is "entity.operation" (e.g., "navigation.focus")
/// - Named arguments become operation parameters
///
/// NOTE: This builder requires `action` to be a template arg (not pre-evaluated).
class ClickableWidgetBuilder {
  const ClickableWidgetBuilder._();

  /// Template arg names that should be kept as RenderExpr
  static const templateArgNames = {'action'};

  static Widget build(
    ResolvedArgs args,
    RenderContext context,
    Widget Function(RenderExpr template, RenderContext rowContext) buildTemplate,
  ) {
    if (args.children.isEmpty) return const SizedBox.shrink();

    final child = args.children[0];
    final actionExpr = args.templates['action'];
    if (actionExpr == null) return child;

    // action must be a function call: (entity.operation param1:val1 ...)
    if (actionExpr is! RenderExpr_FunctionCall) return child;

    final funcName = actionExpr.name;
    final parts = funcName.split('.');
    if (parts.length != 2) return child;

    final entityName = parts[0];
    final opName = parts[1];

    // Collect operation params from action's args
    final params = <String, dynamic>{};
    for (final arg in actionExpr.args) {
      if (arg.name != null) {
        params[arg.name!] = _evaluateGeneric(arg.value, context);
      }
    }

    return GestureDetector(
      onTap: () => context.onOperation?.call(entityName, opName, params),
      child: child,
    );
  }

  /// Evaluate a RenderExpr to a dynamic value.
  static dynamic _evaluateGeneric(RenderExpr expr, RenderContext context) {
    return expr.when(
      functionCall: (name, args, wirings) => null,
      columnRef: (name) => context.rowData[name],
      literal: (value) => value.when(
        integer: (v) => v,
        float: (v) => v,
        string: (v) => v,
        boolean: (v) => v,
        null_: () => null,
        dateTime: (v) => v,
        json: (v) => v,
        reference: (v) => v,
        array: (items) => items,
        object: (fields) => fields,
      ),
      binaryOp: (op, left, right) => null,
      array: (items) => items,
      object: (fields) => fields,
    );
  }
}
