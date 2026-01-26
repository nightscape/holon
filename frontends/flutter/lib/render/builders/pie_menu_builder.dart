import 'package:flutter/material.dart';
import '../../src/rust/third_party/holon_api/render_types.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds pie_menu() widget - wraps child with pie menu for operations.
///
/// Usage: `pie_menu(child)` or `pie_menu(child, fields: "content,name")` or `pie_menu(child, fields: this)`
class PieMenuWidgetBuilder {
  const PieMenuWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    if (args.children.isEmpty) {
      throw ArgumentError('pieMenu() requires a child widget argument');
    }

    final child = args.children[0];

    List<OperationDescriptor> operations;

    // Check if fields came from a column ref (semantic marker like 'this', 'this.*', '*')
    final fieldRefName = args.getFieldName('fields');
    if (fieldRefName != null) {
      // Column ref: check for semantic markers
      if (fieldRefName == 'this' ||
          fieldRefName == 'this.*' ||
          fieldRefName == '*') {
        operations = context.availableOperations;
      } else {
        // Single field name
        operations = context.operationsAffecting([fieldRefName]);
      }
    } else if (args.has('fields')) {
      // String value: parse as comma-separated list
      final fieldsString = args.getString('fields', '');
      if (fieldsString.isEmpty) {
        operations = context.availableOperations;
      } else {
        final fields = fieldsString.split(',').map((e) => e.trim()).toList();
        if (fields.contains('this.*') || fields.contains('this')) {
          operations = context.availableOperations;
        } else {
          operations = context.operationsAffecting(fields);
        }
      }
    } else {
      // Default: use all available operations
      operations = context.availableOperations;
    }

    debugPrint(
      '[DEBUG] PieMenuBuilder: found ${operations.length} operations for fields',
    );

    return OperationHelpers.attachTapPieMenu(child, operations, context);
  }
}
