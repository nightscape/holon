import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds grid() widget - grid layout with configurable columns.
///
/// Usage: `grid(columns: 3, gap: 16, child1, child2, child3, ...)`
class GridWidgetBuilder {
  const GridWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final columns = args.getInt('columns', 2);
    final gap = args.getDouble('gap', 16.0);
    final children = args.children;

    final rows = <Widget>[];
    for (var i = 0; i < children.length; i += columns) {
      final rowChildren = <Widget>[];
      for (var j = 0; j < columns && i + j < children.length; j++) {
        if (j > 0) {
          rowChildren.add(SizedBox(width: gap));
        }
        rowChildren.add(Expanded(child: children[i + j]));
      }
      // Fill remaining columns with empty widgets
      for (var j = children.length - i; j < columns; j++) {
        if (rowChildren.isNotEmpty) {
          rowChildren.add(SizedBox(width: gap));
        }
        rowChildren.add(const Expanded(child: SizedBox()));
      }
      if (rows.isNotEmpty) {
        rows.add(SizedBox(height: gap));
      }
      rows.add(
        Row(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: rowChildren,
        ),
      );
    }

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      mainAxisSize: MainAxisSize.min,
      children: rows,
    );
  }
}
