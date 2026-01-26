import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds block() widget - vertical layout with indentation.
///
/// Usage: `block(row(...), row(...))` or `block(row(...) depth: 2)`
class BlockWidgetBuilder {
  const BlockWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    // Children are already pre-built bottom-up
    final children = args.children;

    // Get depth for indentation (optional)
    final depth = args.getInt('depth', 0);

    // LogSeq uses 29px indent per level
    final indentPixels = depth * 29.0;

    return Container(
      margin: EdgeInsets.only(left: indentPixels),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        mainAxisSize: MainAxisSize.min,
        children: children,
      ),
    );
  }
}
