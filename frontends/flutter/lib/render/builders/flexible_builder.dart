import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds flexible() widget - wraps child in Flexible for Row/Column flex layout.
///
/// Usage: `flexible(text("expand me"))` or `flexible(text("x") flex: 2)`
class FlexibleWidgetBuilder {
  const FlexibleWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    if (args.children.isEmpty) {
      throw ArgumentError('flexible() requires a child argument');
    }

    final child = args.children[0];
    final flex = args.getInt('flex', 1);

    return Flexible(flex: flex, child: child);
  }
}
