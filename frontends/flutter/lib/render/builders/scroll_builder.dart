import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds scroll() widget - scrollable container.
///
/// Usage: `scroll(child)` or `scroll(direction: 'horizontal', child)`
class ScrollWidgetBuilder {
  const ScrollWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final directionStr = args.getString('direction', 'vertical');
    final axis = directionStr == 'horizontal' ? Axis.horizontal : Axis.vertical;

    Widget? child;
    if (args.children.isNotEmpty) {
      child = args.children[0];
    }

    return SingleChildScrollView(scrollDirection: axis, child: child);
  }
}
