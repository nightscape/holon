import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds stack() widget - overlapping widgets.
///
/// Usage: `stack(background_child, overlay_child)`
class StackWidgetBuilder {
  const StackWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    return Stack(children: args.children);
  }
}
