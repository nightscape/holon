import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds spacer() widget - creates horizontal or vertical space.
///
/// Usage: `spacer()` or `spacer(width:8)` or `spacer(height:8)` or `spacer(width:8, height:4)`
class SpacerWidgetBuilder {
  const SpacerWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final width = args.getInt('width', 8);
    final height = args.getInt('height', 0);

    return SizedBox(
      width: width.toDouble(),
      height: height > 0 ? height.toDouble() : null,
    );
  }
}
