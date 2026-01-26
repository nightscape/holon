import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds animated() widget - animation wrapper for property transitions.
///
/// Usage: `animated(duration: 300, child)`
class AnimatedWidgetBuilder {
  const AnimatedWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    if (args.children.isEmpty) {
      return const SizedBox.shrink();
    }

    final durationMs = args.getInt('duration', 200);
    final child = args.children[0];

    return AnimatedSwitcher(
      duration: Duration(milliseconds: durationMs),
      child: child,
    );
  }
}
