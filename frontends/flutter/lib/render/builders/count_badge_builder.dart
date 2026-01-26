import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds count_badge() widget - displays a count value.
///
/// Usage: `count_badge(count: 5)` or `count_badge(count: inbox_count)`
class CountBadgeWidgetBuilder {
  const CountBadgeWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final count = args.getInt('count', 0);

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 2),
      decoration: BoxDecoration(
        color: context.colors.primary.withValues(alpha: 0.15),
        borderRadius: BorderRadius.circular(10),
      ),
      child: Text(
        count.toString(),
        style: TextStyle(
          fontSize: 12,
          fontWeight: FontWeight.w600,
          color: context.colors.primary,
        ),
      ),
    );
  }
}
