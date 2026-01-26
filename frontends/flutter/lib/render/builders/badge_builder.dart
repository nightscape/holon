import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds badge() widget - a subtle badge/chip display.
///
/// Usage: `badge(content: "label")` or `badge(content: "urgent", color: "red")`
class BadgeWidgetBuilder {
  const BadgeWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final content = args.getString('content', '');
    final colorStr = args.getString('color', '').toLowerCase();

    // Map common color names to Flutter Colors (LogSeq-style subtle colors)
    final Color? badgeColor = switch (colorStr) {
      'cyan' => const Color(0xFF06B6D4),
      'blue' => const Color(0xFF3B82F6),
      'green' => const Color(0xFF10B981),
      'red' => const Color(0xFFEF4444),
      'orange' => const Color(0xFFF59E0B),
      'purple' => const Color(0xFF8B5CF6),
      'yellow' => const Color(0xFFEAB308),
      'grey' || 'gray' => context.colors.textSecondary,
      _ => null,
    };

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
      decoration: BoxDecoration(
        color: (badgeColor ?? context.colors.textSecondary).withValues(
          alpha: 0.1,
        ),
        borderRadius: BorderRadius.circular(4),
      ),
      child: Text(
        content,
        style: TextStyle(
          fontSize: 11,
          color: badgeColor ?? context.colors.textSecondary,
          fontWeight: FontWeight.w500,
          letterSpacing: 0.2,
        ),
      ),
    );
  }
}
