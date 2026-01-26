import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds section() widget - card container with title header.
///
/// Usage: `section(title: "Today's Focus", child)` or `section(title: "Inbox", collapsible: true, child)`
class SectionWidgetBuilder {
  const SectionWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final title = args.getString('title', '');
    final collapsible = args.getBool('collapsible', false);

    Widget? child;
    if (args.children.isNotEmpty) {
      child = args.children[0];
    }

    return Container(
      margin: const EdgeInsets.only(bottom: 16),
      decoration: BoxDecoration(
        color: context.colors.backgroundSecondary,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: context.colors.border, width: 1),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        mainAxisSize: MainAxisSize.min,
        children: [
          Padding(
            padding: const EdgeInsets.fromLTRB(16, 12, 16, 8),
            child: Row(
              children: [
                if (collapsible)
                  Icon(
                    Icons.chevron_right,
                    size: 16,
                    color: context.colors.textTertiary,
                  ),
                Text(
                  title.toUpperCase(),
                  style: TextStyle(
                    fontSize: 11,
                    fontWeight: FontWeight.w600,
                    letterSpacing: 0.5,
                    color: context.colors.textTertiary,
                  ),
                ),
              ],
            ),
          ),
          Container(height: 1, color: context.colors.border),
          if (child != null)
            Padding(padding: const EdgeInsets.all(12), child: child),
        ],
      ),
    );
  }
}
