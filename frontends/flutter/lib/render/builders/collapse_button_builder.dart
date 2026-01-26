import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds collapse_button() widget - LogSeq-style bullet that toggles collapse.
///
/// Usage: `collapse_button(is_collapsed: this.collapsed)`
class CollapseButtonWidgetBuilder {
  const CollapseButtonWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final isCollapsed = args.getBool('is_collapsed', false);

    final button = GestureDetector(
      onTap: () {
        // Direct click still works for immediate toggle
        // Pie menu provides additional operations
      },
      child: Container(
        width: 20,
        height: 20,
        margin: const EdgeInsets.only(right: 8, top: 2),
        child: isCollapsed
            ? Icon(
                Icons.chevron_right,
                size: 16,
                color: context.colors.textTertiary,
              )
            : Container(
                width: 6,
                height: 6,
                margin: const EdgeInsets.all(7),
                decoration: BoxDecoration(
                  shape: BoxShape.circle,
                  color: context.colors.textTertiary.withValues(alpha: 0.8),
                ),
              ),
      ),
    );

    return OperationHelpers.autoAttachPieMenu(
      button,
      ['collapsed', 'is_collapsed'],
      context,
    );
  }
}
