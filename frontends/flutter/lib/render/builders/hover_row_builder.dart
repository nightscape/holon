import 'package:flutter/material.dart';
import '../../styles/animation_constants.dart';
import '../../styles/app_styles.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds hover_row() widget - row with hover effects.
///
/// Usage: `hover_row(child)`
class HoverRowWidgetBuilder {
  const HoverRowWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    if (args.children.isEmpty) {
      return const SizedBox.shrink();
    }

    return _HoverRowWidget(colors: context.colors, child: args.children[0]);
  }
}

class _HoverRowWidget extends StatefulWidget {
  final Widget child;
  final AppColors colors;

  const _HoverRowWidget({required this.child, required this.colors});

  @override
  State<_HoverRowWidget> createState() => _HoverRowWidgetState();
}

class _HoverRowWidgetState extends State<_HoverRowWidget> {
  bool _isHovered = false;

  @override
  Widget build(BuildContext context) {
    return MouseRegion(
      onEnter: (_) => setState(() => _isHovered = true),
      onExit: (_) => setState(() => _isHovered = false),
      child: AnimatedContainer(
        duration: AnimDurations.hoverEffect,
        curve: AnimCurves.hover,
        padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
        decoration: BoxDecoration(
          color: _isHovered
              ? widget.colors.primary.withValues(alpha: 0.03)
              : Colors.transparent,
          borderRadius: BorderRadius.circular(6),
        ),
        child: widget.child,
      ),
    );
  }
}
