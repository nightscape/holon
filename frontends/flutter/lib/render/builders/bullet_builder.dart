import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds bullet() widget - LogSeq-style structural bullet point.
///
/// Usage: `bullet()` or `bullet(sizeInPx: 6)`
class BulletWidgetBuilder {
  const BulletWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final size = args.getInt('sizeInPx', 6);

    // Container size is always 20x20 to maintain consistent spacing
    return Container(
      width: 20,
      height: 20,
      margin: const EdgeInsets.only(right: 8, top: 2),
      child: Center(
        child: Container(
          width: size.toDouble(),
          height: size.toDouble(),
          decoration: BoxDecoration(
            shape: BoxShape.circle,
            color: context.colors.textTertiary.withValues(alpha: 0.8),
          ),
        ),
      ),
    );
  }
}
