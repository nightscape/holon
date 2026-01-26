import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds progress() widget - progress indicator as dots or bar.
///
/// Usage: `progress(value: 3, max: 4)` or `progress(value: 0.75, style: 'bar')`
class ProgressWidgetBuilder {
  const ProgressWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final value = args.getNum('value', 0);
    final style = args.getString('style', 'dots');

    if (style == 'bar') {
      final max = args.getNum('max', 1.0);
      final fraction = (value / max).clamp(0.0, 1.0);

      return SizedBox(
        width: 60,
        height: 4,
        child: ClipRRect(
          borderRadius: BorderRadius.circular(2),
          child: LinearProgressIndicator(
            value: fraction.toDouble(),
            backgroundColor: context.colors.border,
            valueColor: AlwaysStoppedAnimation<Color>(context.colors.primary),
          ),
        ),
      );
    }

    // Dots style
    final max = args.getInt('max', 4);
    final filled = value.toInt().clamp(0, max);

    return Row(
      mainAxisSize: MainAxisSize.min,
      children: List.generate(max, (i) {
        final isFilled = i < filled;
        return Padding(
          padding: EdgeInsets.only(left: i > 0 ? 2 : 0),
          child: Container(
            width: 8,
            height: 8,
            decoration: BoxDecoration(
              shape: BoxShape.circle,
              color: isFilled ? context.colors.primary : context.colors.border,
            ),
          ),
        );
      }),
    );
  }
}
