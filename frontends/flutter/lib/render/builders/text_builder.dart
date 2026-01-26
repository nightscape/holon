import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds text() widget - styled text display.
///
/// Usage: `text("hello")` or `text(content: "hello")`
class TextWidgetBuilder {
  const TextWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    String text;
    if (args.positionalValues.isNotEmpty) {
      text = args.getPositionalString(0);
    } else {
      text = args.getString('content', '');
    }

    return Text(
      text,
      style: TextStyle(
        fontSize: 16,
        height: 1.5,
        color: context.colors.textSecondary,
        letterSpacing: 0,
      ),
    );
  }
}
