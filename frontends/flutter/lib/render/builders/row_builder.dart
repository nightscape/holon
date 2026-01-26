import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds row() widget - horizontal layout with LogSeq-style hover state.
///
/// Usage: `row(text("a"), checkbox(...), editable_text(...))`
class RowWidgetBuilder {
  const RowWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    // Children are already pre-built bottom-up
    final wrappedChildren = args.children.map((child) {
      // TextField widgets need bounded width constraints in Row
      if (child is TextField) {
        return Flexible(child: child);
      }
      return child;
    }).toList();

    return MouseRegion(
      cursor: SystemMouseCursors.text,
      child: Container(
        padding: const EdgeInsets.symmetric(vertical: 2, horizontal: 4),
        decoration: BoxDecoration(
          borderRadius: BorderRadius.circular(4),
          color: Colors.transparent,
        ),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.center,
          children: wrappedChildren,
        ),
      ),
    );
  }
}
