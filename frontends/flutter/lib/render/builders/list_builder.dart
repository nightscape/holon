import 'package:flutter/material.dart';
import '../../src/rust/third_party/holon_api/render_types.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds list() widget - ListView from item template.
///
/// Usage: `list(item_template:(row ...))`
class ListWidgetBuilder {
  const ListWidgetBuilder._();

  /// Template arg names that should be kept as RenderExpr
  static const templateArgNames = {'item_template', 'item'};

  static Widget build(
    ResolvedArgs args,
    RenderContext context,
    Widget Function(RenderExpr template, RenderContext rowContext) buildTemplate,
  ) {
    final itemExpr = args.templates['item_template'] ?? args.templates['item'];
    if (itemExpr == null) {
      throw ArgumentError('list() requires "item_template" or "item" argument');
    }

    return ListView.builder(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      itemCount: 1,
      itemBuilder: (buildContext, index) => buildTemplate(itemExpr, context),
    );
  }
}
