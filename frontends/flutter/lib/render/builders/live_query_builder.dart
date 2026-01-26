import 'package:flutter/material.dart';
import '../live_query_widget.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds live_query() primitive - executes a nested PRQL query with context.
///
/// This enables container blocks to delegate rendering of their children
/// to independent LiveQueryWidgets, each with their own CDC subscription.
///
/// Usage: `live_query prql:"from children render (list ...)" context:this.id`
class LiveQueryWidgetBuilder {
  const LiveQueryWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final prql = args.getString('prql');
    final contextBlockId = args.getString('context');
    assert(prql.isNotEmpty, 'live_query requires non-empty prql argument');
    // Expanded so ListView/TreeView children get bounded height inside a Column
    return Expanded(
      child: LiveQueryWidget(
        prql: prql,
        contextBlockId: contextBlockId.isNotEmpty ? contextBlockId : null,
        onOperation: context.onOperation,
      ),
    );
  }
}
