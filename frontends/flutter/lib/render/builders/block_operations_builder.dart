import 'package:flutter/material.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds block_operations() widget - menu button with pie menu for block operations.
///
/// Usage: `block_operations()`
class BlockOperationsWidgetBuilder {
  const BlockOperationsWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final menuButton = IconButton(
      icon: const Icon(Icons.more_horiz),
      iconSize: 20,
      padding: EdgeInsets.zero,
      constraints: const BoxConstraints(),
      onPressed: () {
        // Direct click can show a dropdown menu if needed
        // Pie menu provides radial menu access
      },
    );

    return OperationHelpers.autoAttachPieMenu(menuButton, [
      'parent_id',
      'sort_key',
      'depth',
      'content',
    ], context);
  }
}
