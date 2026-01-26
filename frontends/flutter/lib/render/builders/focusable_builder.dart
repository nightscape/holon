import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../../providers/ui_state_providers.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds focusable() widget - block that can become focus target.
///
/// Usage: `focusable(block_id: id, child)`
class FocusableWidgetBuilder {
  const FocusableWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    if (args.children.isEmpty) {
      return const SizedBox.shrink();
    }

    final child = args.children[0];
    final blockId = args.getString('block_id', '') != ''
        ? args.getString('block_id')
        : context.rowData['id']?.toString();

    return Consumer(
      builder: (buildContext, ref, _) {
        return GestureDetector(
          onTap: () {
            if (blockId != null) {
              ref
                  .read(focusedBlockIdProvider.notifier)
                  .setFocusedBlock(blockId);
            }
          },
          child: child,
        );
      },
    );
  }
}
