import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../../providers/ui_state_providers.dart';
import '../render_context.dart';
import '../renderable_item_ext.dart';
import 'widget_builder.dart';

/// Builds draggable() widget - wraps child to make it draggable.
///
/// Usage: `draggable(child)` or `draggable(child on:'longpress')`
class DraggableWidgetBuilder {
  const DraggableWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    if (args.children.isEmpty) {
      return const SizedBox.shrink();
    }

    final child = args.children[0];
    final trigger = args.getString('on', 'longpress');

    // Create RenderableItem with row data and operations
    final uiIndex = context.rowData['ui'] as int?;
    final template = uiIndex != null
        ? context.rowTemplates.firstWhere(
            (t) => t.index.toInt() == uiIndex,
            orElse: () => context.rowTemplates.first,
          )
        : context.rowTemplates.first;

    final item = RenderableItem(
      rowData: context.rowData,
      template: template,
      operations: context.availableOperations,
    );

    final contentText =
        context.rowData['content']?.toString() ??
        context.rowData['name']?.toString() ??
        'Item';
    final feedback = Material(
      elevation: 4,
      borderRadius: BorderRadius.circular(8),
      color: Colors.white,
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
        child: Text(
          contentText,
          style: const TextStyle(fontSize: 14, color: Colors.black87),
        ),
      ),
    );

    return Consumer(
      builder: (buildContext, ref, _) {
        void onDragStarted() {
          debugPrint('[Draggable] Drag started: $contentText');

          final RenderBox? box = buildContext.findRenderObject() as RenderBox?;
          if (box == null || context.rowCache == null) return;

          final position = box.localToGlobal(Offset.zero);
          final overlayPosition = Offset(
            position.dx + box.size.width + 16,
            position.dy,
          );

          ref
              .read(searchSelectOverlayProvider.notifier)
              .showForDrag(
                position: overlayPosition,
                draggedItem: item,
                rowCache: context.rowCache!,
                rowTemplates: context.rowTemplates,
                onOperation: context.onOperation,
              );
        }

        void onDragEnd(DraggableDetails details) {
          final currentMode = ref.read(searchSelectOverlayProvider).mode;
          if (currentMode == SearchSelectMode.dragActive) {
            ref.read(searchSelectOverlayProvider.notifier).hide();
          }
        }

        void onDraggableCanceled(Velocity velocity, Offset offset) {
          final currentMode = ref.read(searchSelectOverlayProvider).mode;
          if (currentMode == SearchSelectMode.dragActive) {
            ref.read(searchSelectOverlayProvider.notifier).hide();
          }
        }

        if (trigger == 'drag') {
          return Draggable<RenderableItem>(
            data: item,
            feedback: feedback,
            childWhenDragging: Opacity(opacity: 0.3, child: child),
            dragAnchorStrategy: pointerDragAnchorStrategy,
            onDragStarted: onDragStarted,
            onDragEnd: onDragEnd,
            onDraggableCanceled: onDraggableCanceled,
            child: child,
          );
        } else {
          return LongPressDraggable<RenderableItem>(
            data: item,
            feedback: feedback,
            childWhenDragging: Opacity(opacity: 0.3, child: child),
            dragAnchorStrategy: pointerDragAnchorStrategy,
            hapticFeedbackOnStart: true,
            onDragStarted: onDragStarted,
            onDragEnd: onDragEnd,
            onDraggableCanceled: onDraggableCanceled,
            child: child,
          );
        }
      },
    );
  }
}
