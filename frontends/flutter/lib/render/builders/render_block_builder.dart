import 'package:flutter/material.dart';
import '../../src/rust/third_party/holon_api.dart';
import '../../src/rust/third_party/holon_api/render_types.dart';
import '../live_query_widget.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds render_block() primitive - dispatches based on block type.
///
/// Dispatch logic:
/// - content_type: "source" AND source_language: "prql" → LiveQueryWidget
/// - content_type: "source" AND source_language: other → source_editor
/// - content_type: "text" (or default) → editable_text
///
/// Usage: `render_block(this)` or `render_block(content:content)`
class RenderBlockWidgetBuilder {
  const RenderBlockWidgetBuilder._();

  /// Template arg names - we need buildTemplate for nested builder dispatch
  static const templateArgNames = {'content'};

  static Widget build(
    ResolvedArgs args,
    RenderContext context,
    Widget Function(RenderExpr template, RenderContext rowContext) buildTemplate,
  ) {
    final contentType = context.rowData['content_type']?.toString();
    final sourceLanguage = context.rowData['source_language']?.toString()?.toLowerCase();
    final parentId = context.rowData['parent_id']?.toString();

    // Case 1: PRQL query block - execute and render live
    if (contentType == 'source' && sourceLanguage == 'prql') {
      final prqlSource = context.rowData['content']?.toString();
      assert(
        prqlSource != null && prqlSource.isNotEmpty,
        'Query block ${context.rowData['id']} has content_type=source, '
        'source_language=prql but empty content',
      );

      // Use parent_id as context so `from children` resolves to siblings
      return LiveQueryWidget(
        prql: prqlSource!,
        contextBlockId: parentId,
        onOperation: context.onOperation,
      );
    }

    // Case 2: Other source block - syntax highlighted editor
    if (contentType == 'source' && sourceLanguage != null) {
      // Build source_editor via template dispatch
      final sourceEditorExpr = RenderExpr.functionCall(
        name: 'source_editor',
        args: [
          Arg(
            name: 'language',
            value: RenderExpr.literal(value: Value.string(sourceLanguage)),
          ),
          Arg(
            name: 'content',
            value: RenderExpr.literal(
              value: Value.string(context.rowData['content']?.toString() ?? ''),
            ),
          ),
        ],
        operations: const [],
      );
      return buildTemplate(sourceEditorExpr, context);
    }

    // Case 3: Default - editable text
    final contentExpr = args.templates['content'];
    if (contentExpr != null) {
      final editableTextExpr = RenderExpr.functionCall(
        name: 'editable_text',
        args: [
          Arg(name: 'content', value: contentExpr),
        ],
        operations: const [],
      );
      return buildTemplate(editableTextExpr, context);
    }

    // Fallback: render content column as text
    final content = context.rowData['content']?.toString() ?? '';
    return Text(content);
  }
}
