import 'package:flutter/material.dart';
import '../render_context.dart';
import '../source_block_widget.dart';
import 'widget_builder.dart';

/// Builds source_editor() widget - code editor with syntax highlighting.
///
/// Usage: `source_editor(language: 'prql', content: source_code)`
class SourceEditorWidgetBuilder {
  const SourceEditorWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    final language = args.getString('language', 'text');
    final content = args.getString('content', '');

    return SourceEditorWidget(
      language: language,
      initialContent: content,
      onChanged: context.onOperation != null
          ? (newContent) {
              final id = context.rowData['id'];
              if (id != null && context.entityName != null) {
                context.onOperation!(context.entityName!, 'set_field', {
                  'id': id.toString(),
                  'field': 'source',
                  'value': newContent,
                });
              }
            }
          : null,
      colors: context.colors,
    );
  }
}
