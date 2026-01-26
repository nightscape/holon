import 'package:flutter/material.dart';
import '../../src/rust/third_party/holon_api/block.dart' as block_types;
import '../render_context.dart';
import '../source_block_widget.dart';
import 'widget_builder.dart';

/// Builds source_block() widget - syntax highlighted code block.
///
/// Usage: `source_block(language: 'sql', source: code_text)`
/// or `source_block(content: block_content)` for BlockContent.Source
class SourceBlockWidgetBuilder {
  const SourceBlockWidgetBuilder._();

  static Widget build(ResolvedArgs args, RenderContext context) {
    // Option 1: Direct language and source arguments
    final language = args.getString('language', '');
    final source = args.getString('source', '');

    // Option 2: BlockContent from content column
    final hasDirectArgs = language.isNotEmpty && source.isNotEmpty;

    String resolvedLanguage;
    String resolvedSource;
    block_types.BlockResult? results;

    if (hasDirectArgs) {
      resolvedLanguage = language;
      resolvedSource = source;
    } else {
      // Try to get BlockContent from row data
      final contentValue = context.rowData['content'];
      if (contentValue is block_types.BlockContent) {
        switch (contentValue) {
          case block_types.BlockContent_Source(:final field0):
            final sourceBlock = field0 as dynamic;
            resolvedLanguage = sourceBlock.language as String;
            resolvedSource = sourceBlock.source as String;
            results = sourceBlock.results as block_types.BlockResult?;
          case block_types.BlockContent_Text(:final raw):
            return Text(raw);
        }
      } else {
        resolvedLanguage = context.rowData['language']?.toString() ?? 'text';
        resolvedSource =
            context.rowData['source']?.toString() ??
            context.rowData['content']?.toString() ??
            '';
      }
    }

    final name = args.getString('name', '');
    final editable = args.getBool('editable', false);

    return SourceBlockWidget(
      language: resolvedLanguage,
      source: resolvedSource,
      name: name.isNotEmpty ? name : null,
      results: results,
      editable: editable,
      onSourceChanged: editable && context.onOperation != null
          ? (newSource) {
              final id = context.rowData['id'];
              if (id != null && context.entityName != null) {
                context.onOperation!(context.entityName!, 'set_field', {
                  'id': id.toString(),
                  'field': 'source',
                  'value': newSource,
                });
              }
            }
          : null,
      onExecute: context.onOperation != null
          ? () async {
              final id = context.rowData['id'];
              if (id != null && context.entityName != null) {
                context.onOperation!(
                  context.entityName!,
                  'execute_source_block',
                  {'id': id.toString()},
                );
              }
            }
          : null,
      colors: context.colors,
    );
  }
}
