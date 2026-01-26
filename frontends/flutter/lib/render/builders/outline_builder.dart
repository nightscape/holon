import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:outliner_view/outliner_view.dart';
import '../../data/row_data_block_ops.dart';
import '../../src/rust/third_party/holon_api.dart';
import '../../src/rust/third_party/holon_api/render_types.dart';
import '../render_context.dart';
import 'widget_builder.dart';

/// Builds outline() widget - OutlinerListView for hierarchical block editing.
///
/// Usage: `outline(parent_id:parent_id sortkey:order item_template:(row ...))`
class OutlineWidgetBuilder {
  const OutlineWidgetBuilder._();

  /// Template arg names that should be kept as RenderExpr
  static const templateArgNames = {'item_template', 'item', 'parent_id', 'sortkey', 'sort_key'};

  static Widget build(
    ResolvedArgs args,
    RenderContext context,
    Widget Function(RenderExpr template, RenderContext rowContext) buildTemplate,
  ) {
    // Extract column names from template args
    final parentIdExpr = args.templates['parent_id'];
    final sortKeyExpr = args.templates['sortkey'] ?? args.templates['sort_key'];
    final itemTemplateExpr = args.templates['item_template'] ?? args.templates['item'];

    if (parentIdExpr == null) {
      throw ArgumentError('outline() requires "parent_id" argument');
    }
    if (sortKeyExpr == null) {
      throw ArgumentError('outline() requires "sortkey" or "sort_key" argument');
    }
    if (itemTemplateExpr == null) {
      throw ArgumentError('outline() requires "item_template" or "item" argument');
    }

    // Get column names from expressions
    final parentIdColumn = _evaluateToString(parentIdExpr);
    final sortKeyColumn = _evaluateToString(sortKeyExpr);

    if (context.rowCache == null) {
      throw ArgumentError(
        'outline() requires rowCache in RenderContext. '
        'This should be provided by ReactiveQueryWidget.',
      );
    }

    if (context.entityName == null) {
      throw ArgumentError('outline() requires entityName in RenderContext.');
    }

    // Create RowDataBlockOps instance
    final blockOps = RowDataBlockOps(
      rowCache: context.rowCache!,
      parentIdColumn: parentIdColumn,
      sortKeyColumn: sortKeyColumn,
      entityName: context.entityName!,
      onOperation: context.onOperation,
    );

    // Create Riverpod provider for BlockOps
    final opsProvider = Provider<BlockOps<Map<String, dynamic>>>((ref) {
      return blockOps;
    });

    final entityName = context.entityName!;
    final onOperation = context.onOperation;

    return OutlinerListView<Map<String, dynamic>>(
      opsProvider: opsProvider,
      config: const OutlinerConfig(
        keyboardShortcutsEnabled: true,
        padding: EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      ),
      blockBuilder: (buildContext, block) {
        final blockContext = RenderContext(
          rowData: block,
          rowTemplates: context.rowTemplates,
          onOperation: onOperation,
          availableOperations: context.availableOperations,
          entityName: entityName,
          colors: context.colors,
        );
        return buildTemplate(itemTemplateExpr, blockContext);
      },
    );
  }

  /// Evaluate a RenderExpr to a string value.
  static String _evaluateToString(RenderExpr expr) {
    if (expr is RenderExpr_ColumnRef) {
      return expr.name;
    } else if (expr is RenderExpr_Literal) {
      return expr.value.when(
        integer: (v) => v.toString(),
        float: (v) => v.toString(),
        string: (v) => v,
        boolean: (v) => v.toString(),
        null_: () => '',
        dateTime: (v) => v,
        json: (v) => v,
        reference: (v) => v,
        array: (items) => items.toString(),
        object: (fields) => fields.toString(),
      );
    }
    return '';
  }
}
