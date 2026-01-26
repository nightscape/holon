import 'package:flutter/material.dart';
import '../../src/rust/third_party/holon_api/render_types.dart';
import '../render_context.dart';
import '../tree_view_widget.dart';
import 'widget_builder.dart';

/// Builds tree() widget - AnimatedTreeView for hierarchical display.
///
/// Usage: `tree(parent_id:parent_id sortkey:order item_template:(row ...))`
class TreeWidgetBuilder {
  const TreeWidgetBuilder._();

  /// Template arg names that should be kept as RenderExpr
  static const templateArgNames = {'item_template', 'item', 'parent_id', 'sortkey', 'sort_key'};

  static Widget build(
    ResolvedArgs args,
    RenderContext context,
    Widget Function(RenderExpr template, RenderContext rowContext) buildTemplate,
  ) {
    // Extract column names from template args (they're RenderExpr_ColumnRef)
    final parentIdExpr = args.templates['parent_id'];
    final sortKeyExpr = args.templates['sortkey'] ?? args.templates['sort_key'];

    if (parentIdExpr == null) {
      throw ArgumentError('tree() requires "parent_id" argument');
    }
    if (sortKeyExpr == null) {
      throw ArgumentError('tree() requires "sortkey" or "sort_key" argument');
    }

    final parentIdColumn = (parentIdExpr as RenderExpr_ColumnRef).name;
    final sortKeyColumn = (sortKeyExpr as RenderExpr_ColumnRef).name;

    if (context.rowCache == null) {
      throw ArgumentError(
        'tree() requires rowCache in RenderContext. '
        'This should be provided by ReactiveQueryWidget.',
      );
    }

    if (context.entityName == null) {
      throw ArgumentError('tree() requires entityName in RenderContext.');
    }

    if (context.onOperation == null) {
      throw ArgumentError('tree() requires onOperation in RenderContext.');
    }

    // For single-table queries, rowTemplates may be empty but item_template is provided
    final itemTemplateExpr = args.templates['item_template'] ?? args.templates['item'];
    List<RowTemplate> rowTemplates;
    if (context.rowTemplates.isEmpty) {
      if (itemTemplateExpr == null) {
        throw ArgumentError(
          'tree() requires either rowTemplates in RenderContext (for UNION queries) '
          'or item_template argument (for single-table queries).',
        );
      }
      rowTemplates = [
        RowTemplate(
          index: BigInt.zero,
          entityName: context.entityName!,
          entityShortName: context.entityName!.split('_').last,
          expr: itemTemplateExpr,
        ),
      ];
    } else {
      rowTemplates = context.rowTemplates;
    }

    final rowCache = context.rowCache!;
    final entityName = context.entityName!;
    final onOperation = context.onOperation!;

    // Build indices for O(1) lookups
    final rootNodes = <Map<String, dynamic>>[];
    final childrenIndex = <String, List<Map<String, dynamic>>>{};
    final parentMap = <Map<String, dynamic>, Map<String, dynamic>?>{};

    String getId(Map<String, dynamic> row) => row['id']?.toString() ?? '';

    int compareSortKeys(dynamic a, dynamic b) {
      if (a == null && b == null) return 0;
      if (a == null) return -1;
      if (b == null) return 1;
      if (a is num && b is num) return a.compareTo(b);
      return a.toString().compareTo(b.toString());
    }

    for (final row in rowCache.values) {
      final parentId = row[parentIdColumn]?.toString();
      final isRoot = parentId == null || parentId.isEmpty || parentId == 'null';

      if (isRoot) {
        rootNodes.add(row);
        parentMap[row] = null;
      } else {
        childrenIndex.putIfAbsent(parentId, () => []).add(row);
        if (rowCache.containsKey(parentId)) {
          parentMap[row] = rowCache[parentId];
        } else {
          parentMap[row] = null;
        }
      }
    }

    rootNodes.sort((a, b) => compareSortKeys(a[sortKeyColumn], b[sortKeyColumn]));
    for (final children in childrenIndex.values) {
      children.sort((a, b) => compareSortKeys(a[sortKeyColumn], b[sortKeyColumn]));
    }

    List<Map<String, dynamic>> getRootNodes() => rootNodes;
    List<Map<String, dynamic>> getChildren(Map<String, dynamic> node) {
      final id = getId(node);
      return childrenIndex[id] ?? const [];
    }

    final treeKey = '${entityName}_${parentIdColumn}_$sortKeyColumn';

    return TreeViewWidget(
      treeKey: treeKey,
      rowCache: rowCache,
      parentIdColumn: parentIdColumn,
      sortKeyColumn: sortKeyColumn,
      entityName: entityName,
      onOperation: onOperation,
      rowTemplates: rowTemplates,
      buildTemplate: buildTemplate,
      context: context,
      getId: getId,
      getRootNodes: getRootNodes,
      getChildren: getChildren,
      parentMap: parentMap,
      queryParams: context.queryParams,
    );
  }
}
