import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../src/rust/third_party/holon_api/render_types.dart';
import '../providers/settings_provider.dart';
import 'reactive_query_notifier.dart';
import 'render_context.dart';

/// Wrapper for Map that uses content-based equality (mapEquals).
///
/// Riverpod's select() uses == for comparison. By default, Map uses
/// reference equality, so even identical content in different Map objects
/// triggers rebuilds. This wrapper uses mapEquals for content comparison.
class _RowDataWrapper {
  final Map<String, dynamic>? data;
  const _RowDataWrapper(this.data);

  @override
  bool operator ==(Object other) =>
      other is _RowDataWrapper && mapEquals(data, other.data);

  @override
  int get hashCode => data != null ? Object.hashAll(data!.values) : 0;
}

/// A tree node widget that watches only its own row from the cache.
///
/// This enables fine-grained rebuilds: when a row's data changes,
/// only that specific node rebuilds, not the entire tree.
///
/// Uses Riverpod's `select()` to efficiently watch only the specific
/// row data for this node, avoiding unnecessary rebuilds when other
/// rows change.
///
/// IMPORTANT: This widget intentionally does NOT receive `rowCache` as a prop.
/// If it did, every parent rebuild would create a new Map object, causing
/// Flutter to call build() even when the selected row data hasn't changed.
/// Instead, we fetch `rowCache` inside build() using `ref.read()`.
class TreeNodeWidget extends ConsumerWidget {
  /// The unique ID of this node (used to look up row data).
  final String nodeId;

  /// Query params for accessing the ReactiveQueryStateNotifier.
  final ReactiveQueryParams queryParams;

  /// Function to build widgets from RenderExpr templates.
  final Widget Function(RenderExpr, RenderContext) buildTemplate;

  /// The template containing the RenderExpr for this node type.
  final RowTemplate template;

  /// Row templates for heterogeneous UNION queries.
  final List<RowTemplate> rowTemplates;

  /// Callback for executing operations.
  final Future<void> Function(String, String, Map<String, dynamic>)?
      onOperation;

  const TreeNodeWidget({
    required this.nodeId,
    required this.queryParams,
    required this.buildTemplate,
    required this.template,
    required this.rowTemplates,
    required this.onOperation,
    super.key,
  });

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    // Watch ONLY this node's data using select() with content-based equality
    // This is the key optimization: only rebuilds when THIS row's content changes
    //
    // We wrap the Map in _RowDataWrapper which uses mapEquals for comparison.
    // This prevents rebuilds when sync emits all rows as "updates" even though
    // only one row's content actually changed.
    final wrapper = ref.watch(
      reactiveQueryStateProvider(queryParams).select(
        (asyncState) {
          final data = asyncState.value?.rowCache[nodeId];
          debugPrint('[TreeNodeWidget] select callback for node=$nodeId, content="${data?['content']}"');
          return _RowDataWrapper(data);
        },
      ),
    );
    final rowData = wrapper.data;
    debugPrint('[TreeNodeWidget] build() for node=$nodeId, content="${rowData?['content']}"');

    // If row not found, return empty (might happen during transitions)
    if (rowData == null) {
      return const SizedBox.shrink();
    }

    // Watch colors separately (rarely changes)
    final colors = ref.watch(appColorsProvider);

    // Fetch rowCache using read() (not watch!) to avoid triggering rebuilds
    // when other rows change. We only need rowCache for RenderContext operations
    // that may look up other rows.
    final state = ref.read(reactiveQueryStateProvider(queryParams));
    final rowCache = state.value?.rowCache ?? {};

    // Build the node widget from template
    final nodeContext = RenderContext(
      rowData: rowData,
      rowTemplates: rowTemplates,
      onOperation: onOperation,
      entityName: template.entityName,
      colors: colors,
      rowCache: rowCache,
      queryParams: queryParams,
    );

    return buildTemplate(template.expr, nodeContext);
  }
}
