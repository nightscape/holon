import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../src/rust/third_party/holon_api/render_types.dart';
import '../providers/settings_provider.dart';
import 'render_interpreter.dart';
import 'reactive_query_notifier.dart';

/// Wrapper for Map that uses content-based equality (mapEquals).
class _RowDataWrapper {
  final Map<String, dynamic>? data;
  const _RowDataWrapper(this.data);

  @override
  bool operator ==(Object other) =>
      other is _RowDataWrapper && mapEquals(data, other.data);

  @override
  int get hashCode => data != null ? Object.hashAll(data!.values) : 0;
}

/// A list item widget that watches only its own row from the cache.
///
/// This enables fine-grained rebuilds: when a row's data changes,
/// only that specific item rebuilds, not the entire list.
///
/// Uses Riverpod's `select()` to efficiently watch only the specific
/// row data for this item, avoiding unnecessary rebuilds when other
/// rows change.
class ListItemWidget extends ConsumerWidget {
  /// The unique ID of this row (used to look up row data).
  final String rowId;

  /// Query params for accessing the ReactiveQueryStateNotifier.
  final ReactiveQueryParams queryParams;

  /// The interpreter used to build the item widget from RenderExpr.
  final RenderInterpreter interpreter;

  /// The template expression for this item.
  final RenderExpr itemExpr;

  /// Row templates for heterogeneous UNION queries.
  final List<RowTemplate> rowTemplates;

  /// Callback for executing operations.
  final Future<void> Function(String, String, Map<String, dynamic>)?
      onOperation;

  /// Row index in the list.
  final int rowIndex;

  /// Previous row data (for operations that need context).
  final Map<String, dynamic>? previousRowData;

  const ListItemWidget({
    required this.rowId,
    required this.queryParams,
    required this.interpreter,
    required this.itemExpr,
    required this.rowTemplates,
    required this.onOperation,
    required this.rowIndex,
    this.previousRowData,
    super.key,
  });

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    // Watch ONLY this row's data using select() with content-based equality
    final wrapper = ref.watch(
      reactiveQueryStateProvider(queryParams).select(
        (asyncState) {
          final data = asyncState.value?.rowCache[rowId];
          debugPrint('[ListItemWidget] select callback for row=$rowId, content="${data?['content']}"');
          return _RowDataWrapper(data);
        },
      ),
    );
    final rowData = wrapper.data;
    debugPrint('[ListItemWidget] build() for row=$rowId, content="${rowData?['content']}"');

    // If row not found, return empty (might happen during transitions)
    if (rowData == null) {
      return const SizedBox.shrink();
    }

    // Watch colors separately (rarely changes)
    final colors = ref.watch(appColorsProvider);

    final renderContext = RenderContext(
      rowData: rowData,
      rowTemplates: rowTemplates,
      onOperation: onOperation,
      rowIndex: rowIndex,
      previousRowData: previousRowData,
      colors: colors,
    );

    return MouseRegion(
      cursor: SystemMouseCursors.text,
      child: Container(
        padding: const EdgeInsets.symmetric(vertical: 2),
        decoration: BoxDecoration(
          borderRadius: BorderRadius.circular(4),
          color: Colors.transparent,
        ),
        child: interpreter.build(itemExpr, renderContext),
      ),
    );
  }
}
