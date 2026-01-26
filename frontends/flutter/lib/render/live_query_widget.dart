import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../providers/query_providers.dart';
import '../utils/value_converter.dart';
import 'reactive_query_widget.dart';

/// Widget that executes a PRQL query and renders results with live CDC updates.
///
/// This widget uses `queryResultWithContextProvider` which handles:
/// - PRQL compilation to SQL + RenderSpec
/// - Query execution and initial data retrieval
/// - CDC stream setup for live updates
/// - Optional context for `from children` resolution
///
/// Each instance creates its own independent CDC subscription.
///
/// Usage:
/// ```dart
/// LiveQueryWidget(
///   prql: 'from children select { id, content } render (list item_template:(text content))',
///   contextBlockId: 'parent-block-id', // Optional: resolves `from children` to this block's children
/// )
/// ```
class LiveQueryWidget extends ConsumerWidget {
  /// PRQL source code to execute.
  final String prql;

  /// Optional block ID for context.
  /// When set, `from children` in the PRQL resolves to children of this block.
  final String? contextBlockId;

  /// Callback for executing operations (indent, outdent, etc.).
  final Future<void> Function(
    String entityName,
    String operationName,
    Map<String, dynamic> params,
  )? onOperation;

  const LiveQueryWidget({
    super.key,
    required this.prql,
    this.contextBlockId,
    this.onOperation,
  });

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    // Use context-aware provider that handles `from children` resolution
    final queryResult = ref.watch(queryResultWithContextProvider((
      prql: prql,
      contextBlockId: contextBlockId,
    )));

    return queryResult.when(
      loading: () => const Center(child: CircularProgressIndicator()),
      error: (error, _) => Text('Query error: $error'),
      data: (result) => ReactiveQueryWidget(
        sql: contextBlockId != null ? '$prql@$contextBlockId' : prql,
        params: const {},
        renderSpec: result.renderSpec,
        changeStream: result.changeStream,
        initialData: result.initialData
            .map((row) => valueMapToDynamic(row))
            .toList(),
        onOperation: onOperation,
      ),
    );
  }
}
