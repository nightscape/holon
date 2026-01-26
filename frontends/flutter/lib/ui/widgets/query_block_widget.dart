import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../../providers/query_providers.dart';
import '../../providers/settings_provider.dart' show appColorsProvider;
import '../../render/reactive_query_widget.dart';
import '../../utils/value_converter.dart'
    show valueMapToDynamic, dynamicToValueMap;

/// Widget that executes and renders a PRQL query from a block.
///
/// This widget is used for rendering query blocks in the index document
/// and other embedded queries throughout the application.
///
/// Features:
/// - Executes PRQL query via [queryResultByPrqlProvider]
/// - Renders results using [ReactiveQueryWidget] with CDC streaming
/// - Handles loading and error states
/// - Supports operation callbacks for interactive widgets
class QueryBlockWidget extends ConsumerWidget {
  /// The PRQL source code to execute.
  final String prqlSource;

  /// Whether this query is in the main content region (affects sizing).
  final bool isMainRegion;

  /// Optional minimum height for the widget.
  final double? minHeight;

  /// Optional maximum height constraint.
  final double? maxHeight;

  const QueryBlockWidget({
    super.key,
    required this.prqlSource,
    this.isMainRegion = false,
    this.minHeight,
    this.maxHeight,
  });

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final colors = ref.watch(appColorsProvider);
    final queryResult = ref.watch(queryResultByPrqlProvider(prqlSource));

    return queryResult.when(
      data: (result) => ConstrainedBox(
        constraints: BoxConstraints(
          minHeight: minHeight ?? (isMainRegion ? 200 : 50),
          maxHeight: maxHeight ?? double.infinity,
        ),
        child: ReactiveQueryWidget(
          sql: '',
          params: const {},
          renderSpec: result.renderSpec,
          changeStream: result.changeStream,
          initialData: result.initialData
              .map((row) => valueMapToDynamic(row))
              .toList(),
          onOperation: (entity, op, params) async {
            await ref
                .read(backendServiceProvider)
                .executeOperation(
                  entityName: entity,
                  opName: op,
                  params: dynamicToValueMap(params),
                );
          },
        ),
      ),
      loading: () => SizedBox(
        height: minHeight ?? (isMainRegion ? 200 : 100),
        child: Center(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              SizedBox(
                width: 24,
                height: 24,
                child: CircularProgressIndicator(
                  strokeWidth: 2,
                  color: colors.textSecondary,
                ),
              ),
              const SizedBox(height: 8),
              Text(
                'Loading query...',
                style: TextStyle(color: colors.textSecondary, fontSize: 12),
              ),
            ],
          ),
        ),
      ),
      error: (error, stackTrace) => Container(
        padding: const EdgeInsets.all(16),
        decoration: BoxDecoration(
          color: colors.error.withValues(alpha: 0.1),
          borderRadius: BorderRadius.circular(8),
          border: Border.all(color: colors.error.withValues(alpha: 0.3)),
        ),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          mainAxisSize: MainAxisSize.min,
          children: [
            Row(
              children: [
                Icon(Icons.error_outline, color: colors.error, size: 20),
                const SizedBox(width: 8),
                Text(
                  'Query Error',
                  style: TextStyle(
                    color: colors.error,
                    fontWeight: FontWeight.w600,
                  ),
                ),
              ],
            ),
            const SizedBox(height: 8),
            Text(
              error.toString(),
              style: TextStyle(
                color: colors.textSecondary,
                fontSize: 12,
                fontFamily: 'monospace',
              ),
            ),
          ],
        ),
      ),
    );
  }
}
