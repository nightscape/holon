import 'dart:async';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:holon/render/reactive_query_widget.dart';
import 'package:holon/src/rust/api/types.dart' show MapChange;
import 'package:holon/src/rust/third_party/query_render/types.dart'
    show RenderSpec, RenderExpr, Arg;

/// Harness widget for testing ReactiveQueryWidget with property-based tests.
///
/// This widget:
/// - Accepts initial data and a stream controller for CDC events
/// - Renders ReactiveQueryWidget with a deterministic PRQL spec (list of editable_text fields)
class ReactiveQueryHarness extends ConsumerWidget {
  /// Initial data to populate the cache
  final List<Map<String, dynamic>> initialData;

  /// Stream controller for CDC events
  final StreamController<MapChange> streamController;

  const ReactiveQueryHarness({
    super.key,
    required this.initialData,
    required this.streamController,
  });

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    // Create a simple RenderSpec that renders editable_text(content) for each row
    // Wrap editable_text in block() to avoid Flexible placement issues
    final renderSpec = RenderSpec(
      root: RenderExpr.functionCall(
        name: 'list',
        args: [
          Arg(
            name: 'item_template',
            value: RenderExpr.functionCall(
              name: 'block',
              args: [
                Arg(
                  name: null,
                  value: RenderExpr.functionCall(
                    name: 'editable_text',
                    args: [
                      Arg(
                        name: 'content',
                        value: const RenderExpr.columnRef(name: 'content'),
                      ),
                    ],
                    operations: [],
                  ),
                ),
              ],
              operations: [],
            ),
          ),
        ],
        operations: [],
      ),
      nestedQueries: const [],
      operations: const {},
    );

    return ReactiveQueryWidget(
      sql: 'SELECT * FROM test',
      params: const {},
      renderSpec: renderSpec,
      changeStream: streamController.stream,
      initialData: initialData,
    );
  }
}
