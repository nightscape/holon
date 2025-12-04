import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../../lib/render/reactive_query_notifier.dart';
import '../../lib/src/rust/api/types.dart' show MapChange, ChangeOrigin;
import '../../lib/src/rust/third_party/query_render/types.dart' show Value;
import '../../lib/utils/value_converter.dart' show valueMapToDynamic;
import '../helpers/reactive_query_harness.dart';

void main() {
  testWidgets('Editable text keeps latest CDC update after rebuild', (
    WidgetTester tester,
  ) async {
    const widgetSql = 'SELECT * FROM test';
    const widgetParams = <String, dynamic>{};
    final widgetQueryKey = '${widgetSql}_${widgetParams.toString()}';

    final initialData1 = [
      {'id': 'row-0', 'content': 'initial'},
    ];
    // Different object, same content (or different content, doesn't matter if we ignore it)
    // Let's make it different content to be sure we would see a revert if we switched providers.
    final initialData2 = [
      {'id': 'row-0', 'content': 'stale_revert'},
    ];

    final streamController = StreamController<MapChange>.broadcast();

    // Helper to create params
    ReactiveQueryParams createParams(List<Map<String, dynamic>> data) {
      return ReactiveQueryParams(
        queryKey: widgetQueryKey,
        sql: widgetSql,
        params: widgetParams,
        changeStream: streamController.stream,
        initialData: data,
        valueConverter: valueMapToDynamic,
      );
    }

    final container = ProviderContainer();

    // Helper to pump harness with specific data
    Future<void> pumpHarness(List<Map<String, dynamic>> data) async {
      await tester.pumpWidget(
        MaterialApp(
          home: UncontrolledProviderScope(
            container: container,
            child: Scaffold(
              body: ReactiveQueryHarness(
                initialData: data,
                streamController: streamController,
              ),
            ),
          ),
        ),
      );
      await tester.pumpAndSettle();
    }

    // 1. Start with initialData1
    await pumpHarness(initialData1);

    // 2. Apply CDC update
    streamController.add(
      MapChange.updated(
        id: 'row-0',
        data: {'id': Value.string('row-0'), 'content': Value.string('updated')},
        origin: ChangeOrigin.remote,
      ),
    );
    await tester.pump();
    await tester.pumpAndSettle();

    TextField textField() =>
        tester.widget<TextField>(find.byType(TextField).first);

    expect(textField().controller?.text, 'updated');

    // 3. Rebuild with initialData2 (stale/different)
    // If initialData is included in equality, this creates a NEW provider.
    // The new provider will initialize with 'stale_revert'.
    // If initialData is EXCLUDED from equality, this reuses the OLD provider.
    // The old provider has 'updated' in its cache.
    await pumpHarness(initialData2);

    // Expect to still see 'updated'
    expect(
      textField().controller?.text,
      'updated',
      reason:
          'Should retain updated value despite stale initialData in rebuild',
    );

    streamController.close();
    container.dispose();
  });
}
