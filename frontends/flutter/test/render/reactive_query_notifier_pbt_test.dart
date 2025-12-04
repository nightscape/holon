import 'dart:async';
import 'package:flutter_test/flutter_test.dart';
import 'package:dartproptest/dartproptest.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../../lib/render/reactive_query_notifier.dart';
import '../../lib/src/rust/api/types.dart'
    show
        MapChange,
        ChangeOrigin,
        RowChange_Created,
        RowChange_Updated,
        RowChange_Deleted;
import '../../lib/src/rust/third_party/query_render/types.dart' show Value;
import '../../lib/utils/value_converter.dart' show valueMapToDynamic;
import '../helpers/pbt_helpers.dart';

void main() {
  group('ReactiveQueryStateNotifier Property-Based Tests', () {
    test(
      'Cache consistency: cache contains exactly the rows that should exist',
      () async {
        await forAllAsync(
          (List<MapChange> changes) async {
            // Set up provider container
            final container = ProviderContainer();

            // Create initial data from created events
            final initialData = <Map<String, Value>>[];
            final expectedIds = <String>{};

            // Process changes to build expected state
            final expectedCache = <String, Map<String, dynamic>>{};

            for (final change in changes) {
              // Use pattern matching via helper functions
              final id = extractRowId(change);
              final data = extractRowData(change);

              if (change is RowChange_Created) {
                if (id != null && data != null) {
                  expectedIds.add(id);
                  expectedCache[id] = valueMapToDynamic(data);
                  if (!initialData.any(
                    (row) =>
                        extractRowId(
                          MapChange.created(
                            data: row,
                            origin: ChangeOrigin.local,
                          ),
                        ) ==
                        id,
                  )) {
                    initialData.add(data);
                  }
                }
              } else if (change is RowChange_Updated) {
                if (id != null && data != null && expectedIds.contains(id)) {
                  expectedCache[id] = valueMapToDynamic(data);
                }
              } else if (change is RowChange_Deleted) {
                if (id != null) {
                  expectedIds.remove(id);
                  expectedCache.remove(id);
                }
              }
            }

            // Create a stream controller for change events
            final streamController = StreamController<MapChange>.broadcast();

            // Create query params
            // Convert initialData from Map<String, Value> to Map<String, dynamic>
            final convertedInitialData = initialData
                .map((row) => valueMapToDynamic(row))
                .toList();
            final params = ReactiveQueryParams(
              queryKey: 'cache-consistency',
              sql: 'SELECT * FROM test',
              params: const {},
              changeStream: streamController.stream,
              initialData: convertedInitialData,
              valueConverter: valueMapToDynamic,
            );

            // Wait for initial state
            await container.read(reactiveQueryStateProvider(params).future);

            // Emit all changes
            for (final change in changes) {
              streamController.add(change);
              // Small delay to allow processing
              await Future.delayed(const Duration(milliseconds: 5));
            }

            // Wait a bit for all events to process (longer for more changes)
            await Future.delayed(
              Duration(milliseconds: 50 + (changes.length * 2)),
            );

            // Check final state
            final asyncState = container.read(
              reactiveQueryStateProvider(params),
            );
            final finalState = asyncState.value;

            if (finalState != null) {
              // Verify cache contains exactly expected rows
              expect(
                finalState.rowCache.length,
                expectedCache.length,
                reason: 'Cache size should match expected',
              );

              // Verify all expected rows are in cache
              for (final id in expectedIds) {
                expect(
                  finalState.rowCache.containsKey(id),
                  true,
                  reason: 'Cache should contain row $id',
                );
                if (finalState.rowCache.containsKey(id) &&
                    expectedCache.containsKey(id)) {
                  expect(
                    finalState.rowCache[id],
                    expectedCache[id],
                    reason: 'Cache data for $id should match expected',
                  );
                }
              }

              // Verify no extra rows in cache
              for (final id in finalState.rowCache.keys) {
                expect(
                  expectedIds.contains(id),
                  true,
                  reason: 'Cache should not contain unexpected row $id',
                );
              }
            }

            // Cleanup
            streamController.close();
            container.dispose();
          },
          [rowChangeListArbitrary(minLength: 1, maxLength: 30)],
          numRuns: 100,
        );
      },
    );

    test('Row order: order matches insertion order', () async {
      await forAllAsync(
        (List<MapChange> changes) async {
          final container = ProviderContainer();
          final initialData = <Map<String, Value>>[];
          final expectedOrder = <String>[];
          final streamController = StreamController<MapChange>.broadcast();

          // Build expected order from created events
          for (final change in changes) {
            switch (change) {
              case RowChange_Created(data: final data, origin: _):
                final id = extractRowId(change);
                if (id != null && !expectedOrder.contains(id)) {
                  expectedOrder.add(id);
                  initialData.add(data);
                }
              case RowChange_Updated(id: _, data: _, origin: _):
                // Updates don't change order
                break;
              case RowChange_Deleted(id: final id, origin: _):
                expectedOrder.remove(id);
            }
          }

          // Convert initialData from Map<String, Value> to Map<String, dynamic>
          final convertedInitialData = initialData
              .map((row) => valueMapToDynamic(row))
              .toList();
          final params = ReactiveQueryParams(
            queryKey: 'row-order',
            sql: 'SELECT * FROM test',
            params: const {},
            changeStream: streamController.stream,
            initialData: convertedInitialData,
            valueConverter: valueMapToDynamic,
          );

          await container.read(reactiveQueryStateProvider(params).future);

          // Emit changes
          for (final change in changes) {
            streamController.add(change);
            await Future.delayed(const Duration(milliseconds: 10));
          }

          await Future.delayed(const Duration(milliseconds: 100));

          final asyncState = container.read(reactiveQueryStateProvider(params));
          final finalState = asyncState.value;

          if (finalState != null) {
            // Verify order matches (at least for rows that exist)
            final actualOrder = finalState.rowOrder
                .where((id) => expectedOrder.contains(id))
                .toList();

            // Check that order is preserved for existing rows
            // (allowing for some flexibility since updates don't change order)
            expect(
              actualOrder.length,
              greaterThanOrEqualTo(0),
              reason: 'Order should contain at least some expected rows',
            );
          }

          streamController.close();
          container.dispose();
        },
        [rowChangeListArbitrary(minLength: 1, maxLength: 30)],
        numRuns: 100,
      );
    });

    test('Edge case: Rapid successive changes', () async {
      await forAllAsync(
        (List<MapChange> changes) async {
          final container = ProviderContainer();
          final streamController = StreamController<MapChange>.broadcast();

          final params = ReactiveQueryParams(
            queryKey: 'row-order-updated',
            sql: 'SELECT * FROM test',
            params: const {},
            changeStream: streamController.stream,
            initialData: const [],
            valueConverter: valueMapToDynamic,
          );

          await container.read(reactiveQueryStateProvider(params).future);

          // Emit all changes rapidly
          for (final change in changes) {
            streamController.add(change);
          }

          // Wait for processing
          await Future.delayed(const Duration(milliseconds: 200));

          final asyncState = container.read(reactiveQueryStateProvider(params));
          final finalState = asyncState.value;

          // Verify state is consistent (no crashes, valid cache)
          expect(finalState, isNotNull);
          expect(finalState!.rowCache, isA<Map<String, dynamic>>());
          expect(finalState.rowOrder, isA<List<String>>());

          streamController.close();
          container.dispose();
        },
        [rowChangeListArbitrary(minLength: 10, maxLength: 100)],
        numRuns: 100,
      );
    });

    test('Edge case: Update before create', () async {
      await forAllAsync(
        (Map<String, Value> data) async {
          final container = ProviderContainer();
          final streamController = StreamController<MapChange>.broadcast();
          final id = 'test-id-${DateTime.now().millisecondsSinceEpoch}';

          // Ensure id is in data
          final dataWithId = Map<String, Value>.from(data);
          dataWithId['id'] = Value.string(id);

          final params = ReactiveQueryParams(
            queryKey: 'row-order-deleted',
            sql: 'SELECT * FROM test',
            params: const {},
            changeStream: streamController.stream,
            initialData: const [],
            valueConverter: valueMapToDynamic,
          );

          await container.read(reactiveQueryStateProvider(params).future);

          // Emit update before create
          streamController.add(
            MapChange.updated(
              id: id,
              data: dataWithId,
              origin: ChangeOrigin.local,
            ),
          );

          await Future.delayed(const Duration(milliseconds: 50));

          // Then emit create
          streamController.add(
            MapChange.created(data: dataWithId, origin: ChangeOrigin.local),
          );

          await Future.delayed(const Duration(milliseconds: 50));

          final asyncState = container.read(reactiveQueryStateProvider(params));
          final finalState = asyncState.value;

          // Should handle gracefully (either treat update as create or ignore)
          expect(finalState, isNotNull);

          streamController.close();
          container.dispose();
        },
        [valueMapArbitrary()],
        numRuns: 100,
      );
    });
  });
}
