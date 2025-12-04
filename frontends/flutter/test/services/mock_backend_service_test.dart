import 'package:flutter_test/flutter_test.dart';
import '../../lib/services/mock_backend_service.dart';
import '../../lib/src/rust/api/types.dart' show MapChange, ChangeOrigin;
import '../../lib/src/rust/third_party/query_render/types.dart'
    show Value, RenderSpec, RenderExpr, Value_String;
import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart'
    show RustStreamSink;
import '../../lib/src/rust/api/ffi_bridge.dart' as ffi;

void main() {
  group('MockBackendService', () {
    late MockBackendService mockService;

    setUp(() {
      mockService = MockBackendService();
    });

    tearDown(() {
      mockService.dispose();
    });

    test('setQueryResult stores result correctly', () async {
      final renderSpec = RenderSpec(
        root: const RenderExpr.columnRef(name: 'id'),
        nestedQueries: const [],
        operations: const {},
      );
      final initialData = <Map<String, Value>>[
        {'id': const Value_String('test-1')},
      ];

      mockService.setQueryResult(renderSpec, initialData);

      final sink = RustStreamSink<MapChange>();
      final result = await mockService.queryAndWatch(
        prql: 'SELECT * FROM test',
        params: const {},
        sink: ffi.MapChangeSink(sink: sink),
      );

      expect(result.$1, renderSpec);
      expect(result.$2, initialData);
    });

    test('emitChange adds events to stream', () async {
      final sink = RustStreamSink<MapChange>();
      await mockService.queryAndWatch(
        prql: 'SELECT * FROM test',
        params: const {},
        sink: ffi.MapChangeSink(sink: sink),
      );

      final change = MapChange.created(
        data: {'id': const Value_String('test-1')},
        origin: ChangeOrigin.local,
      );

      var receivedChange = false;
      // Use the mock service's change stream directly for testing
      mockService.changeStream.listen((event) {
        receivedChange = true;
        expect(event, change);
      });

      mockService.emitChange(change);

      // Wait for event to be processed
      await Future.delayed(const Duration(milliseconds: 50));

      expect(receivedChange, true);
    });

    test('executeOperation records calls', () async {
      await mockService.executeOperation(
        entityName: 'blocks',
        opName: 'indent',
        params: {'id': const Value_String('test-1')},
      );

      expect(mockService.operationCalls.length, 1);
      expect(mockService.operationCalls.first.entityName, 'blocks');
      expect(mockService.operationCalls.first.opName, 'indent');
    });

    test('hasOperation returns configured value', () async {
      mockService.setAvailableOperations('blocks', {'indent', 'outdent'});

      expect(
        await mockService.hasOperation(entityName: 'blocks', opName: 'indent'),
        true,
      );
      expect(
        await mockService.hasOperation(entityName: 'blocks', opName: 'delete'),
        false,
      );
    });

    test('syncAllProviders succeeds by default', () async {
      await expectLater(mockService.syncAllProviders(), completes);
    });

    test('syncAllProviders fails when configured', () async {
      mockService.setSyncBehavior(
        shouldSucceed: false,
        error: Exception('Test error'),
      );

      await expectLater(mockService.syncAllProviders(), throwsException);
    });
  });
}
