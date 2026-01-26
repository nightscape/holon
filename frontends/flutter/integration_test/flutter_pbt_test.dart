import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:holon/src/rust/api/pbt_proptest.dart';
import 'package:holon/src/rust/api/repository.dart';
import 'package:holon/src/rust/frb_generated.dart';

/// Integration test for property-based testing of Flutter UI
///
/// This test runs the full proptest PBT infrastructure from holon against
/// the actual Flutter UI to prove correctness. It generates random operation sequences
/// and verifies the UI state matches the reference implementation.
void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  late RustDocumentRepository repository;

  setUpAll(() async {
    await RustLib.init();
  });

  setUp(() async {
    // Create a fresh repository for the test
    repository = await RustDocumentRepository.createNew(
      docId: 'test-flutter-pbt-${DateTime.now().millisecondsSinceEpoch}',
    );
  });

  tearDown(() async {
    await repository.dispose();
  });

  test(
    'PBT test - Full Proptest (5 cases, 10 steps each)',
    () async {
      print('\n=== Full Proptest PBT ===');

      try {
        final result = await runFlutterPbtProptest(
          numCases: 5,
          stepsPerCase: BigInt.from(10),
          testId: 'flutter-proptest-${DateTime.now().millisecondsSinceEpoch}',
          getBlocks: () async {
            return await repository.getAllBlocks(
              traversal: await traversalAllButRoot(),
            );
          },
          createBlock: (id, parentId, content) async {
            await repository.createBlock(
              parentId: parentId,
              content: content,
              id: id,
            );
          },
          updateBlock: (id, content) async {
            await repository.updateBlock(id: id, content: content);
          },
          deleteBlock: (id) async {
            await repository.deleteBlock(id: id);
          },
          moveBlock: (id, newParent) async {
            await repository.moveBlock(id: id, newParent: newParent);
          },
        );

        print('\n🎉 Proptest Result: $result\n');
        expect(result, contains('passed'));
      } catch (e, stackTrace) {
        print('❌ Proptest FAILED: $e\n');
        print('Stack trace: $stackTrace\n');
        rethrow;
      }
    },
    timeout: Timeout(Duration(minutes: 5)),
  );
}
