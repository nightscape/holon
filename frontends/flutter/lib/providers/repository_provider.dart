/// Riverpod providers for the document repository and related state.
library;

import 'dart:async' show unawaited;
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:outliner_view/outliner_view.dart'
    show BlockOps, OutlinerNotifier, OutlinerState;
import '../data/rust_block_ops.dart';

/// Provider for the current document ID.
///
/// Change this to open a different document. When changed, the BlockOps
/// provider will automatically dispose the old instance and create a new one.
final documentIdProvider = StateProvider<String>((ref) => 'default');

/// Provider for the BlockOps instance.
///
/// Automatically creates and initializes a BlockOps implementation for the current document ID.
/// Properly disposes of resources when no longer needed or when document ID changes.
final blockOpsProvider = FutureProvider<BlockOps<RustBlock>>((ref) async {
  final docId = ref.watch(documentIdProvider);

  // Create and initialize RustBlockOps
  final ops = await RustBlockOps.createNew(docId);

  // Dispose when provider is disposed
  ref.onDispose(() {
    unawaited(ops.dispose());
  });

  return ops;
});

/// Provider for the RustBlockOps instance (for P2P and backend-specific operations).
///
/// Use this when you need access to P2P methods like getNodeId(), connectToPeer(), etc.
/// For general block operations, use blockOpsProvider instead.
final rustBlockOpsProvider = FutureProvider<RustBlockOps>((ref) async {
  final ops = await ref.watch(blockOpsProvider.future);
  return ops as RustBlockOps;
});

/// Synchronous provider wrapper for blockOpsProvider (for UI widgets that need sync access).
///
/// This provider handles the async initialization by watching blockOpsProvider
/// and returning null until initialization completes.
final blockOpsSyncProvider = Provider<BlockOps<RustBlock>?>((ref) {
  final asyncOps = ref.watch(blockOpsProvider);
  return asyncOps.when<BlockOps<RustBlock>?>(
    data: (ops) => ops,
    loading: () => null,
    error: (_, __) => null,
  );
});

/// Outliner notifier provider for RustBlock.
///
/// This creates an OutlinerNotifier properly typed with RustBlock instead of
/// the default Block type from outliner_view.
final rustOutlinerProvider =
    StateNotifierProvider<
      OutlinerNotifier<RustBlock>,
      OutlinerState<RustBlock>
    >((ref) {
      final ops = ref.watch(blockOpsSyncProvider);
      if (ops == null) {
        throw StateError('BlockOps not yet initialized');
      }
      return OutlinerNotifier<RustBlock>(ops);
    });
