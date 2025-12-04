import 'dart:async';
import 'package:flutter/foundation.dart';
import 'package:riverpod_annotation/riverpod_annotation.dart';
import 'package:holon/src/rust/third_party/holon_api/streaming.dart'
    show BatchMapChangeWithMetadata;
import '../utils/log.dart';
import '../services/logging_service.dart' show LoggingService, LogTraceContext;
import '../data/row_data_block_ops.dart';
import '../src/rust/third_party/holon_api.dart' show Value, Value_String;
import 'reactive_query_widget.dart'
    show RowEvent, RowEventType, rowChangeToRowEvent;

part 'reactive_query_notifier.g.dart';

/// State class for ReactiveQuery widget.
class ReactiveQueryState {
  final Map<String, Map<String, dynamic>> rowCache;
  final List<String> rowOrder;
  final StreamSubscription<List<RowEvent>>? streamSubscription;
  final bool isSyncing;
  final RowDataBlockOps? blockOps;

  ReactiveQueryState({
    Map<String, Map<String, dynamic>>? rowCache,
    List<String>? rowOrder,
    this.streamSubscription,
    this.isSyncing = false,
    this.blockOps,
  }) : rowCache = rowCache ?? {},
       rowOrder = rowOrder ?? [];

  ReactiveQueryState copyWith({
    Map<String, Map<String, dynamic>>? rowCache,
    List<String>? rowOrder,
    StreamSubscription<List<RowEvent>>? streamSubscription,
    bool? isSyncing,
    RowDataBlockOps? blockOps,
    bool clearRowCache = false,
    bool clearRowOrder = false,
  }) {
    return ReactiveQueryState(
      rowCache: clearRowCache ? {} : (rowCache ?? this.rowCache),
      rowOrder: clearRowOrder ? [] : (rowOrder ?? this.rowOrder),
      streamSubscription: streamSubscription ?? this.streamSubscription,
      isSyncing: isSyncing ?? this.isSyncing,
      blockOps: blockOps ?? this.blockOps,
    );
  }
}

/// Parameters needed for ReactiveQueryStateNotifier initialization.
@immutable
class ReactiveQueryParams {
  final String queryKey;
  final String sql;
  final Map<String, dynamic> params;
  final Stream<BatchMapChangeWithMetadata>? changeStream;
  final List<Map<String, dynamic>>? initialData;
  final Future<void> Function(String, String, Map<String, dynamic>)?
  onOperation;
  final Future<void> Function()? onSync;
  final Map<String, dynamic> Function(Map<String, Value>) valueConverter;

  ReactiveQueryParams({
    required this.queryKey,
    required this.sql,
    required Map<String, dynamic> params,
    this.changeStream,
    List<Map<String, dynamic>>? initialData,
    this.onOperation,
    this.onSync,
    required this.valueConverter,
  }) : params = Map.unmodifiable(params),
       initialData = initialData == null
           ? null
           : List<Map<String, dynamic>>.unmodifiable(
               initialData.map((row) => Map<String, dynamic>.unmodifiable(row)),
             );

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;
    return other is ReactiveQueryParams &&
        other.queryKey == queryKey &&
        mapEquals(other.params, params);
  }

  @override
  int get hashCode {
    return Object.hash(queryKey, sql, _mapHash(params));
  }
}

bool _listOfMapsEquals(
  List<Map<String, dynamic>>? a,
  List<Map<String, dynamic>>? b,
) {
  if (identical(a, b)) return true;
  if (a == null || b == null) return a == b;
  if (a.length != b.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (!mapEquals(a[i], b[i])) {
      return false;
    }
  }
  return true;
}

int _listOfMapsHash(List<Map<String, dynamic>>? list) {
  if (list == null || list.isEmpty) return 0;
  return Object.hashAll(list.map(_mapHash));
}

int _mapHash(Map<String, dynamic> map) {
  if (map.isEmpty) return 0;
  final entries = map.entries.toList()..sort((a, b) => a.key.compareTo(b.key));
  return Object.hashAll(
    entries.map((entry) => Object.hash(entry.key, entry.value)),
  );
}

/// AsyncNotifier for managing ReactiveQuery state (Riverpod 3.x with code generation).
@riverpod
class ReactiveQueryStateNotifier extends _$ReactiveQueryStateNotifier {
  ReactiveQueryParams? _params;
  StreamSubscription<List<RowEvent>>? _subscription;
  Stream<List<RowEvent>>? _cdcStream;
  RowDataBlockOps? _blockOps;

  @override
  Future<ReactiveQueryState> build(ReactiveQueryParams params) async {
    final keepAliveLink = ref.keepAlive();
    _params = params;

    // Initialize stream and subscribe
    await _initializeStream();
    _subscribeToStream();

    // Register cleanup
    ref.onDispose(() {
      keepAliveLink.close();
      _subscription?.cancel();
      _subscription = null;
      _blockOps?.dispose();
      _blockOps = null;
    });

    return state.value!;
  }

  ReactiveQueryParams get params {
    if (_params == null) {
      throw StateError('ReactiveQueryStateNotifier not initialized');
    }
    return _params!;
  }

  /// Initialize CDC stream from Rust.
  Future<void> _initializeStream() async {
    // Cancel existing subscription if any
    _subscription?.cancel();
    _subscription = null;

    // Reset state when reinitializing, but try to preserve existing cache
    // to avoid reverting to stale initialData during rebuilds.
    var newCache = <String, Map<String, dynamic>>{};
    var newOrder = <String>[];

    if (state.hasValue &&
        state.value != null &&
        state.value!.rowCache.isNotEmpty) {
      newCache = Map.from(state.value!.rowCache);
      newOrder = List.from(state.value!.rowOrder);
    } else if (params.initialData != null) {
      // Populate cache with initial data only if we don't have existing state
      // Use a Set for O(1) duplicate detection instead of O(n) List.contains()
      final seenIds = <String>{};
      for (final row in params.initialData!) {
        // Extract ID - handle both Value objects and already-converted strings
        String? id;
        final idValue = row['id'];
        if (idValue is String) {
          id = idValue;
        } else if (idValue is Value_String) {
          id = idValue.field0;
        } else if (idValue != null) {
          id = idValue.toString();
        }

        if (id != null && id.isNotEmpty) {
          newCache[id] = row;
          if (seenIds.add(id)) {
            newOrder.add(id);
          }
        }
      }
    }

    state = AsyncData(
      ReactiveQueryState(rowCache: newCache, rowOrder: newOrder),
    );

    // Process BatchMapChangeWithMetadata stream - process entire batches at once
    // to avoid 118k individual state updates
    if (params.changeStream != null) {
      _cdcStream = params.changeStream!.map<List<RowEvent>>((
        batchWithMetadata,
      ) {
        final relationName = batchWithMetadata.metadata.relationName;
        final changeCount = batchWithMetadata.inner.items.length;

        // Set trace context from Rust CDC batch for log correlation
        final traceCtx = batchWithMetadata.metadata.traceContext;
        if (traceCtx != null) {
          LoggingService.setTraceContext(
            LogTraceContext(traceId: traceCtx.traceId, spanId: traceCtx.spanId),
          );
        } else {
          LoggingService.clearTraceContext();
        }

        log.debug(
          'Batch received: relation=$relationName, changes=$changeCount',
        );

        // Convert all changes to RowEvents in one pass
        final events = batchWithMetadata.inner.items
            .map((change) => rowChangeToRowEvent(change, params.valueConverter))
            .toList();

        LoggingService.clearTraceContext();
        return events;
      });
    } else {
      _cdcStream = null;
    }
  }

  /// Subscribe to stream and process all events.
  void _subscribeToStream() {
    if (_cdcStream == null) return;

    _subscription = _cdcStream!.listen(
      (batch) {
        // Process entire batch with single state update
        _updateCacheBatch(batch);
      },
      onError: (error) {
        log.error('CDC stream error: $error');
      },
      onDone: () {
        log.debug('CDC stream closed');
      },
    );

    if (state.value != null) {
      state = AsyncData(
        state.value!.copyWith(streamSubscription: _subscription),
      );
    }
  }

  /// Process an entire batch of events with a single state update.
  /// This is critical for performance - avoids 118k individual state updates.
  void _updateCacheBatch(List<RowEvent> events) {
    if (state.value == null || events.isEmpty) return;

    final currentState = state.value!;
    final cacheSizeBefore = currentState.rowCache.length;

    // Copy once at start, mutate in place
    final newCache = Map<String, Map<String, dynamic>>.from(
      currentState.rowCache,
    );
    final newOrder = List<String>.from(currentState.rowOrder);
    // Use Set for O(1) contains check during batch processing
    final orderSet = newOrder.toSet();

    int addedCount = 0;
    int updatedCount = 0;
    int removedCount = 0;

    for (final event in events) {
      if (event.rowId.isEmpty) continue;

      switch (event.type) {
        case RowEventType.added:
          if (event.data == null) continue;
          newCache[event.rowId] = event.data!;
          if (orderSet.add(event.rowId)) {
            newOrder.add(event.rowId);
          }
          currentState.blockOps?.updateRowCache(event.rowId, event.data!);
          addedCount++;
          break;

        case RowEventType.updated:
          if (event.data == null) continue;
          final wasNew = !newCache.containsKey(event.rowId);
          newCache[event.rowId] = event.data!;
          if (wasNew && orderSet.add(event.rowId)) {
            newOrder.add(event.rowId);
          }
          currentState.blockOps?.updateRowCache(event.rowId, event.data!);
          updatedCount++;
          break;

        case RowEventType.removed:
          newCache.remove(event.rowId);
          orderSet.remove(event.rowId);
          newOrder.remove(event.rowId);
          currentState.blockOps?.updateRowCache(event.rowId, null);
          removedCount++;
          break;
      }
    }

    final cacheSizeAfter = newCache.length;
    log.debug(
      'Batch processed: added=$addedCount, updated=$updatedCount, removed=$removedCount, '
      'cache size: $cacheSizeBefore -> $cacheSizeAfter',
    );

    // Single state update for entire batch
    state = AsyncData(
      currentState.copyWith(rowCache: newCache, rowOrder: newOrder),
    );
  }

  /// Update cache with new initial data (e.g., after sync)
  void updateInitialData(List<Map<String, dynamic>>? initialData) {
    if (state.value == null) return;

    final newCache = <String, Map<String, dynamic>>{};
    final newOrder = <String>[];
    if (initialData != null) {
      // Use a Set for O(1) duplicate detection instead of O(n) List.contains()
      final seenIds = <String>{};
      for (final row in initialData) {
        final id = row['id']?.toString();
        if (id != null) {
          newCache[id] = row;
          if (seenIds.add(id)) {
            newOrder.add(id);
          }
          debugPrint(
            '[ReactiveQueryNotifier] updateInitialData row=$id content="${row['content']}"',
          );
        }
      }
    }
    state = AsyncData(
      state.value!.copyWith(rowCache: newCache, rowOrder: newOrder),
    );
  }

  /// Set blockOps instance
  void setBlockOps(RowDataBlockOps blockOps) {
    if (state.value == null) return;
    _blockOps = blockOps;
    state = AsyncData(state.value!.copyWith(blockOps: blockOps));
  }

  /// Dispose blockOps instance
  void disposeBlockOps() {
    if (state.value == null) return;
    _blockOps?.dispose();
    _blockOps = null;
    state = AsyncData(state.value!.copyWith(blockOps: null));
  }

  /// Start sync operation
  Future<void> sync() async {
    if (params.onSync == null || state.value?.isSyncing == true) return;

    state = AsyncData(state.value!.copyWith(isSyncing: true));
    try {
      await params.onSync!();
    } catch (e) {
      // Error handling could be improved
    } finally {
      if (state.value != null) {
        state = AsyncData(state.value!.copyWith(isSyncing: false));
      }
    }
  }

  /// Reinitialize stream (e.g., when query parameters change)
  Future<void> reinitializeStream() async {
    disposeBlockOps();
    await _initializeStream();
    _subscribeToStream();
  }
}
