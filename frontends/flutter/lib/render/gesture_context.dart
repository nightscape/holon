import 'operation_matcher.dart';
import 'render_context.dart';

/// Accumulates parameters during a gesture (drag, search+select, etc.).
///
/// Widgets contribute params as the gesture progresses:
/// - Source item provides `id` when gesture starts
/// - Drop target provides `tree_position` on drop
/// - Search selector provides `selected_id` on confirm
///
/// When the gesture completes, [findSatisfiableOperations] matches
/// available params against operation requirements.
class GestureContext {
  /// ID of the source item (the item being operated on).
  final String? sourceItemId;

  /// RenderContext of the source item (carries available operations).
  final RenderContext? sourceRenderContext;

  final Map<String, dynamic> _committedParams = {};
  final Map<String, dynamic> _previewParams = {};

  GestureContext({this.sourceItemId, this.sourceRenderContext}) {
    if (sourceItemId != null) {
      _committedParams['id'] = sourceItemId;
    }
  }

  /// Parameters that are definitely available (committed by widgets).
  Map<String, dynamic> get committedParams =>
      Map.unmodifiable(_committedParams);

  /// Parameters that would become available on completion (for preview).
  Map<String, dynamic> get previewParams => Map.unmodifiable(_previewParams);

  /// All params (committed + preview) for UI preview purposes.
  Map<String, dynamic> get allParams => {
    ..._committedParams,
    ..._previewParams,
  };

  /// Widget updates what it would provide (for preview, e.g., during drag hover).
  void updatePreview(Map<String, dynamic> params) {
    _previewParams.addAll(params);
  }

  /// Clear preview params (e.g., when drag leaves a target).
  void clearPreview() {
    _previewParams.clear();
  }

  /// Widget commits params when its trigger fires (e.g., on drop, on confirm).
  void commitParams(Map<String, dynamic> params) {
    _committedParams.addAll(params);
    for (final key in params.keys) {
      _previewParams.remove(key);
    }
  }

  /// Find operations satisfiable with current committed params.
  ///
  /// Uses the operations from [sourceRenderContext] as candidates.
  /// Returns matches sorted by completeness and priority.
  List<MatchedOperation> findSatisfiableOperations() {
    final candidates = sourceRenderContext?.availableOperations ?? [];
    return OperationMatcher.findSatisfiable(candidates, _committedParams);
  }

  /// Check if we have any fully satisfiable operation.
  bool get hasFullySatisfiableOperation {
    return findSatisfiableOperations().any((m) => m.isFullySatisfied);
  }

  /// Get the best match (first fully satisfied, or first partial if none fully satisfied).
  MatchedOperation? get bestMatch {
    final matches = findSatisfiableOperations();
    return matches.isNotEmpty ? matches.first : null;
  }
}

/// Represents a position in a tree hierarchy.
///
/// Provided by tree/outliner widgets on drop.
/// Field names match the Rust move_block operation parameters.
class TreePosition {
  final String parentId;
  final String? afterBlockId;

  const TreePosition({required this.parentId, this.afterBlockId});

  Map<String, dynamic> toMap() => {
    'parent_id': parentId,
    'after_block_id': afterBlockId,
  };

  factory TreePosition.fromMap(Map<String, dynamic> map) {
    return TreePosition(
      parentId: map['parent_id'] as String,
      afterBlockId: map['after_block_id'] as String?,
    );
  }
}
