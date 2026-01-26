import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../render/renderable_item_ext.dart';
import '../src/rust/third_party/holon_api/render_types.dart';

/// Provider for search expansion state.
///
/// Tracks whether the search field is expanded or collapsed.
class SearchExpandedNotifier extends Notifier<bool> {
  @override
  bool build() => false;

  void setExpanded(bool expanded) {
    state = expanded;
  }
}

final searchExpandedProvider = NotifierProvider<SearchExpandedNotifier, bool>(
  SearchExpandedNotifier.new,
);

/// Provider for search text.
///
/// Stores the current search query text.
class SearchTextNotifier extends Notifier<String> {
  @override
  String build() => '';

  void setText(String text) {
    state = text;
  }
}

final searchTextProvider = NotifierProvider<SearchTextNotifier, String>(
  SearchTextNotifier.new,
);

/// Provider for password visibility state in settings.
///
/// Tracks whether the API key field should show obscured text.
class PasswordVisibilityNotifier extends Notifier<bool> {
  @override
  bool build() => true;

  void toggle() {
    state = !state;
  }
}

final passwordVisibilityProvider =
    NotifierProvider<PasswordVisibilityNotifier, bool>(
      PasswordVisibilityNotifier.new,
    );

/// Mode for the search-select overlay during drag operations.
enum SearchSelectMode {
  /// No drag in progress, overlay hidden.
  idle,

  /// Drag started, overlay visible but collapsed.
  dragActive,

  /// User dropped on overlay, search mode active.
  searchMode,
}

/// State for the search-select overlay.
class SearchSelectOverlayState {
  final SearchSelectMode mode;
  final Offset position;
  final RenderableItem? draggedItem;
  final Map<String, Map<String, dynamic>> rowCache;
  final List<RowTemplate> rowTemplates;
  final Future<void> Function(String, String, Map<String, dynamic>)?
  onOperation;

  const SearchSelectOverlayState({
    this.mode = SearchSelectMode.idle,
    this.position = Offset.zero,
    this.draggedItem,
    this.rowCache = const {},
    this.rowTemplates = const [],
    this.onOperation,
  });

  SearchSelectOverlayState copyWith({
    SearchSelectMode? mode,
    Offset? position,
    RenderableItem? draggedItem,
    Map<String, Map<String, dynamic>>? rowCache,
    List<RowTemplate>? rowTemplates,
    Future<void> Function(String, String, Map<String, dynamic>)? onOperation,
  }) {
    return SearchSelectOverlayState(
      mode: mode ?? this.mode,
      position: position ?? this.position,
      draggedItem: draggedItem ?? this.draggedItem,
      rowCache: rowCache ?? this.rowCache,
      rowTemplates: rowTemplates ?? this.rowTemplates,
      onOperation: onOperation ?? this.onOperation,
    );
  }
}

/// Provider for the search-select overlay state.
class SearchSelectOverlayNotifier extends Notifier<SearchSelectOverlayState> {
  @override
  SearchSelectOverlayState build() => const SearchSelectOverlayState();

  void showForDrag({
    required Offset position,
    required RenderableItem draggedItem,
    required Map<String, Map<String, dynamic>> rowCache,
    required List<RowTemplate> rowTemplates,
    required Future<void> Function(String, String, Map<String, dynamic>)?
    onOperation,
  }) {
    state = SearchSelectOverlayState(
      mode: SearchSelectMode.dragActive,
      position: position,
      draggedItem: draggedItem,
      rowCache: rowCache,
      rowTemplates: rowTemplates,
      onOperation: onOperation,
    );
  }

  void activateSearchMode() {
    state = state.copyWith(mode: SearchSelectMode.searchMode);
  }

  void hide() {
    state = const SearchSelectOverlayState();
  }
}

final searchSelectOverlayProvider =
    NotifierProvider<SearchSelectOverlayNotifier, SearchSelectOverlayState>(
      SearchSelectOverlayNotifier.new,
    );

// ============================================================================
// Focus State Infrastructure
// ============================================================================
// These providers model focus as a continuous spectrum (0.0→1.0) rather than
// discrete modes. As focus deepens, the UI progressively conceals peripheral
// elements. See VISION_UI.md and TODO_UI.md for design rationale.

/// Provider for the currently focused block ID.
///
/// When a block has deep focus, this contains its ID.
/// When in overview/orient mode (focusDepth near 0), this is null.
class FocusedBlockIdNotifier extends Notifier<String?> {
  @override
  String? build() => null;

  void setFocusedBlock(String? blockId) {
    state = blockId;
  }

  void clearFocus() {
    state = null;
  }
}

final focusedBlockIdProvider =
    NotifierProvider<FocusedBlockIdNotifier, String?>(
      FocusedBlockIdNotifier.new,
    );

/// Provider for the current focus depth.
///
/// Range: 0.0 (overview/orient) to 1.0 (deep flow).
/// Used to interpolate UI visibility (progressive concealment).
class FocusDepthNotifier extends Notifier<double> {
  @override
  double build() => 0.0;

  void setDepth(double depth) {
    state = depth.clamp(0.0, 1.0);
  }

  void deepen(double amount) {
    state = (state + amount).clamp(0.0, 1.0);
  }

  void release(double amount) {
    state = (state - amount).clamp(0.0, 1.0);
  }

  void reset() {
    state = 0.0;
  }
}

final focusDepthProvider = NotifierProvider<FocusDepthNotifier, double>(
  FocusDepthNotifier.new,
);

/// Provider for flow session start time.
///
/// When focusDepth crosses a threshold (e.g., 0.5), a flow session begins.
/// This tracks when it started for the timer display.
class FlowSessionStartNotifier extends Notifier<DateTime?> {
  @override
  DateTime? build() => null;

  void startSession() {
    state ??= DateTime.now();
  }

  void endSession() {
    state = null;
  }
}

final flowSessionStartProvider =
    NotifierProvider<FlowSessionStartNotifier, DateTime?>(
      FlowSessionStartNotifier.new,
    );

/// Provider for whether capture overlay is visible.
///
/// Capture is a transient overlay, not a mode.
class CaptureOverlayNotifier extends Notifier<bool> {
  @override
  bool build() => false;

  void show() {
    state = true;
  }

  void hide() {
    state = false;
  }

  void toggle() {
    state = !state;
  }
}

final captureOverlayProvider = NotifierProvider<CaptureOverlayNotifier, bool>(
  CaptureOverlayNotifier.new,
);
