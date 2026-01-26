import '../src/rust/third_party/holon_api/render_types.dart';

/// Extension methods for RenderSpec to support multi-view rendering.
extension RenderSpecExtension on RenderSpec {
  /// Get the render expression for a view.
  ///
  /// If viewName is null, uses defaultView.
  /// Falls back to root for backward compatibility with single-view queries.
  ///
  /// For multi-view queries, this accesses `views[viewName ?? defaultView].structure`.
  /// For single-view queries (views empty), this returns the `root` field.
  RenderExpr getViewStructure([String? viewName]) {
    final name = viewName ?? defaultView;
    final viewSpec = views[name];
    if (viewSpec != null) {
      return viewSpec.structure;
    }
    // Backward compat: single-view queries use root directly
    return root;
  }
}
