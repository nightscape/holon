import 'package:riverpod_annotation/riverpod_annotation.dart';
import 'query_providers.dart';

part 'view_selector_provider.g.dart';

/// Provider for selecting and tracking the current view for a query.
/// 
/// Each query can have multiple views defined in its RenderSpec.
/// This provider manages which view is currently selected.
/// 
/// The queryId is a normalized PRQL hash (see query_id.dart).
@riverpod
class ViewSelector extends _$ViewSelector {
  @override
  String build(String queryId) {
    final spec = ref.watch(renderSpecProvider(queryId));
    return spec.defaultView;
  }

  /// Select a different view
  void selectView(String viewName) {
    state = viewName;
  }

  /// Get list of available views for this query
  List<String> get availableViews {
    final spec = ref.read(renderSpecProvider(queryId));
    return spec.views.keys.toList();
  }
}
