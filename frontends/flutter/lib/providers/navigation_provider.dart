import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../models/navigation_state.dart';

/// Notifier for document navigation state.
///
/// Manages the currently viewed document and navigation history,
/// enabling back button functionality and home navigation.
class NavigationNotifier extends Notifier<NavigationState> {
  @override
  NavigationState build() => const NavigationState();

  /// Navigate to a document by path.
  ///
  /// The current document (if any) is pushed onto the history stack.
  void navigateTo(String documentPath) {
    state = state.navigateTo(documentPath);
  }

  /// Go back to the previous document.
  ///
  /// If history is empty, navigates to home.
  void goBack() {
    state = state.goBack();
  }

  /// Navigate directly to the home screen.
  ///
  /// Clears all navigation history.
  void goHome() {
    state = state.goHome();
  }
}

/// Provider for navigation state.
///
/// Watch this provider to react to navigation changes.
/// Use the notifier methods to trigger navigation.
///
/// Example:
/// ```dart
/// // Watch navigation state
/// final navigation = ref.watch(navigationProvider);
/// if (navigation.isAtHome) {
///   // Show home layout
/// } else {
///   // Show document at navigation.currentDocumentPath
/// }
///
/// // Navigate to a document
/// ref.read(navigationProvider.notifier).navigateTo('/path/to/doc.org');
///
/// // Go back
/// ref.read(navigationProvider.notifier).goBack();
/// ```
final navigationProvider = NotifierProvider<NavigationNotifier, NavigationState>(
  NavigationNotifier.new,
);
