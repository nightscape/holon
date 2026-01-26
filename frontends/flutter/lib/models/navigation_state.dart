/// Navigation state for document browsing.
///
/// Tracks the currently viewed document and navigation history
/// to support back button navigation.
class NavigationState {
  /// Currently viewed document ID.
  ///
  /// When null, the home screen (index document layout) is displayed.
  /// When set, shows the specified document.
  /// The document ID is a Loro document identifier (implementation detail).
  final String? currentDocumentId;

  /// Navigation history for back button support.
  ///
  /// Each entry is a document ID that was previously viewed.
  /// The most recent entry is at the end of the list.
  final List<String> history;

  const NavigationState({
    this.currentDocumentId,
    this.history = const [],
  });

  /// Returns true if back navigation is possible.
  bool get canGoBack => history.isNotEmpty || currentDocumentId != null;

  /// Returns true if currently viewing the home screen.
  bool get isAtHome => currentDocumentId == null;

  /// Returns true if currently viewing a document.
  bool get isViewingDocument => currentDocumentId != null;

  /// Navigate to a new document.
  ///
  /// Pushes the current document (if any) onto the history stack
  /// and navigates to the new document.
  NavigationState navigateTo(String documentId) {
    return NavigationState(
      currentDocumentId: documentId,
      history: currentDocumentId != null
          ? [...history, currentDocumentId!]
          : history,
    );
  }

  /// Go back to the previous document or home.
  ///
  /// Pops the most recent document from the history stack.
  /// If history is empty, returns to home.
  NavigationState goBack() {
    if (history.isEmpty) {
      return const NavigationState();
    }
    return NavigationState(
      currentDocumentId: history.last,
      history: history.sublist(0, history.length - 1),
    );
  }

  /// Navigate directly to the home screen.
  ///
  /// Clears the entire navigation history.
  NavigationState goHome() {
    return const NavigationState();
  }

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;
    if (other is! NavigationState) return false;
    if (currentDocumentId != other.currentDocumentId) return false;
    if (history.length != other.history.length) return false;
    for (var i = 0; i < history.length; i++) {
      if (history[i] != other.history[i]) return false;
    }
    return true;
  }

  @override
  int get hashCode => Object.hash(currentDocumentId, Object.hashAll(history));

  @override
  String toString() =>
      'NavigationState(current: $currentDocumentId, history: ${history.length})';
}
