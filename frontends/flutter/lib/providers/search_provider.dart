import 'package:flutter_riverpod/flutter_riverpod.dart';

/// Global search query provider that can be used by any widget
/// to filter content based on search text
final searchQueryProvider = StateProvider<String>((ref) => '');

/// Helper function that can be used by widgets to check if an item matches
/// the current search query. Returns true if the search is empty or if
/// the item matches the search query (case-insensitive).
bool matchesSearch(String searchQuery, String itemText) {
  if (searchQuery.isEmpty) return true;
  return itemText.toLowerCase().contains(searchQuery.toLowerCase());
}
