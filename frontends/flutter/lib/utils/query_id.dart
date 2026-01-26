import 'dart:convert';
import 'package:crypto/crypto.dart';

/// Normalize PRQL query by removing whitespace.
///
/// This creates a canonical form for hashing.
/// Future: extend to remove comments as well.
String normalizePrql(String prql) {
  // Remove all whitespace (spaces, tabs, newlines)
  return prql.replaceAll(RegExp(r'\s+'), '');

  // Future enhancement: remove comments
  // return prql
  //     .replaceAll(RegExp(r'--.*'), '')  // Single-line comments
  //     .replaceAll(RegExp(r'/\*.*?\*/', dotAll: true), '')  // Multi-line comments
  //     .replaceAll(RegExp(r'\s+'), '');
}

/// Generate a stable hash ID from a PRQL query string.
///
/// Uses normalized PRQL (whitespace removed) to ensure same query
/// produces same ID regardless of formatting.
///
/// Returns a hex-encoded SHA-256 hash (64 characters).
String prqlToQueryId(String prql) {
  final normalized = normalizePrql(prql);
  final bytes = utf8.encode(normalized);
  final digest = sha256.convert(bytes);
  return digest.toString();
}

/// Validate that a queryId exists in the provider system.
///
/// Throws [StateError] if queryId is not found (fail hard).
/// This should be called before accessing query-dependent providers.
void validateQueryId(String queryId) {
  if (queryId.isEmpty) {
    throw StateError('QueryId cannot be empty');
  }
  // Additional validation can be added here if needed
  // For now, we rely on Riverpod's provider system to fail if queryId doesn't exist
}
