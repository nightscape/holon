import 'package:flutter/foundation.dart';

import '../src/rust/third_party/holon_api/render_types.dart';
import '../src/rust/third_party/holon_api.dart';

/// Result of matching an operation against available parameters.
class MatchedOperation {
  final OperationDescriptor descriptor;
  final Map<String, dynamic> resolvedParams;
  final List<String> missingParams;

  const MatchedOperation({
    required this.descriptor,
    required this.resolvedParams,
    required this.missingParams,
  });

  bool get isFullySatisfied => missingParams.isEmpty;

  String get operationName => descriptor.name;
  String get entityName => descriptor.entityName;
}

/// Matches operations against available parameters.
///
/// Finds operations that can be (partially) satisfied with the given params,
/// using both direct matching and param mappings from [OperationDescriptor.paramMappings].
///
/// **Intent filtering**: If the available params contain "intent-carrying" params
/// (params that operations declare in param_mappings.from, like `tree_position`),
/// only operations that USE those params are considered. This prevents e.g. `delete`
/// from matching during drag-drop just because it only needs `id`.
class OperationMatcher {
  /// Find operations that can be satisfied with available params.
  ///
  /// Returns matches sorted by:
  /// 1. Fully satisfied operations first
  /// 2. Then by number of missing params (fewer = better)
  static List<MatchedOperation> findSatisfiable(
    List<OperationDescriptor> operations,
    Map<String, dynamic> availableParams,
  ) {
    // Intent filtering: if gesture-specific params are present, only match
    // operations that actually use them.
    final filteredOps = _filterByIntentParams(operations, availableParams);

    final results = <MatchedOperation>[];

    for (final op in filteredOps) {
      final match = _tryMatch(op, availableParams);
      if (match != null) {
        results.add(match);
      }
    }

    results.sort((a, b) {
      // Fully satisfied first
      if (a.isFullySatisfied && !b.isFullySatisfied) return -1;
      if (!a.isFullySatisfied && b.isFullySatisfied) return 1;
      // Then by MORE resolved params (prefer operations that use more of committed params)
      final resolvedCmp = b.resolvedParams.length.compareTo(
        a.resolvedParams.length,
      );
      if (resolvedCmp != 0) return resolvedCmp;
      // Then by fewer missing params
      return a.missingParams.length.compareTo(b.missingParams.length);
    });

    return results;
  }

  /// Find the single best matching operation (if any).
  static MatchedOperation? findBestMatch(
    List<OperationDescriptor> operations,
    Map<String, dynamic> availableParams,
  ) {
    final matches = findSatisfiable(operations, availableParams);
    return matches.isNotEmpty ? matches.first : null;
  }

  /// Find all fully satisfiable operations.
  static List<MatchedOperation> findFullySatisfiable(
    List<OperationDescriptor> operations,
    Map<String, dynamic> availableParams,
  ) {
    return findSatisfiable(
      operations,
      availableParams,
    ).where((m) => m.isFullySatisfied).toList();
  }

  /// Filter operations based on intent-carrying params.
  ///
  /// "Intent params" are params that operations declare in param_mappings.from
  /// (e.g., `tree_position`, `selected_id`). If any of these are present in
  /// availableParams, we only consider operations that actually use them.
  ///
  /// This prevents operations like `delete` (which only needs `id`) from
  /// matching during gestures that clearly indicate different intent
  /// (e.g., drag-drop provides `tree_position` → user wants to move, not delete).
  static List<OperationDescriptor> _filterByIntentParams(
    List<OperationDescriptor> operations,
    Map<String, dynamic> availableParams,
  ) {
    // 1. Collect all "intent param sources" - params that any operation maps from
    final intentParamSources = <String>{};
    for (final op in operations) {
      for (final mapping in op.paramMappings) {
        intentParamSources.add(mapping.from);
      }
    }

    // 2. Which intent params are actually present in available params?
    final presentIntentParams = intentParamSources
        .where((p) => availableParams.containsKey(p))
        .toSet();

    debugPrint('[OperationMatcher] Intent param sources: $intentParamSources');
    debugPrint(
      '[OperationMatcher] Present intent params: $presentIntentParams',
    );

    // 3. If no intent params present, return all operations (no filtering)
    if (presentIntentParams.isEmpty) {
      debugPrint(
        '[OperationMatcher] No intent params present, returning all ${operations.length} operations',
      );
      return operations;
    }

    // 4. Filter to operations that use at least one of the present intent params
    final filtered = operations.where((op) {
      final usesIntentParam = op.paramMappings.any(
        (m) => presentIntentParams.contains(m.from),
      );
      if (!usesIntentParam) {
        debugPrint(
          '[OperationMatcher] Excluding ${op.name}: does not use any intent params',
        );
      }
      return usesIntentParam;
    }).toList();

    debugPrint(
      '[OperationMatcher] Filtered to ${filtered.length} operations that use intent params',
    );
    return filtered;
  }

  static MatchedOperation? _tryMatch(
    OperationDescriptor op,
    Map<String, dynamic> available,
  ) {
    debugPrint(
      '[OperationMatcher] _tryMatch: op=${op.name}, paramMappings=${op.paramMappings.length}',
    );
    for (final m in op.paramMappings) {
      debugPrint('[OperationMatcher]   mapping: ${m.from} -> ${m.provides}');
    }

    final resolved = <String, dynamic>{};
    final missing = <String>[];

    for (final param in op.requiredParams) {
      final value = _resolveParam(param.name, op, available);
      debugPrint(
        '[OperationMatcher]   param ${param.name}: resolved=${value != null ? "YES ($value)" : "NO"}',
      );
      if (value != null) {
        resolved[param.name] = value;
      } else {
        missing.add(param.name);
      }
    }

    // Only return if we resolved at least something useful
    if (resolved.isEmpty && op.requiredParams.isNotEmpty) {
      debugPrint('[OperationMatcher]   -> SKIPPED (no params resolved)');
      return null;
    }

    debugPrint(
      '[OperationMatcher]   -> MATCHED: resolved=$resolved, missing=$missing',
    );
    return MatchedOperation(
      descriptor: op,
      resolvedParams: resolved,
      missingParams: missing,
    );
  }

  static dynamic _resolveParam(
    String paramName,
    OperationDescriptor op,
    Map<String, dynamic> available,
  ) {
    // Direct match
    if (available.containsKey(paramName)) {
      debugPrint(
        '[_resolveParam] $paramName: direct match -> ${available[paramName]}',
      );
      return available[paramName];
    }

    // Try param mappings (from Rust-defined OperationDescriptor.paramMappings)
    debugPrint(
      '[_resolveParam] $paramName: trying ${op.paramMappings.length} mappings, available keys: ${available.keys.toList()}',
    );
    for (final mapping in op.paramMappings) {
      if (!mapping.provides.contains(paramName)) {
        debugPrint(
          '[_resolveParam]   mapping ${mapping.from}->${mapping.provides}: skipped (doesn\'t provide $paramName)',
        );
        continue;
      }

      final sourceValue = available[mapping.from];
      debugPrint(
        '[_resolveParam]   mapping ${mapping.from}->${mapping.provides}: sourceValue=$sourceValue (type: ${sourceValue?.runtimeType})',
      );
      if (sourceValue != null) {
        // Extract from structured source (e.g., tree_position['parent_id'])
        if (sourceValue is Map<String, dynamic>) {
          debugPrint(
            '[_resolveParam]     sourceValue is Map, keys: ${sourceValue.keys.toList()}',
          );
          if (sourceValue.containsKey(paramName)) {
            debugPrint(
              '[_resolveParam]     -> extracted ${sourceValue[paramName]}',
            );
            return sourceValue[paramName];
          }
        }
        // Or use source directly if it's a simple value providing one thing
        if (mapping.provides.length == 1) {
          debugPrint('[_resolveParam]     -> using sourceValue directly');
          return sourceValue;
        }
      }

      // Check defaults (defaults is Map<String, Value> from Rust)
      if (mapping.defaults.containsKey(paramName)) {
        return _valueToNative(mapping.defaults[paramName]!);
      }
    }

    debugPrint('[_resolveParam] $paramName: NOT RESOLVED');
    return null;
  }

  /// Convert Rust Value to native Dart type
  static dynamic _valueToNative(Value value) {
    return value.map(
      string: (v) => v.field0,
      integer: (v) => v.field0,
      float: (v) => v.field0,
      boolean: (v) => v.field0,
      dateTime: (v) => v.field0,
      json: (v) => v.field0,
      reference: (v) => v.field0,
      array: (v) => v.field0.map(_valueToNative).toList(),
      object: (v) => v.field0.map((k, v) => MapEntry(k, _valueToNative(v))),
      null_: (_) => null,
    );
  }
}
