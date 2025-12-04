import '../src/rust/third_party/holon_api.dart'
    show
        Value,
        Value_String,
        Value_Integer,
        Value_Float,
        Value_Boolean,
        Value_DateTime,
        Value_Json,
        Value_Reference,
        Value_Array,
        Value_Object,
        Value_Null;
import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart'
    show PlatformInt64Util;

/// Convert Map<String, Value> to Map<String, dynamic> for widget consumption
Map<String, dynamic> valueMapToDynamic(Map<String, Value> valueMap) {
  return valueMap.map((key, value) => MapEntry(key, valueToDynamic(value)));
}

/// Convert a single Value to dynamic Dart type
dynamic valueToDynamic(Value value) {
  // Use runtime type checking since freezed when() may not be available
  if (value is Value_String) {
    return value.field0;
  } else if (value is Value_Integer) {
    return value.field0.toInt();
  } else if (value is Value_Float) {
    return value.field0;
  } else if (value is Value_Boolean) {
    return value.field0;
  } else if (value is Value_DateTime) {
    return value.field0;
  } else if (value is Value_Json) {
    return value.field0;
  } else if (value is Value_Reference) {
    return value.field0;
  } else if (value is Value_Array) {
    return value.field0.map(valueToDynamic).toList();
  } else if (value is Value_Object) {
    return value.field0.map(
      (key, value) => MapEntry(key, valueToDynamic(value)),
    );
  } else if (value is Value_Null) {
    return null;
  }
  throw ArgumentError('Unknown Value type: ${value.runtimeType}');
}

/// Convert Map<String, dynamic> to Map<String, Value> for Rust FFI
Map<String, Value> dynamicToValueMap(Map<String, dynamic> dynamicMap) {
  return dynamicMap.map((key, value) => MapEntry(key, dynamicToValue(value)));
}

/// Convert a dynamic Dart value to Rust Value type
Value dynamicToValue(dynamic value) {
  if (value == null) {
    return const Value_Null();
  } else if (value is String) {
    return Value_String(value);
  } else if (value is int) {
    return Value.integer(PlatformInt64Util.from(value));
  } else if (value is double) {
    return Value_Float(value);
  } else if (value is bool) {
    return Value_Boolean(value);
  } else if (value is List) {
    return Value_Array(value.map(dynamicToValue).toList());
  } else if (value is Map) {
    return Value_Object(
      value.map((key, val) => MapEntry(key.toString(), dynamicToValue(val))),
    );
  }
  // Fallback: convert to string
  return Value_String(value.toString());
}
