use holon_api::Value;
use serde_json;

/// Convert a `Value` to a SQL literal string suitable for embedding in raw SQL.
///
/// Array and Object types are serialized as JSON strings.
pub fn value_to_sql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) | Value::DateTime(s) | Value::Json(s) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        Value::Array(_) | Value::Object(_) => {
            let json: serde_json::Value = value.clone().into();
            let s = serde_json::to_string(&json).expect("Value→JSON serialization cannot fail");
            format!("'{}'", s.replace('\'', "''"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_escapes_quotes() {
        assert_eq!(
            value_to_sql_literal(&Value::String("it's".into())),
            "'it''s'"
        );
    }

    #[test]
    fn test_null() {
        assert_eq!(value_to_sql_literal(&Value::Null), "NULL");
    }

    #[test]
    fn test_boolean() {
        assert_eq!(value_to_sql_literal(&Value::Boolean(true)), "1");
        assert_eq!(value_to_sql_literal(&Value::Boolean(false)), "0");
    }

    #[test]
    fn test_integer() {
        assert_eq!(value_to_sql_literal(&Value::Integer(42)), "42");
    }

    #[test]
    fn test_float() {
        assert_eq!(value_to_sql_literal(&Value::Float(3.14)), "3.14");
    }

    #[test]
    fn test_array() {
        let val = Value::Array(vec![Value::Integer(1), Value::String("two".into())]);
        assert_eq!(value_to_sql_literal(&val), "'[1,\"two\"]'");
    }

    #[test]
    fn test_object() {
        use std::collections::HashMap;
        let val = Value::Object(HashMap::from([("key".into(), Value::String("val".into()))]));
        assert_eq!(value_to_sql_literal(&val), "'{\"key\":\"val\"}'");
    }
}
