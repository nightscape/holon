use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod block;
pub mod entity;
pub mod render_types;
pub mod streaming;

// Re-export block types
pub use block::{
    Block, BlockContent, BlockMetadata, BlockResult, BlockWithDepth, ResultOutput, SourceBlock,
    NO_PARENT_ID, ROOT_PARENT_ID,
};

// Re-export entity types (for Entity derive macro)
pub use entity::{
    DynamicEntity, EntityFieldSchema, EntitySchema, FieldSchema, FieldType, HasSchema, Schema,
    StorageEntity,
};

// Re-export render types
pub use render_types::{
    Arg, BinaryOperator, Operation, OperationDescriptor, OperationParam, OperationWiring,
    ParamMapping, PreconditionChecker, RenderExpr, RenderSpec, RenderableItem, RowTemplate,
    TypeHint,
};

// Re-export streaming types
pub use streaming::{
    Batch, BatchMapChange, BatchMapChangeWithMetadata, BatchMetadata, BatchTraceContext,
    BatchWithMetadata, BlockChange, Change, ChangeOrigin, MapChange, StreamPosition,
    SyncTokenUpdate, WithMetadata, CHANGE_ORIGIN_COLUMN, CURRENT_TRACE_CONTEXT,
};

/// flutter_rust_bridge:non_opaque
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Number {
    Int(i64),
    Float(f64),
}

/// Value type for flutter_rust_bridge compatibility
///
/// This type is used in query-render and re-exported by holon
/// to ensure type consistency across the codebase.
///
/// flutter_rust_bridge:non_opaque
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    // DateTime variant: stored as RFC3339 string for flutter_rust_bridge compatibility
    // Use as_datetime() to get the parsed chrono::DateTime
    DateTime(String),
    // Json variant: stored as String for flutter_rust_bridge compatibility
    // Use as_json_value() to get the parsed serde_json::Value
    Json(String),
    Reference(String),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
    Null,
}

impl Value {
    /// Get the serde_json::Value if this is a Json variant
    ///
    /// flutter_rust_bridge:ignore
    pub fn as_json_value(&self) -> Option<serde_json::Value> {
        match self {
            Value::Json(s) => serde_json::from_str(s).ok(),
            _ => None,
        }
    }

    /// Create a Value from a serde_json::Value
    ///
    /// flutter_rust_bridge:ignore
    pub fn from_json_value(v: serde_json::Value) -> Self {
        // Try to convert to a more specific variant first
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Boolean(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::Json(
                        serde_json::to_string(&serde_json::Value::Number(n)).unwrap_or_default(),
                    )
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(arr) => {
                Value::Array(arr.into_iter().map(Value::from_json_value).collect())
            }
            serde_json::Value::Object(obj) => Value::Object(
                obj.into_iter()
                    .map(|(k, v)| (k, Value::from_json_value(v)))
                    .collect(),
            ),
        }
    }

    /// Get string value, returning None if not a string
    ///
    /// flutter_rust_bridge:ignore
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get string value as owned String, returning None if not a string
    pub fn as_string_owned(&self) -> Option<String> {
        match self {
            Value::String(s) => Some(s.clone()),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Get datetime value as RFC3339 string
    ///
    /// flutter_rust_bridge:ignore
    pub fn as_datetime_string(&self) -> Option<&str> {
        match self {
            Value::DateTime(s) => Some(s),
            _ => None,
        }
    }

    /// Get datetime value as parsed chrono::DateTime
    ///
    /// flutter_rust_bridge:ignore
    pub fn as_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        match self {
            Value::DateTime(s) => chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc)),
            _ => None,
        }
    }

    /// Create a Value from a chrono::DateTime
    pub fn from_datetime(dt: chrono::DateTime<chrono::Utc>) -> Self {
        Value::DateTime(dt.to_rfc3339())
    }

    /// Get array value
    ///
    /// flutter_rust_bridge:ignore
    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Get object value
    ///
    /// flutter_rust_bridge:ignore
    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Value::Object(obj) => Some(obj),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_json_str(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Boolean(b)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<u32> for Value {
    fn from(u: u32) -> Self {
        Value::Integer(u as i64)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl<T> From<Vec<T>> for Value
where
    T: Into<Value>,
{
    fn from(v: Vec<T>) -> Self {
        Value::Array(v.into_iter().map(|x| x.into()).collect())
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

impl From<HashMap<String, Value>> for Value {
    fn from(map: HashMap<String, Value>) -> Self {
        Value::Object(map)
    }
}

impl TryFrom<Value> for bool {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Boolean(b) => Ok(b),
            Value::Integer(i) => Ok(i != 0),
            _ => Err("Value is not a boolean or integer".into()),
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        value
            .as_i64()
            .ok_or_else(|| "Value is not an integer".into())
    }
}

impl TryFrom<Value> for i32 {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        value
            .as_i64()
            .and_then(|i| i.try_into().ok())
            .ok_or_else(|| "Value is not a valid i32".into())
    }
}

impl TryFrom<Value> for u32 {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        value
            .as_i64()
            .and_then(|i| i.try_into().ok())
            .ok_or_else(|| "Value is not a valid u32".into())
    }
}

impl TryFrom<Value> for f64 {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        value.as_f64().ok_or_else(|| "Value is not a float".into())
    }
}

impl TryFrom<Value> for String {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(s) => Ok(s),
            _ => Err("Value is not a string".into()),
        }
    }
}

impl<T> TryFrom<Value> for Option<T>
where
    T: TryFrom<Value, Error = Box<dyn std::error::Error + Send + Sync>>,
{
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if value.is_null() {
            Ok(None)
        } else {
            T::try_from(value).map(Some)
        }
    }
}

impl<T> TryFrom<Value> for Vec<T>
where
    T: TryFrom<Value, Error = Box<dyn std::error::Error + Send + Sync>>,
{
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Array(arr) => arr.into_iter().map(T::try_from).collect(),
            _ => Err("Value is not an array".into()),
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        Value::from_json_value(v)
    }
}

impl From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::String(s) => serde_json::Value::String(s),
            Value::Integer(i) => serde_json::Value::Number(serde_json::Number::from(i)),
            Value::Float(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::Boolean(b) => serde_json::Value::Bool(b),
            Value::DateTime(s) => serde_json::Value::String(s.clone()),
            Value::Json(s) => serde_json::from_str(&s).unwrap_or(serde_json::Value::Null),
            Value::Reference(r) => serde_json::Value::String(r),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.into_iter().map(Into::into).collect())
            }
            Value::Object(obj) => {
                serde_json::Value::Object(obj.into_iter().map(|(k, v)| (k, v.into())).collect())
            }
            Value::Null => serde_json::Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_accessors() {
        let v = Value::Boolean(true);
        assert_eq!(v.as_bool(), Some(true));
        assert_eq!(v.as_i64(), None);

        let v = Value::Integer(42);
        assert_eq!(v.as_i64(), Some(42));
        assert_eq!(v.as_f64(), Some(42.0));

        let v = Value::String("hello".to_string());
        assert_eq!(v.as_string(), Some("hello"));

        let v = Value::Null;
        assert!(v.is_null());
    }

    #[test]
    fn test_value_from() {
        let v: Value = true.into();
        assert_eq!(v, Value::Boolean(true));

        let v: Value = 42i64.into();
        assert_eq!(v, Value::Integer(42));

        let v: Value = "test".into();
        assert_eq!(v, Value::String("test".to_string()));

        let v: Value = None::<i64>.into();
        assert_eq!(v, Value::Null);

        let v: Value = Some(42).into();
        assert_eq!(v, Value::Integer(42));
    }

    #[test]
    fn test_value_json() {
        let v = Value::Object(
            vec![
                ("name".to_string(), Value::String("test".to_string())),
                ("count".to_string(), Value::Integer(5)),
            ]
            .into_iter()
            .collect(),
        );

        let json = v.to_json_string();
        let parsed = Value::from_json_str(&json).unwrap();
        assert_eq!(v, parsed);
    }

    #[test]
    fn test_value_array() {
        let arr = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
        let v = Value::Array(arr.clone());
        assert_eq!(v.as_array(), Some(&arr));
    }
}

/// Structured error types for API operations.
///
/// These errors are designed to cross FFI boundaries (e.g., Rust to Dart)
/// and provide type-safe error handling in frontends.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum ApiError {
    #[error("Block not found: {id}")]
    BlockNotFound { id: String },

    #[error("Document not found: {doc_id}")]
    DocumentNotFound { doc_id: String },

    #[error("Cyclic move detected: cannot move block {id} to descendant {target_parent}")]
    CyclicMove { id: String, target_parent: String },

    #[error("Invalid operation: {message}")]
    InvalidOperation { message: String },

    #[error("Network error: {message}")]
    NetworkError { message: String },

    #[error("Internal error: {message}")]
    InternalError { message: String },
}
