use holon_api::Value;
use std::collections::HashMap;
use thiserror::Error;

/// StorageEntity type alias for HashMap<String, Value>
/// flutter_rust_bridge:non_opaque
pub type StorageEntity = HashMap<String, Value>;

#[derive(Debug, Clone)]
pub enum Filter {
    Eq(String, Value),
    In(String, Vec<Value>),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    IsNull(String),
    IsNotNull(String),
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Entity not found: {entity} with id {id}")]
    NotFound { entity: String, id: String },

    #[error("Schema error: {0}")]
    SchemaError(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Backend error: {0}")]
    BackendError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Database error: {0}")]
    DatabaseError(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;
