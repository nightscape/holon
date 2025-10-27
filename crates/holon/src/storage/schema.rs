use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntitySchema {
    pub name: String,
    pub fields: Vec<FieldSchema>,
    pub primary_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    pub field_type: FieldType,
    pub required: bool,
    pub indexed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldType {
    String,
    Integer,
    Boolean,
    DateTime,
    Json,
    Reference(String),
}

impl FieldType {
    pub fn to_sqlite_type(&self) -> &'static str {
        match self {
            FieldType::String => "TEXT",
            FieldType::Integer => "INTEGER",
            FieldType::Boolean => "INTEGER",
            FieldType::DateTime => "TEXT",
            FieldType::Json => "TEXT",
            FieldType::Reference(_) => "TEXT",
        }
    }
}
