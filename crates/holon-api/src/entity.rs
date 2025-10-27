//! Entity types and traits for the Entity derive macro.
//!
//! This module provides the core types needed by `#[derive(Entity)]`:
//! - `DynamicEntity`: Runtime entity representation
//! - `Schema`, `FieldSchema`: DDL generation types
//! - `HasSchema`: Trait for entity type introspection
//! - `EntitySchema`, `FieldType`: Schema metadata types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::Value;

/// Result type for entity operations
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// =============================================================================
// DynamicEntity - Runtime entity representation
// =============================================================================

/// A dynamic entity with runtime-determined fields.
///
/// This provides a type-erased representation of any entity,
/// useful for generic storage and serialization.
///
/// flutter_rust_bridge:ignore
#[derive(Debug, Clone, PartialEq)]
pub struct DynamicEntity {
    pub type_name: String,
    pub fields: HashMap<String, Value>,
}

impl DynamicEntity {
    pub fn new(type_name: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
            fields: HashMap::new(),
        }
    }

    pub fn with_field(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.insert(name.into(), value.into());
        self
    }

    pub fn set(&mut self, name: impl Into<String>, value: impl Into<Value>) {
        self.fields.insert(name.into(), value.into());
    }

    /// flutter_rust_bridge:ignore
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }

    /// flutter_rust_bridge:ignore
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Value> {
        self.fields.get_mut(name)
    }

    pub fn remove(&mut self, name: &str) -> Option<Value> {
        self.fields.remove(name)
    }

    pub fn has_field(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    pub fn get_string(&self, name: &str) -> Option<String> {
        self.get(name).and_then(|v| v.as_string().map(String::from))
    }

    pub fn get_i64(&self, name: &str) -> Option<i64> {
        self.get(name).and_then(|v| v.as_i64())
    }

    pub fn get_bool(&self, name: &str) -> Option<bool> {
        self.get(name).and_then(|v| v.as_bool())
    }

    pub fn get_f64(&self, name: &str) -> Option<f64> {
        self.get(name).and_then(|v| v.as_f64())
    }
}

impl Default for DynamicEntity {
    fn default() -> Self {
        Self::new("unknown")
    }
}

// =============================================================================
// Schema types - For DDL generation
// =============================================================================

/// Schema for a database table, used for DDL generation.
#[derive(Debug, Clone)]
pub struct Schema {
    pub table_name: String,
    pub fields: Vec<FieldSchema>,
}

impl Schema {
    pub fn new(table_name: impl Into<String>, fields: Vec<FieldSchema>) -> Self {
        Self {
            table_name: table_name.into(),
            fields,
        }
    }

    /// Create a minimal schema from just a table name (no field info, no JSONB)
    pub fn from_table_name(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            fields: Vec::new(),
        }
    }

    /// Generate CREATE TABLE SQL statement.
    ///
    /// Panics if the schema has no fields (e.g. created via `from_table_name()`).
    /// Use `from_table_name()` only for queries/inserts, not for DDL.
    pub fn to_create_table_sql(&self) -> String {
        assert!(
            !self.fields.is_empty(),
            "Cannot generate CREATE TABLE for schema '{}' with no fields. \
             Schema::from_table_name() creates field-less schemas intended for \
             queries/inserts, not DDL. Use Schema::new() with fields instead.",
            self.table_name
        );
        let mut columns = Vec::new();

        for field in &self.fields {
            let mut col = format!("{} {}", field.name, field.sql_type);

            if field.primary_key {
                col.push_str(" PRIMARY KEY");
            }

            if !field.nullable {
                col.push_str(" NOT NULL");
            }

            columns.push(col);
        }

        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)",
            self.table_name,
            columns.join(",\n  ")
        )
    }

    /// Generate CREATE INDEX SQL statements for indexed fields
    pub fn to_index_sql(&self) -> Vec<String> {
        self.fields
            .iter()
            .filter(|f| f.indexed && !f.primary_key)
            .map(|f| {
                format!(
                    "CREATE INDEX IF NOT EXISTS idx_{}_{} ON {} ({})",
                    self.table_name, f.name, self.table_name, f.name
                )
            })
            .collect()
    }

    /// Check if a field is marked as JSONB
    pub fn field_is_jsonb(&self, field_name: &str) -> bool {
        self.fields
            .iter()
            .find(|f| f.name == field_name)
            .map(|f| f.is_jsonb)
            .unwrap_or(false)
    }
}

/// Schema for a single field in a table.
#[derive(Debug, Clone)]
pub struct FieldSchema {
    pub name: String,
    pub sql_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub indexed: bool,
    pub is_jsonb: bool,
}

impl FieldSchema {
    pub fn new(name: impl Into<String>, sql_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sql_type: sql_type.into(),
            nullable: false,
            primary_key: false,
            indexed: false,
            is_jsonb: false,
        }
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self
    }

    pub fn indexed(mut self) -> Self {
        self.indexed = true;
        self
    }

    pub fn jsonb(mut self) -> Self {
        self.is_jsonb = true;
        self
    }
}

// =============================================================================
// HasSchema trait - For entity introspection
// =============================================================================

/// Trait for types that have a database schema.
///
/// Implemented by `#[derive(Entity)]` to provide schema introspection
/// and conversion to/from `DynamicEntity`.
/// flutter_rust_bridge:ignore
pub trait HasSchema {
    /// Get the schema for this entity type
    fn schema() -> Schema;

    /// Convert this entity to a dynamic representation
    fn to_entity(&self) -> DynamicEntity;

    /// Create an entity from a dynamic representation
    fn from_entity(entity: DynamicEntity) -> Result<Self>
    where
        Self: Sized;
}

// =============================================================================
// EntitySchema types - Schema metadata for macro
// =============================================================================

/// Complete schema for an entity type.
///
/// Used by the Entity macro to store metadata about the entity.
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntitySchema {
    pub name: String,
    pub fields: Vec<EntityFieldSchema>,
    pub primary_key: String,
}

/// Schema for a field in an entity.
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityFieldSchema {
    pub name: String,
    pub field_type: FieldType,
    pub required: bool,
    pub indexed: bool,
}

/// Type of a field in an entity schema.
/// flutter_rust_bridge:non_opaque
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
    /// Convert to SQLite type string
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

// =============================================================================
// StorageEntity type alias
// =============================================================================

/// Type alias for entity storage as HashMap
pub type StorageEntity = HashMap<String, Value>;
