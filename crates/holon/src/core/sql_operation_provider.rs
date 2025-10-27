//! SQL-based operation provider for blocks
//!
//! Provides direct SQL access to block operations, bypassing the Loro CRDT layer.
//! Used when OrgMode is enabled but Loro is disabled, or by any component that
//! needs to write blocks directly to the database.

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::core::datasource::{OperationProvider, OperationResult, Result};
use crate::storage::sql_utils::value_to_sql_literal;
use crate::storage::turso::TursoBackend;
use crate::storage::types::StorageEntity;
use holon_api::{OperationDescriptor, OperationParam, TypeHint, Value};

/// SQL-based operation provider that writes directly to a Turso table.
///
/// This is the non-Loro path for block operations. It executes SQL INSERT/UPDATE/DELETE
/// directly against the database, relying on Turso's CDC for change propagation.
pub struct SqlOperationProvider {
    backend: Arc<RwLock<TursoBackend>>,
    table_name: String,
    entity_name: String,
    entity_short_name: String,
}

impl SqlOperationProvider {
    pub fn new(
        backend: Arc<RwLock<TursoBackend>>,
        table_name: String,
        entity_name: String,
        entity_short_name: String,
    ) -> Self {
        Self {
            backend,
            table_name,
            entity_name,
            entity_short_name,
        }
    }

    fn value_to_sql(value: &Value) -> String {
        value_to_sql_literal(value)
    }
}

#[async_trait]
impl OperationProvider for SqlOperationProvider {
    fn operations(&self) -> Vec<OperationDescriptor> {
        vec![
            OperationDescriptor {
                entity_name: self.entity_name.clone(),
                entity_short_name: self.entity_short_name.clone(),
                id_column: "id".to_string(),
                name: "set_field".to_string(),
                display_name: "Set Field".to_string(),
                description: format!("Set a field on {}", self.entity_short_name),
                required_params: vec![
                    OperationParam {
                        name: "id".to_string(),
                        type_hint: TypeHint::String,
                        description: "Entity ID".to_string(),
                    },
                    OperationParam {
                        name: "field".to_string(),
                        type_hint: TypeHint::String,
                        description: "Field name".to_string(),
                    },
                    OperationParam {
                        name: "value".to_string(),
                        type_hint: TypeHint::String,
                        description: "Field value".to_string(),
                    },
                ],
                affected_fields: vec![],
                param_mappings: vec![],
                precondition: None,
            },
            OperationDescriptor {
                entity_name: self.entity_name.clone(),
                entity_short_name: self.entity_short_name.clone(),
                id_column: "id".to_string(),
                name: "create".to_string(),
                display_name: "Create".to_string(),
                description: format!("Create a new {}", self.entity_short_name),
                required_params: vec![],
                affected_fields: vec![],
                param_mappings: vec![],
                precondition: None,
            },
            OperationDescriptor {
                entity_name: self.entity_name.clone(),
                entity_short_name: self.entity_short_name.clone(),
                id_column: "id".to_string(),
                name: "delete".to_string(),
                display_name: "Delete".to_string(),
                description: format!("Delete {}", self.entity_short_name),
                required_params: vec![OperationParam {
                    name: "id".to_string(),
                    type_hint: TypeHint::String,
                    description: "Entity ID".to_string(),
                }],
                affected_fields: vec![],
                param_mappings: vec![],
                precondition: None,
            },
        ]
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<OperationResult> {
        assert_eq!(
            entity_name, self.entity_name,
            "SqlOperationProvider: expected entity '{}', got '{}'",
            self.entity_name, entity_name
        );

        match op_name {
            "set_field" => {
                let id = params
                    .get("id")
                    .and_then(|v| v.as_string())
                    .ok_or_else(|| "Missing 'id' parameter".to_string())?;
                let field = params
                    .get("field")
                    .and_then(|v| v.as_string())
                    .ok_or_else(|| "Missing 'field' parameter".to_string())?;
                let value = params
                    .get("value")
                    .ok_or_else(|| "Missing 'value' parameter".to_string())?;

                let sql_value = Self::value_to_sql(value);

                let backend = self.backend.write().await;
                let conn = backend
                    .get_connection()
                    .map_err(|e| format!("Failed to get connection: {}", e))?;

                let sql = format!(
                    "UPDATE {} SET {} = {} WHERE id = '{}'",
                    self.table_name,
                    field,
                    sql_value,
                    id.replace('\'', "''")
                );
                conn.execute(&sql, ())
                    .await
                    .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                Ok(OperationResult::irreversible(Vec::new()))
            }
            "create" => {
                let backend = self.backend.write().await;
                let conn = backend
                    .get_connection()
                    .map_err(|e| format!("Failed to get connection: {}", e))?;

                let mut columns = Vec::new();
                let mut values = Vec::new();
                for (key, value) in params.iter() {
                    columns.push(key.clone());
                    values.push(Self::value_to_sql(value));
                }

                let sql = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    self.table_name,
                    columns.join(", "),
                    values.join(", ")
                );
                conn.execute(&sql, ())
                    .await
                    .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                Ok(OperationResult::irreversible(Vec::new()))
            }
            "delete" => {
                let id = params
                    .get("id")
                    .and_then(|v| v.as_string())
                    .ok_or_else(|| "Missing 'id' parameter".to_string())?;

                let backend = self.backend.write().await;
                let conn = backend
                    .get_connection()
                    .map_err(|e| format!("Failed to get connection: {}", e))?;

                let sql = format!(
                    "DELETE FROM {} WHERE id = '{}'",
                    self.table_name,
                    id.replace('\'', "''")
                );
                conn.execute(&sql, ())
                    .await
                    .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                Ok(OperationResult::irreversible(Vec::new()))
            }
            _ => Err(format!("Unknown operation: {}", op_name).into()),
        }
    }
}
