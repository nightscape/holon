//! End-to-end tests for BackendEngine using the E2E test scaffold
//!
//! These tests demonstrate the full workflow:
//! - PRQL query compilation and execution
//! - CDC stream watching
//! - Operation execution
//! - Stream change verification

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use async_trait::async_trait;
use holon::api::backend_engine::BackendEngine;
use holon::core::datasource::{OperationProvider, Result as DatasourceResult};
#[cfg(test)]
use holon::di::test_helpers::TestProviderModule;
use holon::storage::turso::{ChangeData, TursoBackend};
use holon::storage::types::StorageEntity;
use holon::testing::e2e_test_helpers::{
    assert_change_sequence, assert_change_type, wait_for_change, ChangeType, E2ETestContext,
};
use holon_api::{Operation, OperationDescriptor, Value};

/// Simple SQL-based operation provider for testing
struct SqlOperationProvider {
    backend: Arc<RwLock<TursoBackend>>,
    table_name: String,
    entity_name: String,
    entity_short_name: String,
}

impl SqlOperationProvider {
    fn new(backend: Arc<RwLock<TursoBackend>>, table_name: String, entity_name: String) -> Self {
        let short_name = entity_name
            .strip_prefix("test_")
            .unwrap_or(&entity_name)
            .trim_end_matches('s')
            .to_string();
        Self {
            backend,
            table_name,
            entity_name,
            entity_short_name: short_name,
        }
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
                    holon_api::OperationParam {
                        name: "id".to_string(),
                        type_hint: holon_api::TypeHint::String,
                        description: "Entity ID".to_string(),
                    },
                    holon_api::OperationParam {
                        name: "field".to_string(),
                        type_hint: holon_api::TypeHint::String,
                        description: "Field name".to_string(),
                    },
                    holon_api::OperationParam {
                        name: "value".to_string(),
                        type_hint: holon_api::TypeHint::String, // Value can be any type, but use String as fallback
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
                required_params: vec![holon_api::OperationParam {
                    name: "id".to_string(),
                    type_hint: holon_api::TypeHint::String,
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
    ) -> DatasourceResult<Option<Operation>> {
        if entity_name != self.entity_name {
            return Err(format!(
                "Expected entity_name '{}', got '{}'",
                self.entity_name, entity_name
            )
            .into());
        }

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

                let backend = self.backend.write().await;
                let conn = backend
                    .get_connection()
                    .map_err(|e| format!("Failed to get connection: {}", e))?;

                let sql_value = match value {
                    Value::String(s) => format!("'{}'", s.replace("'", "''")),
                    Value::Integer(i) => i.to_string(),
                    Value::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
                    Value::Null => "NULL".to_string(),
                    Value::DateTime(s) => format!("'{}'", s.replace("'", "''")),
                    Value::Json(s) => format!("'{}'", s.replace("'", "''")),
                    Value::Reference(r) => format!("'{}'", r.replace("'", "''")),
                    Value::Float(f) => f.to_string(),
                    Value::Array(_) | Value::Object(_) => {
                        return Err("Complex types not supported in test".into());
                    }
                };

                let sql = format!(
                    "UPDATE {} SET {} = {} WHERE id = '{}'",
                    self.table_name,
                    field,
                    sql_value,
                    id.replace("'", "''")
                );
                conn.execute(&sql, ())
                    .await
                    .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                Ok(None) // No inverse operation for simple test provider
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
                    let sql_value = match value {
                        Value::String(s) => format!("'{}'", s.replace("'", "''")),
                        Value::Integer(i) => i.to_string(),
                        Value::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
                        Value::Null => "NULL".to_string(),
                        Value::DateTime(s) => format!("'{}'", s.replace("'", "''")),
                        Value::Json(s) => format!("'{}'", s.replace("'", "''")),
                        Value::Reference(r) => format!("'{}'", r.replace("'", "''")),
                        Value::Float(f) => f.to_string(),
                        Value::Array(_) | Value::Object(_) => {
                            return Err("Complex types not supported in test".into());
                        }
                    };
                    values.push(sql_value);
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
                Ok(None) // No inverse operation for simple test provider
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
                    id.replace("'", "''")
                );
                conn.execute(&sql, ())
                    .await
                    .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                Ok(None) // No inverse operation for simple test provider
            }
            _ => Err(format!("Unknown operation: {}", op_name).into()),
        }
    }
}

/// Helper to set up a test table with initial data
async fn setup_test_table(ctx: &E2ETestContext, table_name: &str) -> Result<()> {
    let engine = ctx.engine();
    let backend = engine.get_backend();
    let backend_guard = backend.write().await;
    let conn = backend_guard
        .get_connection()
        .map_err(|e| anyhow::anyhow!("Failed to get connection: {}", e))?;

    // Create table
    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (id TEXT PRIMARY KEY, content TEXT, completed INTEGER DEFAULT 0)",
        table_name
    );
    conn.execute(&create_sql, ())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create table: {}", e))?;

    // Insert initial data
    let insert_sql = format!(
        "INSERT OR IGNORE INTO {} (id, content, completed) VALUES ('block-1', 'Initial content', 0)",
        table_name
    );
    conn.execute(&insert_sql, ())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to insert test data: {}", e))?;

    Ok(())
}

#[tokio::test]
async fn test_basic_query_execution() -> Result<()> {
    let ctx = E2ETestContext::new().await?;
    setup_test_table(&ctx, "blocks").await?;

    let prql = r#"
        from blocks
        select {id, content, completed}
        render (list item_template:(row (text content:this.content)))
    "#;

    let (render_spec, results) = ctx.query(prql.to_string(), HashMap::new()).await?;

    // Verify we got results
    assert!(!results.is_empty(), "Should have at least one result");
    assert_eq!(results[0].get("id").unwrap().as_string(), Some("block-1"));

    // Verify render spec has the expected structure
    match render_spec.root {
        query_render::RenderExpr::FunctionCall { name, .. } => {
            assert_eq!(name, "list");
        }
        _ => panic!("Expected list function call in render spec"),
    }

    Ok(())
}

#[tokio::test]
async fn test_query_and_watch_stream() -> Result<()> {
    // Create context with provider factory
    // The factory receives the correct backend at creation time
    let ctx = E2ETestContext::with_providers(|module| {
        module.with_operation_provider_factory(|backend| {
            Arc::new(SqlOperationProvider::new(
                backend,
                "blocks".to_string(),
                "blocks".to_string(),
            ))
        })
    })
    .await?;

    setup_test_table(&ctx, "blocks").await?;

    let prql = r#"
        from blocks
        select {id, content, completed}
        render (list item_template:(row (text content:this.content)))
    "#;

    let (_render_spec, initial_data, stream) = ctx
        .query_and_watch(prql.to_string(), HashMap::new())
        .await?;

    // Verify initial data
    assert!(!initial_data.is_empty());
    assert_eq!(
        initial_data[0].get("id").unwrap().as_string(),
        Some("block-1")
    );

    // Execute an operation that should trigger a stream update
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::String("block-1".to_string()));
    params.insert("field".to_string(), Value::String("content".to_string()));
    params.insert(
        "value".to_string(),
        Value::String("Updated content".to_string()),
    );

    ctx.execute_op("blocks", "set_field", params).await?;

    // Wait for the update change
    let change = wait_for_change(
        stream,
        Duration::from_secs(5),
        ChangeType::Updated,
        Some("block-1"),
    )
    .await?;

    // Verify the change
    match change.change {
        ChangeData::Updated { data, .. } => {
            assert_eq!(
                data.get("content").unwrap().as_string(),
                Some("Updated content")
            );
        }
        _ => panic!("Expected Updated change"),
    }

    Ok(())
}

#[tokio::test]
async fn test_operation_triggers_stream_update() -> Result<()> {
    let ctx = E2ETestContext::with_providers(|module| {
        module.with_operation_provider_factory(|backend| {
            Arc::new(SqlOperationProvider::new(
                backend,
                "blocks".to_string(),
                "blocks".to_string(),
            ))
        })
    })
    .await?;

    setup_test_table(&ctx, "blocks").await?;

    let prql = r#"
        from blocks
        select {id, content, completed}
        render (list item_template:(row (text content:this.content)))
    "#;

    let (_render_spec, _initial_data, stream) = ctx
        .query_and_watch(prql.to_string(), HashMap::new())
        .await?;

    // Execute operation
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::String("block-1".to_string()));
    params.insert("field".to_string(), Value::String("content".to_string()));
    params.insert(
        "value".to_string(),
        Value::String("New content".to_string()),
    );

    ctx.execute_op("blocks", "set_field", params).await?;

    // Collect stream events
    let changes = ctx
        .collect_stream_events(stream, Duration::from_secs(5), None)
        .await?;

    // Assert we got an update
    assert_change_type(&changes, ChangeType::Updated, Some("block-1"))?;

    Ok(())
}

#[tokio::test]
async fn test_create_and_delete_workflow() -> Result<()> {
    let ctx = E2ETestContext::with_providers(|module| {
        module.with_operation_provider_factory(|backend| {
            Arc::new(SqlOperationProvider::new(
                backend,
                "blocks".to_string(),
                "blocks".to_string(),
            ))
        })
    })
    .await?;

    setup_test_table(&ctx, "blocks").await?;

    let prql = r#"
        from blocks
        select {id, content, completed}
        render (list item_template:(row (text content:this.content)))
    "#;

    let (_render_spec, _initial_data, stream) = ctx
        .query_and_watch(prql.to_string(), HashMap::new())
        .await?;

    // Create a new block
    let mut create_params = HashMap::new();
    create_params.insert("id".to_string(), Value::String("block-2".to_string()));
    create_params.insert(
        "content".to_string(),
        Value::String("New block".to_string()),
    );
    create_params.insert("completed".to_string(), Value::Integer(0));

    ctx.execute_op("blocks", "create", create_params).await?;

    // Delete the block
    let mut delete_params = HashMap::new();
    delete_params.insert("id".to_string(), Value::String("block-2".to_string()));

    ctx.execute_op("blocks", "delete", delete_params).await?;

    // Collect stream events
    let changes = ctx
        .collect_stream_events(stream, Duration::from_secs(5), None)
        .await?;

    // Assert sequence: Created then Deleted
    assert_change_sequence(
        &changes,
        &[
            (ChangeType::Created, Some("block-2")),
            (ChangeType::Deleted, Some("block-2")),
        ],
    )?;

    Ok(())
}

#[tokio::test]
async fn test_multiple_operations_sequence() -> Result<()> {
    let ctx = E2ETestContext::with_providers(|module| {
        module.with_operation_provider_factory(|backend| {
            Arc::new(SqlOperationProvider::new(
                backend,
                "blocks".to_string(),
                "blocks".to_string(),
            ))
        })
    })
    .await?;

    setup_test_table(&ctx, "blocks").await?;

    let prql = r#"
        from blocks
        select {id, content, completed}
        render (list item_template:(row (text content:this.content)))
    "#;

    let (_render_spec, _initial_data, stream) = ctx
        .query_and_watch(prql.to_string(), HashMap::new())
        .await?;

    // Execute multiple operations
    for i in 1..=3 {
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String("block-1".to_string()));
        params.insert("field".to_string(), Value::String("content".to_string()));
        params.insert("value".to_string(), Value::String(format!("Update {}", i)));

        ctx.execute_op("blocks", "set_field", params).await?;
    }

    // Collect stream events
    let changes = ctx
        .collect_stream_events(stream, Duration::from_secs(5), Some(10))
        .await?;

    // Should have at least 3 updates
    let update_count = changes
        .iter()
        .flat_map(|batch| &batch.inner.items)
        .filter(|change| matches!(change.change, ChangeData::Updated { .. }))
        .count();

    assert!(
        update_count >= 3,
        "Expected at least 3 updates, got {}",
        update_count
    );

    Ok(())
}
