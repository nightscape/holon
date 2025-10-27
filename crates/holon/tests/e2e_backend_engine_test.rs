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

use async_trait::async_trait;
use holon::core::datasource::{OperationProvider, OperationResult, Result as DatasourceResult};
use holon::storage::sql_utils::value_to_sql_literal;
use holon::storage::turso::{ChangeData, DbHandle};
use holon::storage::types::StorageEntity;
use holon::testing::e2e_test_helpers::{
    ChangeType, E2ETestContext, assert_change_sequence, assert_change_type, wait_for_change,
};
use holon_api::{OperationDescriptor, Value};

/// Simple SQL-based operation provider for testing
struct SqlOperationProvider {
    db_handle: DbHandle,
    table_name: String,
    entity_name: String,
    entity_short_name: String,
}

impl SqlOperationProvider {
    fn new(
        db_handle: DbHandle,
        table_name: String,
        entity_name: String,
        entity_short_name: String,
    ) -> Self {
        Self {
            db_handle,
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
                        type_hint: holon_api::TypeHint::String,
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
    ) -> DatasourceResult<OperationResult> {
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

                let sql_value = Self::value_to_sql(value);
                let sql = format!(
                    "UPDATE {} SET {} = {} WHERE id = '{}'",
                    self.table_name,
                    field,
                    sql_value,
                    id.replace("'", "''")
                );
                self.db_handle
                    .execute(&sql, vec![])
                    .await
                    .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                Ok(OperationResult::irreversible(Vec::new()))
            }
            "create" => {
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
                self.db_handle
                    .execute(&sql, vec![])
                    .await
                    .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                Ok(OperationResult::irreversible(Vec::new()))
            }
            "delete" => {
                let id = params
                    .get("id")
                    .and_then(|v| v.as_string())
                    .ok_or_else(|| "Missing 'id' parameter".to_string())?;

                let sql = format!(
                    "DELETE FROM {} WHERE id = '{}'",
                    self.table_name,
                    id.replace("'", "''")
                );
                self.db_handle
                    .execute(&sql, vec![])
                    .await
                    .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                Ok(OperationResult::irreversible(Vec::new()))
            }
            _ => Err(format!("Unknown operation: {}", op_name).into()),
        }
    }
}

/// Helper to set up a test table with initial data
async fn setup_test_table(ctx: &E2ETestContext, table_name: &str) -> Result<()> {
    let db = ctx.engine().db_handle();

    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (id TEXT PRIMARY KEY, content TEXT, completed INTEGER DEFAULT 0)",
        table_name
    );
    db.execute_ddl(&create_sql)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create table: {}", e))?;

    let insert_sql = format!(
        "INSERT OR IGNORE INTO {} (id, content, completed) VALUES ('block-1', 'Initial content', 0)",
        table_name
    );
    db.execute(&insert_sql, vec![])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to insert test data: {}", e))?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_query_execution() -> Result<()> {
    let ctx = E2ETestContext::new().await?;
    setup_test_table(&ctx, "blocks").await?;

    let prql = r#"
        from blocks
        select {id, content, completed}
        render (list item_template:(row (text content:this.content)))
    "#;

    let widget_spec = ctx.query(prql.to_string(), HashMap::new()).await?;

    // Verify we got results
    assert!(
        !widget_spec.data.is_empty(),
        "Should have at least one result"
    );
    assert_eq!(
        widget_spec.data[0].get("id").unwrap().as_string(),
        Some("block-1")
    );

    // Verify render spec has the expected structure
    match widget_spec.render_spec.root() {
        Some(holon_prql_render::RenderExpr::FunctionCall { name, .. }) => {
            assert_eq!(name, "list");
        }
        _ => panic!("Expected list function call in render spec"),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_and_watch_stream() -> Result<()> {
    // Create context with provider factory
    // The factory receives the correct backend at creation time
    let ctx = E2ETestContext::with_providers(|module| {
        module.with_operation_provider_factory(|backend| {
            let db_handle =
                tokio::task::block_in_place(|| backend.blocking_read().handle().clone());
            Arc::new(SqlOperationProvider::new(
                db_handle,
                "blocks".to_string(),
                "blocks".to_string(),
                "block".to_string(),
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

    let (widget_spec, stream) = ctx
        .query_and_watch(prql.to_string(), HashMap::new())
        .await?;

    // Verify initial data
    assert!(!widget_spec.data.is_empty());
    assert_eq!(
        widget_spec.data[0].get("id").unwrap().as_string(),
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

#[tokio::test(flavor = "multi_thread")]
async fn test_operation_triggers_stream_update() -> Result<()> {
    let ctx = E2ETestContext::with_providers(|module| {
        module.with_operation_provider_factory(|backend| {
            let db_handle =
                tokio::task::block_in_place(|| backend.blocking_read().handle().clone());
            Arc::new(SqlOperationProvider::new(
                db_handle,
                "blocks".to_string(),
                "blocks".to_string(),
                "block".to_string(),
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

    let (_widget_spec, stream) = ctx
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

#[tokio::test(flavor = "multi_thread")]
async fn test_create_and_delete_workflow() -> Result<()> {
    let ctx = E2ETestContext::with_providers(|module| {
        module.with_operation_provider_factory(|backend| {
            let db_handle =
                tokio::task::block_in_place(|| backend.blocking_read().handle().clone());
            Arc::new(SqlOperationProvider::new(
                db_handle,
                "blocks".to_string(),
                "blocks".to_string(),
                "block".to_string(),
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

    let (_widget_spec, stream) = ctx
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

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_operations_sequence() -> Result<()> {
    let ctx = E2ETestContext::with_providers(|module| {
        module.with_operation_provider_factory(|backend| {
            let db_handle =
                tokio::task::block_in_place(|| backend.blocking_read().handle().clone());
            Arc::new(SqlOperationProvider::new(
                db_handle,
                "blocks".to_string(),
                "blocks".to_string(),
                "block".to_string(),
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

    let (_widget_spec, stream) = ctx
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
