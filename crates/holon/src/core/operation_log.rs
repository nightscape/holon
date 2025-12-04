//! Operation log implementation for persistent undo/redo.
//!
//! This module provides `OperationLogStore`, which implements the
//! `OperationLogOperations` trait for persistent operation logging.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::storage::turso::TursoBackend;
use holon_api::{HasSchema, Operation, Value};
use holon_core::{OperationLogEntry, OperationLogOperations, OperationStatus, UndoAction};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Persistent operation log store backed by TursoBackend.
///
/// Stores operations in the `operations` table and provides
/// undo/redo candidate queries.
pub struct OperationLogStore {
    backend: Arc<RwLock<TursoBackend>>,
    max_log_size: usize,
}

impl OperationLogStore {
    /// Create a new operation log store.
    pub fn new(backend: Arc<RwLock<TursoBackend>>) -> Self {
        Self {
            backend,
            max_log_size: 100,
        }
    }

    /// Create a new operation log store with custom max size.
    pub fn with_max_size(backend: Arc<RwLock<TursoBackend>>, max_log_size: usize) -> Self {
        Self {
            backend,
            max_log_size,
        }
    }

    /// Initialize the operations table schema.
    ///
    /// Creates the table and indexes if they don't exist.
    pub async fn initialize_schema(&self) -> Result<()> {
        let schema = OperationLogEntry::schema();
        let create_table_sql = schema.to_create_table_sql();
        let index_sqls = schema.to_index_sql();

        let backend = self.backend.read().await;

        debug!("Creating operations table: {}", create_table_sql);
        backend
            .execute_sql(&create_table_sql, HashMap::new())
            .await
            .map_err(|e| format!("Failed to create operations table: {}", e))?;

        for index_sql in index_sqls {
            debug!("Creating index: {}", index_sql);
            backend
                .execute_sql(&index_sql, HashMap::new())
                .await
                .map_err(|e| format!("Failed to create index: {}", e))?;
        }

        info!("Operation log schema initialized");
        Ok(())
    }

    /// Trim old operations if we're over the max size.
    async fn trim_if_needed(&self) -> Result<()> {
        let backend = self.backend.read().await;

        // Count current entries
        let count_result = backend
            .execute_sql("SELECT COUNT(*) as count FROM operations", HashMap::new())
            .await
            .map_err(|e| format!("Failed to count operations: {}", e))?;

        let count = count_result
            .first()
            .and_then(|row| row.get("count"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;

        if count > self.max_log_size {
            let to_delete = count - self.max_log_size;
            debug!(
                "Trimming {} old operations (current: {}, max: {})",
                to_delete, count, self.max_log_size
            );

            // Delete oldest entries (lowest IDs)
            let delete_sql = format!(
                "DELETE FROM operations WHERE id IN (
                    SELECT id FROM operations ORDER BY id ASC LIMIT {}
                )",
                to_delete
            );

            backend
                .execute_sql(&delete_sql, HashMap::new())
                .await
                .map_err(|e| format!("Failed to trim old operations: {}", e))?;
        }

        Ok(())
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OperationLogOperations for OperationLogStore {
    async fn log_operation(&self, operation: Operation, inverse: UndoAction) -> Result<i64> {
        // Clear redo stack first (new operation invalidates redo history)
        self.clear_redo_stack().await?;

        // Create the entry
        let entry = OperationLogEntry::new(operation, inverse.into_option());

        // Insert into database
        let backend = self.backend.read().await;

        let insert_sql = "INSERT INTO operations (operation, inverse, status, created_at, display_name, entity_name, op_name)
                          VALUES ($operation, $inverse, $status, $created_at, $display_name, $entity_name, $op_name)";

        let mut params = HashMap::new();
        params.insert(
            "operation".to_string(),
            Value::String(entry.operation.clone()),
        );
        params.insert(
            "inverse".to_string(),
            entry
                .inverse
                .as_ref()
                .map(|s| Value::String(s.clone()))
                .unwrap_or(Value::Null),
        );
        params.insert("status".to_string(), Value::String(entry.status.clone()));
        params.insert("created_at".to_string(), Value::Integer(entry.created_at));
        params.insert(
            "display_name".to_string(),
            Value::String(entry.display_name.clone()),
        );
        params.insert(
            "entity_name".to_string(),
            Value::String(entry.entity_name.clone()),
        );
        params.insert("op_name".to_string(), Value::String(entry.op_name.clone()));

        backend
            .execute_sql(insert_sql, params)
            .await
            .map_err(|e| format!("Failed to insert operation log entry: {}", e))?;

        // Get the inserted ID
        let id_result = backend
            .execute_sql("SELECT last_insert_rowid() as id", HashMap::new())
            .await
            .map_err(|e| format!("Failed to get last insert ID: {}", e))?;

        let id = id_result
            .first()
            .and_then(|row| row.get("id"))
            .and_then(|v| v.as_i64())
            .ok_or("Failed to get inserted operation ID")?;

        drop(backend);

        // Trim if needed
        self.trim_if_needed().await?;

        debug!("Logged operation {} with id {}", entry.display_name, id);
        Ok(id)
    }

    async fn mark_undone(&self, id: i64) -> Result<()> {
        let backend = self.backend.read().await;

        let sql = "UPDATE operations SET status = $status WHERE id = $id";
        let mut params = HashMap::new();
        params.insert(
            "status".to_string(),
            Value::String(OperationStatus::Undone.as_str().to_string()),
        );
        params.insert("id".to_string(), Value::Integer(id));

        backend
            .execute_sql(sql, params)
            .await
            .map_err(|e| format!("Failed to mark operation as undone: {}", e))?;

        debug!("Marked operation {} as undone", id);
        Ok(())
    }

    async fn mark_redone(&self, id: i64) -> Result<()> {
        let backend = self.backend.read().await;

        // Restore to PendingSync status (or Synced if we had sync tracking)
        let sql = "UPDATE operations SET status = $status WHERE id = $id";
        let mut params = HashMap::new();
        params.insert(
            "status".to_string(),
            Value::String(OperationStatus::PendingSync.as_str().to_string()),
        );
        params.insert("id".to_string(), Value::Integer(id));

        backend
            .execute_sql(sql, params)
            .await
            .map_err(|e| format!("Failed to mark operation as redone: {}", e))?;

        debug!("Marked operation {} as redone", id);
        Ok(())
    }

    async fn clear_redo_stack(&self) -> Result<()> {
        let backend = self.backend.read().await;

        // Mark all undone operations as cancelled (they can no longer be redone)
        let sql = "UPDATE operations SET status = $new_status WHERE status = $old_status";
        let mut params = HashMap::new();
        params.insert(
            "new_status".to_string(),
            Value::String(OperationStatus::Cancelled.as_str().to_string()),
        );
        params.insert(
            "old_status".to_string(),
            Value::String(OperationStatus::Undone.as_str().to_string()),
        );

        backend
            .execute_sql(sql, params)
            .await
            .map_err(|e| format!("Failed to clear redo stack: {}", e))?;

        debug!("Cleared redo stack");
        Ok(())
    }

    fn max_log_size(&self) -> usize {
        self.max_log_size
    }
}

/// Observer that logs operations to the persistent OperationLogStore.
///
/// This observer implements OperationObserver and delegates to OperationLogStore.
/// It observes all operations (entity_filter = "*") and logs them for undo/redo.
pub struct OperationLogObserver {
    store: Arc<OperationLogStore>,
}

impl OperationLogObserver {
    /// Create a new operation log observer wrapping the given store.
    pub fn new(store: Arc<OperationLogStore>) -> Self {
        Self { store }
    }
}

use crate::core::datasource::OperationObserver;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OperationObserver for OperationLogObserver {
    fn entity_filter(&self) -> &str {
        "*" // Observe all entities for undo/redo
    }

    async fn on_operation_executed(
        &self,
        operation: &holon_api::Operation,
        undo_action: &UndoAction,
    ) {
        if let Err(e) = self
            .store
            .log_operation(operation.clone(), undo_action.clone())
            .await
        {
            tracing::error!("Failed to log operation for undo: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_operation_log_store_basic() {
        // Create in-memory backend
        let backend = TursoBackend::new_in_memory()
            .await
            .expect("Failed to create backend");
        let backend = Arc::new(RwLock::new(backend));

        // Create and initialize store
        let store = OperationLogStore::new(backend.clone());
        store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Log an operation
        let op = Operation::new(
            "test-entity",
            "test_op",
            "Test Operation",
            HashMap::from([("id".to_string(), Value::String("123".to_string()))]),
        );
        let inverse = Operation::new(
            "test-entity",
            "test_op_inverse",
            "Undo Test Operation",
            HashMap::from([("id".to_string(), Value::String("123".to_string()))]),
        );

        let id = store
            .log_operation(op, UndoAction::Undo(inverse))
            .await
            .expect("Failed to log operation");
        assert!(id > 0);

        // Verify operation was inserted by querying directly
        let backend_guard = backend.read().await;
        let result = backend_guard
            .execute_sql(
                "SELECT * FROM operations WHERE id = $id",
                HashMap::from([("id".to_string(), Value::Integer(id))]),
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].get("display_name").and_then(|v| v.as_string()),
            Some("Test Operation")
        );
        assert_eq!(
            result[0].get("status").and_then(|v| v.as_string()),
            Some("pending_sync")
        );
    }

    #[tokio::test]
    async fn test_mark_undone_and_redone() {
        let backend = TursoBackend::new_in_memory()
            .await
            .expect("Failed to create backend");
        let backend = Arc::new(RwLock::new(backend));

        let store = OperationLogStore::new(backend.clone());
        store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Log an operation
        let op = Operation::new("test", "op1", "Op 1", HashMap::new());
        let id = store
            .log_operation(op, UndoAction::Irreversible)
            .await
            .unwrap();

        // Mark as undone
        store.mark_undone(id).await.unwrap();

        // Verify status changed
        let backend_guard = backend.read().await;
        let result = backend_guard
            .execute_sql(
                "SELECT status FROM operations WHERE id = $id",
                HashMap::from([("id".to_string(), Value::Integer(id))]),
            )
            .await
            .unwrap();
        assert_eq!(
            result[0].get("status").and_then(|v| v.as_string()),
            Some("undone")
        );
        drop(backend_guard);

        // Mark as redone
        store.mark_redone(id).await.unwrap();

        // Verify status restored
        let backend_guard = backend.read().await;
        let result = backend_guard
            .execute_sql(
                "SELECT status FROM operations WHERE id = $id",
                HashMap::from([("id".to_string(), Value::Integer(id))]),
            )
            .await
            .unwrap();
        assert_eq!(
            result[0].get("status").and_then(|v| v.as_string()),
            Some("pending_sync")
        );
    }

    #[tokio::test]
    async fn test_clear_redo_stack_on_new_operation() {
        let backend = TursoBackend::new_in_memory()
            .await
            .expect("Failed to create backend");
        let backend = Arc::new(RwLock::new(backend));

        let store = OperationLogStore::new(backend.clone());
        store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Log first operation
        let op1 = Operation::new("test", "op1", "Op 1", HashMap::new());
        let id1 = store
            .log_operation(op1, UndoAction::Irreversible)
            .await
            .unwrap();

        // Mark it as undone
        store.mark_undone(id1).await.unwrap();

        // Log a new operation (this should clear the redo stack - mark undone as cancelled)
        let op2 = Operation::new("test", "op2", "Op 2", HashMap::new());
        store
            .log_operation(op2, UndoAction::Irreversible)
            .await
            .unwrap();

        // Verify first operation is now cancelled (not undone)
        let backend_guard = backend.read().await;
        let result = backend_guard
            .execute_sql(
                "SELECT status FROM operations WHERE id = $id",
                HashMap::from([("id".to_string(), Value::Integer(id1))]),
            )
            .await
            .unwrap();
        assert_eq!(
            result[0].get("status").and_then(|v| v.as_string()),
            Some("cancelled")
        );
    }

    #[tokio::test]
    async fn test_trim_old_operations() {
        let backend = TursoBackend::new_in_memory()
            .await
            .expect("Failed to create backend");
        let backend = Arc::new(RwLock::new(backend));

        // Create store with small max size
        let store = OperationLogStore::with_max_size(backend.clone(), 5);
        store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Log more operations than max size
        for i in 0..10 {
            let op = Operation::new(
                "test",
                &format!("op{}", i),
                &format!("Op {}", i),
                HashMap::new(),
            );
            store
                .log_operation(op, UndoAction::Irreversible)
                .await
                .unwrap();
        }

        // Check that we only have max_size operations
        let backend_guard = backend.read().await;
        let count_result = backend_guard
            .execute_sql("SELECT COUNT(*) as count FROM operations", HashMap::new())
            .await
            .unwrap();
        let count = count_result
            .first()
            .and_then(|row| row.get("count"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        assert_eq!(count, 5);
    }
}
