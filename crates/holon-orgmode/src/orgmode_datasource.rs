//! OrgMode datasource implementations
//!
//! These datasources implement ChangeNotifications and CrudOperations for
//! Directory, OrgFile, and OrgHeadline entities.

use async_trait::async_trait;
use futures::stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::Stream;

use holon::core::datasource::{
    CrudOperations, DataSource, OperationDescriptor, OperationProvider, OperationRegistry, Result,
    StreamPosition as CoreStreamPosition, UndoAction,
};
use holon::storage::types::StorageEntity;
use holon_api::streaming::ChangeNotifications;
use holon_api::{ApiError, Change, StreamPosition};
use holon_api::{Operation, Value};

use crate::models::{OrgFile, OrgHeadline};
use crate::orgmode_sync_provider::OrgModeSyncProvider;
use crate::writer;

/// OrgHeadline-specific operations for file write-back
///
/// These operations modify the underlying .org files and require file_path and byte positions.
#[holon_macros::operations_trait]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait OrgHeadlineOperations: Send + Sync {
    /// Update a headline's TODO keyword in the file
    #[holon_macros::affects("todo_keyword")]
    async fn update_todo(
        &self,
        id: &str,
        file_path: &str,
        byte_start: i64,
        todo_keyword: Option<&str>,
    ) -> Result<UndoAction>;

    /// Update a headline's priority in the file
    #[holon_macros::affects("priority")]
    async fn update_priority(
        &self,
        id: &str,
        file_path: &str,
        byte_start: i64,
        priority: Option<i64>,
    ) -> Result<UndoAction>;

    /// Update a headline's section content in the file
    #[holon_macros::affects("content")]
    async fn update_content(
        &self,
        id: &str,
        file_path: &str,
        byte_start: i64,
        byte_end: i64,
        content: &str,
    ) -> Result<UndoAction>;
}

// DirectoryDataSource is now imported from holon-filesystem
// Use DirectoryDataSource<OrgModeSyncProvider> for the concrete type

/// DataSource for OrgFile
pub struct OrgFileDataSource {
    provider: Arc<OrgModeSyncProvider>,
}

impl OrgFileDataSource {
    pub fn new(provider: Arc<OrgModeSyncProvider>) -> Self {
        Self { provider }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ChangeNotifications<OrgFile> for OrgFileDataSource {
    async fn watch_changes_since(
        &self,
        _position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<Vec<Change<OrgFile>>, ApiError>> + Send>>
    {
        let rx = self.provider.subscribe_files();

        let change_stream = stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(batch) => Some((Ok(batch.inner), rx)),
                Err(broadcast::error::RecvError::Lagged(n)) => Some((
                    Err(ApiError::InternalError {
                        message: format!("Stream lagged by {} messages", n),
                    }),
                    rx,
                )),
                Err(broadcast::error::RecvError::Closed) => None,
            }
        });

        Box::pin(change_stream)
    }

    async fn get_current_version(&self) -> std::result::Result<Vec<u8>, ApiError> {
        Ok(Vec::new())
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl DataSource<OrgFile> for OrgFileDataSource {
    async fn get_all(&self) -> Result<Vec<OrgFile>> {
        Ok(vec![])
    }

    async fn get_by_id(&self, _id: &str) -> Result<Option<OrgFile>> {
        Ok(None)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl CrudOperations<OrgFile> for OrgFileDataSource {
    async fn set_field(&self, _id: &str, _field: &str, _value: Value) -> Result<UndoAction> {
        Err("File field updates not implemented".into())
    }

    async fn create(&self, _fields: HashMap<String, Value>) -> Result<(String, UndoAction)> {
        Err("File creation not implemented".into())
    }

    async fn delete(&self, _id: &str) -> Result<UndoAction> {
        Err("File deletion not implemented".into())
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OperationProvider for OrgFileDataSource {
    fn operations(&self) -> Vec<OperationDescriptor> {
        OrgFile::all_operations()
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        _op_name: &str,
        _params: StorageEntity,
    ) -> Result<UndoAction> {
        if entity_name != "org_files" {
            return Err(format!("Expected entity_name 'org_files', got '{}'", entity_name).into());
        }
        Ok(UndoAction::Irreversible)
    }
}

/// DataSource for OrgHeadline - the main entity with full CRUD support
pub struct OrgHeadlineDataSource {
    provider: Arc<OrgModeSyncProvider>,
}

impl OrgHeadlineDataSource {
    pub fn new(provider: Arc<OrgModeSyncProvider>) -> Self {
        Self { provider }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ChangeNotifications<OrgHeadline> for OrgHeadlineDataSource {
    async fn watch_changes_since(
        &self,
        _position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<Vec<Change<OrgHeadline>>, ApiError>> + Send>>
    {
        let rx = self.provider.subscribe_headlines();

        let change_stream = stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(batch) => Some((Ok(batch.inner), rx)),
                Err(broadcast::error::RecvError::Lagged(n)) => Some((
                    Err(ApiError::InternalError {
                        message: format!("Stream lagged by {} messages", n),
                    }),
                    rx,
                )),
                Err(broadcast::error::RecvError::Closed) => None,
            }
        });

        Box::pin(change_stream)
    }

    async fn get_current_version(&self) -> std::result::Result<Vec<u8>, ApiError> {
        Ok(Vec::new())
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl DataSource<OrgHeadline> for OrgHeadlineDataSource {
    async fn get_all(&self) -> Result<Vec<OrgHeadline>> {
        Ok(vec![])
    }

    async fn get_by_id(&self, _id: &str) -> Result<Option<OrgHeadline>> {
        Ok(None)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl CrudOperations<OrgHeadline> for OrgHeadlineDataSource {
    async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<UndoAction> {
        use tracing::{info, warn};

        info!(
            "[OrgHeadlineDataSource] set_field: id={}, field={}, value={:?}",
            id, field, value
        );

        // For now, log and acknowledge - full implementation requires file path lookup
        // In production, we'd query the database to get file_path and byte_start
        match field {
            "todo_keyword" | "priority" | "title" | "content" | "tags" | "scheduled"
            | "deadline" => {
                warn!(
                    "[OrgHeadlineDataSource] Field '{}' update acknowledged but write-back requires file_path lookup (not implemented)",
                    field
                );
                Ok(UndoAction::Irreversible)
            }
            "depth" | "parent_id" | "byte_start" | "byte_end" | "file_path" | "file_id" => {
                Err(format!("Field '{}' cannot be set directly", field).into())
            }
            _ => Err(format!("Unknown field '{}'", field).into()),
        }
    }

    async fn create(&self, fields: HashMap<String, Value>) -> Result<(String, UndoAction)> {
        use tracing::info;

        let title = fields
            .get("title")
            .and_then(|v| v.as_string())
            .ok_or_else(|| "Missing 'title' field")?;

        let _parent_id = fields
            .get("parent_id")
            .and_then(|v| v.as_string())
            .ok_or_else(|| "Missing 'parent_id' field")?;

        info!(
            "[OrgHeadlineDataSource] create: title='{}' (not implemented)",
            title
        );

        // TODO: Implement headline creation
        // 1. Determine target file from parent_id
        // 2. Find insertion point
        // 3. Generate UUID for :ID: property
        // 4. Write headline to file
        // 5. Trigger sync

        Err("Headline creation not implemented".into())
    }

    async fn delete(&self, id: &str) -> Result<UndoAction> {
        use tracing::info;

        info!(
            "[OrgHeadlineDataSource] delete: id='{}' (not implemented)",
            id
        );

        // TODO: Implement headline deletion
        // 1. Find file containing this headline
        // 2. Remove headline content from file
        // 3. Write back to file
        // 4. Trigger sync

        Err("Headline deletion not implemented".into())
    }
}

impl OrgHeadlineDataSource {
    /// Helper to modify a file and sync afterwards
    async fn modify_file<F>(&self, file_path: &str, transform: F) -> Result<()>
    where
        F: FnOnce(&str) -> Result<String>,
    {
        // Read file
        let content = std::fs::read_to_string(file_path)
            .map_err(|e| format!("Failed to read file: {}", e))?;

        // Apply transformation
        let new_content = transform(&content)?;

        // Write back
        std::fs::write(file_path, new_content)
            .map_err(|e| format!("Failed to write file: {}", e))?;

        // Trigger sync to update database
        use holon::core::datasource::SyncableProvider;
        SyncableProvider::sync(&*self.provider, CoreStreamPosition::Beginning)
            .await
            .map_err(|e| format!("Failed to sync: {}", e))?;

        Ok(())
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OrgHeadlineOperations for OrgHeadlineDataSource {
    async fn update_todo(
        &self,
        id: &str,
        file_path: &str,
        byte_start: i64,
        todo_keyword: Option<&str>,
    ) -> Result<UndoAction> {
        use tracing::info;

        info!(
            "[OrgHeadlineDataSource] update_todo: id={}, file={}, byte_start={}, keyword={:?}",
            id, file_path, byte_start, todo_keyword
        );

        let byte_start = byte_start as usize;
        let keyword_owned = todo_keyword.map(|s| s.to_string());

        self.modify_file(file_path, |content| {
            writer::update_todo_keyword(content, byte_start, keyword_owned.as_deref())
                .map_err(|e| format!("Failed to update TODO keyword: {}", e).into())
        })
        .await?;

        info!("[OrgHeadlineDataSource] update_todo completed successfully");
        Ok(UndoAction::Irreversible)
    }

    async fn update_priority(
        &self,
        id: &str,
        file_path: &str,
        byte_start: i64,
        priority: Option<i64>,
    ) -> Result<UndoAction> {
        use tracing::info;

        // Convert priority integer to char (3=A, 2=B, 1=C, 0/None=remove)
        let priority_char = priority.and_then(|p| match p {
            3 => Some('A'),
            2 => Some('B'),
            1 => Some('C'),
            _ => None,
        });

        info!(
            "[OrgHeadlineDataSource] update_priority: id={}, file={}, byte_start={}, priority={:?}",
            id, file_path, byte_start, priority_char
        );

        let byte_start = byte_start as usize;

        self.modify_file(file_path, |content| {
            writer::update_priority(content, byte_start, priority_char)
                .map_err(|e| format!("Failed to update priority: {}", e).into())
        })
        .await?;

        info!("[OrgHeadlineDataSource] update_priority completed successfully");
        Ok(UndoAction::Irreversible)
    }

    async fn update_content(
        &self,
        id: &str,
        file_path: &str,
        byte_start: i64,
        byte_end: i64,
        content: &str,
    ) -> Result<UndoAction> {
        use tracing::info;

        info!(
            "[OrgHeadlineDataSource] update_content: id={}, file={}, byte_start={}, byte_end={}",
            id, file_path, byte_start, byte_end
        );

        let byte_start = byte_start as usize;
        let byte_end = byte_end as usize;
        let new_content = content.to_string();

        self.modify_file(file_path, |file_content| {
            writer::update_content(file_content, byte_start, byte_end, |_| new_content.clone())
                .map_err(|e| format!("Failed to update content: {}", e).into())
        })
        .await?;

        info!("[OrgHeadlineDataSource] update_content completed successfully");
        Ok(UndoAction::Irreversible)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OperationProvider for OrgHeadlineDataSource {
    fn operations(&self) -> Vec<OperationDescriptor> {
        let entity_name = OrgHeadline::entity_name();
        let short_name = OrgHeadline::short_name().expect("OrgHeadline must have short_name");
        let id_column = "id";

        // Combine operations from all trait sources
        OrgHeadline::all_operations()
            .into_iter()
            .chain(
                __operations_org_headline_operations::org_headline_operations(
                    entity_name,
                    short_name,
                    entity_name,
                    id_column,
                )
                .into_iter(),
            )
            .collect()
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<UndoAction> {
        use holon::core::datasource::{
            UnknownOperationError, __operations_crud_operation_provider,
            __operations_mutable_block_data_source, __operations_mutable_task_data_source,
        };

        if entity_name != "org_headlines" {
            return Err(format!(
                "Expected entity_name 'org_headlines', got '{}'",
                entity_name
            )
            .into());
        }

        // Try OrgHeadline-specific operations first (update_todo, update_priority, update_content)
        match __operations_org_headline_operations::dispatch_operation(self, op_name, &params).await
        {
            Ok(op) => return Ok(op),
            Err(err) => {
                if !UnknownOperationError::is_unknown(err.as_ref()) {
                    return Err(err);
                }
            }
        }

        // Try CRUD operations
        match __operations_crud_operation_provider::dispatch_operation::<_, OrgHeadline>(
            self, op_name, &params,
        )
        .await
        {
            Ok(op) => return Ok(op),
            Err(err) => {
                if !UnknownOperationError::is_unknown(err.as_ref()) {
                    return Err(err);
                }
            }
        }

        // Try block operations
        match __operations_mutable_block_data_source::dispatch_operation::<_, OrgHeadline>(
            self, op_name, &params,
        )
        .await
        {
            Ok(op) => return Ok(op),
            Err(err) => {
                if !UnknownOperationError::is_unknown(err.as_ref()) {
                    return Err(err);
                }
            }
        }

        // Try task operations
        __operations_mutable_task_data_source::dispatch_operation::<_, OrgHeadline>(
            self, op_name, &params,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_headline_operations_include_task_operations() {
        let ops = OrgHeadline::all_operations();
        let op_names: Vec<&str> = ops.iter().map(|op| op.name.as_str()).collect();

        // Should include task operations
        assert!(
            op_names.contains(&"set_completion"),
            "Should have set_completion operation"
        );
        assert!(
            op_names.contains(&"set_priority"),
            "Should have set_priority operation"
        );
    }

    #[test]
    fn test_headline_operations_include_block_operations() {
        let ops = OrgHeadline::all_operations();
        let op_names: Vec<&str> = ops.iter().map(|op| op.name.as_str()).collect();

        // Should include block operations
        assert!(
            op_names.contains(&"move_block"),
            "Should have move_block operation"
        );
    }
}
