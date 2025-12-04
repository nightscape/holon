use async_trait::async_trait;
use flutter_rust_bridge::frb;

use crate::api::{ApiError, BlockMetadata, NewBlock};
/// Flutter UI backend for property-based testing
///
/// This module provides a CoreOperations implementation that drives the Flutter UI
/// for PBT tests. It uses a callback-based approach where:
///
/// 1. Write operations (create/update/delete) are queued and execute asynchronously
/// 2. The `id` parameter is used to ensure both backends use the same IDs
/// 3. Read operations call back into Dart to get the actual UI state
///
/// Architecture:
/// ```
/// PBT Test (Rust) → FlutterPbtBackend → Queue Commands
///                                      ↓
///                                    Dart processes commands
///                                      ↓
/// PBT Test calls get_all_blocks() → Dart callback → Read UI state
/// ```
use flutter_rust_bridge::DartFnFuture;
use holon::api::repository::{CoreOperations, Lifecycle};
use std::sync::Arc;

// Re-export for FRB generated code (explicitly for wildcard imports)
pub use super::types::Block;
pub use holon::api::types::Traversal;

/// Callback type for reading blocks from Dart/Flutter
#[frb(ignore)]
pub type GetBlocksCallback = Arc<dyn Fn() -> DartFnFuture<Vec<Block>> + Send + Sync>;

/// Callback type for creating a block in Dart/Flutter
#[frb(ignore)]
pub type CreateBlockCallback =
    Arc<dyn Fn(String, String, String) -> DartFnFuture<()> + Send + Sync>;

/// Callback type for updating a block in Dart/Flutter
#[frb(ignore)]
pub type UpdateBlockCallback = Arc<dyn Fn(String, String) -> DartFnFuture<()> + Send + Sync>;

/// Callback type for deleting a block in Dart/Flutter
#[frb(ignore)]
pub type DeleteBlockCallback = Arc<dyn Fn(String) -> DartFnFuture<()> + Send + Sync>;

/// Callback type for moving a block in Dart/Flutter
#[frb(ignore)]
pub type MoveBlockCallback = Arc<dyn Fn(String, String) -> DartFnFuture<()> + Send + Sync>;

/// Flutter UI backend for PBT testing
///
/// This backend uses a pure callback-based approach:
/// - Write operations call Dart callbacks and return immediately
/// - Read operations call Dart callback to get actual UI state
#[frb(ignore)]
#[derive(Clone)]
pub struct FlutterPbtBackend {
    #[allow(dead_code)]
    test_id: String,
    /// Callback provided by Dart to read the current block tree
    get_blocks_callback: GetBlocksCallback,
    /// Callback to create a block in the UI
    create_block_callback: CreateBlockCallback,
    /// Callback to update a block in the UI
    update_block_callback: UpdateBlockCallback,
    /// Callback to delete a block in the UI
    delete_block_callback: DeleteBlockCallback,
    /// Callback to move a block in the UI
    move_block_callback: MoveBlockCallback,
}

impl FlutterPbtBackend {
    /// Create a new Flutter PBT backend
    ///
    /// # Arguments
    /// * `test_id` - Unique identifier for this test session (for debugging/logging)
    /// * `get_blocks_callback` - Dart callback that returns current UI state
    /// * `create_block_callback` - Dart callback to create a block (id, parent_id, content)
    /// * `update_block_callback` - Dart callback to update a block (id, content)
    /// * `delete_block_callback` - Dart callback to delete a block (id)
    /// * `move_block_callback` - Dart callback to move a block (id, new_parent_id)
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        test_id: String,
        get_blocks_callback: GetBlocksCallback,
        create_block_callback: CreateBlockCallback,
        update_block_callback: UpdateBlockCallback,
        delete_block_callback: DeleteBlockCallback,
        move_block_callback: MoveBlockCallback,
    ) -> Self {
        Self {
            test_id,
            get_blocks_callback,
            create_block_callback,
            update_block_callback,
            delete_block_callback,
            move_block_callback,
        }
    }

    /// Wait for Flutter UI to settle after write operations
    ///
    /// This simple delay gives Flutter time to process any pending writes.
    /// In production, this could be replaced with a more sophisticated
    /// synchronization mechanism.
    async fn flush_pending_writes(&self) {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[async_trait]
impl CoreOperations for FlutterPbtBackend {
    async fn get_block(&self, id: &str) -> Result<Block, ApiError> {
        // Wait for any pending writes to settle
        self.flush_pending_writes().await;

        // Read from Dart
        let blocks = (self.get_blocks_callback)().await;

        blocks
            .into_iter()
            .find(|b| b.id == id)
            .ok_or_else(|| ApiError::BlockNotFound { id: id.to_string() })
    }

    async fn get_all_blocks(&self, _traversal: Traversal) -> Result<Vec<Block>, ApiError> {
        // Wait for any pending writes to settle
        self.flush_pending_writes().await;

        // Read from Dart callback - this gets the actual UI state!
        let blocks = (self.get_blocks_callback)().await;
        Ok(blocks)
    }

    async fn list_children(&self, parent_id: &str) -> Result<Vec<String>, ApiError> {
        // Wait for any pending writes to settle
        self.flush_pending_writes().await;

        // Read from Dart
        let blocks = (self.get_blocks_callback)().await;

        Ok(blocks
            .into_iter()
            .filter(|b| b.parent_id.as_str() == parent_id)
            .map(|b| b.id)
            .collect())
    }

    async fn create_block(
        &self,
        parent_id: String,
        content: holon_api::BlockContent,
        id: Option<String>,
    ) -> Result<Block, ApiError> {
        // Generate ID if not provided (PBT will provide it)
        let block_id = id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // Call Dart to create the block in the UI - convert to plain text for callback
        let content_str = content.to_plain_text().to_string();
        (self.create_block_callback)(block_id.clone(), parent_id.clone(), content_str).await;

        // Return immediately with expected result
        // The actual state will be verified via get_all_blocks() later
        Ok(Block {
            id: block_id,
            parent_id,
            content,
            properties: std::collections::HashMap::new(),
            children: vec![],
            metadata: BlockMetadata {
                created_at: chrono::Utc::now().timestamp_millis(),
                updated_at: chrono::Utc::now().timestamp_millis(),
            },
        })
    }

    async fn update_block(
        &self,
        id: &str,
        content: holon_api::BlockContent,
    ) -> Result<(), ApiError> {
        // Call Dart to update the block in the UI - convert to plain text for callback
        let content_str = content.to_plain_text().to_string();
        (self.update_block_callback)(id.to_string(), content_str).await;
        Ok(())
    }

    async fn delete_block(&self, id: &str) -> Result<(), ApiError> {
        // Call Dart to delete the block from the UI
        (self.delete_block_callback)(id.to_string()).await;
        Ok(())
    }

    async fn move_block(
        &self,
        id: &str,
        new_parent: String,
        after: Option<String>,
    ) -> Result<(), ApiError> {
        // Call Dart to move the block in the UI
        // Note: The callback doesn't support 'after' parameter yet - that's for future ordering support
        let _ = after; // For future ordering support
        (self.move_block_callback)(id.to_string(), new_parent).await;
        Ok(())
    }

    async fn get_blocks(&self, ids: Vec<String>) -> Result<Vec<Block>, ApiError> {
        // Wait for any pending writes to settle
        self.flush_pending_writes().await;

        // Read from Dart
        let all_blocks = (self.get_blocks_callback)().await;

        Ok(all_blocks
            .into_iter()
            .filter(|b| ids.contains(&b.id))
            .collect())
    }

    async fn create_blocks(&self, blocks: Vec<NewBlock>) -> Result<Vec<Block>, ApiError> {
        // Call Dart to create each block in the UI
        let mut created = Vec::new();

        for new_block in blocks {
            let block_id = new_block
                .id
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            // Call Dart to create the block
            (self.create_block_callback)(
                block_id.clone(),
                new_block.parent_id.clone(),
                new_block.content.to_string(),
            )
            .await;

            // Build the result block
            created.push(Block {
                id: block_id,
                parent_id: new_block.parent_id,
                content: new_block.content,
                properties: std::collections::HashMap::new(),
                children: vec![],
                metadata: BlockMetadata {
                    created_at: chrono::Utc::now().timestamp_millis(),
                    updated_at: chrono::Utc::now().timestamp_millis(),
                },
            });
        }

        Ok(created)
    }

    async fn delete_blocks(&self, ids: Vec<String>) -> Result<(), ApiError> {
        // Call Dart to delete each block from the UI
        for id in ids {
            (self.delete_block_callback)(id).await;
        }
        Ok(())
    }
}

#[async_trait]
impl Lifecycle for FlutterPbtBackend {
    async fn create_new(_doc_id: String) -> Result<Self, ApiError> {
        // FlutterPbtBackend is created externally with callbacks
        // This method should not be called directly
        Err(ApiError::InternalError {
            message: "FlutterPbtBackend must be created with callbacks via new()".to_string(),
        })
    }

    async fn open_existing(_doc_id: String) -> Result<Self, ApiError> {
        // FlutterPbtBackend is created externally with callbacks
        // This method should not be called directly
        Err(ApiError::InternalError {
            message: "FlutterPbtBackend must be created with callbacks via new()".to_string(),
        })
    }

    async fn dispose(&self) -> Result<(), ApiError> {
        // Nothing to clean up - callbacks are owned by Dart
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_flutter_pbt_backend_fire_and_forget() {
        // Create test callbacks
        let get_blocks_callback = Arc::new(|| Box::pin(async { vec![] }));
        let create_callback =
            Arc::new(|_id: String, _parent: Option<String>, _content: String| Box::pin(async {}));
        let update_callback = Arc::new(|_id: String, _content: String| Box::pin(async {}));
        let delete_callback = Arc::new(|_id: String| Box::pin(async {}));
        let move_callback = Arc::new(|_id: String, _parent: Option<String>| Box::pin(async {}));

        let backend = FlutterPbtBackend::new(
            "test-1".to_string(),
            get_blocks_callback,
            create_callback,
            update_callback,
            delete_callback,
            move_callback,
        );

        // Create should return immediately
        let result = backend
            .create_block(None, "Test".to_string(), Some("id-1".to_string()))
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().id, "id-1");
    }
}
