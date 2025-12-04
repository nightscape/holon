//! Directory entity for filesystem hierarchies

use async_trait::async_trait;
use futures::stream;
use holon::core::datasource::MaybeSendSync;
use holon_macros::Entity;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::Stream;

use holon::core::datasource::{
    CrudOperations, DataSource, OperationDescriptor, OperationProvider, OperationRegistry,
    RenameOperations, Result, UndoAction,
};
use holon::storage::types::StorageEntity;
use holon_api::streaming::ChangeNotifications;
use holon_api::{ApiError, Change, StreamPosition};
use holon_api::{BatchMetadata, Value, WithMetadata};

/// Synthetic root ID for top-level directories
pub const ROOT_ID: &str = "null";

/// Directory - represents a folder in a directory hierarchy
#[derive(Debug, Clone, Serialize, Deserialize, Entity)]
#[entity(name = "directories", short_name = "dir")]
pub struct Directory {
    #[primary_key]
    #[indexed]
    pub id: String,

    /// Directory name (relative to parent)
    pub name: String,

    /// Parent directory ID (ROOT_ID for top-level directories)
    #[indexed]
    pub parent_id: String,

    /// Nesting level from root (0 for root children)
    pub depth: i64,
}

impl Directory {
    pub fn new(id: String, name: String, parent_id: String, depth: i64) -> Self {
        Self {
            id,
            name,
            parent_id,
            depth,
        }
    }
}

/// Move operations (for entities with hierarchical structure)
///
/// This trait provides a move operation for entities that can be moved within
/// a hierarchical structure, such as directories, files, or blocks.
#[holon_macros::operations_trait]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait DirectoryOperations: MaybeSendSync {
    #[holon_macros::affects("id", "parent_id", "depth")]
    #[holon_macros::triggered_by(availability_of = "dir_id", providing = ["parent_id"])]
    async fn move_directory(&self, id: &str, parent_id: &str) -> Result<UndoAction>;
}

impl holon::core::datasource::BlockEntity for Directory {
    fn id(&self) -> &str {
        &self.id
    }

    fn parent_id(&self) -> Option<&str> {
        Some(&self.parent_id)
    }

    fn sort_key(&self) -> &str {
        &self.name
    }

    fn depth(&self) -> i64 {
        self.depth
    }

    fn content(&self) -> &str {
        &self.name
    }
}

impl holon::core::datasource::OperationRegistry for Directory {
    fn all_operations() -> Vec<holon::core::datasource::OperationDescriptor> {
        let entity_name = Self::entity_name();
        let short_name = Self::short_name().expect("Directory must have short_name");
        let table = entity_name;
        let id_column = "id";

        #[cfg(not(target_arch = "wasm32"))]
        {
            use holon::core::datasource::{
                __operations_block_operations, __operations_crud_operations,
                __operations_move_operations, __operations_rename_operations,
            };
            __operations_crud_operations::crud_operations(entity_name, short_name, table, id_column)
                .into_iter()
                .chain(
                    __operations_block_operations::block_operations(
                        entity_name,
                        short_name,
                        table,
                        id_column,
                    )
                    .into_iter(),
                )
                .chain(
                    __operations_rename_operations::rename_operations(
                        entity_name,
                        short_name,
                        table,
                        id_column,
                    )
                    .into_iter(),
                )
                .chain(
                    __operations_move_operations::move_operations(
                        entity_name,
                        short_name,
                        table,
                        id_column,
                    )
                    .into_iter(),
                )
                .collect()
        }
        #[cfg(target_arch = "wasm32")]
        {
            Vec::new()
        }
    }

    fn entity_name() -> &'static str {
        "directories"
    }

    fn short_name() -> Option<&'static str> {
        Directory::short_name()
    }
}

/// Changes wrapped with metadata for atomic sync token updates
pub type ChangesWithMetadata<T> = WithMetadata<Vec<Change<T>>, BatchMetadata>;

/// Trait for providers that can supply directory change streams
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait DirectoryChangeProvider: Send + Sync {
    fn subscribe_directories(&self) -> broadcast::Receiver<ChangesWithMetadata<Directory>>;

    /// Get the root directory path for this provider
    fn root_directory(&self) -> std::path::PathBuf;
}

/// DataSource for Directory
pub struct DirectoryDataSource<P> {
    provider: Arc<P>,
}

impl<P> DirectoryDataSource<P> {
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }
}

// Helper trait to allow calling DirectoryChangeProvider methods on Arc<P>
trait AsDirectoryChangeProvider {
    fn subscribe_directories_inner(&self) -> broadcast::Receiver<ChangesWithMetadata<Directory>>;
}

impl<P: DirectoryChangeProvider> AsDirectoryChangeProvider for Arc<P> {
    fn subscribe_directories_inner(&self) -> broadcast::Receiver<ChangesWithMetadata<Directory>> {
        (**self).subscribe_directories()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<P: DirectoryChangeProvider> ChangeNotifications<Directory> for DirectoryDataSource<P> {
    async fn watch_changes_since(
        &self,
        _position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<Vec<Change<Directory>>, ApiError>> + Send>>
    {
        let rx = DirectoryChangeProvider::subscribe_directories(&*self.provider);

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
impl<P: DirectoryChangeProvider> DataSource<Directory> for DirectoryDataSource<P> {
    async fn get_all(&self) -> Result<Vec<Directory>> {
        // Directories are populated via sync, not direct queries
        Ok(vec![])
    }

    async fn get_by_id(&self, _id: &str) -> Result<Option<Directory>> {
        Ok(None)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<P: DirectoryChangeProvider> CrudOperations<Directory> for DirectoryDataSource<P> {
    async fn set_field(&self, _id: &str, _field: &str, _value: Value) -> Result<UndoAction> {
        // Directory modifications not supported yet
        Err("Directory field updates not implemented".into())
    }

    async fn create(&self, _fields: HashMap<String, Value>) -> Result<(String, UndoAction)> {
        Err("Directory creation not implemented".into())
    }

    async fn delete(&self, _id: &str) -> Result<UndoAction> {
        Err("Directory deletion not implemented".into())
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<P: DirectoryChangeProvider> RenameOperations<Directory> for DirectoryDataSource<P> {
    async fn rename(&self, id: &str, name: String) -> Result<UndoAction> {
        self.set_field(id, "name", Value::String(name)).await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<P: DirectoryChangeProvider> DirectoryOperations for DirectoryDataSource<P> {
    async fn move_directory(&self, id: &str, parent_id: &str) -> Result<UndoAction> {
        use std::path::PathBuf;
        use tokio::fs;

        let root_dir = self.provider.root_directory();

        // Directory ID is the relative path from root
        let source_rel_path = PathBuf::from(id);
        let source_path = root_dir.join(&source_rel_path);

        // Resolve target parent directory path
        let target_parent_path = if parent_id == crate::directory::ROOT_ID {
            root_dir.clone()
        } else {
            // Parent ID is also a relative path from root
            root_dir.join(parent_id)
        };

        // Get the directory name from the source relative path
        let dir_name = source_rel_path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("Source path has no file name"))?;

        // Construct target path (relative to root)
        let target_rel_path = if parent_id == crate::directory::ROOT_ID {
            PathBuf::from(dir_name)
        } else {
            PathBuf::from(parent_id).join(dir_name)
        };
        let target_path = root_dir.join(&target_rel_path);

        // Check if source exists
        if !source_path.exists() {
            return Err(anyhow::anyhow!(
                "Source directory does not exist: {}",
                source_path.display()
            )
            .into());
        }

        // Check if target already exists
        if target_path.exists() {
            return Err(anyhow::anyhow!(
                "Target directory already exists: {}",
                target_path.display()
            )
            .into());
        }

        // Ensure target parent directory exists
        if !target_parent_path.exists() {
            return Err(anyhow::anyhow!(
                "Target parent directory does not exist: {}",
                target_parent_path.display()
            )
            .into());
        }

        // Perform the filesystem move operation
        fs::rename(&source_path, &target_path).await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to move directory from {} to {}: {}",
                source_path.display(),
                target_path.display(),
                e
            )
        })?;

        // Return inverse operation (move back to old parent)
        // Note: Directory operations are not undoable via inverse operations
        // since they involve filesystem operations
        Ok(UndoAction::Irreversible)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<P: DirectoryChangeProvider> OperationProvider for DirectoryDataSource<P> {
    fn operations(&self) -> Vec<OperationDescriptor> {
        Directory::all_operations()
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        _op_name: &str,
        _params: StorageEntity,
    ) -> Result<UndoAction> {
        if entity_name != "directories" {
            return Err(
                format!("Expected entity_name 'directories', got '{}'", entity_name).into(),
            );
        }
        Err("Directory operations not implemented".into())
    }
}
