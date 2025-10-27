//! Trait abstracting Todoist API operations.
//!
//! This trait allows swapping between the real `TodoistClient` (HTTP) and
//! `TodoistFakeClient` (in-memory) implementations, enabling the same DI
//! path for both production and testing.

use async_trait::async_trait;

use crate::models::{CreateTaskRequest, SyncResponse, TodoistTaskApiResponse, UpdateTaskRequest};

/// Result type for API client operations
pub type ApiResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Trait abstracting Todoist API operations.
///
/// Implemented by:
/// - `TodoistClient` - Real HTTP client for production
/// - `TodoistFakeClient` - In-memory client for testing
///
/// This allows the `TodoistSyncProvider` to use either implementation
/// while keeping the same code path, ensuring the DDL and wiring are
/// identical between production and test environments.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait TodoistApiClient: Send + Sync {
    /// Sync items (tasks) from Todoist API
    ///
    /// - `sync_token`: Token from previous sync, or None for full sync
    /// - Returns: SyncResponse with items and new sync_token
    async fn sync_items(&self, sync_token: Option<&str>) -> ApiResult<SyncResponse>;

    /// Sync projects from Todoist API
    ///
    /// - `sync_token`: Token from previous sync, or None for full sync
    /// - Returns: JSON value containing projects array
    async fn sync_projects(&self, sync_token: Option<&str>) -> ApiResult<serde_json::Value>;

    /// Create a new task
    async fn create_task(
        &self,
        request: &CreateTaskRequest<'_>,
    ) -> ApiResult<TodoistTaskApiResponse>;

    /// Update an existing task
    async fn update_task(&self, task_id: &str, request: &UpdateTaskRequest<'_>) -> ApiResult<()>;

    /// Close (complete) a task
    async fn close_task(&self, task_id: &str) -> ApiResult<()>;

    /// Reopen a closed task
    async fn reopen_task(&self, task_id: &str) -> ApiResult<()>;

    /// Delete a task
    async fn delete_task(&self, task_id: &str) -> ApiResult<()>;

    /// Move a task to a different parent, project, or section
    async fn move_task(
        &self,
        task_id: &str,
        parent_id: Option<&str>,
        project_id: Option<&str>,
        section_id: Option<&str>,
    ) -> ApiResult<()>;

    /// Create a new project
    ///
    /// Returns the project ID
    async fn create_project(&self, name: &str) -> ApiResult<String>;

    /// Move a project under another project (or to root if parent_id is None)
    async fn move_project(&self, project_id: &str, parent_id: Option<&str>) -> ApiResult<()>;

    /// Delete a project
    async fn delete_project(&self, project_id: &str) -> ApiResult<()>;

    /// Archive a project and its descendants
    async fn archive_project(&self, project_id: &str) -> ApiResult<()>;

    /// Unarchive a project
    async fn unarchive_project(&self, project_id: &str) -> ApiResult<()>;
}
