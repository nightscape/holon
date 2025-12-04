//! Stream-based TodoistSyncProvider with builder pattern
//!
//! This sync provider polls the Todoist API and emits changes on typed streams.
//! Architecture:
//! - ONE sync() call → multiple typed streams (tasks, projects)
//! - Builder pattern for registering caches
//! - Fire-and-forget operations - updates arrive via streams
//! - Sync tokens are included in batch metadata for atomic updates

use async_trait::async_trait;
use tokio::sync::broadcast;

use holon::core::datasource::{
    generate_sync_operation, Change, ChangeOrigin, OperationDescriptor, OperationProvider, Result,
    StreamPosition, SyncTokenStore, SyncableProvider, UndoAction,
};
use holon::storage::types::StorageEntity;
use holon_api::{BatchMetadata, SyncTokenUpdate, WithMetadata};
use std::sync::Arc;

use crate::client::TodoistClient;
use crate::models::{
    SyncResponse, TodoistProject, TodoistProjectApiResponse, TodoistTask, TodoistTaskApiResponse,
};

/// Changes wrapped with metadata for atomic sync token updates
pub type ChangesWithMetadata<T> = WithMetadata<Vec<Change<T>>, BatchMetadata>;

/// Stream-based TodoistSyncProvider that polls API and emits changes on typed streams
///
/// Architecture:
/// - sync() makes ONE API call → splits into task and project changes → emits on separate streams
/// - Builder pattern for registering caches
/// - Sync token is included in batch metadata for atomic updates in QueryableCache
pub struct TodoistSyncProvider {
    pub(crate) client: TodoistClient,
    token_store: Arc<dyn SyncTokenStore>,
    task_tx: broadcast::Sender<ChangesWithMetadata<TodoistTask>>,
    project_tx: broadcast::Sender<ChangesWithMetadata<TodoistProject>>,
}

impl TodoistSyncProvider {
    pub fn new(client: TodoistClient, token_store: Arc<dyn SyncTokenStore>) -> Self {
        Self {
            client,
            token_store,
            task_tx: broadcast::channel(1000).0,
            project_tx: broadcast::channel(1000).0,
        }
    }

    /// Get a receiver for task changes (for testing or manual wiring)
    pub fn subscribe_tasks(&self) -> broadcast::Receiver<ChangesWithMetadata<TodoistTask>> {
        self.task_tx.subscribe()
    }

    /// Get a receiver for project changes (for testing or manual wiring)
    pub fn subscribe_projects(&self) -> broadcast::Receiver<ChangesWithMetadata<TodoistProject>> {
        self.project_tx.subscribe()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl SyncableProvider for TodoistSyncProvider {
    fn provider_name(&self) -> &str {
        "todoist"
    }

    /// Trigger sync - ONE API call, emits on multiple streams
    ///
    /// This method:
    /// 1. Loads current token from token store
    /// 2. Calls sync_items() API (returns tasks + projects in one response)
    /// 3. Splits response into task and project changes
    /// 4. Emits changes on separate typed streams
    /// 5. Saves new token to token store
    /// 6. Returns the new stream position
    #[tracing::instrument(name = "provider.todoist.sync", skip(self, _position))]
    async fn sync(&self, _position: StreamPosition) -> Result<StreamPosition> {
        use tracing::info;

        // Note: Using #[instrument] instead of manual span creation ensures
        // the span is a proper child of the current span, inheriting OTel context
        {
            // Load current token from token store (ignore passed position parameter)
            let current_position = self
                .token_store
                .load_token(self.provider_name())
                .await?
                .unwrap_or(StreamPosition::Beginning);

            // Extract sync token from StreamPosition
            let token_str = match &current_position {
                StreamPosition::Beginning => None, // Full sync
                StreamPosition::Version(bytes) => std::str::from_utf8(bytes).ok(),
            };

            // Make API calls to get tasks and projects
            // Note: Todoist Sync API requires separate calls for items vs projects
            // Span context will be propagated via tracing-opentelemetry bridge
            let response = self.client.sync_items(token_str).await?;

            // Also fetch projects (using same sync token for consistency)
            let project_response = self.client.sync_projects(token_str).await?;

            // Split and emit on separate typed streams
            let task_changes = compute_task_changes(&response);
            let project_changes = compute_project_changes(&project_response);

            let task_count = task_changes.len();
            let project_count = project_changes.len();

            // Count change types for tasks
            let mut task_created = 0;
            let mut task_updated = 0;
            let mut task_deleted = 0;
            for change in &task_changes {
                match change {
                    holon_api::Change::Created { .. } => task_created += 1,
                    holon_api::Change::Updated { .. } => task_updated += 1,
                    holon_api::Change::Deleted { .. } => task_deleted += 1,
                }
            }

            // Count change types for projects
            let mut project_created = 0;
            let mut project_updated = 0;
            let mut project_deleted = 0;
            for change in &project_changes {
                match change {
                    holon_api::Change::Created { .. } => project_created += 1,
                    holon_api::Change::Updated { .. } => project_updated += 1,
                    holon_api::Change::Deleted { .. } => project_deleted += 1,
                }
            }

            // Record OpenTelemetry attributes on the span
            use tracing::Span;
            Span::current().record("sync.task_count", task_count);
            Span::current().record("sync.task_created", task_created);
            Span::current().record("sync.task_updated", task_updated);
            Span::current().record("sync.task_deleted", task_deleted);
            Span::current().record("sync.project_count", project_count);
            Span::current().record("sync.project_created", project_created);
            Span::current().record("sync.project_updated", project_updated);
            Span::current().record("sync.project_deleted", project_deleted);

            // Determine new position from sync token
            let new_position = match response.sync_token {
                Some(token) => StreamPosition::Version(token.as_bytes().to_vec()),
                None => StreamPosition::Beginning, // Fallback - shouldn't happen
            };

            // Create sync token update to be saved atomically with data
            let sync_token_update = SyncTokenUpdate {
                provider_name: self.provider_name().to_string(),
                position: new_position.clone(),
            };

            // Extract trace context from current span for propagation through broadcast channel
            let trace_context = holon_api::BatchTraceContext::from_current_span();
            tracing::info!(
                "[TodoistSyncProvider] Extracted trace_context: {:?}",
                trace_context
            );

            // Create metadata with sync token for atomic updates
            let task_metadata = BatchMetadata {
                relation_name: "todoist_tasks".to_string(),
                trace_context: trace_context.clone(),
                sync_token: Some(sync_token_update.clone()),
            };

            let project_metadata = BatchMetadata {
                relation_name: "todoist_projects".to_string(),
                trace_context,
                sync_token: Some(sync_token_update),
            };

            // Wrap changes with metadata
            let task_batch = WithMetadata {
                inner: task_changes,
                metadata: task_metadata,
            };

            let project_batch = WithMetadata {
                inner: project_changes,
                metadata: project_metadata,
            };

            // Emit changes (fire-and-forget - ignore errors if no receivers)
            info!(
                "[TodoistSyncProvider] Emitting {} task changes (created={}, updated={}, deleted={}) and {} project changes (created={}, updated={}, deleted={})",
                task_count,
                task_created,
                task_updated,
                task_deleted,
                project_count,
                project_created,
                project_updated,
                project_deleted
            );
            let send_result = self.task_tx.send(task_batch);
            if send_result.is_err() {
                info!(
                    "[TodoistSyncProvider] No receivers for task changes (this is ok if no subscribers)"
                );
            }
            let _ = self.project_tx.send(project_batch);

            // Log sync completion
            info!(
                "[TodoistSyncProvider] Sync completed successfully: {} task changes, {} project changes",
                task_count, project_count
            );

            // NOTE: Sync token is NOT saved here anymore - it will be saved atomically
            // with the data changes in QueryableCache.ingest_stream_with_metadata()

            Ok(new_position)
        }
    }
}

// NOTE: StreamProvider trait implementations removed - using direct subscribe_tasks()
// and subscribe_projects() methods with ChangesWithMetadata type instead.
// This allows sync tokens to be included in batch metadata for atomic updates.

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OperationProvider for TodoistSyncProvider {
    fn operations(&self) -> Vec<OperationDescriptor> {
        // Provide sync operation for this provider
        vec![generate_sync_operation(self.provider_name())]
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        _params: StorageEntity,
    ) -> Result<UndoAction> {
        // Validate this is the sync operation for this provider
        let expected_entity_name = format!("{}.sync", self.provider_name());
        if entity_name != expected_entity_name {
            return Err(format!(
                "Expected entity_name '{}', got '{}'",
                expected_entity_name, entity_name
            )
            .into());
        }

        if op_name != "sync" {
            return Err(format!("Expected op_name 'sync', got '{}'", op_name).into());
        }

        // Execute sync (position parameter is ignored, sync loads from token store)
        self.sync(StreamPosition::Beginning).await?;
        Ok(UndoAction::Irreversible)
    }
}

/// Compute task changes from sync response
///
/// Converts API responses to Change<TodoistTask> enum variants.
/// Handles both updates and deletions.
fn compute_task_changes(response: &SyncResponse) -> Vec<Change<TodoistTask>> {
    // Capture trace context ONCE for all changes in this batch
    // This ensures all changes in the sync batch share the same trace context
    let origin = ChangeOrigin::remote_with_current_span();

    response
        .items
        .iter()
        .map(|api_item| {
            // Check if item is deleted
            if api_item.is_deleted.unwrap_or(false) {
                // Emit deletion event
                return Change::Deleted {
                    id: api_item.id.clone(),
                    origin: origin.clone(),
                };
            }

            // Clone the API response to convert it
            let api_item_cloned = TodoistTaskApiResponse {
                id: api_item.id.clone(),
                content: api_item.content.clone(),
                description: api_item.description.clone(),
                project_id: api_item.project_id.clone(),
                section_id: api_item.section_id.clone(),
                parent_id: api_item.parent_id.clone(),
                checked: api_item.checked,
                priority: api_item.priority,
                due: api_item.due.as_ref().map(|d| crate::models::TodoistDue {
                    date: d.date.clone(),
                    timezone: d.timezone.clone(),
                    string: d.string.clone(),
                    is_recurring: d.is_recurring,
                }),
                labels: api_item.labels.clone(),
                added_at: api_item.added_at.clone(),
                updated_at: api_item.updated_at.clone(),
                completed_at: api_item.completed_at.clone(),
                is_deleted: api_item.is_deleted,
            };
            let task: TodoistTask = TodoistTask::from(api_item_cloned);
            // Todoist sync API doesn't distinguish create vs update, so use Updated for both
            Change::Updated {
                id: task.id.clone(),
                data: task,
                origin: origin.clone(),
            }
        })
        .collect()
}

/// Compute project changes from sync_projects() response
///
/// Parses the JSON response from sync_projects() and converts to Change<TodoistProject> variants.
/// Handles both updates and deletions.
fn compute_project_changes(response: &serde_json::Value) -> Vec<Change<TodoistProject>> {
    // Capture trace context ONCE for all changes in this batch
    let origin = ChangeOrigin::remote_with_current_span();

    // Extract projects array from response
    let projects_array = match response.get("projects").and_then(|p| p.as_array()) {
        Some(arr) => arr,
        None => {
            tracing::warn!("[compute_project_changes] No projects array in response");
            return vec![];
        }
    };

    projects_array
        .iter()
        .filter_map(|project_json| {
            // Try to parse as TodoistProjectApiResponse
            match serde_json::from_value::<TodoistProjectApiResponse>(project_json.clone()) {
                Ok(api_project) => {
                    // Check if project is deleted
                    if api_project.is_deleted.unwrap_or(false) {
                        return Some(Change::Deleted {
                            id: api_project.id.clone(),
                            origin: origin.clone(),
                        });
                    }

                    // Convert to TodoistProject and emit as Updated
                    // (Todoist sync API doesn't distinguish create vs update)
                    let project = TodoistProject::from(api_project);
                    Some(Change::Updated {
                        id: project.id.clone(),
                        data: project,
                        origin: origin.clone(),
                    })
                }
                Err(e) => {
                    tracing::warn!("[compute_project_changes] Failed to parse project: {}", e);
                    None
                }
            }
        })
        .collect()
}
