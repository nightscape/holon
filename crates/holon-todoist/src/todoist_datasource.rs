//! Real Todoist datasource implementation for stream-based architecture
//!
//! TodoistTaskOperations implements CrudOperations<TodoistTask> and TaskOperations<TodoistTask>:
//! - Uses QueryableCache for fast lookups (data populated via change streams)
//! - Makes HTTP calls to Todoist API for mutations only
//! - Returns immediately (fire-and-forget)
//! - Changes arrive via TodoistSyncProvider stream into the cache

use async_trait::async_trait;
use holon::core::datasource::{
    __operations_crud_operation_provider, __operations_mutable_block_data_source,
    __operations_mutable_task_data_source, BlockDataSourceHelpers, BlockMaintenanceHelpers,
    BlockOperations, BlockQueryHelpers, CompletionStateInfo, CrudOperations, DataSource,
    OperationDescriptor, OperationProvider, OperationRegistry, OperationResult, Result, TaskEntity,
    TaskOperations, UndoAction, UnknownOperationError,
};
use holon::core::queryable_cache::QueryableCache;
use holon::storage::types::StorageEntity;
use holon_api::streaming::ChangeNotifications;
use holon_api::{ApiError, Change, StreamPosition};
use holon_api::{OperationParam, ParamMapping, TypeHint, Value};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use crate::models::{
    CreateTaskRequest, TodoistProject, TodoistProjectApiResponse, TodoistTask, UpdateTaskRequest,
};

use super::todoist_sync_provider::TodoistSyncProvider;
use tokio::sync::broadcast;
use tokio_stream::Stream;
use tracing::{debug, error, info};

/// Todoist-specific move operations that use entity-typed parameters
///
/// These operations are triggered by entity-typed params (e.g., `project_id`, `task_id`)
/// rather than generic params like `parent_id`. This allows automatic operation matching
/// based on the drop target's entity type.
#[holon_macros::operations_trait]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait TodoistMoveOperations: Send + Sync {
    /// Move a task to a project (at root level, not under another task)
    #[holon_macros::affects("project_id", "parent_id")]
    #[holon_macros::triggered_by(availability_of = "project_id")]
    async fn move_to_project(&self, id: &str, project_id: &str) -> Result<OperationResult>;

    /// Move a task under another task (as a subtask)
    #[holon_macros::affects("parent_id")]
    #[holon_macros::triggered_by(availability_of = "task_id")]
    async fn move_under_task(&self, id: &str, task_id: &str) -> Result<OperationResult>;
}

/// Operations for TodoistTask
///
/// This struct uses a QueryableCache for lookups and TodoistSyncProvider for API mutations.
/// Data flows into the cache via change streams; this struct only performs lookups and mutations.
pub struct TodoistTaskOperations {
    cache: Arc<QueryableCache<TodoistTask>>,
    provider: Arc<TodoistSyncProvider>,
}

impl TodoistTaskOperations {
    pub fn new(
        cache: Arc<QueryableCache<TodoistTask>>,
        provider: Arc<TodoistSyncProvider>,
    ) -> Self {
        Self { cache, provider }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TodoistMoveOperations for TodoistTaskOperations {
    async fn move_to_project(&self, id: &str, project_id: &str) -> Result<OperationResult> {
        info!(
            "[TodoistTaskOperations] move_to_project: task {} -> project {}",
            id, project_id
        );

        // Capture old state for inverse operation
        let old_task = self
            .cache
            .get_by_id(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Task not found"))?;
        let old_project_id = old_task.project_id.clone();
        let old_parent_id = old_task.parent_id.clone();

        self.provider
            .client
            .move_task(id, None, Some(project_id), None)
            .await?;

        if let Some(old_parent_id) = old_parent_id {
            Ok(OperationResult::from_undo(UndoAction::Undo(
                __operations_todoist_move_operations::move_under_task_op(
                    "", // Will be set by OperationProvider
                    id,
                    &old_parent_id,
                ),
            )))
        } else {
            Ok(OperationResult::from_undo(UndoAction::Undo(
                __operations_todoist_move_operations::move_to_project_op(
                    "", // Will be set by OperationProvider
                    id,
                    &old_project_id,
                ),
            )))
        }
    }

    async fn move_under_task(&self, id: &str, task_id: &str) -> Result<OperationResult> {
        info!(
            "[TodoistTaskOperations] move_under_task: task {} -> parent task {}",
            id, task_id
        );

        // Capture old state for inverse operation
        let old_task = self
            .cache
            .get_by_id(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Task not found"))?;
        let old_parent_id = old_task.parent_id.clone();
        let old_project_id = old_task.project_id.clone();

        self.provider
            .client
            .move_task(id, Some(task_id), None, None)
            .await?;

        // Return inverse operation - restore old parent or move to project
        // The macro generates __operations_todoist_move_operations module (in same file)
        if let Some(old_parent) = &old_parent_id {
            Ok(OperationResult::from_undo(UndoAction::Undo(
                __operations_todoist_move_operations::move_under_task_op(
                    "", // Will be set by OperationProvider
                    id, old_parent,
                ),
            )))
        } else {
            // Was at root level, restore to project
            Ok(OperationResult::from_undo(UndoAction::Undo(
                __operations_todoist_move_operations::move_to_project_op(
                    "", // Will be set by OperationProvider
                    id,
                    &old_project_id,
                ),
            )))
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TaskOperations<TodoistTask> for TodoistTaskOperations {
    async fn set_title(&self, id: &str, content: &str) -> Result<OperationResult> {
        self.set_field(id, "content", Value::String(content.to_string()))
            .await
    }
    fn completion_states_with_progress(&self) -> Vec<CompletionStateInfo> {
        vec![
            CompletionStateInfo {
                state: "active".into(),
                progress: 0.0,
                is_done: false,
                is_active: true,
            },
            CompletionStateInfo {
                state: "completed".into(),
                progress: 100.0,
                is_done: true,
                is_active: false,
            },
        ]
    }

    async fn set_state(&self, id: &str, task_state: String) -> Result<OperationResult> {
        info!(
            "[TodoistTaskOperations] set_state: id={}, state={}",
            id, task_state
        );

        // Capture old state before mutation
        let old_task = self
            .cache
            .get_by_id(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Task not found"))?;
        let old_state = if old_task.completed {
            "completed"
        } else {
            "active"
        }
        .to_string();

        // For Todoist, "completed" means close task, anything else means reopen
        let completed = task_state == "completed";

        // Execute API call
        let result = if completed {
            debug!("[TodoistTaskOperations] Closing task");
            self.provider.client.close_task(id).await
        } else {
            debug!("[TodoistTaskOperations] Reopening task");
            self.provider.client.reopen_task(id).await
        };

        match &result {
            Ok(_) => {
                info!(
                    "[TodoistTaskOperations] set_state succeeded: id={}, task_state={}",
                    id, task_state
                );
            }
            Err(e) => {
                error!(
                    "[TodoistTaskOperations] set_state failed: id={}, task_state={}, error={}",
                    id, task_state, e
                );
            }
        }

        // Note: sync is now handled automatically by OperationWrapper

        // Return inverse operation
        result.map(|_| {
            use holon::core::datasource::__operations_mutable_task_data_source;
            OperationResult::from_undo(UndoAction::Undo(
                __operations_mutable_task_data_source::set_state_op(
                    "", // Will be set by OperationProvider
                    id, old_state,
                ),
            ))
        })
    }

    async fn set_priority(&self, id: &str, priority: i64) -> Result<OperationResult> {
        info!(
            "[TodoistTaskOperations] set_priority: id={}, priority={}",
            id, priority
        );

        // Capture old priority value before mutation
        let old_task = self
            .cache
            .get_by_id(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Task not found"))?;
        let old_priority = old_task.priority as i64;

        // Execute API call
        let request = UpdateTaskRequest {
            priority: Some(priority as i32),
            ..Default::default()
        };
        let result = self.provider.client.update_task(id, &request).await;

        match &result {
            Ok(_) => {
                info!(
                    "[TodoistTaskOperations] set_priority succeeded: id={}, priority={}",
                    id, priority
                );
            }
            Err(e) => {
                error!(
                    "[TodoistTaskOperations] set_priority failed: id={}, priority={}, error={}",
                    id, priority, e
                );
            }
        }

        // Note: sync is now handled automatically by OperationWrapper

        // Return inverse operation
        result.map(|_| {
            use holon::core::datasource::__operations_mutable_task_data_source;
            OperationResult::from_undo(UndoAction::Undo(
                __operations_mutable_task_data_source::set_priority_op(
                    "", // Will be set by OperationProvider
                    id,
                    old_priority,
                ),
            ))
        })
    }

    async fn set_due_date(
        &self,
        id: &str,
        due_date: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<OperationResult> {
        info!(
            "[TodoistTaskOperations] set_due_date: id={}, due_date={:?}",
            id, due_date
        );

        // Capture old due_date value before mutation
        let old_task = self
            .cache
            .get_by_id(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Task not found"))?;
        let old_due_date = <TodoistTask as TaskEntity>::due_date(&old_task);

        // Convert Option<DateTime<Utc>> to Todoist date string format
        let due_str = match due_date {
            Some(dt) => Some(dt.to_rfc3339()),
            None => Some("no date".to_string()),
        };

        // Execute API call
        let result = if let Some(d) = due_str {
            let request = UpdateTaskRequest {
                due_string: Some(&d),
                ..Default::default()
            };
            self.provider.client.update_task(id, &request).await
        } else {
            Ok(())
        };

        match &result {
            Ok(_) => {
                info!(
                    "[TodoistTaskOperations] set_due_date succeeded: id={}, due_date={:?}",
                    id, due_date
                );
            }
            Err(e) => {
                error!(
                    "[TodoistTaskOperations] set_due_date failed: id={}, due_date={:?}, error={}",
                    id, due_date, e
                );
            }
        }

        // Note: sync is now handled automatically by OperationWrapper

        // Return inverse operation
        result.map(|_| {
            use holon::core::datasource::__operations_mutable_task_data_source;
            OperationResult::from_undo(UndoAction::Undo(
                __operations_mutable_task_data_source::set_due_date_op(
                    "", // Will be set by OperationProvider
                    id,
                    old_due_date,
                ),
            ))
        })
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ChangeNotifications<TodoistTask> for TodoistTaskOperations {
    async fn watch_changes_since(
        &self,
        _position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<Vec<Change<TodoistTask>>, ApiError>> + Send>>
    {
        let rx = self.provider.subscribe_tasks();

        // Convert broadcast receiver to stream, extracting inner changes from metadata wrapper
        // Note: The sync token in metadata is handled by QueryableCache.ingest_stream_with_metadata()
        let change_stream = futures::stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(batch_with_metadata) => {
                    // Extract inner changes from metadata wrapper
                    Some((Ok(batch_with_metadata.inner), rx))
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    eprintln!("Stream lagged by {} messages", n);
                    Some((
                        Err(ApiError::InternalError {
                            message: format!("Stream lagged by {} messages", n),
                        }),
                        rx,
                    ))
                }
                Err(broadcast::error::RecvError::Closed) => None,
            }
        });

        Box::pin(change_stream)
    }

    async fn get_current_version(&self) -> std::result::Result<Vec<u8>, ApiError> {
        // Note: Sync tokens are now managed externally (by OperationDispatcher or caller)
        // This method should return the current version from the dispatcher or database
        // For now, return empty vec - the version should be retrieved from OperationDispatcher
        // TODO: Get sync token from OperationDispatcher or database
        Ok(Vec::new())
    }
}

// DataSource implementation delegates to the cache for fast lookups
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl holon::core::datasource::DataSource<TodoistTask> for TodoistTaskOperations {
    async fn get_all(&self) -> Result<Vec<TodoistTask>> {
        self.cache.get_all().await
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<TodoistTask>> {
        self.cache.get_by_id(id).await
    }
}

impl BlockQueryHelpers<TodoistTask> for TodoistTaskOperations {}
impl BlockMaintenanceHelpers<TodoistTask> for TodoistTaskOperations {}
impl BlockDataSourceHelpers<TodoistTask> for TodoistTaskOperations {}
impl BlockOperations<TodoistTask> for TodoistTaskOperations {}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl CrudOperations<TodoistTask> for TodoistTaskOperations {
    async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<OperationResult> {
        use tracing::{debug, error, info};

        info!(
            "[TodoistTaskOperations] set_field: id={}, field={}, value={:?}",
            id, field, value
        );

        // Capture old value for inverse operation
        let old_value = self
            .cache
            .get_by_id(id)
            .await?
            .and_then(|task| match field {
                "content" => Some(Value::String(task.content.clone())),
                "description" => task
                    .description
                    .as_ref()
                    .map(|d| Value::String(d.clone()))
                    .or(Some(Value::Null)),
                "parent_id" => task
                    .parent_id
                    .as_ref()
                    .map(|p| Value::String(p.clone()))
                    .or(Some(Value::Null)),
                _ => None,
            })
            .unwrap_or(Value::Null);

        let result = match field {
            "content" => {
                debug!("[TodoistTaskOperations] Updating content field");
                if let Value::String(s) = value {
                    let request = UpdateTaskRequest {
                        content: Some(&s),
                        ..Default::default()
                    };
                    self.provider.client.update_task(id, &request).await
                } else {
                    Ok(())
                }
            }
            "description" => {
                debug!("[TodoistTaskOperations] Updating description field");
                let desc = match value {
                    Value::String(s) => Some(s),
                    Value::Null => Some("no description".to_string()),
                    _ => None,
                };
                if let Some(d) = desc {
                    let request = UpdateTaskRequest {
                        description: Some(&d),
                        ..Default::default()
                    };
                    self.provider.client.update_task(id, &request).await
                } else {
                    Ok(())
                }
            }
            "parent_id" => {
                debug!("[TodoistTaskOperations] Updating parent_id field");
                let current = self
                    .cache
                    .get_by_id(id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("Task not found"))?;
                let project_id = current.project_id.as_str();
                let section_id = current.section_id.as_deref();
                match value {
                    Value::String(s) => {
                        debug!("[TodoistTaskOperations] Moving task to parent: {}", s);
                        self.provider
                            .client
                            .move_task(id, Some(&s), Some(project_id), section_id)
                            .await
                    }
                    Value::Null => {
                        debug!("[TodoistTaskOperations] Removing parent from task");
                        self.provider
                            .client
                            .move_task(id, None, Some(project_id), section_id)
                            .await
                    }
                    _ => {
                        error!("[TodoistTaskOperations] Invalid value type for parent_id");
                        return Err("Invalid value type for parent_id".into());
                    }
                }
            }
            "depth" | "sort_key" => {
                // Local-only metadata fields (used for ordering). Todoist
                // does not expose these via the API, so we treat them as
                // successful no-ops to keep the generic operations flowing.
                debug!(
                    "[TodoistTaskOperations] Field '{}' is local-only, skipping API call",
                    field
                );
                Ok(())
            }
            _ => {
                error!("[TodoistTaskOperations] Field '{}' not supported", field);
                return Err(format!("Field {} not supported", field).into());
            }
        };

        match &result {
            Ok(_) => {
                info!(
                    "[TodoistTaskOperations] set_field succeeded: id={}, field={}",
                    id, field
                );
            }
            Err(e) => {
                error!(
                    "[TodoistTaskOperations] set_field failed: id={}, field={}, error={}",
                    id, field, e
                );
            }
        }

        // Note: sync is now handled automatically by OperationWrapper

        // Return inverse operation
        result.map(|_| {
            use holon::core::datasource::__operations_crud_operation_provider;
            OperationResult::from_undo(UndoAction::Undo(
                __operations_crud_operation_provider::set_field_op(
                    "", // Will be set by OperationProvider
                    id, field, old_value,
                ),
            ))
        })
    }

    async fn create(&self, fields: HashMap<String, Value>) -> Result<(String, OperationResult)> {
        let content = fields
            .get("content")
            .and_then(|v| v.as_string().map(|s| s.to_string()))
            .ok_or_else(|| "Missing content field".to_string())?;
        let description = fields
            .get("description")
            .and_then(|v| v.as_string().map(|s| s.to_string()));
        let project_id = fields
            .get("project_id")
            .and_then(|v| v.as_string().map(|s| s.to_string()))
            .ok_or_else(|| "Missing project_id field".to_string())?;
        let due_string = fields
            .get("due_date")
            .and_then(|v| v.as_string().map(|s| s.to_string()));
        let priority = fields
            .get("priority")
            .and_then(|v| v.as_i64().map(|i| i as i32));
        let parent_id = fields
            .get("parent_id")
            .and_then(|v| v.as_string().map(|s| s.to_string()));

        let request = CreateTaskRequest {
            content: &content,
            description: description.as_deref(),
            project_id: Some(&project_id),
            due_string: due_string.as_deref(),
            priority,
            parent_id: parent_id.as_deref(),
        };

        let created_task_api = self.provider.client.create_task(&request).await?;
        let created_task = TodoistTask::from(created_task_api);
        let task_id = created_task.id.clone();

        // Return inverse operation (delete)
        use holon::core::datasource::__operations_crud_operation_provider;
        let inverse = OperationResult::from_undo(UndoAction::Undo(
            __operations_crud_operation_provider::delete_op(
                "", // Will be set by OperationProvider
                &task_id,
            ),
        ));
        Ok((task_id, inverse))
    }

    async fn delete(&self, id: &str) -> Result<OperationResult> {
        // Capture entity for inverse operation (create)
        let old_task = self.cache.get_by_id(id).await?;

        self.provider.client.delete_task(id).await?;

        // Return inverse operation (create) if we have the old task
        Ok(match old_task {
            Some(task) => {
                use holon::core::datasource::__operations_crud_operation_provider;
                let mut create_fields = HashMap::new();
                create_fields.insert("id".to_string(), Value::String(task.id.clone()));
                create_fields.insert("content".to_string(), Value::String(task.content.clone()));
                if let Some(desc) = &task.description {
                    create_fields.insert("description".to_string(), Value::String(desc.clone()));
                }
                create_fields.insert("completed".to_string(), Value::Boolean(task.completed));
                create_fields.insert("priority".to_string(), Value::Integer(task.priority as i64));
                create_fields.insert(
                    "project_id".to_string(),
                    Value::String(task.project_id.clone()),
                );
                if let Some(parent_id) = &task.parent_id {
                    create_fields.insert("parent_id".to_string(), Value::String(parent_id.clone()));
                }
                if let Some(due_date) = &task.due_date {
                    create_fields.insert("due_date".to_string(), Value::String(due_date.clone()));
                }
                OperationResult::from_undo(UndoAction::Undo(
                    __operations_crud_operation_provider::create_op(
                        "", // Will be set by OperationProvider
                        create_fields,
                    ),
                ))
            }
            None => OperationResult::irreversible(vec![]),
        })
    }
}

/// Get operations for TodoistTask with resolved enum_from annotations.
/// This version calls the resolver function to populate OneOf type hints from datasource methods.
pub fn operations_with_resolver<DS>(ds: &DS) -> Vec<OperationDescriptor>
where
    DS: holon::core::datasource::TaskOperations<TodoistTask> + Send + Sync,
{
    let entity_name = <TodoistTask as OperationRegistry>::entity_name();
    let short_name =
        <TodoistTask as OperationRegistry>::short_name().expect("TodoistTask must have short_name");
    let table = entity_name;
    let id_column = "id";

    use holon::core::datasource::__operations_mutable_task_data_source;

    // Use resolver function for task_operations to resolve enum_from annotations
    let mut ops = __operations_mutable_task_data_source::task_operations_with_resolver(
        ds,
        entity_name,
        short_name,
        table,
        id_column,
    );

    // Add operations from other trait sources
    use holon::core::datasource::{
        __operations_crud_operation_provider, __operations_mutable_block_data_source,
    };
    ops.extend(
        __operations_crud_operation_provider::crud_operations(
            entity_name,
            short_name,
            table,
            id_column,
        )
        .into_iter(),
    );
    ops.extend(
        __operations_mutable_block_data_source::block_operations(
            entity_name,
            short_name,
            table,
            id_column,
        )
        .into_iter(),
    );
    ops.extend(
        __operations_todoist_move_operations::todoist_move_operations(
            entity_name,
            short_name,
            table,
            id_column,
        )
        .into_iter(),
    );

    ops
}

/// OperationProvider implementation for TodoistTaskOperations
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OperationProvider for TodoistTaskOperations {
    fn operations(&self) -> Vec<OperationDescriptor> {
        operations_with_resolver(self)
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<OperationResult> {
        // Validate entity name
        if entity_name != "todoist_tasks" {
            return Err(format!(
                "Expected entity_name 'todoist_tasks', got '{}'",
                entity_name
            )
            .into());
        }

        // Try dispatching to each trait module in order
        // Todoist-specific operations first (move_to_project, move_under_task)
        match __operations_todoist_move_operations::dispatch_operation(self, op_name, &params).await
        {
            Ok(result) => {
                return Ok(OperationResult::from_undo(match result.undo {
                    UndoAction::Undo(mut op) => {
                        op.entity_name = entity_name.to_string();
                        UndoAction::Undo(op)
                    }
                    UndoAction::Irreversible => UndoAction::Irreversible,
                }));
            }
            Err(err) => {
                if !UnknownOperationError::is_unknown(err.as_ref()) {
                    return Err(err);
                }
            }
        }

        // Generic CRUD operations
        match __operations_crud_operation_provider::dispatch_operation::<_, TodoistTask>(
            self, op_name, &params,
        )
        .await
        {
            Ok(result) => {
                return Ok(OperationResult::from_undo(match result.undo {
                    UndoAction::Undo(mut op) => {
                        op.entity_name = entity_name.to_string();
                        UndoAction::Undo(op)
                    }
                    UndoAction::Irreversible => UndoAction::Irreversible,
                }));
            }
            Err(err) => {
                if !UnknownOperationError::is_unknown(err.as_ref()) {
                    return Err(err);
                }
            }
        }

        // Block operations (move_block, indent, outdent, etc.)
        match __operations_mutable_block_data_source::dispatch_operation::<_, TodoistTask>(
            self, op_name, &params,
        )
        .await
        {
            Ok(result) => {
                return Ok(OperationResult::from_undo(match result.undo {
                    UndoAction::Undo(mut op) => {
                        op.entity_name = entity_name.to_string();
                        UndoAction::Undo(op)
                    }
                    UndoAction::Irreversible => UndoAction::Irreversible,
                }));
            }
            Err(err) => {
                if !UnknownOperationError::is_unknown(err.as_ref()) {
                    return Err(err);
                }
            }
        }

        // Task operations (set_completion, set_priority, etc.)
        let result = __operations_mutable_task_data_source::dispatch_operation::<_, TodoistTask>(
            self, op_name, &params,
        )
        .await?;
        Ok(OperationResult::from_undo(match result.undo {
            UndoAction::Undo(mut op) => {
                op.entity_name = entity_name.to_string();
                UndoAction::Undo(op)
            }
            UndoAction::Irreversible => UndoAction::Irreversible,
        }))
    }
}

/// DataSource implementation for TodoistProject
///
/// This wraps TodoistSyncProvider and implements ChangeNotifications<TodoistProject>.
/// Changes come from the sync provider's stream.
pub struct TodoistProjectDataSource {
    provider: Arc<TodoistSyncProvider>,
}

impl TodoistProjectDataSource {
    pub fn new(provider: Arc<TodoistSyncProvider>) -> Self {
        Self { provider }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ChangeNotifications<TodoistProject> for TodoistProjectDataSource {
    async fn watch_changes_since(
        &self,
        _position: StreamPosition,
    ) -> Pin<
        Box<dyn Stream<Item = std::result::Result<Vec<Change<TodoistProject>>, ApiError>> + Send>,
    > {
        let rx = self.provider.subscribe_projects();

        // Convert broadcast receiver to stream, extracting inner changes from metadata wrapper
        // Note: The sync token in metadata is handled by QueryableCache.ingest_stream_with_metadata()
        let change_stream = futures::stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(batch_with_metadata) => {
                    // Extract inner changes from metadata wrapper
                    Some((Ok(batch_with_metadata.inner), rx))
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    eprintln!("Stream lagged by {} messages", n);
                    Some((
                        Err(ApiError::InternalError {
                            message: format!("Stream lagged by {} messages", n),
                        }),
                        rx,
                    ))
                }
                Err(broadcast::error::RecvError::Closed) => None,
            }
        });

        Box::pin(change_stream)
    }

    async fn get_current_version(&self) -> std::result::Result<Vec<u8>, ApiError> {
        // Note: Sync tokens are now managed externally (by OperationDispatcher or caller)
        // This method should return the current version from the dispatcher or database
        // For now, return empty vec - the version should be retrieved from OperationDispatcher
        // TODO: Get sync token from OperationDispatcher or database
        Ok(Vec::new())
    }
}

// Keep DataSource implementation for backward compatibility during migration
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl holon::core::datasource::DataSource<TodoistProject> for TodoistProjectDataSource {
    async fn get_all(&self) -> Result<Vec<TodoistProject>> {
        let sync_resp = self.provider.client.sync_projects(None).await?;

        // Extract projects from response
        let projects_array = sync_resp
            .get("projects")
            .and_then(|p| p.as_array())
            .ok_or_else(|| "No projects array in response".to_string())?;

        // Parse projects
        let projects: Vec<TodoistProject> = projects_array
            .iter()
            .filter_map(|p| {
                serde_json::from_value::<TodoistProjectApiResponse>(p.clone())
                    .ok()
                    .filter(|api: &TodoistProjectApiResponse| !api.is_deleted.unwrap_or(false))
                    .map(|api| TodoistProject::from(api))
            })
            .collect();

        // Update sync token
        let _sync_token = sync_resp
            .get("sync_token")
            .and_then(|t| t.as_str())
            .map(|s| s.to_string());

        // Note: We can't update the provider's sync_token directly since it's private.
        // The sync provider manages its own token via sync() calls.
        // This is fine - the token will be updated when sync() is called.

        Ok(projects)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<TodoistProject>> {
        // For projects, we need to sync to get a specific project
        // Since there's no direct "get project by ID" endpoint, we sync all projects
        let all_projects = self.get_all().await?;
        Ok(all_projects.into_iter().find(|p| p.id == id))
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl CrudOperations<TodoistProject> for TodoistProjectDataSource {
    async fn set_field(&self, _id: &str, field: &str, value: Value) -> Result<OperationResult> {
        match field {
            "name" => {
                if let Value::String(_name) = value {
                    // TODO: Implement project_update command in client
                    // For now, just sync to refresh cache
                    use holon::core::datasource::DataSource;
                    let _ = <Self as DataSource<TodoistProject>>::get_all(self).await?;
                }
            }
            _ => {
                return Err(format!("Field {} not supported for projects", field).into());
            }
        }
        // Project operations cannot be undone (they're external API calls)
        Ok(OperationResult::irreversible(vec![]))
    }

    async fn create(&self, fields: HashMap<String, Value>) -> Result<(String, OperationResult)> {
        let name = fields
            .get("name")
            .and_then(|v| v.as_string().map(|s| s.to_string()))
            .ok_or_else(|| "Missing name field".to_string())?;

        // Create project via Sync API
        let project_id = self.provider.client.create_project(&name).await?;

        // Sync to get the full project details
        let sync_resp = self.provider.client.sync_projects(None).await?;
        let projects_array = sync_resp
            .get("projects")
            .and_then(|p| p.as_array())
            .ok_or_else(|| "No projects array in response".to_string())?;

        // Find the created project (no need to cache it)
        if let Some(project_json) = projects_array
            .iter()
            .find(|p| p.get("id").and_then(|id| id.as_str()) == Some(&project_id))
        {
            // Verify project was created successfully
            if serde_json::from_value::<TodoistProjectApiResponse>(project_json.clone()).is_err() {
                return Err("Failed to parse created project".to_string().into());
            }
        }

        Ok((project_id, OperationResult::irreversible(vec![])))
    }

    async fn delete(&self, id: &str) -> Result<OperationResult> {
        self.provider.client.delete_project(id).await?;
        Ok(OperationResult::irreversible(vec![]))
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OperationProvider for TodoistProjectDataSource {
    fn operations(&self) -> Vec<OperationDescriptor> {
        vec![
            OperationDescriptor {
                entity_name: "todoist_projects".to_string(),
                entity_short_name: "project".to_string(),
                id_column: "id".to_string(),
                name: "move_block".to_string(),
                display_name: "Move Project".to_string(),
                description: "Move a project under another project".to_string(),
                required_params: vec![
                    OperationParam {
                        name: "id".to_string(),
                        type_hint: TypeHint::String,
                        description: "The project ID to move".to_string(),
                    },
                    OperationParam {
                        name: "parent_id".to_string(),
                        type_hint: TypeHint::EntityId {
                            entity_name: "todoist_projects".to_string(),
                        },
                        description: "The parent project ID (or null for root)".to_string(),
                    },
                ],
                affected_fields: vec!["parent_id".to_string()],
                param_mappings: vec![
                    // From tree drop - project_id triggers this operation
                    ParamMapping {
                        from: "project_id".to_string(),
                        provides: vec!["parent_id".to_string()],
                        defaults: Default::default(),
                    },
                ],
                precondition: None,
            },
            OperationDescriptor {
                entity_name: "todoist_projects".to_string(),
                entity_short_name: "project".to_string(),
                id_column: "id".to_string(),
                name: "archive".to_string(),
                display_name: "Archive Project".to_string(),
                description: "Archive a project and its descendants".to_string(),
                required_params: vec![OperationParam {
                    name: "id".to_string(),
                    type_hint: TypeHint::String,
                    description: "The project ID to archive".to_string(),
                }],
                affected_fields: vec!["is_archived".to_string()],
                param_mappings: vec![],
                precondition: None,
            },
            OperationDescriptor {
                entity_name: "todoist_projects".to_string(),
                entity_short_name: "project".to_string(),
                id_column: "id".to_string(),
                name: "unarchive".to_string(),
                display_name: "Unarchive Project".to_string(),
                description: "Unarchive a project".to_string(),
                required_params: vec![OperationParam {
                    name: "id".to_string(),
                    type_hint: TypeHint::String,
                    description: "The project ID to unarchive".to_string(),
                }],
                affected_fields: vec!["is_archived".to_string()],
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
        if entity_name != "todoist_projects" {
            return Err(format!(
                "Expected entity_name 'todoist_projects', got '{}'",
                entity_name
            )
            .into());
        }

        // Project operations cannot be undone (they're external API calls)
        match op_name {
            "move_block" => {
                self.move_project(&params).await?;
                Ok(OperationResult::irreversible(vec![]))
            }
            "archive" => {
                self.archive_project(&params).await?;
                Ok(OperationResult::irreversible(vec![]))
            }
            "unarchive" => {
                self.unarchive_project(&params).await?;
                Ok(OperationResult::irreversible(vec![]))
            }
            _ => Err(format!("Unknown operation '{}' for todoist_projects", op_name).into()),
        }
    }
}

impl TodoistProjectDataSource {
    /// Move a project under another project (or to root)
    async fn move_project(&self, params: &StorageEntity) -> Result<()> {
        let id = params
            .get("id")
            .and_then(|v| v.as_string())
            .ok_or_else(|| "move_block requires 'id' parameter")?;

        // parent_id can be null (move to root) or a project ID
        let new_parent_id = params.get("parent_id").and_then(|v| v.as_string());

        debug!(
            "[TodoistProjectDataSource] Moving project {} to parent {:?}",
            id, new_parent_id
        );

        self.provider.client.move_project(id, new_parent_id).await?;

        // Note: sync is now handled automatically by OperationWrapper

        Ok(())
    }

    /// Archive a project and its descendants
    async fn archive_project(&self, params: &StorageEntity) -> Result<()> {
        let id = params
            .get("id")
            .and_then(|v| v.as_string())
            .ok_or_else(|| "archive requires 'id' parameter")?;

        debug!("[TodoistProjectDataSource] Archiving project {}", id);

        self.provider.client.archive_project(id).await?;

        // Note: sync is now handled automatically by OperationWrapper

        Ok(())
    }

    /// Unarchive a project
    async fn unarchive_project(&self, params: &StorageEntity) -> Result<()> {
        let id = params
            .get("id")
            .and_then(|v| v.as_string())
            .ok_or_else(|| "unarchive requires 'id' parameter")?;

        debug!("[TodoistProjectDataSource] Unarchiving project {}", id);

        self.provider.client.unarchive_project(id).await?;

        // Note: sync is now handled automatically by OperationWrapper

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simple empty DataSource for testing operations structure
    struct EmptyDataSource;

    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    impl holon::core::datasource::DataSource<TodoistTask> for EmptyDataSource {
        async fn get_all(&self) -> holon::core::datasource::Result<Vec<TodoistTask>> {
            Ok(Vec::new())
        }

        async fn get_by_id(
            &self,
            _id: &str,
        ) -> holon::core::datasource::Result<Option<TodoistTask>> {
            Ok(None)
        }
    }

    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    impl holon::core::datasource::TaskOperations<TodoistTask> for EmptyDataSource {
        fn completion_states_with_progress(
            &self,
        ) -> Vec<holon::core::datasource::CompletionStateInfo> {
            vec![
                holon::core::datasource::CompletionStateInfo {
                    state: "active".into(),
                    progress: 0.0,
                    is_done: false,
                    is_active: true,
                },
                holon::core::datasource::CompletionStateInfo {
                    state: "completed".into(),
                    progress: 100.0,
                    is_done: true,
                    is_active: false,
                },
            ]
        }

        async fn set_state(
            &self,
            _id: &str,
            _task_state: String,
        ) -> holon::core::datasource::Result<holon::core::datasource::UndoAction> {
            unreachable!("Not used in tests")
        }
    }

    #[test]
    fn test_operations_with_param_mappings_includes_move_block() {
        let empty_ds = EmptyDataSource;
        let ops = operations_with_resolver(&empty_ds);

        // Find move_block operation
        let move_block = ops.iter().find(|op| op.name == "move_block");
        assert!(move_block.is_some(), "move_block operation should exist");

        let move_block = move_block.unwrap();

        // Check it has exactly 2 required params: id and parent_id
        let param_names: Vec<&str> = move_block
            .required_params
            .iter()
            .map(|p| p.name.as_str())
            .collect();
        println!("move_block required_params: {:?}", param_names);

        assert!(
            param_names.contains(&"id"),
            "move_block should have 'id' param"
        );
        assert!(
            param_names.contains(&"parent_id"),
            "move_block should have 'parent_id' param"
        );
        assert!(
            !param_names.contains(&"after_block_id"),
            "move_block should NOT have 'after_block_id' as required param, but got: {:?}",
            param_names
        );

        // Check param_mappings
        println!("move_block param_mappings: {:?}", move_block.param_mappings);
        assert_eq!(
            move_block.param_mappings.len(),
            2,
            "move_block should have 2 param_mappings (tree_position and selected_id)"
        );

        // Find tree_position mapping
        let tree_position_mapping = move_block
            .param_mappings
            .iter()
            .find(|m| m.from == "tree_position")
            .expect("should have tree_position mapping");
        assert!(
            tree_position_mapping
                .provides
                .contains(&"parent_id".to_string())
        );
    }

    #[test]
    fn test_move_block_should_not_have_after_block_id_as_required() {
        let empty_ds = EmptyDataSource;
        let ops = operations_with_resolver(&empty_ds);
        let move_block = ops.iter().find(|op| op.name == "move_block").unwrap();

        let param_names: Vec<&str> = move_block
            .required_params
            .iter()
            .map(|p| p.name.as_str())
            .collect();
        println!("move_block required_params: {:?}", param_names);
        // after_block_id should NOT be required (it's optional in the trait)
        assert!(
            !param_names.contains(&"after_block_id"),
            "move_block should NOT have 'after_block_id' as required param, but got: {:?}",
            param_names
        );
    }
}
