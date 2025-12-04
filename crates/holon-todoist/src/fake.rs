//! Fake Todoist implementation for optimistic updates with stream-based architecture
//!
//! TodoistTaskFake implements ChangeNotifications<TodoistTask> and CrudOperations<TodoistTask>:
//! - Reads from read-only DataSource (assumed to be kept up-to-date via change stream)
//! - Writes emit changes via broadcast channel (no DB writes)
//! - Simulates external API behavior for testing/offline mode

use async_trait::async_trait;
use holon::core::datasource::{CrudOperations, DataSource, Operation, Result, UndoAction};
use holon_api::streaming::ChangeNotifications;
use holon_api::Value;
use holon_api::{ApiError, Change, ChangeOrigin, StreamPosition};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt};

use crate::models::TodoistTask;

/// Fake Todoist datasource that emits changes via broadcast channel
///
/// Architecture:
/// - Reads from read-only DataSource (assumed to be kept up-to-date via change stream)
/// - Writes emit Change<TodoistTask> on broadcast channel (no DB writes)
/// - Simulates external API behavior for testing/offline mode
/// - Implements ChangeNotifications for stream-based change tracking
pub struct TodoistTaskFake {
    /// Read-only DataSource for reading current state
    /// Assumed to be kept up-to-date by external cache consuming change stream
    read_source: Arc<dyn DataSource<TodoistTask>>,
    /// Broadcast channel sender for emitting changes (batches)
    change_tx: broadcast::Sender<Vec<Change<TodoistTask>>>,
    /// Version counter for tracking changes
    version: Arc<AtomicU64>,
}

impl TodoistTaskFake {
    /// Create a new TodoistTaskFake with a read-only DataSource
    pub fn new(read_source: Arc<dyn DataSource<TodoistTask>>) -> Self {
        let (change_tx, _) = broadcast::channel(1000); // Buffer up to 1000 changes

        Self {
            read_source,
            change_tx,
            version: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get a receiver for the change stream (batches)
    pub fn subscribe(&self) -> broadcast::Receiver<Vec<Change<TodoistTask>>> {
        self.change_tx.subscribe()
    }

    /// Emit a change on the broadcast channel (fire-and-forget)
    /// Wraps single change in a batch to match provider's format
    /// Increments version counter
    fn emit_change(&self, change: Change<TodoistTask>) {
        // Increment version
        self.version.fetch_add(1, Ordering::SeqCst);
        // Ignore errors if no receivers (fire-and-forget)
        let _ = self.change_tx.send(vec![change]);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl ChangeNotifications<TodoistTask> for TodoistTaskFake {
    async fn watch_changes_since(
        &self,
        position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<Vec<Change<TodoistTask>>, ApiError>> + Send>>
    {
        let rx = self.change_tx.subscribe();
        let read_source = Arc::clone(&self.read_source);

        match position {
            StreamPosition::Beginning => {
                // First, collect all current tasks from read_source as Created events
                let current_tasks: Vec<Change<TodoistTask>> = match read_source.get_all().await {
                    Ok(tasks) => tasks
                        .into_iter()
                        .map(|task| Change::Created {
                            data: task,
                            origin: ChangeOrigin::Remote {
                                operation_id: None,
                                trace_id: None,
                            },
                        })
                        .collect(),
                    Err(e) => {
                        // Return error as first item in stream
                        let error_stream = tokio_stream::iter(vec![Err(ApiError::InternalError {
                            message: format!("Failed to read tasks: {}", e),
                        })]);
                        let (_tx, rx) = broadcast::channel(1000);
                        let change_stream = futures::stream::unfold(rx, |mut rx| async move {
                            match rx.recv().await {
                                Ok(changes) => Some((Ok(changes), rx)),
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
                        return Box::pin(error_stream.chain(change_stream));
                    }
                };

                // Create a stream that first yields current tasks, then forwards future changes
                let initial_batch = if current_tasks.is_empty() {
                    vec![]
                } else {
                    vec![current_tasks]
                };
                let initial_stream = tokio_stream::iter(initial_batch.into_iter().map(Ok));

                // Convert broadcast receiver to stream, mapping errors
                let change_stream = futures::stream::unfold(rx, |mut rx| async move {
                    match rx.recv().await {
                        Ok(changes) => Some((Ok(changes), rx)),
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

                Box::pin(initial_stream.chain(change_stream))
            }
            StreamPosition::Version(version_bytes) => {
                // Parse version from bytes
                let start_version = if version_bytes.is_empty() {
                    0
                } else {
                    u64::from_le_bytes(version_bytes.as_slice().try_into().unwrap_or([0; 8]))
                };

                let current_version = self.version.load(Ordering::SeqCst);

                // If start_version >= current_version, just forward future changes
                if start_version >= current_version {
                    let change_stream = futures::stream::unfold(rx, |mut rx| async move {
                        match rx.recv().await {
                            Ok(changes) => Some((Ok(changes), rx)),
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
                    return Box::pin(change_stream);
                }

                // For now, if version is specified, we'll just forward future changes
                // In a more sophisticated implementation, we'd track a change log
                let change_stream = futures::stream::unfold(rx, |mut rx| async move {
                    match rx.recv().await {
                        Ok(changes) => Some((Ok(changes), rx)),
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
        }
    }

    async fn get_current_version(&self) -> std::result::Result<Vec<u8>, ApiError> {
        let version = self.version.load(Ordering::SeqCst);
        Ok(version.to_le_bytes().to_vec())
    }
}

// Implement DataSource by delegating to read_source
// This is needed for BlockOperations and TaskOperations traits
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl DataSource<TodoistTask> for TodoistTaskFake {
    async fn get_all(&self) -> Result<Vec<TodoistTask>> {
        self.read_source.get_all().await
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<TodoistTask>> {
        self.read_source.get_by_id(id).await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl CrudOperations<TodoistTask> for TodoistTaskFake {
    async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<UndoAction> {
        // Read current task from read_source
        let task = self.read_source.get_by_id(id).await?;
        let mut task = task.ok_or_else(|| anyhow::anyhow!("Task not found: {}", id))?;

        // Capture old value for inverse operation
        let old_value = match field {
            "content" => Value::String(task.content.clone()),
            "description" => task
                .description
                .as_ref()
                .map(|d| Value::String(d.clone()))
                .unwrap_or(Value::Null),
            "completed" => Value::Boolean(task.completed),
            "priority" => Value::Integer(task.priority as i64),
            "due_date" => task
                .due_date
                .as_ref()
                .map(|d| Value::String(d.clone()))
                .unwrap_or(Value::Null),
            "parent_id" => task
                .parent_id
                .as_ref()
                .map(|p| Value::String(p.clone()))
                .unwrap_or(Value::Null),
            _ => Value::Null,
        };

        // Update field
        match field {
            "content" => {
                if let Value::String(s) = value {
                    task.content = s;
                }
            }
            "description" => {
                task.description = match value {
                    Value::String(s) => Some(s),
                    Value::Null => None,
                    _ => return Err(anyhow::anyhow!("Invalid value type for description").into()),
                };
            }
            "completed" => {
                if let Value::Boolean(b) = value {
                    task.completed = b;
                }
            }
            "priority" => {
                if let Value::Integer(i) = value {
                    task.priority = i as i32;
                }
            }
            "due_date" => {
                task.due_date = match value {
                    Value::String(s) => Some(s),
                    Value::Null => None,
                    _ => return Err(anyhow::anyhow!("Invalid value type for due_date").into()),
                };
            }
            "parent_id" => {
                task.parent_id = match value {
                    Value::String(s) => Some(s),
                    Value::Null => None,
                    _ => return Err(anyhow::anyhow!("Invalid value type for parent_id").into()),
                };
            }
            _ => {
                return Err(anyhow::anyhow!("Unknown field: {}", field).into());
            }
        }

        // Emit change - this is an update since task already exists
        // No DB write - the change stream will update the cache
        self.emit_change(Change::Updated {
            id: id.to_string(),
            data: task,
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        // Return inverse operation
        use holon::core::datasource::__operations_crud_operation_provider;
        Ok(UndoAction::Undo(
            __operations_crud_operation_provider::set_field_op(
                "", // Will be set by OperationProvider
                id, field, old_value,
            ),
        ))
    }

    async fn create(&self, fields: HashMap<String, Value>) -> Result<(String, UndoAction)> {
        // Generate ID
        let id = format!("fake-{}", uuid::Uuid::new_v4());

        // Build task from fields
        let mut task = TodoistTask::new(
            id.clone(),
            fields
                .get("content")
                .and_then(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| "New Task".to_string()),
            fields
                .get("project_id")
                .and_then(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| "inbox".to_string()),
        );

        // Set optional fields
        if let Some(Value::String(s)) = fields.get("description") {
            task.description = Some(s.clone());
        }
        if let Some(Value::Boolean(b)) = fields.get("completed") {
            task.completed = *b;
        }
        if let Some(Value::Integer(i)) = fields.get("priority") {
            task.priority = *i as i32;
        }
        if let Some(Value::String(s)) = fields.get("due_date") {
            task.due_date = Some(s.clone());
        }
        if let Some(Value::String(s)) = fields.get("parent_id") {
            task.parent_id = Some(s.clone());
        }

        // Emit change - this is a create since it's a new task
        // No DB write - the change stream will update the cache
        self.emit_change(Change::Created {
            data: task,
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        // Return inverse operation (delete)
        use holon::core::datasource::__operations_crud_operation_provider;
        let inverse = UndoAction::Undo(__operations_crud_operation_provider::delete_op(
            "", // Will be set by OperationProvider
            &id,
        ));
        Ok((id, inverse))
    }

    async fn delete(&self, id: &str) -> Result<UndoAction> {
        // Capture entity for inverse operation (create)
        let task = self
            .read_source
            .get_by_id(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Task not found: {}", id))?;

        // Emit change - no DB write, the change stream will update the cache
        self.emit_change(Change::Deleted {
            id: id.to_string(),
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        // Return inverse operation (create)
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

        use holon::core::datasource::__operations_crud_operation_provider;
        Ok(UndoAction::Undo(
            __operations_crud_operation_provider::create_op(
                "", // Will be set by OperationProvider
                create_fields,
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;
    use tokio::time::sleep;

    /// Simple in-memory DataSource for testing
    struct InMemoryDataSource {
        tasks: Arc<RwLock<HashMap<String, TodoistTask>>>,
    }

    impl InMemoryDataSource {
        fn new() -> Self {
            Self {
                tasks: Arc::new(RwLock::new(HashMap::new())),
            }
        }

        /// Update task from change stream (called by test helper)
        async fn apply_change(&self, change: Change<TodoistTask>) {
            let mut tasks = self.tasks.write().await;
            match change {
                Change::Created { data, .. } | Change::Updated { data, .. } => {
                    tasks.insert(data.id.clone(), data);
                }
                Change::Deleted { id, .. } => {
                    tasks.remove(&id);
                }
            }
        }
    }

    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    impl DataSource<TodoistTask> for InMemoryDataSource {
        async fn get_all(&self) -> Result<Vec<TodoistTask>> {
            Ok(self.tasks.read().await.values().cloned().collect())
        }

        async fn get_by_id(&self, id: &str) -> Result<Option<TodoistTask>> {
            Ok(self.tasks.read().await.get(id).cloned())
        }
    }

    /// Helper to create a TodoistTaskFake with an in-memory cache that consumes changes
    async fn create_fake_with_cache() -> (TodoistTaskFake, Arc<InMemoryDataSource>) {
        let cache = Arc::new(InMemoryDataSource::new());
        let fake = TodoistTaskFake::new(Arc::clone(&cache) as Arc<dyn DataSource<TodoistTask>>);

        // Spawn background task to consume changes and update cache
        let cache_clone = Arc::clone(&cache);
        let mut rx = fake.subscribe();
        tokio::spawn(async move {
            while let Ok(changes) = rx.recv().await {
                for change in changes {
                    cache_clone.apply_change(change).await;
                }
            }
        });

        // Give the background task a moment to start
        sleep(Duration::from_millis(10)).await;

        (fake, cache)
    }

    #[tokio::test]
    async fn test_todoist_fake_create_and_read() {
        let (fake, cache) = create_fake_with_cache().await;

        // Create a task
        let mut fields = HashMap::new();
        fields.insert(
            "content".to_string(),
            Value::String("Test Task".to_string()),
        );
        fields.insert(
            "project_id".to_string(),
            Value::String("project-123".to_string()),
        );

        let id = fake.create(fields).await.unwrap();
        assert!(!id.is_empty());

        // Wait for change to propagate to cache
        sleep(Duration::from_millis(50)).await;

        // Read it back from cache
        let task = cache.get_by_id(&id).await.unwrap();
        assert!(task.is_some());
        let task = task.unwrap();
        assert_eq!(task.content, "Test Task");
        assert_eq!(task.project_id, "project-123");
    }

    #[tokio::test]
    async fn test_todoist_fake_set_field() {
        let (fake, cache) = create_fake_with_cache().await;

        // Create a task
        let mut fields = HashMap::new();
        fields.insert(
            "content".to_string(),
            Value::String("Test Task".to_string()),
        );
        fields.insert(
            "project_id".to_string(),
            Value::String("project-123".to_string()),
        );

        let id = fake.create(fields).await.unwrap();

        // Wait for create to propagate
        sleep(Duration::from_millis(50)).await;

        // Update a field
        fake.set_field(&id, "content", Value::String("Updated Task".to_string()))
            .await
            .unwrap();

        // Wait for update to propagate
        sleep(Duration::from_millis(50)).await;

        // Verify update
        let task = cache.get_by_id(&id).await.unwrap().unwrap();
        assert_eq!(task.content, "Updated Task");
    }

    #[tokio::test]
    async fn test_todoist_fake_delete() {
        let (fake, cache) = create_fake_with_cache().await;

        // Create a task
        let mut fields = HashMap::new();
        fields.insert(
            "content".to_string(),
            Value::String("Test Task".to_string()),
        );
        fields.insert(
            "project_id".to_string(),
            Value::String("project-123".to_string()),
        );

        let id = fake.create(fields).await.unwrap();

        // Wait for create to propagate
        sleep(Duration::from_millis(50)).await;

        // Delete it
        fake.delete(&id).await.unwrap();

        // Wait for delete to propagate
        sleep(Duration::from_millis(50)).await;

        // Verify deletion
        let task = cache.get_by_id(&id).await.unwrap();
        assert!(task.is_none());
    }
}
