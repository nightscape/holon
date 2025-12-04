use crate::core::datasource::{CrudOperations, DataSource, Result, UndoAction};
use crate::tasks::Task;
use async_trait::async_trait;
use holon_api::streaming::ChangeNotifications;
use holon_api::Value;
use holon_api::{ApiError, Change, ChangeOrigin, StreamPosition};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};

#[derive(Clone)]
pub struct InMemoryTaskStore {
    tasks: Arc<RwLock<Vec<Task>>>,
    /// Version counter for change tracking
    version: Arc<AtomicU64>,
    /// Channel senders for change notifications
    change_senders:
        Arc<RwLock<Vec<mpsc::Sender<std::result::Result<Vec<Change<Task>>, ApiError>>>>>,
}

impl InMemoryTaskStore {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(Vec::new())),
            version: Arc::new(AtomicU64::new(0)),
            change_senders: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Emit a change to all subscribers
    fn emit_change(&self, change: Change<Task>) {
        self.version.fetch_add(1, Ordering::SeqCst);
        let senders = self.change_senders.read().unwrap();
        let change_batch = vec![change];
        for sender in senders.iter() {
            let _ = sender.try_send(Ok(change_batch.clone()));
        }
    }

    fn flatten_tasks(tasks: &[Task]) -> Vec<Task> {
        let mut result = Vec::new();
        for task in tasks {
            let mut task_copy = task.clone();
            task_copy.children = Vec::new();
            result.push(task_copy);

            if !task.children.is_empty() {
                result.extend(Self::flatten_tasks(&task.children));
            }
        }
        result
    }

    fn rebuild_hierarchy(flat_tasks: Vec<Task>) -> Vec<Task> {
        let mut task_map: std::collections::HashMap<String, Task> =
            flat_tasks.into_iter().map(|t| (t.id.clone(), t)).collect();

        let mut root_tasks = Vec::new();
        let mut child_tasks: Vec<(String, Task)> = Vec::new();

        for (_id, task) in task_map.drain() {
            if let Some(parent_id) = &task.parent_id {
                child_tasks.push((parent_id.clone(), task));
            } else {
                root_tasks.push(task);
            }
        }

        for (parent_id, child) in child_tasks {
            Self::add_child_to_hierarchy(&mut root_tasks, &parent_id, child);
        }

        root_tasks
    }

    fn add_child_to_hierarchy(tasks: &mut [Task], parent_id: &str, child: Task) -> bool {
        for task in tasks {
            if task.id == parent_id {
                task.children.push(child);
                return true;
            }
            if Self::add_child_to_hierarchy(&mut task.children, parent_id, child.clone()) {
                return true;
            }
        }
        false
    }
}

impl Default for InMemoryTaskStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ChangeNotifications<Task> for InMemoryTaskStore {
    async fn watch_changes_since(
        &self,
        position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<Vec<Change<Task>>, ApiError>> + Send>> {
        let (tx, rx) = mpsc::channel(100);

        // Register sender
        {
            let mut senders = self.change_senders.write().unwrap();
            senders.push(tx);
        }

        match position {
            StreamPosition::Beginning => {
                // First, collect all current tasks as Created events
                let current_tasks = {
                    let tasks = self.tasks.read().unwrap();
                    let flat = Self::flatten_tasks(&tasks);
                    flat.into_iter()
                        .map(|task| Change::Created {
                            data: task,
                            origin: ChangeOrigin::Remote {
                                operation_id: None,
                                trace_id: None,
                            },
                        })
                        .collect::<Vec<_>>()
                };

                // Create stream that first yields current tasks, then forwards future changes
                let initial_batch = if current_tasks.is_empty() {
                    vec![]
                } else {
                    vec![current_tasks]
                };
                let initial_stream = tokio_stream::iter(initial_batch.into_iter().map(Ok));
                let change_stream = ReceiverStream::new(rx).map(|result| {
                    result.map_err(|e| ApiError::InternalError {
                        message: format!("Channel receive error: {}", e),
                    })
                });

                Box::pin(initial_stream.chain(change_stream))
            }
            StreamPosition::Version(_version_bytes) => {
                // For version-based sync, just forward future changes
                // In a more sophisticated implementation, we'd track a change log
                let change_stream = ReceiverStream::new(rx).map(|result| {
                    result.map_err(|e| ApiError::InternalError {
                        message: format!("Channel receive error: {}", e),
                    })
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

// Keep DataSource implementation for backward compatibility during migration
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl DataSource<Task> for InMemoryTaskStore {
    async fn get_all(&self) -> Result<Vec<Task>> {
        let tasks = self
            .tasks
            .read()
            .map_err(|e| format!("Failed to read tasks: {}", e))?;
        let flat = Self::flatten_tasks(&tasks);
        Ok(flat)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<Task>> {
        let tasks = self
            .tasks
            .read()
            .map_err(|e| format!("Failed to read tasks: {}", e))?;
        let flat = Self::flatten_tasks(&tasks);
        Ok(flat.into_iter().find(|t| t.id == id))
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl CrudOperations<Task> for InMemoryTaskStore {
    async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<UndoAction> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| format!("Failed to write tasks: {}", e))?;

        let flat = Self::flatten_tasks(&tasks);
        let mut updated_flat = flat;

        if let Some(pos) = updated_flat.iter().position(|t| t.id == id) {
            // Capture old value for inverse operation
            let old_value = match field {
                "title" => Value::String(updated_flat[pos].title.clone()),
                "completed" => Value::Boolean(updated_flat[pos].completed),
                "parent_id" => updated_flat[pos]
                    .parent_id
                    .as_ref()
                    .map(|s| Value::String(s.clone()))
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            };

            match field {
                "title" => {
                    if let Value::String(s) = value {
                        updated_flat[pos].title = s;
                    }
                }
                "completed" => {
                    if let Value::Boolean(b) = value {
                        updated_flat[pos].completed = b;
                    }
                }
                "parent_id" => match value {
                    Value::String(s) => updated_flat[pos].parent_id = Some(s),
                    Value::Null => updated_flat[pos].parent_id = None,
                    _ => {}
                },
                _ => {}
            }
            // Clone the task before moving updated_flat
            let task_to_emit = updated_flat[pos].clone();
            *tasks = Self::rebuild_hierarchy(updated_flat);

            // Emit change
            self.emit_change(Change::Updated {
                id: id.to_string(),
                data: task_to_emit,
                origin: ChangeOrigin::Local {
                    operation_id: None,
                    trace_id: None,
                },
            });

            // Return inverse operation using macro-generated helper
            use holon_core::__operations_crud_operations;
            Ok(UndoAction::Undo(
                __operations_crud_operations::set_field_op(
                    "", // Will be set by OperationProvider
                    id, field, old_value,
                ),
            ))
        } else {
            Err(format!("Task not found: {}", id).into())
        }
    }

    async fn create(&self, fields: HashMap<String, Value>) -> Result<(String, UndoAction)> {
        let id = fields
            .get("id")
            .and_then(|v| v.as_string().map(|s| s.to_string()))
            .unwrap_or_else(|| {
                format!(
                    "task-{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                )
            });
        let title = fields
            .get("title")
            .and_then(|v| v.as_string().map(|s| s.to_string()))
            .unwrap_or_else(|| "Untitled".to_string());
        let completed = fields
            .get("completed")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let parent_id = fields.get("parent_id").and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            Value::Null => None,
            _ => None,
        });

        let task = Task {
            id: id.clone(),
            title,
            completed,
            parent_id,
            children: Vec::new(),
        };

        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| format!("Failed to write tasks: {}", e))?;

        let mut flat = Self::flatten_tasks(&tasks);
        flat.push(task.clone());
        *tasks = Self::rebuild_hierarchy(flat);

        // Emit change
        self.emit_change(Change::Created {
            data: task,
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        // Return inverse operation (delete) using macro-generated helper
        use holon_core::__operations_crud_operations;
        let inverse = UndoAction::Undo(__operations_crud_operations::delete_op(
            "", // Will be set by OperationProvider
            &id,
        ));
        Ok((id, inverse))
    }

    async fn delete(&self, id: &str) -> Result<UndoAction> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| format!("Failed to write tasks: {}", e))?;

        let mut flat = Self::flatten_tasks(&tasks);

        if let Some(pos) = flat.iter().position(|t| t.id == id) {
            // Capture full entity for inverse operation (create)
            let deleted_task = flat[pos].clone();
            let mut create_fields = HashMap::new();
            create_fields.insert("id".to_string(), Value::String(deleted_task.id.clone()));
            create_fields.insert(
                "title".to_string(),
                Value::String(deleted_task.title.clone()),
            );
            create_fields.insert(
                "completed".to_string(),
                Value::Boolean(deleted_task.completed),
            );
            if let Some(ref pid) = deleted_task.parent_id {
                create_fields.insert("parent_id".to_string(), Value::String(pid.clone()));
            } else {
                create_fields.insert("parent_id".to_string(), Value::Null);
            }

            flat.remove(pos);
            *tasks = Self::rebuild_hierarchy(flat);

            // Emit change
            self.emit_change(Change::Deleted {
                id: id.to_string(),
                origin: ChangeOrigin::Local {
                    operation_id: None,
                    trace_id: None,
                },
            });

            // Return inverse operation (create) using macro-generated helper
            use holon_core::__operations_crud_operations;
            Ok(UndoAction::Undo(__operations_crud_operations::create_op(
                "", // Will be set by OperationProvider
                create_fields,
            )))
        } else {
            Err(format!("Task not found: {}", id).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_insert_and_get_all() {
        let store = InMemoryTaskStore::new();
        let task = Task::new("Test Task".to_string(), None);
        let id = task.id.clone();

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::String(id.clone()));
        fields.insert("title".to_string(), Value::String(task.title.clone()));
        fields.insert("completed".to_string(), Value::Boolean(task.completed));
        store.create(fields).await.unwrap();

        let all_tasks = store.get_all().await.unwrap();

        assert_eq!(all_tasks.len(), 1);
        assert_eq!(all_tasks[0].id, id);
        assert_eq!(all_tasks[0].title, "Test Task");
    }

    #[tokio::test]
    async fn test_get_by_id() {
        let store = InMemoryTaskStore::new();
        let task = Task::new("Find Me".to_string(), None);
        let id = task.id.clone();

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::String(id.clone()));
        fields.insert("title".to_string(), Value::String(task.title.clone()));
        fields.insert("completed".to_string(), Value::Boolean(task.completed));
        store.create(fields).await.unwrap();

        let found = store.get_by_id(&id).await.unwrap();

        assert!(found.is_some());
        assert_eq!(found.unwrap().title, "Find Me");
    }

    #[tokio::test]
    async fn test_update() {
        let store = InMemoryTaskStore::new();
        let task = Task::new("Original".to_string(), None);
        let id = task.id.clone();

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::String(id.clone()));
        fields.insert("title".to_string(), Value::String(task.title.clone()));
        fields.insert("completed".to_string(), Value::Boolean(task.completed));
        store.create(fields).await.unwrap();

        store
            .set_field(&id, "title", Value::String("Updated".to_string()))
            .await
            .unwrap();
        store
            .set_field(&id, "completed", Value::Boolean(true))
            .await
            .unwrap();

        let found = store.get_by_id(&id).await.unwrap().unwrap();
        assert_eq!(found.title, "Updated");
        assert!(found.completed);
    }

    #[tokio::test]
    async fn test_delete() {
        let store = InMemoryTaskStore::new();
        let task = Task::new("Delete Me".to_string(), None);
        let id = task.id.clone();

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::String(id.clone()));
        fields.insert("title".to_string(), Value::String(task.title.clone()));
        fields.insert("completed".to_string(), Value::Boolean(task.completed));
        store.create(fields).await.unwrap();

        assert_eq!(store.get_all().await.unwrap().len(), 1);

        store.delete(&id).await.unwrap();
        assert_eq!(store.get_all().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_hierarchical_tasks() {
        let store = InMemoryTaskStore::new();

        let parent = Task::new("Parent".to_string(), None);
        let parent_id = parent.id.clone();

        let mut parent_fields = HashMap::new();
        parent_fields.insert("id".to_string(), Value::String(parent_id.clone()));
        parent_fields.insert("title".to_string(), Value::String(parent.title.clone()));
        parent_fields.insert("completed".to_string(), Value::Boolean(parent.completed));
        store.create(parent_fields).await.unwrap();

        let child = Task::new("Child".to_string(), Some(parent_id.clone()));
        let mut child_fields = HashMap::new();
        child_fields.insert("id".to_string(), Value::String(child.id.clone()));
        child_fields.insert("title".to_string(), Value::String(child.title.clone()));
        child_fields.insert("completed".to_string(), Value::Boolean(child.completed));
        child_fields.insert("parent_id".to_string(), Value::String(parent_id.clone()));
        store.create(child_fields).await.unwrap();

        let all = store.get_all().await.unwrap();
        assert_eq!(all.len(), 2);

        let parent_task = all.iter().find(|t| t.id == parent_id).unwrap();
        assert!(parent_task.parent_id.is_none());

        let child_task = all.iter().find(|t| t.id != parent_id).unwrap();
        assert_eq!(child_task.parent_id, Some(parent_id));
    }
}
