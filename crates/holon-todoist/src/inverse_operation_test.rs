//! Tests for inverse operation roundtrips
//!
//! Verifies that forward operations return correct inverse operations,
//! and that executing the inverse restores the original state.

#[cfg(test)]
#[cfg(feature = "integration-tests")]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;
    use tokio::time::sleep;

    use holon::core::datasource::{
        Change, CrudOperations, DataSource, OperationResult, TaskOperations, UndoAction,
    };
    use holon_api::Value;

    use crate::fake::TodoistTaskFake;
    use crate::models::TodoistTask;

    struct InMemoryDataSource {
        tasks: Arc<RwLock<HashMap<String, TodoistTask>>>,
    }

    impl InMemoryDataSource {
        fn new() -> Self {
            Self {
                tasks: Arc::new(RwLock::new(HashMap::new())),
            }
        }

        async fn apply_change(&self, change: Change<TodoistTask>) {
            let mut tasks = self.tasks.write().await;
            match change {
                Change::Created { data, .. } | Change::Updated { data, .. } => {
                    tasks.insert(data.id.clone(), data);
                }
                Change::Deleted { id, .. } => {
                    tasks.remove(&id);
                }
                _ => {}
            }
        }
    }

    #[async_trait::async_trait]
    impl DataSource<TodoistTask> for InMemoryDataSource {
        async fn get_all(&self) -> holon::core::datasource::Result<Vec<TodoistTask>> {
            Ok(self.tasks.read().await.values().cloned().collect())
        }
        async fn get_by_id(
            &self,
            id: &str,
        ) -> holon::core::datasource::Result<Option<TodoistTask>> {
            Ok(self.tasks.read().await.get(id).cloned())
        }
    }

    async fn setup() -> (Arc<TodoistTaskFake>, Arc<InMemoryDataSource>) {
        let cache = Arc::new(InMemoryDataSource::new());
        let fake = Arc::new(TodoistTaskFake::new(
            Arc::clone(&cache) as Arc<dyn DataSource<TodoistTask>>
        ));

        let cache_clone = Arc::clone(&cache);
        let mut rx = fake.subscribe();
        tokio::spawn(async move {
            while let Ok(changes) = rx.recv().await {
                for change in changes {
                    cache_clone.apply_change(change).await;
                }
            }
        });

        sleep(Duration::from_millis(10)).await;
        (fake, cache)
    }

    async fn create_task(fake: &TodoistTaskFake, content: &str) -> (String, OperationResult) {
        let mut fields = HashMap::new();
        fields.insert("content".into(), Value::String(content.into()));
        fields.insert("project_id".into(), Value::String("inbox".into()));
        fake.create(fields).await.unwrap()
    }

    fn assert_inverse_op(result: &OperationResult, expected_op_name: &str) {
        match &result.undo {
            UndoAction::Undo(op) => {
                assert_eq!(op.op_name, expected_op_name, "inverse op_name mismatch");
            }
            UndoAction::Irreversible => {
                panic!("expected reversible operation, got Irreversible");
            }
        }
    }

    // ====================================================================
    // Create → inverse delete → verify gone
    // ====================================================================

    #[tokio::test]
    async fn create_returns_delete_inverse() {
        let (fake, _cache) = setup().await;
        let (_id, result) = create_task(&fake, "test task").await;
        assert_inverse_op(&result, "delete");
    }

    #[tokio::test]
    async fn create_then_inverse_delete_removes_task() {
        let (fake, cache) = setup().await;
        let (id, _create_result) = create_task(&fake, "doomed task").await;
        sleep(Duration::from_millis(50)).await;

        // Task exists
        assert!(cache.get_by_id(&id).await.unwrap().is_some());

        // Execute inverse (delete)
        let delete_result = fake.delete(&id).await.unwrap();
        sleep(Duration::from_millis(50)).await;

        // Task gone
        assert!(cache.get_by_id(&id).await.unwrap().is_none());

        // Delete inverse should be a create
        assert_inverse_op(&delete_result, "create");
    }

    // ====================================================================
    // Delete → inverse create → verify restored
    // ====================================================================

    #[tokio::test]
    async fn delete_returns_create_inverse_with_fields() {
        let (fake, _cache) = setup().await;
        let (id, _) = create_task(&fake, "to delete").await;
        sleep(Duration::from_millis(50)).await;

        let result = fake.delete(&id).await.unwrap();
        assert_inverse_op(&result, "create");

        // Verify the inverse create operation has the original task fields
        if let UndoAction::Undo(op) = &result.undo {
            let content = op.params.get("content").unwrap();
            assert_eq!(content, &Value::String("to delete".into()));
        }
    }

    // ====================================================================
    // set_field(content) → inverse set_field(old_content) → verify restored
    // ====================================================================

    #[tokio::test]
    async fn set_field_content_returns_inverse_with_old_value() {
        let (fake, _cache) = setup().await;
        let (id, _) = create_task(&fake, "original").await;
        sleep(Duration::from_millis(50)).await;

        let result = fake
            .set_field(&id, "content", Value::String("modified".into()))
            .await
            .unwrap();
        assert_inverse_op(&result, "set_field");

        // Verify inverse has old value
        if let UndoAction::Undo(op) = &result.undo {
            let value_param = op.params.get("value").unwrap();
            assert_eq!(value_param, &Value::String("original".into()));
        }
    }

    #[tokio::test]
    async fn set_field_content_roundtrip_restores_original() {
        let (fake, cache) = setup().await;
        let (id, _) = create_task(&fake, "original").await;
        sleep(Duration::from_millis(50)).await;

        // Forward: change content
        fake.set_field(&id, "content", Value::String("modified".into()))
            .await
            .unwrap();
        sleep(Duration::from_millis(50)).await;
        assert_eq!(
            cache.get_by_id(&id).await.unwrap().unwrap().content,
            "modified"
        );

        // Inverse: restore old content
        fake.set_field(&id, "content", Value::String("original".into()))
            .await
            .unwrap();
        sleep(Duration::from_millis(50)).await;
        assert_eq!(
            cache.get_by_id(&id).await.unwrap().unwrap().content,
            "original"
        );
    }

    // ====================================================================
    // set_state → inverse → verify restored
    // ====================================================================

    #[tokio::test]
    async fn set_state_completed_returns_inverse_active() {
        let (fake, _cache) = setup().await;
        let (id, _) = create_task(&fake, "task").await;
        sleep(Duration::from_millis(50)).await;

        let result = fake.set_state(&id, "completed".into()).await.unwrap();
        assert_inverse_op(&result, "set_state");

        if let UndoAction::Undo(op) = &result.undo {
            let state_param = op.params.get("task_state").unwrap();
            assert_eq!(state_param, &Value::String("active".into()));
        }
    }

    #[tokio::test]
    async fn set_state_roundtrip_restores_original() {
        let (fake, cache) = setup().await;
        let (id, _) = create_task(&fake, "task").await;
        sleep(Duration::from_millis(50)).await;
        assert!(!cache.get_by_id(&id).await.unwrap().unwrap().completed);

        // Complete
        fake.set_state(&id, "completed".into()).await.unwrap();
        sleep(Duration::from_millis(50)).await;
        assert!(cache.get_by_id(&id).await.unwrap().unwrap().completed);

        // Undo → active
        fake.set_state(&id, "active".into()).await.unwrap();
        sleep(Duration::from_millis(50)).await;
        assert!(!cache.get_by_id(&id).await.unwrap().unwrap().completed);
    }

    // ====================================================================
    // set_priority → inverse → verify restored
    // ====================================================================

    #[tokio::test]
    async fn set_priority_returns_inverse_with_old_priority() {
        let (fake, _cache) = setup().await;
        let (id, _) = create_task(&fake, "task").await;
        sleep(Duration::from_millis(50)).await;

        // Default priority is 1
        let result = fake.set_priority(&id, 4).await.unwrap();
        assert_inverse_op(&result, "set_priority");

        if let UndoAction::Undo(op) = &result.undo {
            let prio_param = op.params.get("priority").unwrap();
            assert_eq!(prio_param, &Value::Integer(1)); // old default priority
        }
    }

    #[tokio::test]
    async fn set_priority_roundtrip_restores_original() {
        let (fake, cache) = setup().await;
        let (id, _) = create_task(&fake, "task").await;
        sleep(Duration::from_millis(50)).await;
        assert_eq!(cache.get_by_id(&id).await.unwrap().unwrap().priority, 1);

        // Change to priority 4
        fake.set_priority(&id, 4).await.unwrap();
        sleep(Duration::from_millis(50)).await;
        assert_eq!(cache.get_by_id(&id).await.unwrap().unwrap().priority, 4);

        // Undo → priority 1
        fake.set_priority(&id, 1).await.unwrap();
        sleep(Duration::from_millis(50)).await;
        assert_eq!(cache.get_by_id(&id).await.unwrap().unwrap().priority, 1);
    }

    // ====================================================================
    // set_field(description) → inverse → verify restored
    // ====================================================================

    #[tokio::test]
    async fn set_field_description_roundtrip() {
        let (fake, cache) = setup().await;
        let (id, _) = create_task(&fake, "task").await;
        sleep(Duration::from_millis(50)).await;

        // Set description
        fake.set_field(&id, "description", Value::String("my desc".into()))
            .await
            .unwrap();
        sleep(Duration::from_millis(50)).await;
        assert_eq!(
            cache
                .get_by_id(&id)
                .await
                .unwrap()
                .unwrap()
                .description
                .as_deref(),
            Some("my desc")
        );

        // Clear description
        fake.set_field(&id, "description", Value::Null)
            .await
            .unwrap();
        sleep(Duration::from_millis(50)).await;
        assert!(
            cache
                .get_by_id(&id)
                .await
                .unwrap()
                .unwrap()
                .description
                .is_none()
        );
    }
}
