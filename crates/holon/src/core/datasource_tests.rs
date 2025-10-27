//! Tests for BlockOperations trait methods
//!
//! These tests verify that block movement operations work correctly
//! when implemented via the trait system rather than legacy operations.

#[cfg(test)]
mod tests {
    use crate::core::datasource::{
        BlockEntity, BlockOperations, CrudOperations, DataSource, Result,
    };
    use crate::storage::backend::StorageBackend;
    use crate::storage::gen_key_between;
    use crate::storage::turso::TursoBackend;
    use crate::storage::types::StorageEntity;
    use async_trait::async_trait;
    use holon_api::Value;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Simple test block entity
    #[derive(Debug, Clone)]
    struct TestBlock {
        id: String,
        parent_id: Option<String>,
        sort_key: String,
        depth: i64,
        content: String,
    }

    impl BlockEntity for TestBlock {
        fn id(&self) -> &str {
            &self.id
        }

        fn parent_id(&self) -> Option<&str> {
            self.parent_id.as_deref()
        }

        fn sort_key(&self) -> &str {
            &self.sort_key
        }

        fn depth(&self) -> i64 {
            self.depth
        }

        fn content(&self) -> &str {
            &self.content
        }
    }

    /// Test datasource that wraps TursoBackend for block operations
    struct TestBlockDataSource {
        backend: Arc<tokio::sync::RwLock<TursoBackend>>,
    }

    impl TestBlockDataSource {
        fn new(backend: TursoBackend) -> Self {
            Self {
                backend: Arc::new(tokio::sync::RwLock::new(backend)),
            }
        }

        async fn create_table(&self) -> Result<()> {
            let backend = self.backend.read().await;
            let conn = backend.get_connection()?;
            conn.execute(
                "CREATE TABLE IF NOT EXISTS blocks (
                    id TEXT PRIMARY KEY,
                    parent_id TEXT,
                    depth INTEGER NOT NULL DEFAULT 0,
                    sort_key TEXT NOT NULL,
                    content TEXT,
                    collapsed INTEGER NOT NULL DEFAULT 0,
                    completed INTEGER NOT NULL DEFAULT 0,
                    block_type TEXT NOT NULL DEFAULT 'text',
                    created_at INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL DEFAULT 0
                )",
                (),
            )
            .await?;
            Ok(())
        }
    }

    #[async_trait]
    impl DataSource<TestBlock> for TestBlockDataSource {
        async fn get_all(&self) -> Result<Vec<TestBlock>> {
            let backend = self.backend.read().await;
            let conn = backend.get_connection()?;
            let mut stmt = conn
                .prepare("SELECT id, parent_id, depth, sort_key, content FROM blocks")
                .await?;
            let mut rows = stmt.query(()).await?;
            let mut blocks = Vec::new();

            while let Some(row) = rows.next().await? {
                let id: String = row.get(0)?;
                let parent_id: Option<String> = row.get(1)?;
                let depth: i64 = row.get(2)?;
                let sort_key: String = row.get(3)?;
                let content: String = row.get(4)?;

                blocks.push(TestBlock {
                    id,
                    parent_id,
                    sort_key,
                    depth,
                    content,
                });
            }

            Ok(blocks)
        }

        async fn get_by_id(&self, id: &str) -> Result<Option<TestBlock>> {
            let backend = self.backend.read().await;
            let conn = backend.get_connection()?;
            let mut stmt = conn
                .prepare("SELECT id, parent_id, depth, sort_key, content FROM blocks WHERE id = ?")
                .await?;
            let row_result = stmt.query_row((id,)).await;

            match row_result {
                Ok(row) => {
                    let id: String = row.get(0)?;
                    let parent_id: Option<String> = row.get(1)?;
                    let depth: i64 = row.get(2)?;
                    let sort_key: String = row.get(3)?;
                    let content: String = row.get(4)?;

                    Ok(Some(TestBlock {
                        id,
                        parent_id,
                        sort_key,
                        depth,
                        content,
                    }))
                }
                Err(_) => Ok(None),
            }
        }
    }

    #[async_trait]
    impl CrudOperations<TestBlock> for TestBlockDataSource {
        async fn set_field(
            &self,
            id: &str,
            field: &str,
            value: Value,
        ) -> Result<Option<holon_api::Operation>> {
            // Capture old value for inverse operation
            let old_value = {
                let backend = self.backend.read().await;
                StorageBackend::get(&*backend, "blocks", id)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|entity| entity.get(field).cloned())
                    .unwrap_or(Value::Null)
            };

            let mut backend = self.backend.write().await;
            let mut updates = StorageEntity::new();
            updates.insert(field.to_string(), value.clone());
            StorageBackend::update(&mut *backend, "blocks", id, updates)
                .await
                .map_err(
                    |e: crate::storage::StorageError| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("Failed to update: {}", e).into()
                    },
                )?;

            // Return inverse operation
            use holon_core::__operations_crud_operations;
            Ok(Some(__operations_crud_operations::set_field_op(
                "", // Will be set by OperationProvider
                id, field, old_value,
            )))
        }

        async fn create(
            &self,
            fields: HashMap<String, Value>,
        ) -> Result<(String, Option<holon_api::Operation>)> {
            let mut backend = self.backend.write().await;
            let id = fields
                .get("id")
                .and_then(|v| v.as_string())
                .map(|s| s.to_string())
                .ok_or_else(|| anyhow::anyhow!("Missing 'id' field"))?;

            let mut entity = StorageEntity::new();
            for (k, v) in fields {
                entity.insert(k, v);
            }

            StorageBackend::insert(&mut *backend, "blocks", entity)
                .await
                .map_err(
                    |e: crate::storage::StorageError| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("Failed to insert: {}", e).into()
                    },
                )?;

            // Return inverse operation (delete)
            use holon_core::__operations_crud_operations;
            let inverse = Some(__operations_crud_operations::delete_op(
                "", // Will be set by OperationProvider
                &id,
            ));
            Ok((id, inverse))
        }

        async fn delete(&self, id: &str) -> Result<Option<holon_api::Operation>> {
            // Capture full entity for inverse operation (create)
            let create_fields = {
                let backend = self.backend.read().await;
                StorageBackend::get(&*backend, "blocks", id)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_default()
            };

            let mut backend = self.backend.write().await;
            StorageBackend::delete(&mut *backend, "blocks", id)
                .await
                .map_err(
                    |e: crate::storage::StorageError| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("Failed to delete: {}", e).into()
                    },
                )?;

            // Return inverse operation (create)
            use holon_core::__operations_crud_operations;
            Ok(Some(__operations_crud_operations::create_op(
                "", // Will be set by OperationProvider
                create_fields,
            )))
        }
    }

    // Helper functions for tests
    async fn create_blocks_table(ds: &TestBlockDataSource) {
        ds.create_table().await.unwrap();
    }

    async fn insert_block(
        ds: &TestBlockDataSource,
        id: &str,
        parent_id: Option<&str>,
        prev_key: Option<&str>,
    ) {
        let sort_key = gen_key_between(prev_key, None).unwrap();
        let depth = if parent_id.is_some() { 1 } else { 0 };

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::String(id.to_string()));
        fields.insert("sort_key".to_string(), Value::String(sort_key));
        fields.insert("depth".to_string(), Value::Integer(depth));
        fields.insert(
            "content".to_string(),
            Value::String(format!("Content {}", id)),
        );

        if let Some(pid) = parent_id {
            fields.insert("parent_id".to_string(), Value::String(pid.to_string()));
        } else {
            fields.insert("parent_id".to_string(), Value::Null);
        }

        ds.create(fields).await.unwrap();
    }

    async fn get_sort_key(ds: &TestBlockDataSource, id: &str) -> String {
        let block = ds.get_by_id(id).await.unwrap().unwrap();
        block.sort_key().to_string()
    }

    async fn get_parent_id(ds: &TestBlockDataSource, id: &str) -> Option<String> {
        let block = ds.get_by_id(id).await.unwrap().unwrap();
        block.parent_id().map(|s| s.to_string())
    }

    #[tokio::test]
    async fn test_move_block_to_beginning() {
        let (backend, _db_handle) = TursoBackend::new_in_memory().await.unwrap();
        let ds = TestBlockDataSource::new(backend);
        create_blocks_table(&ds).await;

        // Create siblings: A, B, C under parent P
        insert_block(&ds, "P", None, None).await;
        insert_block(&ds, "A", Some("P"), None).await;
        let sort_a = get_sort_key(&ds, "A").await;
        insert_block(&ds, "B", Some("P"), Some(&sort_a)).await;
        let sort_b = get_sort_key(&ds, "B").await;
        insert_block(&ds, "C", Some("P"), Some(&sort_b)).await;

        // Move C to beginning (before A)
        ds.move_block("C", Some("P"), None).await.unwrap();

        // Verify order: C < A < B
        let sort_c = get_sort_key(&ds, "C").await;
        let sort_a = get_sort_key(&ds, "A").await;
        let sort_b = get_sort_key(&ds, "B").await;

        assert!(sort_c < sort_a);
        assert!(sort_a < sort_b);
    }

    #[tokio::test]
    async fn test_move_block_to_end() {
        let (backend, _db_handle) = TursoBackend::new_in_memory().await.unwrap();
        let ds = TestBlockDataSource::new(backend);
        create_blocks_table(&ds).await;

        // Create siblings: A, B, C under parent P
        insert_block(&ds, "P", None, None).await;
        insert_block(&ds, "A", Some("P"), None).await;
        let sort_a = get_sort_key(&ds, "A").await;
        insert_block(&ds, "B", Some("P"), Some(&sort_a)).await;
        let sort_b = get_sort_key(&ds, "B").await;
        insert_block(&ds, "C", Some("P"), Some(&sort_b)).await;

        // Move A to end (after C)
        ds.move_block("A", Some("P"), Some("C")).await.unwrap();

        // Verify order: B < C < A
        let sort_a = get_sort_key(&ds, "A").await;
        let sort_b = get_sort_key(&ds, "B").await;
        let sort_c = get_sort_key(&ds, "C").await;

        assert!(sort_b < sort_c);
        assert!(sort_c < sort_a);
    }

    #[tokio::test]
    async fn test_move_block_between() {
        let (backend, _db_handle) = TursoBackend::new_in_memory().await.unwrap();
        let ds = TestBlockDataSource::new(backend);
        create_blocks_table(&ds).await;

        // Create siblings: A, B, C under parent P
        insert_block(&ds, "P", None, None).await;
        insert_block(&ds, "A", Some("P"), None).await;
        let sort_a = get_sort_key(&ds, "A").await;
        insert_block(&ds, "B", Some("P"), Some(&sort_a)).await;
        let sort_b = get_sort_key(&ds, "B").await;
        insert_block(&ds, "C", Some("P"), Some(&sort_b)).await;

        // Move C between A and B
        ds.move_block("C", Some("P"), Some("A")).await.unwrap();

        // Verify order: A < C < B
        let sort_a = get_sort_key(&ds, "A").await;
        let sort_b = get_sort_key(&ds, "B").await;
        let sort_c = get_sort_key(&ds, "C").await;

        assert!(sort_a < sort_c);
        assert!(sort_c < sort_b);
    }

    #[tokio::test]
    async fn test_move_block_change_parent() {
        let (backend, _db_handle) = TursoBackend::new_in_memory().await.unwrap();
        let ds = TestBlockDataSource::new(backend);
        create_blocks_table(&ds).await;

        // Create structure: P1 -> A, P2 -> B
        insert_block(&ds, "P1", None, None).await;
        insert_block(&ds, "A", Some("P1"), None).await;
        let sort_p1 = get_sort_key(&ds, "P1").await;
        insert_block(&ds, "P2", None, Some(&sort_p1)).await;
        insert_block(&ds, "B", Some("P2"), None).await;

        // Move A under P2 (after B)
        ds.move_block("A", Some("P2"), Some("B")).await.unwrap();

        // Verify A's parent changed to P2
        let parent = get_parent_id(&ds, "A").await;
        assert_eq!(parent, Some("P2".to_string()));

        // Verify order: B < A under P2
        let sort_a = get_sort_key(&ds, "A").await;
        let sort_b = get_sort_key(&ds, "B").await;
        assert!(sort_b < sort_a);
    }

    #[tokio::test]
    async fn test_indent_block() {
        let (backend, _db_handle) = TursoBackend::new_in_memory().await.unwrap();
        let ds = TestBlockDataSource::new(backend);
        create_blocks_table(&ds).await;

        // Create siblings: A, B, C under root
        insert_block(&ds, "A", None, None).await;
        let sort_a = get_sort_key(&ds, "A").await;
        insert_block(&ds, "B", None, Some(&sort_a)).await;
        let sort_b = get_sort_key(&ds, "B").await;
        insert_block(&ds, "C", None, Some(&sort_b)).await;

        // Indent B (move under A)
        ds.indent("B", "A").await.unwrap();

        // Verify B's parent is now A
        let parent = get_parent_id(&ds, "B").await;
        assert_eq!(parent, Some("A".to_string()));
    }

    #[tokio::test]
    async fn test_outdent_block() {
        let (backend, _db_handle) = TursoBackend::new_in_memory().await.unwrap();
        let ds = TestBlockDataSource::new(backend);
        create_blocks_table(&ds).await;

        // Create structure: A -> B -> C
        insert_block(&ds, "A", None, None).await;
        insert_block(&ds, "B", Some("A"), None).await;
        insert_block(&ds, "C", Some("B"), None).await;

        // Outdent C (move to A's level, after B)
        ds.outdent("C").await.unwrap();

        // Verify C's parent is now A (same as B's parent)
        let parent = get_parent_id(&ds, "C").await;
        assert_eq!(parent, Some("A".to_string()));

        // Verify C comes after B
        let sort_b = get_sort_key(&ds, "B").await;
        let sort_c = get_sort_key(&ds, "C").await;
        assert!(sort_b < sort_c);
    }

    #[tokio::test]
    async fn test_outdent_at_root_fails() {
        let (backend, _db_handle) = TursoBackend::new_in_memory().await.unwrap();
        let ds = TestBlockDataSource::new(backend);
        create_blocks_table(&ds).await;

        // Create block at root
        insert_block(&ds, "A", None, None).await;

        // Try to outdent A (should fail - already at root)
        let result = ds.outdent("A").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot outdent root block"));
    }

    #[tokio::test]
    async fn test_move_up() {
        let (backend, _db_handle) = TursoBackend::new_in_memory().await.unwrap();
        let ds = TestBlockDataSource::new(backend);
        create_blocks_table(&ds).await;

        // Create siblings: A, B, C under parent P
        insert_block(&ds, "P", None, None).await;
        insert_block(&ds, "A", Some("P"), None).await;
        let sort_a = get_sort_key(&ds, "A").await;
        insert_block(&ds, "B", Some("P"), Some(&sort_a)).await;
        let sort_b = get_sort_key(&ds, "B").await;
        insert_block(&ds, "C", Some("P"), Some(&sort_b)).await;

        // Move C up (swap with B)
        ds.move_up("C").await.unwrap();

        // Verify order: A < C < B
        let sort_a = get_sort_key(&ds, "A").await;
        let sort_b = get_sort_key(&ds, "B").await;
        let sort_c = get_sort_key(&ds, "C").await;

        assert!(sort_a < sort_c);
        assert!(sort_c < sort_b);
    }

    #[tokio::test]
    async fn test_move_down() {
        let (backend, _db_handle) = TursoBackend::new_in_memory().await.unwrap();
        let ds = TestBlockDataSource::new(backend);
        create_blocks_table(&ds).await;

        // Create siblings: A, B, C under parent P
        insert_block(&ds, "P", None, None).await;
        insert_block(&ds, "A", Some("P"), None).await;
        let sort_a = get_sort_key(&ds, "A").await;
        insert_block(&ds, "B", Some("P"), Some(&sort_a)).await;
        let sort_b = get_sort_key(&ds, "B").await;
        insert_block(&ds, "C", Some("P"), Some(&sort_b)).await;

        // Move A down (swap with B)
        ds.move_down("A").await.unwrap();

        // Verify order: B < A < C
        let sort_a = get_sort_key(&ds, "A").await;
        let sort_b = get_sort_key(&ds, "B").await;
        let sort_c = get_sort_key(&ds, "C").await;

        assert!(sort_b < sort_a);
        assert!(sort_a < sort_c);
    }

    #[tokio::test]
    async fn test_split_block() {
        let (backend, _db_handle) = TursoBackend::new_in_memory().await.unwrap();
        let ds = TestBlockDataSource::new(backend);
        create_blocks_table(&ds).await;

        // Create a block with content
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::String("A".to_string()));
        fields.insert(
            "sort_key".to_string(),
            Value::String(gen_key_between(None, None).unwrap()),
        );
        fields.insert("depth".to_string(), Value::Integer(0));
        fields.insert(
            "content".to_string(),
            Value::String("Hello World".to_string()),
        );
        fields.insert("parent_id".to_string(), Value::Null);
        ds.create(fields).await.unwrap();

        // Split at position 6 (after "Hello ")
        ds.split_block("A", 6).await.unwrap();

        // Verify original block has truncated content
        let block_a = ds.get_by_id("A").await.unwrap().unwrap();
        assert_eq!(block_a.content(), "Hello");

        // Verify new block was created (should be the only other block)
        let all_blocks = ds.get_all().await.unwrap();
        assert_eq!(all_blocks.len(), 2);
        let new_block = all_blocks.iter().find(|b| b.id() != "A").unwrap();
        assert_eq!(new_block.content(), "World");
    }

    #[tokio::test]
    async fn test_move_block_ordering_preserved() {
        let (backend, _db_handle) = TursoBackend::new_in_memory().await.unwrap();
        let ds = TestBlockDataSource::new(backend);
        create_blocks_table(&ds).await;

        // Create many siblings to test ordering
        insert_block(&ds, "P", None, None).await;
        let mut prev_sort = None;
        for i in 0..10 {
            let id = format!("B{}", i);
            insert_block(&ds, &id, Some("P"), prev_sort.as_deref()).await;
            prev_sort = Some(get_sort_key(&ds, &id).await);
        }

        // Move B5 between B2 and B3
        ds.move_block("B5", Some("P"), Some("B2")).await.unwrap();

        // Verify B2 < B5 < B3
        let sort_2 = get_sort_key(&ds, "B2").await;
        let sort_3 = get_sort_key(&ds, "B3").await;
        let sort_5 = get_sort_key(&ds, "B5").await;

        assert!(sort_2 < sort_5);
        assert!(sort_5 < sort_3);
    }
}
