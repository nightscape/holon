//! Tests for BlockOperations default implementations (indent, outdent, move_block, etc.)

#[cfg(test)]
mod tests {
    use crate::fractional_index::gen_key_between;
    use crate::traits::*;
    use async_trait::async_trait;
    use holon_api::Value;
    use std::collections::HashMap;
    use std::sync::Mutex;

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

    /// In-memory block store for testing
    struct MemStore {
        blocks: Mutex<Vec<TestBlock>>,
    }

    impl MemStore {
        fn new() -> Self {
            Self {
                blocks: Mutex::new(Vec::new()),
            }
        }

        fn insert(&self, block: TestBlock) {
            self.blocks.lock().unwrap().push(block);
        }

        fn get(&self, id: &str) -> Option<TestBlock> {
            self.blocks
                .lock()
                .unwrap()
                .iter()
                .find(|b| b.id == id)
                .cloned()
        }

        fn sorted_children(&self, parent_id: &str) -> Vec<TestBlock> {
            let blocks = self.blocks.lock().unwrap();
            let mut children: Vec<TestBlock> = blocks
                .iter()
                .filter(|b| b.parent_id.as_deref() == Some(parent_id))
                .cloned()
                .collect();
            children.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
            children
        }
    }

    #[async_trait]
    impl DataSource<TestBlock> for MemStore {
        async fn get_all(&self) -> Result<Vec<TestBlock>> {
            Ok(self.blocks.lock().unwrap().clone())
        }
        async fn get_by_id(&self, id: &str) -> Result<Option<TestBlock>> {
            Ok(self.get(id))
        }
    }

    #[async_trait]
    impl CrudOperations<TestBlock> for MemStore {
        async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<OperationResult> {
            let mut blocks = self.blocks.lock().unwrap();
            let block = blocks.iter_mut().find(|b| b.id == id).unwrap();
            let old_value = match field {
                "parent_id" => block
                    .parent_id
                    .as_ref()
                    .map_or(Value::Null, |v| Value::String(v.clone())),
                "sort_key" => Value::String(block.sort_key.clone()),
                "depth" => Value::Integer(block.depth),
                "content" => Value::String(block.content.clone()),
                _ => Value::Null,
            };
            match field {
                "parent_id" => block.parent_id = value.as_string().map(|s| s.to_string()),
                "sort_key" => block.sort_key = value.as_string().unwrap().to_string(),
                "depth" => block.depth = value.as_i64().unwrap(),
                "content" => block.content = value.as_string().unwrap().to_string(),
                _ => {}
            }
            Ok(OperationResult::new(
                vec![FieldDelta::new(id, field, old_value, value)],
                holon_api::Operation::new("test", "set_field", "set_field", HashMap::new()),
            ))
        }

        async fn create(
            &self,
            fields: HashMap<String, Value>,
        ) -> Result<(String, OperationResult)> {
            let id = fields
                .get("id")
                .and_then(|v| v.as_string())
                .unwrap()
                .to_string();
            let block = TestBlock {
                id: id.clone(),
                parent_id: fields
                    .get("parent_id")
                    .and_then(|v| v.as_string())
                    .map(|s| s.to_string()),
                sort_key: fields
                    .get("sort_key")
                    .and_then(|v| v.as_string())
                    .unwrap_or("a0")
                    .to_string(),
                depth: fields.get("depth").and_then(|v| v.as_i64()).unwrap_or(0),
                content: fields
                    .get("title")
                    .and_then(|v| v.as_string())
                    .unwrap_or("")
                    .to_string(),
            };
            self.blocks.lock().unwrap().push(block);
            Ok((id, OperationResult::irreversible(vec![])))
        }

        async fn delete(&self, id: &str) -> Result<OperationResult> {
            self.blocks.lock().unwrap().retain(|b| b.id != id);
            Ok(OperationResult::irreversible(vec![]))
        }
    }

    impl BlockQueryHelpers<TestBlock> for MemStore {}
    impl BlockMaintenanceHelpers<TestBlock> for MemStore {}
    impl BlockDataSourceHelpers<TestBlock> for MemStore {}
    impl BlockOperations<TestBlock> for MemStore {}

    fn insert_block(store: &MemStore, id: &str, parent_id: Option<&str>, prev_key: Option<&str>) {
        let sort_key = gen_key_between(prev_key, None).unwrap();
        let depth: i64 = if parent_id.is_some() { 1 } else { 0 };
        store.insert(TestBlock {
            id: id.to_string(),
            parent_id: parent_id.map(|s| s.to_string()),
            sort_key,
            depth,
            content: format!("Content {}", id),
        });
    }

    #[tokio::test]
    async fn move_block_to_beginning() {
        let store = MemStore::new();
        insert_block(&store, "P", None, None);
        insert_block(&store, "A", Some("P"), None);
        let key_b;
        {
            let children = store.sorted_children("P");
            key_b = children.last().map(|c| c.sort_key.clone());
        }
        insert_block(&store, "B", Some("P"), key_b.as_deref());
        insert_block(&store, "C", Some("P"), {
            let children = store.sorted_children("P");
            children
                .last()
                .map(|c| c.sort_key.as_str().to_string())
                .as_deref()
        });

        // Move C to beginning
        store.move_block("C", "P", None).await.unwrap();

        let children = store.sorted_children("P");
        assert_eq!(children[0].id, "C");
        assert_eq!(children[1].id, "A");
        assert_eq!(children[2].id, "B");
    }

    #[tokio::test]
    async fn move_block_after_specific() {
        let store = MemStore::new();
        insert_block(&store, "P", None, None);
        insert_block(&store, "A", Some("P"), None);
        let key_a = store.sorted_children("P").last().unwrap().sort_key.clone();
        insert_block(&store, "B", Some("P"), Some(&key_a));
        let key_b = store.sorted_children("P").last().unwrap().sort_key.clone();
        insert_block(&store, "C", Some("P"), Some(&key_b));

        // Move A after B
        store.move_block("A", "P", Some("B")).await.unwrap();

        let children = store.sorted_children("P");
        assert_eq!(children[0].id, "B");
        assert_eq!(children[1].id, "A");
        assert_eq!(children[2].id, "C");
    }

    #[tokio::test]
    async fn indent_moves_under_prev_sibling() {
        let store = MemStore::new();
        insert_block(&store, "P", None, None);
        insert_block(&store, "A", Some("P"), None);
        let key_a = store.sorted_children("P").last().unwrap().sort_key.clone();
        insert_block(&store, "B", Some("P"), Some(&key_a));

        // Indent B under A
        store.indent("B", "A").await.unwrap();

        let b = store.get("B").unwrap();
        assert_eq!(b.parent_id.as_deref(), Some("A"));
        assert_eq!(b.depth, 2); // A is depth 1, so B becomes depth 2
    }

    #[tokio::test]
    async fn outdent_moves_to_grandparent() {
        let store = MemStore::new();
        insert_block(&store, "GP", None, None);
        insert_block(&store, "P", Some("GP"), None);

        // B is child of P, depth 2
        let b = TestBlock {
            id: "B".to_string(),
            parent_id: Some("P".to_string()),
            sort_key: gen_key_between(None, None).unwrap(),
            depth: 2,
            content: "Content B".to_string(),
        };
        store.insert(b);

        // Outdent B: should move to GP level, after P
        store.outdent("B").await.unwrap();

        let b = store.get("B").unwrap();
        assert_eq!(b.parent_id.as_deref(), Some("GP"));
        assert_eq!(b.depth, 1); // GP is depth 0, so B becomes depth 1
    }

    #[tokio::test]
    async fn outdent_root_block_fails() {
        let store = MemStore::new();
        insert_block(&store, "R", None, None);

        let result = store.outdent("R").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn move_up_swaps_with_prev_sibling() {
        let store = MemStore::new();
        insert_block(&store, "P", None, None);
        insert_block(&store, "A", Some("P"), None);
        let key_a = store.sorted_children("P").last().unwrap().sort_key.clone();
        insert_block(&store, "B", Some("P"), Some(&key_a));

        // Move B up (before A)
        store.move_up("B").await.unwrap();

        let children = store.sorted_children("P");
        assert_eq!(children[0].id, "B");
        assert_eq!(children[1].id, "A");
    }

    #[tokio::test]
    async fn move_up_first_child_fails() {
        let store = MemStore::new();
        insert_block(&store, "P", None, None);
        insert_block(&store, "A", Some("P"), None);

        let result = store.move_up("A").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn move_down_swaps_with_next_sibling() {
        let store = MemStore::new();
        insert_block(&store, "P", None, None);
        insert_block(&store, "A", Some("P"), None);
        let key_a = store.sorted_children("P").last().unwrap().sort_key.clone();
        insert_block(&store, "B", Some("P"), Some(&key_a));

        // Move A down (after B)
        store.move_down("A").await.unwrap();

        let children = store.sorted_children("P");
        assert_eq!(children[0].id, "B");
        assert_eq!(children[1].id, "A");
    }

    #[tokio::test]
    async fn move_down_last_child_fails() {
        let store = MemStore::new();
        insert_block(&store, "P", None, None);
        insert_block(&store, "A", Some("P"), None);

        let result = store.move_down("A").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn split_block_divides_content() {
        let store = MemStore::new();
        insert_block(&store, "P", None, None);
        store.insert(TestBlock {
            id: "A".to_string(),
            parent_id: Some("P".to_string()),
            sort_key: gen_key_between(None, None).unwrap(),
            depth: 1,
            content: "Hello World".to_string(),
        });

        store.split_block("A", 5).await.unwrap();

        let a = store.get("A").unwrap();
        assert_eq!(a.content, "Hello");

        // Find the new block (not A, not P, child of P)
        let children = store.sorted_children("P");
        assert_eq!(children.len(), 2); // A and the new block
        let new_block = children.iter().find(|b| b.id != "A").unwrap();
        assert_eq!(new_block.content, "World");
    }

    #[tokio::test]
    async fn split_block_at_start() {
        let store = MemStore::new();
        insert_block(&store, "P", None, None);
        store.insert(TestBlock {
            id: "A".to_string(),
            parent_id: Some("P".to_string()),
            sort_key: gen_key_between(None, None).unwrap(),
            depth: 1,
            content: "Hello".to_string(),
        });

        store.split_block("A", 0).await.unwrap();

        let a = store.get("A").unwrap();
        assert_eq!(a.content, "");

        let children = store.sorted_children("P");
        let new_block = children.iter().find(|b| b.id != "A").unwrap();
        assert_eq!(new_block.content, "Hello");
    }

    #[tokio::test]
    async fn split_block_invalid_position_fails() {
        let store = MemStore::new();
        insert_block(&store, "P", None, None);
        store.insert(TestBlock {
            id: "A".to_string(),
            parent_id: Some("P".to_string()),
            sort_key: gen_key_between(None, None).unwrap(),
            depth: 1,
            content: "Hi".to_string(),
        });

        let result = store.split_block("A", 10).await;
        assert!(result.is_err());

        let result = store.split_block("A", -1).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn move_block_returns_inverse() {
        let store = MemStore::new();
        insert_block(&store, "P", None, None);
        insert_block(&store, "A", Some("P"), None);
        let key_a = store.sorted_children("P").last().unwrap().sort_key.clone();
        insert_block(&store, "B", Some("P"), Some(&key_a));
        let key_b = store.sorted_children("P").last().unwrap().sort_key.clone();
        insert_block(&store, "C", Some("P"), Some(&key_b));

        let result = store.move_block("C", "P", None).await.unwrap();
        assert!(result.undo.is_reversible());
    }

    #[tokio::test]
    async fn indent_returns_inverse() {
        let store = MemStore::new();
        insert_block(&store, "P", None, None);
        insert_block(&store, "A", Some("P"), None);
        let key_a = store.sorted_children("P").last().unwrap().sort_key.clone();
        insert_block(&store, "B", Some("P"), Some(&key_a));

        let result = store.indent("B", "A").await.unwrap();
        assert!(result.undo.is_reversible());
    }

    #[tokio::test]
    async fn descendant_depth_update_on_move() {
        let store = MemStore::new();
        insert_block(&store, "P1", None, None); // depth 0
        insert_block(&store, "P2", None, None); // depth 0

        // A is child of P1, depth 1
        store.insert(TestBlock {
            id: "A".to_string(),
            parent_id: Some("P1".to_string()),
            sort_key: gen_key_between(None, None).unwrap(),
            depth: 1,
            content: "A".to_string(),
        });
        // B is child of A, depth 2
        store.insert(TestBlock {
            id: "B".to_string(),
            parent_id: Some("A".to_string()),
            sort_key: gen_key_between(None, None).unwrap(),
            depth: 2,
            content: "B".to_string(),
        });

        // Move A under P2 (depth doesn't change since both parents are depth 0)
        store.move_block("A", "P2", None).await.unwrap();

        let a = store.get("A").unwrap();
        assert_eq!(a.parent_id.as_deref(), Some("P2"));
        assert_eq!(a.depth, 1);

        let b = store.get("B").unwrap();
        assert_eq!(b.depth, 2); // unchanged since depth delta is 0
    }
}
