//! TUI-R3BL backend for property-based testing
//!
//! This module provides a CoreOperations implementation that wraps BackendEngine
//! for PBT tests. It converts between the database representation (HashMap<String, Value>)
//! and the Block API representation.

use anyhow;
use async_trait::async_trait;
use holon::api::backend_engine::BackendEngine;
use holon::api::repository::{CoreOperations, Lifecycle};
use holon::api::types::{NewBlock, Traversal};
use holon::storage::backend::StorageBackend;
use holon::storage::types::StorageEntity;
use holon_api::Value;
use holon_api::{ApiError, Block, BlockMetadata, ROOT_PARENT_ID};
use std::collections::HashMap;
use std::sync::Arc;

/// TUI-R3BL backend for PBT testing
///
/// This backend wraps BackendEngine and implements CoreOperations by:
/// - Querying blocks via SQL queries to the blocks table
/// - Converting HashMap<String, Value> to Block format
/// - Using StorageBackend methods for CRUD operations
#[derive(Clone)]
pub struct TuiR3blPbtBackend {
    engine: Arc<BackendEngine>,
}

impl TuiR3blPbtBackend {
    /// Create a new PBT backend wrapping a BackendEngine
    pub fn new(engine: Arc<BackendEngine>) -> Self {
        Self { engine }
    }

    /// Convert a StorageEntity (HashMap) to a Block
    fn entity_to_block(entity: &StorageEntity, children: Vec<String>) -> Result<Block, ApiError> {
        let id = entity
            .get("id")
            .and_then(|v| v.as_string())
            .ok_or_else(|| ApiError::InternalError {
                message: "Block missing id field".to_string(),
            })?
            .to_string();

        let parent_id = entity
            .get("parent_id")
            .and_then(|v| {
                v.as_string().map(|s| {
                    if s.is_empty() {
                        ROOT_PARENT_ID.to_string()
                    } else {
                        s.to_string()
                    }
                })
            })
            .unwrap_or_else(|| ROOT_PARENT_ID.to_string());

        let content_str = entity
            .get("content")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let content = holon_api::BlockContent::text(&content_str);

        // Parse timestamps - handle both string and integer formats
        let created_at = entity
            .get("created_at")
            .and_then(|v| {
                if let Some(s) = v.as_string() {
                    // Try parsing as ISO8601 datetime string first
                    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                        Some(dt.timestamp_millis())
                    } else if let Ok(ts) = s.parse::<i64>() {
                        // Try parsing as Unix timestamp string
                        Some(ts)
                    } else {
                        None
                    }
                } else {
                    v.as_i64()
                }
            })
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
            });

        let updated_at = entity
            .get("updated_at")
            .and_then(|v| {
                if let Some(s) = v.as_string() {
                    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                        Some(dt.timestamp_millis())
                    } else if let Ok(ts) = s.parse::<i64>() {
                        Some(ts)
                    } else {
                        None
                    }
                } else {
                    v.as_i64()
                }
            })
            .unwrap_or(created_at);

        Ok(Block {
            id,
            parent_id,
            content,
            properties: std::collections::HashMap::new(),
            children,
            metadata: BlockMetadata {
                created_at,
                updated_at,
            },
        })
    }

    /// Query all blocks from the database
    async fn query_all_blocks(&self) -> Result<Vec<StorageEntity>, ApiError> {
        let engine = &*self.engine;

        // Query all blocks using SQL
        let sql = "SELECT * FROM blocks".to_string();
        engine
            .execute_query(sql, HashMap::new())
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to query blocks: {}", e),
            })
    }

    /// Build children list for a block by querying all blocks
    async fn get_children_for_block(&self, parent_id: &str) -> Result<Vec<String>, ApiError> {
        let engine = &*self.engine;

        // Map ROOT_PARENT_ID to NULL for database queries
        let sql = if parent_id == ROOT_PARENT_ID {
            "SELECT * FROM blocks WHERE parent_id IS NULL ORDER BY sort_key".to_string()
        } else {
            format!(
                "SELECT * FROM blocks WHERE parent_id = '{}' ORDER BY sort_key",
                parent_id
            )
        };

        let mut children = engine
            .execute_query(sql, HashMap::new())
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to get children: {}", e),
            })?;

        // Sort by sort_key if present (already sorted by SQL, but ensure)
        children.sort_by(|a, b| {
            let a_sort = a.get("sort_key").and_then(|v| v.as_string()).unwrap_or("");
            let b_sort = b.get("sort_key").and_then(|v| v.as_string()).unwrap_or("");
            a_sort.cmp(b_sort)
        });

        Ok(children
            .iter()
            .filter_map(|e| {
                e.get("id")
                    .and_then(|v| v.as_string())
                    .map(|s| s.to_string())
            })
            .collect())
    }

    /// Ensure blocks table schema exists
    pub async fn ensure_schema(&self) -> Result<(), ApiError> {
        let engine = &*self.engine;

        // Check if blocks table exists by trying to query it
        let check_sql =
            "SELECT name FROM sqlite_master WHERE type='table' AND name='blocks'".to_string();
        let result = engine.execute_query(check_sql, HashMap::new()).await;

        if result.is_err() || result.unwrap().is_empty() {
            // Create blocks table
            let create_table_sql = r#"
                CREATE TABLE IF NOT EXISTS blocks (
                    id TEXT PRIMARY KEY,
                    parent_id TEXT,
                    depth INTEGER NOT NULL DEFAULT 0,
                    sort_key TEXT NOT NULL,
                    content TEXT NOT NULL,
                    collapsed INTEGER NOT NULL DEFAULT 0,
                    completed INTEGER NOT NULL DEFAULT 0,
                    block_type TEXT NOT NULL DEFAULT 'text',
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
            "#
            .to_string();

            engine
                .execute_query(create_table_sql, HashMap::new())
                .await
                .map_err(|e| ApiError::InternalError {
                    message: format!("Failed to create blocks table: {}", e),
                })?;

            // Create root block if it doesn't exist
            let root_exists = {
                let check_sql = format!("SELECT id FROM blocks WHERE id = '{}'", ROOT_PARENT_ID);
                let result = engine.execute_query(check_sql, HashMap::new()).await;
                result.is_ok() && !result.unwrap().is_empty()
            };

            if !root_exists {
                let mut root_entity = StorageEntity::new();
                root_entity.insert("id".to_string(), Value::String(ROOT_PARENT_ID.to_string()));
                root_entity.insert(
                    "parent_id".to_string(),
                    Value::String("__no_parent__".to_string()),
                );
                root_entity.insert("content".to_string(), Value::String(String::new()));
                root_entity.insert("depth".to_string(), Value::Integer(0));
                root_entity.insert("sort_key".to_string(), Value::String("a0".to_string()));
                root_entity.insert("collapsed".to_string(), Value::Integer(0));
                root_entity.insert("completed".to_string(), Value::Integer(0));
                root_entity.insert("block_type".to_string(), Value::String("text".to_string()));

                // Insert root block using backend
                let root_entity_clone = root_entity.clone();
                engine
                    .with_backend_write(|backend: &mut _| -> anyhow::Result<()> {
                        // Use block_in_place to execute async operation in sync context
                        tokio::task::block_in_place(|| {
                            tokio::runtime::Handle::current()
                                .block_on(backend.insert("blocks", root_entity_clone))
                        })
                        .map_err(|e| anyhow::anyhow!("{}", e))
                    })
                    .await
                    .map_err(|e| ApiError::InternalError {
                        message: format!("Failed to create root block: {}", e),
                    })?;

                // Create initial child block (matching MemoryBackend behavior)
                use holon::storage::fractional_index::gen_key_between;
                let first_child_id = "local://0".to_string();
                let first_child_sort_key =
                    gen_key_between(None, None).map_err(|e| ApiError::InternalError {
                        message: format!("Failed to generate sort_key for initial child: {}", e),
                    })?;

                let mut first_child_entity = StorageEntity::new();
                first_child_entity.insert("id".to_string(), Value::String(first_child_id.clone()));
                first_child_entity.insert("parent_id".to_string(), Value::Null); // NULL = root parent
                first_child_entity.insert("content".to_string(), Value::String(String::new()));
                first_child_entity.insert("depth".to_string(), Value::Integer(0));
                first_child_entity
                    .insert("sort_key".to_string(), Value::String(first_child_sort_key));
                first_child_entity.insert("collapsed".to_string(), Value::Integer(0));
                first_child_entity.insert("completed".to_string(), Value::Integer(0));
                first_child_entity
                    .insert("block_type".to_string(), Value::String("text".to_string()));

                let first_child_entity_clone = first_child_entity.clone();
                engine
                    .with_backend_write(|backend: &mut _| -> anyhow::Result<()> {
                        tokio::task::block_in_place(|| {
                            tokio::runtime::Handle::current()
                                .block_on(backend.insert("blocks", first_child_entity_clone))
                        })
                        .map_err(|e| anyhow::anyhow!("{}", e))
                    })
                    .await
                    .map_err(|e| ApiError::InternalError {
                        message: format!("Failed to create initial child block: {}", e),
                    })?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl CoreOperations for TuiR3blPbtBackend {
    async fn get_block(&self, id: &str) -> Result<Block, ApiError> {
        self.ensure_schema().await?;

        let engine = &*self.engine;

        // Query block by ID
        let sql = format!("SELECT * FROM blocks WHERE id = '{}'", id);
        let results = engine
            .execute_query(sql, HashMap::new())
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to get block: {}", e),
            })?;

        let entity = results
            .into_iter()
            .next()
            .ok_or_else(|| ApiError::BlockNotFound { id: id.to_string() })?;

        let children = self.get_children_for_block(id).await?;

        Self::entity_to_block(&entity, children)
    }

    async fn get_all_blocks(&self, traversal: Traversal) -> Result<Vec<Block>, ApiError> {
        self.ensure_schema().await?;

        let all_entities = self.query_all_blocks().await?;

        // Build a map of all blocks for efficient lookup
        let mut block_map: HashMap<String, StorageEntity> = HashMap::new();
        for entity in all_entities {
            if let Some(id) = entity.get("id").and_then(|v| v.as_string()) {
                block_map.insert(id.to_string(), entity);
            }
        }

        // Build children map
        let mut children_map: HashMap<String, Vec<String>> = HashMap::new();
        for (id, entity) in &block_map {
            let parent_id = entity
                .get("parent_id")
                .and_then(|v| v.as_string())
                .map(|s| {
                    if s.is_empty() {
                        ROOT_PARENT_ID.to_string()
                    } else {
                        s.to_string()
                    }
                })
                .unwrap_or_else(|| ROOT_PARENT_ID.to_string());

            children_map
                .entry(parent_id)
                .or_insert_with(Vec::new)
                .push(id.clone());
        }

        // Sort children by sort_key
        for children in children_map.values_mut() {
            children.sort_by(|a_id, b_id| {
                let a_sort = block_map
                    .get(a_id)
                    .and_then(|e| e.get("sort_key").and_then(|v| v.as_string()))
                    .unwrap_or("");
                let b_sort = block_map
                    .get(b_id)
                    .and_then(|e| e.get("sort_key").and_then(|v| v.as_string()))
                    .unwrap_or("");
                a_sort.cmp(b_sort)
            });
        }

        // Depth-first traversal to build result
        let mut result = Vec::new();

        fn traverse(
            block_id: &str,
            current_level: usize,
            traversal: &Traversal,
            block_map: &HashMap<String, StorageEntity>,
            children_map: &HashMap<String, Vec<String>>,
            result: &mut Vec<Block>,
        ) {
            let entity = match block_map.get(block_id) {
                Some(e) => e,
                None => return,
            };

            let children = children_map.get(block_id).cloned().unwrap_or_default();

            // Add current block if it's within the level range
            if traversal.includes_level(current_level) {
                if let Ok(block) = TuiR3blPbtBackend::entity_to_block(entity, children.clone()) {
                    result.push(block);
                }
            }

            // Recursively traverse children
            if current_level < traversal.max_level {
                for child_id in &children {
                    traverse(
                        child_id,
                        current_level + 1,
                        traversal,
                        block_map,
                        children_map,
                        result,
                    );
                }
            }
        }

        traverse(
            ROOT_PARENT_ID,
            0,
            &traversal,
            &block_map,
            &children_map,
            &mut result,
        );

        Ok(result)
    }

    async fn list_children(&self, parent_id: &str) -> Result<Vec<String>, ApiError> {
        self.ensure_schema().await?;
        self.get_children_for_block(parent_id).await
    }

    async fn create_block(
        &self,
        parent_id: String,
        content: holon_api::BlockContent,
        id: Option<String>,
    ) -> Result<Block, ApiError> {
        self.ensure_schema().await?;

        // Generate ID if not provided
        let block_id = id.unwrap_or_else(|| format!("local://{}", uuid::Uuid::new_v4()));

        // Verify parent exists (unless it's root)
        if parent_id != ROOT_PARENT_ID {
            let engine = &*self.engine;
            let sql = format!("SELECT id FROM blocks WHERE id = '{}'", parent_id);
            let results = engine
                .execute_query(sql, HashMap::new())
                .await
                .map_err(|e| ApiError::InternalError {
                    message: format!("Failed to check parent: {}", e),
                })?;
            if results.is_empty() {
                return Err(ApiError::BlockNotFound { id: parent_id });
            }
        }

        // Generate sort_key using fractional indexing
        use holon::storage::fractional_index::gen_key_between;
        let sort_key = {
            let children = self.get_children_for_block(&parent_id).await?;
            if children.is_empty() {
                gen_key_between(None, None).map_err(|e| ApiError::InternalError {
                    message: format!("Failed to generate sort_key: {}", e),
                })?
            } else {
                // Get the last child's sort_key
                let engine = &*self.engine;
                let sql = format!(
                    "SELECT sort_key FROM blocks WHERE id = '{}'",
                    children[children.len() - 1]
                );
                let results = engine
                    .execute_query(sql, HashMap::new())
                    .await
                    .map_err(|e| ApiError::InternalError {
                        message: format!("Failed to get last child: {}", e),
                    })?;

                let last_sort_key = results
                    .into_iter()
                    .next()
                    .and_then(|e| {
                        e.get("sort_key")
                            .and_then(|v| v.as_string())
                            .map(|s| s.to_string())
                    })
                    .unwrap_or_default();

                gen_key_between(Some(&last_sort_key), None).map_err(|e| {
                    ApiError::InternalError {
                        message: format!("Failed to generate sort_key: {}", e),
                    }
                })?
            }
        };

        // Create block entity
        let mut entity = StorageEntity::new();
        entity.insert("id".to_string(), Value::String(block_id.clone()));

        // Map ROOT_PARENT_ID to NULL for database
        if parent_id == ROOT_PARENT_ID {
            entity.insert("parent_id".to_string(), Value::Null);
        } else {
            entity.insert("parent_id".to_string(), Value::String(parent_id.clone()));
        }

        entity.insert(
            "content".to_string(),
            Value::String(content.to_plain_text().to_string()),
        );
        entity.insert("sort_key".to_string(), Value::String(sort_key));
        entity.insert("depth".to_string(), Value::Integer(0)); // Will be calculated if needed
        entity.insert("collapsed".to_string(), Value::Integer(0));
        entity.insert("completed".to_string(), Value::Integer(0));
        entity.insert("block_type".to_string(), Value::String("text".to_string()));

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        entity.insert("created_at".to_string(), Value::Integer(now));
        entity.insert("updated_at".to_string(), Value::Integer(now));

        // Insert into database using backend
        let engine = &*self.engine;
        let entity_clone = entity.clone();
        engine
            .with_backend_write(|backend: &mut _| -> anyhow::Result<()> {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(backend.insert("blocks", entity_clone))
                })
                .map_err(|e| anyhow::anyhow!("{}", e))
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to insert block: {}", e),
            })?;

        // Return the created block
        Ok(Block {
            id: block_id,
            parent_id,
            content,
            properties: std::collections::HashMap::new(),
            children: vec![],
            metadata: BlockMetadata {
                created_at: now,
                updated_at: now,
            },
        })
    }

    async fn update_block(
        &self,
        id: &str,
        content: holon_api::BlockContent,
    ) -> Result<(), ApiError> {
        self.ensure_schema().await?;

        let engine = &*self.engine;

        // Get existing block
        let sql = format!("SELECT * FROM blocks WHERE id = '{}'", id);
        let results = engine
            .execute_query(sql, HashMap::new())
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to get block: {}", e),
            })?;

        let mut entity = results
            .into_iter()
            .next()
            .ok_or_else(|| ApiError::BlockNotFound { id: id.to_string() })?;

        // Update content and timestamp
        entity.insert(
            "content".to_string(),
            Value::String(content.to_plain_text().to_string()),
        );
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        entity.insert("updated_at".to_string(), Value::Integer(now));

        engine
            .with_backend_write(|backend: &mut _| -> anyhow::Result<()> {
                let entity_clone = entity.clone();
                let id_clone = id.to_string();
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(backend.update(
                        "blocks",
                        &id_clone,
                        entity_clone,
                    ))
                })
                .map_err(|e| anyhow::anyhow!("{}", e))
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to update block: {}", e),
            })?;

        Ok(())
    }

    async fn delete_block(&self, id: &str) -> Result<(), ApiError> {
        self.ensure_schema().await?;

        // Don't allow deleting root
        if id == ROOT_PARENT_ID {
            return Err(ApiError::InvalidOperation {
                message: "Cannot delete root block".to_string(),
            });
        }

        let engine = &*self.engine;

        // Check if block exists
        let sql = format!("SELECT id FROM blocks WHERE id = '{}'", id);
        let results = engine
            .execute_query(sql, HashMap::new())
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to check block: {}", e),
            })?;

        if results.is_empty() {
            return Err(ApiError::BlockNotFound { id: id.to_string() });
        }

        // Delete the block
        let id_clone = id.to_string();
        engine
            .with_backend_write(|backend: &mut _| -> anyhow::Result<()> {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(backend.delete("blocks", &id_clone))
                })
                .map_err(|e| anyhow::anyhow!("{}", e))
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to delete block: {}", e),
            })?;

        Ok(())
    }

    async fn move_block(
        &self,
        id: &str,
        new_parent: String,
        _after: Option<String>,
    ) -> Result<(), ApiError> {
        self.ensure_schema().await?;

        // Don't allow moving root
        if id == ROOT_PARENT_ID {
            return Err(ApiError::InvalidOperation {
                message: "Cannot move root block".to_string(),
            });
        }

        let engine = &*self.engine;

        // Get existing block
        let sql = format!("SELECT * FROM blocks WHERE id = '{}'", id);
        let results = engine
            .execute_query(sql, HashMap::new())
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to get block: {}", e),
            })?;

        let mut entity = results
            .into_iter()
            .next()
            .ok_or_else(|| ApiError::BlockNotFound { id: id.to_string() })?;

        // Verify new parent exists (unless it's root)
        if new_parent != ROOT_PARENT_ID {
            let check_sql = format!("SELECT id FROM blocks WHERE id = '{}'", new_parent);
            let check_results = engine
                .execute_query(check_sql, HashMap::new())
                .await
                .map_err(|e| ApiError::InternalError {
                    message: format!("Failed to check new parent: {}", e),
                })?;
            if check_results.is_empty() {
                return Err(ApiError::BlockNotFound { id: new_parent });
            }
        }

        // Generate new sort_key
        use holon::storage::fractional_index::gen_key_between;
        let sort_key = {
            let children = self.get_children_for_block(&new_parent).await?;
            if children.is_empty() {
                gen_key_between(None, None).map_err(|e| ApiError::InternalError {
                    message: format!("Failed to generate sort_key: {}", e),
                })?
            } else {
                let sql = format!(
                    "SELECT sort_key FROM blocks WHERE id = '{}'",
                    children[children.len() - 1]
                );
                let results = engine
                    .execute_query(sql, HashMap::new())
                    .await
                    .map_err(|e| ApiError::InternalError {
                        message: format!("Failed to get last child: {}", e),
                    })?;

                let last_sort_key = results
                    .into_iter()
                    .next()
                    .and_then(|e| {
                        e.get("sort_key")
                            .and_then(|v| v.as_string())
                            .map(|s| s.to_string())
                    })
                    .unwrap_or_default();

                gen_key_between(Some(&last_sort_key), None).map_err(|e| {
                    ApiError::InternalError {
                        message: format!("Failed to generate sort_key: {}", e),
                    }
                })?
            }
        };

        // Update parent_id and sort_key
        if new_parent == ROOT_PARENT_ID {
            entity.insert("parent_id".to_string(), Value::Null);
        } else {
            entity.insert("parent_id".to_string(), Value::String(new_parent));
        }
        entity.insert("sort_key".to_string(), Value::String(sort_key));

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        entity.insert("updated_at".to_string(), Value::Integer(now));

        engine
            .with_backend_write(|backend: &mut _| -> anyhow::Result<()> {
                let entity_clone = entity.clone();
                let id_clone = id.to_string();
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(backend.update(
                        "blocks",
                        &id_clone,
                        entity_clone,
                    ))
                })
                .map_err(|e| anyhow::anyhow!("{}", e))
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to move block: {}", e),
            })?;

        Ok(())
    }

    async fn get_blocks(&self, ids: Vec<String>) -> Result<Vec<Block>, ApiError> {
        self.ensure_schema().await?;

        let mut blocks = Vec::new();
        for id in ids {
            match self.get_block(&id).await {
                Ok(block) => blocks.push(block),
                Err(ApiError::BlockNotFound { .. }) => {
                    // Skip missing blocks
                }
                Err(e) => return Err(e),
            }
        }
        Ok(blocks)
    }

    async fn create_blocks(&self, blocks: Vec<NewBlock>) -> Result<Vec<Block>, ApiError> {
        self.ensure_schema().await?;

        let mut created = Vec::new();
        for new_block in blocks {
            let block = self
                .create_block(new_block.parent_id, new_block.content, new_block.id)
                .await?;
            created.push(block);
        }
        Ok(created)
    }

    async fn delete_blocks(&self, ids: Vec<String>) -> Result<(), ApiError> {
        self.ensure_schema().await?;

        for id in ids {
            // Don't fail on individual deletes, just skip root
            if id != ROOT_PARENT_ID {
                let _ = self.delete_block(&id).await;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Lifecycle for TuiR3blPbtBackend {
    async fn create_new(_doc_id: String) -> Result<Self, ApiError> {
        // TuiR3blPbtBackend must be created with an existing BackendEngine
        Err(ApiError::InternalError {
            message: "TuiR3blPbtBackend must be created with BackendEngine via new()".to_string(),
        })
    }

    async fn open_existing(_doc_id: String) -> Result<Self, ApiError> {
        // TuiR3blPbtBackend must be created with an existing BackendEngine
        Err(ApiError::InternalError {
            message: "TuiR3blPbtBackend must be created with BackendEngine via new()".to_string(),
        })
    }

    async fn dispose(&self) -> Result<(), ApiError> {
        // Nothing to clean up - BackendEngine is managed externally
        Ok(())
    }
}
