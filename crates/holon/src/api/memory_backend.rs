//! In-memory implementation of DocumentRepository
//!
//! This module provides a simple HashMap-based implementation for testing
//! and as a reference implementation. It implements only `CoreOperations`
//! and `Lifecycle` traits (no networking, no change notifications).

use holon_api::streaming::ChangeSubscribers;

use super::repository::{CoreOperations, Lifecycle};
use super::types::NewBlock;
use async_trait::async_trait;
use holon_api::streaming::ChangeNotifications;
use holon_api::{
    ApiError, Block, BlockChange, BlockContent, BlockMetadata, Change, ChangeOrigin, StreamPosition,
};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};

use holon_api::block::{NO_PARENT_ID, ROOT_PARENT_ID};
/// In-memory block storage using HashMaps.
///
/// This is a lightweight, non-persistent backend useful for:
/// - Unit testing without CRDT overhead
/// - Mocking in frontend development
/// - Reference implementation for documentation
/// - Property-based testing baseline
///
/// # Example
///
/// ```rust,no_run
/// use holon::api::{MemoryBackend, CoreOperations, Lifecycle};
///
/// async fn example() -> anyhow::Result<()> {
///     let backend = MemoryBackend::create_new("test-doc".to_string()).await?;
///
///     let block = backend.create_block(None, "Hello".to_string(), None).await?;
///     let retrieved = backend.get_block(&block.id).await?;
///
///     assert_eq!(retrieved.content, "Hello");
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct MemoryBackend {
    /// Document ID
    doc_id: String,
    /// Internal state
    state: Arc<RwLock<MemoryState>>,
}

impl Clone for MemoryBackend {
    fn clone(&self) -> Self {
        let state = self.state.read().unwrap();
        let cloned_state = MemoryState {
            blocks: state.blocks.clone(),
            children_by_parent: state.children_by_parent.clone(),
            next_id_counter: state.next_id_counter,
            version_counter: state.version_counter,
            subscribers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            event_log: state.event_log.clone(),
        };

        Self {
            doc_id: self.doc_id.clone(),
            state: Arc::new(RwLock::new(cloned_state)),
        }
    }
}

/// flutter_rust_bridge:ignore
#[derive(Debug)]
struct MemoryState {
    /// All blocks by ID
    blocks: HashMap<String, MemoryBlock>,
    /// Children by parent ID
    children_by_parent: HashMap<String, Vec<String>>,
    /// Counter for deterministic ID generation (increments with each create)
    next_id_counter: u64,
    /// Version counter (increments with each mutation)
    version_counter: u64,
    /// Active change notification subscribers
    subscribers: ChangeSubscribers<Block>,
    /// Event log for replaying past events to new watchers
    /// Maps version -> events that created that version
    event_log: Vec<BlockChange>,
}

impl Default for MemoryState {
    fn default() -> Self {
        Self {
            blocks: HashMap::new(),
            children_by_parent: HashMap::new(),
            next_id_counter: 0,
            version_counter: 0,
            subscribers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            event_log: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct MemoryBlock {
    id: String,
    parent_id: String,
    content: BlockContent,
    properties: HashMap<String, holon_api::Value>,
    created_at: i64,
    updated_at: i64,
    deleted_at: Option<i64>,
}

impl MemoryBackend {
    /// Generate a local URI-based block ID.
    /// Generate a deterministic block ID using a counter.
    /// This ensures the same sequence of operations always generates the same IDs,
    /// which is crucial for property-based testing with proptest where states are cloned.
    fn generate_block_id(state: &mut MemoryState) -> String {
        let id = format!("local://{}", state.next_id_counter);
        state.next_id_counter += 1;
        id
    }

    fn increment_version(state: &mut MemoryState) {
        state.version_counter += 1;
    }

    /// Get current Unix timestamp in milliseconds.
    fn now_millis() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// Notify all active subscribers of a change event and add to event log.
    /// Removes closed channels automatically.
    /// Sends the change as a single-item batch.
    /// Note: This spawns a task to avoid blocking on async lock.
    fn notify_subscribers(state: &mut MemoryState, change: Change<Block>) {
        state.event_log.push(change.clone());

        let batch = vec![change];
        let subscribers = state.subscribers.clone();
        tokio::spawn(async move {
            let mut subscribers = subscribers.lock().await;
            subscribers.retain(|sender| sender.try_send(Ok(batch.clone())).is_ok());
        });
    }

    /// Count of non-deleted blocks.
    pub fn non_deleted_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state
            .blocks
            .values()
            .filter(|b| b.deleted_at.is_none())
            .count()
    }

    /// Whether any non-deleted blocks exist.
    pub fn has_blocks(&self) -> bool {
        self.non_deleted_count() > 0
    }
}

#[async_trait]
impl Lifecycle for MemoryBackend {
    async fn create_new(doc_id: String) -> Result<Self, ApiError> {
        let backend = Self {
            doc_id,
            state: Arc::new(RwLock::new(MemoryState::default())),
        };

        // Create the root block
        let now = Self::now_millis();
        let mut state = backend.state.write().unwrap();

        let root_block = MemoryBlock {
            id: ROOT_PARENT_ID.to_string(),
            parent_id: NO_PARENT_ID.to_string(),
            content: BlockContent::text(""),
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
            deleted_at: None,
        };

        state.blocks.insert(ROOT_PARENT_ID.to_string(), root_block);

        // Create a default first child block so the UI has something to display
        let first_block_id = Self::generate_block_id(&mut state);
        let first_block = MemoryBlock {
            id: first_block_id.clone(),
            parent_id: ROOT_PARENT_ID.to_string(),
            content: BlockContent::text(""),
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
            deleted_at: None,
        };
        state.blocks.insert(first_block_id.clone(), first_block);

        // Add the first block as a child of root
        state
            .children_by_parent
            .entry(ROOT_PARENT_ID.to_string())
            .or_insert_with(Vec::new)
            .push(first_block_id);

        drop(state);

        Ok(backend)
    }

    async fn open_existing(_doc_id: String) -> Result<Self, ApiError> {
        Err(ApiError::InvalidOperation {
            message: "MemoryBackend does not support persistence".to_string(),
        })
    }

    async fn dispose(&self) -> Result<(), ApiError> {
        // No resources to clean up
        Ok(())
    }
}

#[async_trait]
impl CoreOperations for MemoryBackend {
    async fn get_block(&self, id: &str) -> Result<Block, ApiError> {
        let state = self.state.read().unwrap();

        let block = state
            .blocks
            .get(id)
            .ok_or_else(|| ApiError::BlockNotFound { id: id.to_string() })?;

        // Treat deleted blocks as not found
        if block.deleted_at.is_some() {
            return Err(ApiError::BlockNotFound { id: id.to_string() });
        }

        // Get children
        let children = state
            .children_by_parent
            .get(id)
            .cloned()
            .unwrap_or_default();

        Ok(Block {
            id: block.id.clone(),
            parent_id: block.parent_id.clone(),
            content: block.content.clone(),
            properties: block.properties.clone(),
            children,
            metadata: BlockMetadata {
                created_at: block.created_at,
                updated_at: block.updated_at,
            },
        })
    }

    async fn get_all_blocks(
        &self,
        traversal: super::types::Traversal,
    ) -> Result<Vec<Block>, ApiError> {
        let state = self.state.read().unwrap();
        let mut result = Vec::new();

        // Helper function for depth-first traversal with level tracking
        fn traverse(
            block_id: &str,
            current_level: usize,
            state: &MemoryState,
            traversal: &super::types::Traversal,
            result: &mut Vec<Block>,
        ) {
            let mem_block = match state.blocks.get(block_id) {
                Some(b) if b.deleted_at.is_none() => b,
                _ => return, // Skip deleted or non-existent blocks
            };

            let children = state
                .children_by_parent
                .get(block_id)
                .cloned()
                .unwrap_or_default();

            // Add current block if it's within the level range
            if traversal.includes_level(current_level) {
                result.push(Block {
                    id: mem_block.id.clone(),
                    parent_id: mem_block.parent_id.clone(),
                    content: mem_block.content.clone(),
                    properties: mem_block.properties.clone(),
                    children: children.clone(),
                    metadata: BlockMetadata {
                        created_at: mem_block.created_at,
                        updated_at: mem_block.updated_at,
                    },
                });
            }

            // Recursively traverse children only if we haven't reached max_level
            if current_level < traversal.max_level {
                for child_id in &children {
                    traverse(child_id, current_level + 1, state, traversal, result);
                }
            }
        }

        // Start traversal from the root block itself (level 0)
        traverse(ROOT_PARENT_ID, 0, &state, &traversal, &mut result);

        Ok(result)
    }

    async fn list_children(&self, parent_id: &str) -> Result<Vec<String>, ApiError> {
        let state = self.state.read().unwrap();

        // Verify parent exists
        if !state.blocks.contains_key(parent_id) {
            return Err(ApiError::BlockNotFound {
                id: parent_id.to_string(),
            });
        }

        Ok(state
            .children_by_parent
            .get(parent_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn create_block(
        &self,
        parent_id: String,
        content: BlockContent,
        id: Option<String>,
    ) -> Result<Block, ApiError> {
        let now = Self::now_millis();

        let mut state = self.state.write().unwrap();
        let block_id = id.unwrap_or_else(|| Self::generate_block_id(&mut state));

        // Validate parent exists
        if !state.blocks.contains_key(&parent_id) {
            return Err(ApiError::BlockNotFound { id: parent_id });
        }

        // Create block
        let block = MemoryBlock {
            id: block_id.clone(),
            parent_id: parent_id.clone(),
            content: content.clone(),
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
            deleted_at: None,
        };

        state.blocks.insert(block_id.clone(), block);

        // Add to appropriate list
        if parent_id == ROOT_PARENT_ID {
            state
                .children_by_parent
                .entry(ROOT_PARENT_ID.to_string())
                .or_default()
                .push(block_id.clone());
        } else {
            state
                .children_by_parent
                .entry(parent_id.clone())
                .or_default()
                .push(block_id.clone());
        }

        let result_block = Block {
            id: block_id,
            parent_id,
            content,
            properties: HashMap::new(),
            children: vec![],
            metadata: BlockMetadata {
                created_at: now,
                updated_at: now,
            },
        };

        Self::increment_version(&mut state);

        Self::notify_subscribers(
            &mut state,
            Change::Created {
                data: result_block.clone(),
                origin: ChangeOrigin::Local {
                    operation_id: None,
                    trace_id: None,
                },
            },
        );

        Ok(result_block)
    }

    async fn update_block(&self, id: &str, content: BlockContent) -> Result<(), ApiError> {
        let mut state = self.state.write().unwrap();

        let block = state
            .blocks
            .get_mut(id)
            .ok_or_else(|| ApiError::BlockNotFound { id: id.to_string() })?;

        // Clone values before modifying state
        let parent_id = block.parent_id.clone();
        let properties = block.properties.clone();
        let created_at = block.created_at;
        let updated_at = Self::now_millis();

        block.content = content.clone();
        block.updated_at = updated_at;

        // Get children from children_by_parent
        let children = state
            .children_by_parent
            .get(id)
            .cloned()
            .unwrap_or_default();

        Self::increment_version(&mut state);

        Self::notify_subscribers(
            &mut state,
            Change::Updated {
                id: id.to_string(),
                data: Block {
                    id: id.to_string(),
                    parent_id,
                    content: content.clone(),
                    properties,
                    children,
                    metadata: BlockMetadata {
                        created_at,
                        updated_at,
                    },
                },
                origin: ChangeOrigin::Local {
                    operation_id: None,
                    trace_id: None,
                },
            },
        );

        Ok(())
    }

    async fn delete_block(&self, id: &str) -> Result<(), ApiError> {
        let mut state = self.state.write().unwrap();

        let block = state
            .blocks
            .get_mut(id)
            .ok_or_else(|| ApiError::BlockNotFound { id: id.to_string() })?;

        // Set tombstone
        let now = Self::now_millis();
        block.deleted_at = Some(now);
        block.updated_at = now;

        // Clone parent_id before modifying other parts of state
        let parent_id = block.parent_id.clone();

        // Remove from ordering lists (but keep in blocks map for consistency)
        if parent_id == ROOT_PARENT_ID {
            if let Some(children) = state.children_by_parent.get_mut(ROOT_PARENT_ID) {
                children.retain(|root_id| root_id != id);
            };
        } else if let Some(children) = state.children_by_parent.get_mut(&parent_id) {
            children.retain(|child_id| child_id != id);
        }

        Self::increment_version(&mut state);

        // Notify subscribers
        Self::notify_subscribers(
            &mut state,
            Change::Deleted {
                id: id.to_string(),
                origin: ChangeOrigin::Local {
                    operation_id: None,
                    trace_id: None,
                },
            },
        );

        Ok(())
    }

    async fn move_block(
        &self,
        id: &str,
        new_parent: String,
        after: Option<String>,
    ) -> Result<(), ApiError> {
        // Cycle detection using get_ancestor_chain
        let ancestors = self.get_ancestor_chain(&new_parent).await?;

        if ancestors.contains(&id.to_string()) {
            return Err(ApiError::CyclicMove {
                id: id.to_string(),
                target_parent: new_parent.clone(),
            });
        }

        let mut state = self.state.write().unwrap();

        // Get block and verify it exists
        let block = state
            .blocks
            .get(id)
            .ok_or_else(|| ApiError::BlockNotFound { id: id.to_string() })?;

        let old_parent = block.parent_id.clone();

        // Verify new parent exists
        if !state.blocks.contains_key(&new_parent) {
            return Err(ApiError::BlockNotFound {
                id: new_parent.clone(),
            });
        }

        // Remove from old location
        if old_parent == ROOT_PARENT_ID {
            if let Some(children) = state.children_by_parent.get_mut(ROOT_PARENT_ID) {
                children.retain(|root_id| root_id != id);
            };
        } else if let Some(children) = state.children_by_parent.get_mut(&old_parent) {
            children.retain(|child_id| child_id != id);
        }

        // Add to new location
        if new_parent == ROOT_PARENT_ID {
            // Add to root
            if let Some(ref after_id) = after {
                if let Some(pos) = state
                    .children_by_parent
                    .get(ROOT_PARENT_ID)
                    .unwrap_or(&Vec::new())
                    .iter()
                    .position(|id| id == after_id)
                {
                    if let Some(children) = state.children_by_parent.get_mut(ROOT_PARENT_ID) {
                        children.insert(pos + 1, id.to_string());
                    }
                } else {
                    state
                        .children_by_parent
                        .entry(ROOT_PARENT_ID.to_string())
                        .or_default()
                        .push(id.to_string());
                }
            } else {
                state
                    .children_by_parent
                    .entry(ROOT_PARENT_ID.to_string())
                    .or_default()
                    .push(id.to_string());
            }
        } else {
            let children = state
                .children_by_parent
                .entry(new_parent.clone())
                .or_default();

            // Insert after specified sibling, or at end
            if let Some(ref after_id) = after {
                if let Some(pos) = children.iter().position(|id| id == after_id) {
                    children.insert(pos + 1, id.to_string());
                } else {
                    children.push(id.to_string());
                }
            } else {
                children.push(id.to_string());
            }
        }

        // Update block's parent_id
        let block = state.blocks.get_mut(id).unwrap();
        let content = block.content.clone();
        let properties = block.properties.clone();
        let created_at = block.created_at;
        let updated_at = Self::now_millis();
        block.parent_id = new_parent.clone();
        block.updated_at = updated_at;

        // Get children from children_by_parent
        let children = state
            .children_by_parent
            .get(id)
            .cloned()
            .unwrap_or_default();

        Self::increment_version(&mut state);

        // Notify subscribers
        Self::notify_subscribers(
            &mut state,
            Change::Updated {
                id: id.to_string(),
                data: Block {
                    id: id.to_string(),
                    parent_id: new_parent.clone(),
                    content,
                    properties,
                    children,
                    metadata: BlockMetadata {
                        created_at,
                        updated_at,
                    },
                },
                origin: ChangeOrigin::Local {
                    operation_id: None,
                    trace_id: None,
                },
            },
        );

        Ok(())
    }

    async fn get_blocks(&self, ids: Vec<String>) -> Result<Vec<Block>, ApiError> {
        let state = self.state.read().unwrap();
        let mut blocks = Vec::new();

        for id in ids {
            if let Some(block) = state.blocks.get(&id) {
                // Skip deleted blocks
                if block.deleted_at.is_some() {
                    continue;
                }

                let children = state
                    .children_by_parent
                    .get(&id)
                    .cloned()
                    .unwrap_or_default();

                blocks.push(Block {
                    id: block.id.clone(),
                    parent_id: block.parent_id.clone(),
                    content: block.content.clone(),
                    properties: block.properties.clone(),
                    children,
                    metadata: BlockMetadata {
                        created_at: block.created_at,
                        updated_at: block.updated_at,
                    },
                });
            }
        }

        Ok(blocks)
    }

    async fn create_blocks(&self, blocks: Vec<NewBlock>) -> Result<Vec<Block>, ApiError> {
        let now = Self::now_millis();
        let mut state = self.state.write().unwrap();
        let mut created = Vec::new();

        for new_block in blocks {
            let block_id = new_block
                .id
                .unwrap_or_else(|| Self::generate_block_id(&mut state));

            // Validate parent exists
            if !state.blocks.contains_key(&new_block.parent_id) {
                return Err(ApiError::BlockNotFound {
                    id: new_block.parent_id.clone(),
                });
            }

            // Create block
            let block = MemoryBlock {
                id: block_id.clone(),
                parent_id: new_block.parent_id.clone(),
                content: new_block.content.clone(),
                properties: HashMap::new(),
                created_at: now,
                updated_at: now,
                deleted_at: None,
            };

            state.blocks.insert(block_id.clone(), block);

            // Add to parent's children list
            let children = state
                .children_by_parent
                .entry(new_block.parent_id.clone())
                .or_default();

            if let Some(after_id) = new_block.after {
                if let Some(pos) = children.iter().position(|id| id == &after_id) {
                    children.insert(pos + 1, block_id.clone());
                } else {
                    children.push(block_id.clone());
                }
            } else {
                children.push(block_id.clone());
            }

            let result_block = Block {
                id: block_id,
                parent_id: new_block.parent_id,
                content: new_block.content,
                properties: HashMap::new(),
                children: vec![],
                metadata: BlockMetadata {
                    created_at: now,
                    updated_at: now,
                },
            };

            // Notify subscribers
            Self::notify_subscribers(
                &mut state,
                Change::Created {
                    data: result_block.clone(),
                    origin: ChangeOrigin::Local {
                        operation_id: None,
                        trace_id: None,
                    },
                },
            );

            created.push(result_block);
        }

        Self::increment_version(&mut state);

        Ok(created)
    }

    async fn delete_blocks(&self, ids: Vec<String>) -> Result<(), ApiError> {
        let now = Self::now_millis();
        let mut state = self.state.write().unwrap();

        // Deduplicate IDs to handle cases where the same ID appears multiple times
        let mut seen = std::collections::HashSet::new();

        for id in ids {
            // Skip if we've already processed this ID
            if !seen.insert(id.clone()) {
                continue;
            }

            let block = state
                .blocks
                .get_mut(&id)
                .ok_or_else(|| ApiError::BlockNotFound { id: id.clone() })?;

            // Set tombstone
            block.deleted_at = Some(now);
            block.updated_at = now;

            // Remove from ordering lists
            let parent_id = block.parent_id.clone();
            if let Some(children) = state.children_by_parent.get_mut(&parent_id) {
                children.retain(|child_id| child_id != &id);
            }

            // Notify subscribers
            Self::notify_subscribers(
                &mut state,
                Change::Deleted {
                    id: id.clone(),
                    origin: ChangeOrigin::Local {
                        operation_id: None,
                        trace_id: None,
                    },
                },
            );
        }

        Self::increment_version(&mut state);

        Ok(())
    }
}

// ChangeNotifications trait implementation
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ChangeNotifications<Block> for MemoryBackend {
    async fn watch_changes_since(
        &self,
        position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<Change<Block>>, ApiError>> + Send>> {
        // Collect replay events/blocks synchronously
        let replay_items = match position {
            StreamPosition::Beginning => {
                // Collect blocks while holding the lock
                let state = self.state.read().unwrap();
                state
                    .blocks
                    .iter()
                    .filter_map(|(id, mem_block)| {
                        if mem_block.deleted_at.is_none() {
                            let children = state
                                .children_by_parent
                                .get(id)
                                .cloned()
                                .unwrap_or_default();

                            Some(Change::Created {
                                data: Block {
                                    id: mem_block.id.clone(),
                                    parent_id: mem_block.parent_id.clone(),
                                    content: mem_block.content.clone(),
                                    properties: mem_block.properties.clone(),
                                    children,
                                    metadata: BlockMetadata {
                                        created_at: mem_block.created_at,
                                        updated_at: mem_block.updated_at,
                                    },
                                },
                                origin: ChangeOrigin::Remote {
                                    operation_id: None,
                                    trace_id: None,
                                },
                            })
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            }
            StreamPosition::Version(version) => {
                // Collect events while holding the lock
                let state = self.state.read().unwrap();
                let start_version =
                    u64::from_le_bytes(version.as_slice().try_into().unwrap_or([0; 8]));

                state
                    .event_log
                    .iter()
                    .skip(start_version as usize)
                    .cloned()
                    .collect::<Vec<_>>()
            }
        };

        // Create channel for live updates
        let (tx, rx) = mpsc::channel::<std::result::Result<Vec<Change<Block>>, ApiError>>(100);

        // Subscribe to future changes
        let subscribers = {
            let state = self.state.read().unwrap();
            state.subscribers.clone()
        }; // Drop read lock before async operation
        let mut subscribers = subscribers.lock().await;
        subscribers.push(tx);

        // Create a stream that first yields replay items as a batch, then live updates
        // This avoids spawning tasks which can cause runtime deadlocks
        let replay_batch = if replay_items.is_empty() {
            vec![]
        } else {
            vec![replay_items]
        };
        let replay_stream = tokio_stream::iter(replay_batch.into_iter().map(Ok));
        let live_stream = ReceiverStream::new(rx);
        let combined = replay_stream.chain(live_stream);

        Box::pin(combined)
    }

    async fn get_current_version(&self) -> std::result::Result<Vec<u8>, ApiError> {
        let state = self.state.read().unwrap();
        Ok(state.version_counter.to_le_bytes().to_vec())
    }
}
