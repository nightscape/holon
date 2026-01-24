//! Core datasource traits
//!
//! This module provides traits for datasource operations.
//! These traits are designed to work with external datasources that provide
//! both read and write capabilities.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt;

use crate::fractional_index::{gen_key_between, gen_n_keys, MAX_SORT_KEY_LENGTH};
use holon_api::{Operation, OperationDescriptor, Value};

// Define Result type using Send + Sync for error
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Represents the undo capability of an operation.
///
/// Operations return this type to indicate whether they can be undone
/// and if so, what operation would undo them.
#[derive(Debug, Clone)]
pub enum UndoAction {
    /// The operation can be undone by executing the contained inverse operation.
    Undo(Operation),
    /// The operation cannot be undone (e.g., complex operations like split_block).
    Irreversible,
}

impl UndoAction {
    /// Convert to Option<Operation> for backward compatibility
    pub fn into_option(self) -> Option<Operation> {
        match self {
            UndoAction::Undo(op) => Some(op),
            UndoAction::Irreversible => None,
        }
    }

    /// Check if this action is reversible
    pub fn is_reversible(&self) -> bool {
        matches!(self, UndoAction::Undo(_))
    }
}

impl From<Operation> for UndoAction {
    fn from(op: Operation) -> Self {
        UndoAction::Undo(op)
    }
}

impl From<Option<Operation>> for UndoAction {
    fn from(opt: Option<Operation>) -> Self {
        match opt {
            Some(op) => UndoAction::Undo(op),
            None => UndoAction::Irreversible,
        }
    }
}

// Type alias for operation result to help macro parsing
pub type OperationResult = UndoAction;
pub type CreateResult = (String, UndoAction);

/// Error raised when a trait's dispatch helper does not recognize an operation name.
#[derive(Debug)]
pub struct UnknownOperationError {
    trait_name: String,
    operation: String,
}

impl UnknownOperationError {
    pub fn new(trait_name: &str, operation: &str) -> Self {
        Self {
            trait_name: trait_name.to_string(),
            operation: operation.to_string(),
        }
    }

    /// Helper for callers that need to keep matching logic in one place.
    pub fn is_unknown(err: &(dyn std::error::Error + 'static)) -> bool {
        err.downcast_ref::<UnknownOperationError>().is_some()
    }
}

impl fmt::Display for UnknownOperationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Unknown operation: {} for trait {}",
            self.operation, self.trait_name
        )
    }
}

impl std::error::Error for UnknownOperationError {}

// Define MaybeSendSync trait alias for WASM compatibility
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSendSync: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync + ?Sized> MaybeSendSync for T {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSendSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSendSync for T {}

/// Entities that support hierarchical tree structure
pub trait BlockEntity: MaybeSendSync {
    /// Get the entity's unique identifier
    fn id(&self) -> &str;

    fn parent_id(&self) -> Option<&str>;
    fn sort_key(&self) -> &str;
    fn depth(&self) -> i64;

    /// Get the block content (text content of the block)
    fn content(&self) -> &str;
}

/// Entities that support task management (completion, priority, etc.)
pub trait TaskEntity: MaybeSendSync {
    fn completed(&self) -> bool;
    fn priority(&self) -> Option<i64>;
    fn due_date(&self) -> Option<DateTime<Utc>>;
}

/// CRUD operations provider (fire-and-forget to external system)
///
/// Provides create, update, and delete operations. Changes are confirmed
/// via ChangeNotifications streams, not return values.
///
/// **Note**: This trait is conceptually `CrudOperations` but is named
/// `CrudOperations` for backward compatibility with macro-generated code.
/// New code should refer to it as `CrudOperations` in documentation.
#[holon_macros::operations_trait]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait CrudOperations<T>: MaybeSendSync
where
    T: MaybeSendSync + 'static,
{
    /// Set single field (returns inverse operation for undo)
    /// Note: affected_fields is determined dynamically based on the field parameter
    async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<UndoAction>;

    /// Create new entity (returns new ID and inverse operation for undo)
    async fn create(&self, fields: HashMap<String, Value>) -> Result<(String, UndoAction)>;

    /// Delete entity (returns inverse operation for undo)
    async fn delete(&self, id: &str) -> Result<UndoAction>;

    /// Get operations metadata (automatically delegates to entity type)
    fn operations(&self) -> Vec<OperationDescriptor>
    where
        T: OperationRegistry,
    {
        T::all_operations()
    }
}

/// Trait for aggregating operation metadata from multiple trait sources
///
/// Entity types implement this trait to declare which operations they support.
/// The implementation aggregates operations from all applicable traits:
/// - `CrudOperations` operations (set_field, create, delete)
/// - `BlockOperations` operations (if entity implements `BlockEntity`)
/// - `TaskOperations` operations (if entity implements `TaskEntity`)
pub trait OperationRegistry: MaybeSendSync {
    /// Returns all operations supported by this entity type
    fn all_operations() -> Vec<OperationDescriptor>;

    /// Returns the entity name for this registry (e.g., "todoist_tasks", "logseq_blocks")
    fn entity_name() -> &'static str;

    /// Returns the short name for this entity type (e.g., "task", "project")
    /// Used for generating entity-typed parameters like "task_id", "project_id"
    /// Returns None if not specified in the entity attribute
    fn short_name() -> Option<&'static str> {
        None
    }
}

/// Read-only data access (from cache)
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait DataSource<T>: MaybeSendSync
where
    T: MaybeSendSync + 'static,
{
    async fn get_all(&self) -> Result<Vec<T>>;
    async fn get_by_id(&self, id: &str) -> Result<Option<T>>;

    // Helper queries (default implementations)
    async fn get_children(&self, parent_id: &str) -> Result<Vec<T>>
    where
        T: BlockEntity,
    {
        let all_items: Vec<T> = self.get_all().await?;
        Ok(all_items
            .into_iter()
            .filter(|t: &T| t.parent_id() == Some(parent_id))
            .collect())
    }
}

/// Helper methods for block data source operations
/// These are not operations themselves, but utilities used by operations
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait BlockDataSourceHelpers<T>: CrudOperations<T> + DataSource<T>
where
    T: BlockEntity + MaybeSendSync + 'static,
{
    /// Get all siblings of a block, sorted by sort_key
    async fn get_siblings(&self, block_id: &str) -> Result<Vec<T>> {
        let block: T = self
            .get_by_id(block_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block not found"))?;
        let parent_id = block.parent_id();

        let siblings: Vec<T> = if let Some(pid) = parent_id {
            self.get_children(pid).await?
        } else {
            // For root blocks, get all root blocks
            // We need to filter from all blocks - this is a limitation
            // For now, return empty - will need get_root_blocks method
            return Ok(vec![]);
        };

        Ok(siblings
            .into_iter()
            .filter(|s: &T| s.id() != block_id)
            .collect())
    }

    /// Get the previous sibling (sibling with sort_key < current sort_key)
    async fn get_prev_sibling(&self, block_id: &str) -> Result<Option<T>> {
        let block: T = self
            .get_by_id(block_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block not found"))?;
        let parent_id = block.parent_id();

        let siblings: Vec<T> = if let Some(pid) = parent_id {
            self.get_children(pid).await?
        } else {
            return Ok(None);
        };

        let prev = siblings
            .into_iter()
            .filter(|s: &T| s.sort_key() < block.sort_key())
            .max_by(|a, b| a.sort_key().cmp(b.sort_key()));
        Ok(prev)
    }

    /// Get the next sibling (sibling with sort_key > current sort_key)
    async fn get_next_sibling(&self, block_id: &str) -> Result<Option<T>> {
        let block: T = self
            .get_by_id(block_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block not found"))?;
        let parent_id = block.parent_id();

        let siblings: Vec<T> = if let Some(pid) = parent_id {
            self.get_children(pid).await?
        } else {
            return Ok(None);
        };

        let next = siblings
            .into_iter()
            .filter(|s: &T| s.sort_key() > block.sort_key())
            .min_by(|a: &T, b: &T| a.sort_key().cmp(b.sort_key()));
        Ok(next)
    }

    /// Get the first child of a parent (lowest sort_key)
    async fn get_first_child(&self, parent_id: Option<&str>) -> Result<Option<T>> {
        let children: Vec<T> = if let Some(pid) = parent_id {
            self.get_children(pid).await?
        } else {
            // For root blocks, we need to get all blocks with no parent
            // This is a limitation - we'd need a method to get root blocks
            // For now, return None and handle in move_block
            return Ok(None);
        };

        Ok(children
            .into_iter()
            .min_by(|a, b| a.sort_key().cmp(b.sort_key())))
    }

    /// Get the last child of a parent (highest sort_key)
    async fn get_last_child(&self, parent_id: Option<&str>) -> Result<Option<T>> {
        let children: Vec<T> = if let Some(pid) = parent_id {
            self.get_children(pid).await?
        } else {
            return Ok(None);
        };

        Ok(children
            .into_iter()
            .max_by(|a: &T, b: &T| a.sort_key().cmp(b.sort_key())))
    }

    /// Recursively update depths of all descendants when a parent's depth changes
    async fn update_descendant_depths(&self, parent_id: &str, depth_delta: i64) -> Result<()> {
        if depth_delta == 0 {
            return Ok(());
        }

        let mut queue = vec![parent_id.to_string()];

        while let Some(current_parent_id) = queue.pop() {
            let children: Vec<T> = self.get_children(&current_parent_id).await?;

            for child in children {
                let current_depth = child.depth();
                let new_depth = current_depth + depth_delta;
                self.set_field(child.id(), "depth", Value::Integer(new_depth))
                    .await?;
                queue.push(child.id().to_string());
            }
        }

        Ok(())
    }

    /// Rebalance all siblings of a parent to create uniform spacing
    async fn rebalance_siblings(&self, parent_id: Option<&str>) -> Result<()> {
        let children: Vec<T> = if let Some(pid) = parent_id {
            self.get_children(pid).await?
        } else {
            // For root blocks, we'd need a get_root_blocks method
            // For now, skip rebalancing root blocks
            return Ok(());
        };

        // Sort by current sort_key
        let mut sorted_children: Vec<_> = children.into_iter().collect();
        sorted_children.sort_by(|a, b| a.sort_key().cmp(b.sort_key()));

        // Generate evenly-spaced keys
        let new_keys = gen_n_keys(sorted_children.len())?;

        // Update all siblings
        for (child, new_key) in sorted_children.iter().zip(new_keys.iter()) {
            self.set_field(child.id(), "sort_key", Value::String(new_key.clone()))
                .await?;
        }

        Ok(())
    }
}

/// Hierarchical structure operations (for any block-like entity)
///
/// This trait provides operations for manipulating block hierarchies.
/// It requires that the entity type implements `BlockEntity` and that
/// the datasource implements `BlockDataSourceHelpers`.
#[holon_macros::operations_trait]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait BlockOperations<T>: BlockDataSourceHelpers<T>
where
    T: BlockEntity + MaybeSendSync + 'static,
{
    /// Move block under a new parent (increase indentation)
    #[holon_macros::affects("parent_id", "depth", "sort_key")]
    async fn indent(&self, id: &str, parent_id: &str) -> Result<UndoAction> {
        // Capture old state before mutation
        let block = self
            .get_by_id(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block not found"))?;
        let old_parent_id = block
            .parent_id()
            .ok_or_else(|| anyhow::anyhow!("Cannot indent root block"))?
            .to_string();
        let old_predecessor = self.get_prev_sibling(id).await?;

        // Query cache for current state (fast - no network)
        let maybe_parent: Option<T> = self.get_by_id(parent_id).await?;
        let parent: T = maybe_parent.ok_or_else(|| anyhow::anyhow!("Parent not found"))?;
        let siblings: Vec<T> = self.get_children(parent_id).await?;

        // Calculate new position via fractional indexing
        let sort_key = gen_key_between(siblings.last().map(|s| s.sort_key()), None)
            .map_err(|e| anyhow::anyhow!(e))?;

        // Execute primitives (delegates to self.set_field)
        self.set_field(id, "parent_id", Value::String(parent_id.to_string()))
            .await?;
        self.set_field(id, "depth", Value::Integer(parent.depth() + 1))
            .await?;
        self.set_field(id, "sort_key", Value::String(sort_key))
            .await?;

        // Return inverse operation using macro-generated helper
        use crate::__operations_block_operations;

        // Entity name will be set by OperationProvider when operation is executed
        Ok(UndoAction::Undo(
            __operations_block_operations::move_block_op(
                "", // Will be set by OperationProvider::execute_operation
                id,
                &old_parent_id,
                old_predecessor.as_ref().map(|p| p.id()),
            ),
        ))
    }

    /// Move block to different position (reorder within same parent or different parent)
    ///
    /// # Parameters
    /// * `id` - Block ID to move
    /// * `parent_id` - Target parent ID (must always have a parent)
    /// * `after_block_id` - Optional anchor block (move after this block, or beginning if None)
    #[holon_macros::affects("parent_id", "depth", "sort_key")]
    #[holon_macros::triggered_by(availability_of = "tree_position", providing = ["parent_id", "after_block_id"])]
    #[holon_macros::triggered_by(availability_of = "selected_id", providing = ["parent_id"])]
    async fn move_block(
        &self,
        id: &str,
        parent_id: &str,
        after_block_id: Option<&str>,
    ) -> Result<UndoAction> {
        // Capture old state before mutation
        let maybe_block: Option<T> = self.get_by_id(id).await?;
        let block: T = maybe_block.ok_or_else(|| anyhow::anyhow!("Block not found"))?;
        let old_parent_id = block
            .parent_id()
            .ok_or_else(|| anyhow::anyhow!("Cannot move root block"))?
            .to_string();
        let old_predecessor = self.get_prev_sibling(id).await?;
        let old_depth = block.depth();

        // Query predecessor and successor sort_keys
        let (prev_key, next_key): (Option<String>, Option<String>) = match after_block_id {
            None => {
                // No after_block_id means "move to beginning" - insert before first child
                let first_child: Option<T> = self.get_first_child(Some(parent_id)).await?;
                let first_key = first_child.map(|c| c.sort_key().to_string());
                (None, first_key)
            }
            Some(after_id) => {
                // Insert after specific block
                let maybe_after_block: Option<T> = self.get_by_id(after_id).await?;
                let after_block: T =
                    maybe_after_block.ok_or_else(|| anyhow::anyhow!("Reference block not found"))?;
                let prev_key = Some(after_block.sort_key().to_string());

                // Find next sibling after the anchor block
                let next_sibling: Option<T> = self.get_next_sibling(after_id).await?;
                let next_key: Option<String> = next_sibling.map(|s: T| s.sort_key().to_string());
                (prev_key, next_key)
            }
        };

        // Generate new sort_key
        let mut new_sort_key = gen_key_between(prev_key.as_deref(), next_key.as_deref())
            .map_err(|e| anyhow::anyhow!(e))?;

        // Check if rebalancing needed
        if new_sort_key.len() > MAX_SORT_KEY_LENGTH {
            self.rebalance_siblings(Some(parent_id)).await?;

            // Re-query neighbors after rebalancing
            let (prev_key, next_key): (Option<String>, Option<String>) = match after_block_id {
                None => {
                    let first_child: Option<T> = self.get_first_child(Some(parent_id)).await?;
                    let first_key = first_child.map(|c| c.sort_key().to_string());
                    (None, first_key)
                }
                Some(after_id) => {
                    let maybe_after_block: Option<T> = self.get_by_id(after_id).await?;
                    let after_block: T = maybe_after_block
                        .ok_or_else(|| anyhow::anyhow!("Reference block not found"))?;
                    let prev_key = Some(after_block.sort_key().to_string());
                    let next_sibling: Option<T> = self.get_next_sibling(after_id).await?;
                    let next_key: Option<String> =
                        next_sibling.map(|s: T| s.sort_key().to_string());
                    (prev_key, next_key)
                }
            };

            new_sort_key = gen_key_between(prev_key.as_deref(), next_key.as_deref())
                .map_err(|e| anyhow::anyhow!(e))?;
        }

        // Calculate new depth based on parent
        let maybe_parent: Option<T> = self.get_by_id(parent_id).await?;
        let parent: T = maybe_parent.ok_or_else(|| anyhow::anyhow!("Parent not found"))?;
        let new_depth = parent.depth() + 1;

        // Calculate depth delta for recursive updates
        let depth_delta = new_depth - old_depth;

        // Update block atomically
        self.set_field(id, "parent_id", Value::String(parent_id.to_string()))
            .await?;
        self.set_field(id, "sort_key", Value::String(new_sort_key))
            .await?;
        self.set_field(id, "depth", Value::Integer(new_depth))
            .await?;

        // Recursively update all descendants' depths by the same delta
        if depth_delta != 0 {
            self.update_descendant_depths(id, depth_delta).await?;
        }

        // Return inverse operation using macro-generated helper
        use crate::__operations_block_operations;

        // Entity name will be set by OperationProvider when operation is executed
        Ok(UndoAction::Undo(
            __operations_block_operations::move_block_op(
                "", // Will be set by OperationProvider::execute_operation
                id,
                &old_parent_id,
                old_predecessor.as_ref().map(|p| p.id()),
            ),
        ))
    }

    /// Move block out to parent's level (decrease indentation)
    #[holon_macros::affects("parent_id", "depth", "sort_key")]
    async fn outdent(&self, id: &str) -> Result<UndoAction> {
        let maybe_block: Option<T> = self.get_by_id(id).await?;
        let block: T = maybe_block.ok_or_else(|| anyhow::anyhow!("Block not found"))?;
        let parent_id = block
            .parent_id()
            .ok_or_else(|| anyhow::anyhow!("Cannot outdent root block"))?;

        let maybe_parent: Option<T> = self.get_by_id(parent_id).await?;
        let parent: T = maybe_parent.ok_or_else(|| anyhow::anyhow!("Parent not found"))?;
        let grandparent_id = parent
            .parent_id()
            .ok_or_else(|| anyhow::anyhow!("Cannot outdent: parent is already at root level"))?;

        // Move to grandparent's children, after parent
        // move_block returns the inverse, but we need to return the inverse of outdent
        // which is indent. So we need to capture state before move_block
        let block = self
            .get_by_id(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block not found"))?;
        let old_parent_id = block
            .parent_id()
            .ok_or_else(|| anyhow::anyhow!("Cannot outdent root block"))?
            .to_string();

        self.move_block(id, grandparent_id, Some(parent_id)).await?;

        // Return inverse operation using macro-generated helper
        use crate::__operations_block_operations;

        // Entity name will be set by OperationProvider when operation is executed
        Ok(UndoAction::Undo(__operations_block_operations::indent_op(
            "", // Will be set by OperationProvider::execute_operation
            id,
            &old_parent_id,
        )))
    }

    /// Split a block at a given position
    ///
    /// Creates a new block with content after the cursor and truncates
    /// the original block to content before the cursor. The new block
    /// appears directly below the original block using fractional indexing.
    ///
    /// # Parameters
    /// * `id` - Block ID to split
    /// * `position` - Character position to split at (as i64, will be converted to usize)
    #[holon_macros::affects("content")]
    async fn split_block(&self, id: &str, position: i64) -> Result<UndoAction> {
        use uuid::Uuid;

        let maybe_block: Option<T> = self.get_by_id(id).await?;
        let block: T = maybe_block.ok_or_else(|| anyhow::anyhow!("Block not found"))?;

        let content = block.content();

        // Convert i64 to usize (validate it's non-negative and fits in usize)
        if position < 0 {
            return Err(anyhow::anyhow!("Position must be non-negative").into());
        }
        let position = position as usize;

        // Validate offset is within bounds
        if position > content.len() {
            return Err(anyhow::anyhow!(
                "Split position {} exceeds content length {}",
                position,
                content.len()
            )
            .into());
        }

        // Split content at cursor
        let mut content_before = content[..position].to_string();
        let mut content_after = content[position..].to_string();

        // Strip trailing whitespace from the old block
        content_before = content_before.trim_end().to_string();

        // Strip leading whitespace from the new block
        content_after = content_after.trim_start().to_string();

        // Generate new block ID
        let new_block_id = Uuid::new_v4().to_string();

        // Get next sibling's sort_key to position new block correctly
        let next_sibling: Option<T> = self.get_next_sibling(id).await?;
        let next_sort_key: Option<String> = next_sibling.map(|s: T| s.sort_key().to_string());

        // Generate sort_key for new block (between current block and next sibling)
        let new_sort_key = gen_key_between(Some(block.sort_key()), next_sort_key.as_deref())
            .map_err(|e| anyhow::anyhow!(e))?;

        // Get current timestamp
        let now = chrono::Utc::now().timestamp_millis();

        // Create new block using create method
        let mut new_block_fields = HashMap::new();
        new_block_fields.insert("id".to_string(), Value::String(new_block_id.clone()));
        new_block_fields.insert("content".to_string(), Value::String(content_after));
        new_block_fields.insert("parent_id".to_string(), {
            if let Some(ref pid) = block.parent_id() {
                Value::String(pid.to_string())
            } else {
                Value::Null
            }
        });
        new_block_fields.insert("depth".to_string(), Value::Integer(block.depth()));
        new_block_fields.insert("sort_key".to_string(), Value::String(new_sort_key));
        new_block_fields.insert("created_at".to_string(), Value::Integer(now));
        new_block_fields.insert("updated_at".to_string(), Value::Integer(now));
        new_block_fields.insert("collapsed".to_string(), Value::Boolean(false));
        new_block_fields.insert("completed".to_string(), Value::Boolean(false));
        new_block_fields.insert("block_type".to_string(), Value::String("text".to_string()));

        self.create(new_block_fields).await?;

        // Update current block with truncated content
        self.set_field(id, "content", Value::String(content_before))
            .await?;
        self.set_field(id, "updated_at", Value::Integer(now))
            .await?;

        // TODO: Return inverse operation (combine set_field inverses + delete for new block)
        Ok(UndoAction::Irreversible)
    }

    /// Move a block up (swap with previous sibling)
    #[holon_macros::affects("parent_id", "sort_key")]
    async fn move_up(&self, id: &str) -> Result<UndoAction> {
        // Capture old state
        let block = self
            .get_by_id(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block not found"))?;
        let parent_id = block
            .parent_id()
            .ok_or_else(|| anyhow::anyhow!("Cannot move root block"))?
            .to_string();
        let old_predecessor = self.get_prev_sibling(id).await?;
        let _next_sibling = self.get_next_sibling(id).await?;

        let prev_sibling: T = self
            .get_prev_sibling(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Cannot move up: no previous sibling"))?;

        // Get the sibling before prev_sibling
        let before_prev: Option<T> = self.get_prev_sibling(prev_sibling.id()).await?;

        // Execute move
        if let Some(before_id) = before_prev {
            self.move_block(id, &parent_id, Some(before_id.id()))
                .await?;
        } else {
            // Move to beginning
            self.move_block(id, &parent_id, None).await?;
        }

        // Return inverse (move down - restore original position) using macro-generated helper
        // Use move_block_op to restore exact old position (move_up_op is relative, not absolute)
        use crate::__operations_block_operations;

        Ok(UndoAction::Undo(
            __operations_block_operations::move_block_op(
                "", // Will be set by OperationProvider::execute_operation
                id,
                &parent_id,
                old_predecessor.as_ref().map(|p| p.id()),
            ),
        ))
    }

    /// Move a block down (swap with next sibling)
    #[holon_macros::affects("parent_id", "sort_key")]
    async fn move_down(&self, id: &str) -> Result<UndoAction> {
        // Capture old state
        let block = self
            .get_by_id(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block not found"))?;
        let parent_id = block
            .parent_id()
            .ok_or_else(|| anyhow::anyhow!("Cannot move root block"))?
            .to_string();
        let old_predecessor = self.get_prev_sibling(id).await?;

        let next_sibling: T = self
            .get_next_sibling(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Cannot move down: no next sibling"))?;

        // Execute move after next_sibling
        self.move_block(id, &parent_id, Some(next_sibling.id()))
            .await?;

        // Return inverse (move up - restore original position) using macro-generated helper
        use crate::__operations_block_operations;

        Ok(UndoAction::Undo(
            __operations_block_operations::move_block_op(
                "", // Will be set by OperationProvider::execute_operation
                id,
                &parent_id,
                old_predecessor.as_ref().map(|p| p.id()),
            ),
        ))
    }
}

/// Rename operations (for entities with a name field)
///
/// This trait provides a rename operation for entities that have a name or title
/// that can be changed.
#[holon_macros::operations_trait]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait RenameOperations<T>: MaybeSendSync
where
    T: MaybeSendSync + 'static,
{
    /// Rename an entity
    #[holon_macros::affects("name")]
    async fn rename(&self, id: &str, name: String) -> Result<UndoAction>;
}

/// Move operations (for entities with hierarchical structure)
///
/// This trait provides a move operation for entities that can be moved within
/// a hierarchical structure, such as directories, files, or blocks.
#[holon_macros::operations_trait]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MoveOperations<T>: MaybeSendSync
where
    T: MaybeSendSync + 'static,
{
    /// Move an entity to a different position within a hierarchical structure
    ///
    /// # Parameters
    /// * `id` - Entity ID to move
    /// * `parent_id` - Target parent ID
    /// * `after_id` - Optional anchor entity (move after this entity, or beginning if None)
    #[holon_macros::affects("parent_id", "depth", "sort_key")]
    async fn move_entity(
        &self,
        id: &str,
        parent_id: &str,
        after_id: Option<&str>,
    ) -> Result<UndoAction>;
}

/// Task management operations (for any task-like entity)
///
/// This trait provides operations for managing task properties like completion,
/// priority, and due dates. It requires that the entity type implements `TaskEntity`
/// and that the datasource implements `CrudOperations`.
#[holon_macros::operations_trait]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait TaskOperations<T>: CrudOperations<T>
where
    T: TaskEntity + MaybeSendSync + 'static,
{
    /// Toggle or set task completion status
    #[holon_macros::triggered_by(availability_of = "completed")]
    async fn set_completion(&self, id: &str, completed: bool) -> Result<UndoAction> {
        self.set_field(id, "completed", Value::Boolean(completed))
            .await
    }

    /// Set task priority (1=highest, 4=lowest in Todoist)
    #[holon_macros::affects("priority")]
    #[holon_macros::triggered_by(availability_of = "priority")]
    async fn set_priority(&self, id: &str, priority: i64) -> Result<UndoAction> {
        self.set_field(id, "priority", Value::Integer(priority))
            .await
    }

    /// Set task due date
    #[holon_macros::affects("due_date")]
    async fn set_due_date(&self, id: &str, due_date: Option<DateTime<Utc>>) -> Result<UndoAction> {
        self.set_field(
            id,
            "due_date",
            due_date
                .map(Value::from_datetime)
                .unwrap_or(Value::Null),
        )
        .await
    }
}

// Blanket implementations: Automatically provide helper methods for any compatible type
impl<T, D> BlockDataSourceHelpers<T> for D
where
    T: BlockEntity + MaybeSendSync + 'static,
    D: CrudOperations<T> + DataSource<T>,
{
    // All methods have default implementations, so nothing to implement here
}

// Blanket implementation: Automatically provide BlockOperations for types that implement BlockDataSourceHelpers
impl<T, D> BlockOperations<T> for D
where
    T: BlockEntity + MaybeSendSync + 'static,
    D: BlockDataSourceHelpers<T>,
{
    // All methods have default implementations in the trait, so nothing to implement here
}

// Blanket implementation: Automatically provide TaskOperations for types that implement CrudOperations
impl<T, D> TaskOperations<T> for D
where
    T: TaskEntity + MaybeSendSync + 'static,
    D: CrudOperations<T>,
{
    // All methods have default implementations in the trait, so nothing to implement here
}

/// Operations on the operation log for undo/redo functionality.
///
/// This trait provides methods for:
/// - Logging new operations with their inverses
/// - Marking operations as undone/redone
/// - Trimming old operations
///
/// Undo/redo candidates are retrieved via PRQL queries, not through this trait.
/// Implementors interact with the persistent `operations` table.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait OperationLogOperations: MaybeSendSync {
    /// Log a new operation with its inverse.
    ///
    /// Inserts the operation into the log and trims old entries if needed.
    /// Returns the assigned log entry ID.
    async fn log_operation(&self, operation: Operation, inverse: UndoAction) -> Result<i64>;

    /// Mark an operation as undone.
    async fn mark_undone(&self, id: i64) -> Result<()>;

    /// Mark an operation as redone (restore to normal status).
    async fn mark_redone(&self, id: i64) -> Result<()>;

    /// Clear the redo stack (mark all undone operations as cancelled).
    ///
    /// Called when a new operation is executed to invalidate the redo history.
    async fn clear_redo_stack(&self) -> Result<()>;

    /// Get the maximum number of operations to retain.
    fn max_log_size(&self) -> usize {
        100
    }
}
