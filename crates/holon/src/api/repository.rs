//! DocumentRepository trait and related types
//!
//! This module defines the repository pattern interface for interacting
//! with hierarchical block documents. All frontends use this trait.
//!
//! # Trait Architecture
//!
//! The API is split into 4 focused traits that backends can implement selectively:
//!
//! - `CoreOperations`: CRUD and batch operations (required for all backends)
//! - `Lifecycle`: Document creation and disposal (required for all backends)
//! - `ChangeNotifications`: Real-time state sync and change streams
//! - `P2POperations`: Peer-to-peer networking and synchronization
//!
//! The `DocumentRepository` supertrait combines all four for convenience.
//! Backends implementing all four automatically satisfy `DocumentRepository`.
//!
//! ## Examples
//!
//! ```rust,no_run
//! use holon::api::{CoreOperations, Lifecycle};
//!
//! // Minimal backend (no networking, no change notifications)
//! struct MemoryBackend { /* ... */ }
//! impl CoreOperations for MemoryBackend { /* ... */ }
//! impl Lifecycle for MemoryBackend { /* ... */ }
//!
//! // Full-featured backend
//! struct LoroBackend { /* ... */ }
//! impl CoreOperations for LoroBackend { /* ... */ }
//! impl Lifecycle for LoroBackend { /* ... */ }
//! impl ChangeNotifications for LoroBackend { /* ... */ }
//! impl P2POperations for LoroBackend { /* ... */ }
//! // LoroBackend automatically implements DocumentRepository via blanket impl
//! ```

use holon_api::streaming::ChangeNotifications;

use super::types::{NewBlock, Traversal};
use async_trait::async_trait;
use holon_api::{ApiError, Block};

/// Core CRUD and batch operations for block documents.
///
/// This trait provides the fundamental operations for creating, reading,
/// updating, and deleting blocks. All backends must implement this trait.
///
/// # Example
///
/// ```rust,no_run
/// use holon::api::CoreOperations;
///
/// async fn example(repo: impl CoreOperations) -> anyhow::Result<()> {
///     // Create a new root block
///     let block = repo.create_block(None, "Hello".to_string(), None).await?;
///
///     // Update its content
///     repo.update_block(&block.id, "Hello, world!".to_string()).await?;
///
///     // Get it back
///     let updated = repo.get_block(&block.id).await?;
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait CoreOperations: Send + Sync {
    // ===== Single-Block Operations =====

    /// Get a block by ID.
    ///
    /// # Arguments
    ///
    /// * `id` - Block ID (URI format)
    ///
    /// # Errors
    ///
    /// Returns `ApiError::BlockNotFound` if block doesn't exist or is deleted.
    async fn get_block(&self, id: &str) -> Result<Block, ApiError>;

    /// Get ancestor chain by traversing parent_id links.
    ///
    /// Returns a vector of parent IDs from immediate parent up to (but not including) the root.
    /// Stops when encountering NO_PARENT_ID sentinel value or the root block itself.
    ///
    /// # Arguments
    ///
    /// * `id` - Starting block ID
    ///
    /// # Returns
    ///
    /// Vector of ancestor IDs in order: [immediate_parent, grandparent, ..., root_child]
    ///
    /// # Errors
    ///
    /// Returns `ApiError::BlockNotFound` if any block in the chain doesn't exist.
    async fn get_ancestor_chain(&self, id: &str) -> Result<Vec<String>, ApiError> {
        use holon_api::block::NO_PARENT_ID;

        let mut ancestors = Vec::new();
        let mut current_id = id.to_string();

        loop {
            let block = self.get_block(&current_id).await?;

            if block.parent_id == NO_PARENT_ID {
                break;
            }

            ancestors.push(block.parent_id.clone());
            current_id = block.parent_id;
        }

        Ok(ancestors)
    }

    /// Get all non-deleted blocks in tree order, filtered by depth.
    ///
    /// Returns blocks in depth-first traversal order. Use `Block::depth()`
    /// if you need nesting level information.
    ///
    /// # Example Order
    ///
    /// ```text
    /// Block A (root)
    ///   Block A1 (child)
    ///     Block A1a (grandchild)
    ///   Block A2 (child)
    /// Block B (root)
    /// ```
    ///
    /// # Arguments
    ///
    /// * `traversal` - Depth filter configuration (use `Traversal::ALL_BUT_ROOT` for typical use)
    ///
    /// # Performance
    ///
    /// This method may be expensive for large documents. Use with caution
    /// in performance-critical code paths. The traversal can stop descending
    /// once max_level is reached for optimization.
    async fn get_all_blocks(&self, traversal: Traversal) -> Result<Vec<Block>, ApiError>;

    /// List children IDs of a block in display order.
    ///
    /// # Arguments
    ///
    /// * `parent_id` - Parent block ID
    ///
    /// # Errors
    ///
    /// Returns `ApiError::BlockNotFound` if parent doesn't exist.
    async fn list_children(&self, parent_id: &str) -> Result<Vec<String>, ApiError>;

    /// Create a new block.
    ///
    /// # Arguments
    ///
    /// * `parent_id` - Parent block ID
    /// * `content` - Initial content (text, source block, etc.)
    /// * `id` - Optional custom ID (None = generate `local://<uuid-v4>`)
    ///
    /// # Returns
    ///
    /// The created `Block` with generated or provided ID.
    ///
    /// # Errors
    ///
    /// Returns `ApiError::BlockNotFound` if parent_id doesn't exist.
    async fn create_block(
        &self,
        parent_id: String,
        content: holon_api::BlockContent,
        id: Option<String>,
    ) -> Result<Block, ApiError>;

    /// Update block content.
    ///
    /// # Arguments
    ///
    /// * `id` - Block ID to update
    /// * `content` - New content (replaces existing)
    ///
    /// # Errors
    ///
    /// Returns `ApiError::BlockNotFound` if block doesn't exist.
    async fn update_block(
        &self,
        id: &str,
        content: holon_api::BlockContent,
    ) -> Result<(), ApiError>;

    /// Delete a block (tombstone).
    ///
    /// Sets `deleted_at` timestamp but keeps block in CRDT for consistency.
    /// Children are NOT deleted (no cascading delete).
    ///
    /// # Arguments
    ///
    /// * `id` - Block ID to delete
    ///
    /// # Errors
    ///
    /// Returns `ApiError::BlockNotFound` if block doesn't exist.
    async fn delete_block(&self, id: &str) -> Result<(), ApiError>;

    /// Move block to new parent and position.
    ///
    /// Uses anchor-based positioning (after sibling) which is more CRDT-friendly
    /// than index-based positioning.
    ///
    /// # Arguments
    ///
    /// * `id` - Block to move
    /// * `new_parent` - New parent ID
    /// * `after` - Insert after this sibling (None = insert at start)
    ///
    /// # Errors
    ///
    /// * `ApiError::BlockNotFound` - Block or parent doesn't exist
    /// * `ApiError::CyclicMove` - Would create a cycle (block to its descendant)
    async fn move_block(
        &self,
        id: &str,
        new_parent: String,
        after: Option<String>,
    ) -> Result<(), ApiError>;

    // ===== Block-Based Convenience Methods =====
    //
    // These methods accept Block references instead of IDs, making it easier
    // to work with opaque block types in FFI scenarios (e.g., Flutter-Rust-Bridge).
    // They provide default implementations that extract IDs and call the
    // corresponding ID-based methods.

    /// Update block content using a block reference.
    ///
    /// This is a convenience method that extracts the ID internally,
    /// making it easier to work with opaque block types in FFI scenarios.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to update
    /// * `content` - New content (replaces existing)
    ///
    /// # Errors
    ///
    /// Returns `ApiError::BlockNotFound` if block doesn't exist.
    async fn update_block_by_ref(
        &self,
        block: &Block,
        content: holon_api::BlockContent,
    ) -> Result<(), ApiError> {
        self.update_block(&block.id, content).await
    }

    /// Delete a block using a block reference.
    ///
    /// This is a convenience method that extracts the ID internally.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to delete
    ///
    /// # Errors
    ///
    /// Returns `ApiError::BlockNotFound` if block doesn't exist.
    async fn delete_block_by_ref(&self, block: &Block) -> Result<(), ApiError> {
        self.delete_block(&block.id).await
    }

    /// Move block to new parent and position using block references.
    ///
    /// This is a convenience method that accepts block references instead of IDs,
    /// making it more ergonomic when working with opaque types in FFI scenarios.
    ///
    /// # Arguments
    ///
    /// * `block` - Block to move
    /// * `new_parent` - New parent block (None = move to root)
    /// * `after` - Insert after this sibling block (None = insert at start)
    ///
    /// # Errors
    ///
    /// * `ApiError::BlockNotFound` - Block or parent doesn't exist
    /// * `ApiError::CyclicMove` - Would create a cycle
    async fn move_block_by_ref(
        &self,
        block: &Block,
        new_parent: Option<&Block>,
        after: Option<&Block>,
    ) -> Result<(), ApiError> {
        let parent_id = new_parent.map(|p| p.id.clone()).unwrap_or_else(|| {
            // If no parent specified, keep block's current parent
            block.parent_id.clone()
        });
        let after_id = after.map(|a| a.id.clone());

        self.move_block(&block.id, parent_id, after_id).await
    }

    // ===== Batch Operations =====

    /// Get multiple blocks by ID.
    ///
    /// More efficient than multiple `get_block()` calls when fetching many blocks.
    ///
    /// # Arguments
    ///
    /// * `ids` - List of block IDs to fetch
    ///
    /// # Returns
    ///
    /// Vector of blocks that exist. Non-existent blocks are omitted (no error).
    async fn get_blocks(&self, ids: Vec<String>) -> Result<Vec<Block>, ApiError>;

    /// Create multiple blocks in a single transaction.
    ///
    /// All blocks are created atomically. If any fails, entire operation rolls back.
    ///
    /// # Arguments
    ///
    /// * `blocks` - List of block templates
    ///
    /// # Returns
    ///
    /// Vector of created blocks in same order as input.
    ///
    /// # Errors
    ///
    /// Returns error if any parent doesn't exist or operation fails.
    async fn create_blocks(&self, blocks: Vec<NewBlock>) -> Result<Vec<Block>, ApiError>;

    /// Delete multiple blocks in a single transaction.
    ///
    /// All blocks are deleted atomically (tombstone pattern).
    ///
    /// # Arguments
    ///
    /// * `ids` - List of block IDs to delete
    ///
    /// # Errors
    ///
    /// Returns error if operation fails. Non-existent blocks are silently ignored.
    async fn delete_blocks(&self, ids: Vec<String>) -> Result<(), ApiError>;
}

/// Document lifecycle management.
///
/// This trait handles creating new documents, opening existing ones,
/// and resource cleanup. All backends must implement this trait.
///
/// # Example
///
/// ```rust,no_run
/// use holon::api::Lifecycle;
///
/// async fn example<R: Lifecycle>() -> anyhow::Result<()> {
///     // Create a new document
///     let repo = R::create_new("my-doc".to_string()).await?;
///
///     // ... use the repository ...
///
///     // Clean up resources
///     repo.dispose().await?;
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait Lifecycle: Send + Sync {
    /// Create a new document.
    ///
    /// # Arguments
    ///
    /// * `doc_id` - Unique identifier for the document
    ///
    /// # Errors
    ///
    /// Returns `ApiError::InternalError` if document creation fails.
    async fn create_new(doc_id: String) -> Result<Self, ApiError>
    where
        Self: Sized;

    /// Open an existing document.
    ///
    /// # Arguments
    ///
    /// * `doc_id` - Identifier of document to open
    ///
    /// # Errors
    ///
    /// Returns `ApiError::DocumentNotFound` if document doesn't exist.
    async fn open_existing(doc_id: String) -> Result<Self, ApiError>
    where
        Self: Sized;

    /// Dispose of document and release resources.
    ///
    /// Should be called before dropping the repository to ensure clean shutdown.
    /// Critical for Flutter hot-restart support.
    async fn dispose(&self) -> Result<(), ApiError>;
}

/// Peer-to-peer networking and synchronization.
///
/// This trait provides P2P connectivity for distributed document synchronization.
/// Backends that support networking implement this trait.
///
/// # Example
///
/// ```rust,no_run
/// use holon::api::P2POperations;
///
/// async fn example(repo: impl P2POperations) -> anyhow::Result<()> {
///     // Get this node's ID
///     let my_id = repo.get_node_id().await;
///     println!("Node ID: {}", my_id);
///
///     // Start accepting connections
///     repo.accept_connections().await?;
///
///     // Connect to a peer
///     repo.connect_to_peer("peer-node-id".to_string()).await?;
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait P2POperations: Send + Sync {
    /// Get this node's P2P identifier.
    ///
    /// Used by other peers to connect to this instance.
    async fn get_node_id(&self) -> String;

    /// Connect to a remote peer for P2P synchronization.
    ///
    /// # Arguments
    ///
    /// * `peer_node_id` - Remote peer's node ID
    ///
    /// # Errors
    ///
    /// Returns `ApiError::NetworkError` if connection fails.
    async fn connect_to_peer(&self, peer_node_id: String) -> Result<(), ApiError>;

    /// Start accepting incoming P2P connections.
    ///
    /// # Errors
    ///
    /// Returns `ApiError::NetworkError` if unable to listen.
    async fn accept_connections(&self) -> Result<(), ApiError>;
}

/// Complete repository interface combining all capabilities.
///
/// This is a convenience supertrait that combines `CoreOperations`, `Lifecycle`,
/// `ChangeNotifications`, and `P2POperations`. Any type implementing all four
/// automatically satisfies this trait via the blanket implementation.
///
/// # Example
///
/// ```rust,no_run
/// use holon::api::DocumentRepository;
///
/// async fn example(repo: impl DocumentRepository) -> anyhow::Result<()> {
///     // Can use all operations from all four traits
///     let initial = repo.get_initial_state().await?;
///     let block = repo.create_block(None, "Hello".to_string(), None).await?;
///     let node_id = repo.get_node_id().await;
///     repo.dispose().await?;
///     Ok(())
/// }
/// ```
pub trait DocumentRepository:
    CoreOperations + Lifecycle + ChangeNotifications<Block> + P2POperations
{
}

/// Blanket implementation: any type implementing all four traits automatically
/// implements DocumentRepository.
impl<T> DocumentRepository for T where
    T: CoreOperations + Lifecycle + ChangeNotifications<Block> + P2POperations
{
}
