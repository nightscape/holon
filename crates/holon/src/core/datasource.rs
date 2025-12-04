//! Stream-based datasource traits for external system integration
//!
//! This module provides traits for reactive sync architecture where:
//! - DataSources provide read-only access (from cache)
//! - CrudOperationss provide write operations (fire-and-forget to external systems)
//! - Changes flow via ChangeNotifications streams from SyncProviders to Caches
//! - QueryableCache wraps datasources and implements both read and write traits
//!
//! # WASM Compatibility
//! On WASM, futures are often !Send (because they wrap JS promises).
//! Therefore, we use `MaybeSendSync` trait alias and `#[async_trait(?Send)]`
//! to relax thread-safety requirements on WASM builds.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt;

use crate::storage::types::StorageEntity;
use holon_api::Value;

// Re-export core traits from holon-core
pub use holon_core::{
    BlockDataSourceHelpers, BlockEntity, BlockOperations, CrudOperations, DataSource,
    MaybeSendSync, MoveOperations, OperationRegistry, RenameOperations, Result, TaskEntity,
    TaskOperations, UndoAction, UnknownOperationError,
};

// Re-export undo types for external crates
pub use holon_api::Operation;
pub use holon_core::undo::UndoStack;

// Re-export macro-generated operation dispatch functions from holon-core
#[cfg(not(target_arch = "wasm32"))]
pub use holon_core::{
    __operations_block_operations, __operations_crud_operations, __operations_move_operations,
    __operations_rename_operations, __operations_task_operations,
};

// Backwards compatibility aliases for old module names
#[cfg(not(target_arch = "wasm32"))]
pub use __operations_block_operations as __operations_mutable_block_data_source;
#[cfg(not(target_arch = "wasm32"))]
pub use __operations_crud_operations as __operations_crud_operation_provider;
#[cfg(not(target_arch = "wasm32"))]
pub use __operations_task_operations as __operations_mutable_task_data_source;

// Re-export OperationDescriptor and OperationParam from holon-api
pub use holon_api::{OperationDescriptor, OperationParam};

// Re-export Change types from api (which re-exports from holon-api)
pub use crate::api::{Change, ChangeOrigin, StreamPosition};

// Result and UnknownOperationError are now defined in holon-core and re-exported above.

/// Parameter descriptor for operation metadata (legacy, kept for backward compatibility)
#[derive(Debug, Clone)]
pub struct ParamDescriptor {
    pub name: String,
    pub param_type: String, // "String", "bool", "i64", etc.
    pub required: bool,
    pub default: Option<Value>,
}

// DataSource, BlockDataSourceHelpers, BlockOperations, and TaskOperations
// are now defined in holon-core and re-exported above.

// All traits (DataSource, BlockDataSourceHelpers, BlockOperations, TaskOperations)
// and their blanket implementations are now defined in holon-core and re-exported above.

// Note: BlockOperations is defined in holon-core, so we can't provide blanket impls here.
// Types that implement BlockDataSourceHelpers<T> should manually implement BlockOperations<T>
// using the helper functions below, or use the provided implementations via a wrapper type.
//
// For convenience, we provide a helper module with default implementations that can be used
// by types implementing BlockDataSourceHelpers<T>.
pub mod mutable_block_impls {
    use super::*;

    // Helper functions that can be used by BlockOperations implementations
    // These are kept here for reference, but types should implement BlockOperations
    // methods directly using BlockDataSourceHelpers methods.
}

// All trait implementations are now in holon-core.

// TaskOperations has default implementations in holon-core, so no blanket impl needed here.

/// Type-independent operation provider trait
///
/// Supports both local (cache-based) and external (API-based) providers.
/// Operations are self-describing via OperationDescriptor metadata.
///
/// # Design
/// - **OperationProvider = QueryableCache + dispatch layer**: Routes `execute_operation` to
///   CRUD operations (create/set_field/delete) or custom operations
/// - **Composite dispatcher pattern**: OperationDispatcher itself implements OperationProvider,
///   allowing composition
/// - **Provider registry discovery**: Providers expose introspection via `operations()`,
///   allowing OperationDispatcher to discover all providers at runtime via DI
///
/// # Examples
/// ```ignore
/// // Individual cache:
/// cache.execute_operation("todoist-task", "set_completion", params).await?;
///
/// // Composite dispatcher:
/// dispatcher.execute_operation("todoist-task", "set_completion", params).await?;
/// // → Routes to TodoistQueryableCache → set_field("completed", true)
/// ```
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait OperationProvider: Send + Sync {
    /// Get all operations this provider supports
    fn operations(&self) -> Vec<OperationDescriptor>;

    /// Find operations that can be executed with given arguments
    ///
    /// Filters to operations where required_params can be satisfied either:
    /// 1. Directly from available_args, OR
    /// 2. Via param_mappings (from other widgets like drop targets)
    ///
    /// Example:
    /// ```
    /// // Lineage: checkbox modifies "completed" field
    /// // Available: ["id", "completed"]
    /// let ops = provider.find_operations("todoist-task", &["id", "completed"]);
    /// // Returns: ["set_field", "set_completion", "delete"]
    /// // Also returns: ["move_block"] if it has param_mappings for parent_id
    /// ```
    fn find_operations(
        &self,
        entity_name: &str,
        available_args: &[String],
    ) -> Vec<OperationDescriptor> {
        self.operations()
            .into_iter()
            .filter(|op| {
                if op.entity_name != entity_name {
                    return false;
                }

                // Check each required param
                op.required_params.iter().all(|p| {
                    // Param is directly available
                    if available_args.contains(&p.name) {
                        return true;
                    }

                    // Param can be provided via a param_mapping
                    // (from another widget like drop target)
                    op.param_mappings
                        .iter()
                        .any(|mapping| mapping.provides.contains(&p.name))
                })
            })
            .collect()
    }

    /// Execute an operation
    ///
    /// - Individual caches: validate entity_name, dispatch to trait methods
    /// - Composite dispatcher: route to correct registered provider
    ///
    /// Returns the UndoAction for undo support.
    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<UndoAction>;

    /// Get the last created entity ID (if any)
    ///
    /// This is used by GenericProviderState to track entity creation.
    /// Providers that support this should override this method to return
    /// the ID of the last entity created via execute_operation.
    /// Default implementation returns None.
    fn get_last_created_id(&self) -> Option<String> {
        None
    }
}

/// Observer for operation execution events
///
/// Observers are notified after an operation is successfully executed.
/// This enables cross-cutting concerns like:
/// - Operation logging for undo/redo
/// - Audit trails
/// - Analytics
/// - Sync queue management
///
/// Unlike OperationProvider (which executes operations), observers only
/// observe the results. They cannot modify or veto operations.
///
/// # Entity Filter
/// Observers specify which entities they're interested in via `entity_filter()`:
/// - Return `"*"` to observe all operations (e.g., operation log, audit)
/// - Return a specific entity name to observe only that entity
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait OperationObserver: Send + Sync {
    /// Entity filter for this observer
    ///
    /// Returns `"*"` to observe all entities, or a specific entity name.
    fn entity_filter(&self) -> &str;

    /// Called after an operation is successfully executed
    ///
    /// # Arguments
    /// * `operation` - The operation that was executed
    /// * `undo_action` - The undo action returned by the operation (may be Irreversible)
    ///
    /// # Note
    /// This is called only for successful operations. Failed operations are not observed.
    /// Observers should not perform operations that could fail and block the main flow.
    async fn on_operation_executed(&self, operation: &Operation, undo_action: &UndoAction);
}

// OperationRegistry trait is now defined in holon-core and re-exported above.

/// Trait for persisting and loading sync tokens
///
/// SyncableProviders use this trait to persist their sync tokens across app restarts.
/// Implementations typically store tokens in a database or file system.
/// Trait for storing sync tokens for external providers.
///
/// This trait is used internally for dependency injection and should not be exposed to FFI.
/// flutter_rust_bridge:ignore
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait SyncTokenStore: Send + Sync {
    /// Load sync token for a provider
    ///
    /// Returns None if no token exists (first sync).
    async fn load_token(&self, provider_name: &str) -> Result<Option<StreamPosition>>;

    /// Save sync token for a provider
    async fn save_token(&self, provider_name: &str, position: StreamPosition) -> Result<()>;
}

/// Type-independent sync trait for providers
///
/// Providers that can sync from external systems implement this trait.
/// Sync operations are generated dynamically when providers are registered,
/// using the format "{provider_name}.sync" (e.g., "todoist.sync", "jira.sync").
///
/// SyncableProviders should:
/// - Load current token using SyncTokenStore before syncing
/// - Perform sync operation
/// - Save new token using SyncTokenStore after syncing
/// - Return the new token
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait SyncableProvider: Send + Sync {
    /// Get the provider name (e.g., "todoist", "jira")
    ///
    /// This name is used to generate sync operations and identify the provider.
    fn provider_name(&self) -> &str;

    /// Sync data from the external system
    ///
    /// This method should:
    /// - Load current token using SyncTokenStore
    /// - Fetch updates from the external system using the stream position
    /// - Emit changes via streams (if applicable)
    /// - Save new token using SyncTokenStore
    /// - Return the new stream position
    ///
    /// # Arguments
    /// * `position` - Current stream position (StreamPosition::Beginning for full sync, StreamPosition::Version(token) for incremental sync)
    ///
    /// # Returns
    /// The new stream position (typically StreamPosition::Version with new token, or StreamPosition::Beginning if no token)
    async fn sync(&self, position: StreamPosition) -> Result<StreamPosition>;
}

/// Trait for external sync providers that emit typed change streams
///
/// This trait allows QueryableCache to register and consume change streams
/// from external systems (Todoist, etc.) in a type-safe way.
/// ExternalServiceDiscovery
pub trait StreamProvider<T>: MaybeSendSync
where
    T: MaybeSendSync + 'static,
{
    /// Get a receiver for changes of type T
    ///
    /// Returns a broadcast receiver that emits batches of changes.
    /// Multiple QueryableCache instances can subscribe to the same stream.
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<Vec<Change<T>>>;
}

/// Generate a sync operation descriptor for a provider
///
/// This is used by OperationDispatcher when registering SyncableProviders
/// to create operation descriptors with the correct entity_name format.
pub fn generate_sync_operation(provider_name: &str) -> OperationDescriptor {
    OperationDescriptor {
        entity_name: format!("{}.sync", provider_name),
        entity_short_name: "all".to_string(), // Sync operations affect all entities
        id_column: String::new(),             // Sync operations don't need an ID column
        name: "sync".to_string(),
        display_name: format!("Sync {}", provider_name),
        description: format!("Sync data from {} provider", provider_name),
        required_params: vec![],
        affected_fields: vec![], // Sync operations don't affect specific fields
        param_mappings: vec![],
        precondition: None,
    }
}
