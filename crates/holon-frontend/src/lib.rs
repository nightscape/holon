//! Frontend session abstraction for Holon
//!
//! This crate provides a unified initialization protocol for all frontend consumers
//! (Flutter, TUI, integration tests, etc.). It ensures consistent setup including:
//!
//! - Module registration (OrgMode, Todoist)
//! - Waiting for background tasks to be ready
//! - Tracking startup errors
//!
//! # Usage
//!
//! ```rust,ignore
//! use holon_frontend::{FrontendConfig, FrontendSession};
//!
//! let config = FrontendConfig::new()
//!     .with_db_path("/path/to/db".into())
//!     .with_orgmode("/path/to/org/files".into());
//!
//! let session = FrontendSession::new(config).await?;
//!
//! // Use session methods directly - this guarantees initialization is complete
//! let (widget_spec, stream) = session.initial_widget().await?;
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use holon::api::BackendEngine;
use holon::di::create_backend_engine_with_extras;
use holon::sync::{
    CacheEventSubscriber, LoroConfig, LoroModule, PublishErrorTracker, TursoEventBus,
};
use holon_api::block::Block;
use holon_orgmode::di::{FileWatcherReadySignal, OrgModeServiceCollectionExt};
use holon_todoist::di::TodoistServiceCollectionExt;

use holon::core::queryable_cache::QueryableCache;
use holon::di::DbHandleProvider;
use holon::sync::event_bus::EventBus;

// Re-export DiResolver for use in extra_resolve closures
pub use holon::di::DiResolver;

// Re-export types needed by consumers
pub use holon::api::backend_engine::QueryContext;
pub use holon::storage::turso::RowChangeStream;
pub use holon_api::{OperationDescriptor, Value, WidgetSpec};

/// Marker type for the CacheEventSubscriber background wiring.
/// Resolving this from DI triggers the EventBus → QueryableCache subscription.
struct CacheEventSubscriberHandle;

/// Configuration for frontend session initialization
#[derive(Debug, Clone, Default)]
pub struct FrontendConfig {
    /// Database file path (None = temporary file with random name)
    pub db_path: Option<PathBuf>,
    /// OrgMode root directory (None = disabled)
    pub orgmode_root: Option<PathBuf>,
    /// Enable Loro CRDT layer (default: false)
    pub loro_enabled: bool,
    /// Loro storage directory (default: orgmode_root/.loro or db_path dir/.loro)
    pub loro_storage_dir: Option<PathBuf>,
    /// Todoist API key (None = disabled)
    pub todoist_api_key: Option<String>,
    /// Enable Todoist with fake client (for testing)
    pub todoist_fake: bool,
    /// Whether to wait for file watcher readiness (default: true)
    pub wait_for_ready: bool,
    /// Additional settle time after ready signal (milliseconds, default: 100)
    pub settle_delay_ms: u64,
}

impl FrontendConfig {
    pub fn new() -> Self {
        Self {
            db_path: None,
            orgmode_root: None,
            loro_enabled: false,
            loro_storage_dir: None,
            todoist_api_key: None,
            todoist_fake: false,
            wait_for_ready: true,
            settle_delay_ms: 100,
        }
    }

    pub fn with_db_path(mut self, path: PathBuf) -> Self {
        self.db_path = Some(path);
        self
    }

    pub fn with_orgmode(mut self, root: PathBuf) -> Self {
        self.orgmode_root = Some(root);
        self
    }

    pub fn with_loro(mut self) -> Self {
        self.loro_enabled = true;
        self
    }

    pub fn with_loro_storage(mut self, dir: PathBuf) -> Self {
        self.loro_storage_dir = Some(dir);
        self
    }

    pub fn with_todoist(mut self, api_key: String) -> Self {
        self.todoist_api_key = Some(api_key);
        self
    }

    /// Enable Todoist with a fake in-memory client (for testing)
    ///
    /// This enables the same DI path as production (DDL, caches, streams),
    /// but uses a fake client instead of making real API calls.
    pub fn with_todoist_fake(mut self) -> Self {
        self.todoist_fake = true;
        self
    }

    pub fn without_wait(mut self) -> Self {
        self.wait_for_ready = false;
        self
    }

    pub fn with_settle_delay(mut self, ms: u64) -> Self {
        self.settle_delay_ms = ms;
        self
    }
}

/// Unified session for all frontend consumers (Flutter, TUI, tests)
///
/// Ensures consistent initialization:
/// 1. Registers configured modules
/// 2. Waits for background tasks to be ready
/// 3. Tracks startup errors
///
/// For tests that need additional services (e.g., LoroDocumentStore), use
/// `new_with_extras` which allows resolving additional services from DI.
pub struct FrontendSession<T = ()> {
    engine: Arc<BackendEngine>,
    error_tracker: PublishErrorTracker,
    ready_signal: Option<FileWatcherReadySignal>,
    /// Extra services resolved from DI (for tests)
    extras: T,
}

impl FrontendSession<()> {
    /// Create a new frontend session with the given configuration
    ///
    /// This blocks until the system is ready (file watcher initialized, etc.)
    /// unless `wait_for_ready` is set to false in the config.
    pub async fn new(config: FrontendConfig) -> Result<Self> {
        Self::new_with_extras(config, |_| ()).await
    }
}

impl<T> FrontendSession<T> {
    /// Create a new frontend session with additional services resolved from DI
    ///
    /// This is useful for tests that need access to internal services like
    /// `LoroDocumentStore` for assertions.
    ///
    /// # Example
    /// ```rust,ignore
    /// let session = FrontendSession::new_with_extras(config, |provider| {
    ///     let store = DiResolver::get_required::<LoroDocumentStore>(provider);
    ///     Arc::new(RwLock::new((*store).clone()))
    /// }).await?;
    /// ```
    pub async fn new_with_extras<F>(config: FrontendConfig, extra_resolve: F) -> Result<Self>
    where
        F: FnOnce(&ferrous_di::ServiceProvider) -> T,
    {
        #[cfg(not(target_arch = "wasm32"))]
        let db_path = config.db_path.unwrap_or_else(|| {
            std::env::temp_dir().join(format!("holon-{}.db", uuid::Uuid::new_v4()))
        });

        #[cfg(target_arch = "wasm32")]
        let db_path = config.db_path.unwrap_or_else(|| PathBuf::from(":memory:"));

        let orgmode_root = config.orgmode_root.clone();
        let loro_enabled = config.loro_enabled;
        let loro_storage_dir = config.loro_storage_dir.clone();
        let todoist_key = config.todoist_api_key.clone();
        let todoist_fake = config.todoist_fake;

        let (engine, resolved) = create_backend_engine_with_extras(
            db_path.clone(),
            move |services| {
                use ferrous_di::ServiceCollectionModuleExt;

                // Register shared event infrastructure when any data pipeline is active.
                // TursoEventBus, QueryableCache<Block>, and CacheEventSubscriber are needed
                // by both Loro and OrgMode. Register them here so either can work standalone.
                if loro_enabled || orgmode_root.is_some() {
                    services.add_singleton_factory::<QueryableCache<Block>, _>(|r| {
                        holon::di::create_queryable_cache(r)
                    });

                    services.add_singleton_factory::<TursoEventBus, _>(|resolver| {
                        let backend_provider = ferrous_di::Resolver::get_required_trait::<
                            dyn holon::di::TursoBackendProvider,
                        >(resolver);
                        let backend = backend_provider.backend();
                        let event_bus = TursoEventBus::new(backend);
                        tokio::task::block_in_place(|| {
                            tokio::runtime::Handle::current().block_on(async {
                                event_bus
                                    .init_schema()
                                    .await
                                    .expect("Failed to initialize EventBus schema");
                            })
                        });
                        event_bus
                    });

                    services.add_singleton(PublishErrorTracker::new());

                    // Wire CacheEventSubscriber: EventBus → QueryableCache<Block>
                    // Registered as a marker type so DI triggers the wiring.
                    services.add_singleton_factory::<CacheEventSubscriberHandle, _>(
                        |resolver| {
                            let block_cache =
                                ferrous_di::Resolver::get_required::<QueryableCache<Block>>(
                                    resolver,
                                );
                            let event_bus =
                                ferrous_di::Resolver::get_required::<TursoEventBus>(resolver);
                            let event_bus_arc: Arc<dyn EventBus> = event_bus.clone();

                            tokio::task::block_in_place(|| {
                                tokio::runtime::Handle::current().block_on(async {
                                    let subscriber = CacheEventSubscriber::new(block_cache);
                                    if let Err(e) = subscriber.start(event_bus_arc).await {
                                        tracing::error!(
                                            "[FrontendSession] Failed to start CacheEventSubscriber: {}",
                                            e
                                        );
                                    }
                                });
                            });

                            CacheEventSubscriberHandle
                        },
                    );
                }

                // Register Loro module if enabled (must be before OrgMode so OrgMode can detect it)
                let resolved_loro_dir = if loro_enabled {
                    let loro_dir = loro_storage_dir
                        .clone()
                        .or_else(|| orgmode_root.as_ref().map(|r| r.join(".loro")))
                        .unwrap_or_else(|| db_path.parent().unwrap_or(&db_path).join(".loro"));
                    services.add_singleton(LoroConfig::new(loro_dir.clone()));
                    services
                        .add_module_mut(LoroModule)
                        .map_err(|e| anyhow::anyhow!("Failed to register LoroModule: {}", e))?;
                    Some(loro_dir)
                } else {
                    None
                };

                if let Some(root) = orgmode_root {
                    if let Some(loro_dir) = resolved_loro_dir {
                        use holon_orgmode::di::{OrgModeConfig, OrgModeModule};
                        services.add_singleton(OrgModeConfig::with_loro_storage(root, loro_dir));
                        services.add_module_mut(OrgModeModule).map_err(|e| {
                            anyhow::anyhow!("Failed to register OrgModeModule: {}", e)
                        })?;
                    } else {
                        services.add_orgmode(root)?;
                    }
                }
                if let Some(key) = todoist_key {
                    services.add_todoist(key)?;
                } else if todoist_fake {
                    #[cfg(not(target_arch = "wasm32"))]
                    services.add_todoist_fake()?;
                }
                Ok(())
            },
            |provider| {
                // Trigger lazy DI resolution of background wiring handles
                let _ = DiResolver::get::<CacheEventSubscriberHandle>(provider);
                let _ = DiResolver::get::<holon::sync::LoroEventAdapterHandle>(provider);

                let error_tracker: PublishErrorTracker =
                    DiResolver::get::<PublishErrorTracker>(provider)
                        .map(|t| (*t).clone())
                        .unwrap_or_else(|_| PublishErrorTracker::new());
                // FileWatcherReadySignal is only registered when OrgMode is enabled
                let ready_signal: Option<FileWatcherReadySignal> =
                    DiResolver::get::<FileWatcherReadySignal>(provider)
                        .ok()
                        .map(|s| (*s).clone());

                // Resolve DbHandle for transition_to_ready()
                let db_handle_provider: Option<std::sync::Arc<dyn DbHandleProvider>> =
                    DiResolver::get_trait::<dyn DbHandleProvider>(provider).ok();

                // Resolve caller's extra services
                let extras = extra_resolve(provider);

                (
                    error_tracker,
                    ready_signal,
                    db_handle_provider,
                    extras,
                )
            },
        )
        .await?;

        let (error_tracker, ready_signal, db_handle_provider, extras) = resolved;

        // CRITICAL: Signal that DDL phase is complete AFTER BackendEngine is fully resolved.
        // This ensures ALL DDL (including OperationLogStore, NavigationProvider, etc.)
        // is complete before OrgMode background tasks start publishing events.
        // The DatabaseActor serializes all database operations, eliminating race conditions.
        if let Some(provider) = db_handle_provider {
            let handle = provider.handle();
            if let Err(e) = handle.transition_to_ready().await {
                tracing::warn!(
                    "[FrontendSession] Failed to transition actor to ready: {}",
                    e
                );
            }
        }

        // CRITICAL: Wait for readiness if configured and orgmode is enabled
        if config.wait_for_ready {
            if let Some(ref signal) = ready_signal {
                signal.wait_ready().await;
                if config.settle_delay_ms > 0 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(config.settle_delay_ms))
                        .await;
                }
            }
        }

        Ok(Self {
            engine,
            error_tracker,
            ready_signal,
            extras,
        })
    }

    /// Get the extra services resolved from DI
    pub fn extras(&self) -> &T {
        &self.extras
    }

    /// Get the backend engine
    pub fn engine(&self) -> &Arc<BackendEngine> {
        &self.engine
    }

    /// Check if there were any startup errors (DDL/sync races)
    pub fn has_startup_errors(&self) -> bool {
        self.error_tracker.has_errors()
    }

    /// Get the number of startup errors
    pub fn startup_error_count(&self) -> usize {
        self.error_tracker.errors()
    }

    /// Get the error tracker for detailed monitoring
    pub fn error_tracker(&self) -> &PublishErrorTracker {
        &self.error_tracker
    }

    /// Check if the file watcher is ready (useful for tests)
    pub fn is_ready(&self) -> bool {
        self.ready_signal.as_ref().map_or(true, |s| s.is_ready())
    }

    // =========================================================================
    // Query Methods - These can only be called after initialization completes
    // =========================================================================

    /// Get the initial widget for the application root
    ///
    /// This is the primary entry point for the backend-driven UI.
    /// Because this method is on `FrontendSession`, it can only be called
    /// after `FrontendSession::new()` completes, which guarantees all schema
    /// initialization (including `blocks_with_paths` materialized view) is done.
    ///
    /// # Returns
    /// - `Ok((WidgetSpec, stream))` if index.org has a valid root layout block with PRQL
    /// - `Err` if index.org is missing or malformed
    ///
    /// # Example
    /// ```rust,ignore
    /// let session = FrontendSession::new(config).await?;
    /// let (widget_spec, stream) = session.initial_widget().await?;
    /// // widget_spec contains the root layout specification
    /// // stream provides CDC updates
    /// ```
    pub async fn initial_widget(&self) -> Result<(WidgetSpec, RowChangeStream)> {
        self.engine.initial_widget().await
    }

    /// Execute a PRQL query and set up CDC streaming
    ///
    /// This is the main query method for reactive UI updates.
    /// Returns a `WidgetSpec` with initial data and a stream for CDC updates.
    ///
    /// # Arguments
    /// * `prql` - The PRQL query to execute
    /// * `params` - Query parameters
    /// * `context` - Optional query context (for `from children` resolution)
    pub async fn query_and_watch(
        &self,
        prql: String,
        params: HashMap<String, Value>,
        context: Option<QueryContext>,
    ) -> Result<(WidgetSpec, RowChangeStream)> {
        self.engine.query_and_watch(prql, params, context).await
    }

    /// Execute an operation on an entity
    ///
    /// Operations mutate the database. UI updates happen via CDC streams.
    /// This follows unidirectional data flow: Action → Model → View
    ///
    /// # Arguments
    /// * `entity_name` - The entity to operate on (e.g., "blocks", "documents")
    /// * `op_name` - The operation name (e.g., "create", "delete", "set_field")
    /// * `params` - Operation parameters
    pub async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: HashMap<String, Value>,
    ) -> Result<()> {
        self.engine
            .execute_operation(entity_name, op_name, params)
            .await
    }

    /// Get available operations for an entity
    ///
    /// Returns a list of operation descriptors available for the given entity_name.
    /// Use "*" as entity_name to get wildcard operations.
    pub async fn available_operations(&self, entity_name: &str) -> Vec<OperationDescriptor> {
        self.engine.available_operations(entity_name).await
    }

    /// Check if an operation is available for an entity
    pub async fn has_operation(&self, entity_name: &str, op_name: &str) -> bool {
        self.engine.has_operation(entity_name, op_name).await
    }

    /// Undo the last operation
    ///
    /// Returns true if an operation was undone, false if the undo stack is empty.
    pub async fn undo(&self) -> Result<bool> {
        self.engine.undo().await
    }

    /// Redo the last undone operation
    ///
    /// Returns true if an operation was redone, false if the redo stack is empty.
    pub async fn redo(&self) -> Result<bool> {
        self.engine.redo().await
    }

    /// Check if undo is available
    pub async fn can_undo(&self) -> bool {
        self.engine.can_undo().await
    }

    /// Check if redo is available
    pub async fn can_redo(&self) -> bool {
        self.engine.can_redo().await
    }

    /// Look up a block's path from the blocks_with_paths materialized view
    ///
    /// Returns the hierarchical path for a block (e.g., "/parent/block_id").
    /// This path is used for descendants queries via path prefix matching.
    pub async fn lookup_block_path(&self, block_id: &str) -> Result<String> {
        self.engine.lookup_block_path(block_id).await
    }

    /// Execute a raw SQL query
    ///
    /// This is a lower-level method for direct SQL access.
    /// Prefer `query_and_watch` for reactive queries.
    pub async fn execute_query(
        &self,
        sql: String,
        params: HashMap<String, Value>,
        context: Option<QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        self.engine.execute_query(sql, params, context).await
    }
}
