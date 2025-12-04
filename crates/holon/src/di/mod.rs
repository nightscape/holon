//! Dependency Injection module for holon
//!
//! This module provides service registration and resolution using ferrous-di.
//! It centralizes dependency wiring and makes it easier to test and configure services.

#[cfg(any(test, feature = "test-helpers"))]
pub mod test_helpers;

use anyhow::Result;
use ferrous_di::{Lifetime, Resolver, ServiceCollection, ServiceCollectionModuleExt};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::api::backend_engine::BackendEngine;
use crate::api::operation_dispatcher::{OperationDispatcher, OperationModule};
use crate::core::datasource::{OperationObserver, SyncTokenStore};
use crate::core::operation_log::{OperationLogObserver, OperationLogStore};
use crate::core::transform::{AstTransformer, TransformPipeline};
use crate::core::transform::{ColumnPreservationTransformer, JsonAggregationTransformer};
use crate::storage::sync_token_store::DatabaseSyncTokenStore;
use crate::storage::turso::TursoBackend;

/// Configuration for database path
#[derive(Clone, Debug)]
pub struct DatabasePathConfig {
    pub path: PathBuf,
}

impl DatabasePathConfig {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

/// Shared setup function for creating BackendEngine with DI
///
/// This function sets up the DI container and returns a BackendEngine.
/// It can be used by both TUI and Flutter frontends.
///
/// # Arguments
/// * `db_path` - Path to the database file
/// * `setup_fn` - Optional closure to register additional modules/services before core services
///
/// # Example
/// ```rust,no_run
/// use holon::di;
///
/// let engine = di::create_backend_engine(
///     "/path/to/db".into(),
///     |services| {
///         // Register Todoist module if needed
///         services.add_module_mut(todoist_module)?;
///         Ok(())
///     }
/// ).await?;
/// ```
pub async fn create_backend_engine<F>(db_path: PathBuf, setup_fn: F) -> Result<Arc<BackendEngine>>
where
    F: FnOnce(&mut ServiceCollection) -> Result<()>,
{
    let mut services = ServiceCollection::new();

    // Register core services FIRST (this includes SyncTokenStore and OperationModule)
    // This ensures dependencies are available when custom modules register their factories
    register_core_services(&mut services, db_path)?;

    // Then allow caller to register custom services/modules (e.g., Todoist)
    // These modules can now safely depend on core services like SyncTokenStore
    setup_fn(&mut services)?;

    // Build the DI container and resolve BackendEngine
    let provider = services.build();
    let engine = Resolver::get_required::<BackendEngine>(&provider);

    Ok(engine)
}

/// Register core services in the DI container
///
/// This registers:
/// - `DatabasePathConfig` (singleton) - Database path configuration
/// - `RwLock<TursoBackend>` (singleton) - Database backend (wrapped in RwLock for BackendEngine)
/// - `OperationDispatcher` (singleton) - Operation dispatcher
/// - `BackendEngine` (singleton) - Render engine (no longer wrapped in RwLock)
///
/// Note: Services are registered as Arc-wrapped types to avoid Clone requirements.
/// The async initialization is handled by blocking in sync factories.
///
/// # Arguments
/// * `services` - Service collection to register services in
/// * `db_path` - Path to the database file
pub fn register_core_services(services: &mut ServiceCollection, db_path: PathBuf) -> Result<()> {
    // Register database path configuration
    services.add_singleton(DatabasePathConfig::new(db_path.clone()));

    // Register Arc<RwLock<TursoBackend>> as singleton factory with blocking async initialization
    // This matches what BackendEngine::from_dependencies expects
    let db_path_clone = db_path.clone();
    services.add_singleton_factory::<RwLock<TursoBackend>, _>(move |_resolver| {
        #[cfg(not(target_arch = "wasm32"))]
        {
            // Create backend in a new thread with its own runtime to avoid "runtime within runtime" error
            let db_path_for_thread = db_path_clone.clone();
            let backend = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
                rt.block_on(TursoBackend::new(db_path_for_thread))
                    .expect("Failed to create TursoBackend")
            })
            .join()
            .expect("Thread panicked while creating TursoBackend");
            RwLock::new(backend)
        }
        #[cfg(target_arch = "wasm32")]
        {
            // On WASM, we can't spawn threads, so we need to use the current runtime
            // This assumes we're already in an async context with a runtime
            let rt = tokio::runtime::Handle::current();
            let backend = rt
                .block_on(TursoBackend::new(db_path_clone.clone()))
                .expect("Failed to create TursoBackend");
            RwLock::new(backend)
        }
    });

    // Register DatabaseSyncTokenStore as SyncTokenStore implementation
    // Use add_trait_factory to register as trait object
    services.add_trait_factory::<dyn SyncTokenStore, _>(Lifetime::Singleton, move |resolver| {
        let backend_arc = resolver.get_required::<RwLock<TursoBackend>>();
        let backend = backend_arc.clone();

        #[cfg(not(target_arch = "wasm32"))]
        {
            // Initialize sync_states table in a new thread with its own runtime
            let backend_for_init = backend.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
                rt.block_on(async {
                    let token_store = DatabaseSyncTokenStore::new(backend_for_init);
                    token_store
                        .initialize_sync_state_table()
                        .await
                        .expect("Failed to initialize sync_states table");
                });
            })
            .join()
            .expect("Thread panicked while initializing sync_states table");
        }
        #[cfg(target_arch = "wasm32")]
        {
            // On WASM, use current runtime
            let rt = tokio::runtime::Handle::current();
            let backend_for_init = backend.clone();
            rt.block_on(async {
                let token_store = DatabaseSyncTokenStore::new(backend_for_init);
                token_store
                    .initialize_sync_state_table()
                    .await
                    .expect("Failed to initialize sync_states table");
            });
        }

        Arc::new(DatabaseSyncTokenStore::new(backend)) as Arc<dyn SyncTokenStore>
    });

    // Register OperationLogStore for persistent undo/redo
    services.add_singleton_factory::<OperationLogStore, _>(move |resolver| {
        let backend_arc = resolver.get_required::<RwLock<TursoBackend>>();
        let backend = backend_arc.clone();

        #[cfg(not(target_arch = "wasm32"))]
        {
            // Initialize operations table in a new thread with its own runtime
            let backend_for_init = backend.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
                rt.block_on(async {
                    let store = OperationLogStore::new(backend_for_init);
                    store
                        .initialize_schema()
                        .await
                        .expect("Failed to initialize operations table");
                });
            })
            .join()
            .expect("Thread panicked while initializing operations table");
        }
        #[cfg(target_arch = "wasm32")]
        {
            // On WASM, use current runtime
            let rt = tokio::runtime::Handle::current();
            let backend_for_init = backend.clone();
            rt.block_on(async {
                let store = OperationLogStore::new(backend_for_init);
                store
                    .initialize_schema()
                    .await
                    .expect("Failed to initialize operations table");
            });
        }

        OperationLogStore::new(backend)
    });

    // Register OperationLogObserver as OperationObserver for persistent undo/redo
    services.add_trait_factory::<dyn OperationObserver, _>(Lifetime::Singleton, move |resolver| {
        let store = resolver.get_required::<OperationLogStore>();
        Arc::new(OperationLogObserver::new(store)) as Arc<dyn OperationObserver>
    });

    // Register OperationModule to collect providers from DI and create OperationDispatcher
    services
        .add_module_mut(OperationModule)
        .map_err(|e| anyhow::anyhow!("Failed to register OperationModule: {}", e))?;

    // Register AST transformers
    // Additional transformers can be registered by modules via add_trait_factory

    // ColumnPreservationTransformer - converts select to this.* for UNION queries (PL phase)
    // This must run BEFORE JsonAggregationTransformer to ensure all columns are preserved
    services.add_trait_factory::<dyn AstTransformer, _>(Lifetime::Singleton, |_resolver| {
        Arc::new(ColumnPreservationTransformer) as Arc<dyn AstTransformer>
    });

    // JsonAggregationTransformer - automatically injects json_object for UNION queries (RQ phase)
    services.add_trait_factory::<dyn AstTransformer, _>(Lifetime::Singleton, |_resolver| {
        Arc::new(JsonAggregationTransformer) as Arc<dyn AstTransformer>
    });

    // Register TransformPipeline that collects all AstTransformer implementations
    // The pipeline will sort transformers by phase and priority
    services.add_singleton_factory::<TransformPipeline, _>(|resolver| {
        // Collect all registered AstTransformer trait objects
        // ferrous-di's get_all_trait returns Result<Vec<Arc<dyn Trait>>>
        let transformers = resolver
            .get_all_trait::<dyn AstTransformer>()
            .unwrap_or_else(|_| vec![]);
        TransformPipeline::new(transformers)
    });

    // Register BackendEngine as singleton factory with blocking async initialization
    // BackendEngine no longer needs RwLock wrapper since it uses interior mutability
    services.add_singleton_factory::<BackendEngine, _>(move |resolver| {
        // ferrous-di wraps services in Arc, so we get Arc<Arc<T>> when registering Arc<T>
        // Extract the inner Arc<T> by dereferencing the outer Arc
        let backend_arc = resolver.get_required::<RwLock<TursoBackend>>();
        let backend = backend_arc.clone(); // Dereference to get Arc<RwLock<TursoBackend>>

        // Get dispatcher - ferrous-di wraps singletons in Arc, so we get Arc<OperationDispatcher>
        let dispatcher = resolver.get_required::<OperationDispatcher>();

        // Get transform pipeline
        let transform_pipeline = resolver.get_required::<TransformPipeline>();

        let db_path_config: Arc<DatabasePathConfig> = resolver.get_required::<DatabasePathConfig>();
        let db_path_for_thread = db_path_config.path.clone();

        #[cfg(not(target_arch = "wasm32"))]
        {
            // Create engine in a new thread with its own runtime to avoid "runtime within runtime" error
            let backend_clone = backend.clone();
            let dispatcher_clone = dispatcher.clone();
            let pipeline_clone = transform_pipeline.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

                rt.block_on(async {
                    let engine = BackendEngine::from_dependencies(
                        backend_clone,
                        dispatcher_clone,
                        pipeline_clone,
                    )
                    .expect("Failed to create BackendEngine");

                    // Initialize database schema and sample data if needed
                    engine
                        .initialize_database_if_needed(&db_path_for_thread)
                        .await
                        .expect("Failed to initialize database");

                    engine
                })
            })
            .join()
            .expect("Thread panicked while creating BackendEngine")
        }
        #[cfg(target_arch = "wasm32")]
        {
            // On WASM, we can't spawn threads, so we need to use the current runtime
            // This assumes we're already in an async context with a runtime
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let engine =
                    BackendEngine::from_dependencies(backend, dispatcher, transform_pipeline)
                        .expect("Failed to create BackendEngine");

                // Initialize database schema and sample data if needed
                engine
                    .initialize_database_if_needed(&db_path_for_thread)
                    .await
                    .expect("Failed to initialize database");

                engine
            })
        }
    });

    Ok(())
}
