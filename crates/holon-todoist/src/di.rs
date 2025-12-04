//! Dependency Injection module for Todoist integration
//!
//! This module provides DI registration for Todoist-specific services using ferrous-di.

use ferrous_di::Resolver;
use ferrous_di::{DiResult, Lifetime, ServiceCollection, ServiceModule};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::models::{TodoistProject, TodoistTask};
use crate::todoist_datasource::{TodoistProjectDataSource, TodoistTaskDataSource};
use crate::TodoistClient;
use crate::TodoistSyncProvider;
use holon::core::datasource::{OperationProvider, SyncTokenStore, SyncableProvider};
use holon::core::queryable_cache::QueryableCache;
use holon::storage::turso::TursoBackend;

/// Configuration for Todoist API key
#[derive(Clone, Debug)]
pub struct TodoistConfig {
    pub api_key: Option<String>,
}

impl TodoistConfig {
    pub fn new(api_key: Option<String>) -> Self {
        Self { api_key }
    }
}

/// ServiceModule for Todoist integration
///
/// Registers Todoist-specific services in the DI container:
/// - `TodoistConfig` - Configuration with API key
/// - `Arc<dyn SyncableProvider>` - The syncable provider (if API key is provided)
///
/// Note: Providers are registered as `Arc<dyn SyncableProvider>` (not wrapped in Mutex)
/// because `sync()` no longer requires `&mut self` - it takes and returns `StreamPosition`.
pub struct TodoistModule;

impl ServiceModule for TodoistModule {
    fn register_services(self, services: &mut ServiceCollection) -> DiResult<()> {
        use std::println;
        use tracing::info;

        println!("[TodoistModule] register_services called");
        info!("[TodoistModule] register_services called");

        // Register TodoistSyncProvider as a factory that reads TodoistConfig and SyncTokenStore from DI
        // This allows the API key to be passed via DI instead of environment variables
        // Note: This factory will only be called if TodoistConfig is registered.
        // If TodoistConfig is not registered, don't register TodoistModule.
        services.add_singleton_factory::<TodoistSyncProvider, _>(|resolver| {
            use ferrous_di::Resolver;
            use std::println;

            println!("[TodoistModule] TodoistSyncProvider factory called");

            // Get TodoistConfig from DI (required - should be registered before TodoistModule)
            let config = match resolver.get::<TodoistConfig>() {
                Ok(c) => {
                    println!("[TodoistModule] TodoistConfig found in DI");
                    c
                }
                Err(e) => {
                    let msg = format!("[TodoistModule] ERROR: TodoistConfig not found in DI! Make sure TodoistConfig is registered before TodoistModule. Error: {}", e);
                    println!("{}", msg);
                    eprintln!("{}", msg);
                    panic!("{}", msg);
                }
            };

            // Get SyncTokenStore from DI (required - should be registered in core services)
            // When using add_trait_factory, use get_trait() instead of get() for trait objects
            // get_trait returns Arc<dyn Trait> directly (not wrapped in another Arc)
            // Use custom error handling to avoid panic message that FRB tries to decode
            let token_store = resolver
                .get_trait::<dyn SyncTokenStore>()
                .unwrap_or_else(|e| {
                    let msg = "[TodoistModule] ERROR: SyncTokenStore not found in DI! Make sure it's registered in core services.";
                    println!("{} Error: {:?}", msg, e);
                    eprintln!("{} Error: {:?}", msg, e);
                    panic!("{}", msg);
                });

            if let Some(api_key) = &config.api_key {
                println!("[TodoistModule] API key found in TodoistConfig, setting up Todoist integration");
                info!("[TodoistModule] API key found in TodoistConfig, setting up Todoist integration");
                TodoistSyncProvider::new(TodoistClient::new(api_key), token_store)
            } else {
                // TodoistConfig registered but no API key - this is a configuration error
                let msg = "[TodoistModule] ERROR: TodoistConfig registered but no API key provided. Either provide an API key in TodoistConfig or don't register TodoistModule.";
                println!("{}", msg);
                eprintln!("{}", msg);
                panic!("{}", msg);
            }
        });

        // Register SyncableProvider trait implementation (for sync operations)
        // This factory will only succeed if TodoistConfig has an API key
        services.add_trait_factory::<dyn SyncableProvider, _>(Lifetime::Singleton, |resolver| {
            // ferrous-di wraps in Arc, so we get Arc<TodoistSyncProvider>
            let sync_provider = resolver.get_required::<TodoistSyncProvider>();
            // Clone and cast to trait object
            sync_provider.clone() as Arc<dyn SyncableProvider>
        });

        // Register OperationProvider trait implementation (for sync operation discovery)
        // TodoistSyncProvider implements OperationProvider to provide "todoist.sync" operation
        services.add_trait_factory::<dyn OperationProvider, _>(Lifetime::Singleton, |resolver| {
            let sync_provider = resolver.get_required::<TodoistSyncProvider>();
            sync_provider.clone() as Arc<dyn OperationProvider>
        });

        // Register QueryableCache as a factory that:
        // 1. Gets the backend from DI
        // 2. Gets the sync provider from DI
        // 3. Creates cache with datasource and backend
        services.add_singleton_factory::<QueryableCache<TodoistTaskDataSource, TodoistTask>, _>(|resolver| {
            use ferrous_di::Resolver;
            use std::println;

            println!("[TodoistModule] QueryableCache factory called");

            // Get backend from DI (same one used by BackendEngine)
            let backend = Resolver::get_required::<RwLock<TursoBackend>>(resolver);
            println!("[TodoistModule] Got backend from DI");

            // Get sync provider from DI
            let sync_provider = resolver.get_required::<TodoistSyncProvider>();
            println!("[TodoistModule] Got sync provider from DI");

            // Create cache in a blocking thread (since we're in a sync factory)
            let sync_provider_clone = sync_provider.clone();
            #[cfg(not(target_arch = "wasm32"))]
            let cache = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new()
                    .expect("Failed to create tokio runtime");
                rt.block_on(async {
                    println!("[TodoistModule] Creating TodoistTaskDataSource...");
                    // Create a new TodoistTaskDataSource from the sync provider
                    // (TodoistTaskDataSource doesn't implement Clone, so we create a new one)
                    let datasource = TodoistTaskDataSource::new(sync_provider_clone.clone());

                    println!("[TodoistModule] Creating QueryableCache with backend...");
                    // Create cache with datasource and backend
                    // This will call initialize_schema() which creates the todoist_tasks table
                    let cache = match QueryableCache::new_with_backend(
                        datasource,
                        backend.clone(),
                    ).await {
                        Ok(c) => {
                            println!("[TodoistModule] QueryableCache created successfully - todoist_tasks table should now exist");
                            c
                        }
                        Err(e) => {
                            let msg = format!("[TodoistModule] ERROR: Failed to create QueryableCache: {}", e);
                            println!("{}", msg);
                            eprintln!("{}", msg);
                            panic!("{}", msg);
                        }
                    };

                    use tracing::info;
                    info!("[TodoistModule] QueryableCache created (stream subscription will be set up in OperationProvider factory)");

                    cache
                })
            })
            .join()
            .expect("Thread panicked while creating QueryableCache");
            #[cfg(target_arch = "wasm32")]
            let cache = {
                // On WASM, we can't spawn threads, so we need to use the current runtime
                let rt = tokio::runtime::Handle::current();
                rt.block_on(async {
                    println!("[TodoistModule] Creating TodoistTaskDataSource...");
                    let datasource = TodoistTaskDataSource::new(sync_provider_clone.clone());
                    println!("[TodoistModule] Creating QueryableCache with backend...");
                    QueryableCache::new_with_backend(datasource, backend.clone())
                        .await
                        .expect("Failed to create QueryableCache")
                })
            };

            println!("[TodoistModule] QueryableCache<TodoistTask> factory completed successfully");
            cache
        });

        // Register QueryableCache for TodoistProject
        // This creates the todoist_projects table and enables project queries
        services.add_singleton_factory::<QueryableCache<TodoistProjectDataSource, TodoistProject>, _>(|resolver| {
            use ferrous_di::Resolver;
            use std::println;

            println!("[TodoistModule] QueryableCache<TodoistProject> factory called");

            // Get backend from DI (same one used by BackendEngine)
            let backend = Resolver::get_required::<RwLock<TursoBackend>>(resolver);
            println!("[TodoistModule] Got backend from DI for projects");

            // Get sync provider from DI
            let sync_provider = resolver.get_required::<TodoistSyncProvider>();
            println!("[TodoistModule] Got sync provider from DI for projects");

            // Create cache in a blocking thread (since we're in a sync factory)
            let sync_provider_clone = sync_provider.clone();
            #[cfg(not(target_arch = "wasm32"))]
            let cache = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new()
                    .expect("Failed to create tokio runtime");
                rt.block_on(async {
                    println!("[TodoistModule] Creating TodoistProjectDataSource...");
                    let datasource = TodoistProjectDataSource::new(sync_provider_clone.clone());

                    println!("[TodoistModule] Creating QueryableCache<TodoistProject> with backend...");
                    let cache = match QueryableCache::new_with_backend(
                        datasource,
                        backend.clone(),
                    ).await {
                        Ok(c) => {
                            println!("[TodoistModule] QueryableCache<TodoistProject> created successfully - todoist_projects table should now exist");
                            c
                        }
                        Err(e) => {
                            let msg = format!("[TodoistModule] ERROR: Failed to create QueryableCache<TodoistProject>: {}", e);
                            println!("{}", msg);
                            eprintln!("{}", msg);
                            panic!("{}", msg);
                        }
                    };

                    use tracing::info;
                    info!("[TodoistModule] QueryableCache<TodoistProject> created (stream subscription will be set up separately)");

                    cache
                })
            })
            .join()
            .expect("Thread panicked while creating QueryableCache<TodoistProject>");
            #[cfg(target_arch = "wasm32")]
            let cache = {
                let rt = tokio::runtime::Handle::current();
                rt.block_on(async {
                    println!("[TodoistModule] Creating TodoistProjectDataSource...");
                    let datasource = TodoistProjectDataSource::new(sync_provider_clone.clone());
                    println!("[TodoistModule] Creating QueryableCache<TodoistProject> with backend...");
                    QueryableCache::new_with_backend(datasource, backend.clone())
                        .await
                        .expect("Failed to create QueryableCache<TodoistProject>")
                })
            };

            println!("[TodoistModule] QueryableCache<TodoistProject> factory completed successfully");
            cache
        });

        // Register QueryableCache as OperationProvider so it can be discovered by OperationDispatcher
        // This enables operations like set_field to work on todoist_tasks
        // The cache will be created when OperationModule collects providers (during BackendEngine creation)
        //
        // IMPORTANT: This factory is called during BackendEngine creation, which happens in the
        // launcher's async context on the main runtime. This means we can safely subscribe the
        // cache to the stream here - tokio::spawn will use the main runtime, not a temporary one.
        services.add_trait_factory::<dyn OperationProvider, _>(Lifetime::Singleton, |resolver| {
            use tracing::info;

            // Get the task cache (creates it if needed)
            let task_cache =
                resolver.get_required::<QueryableCache<TodoistTaskDataSource, TodoistTask>>();

            // Get the project cache (creates it if needed) - this triggers todoist_projects table creation
            let project_cache =
                resolver.get_required::<QueryableCache<TodoistProjectDataSource, TodoistProject>>();

            // Get sync provider to subscribe to its streams
            let sync_provider = resolver.get_required::<TodoistSyncProvider>();

            // Subscribe task cache to sync provider's task stream with metadata
            // This enables atomic sync token + data updates in a single transaction
            // (prevents "database is locked" errors and ensures consistency)
            info!("[Todoist] Subscribing task cache to sync provider stream with metadata");
            let task_rx = sync_provider.subscribe_tasks();
            task_cache.ingest_stream_with_metadata(task_rx);
            info!(
                "[Todoist] Task stream subscription complete - atomic sync token updates enabled!"
            );

            // Subscribe project cache to sync provider's project stream with metadata
            info!("[Todoist] Subscribing project cache to sync provider stream with metadata");
            let project_rx = sync_provider.subscribe_projects();
            project_cache.ingest_stream_with_metadata(project_rx);
            info!("[Todoist] Project stream subscription complete!");

            task_cache
        });

        // Register TodoistProjectDataSource as a separate OperationProvider
        // This enables move_block operations on todoist_projects
        // We use the datasource directly (not the cache) since TodoistProject
        // doesn't implement OperationRegistry (projects don't have the same
        // complex operations that tasks do)
        services.add_singleton_factory::<TodoistProjectDataSource, _>(|resolver| {
            let sync_provider = resolver.get_required::<TodoistSyncProvider>();
            TodoistProjectDataSource::new(sync_provider.clone())
        });
        services.add_trait_factory::<dyn OperationProvider, _>(Lifetime::Singleton, |resolver| {
            resolver.get_required::<TodoistProjectDataSource>()
        });

        Ok(())
    }
}
