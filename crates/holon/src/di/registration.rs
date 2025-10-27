//! DI service registration functions.

use anyhow::Result;
use ferrous_di::{Lifetime, Resolver, ServiceCollection, ServiceCollectionModuleExt};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::api::backend_engine::BackendEngine;
use crate::api::operation_dispatcher::{OperationDispatcher, OperationModule};
use crate::core::datasource::{OperationObserver, OperationProvider, SyncTokenStore};
use crate::core::operation_log::{OperationLogObserver, OperationLogStore};
use crate::core::transform::{AstTransformer, JsonAggregationTransformer, TransformPipeline};
use crate::navigation::NavigationProvider;
use crate::storage::sync_token_store::DatabaseSyncTokenStore;
use crate::storage::turso::{DbHandle, TursoBackend};

use super::runtime::run_async_in_sync_factory;
use super::{
    DatabasePathConfig, DbHandleProvider, DbHandleProviderImpl, TursoBackendProvider,
    TursoBackendProviderImpl,
};

use super::lifecycle::preload_startup_views;

/// Register core services in the DI container.
///
/// This registers:
/// - `DatabasePathConfig` (singleton) - Database path configuration
/// - `RwLock<TursoBackend>` (singleton) - Database backend
/// - `OperationDispatcher` (singleton) - Operation dispatcher
/// - `BackendEngine` (singleton) - Render engine
pub fn register_core_services(services: &mut ServiceCollection, db_path: PathBuf) -> Result<()> {
    eprintln!(
        "[DI] register_core_services called with db_path: {:?}",
        db_path
    );

    services.add_singleton(DatabasePathConfig::new(db_path.clone()));
    eprintln!("[DI] Registered DatabasePathConfig");

    eprintln!("[DI] Registering RwLock<TursoBackend> factory");
    let db_path_clone = db_path.clone();
    services.add_singleton_factory::<RwLock<TursoBackend>, _>(move |_resolver| {
        eprintln!("[DI] RwLock<TursoBackend> factory called - about to spawn thread");
        #[cfg(not(target_arch = "wasm32"))]
        {
            let db_path_for_thread = db_path_clone.clone();
            eprintln!("[DI] Spawning thread to create TursoBackend");
            let backend = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
                rt.block_on(async {
                    let db = TursoBackend::open_database(&db_path_for_thread)
                        .expect("Failed to open database");
                    let (cdc_tx, _) = tokio::sync::broadcast::channel(1024);
                    let (backend, _db_handle) =
                        TursoBackend::new(db, cdc_tx).expect("Failed to create TursoBackend");
                    backend
                })
            })
            .join()
            .expect("Thread panicked while creating TursoBackend");
            eprintln!("[DI] TursoBackend created successfully, wrapping in RwLock");
            RwLock::new(backend)
        }
        #[cfg(target_arch = "wasm32")]
        {
            let rt = tokio::runtime::Handle::current();
            let backend = rt.block_on(async {
                let db =
                    TursoBackend::open_database(&db_path_clone).expect("Failed to open database");
                let (cdc_tx, _) = tokio::sync::broadcast::channel(1024);
                let (backend, _db_handle) =
                    TursoBackend::new(db, cdc_tx).expect("Failed to create TursoBackend");
                backend
            });
            RwLock::new(backend)
        }
    });

    services.add_trait_factory::<dyn TursoBackendProvider, _>(Lifetime::Singleton, |resolver| {
        let backend = resolver.get_required::<RwLock<TursoBackend>>();
        Arc::new(TursoBackendProviderImpl {
            backend: backend.clone(),
        }) as Arc<dyn TursoBackendProvider>
    });

    services.add_trait_factory::<dyn SyncTokenStore, _>(Lifetime::Singleton, move |resolver| {
        let backend_arc = resolver.get_required::<RwLock<TursoBackend>>();
        let backend = backend_arc.clone();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let token_store = DatabaseSyncTokenStore::new(backend.clone());
                token_store
                    .initialize_sync_state_table()
                    .await
                    .expect("Failed to initialize sync_states table");
            });
        });

        Arc::new(DatabaseSyncTokenStore::new(backend)) as Arc<dyn SyncTokenStore>
    });

    services.add_singleton_factory::<OperationLogStore, _>(move |resolver| {
        let backend_arc = resolver.get_required::<RwLock<TursoBackend>>();
        let backend = backend_arc.clone();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let store = OperationLogStore::new(backend.clone());
                store
                    .initialize_schema()
                    .await
                    .expect("Failed to initialize operations table");
            });
        });

        OperationLogStore::new(backend)
    });

    services.add_trait_factory::<dyn OperationObserver, _>(Lifetime::Singleton, move |resolver| {
        let store = resolver.get_required::<OperationLogStore>();
        Arc::new(OperationLogObserver::new(store)) as Arc<dyn OperationObserver>
    });

    services.add_singleton_factory::<NavigationProvider, _>(move |resolver| {
        let backend = resolver.get_required::<RwLock<TursoBackend>>();
        NavigationProvider::new(backend)
    });

    services.add_trait_factory::<dyn OperationProvider, _>(Lifetime::Singleton, |resolver| {
        let nav_provider = resolver.get_required::<NavigationProvider>();
        nav_provider as Arc<dyn OperationProvider>
    });

    services
        .add_module_mut(OperationModule)
        .map_err(|e| anyhow::anyhow!("Failed to register OperationModule: {}", e))?;

    services.add_trait_factory::<dyn AstTransformer, _>(Lifetime::Singleton, |_resolver| {
        Arc::new(JsonAggregationTransformer) as Arc<dyn AstTransformer>
    });

    services.add_singleton_factory::<TransformPipeline, _>(|resolver| {
        let transformers = resolver
            .get_all_trait::<dyn AstTransformer>()
            .unwrap_or_else(|_| vec![]);
        TransformPipeline::new(transformers)
    });

    services.add_singleton_factory::<BackendEngine, _>(move |resolver| {
        eprintln!("[DI] BackendEngine factory called");
        eprintln!("[DI] BackendEngine: resolving RwLock<TursoBackend>...");
        let backend_arc = resolver.get_required::<RwLock<TursoBackend>>();
        eprintln!("[DI] BackendEngine: RwLock<TursoBackend> resolved");
        let backend = backend_arc.clone();

        eprintln!("[DI] BackendEngine: resolving OperationDispatcher...");
        let dispatcher = resolver.get_required::<OperationDispatcher>();
        eprintln!("[DI] BackendEngine: OperationDispatcher resolved");

        eprintln!("[DI] BackendEngine: resolving TransformPipeline...");
        let transform_pipeline = resolver.get_required::<TransformPipeline>();
        eprintln!("[DI] BackendEngine: TransformPipeline resolved");

        let db_path_config: Arc<DatabasePathConfig> = resolver.get_required::<DatabasePathConfig>();
        let db_path_for_thread = db_path_config.path.clone();

        #[cfg(not(target_arch = "wasm32"))]
        {
            run_async_in_sync_factory(async move {
                let backend_guard = backend.read().await;
                let db_handle = backend_guard.handle();
                let cdc_broadcast = backend_guard.cdc_broadcast().clone();
                drop(backend_guard);

                let engine = BackendEngine::new(
                    db_handle,
                    cdc_broadcast,
                    dispatcher,
                    transform_pipeline,
                    backend.clone(),
                );

                engine
                    .initialize_database_if_needed(&db_path_for_thread)
                    .await
                    .expect("Failed to initialize database");

                preload_startup_views(&engine, None)
                    .await
                    .expect("Failed to preload startup views");

                engine
            })
        }
        #[cfg(target_arch = "wasm32")]
        {
            let backend_clone = backend.clone();
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let backend_guard = backend_clone.read().await;
                let db_handle = backend_guard.handle().clone();
                let cdc_broadcast = backend_guard.cdc_broadcast().clone();
                drop(backend_guard);

                let engine = BackendEngine::new(
                    db_handle,
                    cdc_broadcast,
                    dispatcher,
                    transform_pipeline,
                    backend_clone.clone(),
                );

                engine
                    .initialize_database_if_needed(&db_path_for_thread)
                    .await
                    .expect("Failed to initialize database");

                preload_startup_views(&engine, None)
                    .await
                    .expect("Failed to preload startup views");

                engine
            })
        }
    });

    Ok(())
}

/// Register core services with a pre-created TursoBackend and DbHandle.
///
/// This variant takes a pre-created backend and DbHandle instead of creating them in factories.
/// Use this to avoid TypeId mismatch issues when cross-crate code needs the backend.
pub fn register_core_services_with_backend(
    services: &mut ServiceCollection,
    db_path: PathBuf,
    backend: Arc<RwLock<TursoBackend>>,
    db_handle: DbHandle,
) -> Result<()> {
    eprintln!(
        "[DI] register_core_services_with_backend called with db_path: {:?}",
        db_path
    );

    services.add_singleton(DatabasePathConfig::new(db_path.clone()));
    eprintln!("[DI] Registered DatabasePathConfig");

    let backend_for_provider = backend.clone();
    services.add_trait_factory::<dyn TursoBackendProvider, _>(
        Lifetime::Singleton,
        move |_resolver| {
            Arc::new(TursoBackendProviderImpl {
                backend: backend_for_provider.clone(),
            }) as Arc<dyn TursoBackendProvider>
        },
    );

    {
        services.add_trait_factory::<dyn DbHandleProvider, _>(
            Lifetime::Singleton,
            move |_resolver| {
                eprintln!("[DI] Registering pre-created DbHandle");
                Arc::new(DbHandleProviderImpl {
                    handle: db_handle.clone(),
                }) as Arc<dyn DbHandleProvider>
            },
        );
    }

    let backend_for_sync = backend.clone();
    services.add_trait_factory::<dyn SyncTokenStore, _>(Lifetime::Singleton, move |_resolver| {
        let backend = backend_for_sync.clone();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let token_store = DatabaseSyncTokenStore::new(backend.clone());
                token_store
                    .initialize_sync_state_table()
                    .await
                    .expect("Failed to initialize sync_states table");
            });
        });

        Arc::new(DatabaseSyncTokenStore::new(backend)) as Arc<dyn SyncTokenStore>
    });

    let backend_for_log = backend.clone();
    services.add_singleton_factory::<OperationLogStore, _>(move |_resolver| {
        let backend = backend_for_log.clone();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let store = OperationLogStore::new(backend.clone());
                store
                    .initialize_schema()
                    .await
                    .expect("Failed to initialize operations table");
            });
        });

        OperationLogStore::new(backend)
    });

    services.add_trait_factory::<dyn OperationObserver, _>(Lifetime::Singleton, move |resolver| {
        let store = resolver.get_required::<OperationLogStore>();
        Arc::new(OperationLogObserver::new(store)) as Arc<dyn OperationObserver>
    });

    let backend_for_nav = backend.clone();
    services.add_singleton_factory::<NavigationProvider, _>(move |_resolver| {
        NavigationProvider::new(backend_for_nav.clone())
    });

    services.add_trait_factory::<dyn OperationProvider, _>(Lifetime::Singleton, |resolver| {
        let nav_provider = resolver.get_required::<NavigationProvider>();
        nav_provider as Arc<dyn OperationProvider>
    });

    services
        .add_module_mut(OperationModule)
        .map_err(|e| anyhow::anyhow!("Failed to register OperationModule: {}", e))?;

    services.add_trait_factory::<dyn AstTransformer, _>(Lifetime::Singleton, |_resolver| {
        Arc::new(JsonAggregationTransformer) as Arc<dyn AstTransformer>
    });

    services.add_singleton_factory::<TransformPipeline, _>(|resolver| {
        let transformers = resolver
            .get_all_trait::<dyn AstTransformer>()
            .unwrap_or_else(|_| vec![]);
        TransformPipeline::new(transformers)
    });

    let backend_for_engine = backend.clone();
    services.add_singleton_factory::<BackendEngine, _>(move |resolver| {
        eprintln!("[DI] BackendEngine factory called (with pre-created backend)");
        let backend = backend_for_engine.clone();

        let dispatcher = resolver.get_required::<OperationDispatcher>();
        let transform_pipeline = resolver.get_required::<TransformPipeline>();
        let db_path_config: Arc<DatabasePathConfig> = resolver.get_required::<DatabasePathConfig>();
        let db_path_for_thread = db_path_config.path.clone();

        #[cfg(not(target_arch = "wasm32"))]
        {
            run_async_in_sync_factory(async move {
                let backend_guard = backend.read().await;
                let db_handle = backend_guard.handle();
                let cdc_broadcast = backend_guard.cdc_broadcast().clone();
                drop(backend_guard);

                let engine = BackendEngine::new(
                    db_handle,
                    cdc_broadcast,
                    dispatcher,
                    transform_pipeline,
                    backend.clone(),
                );

                engine
                    .initialize_database_if_needed(&db_path_for_thread)
                    .await
                    .expect("Failed to initialize database");

                preload_startup_views(&engine, None)
                    .await
                    .expect("Failed to preload startup views");

                engine
            })
        }
        #[cfg(target_arch = "wasm32")]
        {
            let backend_clone = backend.clone();
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let backend_guard = backend_clone.read().await;
                let db_handle = backend_guard.handle().clone();
                let cdc_broadcast = backend_guard.cdc_broadcast().clone();
                drop(backend_guard);

                let engine = BackendEngine::new(
                    db_handle,
                    cdc_broadcast,
                    dispatcher,
                    transform_pipeline,
                    backend_clone.clone(),
                );

                engine
                    .initialize_database_if_needed(&db_path_for_thread)
                    .await
                    .expect("Failed to initialize database");

                preload_startup_views(&engine, None)
                    .await
                    .expect("Failed to preload startup views");

                engine
            })
        }
    });

    Ok(())
}
