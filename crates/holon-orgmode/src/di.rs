//! Dependency Injection module for OrgMode integration
//!
//! This module provides DI registration for OrgMode-specific services using ferrous-di.

use ferrous_di::{DiResult, Lifetime, Resolver, ServiceCollection, ServiceModule};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use holon_filesystem::{directory::Directory, directory::DirectoryDataSource};

use crate::models::{OrgFile, OrgHeadline};
use crate::orgmode_datasource::{OrgFileDataSource, OrgHeadlineDataSource};
use crate::OrgModeSyncProvider;
use holon::core::datasource::{OperationProvider, SyncTokenStore, SyncableProvider};
use holon::core::queryable_cache::QueryableCache;
use holon::storage::turso::TursoBackend;

/// Configuration for OrgMode integration
#[derive(Clone, Debug)]
pub struct OrgModeConfig {
    /// Root directory containing .org files
    pub root_directory: PathBuf,
}

impl OrgModeConfig {
    pub fn new(root_directory: PathBuf) -> Self {
        Self { root_directory }
    }
}

/// ServiceModule for OrgMode integration
///
/// Registers OrgMode-specific services in the DI container:
/// - `OrgModeConfig` - Configuration with root directory
/// - `OrgModeSyncProvider` - Provider for syncing org files
/// - `QueryableCache` for directories, files, and headlines
pub struct OrgModeModule;

impl ServiceModule for OrgModeModule {
    fn register_services(self, services: &mut ServiceCollection) -> DiResult<()> {
        use std::println;
        use tracing::info;

        println!("[OrgModeModule] register_services called");
        info!("[OrgModeModule] register_services called");

        // Register OrgModeSyncProvider as a factory
        services.add_singleton_factory::<OrgModeSyncProvider, _>(|resolver| {
            println!("[OrgModeModule] OrgModeSyncProvider factory called");

            // Get OrgModeConfig from DI
            let config = match resolver.get::<OrgModeConfig>() {
                Ok(c) => {
                    println!("[OrgModeModule] OrgModeConfig found in DI");
                    c
                }
                Err(e) => {
                    let msg = format!(
                        "[OrgModeModule] ERROR: OrgModeConfig not found in DI! Error: {}",
                        e
                    );
                    println!("{}", msg);
                    panic!("{}", msg);
                }
            };

            // Get SyncTokenStore from DI
            let token_store = resolver
                .get_trait::<dyn SyncTokenStore>()
                .unwrap_or_else(|e| {
                    let msg = "[OrgModeModule] ERROR: SyncTokenStore not found in DI!";
                    println!("{} Error: {:?}", msg, e);
                    panic!("{}", msg);
                });

            let root_dir = config.root_directory.clone();
            println!(
                "[OrgModeModule] Creating OrgModeSyncProvider for: {}",
                root_dir.display()
            );
            println!("[OrgModeModule] Directory exists: {}", root_dir.exists());
            if root_dir.exists() {
                println!("[OrgModeModule] Directory is_dir: {}", root_dir.is_dir());
            }
            OrgModeSyncProvider::new(root_dir, token_store)
        });

        // Register SyncableProvider trait implementation
        services.add_trait_factory::<dyn SyncableProvider, _>(Lifetime::Singleton, |resolver| {
            let sync_provider = resolver.get_required::<OrgModeSyncProvider>();
            sync_provider.clone() as Arc<dyn SyncableProvider>
        });

        // Register OperationProvider for sync operations
        services.add_trait_factory::<dyn OperationProvider, _>(Lifetime::Singleton, |resolver| {
            let sync_provider = resolver.get_required::<OrgModeSyncProvider>();
            sync_provider.clone() as Arc<dyn OperationProvider>
        });

        // Register QueryableCache for Directory
        services.add_singleton_factory::<
            QueryableCache<DirectoryDataSource<OrgModeSyncProvider>, Directory>,
            _,
        >(|resolver| {
            println!("[OrgModeModule] QueryableCache<Directory> factory called");

            let backend = Resolver::get_required::<RwLock<TursoBackend>>(resolver);
            let sync_provider = resolver.get_required::<OrgModeSyncProvider>();

            let sync_provider_clone = sync_provider.clone();
            #[cfg(not(target_arch = "wasm32"))]
            let cache = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
                rt.block_on(async {
                    let datasource: DirectoryDataSource<OrgModeSyncProvider> = DirectoryDataSource::new(sync_provider_clone);
                    QueryableCache::new_with_backend(datasource, backend.clone())
                        .await
                        .expect("Failed to create QueryableCache<Directory>")
                })
            })
            .join()
            .expect("Thread panicked while creating QueryableCache<Directory>");

            #[cfg(target_arch = "wasm32")]
            let cache = {
                let rt = tokio::runtime::Handle::current();
                rt.block_on(async {
                    let datasource: DirectoryDataSource<OrgModeSyncProvider> = DirectoryDataSource::new(sync_provider_clone);
                    QueryableCache::new_with_backend(datasource, backend.clone())
                        .await
                        .expect("Failed to create QueryableCache<Directory>")
                })
            };

            println!("[OrgModeModule] QueryableCache<Directory> created");
            cache
        });

        // Register Directory cache as OperationProvider
        services.add_trait_factory::<dyn OperationProvider, _>(Lifetime::Singleton, |resolver| {
            resolver.get_required::<QueryableCache<DirectoryDataSource<OrgModeSyncProvider>, Directory>>()
        });

        // Register QueryableCache for OrgFile
        services.add_singleton_factory::<QueryableCache<OrgFileDataSource, OrgFile>, _>(
            |resolver| {
                println!("[OrgModeModule] QueryableCache<OrgFile> factory called");

                let backend = Resolver::get_required::<RwLock<TursoBackend>>(resolver);
                let sync_provider = resolver.get_required::<OrgModeSyncProvider>();

                let sync_provider_clone = sync_provider.clone();
                #[cfg(not(target_arch = "wasm32"))]
                let cache = std::thread::spawn(move || {
                    let rt =
                        tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
                    rt.block_on(async {
                        let datasource = OrgFileDataSource::new(sync_provider_clone.clone());
                        QueryableCache::new_with_backend(datasource, backend.clone())
                            .await
                            .expect("Failed to create QueryableCache<OrgFile>")
                    })
                })
                .join()
                .expect("Thread panicked while creating QueryableCache<OrgFile>");

                #[cfg(target_arch = "wasm32")]
                let cache = {
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(async {
                        let datasource = OrgFileDataSource::new(sync_provider_clone.clone());
                        QueryableCache::new_with_backend(datasource, backend.clone())
                            .await
                            .expect("Failed to create QueryableCache<OrgFile>")
                    })
                };

                println!("[OrgModeModule] QueryableCache<OrgFile> created");
                cache
            },
        );

        // Register OrgFile cache as OperationProvider
        services.add_trait_factory::<dyn OperationProvider, _>(Lifetime::Singleton, |resolver| {
            resolver.get_required::<QueryableCache<OrgFileDataSource, OrgFile>>()
        });

        // Register QueryableCache for OrgHeadline
        services.add_singleton_factory::<QueryableCache<OrgHeadlineDataSource, OrgHeadline>, _>(
            |resolver| {
                println!("[OrgModeModule] QueryableCache<OrgHeadline> factory called");

                let backend = Resolver::get_required::<RwLock<TursoBackend>>(resolver);
                let sync_provider = resolver.get_required::<OrgModeSyncProvider>();

                let sync_provider_clone = sync_provider.clone();
                #[cfg(not(target_arch = "wasm32"))]
                let cache = std::thread::spawn(move || {
                    let rt =
                        tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
                    rt.block_on(async {
                        let datasource = OrgHeadlineDataSource::new(sync_provider_clone.clone());
                        QueryableCache::new_with_backend(datasource, backend.clone())
                            .await
                            .expect("Failed to create QueryableCache<OrgHeadline>")
                    })
                })
                .join()
                .expect("Thread panicked while creating QueryableCache<OrgHeadline>");

                #[cfg(target_arch = "wasm32")]
                let cache = {
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(async {
                        let datasource = OrgHeadlineDataSource::new(sync_provider_clone.clone());
                        QueryableCache::new_with_backend(datasource, backend.clone())
                            .await
                            .expect("Failed to create QueryableCache<OrgHeadline>")
                    })
                };

                println!("[OrgModeModule] QueryableCache<OrgHeadline> created");
                cache
            },
        );

        // Register headline cache as OperationProvider and set up sequential stream processing
        services.add_trait_factory::<dyn OperationProvider, _>(Lifetime::Singleton, |resolver| {
            use tracing::{info, error};

            // Get caches
            let dir_cache = resolver
                .get_required::<QueryableCache<DirectoryDataSource<OrgModeSyncProvider>, Directory>>();
            let file_cache =
                resolver.get_required::<QueryableCache<OrgFileDataSource, OrgFile>>();
            let headline_cache =
                resolver.get_required::<QueryableCache<OrgHeadlineDataSource, OrgHeadline>>();

            // Get sync provider for stream subscriptions
            let sync_provider = resolver.get_required::<OrgModeSyncProvider>();

            // Subscribe to all three streams
            let mut dir_rx = sync_provider.subscribe_directories();
            let mut file_rx = sync_provider.subscribe_files();
            let mut headline_rx = sync_provider.subscribe_headlines();

            info!("[OrgMode] Setting up sequential stream processing (directories → files → headlines)");

            // Clone caches for the async task (they're Arc-wrapped, so this is cheap)
            let dir_cache_clone = dir_cache.clone();
            let file_cache_clone = file_cache.clone();
            let headline_cache_clone = headline_cache.clone();

            // Spawn a SINGLE task that processes all three streams SEQUENTIALLY
            // This ensures referential integrity: directories before files before headlines
            tokio::spawn(async move {
                let dir_cache = dir_cache_clone;
                let file_cache = file_cache_clone;
                let headline_cache = headline_cache_clone;
                loop {
                    // Wait for directory batch
                    match dir_rx.recv().await {
                        Ok(batch) => {
                            let changes = &batch.inner;
                            let sync_token = batch.metadata.sync_token.as_ref();
                            info!("[OrgMode] Processing {} directory changes", changes.len());
                            if let Err(e) = dir_cache.apply_batch(changes, sync_token).await {
                                error!("[OrgMode] Error applying directory batch: {}", e);
                                continue;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            info!("[OrgMode] Directory stream closed");
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            error!("[OrgMode] Directory stream lagged by {} messages", n);
                        }
                    }

                    // Wait for file batch
                    match file_rx.recv().await {
                        Ok(batch) => {
                            let changes = &batch.inner;
                            let sync_token = batch.metadata.sync_token.as_ref();
                            info!("[OrgMode] Processing {} file changes", changes.len());
                            if let Err(e) = file_cache.apply_batch(changes, sync_token).await {
                                error!("[OrgMode] Error applying file batch: {}", e);
                                continue;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            info!("[OrgMode] File stream closed");
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            error!("[OrgMode] File stream lagged by {} messages", n);
                        }
                    }

                    // Wait for headline batch
                    match headline_rx.recv().await {
                        Ok(batch) => {
                            let changes = &batch.inner;
                            let sync_token = batch.metadata.sync_token.as_ref();
                            info!("[OrgMode] Processing {} headline changes", changes.len());
                            if let Err(e) = headline_cache.apply_batch(changes, sync_token).await {
                                error!("[OrgMode] Error applying headline batch: {}", e);
                                continue;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            info!("[OrgMode] Headline stream closed");
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            error!("[OrgMode] Headline stream lagged by {} messages", n);
                        }
                    }

                    info!("[OrgMode] Completed sequential processing of all batches");
                }
            });

            info!("[OrgMode] Sequential stream processing task spawned");

            // Return headline cache as the primary OperationProvider
            headline_cache
        });

        Ok(())
    }
}
