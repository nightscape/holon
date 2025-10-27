//! Dependency Injection module for OrgMode integration
//!
//! This module provides DI registration for OrgMode-specific services using ferrous-di.
//! OrgMode is now independent of Loro — it will use LoroBlockOperations if available in DI,
//! otherwise falls back to SqlOperationProvider for direct database writes.
//!
//! # Usage
//!
//! ```rust,ignore
//! use holon_orgmode::di::OrgModeServiceCollectionExt;
//! use std::path::PathBuf;
//!
//! services.add_orgmode(PathBuf::from("/path/to/org/files"))?;
//! ```

use ferrous_di::{
    DiResult, Lifetime, Resolver, ServiceCollection, ServiceCollectionModuleExt, ServiceModule,
};
use std::path::PathBuf;
use std::sync::Arc;

use holon_filesystem::directory::Directory;
use holon_filesystem::File;

use crate::file_watcher::OrgFileWatcher;
use crate::org_renderer::OrgRenderer;
use crate::orgmode_adapter::OrgAdapter;
use crate::orgmode_event_adapter::OrgModeEventAdapter;
use crate::orgmode_file_writer::OrgFileWriter;
use crate::write_tracker::WriteTracker;
use crate::OrgModeSyncProvider;
use holon::core::datasource::{OperationProvider, SyncTokenStore, SyncableProvider};
use holon::core::operation_wrapper::OperationWrapper;
use holon::core::queryable_cache::QueryableCache;
use holon::sync::event_bus::{EventBus, PublishErrorTracker};
use holon::sync::{
    CanonicalPath, Document, DocumentOperations, LoroBlockOperations, LoroDocumentStore,
    TursoEventBus,
};
use holon_api::block::Block;
use tokio::sync::RwLock;

/// Signal that indicates the FileWatcher is ready to receive file change events.
///
/// Tests can wait on this signal to ensure the file watcher is established
/// before making external file modifications.
#[derive(Clone)]
pub struct FileWatcherReadySignal {
    receiver: tokio::sync::watch::Receiver<bool>,
}

impl FileWatcherReadySignal {
    /// Create a new ready signal (sender/receiver pair)
    pub fn new() -> (FileWatcherReadySender, Self) {
        let (tx, rx) = tokio::sync::watch::channel(false);
        (FileWatcherReadySender { sender: tx }, Self { receiver: rx })
    }

    /// Wait until the file watcher is ready.
    ///
    /// Returns immediately if already ready, otherwise blocks until signaled.
    /// This takes &self (not &mut self) so it works with Arc<FileWatcherReadySignal>.
    pub async fn wait_ready(&self) {
        // If already ready, return immediately
        if *self.receiver.borrow() {
            return;
        }
        // Clone the receiver for mutable access (watch receivers are designed to be cloned)
        let mut receiver = self.receiver.clone();
        // Wait for the signal to become true
        let _ = receiver.wait_for(|ready| *ready).await;
    }

    /// Check if the file watcher is ready without blocking.
    pub fn is_ready(&self) -> bool {
        *self.receiver.borrow()
    }
}

/// Sender half of the FileWatcher ready signal
pub struct FileWatcherReadySender {
    sender: tokio::sync::watch::Sender<bool>,
}

impl FileWatcherReadySender {
    /// Signal that the file watcher is ready
    pub fn signal_ready(self) {
        let _ = self.sender.send(true);
    }
}

/// Scan a directory recursively for .org files.
///
/// Returns a list of paths to all .org files found.
fn scan_org_files(dir: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
    let mut org_files = Vec::new();

    if !dir.exists() {
        return Ok(org_files);
    }

    fn walk_dir(dir: &std::path::Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                // Skip hidden directories
                if !path
                    .file_name()
                    .map(|n| n.to_string_lossy().starts_with('.'))
                    .unwrap_or(false)
                {
                    walk_dir(&path, files)?;
                }
            } else if path.extension().map(|e| e == "org").unwrap_or(false) {
                files.push(path);
            }
        }
        Ok(())
    }

    walk_dir(dir, &mut org_files)?;
    Ok(org_files)
}

/// Configuration for OrgMode integration
#[derive(Clone, Debug)]
pub struct OrgModeConfig {
    /// Root directory containing .org files
    pub root_directory: PathBuf,
    /// Directory where .loro files are stored (legacy, used when Loro is managed by OrgMode)
    pub loro_storage_dir: PathBuf,
    /// Debounce window in milliseconds for OrgFileWriter.
    /// Events are batched and rendered after this quiet period.
    pub debounce_ms: u64,
}

impl OrgModeConfig {
    pub fn new(root_directory: PathBuf) -> Self {
        // Canonicalize to resolve symlinks (e.g., /var -> /private/var on macOS)
        // This ensures path comparisons work correctly when file watcher reports
        // canonicalized paths
        let root_directory = std::fs::canonicalize(&root_directory).unwrap_or(root_directory);
        let loro_storage_dir = root_directory.join(".loro");
        Self {
            root_directory,
            loro_storage_dir,
            debounce_ms: 500,
        }
    }

    pub fn with_loro_storage(root_directory: PathBuf, loro_storage_dir: PathBuf) -> Self {
        // Canonicalize to resolve symlinks (e.g., /var -> /private/var on macOS)
        let root_directory = std::fs::canonicalize(&root_directory).unwrap_or(root_directory);
        let loro_storage_dir = std::fs::canonicalize(&loro_storage_dir).unwrap_or(loro_storage_dir);
        Self {
            root_directory,
            loro_storage_dir,
            debounce_ms: 500,
        }
    }
}

/// ServiceModule for OrgMode integration
///
/// Registers OrgMode-specific services in the DI container.
/// Loro services are NOT registered here — they come from LoroModule (if enabled).
/// OrgMode will detect if LoroBlockOperations is available in DI and use it;
/// otherwise it falls back to SqlOperationProvider.
pub struct OrgModeModule;

impl ServiceModule for OrgModeModule {
    fn register_services(self, services: &mut ServiceCollection) -> DiResult<()> {
        use tracing::{error, info};

        info!("[OrgModeModule] register_services called");

        // Create and register FileWatcherReadySignal
        // Tests can wait on this to ensure file watcher is ready before external mutations
        let (ready_sender, ready_signal) = FileWatcherReadySignal::new();
        services.add_singleton(ready_signal);
        // Store sender in Arc<Mutex> so we can move it into the spawned task later
        let ready_sender = std::sync::Arc::new(std::sync::Mutex::new(Some(ready_sender)));
        let ready_sender_for_factory = ready_sender.clone();

        // Register OrgModeSyncProvider as a factory
        services.add_singleton_factory::<OrgModeSyncProvider, _>(|resolver| {
            let config = resolver.get_required::<OrgModeConfig>();
            let token_store = resolver
                .get_trait::<dyn SyncTokenStore>()
                .expect("[OrgModeModule] SyncTokenStore not found in DI");
            OrgModeSyncProvider::new(config.root_directory.clone(), token_store)
        });

        // Register SyncableProvider trait implementation
        services.add_trait_factory::<dyn SyncableProvider, _>(Lifetime::Singleton, |resolver| {
            let sync_provider = resolver.get_required::<OrgModeSyncProvider>();
            sync_provider.clone() as Arc<dyn SyncableProvider>
        });

        // Register OrgMode-specific QueryableCaches (Block cache is registered by FrontendConfig)
        services.add_singleton_factory::<QueryableCache<Directory>, _>(|r| {
            holon::di::create_queryable_cache(r)
        });
        services.add_singleton_factory::<QueryableCache<Document>, _>(|r| {
            holon::di::create_queryable_cache(r)
        });
        services.add_singleton_factory::<QueryableCache<File>, _>(|r| {
            holon::di::create_queryable_cache(r)
        });

        // Register DocumentOperations
        services.add_singleton_factory::<DocumentOperations, _>(|resolver| {
            let backend_provider =
                resolver.get_required_trait::<dyn holon::di::TursoBackendProvider>();
            let backend = backend_provider.backend();
            let cache = resolver.get_required::<QueryableCache<Document>>();

            let ops = DocumentOperations::new(backend, cache);

            // Initialize schema synchronously
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    ops.init_schema()
                        .await
                        .expect("Failed to initialize documents schema");
                })
            });

            ops
        });

        // Register DocumentOperations as OperationProvider for "documents" entity
        services.add_trait_factory::<dyn OperationProvider, _>(Lifetime::Singleton, |resolver| {
            let doc_ops = resolver.get_required::<DocumentOperations>();
            doc_ops as Arc<dyn OperationProvider>
        });

        // TursoEventBus is registered by FrontendConfig shared infrastructure

        // Register OrgRenderer
        services.add_singleton_factory::<Arc<OrgRenderer>, _>(|_resolver| Arc::new(OrgRenderer));

        // Set up event bus wiring and background tasks.
        // This factory resolves LoroBlockOperations if available (Loro enabled),
        // otherwise creates a SqlOperationProvider for direct SQL block operations.
        services.add_trait_factory::<dyn OperationProvider, _>(
            Lifetime::Singleton,
            move |resolver| {
                // ============================================================
                // PHASE 1: Resolve ALL services that run DDL
                // This ensures all schema initialization completes BEFORE
                // any background tasks start using the database.
                // ============================================================
                info!("[OrgMode] Phase 1: Resolving services (DDL)");

                let dir_cache = resolver.get_required::<QueryableCache<Directory>>();
                let doc_cache = resolver.get_required::<QueryableCache<Document>>();
                let file_cache = resolver.get_required::<QueryableCache<File>>();
                let block_cache = resolver.get_required::<QueryableCache<Block>>();
                let sync_provider = resolver.get_required::<OrgModeSyncProvider>();

                // IMPORTANT: Resolve TursoEventBus HERE, not after spawns!
                // TursoEventBus::init_schema() runs DDL that must complete
                // before any background tasks use the database.
                let event_bus = resolver.get_required::<TursoEventBus>();
                let event_bus_arc: Arc<dyn EventBus> = event_bus.clone();

                // Resolve remaining services that might run DDL
                let config = resolver.get_required::<OrgModeConfig>();
                let doc_ops = resolver.get_required::<DocumentOperations>();

                // Try to resolve Loro services (available if LoroModule was registered)
                let loro_ops: Option<Arc<LoroBlockOperations>> =
                    resolver.get::<LoroBlockOperations>().ok();
                let loro_doc_store: Option<Arc<LoroDocumentStore>> =
                    resolver.get::<LoroDocumentStore>().ok();

                let loro_available = loro_ops.is_some();
                info!(
                    "[OrgMode] Phase 1 complete: All DDL finished (loro={})",
                    loro_available
                );

                // Determine the block operations provider (command bus):
                // - If Loro is available, use LoroBlockOperations
                // - Otherwise, create SqlOperationProvider for direct DB writes
                let sql_ops: Option<Arc<holon::core::SqlOperationProvider>> = if loro_ops.is_none()
                {
                    let backend_provider =
                        resolver.get_required_trait::<dyn holon::di::TursoBackendProvider>();
                    let backend = backend_provider.backend();
                    Some(Arc::new(holon::core::SqlOperationProvider::new(
                        backend,
                        "blocks".to_string(),
                        "blocks".to_string(),
                        "block".to_string(),
                    )))
                } else {
                    None
                };

                // Get a trait-object command bus for OrgAdapter
                let command_bus: Arc<dyn OperationProvider> =
                    if let Some(ref ops) = loro_ops {
                        ops.clone() as Arc<dyn OperationProvider>
                    } else {
                        sql_ops.clone().unwrap() as Arc<dyn OperationProvider>
                    };

                // ============================================================
                // PHASE 2: Set up synchronous wiring (no spawns yet)
                // CacheEventSubscriber (EventBus → QueryableCache<Block>) is wired
                // by FrontendConfig shared infrastructure.
                // ============================================================
                info!("[OrgMode] Phase 2: Synchronous wiring");

                // Shared WriteTracker to prevent sync loops
                let write_tracker = Arc::new(RwLock::new(WriteTracker::new()));

                // Create shared known_hashes for FileWatcher and OrgFileWriter
                let known_hashes: Arc<RwLock<std::collections::HashMap<CanonicalPath, String>>> =
                    Arc::new(RwLock::new(std::collections::HashMap::new()));

                // EventBus → Org file (only when Loro is available, since OrgFileWriter
                // reads blocks from LoroDocumentStore to render org files)
                if let Some(ref doc_store) = loro_doc_store {
                    let event_bus_clone = event_bus_arc.clone();
                    let write_tracker_clone = write_tracker.clone();
                    let known_hashes_clone = known_hashes.clone();
                    let doc_store_clone = doc_store.clone();

                    tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(async {
                            let doc_store_arc =
                                Arc::new(RwLock::new((*doc_store_clone).clone()));
                            let writer = OrgFileWriter::with_hash_tracking(
                                doc_store_arc,
                                write_tracker_clone,
                                known_hashes_clone,
                                config.debounce_ms,
                            );
                            if let Err(e) = writer.start(event_bus_clone).await {
                                error!("[OrgMode] Failed to start OrgFileWriter: {}", e);
                            }
                            eprintln!("[OrgMode/DI] OrgFileWriter started");
                        });
                    });
                }

                info!("[OrgMode] Phase 2 complete: Synchronous wiring done");

                // ============================================================
                // PHASE 3: Spawn background tasks
                // The DatabaseActor serializes all operations, eliminating race conditions
                // between DDL and DML operations.
                // ============================================================
                info!("[OrgMode] Phase 3: Spawning background tasks");

                // Subscribe to streams
                let mut dir_rx = sync_provider.subscribe_directories();
                let mut file_rx = sync_provider.subscribe_files();
                let mut block_rx = sync_provider.subscribe_blocks();

                let dir_cache_clone = dir_cache.clone();
                let file_cache_clone = file_cache.clone();
                let block_cache_clone = block_cache.clone();

                // Sequential stream processing task
                tokio::spawn(async move {
                    let dir_cache = dir_cache_clone;
                    let file_cache = file_cache_clone;
                    let block_cache = block_cache_clone;
                    loop {
                        match dir_rx.recv().await {
                            Ok(batch) => {
                                let changes = &batch.inner;
                                let sync_token = batch.metadata.sync_token.as_ref();
                                if let Err(e) = dir_cache.apply_batch(changes, sync_token).await {
                                    error!("[OrgMode] Error applying directory batch: {}", e);
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                error!("[OrgMode] Directory stream lagged by {} messages", n);
                            }
                        }

                        match file_rx.recv().await {
                            Ok(batch) => {
                                let changes = &batch.inner;
                                let sync_token = batch.metadata.sync_token.as_ref();
                                if let Err(e) = file_cache.apply_batch(changes, sync_token).await {
                                    error!("[OrgMode] Error applying file batch: {}", e);
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                error!("[OrgMode] File stream lagged by {} messages", n);
                            }
                        }

                        match block_rx.recv().await {
                            Ok(batch) => {
                                let changes = &batch.inner;
                                let sync_token = batch.metadata.sync_token.as_ref();
                                if let Err(e) = block_cache.apply_batch(changes, sync_token).await {
                                    error!("[OrgMode] Error applying block batch: {}", e);
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                error!("[OrgMode] Block stream lagged by {} messages", n);
                            }
                        }
                    }
                });

                // Initial sync task
                // The DatabaseActor serializes all operations, eliminating race conditions.
                {
                    let sync_provider_clone = sync_provider.clone();
                    tokio::spawn(async move {
                        use holon::core::datasource::SyncableProvider;
                        if let Err(e) = sync_provider_clone
                            .sync(holon::core::datasource::StreamPosition::Beginning)
                            .await
                        {
                            error!("[OrgMode] Initial sync failed: {}", e);
                        }
                    });
                }

                // LoroBlockOperations → EventBus is wired by LoroModule (LoroEventAdapterHandle)

                // OrgModeSyncProvider → EventBus
                // The DatabaseActor serializes all operations, eliminating race conditions.
                {
                    let sync_provider_clone = sync_provider.clone();
                    let event_bus_clone = event_bus_arc.clone();
                    let error_tracker = resolver.get::<PublishErrorTracker>()
                        .map(|t| (*t).clone())
                        .unwrap_or_else(|_| PublishErrorTracker::new());
                    tokio::spawn(async move {
                        let adapter =
                            OrgModeEventAdapter::with_error_tracker(event_bus_clone, error_tracker);
                        let dir_rx = sync_provider_clone.subscribe_directories();
                        let file_rx = sync_provider_clone.subscribe_files();
                        let block_rx = sync_provider_clone.subscribe_blocks();
                        if let Err(e) = adapter.start(dir_rx, file_rx, block_rx) {
                            error!("[OrgMode] Failed to start OrgModeEventAdapter: {}", e);
                        }
                    });
                }

                // File watcher → OrgAdapter → Command Bus
                // The DatabaseActor serializes all operations, eliminating race conditions.
                {
                    let command_bus = command_bus.clone();
                    let write_tracker_clone = write_tracker.clone();
                    let known_hashes_clone = known_hashes.clone();
                    let event_bus_for_adapter = event_bus_arc.clone();
                    let config_clone = config.clone();
                    let doc_ops_clone = doc_ops.clone();
                    let loro_doc_store_clone = loro_doc_store.clone();
                    // Take the ready sender to signal when file watcher is ready
                    let ready_sender_clone = ready_sender_for_factory.clone();

                    tokio::spawn(async move {
                        let mut org_adapter = OrgAdapter::with_write_tracker(
                            command_bus,
                            doc_ops_clone,
                            config_clone.root_directory.clone(),
                            write_tracker_clone,
                        );
                        if let Some(store) = loro_doc_store_clone {
                            org_adapter = org_adapter.with_loro_doc_store(store);
                        }
                        let org_adapter = Arc::new(org_adapter);

                        // Start event subscription to keep known_state in sync with UI mutations
                        if let Err(e) = org_adapter
                            .clone()
                            .start_event_subscription(event_bus_for_adapter)
                            .await
                        {
                            error!(
                                "[OrgMode] Failed to start OrgAdapter event subscription: {}",
                                e
                            );
                        } else {
                            eprintln!("[OrgMode/DI] OrgAdapter event subscription started");
                        }

                        eprintln!(
                            "[OrgMode/DI] About to start file watcher for: {}",
                            config_clone.root_directory.display()
                        );
                        match OrgFileWatcher::with_hashes(
                            &config_clone.root_directory,
                            known_hashes_clone,
                        ) {
                            Ok(watcher) => {
                                // IMPORTANT: Keep _watcher alive for the entire duration of the loop!
                                // Dropping _watcher stops file watching.
                                let (_watcher, mut receiver, _known_hashes) = watcher.into_parts();
                                eprintln!(
                                    "[OrgMode/DI] File watcher started for: {}",
                                    config_clone.root_directory.display()
                                );
                                info!(
                                    "[OrgMode] File watcher started for: {}",
                                    config_clone.root_directory.display()
                                );

                                // Process existing org files BEFORE signaling ready
                                eprintln!(
                                    "[OrgMode/DI] Processing existing org files in: {}",
                                    config_clone.root_directory.display()
                                );
                                if let Ok(existing_files) =
                                    scan_org_files(&config_clone.root_directory)
                                {
                                    eprintln!(
                                        "[OrgMode/DI] Found {} existing org files to process",
                                        existing_files.len()
                                    );
                                    for file_path in existing_files {
                                        // Compute hash to track initial state
                                        let canonical = CanonicalPath::new(&file_path);
                                        if let Ok(hash) = OrgFileWatcher::hash_file(&file_path) {
                                            _known_hashes.write().await.insert(canonical, hash);
                                        }

                                        // Process through OrgAdapter to populate blocks
                                        if let Err(e) =
                                            org_adapter.on_file_changed(&file_path).await
                                        {
                                            eprintln!(
                                                "[OrgMode/DI] Failed to process existing file {}: {}",
                                                file_path.display(),
                                                e
                                            );
                                            error!(
                                                "[OrgMode] Failed to process existing file {}: {}",
                                                file_path.display(),
                                                e
                                            );
                                        } else {
                                            eprintln!(
                                                "[OrgMode/DI] Processed existing file: {}",
                                                file_path.display()
                                            );
                                        }
                                    }
                                    eprintln!("[OrgMode/DI] Finished processing existing org files");
                                }

                                // Signal that the file watcher is ready
                                if let Some(sender) = ready_sender_clone.lock().unwrap().take() {
                                    sender.signal_ready();
                                    eprintln!("[OrgMode/DI] FileWatcher ready signal sent");
                                }

                                while let Some(file_path) = receiver.recv().await {
                                    eprintln!("[OrgMode/DI] File changed: {}", file_path.display());

                                    // Check if content actually changed (filter out duplicate events from FSEvents)
                                    let current_hash = match OrgFileWatcher::hash_file(&file_path) {
                                        Ok(h) => h,
                                        Err(_) => {
                                            eprintln!("[OrgMode/DI] Cannot read file for hashing, skipping");
                                            continue;
                                        }
                                    };

                                    let canonical = CanonicalPath::new(&file_path);
                                    let known = _known_hashes.read().await.get(&canonical).cloned();

                                    if Some(&current_hash) == known.as_ref() {
                                        eprintln!("[OrgMode/DI] Content unchanged (hash match), skipping");
                                        continue;
                                    }

                                    // Update known hash before processing
                                    _known_hashes.write().await.insert(canonical, current_hash);

                                    if let Err(e) = org_adapter.on_file_changed(&file_path).await {
                                        eprintln!(
                                            "[OrgMode/DI] Failed to process file change {}: {}",
                                            file_path.display(),
                                            e
                                        );
                                        error!(
                                            "[OrgMode] Failed to process file change {}: {}",
                                            file_path.display(),
                                            e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("[OrgMode/DI] Failed to start file watcher: {}", e);
                                error!("[OrgMode] Failed to start file watcher: {}", e);
                                // Still signal ready (even on error) so tests don't hang forever
                                if let Some(sender) = ready_sender_clone.lock().unwrap().take() {
                                    sender.signal_ready();
                                }
                            }
                        }
                    });
                }

                // Build the OperationWrapper with the appropriate concrete type
                if let Some(ref ops) = loro_ops {
                    let wrapper = OperationWrapper::new(ops.clone(), Some(sync_provider));
                    Arc::new(wrapper) as Arc<dyn OperationProvider>
                } else {
                    let wrapper =
                        OperationWrapper::new(sql_ops.unwrap(), Some(sync_provider));
                    Arc::new(wrapper) as Arc<dyn OperationProvider>
                }
            },
        );

        Ok(())
    }
}

/// Extension trait for registering OrgMode services in a [`ServiceCollection`]
///
/// This trait provides a convenient method to register all OrgMode-related
/// services with a single call, taking just the root directory as a parameter.
///
/// # Example
///
/// ```rust,ignore
/// use holon_orgmode::di::OrgModeServiceCollectionExt;
/// use std::path::PathBuf;
///
/// // In your DI setup closure:
/// services.add_orgmode(PathBuf::from("/path/to/org/files"))?;
/// ```
pub trait OrgModeServiceCollectionExt {
    /// Register OrgMode services with the given root directory
    ///
    /// This registers:
    /// - `OrgModeConfig` with the provided root directory
    /// - `OrgModeModule` which sets up all OrgMode-related services
    ///
    /// # Errors
    ///
    /// Returns an error if module registration fails.
    fn add_orgmode(&mut self, root_directory: PathBuf) -> DiResult<()>;
}

impl OrgModeServiceCollectionExt for ServiceCollection {
    fn add_orgmode(&mut self, root_directory: PathBuf) -> DiResult<()> {
        self.add_singleton(OrgModeConfig::new(root_directory));
        self.add_module_mut(OrgModeModule)?;
        Ok(())
    }
}
