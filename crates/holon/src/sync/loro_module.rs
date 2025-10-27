//! Standalone Loro DI module
//!
//! Registers Loro CRDT services independently of OrgMode.
//! When enabled, Loro provides:
//! - `LoroDocumentStore` for managing CRDT documents
//! - `LoroBlocksDataSource` for populating QueryableCache
//! - `LoroBlockOperations` as an OperationProvider for blocks
//! - `LoroEventAdapter` bridging Loro changes → EventBus
//!
//! The module also subscribes to non-Loro events on the EventBus and applies them
//! to Loro, enabling the reverse flow (EventBus → Loro).

use std::path::PathBuf;
use std::sync::Arc;

use ferrous_di::{DiResult, Lifetime, Resolver, ServiceCollection, ServiceModule};
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::core::datasource::OperationProvider;
use crate::core::queryable_cache::QueryableCache;
use crate::sync::event_bus::EventBus;
use crate::sync::{
    LoroBlockOperations, LoroBlocksDataSource, LoroDocumentStore, LoroEventAdapter, TursoEventBus,
};
use holon_api::block::Block;

/// Configuration for standalone Loro CRDT support
#[derive(Clone, Debug)]
pub struct LoroConfig {
    /// Root directory for Loro document storage
    pub storage_dir: PathBuf,
}

impl LoroConfig {
    pub fn new(storage_dir: PathBuf) -> Self {
        let storage_dir = std::fs::canonicalize(&storage_dir).unwrap_or(storage_dir);
        Self { storage_dir }
    }
}

/// ServiceModule for standalone Loro CRDT support
///
/// Registers Loro-specific services in the DI container without requiring OrgMode.
/// When both OrgMode and Loro are enabled, OrgMode's DI should detect that
/// LoroBlockOperations is already registered and use it instead of creating its own.
pub struct LoroModule;

impl ServiceModule for LoroModule {
    fn register_services(self, services: &mut ServiceCollection) -> DiResult<()> {
        info!("[LoroModule] register_services called");

        // Register LoroDocumentStore
        services.add_singleton_factory::<LoroDocumentStore, _>(|resolver| {
            let config = resolver.get_required::<LoroConfig>();
            LoroDocumentStore::new(config.storage_dir.clone())
        });

        // Register LoroBlocksDataSource
        services.add_singleton_factory::<LoroBlocksDataSource, _>(|resolver| {
            let doc_store = resolver.get_required::<LoroDocumentStore>();
            LoroBlocksDataSource::new(Arc::new(RwLock::new((*doc_store).clone())))
        });

        // Register LoroBlockOperations
        services.add_singleton_factory::<LoroBlockOperations, _>(|resolver| {
            let doc_store = resolver.get_required::<LoroDocumentStore>();
            let cache = resolver.get_required::<QueryableCache<Block>>();
            LoroBlockOperations::new(Arc::new(RwLock::new((*doc_store).clone())), cache)
        });

        // Register LoroBlockOperations as an OperationProvider
        services.add_trait_factory::<dyn OperationProvider, _>(Lifetime::Singleton, |resolver| {
            let loro_ops = resolver.get_required::<LoroBlockOperations>();
            loro_ops as Arc<dyn OperationProvider>
        });

        // Wire up LoroBlockOperations → EventBus (Loro changes publish to EventBus)
        // This is registered as a factory to defer execution until DI resolution.
        services.add_singleton_factory::<LoroEventAdapterHandle, _>(|resolver| {
            let loro_ops = resolver.get_required::<LoroBlockOperations>();
            let event_bus = resolver.get_required::<TursoEventBus>();
            let event_bus_arc: Arc<dyn EventBus> = event_bus.clone();

            tokio::spawn(async move {
                let adapter = LoroEventAdapter::new(event_bus_arc);
                let loro_rx = loro_ops.subscribe();
                if let Err(e) = adapter.start(loro_rx) {
                    error!("[LoroModule] Failed to start LoroEventAdapter: {}", e);
                }
            });

            LoroEventAdapterHandle
        });

        info!("[LoroModule] register_services complete");
        Ok(())
    }
}

/// Marker type for the LoroEventAdapter background task.
/// Resolving this from DI triggers the adapter to start.
pub struct LoroEventAdapterHandle;
