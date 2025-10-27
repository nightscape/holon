//! Dependency Injection module for holon
//!
//! This module provides service registration and resolution using ferrous-di.
//! It centralizes dependency wiring and makes it easier to test and configure services.
//!
//! Submodules:
//! - `runtime`: Async-in-sync bridging utilities for DI factories
//! - `lifecycle`: App lifecycle functions (creating/initializing BackendEngine)
//! - `registration`: DI service registration functions

pub mod lifecycle;
pub mod registration;
pub mod runtime;
#[cfg(any(test, feature = "test-helpers"))]
pub mod test_helpers;

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::storage::turso::{DbHandle, TursoBackend};

// Re-export public API to preserve backwards compatibility
pub use lifecycle::{
    DiResolver, create_backend_engine, create_backend_engine_with_extras, preload_startup_views,
};
pub use registration::{register_core_services, register_core_services_with_backend};
pub use runtime::{
    create_queryable_cache, create_queryable_cache_with_backend, resolve_turso_backend,
    run_async_in_sync_factory,
};

/// Trait for providing the TursoBackend.
///
/// This trait allows DI resolution using string-based keys instead of TypeId,
/// avoiding TypeId mismatches across crates.
pub trait TursoBackendProvider: Send + Sync {
    fn backend(&self) -> Arc<RwLock<TursoBackend>>;
}

pub(crate) struct TursoBackendProviderImpl {
    pub(crate) backend: Arc<RwLock<TursoBackend>>,
}

impl TursoBackendProvider for TursoBackendProviderImpl {
    fn backend(&self) -> Arc<RwLock<TursoBackend>> {
        self.backend.clone()
    }
}

/// Trait for providing the DbHandle for the database actor.
///
/// This trait allows DI resolution of the database actor handle across crates.
pub trait DbHandleProvider: Send + Sync {
    fn handle(&self) -> DbHandle;
}

pub(crate) struct DbHandleProviderImpl {
    pub(crate) handle: DbHandle,
}

impl DbHandleProvider for DbHandleProviderImpl {
    fn handle(&self) -> DbHandle {
        self.handle.clone()
    }
}

/// Common PRQL queries used during app startup.
///
/// These queries are pre-compiled into materialized views BEFORE file watching
/// or data sync starts. This eliminates the "database is locked" bug that occurs
/// when `query_and_watch` tries to CREATE MATERIALIZED VIEW while IVM is busy
/// processing incoming data.
///
/// IMPORTANT: Only context-independent queries can be preloaded here.
pub const STARTUP_QUERIES: &[&str] = &[
    r#"from blocks
select {id, parent_id, content, content_type, source_language}
render (list item_template:(row (text content:this.content)))"#,
    r#"from blocks
filter content_type == "text"
select {id, content}
render (list item_template:(text this.content))"#,
];

/// Configuration for database path
#[derive(Clone, Debug)]
pub struct DatabasePathConfig {
    pub path: std::path::PathBuf,
}

impl DatabasePathConfig {
    pub fn new(path: std::path::PathBuf) -> Self {
        Self { path }
    }
}
