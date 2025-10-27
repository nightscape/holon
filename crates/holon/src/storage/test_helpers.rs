//! Test helpers for storage module tests.
//!
//! This module provides reusable utilities for creating TursoBackend instances
//! for unit and integration testing.

use super::turso::{DbHandle, TursoBackend};
use std::path::Path;
use tempfile::TempDir;
use tokio::sync::broadcast;

/// A test backend that keeps the temporary directory alive for the duration of the test.
pub struct TestBackend {
    pub backend: TursoBackend,
    pub db_handle: DbHandle,
    _temp_dir: TempDir,
}

impl TestBackend {
    /// Create a new test backend with a fresh temporary database.
    pub async fn new() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test.db");
        Self::new_with_path(&db_path, temp_dir).await
    }

    /// Create a test backend with a specific path and temp directory.
    async fn new_with_path(db_path: &Path, temp_dir: TempDir) -> Self {
        let db = TursoBackend::open_database(db_path).expect("Failed to open database");
        let (cdc_tx, _cdc_rx) = broadcast::channel(1024);

        let (backend, db_handle) =
            TursoBackend::new(db, cdc_tx).expect("Failed to create TursoBackend");

        Self {
            backend,
            db_handle,
            _temp_dir: temp_dir,
        }
    }

    /// Get a mutable reference to the backend.
    pub fn backend_mut(&mut self) -> &mut TursoBackend {
        &mut self.backend
    }
}

/// Create a test backend for unit testing.
///
/// This function creates a test setup using a temporary file.
/// Returns the TursoBackend ready for use.
///
/// Note: The temporary directory is kept alive using `std::mem::forget`.
/// This is acceptable for short-lived tests.
pub async fn create_test_backend() -> TursoBackend {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let db = TursoBackend::open_database(&db_path).expect("Failed to open database");
    let (cdc_tx, _cdc_rx) = broadcast::channel(1024);

    let (backend, _handle) = TursoBackend::new(db, cdc_tx).expect("Failed to create TursoBackend");

    // Keep the temp dir alive - this leaks memory but is acceptable for tests
    std::mem::forget(temp_dir);

    backend
}

/// Create a test backend with a specific database path.
///
/// The caller is responsible for ensuring the path's parent directory exists.
pub async fn create_test_backend_at_path(db_path: &Path) -> TursoBackend {
    let db = TursoBackend::open_database(db_path).expect("Failed to open database");
    let (cdc_tx, _cdc_rx) = broadcast::channel(1024);

    let (backend, _handle) = TursoBackend::new(db, cdc_tx).expect("Failed to create TursoBackend");

    backend
}
