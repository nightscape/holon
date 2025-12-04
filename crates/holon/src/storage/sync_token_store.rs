//! Database-backed sync token store implementation
//!
//! This module provides a SyncTokenStore implementation that persists sync tokens
//! to a SQLite database using the sync_states table.

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::core::datasource::{Result, StreamPosition, SyncTokenStore};
use crate::storage::turso::TursoBackend;

/// Database-backed sync token store
///
/// Stores sync tokens in the sync_states table in SQLite.
/// This avoids circular dependencies by not requiring BackendEngine.
pub struct DatabaseSyncTokenStore {
    backend: Arc<RwLock<TursoBackend>>,
}

impl DatabaseSyncTokenStore {
    /// Create a new DatabaseSyncTokenStore
    pub fn new(backend: Arc<RwLock<TursoBackend>>) -> Self {
        Self { backend }
    }

    /// Initialize sync_states table for persisting sync tokens
    ///
    /// This table stores the last sync position for each provider to enable
    /// incremental syncs across app restarts.
    pub async fn initialize_sync_state_table(&self) -> Result<()> {
        let create_table_sql = r#"
            CREATE TABLE IF NOT EXISTS sync_states (
                provider_name TEXT PRIMARY KEY,
                sync_token TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        "#;

        let backend = self.backend.read().await;
        let conn = backend
            .get_connection()
            .map_err(|e| format!("Failed to get connection: {}", e))?;

        conn.execute(create_table_sql, ())
            .await
            .map_err(|e| format!("Failed to create sync_states table: {}", e))?;

        info!("[DatabaseSyncTokenStore] sync_states table initialized");
        Ok(())
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl SyncTokenStore for DatabaseSyncTokenStore {
    /// Load sync token for a provider from database
    ///
    /// Returns None if no token exists (first sync).
    async fn load_token(&self, provider_name: &str) -> Result<Option<StreamPosition>> {
        debug!(
            "[DatabaseSyncTokenStore] load_token called for provider '{}'",
            provider_name
        );

        let sql = "SELECT sync_token FROM sync_states WHERE provider_name = ?";
        let backend = self.backend.read().await;
        debug!("[DatabaseSyncTokenStore] Got backend read lock, getting connection...");

        let conn = backend
            .get_connection()
            .map_err(|e| format!("Failed to get connection: {}", e))?;

        let autocommit_before = conn.is_autocommit().unwrap_or(true);
        debug!(
            "[DatabaseSyncTokenStore] Got connection for load_token. Autocommit: {}",
            autocommit_before
        );

        debug!("[DatabaseSyncTokenStore] Preparing SELECT statement...");
        let mut stmt = conn
            .prepare(sql)
            .await
            .map_err(|e| format!("Failed to prepare statement: {}", e))?;

        debug!("[DatabaseSyncTokenStore] Executing query...");
        let mut rows = stmt
            .query(turso::params![turso::Value::Text(
                provider_name.to_string()
            )])
            .await
            .map_err(|e| format!("Failed to query sync token: {}", e))?;

        debug!("[DatabaseSyncTokenStore] Query executed, fetching row...");
        if let Ok(Some(row)) = rows.next().await {
            if let Ok(token_str) = row.get::<String>(0) {
                debug!(
                    "[DatabaseSyncTokenStore] Loaded sync token for provider '{}': {}",
                    provider_name, token_str
                );

                // Explicitly drop rows and stmt to release any locks
                drop(rows);
                drop(stmt);

                let autocommit_after = conn.is_autocommit().unwrap_or(true);
                debug!(
                    "[DatabaseSyncTokenStore] After dropping rows/stmt. Autocommit: {}",
                    autocommit_after
                );

                // Convert string token back to StreamPosition
                let position = if token_str == "*" {
                    StreamPosition::Beginning
                } else {
                    StreamPosition::Version(token_str.as_bytes().to_vec())
                };

                debug!(
                    "[DatabaseSyncTokenStore] load_token returning token for '{}'",
                    provider_name
                );
                return Ok(Some(position));
            }
        }

        // Explicitly drop rows and stmt to release any locks
        drop(rows);
        drop(stmt);

        let autocommit_after = conn.is_autocommit().unwrap_or(true);
        debug!(
            "[DatabaseSyncTokenStore] No sync token found for provider '{}'. Autocommit after: {}",
            provider_name, autocommit_after
        );
        Ok(None)
    }

    /// Save sync token for a provider to database
    async fn save_token(&self, provider_name: &str, position: StreamPosition) -> Result<()> {
        debug!(
            "[DatabaseSyncTokenStore] save_token called for provider '{}'",
            provider_name
        );

        // Convert StreamPosition to string for storage
        let token_str = match position {
            StreamPosition::Beginning => "*".to_string(),
            StreamPosition::Version(bytes) => {
                // Convert bytes to string (assuming UTF-8)
                String::from_utf8(bytes).unwrap_or_else(|_| "*".to_string())
            }
        };

        let sql = r#"
            INSERT INTO sync_states (provider_name, sync_token, updated_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(provider_name) DO UPDATE SET
                sync_token = excluded.sync_token,
                updated_at = excluded.updated_at
        "#;

        let backend = self.backend.read().await;
        debug!(
            "[DatabaseSyncTokenStore] Got backend read lock for save_token, getting connection..."
        );

        let conn = backend
            .get_connection()
            .map_err(|e| format!("Failed to get connection: {}", e))?;

        let autocommit_before = conn.is_autocommit().unwrap_or(true);
        debug!(
            "[DatabaseSyncTokenStore] Got connection for save_token. Autocommit: {}",
            autocommit_before
        );

        debug!("[DatabaseSyncTokenStore] Executing INSERT/UPDATE...");
        conn.execute(
            sql,
            turso::params![
                turso::Value::Text(provider_name.to_string()),
                turso::Value::Text(token_str.clone())
            ],
        )
        .await
        .map_err(|e| format!("Failed to save sync token: {}", e))?;

        let autocommit_after = conn.is_autocommit().unwrap_or(true);
        info!(
            "[DatabaseSyncTokenStore] Saved sync token for provider '{}': {}. Autocommit after: {}",
            provider_name, token_str, autocommit_after
        );
        Ok(())
    }
}
