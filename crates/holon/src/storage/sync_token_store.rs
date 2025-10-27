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
    /// Uses execute_ddl to route through the database actor for serialization.
    pub async fn initialize_sync_state_table(&self) -> Result<()> {
        let create_table_sql = r#"
            CREATE TABLE IF NOT EXISTS sync_states (
                provider_name TEXT PRIMARY KEY,
                sync_token TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        "#;

        let backend = self.backend.read().await;

        backend
            .execute_ddl(create_table_sql)
            .await
            .map_err(|e| format!("Failed to create sync_states table: {}", e))?;

        info!("[DatabaseSyncTokenStore] sync_states table initialized");
        Ok(())
    }

    /// Clear all sync tokens from the database
    ///
    /// Used for full sync operations where all cached data is being pruned
    /// and sync needs to start from the beginning for all providers.
    /// Routes through the database actor for proper serialization.
    pub async fn clear_all_tokens(&self) -> Result<()> {
        let backend = self.backend.read().await;

        backend
            .execute_via_actor("DELETE FROM sync_states", vec![])
            .await
            .map_err(|e| format!("Failed to clear sync tokens: {}", e))?;

        info!("[DatabaseSyncTokenStore] Cleared all sync tokens");
        Ok(())
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl SyncTokenStore for DatabaseSyncTokenStore {
    /// Load sync token for a provider from database
    ///
    /// Returns None if no token exists (first sync).
    /// Routes through the database actor for proper serialization.
    async fn load_token(&self, provider_name: &str) -> Result<Option<StreamPosition>> {
        debug!(
            "[DatabaseSyncTokenStore] load_token called for provider '{}'",
            provider_name
        );

        let backend = self.backend.read().await;

        // Use execute_sql which routes through db_handle if available
        let sql = "SELECT sync_token FROM sync_states WHERE provider_name = $provider_name";
        let mut params = std::collections::HashMap::new();
        params.insert(
            "provider_name".to_string(),
            holon_api::Value::String(provider_name.to_string()),
        );

        let results = backend
            .execute_sql(sql, params)
            .await
            .map_err(|e| format!("Failed to query sync token: {}", e))?;

        if let Some(row) = results.into_iter().next() {
            if let Some(holon_api::Value::String(token_str)) = row.get("sync_token") {
                debug!(
                    "[DatabaseSyncTokenStore] Loaded sync token for provider '{}': {}",
                    provider_name, token_str
                );

                let position = if token_str == "*" {
                    StreamPosition::Beginning
                } else {
                    StreamPosition::Version(token_str.as_bytes().to_vec())
                };

                return Ok(Some(position));
            }
        }

        debug!(
            "[DatabaseSyncTokenStore] No sync token found for provider '{}'",
            provider_name
        );
        Ok(None)
    }

    /// Save sync token for a provider to database
    /// Routes through the database actor for proper serialization.
    async fn save_token(&self, provider_name: &str, position: StreamPosition) -> Result<()> {
        debug!(
            "[DatabaseSyncTokenStore] save_token called for provider '{}'",
            provider_name
        );

        // Convert StreamPosition to string for storage
        let token_str = match position {
            StreamPosition::Beginning => "*".to_string(),
            StreamPosition::Version(bytes) => {
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

        backend
            .execute_via_actor(
                sql,
                vec![
                    turso::Value::Text(provider_name.to_string()),
                    turso::Value::Text(token_str.clone()),
                ],
            )
            .await
            .map_err(|e| format!("Failed to save sync token: {}", e))?;

        info!(
            "[DatabaseSyncTokenStore] Saved sync token for provider '{}': {}",
            provider_name, token_str
        );
        Ok(())
    }

    /// Clear all sync tokens from the database (trait implementation)
    async fn clear_all_tokens(&self) -> Result<()> {
        DatabaseSyncTokenStore::clear_all_tokens(self).await
    }
}
