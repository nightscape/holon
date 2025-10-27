//! Command sourcing infrastructure for offline-first operations
//!
//! This module provides the foundation for optimistic updates and background sync:
//! - Command log for tracking operations
//! - Shadow ID mapping for handling unknown external IDs

use crate::storage::{backend::StorageBackend, turso::TursoBackend, types::*};
use anyhow::Result;
use std::collections::HashMap;

/// Initialize command sourcing tables
/// Routes through the database actor for proper serialization.
pub async fn init_command_sourcing_schema(backend: &TursoBackend) -> Result<()> {
    // Commands table: Append-only log of all operations
    backend
        .execute_ddl(
            "CREATE TABLE IF NOT EXISTS commands (
            id TEXT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            command_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            target_system TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            synced_at INTEGER,
            error_details TEXT
        )",
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create commands table: {}", e))?;

    // Index for finding pending commands to sync
    backend
        .execute_ddl(
            "CREATE INDEX IF NOT EXISTS idx_commands_pending
         ON commands(\"status\", \"created_at\")
         WHERE status = 'pending'",
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create pending index: {}", e))?;

    // Index for finding commands by entity
    backend
        .execute_ddl(
            "CREATE INDEX IF NOT EXISTS idx_commands_entity
         ON commands(\"entity_id\", \"created_at\")",
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create entity index: {}", e))?;

    // ID Mappings table: Shadow ID mapping for optimistic updates
    backend
        .execute_ddl(
            "CREATE TABLE IF NOT EXISTS id_mappings (
            internal_id TEXT PRIMARY KEY,
            external_id TEXT,
            source TEXT NOT NULL,
            command_id TEXT NOT NULL,
            state TEXT DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            synced_at INTEGER,
            FOREIGN KEY (command_id) REFERENCES commands(id)
        )",
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create id_mappings table: {}", e))?;

    // Index for finding mappings by command
    backend
        .execute_ddl(
            "CREATE INDEX IF NOT EXISTS idx_id_mappings_command
         ON id_mappings(\"command_id\")",
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create command index: {}", e))?;

    // Index for finding mappings by external ID
    backend
        .execute_ddl(
            "CREATE INDEX IF NOT EXISTS idx_id_mappings_external
         ON id_mappings(\"source\", \"external_id\")
         WHERE external_id IS NOT NULL",
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create external index: {}", e))?;

    Ok(())
}

/// In-memory StateAccess implementation for contract evaluation
///
/// Pre-fetches all required state before contract evaluation to avoid
/// async-in-sync issues. Use `InMemoryStateAccess::from_backend` to
/// create an instance with pre-loaded state.
pub struct InMemoryStateAccess {
    entities: HashMap<String, StorageEntity>,
}

impl InMemoryStateAccess {
    /// Create an empty state access
    pub fn new() -> Self {
        Self {
            entities: HashMap::new(),
        }
    }

    /// Add an entity to the in-memory state
    pub fn insert(&mut self, key: String, entity: StorageEntity) {
        self.entities.insert(key, entity);
    }

    /// Pre-fetch entities from backend for contract evaluation
    ///
    /// Fetches all entities needed by the contract specification.
    /// Call this from async context before passing to sync contract evaluation.
    pub async fn from_backend(backend: &TursoBackend, entity_ids: &[&str]) -> Result<Self> {
        let mut state = Self::new();

        for id in entity_ids {
            if let Some(entity) = backend.get("blocks", id).await? {
                state.insert(id.to_string(), entity);
            }
        }

        Ok(state)
    }
}

impl Default for InMemoryStateAccess {
    fn default() -> Self {
        Self::new()
    }
}

// NOTE: The DataSource trait implementation was removed as it was incompatible
// with the new stream-based DataSource trait. If needed, this should be
// reimplemented using the new datasource::DataSource trait.

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_init_command_sourcing_schema() {
        let (backend, _db_handle) = TursoBackend::new_in_memory().await.unwrap();

        init_command_sourcing_schema(&backend).await.unwrap();

        // Verify tables exist by checking schema using execute_sql
        let results = backend
            .execute_sql(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='commands'",
                std::collections::HashMap::new(),
            )
            .await
            .expect("Failed to query schema");

        assert!(!results.is_empty(), "commands table not found");

        let results = backend
            .execute_sql(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='id_mappings'",
                std::collections::HashMap::new(),
            )
            .await
            .expect("Failed to query schema");

        assert!(!results.is_empty(), "id_mappings table not found");
    }
}
