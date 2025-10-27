//! Concrete schema module implementations for core database objects.
//!
//! This module provides `SchemaModule` implementations for the core database
//! schema objects in Holon:
//!
//! - `CoreSchemaModule`: blocks, documents, directories tables
//! - `BlockHierarchySchemaModule`: blocks_with_paths materialized view
//! - `NavigationSchemaModule`: navigation_history, navigation_cursor, current_focus
//! - `SyncStateSchemaModule`: sync_states table
//! - `OperationsSchemaModule`: operations table for undo/redo

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use super::resource::Resource;
use super::schema_module::SchemaModule;
use super::turso::TursoBackend;
use super::types::Result;

/// Core schema module providing the fundamental tables: blocks, documents, directories.
///
/// This module has no dependencies and should be initialized first.
pub struct CoreSchemaModule;

impl CoreSchemaModule {
    /// SQL for creating the blocks table with all columns used by the system.
    const BLOCKS_TABLE_SQL: &'static str = r#"
        CREATE TABLE IF NOT EXISTS blocks (
            id TEXT PRIMARY KEY,
            parent_id TEXT,
            depth INTEGER NOT NULL DEFAULT 0,
            sort_key TEXT NOT NULL DEFAULT 'a0',
            content TEXT NOT NULL DEFAULT '',
            content_type TEXT NOT NULL DEFAULT 'text',
            source_language TEXT,
            source_name TEXT,
            properties TEXT,
            collapsed INTEGER NOT NULL DEFAULT 0,
            completed INTEGER NOT NULL DEFAULT 0,
            block_type TEXT NOT NULL DEFAULT 'text',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            _change_origin TEXT
        )
    "#;

    /// SQL for creating the documents table.
    /// NOTE: This schema MUST match the Document entity in holon-api/src/document.rs
    /// The fields are: id (PK), parent_id (indexed), name (indexed), sort_key, properties, created_at, updated_at
    const DOCUMENTS_TABLE_SQL: &'static str = r#"
        CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY NOT NULL,
            parent_id TEXT NOT NULL,
            name TEXT NOT NULL,
            sort_key TEXT NOT NULL,
            properties TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            _change_origin TEXT
        )
    "#;

    /// SQL for creating indexes on the documents table.
    const DOCUMENTS_PARENT_INDEX_SQL: &'static str = r#"
        CREATE INDEX IF NOT EXISTS idx_documents_parent_id ON documents(parent_id)
    "#;

    const DOCUMENTS_NAME_INDEX_SQL: &'static str = r#"
        CREATE INDEX IF NOT EXISTS idx_documents_name ON documents(name)
    "#;

    /// SQL for creating the directories table.
    /// NOTE: This schema MUST match the Directory entity in holon-filesystem/src/directory.rs
    /// The fields are: id (PK, indexed), name, parent_id (indexed), depth
    const DIRECTORIES_TABLE_SQL: &'static str = r#"
        CREATE TABLE IF NOT EXISTS directories (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            parent_id TEXT NOT NULL,
            depth INTEGER NOT NULL,
            _change_origin TEXT
        )
    "#;

    /// SQL for creating index on directories.parent_id for efficient child queries.
    const DIRECTORIES_PARENT_INDEX_SQL: &'static str = r#"
        CREATE INDEX IF NOT EXISTS idx_directories_parent_id ON directories(parent_id)
    "#;

    /// SQL for creating the files table.
    /// NOTE: This schema MUST match the File entity in holon-filesystem/src/file.rs
    const FILES_TABLE_SQL: &'static str = r#"
        CREATE TABLE IF NOT EXISTS files (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            parent_id TEXT NOT NULL,
            content_hash TEXT NOT NULL DEFAULT '',
            document_id TEXT,
            _change_origin TEXT
        )
    "#;

    const FILES_PARENT_INDEX_SQL: &'static str = r#"
        CREATE INDEX IF NOT EXISTS idx_files_parent_id ON files(parent_id)
    "#;

    const FILES_DOCUMENT_INDEX_SQL: &'static str = r#"
        CREATE INDEX IF NOT EXISTS idx_files_document_id ON files(document_id)
    "#;

    /// SQL for creating index on blocks.parent_id for efficient child queries.
    const BLOCKS_PARENT_INDEX_SQL: &'static str = r#"
        CREATE INDEX IF NOT EXISTS idx_blocks_parent_id ON blocks(parent_id)
    "#;
}

#[async_trait]
impl SchemaModule for CoreSchemaModule {
    fn name(&self) -> &str {
        "core"
    }

    fn provides(&self) -> Vec<Resource> {
        vec![
            Resource::schema("blocks"),
            Resource::schema("documents"),
            Resource::schema("directories"),
            Resource::schema("files"),
        ]
    }

    fn requires(&self) -> Vec<Resource> {
        vec![] // No dependencies - this is the root
    }

    async fn ensure_schema(&self, backend: &TursoBackend) -> Result<()> {
        tracing::info!("[CoreSchemaModule] Creating core tables");

        backend.execute_ddl(Self::BLOCKS_TABLE_SQL).await?;
        tracing::debug!("[CoreSchemaModule] blocks table created");

        backend.execute_ddl(Self::DOCUMENTS_TABLE_SQL).await?;
        tracing::debug!("[CoreSchemaModule] documents table created");

        backend.execute_ddl(Self::DIRECTORIES_TABLE_SQL).await?;
        tracing::debug!("[CoreSchemaModule] directories table created");

        backend.execute_ddl(Self::BLOCKS_PARENT_INDEX_SQL).await?;
        tracing::debug!("[CoreSchemaModule] blocks parent_id index created");

        backend
            .execute_ddl(Self::DOCUMENTS_PARENT_INDEX_SQL)
            .await?;
        tracing::debug!("[CoreSchemaModule] documents parent_id index created");

        backend.execute_ddl(Self::DOCUMENTS_NAME_INDEX_SQL).await?;
        tracing::debug!("[CoreSchemaModule] documents name index created");

        backend
            .execute_ddl(Self::DIRECTORIES_PARENT_INDEX_SQL)
            .await?;
        tracing::debug!("[CoreSchemaModule] directories parent_id index created");

        backend.execute_ddl(Self::FILES_TABLE_SQL).await?;
        tracing::debug!("[CoreSchemaModule] files table created");

        backend.execute_ddl(Self::FILES_PARENT_INDEX_SQL).await?;
        backend.execute_ddl(Self::FILES_DOCUMENT_INDEX_SQL).await?;
        tracing::debug!("[CoreSchemaModule] files indexes created");

        tracing::info!("[CoreSchemaModule] Core tables created successfully");
        Ok(())
    }
}

/// Block hierarchy schema module providing the blocks_with_paths materialized view.
///
/// This view computes hierarchical paths using a recursive CTE, enabling
/// efficient ancestor/descendant queries via path prefix matching.
pub struct BlockHierarchySchemaModule;

impl BlockHierarchySchemaModule {
    /// SQL for creating the blocks_with_paths materialized view.
    ///
    /// Uses recursive CTE to compute paths like "/ancestor/parent/block_id".
    const BLOCKS_WITH_PATHS_SQL: &'static str = r#"
        CREATE MATERIALIZED VIEW IF NOT EXISTS blocks_with_paths AS
        WITH RECURSIVE paths AS (
            -- Base case: root blocks (those whose parent is a document, not another block)
            SELECT
                id,
                parent_id,
                content,
                content_type,
                source_language,
                source_name,
                properties,
                created_at,
                updated_at,
                '/' || id as path
            FROM blocks
            WHERE parent_id LIKE 'holon-doc://%'
               OR parent_id = '__no_parent__'

            UNION ALL

            -- Recursive case: build path from parent
            SELECT
                b.id,
                b.parent_id,
                b.content,
                b.content_type,
                b.source_language,
                b.source_name,
                b.properties,
                b.created_at,
                b.updated_at,
                p.path || '/' || b.id as path
            FROM blocks b
            INNER JOIN paths p ON b.parent_id = p.id
        )
        SELECT * FROM paths
    "#;
}

#[async_trait]
impl SchemaModule for BlockHierarchySchemaModule {
    fn name(&self) -> &str {
        "block_hierarchy"
    }

    fn provides(&self) -> Vec<Resource> {
        vec![Resource::schema("blocks_with_paths")]
    }

    fn requires(&self) -> Vec<Resource> {
        vec![Resource::schema("blocks")]
    }

    async fn ensure_schema(&self, backend: &TursoBackend) -> Result<()> {
        tracing::info!("[BlockHierarchySchemaModule] Creating blocks_with_paths view");
        backend.execute_ddl(Self::BLOCKS_WITH_PATHS_SQL).await?;
        tracing::info!("[BlockHierarchySchemaModule] blocks_with_paths view created");
        Ok(())
    }
}

/// Navigation schema module providing tables for navigation state persistence.
///
/// Provides:
/// - navigation_history: Back/forward history
/// - navigation_cursor: Current position in history per region
/// - current_focus: Materialized view for efficient focus lookups
pub struct NavigationSchemaModule;

impl NavigationSchemaModule {
    const NAVIGATION_HISTORY_SQL: &'static str = r#"
        CREATE TABLE IF NOT EXISTS navigation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT NOT NULL,
            block_id TEXT,
            timestamp TEXT DEFAULT (datetime('now'))
        )
    "#;

    const NAVIGATION_HISTORY_INDEX_SQL: &'static str = r#"
        CREATE INDEX IF NOT EXISTS idx_navigation_history_region
        ON navigation_history(region)
    "#;

    const NAVIGATION_CURSOR_SQL: &'static str = r#"
        CREATE TABLE IF NOT EXISTS navigation_cursor (
            region TEXT PRIMARY KEY,
            history_id INTEGER REFERENCES navigation_history(id)
        )
    "#;

    const DROP_CURRENT_FOCUS_SQL: &'static str = r#"
        DROP VIEW IF EXISTS current_focus
    "#;

    const CURRENT_FOCUS_SQL: &'static str = r#"
        CREATE MATERIALIZED VIEW current_focus AS
        SELECT
            nc.region,
            nh.block_id,
            nh.timestamp
        FROM navigation_cursor nc
        JOIN navigation_history nh ON nc.history_id = nh.id
    "#;
}

#[async_trait]
impl SchemaModule for NavigationSchemaModule {
    fn name(&self) -> &str {
        "navigation"
    }

    fn provides(&self) -> Vec<Resource> {
        vec![
            Resource::schema("navigation_history"),
            Resource::schema("navigation_cursor"),
            Resource::schema("current_focus"),
        ]
    }

    fn requires(&self) -> Vec<Resource> {
        vec![] // No dependencies on other modules
    }

    async fn ensure_schema(&self, backend: &TursoBackend) -> Result<()> {
        tracing::info!("[NavigationSchemaModule] Creating navigation tables");

        backend.execute_ddl(Self::NAVIGATION_HISTORY_SQL).await?;
        backend
            .execute_ddl(Self::NAVIGATION_HISTORY_INDEX_SQL)
            .await?;
        backend.execute_ddl(Self::NAVIGATION_CURSOR_SQL).await?;

        // Drop and recreate the view to handle schema changes
        backend.execute_ddl(Self::DROP_CURRENT_FOCUS_SQL).await?;
        backend.execute_ddl(Self::CURRENT_FOCUS_SQL).await?;

        tracing::info!("[NavigationSchemaModule] Navigation tables created");
        Ok(())
    }

    async fn initialize_data(&self, backend: &TursoBackend) -> Result<()> {
        tracing::info!("[NavigationSchemaModule] Initializing default regions");

        for region in &["main", "left_sidebar", "right_sidebar"] {
            let mut params = HashMap::new();
            params.insert(
                "region".to_string(),
                holon_api::Value::String(region.to_string()),
            );

            backend
                .execute_sql(
                    "INSERT OR IGNORE INTO navigation_cursor (region, history_id) VALUES ($region, NULL)",
                    params,
                )
                .await?;
        }

        tracing::info!("[NavigationSchemaModule] Default regions initialized");
        Ok(())
    }
}

/// Sync state schema module for tracking synchronization tokens.
pub struct SyncStateSchemaModule;

impl SyncStateSchemaModule {
    const SYNC_STATES_SQL: &'static str = r#"
        CREATE TABLE IF NOT EXISTS sync_states (
            provider_name TEXT PRIMARY KEY,
            sync_token TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    "#;
}

#[async_trait]
impl SchemaModule for SyncStateSchemaModule {
    fn name(&self) -> &str {
        "sync_state"
    }

    fn provides(&self) -> Vec<Resource> {
        vec![Resource::schema("sync_states")]
    }

    fn requires(&self) -> Vec<Resource> {
        vec![]
    }

    async fn ensure_schema(&self, backend: &TursoBackend) -> Result<()> {
        tracing::info!("[SyncStateSchemaModule] Creating sync_states table");
        backend.execute_ddl(Self::SYNC_STATES_SQL).await?;
        tracing::info!("[SyncStateSchemaModule] sync_states table created");
        Ok(())
    }
}

/// Operations schema module for undo/redo persistence.
/// NOTE: This schema MUST match the OperationLogEntry entity in holon-core/src/operation_log.rs
pub struct OperationsSchemaModule;

impl OperationsSchemaModule {
    const OPERATIONS_SQL: &'static str = r#"
        CREATE TABLE IF NOT EXISTS operations (
            id INTEGER PRIMARY KEY NOT NULL,
            operation TEXT NOT NULL,
            inverse TEXT,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            display_name TEXT NOT NULL,
            entity_name TEXT NOT NULL,
            op_name TEXT NOT NULL,
            _change_origin TEXT
        )
    "#;

    const OPERATIONS_ENTITY_NAME_INDEX_SQL: &'static str = r#"
        CREATE INDEX IF NOT EXISTS idx_operations_entity_name
        ON operations(entity_name)
    "#;

    const OPERATIONS_CREATED_AT_INDEX_SQL: &'static str = r#"
        CREATE INDEX IF NOT EXISTS idx_operations_created_at
        ON operations(created_at)
    "#;
}

#[async_trait]
impl SchemaModule for OperationsSchemaModule {
    fn name(&self) -> &str {
        "operations"
    }

    fn provides(&self) -> Vec<Resource> {
        vec![Resource::schema("operations")]
    }

    fn requires(&self) -> Vec<Resource> {
        vec![]
    }

    async fn ensure_schema(&self, backend: &TursoBackend) -> Result<()> {
        tracing::info!("[OperationsSchemaModule] Creating operations table");
        backend.execute_ddl(Self::OPERATIONS_SQL).await?;
        backend
            .execute_ddl(Self::OPERATIONS_ENTITY_NAME_INDEX_SQL)
            .await?;
        backend
            .execute_ddl(Self::OPERATIONS_CREATED_AT_INDEX_SQL)
            .await?;
        tracing::info!("[OperationsSchemaModule] operations table created");
        Ok(())
    }
}

/// Creates a SchemaRegistry with all core schema modules registered.
///
/// This is the standard configuration for Holon applications.
/// Modules are registered (but not yet initialized) in this function.
///
/// # Example
///
/// ```rust,ignore
/// let registry = create_core_schema_registry();
/// registry.initialize_all(backend, scheduler_handle, vec![]).await?;
/// ```
pub fn create_core_schema_registry() -> super::schema_module::SchemaRegistry {
    let mut registry = super::schema_module::SchemaRegistry::new();

    // Register all core modules
    registry.register(Arc::new(CoreSchemaModule));
    registry.register(Arc::new(BlockHierarchySchemaModule));
    registry.register(Arc::new(NavigationSchemaModule));
    registry.register(Arc::new(SyncStateSchemaModule));
    registry.register(Arc::new(OperationsSchemaModule));

    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_schema_module_provides() {
        let module = CoreSchemaModule;
        let provides = module.provides();

        assert!(provides.contains(&Resource::schema("blocks")));
        assert!(provides.contains(&Resource::schema("documents")));
        assert!(provides.contains(&Resource::schema("directories")));
        assert!(provides.contains(&Resource::schema("files")));
    }

    #[test]
    fn test_block_hierarchy_requires_blocks() {
        let module = BlockHierarchySchemaModule;
        let requires = module.requires();

        assert!(requires.contains(&Resource::schema("blocks")));
    }

    #[test]
    fn test_core_registry_ordering() {
        let registry = create_core_schema_registry();

        // Should have 5 modules
        assert_eq!(registry.len(), 5);

        // Topological sort should succeed (no cycles)
        // Note: We can't test the actual order without accessing private methods,
        // but we can verify it doesn't panic
    }
}
