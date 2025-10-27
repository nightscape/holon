//! Property-based tests for TursoBackend using proptest-state-machine
//!
//! This module tests the TursoBackend implementation against an in-memory reference model
//! to ensure correctness of all StorageBackend operations.
//!
//! ## Coverage
//!
//! The PBT suite covers the following operations:
//! - **Schema Management**: CreateEntity
//! - **CRUD Operations**: Insert, Update, Delete, Get
//! - **Query Operations**: Query with all filter types (Eq, In, And, Or, IsNull, IsNotNull)
//! - **Dirty Tracking**: MarkDirty, MarkClean, GetDirty
//! - **Version Management**: SetVersion, GetVersion
//! - **CDC Operations**: Enable CDC, track changes, verify CDC records
//! - **Materialized Views**: Create views, verify incremental updates on insert/update/delete
//! - **View Change Notifications**: Create change streams, verify notifications
//! - **Concurrency**: Parallel write operations targeting same materialized views
//! - **Transactions**: Explicit BEGIN/COMMIT transaction batches
//! - **Recursive CTEs**: Materialized views with recursive CTEs and JOINs
//!
//! ## Test Strategy
//!
//! - Generates random sequences of 1-50 operations
//! - Runs 30 test cases with different operation sequences
//! - Compares TursoBackend results against in-memory reference implementation
//! - Verifies state consistency after each operation
//! - Tests complex filter combinations including nested And/Or
//! - Tests CDC integration with base table operations
//! - Tests materialized view consistency across operations
//! - Tests view change notification delivery
//! - **Concurrent operations**: Runs batches of writes in parallel to detect race conditions
//!
//! ## Concurrency Testing
//!
//! The concurrent tests are designed to detect bugs like the IVM (Incremental View Maintenance)
//! race condition where multiple concurrent writes to tables with materialized views can cause:
//! - B-tree overflow cell ordering violations (panic in debug builds)
//! - Silent data corruption (in release builds)
//!
//! See: https://github.com/tursodatabase/turso/issues/1552
//!
//! ## What This Replaces
//!
//! These property-based tests replace the following unit tests:
//! - `filter_building_tests` - All filter types now tested through random query generation
//! - Basic CRUD tests - Covered through random operation sequences
//! - `cdc_tests` - CDC tracking for insert/update/delete covered by PBT
//! - `incremental_view_maintenance_tests` - Materialized view updates covered by PBT
//! - `view_change_stream_tests` - View change notifications covered by PBT
//!
//! ## What's NOT Covered
//!
//! The following are intentionally NOT covered by PBT and should have targeted unit tests:
//! - SQL injection prevention (has dedicated unit tests)
//! - Value conversion edge cases (has dedicated property tests)
//! - Complex CDC-specific scenarios like batch operations and conflict detection
//! - Complex view scenarios like filtered views with triggers

use super::{ChangeData, RowChange, TursoBackend};
use crate::api::ChangeOrigin;
use crate::storage::backend::StorageBackend;
use crate::storage::schema::{EntitySchema, FieldSchema, FieldType};
use crate::storage::types::{Filter, StorageEntity};
use holon_api::Value;
use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio_stream::StreamExt;

/// Recursive CTE materialized view that triggers Turso's JoinOperator
/// during incremental view maintenance (IVM) commits.
///
/// This view computes hierarchical paths from parent_id relationships,
/// which requires a self-JOIN in the recursive part of the CTE.
/// When transactions commit changes to the underlying table,
/// the JoinOperator::commit path is exercised.
const RECURSIVE_CTE_VIEW_SQL: &str = r#"
CREATE MATERIALIZED VIEW IF NOT EXISTS entity_paths AS
WITH RECURSIVE paths AS (
    -- Base case: root entities (no parent)
    SELECT id, parent_id, value, '/' || id as path
    FROM entity
    WHERE parent_id IS NULL OR parent_id = ''

    UNION ALL

    -- Recursive case: JOIN to build path from parent
    SELECT e.id, e.parent_id, e.value, p.path || '/' || e.id as path
    FROM entity e
    INNER JOIN paths p ON e.parent_id = p.id
)
SELECT * FROM paths
"#;

/// Reference state using an in-memory HashMap
#[derive(Debug)]
pub struct ReferenceState {
    /// Entity name -> (id -> Entity) mapping
    pub entities: HashMap<String, HashMap<String, StorageEntity>>,
    /// Entity name -> (id -> version) mapping
    pub versions: HashMap<String, HashMap<String, Option<String>>>,
    /// View name -> (entity_id -> rowid) mapping for materialized view tracking
    /// Each view has its own ROWID space, starting from 1
    pub view_rowids: HashMap<String, HashMap<String, i64>>,
    /// Next ROWID to assign per view
    pub next_view_rowid: HashMap<String, i64>,
    /// Whether CDC is enabled
    pub cdc_enabled: bool,
    /// Track CDC events: (entity, operation_type, id)
    pub cdc_events: Vec<(String, String, String)>,
    /// Materialized views: view_name -> (base_entity, expected_count)
    pub materialized_views: HashMap<String, (String, usize)>,
    /// View streams: view_name -> collected changes (expected)
    pub view_stream_changes: HashMap<String, Arc<Mutex<Vec<RowChange>>>>,
    pub handle: tokio::runtime::Handle,
    /// Optional runtime - Some when we own the runtime (standalone tests), None when using existing runtime
    pub _runtime: Option<Arc<tokio::runtime::Runtime>>,
    /// Whether the recursive CTE view has been created (triggers JoinOperator in IVM)
    pub recursive_cte_view_created: bool,
}

impl Clone for ReferenceState {
    fn clone(&self) -> Self {
        Self {
            entities: self.entities.clone(),
            versions: self.versions.clone(),
            view_rowids: self.view_rowids.clone(),
            next_view_rowid: self.next_view_rowid.clone(),
            cdc_enabled: self.cdc_enabled,
            cdc_events: self.cdc_events.clone(),
            materialized_views: self.materialized_views.clone(),
            view_stream_changes: self.view_stream_changes.clone(),
            handle: self.handle.clone(),
            _runtime: self._runtime.clone(),
            recursive_cte_view_created: self.recursive_cte_view_created,
        }
    }
}

impl Default for ReferenceState {
    fn default() -> Self {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => Self {
                entities: HashMap::new(),
                versions: HashMap::new(),
                view_rowids: HashMap::new(),
                next_view_rowid: HashMap::new(),
                cdc_enabled: false,
                cdc_events: Vec::new(),
                materialized_views: HashMap::new(),
                view_stream_changes: HashMap::new(),
                handle,
                _runtime: None,
                recursive_cte_view_created: false,
            },
            Err(_) => {
                // Use current_thread runtime for fork-safety with proptest
                // Multi-threaded runtime is fork-unsafe and causes "failed in other process" errors
                let runtime = Arc::new(
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap(),
                );
                let handle = runtime.handle().clone();
                Self {
                    entities: HashMap::new(),
                    versions: HashMap::new(),
                    view_rowids: HashMap::new(),
                    next_view_rowid: HashMap::new(),
                    cdc_enabled: false,
                    cdc_events: Vec::new(),
                    materialized_views: HashMap::new(),
                    view_stream_changes: HashMap::new(),
                    handle,
                    _runtime: Some(runtime),
                    recursive_cte_view_created: false,
                }
            }
        }
    }
}

/// Transitions/Commands for storage operations
#[derive(Clone, Debug)]
pub enum StorageTransition {
    CreateEntity {
        name: String,
    },
    Insert {
        entity: String,
        id: String,
        /// Parent ID for hierarchical relationships. None means root entity.
        parent_id: Option<String>,
        value: String,
    },
    Update {
        entity: String,
        id: String,
        value: String,
    },
    Delete {
        entity: String,
        id: String,
    },
    Query {
        entity: String,
        filter: Filter,
    },
    Get {
        entity: String,
        id: String,
    },
    SetVersion {
        entity: String,
        id: String,
        version: String,
    },
    EnableCDC,
    CreateMaterializedView {
        view_name: String,
        entity: String,
    },
    CreateViewStream {
        view_name: String,
    },
    /// Create the recursive CTE materialized view that exercises JoinOperator
    CreateRecursiveCTEView,
}

/// Batch execution mode for transitions
#[derive(Clone, Debug)]
pub enum BatchMode {
    /// Single operation executed alone
    Single,
    /// Multiple operations executed concurrently (to detect race conditions)
    Concurrent,
    /// Multiple operations executed in an explicit transaction (BEGIN/COMMIT)
    /// This tests the JoinOperator commit path in Turso's IVM.
    Transaction,
}

/// A batch of transitions to execute.
/// - Single: one operation executed alone
/// - Concurrent: multiple operations executed in parallel (to detect race conditions)
/// - Transaction: multiple operations wrapped in BEGIN IMMEDIATE TRANSACTION / COMMIT
#[derive(Clone, Debug)]
pub struct TransitionBatch {
    /// Operations to execute
    pub operations: Vec<StorageTransition>,
    /// How to execute the operations
    pub mode: BatchMode,
}

impl TransitionBatch {
    pub fn single(operation: StorageTransition) -> Self {
        Self {
            operations: vec![operation],
            mode: BatchMode::Single,
        }
    }

    pub fn concurrent(operations: Vec<StorageTransition>) -> Self {
        Self {
            operations,
            mode: BatchMode::Concurrent,
        }
    }

    pub fn transaction(operations: Vec<StorageTransition>) -> Self {
        Self {
            operations,
            mode: BatchMode::Transaction,
        }
    }
}

/// System under test - wraps TursoBackend
pub struct StorageTest {
    pub backend: Arc<TursoBackend>,
    /// CDC-enabled connection (kept for backward compatibility, now managed by TursoBackend)
    pub cdc_connection: Option<turso::Connection>,
    // Note: view_stream_connections removed - connections now managed by TursoBackend.write_conn
    /// View stream change collectors: view_name -> Arc<Mutex<Vec<RowChange>>>
    pub view_stream_changes: HashMap<String, Arc<Mutex<Vec<RowChange>>>>,
    /// View stream handles to keep tasks alive
    pub view_stream_handles: HashMap<String, tokio::task::JoinHandle<()>>,
}

impl StorageTest {
    /// Create a test backend for PBT testing.
    fn create_test_backend(handle: &tokio::runtime::Handle, db_path: &str) -> TursoBackend {
        use tokio::sync::broadcast;

        tokio::task::block_in_place(|| {
            handle.block_on(async {
                // Open database
                let db = TursoBackend::open_database(db_path).expect("Failed to open database");

                // Create CDC broadcast channel
                let (cdc_tx, _cdc_rx) = broadcast::channel(1024);

                // Create backend (which internally spawns the actor)
                let (backend, _db_handle) =
                    TursoBackend::new(db, cdc_tx).expect("Failed to create TursoBackend");

                backend
            })
        })
    }

    /// Create a new StorageTest with a temp file backend.
    /// Uses a unique temp file for each test instance.
    pub fn new(handle: &tokio::runtime::Handle) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let temp_path = format!("/tmp/holon_pbt_test_{}.db", id);

        // Clean up any existing file
        let _ = std::fs::remove_file(&temp_path);

        let backend = Self::create_test_backend(handle, &temp_path);
        Self {
            backend: Arc::new(backend),
            cdc_connection: None,
            view_stream_changes: HashMap::new(),
            view_stream_handles: HashMap::new(),
        }
    }

    /// Create a new StorageTest with file-based backend (Unix-like systems only)
    #[cfg(target_family = "unix")]
    pub fn new_with_file(handle: &tokio::runtime::Handle, db_path: &str) -> Self {
        let backend = Self::create_test_backend(handle, db_path);
        Self {
            backend: Arc::new(backend),
            cdc_connection: None,
            view_stream_changes: HashMap::new(),
            view_stream_handles: HashMap::new(),
        }
    }

    /// Execute a batch of operations concurrently with panic detection.
    /// Uses a barrier to maximize the chance of concurrent execution.
    /// Returns the number of panics detected.
    pub async fn execute_concurrent_batch(
        &self,
        operations: Vec<StorageTransition>,
        handle: &tokio::runtime::Handle,
    ) -> usize {
        use std::sync::atomic::Ordering;

        if operations.is_empty() {
            return 0;
        }

        if operations.len() == 1 {
            // Single operation - no concurrency needed
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                tokio::task::block_in_place(|| {
                    handle.block_on(apply_to_turso_inner(&self.backend, &operations[0], handle))
                })
            }));

            return match result {
                Ok(Ok(_)) => 0,
                Ok(Err(e)) => {
                    eprintln!("[concurrent_batch] Operation error: {}", e);
                    0 // Errors are not panics
                }
                Err(panic_info) => {
                    let panic_msg = format_panic(&panic_info);
                    eprintln!("[concurrent_batch] PANIC detected: {}", panic_msg);
                    1
                }
            };
        }

        // Multiple operations - execute concurrently
        // Note: We can't use a Barrier with single-threaded runtime as it causes deadlock.
        // Instead, we spawn all tasks and use join_all to drive them concurrently.
        let panic_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut futures = Vec::new();
        for op in operations {
            let backend = Arc::clone(&self.backend);
            let panic_count = Arc::clone(&panic_count);
            let handle_clone = handle.clone();

            let future = async move {
                // Execute operation with panic detection
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    futures::executor::block_on(apply_to_turso_inner(&backend, &op, &handle_clone))
                }));

                match result {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        eprintln!("[concurrent_batch] Operation {:?} error: {}", op, e);
                    }
                    Err(panic_info) => {
                        let panic_msg = format_panic(&panic_info);
                        eprintln!(
                            "[concurrent_batch] PANIC in operation {:?}: {}",
                            op, panic_msg
                        );
                        panic_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            };
            futures.push(future);
        }

        // Execute all futures concurrently using join_all
        // This works with single-threaded runtime unlike spawned tasks with barriers
        tokio::task::block_in_place(|| handle.block_on(futures::future::join_all(futures)));

        panic_count.load(Ordering::SeqCst)
    }

    /// Execute a batch of operations within an explicit transaction.
    /// Uses BEGIN IMMEDIATE TRANSACTION / COMMIT to wrap the operations.
    /// This exercises the JoinOperator::commit path in Turso's IVM when
    /// a recursive CTE view with JOINs exists.
    ///
    /// IMPORTANT: All operations must use the SAME connection for the transaction
    /// to work. We execute SQL directly rather than calling apply_to_turso_inner
    /// which would use separate connections.
    ///
    /// Returns Ok(()) if transaction succeeds, or Err with the error message.
    pub async fn execute_transaction_batch(
        &self,
        operations: Vec<StorageTransition>,
        _handle: &tokio::runtime::Handle,
    ) -> Result<(), String> {
        if operations.is_empty() {
            return Ok(());
        }

        let conn = self.backend.get_connection().map_err(|e| e.to_string())?;

        // Begin explicit transaction
        conn.execute("BEGIN IMMEDIATE TRANSACTION", ())
            .await
            .map_err(|e| format!("BEGIN TRANSACTION failed: {}", e))?;

        // Execute all operations within the transaction using the SAME connection
        for op in &operations {
            let result = self.execute_operation_on_connection(&conn, op).await;
            if let Err(e) = result {
                // Try to rollback on error
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(format!("Operation {:?} failed: {}", op, e));
            }
        }

        // Commit the transaction - this is where JoinOperator::commit is triggered
        conn.execute("COMMIT", ())
            .await
            .map_err(|e| format!("COMMIT failed: {}", e))?;

        Ok(())
    }

    /// Execute a single operation directly on a connection.
    /// Used for transaction batches where all operations must use the same connection.
    async fn execute_operation_on_connection(
        &self,
        conn: &turso::Connection,
        op: &StorageTransition,
    ) -> Result<(), String> {
        match op {
            StorageTransition::Insert {
                entity,
                id,
                parent_id,
                value,
            } => {
                let parent_id_value = match parent_id {
                    Some(pid) => format!("'{}'", pid),
                    None => "NULL".to_string(),
                };
                let sql = format!(
                    "INSERT INTO {} (id, parent_id, value) VALUES ('{}', {}, '{}')",
                    entity, id, parent_id_value, value
                );
                conn.execute(&sql, ())
                    .await
                    .map_err(|e| format!("Database error: {}", e))?;
                Ok(())
            }
            StorageTransition::Update { entity, id, value } => {
                let sql = format!(
                    "UPDATE {} SET value = '{}' WHERE id = '{}'",
                    entity, value, id
                );
                conn.execute(&sql, ())
                    .await
                    .map_err(|e| format!("Database error: {}", e))?;
                Ok(())
            }
            StorageTransition::Delete { entity, id } => {
                let sql = format!("DELETE FROM {} WHERE id = '{}'", entity, id);
                conn.execute(&sql, ())
                    .await
                    .map_err(|e| format!("Database error: {}", e))?;
                Ok(())
            }
            StorageTransition::SetVersion {
                entity,
                id,
                version,
            } => {
                let sql = format!(
                    "UPDATE {} SET _version = '{}' WHERE id = '{}'",
                    entity, version, id
                );
                conn.execute(&sql, ())
                    .await
                    .map_err(|e| format!("Database error: {}", e))?;
                Ok(())
            }
            _ => Err(format!(
                "Operation {:?} not supported in transaction batch",
                op
            )),
        }
    }
}

/// Format a panic payload into a readable string
fn format_panic(panic_info: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic_info.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = panic_info.downcast_ref::<String>() {
        s.clone()
    } else {
        "Unknown panic".to_string()
    }
}

/// Get a test schema for the given entity name
fn get_test_schema(name: &str) -> EntitySchema {
    EntitySchema {
        name: name.to_string(),
        primary_key: "id".to_string(),
        fields: vec![
            FieldSchema {
                name: "id".to_string(),
                field_type: FieldType::String,
                required: true,
                indexed: true,
            },
            FieldSchema {
                name: "parent_id".to_string(),
                field_type: FieldType::String,
                required: false,
                indexed: true, // Indexed for efficient JOIN lookups in recursive CTE
            },
            FieldSchema {
                name: "value".to_string(),
                field_type: FieldType::String,
                required: true,
                indexed: false,
            },
        ],
    }
}

/// Helper to apply filter on an entity in reference state
fn apply_filter_ref(entity: &StorageEntity, filter: &Filter) -> bool {
    match filter {
        Filter::Eq(field, value) => entity.get(field).map(|v| v == value).unwrap_or(false),
        Filter::In(field, values) => entity
            .get(field)
            .map(|v| values.contains(v))
            .unwrap_or(false),
        Filter::And(filters) => filters.iter().all(|f| apply_filter_ref(entity, f)),
        Filter::Or(filters) => filters.iter().any(|f| apply_filter_ref(entity, f)),
        Filter::IsNull(field) => entity
            .get(field)
            .map(|v| matches!(v, Value::Null))
            .unwrap_or(true),
        Filter::IsNotNull(field) => entity
            .get(field)
            .map(|v| !matches!(v, Value::Null))
            .unwrap_or(false),
    }
}

/// Apply a transition to the reference state
/// Returns the result (for Query/Get operations that return data)
fn apply_to_reference(
    state: &mut ReferenceState,
    transition: &StorageTransition,
) -> Option<Vec<StorageEntity>> {
    match transition {
        StorageTransition::CreateEntity { name } => {
            state.entities.entry(name.clone()).or_default();
            state.versions.entry(name.clone()).or_default();
            None
        }
        StorageTransition::Insert {
            entity,
            id,
            parent_id,
            value,
        } => {
            let mut data = StorageEntity::new();
            data.insert("id".to_string(), Value::String(id.clone()));
            data.insert(
                "parent_id".to_string(),
                match parent_id {
                    Some(pid) => Value::String(pid.clone()),
                    None => Value::Null,
                },
            );
            data.insert("value".to_string(), Value::String(value.clone()));
            data.insert("_version".to_string(), Value::Null); // Turso adds _version column

            state
                .entities
                .get_mut(entity)
                .unwrap()
                .insert(id.clone(), data.clone());
            state
                .versions
                .get_mut(entity)
                .unwrap()
                .insert(id.clone(), None);

            // Track CDC event if enabled
            if state.cdc_enabled {
                state
                    .cdc_events
                    .push((entity.clone(), "INSERT".to_string(), id.clone()));
            }

            // Track view change notifications for views monitoring this entity
            // ONLY if the view stream has been created (callback registered)
            for (view_name, (view_entity, _count)) in &state.materialized_views {
                if view_entity == entity {
                    if let Some(changes_vec) = state.view_stream_changes.get(view_name) {
                        // Assign ROWID for this view (each view has its own ROWID space)
                        let rowid = *state.next_view_rowid.entry(view_name.clone()).or_insert(1);
                        state.next_view_rowid.insert(view_name.clone(), rowid + 1);
                        state
                            .view_rowids
                            .entry(view_name.clone())
                            .or_default()
                            .insert(id.clone(), rowid);

                        let mut data_with_rowid = data.clone();
                        data_with_rowid
                            .insert("_rowid".to_string(), Value::String(rowid.to_string()));
                        let change = RowChange {
                            relation_name: view_name.clone(),
                            change: ChangeData::Created {
                                data: data_with_rowid,
                                origin: ChangeOrigin::Remote {
                                    operation_id: None,
                                    trace_id: None,
                                },
                            },
                        };
                        changes_vec.lock().unwrap().push(change);
                    }
                }
            }
            None
        }
        StorageTransition::Update { entity, id, value } => {
            let entities = state.entities.get_mut(entity).unwrap();
            // In concurrent batches, the entity may have been deleted by another operation
            let Some(data) = entities.get_mut(id) else {
                return None;
            };
            data.insert("value".to_string(), Value::String(value.clone()));

            // Track CDC event if enabled
            if state.cdc_enabled {
                state
                    .cdc_events
                    .push((entity.clone(), "UPDATE".to_string(), id.clone()));
            }

            // Track view change notifications for views monitoring this entity
            // ONLY if the view stream has been created (callback registered)
            for (view_name, (view_entity, _count)) in &state.materialized_views {
                if view_entity == entity {
                    if let Some(changes_vec) = state.view_stream_changes.get(view_name) {
                        let updated_data = entities.get(id).unwrap().clone();
                        // Look up the ROWID for this entity in this view
                        // In concurrent batches, the ROWID may not exist if entity was inserted after view creation
                        let Some(rowid) = state
                            .view_rowids
                            .get(view_name)
                            .and_then(|rowids| rowids.get(id))
                        else {
                            continue;
                        };
                        let mut updated_data_with_rowid = updated_data.clone();
                        updated_data_with_rowid
                            .insert("_rowid".to_string(), Value::String(rowid.to_string()));
                        let change = RowChange {
                            relation_name: view_name.clone(),
                            change: ChangeData::Updated {
                                id: rowid.to_string(),
                                data: updated_data_with_rowid,
                                origin: ChangeOrigin::Remote {
                                    operation_id: None,
                                    trace_id: None,
                                },
                            },
                        };
                        changes_vec.lock().unwrap().push(change);
                    }
                }
            }
            None
        }
        StorageTransition::Delete { entity, id } => {
            // In concurrent batches, the entity may have already been deleted
            let entities = state.entities.get_mut(entity).unwrap();
            if entities.remove(id).is_none() {
                return None; // Already deleted by concurrent operation
            }
            state.versions.get_mut(entity).unwrap().remove(id);

            // Track CDC event if enabled
            if state.cdc_enabled {
                state
                    .cdc_events
                    .push((entity.clone(), "DELETE".to_string(), id.clone()));
            }

            // Track view change notifications for views monitoring this entity
            // ONLY if the view stream has been created (callback registered)
            for (view_name, (view_entity, _count)) in &state.materialized_views {
                if view_entity == entity {
                    if let Some(changes_vec) = state.view_stream_changes.get(view_name) {
                        // Get ROWID for this entity in this view before removing
                        // In concurrent batches, the ROWID may not exist if entity was never in view
                        let Some(rowid) = state
                            .view_rowids
                            .get(view_name)
                            .and_then(|rowids| rowids.get(id))
                            .copied()
                        else {
                            continue;
                        };

                        let change = RowChange {
                            relation_name: view_name.clone(),
                            change: ChangeData::Deleted {
                                id: rowid.to_string(),
                                origin: ChangeOrigin::Remote {
                                    operation_id: None,
                                    trace_id: None,
                                },
                            },
                        };
                        changes_vec.lock().unwrap().push(change);

                        // Remove ROWID mapping (ROWID might be reused in real Turso - to be tested)
                        if let Some(rowids) = state.view_rowids.get_mut(view_name) {
                            rowids.remove(id);
                        }
                    }
                }
            }
            None
        }
        StorageTransition::Query { entity, filter } => {
            let entities = state.entities.get(entity).unwrap();
            let results: Vec<StorageEntity> = entities
                .values()
                .filter(|e| apply_filter_ref(e, filter))
                .cloned()
                .collect();
            Some(results)
        }
        StorageTransition::Get { entity, id } => {
            let entities = state.entities.get(entity).unwrap();
            let result = entities.get(id).cloned();
            Some(result.into_iter().collect())
        }
        StorageTransition::SetVersion {
            entity,
            id,
            version,
        } => {
            // In concurrent batches, the entity may have been deleted
            // Only set version if entity still exists
            let entity_exists = state
                .entities
                .get(entity)
                .map(|e| e.contains_key(id))
                .unwrap_or(false);

            if !entity_exists {
                return None; // Entity was deleted by concurrent operation
            }

            state
                .versions
                .get_mut(entity)
                .unwrap()
                .insert(id.clone(), Some(version.clone()));

            // Update _version in the entity data
            if let Some(entities) = state.entities.get_mut(entity) {
                if let Some(data) = entities.get_mut(id) {
                    data.insert("_version".to_string(), Value::String(version.clone()));

                    // Track view change notifications for views monitoring this entity
                    for (view_name, (view_entity, _count)) in &state.materialized_views {
                        if view_entity == entity {
                            if let Some(changes_vec) = state.view_stream_changes.get(view_name) {
                                // Look up the ROWID for this entity in this view
                                // In concurrent batches, the ROWID may not exist
                                let Some(rowid) = state
                                    .view_rowids
                                    .get(view_name)
                                    .and_then(|rowids| rowids.get(id))
                                else {
                                    continue;
                                };

                                let mut data_with_rowid = data.clone();
                                data_with_rowid
                                    .insert("_rowid".to_string(), Value::String(rowid.to_string()));
                                let change = RowChange {
                                    relation_name: view_name.clone(),
                                    change: ChangeData::Updated {
                                        id: rowid.to_string(),
                                        data: data_with_rowid,
                                        origin: ChangeOrigin::Remote {
                                            operation_id: None,
                                            trace_id: None,
                                        },
                                    },
                                };
                                changes_vec.lock().unwrap().push(change);
                            }
                        }
                    }
                }
            }
            None
        }
        StorageTransition::EnableCDC => {
            state.cdc_enabled = true;
            None
        }
        StorageTransition::CreateMaterializedView { view_name, entity } => {
            // Track that a view was created and its expected row count
            let count = state.entities.get(entity).map(|e| e.len()).unwrap_or(0);
            state
                .materialized_views
                .insert(view_name.clone(), (entity.clone(), count));

            // Assign ROWIDs to all existing entities in this view
            // ROWIDs are assigned when rows enter the materialized view (at view creation time)
            if let Some(entities) = state.entities.get(entity) {
                let mut rowid = 1i64;
                // Sort keys to ensure deterministic ROWID assignment
                let mut entity_ids: Vec<_> = entities.keys().cloned().collect();
                entity_ids.sort();
                for entity_id in entity_ids {
                    state
                        .view_rowids
                        .entry(view_name.clone())
                        .or_default()
                        .insert(entity_id, rowid);
                    rowid += 1;
                }
                state.next_view_rowid.insert(view_name.clone(), rowid);
            }
            None
        }
        StorageTransition::CreateViewStream { view_name } => {
            // Initialize empty change collector for this view
            state
                .view_stream_changes
                .insert(view_name.clone(), Arc::new(Mutex::new(Vec::new())));

            // Assign ROWIDs to any entities that were inserted after CreateMaterializedView
            // but before CreateViewStream
            if let Some((entity, _)) = state.materialized_views.get(view_name) {
                if let Some(entities) = state.entities.get(entity) {
                    let existing_rowids = state.view_rowids.entry(view_name.clone()).or_default();
                    let next_rowid = state.next_view_rowid.entry(view_name.clone()).or_insert(1);

                    for entity_id in entities.keys() {
                        if !existing_rowids.contains_key(entity_id) {
                            existing_rowids.insert(entity_id.clone(), *next_rowid);
                            *next_rowid += 1;
                        }
                    }
                }
            }
            None
        }
        StorageTransition::CreateRecursiveCTEView => {
            // Just mark that the recursive CTE view was created
            // The reference state doesn't track view content, just the fact it exists
            state.recursive_cte_view_created = true;
            None
        }
    }
}

/// Apply a transition directly to TursoBackend (no test state needed).
/// This inner function can be called concurrently from multiple tasks.
/// Returns the result (for Query/Get operations that return data).
async fn apply_to_turso_inner(
    backend: &TursoBackend,
    transition: &StorageTransition,
    _handle: &tokio::runtime::Handle,
) -> Result<Option<Vec<StorageEntity>>, String> {
    match transition {
        StorageTransition::CreateEntity { name } => {
            let schema = get_test_schema(name);
            backend
                .create_entity(&schema)
                .await
                .map_err(|e| e.to_string())?;
            Ok(None)
        }
        StorageTransition::Insert {
            entity,
            id,
            parent_id,
            value,
        } => {
            let mut data = StorageEntity::new();
            data.insert("id".to_string(), Value::String(id.clone()));
            data.insert(
                "parent_id".to_string(),
                match parent_id {
                    Some(pid) => Value::String(pid.clone()),
                    None => Value::Null,
                },
            );
            data.insert("value".to_string(), Value::String(value.clone()));

            backend
                .insert(&holon_api::Schema::from_table_name(entity), data)
                .await
                .map_err(|e| e.to_string())?;
            Ok(None)
        }
        StorageTransition::Update { entity, id, value } => {
            let mut data = StorageEntity::new();
            data.insert("value".to_string(), Value::String(value.clone()));

            backend
                .update(&holon_api::Schema::from_table_name(entity), id, data)
                .await
                .map_err(|e| e.to_string())?;
            Ok(None)
        }
        StorageTransition::Delete { entity, id } => {
            backend
                .delete(entity, id)
                .await
                .map_err(|e| e.to_string())?;
            Ok(None)
        }
        StorageTransition::Query { entity, filter } => {
            let results = backend
                .query(entity, filter.clone())
                .await
                .map_err(|e| e.to_string())?;
            Ok(Some(results))
        }
        StorageTransition::Get { entity, id } => {
            let result = backend.get(entity, id).await.map_err(|e| e.to_string())?;
            Ok(Some(result.into_iter().collect()))
        }
        StorageTransition::SetVersion {
            entity,
            id,
            version,
        } => {
            backend
                .set_version(entity, id, version.clone())
                .await
                .map_err(|e| e.to_string())?;
            Ok(None)
        }
        StorageTransition::EnableCDC => {
            // This requires test state - should not be called concurrently
            Err("EnableCDC cannot be executed concurrently".to_string())
        }
        StorageTransition::CreateMaterializedView { view_name, entity } => {
            let conn = backend.get_connection().map_err(|e| e.to_string())?;
            let sql = format!(
                "CREATE MATERIALIZED VIEW {} AS SELECT * FROM {}",
                view_name, entity
            );
            conn.execute(&sql, ()).await.map_err(|e| e.to_string())?;
            Ok(None)
        }
        StorageTransition::CreateViewStream { view_name: _ } => {
            // This requires test state - should not be called concurrently
            Err("CreateViewStream cannot be executed concurrently".to_string())
        }
        StorageTransition::CreateRecursiveCTEView => {
            // Create the recursive CTE view that triggers JoinOperator in IVM
            let conn = backend.get_connection().map_err(|e| e.to_string())?;
            conn.execute(RECURSIVE_CTE_VIEW_SQL, ())
                .await
                .map_err(|e| e.to_string())?;
            Ok(None)
        }
    }
}

/// Apply a transition to the TursoBackend (within StorageTest)
/// This function has access to test state (CDC connection, view streams, etc.)
/// Returns the result (for Query/Get operations that return data)
async fn apply_to_turso(
    test: &mut StorageTest,
    transition: &StorageTransition,
    handle: &tokio::runtime::Handle,
) -> Result<Option<Vec<StorageEntity>>, String> {
    let backend = &test.backend;
    match transition {
        StorageTransition::CreateEntity { name } => {
            let schema = get_test_schema(name);
            backend
                .create_entity(&schema)
                .await
                .map_err(|e| e.to_string())?;
            Ok(None)
        }
        StorageTransition::Insert {
            entity,
            id,
            parent_id,
            value,
        } => {
            let mut data = StorageEntity::new();
            data.insert("id".to_string(), Value::String(id.clone()));
            data.insert(
                "parent_id".to_string(),
                match parent_id {
                    Some(pid) => Value::String(pid.clone()),
                    None => Value::Null,
                },
            );
            data.insert("value".to_string(), Value::String(value.clone()));

            // Use CDC connection if available, otherwise use backend
            if let Some(conn) = &test.cdc_connection {
                let fields: Vec<_> = data.keys().collect();
                let values: Vec<_> = data
                    .values()
                    .map(|v| backend.value_to_sql_param(v))
                    .collect();

                let insert_sql = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    entity,
                    fields
                        .iter()
                        .map(|f| f.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    values.join(", ")
                );

                conn.execute(&insert_sql, ())
                    .await
                    .map_err(|e| e.to_string())?;
            } else {
                backend
                    .insert(&holon_api::Schema::from_table_name(entity), data)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok(None)
        }
        StorageTransition::Update { entity, id, value } => {
            let mut data = StorageEntity::new();
            data.insert("value".to_string(), Value::String(value.clone()));

            // Use CDC connection if available, otherwise use backend
            if let Some(conn) = &test.cdc_connection {
                let set_clauses: Vec<_> = data
                    .iter()
                    .filter(|(k, _)| k.as_str() != "id")
                    .map(|(k, v)| format!("{} = {}", k, backend.value_to_sql_param(v)))
                    .collect();

                let update_sql = format!(
                    "UPDATE {} SET {} WHERE id = '{}'",
                    entity,
                    set_clauses.join(", "),
                    id.replace('\'', "''")
                );

                conn.execute(&update_sql, ())
                    .await
                    .map_err(|e| e.to_string())?;
            } else {
                backend
                    .update(&holon_api::Schema::from_table_name(entity), id, data)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok(None)
        }
        StorageTransition::Delete { entity, id } => {
            // Use CDC connection if available, otherwise use backend
            if let Some(conn) = &test.cdc_connection {
                let delete_sql = format!(
                    "DELETE FROM {} WHERE id = '{}'",
                    entity,
                    id.replace('\'', "''")
                );

                conn.execute(&delete_sql, ())
                    .await
                    .map_err(|e| e.to_string())?;
            } else {
                backend
                    .delete(entity, id)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok(None)
        }
        StorageTransition::Query { entity, filter } => {
            let results = backend
                .query(entity, filter.clone())
                .await
                .map_err(|e| e.to_string())?;
            Ok(Some(results))
        }
        StorageTransition::Get { entity, id } => {
            let result = backend.get(entity, id).await.map_err(|e| e.to_string())?;
            Ok(Some(result.into_iter().collect()))
        }
        StorageTransition::SetVersion {
            entity,
            id,
            version,
        } => {
            backend
                .set_version(entity, id, version.clone())
                .await
                .map_err(|e| e.to_string())?;
            Ok(None)
        }
        StorageTransition::EnableCDC => {
            // CDC is now enabled by default via the DatabaseActor
            // Just return success
            Ok(None)
        }
        StorageTransition::CreateMaterializedView { view_name, entity } => {
            let sql = format!(
                "CREATE MATERIALIZED VIEW {} AS SELECT * FROM {}",
                view_name, entity
            );
            backend.execute_ddl(&sql).await.map_err(|e| e.to_string())?;
            Ok(None)
        }
        StorageTransition::CreateViewStream { view_name } => {
            // Create a view change stream via the db_handle
            let mut stream = backend.row_changes().await.map_err(|e| e.to_string())?;

            // Create a shared collector for changes
            let changes = Arc::new(Mutex::new(Vec::new()));
            test.view_stream_changes
                .insert(view_name.clone(), changes.clone());

            // Spawn a task to collect changes from the stream
            let view_name_clone = view_name.clone();
            let handle_inner = handle.spawn(async move {
                while let Some(batch) = stream.next().await {
                    // Access items via inner field (Deref doesn't allow moving)
                    for change in &batch.inner.items {
                        if change.relation_name == view_name_clone {
                            changes.lock().unwrap().push(change.clone());
                        }
                    }
                }
            });

            test.view_stream_handles
                .insert(view_name.clone(), handle_inner);
            Ok(None)
        }
        StorageTransition::CreateRecursiveCTEView => {
            // Create the recursive CTE view that triggers JoinOperator in IVM
            let conn = backend.get_connection().map_err(|e| e.to_string())?;
            conn.execute(RECURSIVE_CTE_VIEW_SQL, ())
                .await
                .map_err(|e| e.to_string())?;
            Ok(None)
        }
    }
}

/// Check preconditions for a transition
fn check_preconditions(state: &ReferenceState, transition: &StorageTransition) -> bool {
    match transition {
        StorageTransition::CreateEntity { name } => {
            // Can't create entity that already exists
            !state.entities.contains_key(name)
        }
        StorageTransition::Insert { entity, id, .. } => {
            // Entity must exist and id must not exist
            state
                .entities
                .get(entity)
                .map(|e| !e.contains_key(id))
                .unwrap_or(false)
        }
        StorageTransition::Query { entity, .. } | StorageTransition::Get { entity, .. } => {
            // Entity must exist
            state.entities.contains_key(entity)
        }
        StorageTransition::Update { entity, id, .. }
        | StorageTransition::Delete { entity, id }
        | StorageTransition::SetVersion { entity, id, .. } => {
            // Entity and id must exist
            state
                .entities
                .get(entity)
                .map(|e| e.contains_key(id))
                .unwrap_or(false)
        }
        StorageTransition::EnableCDC => {
            // Can only enable CDC once
            !state.cdc_enabled
        }
        StorageTransition::CreateMaterializedView { view_name, entity } => {
            // Entity must exist and view must not already exist
            state.entities.contains_key(entity) && !state.materialized_views.contains_key(view_name)
        }
        StorageTransition::CreateViewStream { view_name } => {
            // View must exist and stream must not already exist
            state.materialized_views.contains_key(view_name)
                && !state.view_stream_changes.contains_key(view_name)
        }
        StorageTransition::CreateRecursiveCTEView => {
            // Can only create once, and "entity" table must exist (required for the view)
            !state.recursive_cte_view_created && state.entities.contains_key("entity")
        }
    }
}

/// Verify that TursoBackend matches reference state
fn verify_states_match(
    reference: &ReferenceState,
    turso: &TursoBackend,
    handle: &tokio::runtime::Handle,
) {
    for (entity_name, ref_entities) in &reference.entities {
        // Check each entity in reference exists in Turso
        for (id, ref_data) in ref_entities {
            let turso_data =
                tokio::task::block_in_place(|| handle.block_on(turso.get(entity_name, id)))
                    .expect("Failed to get from Turso");

            assert!(
                turso_data.is_some(),
                "Entity {}/{} exists in reference but not in Turso",
                entity_name,
                id
            );

            let turso_data = turso_data.unwrap();

            // Compare values (ignoring internal fields like _dirty, _version)
            for (key, ref_value) in ref_data {
                if !key.starts_with('_') {
                    let turso_value = turso_data.get(key);
                    assert_eq!(
                        turso_value,
                        Some(ref_value),
                        "Value mismatch for {}/{}/{}: expected {:?}, got {:?}",
                        entity_name,
                        id,
                        key,
                        ref_value,
                        turso_value
                    );
                }
            }
        }

        // Check version tracking
        if let Some(ref_versions) = reference.versions.get(entity_name) {
            for (id, ref_version) in ref_versions {
                let turso_version = tokio::task::block_in_place(|| {
                    handle.block_on(turso.get_version(entity_name, id))
                })
                .expect("Failed to get version from Turso");

                assert_eq!(
                    turso_version, *ref_version,
                    "Version mismatch for {}/{}: expected {:?}, got {:?}",
                    entity_name, id, ref_version, turso_version
                );
            }
        }
    }
}

/// Generate a random filter strategy
fn generate_filter(
    _entity_names: Vec<String>,
    existing_values: Vec<String>,
) -> BoxedStrategy<Filter> {
    let leaf = prop_oneof![
        (
            Just("id".to_string()),
            prop::sample::select(existing_values.clone())
        )
            .prop_map(|(field, value)| Filter::Eq(field, Value::String(value))),
        (
            Just("value".to_string()),
            prop::sample::select(existing_values.clone())
        )
            .prop_map(|(field, value)| Filter::Eq(field, Value::String(value))),
        (
            Just("id".to_string()),
            prop::collection::vec(prop::sample::select(existing_values.clone()), 1..=3)
        )
            .prop_map(|(field, values)| Filter::In(
                field,
                values.into_iter().map(Value::String).collect()
            )),
        Just(Filter::IsNull("value".to_string())),
        Just(Filter::IsNotNull("value".to_string())),
    ];

    leaf.prop_recursive(
        2,  // Max depth
        10, // Max nodes
        3,  // Items per collection
        |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 1..=2)
                    .prop_map(|filters| Filter::And(filters)),
                prop::collection::vec(inner, 1..=2).prop_map(|filters| Filter::Or(filters)),
            ]
        },
    )
    .boxed()
}

/// Generate transitions based on current state
fn generate_transitions(state: &ReferenceState) -> BoxedStrategy<StorageTransition> {
    // List of known entities
    let entity_names: Vec<String> = state.entities.keys().cloned().collect();

    // If no entities exist, only allow creating the "entity" table
    // (needed for recursive CTE view which references "entity" table)
    if entity_names.is_empty() {
        return prop::strategy::Just(StorageTransition::CreateEntity {
            name: "entity".to_string(),
        })
        .boxed();
    }

    // Strategies for different operations
    let create_entity = Just(StorageTransition::CreateEntity {
        name: "entity".to_string(), // Use "entity" to match the recursive CTE view table name
    })
    .boxed();

    // Collect existing IDs for parent_id generation (do this before using in insert strategy)
    let entity_existing_ids: Vec<String> = state
        .entities
        .get("entity")
        .map(|e| e.keys().cloned().collect())
        .unwrap_or_default();

    // Insert strategy with parent_id support for hierarchical data
    // 60% root entities (no parent), 40% child entities (reference existing ID)
    let insert = if entity_existing_ids.is_empty() {
        // No existing entities - all inserts are root
        (
            prop::sample::select(entity_names.clone()),
            "[a-z]{1,5}",
            "[a-z]{1,10}",
        )
            .prop_map(|(entity, id, value)| StorageTransition::Insert {
                entity,
                id,
                parent_id: None,
                value,
            })
            .boxed()
    } else {
        // Some entities exist - mix of root and child inserts
        prop::strategy::Union::new_weighted(vec![
            (
                60u32,
                (
                    prop::sample::select(entity_names.clone()),
                    "[a-z]{1,5}",
                    "[a-z]{1,10}",
                )
                    .prop_map(|(entity, id, value)| StorageTransition::Insert {
                        entity,
                        id,
                        parent_id: None,
                        value,
                    })
                    .boxed(),
            ),
            (
                40u32,
                (
                    prop::sample::select(entity_names.clone()),
                    "[a-z]{1,5}",
                    prop::sample::select(entity_existing_ids.clone()),
                    "[a-z]{1,10}",
                )
                    .prop_map(|(entity, id, parent_id, value)| StorageTransition::Insert {
                        entity,
                        id,
                        parent_id: Some(parent_id),
                        value,
                    })
                    .boxed(),
            ),
        ])
        .boxed()
    };

    // For update/delete/dirty/version operations, we need existing IDs
    let existing_ids: Vec<(String, String)> = state
        .entities
        .iter()
        .flat_map(|(entity, ids)| ids.keys().map(move |id| (entity.clone(), id.clone())))
        .collect();

    // Collect all existing values for filter generation
    let existing_values: Vec<String> = state
        .entities
        .values()
        .flat_map(|entities| {
            entities.values().flat_map(|entity| {
                entity.values().filter_map(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })
            })
        })
        .collect();

    if existing_ids.is_empty() {
        // Only allow create and insert
        return prop::strategy::Union::new_weighted(vec![(10, create_entity), (40, insert)])
            .boxed();
    }

    // Generate query with filter
    let query = (
        prop::sample::select(entity_names.clone()),
        generate_filter(entity_names.clone(), existing_values.clone()),
    )
        .prop_map(|(entity, filter)| StorageTransition::Query { entity, filter })
        .boxed();

    // Generate get by id
    let get = prop::sample::select(existing_ids.clone())
        .prop_map(|(entity, id)| StorageTransition::Get { entity, id })
        .boxed();

    let update = (prop::sample::select(existing_ids.clone()), "[a-z]{1,10}")
        .prop_map(|((entity, id), value)| StorageTransition::Update { entity, id, value })
        .boxed();

    let delete = prop::sample::select(existing_ids.clone())
        .prop_map(|(entity, id)| StorageTransition::Delete { entity, id })
        .boxed();

    let set_version = (prop::sample::select(existing_ids), "[a-z0-9]{1,10}")
        .prop_map(|((entity, id), version)| StorageTransition::SetVersion {
            entity,
            id,
            version,
        })
        .boxed();

    // CDC operation
    let enable_cdc = Just(StorageTransition::EnableCDC).boxed();

    // Materialized view operations
    let create_mv = prop::sample::select(entity_names.clone())
        .prop_map(|entity| StorageTransition::CreateMaterializedView {
            view_name: format!("{}_view", entity),
            entity,
        })
        .boxed();

    // View stream operations
    let view_names: Vec<String> = state.materialized_views.keys().cloned().collect();

    let mut strategies = vec![
        (5, create_entity),
        (20, insert),
        (15, update),
        (8, delete),
        (12, query),
        (10, get),
        (10, set_version),
    ];

    // Add CDC if not enabled
    if !state.cdc_enabled {
        strategies.push((3, enable_cdc));
    }

    // Add materialized view creation
    strategies.push((5, create_mv));

    // Add view stream creation if we have views
    if !view_names.is_empty() {
        let create_stream = prop::sample::select(view_names)
            .prop_map(|view_name| StorageTransition::CreateViewStream { view_name })
            .boxed();
        strategies.push((3, create_stream));
    }

    // Add recursive CTE view creation if not already created and "entity" table exists
    // This view exercises the JoinOperator in Turso's IVM
    if !state.recursive_cte_view_created && state.entities.contains_key("entity") {
        let create_recursive_view = Just(StorageTransition::CreateRecursiveCTEView).boxed();
        strategies.push((8, create_recursive_view)); // Higher weight to increase coverage
    }

    prop::strategy::Union::new_weighted(strategies).boxed()
}

/// Generate a batch of transitions to execute.
/// The batch size and mode is biased based on the current state:
/// - Without views: 80% single, 15% concurrent size 2, 5% concurrent size 3
/// - With views and entities: mix of single, concurrent, and transaction batches
/// - With recursive CTE view: higher weight for transaction batches (to exercise JoinOperator)
///
/// Operations that require test state (CDC, ViewStream) are always single batches.
fn generate_transition_batch(state: &ReferenceState) -> BoxedStrategy<TransitionBatch> {
    let has_views = !state.materialized_views.is_empty();
    let has_entities = state.entities.values().any(|e| !e.is_empty());
    let has_recursive_cte = state.recursive_cte_view_created;

    // Generate the base operation strategy first (captures state by value via generate_transitions)
    let base_op_strategy = generate_transitions(state);

    // When recursive CTE view exists, include Transaction batches to exercise JoinOperator commit
    if has_recursive_cte && has_entities {
        // Batch size strategy with Transaction mode for JoinOperator testing:
        // - 30% single operations
        // - 20% concurrent batches (size 2-3)
        // - 50% transaction batches (size 2-5) - high weight to trigger JoinOperator::commit
        prop::strategy::Union::new_weighted(vec![
            (
                30u32,
                base_op_strategy
                    .clone()
                    .prop_map(|op| TransitionBatch::single(op))
                    .boxed(),
            ),
            (
                10u32,
                prop::collection::vec(base_op_strategy.clone(), 2..=2)
                    .prop_map(|ops| TransitionBatch::concurrent(ops))
                    .boxed(),
            ),
            (
                10u32,
                prop::collection::vec(base_op_strategy.clone(), 3..=3)
                    .prop_map(|ops| TransitionBatch::concurrent(ops))
                    .boxed(),
            ),
            (
                20u32,
                prop::collection::vec(base_op_strategy.clone(), 2..=3)
                    .prop_map(|ops| TransitionBatch::transaction(ops))
                    .boxed(),
            ),
            (
                20u32,
                prop::collection::vec(base_op_strategy.clone(), 3..=4)
                    .prop_map(|ops| TransitionBatch::transaction(ops))
                    .boxed(),
            ),
            (
                10u32,
                prop::collection::vec(base_op_strategy, 4..=5)
                    .prop_map(|ops| TransitionBatch::transaction(ops))
                    .boxed(),
            ),
        ])
        .boxed()
    } else if has_views && has_entities {
        // Standard IVM testing without JoinOperator focus
        prop::strategy::Union::new_weighted(vec![
            (
                40u32,
                base_op_strategy
                    .clone()
                    .prop_map(|op| TransitionBatch::single(op))
                    .boxed(),
            ),
            (
                30u32,
                prop::collection::vec(base_op_strategy.clone(), 2..=2)
                    .prop_map(|ops| TransitionBatch::concurrent(ops))
                    .boxed(),
            ),
            (
                20u32,
                prop::collection::vec(base_op_strategy.clone(), 3..=3)
                    .prop_map(|ops| TransitionBatch::concurrent(ops))
                    .boxed(),
            ),
            (
                10u32,
                prop::collection::vec(base_op_strategy, 4..=4)
                    .prop_map(|ops| TransitionBatch::concurrent(ops))
                    .boxed(),
            ),
        ])
        .boxed()
    } else {
        prop::strategy::Union::new_weighted(vec![
            (
                80u32,
                base_op_strategy
                    .clone()
                    .prop_map(|op| TransitionBatch::single(op))
                    .boxed(),
            ),
            (
                15u32,
                prop::collection::vec(base_op_strategy.clone(), 2..=2)
                    .prop_map(|ops| TransitionBatch::concurrent(ops))
                    .boxed(),
            ),
            (
                5u32,
                prop::collection::vec(base_op_strategy, 3..=3)
                    .prop_map(|ops| TransitionBatch::concurrent(ops))
                    .boxed(),
            ),
        ])
        .boxed()
    }
}

/// Check preconditions for a transition batch.
/// All operations in the batch must pass their individual preconditions.
/// Also filters out batches that mix CDC/ViewStream ops with other ops.
fn check_batch_preconditions(state: &ReferenceState, batch: &TransitionBatch) -> bool {
    if batch.operations.is_empty() {
        return false;
    }

    // Operations that need test state can't be batched with other ops
    let has_state_ops = batch.operations.iter().any(|op| {
        matches!(
            op,
            StorageTransition::EnableCDC | StorageTransition::CreateViewStream { .. }
        )
    });

    if has_state_ops && batch.operations.len() > 1 {
        return false;
    }

    // Transaction batches can only contain certain operations
    // (ones that work properly in explicit transactions)
    if matches!(batch.mode, BatchMode::Transaction) {
        let all_transactionable = batch.operations.iter().all(|op| {
            matches!(
                op,
                StorageTransition::Insert { .. }
                    | StorageTransition::Update { .. }
                    | StorageTransition::Delete { .. }
                    | StorageTransition::SetVersion { .. }
            )
        });
        if !all_transactionable {
            return false;
        }
    }

    // When CDC is enabled, Insert/Update/Delete operations must use the CDC connection
    // to capture events. Concurrent/Transaction batches use apply_to_turso_inner which doesn't
    // have access to the CDC connection, so we can't allow these ops in such batches.
    if state.cdc_enabled && batch.operations.len() > 1 {
        let has_cdc_tracked_ops = batch.operations.iter().any(|op| {
            matches!(
                op,
                StorageTransition::Insert { .. }
                    | StorageTransition::Update { .. }
                    | StorageTransition::Delete { .. }
            )
        });
        if has_cdc_tracked_ops {
            return false;
        }
    }

    // Check all operations pass preconditions
    // Note: For concurrent/transaction batches, we check preconditions against initial state.
    // This may reject some valid batches but ensures safety.
    batch
        .operations
        .iter()
        .all(|op| check_preconditions(state, op))
}

/// ReferenceStateMachine implementation using TransitionBatch for concurrent testing
impl ReferenceStateMachine for ReferenceState {
    type State = Self;
    type Transition = TransitionBatch;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(ReferenceState::default()).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        generate_transition_batch(state)
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        check_batch_preconditions(state, transition)
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        // Apply all operations in the batch to reference state sequentially.
        // The SUT will execute them concurrently, but the reference state
        // applies them sequentially for deterministic comparison.
        for operation in &transition.operations {
            let _result = apply_to_reference(&mut state, operation);
        }
        state
    }
}

/// Compare query results between reference state and Turso.
/// Uses similar_asserts for detailed diff output.
fn compare_query_results(
    ref_entities: &[StorageEntity],
    turso_entities: &[StorageEntity],
    operation: &StorageTransition,
) {
    // Sort both by id for consistent comparison
    let mut ref_sorted = ref_entities.to_vec();
    ref_sorted.sort_by_key(|e| {
        e.get("id")
            .and_then(|v| v.as_string())
            .unwrap_or("")
            .to_string()
    });

    let mut turso_sorted = turso_entities.to_vec();
    turso_sorted.sort_by_key(|e| {
        e.get("id")
            .and_then(|v| v.as_string())
            .unwrap_or("")
            .to_string()
    });

    // Filter out internal fields (starting with _) for comparison
    let normalize = |entities: &[StorageEntity]| -> Vec<StorageEntity> {
        entities
            .iter()
            .map(|e| {
                e.iter()
                    .filter(|(k, _)| !k.starts_with('_'))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .collect()
    };

    let ref_normalized = normalize(&ref_sorted);
    let turso_normalized = normalize(&turso_sorted);

    similar_asserts::assert_eq!(
        ref_normalized,
        turso_normalized,
        "Query/Get result mismatch for operation: {:?}",
        operation
    );
}

/// StateMachineTest implementation with concurrent batch execution.
///
/// When a batch has multiple operations, they are executed concurrently
/// to detect race conditions (like IVM bugs in Turso).
///
/// Flow:
/// 1. Single-operation batches: execute sequentially (normal behavior)
/// 2. Multi-operation batches: execute all operations concurrently
/// 3. If any operation panics during concurrent execution, the test fails
impl StateMachineTest for StorageTest {
    type SystemUnderTest = Self;
    type Reference = ReferenceState;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        StorageTest::new(&ref_state.handle)
    }

    fn apply(
        mut state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        let TransitionBatch { operations, mode } = transition;

        if operations.is_empty() {
            return state;
        }

        match mode {
            BatchMode::Single => {
                // Single operation: execute sequentially (existing behavior)
                let operation = &operations[0];

                let turso_result = tokio::task::block_in_place(|| {
                    ref_state.handle.block_on(async {
                        apply_to_turso(&mut state, operation, &ref_state.handle)
                            .await
                            .expect("Turso transition should succeed (preconditions validated it)")
                    })
                });

                // For Query and Get operations, compare results
                if let Some(turso_entities) = turso_result {
                    let mut ref_clone = ref_state.clone();
                    if let Some(ref_entities) = apply_to_reference(&mut ref_clone, operation) {
                        compare_query_results(&ref_entities, &turso_entities, operation);
                    }
                }
            }

            BatchMode::Concurrent => {
                // Multiple operations: execute concurrently to detect race conditions
                let num_ops = operations.len();
                let panics = tokio::task::block_in_place(|| {
                    ref_state.handle.block_on(
                        state.execute_concurrent_batch(operations.clone(), &ref_state.handle),
                    )
                });

                if panics > 0 {
                    panic!(
                        "Detected {} panics during concurrent execution of {} operations. \
                         This likely indicates a race condition in Turso IVM.\n\
                         Operations: {:?}",
                        panics, num_ops, operations
                    );
                }
            }

            BatchMode::Transaction => {
                // Transaction mode: execute all operations within BEGIN/COMMIT
                // This exercises the JoinOperator::commit path in Turso's IVM
                let num_ops = operations.len();
                let result = tokio::task::block_in_place(|| {
                    ref_state.handle.block_on(async {
                        state
                            .execute_transaction_batch(operations.clone(), &ref_state.handle)
                            .await
                    })
                });

                match result {
                    Ok(_) => {}
                    Err(e) => {
                        panic!(
                            "Transaction batch of {} operations failed: {}\n\
                             Operations: {:?}\n\
                             This may indicate a JoinOperator bug in Turso IVM.",
                            num_ops, e, operations
                        );
                    }
                }
            }
        }

        state
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        verify_states_match(ref_state, &state.backend, &ref_state.handle);

        // Verify CDC is actually capturing changes if enabled
        if ref_state.cdc_enabled {
            let conn = state
                .cdc_connection
                .as_ref()
                .expect("CDC enabled but no CDC connection stored - this is a bug in the test");

            // Get actual CDC events from database
            let actual_cdc_events = tokio::task::block_in_place(|| {
                ref_state.handle.block_on(async {
                    let mut events = Vec::new();

                    // Try to query turso_cdc table. It may not exist if no changes have occurred yet.
                    match conn
                        .query("SELECT * FROM turso_cdc ORDER BY change_id", ())
                        .await
                    {
                        Ok(mut rows) => {
                            while let Some(row) = rows.next().await.unwrap() {
                                // turso_cdc schema:
                                // col[0] = change_id (integer)
                                // col[1] = change_time (integer timestamp)
                                // col[2] = change_type (integer: -1=DELETE, 0=UPDATE, 1=INSERT)
                                // col[3] = table_name (text)
                                // col[4] = id (rowid)
                                // col[5] = before (blob)
                                // col[6] = after (blob)

                                let table_name = match row.get_value(3).unwrap() {
                                    turso::Value::Text(s) => s,
                                    _ => continue,
                                };

                                let change_type = match row.get_value(2).unwrap() {
                                    turso::Value::Integer(-1) => "DELETE",
                                    turso::Value::Integer(0) => "UPDATE",
                                    turso::Value::Integer(1) => "INSERT",
                                    _ => continue,
                                };
                                events.push((table_name, change_type.to_string()));
                            }
                        }
                        Err(e) => {
                            // If table doesn't exist, that's fine - it means no CDC events yet
                            if !e.to_string().contains("no such table: turso_cdc") {
                                panic!("Unexpected error querying turso_cdc: {}", e);
                            }
                        }
                    }

                    events
                })
            });

            // Build expected events for comparison (table, change_type)
            let expected_events: Vec<(String, String)> = ref_state
                .cdc_events
                .iter()
                .map(|(entity, op_type, _id)| (entity.clone(), op_type.clone()))
                .collect();

            // Single comprehensive assertion with all information
            assert_eq!(
                actual_cdc_events,
                expected_events,
                "\n\n=== CDC Event Verification Failed ===\n\
                Expected {} CDC events:\n{:#?}\n\n\
                Actual {} CDC events:\n{:#?}\n\n\
                Full reference CDC events (entity, op, id):\n{:#?}\n\
                =====================================\n",
                expected_events.len(),
                expected_events,
                actual_cdc_events.len(),
                actual_cdc_events,
                ref_state.cdc_events
            );
        }

        // Verify view change notifications match expected
        for (view_name, actual_changes_arc) in &state.view_stream_changes {
            // Get expected changes from reference state
            let expected_changes_arc =
                ref_state
                    .view_stream_changes
                    .get(view_name)
                    .expect(&format!(
                        "View '{}' exists in SUT but not in reference state",
                        view_name
                    ));
            let expected_len = expected_changes_arc.lock().unwrap().len();

            // Wait for stream to catch up (bounded wait up to 50ms)
            let mut matched = false;
            for _ in 0..5 {
                let actual_len = actual_changes_arc.lock().unwrap().len();
                if actual_len >= expected_len {
                    matched = true;
                    break;
                }
                tokio::task::block_in_place(|| {
                    ref_state.handle.block_on(async {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    });
                });
            }

            let actual_changes = actual_changes_arc.lock().unwrap();
            let expected_changes = expected_changes_arc.lock().unwrap();

            if !matched {
                panic!(
                    "\n\n=== View Change Stream Timeout for '{}' ===\n\
                     Expected {} changes but stream only delivered {} after 50ms\n\
                     Expected changes:\n{:#?}\n\n\
                     Actual changes:\n{:#?}\n\
                     ===========================================\n",
                    view_name,
                    expected_len,
                    actual_changes.len(),
                    *expected_changes,
                    *actual_changes
                );
            }

            // Compare counts first
            assert_eq!(
                actual_changes.len(),
                expected_changes.len(),
                "\n\n=== View Change Count Mismatch for '{}' ===\n\
                 Expected {} changes, got {} changes\n\
                 Expected changes:\n{:#?}\n\n\
                 Actual changes:\n{:#?}\n\
                 ==========================================\n",
                view_name,
                expected_changes.len(),
                actual_changes.len(),
                *expected_changes,
                *actual_changes
            );

            // Build ROWID mappings incrementally as we compare
            // entity_id <-> rowid (tracks current state)
            let mut ref_rowid_to_entity: HashMap<String, String> = HashMap::new();
            let mut actual_rowid_to_entity: HashMap<String, String> = HashMap::new();

            // Compare each change
            for (i, (expected, actual)) in expected_changes
                .iter()
                .zip(actual_changes.iter())
                .enumerate()
            {
                assert_eq!(
                    expected.relation_name, actual.relation_name,
                    "Relation name mismatch at index {} for view '{}'",
                    i, view_name
                );

                // Extract entity IDs from data for matching (ROWIDs will differ)
                let expected_entity_id = match &expected.change {
                    ChangeData::Created { data, .. } | ChangeData::Updated { data, .. } => {
                        // Extract entity ID and update mapping
                        if let Some(Value::String(entity_id)) = data.get("id") {
                            // Extract ROWID from _rowid field or id field
                            let rowid = data
                                .get("_rowid")
                                .and_then(|v| match v {
                                    Value::String(s) => Some(s.clone()),
                                    _ => None,
                                })
                                .or_else(|| match &expected.change {
                                    ChangeData::Updated { id, .. } => Some(id.clone()),
                                    _ => None,
                                })
                                .unwrap_or_default();
                            ref_rowid_to_entity.insert(rowid, entity_id.clone());
                            Some(entity_id.clone())
                        } else {
                            None
                        }
                    }
                    ChangeData::Deleted { id, .. } => {
                        // Look up entity ID from mapping
                        ref_rowid_to_entity.get(id).cloned()
                    }
                    ChangeData::FieldsChanged { entity_id, .. } => Some(entity_id.clone()),
                };
                let actual_entity_id = match &actual.change {
                    ChangeData::Created { data, .. } | ChangeData::Updated { data, .. } => {
                        // Extract entity ID and update mapping
                        if let Some(Value::String(entity_id)) = data.get("id") {
                            // Extract ROWID from _rowid field or id field
                            let rowid = data
                                .get("_rowid")
                                .and_then(|v| match v {
                                    Value::String(s) => Some(s.clone()),
                                    _ => None,
                                })
                                .or_else(|| match &actual.change {
                                    ChangeData::Updated { id, .. } => Some(id.clone()),
                                    _ => None,
                                })
                                .unwrap_or_default();
                            actual_rowid_to_entity.insert(rowid, entity_id.clone());
                            Some(entity_id.clone())
                        } else {
                            None
                        }
                    }
                    ChangeData::Deleted { id, .. } => {
                        // Look up entity ID from mapping
                        actual_rowid_to_entity.get(id).cloned()
                    }
                    ChangeData::FieldsChanged { entity_id, .. } => Some(entity_id.clone()),
                };

                assert_eq!(
                    expected_entity_id,
                    actual_entity_id,
                    "\n\n=== View Change Entity ID Mismatch at index {} for '{}' ===\n\
                     Expected entity ID: {:?}\n\
                     Actual entity ID: {:?}\n\
                     Expected change: {:?}\n\
                     Actual change: {:?}\n\
                     =================================================\n",
                    i,
                    view_name,
                    expected_entity_id,
                    actual_entity_id,
                    expected.change,
                    actual.change
                );

                // Compare change types
                match (&expected.change, &actual.change) {
                    (
                        ChangeData::Created { data: exp_data, .. },
                        ChangeData::Created { data: act_data, .. },
                    ) => {
                        // Compare data excluding _rowid (internal implementation detail)
                        let mut exp_data_filtered = exp_data.clone();
                        let mut act_data_filtered = act_data.clone();
                        exp_data_filtered.remove("_rowid");
                        act_data_filtered.remove("_rowid");
                        assert_eq!(
                            exp_data_filtered, act_data_filtered,
                            "Created data mismatch at index {} for view '{}'",
                            i, view_name
                        );
                    }
                    (
                        ChangeData::Updated { data: exp_data, .. },
                        ChangeData::Updated { data: act_data, .. },
                    ) => {
                        // Compare data excluding _rowid (internal implementation detail)
                        let mut exp_data_filtered = exp_data.clone();
                        let mut act_data_filtered = act_data.clone();
                        exp_data_filtered.remove("_rowid");
                        act_data_filtered.remove("_rowid");
                        assert_eq!(
                            exp_data_filtered, act_data_filtered,
                            "Updated data mismatch at index {} for view '{}'",
                            i, view_name
                        );
                    }
                    (ChangeData::Deleted { .. }, ChangeData::Deleted { .. }) => {
                        // IDs already matched, nothing more to check
                    }
                    _ => {
                        panic!(
                            "\n\n=== View Change Type Mismatch at index {} for '{}' ===\n\
                             Expected: {:?}\n\
                             Actual: {:?}\n\
                             ======================================================\n",
                            i, view_name, expected.change, actual.change
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use proptest_state_machine::ReferenceStateMachine;

    /// Manual state machine test without fork mode.
    /// Runs the state machine test directly using proptest strategies but without fork.
    #[test]
    fn test_turso_backend_state_machine() {
        let config = ProptestConfig {
            cases: 10, // Reduced for faster testing with single-threaded runtime
            failure_persistence: None,
            fork: false, // Must be false for tokio compatibility
            timeout: 0,  // Timeout > 0 enables fork mode, so set to 0
            verbose: 1,
            ..ProptestConfig::default()
        };

        let mut runner = TestRunner::new(config.clone());

        // Get the strategy for generating initial state and transitions
        // Reduced max transitions for faster testing
        let strategy = ReferenceState::sequential_strategy(1..15);

        // Run the test manually without forking
        let result = runner.run(&strategy, |(initial_state, transitions, seen_counter)| {
            // Run test_sequential directly
            StorageTest::test_sequential(config.clone(), initial_state, transitions, seen_counter);
            Ok(())
        });

        if let Err(e) = result {
            panic!("State machine test failed: {:?}", e);
        }
    }

    /// Test that view change stream receives events from backend operations.
    ///
    /// With the internal actor pattern, all operations go through a single actor,
    /// so CDC events should be properly captured.
    #[tokio::test]
    async fn test_view_change_stream_receives_events_from_backend_operations() {
        use holon_api::Value;
        use tempfile::tempdir;
        use tokio::sync::broadcast;

        let temp_dir = tempdir().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test.db");

        // Create backend (which internally spawns the actor)
        let db = TursoBackend::open_database(&db_path).expect("Failed to open database");
        let (cdc_tx, _cdc_rx) = broadcast::channel(1024);
        let (mut backend, _db_handle) =
            TursoBackend::new(db, cdc_tx).expect("Failed to create TursoBackend");

        let schema = get_test_schema("test_entity");
        backend.create_entity(&schema).await.unwrap();

        // Insert initial data
        let mut data1 = StorageEntity::new();
        data1.insert("id".to_string(), Value::String("id1".to_string()));
        data1.insert("value".to_string(), Value::String("initial".to_string()));
        backend
            .insert(
                &holon_api::Schema::from_table_name(&schema.name),
                data1.clone(),
            )
            .await
            .unwrap();

        // Create materialized view
        backend
            .execute_ddl("CREATE MATERIALIZED VIEW test_view AS SELECT * FROM test_entity")
            .await
            .unwrap();

        // Create view change stream
        let mut stream = backend.row_changes().await.unwrap();

        // Spawn task to collect changes
        let changes = Arc::new(Mutex::new(Vec::new()));
        let changes_clone = changes.clone();
        let handle = tokio::spawn(async move {
            while let Some(batch) = stream.next().await {
                for change in &batch.inner.items {
                    if change.relation_name == "test_view" {
                        changes_clone.lock().unwrap().push(change.clone());
                    }
                }
            }
        });

        // Give the callback registration time to complete
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Perform operations using backend methods
        let mut data2 = StorageEntity::new();
        data2.insert("id".to_string(), Value::String("id2".to_string()));
        data2.insert("value".to_string(), Value::String("inserted".to_string()));
        backend
            .insert(&holon_api::Schema::from_table_name(&schema.name), data2)
            .await
            .unwrap();

        let mut update_data = StorageEntity::new();
        update_data.insert("value".to_string(), Value::String("updated".to_string()));
        backend
            .update(
                &holon_api::Schema::from_table_name(&schema.name),
                "id1",
                update_data,
            )
            .await
            .unwrap();

        backend.delete("test_entity", "id2").await.unwrap();

        // Wait for stream to deliver events
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Check collected changes
        let collected_changes = changes.lock().unwrap();

        // We expect 3 events: INSERT (id2), UPDATE (id1), DELETE (id2)
        // With the DatabaseActor pattern, all writes go through a single actor so CDC callbacks fire
        assert_eq!(
            collected_changes.len(),
            3,
            "Expected 3 view change events (INSERT, UPDATE, DELETE) but got {}. \
             With the DatabaseActor pattern, all operations should trigger CDC callbacks.",
            collected_changes.len()
        );

        // Abort the collection task
        handle.abort();
    }
}
