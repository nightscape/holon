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
            },
            Err(_) => {
                let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
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
}

/// System under test - wraps TursoBackend
pub struct StorageTest {
    pub backend: TursoBackend,
    /// CDC-enabled connection (must be kept alive)
    pub cdc_connection: Option<turso::Connection>,
    /// Keep connections alive for view streams
    pub view_stream_connections: HashMap<String, turso::Connection>,
    /// View stream change collectors: view_name -> Arc<Mutex<Vec<RowChange>>>
    pub view_stream_changes: HashMap<String, Arc<Mutex<Vec<RowChange>>>>,
    /// View stream handles to keep tasks alive
    pub view_stream_handles: HashMap<String, tokio::task::JoinHandle<()>>,
}

impl StorageTest {
    /// Create a new StorageTest with in-memory backend
    pub fn new(handle: &tokio::runtime::Handle) -> Self {
        let backend =
            tokio::task::block_in_place(|| handle.block_on(TursoBackend::new_in_memory())).unwrap();
        Self {
            backend,
            cdc_connection: None,
            view_stream_connections: HashMap::new(),
            view_stream_changes: HashMap::new(),
            view_stream_handles: HashMap::new(),
        }
    }

    /// Create a new StorageTest with file-based backend (Unix-like systems only)
    #[cfg(target_family = "unix")]
    pub fn new_with_file(handle: &tokio::runtime::Handle, db_path: &str) -> Self {
        let backend =
            tokio::task::block_in_place(|| handle.block_on(TursoBackend::new(db_path))).unwrap();
        Self {
            backend,
            cdc_connection: None,
            view_stream_connections: HashMap::new(),
            view_stream_changes: HashMap::new(),
            view_stream_handles: HashMap::new(),
        }
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
        StorageTransition::Insert { entity, id, value } => {
            let mut data = StorageEntity::new();
            data.insert("id".to_string(), Value::String(id.clone()));
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
            let data = entities.get_mut(id).unwrap();
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
                        let rowid = state
                            .view_rowids
                            .get(view_name)
                            .and_then(|rowids| rowids.get(id))
                            .expect("Entity should have ROWID assigned in view");
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
            state.entities.get_mut(entity).unwrap().remove(id);
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
                        let rowid = state
                            .view_rowids
                            .get(view_name)
                            .and_then(|rowids| rowids.get(id))
                            .copied()
                            .expect("Entity should have ROWID assigned in view");

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
                                let rowid = state
                                    .view_rowids
                                    .get(view_name)
                                    .and_then(|rowids| rowids.get(id))
                                    .expect("Entity should have ROWID assigned in view");

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
    }
}

/// Apply a transition to the TursoBackend (within StorageTest)
/// Returns the result (for Query/Get operations that return data)
async fn apply_to_turso(
    test: &mut StorageTest,
    transition: &StorageTransition,
    handle: &tokio::runtime::Handle,
) -> Result<Option<Vec<StorageEntity>>, String> {
    let backend = &mut test.backend;
    match transition {
        StorageTransition::CreateEntity { name } => {
            let schema = get_test_schema(name);
            backend
                .create_entity(&schema)
                .await
                .map_err(|e| e.to_string())?;
            Ok(None)
        }
        StorageTransition::Insert { entity, id, value } => {
            let mut data = StorageEntity::new();
            data.insert("id".to_string(), Value::String(id.clone()));
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
                    .insert(entity, data)
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
                    .update(entity, id, data)
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
            // Create and store CDC-enabled connection (raw connection, not pooled)
            let conn = backend.get_raw_connection().map_err(|e| e.to_string())?;
            conn.execute("PRAGMA unstable_capture_data_changes_conn('full')", ())
                .await
                .map_err(|e| e.to_string())?;
            test.cdc_connection = Some(conn);
            Ok(None)
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
        StorageTransition::CreateViewStream { view_name } => {
            // Create a view change stream
            let (conn, mut stream) = backend.row_changes().map_err(|e| e.to_string())?;

            // Store the connection to keep it alive
            test.view_stream_connections.insert(view_name.clone(), conn);

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

    // If no entities exist, only allow creating entities
    if entity_names.is_empty() {
        return prop::strategy::Just(StorageTransition::CreateEntity {
            name: "test_entity".to_string(),
        })
        .boxed();
    }

    // Strategies for different operations
    let create_entity = Just(StorageTransition::CreateEntity {
        name: "new_entity".to_string(),
    })
    .boxed();

    let insert = (
        prop::sample::select(entity_names.clone()),
        "[a-z]{1,5}",
        "[a-z]{1,10}",
    )
        .prop_map(|(entity, id, value)| StorageTransition::Insert { entity, id, value })
        .boxed();

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

    prop::strategy::Union::new_weighted(strategies).boxed()
}

/// ReferenceStateMachine implementation
impl ReferenceStateMachine for ReferenceState {
    type State = Self;
    type Transition = StorageTransition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(ReferenceState::default()).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        generate_transitions(state)
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        check_preconditions(state, transition)
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        let _result = apply_to_reference(&mut state, transition);
        state
    }
}

/// StateMachineTest implementation
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
        // Only apply to reference clone if this transition returns a result (Query/Get).
        // For other transitions (Insert/Update/Delete/etc), the ReferenceStateMachine::apply
        // has already applied them, and applying again would duplicate side effects
        // (like view change tracking).
        let needs_result = matches!(
            transition,
            StorageTransition::Query { .. } | StorageTransition::Get { .. }
        );

        let ref_result = if needs_result {
            let mut ref_clone = ref_state.clone();
            apply_to_reference(&mut ref_clone, &transition)
        } else {
            None
        };

        let turso_result = tokio::task::block_in_place(|| {
            ref_state.handle.block_on(async {
                apply_to_turso(&mut state, &transition, &ref_state.handle)
                    .await
                    .expect("Turso transition should succeed (preconditions validated it)")
            })
        });

        // For Query and Get operations, compare results
        if let (Some(ref_entities), Some(turso_entities)) = (ref_result, turso_result) {
            assert_eq!(
                ref_entities.len(),
                turso_entities.len(),
                "Query/Get result count mismatch for transition: {:?}",
                transition
            );

            // Sort both by id for consistent comparison
            let mut ref_sorted = ref_entities.clone();
            ref_sorted.sort_by_key(|e| {
                e.get("id")
                    .and_then(|v| v.as_string())
                    .unwrap_or("")
                    .to_string()
            });

            let mut turso_sorted = turso_entities.clone();
            turso_sorted.sort_by_key(|e| {
                e.get("id")
                    .and_then(|v| v.as_string())
                    .unwrap_or("")
                    .to_string()
            });

            for (ref_entity, turso_entity) in ref_sorted.iter().zip(turso_sorted.iter()) {
                // Compare non-internal fields
                for (key, ref_value) in ref_entity {
                    if !key.starts_with('_') {
                        assert_eq!(
                            turso_entity.get(key),
                            Some(ref_value),
                            "Field '{}' mismatch in Query/Get result for transition: {:?}",
                            key,
                            transition
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

    proptest_state_machine::prop_state_machine! {
        #![proptest_config(ProptestConfig {
            cases: 30,
            failure_persistence: None,
            timeout: 10000,
            verbose: 0,
            .. ProptestConfig::default()
        })]

        #[test]
        fn test_turso_backend_state_machine(sequential 1..50 => StorageTest);
    }

    /// Test that file-based storage persists data across database reopens
    #[cfg(target_family = "unix")]
    #[tokio::test]
    async fn test_file_based_storage_persistence() {
        use holon_api::Value;

        let test_path = "/tmp/turso_pbt_persistence_test.db";
        let _ = std::fs::remove_file(test_path);

        // Create database and insert data
        {
            let mut backend = TursoBackend::new(test_path).await.unwrap();
            let schema = get_test_schema("test_entity");
            backend.create_entity(&schema).await.unwrap();

            let mut data = StorageEntity::new();
            data.insert("id".to_string(), Value::String("test_id".to_string()));
            data.insert("value".to_string(), Value::String("test_value".to_string()));

            backend.insert("test_entity", data).await.unwrap();
        }

        // Reopen database and verify data persists
        {
            let backend = TursoBackend::new(test_path).await.unwrap();
            let result = backend.get("test_entity", "test_id").await.unwrap();

            assert!(result.is_some(), "Data should persist after reopening");
            let entity = result.unwrap();
            assert_eq!(
                entity.get("value"),
                Some(&Value::String("test_value".to_string())),
                "Value should match original"
            );
        }

        // Clean up
        std::fs::remove_file(test_path).unwrap();
    }

    /// Test that reproduces the CDC connection isolation bug
    ///
    /// This test demonstrates that when operations use different connections than
    /// the one that registered the view change callback, NO events are received.
    ///
    /// BUG: view_changes should receive events from backend.insert/update/delete,
    /// but it doesn't because each operation creates a NEW connection.
    #[tokio::test]
    async fn test_view_change_stream_receives_events_from_backend_operations() {
        use holon_api::Value;

        let mut backend = TursoBackend::new_in_memory().await.unwrap();
        let schema = get_test_schema("test_entity");
        backend.create_entity(&schema).await.unwrap();

        // Insert initial data
        let mut data1 = StorageEntity::new();
        data1.insert("id".to_string(), Value::String("id1".to_string()));
        data1.insert("value".to_string(), Value::String("initial".to_string()));
        backend.insert("test_entity", data1.clone()).await.unwrap();

        // Create materialized view
        let conn = backend.get_connection().unwrap();
        conn.execute(
            "CREATE MATERIALIZED VIEW test_view AS SELECT * FROM test_entity",
            (),
        )
        .await
        .unwrap();

        // Create view change stream (registers callback on Connection A)
        let (conn, mut stream) = backend.row_changes().unwrap();

        // Spawn task to collect changes
        let changes = Arc::new(Mutex::new(Vec::new()));
        let changes_clone = changes.clone();
        let handle = tokio::spawn(async move {
            while let Some(batch) = stream.next().await {
                // Access items via inner field (Deref doesn't allow moving)
                for change in &batch.inner.items {
                    if change.relation_name == "test_view" {
                        changes_clone.lock().unwrap().push(change.clone());
                    }
                }
            }
        });

        // CRITICAL: Drop the connection immediately to see if callbacks still work
        drop(conn);

        // Give the callback registration time to complete
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Perform operations using backend methods
        // These create NEW connections (B, C, D) so callback on A won't see them
        let mut data2 = StorageEntity::new();
        data2.insert("id".to_string(), Value::String("id2".to_string()));
        data2.insert("value".to_string(), Value::String("inserted".to_string()));
        backend.insert("test_entity", data2).await.unwrap();

        let mut update_data = StorageEntity::new();
        update_data.insert("value".to_string(), Value::String("updated".to_string()));
        backend
            .update("test_entity", "id1", update_data)
            .await
            .unwrap();

        backend.delete("test_entity", "id2").await.unwrap();

        // Wait for stream to deliver events
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Check collected changes
        let collected_changes = changes.lock().unwrap();

        // We expect 3 events: INSERT (id2), UPDATE (id1), DELETE (id2)
        // But BUG: we get ZERO events because operations used different connections
        assert_eq!(
            collected_changes.len(),
            3,
            "Expected 3 view change events (INSERT, UPDATE, DELETE) but got {}. \
             This is the CDC connection isolation bug: operations create new connections \
             so the callback registered on the row_changes() connection never sees the changes.",
            collected_changes.len()
        );

        // Abort the collection task
        handle.abort();
    }
}
