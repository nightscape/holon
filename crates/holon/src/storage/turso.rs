use async_trait::async_trait;
use serde_json;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
#[cfg(target_family = "unix")]
use turso_core::UnixIO;
use turso_core::types::RelationChangeEvent;
use turso_core::{Database, DatabaseOpts, OpenFlags};
use turso_sdk_kit::rsapi::{DatabaseChangeType, TursoConnection, TursoDatabaseConfig};

use crate::api::{Change, ChangeOrigin};
use crate::storage::{
    backend::StorageBackend,
    resource::Resource,
    schema::EntitySchema,
    sql_parser::{extract_created_tables, extract_table_refs},
    types::{Filter, Result, StorageEntity, StorageError},
};
use holon_api::{
    Batch, BatchMetadata, BatchTraceContext, BatchWithMetadata, CHANGE_ORIGIN_COLUMN, Value,
};

// ============================================================================
// Types moved from turso_actor.rs
// ============================================================================

/// Database operation phase for observability and debugging
///
/// Note: DDL is allowed in ALL phases because MatViews are created dynamically
/// when users navigate to blocks with PRQL queries. The actor's value is
/// SERIALIZATION, not phase-based blocking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabasePhase {
    /// Startup phase - schema initialization in progress
    SchemaInit,
    /// Normal operation - all DDL complete, application running
    Ready,
    /// Shutting down - rejecting new commands
    ShuttingDown,
}

impl Default for DatabasePhase {
    fn default() -> Self {
        Self::SchemaInit
    }
}

/// Priority levels for different operation types.
pub mod priority {
    /// Core schema DDL (blocks, commands, etc.)
    pub const DDL_CORE: u32 = 100;
    /// Module-specific DDL (todoist tables, etc.)
    pub const DDL_MODULE: u32 = 90;
    /// Materialized views
    pub const DDL_MATVIEW: u32 = 50;
    /// Data manipulation operations
    pub const DML: u32 = 0;
}

/// Unique identifier for a pending DDL operation.
pub type OperationId = u64;

/// A pending DDL operation with dependency information.
struct PendingDdl {
    id: OperationId,
    sql: String,
    provides: Vec<Resource>,
    requires: Vec<Resource>,
    priority: u32,
    response: oneshot::Sender<Result<()>>,
}

/// Commands that can be sent to the database actor
pub enum DbCommand {
    /// Execute a query (SELECT) with named parameters and return results
    Query {
        sql: String,
        params: HashMap<String, Value>,
        response: oneshot::Sender<Result<Vec<StorageEntity>>>,
    },

    /// Execute a query (SELECT) with positional parameters and return results
    QueryPositional {
        sql: String,
        params: Vec<turso::Value>,
        response: oneshot::Sender<Result<Vec<StorageEntity>>>,
    },

    /// Execute a statement (INSERT, UPDATE, DELETE) and return affected row count
    Execute {
        sql: String,
        params: Vec<turso::Value>,
        response: oneshot::Sender<Result<u64>>,
    },

    /// Execute DDL (CREATE TABLE, CREATE VIEW, etc.) immediately
    ExecuteDdl {
        sql: String,
        response: oneshot::Sender<Result<()>>,
    },

    /// Execute DDL with explicit dependency tracking
    ExecuteDdlWithDeps {
        sql: String,
        provides: Vec<Resource>,
        requires: Vec<Resource>,
        priority: u32,
        response: oneshot::Sender<Result<()>>,
    },

    /// Execute DDL with auto-inferred dependencies
    ExecuteDdlAuto {
        sql: String,
        priority: u32,
        response: oneshot::Sender<Result<()>>,
    },

    /// Mark resources as available (for bootstrapping existing schemas)
    MarkAvailable { resources: Vec<Resource> },

    /// Check if a resource is currently available
    ResourceExists {
        resource: Resource,
        response: oneshot::Sender<bool>,
    },

    /// Execute multiple statements in a transaction
    Transaction {
        statements: Vec<(String, Vec<turso::Value>)>,
        response: oneshot::Sender<Result<()>>,
    },

    /// Subscribe to CDC events for a specific relation
    SubscribeCdc {
        relation: String,
        response: oneshot::Sender<Result<broadcast::Receiver<BatchWithMetadata<RowChange>>>>,
    },

    /// Transition to Ready phase (called after all startup DDL is complete)
    TransitionToReady {
        response: oneshot::Sender<Result<()>>,
    },

    /// Get current database phase
    GetPhase {
        response: oneshot::Sender<DatabasePhase>,
    },

    /// Graceful shutdown
    Shutdown { response: oneshot::Sender<()> },
}

/// Handle for sending commands to the database actor
///
/// This is the public API for database operations. Clone freely - all clones
/// share the same underlying actor.
#[derive(Clone)]
pub struct DbHandle {
    tx: mpsc::Sender<DbCommand>,
}

impl DbHandle {
    /// Execute a query (SELECT) with named parameters and return results
    pub async fn query(
        &self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<Vec<StorageEntity>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::Query {
                sql: sql.to_string(),
                params,
                response: response_tx,
            })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| StorageError::DatabaseError("Actor response channel closed".to_string()))?
    }

    /// Execute a query (SELECT) with positional parameters and return results
    pub async fn query_positional(
        &self,
        sql: &str,
        params: Vec<turso::Value>,
    ) -> Result<Vec<StorageEntity>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::QueryPositional {
                sql: sql.to_string(),
                params,
                response: response_tx,
            })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| StorageError::DatabaseError("Actor response channel closed".to_string()))?
    }

    /// Execute a statement (INSERT, UPDATE, DELETE) and return affected row count
    pub async fn execute(&self, sql: &str, params: Vec<turso::Value>) -> Result<u64> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::Execute {
                sql: sql.to_string(),
                params,
                response: response_tx,
            })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| StorageError::DatabaseError("Actor response channel closed".to_string()))?
    }

    /// Execute DDL (CREATE TABLE, CREATE VIEW, etc.)
    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::ExecuteDdl {
                sql: sql.to_string(),
                response: response_tx,
            })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| StorageError::DatabaseError("Actor response channel closed".to_string()))?
    }

    /// Execute multiple statements in a transaction
    pub async fn transaction(&self, statements: Vec<(String, Vec<turso::Value>)>) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::Transaction {
                statements,
                response: response_tx,
            })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| StorageError::DatabaseError("Actor response channel closed".to_string()))?
    }

    /// Subscribe to CDC events for a specific relation
    pub async fn subscribe_cdc(
        &self,
        relation: &str,
    ) -> Result<broadcast::Receiver<BatchWithMetadata<RowChange>>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::SubscribeCdc {
                relation: relation.to_string(),
                response: response_tx,
            })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| StorageError::DatabaseError("Actor response channel closed".to_string()))?
    }

    /// Transition to Ready phase
    ///
    /// Call this after all startup DDL is complete. This signals to the system
    /// that the database schema is stable and background tasks can begin.
    pub async fn transition_to_ready(&self) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::TransitionToReady {
                response: response_tx,
            })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| StorageError::DatabaseError("Actor response channel closed".to_string()))?
    }

    /// Get current database phase
    pub async fn get_phase(&self) -> Result<DatabasePhase> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::GetPhase {
                response: response_tx,
            })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| StorageError::DatabaseError("Actor response channel closed".to_string()))
    }

    /// Graceful shutdown
    pub async fn shutdown(&self) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::Shutdown {
                response: response_tx,
            })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))?;

        response_rx.await.map_err(|_| {
            StorageError::DatabaseError("Actor response channel closed".to_string())
        })?;
        Ok(())
    }

    /// Execute DDL with explicit dependency tracking.
    ///
    /// The actor ensures dependencies are satisfied before execution.
    /// Operations are queued until their required resources are available.
    ///
    /// # Arguments
    /// * `sql` - The DDL SQL to execute
    /// * `provides` - Resources this operation creates
    /// * `requires` - Resources this operation depends on
    /// * `priority` - Execution priority (higher = sooner among ready operations)
    pub async fn execute_ddl_with_deps(
        &self,
        sql: &str,
        provides: Vec<Resource>,
        requires: Vec<Resource>,
        priority: u32,
    ) -> Result<()> {
        use std::time::Duration;

        let requires_for_error = requires.clone();
        let sql_preview: String = sql.chars().take(80).collect();

        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::ExecuteDdlWithDeps {
                sql: sql.to_string(),
                provides,
                requires,
                priority,
                response: response_tx,
            })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))?;

        // Timeout to detect missing mark_available() calls
        const DEPENDENCY_TIMEOUT: Duration = Duration::from_secs(30);

        match tokio::time::timeout(DEPENDENCY_TIMEOUT, response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(StorageError::DatabaseError(
                "Actor response channel closed".to_string(),
            )),
            Err(_elapsed) => {
                let missing_resources: Vec<String> = requires_for_error
                    .iter()
                    .map(|r| r.name().to_string())
                    .collect();

                Err(StorageError::DatabaseError(format!(
                    "DDL timed out after {:?} waiting for dependencies.\n\
                     SQL: {}...\n\
                     Required: {:?}\n\n\
                     Call mark_available() for resources created outside the actor.",
                    DEPENDENCY_TIMEOUT, sql_preview, missing_resources
                )))
            }
        }
    }

    /// Execute DDL with auto-inferred dependencies.
    ///
    /// Dependencies are extracted from the SQL using sqlparser.
    pub async fn execute_ddl_auto(&self, sql: &str, priority: u32) -> Result<()> {
        use std::time::Duration;

        let sql_preview: String = sql.chars().take(80).collect();

        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::ExecuteDdlAuto {
                sql: sql.to_string(),
                priority,
                response: response_tx,
            })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))?;

        const DEPENDENCY_TIMEOUT: Duration = Duration::from_secs(30);

        match tokio::time::timeout(DEPENDENCY_TIMEOUT, response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(StorageError::DatabaseError(
                "Actor response channel closed".to_string(),
            )),
            Err(_elapsed) => {
                let inferred_deps = extract_table_refs(sql);
                let missing_resources: Vec<String> =
                    inferred_deps.iter().map(|r| r.name().to_string()).collect();

                Err(StorageError::DatabaseError(format!(
                    "DDL timed out after {:?} waiting for dependencies.\n\
                     SQL: {}...\n\
                     Inferred required: {:?}\n\n\
                     Call mark_available() for resources created outside the actor.",
                    DEPENDENCY_TIMEOUT, sql_preview, missing_resources
                )))
            }
        }
    }

    /// Mark resources as available (for bootstrapping existing schemas).
    ///
    /// Call this during startup to register tables that already exist.
    pub async fn mark_available(&self, resources: Vec<Resource>) -> Result<()> {
        self.tx
            .send(DbCommand::MarkAvailable { resources })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))
    }

    /// Check if a resource is currently available.
    ///
    /// Returns true if the resource has been marked as available (either by DDL
    /// execution or by explicit `mark_available()` call).
    pub async fn resource_exists(&self, resource: &Resource) -> Result<bool> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::ResourceExists {
                resource: resource.clone(),
                response: response_tx,
            })
            .await
            .map_err(|_| StorageError::DatabaseError("Actor channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| StorageError::DatabaseError("Actor response channel closed".to_string()))
    }
}

// ============================================================================
// Helper functions moved from turso_actor.rs
// ============================================================================

/// Extract ChangeOrigin from row data's _change_origin column
fn extract_change_origin_from_data(data: &StorageEntity) -> ChangeOrigin {
    data.get(CHANGE_ORIGIN_COLUMN)
        .and_then(|v| match v {
            Value::String(json) => ChangeOrigin::from_json(json),
            _ => None,
        })
        .unwrap_or_else(|| ChangeOrigin::Remote {
            operation_id: None,
            trace_id: None,
        })
}

/// Convert holon_api::Value to turso::Value for parameter binding
pub(crate) fn value_to_turso_param(value: &Value) -> turso::Value {
    match value {
        Value::String(s) => turso::Value::Text(s.clone()),
        Value::Integer(i) => turso::Value::Integer(*i),
        Value::Float(f) => turso::Value::Real(*f),
        Value::Boolean(b) => turso::Value::Integer(if *b { 1 } else { 0 }),
        Value::DateTime(s) => turso::Value::Text(s.clone()),
        Value::Json(s) => turso::Value::Text(s.clone()),
        Value::Array(arr) => {
            let json_arr: Vec<serde_json::Value> = arr
                .iter()
                .map(|v| serde_json::Value::from(v.clone()))
                .collect();
            turso::Value::Text(serde_json::to_string(&serde_json::Value::Array(json_arr)).unwrap())
        }
        Value::Object(obj) => {
            let json_obj: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::from(v.clone())))
                .collect();
            turso::Value::Text(serde_json::to_string(&serde_json::Value::Object(json_obj)).unwrap())
        }
        Value::Null => turso::Value::Null,
    }
}

/// Bind named parameters ($param_name) to positional placeholders (?)
fn bind_parameters(
    sql: &str,
    params: &HashMap<String, Value>,
) -> Result<(String, Vec<turso::Value>)> {
    let mut result_sql = String::with_capacity(sql.len());
    let mut param_values = Vec::new();
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' {
            if let Some(&next_ch) = chars.peek() {
                if next_ch.is_alphanumeric() || next_ch == '_' {
                    let mut param_name = String::new();
                    while let Some(&next_ch) = chars.peek() {
                        if next_ch.is_alphanumeric() || next_ch == '_' {
                            param_name.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }

                    if let Some(value) = params.get(&param_name) {
                        result_sql.push('?');
                        param_values.push(value_to_turso_param(value));
                    } else {
                        return Err(StorageError::QueryError(format!(
                            "Parameter ${} not found",
                            param_name
                        )));
                    }
                } else {
                    result_sql.push('$');
                }
            } else {
                result_sql.push('$');
            }
        } else {
            result_sql.push(ch);
        }
    }

    Ok((result_sql, param_values))
}

/// Convert turso_core::Value to holon_api::Value
fn turso_value_to_value(value: turso_core::Value) -> Value {
    match value {
        turso_core::Value::Null => Value::Null,
        turso_core::Value::Integer(i) => Value::Integer(i),
        turso_core::Value::Float(f) => Value::Float(f),
        turso_core::Value::Text(s) => {
            let s_str = s.to_string();
            let trimmed = s_str.trim();
            if (trimmed.starts_with('[') && trimmed.ends_with(']'))
                || (trimmed.starts_with('{') && trimmed.ends_with('}'))
            {
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&s_str) {
                    Value::from(json_val)
                } else {
                    Value::String(s_str)
                }
            } else {
                Value::String(s_str)
            }
        }
        turso_core::Value::Blob(_) => Value::Null,
    }
}

/// Flatten a 'data' column value into key-value pairs
fn flatten_data_column(data_value: Value) -> Option<HashMap<String, Value>> {
    match data_value {
        Value::Object(obj) => Some(obj),
        Value::String(s) => serde_json::from_str::<serde_json::Value>(&s)
            .ok()
            .and_then(|v| {
                if let serde_json::Value::Object(map) = v {
                    Some(map.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
                } else {
                    None
                }
            }),
        _ => None,
    }
}

// ============================================================================
// Original turso.rs types
// ============================================================================

pub(crate) fn default_turso_config() -> TursoDatabaseConfig {
    TursoDatabaseConfig {
        path: String::new(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: None,
        io: None,
        db_file: None,
    }
}

/// A change notification from a materialized view
///
/// Note: The row_changes() method automatically coalesces DELETE+INSERT pairs
/// into UPDATE events to prevent UI flicker when materialized views are updated.
///
/// **IMPORTANT - UI Keying Requirements**:
///
/// The `id` field in `ChangeData` is the SQLite ROWID, which is:
/// - Unique per view (not globally unique)
/// - Can be reused after DELETE operations
/// - Used for transport and coalescing only
///
/// **UI MUST KEY BY ENTITY ID from `data.get("id")`, NOT BY ROWID**
///
/// Example:
/// ```rust
/// match change.change {
///     ChangeData::Created { data, .. } => {
///         let entity_id = data.get("id").unwrap(); // Use this for widget key
///         // Don't use ROWID (from `data.get("_rowid")`) as widget key!
///     }
///     ChangeData::Updated { id: rowid, data, .. } => {
///         let entity_id = data.get("id").unwrap(); // Use this for widget key
///         // Don't use `rowid` as widget key!
///     }
///     ChangeData::Deleted { id: entity_id, .. } => {
///         // Use entity_id directly - it's extracted from the deleted row data
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct RowChange {
    pub relation_name: String,
    pub change: ChangeData,
}

/// The type of change and associated data
///
/// **Note**: For `Created` and `Updated` variants, the ROWID is stored in `data["_rowid"]`.
/// For `Deleted`, the `id` field is the entity ID (extracted from the deleted row data).
/// See `RowChange` documentation for UI keying requirements.
pub type ChangeData = Change<StorageEntity>;

/// Stream of batched view changes with metadata
pub type RowChangeStream = ReceiverStream<BatchWithMetadata<RowChange>>;

/// Batches and coalesces CDC events to prevent UI flicker from DELETE+INSERT pairs
pub(crate) struct CdcCoalescer {
    changes: Vec<Option<RowChange>>,
    pending_deletes: HashMap<(String, String), usize>,
    pending_inserts: HashMap<(String, String), usize>,
}

impl CdcCoalescer {
    pub(crate) fn new() -> Self {
        Self {
            changes: Vec::new(),
            pending_deletes: HashMap::new(),
            pending_inserts: HashMap::new(),
        }
    }

    pub(crate) fn add(&mut self, change: RowChange) {
        self.changes.push(Some(change));
    }

    pub(crate) fn flush(&mut self) -> Vec<RowChange> {
        for idx in 0..self.changes.len() {
            if let Some(change) = self.changes[idx].clone() {
                // Use entity ID for coalescing key (consistent across DELETE and INSERT)
                // This allows DELETE+INSERT pairs for the same entity to be coalesced into UPDATE
                let key = (
                    change.relation_name.clone(),
                    match &change.change {
                        ChangeData::Deleted { id, .. } => id.clone(),
                        ChangeData::Created { data, .. } => {
                            // Extract entity ID from data for matching with DELETE
                            // Falls back to ROWID if no entity ID is found
                            data.get("id")
                                .and_then(|v| match v {
                                    Value::String(s) => Some(s.clone()),
                                    _ => None,
                                })
                                .or_else(|| {
                                    data.get("_rowid").and_then(|v| match v {
                                        Value::String(s) => Some(s.clone()),
                                        _ => None,
                                    })
                                })
                                .unwrap_or_else(|| "".to_string())
                        }
                        ChangeData::Updated { id, .. } => id.clone(),
                        ChangeData::FieldsChanged { entity_id, .. } => entity_id.clone(),
                    },
                );

                match &change.change {
                    ChangeData::Deleted { .. } => {
                        // Check if there's a pending INSERT for same key
                        if let Some(insert_idx) = self.pending_inserts.remove(&key) {
                            // INSERT then DELETE → no-op (drop both)
                            self.changes[insert_idx] = None;
                            self.changes[idx] = None;
                        } else {
                            // Track DELETE in case INSERT follows
                            self.pending_deletes.insert(key, idx);
                        }
                    }
                    ChangeData::Created { data, origin } => {
                        // Extract ROWID for coalescing
                        let rowid = data
                            .get("_rowid")
                            .and_then(|v| match v {
                                Value::String(s) => Some(s.clone()),
                                _ => None,
                            })
                            .unwrap_or_else(|| "".to_string());

                        // Check if there's a pending DELETE for same key
                        if let Some(delete_idx) = self.pending_deletes.remove(&key) {
                            // DELETE then INSERT → UPDATE
                            self.changes[delete_idx] = None;
                            self.changes[idx] = Some(RowChange {
                                relation_name: change.relation_name.clone(),
                                change: ChangeData::Updated {
                                    id: rowid,
                                    data: data.clone(),
                                    origin: origin.clone(),
                                },
                            });
                        } else {
                            // Track INSERT in case DELETE follows
                            self.pending_inserts.insert(key, idx);
                        }
                    }
                    ChangeData::Updated { .. } => {}
                    ChangeData::FieldsChanged { .. } => {
                        // FieldsChanged doesn't need special coalescing - pass through
                    }
                }
            }
        }

        self.pending_deletes.clear();
        self.pending_inserts.clear();
        self.changes.drain(..).flatten().collect()
    }
}

// ============================================================================
// TursoBackend with merged actor logic
// ============================================================================

pub struct TursoBackend {
    db: Arc<Database>,
    /// Broadcast channel for CDC events - all subscribers share this channel.
    cdc_broadcast: broadcast::Sender<BatchWithMetadata<RowChange>>,
    /// Command channel sender for creating DbHandles
    tx: mpsc::Sender<DbCommand>,
}

impl std::fmt::Debug for TursoBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TursoBackend")
            .field("db", &"Arc<Database>")
            .field(
                "cdc_broadcast",
                &format!(
                    "broadcast::Sender(receivers={})",
                    self.cdc_broadcast.receiver_count()
                ),
            )
            .field("tx", &"mpsc::Sender<DbCommand>")
            .finish()
    }
}

/// Turso-based storage backend
/// Note that this is the Turso Database, not Turso libsql.
///
/// From the docs:
/// How is Turso Database different from Turso's libSQL?
/// Turso Database is a project to build the next evolution of SQLite in Rust, with a strong open contribution focus and features like native async support, vector search, and more.
/// The libSQL project is also an attempt to evolve SQLite in a similar direction, but through a fork rather than a rewrite.
/// Rewriting SQLite in Rust started as an unassuming experiment, and due to its incredible success, replaces libSQL as our intended direction.
impl TursoBackend {
    /// Open a Turso database file and return the Database handle.
    ///
    /// This is used internally by `new()` to create the database before setting up the actor.
    ///
    /// # Platform Support
    /// - **Unix-like systems** (macOS, Linux, BSD, iOS): Full file-based storage support via UnixIO
    /// - **Windows**: Not yet supported
    #[cfg(target_family = "unix")]
    pub fn open_database<P: AsRef<Path>>(db_path: P) -> Result<Arc<Database>> {
        let io = Arc::new(UnixIO::new().map_err(|e| StorageError::DatabaseError(e.to_string()))?);
        let opts = DatabaseOpts::default().with_views(true);

        let db_path_str = db_path
            .as_ref()
            .to_str()
            .ok_or_else(|| StorageError::DatabaseError("Invalid path".to_string()))?;

        let db = Database::open_file_with_flags(io, db_path_str, OpenFlags::default(), opts, None)
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        tracing::info!("Turso database opened at: {}", db_path_str);
        Ok(db)
    }

    #[cfg(not(target_family = "unix"))]
    pub fn open_database<P: AsRef<Path>>(_db_path: P) -> Result<Arc<Database>> {
        Err(StorageError::DatabaseError(
            "File-based storage not yet supported on this platform".to_string(),
        ))
    }

    /// Create a new TursoBackend, spawning an internal actor for database operations.
    ///
    /// This creates a single connection that is owned by the actor and processes
    /// all commands sequentially, eliminating race conditions.
    ///
    /// Returns `(Self, DbHandle)` - the backend and a handle for sending commands.
    pub fn new(
        db: Arc<Database>,
        cdc_broadcast: broadcast::Sender<BatchWithMetadata<RowChange>>,
    ) -> Result<(Self, DbHandle)> {
        // Create connection for actor
        let conn = Self::create_connection_internal(&db)?;

        // Set up CDC callback to broadcast to all subscribers
        let cdc_tx_for_callback = cdc_broadcast.clone();
        conn.set_change_callback(move |event: &RelationChangeEvent| {
            let batch = Self::process_cdc_event(event);
            if !batch.inner.items.is_empty() {
                let _ = cdc_tx_for_callback.send(batch);
            }
        })
        .map_err(|e| StorageError::DatabaseError(format!("Failed to set CDC callback: {}", e)))?;

        // Create command channel
        let (tx, rx) = mpsc::channel(256);

        // Spawn actor loop
        let cdc_broadcast_for_actor = cdc_broadcast.clone();
        tokio::spawn(Self::run_actor(rx, conn, cdc_broadcast_for_actor));

        tracing::info!(
            "[TursoBackend] Created - all database operations will be serialized through internal actor"
        );

        let backend = Self {
            db,
            cdc_broadcast,
            tx: tx.clone(),
        };
        let handle = DbHandle { tx };

        Ok((backend, handle))
    }

    /// Create a new TursoBackend with an in-memory database for testing.
    ///
    /// This convenience method creates a temporary file-based database,
    /// sets up the internal actor, and returns a ready-to-use backend.
    ///
    /// Note: The temporary directory is leaked using `std::mem::forget` to keep
    /// it alive for the duration of the test. This is acceptable for short-lived tests.
    #[cfg(test)]
    pub async fn new_in_memory() -> Result<(Self, DbHandle)> {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().map_err(|e| {
            StorageError::DatabaseError(format!("Failed to create temp dir: {}", e))
        })?;
        let db_path = temp_dir.path().join("test.db");

        let db = Self::open_database(&db_path)?;
        let (cdc_tx, _cdc_rx) = broadcast::channel(1024);

        // Keep the temp dir alive - this leaks memory but is acceptable for tests
        std::mem::forget(temp_dir);

        Self::new(db, cdc_tx)
    }

    /// Get a handle to send commands to the database actor.
    pub fn handle(&self) -> DbHandle {
        DbHandle {
            tx: self.tx.clone(),
        }
    }

    /// Get a reference to the CDC broadcast channel.
    pub fn cdc_broadcast(&self) -> &broadcast::Sender<BatchWithMetadata<RowChange>> {
        &self.cdc_broadcast
    }

    /// Execute DDL with explicit dependency tracking.
    ///
    /// This routes the DDL through the DbHandle which ensures dependencies
    /// are satisfied before execution. Use this for materialized views and other
    /// DDL that depends on tables.
    pub async fn execute_ddl_with_deps(
        &self,
        sql: &str,
        provides: Vec<Resource>,
        requires: Vec<Resource>,
        ddl_priority: u32,
    ) -> Result<()> {
        self.handle()
            .execute_ddl_with_deps(sql, provides, requires, ddl_priority)
            .await
    }

    /// Create a connection from database (internal helper).
    fn create_connection_internal(db: &Arc<Database>) -> Result<turso::Connection> {
        use std::sync::atomic::Ordering;
        static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);
        let conn_id = CONNECTION_COUNTER.fetch_add(1, Ordering::SeqCst);

        tracing::debug!("[CONN-{}] Creating new raw database connection...", conn_id);

        let conn_core = db.connect().map_err(|e| {
            tracing::error!("[CONN-{}] Failed to create connection: {}", conn_id, e);
            StorageError::DatabaseError(e.to_string())
        })?;

        let turso_conn = TursoConnection::new(&default_turso_config(), conn_core);
        let conn = turso::Connection::create(turso_conn, None);

        // Set busy timeout to prevent indefinite hangs on lock contention
        const BUSY_TIMEOUT_SECS: u64 = 30;
        if let Err(e) = conn.busy_timeout(std::time::Duration::from_secs(BUSY_TIMEOUT_SECS)) {
            tracing::warn!(
                "[CONN-{}] Failed to set busy_timeout on raw connection: {}",
                conn_id,
                e
            );
        }

        let autocommit = conn.is_autocommit().unwrap_or(true);
        tracing::debug!(
            "[CONN-{}] Raw connection created with busy_timeout={}s. Autocommit: {}",
            conn_id,
            BUSY_TIMEOUT_SECS,
            autocommit
        );

        Ok(conn)
    }

    /// Get a new connection to the database for direct SQL access.
    ///
    /// This creates a fresh connection without CDC callbacks. Use this for:
    /// - Test code that needs direct SQL access
    /// - Read-only queries that don't need CDC
    /// - Debugging and inspection
    ///
    /// For writes that should trigger CDC, use `handle()` methods instead.
    pub fn get_connection(&self) -> Result<turso::Connection> {
        Self::create_connection_internal(&self.db)
    }

    /// Subscribe to view change notifications.
    ///
    /// Returns a stream of CDC events from the shared broadcast channel.
    /// Multiple callers can subscribe and all will receive the same events.
    ///
    /// The internal actor owns the write connection and sets up CDC callbacks.
    /// All CDC events are broadcast through the shared channel.
    pub async fn row_changes(&self) -> Result<RowChangeStream> {
        tracing::debug!("[TursoBackend] row_changes called, subscribing to broadcast channel...");

        // Subscribe to the broadcast channel
        let mut broadcast_rx = self.cdc_broadcast.subscribe();
        tracing::debug!(
            "[TursoBackend] Subscribed to CDC broadcast, receiver_count={}",
            self.cdc_broadcast.receiver_count()
        );

        // Convert broadcast receiver to mpsc-based stream for compatibility
        // (ReceiverStream expects mpsc::Receiver, not broadcast::Receiver)
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            loop {
                match broadcast_rx.recv().await {
                    Ok(batch) => {
                        if tx.send(batch).await.is_err() {
                            tracing::debug!("[TursoBackend] CDC subscriber stream closed");
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            "[TursoBackend] CDC subscriber lagged by {} messages, continuing",
                            n
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::debug!("[TursoBackend] CDC broadcast channel closed");
                        break;
                    }
                }
            }
        });

        Ok(ReceiverStream::new(rx))
    }

    /// Helper to parse a row of turso_core::Value into our Entity type using schema
    pub fn parse_row_values_with_schema(
        values: &[turso_core::Value],
        columns: &[String],
    ) -> StorageEntity {
        let mut entity = StorageEntity::new();

        for (idx, value) in values.iter().enumerate() {
            let our_value = match value {
                turso_core::Value::Null => Value::Null,
                turso_core::Value::Integer(i) => Value::Integer(*i),
                turso_core::Value::Float(f) => Value::Float(*f),
                turso_core::Value::Text(s) => Value::String(s.to_string()),
                turso_core::Value::Blob(_) => Value::Null,
            };

            // Use column name from schema, or fall back to col_N if schema is incomplete
            let column_name = columns.get(idx).map(|s| s.as_str()).unwrap_or_else(|| {
                eprintln!(
                    "Warning: Column index {} exceeds schema length {}",
                    idx,
                    columns.len()
                );
                "unknown"
            });

            entity.insert(column_name.to_string(), our_value);
        }

        // Flatten 'data' JSON column if present (for heterogeneous UNION queries)
        // Handle both Value::Object (already parsed) and Value::String (needs parsing)
        if let Some(data_value) = entity.remove("data") {
            let data_obj =
                match data_value {
                    Value::Object(obj) => Some(obj),
                    Value::String(s) => serde_json::from_str::<serde_json::Value>(&s)
                        .ok()
                        .and_then(|v| {
                            if let serde_json::Value::Object(map) = v {
                                Some(map.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
                            } else {
                                None
                            }
                        }),
                    _ => None,
                };
            if let Some(obj) = data_obj {
                for (key, value) in obj {
                    entity.entry(key).or_insert(value);
                }
            }
        }

        entity
    }

    pub fn value_to_sql_param(&self, value: &Value) -> String {
        super::sql_utils::value_to_sql_literal(value)
    }

    fn build_where_clause(&self, filter: &Filter, params: &mut Vec<turso::Value>) -> String {
        match filter {
            Filter::Eq(field, value) => {
                params.push(value_to_turso_param(value));
                format!("{} = ?", field)
            }
            Filter::In(field, values) => {
                let placeholders = values
                    .iter()
                    .map(|v| {
                        params.push(value_to_turso_param(v));
                        "?"
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{} IN ({})", field, placeholders)
            }
            Filter::And(filters) => {
                let clauses = filters
                    .iter()
                    .map(|f| self.build_where_clause(f, params))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                format!("({})", clauses)
            }
            Filter::Or(filters) => {
                let clauses = filters
                    .iter()
                    .map(|f| self.build_where_clause(f, params))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                format!("({})", clauses)
            }
            Filter::IsNull(field) => format!("{} IS NULL", field),
            Filter::IsNotNull(field) => format!("{} IS NOT NULL", field),
        }
    }

    /// Execute DDL statement (CREATE TABLE, CREATE INDEX, CREATE VIEW, etc.)
    ///
    /// All DDL is routed through the internal actor which serializes ALL database
    /// operations through a single connection. This eliminates race conditions
    /// between DDL and DML/IVM.
    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        tracing::debug!(
            "[TursoBackend] execute_ddl via actor: {}",
            &sql[..sql.len().min(80)]
        );
        self.handle().execute_ddl(sql).await
    }

    /// Execute a batch of statements as a transaction via the database actor.
    ///
    /// All statements execute on the same connection, preventing "Database schema
    /// changed" errors.
    pub async fn execute_batch_transaction(
        &self,
        statements: Vec<(String, Vec<turso::Value>)>,
    ) -> Result<()> {
        tracing::debug!(
            "[TursoBackend] execute_batch_transaction via actor: {} statements",
            statements.len()
        );
        self.handle().transaction(statements).await
    }

    /// Execute a single statement through the actor (for writes that need CDC).
    pub async fn execute_via_actor(&self, sql: &str, params: Vec<turso::Value>) -> Result<()> {
        tracing::debug!(
            "[TursoBackend] execute_via_actor: {}",
            &sql[..sql.len().min(80)]
        );
        self.handle().execute(sql, params).await?;
        Ok(())
    }

    /// Execute raw SQL query with parameter binding.
    pub async fn execute_sql(
        &self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<Vec<StorageEntity>> {
        self.handle().query(sql, params).await
    }

    /// Execute raw SQL query - alias for execute_sql.
    pub async fn execute_sql_fresh(
        &self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<Vec<StorageEntity>> {
        self.execute_sql(sql, params).await
    }

    // ========================================================================
    // Actor loop and internal handlers
    // ========================================================================

    /// Process a CDC event into a BatchWithMetadata<RowChange>
    fn process_cdc_event(event: &RelationChangeEvent) -> BatchWithMetadata<RowChange> {
        let mut coalescer = CdcCoalescer::new();
        let mut batch_trace_context: Option<BatchTraceContext> = None;

        for change in &event.changes {
            let change_data = match &change.change {
                DatabaseChangeType::Insert { .. } => {
                    if let Some(values) = change.parse_record() {
                        let mut data =
                            TursoBackend::parse_row_values_with_schema(&values, &event.columns);
                        data.insert("_rowid".to_string(), Value::String(change.id.to_string()));
                        let origin = extract_change_origin_from_data(&data);
                        if batch_trace_context.is_none() {
                            batch_trace_context = origin.to_batch_trace_context();
                        }
                        ChangeData::Created { data, origin }
                    } else {
                        continue;
                    }
                }
                DatabaseChangeType::Update { .. } => {
                    if let Some(values) = change.parse_record() {
                        let mut data =
                            TursoBackend::parse_row_values_with_schema(&values, &event.columns);
                        data.insert("_rowid".to_string(), Value::String(change.id.to_string()));
                        let origin = extract_change_origin_from_data(&data);
                        if batch_trace_context.is_none() {
                            batch_trace_context = origin.to_batch_trace_context();
                        }
                        ChangeData::Updated {
                            id: change.id.to_string(),
                            data,
                            origin,
                        }
                    } else {
                        continue;
                    }
                }
                DatabaseChangeType::Delete { .. } => {
                    if let Some(values) = change.parse_record() {
                        let data =
                            TursoBackend::parse_row_values_with_schema(&values, &event.columns);
                        let entity_id = data
                            .get("id")
                            .and_then(|v| match v {
                                Value::String(s) => Some(s.clone()),
                                _ => None,
                            })
                            .unwrap_or_else(|| change.id.to_string());
                        let origin = extract_change_origin_from_data(&data);
                        if batch_trace_context.is_none() {
                            batch_trace_context = origin.to_batch_trace_context();
                        }
                        ChangeData::Deleted {
                            id: entity_id,
                            origin,
                        }
                    } else {
                        ChangeData::Deleted {
                            id: change.id.to_string(),
                            origin: ChangeOrigin::Remote {
                                operation_id: None,
                                trace_id: None,
                            },
                        }
                    }
                }
            };

            let row_change = RowChange {
                relation_name: event.relation_name.clone(),
                change: change_data,
            };
            coalescer.add(row_change);
        }

        let coalesced_changes = coalescer.flush();
        let batch = Batch {
            items: coalesced_changes,
        };
        let metadata = BatchMetadata {
            relation_name: event.relation_name.clone(),
            trace_context: batch_trace_context,
            sync_token: None,
        };

        BatchWithMetadata {
            inner: batch,
            metadata,
        }
    }

    /// Internal actor loop - runs in spawned task
    async fn run_actor(
        mut rx: mpsc::Receiver<DbCommand>,
        conn: turso::Connection,
        cdc_broadcast: broadcast::Sender<BatchWithMetadata<RowChange>>,
    ) {
        tracing::info!("[TursoBackend::Actor] Starting actor loop");

        // Actor state
        let mut phase = DatabasePhase::SchemaInit;
        let mut pending_ddl: VecDeque<PendingDdl> = VecDeque::new();
        let mut available_resources: HashSet<Resource> = HashSet::new();
        let next_op_id = AtomicU64::new(1);

        while let Some(cmd) = rx.recv().await {
            match cmd {
                DbCommand::Query {
                    sql,
                    params,
                    response,
                } => {
                    let result = Self::handle_query(&conn, &sql, params).await;
                    let _ = response.send(result);
                }

                DbCommand::QueryPositional {
                    sql,
                    params,
                    response,
                } => {
                    let result = Self::handle_query_positional(&conn, &sql, params).await;
                    let _ = response.send(result);
                }

                DbCommand::Execute {
                    sql,
                    params,
                    response,
                } => {
                    let result = Self::handle_execute(&conn, &sql, params).await;
                    let _ = response.send(result);
                }

                DbCommand::ExecuteDdl { sql, response } => {
                    let result = Self::handle_ddl(&conn, &sql).await;
                    // Also mark any created resources as available
                    if result.is_ok() {
                        let provides = extract_created_tables(&sql);
                        Self::mark_resources_available(&mut available_resources, &provides);
                    }
                    let _ = response.send(result);
                }

                DbCommand::ExecuteDdlWithDeps {
                    sql,
                    provides,
                    requires,
                    priority,
                    response,
                } => {
                    Self::handle_ddl_with_deps_internal(
                        &conn,
                        &next_op_id,
                        &mut pending_ddl,
                        &mut available_resources,
                        sql,
                        provides,
                        requires,
                        priority,
                        response,
                    )
                    .await;
                }

                DbCommand::ExecuteDdlAuto {
                    sql,
                    priority,
                    response,
                } => {
                    // Auto-infer dependencies from SQL
                    let provides = extract_created_tables(&sql);
                    let mut requires = extract_table_refs(&sql);
                    // Remove self-references
                    for provided in &provides {
                        requires.retain(|r| r != provided);
                    }
                    Self::handle_ddl_with_deps_internal(
                        &conn,
                        &next_op_id,
                        &mut pending_ddl,
                        &mut available_resources,
                        sql,
                        provides,
                        requires,
                        priority,
                        response,
                    )
                    .await;
                }

                DbCommand::MarkAvailable { resources } => {
                    Self::mark_resources_available(&mut available_resources, &resources);
                    // Process any pending operations that may now be ready
                    Self::process_pending_ddl(&conn, &mut pending_ddl, &mut available_resources)
                        .await;
                }

                DbCommand::ResourceExists { resource, response } => {
                    let exists = available_resources.contains(&resource);
                    let _ = response.send(exists);
                }

                DbCommand::Transaction {
                    statements,
                    response,
                } => {
                    let result = Self::handle_transaction(&conn, statements).await;
                    let _ = response.send(result);
                }

                DbCommand::SubscribeCdc { relation, response } => {
                    // Return a receiver that filters for the requested relation
                    let rx = cdc_broadcast.subscribe();
                    let _ = response.send(Ok(rx));
                    tracing::debug!(
                        "[TursoBackend::Actor] CDC subscription created for relation: {}",
                        relation
                    );
                }

                DbCommand::TransitionToReady { response } => {
                    phase = DatabasePhase::Ready;
                    tracing::info!("[TursoBackend::Actor] Transitioned to Ready phase");
                    let _ = response.send(Ok(()));
                }

                DbCommand::GetPhase { response } => {
                    let _ = response.send(phase);
                }

                DbCommand::Shutdown { response } => {
                    phase = DatabasePhase::ShuttingDown;
                    tracing::info!("[TursoBackend::Actor] Shutting down");
                    let _ = response.send(());
                    break;
                }
            }
        }

        tracing::info!("[TursoBackend::Actor] Actor loop ended");
    }

    /// Handle a query command
    async fn handle_query(
        conn: &turso::Connection,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<Vec<StorageEntity>> {
        // Bind named parameters to positional placeholders
        let (sql_with_placeholders, param_values) = bind_parameters(sql, &params)?;

        let mut stmt = conn
            .prepare(&sql_with_placeholders)
            .await
            .map_err(|e| StorageError::DatabaseError(format!("Failed to prepare query: {}", e)))?;

        let columns = stmt.columns();

        let mut rows = stmt
            .query(param_values)
            .await
            .map_err(|e| StorageError::QueryError(format!("Failed to execute query: {}", e)))?;

        let mut results = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| StorageError::QueryError(format!("Failed to fetch row: {}", e)))?
        {
            let mut entity = StorageEntity::new();

            for (idx, column) in columns.iter().enumerate() {
                let col_name = column.name();
                let value = row.get_value(idx).map_err(|e| {
                    StorageError::QueryError(format!("Failed to get column value: {}", e))
                })?;

                entity.insert(col_name.to_string(), turso_value_to_value(value.into()));
            }

            // Flatten 'data' JSON column if present
            if let Some(data_value) = entity.remove("data") {
                if let Some(obj) = flatten_data_column(data_value) {
                    for (key, value) in obj {
                        entity.entry(key).or_insert(value);
                    }
                }
            }

            results.push(entity);
        }

        Ok(results)
    }

    /// Handle a query command with positional parameters
    async fn handle_query_positional(
        conn: &turso::Connection,
        sql: &str,
        params: Vec<turso::Value>,
    ) -> Result<Vec<StorageEntity>> {
        let mut stmt = conn
            .prepare(sql)
            .await
            .map_err(|e| StorageError::DatabaseError(format!("Failed to prepare query: {}", e)))?;

        let columns = stmt.columns();

        let mut rows = stmt
            .query(params)
            .await
            .map_err(|e| StorageError::QueryError(format!("Failed to execute query: {}", e)))?;

        let mut results = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| StorageError::QueryError(format!("Failed to fetch row: {}", e)))?
        {
            let mut entity = StorageEntity::new();

            for (idx, column) in columns.iter().enumerate() {
                let col_name = column.name();
                let value = row.get_value(idx).map_err(|e| {
                    StorageError::QueryError(format!("Failed to get column value: {}", e))
                })?;

                entity.insert(col_name.to_string(), turso_value_to_value(value.into()));
            }

            // Flatten 'data' JSON column if present
            if let Some(data_value) = entity.remove("data") {
                if let Some(obj) = flatten_data_column(data_value) {
                    for (key, value) in obj {
                        entity.entry(key).or_insert(value);
                    }
                }
            }

            results.push(entity);
        }

        Ok(results)
    }

    /// Handle an execute command
    async fn handle_execute(
        conn: &turso::Connection,
        sql: &str,
        params: Vec<turso::Value>,
    ) -> Result<u64> {
        let mut stmt = conn.prepare(sql).await.map_err(|e| {
            StorageError::DatabaseError(format!("Failed to prepare statement: {}", e))
        })?;

        let rows_affected = stmt.execute(params).await.map_err(|e| {
            StorageError::DatabaseError(format!("Failed to execute statement: {}", e))
        })?;

        Ok(rows_affected)
    }

    /// Handle a DDL command
    async fn handle_ddl(conn: &turso::Connection, sql: &str) -> Result<()> {
        tracing::debug!("[TursoBackend::Actor] Executing DDL: {}", sql);

        conn.execute(sql, ())
            .await
            .map_err(|e| StorageError::DatabaseError(format!("Failed to execute DDL: {}", e)))?;

        tracing::debug!("[TursoBackend::Actor] DDL completed successfully");
        Ok(())
    }

    /// Handle a transaction command
    async fn handle_transaction(
        conn: &turso::Connection,
        statements: Vec<(String, Vec<turso::Value>)>,
    ) -> Result<()> {
        // Begin transaction
        conn.execute("BEGIN TRANSACTION", ()).await.map_err(|e| {
            StorageError::DatabaseError(format!("Failed to begin transaction: {}", e))
        })?;

        // Execute each statement, rolling back on any error
        let result = Self::execute_statements_in_transaction(conn, statements).await;

        if result.is_err() {
            // Rollback on error
            if let Err(rollback_err) = conn.execute("ROLLBACK", ()).await {
                tracing::error!(
                    "[TursoBackend::Actor] Failed to rollback transaction: {}",
                    rollback_err
                );
            }
            return result;
        }

        // Commit transaction
        conn.execute("COMMIT", ()).await.map_err(|e| {
            // Try to rollback on commit failure (best effort)
            tracing::error!(
                "[TursoBackend::Actor] Commit failed, attempting rollback: {}",
                e
            );
            // Note: We can't easily rollback here since commit failed, connection may be in bad state
            StorageError::DatabaseError(format!("Failed to commit transaction: {}", e))
        })?;

        Ok(())
    }

    /// Execute statements within a transaction (helper for proper error handling)
    async fn execute_statements_in_transaction(
        conn: &turso::Connection,
        statements: Vec<(String, Vec<turso::Value>)>,
    ) -> Result<()> {
        for (sql, params) in statements {
            let mut stmt = conn.prepare(&sql).await.map_err(|e| {
                StorageError::DatabaseError(format!("Failed to prepare statement: {}", e))
            })?;

            stmt.execute(params).await.map_err(|e| {
                StorageError::DatabaseError(format!("Failed to execute statement: {}", e))
            })?;
        }
        Ok(())
    }

    // --- Dependency tracking methods ---

    /// Mark resources as available and log them
    fn mark_resources_available(
        available_resources: &mut HashSet<Resource>,
        resources: &[Resource],
    ) {
        for resource in resources {
            available_resources.insert(resource.clone());
        }
        if !resources.is_empty() {
            tracing::debug!(
                "[TursoBackend::Actor] Marked {} resources as available: {:?}",
                resources.len(),
                resources.iter().map(|r| r.name()).collect::<Vec<_>>()
            );
        }
    }

    /// Check if all required resources are available
    fn can_execute_ddl(available_resources: &HashSet<Resource>, op: &PendingDdl) -> bool {
        op.requires.iter().all(|r| available_resources.contains(r))
    }

    /// Handle DDL with dependency tracking
    async fn handle_ddl_with_deps_internal(
        conn: &turso::Connection,
        next_op_id: &AtomicU64,
        pending_ddl: &mut VecDeque<PendingDdl>,
        available_resources: &mut HashSet<Resource>,
        sql: String,
        provides: Vec<Resource>,
        requires: Vec<Resource>,
        priority: u32,
        response: oneshot::Sender<Result<()>>,
    ) {
        let op_id = next_op_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let op = PendingDdl {
            id: op_id,
            sql,
            provides,
            requires,
            priority,
            response,
        };

        // Check if we can execute immediately
        if Self::can_execute_ddl(available_resources, &op) {
            Self::execute_pending_ddl(conn, available_resources, op).await;
        } else {
            tracing::debug!(
                "[TursoBackend::Actor] DDL op {} queued, waiting for: {:?}",
                op_id,
                op.requires
                    .iter()
                    .filter(|r| !available_resources.contains(r))
                    .map(|r| r.name())
                    .collect::<Vec<_>>()
            );
            pending_ddl.push_back(op);
        }
    }

    /// Execute a pending DDL operation
    async fn execute_pending_ddl(
        conn: &turso::Connection,
        available_resources: &mut HashSet<Resource>,
        op: PendingDdl,
    ) {
        tracing::debug!("[TursoBackend::Actor] Executing DDL op {}", op.id);

        let result = Self::handle_ddl(conn, &op.sql).await;

        if result.is_ok() {
            // Mark provided resources as available
            Self::mark_resources_available(available_resources, &op.provides);
        }

        let _ = op.response.send(result);
    }

    /// Process pending DDL operations that may now be ready
    async fn process_pending_ddl(
        conn: &turso::Connection,
        pending_ddl: &mut VecDeque<PendingDdl>,
        available_resources: &mut HashSet<Resource>,
    ) {
        // Collect ready operations
        let mut ready = Vec::new();
        let mut still_pending = VecDeque::new();

        while let Some(op) = pending_ddl.pop_front() {
            if Self::can_execute_ddl(available_resources, &op) {
                ready.push(op);
            } else {
                still_pending.push_back(op);
            }
        }

        *pending_ddl = still_pending;

        // Sort by priority (highest first)
        ready.sort_by(|a, b| b.priority.cmp(&a.priority));

        // Execute ready operations
        for op in ready {
            Self::execute_pending_ddl(conn, available_resources, op).await;
            // After each execution, more ops may become ready
            // Recursively process (this is safe since we drain the queue)
        }

        // Recursively check if new ops are now ready
        if !pending_ddl.is_empty() {
            let still_waiting: Vec<_> = pending_ddl
                .iter()
                .filter(|op| Self::can_execute_ddl(available_resources, op))
                .map(|op| op.id)
                .collect();

            if !still_waiting.is_empty() {
                // Some ops became ready during execution, recurse
                Box::pin(Self::process_pending_ddl(
                    conn,
                    pending_ddl,
                    available_resources,
                ))
                .await;
            }
        }
    }

    // ========================================================================
    // Deprecated method for backward compatibility during transition
    // ========================================================================

    /// Get a reference to the database actor handle.
    ///
    /// DEPRECATED: Use `handle()` instead. This method exists only for backward
    /// compatibility during the DatabaseActor -> TursoBackend merge.
    #[deprecated(since = "0.1.0", note = "Use handle() instead")]
    pub fn db_handle(&self) -> DbHandle {
        self.handle()
    }
}

#[async_trait]
impl StorageBackend for TursoBackend {
    async fn create_entity(&self, schema: &EntitySchema) -> Result<()> {
        let mut field_defs = Vec::new();

        for field in &schema.fields {
            let mut def = format!("{} {}", field.name, field.field_type.to_sqlite_type());

            if field.name == schema.primary_key {
                def.push_str(" PRIMARY KEY");
            }

            if field.required {
                def.push_str(" NOT NULL");
            }

            field_defs.push(def);
        }

        field_defs.push("_version TEXT".to_string());

        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            schema.name,
            field_defs.join(", ")
        );

        self.handle().execute_ddl(&create_table_sql).await?;

        for field in &schema.fields {
            if field.indexed {
                let index_sql = format!(
                    "CREATE INDEX IF NOT EXISTS idx_{}_{} ON {} ({})",
                    schema.name, field.name, schema.name, field.name
                );
                self.handle().execute_ddl(&index_sql).await?;
            }
        }

        Ok(())
    }

    async fn get(&self, entity: &str, id: &str) -> Result<Option<StorageEntity>> {
        let query_str = format!("SELECT * FROM {} WHERE id = $id", entity);
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(id.to_string()));
        let results = self.handle().query(&query_str, params).await?;
        Ok(results.into_iter().next())
    }

    async fn query(&self, entity: &str, filter: Filter) -> Result<Vec<StorageEntity>> {
        let mut params = Vec::new();
        let where_clause = self.build_where_clause(&filter, &mut params);
        let query_str = format!("SELECT * FROM {} WHERE {}", entity, where_clause);
        self.handle().query_positional(&query_str, params).await
    }

    async fn insert(&self, schema: &holon_api::Schema, data: StorageEntity) -> Result<()> {
        let fields: Vec<_> = data.keys().collect();
        let placeholders: Vec<String> = fields
            .iter()
            .map(|f| {
                if schema.field_is_jsonb(f) {
                    "jsonb(?)".to_string()
                } else {
                    "?".to_string()
                }
            })
            .collect();

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            schema.table_name,
            fields
                .iter()
                .map(|f| f.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            placeholders.join(", ")
        );

        let params: Vec<turso::Value> = data.values().map(|v| value_to_turso_param(v)).collect();

        self.handle().execute(&insert_sql, params).await?;
        Ok(())
    }

    async fn update(
        &self,
        schema: &holon_api::Schema,
        id: &str,
        data: StorageEntity,
    ) -> Result<()> {
        let filtered_data: Vec<_> = data.iter().filter(|(k, _)| k.as_str() != "id").collect();

        let set_clauses: Vec<String> = filtered_data
            .iter()
            .map(|(k, _)| {
                if schema.field_is_jsonb(k) {
                    format!("{} = jsonb(?)", k)
                } else {
                    format!("{} = ?", k)
                }
            })
            .collect();

        let update_sql = format!(
            "UPDATE {} SET {} WHERE id = ?",
            schema.table_name,
            set_clauses.join(", ")
        );

        let mut params: Vec<turso::Value> = filtered_data
            .iter()
            .map(|(_, v)| value_to_turso_param(v))
            .collect();
        params.push(turso::Value::Text(id.to_string()));

        self.handle().execute(&update_sql, params).await?;
        Ok(())
    }

    async fn delete(&self, entity: &str, id: &str) -> Result<()> {
        let delete_sql = format!("DELETE FROM {} WHERE id = ?", entity);
        let params = vec![turso::Value::Text(id.to_string())];
        self.handle().execute(&delete_sql, params).await?;
        Ok(())
    }

    async fn get_version(&self, entity: &str, id: &str) -> Result<Option<String>> {
        let query = format!("SELECT _version FROM {} WHERE id = ?", entity);
        let params = vec![turso::Value::Text(id.to_string())];
        let results = self.handle().query_positional(&query, params).await?;
        if let Some(row) = results.into_iter().next() {
            return match row.get("_version") {
                Some(Value::String(s)) => Ok(Some(s.clone())),
                Some(Value::Null) | None => Ok(None),
                _ => Ok(None),
            };
        }
        Ok(None)
    }

    async fn set_version(&self, entity: &str, id: &str, version: String) -> Result<()> {
        let update_sql = format!("UPDATE {} SET _version = ? WHERE id = ?", entity);
        let params = vec![
            turso::Value::Text(version.clone()),
            turso::Value::Text(id.to_string()),
        ];
        self.handle().execute(&update_sql, params).await?;
        Ok(())
    }

    async fn get_children(
        &self,
        entity: &str,
        parent_field: &str,
        parent_id: &str,
    ) -> Result<Vec<StorageEntity>> {
        let filter = Filter::Eq(
            parent_field.to_string(),
            Value::String(parent_id.to_string()),
        );
        self.query(entity, filter).await
    }

    async fn get_related(
        &self,
        entity: &str,
        foreign_key: &str,
        related_id: &str,
    ) -> Result<Vec<StorageEntity>> {
        let filter = Filter::Eq(
            foreign_key.to_string(),
            Value::String(related_id.to_string()),
        );
        self.query(entity, filter).await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[path = "turso_tests.rs"]
mod turso_tests;

#[cfg(test)]
#[path = "turso_pbt_tests.rs"]
mod turso_pbt_tests;

#[cfg(test)]
#[path = "turso_matview_test.rs"]
mod turso_matview_test;

#[cfg(test)]
#[path = "turso_ivm_join_test.rs"]
mod turso_ivm_join_test;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_phase_default() {
        let phase = DatabasePhase::default();
        assert_eq!(phase, DatabasePhase::SchemaInit);
    }

    #[test]
    fn test_flatten_data_column_object() {
        let mut obj = HashMap::new();
        obj.insert("key1".to_string(), Value::String("value1".to_string()));
        obj.insert("key2".to_string(), Value::Integer(42));

        let result = flatten_data_column(Value::Object(obj.clone()));
        assert!(result.is_some());
        let flattened = result.unwrap();
        assert_eq!(
            flattened.get("key1"),
            Some(&Value::String("value1".to_string()))
        );
        assert_eq!(flattened.get("key2"), Some(&Value::Integer(42)));
    }

    #[test]
    fn test_flatten_data_column_json_string() {
        let json_str = r#"{"key1": "value1", "key2": 42}"#;
        let result = flatten_data_column(Value::String(json_str.to_string()));
        assert!(result.is_some());
        let flattened = result.unwrap();
        assert_eq!(
            flattened.get("key1"),
            Some(&Value::String("value1".to_string()))
        );
    }

    #[test]
    fn test_flatten_data_column_non_json() {
        let result = flatten_data_column(Value::String("not json".to_string()));
        assert!(result.is_none());
    }

    #[test]
    fn test_flatten_data_column_null() {
        let result = flatten_data_column(Value::Null);
        assert!(result.is_none());
    }

    #[test]
    fn test_turso_value_to_value_conversions() {
        assert_eq!(turso_value_to_value(turso_core::Value::Null), Value::Null);
        assert_eq!(
            turso_value_to_value(turso_core::Value::Integer(42)),
            Value::Integer(42)
        );
        assert_eq!(
            turso_value_to_value(turso_core::Value::Float(3.14)),
            Value::Float(3.14)
        );

        // Plain string
        let text_val = turso_value_to_value(turso_core::Value::Text("hello".into()));
        assert_eq!(text_val, Value::String("hello".to_string()));

        // JSON array string gets parsed
        let arr_val = turso_value_to_value(turso_core::Value::Text("[1, 2, 3]".into()));
        assert!(matches!(arr_val, Value::Array(_)));
    }
}

#[cfg(test)]
mod cdc_coalescer_tests {
    use super::*;

    fn make_insert(view: &str, id: &str, value: &str) -> RowChange {
        let mut data = StorageEntity::new();
        data.insert("id".to_string(), Value::String(id.to_string()));
        data.insert("value".to_string(), Value::String(value.to_string()));
        data.insert("_rowid".to_string(), Value::String(id.to_string()));
        RowChange {
            relation_name: view.to_string(),
            change: ChangeData::Created {
                data,
                origin: ChangeOrigin::Remote {
                    operation_id: None,
                    trace_id: None,
                },
            },
        }
    }

    fn make_delete(view: &str, id: &str) -> RowChange {
        RowChange {
            relation_name: view.to_string(),
            change: ChangeData::Deleted {
                id: id.to_string(),
                origin: ChangeOrigin::Remote {
                    operation_id: None,
                    trace_id: None,
                },
            },
        }
    }

    fn make_update(view: &str, id: &str, value: &str) -> RowChange {
        let mut data = StorageEntity::new();
        data.insert("id".to_string(), Value::String(id.to_string()));
        data.insert("value".to_string(), Value::String(value.to_string()));
        data.insert("_rowid".to_string(), Value::String(id.to_string()));
        RowChange {
            relation_name: view.to_string(),
            change: ChangeData::Updated {
                id: id.to_string(),
                data,
                origin: ChangeOrigin::Remote {
                    operation_id: None,
                    trace_id: None,
                },
            },
        }
    }

    #[test]
    fn test_coalesce_delete_insert_becomes_update() {
        let mut coalescer = CdcCoalescer::new();
        coalescer.add(make_delete("view1", "id1"));
        coalescer.add(make_insert("view1", "id1", "new_value"));

        let result = coalescer.flush();
        assert_eq!(result.len(), 1);
        match &result[0].change {
            ChangeData::Updated { id, data, .. } => {
                assert_eq!(id, "id1");
                assert_eq!(
                    data.get("value").unwrap(),
                    &Value::String("new_value".to_string())
                );
            }
            _ => panic!("Expected Update, got {:?}", result[0].change),
        }
    }

    #[test]
    fn test_coalesce_standalone_delete_unchanged() {
        let mut coalescer = CdcCoalescer::new();
        coalescer.add(make_delete("view1", "id1"));

        let result = coalescer.flush();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0].change, ChangeData::Deleted { .. }));
    }

    #[test]
    fn test_coalesce_standalone_insert_unchanged() {
        let mut coalescer = CdcCoalescer::new();
        coalescer.add(make_insert("view1", "id1", "value1"));

        let result = coalescer.flush();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0].change, ChangeData::Created { .. }));
    }

    #[test]
    fn test_coalesce_update_unchanged() {
        let mut coalescer = CdcCoalescer::new();
        coalescer.add(make_update("view1", "id1", "value1"));

        let result = coalescer.flush();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0].change, ChangeData::Updated { .. }));
    }

    #[test]
    fn test_coalesce_multiple_different_ids() {
        let mut coalescer = CdcCoalescer::new();
        coalescer.add(make_delete("view1", "id1"));
        coalescer.add(make_insert("view1", "id1", "new1"));
        coalescer.add(make_delete("view1", "id2"));
        coalescer.add(make_insert("view1", "id2", "new2"));

        let result = coalescer.flush();
        assert_eq!(result.len(), 2);
        for change in &result {
            assert!(matches!(change.change, ChangeData::Updated { .. }));
        }
    }

    #[test]
    fn test_coalesce_different_views_not_coalesced() {
        let mut coalescer = CdcCoalescer::new();
        coalescer.add(make_delete("view1", "id1"));
        coalescer.add(make_insert("view2", "id1", "value1"));

        let result = coalescer.flush();
        assert_eq!(result.len(), 2);
        assert!(matches!(result[0].change, ChangeData::Deleted { .. }));
        assert!(matches!(result[1].change, ChangeData::Created { .. }));
    }

    #[test]
    fn test_coalesce_insert_delete_different_id() {
        let mut coalescer = CdcCoalescer::new();
        coalescer.add(make_delete("view1", "id1"));
        coalescer.add(make_insert("view1", "id2", "value"));

        let result = coalescer.flush();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_coalesce_insert_delete_becomes_noop() {
        let mut coalescer = CdcCoalescer::new();
        coalescer.add(make_insert("view1", "id1", "value1"));
        coalescer.add(make_delete("view1", "id1"));

        let result = coalescer.flush();
        assert_eq!(result.len(), 0, "INSERT then DELETE should result in no-op");
    }

    #[test]
    fn test_coalesce_insert_delete_insert_becomes_update() {
        let mut coalescer = CdcCoalescer::new();
        coalescer.add(make_insert("view1", "id1", "value1"));
        coalescer.add(make_delete("view1", "id1"));
        coalescer.add(make_insert("view1", "id1", "value2"));

        let result = coalescer.flush();
        assert_eq!(result.len(), 1);
        match &result[0].change {
            ChangeData::Created { data, .. } => {
                assert_eq!(
                    data.get("value").unwrap(),
                    &Value::String("value2".to_string())
                );
            }
            _ => panic!("Expected Created, got {:?}", result[0].change),
        }
    }
}

/// Integration tests that require a real database
/// These tests verify the backend's core functionality:
/// - Serialization of concurrent operations
/// - DDL works in all phases
/// - CDC subscriptions work correctly
#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::RwLock;

    /// Helper to create a test backend
    async fn create_test_backend() -> Result<(Arc<RwLock<TursoBackend>>, DbHandle)> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_actor.db");

        // Open database
        let db = TursoBackend::open_database(&db_path)?;

        // Create CDC broadcast channel
        let (cdc_tx, _) = broadcast::channel(1024);

        // Create backend (which internally spawns the actor)
        let (backend, handle) = TursoBackend::new(db, cdc_tx)?;

        // Keep the temp dir alive
        std::mem::forget(temp_dir);

        Ok((Arc::new(RwLock::new(backend)), handle))
    }

    /// Test that DDL operations work and are properly serialized
    #[tokio::test]
    async fn test_ddl_operations() {
        let (_backend, handle) = create_test_backend().await.unwrap();

        // Create a table
        handle
            .execute_ddl("CREATE TABLE test_ddl (id TEXT PRIMARY KEY, value TEXT)")
            .await
            .expect("DDL should succeed");

        // Create an index
        handle
            .execute_ddl("CREATE INDEX idx_test_ddl_value ON test_ddl(value)")
            .await
            .expect("DDL for index should succeed");

        // Verify table exists by inserting data
        let insert_result = handle
            .execute(
                "INSERT INTO test_ddl (id, value) VALUES (?, ?)",
                vec![
                    turso::Value::Text("id1".to_string()),
                    turso::Value::Text("value1".to_string()),
                ],
            )
            .await;
        assert!(insert_result.is_ok(), "Insert after DDL should succeed");

        // Shutdown
        handle.shutdown().await.unwrap();
    }

    /// Test that DDL is allowed in Ready phase (for dynamic MatView creation)
    #[tokio::test]
    async fn test_ddl_allowed_in_ready_phase() {
        let (_backend, handle) = create_test_backend().await.unwrap();

        // Create initial table
        handle
            .execute_ddl("CREATE TABLE test_ready (id TEXT PRIMARY KEY, value INTEGER)")
            .await
            .unwrap();

        // Transition to Ready phase
        handle.transition_to_ready().await.unwrap();

        // Verify we're in Ready phase
        let phase = handle.get_phase().await.unwrap();
        assert_eq!(phase, DatabasePhase::Ready);

        // DDL should STILL work in Ready phase (for dynamic MatView creation)
        let ddl_result = handle
            .execute_ddl("CREATE TABLE another_table (id TEXT PRIMARY KEY)")
            .await;
        assert!(ddl_result.is_ok(), "DDL should work in Ready phase");

        // Create a view in Ready phase (simulates PRQL block MatView creation)
        let view_result = handle
            .execute_ddl("CREATE VIEW test_view AS SELECT * FROM test_ready WHERE value > 0")
            .await;
        assert!(
            view_result.is_ok(),
            "View creation should work in Ready phase"
        );

        handle.shutdown().await.unwrap();
    }

    /// Test that concurrent queries are serialized (no "database locked" errors)
    #[tokio::test]
    async fn test_query_serialization() {
        let (_backend, handle) = create_test_backend().await.unwrap();

        // Create table
        handle
            .execute_ddl("CREATE TABLE test_serial (id INTEGER PRIMARY KEY, value INTEGER)")
            .await
            .unwrap();

        // Insert some data
        for i in 0..10 {
            handle
                .execute(
                    "INSERT INTO test_serial (id, value) VALUES (?, ?)",
                    vec![turso::Value::Integer(i), turso::Value::Integer(i * 10)],
                )
                .await
                .unwrap();
        }

        handle.transition_to_ready().await.unwrap();

        // Spawn 100 concurrent queries
        let mut query_handles = Vec::new();
        for _ in 0..100 {
            let h = handle.clone();
            query_handles.push(tokio::spawn(async move {
                h.query("SELECT * FROM test_serial", HashMap::new()).await
            }));
        }

        // All queries should succeed (serialized by actor, no "database locked")
        let mut success_count = 0;
        for query_handle in query_handles {
            match query_handle.await {
                Ok(Ok(results)) => {
                    assert_eq!(results.len(), 10, "Each query should return 10 rows");
                    success_count += 1;
                }
                Ok(Err(e)) => {
                    panic!("Query failed with error: {:?}", e);
                }
                Err(e) => {
                    panic!("Task panicked: {:?}", e);
                }
            }
        }
        assert_eq!(
            success_count, 100,
            "All 100 concurrent queries should succeed"
        );

        handle.shutdown().await.unwrap();
    }

    /// Test that interleaved DDL and DML operations are serialized correctly
    #[tokio::test]
    async fn test_ddl_dml_interleaved() {
        let (_backend, handle) = create_test_backend().await.unwrap();

        // Create initial table
        handle
            .execute_ddl("CREATE TABLE test_interleave (id TEXT PRIMARY KEY, value TEXT)")
            .await
            .unwrap();

        handle.transition_to_ready().await.unwrap();

        // Spawn interleaved DDL and DML operations
        let mut dml_handles = Vec::new();
        let mut ddl_handles = Vec::new();

        // DML operations (inserts)
        for i in 0..20 {
            let h = handle.clone();
            dml_handles.push(tokio::spawn(async move {
                h.execute(
                    "INSERT INTO test_interleave (id, value) VALUES (?, ?)",
                    vec![
                        turso::Value::Text(format!("id_{}", i)),
                        turso::Value::Text(format!("value_{}", i)),
                    ],
                )
                .await
            }));
        }

        // DDL operations (create views) - simulates PRQL block navigation
        for i in 0..5 {
            let h = handle.clone();
            ddl_handles.push(tokio::spawn(async move {
                h.execute_ddl(&format!(
                    "CREATE VIEW IF NOT EXISTS view_{} AS SELECT * FROM test_interleave WHERE id LIKE 'id_%'",
                    i
                ))
                .await
            }));
        }

        // All operations should succeed without "Database schema changed" errors
        let mut errors = Vec::new();
        for join_handle in dml_handles {
            match join_handle.await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => errors.push(format!("{:?}", e)),
                Err(e) => errors.push(format!("Task panicked: {:?}", e)),
            }
        }
        for join_handle in ddl_handles {
            match join_handle.await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => errors.push(format!("{:?}", e)),
                Err(e) => errors.push(format!("Task panicked: {:?}", e)),
            }
        }

        assert!(
            errors.is_empty(),
            "No errors expected from interleaved DDL/DML, got: {:?}",
            errors
        );

        handle.shutdown().await.unwrap();
    }

    /// Test phase transitions
    #[tokio::test]
    async fn test_phase_transitions() {
        let (_backend, handle) = create_test_backend().await.unwrap();

        // Initially in SchemaInit phase
        let phase = handle.get_phase().await.unwrap();
        assert_eq!(phase, DatabasePhase::SchemaInit);

        // Transition to Ready
        handle.transition_to_ready().await.unwrap();
        let phase = handle.get_phase().await.unwrap();
        assert_eq!(phase, DatabasePhase::Ready);

        // Shutdown transitions to ShuttingDown (implicitly during shutdown)
        handle.shutdown().await.unwrap();
    }

    /// Test transaction support
    #[tokio::test]
    async fn test_transactions() {
        let (_backend, handle) = create_test_backend().await.unwrap();

        // Create table
        handle
            .execute_ddl("CREATE TABLE test_tx (id INTEGER PRIMARY KEY, value TEXT)")
            .await
            .unwrap();

        // Execute multiple statements in a transaction
        let statements = vec![
            (
                "INSERT INTO test_tx (id, value) VALUES (1, 'first')".to_string(),
                vec![],
            ),
            (
                "INSERT INTO test_tx (id, value) VALUES (2, 'second')".to_string(),
                vec![],
            ),
            (
                "UPDATE test_tx SET value = 'updated' WHERE id = 1".to_string(),
                vec![],
            ),
        ];

        handle.transaction(statements).await.unwrap();

        // Verify transaction results
        let results = handle
            .query("SELECT * FROM test_tx ORDER BY id", HashMap::new())
            .await
            .unwrap();
        assert_eq!(results.len(), 2, "Should have 2 rows after transaction");

        handle.shutdown().await.unwrap();
    }
}
