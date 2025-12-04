use async_trait::async_trait;
use serde_json;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
#[cfg(target_family = "unix")]
use turso_core::UnixIO;
use turso_core::{Database, DatabaseOpts, MemoryIO, OpenFlags};

use crate::api::{Change, ChangeOrigin};
use crate::storage::{
    backend::StorageBackend,
    schema::{EntitySchema, FieldType},
    types::{Filter, Result, StorageEntity, StorageError},
};
use holon_api::{
    Batch, BatchMetadata, BatchTraceContext, BatchWithMetadata, Value, CHANGE_ORIGIN_COLUMN,
};

/// Extract ChangeOrigin from row data's _change_origin column
///
/// If the column is present and contains valid JSON, parse it as ChangeOrigin.
/// Otherwise, return a default Remote origin without trace context.
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
struct CdcCoalescer {
    changes: Vec<Option<RowChange>>,
    pending_deletes: HashMap<(String, String), usize>,
    pending_inserts: HashMap<(String, String), usize>,
}

impl CdcCoalescer {
    fn new() -> Self {
        Self {
            changes: Vec::new(),
            pending_deletes: HashMap::new(),
            pending_inserts: HashMap::new(),
        }
    }

    fn add(&mut self, change: RowChange) {
        self.changes.push(Some(change));
    }

    fn flush(&mut self) -> Vec<RowChange> {
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
                }
            }
        }

        self.pending_deletes.clear();
        self.pending_inserts.clear();
        self.changes.drain(..).flatten().collect()
    }
}

/// Connection pool for reusing database connections
///
/// Uses a semaphore to limit concurrent connections and a channel
/// to manage connection reuse. This prevents creating dozens of
/// connections for rapid CRUD operations.
#[derive(Clone)]
struct ConnectionPool {
    /// Semaphore to limit total concurrent connections
    semaphore: Arc<Semaphore>,
    /// Channel for available connections (reused connections)
    available: Arc<Mutex<mpsc::UnboundedReceiver<turso::Connection>>>,
    /// Sender to return connections to the pool
    return_tx: mpsc::UnboundedSender<turso::Connection>,
    /// Maximum pool size
    max_pool_size: usize,
    /// Database to create new connections from
    db: Arc<Database>,
}

impl ConnectionPool {
    fn new(db: Arc<Database>, max_pool_size: usize) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            semaphore: Arc::new(Semaphore::new(max_pool_size)),
            available: Arc::new(Mutex::new(rx)),
            return_tx: tx,
            max_pool_size,
            db,
        }
    }

    /// Get a connection from the pool, creating a new one if needed
    fn get_connection(&self) -> Result<PooledConnection> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);
        let conn_id = CONNECTION_COUNTER.fetch_add(1, Ordering::SeqCst);

        // Try to get a connection from the pool first
        let mut available = self.available.try_lock().map_err(|_| {
            StorageError::DatabaseError("Failed to lock connection pool".to_string())
        })?;

        match available.try_recv() {
            Ok(conn) => {
                tracing::debug!("[CONN-{}] Reusing connection from pool", conn_id);
                return Ok(PooledConnection {
                    conn: Some(conn),
                    return_tx: Some(self.return_tx.clone()),
                    conn_id,
                });
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                // Pool is empty, will create new connection below
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                // Channel disconnected, will create new connection below
            }
        }

        // No available connection, create a new one
        tracing::debug!("[CONN-{}] Creating new database connection...", conn_id);

        let conn_core = self.db.connect().map_err(|e| {
            tracing::error!("[CONN-{}] Failed to create connection: {}", conn_id, e);
            StorageError::DatabaseError(e.to_string())
        })?;

        let conn = turso::Connection::create(conn_core);

        let autocommit = conn.is_autocommit().unwrap_or(true);
        tracing::debug!(
            "[CONN-{}] Connection created. Autocommit: {}",
            conn_id,
            autocommit
        );

        Ok(PooledConnection {
            conn: Some(conn),
            return_tx: Some(self.return_tx.clone()),
            conn_id,
        })
    }
}

/// A connection that returns itself to the pool when dropped
pub struct PooledConnection {
    conn: Option<turso::Connection>,
    return_tx: Option<mpsc::UnboundedSender<turso::Connection>>,
    conn_id: u64,
}

impl PooledConnection {
    /// Take the connection (for long-lived connections like CDC)
    fn take(mut self) -> turso::Connection {
        self.return_tx.take(); // Don't return to pool
        self.conn.take().expect("Connection already taken")
    }
}

impl Deref for PooledConnection {
    type Target = turso::Connection;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("Connection already taken")
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().expect("Connection already taken")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        // Return connection to pool if we still have the sender
        if let (Some(conn), Some(tx)) = (self.conn.take(), self.return_tx.take()) {
            // Try to return to pool, but don't block if channel is full
            // This prevents deadlocks and allows connections to be dropped if pool is full
            if tx.send(conn).is_err() {
                tracing::debug!(
                    "[CONN-{}] Pool return channel closed, dropping connection",
                    self.conn_id
                );
            } else {
                tracing::debug!("[CONN-{}] Connection returned to pool", self.conn_id);
            }
        }
    }
}

pub struct TursoBackend {
    db: Arc<Database>,
    /// Connection pool for reusing connections
    pool: Arc<ConnectionPool>,
}

impl std::fmt::Debug for TursoBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TursoBackend")
            .field("db", &"Arc<Database>")
            .field(
                "pool",
                &format!("ConnectionPool(max_size={})", self.pool.max_pool_size),
            )
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
    /// Create a new file-based TursoBackend
    ///
    /// # Platform Support
    /// - **Unix-like systems** (macOS, Linux, BSD, iOS): Full file-based storage support via UnixIO
    /// - **Windows**: Not yet supported - falls back to in-memory storage
    ///
    /// The turso-core library currently does not export a public cross-platform IO implementation.
    /// Windows support will be added once turso-core exposes the necessary APIs.
    pub async fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        #[cfg(target_family = "unix")]
        {
            let io =
                Arc::new(UnixIO::new().map_err(|e| StorageError::DatabaseError(e.to_string()))?);
            let opts = DatabaseOpts::default().with_views(true);

            let db_path_str = db_path
                .as_ref()
                .to_str()
                .ok_or_else(|| StorageError::DatabaseError("Invalid path".to_string()))?;

            let db =
                Database::open_file_with_flags(io, db_path_str, OpenFlags::default(), opts, None)
                    .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

            tracing::info!("Turso database opened at: {}", db_path_str);

            // Create connection pool with reasonable default size
            // SQLite can handle many connections, but we limit to prevent resource exhaustion
            const DEFAULT_POOL_SIZE: usize = 10;
            let db_arc = Arc::new(db);
            let pool = Arc::new(ConnectionPool::new(Arc::clone(&db_arc), DEFAULT_POOL_SIZE));

            Ok(Self {
                db: Arc::clone(&db_arc),
                pool,
            })
        }
        #[cfg(not(target_family = "unix"))]
        {
            // Windows/other platforms: fall back to in-memory until turso-core exports cross-platform IO
            eprintln!(
                "Warning: File-based storage not yet supported on this platform. Using in-memory storage."
            );
            let _ = db_path; // Suppress unused variable warning
            Self::new_in_memory().await
        }
    }

    pub async fn new_in_memory() -> Result<Self> {
        let io = Arc::new(MemoryIO::new());
        let opts = DatabaseOpts::default().with_views(true); // Enable experimental views

        let _db = Database::open_file_with_flags(io, ":memory:", OpenFlags::default(), opts, None)
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        //Ok(Self { db })
        Err(StorageError::DatabaseError(
            "In-memory storage not supported".to_string(),
        ))
    }

    /// Get a connection from the pool
    ///
    /// The connection will be automatically returned to the pool when dropped,
    /// unless `take()` is called on it (for long-lived connections like CDC).
    pub fn get_connection(&self) -> Result<PooledConnection> {
        self.pool.get_connection()
    }

    /// Get a raw connection (for compatibility with code that expects turso::Connection)
    ///
    /// **Note**: This creates a new connection that is NOT pooled. Use `get_connection()`
    /// and call `take()` on the PooledConnection if you need a long-lived connection.
    pub fn get_raw_connection(&self) -> Result<turso::Connection> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);
        let conn_id = CONNECTION_COUNTER.fetch_add(1, Ordering::SeqCst);

        tracing::debug!("[CONN-{}] Creating new raw database connection...", conn_id);

        let conn_core = self.db.connect().map_err(|e| {
            tracing::error!("[CONN-{}] Failed to create connection: {}", conn_id, e);
            StorageError::DatabaseError(e.to_string())
        })?;

        let conn = turso::Connection::create(conn_core);

        let autocommit = conn.is_autocommit().unwrap_or(true);
        tracing::debug!(
            "[CONN-{}] Raw connection created. Autocommit: {}",
            conn_id,
            autocommit
        );

        Ok(conn)
    }

    /// Set up a stream to receive view change notifications
    /// Returns a connection (which must be kept alive) and a stream of changes
    ///
    /// Uses a bounded channel (capacity 1024) to prevent memory exhaustion under bursty changes.
    /// If the channel fills up, new events are dropped with a warning.
    pub fn row_changes(&self) -> Result<(turso::Connection, RowChangeStream)> {
        tracing::debug!("[TursoBackend] row_changes called, creating CDC connection...");

        // CDC connections must stay alive, so we use get_raw_connection()
        // instead of pooled connections
        let conn = self.get_raw_connection()?;

        let autocommit = conn.is_autocommit().unwrap_or(true);
        tracing::debug!(
            "[TursoBackend] CDC connection created. Autocommit: {}",
            autocommit
        );

        let (tx, rx) = mpsc::channel(1024);
        tracing::debug!("[TursoBackend] Setting up view change callback...");

        conn.set_view_change_callback(move |event: &turso_core::types::RelationChangeEvent| {
            tracing::debug!(
                "[TursoBackend] CDC callback triggered for relation: {}",
                event.relation_name
            );
            let mut coalescer = CdcCoalescer::new();
            let mut batch_trace_context: Option<BatchTraceContext> = None;

            for change in &event.changes {
                let change_data = match &change.change {
                    turso_core::DatabaseChangeType::Insert { .. } => {
                        if let Some(values) = change.parse_record() {
                            let mut data =
                                Self::parse_row_values_with_schema(&values, &event.columns);
                            // Store ROWID in data for coalescing
                            data.insert("_rowid".to_string(), Value::String(change.id.to_string()));

                            // Extract ChangeOrigin from _change_origin column if present
                            let origin = extract_change_origin_from_data(&data);

                            // Capture trace context from first change with valid trace_id
                            if batch_trace_context.is_none() {
                                batch_trace_context = origin.to_batch_trace_context();
                            }

                            ChangeData::Created { data, origin }
                        } else {
                            continue;
                        }
                    }
                    turso_core::DatabaseChangeType::Update { .. } => {
                        if let Some(values) = change.parse_record() {
                            let mut data =
                                Self::parse_row_values_with_schema(&values, &event.columns);
                            // Store ROWID in data for coalescing
                            data.insert("_rowid".to_string(), Value::String(change.id.to_string()));

                            // Extract ChangeOrigin from _change_origin column if present
                            let origin = extract_change_origin_from_data(&data);

                            // Capture trace context from first change with valid trace_id
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
                    turso_core::DatabaseChangeType::Delete { .. } => {
                        // Parse the deleted row data to extract entity ID
                        if let Some(values) = change.parse_record() {
                            let data = Self::parse_row_values_with_schema(&values, &event.columns);

                            // Extract entity ID from row data
                            let entity_id = data
                                .get("id")
                                .and_then(|v| match v {
                                    Value::String(s) => Some(s.clone()),
                                    _ => None,
                                })
                                .unwrap_or_else(|| change.id.to_string());

                            // Extract ChangeOrigin from _change_origin column if present
                            let origin = extract_change_origin_from_data(&data);

                            // Capture trace context from first change with valid trace_id
                            if batch_trace_context.is_none() {
                                batch_trace_context = origin.to_batch_trace_context();
                            }

                            ChangeData::Deleted {
                                id: entity_id,
                                origin,
                            }
                        } else {
                            // Fallback to rowid if parsing fails
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

                let view_change = RowChange {
                    relation_name: event.relation_name.clone(),
                    change: change_data,
                };

                coalescer.add(view_change);
            }

            // Collect all coalesced changes into a batch
            let coalesced_changes = coalescer.flush();

            // Create batch from all changes (even if empty)
            let batch = Batch {
                items: coalesced_changes,
            };

            // Use trace context extracted from row data (via _change_origin column)
            // This solves cross-thread propagation since context travels with the data
            let trace_context = batch_trace_context;

            // Create metadata for the batch
            let metadata = BatchMetadata {
                relation_name: event.relation_name.clone(),
                trace_context,
                sync_token: None, // CDC batches don't carry sync tokens
            };

            // Wrap batch with metadata
            let batch_with_metadata = BatchWithMetadata {
                inner: batch,
                metadata,
            };

            tracing::info!(
                "[TursoBackend] Emitting CDC batch: relation={} change_count={} trace_context={:?}",
                event.relation_name,
                batch_with_metadata.items.len(),
                batch_with_metadata.metadata.trace_context
            );

            if tx.try_send(batch_with_metadata).is_err() {
                tracing::warn!(
                    "[TursoBackend] Warning: View change stream full (UI is behind), dropping batch"
                );
            } else {
                tracing::debug!("[TursoBackend] CDC batch enqueued successfully");
            }
        })
        .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let final_autocommit = conn.is_autocommit().unwrap_or(true);
        tracing::debug!(
            "[TursoBackend] row_changes setup complete. CDC connection autocommit: {}",
            final_autocommit
        );

        Ok((conn, ReceiverStream::new(rx)))
    }

    /// Helper to parse a row of turso_core::Value into our Entity type using schema
    fn parse_row_values_with_schema(
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
        match value {
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
            Value::DateTime(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Json(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Reference(r) => format!("'{}'", r.replace('\'', "''")),
            Value::Array(arr) => {
                let json_arr: Vec<serde_json::Value> = arr
                    .iter()
                    .map(|v| serde_json::Value::from(v.clone()))
                    .collect();
                format!(
                    "'{}'",
                    serde_json::to_string(&serde_json::Value::Array(json_arr))
                        .unwrap()
                        .replace('\'', "''")
                )
            }
            Value::Object(obj) => {
                let json_obj: serde_json::Map<String, serde_json::Value> = obj
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::Value::from(v.clone())))
                    .collect();
                format!(
                    "'{}'",
                    serde_json::to_string(&serde_json::Value::Object(json_obj))
                        .unwrap()
                        .replace('\'', "''")
                )
            }
            Value::Null => "NULL".to_string(),
        }
    }

    fn value_to_turso_param(&self, value: &Value) -> turso::Value {
        match value {
            Value::String(s) => turso::Value::Text(s.clone()),
            Value::Integer(i) => turso::Value::Integer(*i),
            Value::Float(f) => turso::Value::Real(*f),
            Value::Boolean(b) => turso::Value::Integer(if *b { 1 } else { 0 }),
            Value::DateTime(s) => turso::Value::Text(s.clone()),
            Value::Json(s) => turso::Value::Text(s.clone()),
            Value::Reference(r) => turso::Value::Text(r.clone()),
            Value::Array(arr) => {
                let json_arr: Vec<serde_json::Value> = arr
                    .iter()
                    .map(|v| serde_json::Value::from(v.clone()))
                    .collect();
                turso::Value::Text(
                    serde_json::to_string(&serde_json::Value::Array(json_arr)).unwrap(),
                )
            }
            Value::Object(obj) => {
                let json_obj: serde_json::Map<String, serde_json::Value> = obj
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::Value::from(v.clone())))
                    .collect();
                turso::Value::Text(
                    serde_json::to_string(&serde_json::Value::Object(json_obj)).unwrap(),
                )
            }
            Value::Null => turso::Value::Null,
        }
    }

    #[allow(dead_code)]
    fn sql_value_to_value(&self, raw: &str, field_type: &FieldType) -> Result<Value> {
        match field_type {
            FieldType::String => Ok(Value::String(raw.to_string())),
            FieldType::Integer => raw
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|e| StorageError::SerializationError(e.to_string())),
            FieldType::Boolean => Ok(Value::Boolean(raw == "1")),
            FieldType::DateTime => Ok(Value::DateTime(raw.to_string())),
            FieldType::Json => serde_json::from_str(raw)
                .map(Value::Json)
                .map_err(|e| StorageError::SerializationError(e.to_string())),
            FieldType::Reference(_) => Ok(Value::Reference(raw.to_string())),
        }
    }

    fn build_where_clause(&self, filter: &Filter, params: &mut Vec<turso::Value>) -> String {
        match filter {
            Filter::Eq(field, value) => {
                params.push(self.value_to_turso_param(value));
                format!("{} = ?", field)
            }
            Filter::In(field, values) => {
                let placeholders = values
                    .iter()
                    .map(|v| {
                        params.push(self.value_to_turso_param(v));
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

    fn turso_value_to_value(&self, value: turso_core::Value) -> Value {
        match value {
            turso_core::Value::Null => Value::Null,
            turso_core::Value::Integer(i) => Value::Integer(i),
            turso_core::Value::Float(f) => Value::Float(f),
            turso_core::Value::Text(s) => {
                let s_str = s.to_string();
                // Try to parse as JSON first (for Array/Object), fall back to String
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&s_str) {
                    Value::from(json_val)
                } else {
                    Value::String(s_str)
                }
            }
            turso_core::Value::Blob(_) => Value::Null,
        }
    }

    /// Execute raw SQL query with parameter binding
    ///
    /// Supports named parameters ($param_name) which are replaced with positional placeholders.
    /// Returns a vector of Entity (HashMap<String, Value>) representing the result rows.
    pub async fn execute_sql(
        &self,
        sql: &str,
        params: HashMap<String, Value>,
    ) -> Result<Vec<StorageEntity>> {
        let conn = self.get_connection()?;

        // Replace named parameters ($param_name) with positional placeholders (?)
        let (sql_with_placeholders, param_values) = self.bind_parameters(sql, &params)?;

        // Prepare and execute the statement
        let mut stmt = conn
            .prepare(&sql_with_placeholders)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let columns = stmt.columns();

        let mut rows = stmt
            .query(param_values)
            .await
            .map_err(|e| StorageError::QueryError(e.to_string()))?;

        // Collect results
        let mut results = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| StorageError::QueryError(e.to_string()))?
        {
            let mut entity = StorageEntity::new();

            for (idx, column) in columns.iter().enumerate() {
                let col_name = column.name();
                let value = row
                    .get_value(idx)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?;

                entity.insert(
                    col_name.to_string(),
                    self.turso_value_to_value(value.into()),
                );
            }

            // Flatten 'data' JSON column if present (for heterogeneous UNION queries)
            // Handle both Value::Object (already parsed) and Value::String (needs parsing)
            if let Some(data_value) = entity.remove("data") {
                let data_obj = match data_value {
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

            results.push(entity);
        }

        Ok(results)
    }

    /// Bind named parameters in SQL ($param_name) to positional placeholders (?)
    ///
    /// Returns the modified SQL and a Vec of parameter values in the correct order.
    fn bind_parameters(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
    ) -> Result<(String, Vec<turso::Value>)> {
        let mut result_sql = String::with_capacity(sql.len());
        let mut param_values = Vec::new();
        let mut chars = sql.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '$' {
                // Extract parameter name
                let mut param_name = String::new();
                while let Some(&next_ch) = chars.peek() {
                    if next_ch.is_alphanumeric() || next_ch == '_' {
                        param_name.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }

                // Look up parameter value
                if let Some(value) = params.get(&param_name) {
                    result_sql.push('?');
                    param_values.push(self.value_to_turso_param(value));
                } else {
                    return Err(StorageError::QueryError(format!(
                        "Parameter ${} not found",
                        param_name
                    )));
                }
            } else {
                result_sql.push(ch);
            }
        }

        Ok((result_sql, param_values))
    }
}

#[async_trait]
impl StorageBackend for TursoBackend {
    async fn create_entity(&mut self, schema: &EntitySchema) -> Result<()> {
        let conn = self.get_connection()?;

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

        conn.execute(&create_table_sql, ())
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        for field in &schema.fields {
            if field.indexed {
                let index_sql = format!(
                    "CREATE INDEX IF NOT EXISTS idx_{}_{} ON {} ({})",
                    schema.name, field.name, schema.name, field.name
                );
                conn.execute(&index_sql, ())
                    .await
                    .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
            }
        }

        Ok(())
    }

    async fn get(&self, entity: &str, id: &str) -> Result<Option<StorageEntity>> {
        let conn = self.get_connection()?;

        let query_str = format!("SELECT * FROM {} WHERE id = ?", entity);

        let mut stmt = conn
            .prepare(&query_str)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let columns = stmt.columns();

        let mut rows = stmt
            .query([turso::Value::Text(id.to_string())])
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        if let Some(row) = rows
            .next()
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?
        {
            let mut entity_data = HashMap::new();

            for (idx, column) in columns.iter().enumerate() {
                let col_name = column.name();

                if col_name.starts_with('_') {
                    continue;
                }

                let value = row
                    .get_value(idx)
                    .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

                entity_data.insert(
                    col_name.to_string(),
                    self.turso_value_to_value(value.into()),
                );
            }

            Ok(Some(entity_data))
        } else {
            Ok(None)
        }
    }

    async fn query(&self, entity: &str, filter: Filter) -> Result<Vec<StorageEntity>> {
        let conn = self.get_connection()?;

        let mut params = Vec::new();
        let where_clause = self.build_where_clause(&filter, &mut params);
        let query_str = format!("SELECT * FROM {} WHERE {}", entity, where_clause);

        let mut stmt = conn
            .prepare(&query_str)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let columns = stmt.columns();

        let mut rows = stmt
            .query(params)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let mut results = Vec::new();

        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?
        {
            let mut entity_data = HashMap::new();

            for (idx, column) in columns.iter().enumerate() {
                let col_name = column.name();

                if col_name.starts_with('_') {
                    continue;
                }

                let value = row
                    .get_value(idx)
                    .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

                entity_data.insert(
                    col_name.to_string(),
                    self.turso_value_to_value(value.into()),
                );
            }

            results.push(entity_data);
        }

        Ok(results)
    }

    async fn insert(&mut self, entity: &str, data: StorageEntity) -> Result<()> {
        let conn = self.get_connection()?;

        let fields: Vec<_> = data.keys().collect();
        let placeholders: Vec<_> = (1..=fields.len()).map(|_| "?").collect();

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            entity,
            fields
                .iter()
                .map(|f| f.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            placeholders.join(", ")
        );

        let mut stmt = conn
            .prepare(&insert_sql)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let params: Vec<turso::Value> = data
            .values()
            .map(|v| self.value_to_turso_param(v))
            .collect();

        stmt.execute(params)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    async fn update(&mut self, entity: &str, id: &str, data: StorageEntity) -> Result<()> {
        let conn = self.get_connection()?;

        let filtered_data: Vec<_> = data.iter().filter(|(k, _)| k.as_str() != "id").collect();

        let set_clauses: Vec<_> = filtered_data
            .iter()
            .map(|(k, _)| format!("{} = ?", k))
            .collect();

        let update_sql = format!(
            "UPDATE {} SET {} WHERE id = ?",
            entity,
            set_clauses.join(", ")
        );

        let mut stmt = conn
            .prepare(&update_sql)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let mut params: Vec<turso::Value> = filtered_data
            .iter()
            .map(|(_, v)| self.value_to_turso_param(v))
            .collect();
        params.push(turso::Value::Text(id.to_string()));

        stmt.execute(params)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    async fn delete(&mut self, entity: &str, id: &str) -> Result<()> {
        let conn = self.get_connection()?;

        let delete_sql = format!("DELETE FROM {} WHERE id = ?", entity);

        let mut stmt = conn
            .prepare(&delete_sql)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        stmt.execute([turso::Value::Text(id.to_string())])
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    async fn get_version(&self, entity: &str, id: &str) -> Result<Option<String>> {
        let conn = self.get_connection()?;

        let query = format!("SELECT _version FROM {} WHERE id = ?", entity);

        let mut stmt = conn
            .prepare(&query)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let mut rows = stmt
            .query([turso::Value::Text(id.to_string())])
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        if let Some(row) = rows
            .next()
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?
        {
            let value = row
                .get_value(0)
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

            match value {
                turso::Value::Text(s) => Ok(Some(s)),
                turso::Value::Null => Ok(None),
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    async fn set_version(&mut self, entity: &str, id: &str, version: String) -> Result<()> {
        let conn = self.get_connection()?;

        let update_sql = format!("UPDATE {} SET _version = ? WHERE id = ?", entity);

        let mut stmt = conn
            .prepare(&update_sql)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        stmt.execute([
            turso::Value::Text(version),
            turso::Value::Text(id.to_string()),
        ])
        .await
        .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

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

#[cfg(test)]
#[path = "turso_tests.rs"]
mod turso_tests;

#[cfg(test)]
#[path = "turso_pbt_tests.rs"]
mod turso_pbt_tests;

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
