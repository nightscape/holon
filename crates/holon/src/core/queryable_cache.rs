use async_trait::async_trait;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::Stream;
use tracing;

use super::datasource::{
    CrudOperations, DataSource, OperationDescriptor, OperationProvider, OperationRegistry,
    UndoAction,
};
use super::traits::{HasSchema, Predicate, Queryable, Result, Schema};
use crate::storage::turso::TursoBackend;
use crate::storage::types::StorageEntity;
use holon_api::streaming::ChangeNotifications;
use holon_api::DynamicEntity;
use holon_api::{ApiError, Change, StreamPosition};
use holon_api::{
    BatchMetadata, ChangeOrigin, SyncTokenUpdate, Value, WithMetadata, CHANGE_ORIGIN_COLUMN,
};

pub struct QueryableCache<S, T>
where
    S: DataSource<T>,
    T: HasSchema + Send + Sync + 'static,
{
    source: Arc<S>,
    backend: Arc<RwLock<TursoBackend>>, // Changed from Database to TursoBackend
    // CDC connection kept alive for streaming
    // CRITICAL: This must stay alive for CDC callbacks to work
    // The callback closure captures the channel sender, which closes the stream if dropped
    _cdc_conn: Option<Arc<tokio::sync::Mutex<turso::Connection>>>,
    _phantom: PhantomData<T>,
}

impl<S, T> QueryableCache<S, T>
where
    S: DataSource<T>,
    T: HasSchema + Send + Sync + 'static,
{
    /// Create QueryableCache with TursoBackend
    ///
    /// The backend is shared with BackendEngine to enable CDC streaming.
    pub async fn new_with_backend(source: S, backend: Arc<RwLock<TursoBackend>>) -> Result<Self> {
        let cache = Self {
            source: Arc::new(source),
            backend,
            _cdc_conn: None, // Will be initialized when watch_changes_since is called
            _phantom: PhantomData,
        };

        cache.initialize_schema().await?;
        Ok(cache)
    }

    // Keep old methods for backward compatibility during transition
    #[allow(dead_code)]
    pub async fn new(source: S) -> Result<Self> {
        // Create in-memory backend for backward compatibility
        let backend = Arc::new(RwLock::new(
            TursoBackend::new_in_memory()
                .await
                .map_err(|e| format!("Failed to create backend: {}", e))?,
        ));
        Self::new_with_backend(source, backend).await
    }

    #[allow(dead_code)]
    pub async fn with_database(source: S, db_path: &str) -> Result<Self> {
        let backend = Arc::new(RwLock::new(
            TursoBackend::new(db_path)
                .await
                .map_err(|e| format!("Failed to create backend: {}", e))?,
        ));
        Self::new_with_backend(source, backend).await
    }

    async fn initialize_schema(&self) -> Result<()> {
        let schema = T::schema();
        let table_name = &schema.table_name;

        tracing::debug!(
            "[QueryableCache] initialize_schema called for table '{}'",
            table_name
        );

        let backend = self.backend.read().await;
        tracing::debug!("[QueryableCache] Got backend read lock for initialize_schema");

        let conn = backend
            .get_connection()
            .map_err(|e| format!("Failed to get connection: {}", e))?;

        let autocommit_before = conn.is_autocommit().unwrap_or(true);
        tracing::debug!(
            "[QueryableCache] Got connection for initialize_schema. Autocommit: {}",
            autocommit_before
        );

        let create_table_sql = generate_create_table_sql_with_change_origin(&schema);
        tracing::debug!(
            "[QueryableCache] Executing CREATE TABLE for '{}'...",
            table_name
        );
        conn.execute(&create_table_sql, ())
            .await
            .map_err(|e| format!("Failed to create table: {}", e))?;

        let autocommit_after_create = conn.is_autocommit().unwrap_or(true);
        tracing::debug!(
            "[QueryableCache] CREATE TABLE completed. Autocommit: {}",
            autocommit_after_create
        );

        let index_sqls = schema.to_index_sql();
        tracing::debug!(
            "[QueryableCache] Creating {} indexes for '{}'...",
            index_sqls.len(),
            table_name
        );

        for (i, index_sql) in index_sqls.iter().enumerate() {
            tracing::debug!(
                "[QueryableCache] Creating index {}/{} for '{}'...",
                i + 1,
                index_sqls.len(),
                table_name
            );
            conn.execute(index_sql, ())
                .await
                .map_err(|e| format!("Failed to create index: {}", e))?;
        }

        let autocommit_final = conn.is_autocommit().unwrap_or(true);
        tracing::debug!(
            "[QueryableCache] initialize_schema completed for '{}'. Autocommit: {}",
            table_name,
            autocommit_final
        );

        Ok(())
    }

    pub async fn sync(&self) -> Result<()> {
        let items = self.source.get_all().await?;

        for item in items {
            self.upsert_to_cache(&item).await?;
        }

        Ok(())
    }

    pub async fn upsert_to_cache(&self, item: &T) -> Result<()> {
        self.upsert_to_cache_with_origin(item, None).await
    }

    pub async fn upsert_to_cache_with_origin(
        &self,
        item: &T,
        change_origin: Option<&ChangeOrigin>,
    ) -> Result<()> {
        let backend = self.backend.read().await;
        let conn = backend
            .get_connection()
            .map_err(|e| format!("Failed to get connection: {}", e))?;
        let entity = item.to_entity();
        let schema = T::schema();

        let mut columns = Vec::new();
        let mut placeholders = Vec::new();
        let mut values = Vec::new();

        for field in &schema.fields {
            if let Some(value) = entity.fields.get(&field.name) {
                columns.push(field.name.clone());
                placeholders.push("?");

                let libsql_value = match value {
                    Value::String(s) => turso::Value::Text(s.clone()),
                    Value::Integer(i) => turso::Value::Integer(*i),
                    Value::Float(f) => turso::Value::Real(*f),
                    Value::Boolean(b) => turso::Value::Integer(if *b { 1 } else { 0 }),
                    Value::Null => turso::Value::Null,
                    _ => turso::Value::Null,
                };
                values.push(libsql_value);
            }
        }

        // Add _change_origin column for trace context propagation
        columns.push(CHANGE_ORIGIN_COLUMN.to_string());
        placeholders.push("?");
        let change_origin_json = change_origin
            .map(|co| co.to_json())
            .unwrap_or_else(|| "null".to_string());
        values.push(turso::Value::Text(change_origin_json));

        let id_field = schema
            .fields
            .iter()
            .find(|f| f.primary_key)
            .map(|f| f.name.as_str())
            .unwrap_or("id");

        let update_clause = columns
            .iter()
            .map(|c| format!("{} = excluded.{}", c, c))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT({}) DO UPDATE SET {}",
            schema.table_name,
            columns.join(", "),
            placeholders.join(", "),
            id_field,
            update_clause
        );

        conn.execute(&sql, turso::params_from_iter(values))
            .await
            .map_err(|e| format!("Failed to execute upsert: {}", e))?;
        Ok(())
    }

    async fn get_from_cache(&self, id: &str) -> Result<Option<T>> {
        let backend = self.backend.read().await;
        let conn = backend
            .get_connection()
            .map_err(|e| format!("Failed to get connection: {}", e))?;

        let schema = T::schema();
        let id_field = schema
            .fields
            .iter()
            .find(|f| f.primary_key)
            .map(|f| f.name.as_str())
            .unwrap_or("id");

        let sql = format!(
            "SELECT * FROM {} WHERE {} = ? LIMIT 1",
            schema.table_name, id_field
        );

        let mut rows = conn
            .query(&sql, [turso::Value::Text(id.to_string())])
            .await?;

        if let Some(row) = rows.next().await? {
            let entity = self.row_to_entity(&row, &schema)?;
            T::from_entity(entity).map(Some)
        } else {
            Ok(None)
        }
    }

    async fn update_cache(&self, _id: &str, item: &T) -> Result<()> {
        self.upsert_to_cache(item).await
    }

    pub async fn delete_from_cache(&self, id: &str) -> Result<()> {
        let backend = self.backend.read().await;
        let conn = backend
            .get_connection()
            .map_err(|e| format!("Failed to get connection: {}", e))?;

        let schema = T::schema();
        let id_field = schema
            .fields
            .iter()
            .find(|f| f.primary_key)
            .map(|f| f.name.as_str())
            .unwrap_or("id");

        let sql = format!("DELETE FROM {} WHERE {} = ?", schema.table_name, id_field);
        conn.execute(&sql, [turso::Value::Text(id.to_string())])
            .await
            .map_err(|e| format!("Failed to execute delete: {}", e))?;

        Ok(())
    }

    /// Wire up stream ingestion from a broadcast receiver (spawns background task)
    ///
    /// This method subscribes to a broadcast channel and updates the local cache
    /// as changes arrive from the provider. The background task runs until the
    /// stream is closed or the cache is dropped.
    /// ExternalServiceDiscovery
    pub fn ingest_stream(&self, rx: broadcast::Receiver<Vec<Change<T>>>)
    where
        T: Clone + Send + Sync + 'static,
    {
        let backend = Arc::clone(&self.backend);
        let schema = T::schema();
        let table_name = schema.table_name.clone();
        let id_field = schema
            .fields
            .iter()
            .find(|f| f.primary_key)
            .map(|f| f.name.clone())
            .unwrap_or_else(|| "id".to_string());

        // Spawn the ingestion task on the current runtime
        // IMPORTANT: This must be called from an async context on a runtime that stays alive
        // If called from a blocking thread with a temporary runtime, the task will be dropped
        // when that runtime is dropped. The caller should ensure this is called from a persistent runtime.
        tokio::spawn(async move {
            let mut rx = rx;
            tracing::info!(
                "[QueryableCache] Started ingesting stream for table: {}",
                table_name
            );
            loop {
                match rx.recv().await {
                    Ok(changes) => {
                        let change_count = changes.len();
                        tracing::info!(
                            "[QueryableCache] Received {} changes for table: {}",
                            change_count,
                            table_name
                        );

                        // Create OpenTelemetry span for batch ingestion
                        let ingestion_span = tracing::span!(
                            tracing::Level::INFO,
                            "queryable_cache.ingest_batch",
                            "table_name" = %table_name,
                            "change_count" = change_count,
                        );
                        let _ingestion_guard = ingestion_span.enter();

                        // Process all changes in a single batch transaction
                        if let Err(e) =
                            Self::apply_batch_to_cache(&backend, &table_name, &id_field, &changes)
                                .await
                        {
                            tracing::error!(
                                "[QueryableCache] Error ingesting batch into cache: {}",
                                e
                            );
                        } else {
                            tracing::debug!(
                                "[QueryableCache] Successfully ingested batch of {} changes for table: {}",
                                change_count,
                                table_name
                            );
                        }

                        // Log batch ingestion completion
                        tracing::info!(
                            "[QueryableCache] Completed ingesting {} changes for table: {}",
                            change_count,
                            table_name
                        );
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Stream lagged by {} messages, triggering resync", n);
                        // TODO: Trigger full resync
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::info!("Change stream closed");
                        break;
                    }
                }
            }
        });
    }

    /// Wire up stream ingestion from a broadcast receiver with metadata (spawns background task)
    ///
    /// Applies a batch of changes directly to the cache (synchronous, blocking).
    ///
    /// This method is useful when you need to ensure ordering between different
    /// entity types (e.g., directories before files before headlines for referential integrity).
    /// Unlike `ingest_stream_with_metadata`, this method blocks until the batch is fully applied.
    pub async fn apply_batch(
        &self,
        changes: &[Change<T>],
        sync_token: Option<&SyncTokenUpdate>,
    ) -> Result<()>
    where
        T: Clone,
    {
        let schema = T::schema();
        let table_name = schema.table_name.clone();
        let id_field = schema
            .fields
            .iter()
            .find(|f| f.primary_key)
            .map(|f| f.name.clone())
            .unwrap_or_else(|| "id".to_string());

        tracing::info!(
            "[QueryableCache] Applying batch of {} changes to table: {}",
            changes.len(),
            table_name
        );

        Self::apply_batch_to_cache_with_token(
            &self.backend,
            &table_name,
            &id_field,
            changes,
            sync_token,
        )
        .await
    }

    /// This method subscribes to a broadcast channel that includes metadata (sync tokens)
    /// and updates the local cache as changes arrive from the provider. The sync token
    /// is saved atomically with the data changes in a single transaction.
    ///
    /// This method is preferred over `ingest_stream` when using providers that include
    /// sync tokens in their batch metadata (e.g., TodoistSyncProvider).
    pub fn ingest_stream_with_metadata(
        &self,
        rx: broadcast::Receiver<WithMetadata<Vec<Change<T>>, BatchMetadata>>,
    ) where
        T: Clone + Send + Sync + 'static,
    {
        let backend = Arc::clone(&self.backend);
        let schema = T::schema();
        let table_name = schema.table_name.clone();
        let id_field = schema
            .fields
            .iter()
            .find(|f| f.primary_key)
            .map(|f| f.name.clone())
            .unwrap_or_else(|| "id".to_string());

        tokio::spawn(async move {
            let mut rx = rx;
            tracing::info!(
                "[QueryableCache] Started ingesting stream with metadata for table: {}",
                table_name
            );
            loop {
                match rx.recv().await {
                    Ok(batch_with_metadata) => {
                        let changes = &batch_with_metadata.inner;
                        let sync_token = batch_with_metadata.metadata.sync_token.clone();
                        let change_count = changes.len();

                        tracing::info!(
                            "[QueryableCache] Received {} changes for table: {} (sync_token: {})",
                            change_count,
                            table_name,
                            sync_token
                                .as_ref()
                                .map(|t| t.provider_name.as_str())
                                .unwrap_or("none")
                        );

                        let ingestion_span = tracing::span!(
                            tracing::Level::INFO,
                            "queryable_cache.ingest_batch_with_metadata",
                            "table_name" = %table_name,
                            "change_count" = change_count,
                            "has_sync_token" = sync_token.is_some(),
                        );
                        let _ingestion_guard = ingestion_span.enter();

                        // Process all changes AND sync token in a single atomic transaction
                        if let Err(e) = Self::apply_batch_to_cache_with_token(
                            &backend,
                            &table_name,
                            &id_field,
                            changes,
                            sync_token.as_ref(),
                        )
                        .await
                        {
                            tracing::error!(
                                "[QueryableCache] Error ingesting batch into cache: {}",
                                e
                            );
                        } else {
                            tracing::debug!(
                                "[QueryableCache] Successfully ingested batch of {} changes for table: {}",
                                change_count,
                                table_name
                            );
                        }

                        tracing::info!(
                            "[QueryableCache] Completed ingesting {} changes for table: {}",
                            change_count,
                            table_name
                        );
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Stream lagged by {} messages, triggering resync", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::info!("Change stream closed");
                        break;
                    }
                }
            }
        });
    }

    // Helper method for applying a batch of changes to cache in a single transaction
    // This reduces database lock contention by processing all changes atomically
    // Includes retry logic with exponential backoff for "database is locked" errors
    async fn apply_batch_to_cache(
        backend: &Arc<RwLock<TursoBackend>>,
        table_name: &str,
        id_field: &str,
        changes: &[Change<T>],
    ) -> Result<()>
    where
        T: HasSchema + Clone,
    {
        if changes.is_empty() {
            return Ok(());
        }

        const MAX_RETRIES: u32 = 5;
        const INITIAL_DELAY_MS: u64 = 10;

        let mut attempt = 0;
        loop {
            attempt += 1;
            match Self::apply_batch_to_cache_inner(backend, table_name, id_field, changes).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let error_str = e.to_string();
                    let is_locked = error_str.contains("database is locked")
                        || error_str.contains("SQLITE_BUSY");

                    if is_locked && attempt < MAX_RETRIES {
                        let delay_ms = INITIAL_DELAY_MS * (1 << (attempt - 1)); // Exponential backoff
                        tracing::warn!(
                            "[QueryableCache] Database locked on attempt {}/{}, retrying in {}ms",
                            attempt,
                            MAX_RETRIES,
                            delay_ms
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }

                    // Not a lock error or max retries exceeded
                    return Err(e);
                }
            }
        }
    }

    // Helper method for applying a batch of changes + sync token in a single transaction
    // This ensures data and sync token are saved atomically, preventing lock contention
    // and ensuring consistency (no partial updates on failure)
    async fn apply_batch_to_cache_with_token(
        backend: &Arc<RwLock<TursoBackend>>,
        table_name: &str,
        id_field: &str,
        changes: &[Change<T>],
        sync_token: Option<&SyncTokenUpdate>,
    ) -> Result<()>
    where
        T: HasSchema + Clone,
    {
        // Allow empty changes if we have a sync token to save
        if changes.is_empty() && sync_token.is_none() {
            return Ok(());
        }

        const MAX_RETRIES: u32 = 5;
        const INITIAL_DELAY_MS: u64 = 10;

        let mut attempt = 0;
        loop {
            attempt += 1;
            match Self::apply_batch_to_cache_inner_with_token(
                backend, table_name, id_field, changes, sync_token,
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let error_str = e.to_string();
                    let is_locked = error_str.contains("database is locked")
                        || error_str.contains("SQLITE_BUSY");

                    if is_locked && attempt < MAX_RETRIES {
                        let delay_ms = INITIAL_DELAY_MS * (1 << (attempt - 1));
                        tracing::warn!(
                            "[QueryableCache] Database locked on attempt {}/{}, retrying in {}ms",
                            attempt,
                            MAX_RETRIES,
                            delay_ms
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }

                    return Err(e);
                }
            }
        }
    }

    // Inner implementation of batch application with sync token (called by retry wrapper)
    // Uses manual SQL transaction statements to avoid Transaction API's drop behavior complexity
    #[tracing::instrument(
        name = "atomic_transaction",
        skip(backend, changes, sync_token),
        fields(table = %table_name, changes = changes.len(), has_token = sync_token.is_some())
    )]
    async fn apply_batch_to_cache_inner_with_token(
        backend: &Arc<RwLock<TursoBackend>>,
        table_name: &str,
        id_field: &str,
        changes: &[Change<T>],
        sync_token: Option<&SyncTokenUpdate>,
    ) -> Result<()>
    where
        T: HasSchema + Clone,
    {
        tracing::debug!("[TX] Getting connection from backend...");
        let backend_guard = backend.read().await;
        let conn = backend_guard
            .get_connection()
            .map_err(|e| format!("Failed to get connection: {}", e))?;
        tracing::debug!("[TX] Got connection");

        // Check autocommit state before transaction
        let autocommit_before = conn.is_autocommit().unwrap_or(false);
        tracing::debug!("[TX] Autocommit before BEGIN: {}", autocommit_before);

        // Diagnostic: Check journal mode and connection state
        if let Ok(mut rows) = conn.query("PRAGMA journal_mode", ()).await {
            if let Ok(Some(row)) = rows.next().await {
                let journal_mode: String = row.get(0).unwrap_or_default();
                tracing::debug!("[TX] Journal mode: {}", journal_mode);
            }
        }

        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|e| format!("Failed to set busy_timeout: {}", e))?;

        // IMPORTANT: Use prepare() + execute() instead of conn.execute() to bypass
        // turso's maybe_handle_dangling_tx() which can auto-commit our transaction
        // See: https://github.com/anthropics/claude-code/issues/XXX
        tracing::debug!("[TX] Executing BEGIN IMMEDIATE TRANSACTION (via prepare)...");
        let begin_start = std::time::Instant::now();
        match conn.prepare("BEGIN IMMEDIATE TRANSACTION").await {
            Ok(mut stmt) => match stmt.execute(()).await {
                Ok(_) => {
                    let elapsed = begin_start.elapsed();
                    let autocommit_after_begin = conn.is_autocommit().unwrap_or(false);
                    tracing::debug!(
                        "[TX] BEGIN succeeded in {:?}. Autocommit after BEGIN: {}",
                        elapsed,
                        autocommit_after_begin
                    );
                }
                Err(e) => {
                    let elapsed = begin_start.elapsed();
                    tracing::error!(
                        "[TX] BEGIN execute FAILED after {:?}. Error: {}",
                        elapsed,
                        e
                    );
                    return Err(format!("Failed to begin transaction: {}", e).into());
                }
            },
            Err(e) => {
                let elapsed = begin_start.elapsed();
                tracing::error!(
                    "[TX] BEGIN prepare FAILED after {:?}. Error: {}",
                    elapsed,
                    e
                );
                return Err(format!("Failed to prepare begin transaction: {}", e).into());
            }
        }

        let mut error_count = 0;
        let mut last_error: Option<String> = None;
        let mut ops_executed = 0;

        // Build SQL templates and prepare statements ONCE before the loop
        let schema = T::schema();
        let columns: Vec<String> = schema
            .fields
            .iter()
            .map(|f| f.name.clone())
            .chain(std::iter::once(CHANGE_ORIGIN_COLUMN.to_string()))
            .collect();
        let placeholders: Vec<&str> = (0..columns.len()).map(|_| "?").collect();
        let update_clause = columns
            .iter()
            .map(|c| format!("{} = excluded.{}", c, c))
            .collect::<Vec<_>>()
            .join(", ");

        let upsert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT({}) DO UPDATE SET {}",
            table_name,
            columns.join(", "),
            placeholders.join(", "),
            id_field,
            update_clause
        );
        let delete_sql = format!("DELETE FROM {} WHERE {} = ?", table_name, id_field);

        tracing::debug!(
            "[TX] Prepared SQL templates for {} changes. Upsert columns: {:?}",
            changes.len(),
            columns
        );

        // Process all data changes
        for (idx, change) in changes.iter().enumerate() {
            match change {
                Change::Created { data, origin } | Change::Updated { data, origin, .. } => {
                    let entity = data.to_entity();

                    // Extract values in the same order as columns
                    let mut values: Vec<turso::Value> = Vec::with_capacity(columns.len());
                    for field in &schema.fields {
                        let libsql_value = match entity.fields.get(&field.name) {
                            Some(Value::String(s)) => turso::Value::Text(s.clone()),
                            Some(Value::Integer(i)) => turso::Value::Integer(*i),
                            Some(Value::Float(f)) => turso::Value::Real(*f),
                            Some(Value::Boolean(b)) => {
                                turso::Value::Integer(if *b { 1 } else { 0 })
                            }
                            Some(Value::Null) | None => turso::Value::Null,
                            Some(_) => turso::Value::Null,
                        };
                        values.push(libsql_value);
                    }
                    // Add _change_origin as the last column
                    values.push(turso::Value::Text(origin.to_json()));

                    if idx % 10000 == 0 {
                        tracing::debug!(
                            "[TX] Executing upsert {}/{} for table {}...",
                            idx + 1,
                            changes.len(),
                            table_name
                        );
                    }

                    match conn.prepare(&upsert_sql).await {
                        Ok(mut stmt) => match stmt.execute(turso::params_from_iter(values)).await {
                            Ok(_) => {
                                ops_executed += 1;
                            }
                            Err(e) => {
                                error_count += 1;
                                last_error = Some(e.to_string());
                                tracing::error!("[TX] Error in batch upsert execute: {}", e);
                            }
                        },
                        Err(e) => {
                            error_count += 1;
                            last_error = Some(e.to_string());
                            tracing::error!("[TX] Error in batch upsert prepare: {}", e);
                        }
                    }
                }
                Change::Deleted { id, .. } => {
                    if idx % 10000 == 0 {
                        tracing::debug!(
                            "[TX] Executing delete {}/{} for id {}...",
                            idx + 1,
                            changes.len(),
                            id
                        );
                    }

                    match conn.prepare(&delete_sql).await {
                        Ok(mut stmt) => {
                            match stmt.execute([turso::Value::Text(id.to_string())]).await {
                                Ok(_) => {
                                    ops_executed += 1;
                                }
                                Err(e) => {
                                    error_count += 1;
                                    last_error = Some(e.to_string());
                                    tracing::error!("[TX] Error in batch delete execute: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error_count += 1;
                            last_error = Some(e.to_string());
                            tracing::error!("[TX] Error in batch delete prepare: {}", e);
                        }
                    }
                }
            }
        }

        // Save sync token atomically with data changes (if provided)
        if let Some(token) = sync_token {
            let token_str = match &token.position {
                StreamPosition::Beginning => "*".to_string(),
                StreamPosition::Version(bytes) => {
                    String::from_utf8(bytes.clone()).unwrap_or_else(|_| "*".to_string())
                }
            };

            let sql = r#"
                INSERT INTO sync_states (provider_name, sync_token, updated_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(provider_name) DO UPDATE SET
                    sync_token = excluded.sync_token,
                    updated_at = excluded.updated_at
            "#;

            tracing::debug!(
                "[TX] Executing sync token save for provider '{}'...",
                token.provider_name
            );
            // Use prepare() + execute() to bypass turso's maybe_handle_dangling_tx()
            match conn.prepare(sql).await {
                Ok(mut stmt) => {
                    match stmt
                        .execute(turso::params![
                            turso::Value::Text(token.provider_name.clone()),
                            turso::Value::Text(token_str.clone())
                        ])
                        .await
                    {
                        Ok(rows) => {
                            ops_executed += 1;
                            let autocommit_now = conn.is_autocommit().unwrap_or(false);
                            tracing::debug!(
                                "[TX] Sync token save succeeded (rows: {}). Autocommit: {}. Token: {}",
                                rows,
                                autocommit_now,
                                token_str
                            );
                        }
                        Err(e) => {
                            error_count += 1;
                            last_error = Some(e.to_string());
                            tracing::error!("[TX] Error saving sync token execute: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error_count += 1;
                    last_error = Some(e.to_string());
                    tracing::error!("[TX] Error saving sync token prepare: {}", e);
                }
            }
        }

        tracing::debug!(
            "[TX] All operations complete. ops_executed={}, error_count={}",
            ops_executed,
            error_count
        );

        // Check autocommit before commit
        let autocommit_before_commit = conn.is_autocommit().unwrap_or(false);
        tracing::debug!(
            "[TX] Autocommit before COMMIT: {}",
            autocommit_before_commit
        );

        // Commit or rollback using prepare/execute to bypass maybe_handle_dangling_tx()
        if error_count > 0 {
            tracing::debug!("[TX] Errors occurred, executing ROLLBACK (via prepare)...");
            if let Ok(mut stmt) = conn.prepare("ROLLBACK").await {
                if let Err(e) = stmt.execute(()).await {
                    tracing::error!("[TX] Failed to rollback transaction: {}", e);
                }
            }
            return Err(format!(
                "Failed to process {} changes in batch: {}",
                error_count,
                last_error.unwrap_or_default()
            )
            .into());
        }

        tracing::debug!("[TX] Preparing COMMIT statement...");
        let mut commit_stmt = conn.prepare("COMMIT").await.map_err(|e| {
            tracing::error!("[TX] COMMIT prepare failed! Error: {}", e);
            format!("Failed to prepare commit: {}", e)
        })?;

        // Check autocommit AFTER prepare but BEFORE execute
        let autocommit_after_prepare = conn.is_autocommit().unwrap_or(false);
        tracing::debug!(
            "[TX] COMMIT prepared. Autocommit after prepare: {}",
            autocommit_after_prepare
        );

        if autocommit_after_prepare {
            tracing::error!("[TX] Transaction was already committed/rolled back after prepare!");
        }

        // Note: CDC trace context is set at the FFI layer (ffi_bridge.rs) before execute_operation
        // The CDC callback in turso.rs will pick it up via get_cdc_trace_context()

        tracing::debug!("[TX] Executing COMMIT...");
        commit_stmt.execute(()).await.map_err(|e| {
            let autocommit_on_error = conn.is_autocommit().unwrap_or(false);
            tracing::error!(
                "[TX] COMMIT execute failed! Autocommit at failure: {}. Error: {}",
                autocommit_on_error,
                e
            );
            format!("Failed to commit transaction: {}", e)
        })?;

        let autocommit_after_commit = conn.is_autocommit().unwrap_or(false);
        tracing::debug!(
            "[TX] COMMIT succeeded. Autocommit after COMMIT: {}",
            autocommit_after_commit
        );

        Ok(())
    }

    // Inner implementation of batch application (called by retry wrapper)
    async fn apply_batch_to_cache_inner(
        backend: &Arc<RwLock<TursoBackend>>,
        table_name: &str,
        id_field: &str,
        changes: &[Change<T>],
    ) -> Result<()>
    where
        T: HasSchema + Clone,
    {
        let backend_guard = backend.read().await;
        let conn = backend_guard
            .get_connection()
            .map_err(|e| format!("Failed to get connection: {}", e))?;

        // Set busy_timeout to wait for locks (5 seconds)
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|e| format!("Failed to set busy_timeout: {}", e))?;

        // Start transaction (IMMEDIATE acquires write lock immediately)
        conn.execute("BEGIN IMMEDIATE TRANSACTION", ())
            .await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        // Process all changes
        let mut error_count = 0;
        let mut last_error: Option<String> = None;

        for change in changes {
            match change {
                Change::Created { data, .. } | Change::Updated { data, .. } => {
                    let entity = data.to_entity();
                    let schema = T::schema();

                    let mut columns = Vec::new();
                    let mut placeholders = Vec::new();
                    let mut values = Vec::new();

                    for field in &schema.fields {
                        if let Some(value) = entity.fields.get(&field.name) {
                            columns.push(field.name.clone());
                            placeholders.push("?");

                            let libsql_value = match value {
                                Value::String(s) => turso::Value::Text(s.clone()),
                                Value::Integer(i) => turso::Value::Integer(*i),
                                Value::Float(f) => turso::Value::Real(*f),
                                Value::Boolean(b) => turso::Value::Integer(if *b { 1 } else { 0 }),
                                Value::Null => turso::Value::Null,
                                _ => turso::Value::Null,
                            };
                            values.push(libsql_value);
                        }
                    }

                    let update_clause = columns
                        .iter()
                        .map(|c| format!("{} = excluded.{}", c, c))
                        .collect::<Vec<_>>()
                        .join(", ");

                    let sql = format!(
                        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT({}) DO UPDATE SET {}",
                        table_name,
                        columns.join(", "),
                        placeholders.join(", "),
                        id_field,
                        update_clause
                    );

                    if let Err(e) = conn.execute(&sql, turso::params_from_iter(values)).await {
                        error_count += 1;
                        last_error = Some(e.to_string());
                        tracing::error!("[QueryableCache] Error in batch upsert: {}", e);
                    }
                }
                Change::Deleted { id, .. } => {
                    let sql = format!("DELETE FROM {} WHERE {} = ?", table_name, id_field);
                    if let Err(e) = conn
                        .execute(&sql, [turso::Value::Text(id.to_string())])
                        .await
                    {
                        error_count += 1;
                        last_error = Some(e.to_string());
                        tracing::error!("[QueryableCache] Error in batch delete: {}", e);
                    }
                }
            }
        }

        // Commit transaction (or rollback on error)
        if error_count > 0 {
            if let Err(e) = conn.execute("ROLLBACK", ()).await {
                tracing::error!("[QueryableCache] Failed to rollback transaction: {}", e);
            }
            return Err(format!(
                "Failed to process {} out of {} changes in batch: {}",
                error_count,
                changes.len(),
                last_error.unwrap_or_default()
            )
            .into());
        }

        // Note: CDC trace context is set at the FFI layer (ffi_bridge.rs) before execute_operation
        // The CDC callback in turso.rs will pick it up via get_cdc_trace_context()
        conn.execute("COMMIT", ())
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        Ok(())
    }

    fn row_to_entity(&self, row: &turso::Row, schema: &Schema) -> Result<DynamicEntity> {
        let mut entity = DynamicEntity::new(&schema.table_name);

        for (idx, field) in schema.fields.iter().enumerate() {
            let value = row.get_value(idx).map_err(|e| e.to_string())?;

            let converted_value = match value {
                turso::Value::Null => Value::Null,
                turso::Value::Integer(i) => Value::Integer(i),
                turso::Value::Real(f) => Value::Float(f),
                turso::Value::Text(s) => Value::String(s),
                turso::Value::Blob(_) => Value::Null,
            };

            entity.set(&field.name, converted_value);
        }

        Ok(entity)
    }
}

#[async_trait]
impl<S, T> DataSource<T> for QueryableCache<S, T>
where
    S: DataSource<T>,
    T: HasSchema + Send + Sync + 'static,
{
    async fn get_all(&self) -> Result<Vec<T>> {
        self.source.get_all().await
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<T>> {
        // Try cache first
        if let Ok(cached) = self.get_from_cache(id).await {
            if cached.is_some() {
                return Ok(cached);
            }
        }

        // Fall back to source
        self.source.get_by_id(id).await
    }
}

// Implement CrudOperations when source also implements it
#[async_trait]
impl<S, T> CrudOperations<T> for QueryableCache<S, T>
where
    S: DataSource<T> + CrudOperations<T>,
    T: HasSchema + Send + Sync + 'static,
{
    async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<UndoAction> {
        let expected_value = value.clone();
        tracing::info!(
            "[QueryableCache] set_field request: entity={} field={} value={:?}",
            id,
            field,
            expected_value
        );

        // Source now returns the undo action
        let undo_action = self.source.set_field(id, field, value).await?;

        match self.source.get_by_id(id).await {
            Ok(Some(item)) => {
                let entity = item.to_entity();
                tracing::info!(
                    "[QueryableCache] Post-set_field fetch: entity={} field={} value={:?}",
                    id,
                    field,
                    entity.get(field)
                );
                let _ = self.update_cache(id, &item).await;
            }
            Ok(None) => {
                tracing::error!(
                    "[QueryableCache] Post-set_field fetch returned None for entity {}",
                    id
                );
            }
            Err(err) => {
                tracing::error!(
                    "[QueryableCache] Error fetching entity {} after set_field: {}",
                    id,
                    err
                );
            }
        }
        Ok(undo_action)
    }

    async fn create(&self, fields: HashMap<String, Value>) -> Result<(String, UndoAction)> {
        // Source now returns (id, undo_action)
        let (id, undo_action) = self.source.create(fields).await?;
        // Update cache if we have the item
        if let Ok(Some(item)) = self.source.get_by_id(&id).await {
            let _ = self.update_cache(&id, &item).await;
        }
        Ok((id, undo_action))
    }

    async fn delete(&self, id: &str) -> Result<UndoAction> {
        // Source now returns the undo action
        let undo_action = self.source.delete(id).await?;
        let _ = self.delete_from_cache(id).await;
        Ok(undo_action)
    }
}

// Implement OperationProvider for QueryableCache
// This enables QueryableCache to be registered with OperationDispatcher
#[async_trait]
impl<S, T> OperationProvider for QueryableCache<S, T>
where
    S: DataSource<T> + CrudOperations<T> + OperationProvider,
    T: HasSchema + Send + Sync + 'static + OperationRegistry,
{
    fn operations(&self) -> Vec<OperationDescriptor> {
        // Delegate to datasource's OperationProvider::operations (which may add param_mappings)
        OperationProvider::operations(self.source.as_ref())
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<UndoAction> {
        // Validate entity name matches the registry
        let expected_entity_name = T::entity_name();
        if entity_name != expected_entity_name {
            return Err(format!(
                "Expected entity_name '{}', got '{}'",
                expected_entity_name, entity_name
            )
            .into());
        }

        // Dispatch to CrudOperations methods (which now return UndoAction)
        match op_name {
            "set_field" => {
                let id = params
                    .get("id")
                    .and_then(|v| v.as_string())
                    .ok_or_else(|| "Missing 'id' parameter".to_string())?;
                let field = params
                    .get("field")
                    .and_then(|v| v.as_string())
                    .ok_or_else(|| "Missing 'field' parameter".to_string())?;
                let value = params
                    .get("value")
                    .ok_or_else(|| "Missing 'value' parameter".to_string())?
                    .clone();
                // set_field returns UndoAction
                let undo_action = self.set_field(&id, &field, value).await?;
                // Set entity_name on the inverse operation if present
                Ok(match undo_action {
                    UndoAction::Undo(mut op) => {
                        op.entity_name = entity_name.to_string();
                        UndoAction::Undo(op)
                    }
                    UndoAction::Irreversible => UndoAction::Irreversible,
                })
            }
            "create" => {
                // Create expects fields as params (excluding id which is generated)
                let (_id, undo_action) = self.create(params).await?;
                // Set entity_name on the inverse operation if present
                Ok(match undo_action {
                    UndoAction::Undo(mut op) => {
                        op.entity_name = entity_name.to_string();
                        UndoAction::Undo(op)
                    }
                    UndoAction::Irreversible => UndoAction::Irreversible,
                })
            }
            "delete" => {
                let id = params
                    .get("id")
                    .and_then(|v| v.as_string())
                    .ok_or_else(|| "Missing 'id' parameter".to_string())?;
                // delete returns UndoAction
                let undo_action = self.delete(&id).await?;
                // Set entity_name on the inverse operation if present
                Ok(match undo_action {
                    UndoAction::Undo(mut op) => {
                        op.entity_name = entity_name.to_string();
                        UndoAction::Undo(op)
                    }
                    UndoAction::Irreversible => UndoAction::Irreversible,
                })
            }
            _ => {
                let refresh_id = params
                    .get("id")
                    .and_then(|v| v.as_string())
                    .map(|s| s.to_string());

                // Dispatch to the source datasource which implements operation traits
                // The source's execute_operation will handle routing to the appropriate
                // trait methods (CrudOperations, BlockOperations, etc.)
                let result = self
                    .source
                    .execute_operation(entity_name, op_name, params)
                    .await;

                if result.is_ok() {
                    if let Some(id) = refresh_id {
                        if let Ok(Some(item)) = self.source.get_by_id(&id).await {
                            let _ = self.update_cache(&id, &item).await;
                        }
                    }
                }

                result
            }
        }
    }
}

#[async_trait]
impl<S, T> Queryable<T> for QueryableCache<S, T>
where
    S: DataSource<T>,
    T: HasSchema + Send + Sync + 'static,
{
    async fn query<P>(&self, predicate: P) -> Result<Vec<T>>
    where
        P: Predicate<T> + Send + 'static,
    {
        if let Some(sql_pred) = predicate.to_sql() {
            let backend = self.backend.read().await;
            let conn = backend
                .get_connection()
                .map_err(|e| format!("Failed to get connection: {}", e))?;
            let schema = T::schema();
            let sql = format!("SELECT * FROM {} WHERE {}", schema.table_name, sql_pred.sql);

            let params: Vec<turso::Value> = sql_pred
                .params
                .iter()
                .map(|param| match param {
                    Value::String(s) => turso::Value::Text(s.clone()),
                    Value::Integer(i) => turso::Value::Integer(*i),
                    Value::Float(f) => turso::Value::Real(*f),
                    Value::Boolean(b) => turso::Value::Integer(if *b { 1 } else { 0 }),
                    Value::Null => turso::Value::Null,
                    _ => turso::Value::Null,
                })
                .collect();

            let mut rows = conn
                .query(&sql, turso::params_from_iter(params))
                .await
                .map_err(|e| format!("Failed to execute query: {}", e))?;
            let mut results = Vec::new();

            while let Some(row) = rows
                .next()
                .await
                .map_err(|e| format!("Failed to read row: {}", e))?
            {
                let entity = self.row_to_entity(&row, &schema)?;
                if let Ok(item) = T::from_entity(entity) {
                    results.push(item);
                }
            }

            return Ok(results);
        }

        // Fall back to in-memory filtering if no SQL predicate
        let all_items = self.source.get_all().await?;
        Ok(all_items
            .into_iter()
            .filter(|item| predicate.test(item))
            .collect())
    }
}

// Implement ChangeNotifications<StorageEntity> via TursoBackend
// TODO: Option A - Each QueryableCache filters by table name
// This is inefficient when multiple caches share the same backend (all receive all events).
// Consider optimizing to Option B (table-specific subscriptions) in the future.
#[async_trait]
impl<S, T> ChangeNotifications<StorageEntity> for QueryableCache<S, T>
where
    S: DataSource<T>,
    T: HasSchema + Send + Sync + 'static,
{
    async fn watch_changes_since(
        &self,
        _position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<Vec<Change<StorageEntity>>, ApiError>> + Send>>
    {
        // IMPORTANT: No auto-sync here - caller must sync first
        // This allows offline startup without sync attempts

        let schema = T::schema();
        let table_name = schema.table_name.clone();
        let backend = self.backend.read().await;

        // Get CDC stream from TursoBackend
        let (cdc_conn, row_change_stream) = match backend.row_changes() {
            Ok(result) => result,
            Err(e) => {
                // Return an error stream if we can't get the CDC stream
                let error = ApiError::InternalError {
                    message: e.to_string(),
                };
                return Box::pin(tokio_stream::once(Err(error)));
            }
        };

        // Store connection to keep it alive for CDC callbacks
        // CRITICAL: The connection MUST stay alive for the callback closure to stay alive
        // The callback closure captures the channel sender (tx), which closes the stream if dropped
        // We keep it in an Arc<Mutex> and move it into the stream state
        let conn_guard = Arc::new(tokio::sync::Mutex::new(cdc_conn));

        // TODO: Option A - Filter stream for this table and convert RowChange to Change<StorageEntity>
        // This is inefficient when multiple QueryableCache instances share the same backend.
        // Consider optimizing to Option B (table-specific subscriptions) in the future.
        use crate::storage::turso::{ChangeData, RowChange};
        use holon_api::BatchWithMetadata;
        use tokio_stream::StreamExt;

        // Create a wrapper stream that holds the connection to keep it alive
        // The connection must stay alive for CDC callbacks to work
        let table_name_clone = table_name.clone();

        // Use a custom stream wrapper that holds the connection
        // This ensures the connection stays alive for the lifetime of the stream
        struct ConnectionStream<S> {
            _conn: Arc<tokio::sync::Mutex<turso::Connection>>,
            stream: S,
        }

        impl<S> Stream for ConnectionStream<S>
        where
            S: Stream + Unpin,
        {
            type Item = S::Item;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                Pin::new(&mut self.stream).poll_next(cx)
            }
        }

        let wrapped_stream = ConnectionStream {
            _conn: conn_guard,
            stream: row_change_stream,
        };

        // Filter batches by relation_name in metadata, then flatten to individual RowChanges
        // Use futures::stream::StreamExt for flat_map which has better trait implementations
        let filtered_stream = wrapped_stream
            .filter_map(move |batch: BatchWithMetadata<RowChange>| {
                // Filter by relation_name in metadata
                if batch.metadata.relation_name != table_name_clone {
                    return None;
                }

                // Log CDC event emission with OpenTelemetry
                let change_count = batch.inner.items.len();
                let relation_name = batch.metadata.relation_name.clone();
                let trace_context = batch.metadata.trace_context.clone();

                // Count change types
                let mut created_count = 0;
                let mut updated_count = 0;
                let mut deleted_count = 0;
                for row_change in &batch.inner.items {
                    match &row_change.change {
                        ChangeData::Created { .. } => created_count += 1,
                        ChangeData::Updated { .. } => updated_count += 1,
                        ChangeData::Deleted { .. } => deleted_count += 1,
                    }
                }

                // Create OpenTelemetry span for CDC emission
                let cdc_span = tracing::span!(
                    tracing::Level::INFO,
                    "queryable_cache.cdc_emission",
                    "relation_name" = %relation_name,
                    "change_count" = change_count,
                    "created_count" = created_count,
                    "updated_count" = updated_count,
                    "deleted_count" = deleted_count,
                );
                let _cdc_guard = cdc_span.enter();

                if let Some(ref trace_ctx) = trace_context {
                    // Use tracing macros instead of record() for string values
                    tracing::debug!("trace_id={}, span_id={}", trace_ctx.trace_id, trace_ctx.span_id);
                }

                tracing::info!(
                    "[QueryableCache] Emitting CDC batch: relation={}, changes={} (created={}, updated={}, deleted={})",
                    relation_name,
                    change_count,
                    created_count,
                    updated_count,
                    deleted_count
                );

                // Convert batch items into individual RowChanges and process them
                let mut results = Vec::new();
                for row_change in batch.inner.items {
                    // Convert RowChange to Change<StorageEntity>
                    // StorageEntity is HashMap<String, Value>, so we can use data directly
                    let result = match row_change.change {
                        ChangeData::Created { data, origin } => {
                            Change::Created {
                                data, // data is already HashMap<String, Value> = StorageEntity
                                origin,
                            }
                        }
                        ChangeData::Updated {
                            id: _rowid,
                            data,
                            origin,
                        } => {
                            // Extract entity ID from data, not ROWID
                            let entity_id = data
                                .get("id")
                                .and_then(|v| v.as_string())
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| "unknown".to_string());
                            Change::Updated {
                                id: entity_id,
                                data, // data is already HashMap<String, Value> = StorageEntity
                                origin,
                            }
                        }
                        ChangeData::Deleted { id: _rowid, origin } => {
                            // TODO: For deletes, we need the entity ID, not ROWID
                            // This is a limitation - we may need to track entity_id separately
                            // For now, use a placeholder - proper fix requires enhancing CDC system
                            Change::Deleted {
                                id: format!("rowid_{}", _rowid), // Placeholder - not ideal
                                origin,
                            }
                        }
                    };
                    results.push(result);
                }
                Some(Ok(results))
            });

        Box::pin(filtered_stream)
    }

    async fn get_current_version(&self) -> std::result::Result<Vec<u8>, ApiError> {
        // Return empty version vector for now
        // Could be enhanced to track sync tokens
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::{FieldSchema, SqlPredicate};
    // Note: CoreValue was removed from crate::core::value - using holon_api::Value instead
    use holon_api::Value;

    #[derive(Debug, Clone, PartialEq)]
    struct TestTask {
        id: String,
        title: String,
        priority: i64,
    }

    impl HasSchema for TestTask {
        fn schema() -> Schema {
            Schema::new(
                "test_tasks",
                vec![
                    FieldSchema::new("id", "TEXT").primary_key(),
                    FieldSchema::new("title", "TEXT"),
                    FieldSchema::new("priority", "INTEGER"),
                ],
            )
        }

        fn to_entity(&self) -> DynamicEntity {
            DynamicEntity::new("TestTask")
                .with_field("id", self.id.clone())
                .with_field("title", self.title.clone())
                .with_field("priority", self.priority)
        }

        fn from_entity(entity: DynamicEntity) -> Result<Self> {
            Ok(TestTask {
                id: entity.get_string("id").ok_or("Missing id")?,
                title: entity.get_string("title").ok_or("Missing title")?,
                priority: entity.get_i64("priority").ok_or("Missing priority")?,
            })
        }
    }

    struct InMemoryDataSource {
        items: Arc<RwLock<Vec<TestTask>>>,
    }

    impl InMemoryDataSource {
        fn new() -> Self {
            Self {
                items: Arc::new(RwLock::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl DataSource<TestTask> for InMemoryDataSource {
        async fn get_all(&self) -> Result<Vec<TestTask>> {
            Ok(self.items.read().await.clone())
        }

        async fn get_by_id(&self, id: &str) -> Result<Option<TestTask>> {
            Ok(self.items.read().await.iter().find(|t| t.id == id).cloned())
        }
    }

    #[async_trait]
    impl CrudOperations<TestTask> for InMemoryDataSource {
        async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<()> {
            let mut items = self.items.write().await;
            if let Some(task) = items.iter_mut().find(|t| t.id == id) {
                match field {
                    "title" => {
                        if let Value::String(s) = value {
                            task.title = s;
                        }
                    }
                    "priority" => {
                        if let Value::Integer(i) = value {
                            task.priority = i;
                        }
                    }
                    _ => {}
                }
            }
            Ok(())
        }

        async fn create(&self, fields: HashMap<String, Value>) -> Result<String> {
            let id = fields
                .get("id")
                .and_then(|v| v.as_string().map(|s| s.to_string()))
                .unwrap_or_else(|| {
                    format!(
                        "task-{}",
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos()
                    )
                });
            let title = fields
                .get("title")
                .and_then(|v| v.as_string().map(|s| s.to_string()))
                .unwrap_or_else(|| "Untitled".to_string());
            let priority = fields.get("priority").and_then(|v| v.as_i64()).unwrap_or(0);

            let task = TestTask {
                id: id.clone(),
                title,
                priority,
            };
            self.items.write().await.push(task);
            Ok(id)
        }

        async fn delete(&self, id: &str) -> Result<()> {
            let mut items = self.items.write().await;
            items.retain(|t| t.id != id);
            Ok(())
        }
    }

    struct PriorityPredicate {
        min: i64,
    }

    impl Predicate<TestTask> for PriorityPredicate {
        fn test(&self, item: &TestTask) -> bool {
            item.priority >= self.min
        }

        fn to_sql(&self) -> Option<SqlPredicate> {
            Some(SqlPredicate::new(
                "priority >= ?".to_string(),
                vec![Value::Integer(self.min)],
            ))
        }
    }

    #[tokio::test]
    async fn test_queryable_cache_creation() {
        let source = InMemoryDataSource::new();
        let cache = QueryableCache::new(source).await.unwrap();
        // Verify backend exists and can get a connection
        let backend = cache.backend.read().await;
        let conn = backend.get_connection();
        assert!(conn.is_ok());
    }

    #[tokio::test]
    async fn test_queryable_cache_with_database() {
        let source = InMemoryDataSource::new();
        let cache = QueryableCache::with_database(source, ":memory:")
            .await
            .unwrap();
        // Verify backend exists and can get a connection
        let backend = cache.backend.read().await;
        let conn = backend.get_connection();
        assert!(conn.is_ok());
    }

    #[tokio::test]
    async fn test_insert_and_retrieve() {
        let source = InMemoryDataSource::new();
        let cache = QueryableCache::with_database(source, ":memory:")
            .await
            .unwrap();

        let task = TestTask {
            id: "1".to_string(),
            title: "Test Task".to_string(),
            priority: 5,
        };

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::String(task.id.clone()));
        fields.insert("title".to_string(), Value::String(task.title.clone()));
        fields.insert("priority".to_string(), Value::Integer(task.priority));
        let id = cache.create(fields).await.unwrap();
        assert_eq!(id, "1");

        let retrieved = cache.get_by_id(&id).await.unwrap();
        assert_eq!(retrieved, Some(task));
    }

    #[tokio::test]
    async fn test_sync() {
        let source = InMemoryDataSource::new();

        let mut fields1 = HashMap::new();
        fields1.insert("id".to_string(), Value::String("1".to_string()));
        fields1.insert("title".to_string(), Value::String("Task 1".to_string()));
        fields1.insert("priority".to_string(), Value::Integer(3));
        source.create(fields1).await.unwrap();

        let mut fields2 = HashMap::new();
        fields2.insert("id".to_string(), Value::String("2".to_string()));
        fields2.insert("title".to_string(), Value::String("Task 2".to_string()));
        fields2.insert("priority".to_string(), Value::Integer(7));
        source.create(fields2).await.unwrap();

        let cache = QueryableCache::with_database(source, ":memory:")
            .await
            .unwrap();
        cache.sync().await.unwrap();

        let task1 = cache.get_by_id("1").await.unwrap();
        assert!(task1.is_some());

        let task2 = cache.get_by_id("2").await.unwrap();
        assert!(task2.is_some());
    }

    #[tokio::test]
    async fn test_query_with_sql() {
        let source = InMemoryDataSource::new();
        let cache = QueryableCache::with_database(source, ":memory:")
            .await
            .unwrap();

        let mut fields1 = HashMap::new();
        fields1.insert("id".to_string(), Value::String("1".to_string()));
        fields1.insert(
            "title".to_string(),
            Value::String("Low Priority".to_string()),
        );
        fields1.insert("priority".to_string(), Value::Integer(2));
        cache.create(fields1).await.unwrap();

        let mut fields2 = HashMap::new();
        fields2.insert("id".to_string(), Value::String("2".to_string()));
        fields2.insert(
            "title".to_string(),
            Value::String("High Priority".to_string()),
        );
        fields2.insert("priority".to_string(), Value::Integer(8));
        cache.create(fields2).await.unwrap();

        let results = cache.query(PriorityPredicate { min: 5 }).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "High Priority");
    }

    #[tokio::test]
    async fn test_update_and_delete() {
        let source = InMemoryDataSource::new();
        let cache = QueryableCache::with_database(source, ":memory:")
            .await
            .unwrap();

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::String("1".to_string()));
        fields.insert("title".to_string(), Value::String("Original".to_string()));
        fields.insert("priority".to_string(), Value::Integer(3));
        cache.create(fields).await.unwrap();

        cache
            .set_field("1", "title", Value::String("Updated".to_string()))
            .await
            .unwrap();
        cache
            .set_field("1", "priority", Value::Integer(5))
            .await
            .unwrap();

        let updated = cache.get_by_id("1").await.unwrap().unwrap();
        assert_eq!(updated.title, "Updated");

        cache.delete("1").await.unwrap();
        let deleted = cache.get_by_id("1").await.unwrap();
        assert!(deleted.is_none());
    }
}

/// Generate CREATE TABLE SQL with automatic `_change_origin` column
///
/// This wraps Schema's field definitions and adds the `_change_origin` column
/// for trace context propagation. The column stores JSON-serialized `ChangeOrigin`
/// which allows CDC callbacks to read trace context from each row.
fn generate_create_table_sql_with_change_origin(schema: &Schema) -> String {
    let mut columns = Vec::new();

    for field in &schema.fields {
        let mut col = format!("{} {}", field.name, field.sql_type);

        if field.primary_key {
            col.push_str(" PRIMARY KEY");
        }

        if !field.nullable {
            col.push_str(" NOT NULL");
        }

        columns.push(col);
    }

    // Add _change_origin column for trace context propagation
    columns.push(format!("{} TEXT", CHANGE_ORIGIN_COLUMN));

    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)",
        schema.table_name,
        columns.join(",\n  ")
    )
}
