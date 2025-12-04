//! QueryableCache as transparent proxy for stream-based external system integration
//!
//! QueryableCache wraps a CrudOperations and provides:
//! - Read operations from local cache (fast)
//! - Write operations that delegate to datasource (fire-and-forget)
//! - Stream ingestion that updates local cache asynchronously
//!
//! Architecture:
//! - UI calls operations on cache → delegates to datasource → update arrives via stream → cache updated

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{broadcast, RwLock};

use crate::core::datasource::{CrudOperations, DataSource, Result, UndoAction};
use crate::storage::backend::StorageBackend;
use crate::storage::types::StorageEntity;
use holon_api::streaming::ChangeNotifications;
use holon_api::Value;
use holon_api::{ApiError, Change, StreamPosition};
use tokio_stream::{Stream, StreamExt};

/// Helper trait for datasources that implement both ChangeNotifications and CrudOperations
pub trait ChangeNotifyingDataSource<T>:
    ChangeNotifications<T> + CrudOperations<T> + Send + Sync
where
    T: Send + Sync + 'static,
{
}

// Blanket implementation
impl<T, D> ChangeNotifyingDataSource<T> for D
where
    T: Send + Sync + 'static,
    D: ChangeNotifications<T> + CrudOperations<T> + Send + Sync,
{
}

/// Transparent proxy cache that wraps a datasource and provides local caching
///
/// Implements:
/// - DataSource (reads from cache)
/// - CrudOperations (CrudOperations) (delegates writes)
/// - ChangeNotifications (forwards changes from datasource, updates cache)
/// Stream ingestion updates the cache asynchronously as changes arrive from providers.
pub struct QueryableCache<T> {
    datasource: Arc<dyn ChangeNotifyingDataSource<T>>,
    db: Arc<RwLock<Box<dyn StorageBackend>>>,
    table: String,
}

impl<T> QueryableCache<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(
        datasource: Arc<dyn ChangeNotifyingDataSource<T>>,
        db: Arc<RwLock<Box<dyn StorageBackend>>>,
        table: String,
    ) -> Self {
        Self {
            datasource,
            db,
            table,
        }
    }

    /// Wire up stream ingestion from ChangeNotifications (spawns background task)
    ///
    /// This method subscribes to a datasource's ChangeNotifications stream and updates the local cache
    /// as changes arrive. The background task runs until the stream is closed or the cache is dropped.
    pub fn ingest_change_stream<D>(&self, datasource: Arc<D>)
    where
        D: ChangeNotifications<T> + Send + Sync + 'static,
        T: DeserializeOwned + serde::Serialize + Send + Sync + Clone + 'static,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let db = Arc::clone(&self.db);
            let table = self.table.clone();

            tokio::spawn(async move {
                let mut stream = datasource
                    .watch_changes_since(StreamPosition::Beginning)
                    .await;
                while let Some(result) = stream.next().await {
                    match result {
                        Ok(changes) => {
                            for change in changes {
                                match change {
                                    Change::Created { data, .. } | Change::Updated { data, .. } => {
                                        if let Err(e) = Self::upsert_to_db(&db, &table, &data).await
                                        {
                                            eprintln!("Error ingesting change: {}", e);
                                        }
                                    }
                                    Change::Deleted { id, .. } => {
                                        let mut db_guard = db.write().await;
                                        if let Err(e) = db_guard.delete(&table, &id).await {
                                            eprintln!("Error ingesting delete: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Error in change stream: {:?}", e);
                            // Continue processing - don't break on errors
                        }
                    }
                }
            });
        }
        #[cfg(target_arch = "wasm32")]
        {
            // On WASM, we can't spawn tasks with non-Send futures
            // Change notifications need to be polled manually or handled differently
            eprintln!(
                "Warning: Change stream ingestion not available on WASM - cache will not update automatically"
            );
        }
    }

    /// Wire up stream ingestion (spawns background task)
    ///
    /// This method subscribes to a broadcast channel and updates the local cache
    /// as changes arrive from the provider. The background task runs until the
    /// stream is closed or the cache is dropped.
    ///
    /// **Deprecated**: Use `ingest_change_stream` with ChangeNotifications instead.
    pub fn ingest_stream(&self, rx: broadcast::Receiver<Vec<Change<T>>>)
    where
        T: DeserializeOwned + serde::Serialize + Send + Sync + Clone + 'static,
    {
        let db = Arc::clone(&self.db);
        let table = self.table.clone();

        tokio::spawn(async move {
            let mut rx = rx;
            loop {
                match rx.recv().await {
                    Ok(changes) => {
                        for change in changes {
                            match change {
                                Change::Created { data, .. } | Change::Updated { data, .. } => {
                                    if let Err(e) = Self::upsert_to_db(&db, &table, &data).await {
                                        eprintln!("Error ingesting change: {}", e);
                                    }
                                }
                                Change::Deleted { id, .. } => {
                                    let mut db_guard = db.write().await;
                                    if let Err(e) = db_guard.delete(&table, &id).await {
                                        eprintln!("Error ingesting delete: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("Stream lagged by {} messages, triggering resync", n);
                        // TODO: Trigger full resync
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });
    }

    /// Helper to upsert an item to the database
    async fn upsert_to_db(
        db: &Arc<RwLock<Box<dyn StorageBackend>>>,
        table: &str,
        item: &T,
    ) -> Result<()>
    where
        T: serde::Serialize,
    {
        // Convert item to StorageEntity
        let json_value = serde_json::to_value(item).map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;

        let mut entity = StorageEntity::new();
        if let serde_json::Value::Object(map) = json_value {
            for (key, value) in map {
                let our_value = match value {
                    serde_json::Value::Null => Value::Null,
                    serde_json::Value::Bool(b) => Value::Boolean(b),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Value::Integer(i)
                        } else if let Some(_f) = n.as_f64() {
                            Value::Json(
                                serde_json::to_string(&serde_json::Value::Number(n.clone()))
                                    .unwrap_or_default(),
                            )
                        } else {
                            Value::Json(
                                serde_json::to_string(&serde_json::Value::Number(n.clone()))
                                    .unwrap_or_default(),
                            )
                        }
                    }
                    serde_json::Value::String(s) => Value::String(s),
                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                        Value::Json(serde_json::to_string(&value).unwrap_or_default())
                    }
                };
                entity.insert(key, our_value);
            }
        }

        // Extract ID for upsert (before moving entity)
        let id = entity
            .get("id")
            .and_then(|v| v.as_string())
            .ok_or_else(|| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Item missing id field",
                )) as Box<dyn std::error::Error + Send + Sync>
            })?
            .to_string();

        // Upsert to database (entity is moved here)
        let mut db_guard = db.write().await;
        db_guard.update(table, &id, entity).await.map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(())
    }
}

// Implement ChangeNotifications (forwards from datasource, updates cache)
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<T> ChangeNotifications<T> for QueryableCache<T>
where
    T: DeserializeOwned + serde::Serialize + Send + Sync + Clone + 'static,
{
    async fn watch_changes_since(
        &self,
        position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<Vec<Change<T>>, ApiError>> + Send>> {
        // Forward stream from datasource, applying changes to cache as they arrive
        let db = Arc::clone(&self.db);
        let table = self.table.clone();
        let datasource_stream = self.datasource.watch_changes_since(position).await;

        // Create a stream that applies changes to cache and forwards them
        // We'll use a manual implementation since async_stream isn't available
        struct CacheUpdatingStream<T> {
            datasource_stream:
                Pin<Box<dyn Stream<Item = std::result::Result<Vec<Change<T>>, ApiError>> + Send>>,
            db: Arc<RwLock<Box<dyn StorageBackend>>>,
            table: String,
        }

        impl<T> Stream for CacheUpdatingStream<T>
        where
            T: DeserializeOwned + serde::Serialize + Send + Sync + Clone + 'static,
        {
            type Item = std::result::Result<Vec<Change<T>>, ApiError>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                match self.datasource_stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(changes))) => {
                        // Apply changes to cache (spawn async task for this)
                        let db = Arc::clone(&self.db);
                        let table = self.table.clone();
                        let changes_clone = changes.clone();

                        tokio::spawn(async move {
                            for change in &changes_clone {
                                match change {
                                    Change::Created { data, .. } | Change::Updated { data, .. } => {
                                        if let Err(e) =
                                            QueryableCache::<T>::upsert_to_db(&db, &table, data)
                                                .await
                                        {
                                            eprintln!(
                                                "Error updating cache from change stream: {}",
                                                e
                                            );
                                        }
                                    }
                                    Change::Deleted { id, .. } => {
                                        let mut db_guard = db.write().await;
                                        if let Err(e) = db_guard.delete(&table, id).await {
                                            eprintln!("Error deleting from cache: {}", e);
                                        }
                                    }
                                }
                            }
                        });

                        Poll::Ready(Some(Ok(changes)))
                    }
                    Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Pending => Poll::Pending,
                }
            }
        }

        Box::pin(CacheUpdatingStream {
            datasource_stream,
            db,
            table,
        })
    }

    async fn get_current_version(&self) -> std::result::Result<Vec<u8>, ApiError> {
        self.datasource.get_current_version().await
    }
}

// Implement DataSource (reads from cache)
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<T> DataSource<T> for QueryableCache<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    async fn get_all(&self) -> Result<Vec<T>> {
        let db_guard = self.db.read().await;
        let entities = db_guard
            .query(&self.table, crate::storage::Filter::And(vec![]))
            .await
            .map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )) as Box<dyn std::error::Error + Send + Sync>
            })?;

        let mut results = Vec::new();
        for entity in entities {
            // Convert StorageEntity to JSON and then to T
            let json_value = serde_json::to_value(&entity).map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )) as Box<dyn std::error::Error + Send + Sync>
            })?;
            let item: T = serde_json::from_value(json_value).map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )) as Box<dyn std::error::Error + Send + Sync>
            })?;
            results.push(item);
        }
        Ok(results)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<T>> {
        let db_guard = self.db.read().await;
        match db_guard.get(&self.table, id).await.map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })? {
            Some(entity) => {
                // Convert StorageEntity to JSON and then to T
                let json_value = serde_json::to_value(&entity).map_err(|e| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )) as Box<dyn std::error::Error + Send + Sync>
                })?;
                let item: T = serde_json::from_value(json_value).map_err(|e| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )) as Box<dyn std::error::Error + Send + Sync>
                })?;
                Ok(Some(item))
            }
            None => Ok(None),
        }
    }
}

// Implement CrudOperations (CrudOperations) (delegates to wrapped datasource)
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<T> CrudOperations<T> for QueryableCache<T>
where
    T: Send + Sync + 'static,
{
    async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<UndoAction> {
        // Delegate to datasource - update arrives via stream
        self.datasource.set_field(id, field, value).await
    }

    async fn create(&self, fields: HashMap<String, Value>) -> Result<(String, UndoAction)> {
        // Delegate to datasource - full entity arrives via stream
        self.datasource.create(fields).await
    }

    async fn delete(&self, id: &str) -> Result<UndoAction> {
        // Delegate to datasource - deletion confirmed via stream
        self.datasource.delete(id).await
    }
}
