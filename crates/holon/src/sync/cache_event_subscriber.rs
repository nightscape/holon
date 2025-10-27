//! QueryableCache Event Subscriber
//!
//! Subscribes to events from the EventBus and ingests them into QueryableCache.
//! Converts Events back to Changes for cache ingestion.

use std::sync::Arc;
use tokio_stream::StreamExt;
use tracing;

use crate::core::queryable_cache::QueryableCache;
use crate::storage::types::{Result, StorageError};
use crate::sync::event_bus::{Event, EventBus, EventFilter, EventStatus};
use crate::sync::event_subscriber::EventSubscriber;
use holon_api::block::Block;
use holon_api::streaming::{Change, ChangeOrigin};

/// QueryableCache Event Subscriber
///
/// Subscribes to events from the EventBus and ingests them into QueryableCache.
/// Only processes confirmed events (skips speculative events).
pub struct CacheEventSubscriber {
    cache: Arc<QueryableCache<Block>>,
    event_bus: Option<Arc<dyn EventBus>>,
    origin: String,
}

impl CacheEventSubscriber {
    /// Create a new CacheEventSubscriber
    pub fn new(cache: Arc<QueryableCache<Block>>) -> Self {
        Self {
            cache,
            event_bus: None,
            origin: "cache".to_string(),
        }
    }

    /// Create a new CacheEventSubscriber with EventBus reference (for mark_processed)
    pub fn with_event_bus(cache: Arc<QueryableCache<Block>>, event_bus: Arc<dyn EventBus>) -> Self {
        Self {
            cache,
            event_bus: Some(event_bus),
            origin: "cache".to_string(),
        }
    }

    /// Start subscribing to events and ingesting to cache
    ///
    /// This spawns a background task that listens to the EventBus and processes
    /// events into the cache.
    pub async fn start(&self, event_bus: Arc<dyn EventBus>) -> Result<()> {
        let cache = Arc::clone(&self.cache);
        let _origin = self.origin.clone();

        // Subscribe to confirmed events only (skip speculative)
        let filter = EventFilter::new()
            .with_status(EventStatus::Confirmed)
            .with_aggregate_type("block");

        eprintln!(
            "[CacheEventSubscriber] About to subscribe to event_bus with filter: {:?}",
            filter
        );
        let mut event_stream = event_bus.subscribe(filter).await?;
        eprintln!("[CacheEventSubscriber] Successfully subscribed to event_bus");

        let event_bus_clone = event_bus.clone();
        tokio::spawn(async move {
            eprintln!("[CacheEventSubscriber] Started listening to events");
            tracing::info!("[CacheEventSubscriber] Started listening to events");

            while let Some(event) = event_stream.next().await {
                eprintln!(
                    "[CacheEventSubscriber] Received event: type={}, id={}",
                    event.event_type, event.id
                );
                // Note: Origin filtering is handled by EventFilter in subscribe() call above.
                // Status filtering (confirmed only) is also handled by EventFilter.
                // No need for redundant checks here.

                // Convert event to change and ingest
                // IMPORTANT: Spawn a new task for apply_batch to avoid deadlock!
                // The CDC callback fires while TursoEventBus::publish() holds the write_conn lock.
                // If we try to apply_batch synchronously, we'll block waiting for the same lock.
                // By spawning a task, we allow publish() to complete and release the lock first.
                match Self::event_to_change(&event) {
                    Ok(change) => {
                        let cache_clone = Arc::clone(&cache);
                        let event_bus_for_mark = event_bus_clone.clone();
                        let event_id = event.id.clone();

                        tokio::spawn(async move {
                            eprintln!(
                                "[CacheEventSubscriber] Spawned task to apply_batch for event_id={}",
                                event_id
                            );
                            if let Err(e) = cache_clone.apply_batch(&[change], None).await {
                                eprintln!(
                                    "[CacheEventSubscriber] FAILED to apply change to cache: {}",
                                    e
                                );
                                tracing::error!(
                                    "[CacheEventSubscriber] Failed to apply change to cache: {}",
                                    e
                                );
                            } else {
                                eprintln!(
                                    "[CacheEventSubscriber] Successfully applied change to cache for event_id={}",
                                    event_id
                                );
                                // Mark event as processed by cache
                                if let Err(e) =
                                    event_bus_for_mark.mark_processed(&event_id, "cache").await
                                {
                                    tracing::warn!(
                                        "[CacheEventSubscriber] Failed to mark event as processed: {}",
                                        e
                                    );
                                }
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!(
                            "[CacheEventSubscriber] FAILED to convert event to change: {}",
                            e
                        );
                        tracing::error!(
                            "[CacheEventSubscriber] Failed to convert event to change: {}",
                            e
                        );
                    }
                }
            }

            tracing::info!("[CacheEventSubscriber] Event stream closed");
        });

        Ok(())
    }

    /// Convert an Event back to a Change<Block>
    fn event_to_change(event: &Event) -> Result<Change<Block>> {
        let change_origin = match event.origin {
            crate::sync::event_bus::EventOrigin::Loro => ChangeOrigin::Remote {
                operation_id: None,
                trace_id: event.trace_id.clone(),
            },
            crate::sync::event_bus::EventOrigin::Org => ChangeOrigin::Remote {
                operation_id: None,
                trace_id: event.trace_id.clone(),
            },
            crate::sync::event_bus::EventOrigin::Todoist => ChangeOrigin::Remote {
                operation_id: None,
                trace_id: event.trace_id.clone(),
            },
            crate::sync::event_bus::EventOrigin::Ui => ChangeOrigin::Local {
                operation_id: None,
                trace_id: event.trace_id.clone(),
            },
            crate::sync::event_bus::EventOrigin::Other(_) => ChangeOrigin::Remote {
                operation_id: None,
                trace_id: event.trace_id.clone(),
            },
        };

        match event.event_type.as_str() {
            "block.created" => {
                let data_value = event.payload.get("data").ok_or_else(|| {
                    StorageError::DatabaseError("Missing 'data' in event payload".to_string())
                })?;
                let block: Block = serde_json::from_value(data_value.clone()).map_err(|e| {
                    StorageError::SerializationError(format!("Failed to deserialize Block: {}", e))
                })?;
                Ok(Change::Created {
                    data: block,
                    origin: change_origin,
                })
            }
            "block.updated" => {
                let data_value = event.payload.get("data").ok_or_else(|| {
                    StorageError::DatabaseError("Missing 'data' in event payload".to_string())
                })?;
                let block: Block = serde_json::from_value(data_value.clone()).map_err(|e| {
                    StorageError::SerializationError(format!("Failed to deserialize Block: {}", e))
                })?;
                Ok(Change::Updated {
                    id: event.aggregate_id.clone(),
                    data: block,
                    origin: change_origin,
                })
            }
            "block.deleted" => Ok(Change::Deleted {
                id: event.aggregate_id.clone(),
                origin: change_origin,
            }),
            "block.fields_changed" => {
                let fields_value = event.payload.get("fields").ok_or_else(|| {
                    StorageError::DatabaseError("Missing 'fields' in event payload".to_string())
                })?;
                let fields: Vec<(String, holon_api::Value, holon_api::Value)> =
                    serde_json::from_value(fields_value.clone()).map_err(|e| {
                        StorageError::SerializationError(format!(
                            "Failed to deserialize fields: {}",
                            e
                        ))
                    })?;
                Ok(Change::FieldsChanged {
                    entity_id: event.aggregate_id.clone(),
                    fields,
                    origin: change_origin,
                })
            }
            _ => Err(StorageError::DatabaseError(format!(
                "Unknown event type: {}",
                event.event_type
            ))),
        }
    }
}

#[async_trait::async_trait]
impl EventSubscriber for CacheEventSubscriber {
    fn origin(&self) -> &str {
        &self.origin
    }

    async fn process_event(&self, event: &Event) -> Result<()> {
        // Note: Status filtering should be done by the caller via EventFilter.
        // This method assumes the event has already been filtered.

        let change = Self::event_to_change(event)?;
        self.cache.apply_batch(&[change], None).await.map_err(|e| {
            StorageError::DatabaseError(format!("Failed to apply batch to cache: {}", e))
        })?;

        // Mark event as processed if EventBus reference is available
        if let Some(ref event_bus) = self.event_bus {
            if let Err(e) = event_bus.mark_processed(&event.id, "cache").await {
                tracing::warn!(
                    "[CacheEventSubscriber] Failed to mark event as processed: {}",
                    e
                );
            }
        }

        Ok(())
    }
}
