//! Adapter that subscribes to Loro changes and publishes to EventBus
//!
//! This adapter bridges the gap between LoroBlockOperations' broadcast channel
//! and the EventBus. It subscribes to Loro changes and converts them to Events
//! for publishing to the EventBus.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing;

use crate::storage::types::{Result, StorageError};
use crate::sync::event_bus::{Event, EventBus, EventOrigin};
use holon_api::block::Block;
use holon_api::streaming::{Change, ChangeOrigin};

/// Adapter that subscribes to Loro changes and publishes to EventBus
pub struct LoroEventAdapter {
    event_bus: Arc<dyn EventBus>,
}

impl LoroEventAdapter {
    /// Create a new LoroEventAdapter
    pub fn new(event_bus: Arc<dyn EventBus>) -> Self {
        Self { event_bus }
    }

    /// Start subscribing to Loro changes and publishing to EventBus
    ///
    /// This spawns a background task that listens to the Loro broadcast channel
    /// and publishes events to the EventBus.
    pub fn start(&self, mut loro_rx: broadcast::Receiver<Vec<Change<Block>>>) -> Result<()> {
        let event_bus = Arc::clone(&self.event_bus);

        tokio::spawn(async move {
            tracing::info!("[LoroEventAdapter] Started listening to Loro changes");
            eprintln!("[LoroEventAdapter] Started listening to Loro changes");

            loop {
                match loro_rx.recv().await {
                    Ok(changes) => {
                        eprintln!("[LoroEventAdapter] Received {} changes", changes.len());
                        for change in changes {
                            if let Err(e) = Self::publish_change(&event_bus, &change).await {
                                tracing::error!(
                                    "[LoroEventAdapter] Failed to publish change: {}",
                                    e
                                );
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("[LoroEventAdapter] Stream lagged by {} messages", n);
                        // Continue processing - don't break on lag
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::info!("[LoroEventAdapter] Loro stream closed");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Convert a Loro Change to an Event and publish it
    async fn publish_change(event_bus: &Arc<dyn EventBus>, change: &Change<Block>) -> Result<()> {
        let (event_type, aggregate_id, payload_map, trace_id) = match change {
            Change::Created { data, origin } => {
                let payload = serde_json::to_value(data).map_err(|e| {
                    StorageError::SerializationError(format!("Failed to serialize Block: {}", e))
                })?;
                let mut payload_map = HashMap::new();
                payload_map.insert("data".to_string(), payload);
                payload_map.insert(
                    "change_type".to_string(),
                    serde_json::Value::String("created".to_string()),
                );

                let trace_id = match origin {
                    ChangeOrigin::Local { trace_id, .. }
                    | ChangeOrigin::Remote { trace_id, .. } => trace_id.clone(),
                };

                (
                    "block.created".to_string(),
                    data.id.clone(),
                    payload_map,
                    trace_id,
                )
            }
            Change::Updated { id, data, origin } => {
                let payload = serde_json::to_value(data).map_err(|e| {
                    StorageError::SerializationError(format!("Failed to serialize Block: {}", e))
                })?;
                let mut payload_map = HashMap::new();
                payload_map.insert("data".to_string(), payload);
                payload_map.insert(
                    "change_type".to_string(),
                    serde_json::Value::String("updated".to_string()),
                );

                let trace_id = match origin {
                    ChangeOrigin::Local { trace_id, .. }
                    | ChangeOrigin::Remote { trace_id, .. } => trace_id.clone(),
                };

                (
                    "block.updated".to_string(),
                    id.clone(),
                    payload_map,
                    trace_id,
                )
            }
            Change::Deleted { id, origin } => {
                let mut payload_map = HashMap::new();
                payload_map.insert(
                    "change_type".to_string(),
                    serde_json::Value::String("deleted".to_string()),
                );

                let trace_id = match origin {
                    ChangeOrigin::Local { trace_id, .. }
                    | ChangeOrigin::Remote { trace_id, .. } => trace_id.clone(),
                };

                (
                    "block.deleted".to_string(),
                    id.clone(),
                    payload_map,
                    trace_id,
                )
            }
            Change::FieldsChanged {
                entity_id,
                fields,
                origin,
            } => {
                let fields_json = serde_json::to_value(fields).map_err(|e| {
                    StorageError::SerializationError(format!("Failed to serialize fields: {}", e))
                })?;
                let mut payload_map = HashMap::new();
                payload_map.insert("fields".to_string(), fields_json);
                payload_map.insert(
                    "change_type".to_string(),
                    serde_json::Value::String("fields_changed".to_string()),
                );

                let trace_id = match origin {
                    ChangeOrigin::Local { trace_id, .. }
                    | ChangeOrigin::Remote { trace_id, .. } => trace_id.clone(),
                };

                (
                    "block.fields_changed".to_string(),
                    entity_id.clone(),
                    payload_map,
                    trace_id,
                )
            }
        };

        let mut event = Event::new(
            event_type,
            "block",
            aggregate_id,
            EventOrigin::Loro,
            payload_map,
        );

        // Set trace_id if available
        event.trace_id = trace_id;

        eprintln!(
            "[LoroEventAdapter::publish_change] About to publish event: type={}, aggregate_id={}",
            event.event_type, event.aggregate_id
        );
        let event_id = event_bus.publish(event, None).await?;
        eprintln!(
            "[LoroEventAdapter::publish_change] Published event, id={}",
            event_id
        );

        Ok(())
    }
}
