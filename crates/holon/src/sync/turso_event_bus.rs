//! Turso-based EventBus implementation
//!
//! Uses Turso CDC (Change Data Capture) for event subscription.

use async_trait::async_trait;
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tracing;

use crate::storage::turso::TursoBackend;
use crate::storage::types::{Result, StorageError};
use crate::sync::event_bus::{
    Event, EventBus, EventFilter, EventId, EventOrigin, EventStatus, EventStream,
};

/// Turso-based EventBus implementation
///
/// **Note on Connection Management**: The CDC connection is now managed by TursoBackend.
/// This ensures all writes go through the same connection, allowing CDC callbacks to fire.
pub struct TursoEventBus {
    backend: Arc<RwLock<TursoBackend>>,
    // Note: CDC connection is now managed by TursoBackend.write_conn
}

impl TursoEventBus {
    /// Create a new TursoEventBus
    pub fn new(backend: Arc<RwLock<TursoBackend>>) -> Self {
        Self { backend }
    }

    /// Initialize the events table schema
    pub async fn init_schema(&self) -> Result<()> {
        let backend = self.backend.read().await;

        // Create events table
        backend
            .execute_ddl(
                "CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                origin TEXT NOT NULL,
                status TEXT DEFAULT 'confirmed',
                payload TEXT NOT NULL,
                trace_id TEXT,
                command_id TEXT,
                created_at INTEGER NOT NULL,
                processed_by_loro INTEGER DEFAULT 0,
                processed_by_org INTEGER DEFAULT 0,
                processed_by_cache INTEGER DEFAULT 0,
                speculative_id TEXT,
                rejection_reason TEXT
            )",
            )
            .await
            .map_err(|e| {
                StorageError::DatabaseError(format!("Failed to create events table: {}", e))
            })?;

        // Create indexes for consumers to find unprocessed events
        backend
            .execute_ddl(
                "CREATE INDEX IF NOT EXISTS idx_events_loro_pending
             ON events(created_at)
             WHERE processed_by_loro = 0 AND origin != 'loro' AND status = 'confirmed'",
            )
            .await
            .map_err(|e| {
                StorageError::DatabaseError(format!("Failed to create loro pending index: {}", e))
            })?;

        backend
            .execute_ddl(
                "CREATE INDEX IF NOT EXISTS idx_events_org_pending
             ON events(created_at)
             WHERE processed_by_org = 0 AND origin != 'org' AND status = 'confirmed'",
            )
            .await
            .map_err(|e| {
                StorageError::DatabaseError(format!("Failed to create org pending index: {}", e))
            })?;

        backend
            .execute_ddl(
                "CREATE INDEX IF NOT EXISTS idx_events_cache_pending
             ON events(created_at)
             WHERE processed_by_cache = 0 AND status = 'confirmed'",
            )
            .await
            .map_err(|e| {
                StorageError::DatabaseError(format!("Failed to create cache pending index: {}", e))
            })?;

        // Index for aggregate history
        backend
            .execute_ddl(
                "CREATE INDEX IF NOT EXISTS idx_events_aggregate
             ON events(aggregate_type, aggregate_id, created_at)",
            )
            .await
            .map_err(|e| {
                StorageError::DatabaseError(format!("Failed to create aggregate index: {}", e))
            })?;

        // Index for undo correlation
        backend
            .execute_ddl(
                "CREATE INDEX IF NOT EXISTS idx_events_command
             ON events(command_id)
             WHERE command_id IS NOT NULL",
            )
            .await
            .map_err(|e| {
                StorageError::DatabaseError(format!("Failed to create command index: {}", e))
            })?;

        tracing::info!("[TursoEventBus] Schema initialized");
        Ok(())
    }

    /// Parse a RowChange (Change<StorageEntity>) into an Event
    fn parse_row_change_to_event(change: &crate::storage::turso::ChangeData) -> Result<Event> {
        use crate::storage::turso::ChangeData;
        use holon_api::Value;

        match change {
            ChangeData::Created { data, .. } | ChangeData::Updated { data, .. } => {
                // Extract fields from StorageEntity
                let id = data
                    .get("id")
                    .and_then(|v| match v {
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        StorageError::DatabaseError("Missing 'id' in event row".to_string())
                    })?;

                let event_type = data
                    .get("event_type")
                    .and_then(|v| match v {
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        StorageError::DatabaseError("Missing 'event_type' in event row".to_string())
                    })?;

                let aggregate_type = data
                    .get("aggregate_type")
                    .and_then(|v| match v {
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        StorageError::DatabaseError(
                            "Missing 'aggregate_type' in event row".to_string(),
                        )
                    })?;

                let aggregate_id = data
                    .get("aggregate_id")
                    .and_then(|v| match v {
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        StorageError::DatabaseError(
                            "Missing 'aggregate_id' in event row".to_string(),
                        )
                    })?;

                let origin_str = data
                    .get("origin")
                    .and_then(|v| match v {
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        StorageError::DatabaseError("Missing 'origin' in event row".to_string())
                    })?;

                let status_str = data
                    .get("status")
                    .and_then(|v| match v {
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(|| "confirmed".to_string());

                let payload_json = data
                    .get("payload")
                    .and_then(|v| match v {
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        StorageError::DatabaseError("Missing 'payload' in event row".to_string())
                    })?;

                let payload: HashMap<String, serde_json::Value> =
                    serde_json::from_str(&payload_json).map_err(|e| {
                        StorageError::SerializationError(format!(
                            "Failed to parse payload JSON: {}",
                            e
                        ))
                    })?;

                let trace_id = data.get("trace_id").and_then(|v| match v {
                    Value::String(s) if !s.is_empty() => Some(s.clone()),
                    _ => None,
                });

                let command_id = data.get("command_id").and_then(|v| match v {
                    Value::String(s) if !s.is_empty() => Some(s.clone()),
                    _ => None,
                });

                let created_at = data
                    .get("created_at")
                    .and_then(|v| match v {
                        Value::Integer(i) => Some(*i),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        StorageError::DatabaseError("Missing 'created_at' in event row".to_string())
                    })?;

                let speculative_id = data.get("speculative_id").and_then(|v| match v {
                    Value::String(s) if !s.is_empty() => Some(s.clone()),
                    _ => None,
                });

                let rejection_reason = data.get("rejection_reason").and_then(|v| match v {
                    Value::String(s) if !s.is_empty() => Some(s.clone()),
                    _ => None,
                });

                let origin = EventOrigin::from_str(&origin_str);
                let status = EventStatus::from_str(&status_str).unwrap_or(EventStatus::Confirmed);

                Ok(Event {
                    id,
                    event_type,
                    aggregate_type,
                    aggregate_id,
                    origin,
                    status,
                    payload,
                    trace_id,
                    command_id,
                    created_at,
                    speculative_id,
                    rejection_reason,
                })
            }
            ChangeData::Deleted { id, .. } => {
                // For deleted events, we can't reconstruct the full event
                // This shouldn't happen in practice (events table is append-only)
                Err(StorageError::DatabaseError(format!(
                    "Unexpected DELETE event for event ID: {}",
                    id
                )))
            }
            ChangeData::FieldsChanged { .. } => {
                // FieldsChanged is not used for events table (events are immutable)
                Err(StorageError::DatabaseError(
                    "Unexpected FieldsChanged event for events table".to_string(),
                ))
            }
        }
    }

    /// Check if an event matches the filter criteria
    fn event_matches_filter(event: &Event, filter: &EventFilter) -> bool {
        // Filter by origin
        if !filter.origins.is_empty() {
            if !filter
                .origins
                .iter()
                .any(|o| o.as_str() == event.origin.as_str())
            {
                return false;
            }
        }

        // Filter by status
        if !filter.statuses.is_empty() {
            if !filter.statuses.iter().any(|s| *s == event.status) {
                return false;
            }
        }

        // Filter by aggregate type
        if !filter.aggregate_types.is_empty() {
            if !filter
                .aggregate_types
                .iter()
                .any(|t| t == &event.aggregate_type)
            {
                return false;
            }
        }

        // Filter by timestamp
        if let Some(after_timestamp) = filter.after_timestamp {
            if event.created_at <= after_timestamp {
                return false;
            }
        }

        true
    }

    /// SQL for inserting events
    const INSERT_EVENT_SQL: &'static str = "INSERT INTO events (
        id, event_type, aggregate_type, aggregate_id, origin, status,
        payload, trace_id, command_id, created_at, speculative_id, rejection_reason
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    /// Convert an Event to SQL parameters
    fn event_to_params(event: &Event, payload_json: &str) -> Vec<turso::Value> {
        vec![
            turso::Value::Text(event.id.clone()),
            turso::Value::Text(event.event_type.clone()),
            turso::Value::Text(event.aggregate_type.clone()),
            turso::Value::Text(event.aggregate_id.clone()),
            turso::Value::Text(event.origin.as_str().to_string()),
            turso::Value::Text(event.status.as_str().to_string()),
            turso::Value::Text(payload_json.to_string()),
            event
                .trace_id
                .clone()
                .map(turso::Value::Text)
                .unwrap_or(turso::Value::Null),
            event
                .command_id
                .clone()
                .map(turso::Value::Text)
                .unwrap_or(turso::Value::Null),
            turso::Value::Integer(event.created_at),
            event
                .speculative_id
                .clone()
                .map(turso::Value::Text)
                .unwrap_or(turso::Value::Null),
            event
                .rejection_reason
                .clone()
                .map(turso::Value::Text)
                .unwrap_or(turso::Value::Null),
        ]
    }
}

#[async_trait]
impl EventBus for TursoEventBus {
    async fn publish(&self, event: Event, command_id: Option<EventId>) -> Result<EventId> {
        let payload_json = serde_json::to_string(&event.payload).map_err(|e| {
            StorageError::SerializationError(format!("Failed to serialize payload: {}", e))
        })?;

        let mut event = event;
        if let Some(cmd_id) = command_id {
            event.command_id = Some(cmd_id);
        }

        let event_id = event.id.clone();
        let event_type = event.event_type.clone();
        let params = Self::event_to_params(&event, &payload_json);

        let backend = self.backend.read().await;
        backend
            .execute_via_actor(Self::INSERT_EVENT_SQL, params)
            .await
            .map_err(|e| StorageError::DatabaseError(format!("Failed to insert event: {}", e)))?;

        tracing::debug!("[TursoEventBus] Published event: {}", event_id);
        eprintln!(
            "[TursoEventBus::publish] Published event id={}, type={}",
            event_id, event_type
        );
        Ok(event_id)
    }

    async fn publish_batch(&self, events: Vec<Event>) -> Result<Vec<EventId>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        tracing::debug!(
            "[TursoEventBus] Publishing batch of {} events",
            events.len()
        );

        let mut statements = Vec::with_capacity(events.len());
        let mut event_ids = Vec::with_capacity(events.len());

        for event in &events {
            let payload_json = serde_json::to_string(&event.payload).map_err(|e| {
                StorageError::SerializationError(format!("Failed to serialize payload: {}", e))
            })?;
            let params = Self::event_to_params(event, &payload_json);
            event_ids.push(event.id.clone());
            statements.push((Self::INSERT_EVENT_SQL.to_string(), params));
        }

        let backend = self.backend.read().await;
        backend
            .execute_batch_transaction(statements)
            .await
            .map_err(|e| {
                StorageError::DatabaseError(format!("Failed to insert event batch: {}", e))
            })?;

        tracing::debug!(
            "[TursoEventBus] Published batch of {} events",
            event_ids.len()
        );
        Ok(event_ids)
    }

    async fn subscribe(&self, filter: EventFilter) -> Result<EventStream> {
        // Generate unique view name from filter (CDC only works with materialized views)
        // Include origin in view name to ensure different filters get different views
        let origin_suffix = filter
            .origins
            .first()
            .map(|o| format!("_{}", o.as_str()))
            .unwrap_or_default();
        let view_name = format!(
            "events_view_{}{}",
            filter
                .aggregate_types
                .first()
                .map(|s| s.as_str())
                .unwrap_or("all"),
            origin_suffix
        );

        // Build WHERE clause from filter
        // NOTE: Turso materialized views only support simple predicates: column = 'value' or column = column
        //       NOT supported: 1=1, IN(...), OR, etc.
        // For single values we use: column = 'value'
        // For multiple values we need multiple views or use the first value only
        let mut where_clauses = Vec::new();

        // Status filter - use first status only (Turso limitation)
        if let Some(status) = filter.statuses.first() {
            where_clauses.push(format!("status = '{}'", status.as_str()));
        }

        // Aggregate type filter - use first type only (Turso limitation)
        if let Some(agg_type) = filter.aggregate_types.first() {
            where_clauses.push(format!("aggregate_type = '{}'", agg_type));
        }

        // Origin filter - use first origin only (Turso limitation)
        if let Some(origin) = filter.origins.first() {
            where_clauses.push(format!("origin = '{}'", origin.as_str()));
        }

        // If no filters, select all events
        let where_clause = if where_clauses.is_empty() {
            // Turso requires a WHERE clause for materialized views, use a tautology
            // that it can parse: id = id (column = column is supported)
            "id = id".to_string()
        } else {
            where_clauses.join(" AND ")
        };

        // Create materialized view for this subscription
        // Since view_name is deterministic (based on filter), we can reuse existing views
        let create_view_sql = format!(
            "CREATE MATERIALIZED VIEW {} AS SELECT * FROM events WHERE {}",
            view_name, where_clause
        );

        // IMPORTANT: Create the materialized view on the ACTOR's connection (or write connection).
        // All DDL should go through the actor to prevent schema change errors.
        let backend = self.backend.read().await;

        // Check if view already exists first (using execute_sql which routes through actor)
        let check_view_sql = format!(
            "SELECT name FROM sqlite_master WHERE type='view' AND name='{}'",
            view_name
        );
        let view_exists = match backend
            .execute_sql(&check_view_sql, std::collections::HashMap::new())
            .await
        {
            Ok(results) => !results.is_empty(),
            Err(_) => false,
        };

        if view_exists {
            eprintln!(
                "[TursoEventBus::subscribe] View {} already exists, reusing (skipping DDL)",
                view_name
            );
        } else {
            eprintln!(
                "[TursoEventBus::subscribe] Creating materialized view: {}",
                create_view_sql
            );
            // Use execute_ddl which routes through the actor when available
            backend.execute_ddl(&create_view_sql).await?;
            eprintln!("[TursoEventBus::subscribe] CREATE VIEW succeeded");
        }

        // Now set up CDC stream for watching the view (write connection already exists)

        let mut cdc_stream = backend.row_changes().await?;
        eprintln!(
            "[TursoEventBus::subscribe] CDC stream established (connection managed by TursoBackend)"
        );

        let (tx, rx) = mpsc::channel(1024);
        let filter_clone = filter.clone();
        let view_name_clone = view_name.clone();

        // Spawn task to parse CDC events and apply filter
        // Track already-delivered event IDs to prevent re-delivery when mark_processed updates the row
        let mut delivered_event_ids = std::collections::HashSet::new();

        tokio::spawn(async move {
            eprintln!(
                "[TursoEventBus::subscribe] CDC listener task started for view: {}",
                view_name_clone
            );
            while let Some(batch) = cdc_stream.next().await {
                eprintln!(
                    "[TursoEventBus::subscribe] CDC received batch with {} items",
                    batch.items.len()
                );
                for row_change in &batch.items {
                    eprintln!(
                        "[TursoEventBus::subscribe] CDC row_change: relation={}",
                        row_change.relation_name
                    );
                    // Only process events from our materialized view
                    if row_change.relation_name != view_name_clone {
                        continue;
                    }

                    // Parse RowChange into Event
                    match TursoEventBus::parse_row_change_to_event(&row_change.change) {
                        Ok(event) => {
                            // Skip events we've already delivered (prevents re-delivery when mark_processed updates the row)
                            if delivered_event_ids.contains(&event.id) {
                                eprintln!(
                                    "[TursoEventBus::subscribe] Skipping already-delivered event: {}",
                                    event.id
                                );
                                continue;
                            }

                            eprintln!(
                                "[TursoEventBus::subscribe] Parsed event: type={}, matches_filter={}",
                                event.event_type,
                                TursoEventBus::event_matches_filter(&event, &filter_clone)
                            );
                            // Apply filter
                            if TursoEventBus::event_matches_filter(&event, &filter_clone) {
                                // Remember this event was delivered
                                delivered_event_ids.insert(event.id.clone());
                                if tx.send(event).await.is_err() {
                                    tracing::warn!("[TursoEventBus] Event stream receiver closed");
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[TursoEventBus::subscribe] Failed to parse: {}", e);
                            tracing::warn!(
                                "[TursoEventBus] Failed to parse event from row change: {}",
                                e
                            );
                        }
                    }
                }
            }
            tracing::info!("[TursoEventBus] CDC stream closed");
            eprintln!("[TursoEventBus::subscribe] CDC stream closed");
        });

        Ok(ReceiverStream::new(rx))
    }

    async fn mark_processed(&self, event_id: &EventId, consumer: &str) -> Result<()> {
        let backend = self.backend.read().await;

        let column = match consumer {
            "loro" => "processed_by_loro",
            "org" => "processed_by_org",
            "cache" => "processed_by_cache",
            _ => {
                return Err(StorageError::DatabaseError(format!(
                    "Unknown consumer: {}",
                    consumer
                )));
            }
        };

        // Use execute_via_actor which routes through the database actor
        let sql = format!("UPDATE events SET {} = 1 WHERE id = ?", column);
        backend
            .execute_via_actor(&sql, vec![turso::Value::Text(event_id.clone())])
            .await
            .map_err(|e| {
                StorageError::DatabaseError(format!("Failed to mark event as processed: {}", e))
            })?;

        Ok(())
    }

    async fn update_status(
        &self,
        event_id: &EventId,
        status: EventStatus,
        rejection_reason: Option<String>,
    ) -> Result<()> {
        let backend = self.backend.read().await;

        // Use execute_via_actor which routes through the database actor
        let rejection_reason_value = rejection_reason
            .map(|r| turso::Value::Text(r))
            .unwrap_or(turso::Value::Null);

        let sql = "UPDATE events SET status = ?, rejection_reason = ? WHERE id = ?";
        backend
            .execute_via_actor(
                sql,
                vec![
                    turso::Value::Text(status.as_str().to_string()),
                    rejection_reason_value,
                    turso::Value::Text(event_id.clone()),
                ],
            )
            .await
            .map_err(|e| {
                StorageError::DatabaseError(format!("Failed to update event status: {}", e))
            })?;

        Ok(())
    }

    async fn link_speculative(
        &self,
        confirmed_event_id: &EventId,
        speculative_event_id: &EventId,
    ) -> Result<()> {
        let backend = self.backend.read().await;

        // Use execute_via_actor which routes through the database actor
        let sql = "UPDATE events SET speculative_id = ? WHERE id = ?";
        backend
            .execute_via_actor(
                sql,
                vec![
                    turso::Value::Text(speculative_event_id.clone()),
                    turso::Value::Text(confirmed_event_id.clone()),
                ],
            )
            .await
            .map_err(|e| {
                StorageError::DatabaseError(format!("Failed to link speculative event: {}", e))
            })?;

        Ok(())
    }
}
