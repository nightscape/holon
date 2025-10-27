use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use async_trait::async_trait;
use std::{pin::Pin, sync::Arc};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::Stream;

use crate::{ApiError, Value};

// Task-local storage for trace context propagation through async call chains
tokio::task_local! {
    /// Current trace context for the executing task
    /// Set at FFI boundary, read by BatchTraceContext::from_current_span()
    pub static CURRENT_TRACE_CONTEXT: BatchTraceContext;
}

/// Position in the change stream to start watching from.
///
/// Used with `watch_changes_since()` to control whether to receive current state or only new changes.
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StreamPosition {
    /// Start from the beginning: first stream all current blocks as Created events,
    /// then stream subsequent changes
    Beginning,
    /// Start from a specific version: stream only changes that occurred after this version
    Version(Vec<u8>),
}

/// Origin of a change event (local vs. remote).
///
/// Used to prevent UI echo when local changes sync back via P2P.
/// Includes optional trace context for distributed tracing and audit trails.
/// Stored in `_change_origin` column to propagate trace context across threads (CDC callbacks).
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChangeOrigin {
    /// Change initiated by this client
    Local {
        /// Span ID (16 hex chars) linking this change to the originating operation
        operation_id: Option<String>,
        /// Trace ID (32 hex chars) for distributed tracing
        trace_id: Option<String>,
    },
    /// Change received from P2P sync or external system
    Remote {
        /// Span ID (16 hex chars) linking this change to the originating operation
        operation_id: Option<String>,
        /// Trace ID (32 hex chars) for distributed tracing
        trace_id: Option<String>,
    },
}

/// Column name for change origin metadata stored in each row
pub const CHANGE_ORIGIN_COLUMN: &str = "_change_origin";

impl ChangeOrigin {
    /// Create Local origin with trace context extracted from current OpenTelemetry span
    ///
    /// flutter_rust_bridge:ignore
    pub fn local_with_current_span() -> Self {
        let (operation_id, trace_id) = Self::extract_trace_context_from_current_span();
        Self::Local {
            operation_id,
            trace_id,
        }
    }

    /// Create Remote origin with trace context extracted from current OpenTelemetry span
    ///
    /// flutter_rust_bridge:ignore
    pub fn remote_with_current_span() -> Self {
        let (operation_id, trace_id) = Self::extract_trace_context_from_current_span();
        Self::Remote {
            operation_id,
            trace_id,
        }
    }

    /// Create Local origin with explicit trace context
    pub fn local_with_trace(trace_id: Option<String>, operation_id: Option<String>) -> Self {
        Self::Local {
            operation_id,
            trace_id,
        }
    }

    /// Create Remote origin with explicit trace context
    pub fn remote_with_trace(trace_id: Option<String>, operation_id: Option<String>) -> Self {
        Self::Remote {
            operation_id,
            trace_id,
        }
    }

    /// Create from BatchTraceContext
    pub fn local_from_batch_trace_context(ctx: &BatchTraceContext) -> Self {
        Self::Local {
            operation_id: Some(ctx.span_id.clone()),
            trace_id: Some(ctx.trace_id.clone()),
        }
    }

    /// Create Remote from BatchTraceContext
    pub fn remote_from_batch_trace_context(ctx: &BatchTraceContext) -> Self {
        Self::Remote {
            operation_id: Some(ctx.span_id.clone()),
            trace_id: Some(ctx.trace_id.clone()),
        }
    }

    /// Extract trace context (operation_id, trace_id) from task-local or current span
    ///
    /// Priority:
    /// 1. Task-local CURRENT_TRACE_CONTEXT (most reliable for async propagation)
    /// 2. OpenTelemetry span context (via set_parent)
    /// 3. Fallback to tracing span ID
    ///
    /// flutter_rust_bridge:ignore
    fn extract_trace_context_from_current_span() -> (Option<String>, Option<String>) {
        // First, try task-local storage (most reliable for async propagation)
        if let Ok(ctx) = CURRENT_TRACE_CONTEXT.try_with(|ctx| ctx.clone()) {
            return (Some(ctx.span_id), Some(ctx.trace_id));
        }

        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let span = tracing::Span::current();

        if span.is_none() {
            return (None, None);
        }

        // Get the OpenTelemetry context that was set on this span (via set_parent)
        let otel_ctx = span.context();
        let span_ref = otel_ctx.span();
        let span_ctx = span_ref.span_context();

        if span_ctx.is_valid() {
            // We have a valid OTel context - use the trace_id from it
            // For operation_id, use the tracing span's own ID
            let trace_id = format!("{:032x}", span_ctx.trace_id());

            // Get the tracing span's ID for operation correlation
            let operation_id = span
                .id()
                .map(|id| format!("{:016x}", id.into_u64()))
                .unwrap_or_else(|| format!("{:016x}", span_ctx.span_id()));

            return (Some(operation_id), Some(trace_id));
        }

        // Fallback: use tracing span ID if no OTel context available
        if let Some(id) = span.id() {
            let operation_id = format!("{:016x}", id.into_u64());
            let trace_id = format!("{:032x}", id.into_u64());
            return (Some(operation_id), Some(trace_id));
        }

        (None, None)
    }

    /// Get trace_id if available
    ///
    /// flutter_rust_bridge:ignore
    pub fn trace_id(&self) -> Option<&str> {
        match self {
            Self::Local { trace_id, .. } | Self::Remote { trace_id, .. } => trace_id.as_deref(),
        }
    }

    /// Get operation_id (span_id) if available
    ///
    /// flutter_rust_bridge:ignore
    pub fn operation_id(&self) -> Option<&str> {
        match self {
            Self::Local { operation_id, .. } | Self::Remote { operation_id, .. } => {
                operation_id.as_deref()
            }
        }
    }

    /// Check if this is a local change
    ///
    /// flutter_rust_bridge:ignore
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }

    /// Convert to BatchTraceContext if trace context is available
    ///
    /// flutter_rust_bridge:ignore
    pub fn to_batch_trace_context(&self) -> Option<BatchTraceContext> {
        let (trace_id, operation_id) = match self {
            Self::Local {
                trace_id,
                operation_id,
            }
            | Self::Remote {
                trace_id,
                operation_id,
            } => (trace_id.as_ref()?, operation_id.as_ref()?),
        };
        Some(BatchTraceContext {
            trace_id: trace_id.clone(),
            span_id: operation_id.clone(),
            trace_flags: 0x01, // Sampled
            trace_state: None,
        })
    }

    /// Serialize to JSON string for storage in _change_origin column
    ///
    /// flutter_rust_bridge:ignore
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "null".to_string())
    }

    /// Deserialize from JSON string read from _change_origin column
    ///
    /// flutter_rust_bridge:ignore
    pub fn from_json(json: &str) -> Option<Self> {
        serde_json::from_str(json).ok()
    }
}

/// Change notification event.
///
/// Emitted by the change stream to notify frontends of document updates.
/// Includes origin tracking to suppress echo of local edits.
///
/// Note: This generic type is not directly exposed to FRB. Use concrete type aliases like `BlockChange` or `MapChange`.
/// flutter_rust_bridge:ignore
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Change<T> {
    /// Block was created
    Created { data: T, origin: ChangeOrigin },
    /// Block content was updated (character-level)
    Updated {
        id: String,
        data: T,
        origin: ChangeOrigin,
    },
    /// Block was deleted (tombstone set)
    Deleted { id: String, origin: ChangeOrigin },
    /// Partial field update (efficient delta propagation)
    FieldsChanged {
        entity_id: String,
        /// (field_name, old_value, new_value)
        fields: Vec<(String, crate::Value, crate::Value)>,
        origin: ChangeOrigin,
    },
}

/// Type alias for Change<HashMap<String, Value>>
///
/// Used for streaming query result changes.
/// flutter_rust_bridge:non_opaque
pub type MapChange = Change<HashMap<String, Value>>;

/// Type alias for Change<Block>
///
/// Used for streaming block changes.
/// flutter_rust_bridge:non_opaque
pub type BlockChange = Change<crate::Block>;

/// Batch of changes for efficient transmission
///
/// Groups multiple changes together to reduce overhead when multiple changes
/// occur simultaneously (e.g., from a single RelationChangeEvent).
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch<T> {
    pub items: Vec<T>,
}

/// Type alias for Batch<MapChange>
///
/// Used for streaming batched query result changes.
/// flutter_rust_bridge:non_opaque
pub type BatchMapChange = Batch<MapChange>;

/// Type alias for Batch<MapChange> wrapped with metadata
///
/// Used for streaming batched query result changes with tracing context.
/// flutter_rust_bridge:non_opaque
pub type BatchMapChangeWithMetadata = WithMetadata<BatchMapChange, BatchMetadata>;

/// Generic wrapper for adding metadata to any type
///
/// This allows you to add metadata to any type without modifying the original type.
/// Implements Deref/DerefMut for ergonomic access to the inner type.
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithMetadata<T, M> {
    /// The inner value
    pub inner: T,
    /// The metadata associated with this value
    pub metadata: M,
}

impl<T, M> Deref for WithMetadata<T, M> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, M> DerefMut for WithMetadata<T, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T, M> From<T> for WithMetadata<T, M>
where
    M: Default,
{
    fn from(inner: T) -> Self {
        Self {
            inner,
            metadata: M::default(),
        }
    }
}

/// Sync token update to be persisted atomically with data changes
///
/// Used to ensure sync token and data are written in the same transaction,
/// preventing "database is locked" errors and ensuring consistency.
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncTokenUpdate {
    /// Provider name (e.g., "todoist")
    pub provider_name: String,
    /// New stream position to save
    pub position: StreamPosition,
}

/// Metadata associated with a batch of changes
///
/// Contains information about where the batch originated from, including
/// the relation/view name and trace context for observability.
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchMetadata {
    /// The view/relation that generated this batch
    pub relation_name: String,
    /// OpenTelemetry trace context for the batch (if available)
    pub trace_context: Option<BatchTraceContext>,
    /// Sync token to update atomically with the data changes
    pub sync_token: Option<SyncTokenUpdate>,
}

/// Trace context for distributed tracing across FFI and async boundaries
///
/// Uses W3C TraceContext format for serialization. This type can be used:
/// - In batch metadata for CDC streaming
/// - Across FFI boundaries (Flutter, Tauri, etc.)
/// - For propagating trace context through async call chains
///
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchTraceContext {
    /// Trace ID (32 hex characters)
    pub trace_id: String,
    /// Span ID (16 hex characters)
    pub span_id: String,
    /// Trace flags (1 byte, typically 0x01 for sampled)
    pub trace_flags: u8,
    /// Optional trace state (W3C tracestate header value)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_state: Option<String>,
}

impl BatchTraceContext {
    /// Create a new BatchTraceContext with all fields
    pub fn new(
        trace_id: String,
        span_id: String,
        trace_flags: u8,
        trace_state: Option<String>,
    ) -> Self {
        Self {
            trace_id,
            span_id,
            trace_flags,
            trace_state,
        }
    }

    /// Create a BatchTraceContext from OpenTelemetry span context
    ///
    /// flutter_rust_bridge:ignore
    pub fn from_span_context(span_context: &opentelemetry::trace::SpanContext) -> Self {
        // Extract trace state if present
        let trace_state = {
            let ts = span_context.trace_state();
            let header = ts.header();
            if header.is_empty() {
                None
            } else {
                Some(header)
            }
        };

        Self {
            trace_id: format!("{:032x}", span_context.trace_id()),
            span_id: format!("{:016x}", span_context.span_id()),
            trace_flags: if span_context.is_sampled() {
                0x01
            } else {
                0x00
            },
            trace_state,
        }
    }

    /// Convert to OpenTelemetry span context
    ///
    /// Returns None if the trace_id or span_id cannot be parsed.
    ///
    /// flutter_rust_bridge:ignore
    pub fn to_span_context(&self) -> Option<opentelemetry::trace::SpanContext> {
        use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
        use std::str::FromStr;

        let trace_id = TraceId::from_hex(&self.trace_id).ok()?;
        let span_id = SpanId::from_hex(&self.span_id).ok()?;
        let trace_flags = TraceFlags::new(self.trace_flags);
        let trace_state = self
            .trace_state
            .as_ref()
            .and_then(|s| TraceState::from_str(s).ok())
            .unwrap_or_default();

        Some(SpanContext::new(
            trace_id,
            span_id,
            trace_flags,
            true, // is_remote = true since this came from serialized context
            trace_state,
        ))
    }

    /// Get operation ID (alias for span_id, for compatibility)
    pub fn operation_id(&self) -> &str {
        &self.span_id
    }

    /// Check if this context is sampled
    pub fn is_sampled(&self) -> bool {
        self.trace_flags & 0x01 != 0
    }

    /// Extract trace context from task-local storage or current span
    ///
    /// Priority:
    /// 1. Task-local CURRENT_TRACE_CONTEXT (set at FFI boundary)
    /// 2. OpenTelemetry Context::current()
    /// 3. Tracing span context (via set_parent)
    /// 4. Fallback to tracing span ID
    ///
    /// flutter_rust_bridge:ignore
    pub fn from_current_span() -> Option<Self> {
        // First, try task-local storage (most reliable for async propagation)
        if let Ok(ctx) = CURRENT_TRACE_CONTEXT.try_with(|ctx| ctx.clone()) {
            return Some(ctx);
        }

        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        // Second, try Context::current() which is propagated by tracing-opentelemetry
        let current_ctx = opentelemetry::Context::current();
        let current_span = current_ctx.span();
        let current_span_ctx = current_span.span_context();

        if current_span_ctx.is_valid() {
            return Some(Self::from_span_context(current_span_ctx));
        }

        // Third, try the tracing span's context (set via set_parent)
        let span = tracing::Span::current();

        if !span.is_none() {
            let otel_ctx = span.context();
            let span_ref = otel_ctx.span();
            let span_ctx = span_ref.span_context();

            if span_ctx.is_valid() {
                return Some(Self::from_span_context(span_ctx));
            }

            // Fallback: try to create from tracing span ID
            if let Some(id) = span.id() {
                return Some(Self {
                    trace_id: format!("{:032x}", id.into_u64()),
                    span_id: format!("{:016x}", id.into_u64()),
                    trace_flags: 0x01, // Sampled
                    trace_state: None,
                });
            }
        }

        None
    }
}

/// Type alias for Batch wrapped with metadata
pub type BatchWithMetadata<T> = WithMetadata<Batch<T>, BatchMetadata>;

/// Real-time change notification and state synchronization.
///
/// This trait provides race-free state sync by streaming the current document state
/// followed by all subsequent changes. Backends that support real-time updates implement this trait.
///
/// # Architecture
///
/// This trait uses vendor-neutral Rust async Streams (`tokio_stream::Stream`)
/// which can be adapted to any frontend technology:
/// - Flutter: Adapted via `StreamSink` in FRB bridge layer
/// - Tauri: Adapted via event emission in command layer
/// - REST/Web: Adapted via Server-Sent Events or WebSocket
///
/// # Example
///
/// ```rust,no_run
/// use holon::api::ChangeNotifications;
/// use tokio_stream::StreamExt;
///
/// async fn example(repo: impl ChangeNotifications<Block>) -> anyhow::Result<()> {
///     // Start watching - first receives all current blocks as Created events,
///     // then streams subsequent changes
///     let mut stream = repo.watch_changes_since(StreamPosition::Beginning).await;
///
///     // Process batched changes as they arrive
///     while let Some(result) = stream.next().await {
///         match result {
///             Ok(changes) => {
///                 for change in changes {
///                     println!("Block changed: {:?}", change);
///                 }
///             }
///             Err(e) => eprintln!("Change stream error: {:?}", e),
///         }
///     }
///
///     // Stream automatically unsubscribes when dropped
///     Ok(())
/// }
/// ```
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait ChangeNotifications<T>: Send + Sync {
    /// Subscribe to document changes since a specific position.
    ///
    /// Returns a Stream that emits batched document changes. Behavior depends on the `position` parameter:
    /// - `StreamPosition::Beginning`: First emits all current blocks as `Change::Created` events in a batch,
    ///   then continues streaming subsequent changes (initial sync mode)
    /// - `StreamPosition::Version(v)`: Streams only changes that occurred after version `v`
    ///   (incremental sync mode)
    ///
    /// # Arguments
    ///
    /// * `position` - Stream position to start from (beginning or specific version)
    ///
    /// # Returns
    ///
    /// A Stream that yields `Result<Vec<Change<T>>, ApiError>` items. Each item is a batch of changes.
    /// The stream continues until either:
    /// - It is explicitly dropped (automatic unsubscription)
    /// - An error occurs (yielded as `Err`)
    /// - The backend shuts down (stream closes)
    ///
    /// # Error Propagation
    ///
    /// Errors are propagated through the stream's Result type rather than
    /// terminating the stream. Backends may choose to:
    /// - Continue streaming after recoverable errors
    /// - Close the stream after fatal errors
    ///
    /// # Resource Management
    ///
    /// The stream automatically unsubscribes and releases resources when dropped.
    /// No explicit cleanup method needed.
    async fn watch_changes_since(
        &self,
        position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<Change<T>>, ApiError>> + Send>>;

    /// Get the current version vector of the document.
    ///
    /// Returns the version vector representing the current state of the document.
    /// This can be used to track document evolution over time.
    ///
    /// # Returns
    ///
    /// A version vector as a byte array.
    async fn get_current_version(&self) -> Result<Vec<u8>, ApiError>;
}

/// Type alias for change notification subscribers
pub type ChangeSubscribers<T> = Arc<Mutex<Vec<mpsc::Sender<Result<Vec<Change<T>>, ApiError>>>>>;

// Types are now imported from holon-api and re-exported from api::mod
// No need to re-export here to avoid conflicts

// BlockChange is now defined in holon-api
// Re-exported above for convenience
