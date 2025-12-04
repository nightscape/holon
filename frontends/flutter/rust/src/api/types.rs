//! Type wrappers and re-exports for Flutter-Rust-Bridge
//!
//! This module re-exports opaque types and defines enums for proper Dart pattern matching.

use flutter_rust_bridge::frb;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
// Re-export SpanContext for generated code
pub use opentelemetry::trace::SpanContext;

// Re-export opaque types from backend
// Block will be generated as an opaque type in Dart
// Fields are accessed via BlockOps methods (getId, getContent, etc.)
pub use super::{Block, BlockMetadata, NewBlock, Traversal};

// Re-export Change from holon-api (moved from holon)
pub use holon_api::Change;

// Re-export backend types for use in Rust code (not mirror types!)
// These are what we actually use in Rust code
pub use super::ApiError;
// Re-export streaming types from holon-api (moved from holon)
pub use holon_api::{ChangeOrigin, StreamPosition};

// Value is marked non-opaque via comment in holon-api/src/lib.rs - no mirror needed

// All these types are marked non-opaque via comments in holon-api - no mirrors needed:
// - BlockContent, SourceBlock, BlockResult, ResultOutput (in block.rs)
// - DynamicEntity (in entity.rs)
// - Value (in lib.rs)

// Type aliases for Change<T> variants
// BlockChange is now exported directly from holon-api
pub use holon_api::{
    Batch, BatchMapChange, BatchMapChangeWithMetadata, BatchMetadata, BatchTraceContext,
    BatchWithMetadata, BlockChange, MapChange, WithMetadata,
};

// Type alias for RowChange (same as MapChange, used for query result changes)
// This is a Dart-only type alias, so we add it via dart_code
#[frb(dart_code = "
  import 'third_party/holon_api/streaming.dart' show MapChange;
  typedef RowChange = MapChange;
")]
pub struct _RowChangePlaceholder;

/// Position in the change stream to start watching from.
/// Now mirrored from holon-api
#[frb(mirror(holon_api::StreamPosition))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum _StreamPosition {
    Beginning,
    Version(Vec<u8>),
}

/// Origin of a change event (local vs. remote).
/// Now mirrored from holon-api
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[frb(mirror(holon_api::ChangeOrigin))]
pub enum _ChangeOrigin {
    Local { operation_id: Option<String> },
    Remote { operation_id: Option<String> },
}

/// Structured error types for API operations.
#[frb(mirror(ApiError))]
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum _ApiError {
    #[error("Block not found: {id}")]
    BlockNotFound { id: String },

    #[error("Document not found: {doc_id}")]
    DocumentNotFound { doc_id: String },

    #[error("Cyclic move detected: cannot move block {id} to descendant {target_parent}")]
    CyclicMove { id: String, target_parent: String },

    #[error("Invalid operation: {message}")]
    InvalidOperation { message: String },

    #[error("Network error: {message}")]
    NetworkError { message: String },

    #[error("Internal error: {message}")]
    InternalError { message: String },
}

/// Trace context for propagating OpenTelemetry trace information across FFI boundary.
///
/// Uses W3C TraceContext format (traceparent header format) for serialization.
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TraceContext {
    /// Trace ID (16-byte hex string, 32 hex characters)
    pub trace_id: String,
    /// Span ID (8-byte hex string, 16 hex characters)
    pub span_id: String,
    /// Trace flags (1 byte, typically 0x01 for sampled)
    pub trace_flags: u8,
    /// Optional trace state (key-value pairs)
    pub trace_state: Option<String>,
}

#[frb(dart_code = "
  /// Get operation ID from trace context (uses span_id)
  String? get operationId {
    return spanId;
  }
")]
impl TraceContext {
    /// Create a new TraceContext from OpenTelemetry span context
    pub fn from_span_context(span_context: &opentelemetry::trace::SpanContext) -> Self {
        Self {
            trace_id: format!("{:032x}", span_context.trace_id()),
            span_id: format!("{:016x}", span_context.span_id()),
            trace_flags: if span_context.is_sampled() {
                0x01
            } else {
                0x00
            },
            trace_state: None, // TODO: Extract trace state if needed
        }
    }

    /// Convert to OpenTelemetry span context
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
            true,
            trace_state,
        ))
    }
}
