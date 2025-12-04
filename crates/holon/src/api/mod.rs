//! Shared API crate for holon frontends
//!
//! This crate provides technology-agnostic types and traits for all
//! holon frontends (Tauri, Flutter, future REST API, etc.).
//!
//! # Architecture
//!
//! - `types`: Core data types (Block, InitialState, ApiError, etc.)
//! - `repository`: DocumentRepository trait defining backend operations
//! - `backend_engine`: PRQL render engine for reactive UI (Phase 4.1)
//! - `ffi_bridge`: FFI functions exposed to Flutter (Phase 4.1)
//!
//! # Design Principles
//!
//! - Technology-agnostic: No frontend-specific dependencies
//! - Clean domain model: Hides CRDT implementation details
//! - Type-safe errors: Structured error handling across FFI boundaries
//! - Async-first: All operations return Futures for flexibility

pub mod loro_backend;
pub mod memory_backend;
pub mod pbt_infrastructure;
pub mod repository;
pub mod types;

pub mod backend_engine;
pub mod operation_dispatcher;
pub mod ui_types;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod loro_backend_pbt;

// Re-export commonly used types
pub use loro_backend::LoroBackend;
pub use memory_backend::MemoryBackend;
pub use repository::{CoreOperations, DocumentRepository, Lifecycle, P2POperations};
// Re-export streaming types from holon-api (moved from streaming module)
pub use holon_api::{
    ApiError, Batch, BatchMapChange, BatchMetadata, BatchTraceContext, BatchWithMetadata, Block,
    BlockChange, BlockMetadata, BlockWithDepth, Change, ChangeOrigin, MapChange, StreamPosition,
    WithMetadata,
};

// Re-export render engine types for FFI
pub use backend_engine::BackendEngine;
pub use operation_dispatcher::OperationDispatcher;
pub use ui_types::{CursorPosition, UiState};

// Re-export OperationDescriptor and OperationParam for FRB type generation
pub use holon_api::{OperationDescriptor, OperationParam};

// Re-export CDC streaming types
pub use crate::storage::turso::{ChangeData, RowChange, RowChangeStream};
