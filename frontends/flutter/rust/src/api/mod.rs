pub mod ffi_bridge;
pub mod flutter_pbt_backend;
pub mod flutter_pbt_runner;
pub mod flutter_pbt_state_machine;
pub mod pbt_proptest;
pub mod types;

pub use holon::api::types::{NewBlock, Traversal};
pub use holon::api::BackendEngine;
use holon::core::DynamicEntity;
pub use holon::storage::turso::RowChangeStream;
pub use holon::storage::types::StorageEntity;
pub use holon_api::ApiError;
// Note: Block is NOT re-exported here - it comes directly from holon_api via FRB config
// to avoid duplicate class generation in Dart
pub use holon_api::{BlockChange, BlockMetadata};
pub use holon_api::{Change, ChangeOrigin, MapChange, StreamPosition};
pub use holon_api::{OperationDescriptor, OperationParam, RenderSpec};
