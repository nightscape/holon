pub mod api;
mod frb_generated;

// Note: Block is NOT re-exported here to avoid FRB duplicate class issue
// Block comes from holon_api via rust_input config
pub use api::types::Traversal;

// Re-export only essential types for BackendEngine API
// Make BackendEngine and Change available through crate root for generated code
pub use api::{BackendEngine, Change, RowChangeStream, StorageEntity};
pub use api::{OperationDescriptor, OperationParam, RenderSpec};
pub use holon_api::BatchMapChangeWithMetadata;

// Re-export BlockChange from holon-api
pub use holon_api::BlockChange;
