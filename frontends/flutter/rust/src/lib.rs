pub mod api;
mod frb_generated;

pub use api::types::{Block, Traversal};

// Re-export only essential types for BackendEngine API
// Make BackendEngine and Change available through crate root for generated code
pub use api::{BackendEngine, Change, RowChangeStream, StorageEntity};
pub use api::{OperationDescriptor, OperationParam, RenderSpec};

// Re-export BlockChange from holon-api
pub use holon_api::BlockChange;
