//! Re-export storage types for macro compatibility
//!
//! This module exists to match the path structure expected by the operations_trait macro:
//! `#crate_path::storage::types::StorageEntity`

use holon_api::Value;
use std::collections::HashMap;

/// StorageEntity type alias for HashMap<String, Value>
/// This matches the type used in the holon crate for macro compatibility
pub type StorageEntity = HashMap<String, Value>;
