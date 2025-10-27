//! Re-export datasource types for macro compatibility
//!
//! This module exists to match the path structure expected by the operations_trait macro:
//! `#crate_path::core::datasource::UnknownOperationError`

pub use crate::{Result, UnknownOperationError};
