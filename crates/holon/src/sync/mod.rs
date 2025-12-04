//! Synchronization infrastructure
//!
//! - `collaborative_doc`: Loro-based real-time document collaboration
//! - `external_system`: External system integration with contract-based validation

pub mod collaborative_doc;
pub mod external_system;

pub use collaborative_doc::*;
pub use external_system::*;
