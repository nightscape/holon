//! Rusty Knowledge Org-mode integration
//!
//! This crate provides integration with org-mode files for the holon PKM system.
//! It parses org-mode files into structured entities (Directory, Document, Block)
//! that can be queried and modified through the standard operation system.
//!
//! # Type System
//!
//! This crate uses generic `Document` and `Block` types from the core holon crate,
//! with org-specific fields stored in the `properties` JSON field. Extension traits
//! (`OrgDocumentExt`, `OrgBlockExt`) provide accessors for these org-specific fields.
//!
//! - `Document` + `OrgDocumentExt`: Represents an org file
//! - `Block` + `OrgBlockExt`: Represents an org headline

pub mod block_diff;
#[cfg(feature = "di")]
pub mod di;
pub mod file_io;
pub mod file_utils;
pub mod file_watcher;
pub mod link_parser;
pub mod models;
pub mod org_renderer;
pub mod orgmode_adapter;
pub mod orgmode_event_adapter;
pub mod orgmode_file_writer;
pub mod orgmode_sync_provider;
pub mod parser;
pub mod write_tracker;
pub mod writer;

// Re-export key types
#[cfg(feature = "di")]
pub use di::{FileWatcherReadySignal, OrgModeConfig, OrgModeModule};

// Core types - use generic Document with extension traits
// Note: Block is NOT re-exported here to avoid duplicate type issues with flutter_rust_bridge
// Use holon_api::block::Block directly instead
pub use holon::sync::Document;
pub use holon_filesystem::directory::{Directory, ROOT_ID};

// Extension traits for org-specific functionality
pub use models::ParsedSectionContent;
pub use models::{find_document_id, get_block_file_path, BlockResolver, HashMapBlockResolver};
pub use models::{is_document_uri, org_props, parse_document_uri};
pub use models::{OrgBlockExt, OrgDocumentExt, SourceBlock, ToOrg};

// Sync providers and adapters
#[allow(deprecated)]
pub use block_diff::{blocks_to_map, blocks_to_parsed_map, diff_blocks, BlockDiff};
pub use file_watcher::OrgFileWatcher;
pub use holon_filesystem::directory::DirectoryDataSource;
pub use org_renderer::OrgRenderer;
pub use orgmode_adapter::OrgAdapter;
pub use orgmode_event_adapter::OrgModeEventAdapter;
pub use orgmode_file_writer::OrgFileWriter;
pub use orgmode_sync_provider::OrgModeSyncProvider;
pub use parser::{parse_org_file, ParseResult};
pub use write_tracker::WriteTracker;

// File I/O utilities for org-mode files
pub use file_io::{
    delete_source_block, format_api_source_block, format_block_result, format_header_args,
    format_header_args_from_values, format_org_source_block, insert_source_block, reconstruct_file,
    update_source_block, value_to_header_arg_string, write_id_properties,
};

// Re-export orgize for direct access if needed
pub mod orgize_lib {
    pub use orgize::*;
}
