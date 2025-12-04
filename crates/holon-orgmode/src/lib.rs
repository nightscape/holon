//! Rusty Knowledge Org-mode integration
//!
//! This crate provides integration with org-mode files for the holon PKM system.
//! It parses org-mode files into structured entities (Directory, OrgFile, OrgHeadline)
//! that can be queried and modified through the standard operation system.

#[cfg(feature = "di")]
pub mod di;
pub mod models;
pub mod orgmode_datasource;
pub mod orgmode_sync_provider;
pub mod parser;
pub mod writer;

// Re-export key types
#[cfg(feature = "di")]
pub use di::{OrgModeConfig, OrgModeModule};
pub use models::{OrgFile, OrgHeadline};
// Re-export Directory and ROOT_ID from holon-filesystem for convenience
pub use holon_filesystem::directory::{Directory, ROOT_ID};
pub use orgmode_datasource::{OrgFileDataSource, OrgHeadlineDataSource};
// Re-export DirectoryDataSource from holon-filesystem
pub use holon_filesystem::directory::DirectoryDataSource;
pub use orgmode_sync_provider::OrgModeSyncProvider;
pub use parser::{parse_org_file, ParseResult};
pub use writer::{
    delete_source_block, format_api_source_block, format_block_result, format_header_args,
    format_header_args_from_values, format_org_source_block, insert_api_source_block,
    insert_source_block, update_api_source_block, update_source_block, value_to_header_arg_string,
    write_id_properties,
};

// Re-export orgize for direct access if needed
pub mod orgize_lib {
    pub use orgize::*;
}
