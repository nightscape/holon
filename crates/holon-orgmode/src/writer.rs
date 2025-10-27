//! Write-back support for org-mode files
//!
//! **Note**: All writer functions are now in `file_io.rs`.
//! This module is kept for backward compatibility but is now empty.
//!
//! Use `file_io` functions instead:
//! - `write_id_properties` -> `file_io::write_id_properties`
//! - `reconstruct_file` -> `file_io::reconstruct_file`
//! - `format_org_source_block` -> `file_io::format_org_source_block`
//! - `format_api_source_block` -> `file_io::format_api_source_block`
//! - `format_block_result` -> `file_io::format_block_result`
//! - `insert_source_block` -> `file_io::insert_source_block`
//! - `update_source_block` -> `file_io::update_source_block`
//! - `delete_source_block` -> `file_io::delete_source_block`
//! - `format_header_args` -> `file_io::format_header_args`
//! - `format_header_args_from_values` -> `file_io::format_header_args_from_values`
//! - `value_to_header_arg_string` -> `file_io::value_to_header_arg_string`
