//! Fractional index utilities for block ordering
//!
//! This module provides utilities for generating fractional index keys
//! used for maintaining block order in hierarchical structures.

use anyhow::{Context, Result};
use loro_fractional_index::FractionalIndex;

/// Maximum length for sort_key before triggering rebalancing
pub const MAX_SORT_KEY_LENGTH: usize = 32;

/// Generate a fractional index between two optional keys
///
/// # Arguments
/// * `prev_key` - The sort_key of the predecessor block (None if inserting at beginning)
/// * `next_key` - The sort_key of the successor block (None if inserting at end)
///
/// # Returns
/// A new sort_key that sorts between prev_key and next_key
pub fn gen_key_between(prev_key: Option<&str>, next_key: Option<&str>) -> Result<String> {
    let prev_index = prev_key
        .map(FractionalIndex::from_hex_string)
        .map(|idx| idx);

    let next_index = next_key
        .map(FractionalIndex::from_hex_string)
        .map(|idx| idx);

    let new_index = FractionalIndex::new(prev_index.as_ref(), next_index.as_ref())
        .context("Failed to generate fractional index between given keys")?;

    Ok(new_index.to_string())
}

/// Generate N evenly-spaced fractional index keys
///
/// Used for rebalancing siblings to create uniform spacing.
///
/// # Arguments
/// * `count` - Total number of keys to generate
///
/// # Returns
/// A vector of evenly-spaced sort_keys
pub fn gen_n_keys(count: usize) -> Result<Vec<String>> {
    let indices = FractionalIndex::generate_n_evenly(None, None, count)
        .context("Failed to generate evenly-spaced fractional indices")?;

    Ok(indices.into_iter().map(|idx| idx.to_string()).collect())
}
