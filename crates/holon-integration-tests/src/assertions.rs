//! Assertion helpers for block comparison

use holon::sync::document_entity::is_document_uri;
use holon_api::block::Block;
use std::collections::HashMap;

use crate::org_utils::INTERNAL_PROPS;

/// Normalize a block for comparison by zeroing out timestamps and trimming content.
///
/// Document URIs in parent_id are normalized to a canonical form so that
/// file-based URIs (holon-doc://test.org) and UUID-based URIs (holon-doc://{uuid})
/// for the same document compare equal.
pub fn normalize_block(block: &Block) -> Block {
    let mut normalized = block.clone();
    normalized.created_at = 0;
    normalized.updated_at = 0;
    normalized.content = normalized.content.trim().to_string();
    if is_document_uri(&normalized.parent_id) {
        normalized.parent_id = "__document_root__".to_string();
    }
    for prop in INTERNAL_PROPS {
        normalized.properties.remove(*prop);
    }
    normalized
}

/// Assert that two Block slices are equivalent (using normalize_block)
pub fn assert_blocks_equivalent(actual_blocks: &[Block], expected_blocks: &[Block], message: &str) {
    let mut actual_sorted: Vec<_> = actual_blocks.iter().map(normalize_block).collect();
    let mut expected_sorted: Vec<_> = expected_blocks.iter().map(normalize_block).collect();
    actual_sorted.sort_by(|a, b| a.id.cmp(&b.id));
    expected_sorted.sort_by(|a, b| a.id.cmp(&b.id));

    assert_eq!(
        actual_sorted, expected_sorted,
        "{}: Blocks differ between actual and expected",
        message
    );
}

/// Check if a block belongs to a specific document (directly or through ancestors)
pub fn block_belongs_to_document(block: &Block, all_blocks: &[Block], doc_uri: &str) -> bool {
    if block.parent_id == doc_uri {
        return true;
    }
    if let Some(parent) = all_blocks.iter().find(|b| b.id == block.parent_id) {
        return block_belongs_to_document(parent, all_blocks, doc_uri);
    }
    false
}

/// Reference state for tracking blocks (used by find_document_for_block)
pub struct ReferenceState {
    pub blocks: HashMap<String, Block>,
}

/// Find the document URI that a block belongs to
pub fn find_document_for_block(block_id: &str, ref_state: &ReferenceState) -> Option<String> {
    let block = ref_state.blocks.get(block_id)?;

    if is_document_uri(&block.parent_id) {
        return Some(block.parent_id.clone());
    }

    find_document_for_block(&block.parent_id, ref_state)
}
