//! Unit tests for API types
//!
//! These tests verify that API types can be properly serialized/deserialized
//! and implement the required traits.

use super::types::*;
use holon_api::{ApiError, Block, BlockContent, BlockMetadata, Change, ChangeOrigin};

#[test]
fn test_block_serialization() {
    let mut block = Block::new_text("local://test-123", "test-parent", "Test block");
    block.created_at = 1000;
    block.updated_at = 2000;

    // Serialize to JSON
    let json = serde_json::to_string(&block).expect("Failed to serialize");

    // Deserialize back
    let deserialized: Block = serde_json::from_str(&json).expect("Failed to deserialize");

    assert_eq!(block, deserialized);
}

#[test]
fn test_api_error_serialization() {
    let errors = vec![
        ApiError::BlockNotFound {
            id: "test-id".to_string(),
        },
        ApiError::DocumentNotFound {
            doc_id: "doc-123".to_string(),
        },
        ApiError::CyclicMove {
            id: "block-1".to_string(),
            target_parent: "block-2".to_string(),
        },
        ApiError::InvalidOperation {
            message: "Test error".to_string(),
        },
        ApiError::NetworkError {
            message: "Connection failed".to_string(),
        },
        ApiError::InternalError {
            message: "Internal error".to_string(),
        },
    ];

    for error in errors {
        let json = serde_json::to_string(&error).expect("Failed to serialize error");
        let deserialized: ApiError =
            serde_json::from_str(&json).expect("Failed to deserialize error");

        // Check error message matches
        assert_eq!(error.to_string(), deserialized.to_string());
    }
}

#[test]
fn test_new_block_serialization() {
    let new_block = NewBlock {
        parent_id: "parent-123".to_string(),
        content: BlockContent::text("New block content"),
        after: Some("sibling-456".to_string()),
        id: Some("todoist://task/789".to_string()),
    };

    let json = serde_json::to_string(&new_block).expect("Failed to serialize");
    let deserialized: NewBlock = serde_json::from_str(&json).expect("Failed to deserialize");

    assert_eq!(new_block.parent_id, deserialized.parent_id);
    assert_eq!(new_block.content, deserialized.content);
    assert_eq!(new_block.after, deserialized.after);
    assert_eq!(new_block.id, deserialized.id);
}

#[test]
fn test_block_change_serialization() {
    let changes = vec![
        Change::Created {
            data: Block::new_text("new-block", "test-parent", "Content"),
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        },
        Change::Updated {
            id: "block-1".to_string(),
            data: Block::new_text("block-1", "test-parent", "Updated content"),
            origin: ChangeOrigin::Remote {
                operation_id: None,
                trace_id: None,
            },
        },
        Change::Deleted {
            id: "block-2".to_string(),
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        },
    ];

    for change in changes {
        let json = serde_json::to_string(&change).expect("Failed to serialize change");
        let _deserialized: Change<Block> =
            serde_json::from_str(&json).expect("Failed to deserialize change");
        // Successfully round-tripped
    }
}

#[test]
fn test_change_origin_clone() {
    // Verify ChangeOrigin can be cloned (no longer Copy due to String field)
    let origin = ChangeOrigin::Local {
        operation_id: None,
        trace_id: None,
    };
    let cloned = origin.clone();

    // Can still use original
    assert_eq!(origin, cloned);
}

#[test]
fn test_uri_block_ids() {
    // Test various URI formats
    let local_id = "local://550e8400-e29b-41d4-a716-446655440000";
    let todoist_id = "todoist://task/12345";
    let logseq_id = "logseq://page/abc123";

    assert!(local_id.starts_with("local://"));
    assert!(todoist_id.starts_with("todoist://"));
    assert!(logseq_id.starts_with("logseq://"));
}

#[test]
fn test_block_metadata_default() {
    let metadata = BlockMetadata::default();
    assert_eq!(metadata.created_at, 0);
    assert_eq!(metadata.updated_at, 0);
}
