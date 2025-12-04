//! Operation Log entity for undo/redo and offline sync.
//!
//! The `OperationLogEntry` entity stores executed operations with their inverses,
//! enabling persistent undo/redo functionality and future offline sync support.

use holon_macros::Entity;
use serde::{Deserialize, Serialize};

use holon_api::Operation;

/// Status of an operation in the log.
///
/// Used for tracking undo/redo state and future sync status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationStatus {
    /// Operation is pending sync to external system (future use)
    PendingSync,
    /// Operation has been synced to external system (future use)
    Synced,
    /// Operation was undone
    Undone,
    /// Operation was undone before sync completed (future use)
    Cancelled,
}

impl OperationStatus {
    /// Convert status to string for database storage
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationStatus::PendingSync => "pending_sync",
            OperationStatus::Synced => "synced",
            OperationStatus::Undone => "undone",
            OperationStatus::Cancelled => "cancelled",
        }
    }

    /// Parse status from database string
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending_sync" => Some(OperationStatus::PendingSync),
            "synced" => Some(OperationStatus::Synced),
            "undone" => Some(OperationStatus::Undone),
            "cancelled" => Some(OperationStatus::Cancelled),
            _ => None,
        }
    }
}

impl std::fmt::Display for OperationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A logged operation with undo/redo and sync metadata.
///
/// Each executed operation is logged with its inverse operation,
/// enabling undo functionality. The status tracks whether the
/// operation is active, undone, or synced.
///
/// Table name: `operations`
#[derive(Debug, Clone, Serialize, Deserialize, Entity)]
#[entity(name = "operations", short_name = "op")]
pub struct OperationLogEntry {
    /// Primary key (auto-incremented)
    #[primary_key]
    pub id: i64,

    /// The operation that was executed (serialized as JSON)
    pub operation: String,

    /// The inverse operation for undo (serialized as JSON, None if not undoable)
    pub inverse: Option<String>,

    /// Current status of this operation (stored as TEXT)
    pub status: String,

    /// When the operation was executed (Unix timestamp in milliseconds)
    #[indexed]
    pub created_at: i64,

    /// Display name (denormalized from operation for efficient queries)
    pub display_name: String,

    /// Entity name (denormalized from operation for efficient queries)
    #[indexed]
    pub entity_name: String,

    /// Operation name (denormalized from operation for efficient queries)
    pub op_name: String,
}

impl OperationLogEntry {
    /// Create a new operation log entry
    pub fn new(operation: Operation, inverse: Option<Operation>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id: 0, // Will be set by database
            display_name: operation.display_name.clone(),
            entity_name: operation.entity_name.clone(),
            op_name: operation.op_name.clone(),
            operation: serde_json::to_string(&operation).unwrap_or_default(),
            inverse: inverse.map(|inv| serde_json::to_string(&inv).unwrap_or_default()),
            status: OperationStatus::PendingSync.as_str().to_string(),
            created_at: now,
        }
    }

    /// Get the operation struct
    pub fn get_operation(&self) -> Option<Operation> {
        serde_json::from_str(&self.operation).ok()
    }

    /// Get the inverse operation struct
    pub fn get_inverse(&self) -> Option<Operation> {
        self.inverse
            .as_ref()
            .and_then(|inv| serde_json::from_str(inv).ok())
    }

    /// Get the operation status
    pub fn get_status(&self) -> Option<OperationStatus> {
        OperationStatus::from_str(&self.status)
    }

    /// Check if this operation can be undone
    pub fn can_undo(&self) -> bool {
        self.inverse.is_some()
            && matches!(
                self.get_status(),
                Some(OperationStatus::PendingSync) | Some(OperationStatus::Synced)
            )
    }

    /// Check if this operation can be redone (was previously undone)
    pub fn can_redo(&self) -> bool {
        matches!(self.get_status(), Some(OperationStatus::Undone))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use holon_api::Value;
    use std::collections::HashMap;

    #[test]
    fn test_operation_status_roundtrip() {
        for status in [
            OperationStatus::PendingSync,
            OperationStatus::Synced,
            OperationStatus::Undone,
            OperationStatus::Cancelled,
        ] {
            let s = status.as_str();
            let parsed = OperationStatus::from_str(s).unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_operation_log_entry_creation() {
        let operation = Operation::new(
            "todoist-task",
            "set_completion",
            "Mark as complete",
            HashMap::from([
                ("id".to_string(), Value::String("123".to_string())),
                ("completed".to_string(), Value::Boolean(true)),
            ]),
        );

        let inverse = Operation::new(
            "todoist-task",
            "set_completion",
            "Mark as incomplete",
            HashMap::from([
                ("id".to_string(), Value::String("123".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        );

        let entry = OperationLogEntry::new(operation.clone(), Some(inverse.clone()));

        assert_eq!(entry.display_name, "Mark as complete");
        assert_eq!(entry.entity_name, "todoist-task");
        assert_eq!(entry.op_name, "set_completion");
        assert_eq!(entry.status, "pending_sync");
        assert!(entry.can_undo());
        assert!(!entry.can_redo());

        // Test deserialization
        let op = entry.get_operation().unwrap();
        assert_eq!(op.display_name, "Mark as complete");

        let inv = entry.get_inverse().unwrap();
        assert_eq!(inv.display_name, "Mark as incomplete");
    }
}
