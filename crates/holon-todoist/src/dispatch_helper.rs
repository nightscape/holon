//! Shared helpers for OperationProvider dispatch logic
//!
//! This module provides utilities to reduce duplication between
//! `fake_wrapper.rs` and `provider_wrapper.rs`.

use holon::core::datasource::UndoAction;

/// Transform an UndoAction by setting the entity_name field
pub fn transform_undo_action(action: UndoAction, entity_name: &str) -> UndoAction {
    match action {
        UndoAction::Undo(mut op) => {
            op.entity_name = entity_name.to_string();
            UndoAction::Undo(op)
        }
        UndoAction::Irreversible => UndoAction::Irreversible,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use holon::core::datasource::Operation;
    use holon::storage::types::StorageEntity;

    #[test]
    fn test_transform_undo_action_with_undo() {
        let op = Operation {
            entity_name: "".to_string(),
            operation_name: "delete".to_string(),
            params: StorageEntity::new(),
        };
        let action = UndoAction::Undo(op);
        let result = transform_undo_action(action, "todoist_tasks");

        if let UndoAction::Undo(op) = result {
            assert_eq!(op.entity_name, "todoist_tasks");
        } else {
            panic!("Expected Undo variant");
        }
    }

    #[test]
    fn test_transform_undo_action_irreversible() {
        let action = UndoAction::Irreversible;
        let result = transform_undo_action(action, "todoist_tasks");
        assert!(matches!(result, UndoAction::Irreversible));
    }
}
