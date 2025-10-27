//! Demonstration of the hybrid operations() approach
//!
//! This module shows how operations metadata is automatically available through:
//! 1. CrudOperations trait's default operations() method (delegates to T::all_operations())
//! 2. QueryableCache's blanket impl of CacheOperations (delegates to T::all_operations())
//! 3. Manual OperationRegistry impl on TodoistTask (one-time, simple aggregation)

#[cfg(test)]
mod tests {
    use crate::models::TodoistTask;
    use holon::core::datasource::OperationRegistry;

    #[tokio::test]
    async fn test_operations_on_entity_type() {
        // Entity types expose operations via OperationRegistry
        let ops = TodoistTask::all_operations();

        // Should have operations from all four traits:
        // - CrudOperations: set_field, create, delete (3 ops)
        // - BlockOperations: indent, move_block, outdent (3 ops)
        // - TaskOperations: set_state, set_priority, set_due_date (3 ops)
        // - TodoistMoveOperations: move_to_project, move_under_task (2 ops)
        // Note: OperationProvider also adds "sync" operation (1 op)
        assert_eq!(ops.len(), 12, "TodoistTask should have 12 operations total");

        // Check for presence of operations from each trait
        let op_names: Vec<String> = ops.iter().map(|op| op.name.clone()).collect();

        // CrudOperations operations
        assert!(op_names.contains(&"set_field".to_string()));
        assert!(op_names.contains(&"create".to_string()));
        assert!(op_names.contains(&"delete".to_string()));

        // BlockOperations operations
        assert!(op_names.contains(&"indent".to_string()));
        assert!(op_names.contains(&"move_block".to_string()));
        assert!(op_names.contains(&"outdent".to_string()));

        // TaskOperations operations
        assert!(op_names.contains(&"set_state".to_string()));
        assert!(op_names.contains(&"set_priority".to_string()));
        assert!(op_names.contains(&"set_due_date".to_string()));
    }

    #[test]
    fn test_operation_descriptors_have_metadata() {
        // Verify that generated operations have rich metadata
        let ops = TodoistTask::all_operations();

        for op in &ops {
            // Every operation should have a name
            assert!(!op.name.is_empty(), "Operation should have non-empty name");

            // Parameters should have type information
            for param in &op.required_params {
                assert!(!param.name.is_empty(), "Param should have name");
                // TypeHint is an enum, not a string, so we check it exists
                match &param.type_hint {
                    holon_prql_render::TypeHint::Bool
                    | holon_prql_render::TypeHint::String
                    | holon_prql_render::TypeHint::Number
                    | holon_prql_render::TypeHint::EntityId { .. }
                    | holon_prql_render::TypeHint::OneOf { .. } => {}
                }
            }
        }

        // Check a specific operation for correctness
        let indent_op = ops.iter().find(|op| op.name == "indent").unwrap();
        assert_eq!(indent_op.required_params.len(), 2); // id and parent_id
        assert_eq!(indent_op.required_params[0].name, "id");
        // id could be String or EntityId - both are valid
        match &indent_op.required_params[0].type_hint {
            holon_prql_render::TypeHint::String | holon_prql_render::TypeHint::EntityId { .. } => {}
            _ => panic!(
                "Expected String or EntityId type hint for id param, got {:?}",
                indent_op.required_params[0].type_hint
            ),
        }
        assert_eq!(indent_op.required_params[1].name, "parent_id");
        // parent_id could be String or EntityId - both are valid
        match &indent_op.required_params[1].type_hint {
            holon_prql_render::TypeHint::String | holon_prql_render::TypeHint::EntityId { .. } => {}
            _ => panic!(
                "Expected String or EntityId type hint for parent_id param, got {:?}",
                indent_op.required_params[1].type_hint
            ),
        }
    }
}
