//! OperationProvider wrapper for TodoistTaskFake
//!
//! This wrapper implements OperationProvider for TodoistTaskFake, enabling
//! generic property-based testing using GenericProviderState.

use crate::fake::TodoistTaskFake;
use crate::models::TodoistTask;
use async_trait::async_trait;
use holon::core::datasource::{
    CrudOperations, Operation, OperationDescriptor, OperationProvider, OperationRegistry, Result,
    UndoAction, UnknownOperationError, __operations_crud_operation_provider,
    __operations_mutable_block_data_source, __operations_mutable_task_data_source,
};
use holon::storage::types::StorageEntity;
use std::sync::Arc;

/// OperationProvider wrapper for TodoistTaskFake
///
/// This wrapper enables TodoistTaskFake to be used with GenericProviderState
/// for property-based testing. It delegates to the generated dispatch_operation
/// functions from the #[operations_trait] macro.
pub struct TodoistFakeOperationProvider {
    fake: Arc<TodoistTaskFake>,
    /// Store the last created entity ID (for GenericProviderState to retrieve)
    last_created_id: Arc<std::sync::Mutex<Option<String>>>,
}

impl TodoistFakeOperationProvider {
    /// Create a new TodoistFakeOperationProvider wrapping the given fake datasource
    pub fn new(fake: Arc<TodoistTaskFake>) -> Self {
        Self {
            fake,
            last_created_id: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Get a reference to the underlying fake datasource
    pub fn fake(&self) -> &Arc<TodoistTaskFake> {
        &self.fake
    }

    /// Get the last created entity ID (for GenericProviderState)
    pub fn get_last_created_id(&self) -> Option<String> {
        self.last_created_id.lock().unwrap().take()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OperationProvider for TodoistFakeOperationProvider {
    fn operations(&self) -> Vec<OperationDescriptor> {
        // Get base operations and add param_mappings for move_block
        crate::todoist_datasource::operations_with_param_mappings()
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<UndoAction> {
        // Validate entity name
        if entity_name != "todoist_tasks" {
            return Err(format!(
                "Expected entity_name 'todoist_tasks', got '{}'",
                entity_name
            )
            .into());
        }

        // Special handling for create operation - need to extract the returned ID
        // The dispatch_operation functions now return Result<UndoAction>, so we
        // need to call create() directly to get the ID for state tracking
        if op_name == "create" {
            // Call create() directly to get the ID
            // Dereference Arc to get &TodoistTaskFake, then call trait method
            let (id, inverse) = <TodoistTaskFake as CrudOperations<TodoistTask>>::create(
                self.fake.as_ref(),
                params,
            )
            .await?;
            // Store the ID for GenericProviderState to retrieve
            *self.last_created_id.lock().unwrap() = Some(id.clone());
            // Return inverse operation with entity_name set
            return Ok(match inverse {
                UndoAction::Undo(mut op) => {
                    op.entity_name = entity_name.to_string();
                    UndoAction::Undo(op)
                }
                UndoAction::Irreversible => UndoAction::Irreversible,
            });
        }

        // Try dispatching to each trait module in order
        // The first one that succeeds wins
        match __operations_crud_operation_provider::dispatch_operation(
            self.fake.as_ref(),
            op_name,
            &params,
        )
        .await
        {
            Ok(inverse) => {
                return Ok(match inverse {
                    UndoAction::Undo(mut op) => {
                        op.entity_name = entity_name.to_string();
                        UndoAction::Undo(op)
                    }
                    UndoAction::Irreversible => UndoAction::Irreversible,
                });
            }
            Err(err) => {
                if !UnknownOperationError::is_unknown(err.as_ref()) {
                    return Err(err);
                }
            }
        }

        match __operations_mutable_block_data_source::dispatch_operation(
            self.fake.as_ref(),
            op_name,
            &params,
        )
        .await
        {
            Ok(inverse) => {
                return Ok(match inverse {
                    UndoAction::Undo(mut op) => {
                        op.entity_name = entity_name.to_string();
                        UndoAction::Undo(op)
                    }
                    UndoAction::Irreversible => UndoAction::Irreversible,
                });
            }
            Err(err) => {
                if !UnknownOperationError::is_unknown(err.as_ref()) {
                    return Err(err);
                }
            }
        }

        let result = __operations_mutable_task_data_source::dispatch_operation(
            self.fake.as_ref(),
            op_name,
            &params,
        )
        .await?;
        Ok(match result {
            UndoAction::Undo(mut op) => {
                op.entity_name = entity_name.to_string();
                UndoAction::Undo(op)
            }
            UndoAction::Irreversible => UndoAction::Irreversible,
        })
    }

    fn get_last_created_id(&self) -> Option<String> {
        // Call the struct method, not the trait method (to avoid infinite recursion)
        TodoistFakeOperationProvider::get_last_created_id(self)
    }
}
