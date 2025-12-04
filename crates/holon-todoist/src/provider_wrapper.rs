//! OperationProvider wrapper for TodoistTaskDataSource
//!
//! This wrapper implements OperationProvider for TodoistTaskDataSource, enabling
//! generic property-based testing using GenericProviderState.

use crate::models::TodoistTask;
use crate::todoist_datasource::TodoistTaskDataSource;
use async_trait::async_trait;
use holon::core::datasource::{
    CrudOperations, Operation, OperationDescriptor, OperationProvider, OperationRegistry, Result,
    UndoAction, UnknownOperationError, __operations_crud_operation_provider,
    __operations_mutable_block_data_source, __operations_mutable_task_data_source,
};
use holon::storage::types::StorageEntity;
use std::sync::Arc;
use tracing::{debug, error, info};

/// OperationProvider wrapper for TodoistTaskDataSource
///
/// This wrapper enables TodoistTaskDataSource to be used with GenericProviderState
/// for property-based testing. It delegates to the generated dispatch_operation
/// functions from the #[operations_trait] macro.
pub struct TodoistOperationProvider {
    datasource: Arc<TodoistTaskDataSource>,
    /// Store the last created entity ID (for GenericProviderState to retrieve)
    last_created_id: Arc<std::sync::Mutex<Option<String>>>,
}

impl TodoistOperationProvider {
    /// Create a new TodoistOperationProvider wrapping the given datasource
    pub fn new(datasource: Arc<TodoistTaskDataSource>) -> Self {
        Self {
            datasource,
            last_created_id: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Get a reference to the underlying datasource
    pub fn datasource(&self) -> &Arc<TodoistTaskDataSource> {
        &self.datasource
    }

    /// Get the last created entity ID (for GenericProviderState)
    pub fn get_last_created_id(&self) -> Option<String> {
        self.last_created_id.lock().unwrap().take()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OperationProvider for TodoistOperationProvider {
    fn operations(&self) -> Vec<OperationDescriptor> {
        // Delegate to shared function with proper param_mappings
        crate::todoist_datasource::operations_with_param_mappings()
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<UndoAction> {
        use tracing::{debug, info};

        info!(
            "[TodoistOperationProvider] execute_operation: entity={}, op={}, params={:?}",
            entity_name, op_name, params
        );

        // Validate entity name
        if entity_name != "todoist_tasks" {
            error!(
                "[TodoistOperationProvider] Entity name mismatch: expected 'todoist_tasks', got '{}'",
                entity_name
            );
            return Err(format!(
                "Expected entity_name 'todoist_tasks', got '{}'",
                entity_name
            )
            .into());
        }

        // Special handling for create operation - need to extract the returned ID
        // The dispatch_operation functions map Result<(String, Option<Operation>)> -> Result<Option<Operation>>, so we
        // need to call create() directly to get the ID for state tracking
        if op_name == "create" {
            info!("[TodoistOperationProvider] Executing create operation");
            // Call create() directly to get the ID
            // Dereference Arc to get &TodoistTaskDataSource, then call trait method
            let (id, inverse) = <TodoistTaskDataSource as CrudOperations<TodoistTask>>::create(
                self.datasource.as_ref(),
                params,
            )
            .await?;
            // Store the ID for GenericProviderState to retrieve
            *self.last_created_id.lock().unwrap() = Some(id.clone());
            info!(
                "[TodoistOperationProvider] Create operation succeeded: id={}",
                id
            );
            // Return the inverse operation (if any) with entity_name set
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
        debug!("[TodoistOperationProvider] Trying CrudOperations dispatch");
        match __operations_crud_operation_provider::dispatch_operation(
            self.datasource.as_ref(),
            op_name,
            &params,
        )
        .await
        {
            Ok(inverse) => {
                info!(
                    "[TodoistOperationProvider] Operation succeeded via CrudOperations: op={}",
                    op_name
                );
                // Set entity_name on the inverse operation if present
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
                    error!(
                        "[TodoistOperationProvider] CrudOperations dispatch failed: op={}, error={}",
                        op_name, err
                    );
                    return Err(err);
                }
            }
        }

        debug!("[TodoistOperationProvider] Trying BlockOperations dispatch");
        match __operations_mutable_block_data_source::dispatch_operation(
            self.datasource.as_ref(),
            op_name,
            &params,
        )
        .await
        {
            Ok(inverse) => {
                info!(
                    "[TodoistOperationProvider] Operation succeeded via BlockOperations: op={}",
                    op_name
                );
                // Set entity_name on the inverse operation if present
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
                    error!(
                        "[TodoistOperationProvider] BlockOperations dispatch failed: op={}, error={}",
                        op_name, err
                    );
                    return Err(err);
                }
            }
        }

        debug!("[TodoistOperationProvider] Trying TaskOperations dispatch");
        let result = __operations_mutable_task_data_source::dispatch_operation(
            self.datasource.as_ref(),
            op_name,
            &params,
        )
        .await;

        match &result {
            Ok(_) => {
                info!(
                    "[TodoistOperationProvider] Operation succeeded via TaskOperations: op={}",
                    op_name
                );
            }
            Err(e) => {
                error!(
                    "[TodoistOperationProvider] All dispatch attempts failed: op={}, error={}",
                    op_name, e
                );
            }
        }

        // Set entity_name on the inverse operation if present
        result.map(|inverse| match inverse {
            UndoAction::Undo(mut op) => {
                op.entity_name = entity_name.to_string();
                UndoAction::Undo(op)
            }
            UndoAction::Irreversible => UndoAction::Irreversible,
        })
    }

    fn get_last_created_id(&self) -> Option<String> {
        // Call the struct method, not the trait method (to avoid infinite recursion)
        TodoistOperationProvider::get_last_created_id(self)
    }
}
