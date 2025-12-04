//! Core traits for Holon datasources
//!
//! This crate provides the core traits for datasource operations:
//! - `CrudOperations`: Basic CRUD operations (create, update, delete)
//! - `BlockOperations`: Block-specific operations (indent, outdent, move_block, etc.)
//! - `TaskOperations`: Task-specific operations (set_completion, set_priority, set_due_date)

pub mod core;
pub mod fractional_index;
pub mod operation_log;
pub mod storage;
pub mod traits;
pub mod undo;

pub use operation_log::{OperationLogEntry, OperationStatus};
pub use traits::{
    BlockDataSourceHelpers, BlockEntity, BlockOperations, CrudOperations, DataSource,
    MaybeSendSync, MoveOperations, OperationLogOperations, OperationRegistry, RenameOperations,
    Result, TaskEntity, TaskOperations, UndoAction, UnknownOperationError,
};
pub use undo::UndoStack;

// Re-export macro-generated operation dispatch functions
#[cfg(not(target_arch = "wasm32"))]
pub use traits::{
    __operations_block_operations, __operations_crud_operations, __operations_move_operations,
    __operations_rename_operations, __operations_task_operations,
};
