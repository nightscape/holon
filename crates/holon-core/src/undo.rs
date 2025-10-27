//! Undo/Redo functionality for operations
//!
//! This module provides types and structures for implementing undo/redo
//! functionality through inverse operations.

use holon_api::Operation;

/// Undo/redo history stack
///
/// Maintains two stacks:
/// - `undo`: (original_operation, inverse_operation) pairs for operations that can be undone
/// - `redo`: (inverse_operation, new_inverse) pairs for operations that were undone and can be redone
pub struct UndoStack {
    /// Stack of (original, inverse) operation pairs for undo
    undo: Vec<(Operation, Operation)>,
    /// Stack of (inverse, new_inverse) operation pairs for redo
    redo: Vec<(Operation, Operation)>,
    /// Maximum number of operations to keep in undo stack
    max_size: usize,
}

impl UndoStack {
    /// Create a new undo stack with default max size
    pub fn new() -> Self {
        Self::with_max_size(100)
    }

    /// Create a new undo stack with specified max size
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            undo: Vec::new(),
            redo: Vec::new(),
            max_size,
        }
    }

    /// Push an operation pair to the undo stack
    ///
    /// When a new operation is executed, push (original, inverse) to undo stack
    /// and clear the redo stack.
    pub fn push(&mut self, original: Operation, inverse: Operation) {
        // Clear redo stack when new operation is executed
        self.redo.clear();

        // Add to undo stack
        self.undo.push((original, inverse));

        // Trim if over max size
        if self.undo.len() > self.max_size {
            self.undo.remove(0);
        }
    }

    /// Pop an operation pair from undo stack for undo operation
    ///
    /// Returns the inverse operation that should be executed to undo.
    /// Moves the pair to redo stack.
    pub fn pop_for_undo(&mut self) -> Option<Operation> {
        let (original, inverse) = self.undo.pop()?;
        // Move to redo stack (will be updated with new inverse after execution)
        self.redo.push((inverse.clone(), original));
        Some(inverse)
    }

    /// Pop an operation pair from redo stack for redo operation
    ///
    /// Returns the operation that should be executed to redo.
    /// Moves the pair back to undo stack.
    pub fn pop_for_redo(&mut self) -> Option<Operation> {
        let (inverse, new_inverse) = self.redo.pop()?;
        // Move back to undo stack (will be updated with new inverse after execution)
        self.undo.push((inverse.clone(), new_inverse.clone()));
        Some(new_inverse)
    }

    /// Check if undo is available
    pub fn can_undo(&self) -> bool {
        !self.undo.is_empty()
    }

    /// Check if redo is available
    pub fn can_redo(&self) -> bool {
        !self.redo.is_empty()
    }

    /// Clear the redo stack (called when new operation is executed)
    pub fn clear_redo(&mut self) {
        self.redo.clear();
    }

    /// Get the display name of the next undo operation (for UI)
    pub fn next_undo_display_name(&self) -> Option<&str> {
        self.undo
            .last()
            .map(|(_, inverse)| inverse.display_name.as_str())
    }

    /// Get the display name of the next redo operation (for UI)
    pub fn next_redo_display_name(&self) -> Option<&str> {
        self.redo
            .last()
            .map(|(_, new_inverse)| new_inverse.display_name.as_str())
    }

    /// Update the top of the redo stack with a new inverse operation
    ///
    /// Called after executing an undo operation to update the redo stack
    /// with the new inverse operation returned from execution.
    pub fn update_redo_top(&mut self, new_inverse: Operation) {
        if let Some((inverse, _original)) = self.redo.last_mut() {
            // Update the second element (new_inverse) with the new inverse from execution
            *self.redo.last_mut().unwrap() = (inverse.clone(), new_inverse);
        }
    }

    /// Update the top of the undo stack with a new inverse operation
    ///
    /// Called after executing a redo operation to update the undo stack
    /// with the new inverse operation returned from execution.
    pub fn update_undo_top(&mut self, new_inverse: Operation) {
        if let Some((_original, inverse)) = self.undo.last_mut() {
            // Update the second element (inverse) with the new inverse from execution
            *inverse = new_inverse;
        }
    }
}

impl Default for UndoStack {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use holon_api::Value;
    use std::collections::HashMap;

    fn make_op(name: &str) -> Operation {
        Operation::new(
            "test",
            name,
            name,
            HashMap::from([("id".to_string(), Value::String("1".to_string()))]),
        )
    }

    #[test]
    fn empty_stack() {
        let stack = UndoStack::new();
        assert!(!stack.can_undo());
        assert!(!stack.can_redo());
        assert!(stack.next_undo_display_name().is_none());
        assert!(stack.next_redo_display_name().is_none());
    }

    #[test]
    fn push_and_undo() {
        let mut stack = UndoStack::new();
        let op = make_op("set_field");
        let inv = make_op("set_field_inv");

        stack.push(op, inv.clone());
        assert!(stack.can_undo());
        assert!(!stack.can_redo());

        let undone = stack.pop_for_undo().unwrap();
        assert_eq!(undone.op_name, inv.op_name);
        assert!(!stack.can_undo());
        assert!(stack.can_redo());
    }

    #[test]
    fn undo_then_redo() {
        let mut stack = UndoStack::new();
        let op = make_op("create");
        let inv = make_op("delete");

        stack.push(op.clone(), inv.clone());
        let _undone = stack.pop_for_undo().unwrap();

        let redone = stack.pop_for_redo().unwrap();
        assert_eq!(redone.op_name, op.op_name);
        assert!(stack.can_undo());
        assert!(!stack.can_redo());
    }

    #[test]
    fn new_op_clears_redo_stack() {
        let mut stack = UndoStack::new();
        stack.push(make_op("op1"), make_op("inv1"));
        stack.push(make_op("op2"), make_op("inv2"));

        stack.pop_for_undo().unwrap();
        assert!(stack.can_redo());

        // New operation should clear redo
        stack.push(make_op("op3"), make_op("inv3"));
        assert!(!stack.can_redo());
        assert!(stack.can_undo());
    }

    #[test]
    fn trim_at_max_size() {
        let mut stack = UndoStack::with_max_size(3);

        stack.push(make_op("op1"), make_op("inv1"));
        stack.push(make_op("op2"), make_op("inv2"));
        stack.push(make_op("op3"), make_op("inv3"));
        stack.push(make_op("op4"), make_op("inv4"));

        // Should have trimmed the oldest, so undoing 3 times should exhaust the stack
        assert!(stack.pop_for_undo().is_some()); // op4
        assert!(stack.pop_for_undo().is_some()); // op3
        assert!(stack.pop_for_undo().is_some()); // op2
        assert!(stack.pop_for_undo().is_none()); // op1 was trimmed
    }

    #[test]
    fn multiple_undo_redo_cycles() {
        let mut stack = UndoStack::new();
        stack.push(make_op("a"), make_op("a_inv"));
        stack.push(make_op("b"), make_op("b_inv"));

        // Undo b, then undo a
        let inv_b = stack.pop_for_undo().unwrap();
        assert_eq!(inv_b.op_name, "b_inv");
        let inv_a = stack.pop_for_undo().unwrap();
        assert_eq!(inv_a.op_name, "a_inv");

        assert!(!stack.can_undo());
        assert!(stack.can_redo());

        // Redo a, then redo b
        let redo_a = stack.pop_for_redo().unwrap();
        assert_eq!(redo_a.op_name, "a");
        let redo_b = stack.pop_for_redo().unwrap();
        assert_eq!(redo_b.op_name, "b");

        assert!(stack.can_undo());
        assert!(!stack.can_redo());
    }

    #[test]
    fn display_names() {
        let mut stack = UndoStack::new();
        stack.push(make_op("create"), make_op("delete"));
        stack.push(make_op("update"), make_op("revert"));

        assert_eq!(stack.next_undo_display_name(), Some("revert"));

        stack.pop_for_undo().unwrap();
        assert_eq!(stack.next_undo_display_name(), Some("delete"));
        assert_eq!(stack.next_redo_display_name(), Some("update"));
    }

    #[test]
    fn update_redo_top_after_undo() {
        let mut stack = UndoStack::new();
        stack.push(make_op("op"), make_op("inv"));

        stack.pop_for_undo().unwrap();
        // After executing the inverse, we get a new inverse back
        let new_inv = make_op("new_inv");
        stack.update_redo_top(new_inv.clone());

        // Redo should return the updated inverse
        let redone = stack.pop_for_redo().unwrap();
        assert_eq!(redone.op_name, "new_inv");
    }

    #[test]
    fn update_undo_top_after_redo() {
        let mut stack = UndoStack::new();
        stack.push(make_op("op"), make_op("inv"));
        stack.pop_for_undo().unwrap();
        stack.pop_for_redo().unwrap();

        // After redo execution, update the inverse
        let updated_inv = make_op("updated_inv");
        stack.update_undo_top(updated_inv.clone());

        // Undo should return the updated inverse
        let undone = stack.pop_for_undo().unwrap();
        assert_eq!(undone.op_name, "updated_inv");
    }
}
