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
