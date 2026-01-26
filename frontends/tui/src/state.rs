use crate::config::KeyBindingConfig;
use holon::api::backend_engine::BackendEngine;
use holon::storage::turso::{ChangeData, RowChange};
use holon::storage::types::StorageEntity; // StorageEntity is HashMap<String, Value>
use holon_api::Value;
use holon_prql_render::RenderSpec;
use r3bl_tui::{row, DialogBuffer, EditorBuffer, FlexBoxId, HasDialogBuffers, HasEditorBuffers};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct State {
    pub engine: Arc<BackendEngine>,
    pub render_spec: RenderSpec,
    pub data: Vec<StorageEntity>, // Generic StorageEntity (no Todoist-specific types)
    pub selected_index: usize,
    pub status_message: String,
    pub cdc_receiver: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<RowChange>>>,
    /// Track the ID of the currently selected block to maintain selection after re-sorting
    pub selected_block_id_cache: Option<String>,
    /// Channel to send main thread signal sender to CDC watcher task
    pub main_thread_sender_channel: Arc<
        Mutex<
            Option<tokio::sync::mpsc::Sender<r3bl_tui::TerminalWindowMainThreadSignal<AppSignal>>>,
        >,
    >,
    /// Flag indicating there are pending CDC changes to process
    pub has_pending_cdc_changes: Arc<Mutex<bool>>,

    // r3bl framework integration - enables use of EditorComponent and DialogComponent
    pub editor_buffers: HashMap<FlexBoxId, EditorBuffer>,
    pub dialog_buffers: HashMap<FlexBoxId, DialogBuffer>,

    /// Edit mode tracking for editable_text widgets
    /// Some(block_index) means we're editing the editable_text in that block
    pub editing_block_index: Option<usize>,
    /// Editor buffer for the currently editing block (only one block can be edited at a time)
    /// Keyed by a fixed FlexBoxId since we only edit one at a time
    pub editing_buffer: Option<EditorBuffer>,

    /// Keybindings configuration
    pub keybindings: Arc<KeyBindingConfig>,
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("State")
            .field("data_len", &self.data.len())
            .field("selected_index", &self.selected_index)
            .field("status_message", &self.status_message)
            .finish()
    }
}

// Provide a minimal Default for r3bl framework compatibility
// Note: This should not be used directly - use State::new() instead
impl Default for State {
    fn default() -> Self {
        // r3bl requires Default trait, but we can't create a valid State without async context
        // This implementation will panic if actually called - State should only be created via State::new()
        panic!("State::default() should not be called - use State::new() with properly initialized BackendEngine")
    }
}

impl State {
    pub fn new(
        engine: Arc<BackendEngine>,
        render_spec: RenderSpec,
        initial_data: Vec<holon::storage::types::StorageEntity>, // Generic StorageEntity
        cdc_receiver: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<RowChange>>>,
        keybindings: Arc<KeyBindingConfig>,
    ) -> Self {
        let mut state = Self {
            engine,
            render_spec,
            data: initial_data,
            selected_index: 0,
            status_message: "Ready".to_string(),
            cdc_receiver,
            selected_block_id_cache: None,
            main_thread_sender_channel: Arc::new(Mutex::new(None)),
            has_pending_cdc_changes: Arc::new(Mutex::new(false)),
            editor_buffers: HashMap::new(),
            dialog_buffers: HashMap::new(),
            editing_block_index: None,
            editing_buffer: None,
            keybindings,
        };

        // Sort initial data hierarchically to match renderer's visual order
        state.sort_hierarchically();

        state
    }

    /// Initialize the CDC watcher task with the main thread signal sender
    pub fn start_cdc_watcher(
        &self,
        sender: tokio::sync::mpsc::Sender<r3bl_tui::TerminalWindowMainThreadSignal<AppSignal>>,
    ) {
        let mut channel_guard = self.main_thread_sender_channel.lock().unwrap();
        *channel_guard = Some(sender);
    }

    pub fn selected_block_id(&self) -> Option<String> {
        self.data
            .get(self.selected_index)
            .and_then(|row| row.get("id"))
            .and_then(|v| v.as_string())
            .map(|s| s.to_string())
    }

    pub fn select_previous(&mut self) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
        }
    }

    pub fn select_next(&mut self) {
        if self.selected_index < self.data.len().saturating_sub(1) {
            self.selected_index += 1;
        }
    }

    /// Execute an operation on the selected block
    ///
    /// Spawns the async operation in the background without blocking the UI.
    /// The operation result will be reflected through CDC updates.
    /// Errors are communicated back to the UI via AppSignal::OperationResult.
    ///
    /// Dynamically checks if the operation is available before executing.
    ///
    /// # Arguments
    /// * `op_name` - Name of the operation to execute (e.g., "indent", "outdent", "move_up")
    /// * `operation_descriptor` - Operation descriptor containing entity and table metadata
    /// * `ui_state_override` - Optional UI state to set before executing (for operations like split that need cursor position)
    pub fn execute_operation_on_selected(
        &mut self,
        op_name: &str,
        operation_descriptor: &holon_api::OperationDescriptor,
        _ui_state_override: Option<holon::api::UiState>,
    ) -> Result<(), String> {
        // Get the full row data for the selected block - operations may need more than just id
        let mut row_data = match self.data.get(self.selected_index) {
            Some(row) => row.clone(),
            None => return Err("No block selected".to_string()),
        };

        // Allow operations to inject additional parameters that are not present
        // in the selected row itself.
        match operation_descriptor.name.as_str() {
            "indent" => {
                if self.selected_index == 0 {
                    return Err("Cannot indent the first block".to_string());
                }
                let previous_row = self
                    .data
                    .get(self.selected_index - 1)
                    .ok_or_else(|| "No previous block to indent under".to_string())?;
                let new_parent_id = previous_row
                    .get("id")
                    .and_then(|v| v.as_string())
                    .ok_or_else(|| "Previous block has no id".to_string())?;
                row_data.insert(
                    "parent_id".to_string(),
                    Value::String(new_parent_id.to_string()),
                );
            }
            _ => {}
        }

        // Clone Arc for async operation
        let engine = self.engine.clone();
        let op_name_owned = op_name.to_string();

        // Get the signal sender to communicate results back to UI
        let sender_opt = self.main_thread_sender_channel.lock().unwrap().clone();

        // Cache selected block ID so selection follows the block after CDC re-sort
        self.selected_block_id_cache = self.selected_block_id();

        // Use entity_name directly from operation descriptor
        let entity_name = operation_descriptor.entity_name.clone();
        let mapped_op_name = operation_descriptor.name.clone();

        // Spawn async operation in background - does NOT block the UI
        tokio::spawn(async move {
            // Check if operation is available before executing
            let has_op = engine.has_operation(&entity_name, &mapped_op_name).await;

            if !has_op {
                // Operation not available - send error signal
                if let Some(sender) = sender_opt {
                    let signal = AppSignal::OperationResult {
                        operation_name: op_name_owned.clone(),
                        success: false,
                        error_message: Some(format!(
                            "Operation '{}' is not available",
                            op_name_owned
                        )),
                    };
                    let _ = sender
                        .send(r3bl_tui::TerminalWindowMainThreadSignal::ApplyAppSignal(
                            signal,
                        ))
                        .await;
                } else {
                    eprintln!("Operation '{}' is not available", op_name_owned);
                }
                return;
            }

            // Execute the operation with mapped name
            let result: anyhow::Result<()> = engine
                .execute_operation(&entity_name, &mapped_op_name, row_data)
                .await;

            // Send result back to UI thread via signal
            if let Some(sender) = sender_opt {
                let signal = AppSignal::OperationResult {
                    operation_name: op_name_owned.clone(),
                    success: result.is_ok(),
                    error_message: result.err().map(|e: anyhow::Error| e.to_string()),
                };

                // Send signal to main thread
                if sender
                    .send(r3bl_tui::TerminalWindowMainThreadSignal::ApplyAppSignal(
                        signal,
                    ))
                    .await
                    .is_err()
                {
                    eprintln!(
                        "Failed to send operation result signal for '{}'",
                        op_name_owned
                    );
                }
            } else {
                // Fallback: log errors if signal channel is not available
                if let Err(e) = result {
                    eprintln!("Operation '{}' failed: {}", op_name_owned, e);
                }
            }
        });

        // Return immediately - the operation runs in background
        // UI will be updated via CDC when database changes
        // Error results will be communicated via AppSignal::OperationResult
        Ok(())
    }

    /// Calculate cursor offset from the editing buffer
    ///
    /// Helper method for operations that need cursor position (like split).
    /// Returns the character offset from the start of the text.
    pub fn calculate_cursor_offset(&self) -> Result<u32, String> {
        let buffer = self.editing_buffer.as_ref().ok_or_else(|| {
            "Cannot calculate cursor offset: block is not being edited".to_string()
        })?;

        let caret = buffer.get_caret_raw();
        let caret_row = caret.row_index.as_usize();
        let caret_col = caret.col_index.as_usize();

        // Calculate character offset: sum of characters in previous lines + current column
        let lines = buffer.get_lines();
        let mut offset = 0u32;

        // Get line count by iterating until get_line_content returns None
        let mut line_count = 0usize;
        loop {
            if lines.get_line_content(row(line_count)).is_some() {
                line_count += 1;
            } else {
                break;
            }
        }

        // Add lengths of all lines before the cursor row
        for i in 0..caret_row.min(line_count) {
            if let Some(line_content) = lines.get_line_content(row(i)) {
                offset += line_content.chars().count() as u32;
                // Add 1 for the newline character (except for the last line)
                if i < line_count.saturating_sub(1) {
                    offset += 1;
                }
            }
        }

        // Add the column offset on the current line
        if let Some(line_content) = lines.get_line_content(row(caret_row)) {
            // Count graphemes up to caret_col (to handle multi-byte characters correctly)
            use unicode_segmentation::UnicodeSegmentation;
            let graphemes: Vec<&str> = line_content.graphemes(true).collect();
            offset += graphemes
                .iter()
                .take(caret_col.min(graphemes.len()))
                .map(|g| g.chars().count())
                .sum::<usize>() as u32;
        } else {
            // If no line content, just use caret_col as character offset
            offset += caret_col as u32;
        }

        Ok(offset)
    }

    /// Poll the CDC channel and apply all pending changes
    ///
    /// This is called when CDC changes are detected to keep UI in sync with database.
    /// Uses try_recv to avoid blocking - returns number of changes applied.
    pub fn poll_cdc_changes(&mut self) -> usize {
        // Collect all pending changes FIRST, before clearing the flag
        let changes = {
            let mut receiver = match self.cdc_receiver.lock() {
                Ok(r) => r,
                Err(_) => {
                    eprintln!("poll_cdc_changes: Mutex poisoned");
                    return 0;
                }
            };

            let mut changes = Vec::new();
            while let Ok(change) = receiver.try_recv() {
                changes.push(change);
            }
            changes
        }; // Lock released here

        // Only clear the flag AFTER we've checked for changes
        // This prevents race conditions where the flag is cleared before changes are processed
        if changes.is_empty() {
            if let Ok(mut flag) = self.has_pending_cdc_changes.lock() {
                *flag = false;
            }
        }

        // Apply all changes
        let count = changes.len();
        for change in changes {
            self.apply_change(change);
        }

        // CRITICAL: Re-sort data hierarchically to match visual order
        // This ensures selected_index refers to the correct visual item
        if count > 0 {
            self.sort_hierarchically();
            // Clear flag after processing changes
            if let Ok(mut flag) = self.has_pending_cdc_changes.lock() {
                *flag = false;
            }
        }

        count
    }

    /// Sort data hierarchically by [parent_id, sort_key] to match renderer's visual order
    fn sort_hierarchically(&mut self) {
        // Build parent -> children mapping
        let mut children_map: HashMap<Option<String>, Vec<HashMap<String, Value>>> = HashMap::new();

        for row in self.data.drain(..) {
            let parent_id = row
                .get("parent_id")
                .and_then(|v| v.as_string())
                .map(|s| s.to_string());

            children_map.entry(parent_id).or_default().push(row);
        }

        // Sort each parent's children by sort_key
        for children in children_map.values_mut() {
            children.sort_by(|a, b| {
                let a_sort = a.get("sort_key").and_then(|v| v.as_string()).unwrap_or("");
                let b_sort = b.get("sort_key").and_then(|v| v.as_string()).unwrap_or("");
                a_sort.cmp(b_sort)
            });
        }

        // Depth-first traversal to rebuild data in hierarchical order
        // Start with depth 0 for root-level items
        Self::collect_children_recursively(&mut self.data, &children_map, None, 0);

        // Restore selection if we have a cached block ID
        if let Some(ref block_id) = self.selected_block_id_cache {
            if let Some(new_index) = self.data.iter().position(|row| {
                row.get("id")
                    .and_then(|v| v.as_string())
                    .map(|id| id == block_id)
                    .unwrap_or(false)
            }) {
                self.selected_index = new_index;
            }
            // Clear the cache after using it
            self.selected_block_id_cache = None;
        }
    }

    /// Recursively collect children in depth-first order
    /// Computes and adds depth field to each item based on its position in the tree
    fn collect_children_recursively(
        result: &mut Vec<HashMap<String, Value>>,
        children_map: &HashMap<Option<String>, Vec<HashMap<String, Value>>>,
        parent_id: Option<String>,
        current_depth: usize,
    ) {
        if let Some(children) = children_map.get(&parent_id) {
            for child in children {
                let child_id = child
                    .get("id")
                    .and_then(|v| v.as_string())
                    .map(|s| s.to_string());

                // Clone child and add depth field
                let mut child_with_depth = child.clone();
                child_with_depth.insert("depth".to_string(), Value::Integer(current_depth as i64));

                result.push(child_with_depth);

                // Recursively collect this child's children with incremented depth
                Self::collect_children_recursively(
                    result,
                    children_map,
                    child_id,
                    current_depth + 1,
                );
            }
        }
    }

    /// Apply a single CDC change to the data vector
    fn apply_change(&mut self, change: RowChange) {
        match change.change {
            ChangeData::Created { data, .. } => {
                self.apply_insert(data);
            }
            ChangeData::Updated { data, .. } => {
                self.apply_update(data);
            }
            ChangeData::Deleted { id, .. } => {
                self.apply_delete(&id);
            }
            ChangeData::FieldsChanged {
                entity_id, fields, ..
            } => {
                // Apply partial field update
                if let Some(pos) = self.data.iter().position(|row| {
                    row.get("id")
                        .and_then(|v| v.as_string())
                        .map(|id| id == entity_id)
                        .unwrap_or(false)
                }) {
                    for (field, _old, new) in fields {
                        self.data[pos].insert(field, new);
                    }
                }
            }
        }
    }

    /// Apply an insert - just append to data, sorting happens later
    fn apply_insert(&mut self, entity: HashMap<String, Value>) {
        self.data.push(entity);
    }

    /// Apply an update - just update in place, sorting happens later
    fn apply_update(&mut self, entity: HashMap<String, Value>) {
        let entity_id = entity.get("id").and_then(|v| v.as_string());
        if entity_id.is_none() {
            return;
        }
        let entity_id = entity_id.unwrap();

        // Find and update existing entity
        if let Some(pos) = self.data.iter().position(|row| {
            row.get("id")
                .and_then(|v| v.as_string())
                .map(|id| id == entity_id)
                .unwrap_or(false)
        }) {
            self.data[pos] = entity;
        }
    }

    /// Apply a delete
    fn apply_delete(&mut self, _id: &str) {
        // Note: The id parameter from CDC is SQLite ROWID, not the entity ID
        // We would need the actual entity ID to delete properly
        // For now, this is a limitation - we may need to enhance the CDC system
        // to include the entity ID in Delete events

        // WORKAROUND: Since operations typically modify rather than delete,
        // and we don't have proper entity ID in Delete events, we'll skip this for now
        // A proper fix would be to enhance turso.rs CDC to include entity ID
    }
}

#[derive(Clone, Debug)]
pub enum AppSignal {
    Noop,
    /// Execute an operation from a UIElement
    ExecuteOperation {
        operation_name: String, // Operation name from descriptor.name
        table: String,
        id_column: String,
        id_value: String,
        field: String,
        new_value: holon_api::Value,
    },
    /// Operation result notification (success or failure)
    OperationResult {
        operation_name: String,
        success: bool,
        error_message: Option<String>,
    },
}

impl Default for AppSignal {
    fn default() -> Self {
        AppSignal::Noop
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "State {{ tasks: {}, selected: {} }}",
            self.data.len(),
            self.selected_index
        )
    }
}

// r3bl framework trait implementations - enables use of EditorComponent and DialogComponent
impl HasEditorBuffers for State {
    fn get_mut_editor_buffer(&mut self, id: FlexBoxId) -> Option<&mut EditorBuffer> {
        self.editor_buffers.get_mut(&id)
    }

    fn insert_editor_buffer(&mut self, id: FlexBoxId, buffer: EditorBuffer) {
        self.editor_buffers.insert(id, buffer);
    }

    fn contains_editor_buffer(&self, id: FlexBoxId) -> bool {
        self.editor_buffers.contains_key(&id)
    }
}

impl HasDialogBuffers for State {
    fn get_mut_dialog_buffer(&mut self, id: FlexBoxId) -> Option<&mut DialogBuffer> {
        self.dialog_buffers.get_mut(&id)
    }
}
