use crate::config::{Action, BindingContext};
use crate::render_interpreter::RenderInterpreter;
use crate::state::{AppSignal, State};
use crate::ui_element::UIElement;
use r3bl_tui::{
    engine_public_api, height, render_pipeline, row, send_signal, throws_with_return, width,
    BoxedSafeComponent, CommonResult, Component, EditorEngine, EditorEngineApplyEventResult,
    EditorEngineConfig, EditorEvent, EventPropagation, FlexBox, FlexBoxId, GlobalData, HasFocus,
    InputEvent, Key, KeyPress, KeyState, RenderPipeline, Size, SpecialKey, SurfaceBounds,
    SystemClipboard, TerminalWindowMainThreadSignal, ZOrder,
};
use tracing::debug;

// Helper function to extract field name from OperationWiring
// For "set_field" operations, tries to extract from descriptor params, otherwise uses modified_param
fn get_field_name(op: &query_render::OperationWiring) -> String {
    // For "set_field" operations, the field name is typically in modified_param
    // or we can try to extract it from the operation descriptor
    // For now, use modified_param as it often matches the database field name
    op.modified_param.clone()
}

/// Extract operation info from an element at the given index
/// Returns (operation_name, table, id_column, field) if found
fn extract_operation_info(
    element_tree: &[UIElement],
    index: usize,
) -> Option<(String, String, String, String)> {
    element_tree
        .get(index)
        .and_then(|element| element.find_editable_text())
        .and_then(|editable| {
            if let UIElement::EditableText { operations, .. } = editable {
                operations.first().map(|op| {
                    (
                        op.descriptor.name.clone(),
                        op.descriptor.entity_name.clone(), // Use entity_name instead of table
                        op.descriptor.id_column.clone(),
                        get_field_name(op),
                    )
                })
            } else {
                None
            }
        })
}

/// Extract buffer content as a string (joining lines with \n)
fn extract_buffer_content(buffer: &r3bl_tui::EditorBuffer) -> String {
    let lines = buffer.get_lines();
    let mut buffer_content = String::new();
    let mut i = 0;
    loop {
        if let Some(line) = lines.get_line_content(row(i)) {
            if !buffer_content.is_empty() {
                buffer_content.push('\n');
            }
            buffer_content.push_str(line);
            i += 1;
        } else {
            break;
        }
    }
    buffer_content
}

/// Send an operation signal to save block content
fn send_save_operation_signal(
    global_data: &mut GlobalData<State, AppSignal>,
    editing_idx: usize,
    operation_info: (String, String, String, String),
    buffer_content: String,
) {
    let (operation_name, table, id_column, field) = operation_info;
    if let Some(row_data) = global_data.state.data.get(editing_idx) {
        if let Some(id_value) = row_data.get(&id_column) {
            if let Some(id_str) = id_value.as_string() {
                let new_value = holon_api::Value::String(buffer_content);
                let op_signal = AppSignal::ExecuteOperation {
                    operation_name: operation_name.clone(),
                    table,
                    id_column,
                    id_value: id_str.to_string(),
                    field,
                    new_value,
                };
                send_signal!(
                    global_data.main_thread_channel_sender,
                    TerminalWindowMainThreadSignal::ApplyAppSignal(op_signal)
                );
                global_data.state.status_message = "Saving...".to_string();
            }
        }
    }
}

/// Exit edit mode and clear editor state
fn exit_edit_mode(
    component: &mut BlockListComponent,
    global_data: &mut GlobalData<State, AppSignal>,
) {
    global_data.state.editing_block_index = None;
    global_data.state.editing_buffer = None;
    component.editor_engine = None;
}

/// Save and exit edit mode for a block at the given index
fn save_and_exit_edit_mode(
    component: &mut BlockListComponent,
    global_data: &mut GlobalData<State, AppSignal>,
    editing_idx: usize,
) -> bool {
    if let Some(buffer) = &global_data.state.editing_buffer {
        if let Some(operation_info) = extract_operation_info(&component.element_tree, editing_idx) {
            let buffer_content = extract_buffer_content(buffer);
            exit_edit_mode(component, global_data);
            send_save_operation_signal(global_data, editing_idx, operation_info, buffer_content);
            return true;
        }
    }
    false
}

/// Ensure editor engine exists and has a valid viewport
fn ensure_editor_engine(component: &mut BlockListComponent) -> &mut EditorEngine {
    if component.editor_engine.is_none() {
        component.editor_engine = Some(EditorEngine::new(EditorEngineConfig {
            multiline_mode: r3bl_tui::LineMode::MultiLine,
            syntax_highlight: r3bl_tui::SyntaxHighlightMode::Disable,
            edit_mode: r3bl_tui::EditMode::ReadWrite,
        }));
    }
    let editor_engine = component.editor_engine.as_mut().unwrap();
    if editor_engine.viewport() == Size::default() {
        editor_engine.current_box.style_adjusted_bounds_size = width(200) + height(50);
    }
    editor_engine
}

/// Save the current editing block without exiting edit mode
/// Used when switching between blocks
fn save_current_block_without_exit(
    component: &mut BlockListComponent,
    global_data: &mut GlobalData<State, AppSignal>,
    editing_idx: usize,
) {
    if let Some(buffer) = &global_data.state.editing_buffer {
        if let Some(operation_info) = extract_operation_info(&component.element_tree, editing_idx) {
            let buffer_content = extract_buffer_content(buffer);
            send_save_operation_signal(global_data, editing_idx, operation_info, buffer_content);
        }
    }
}

/// Execute an operation and set status message
fn execute_operation_with_status(
    component: &BlockListComponent,
    global_data: &mut GlobalData<State, AppSignal>,
    op_name: &str,
    success_msg: &str,
    ui_state: Option<holon::api::UiState>,
) -> bool {
    // Get the selected UIElement
    let selected_element = match component.element_tree.get(global_data.state.selected_index) {
        Some(element) => element,
        None => {
            global_data.state.status_message = format!(
                "No element at selected index {}",
                global_data.state.selected_index
            );
            debug!(
                "No element at index {} (tree len: {})",
                global_data.state.selected_index,
                component.element_tree.len()
            );
            return false;
        }
    };

    // Debug: Log all available operations on this element
    let available_ops = BlockListComponent::collect_all_operation_names(selected_element);
    debug!(
        "Looking for operation '{}' on element at index {}",
        op_name, global_data.state.selected_index
    );
    debug!("Available operations: {:?}", available_ops);
    debug!(
        "Element type: {:?}",
        std::mem::discriminant(selected_element)
    );

    // Find the operation descriptor
    let operation_descriptor = match selected_element.find_operation_descriptor(op_name) {
        Some(descriptor) => {
            debug!(
                "Found operation '{}' with entity_name='{}', name='{}'",
                op_name, descriptor.entity_name, descriptor.name
            );
            descriptor
        }
        None => {
            let available_ops_str = if available_ops.is_empty() {
                "none".to_string()
            } else {
                format!("{:?}", available_ops)
            };
            global_data.state.status_message = format!(
                "Operation '{}' not found. Available: {}",
                op_name, available_ops_str
            );
            debug!(
                "Operation '{}' not found on selected element. Available operations: {:?}",
                op_name, available_ops
            );
            debug!("Element structure: {:?}", selected_element);
            return false;
        }
    };

    // Execute with descriptor
    match global_data
        .state
        .execute_operation_on_selected(op_name, operation_descriptor, ui_state)
    {
        Ok(_) => {
            global_data.state.status_message = success_msg.to_string();
            true
        }
        Err(e) => {
            global_data.state.status_message = format!("{} failed: {}", op_name, e);
            false
        }
    }
}

/// Create an empty modifier keys mask
fn empty_modifier_mask() -> r3bl_tui::ModifierKeysMask {
    r3bl_tui::ModifierKeysMask {
        ctrl_key_state: KeyState::NotPressed,
        shift_key_state: KeyState::NotPressed,
        alt_key_state: KeyState::NotPressed,
    }
}

/// Toggle completion for the selected block
fn toggle_completion(
    component: &mut BlockListComponent,
    global_data: &mut GlobalData<State, AppSignal>,
) -> CommonResult<EventPropagation> {
    throws_with_return!({
        if global_data.state.selected_index < component.element_tree.len() {
            let element = &component.element_tree[global_data.state.selected_index];
            if let Some(operation) = element.get_operation() {
                if let Some(row) = global_data.state.data.get(global_data.state.selected_index) {
                    if let Some(id_value) = row.get(&operation.descriptor.id_column) {
                        if let Some(id_str) = id_value.as_string() {
                            if let Some(field_value) = row.get(&get_field_name(operation)) {
                                let bool_val = match field_value {
                                    holon_api::Value::Boolean(b) => *b,
                                    holon_api::Value::Integer(i) => *i != 0,
                                    _ => {
                                        global_data.state.status_message = format!(
                                            "Field {} is not a boolean or integer",
                                            get_field_name(operation)
                                        );
                                        return Ok(EventPropagation::ConsumedRender);
                                    }
                                };
                                let op_signal = AppSignal::ExecuteOperation {
                                    operation_name: operation.descriptor.name.clone(),
                                    table: operation.descriptor.entity_name.clone(),
                                    id_column: operation.descriptor.id_column.clone(),
                                    id_value: id_str.to_string(),
                                    field: get_field_name(operation),
                                    new_value: holon_api::Value::Boolean(!bool_val),
                                };
                                send_signal!(
                                    global_data.main_thread_channel_sender,
                                    TerminalWindowMainThreadSignal::ApplyAppSignal(op_signal)
                                );
                                global_data.state.status_message =
                                    "Toggling completion...".to_string();
                                EventPropagation::ConsumedRender
                            } else {
                                global_data.state.status_message =
                                    format!("Field {} not found in row", get_field_name(operation));
                                EventPropagation::ConsumedRender
                            }
                        } else {
                            global_data.state.status_message = format!(
                                "ID column {} is not a string",
                                operation.descriptor.id_column
                            );
                            EventPropagation::ConsumedRender
                        }
                    } else {
                        global_data.state.status_message =
                            format!("ID column {} not found", operation.descriptor.id_column);
                        EventPropagation::ConsumedRender
                    }
                } else {
                    global_data.state.status_message =
                        format!("No data at index {}", global_data.state.selected_index);
                    EventPropagation::ConsumedRender
                }
            } else {
                global_data.state.status_message = format!(
                    "No operation found for element at index {}",
                    global_data.state.selected_index
                );
                EventPropagation::ConsumedRender
            }
        } else {
            global_data.state.status_message = format!(
                "Selected index {} out of bounds (len: {})",
                global_data.state.selected_index,
                component.element_tree.len()
            );
            EventPropagation::ConsumedRender
        }
    });
}

/// Handle split block operation
fn handle_split_block(
    component: &mut BlockListComponent,
    global_data: &mut GlobalData<State, AppSignal>,
) -> CommonResult<EventPropagation> {
    throws_with_return!({
        let cursor_offset = match global_data.state.calculate_cursor_offset() {
            Ok(offset) => offset,
            Err(e) => {
                global_data.state.status_message = format!("Split failed: {}", e);
                return Ok(EventPropagation::ConsumedRender);
            }
        };
        let block_id = match global_data.state.selected_block_id() {
            Some(id) => id,
            None => {
                global_data.state.status_message = "Split failed: No block selected".to_string();
                return Ok(EventPropagation::ConsumedRender);
            }
        };
        let ui_state = holon::api::UiState {
            cursor_pos: Some(holon::api::CursorPosition {
                block_id: block_id.clone(),
                offset: cursor_offset,
            }),
            focused_id: Some(block_id),
        };
        if execute_operation_with_status(
            component,
            global_data,
            "split_block",
            "Splitting block...",
            Some(ui_state),
        ) {
            exit_edit_mode(component, global_data);
        }
        EventPropagation::ConsumedRender
    });
}

/// Component that displays and manages the block list with hierarchical structure
pub struct BlockListComponent {
    id: FlexBoxId,
    element_tree: Vec<UIElement>,
    /// Editor engine for handling text input (created once, reused)
    editor_engine: Option<EditorEngine>,
}

impl BlockListComponent {
    pub fn new(id: FlexBoxId) -> Self {
        Self {
            id,
            element_tree: Vec::new(),
            editor_engine: None,
        }
    }

    pub fn new_boxed(id: FlexBoxId) -> BoxedSafeComponent<State, AppSignal> {
        Box::new(Self::new(id))
    }

    /// Collect all operation names from a UIElement recursively
    fn collect_all_operation_names(element: &UIElement) -> Vec<String> {
        let mut ops = Vec::new();
        match element {
            UIElement::Checkbox { operations, .. } | UIElement::EditableText { operations, .. } => {
                for op in operations {
                    ops.push(op.descriptor.name.clone());
                }
            }
            UIElement::Row { children } => {
                for child in children {
                    ops.extend(Self::collect_all_operation_names(child));
                }
            }
            _ => {}
        }
        ops
    }

    /// Rebuild element tree from current state
    pub fn rebuild_element_tree(&mut self, global_data: &GlobalData<State, AppSignal>) {
        self.element_tree = RenderInterpreter::build_element_tree(
            &global_data.state.render_spec,
            &global_data.state.data,
            global_data.state.selected_index,
        );
    }

    /// Get the current context (Editing or Navigation)
    fn get_current_context(&self, global_data: &GlobalData<State, AppSignal>) -> BindingContext {
        if global_data.state.editing_block_index.is_some() {
            BindingContext::Editing
        } else {
            BindingContext::Navigation
        }
    }

    /// Handle a key binding action
    fn handle_key_binding_action(
        &mut self,
        action: &Action,
        global_data: &mut GlobalData<State, AppSignal>,
    ) -> CommonResult<EventPropagation> {
        throws_with_return!({
            match action {
                Action::Operation(op_name) => {
                    // Execute operation on selected block
                    execute_operation_with_status(
                        self,
                        global_data,
                        op_name,
                        &format!("Executing {}...", op_name),
                        None,
                    );
                    EventPropagation::ConsumedRender
                }
                Action::Special(special_action) => {
                    match special_action.as_str() {
                        "toggle_completion" => {
                            // Handle toggle completion (the 'x' key behavior)
                            return toggle_completion(self, global_data);
                        }
                        "start_editing" => {
                            // Start editing the selected block
                            return self.start_editing_selected_block(global_data);
                        }
                        "save_and_exit" => {
                            // Save current editing block and exit edit mode
                            if self.save_current_editing_block(global_data) {
                                EventPropagation::ConsumedRender
                            } else {
                                EventPropagation::Propagate
                            }
                        }
                        "split_block" => {
                            // Split block at cursor position
                            return handle_split_block(self, global_data);
                        }
                        _ => {
                            global_data.state.status_message =
                                format!("Unknown special action: {}", special_action);
                            EventPropagation::ConsumedRender
                        }
                    }
                }
            }
        });
    }

    /// Save the currently editing block if there is one
    /// Returns true if a block was saved, false otherwise
    pub fn save_current_editing_block(
        &mut self,
        global_data: &mut GlobalData<State, AppSignal>,
    ) -> bool {
        if let Some(editing_idx) = global_data.state.editing_block_index {
            save_and_exit_edit_mode(self, global_data, editing_idx)
        } else {
            false
        }
    }

    /// Start editing the currently selected block
    fn start_editing_selected_block(
        &mut self,
        global_data: &mut GlobalData<State, AppSignal>,
    ) -> CommonResult<EventPropagation> {
        throws_with_return!({
            if global_data.state.selected_index < self.element_tree.len() {
                let element = &self.element_tree[global_data.state.selected_index];
                // Search recursively for EditableText (might be inside a Row)
                if let Some(editable_text) = element.find_editable_text() {
                    if let UIElement::EditableText { content, .. } = editable_text {
                        // Create editor buffer with initial content
                        // Split content by newlines to initialize buffer with multiple lines
                        let mut buffer = r3bl_tui::EditorBuffer::new_empty(None, None);
                        let lines: Vec<&str> = content.split('\n').collect();
                        buffer.init_with(lines);

                        // Create editor engine and move cursor to end
                        let mut editor_engine = EditorEngine::new(EditorEngineConfig {
                            multiline_mode: r3bl_tui::LineMode::MultiLine,
                            syntax_highlight: r3bl_tui::SyntaxHighlightMode::Disable,
                            edit_mode: r3bl_tui::EditMode::ReadWrite,
                        });

                        // CRITICAL: Set viewport on EditorEngine before processing events
                        // Operations require a valid viewport size
                        editor_engine.current_box.style_adjusted_bounds_size =
                            width(200) + height(50);

                        // Move cursor to end of initial content
                        let end_key_event = InputEvent::Keyboard(KeyPress::Plain {
                            key: Key::SpecialKey(SpecialKey::End),
                        });
                        let _ = engine_public_api::apply_event(
                            &mut buffer,
                            &mut editor_engine,
                            end_key_event,
                            &mut SystemClipboard,
                        );

                        global_data.state.editing_buffer = Some(buffer);
                        self.editor_engine = Some(editor_engine);

                        global_data.state.editing_block_index =
                            Some(global_data.state.selected_index);
                        global_data.state.status_message =
                            "Editing... (Esc or Ctrl+Enter to save, Enter for newline)".to_string();
                        EventPropagation::ConsumedRender
                    } else {
                        EventPropagation::Propagate
                    }
                } else {
                    EventPropagation::Propagate
                }
            } else {
                EventPropagation::Propagate
            }
        });
    }
}

impl Component<State, AppSignal> for BlockListComponent {
    fn get_id(&self) -> FlexBoxId {
        self.id
    }

    fn reset(&mut self) {
        // Clear editor engine when resetting
        self.editor_engine = None;
    }

    fn handle_event(
        &mut self,
        global_data: &mut GlobalData<State, AppSignal>,
        input_event: InputEvent,
        has_focus: &mut HasFocus,
    ) -> CommonResult<EventPropagation> {
        throws_with_return!({
            // Only handle events if this component has focus
            if !has_focus.does_id_have_focus(self.id) {
                return Ok(EventPropagation::Propagate);
            }

            // Rebuild element tree to ensure it's up to date
            self.rebuild_element_tree(global_data);

            // Ensure we're always editing the selected block (unless we're already editing it)
            // Only do this if the selected block has an editable_text widget
            if global_data.state.editing_block_index != Some(global_data.state.selected_index) {
                // We should be editing but aren't, or we're editing a different block
                // Start editing the selected block ONLY if it has editable_text
                if global_data.state.selected_index < self.element_tree.len() {
                    let element = &self.element_tree[global_data.state.selected_index];
                    if element.find_editable_text().is_some() {
                        // Save current editing block if we were editing a different one
                        if let Some(editing_idx) = global_data.state.editing_block_index {
                            if editing_idx != global_data.state.selected_index {
                                // Save the previous block before switching
                                save_current_block_without_exit(self, global_data, editing_idx);
                            }
                        }
                        // Now start editing the selected block
                        return self.start_editing_selected_block(global_data);
                    } else {
                        // Selected block doesn't have editable_text - exit edit mode if we were editing
                        if global_data.state.editing_block_index.is_some() {
                            let editing_idx = global_data.state.editing_block_index.unwrap();
                            save_current_block_without_exit(self, global_data, editing_idx);
                            exit_edit_mode(self, global_data);
                        }
                    }
                }
            }

            let mut event_consumed = false;

            match input_event {
                InputEvent::Keyboard(KeyPress::Plain { key }) => {
                    // DEBUG: Log all Plain events when editing
                    if global_data.state.editing_block_index.is_some() {
                        global_data.state.status_message = format!("Plain event: key={:?}", key);
                    }

                    // Handle edit mode first - use EditorEngine to process input
                    if let Some(_editing_idx) = global_data.state.editing_block_index {
                        if let Some(buffer) = &mut global_data.state.editing_buffer {
                            let editor_engine = ensure_editor_engine(self);

                            // Handle Esc: save and exit edit mode (check BEFORE passing to EditorEngine)
                            if let Key::SpecialKey(SpecialKey::Esc) = key {
                                // Escape: save and exit edit mode
                                global_data.state.status_message =
                                    "Esc - saving and exiting".to_string();
                                let editing_idx = global_data.state.editing_block_index.unwrap();
                                save_and_exit_edit_mode(self, global_data, editing_idx);
                                return Ok(EventPropagation::ConsumedRender);
                            }

                            // Handle Up/Down arrows: check if at boundary, otherwise let EditorEngine handle
                            if let Key::SpecialKey(SpecialKey::Up) = key {
                                let caret_raw = buffer.get_caret_raw();
                                let caret_row = caret_raw.row_index.as_usize();

                                // Check if we're at the first line
                                if caret_row == 0 {
                                    // At first line: save current block and move to previous
                                    let editing_idx =
                                        global_data.state.editing_block_index.unwrap();
                                    save_current_block_without_exit(self, global_data, editing_idx);
                                    exit_edit_mode(self, global_data);
                                    global_data.state.select_previous();
                                    // Start editing the new block
                                    return self.start_editing_selected_block(global_data);
                                }
                                // Not at first line: pass to EditorEngine to move cursor up within text
                                // Fall through to EditorEngine handler below
                            }

                            if let Key::SpecialKey(SpecialKey::Down) = key {
                                let lines = buffer.get_lines();
                                let caret_raw = buffer.get_caret_raw();
                                let caret_row = caret_raw.row_index.as_usize();

                                // Count total lines in buffer
                                let mut line_count: usize = 0;
                                loop {
                                    if lines.get_line_content(row(line_count)).is_some() {
                                        line_count += 1;
                                    } else {
                                        break;
                                    }
                                }

                                // Check if we're at the last line (caret_row is 0-indexed, so last line is line_count - 1)
                                let is_last_line = caret_row >= line_count.saturating_sub(1);

                                if is_last_line {
                                    // At last line: save current block and move to next
                                    let editing_idx =
                                        global_data.state.editing_block_index.unwrap();
                                    save_current_block_without_exit(self, global_data, editing_idx);
                                    exit_edit_mode(self, global_data);
                                    global_data.state.select_next();
                                    // Start editing the new block
                                    return self.start_editing_selected_block(global_data);
                                }
                                // Not at last line: pass to EditorEngine to move cursor down within text
                                // Fall through to EditorEngine handler below
                            }

                            // Handle Enter: let EditorEngine insert newline (default behavior)
                            // Also check for Ctrl+Enter here in case it comes through as Plain
                            // Since many terminals don't send modifier keys reliably
                            if let Key::SpecialKey(SpecialKey::Enter) = key {
                                // Plain Enter: let EditorEngine handle it (inserts newline)
                                // Don't intercept - pass through to EditorEngine
                                global_data.state.status_message =
                                    "Enter - inserting newline (use Esc or Ctrl+Enter to save)"
                                        .to_string();
                            }

                            // Check for Ctrl+Enter in Plain handler (some terminals send it as Plain)
                            // We can't detect Ctrl in Plain events, so we'll need a different approach
                            // For now, let's use a different key combination or check if there's a way to detect it
                            match engine_public_api::apply_event(
                                buffer,
                                editor_engine,
                                input_event.clone(),
                                &mut SystemClipboard,
                            ) {
                                Ok(EditorEngineApplyEventResult::Applied) => {
                                    // Event was handled by editor - consume and render
                                    return Ok(EventPropagation::ConsumedRender);
                                }
                                Ok(EditorEngineApplyEventResult::NotApplied) => {
                                    // Event not handled by editor - Esc should have been handled above
                                    // For other NotApplied events, still consume them (don't let them fall through)
                                    // The editor might not handle all events, but we're in edit mode so consume everything
                                    return Ok(EventPropagation::ConsumedRender);
                                }
                                Err(e) => {
                                    global_data.state.status_message =
                                        format!("Editor error: {}", e);
                                    return Ok(EventPropagation::ConsumedRender);
                                }
                            }
                        }
                    }

                    if let Key::Character(typed_char) = key {
                        // Check keybindings config first
                        let context = self.get_current_context(global_data);
                        let empty_mask = empty_modifier_mask();

                        if let Some(action) = global_data.state.keybindings.find_binding(
                            &Key::Character(typed_char),
                            &empty_mask,
                            context,
                        ) {
                            // Found binding - clone action and execute it
                            let action = action.clone();
                            return self.handle_key_binding_action(&action, global_data);
                        }

                        // No binding found - fall back to default behavior
                        match typed_char {
                            // 'x' is now handled via keybindings (Ctrl+x), so removed from fallback
                            ']' => {
                                event_consumed = true;
                                execute_operation_with_status(
                                    self,
                                    global_data,
                                    "indent",
                                    "Indenting...",
                                    None,
                                );
                            }
                            '[' => {
                                event_consumed = true;
                                execute_operation_with_status(
                                    self,
                                    global_data,
                                    "outdent",
                                    "Outdenting...",
                                    None,
                                );
                            }
                            _ => {}
                        }
                    }

                    if let Key::SpecialKey(special_key) = key {
                        // Check keybindings config first
                        let context = self.get_current_context(global_data);
                        let empty_mask = empty_modifier_mask();

                        if let Some(action) = global_data.state.keybindings.find_binding(
                            &Key::SpecialKey(special_key),
                            &empty_mask,
                            context,
                        ) {
                            // Found binding - clone action and execute it
                            let action = action.clone();
                            return self.handle_key_binding_action(&action, global_data);
                        }

                        // No binding found - fall back to default behavior
                        match special_key {
                            SpecialKey::Up => {
                                // Not editing (editing case handled earlier): move selection
                                global_data.state.select_previous();
                                // Only try to start editing if the selected block has editable_text
                                if global_data.state.selected_index < self.element_tree.len() {
                                    let element =
                                        &self.element_tree[global_data.state.selected_index];
                                    if element.find_editable_text().is_some() {
                                        return self.start_editing_selected_block(global_data);
                                    }
                                }
                                // No editable_text - just consume and render to show new selection
                                return Ok(EventPropagation::ConsumedRender);
                            }
                            SpecialKey::Down => {
                                // Not editing (editing case handled earlier): move selection
                                global_data.state.select_next();
                                // Only try to start editing if the selected block has editable_text
                                if global_data.state.selected_index < self.element_tree.len() {
                                    let element =
                                        &self.element_tree[global_data.state.selected_index];
                                    if element.find_editable_text().is_some() {
                                        return self.start_editing_selected_block(global_data);
                                    }
                                }
                                // No editable_text - just consume and render to show new selection
                                return Ok(EventPropagation::ConsumedRender);
                            }
                            SpecialKey::Enter => {
                                // Enter: start editing if editable_text is selected
                                if global_data.state.editing_block_index.is_none() {
                                    return self.start_editing_selected_block(global_data);
                                }
                            }
                            SpecialKey::Tab => {
                                event_consumed = true;
                                execute_operation_with_status(
                                    self,
                                    global_data,
                                    "indent",
                                    "Indenting...",
                                    None,
                                );
                            }
                            _ => {}
                        }
                    }
                }
                InputEvent::Keyboard(KeyPress::WithModifiers { key, mask }) => {
                    // Check keybindings config first (before handling edit mode)
                    let context = self.get_current_context(global_data);

                    if let Some(action) = global_data
                        .state
                        .keybindings
                        .find_binding(&key, &mask, context)
                    {
                        // Found binding - clone action and execute it
                        let action = action.clone();
                        return self.handle_key_binding_action(&action, global_data);
                    }

                    // DEBUG: Log all WithModifiers events when editing
                    if global_data.state.editing_block_index.is_some() {
                        global_data.state.status_message = format!(
                            "WithModifiers: key={:?}, shift={:?}, ctrl={:?}",
                            key, mask.shift_key_state, mask.ctrl_key_state
                        );
                    }

                    // Handle edit mode first - forward WithModifiers events to EditorEngine
                    if let Some(_editing_idx) = global_data.state.editing_block_index {
                        if let Some(buffer) = &mut global_data.state.editing_buffer {
                            let editor_engine = ensure_editor_engine(self);

                            // Handle Ctrl+Enter: save and exit edit mode
                            // Shift+Enter: insert newline (if terminal supports it as WithModifiers)
                            // Plain Enter: insert newline (handled by EditorEngine in Plain handler)
                            // Alt+Enter: split block at cursor position
                            if let Key::SpecialKey(SpecialKey::Enter) = key {
                                if mask.alt_key_state == KeyState::Pressed {
                                    // Alt+Enter: split block at cursor
                                    return handle_split_block(self, global_data);
                                } else if mask.ctrl_key_state == KeyState::Pressed {
                                    // Ctrl+Enter: save and exit edit mode
                                    global_data.state.status_message =
                                        "Ctrl+Enter - saving".to_string();
                                    let editing_idx =
                                        global_data.state.editing_block_index.unwrap();
                                    save_and_exit_edit_mode(self, global_data, editing_idx);
                                    return Ok(EventPropagation::ConsumedRender);
                                } else if mask.shift_key_state == KeyState::Pressed {
                                    // Shift+Enter: manually create InsertNewLine event and apply it
                                    // EditorEvent::try_from() only converts Plain Enter, not WithModifiers Enter
                                    global_data.state.status_message =
                                        "Shift+Enter detected - inserting newline".to_string();
                                    EditorEvent::apply_editor_event(
                                        editor_engine,
                                        buffer,
                                        EditorEvent::InsertNewLine,
                                        &mut SystemClipboard,
                                    );
                                    return Ok(EventPropagation::ConsumedRender);
                                } else {
                                    // Enter without modifiers in WithModifiers: shouldn't happen, but let EditorEngine handle it
                                    global_data.state.status_message = "Enter (WithModifiers but no modifiers) - passing to EditorEngine".to_string();
                                    // Enter without Shift: save and exit edit mode
                                    let editing_idx =
                                        global_data.state.editing_block_index.unwrap();
                                    save_and_exit_edit_mode(self, global_data, editing_idx);
                                    return Ok(EventPropagation::ConsumedRender);
                                }
                            } else {
                                // Process other input events through EditorEngine
                                match engine_public_api::apply_event(
                                    buffer,
                                    editor_engine,
                                    input_event.clone(),
                                    &mut SystemClipboard,
                                ) {
                                    Ok(EditorEngineApplyEventResult::Applied) => {
                                        // Event was handled by editor - consume and render
                                        return Ok(EventPropagation::ConsumedRender);
                                    }
                                    Ok(EditorEngineApplyEventResult::NotApplied) => {
                                        // Event not handled by editor - fall through to app-level handlers below
                                    }
                                    Err(e) => {
                                        global_data.state.status_message =
                                            format!("Editor error: {}", e);
                                        return Ok(EventPropagation::ConsumedRender);
                                    }
                                }
                            }
                        }
                    }

                    // Handle WithModifiers keys in navigation mode (fallback to default behavior)
                    if let Key::SpecialKey(special_key) = key {
                        match special_key {
                            SpecialKey::Tab if mask.shift_key_state == KeyState::Pressed => {
                                event_consumed = true;
                                execute_operation_with_status(
                                    self,
                                    global_data,
                                    "outdent",
                                    "Outdenting...",
                                    None,
                                );
                            }
                            SpecialKey::Up if mask.ctrl_key_state == KeyState::Pressed => {
                                event_consumed = true;
                                execute_operation_with_status(
                                    self,
                                    global_data,
                                    "move_up",
                                    "Moving up...",
                                    None,
                                );
                            }
                            SpecialKey::Down if mask.ctrl_key_state == KeyState::Pressed => {
                                event_consumed = true;
                                execute_operation_with_status(
                                    self,
                                    global_data,
                                    "move_down",
                                    "Moving down...",
                                    None,
                                );
                            }
                            SpecialKey::Right if mask.ctrl_key_state == KeyState::Pressed => {
                                event_consumed = true;
                                execute_operation_with_status(
                                    self,
                                    global_data,
                                    "indent",
                                    "Indenting...",
                                    None,
                                );
                            }
                            SpecialKey::Left if mask.ctrl_key_state == KeyState::Pressed => {
                                event_consumed = true;
                                execute_operation_with_status(
                                    self,
                                    global_data,
                                    "outdent",
                                    "Outdenting...",
                                    None,
                                );
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }

            if event_consumed {
                EventPropagation::ConsumedRender
            } else {
                EventPropagation::Propagate
            }
        });
    }

    fn render(
        &mut self,
        global_data: &mut GlobalData<State, AppSignal>,
        _current_box: FlexBox,
        surface_bounds: SurfaceBounds,
        has_focus: &mut HasFocus,
    ) -> CommonResult<RenderPipeline> {
        throws_with_return!({
            // Rebuild element tree on each render to reflect state changes
            self.rebuild_element_tree(global_data);

            // Check if this component has focus
            let is_focused = has_focus.does_id_have_focus(self.id);

            let mut pipeline = render_pipeline!();

            pipeline.push(ZOrder::Normal, {
                let mut render_ops = r3bl_tui::RenderOpIRVec::new();

                // Use new element tree rendering with focus state
                RenderInterpreter::render_element_tree(
                    &self.element_tree,
                    &mut render_ops,
                    surface_bounds.origin_pos.row_index.as_usize(),
                    &global_data.state.data,
                    is_focused,
                    global_data.state.editing_block_index,
                    global_data.state.editing_buffer.as_ref(),
                );

                render_ops
            });

            pipeline
        });
    }
}
