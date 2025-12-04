use super::components::{BlockListComponent, ComponentId};
use super::render_interpreter::RenderInterpreter;
use super::state::{AppSignal, State};
use super::stylesheet::{self, StyleId};
use super::ui_element::UIElement;
use r3bl_tui::{
    box_end, box_start, col, height, new_style, render_component_in_current_box,
    render_tui_styled_texts_into, req_size_pc, row, surface, throws, throws_with_return, tui_color,
    tui_styled_text, tui_styled_texts, App, BoxedSafeApp, CommonResult, ComponentRegistry,
    ComponentRegistryMap, EventPropagation, FlexBoxId, GlobalData, HasFocus, InputEvent, Key,
    KeyPress, LayoutDirection, LayoutManagement, LengthOps, PerformPositioningAndSizing, Pos,
    RenderOpCommon, RenderOpIR, RenderOpIRVec, RenderPipeline, Size, Surface, SurfaceProps,
    SurfaceRender, ZOrder, SPACER_GLYPH,
};
use std::marker::PhantomData;

// Helper function to extract field name from OperationWiring
// For "set_field" operations, tries to extract from descriptor params, otherwise uses modified_param
fn get_field_name(op: &query_render::OperationWiring) -> String {
    // For "set_field" operations, the field name is typically in modified_param
    // or we can try to extract it from the operation descriptor
    // For now, use modified_param as it often matches the database field name
    op.modified_param.clone()
}

pub struct AppMain {
    _phantom: PhantomData<(State, AppSignal)>,
}

impl Default for AppMain {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl AppMain {
    pub fn new_boxed() -> BoxedSafeApp<State, AppSignal> {
        Box::new(Self::default())
    }
}

impl App for AppMain {
    type S = State;
    type AS = AppSignal;

    fn app_init(
        &mut self,
        component_registry_map: &mut ComponentRegistryMap<State, AppSignal>,
        has_focus: &mut HasFocus,
    ) {
        // Register the block list component
        let block_list_component =
            BlockListComponent::new_boxed(FlexBoxId::from(ComponentId::BlockList));
        ComponentRegistry::put(
            component_registry_map,
            FlexBoxId::from(ComponentId::BlockList),
            block_list_component,
        );

        // Set initial focus to the block list
        has_focus.set_id(FlexBoxId::from(ComponentId::BlockList));
    }

    fn app_handle_input_event(
        &mut self,
        input_event: InputEvent,
        global_data: &mut GlobalData<State, AppSignal>,
        component_registry_map: &mut ComponentRegistryMap<State, AppSignal>,
        has_focus: &mut HasFocus,
    ) -> CommonResult<EventPropagation> {
        throws_with_return!({
            // Initialize CDC watcher on first run
            {
                let sender_opt = global_data.state.main_thread_sender_channel.lock().unwrap();
                if sender_opt.is_none() {
                    drop(sender_opt); // Release lock before calling start_cdc_watcher
                    global_data
                        .state
                        .start_cdc_watcher(global_data.main_thread_channel_sender.clone());
                }
            }

            // Poll CDC changes if there are any pending
            if global_data.state.poll_cdc_changes() > 0 {
                return Ok(EventPropagation::ConsumedRender);
            }

            // Handle Ctrl+Q exit - save current editing block before exiting
            if let InputEvent::Keyboard(KeyPress::WithModifiers {
                key: Key::Character('q'),
                mask,
            }) = input_event
            {
                if mask.ctrl_key_state == r3bl_tui::KeyState::Pressed {
                    // If we're editing, save the current block before exiting
                    if global_data.state.editing_block_index.is_some() {
                        // Use a helper function to save the editing block
                        // This rebuilds the element tree and saves the block
                        save_editing_block_on_exit(global_data);
                    }
                    // Allow the exit to proceed by propagating the event
                    return Ok(EventPropagation::Propagate);
                }
            }

            // Skip app-level shortcuts when editing (let component handle all input)
            if global_data.state.editing_block_index.is_some() {
                // Route all events to the focused component when editing
                return Ok(ComponentRegistry::route_event_to_focused_component(
                    global_data,
                    input_event,
                    component_registry_map,
                    has_focus,
                )?);
            }

            // Handle app-level shortcuts (like Ctrl+r for sync)
            // Only when NOT editing
            if let InputEvent::Keyboard(KeyPress::WithModifiers {
                key: Key::Character('r'),
                mask,
            }) = input_event
            {
                if mask.ctrl_key_state == r3bl_tui::KeyState::Pressed {
                    // Generic sync trigger (works for any SyncableProvider)
                    let engine = global_data.state.engine.clone();
                    let sender_opt = global_data
                        .state
                        .main_thread_sender_channel
                        .lock()
                        .unwrap()
                        .clone();

                    tracing::info!("[TUI] Sync triggered by user (Ctrl+r)");
                    tokio::spawn(async move {
                        tracing::info!("[TUI] Starting wildcard sync operation");
                        // Use wildcard operation dispatch: entity_name="*", op_name="sync"
                        let params = std::collections::HashMap::new();
                        match engine.execute_operation("*", "sync", params).await {
                            Ok(_) => {
                                tracing::info!(
                                    "[TUI] Wildcard sync operation completed successfully"
                                );
                                if let Some(sender) = sender_opt {
                                    let _ = sender.send(
                                        r3bl_tui::TerminalWindowMainThreadSignal::ApplyAppSignal(
                                            AppSignal::OperationResult {
                                                operation_name: "sync".to_string(),
                                                success: true,
                                                error_message: None,
                                            }
                                        )
                                    ).await;
                                }
                            }
                            Err(e) => {
                                tracing::error!("[TUI] Wildcard sync operation failed: {}", e);
                                if let Some(sender) = sender_opt {
                                    let _ = sender.send(
                                        r3bl_tui::TerminalWindowMainThreadSignal::ApplyAppSignal(
                                            AppSignal::OperationResult {
                                                operation_name: "sync".to_string(),
                                                success: false,
                                                error_message: Some(e.to_string()),
                                            }
                                        )
                                    ).await;
                                }
                            }
                        }
                    });

                    global_data.state.status_message = "Syncing...".to_string();
                    return Ok(EventPropagation::ConsumedRender);
                }
            }

            // Route all other events to the focused component
            ComponentRegistry::route_event_to_focused_component(
                global_data,
                input_event,
                component_registry_map,
                has_focus,
            )?
        });
    }

    fn app_handle_signal(
        &mut self,
        action: &AppSignal,
        global_data: &mut GlobalData<State, AppSignal>,
        _component_registry_map: &mut ComponentRegistryMap<State, AppSignal>,
        _has_focus: &mut HasFocus,
    ) -> CommonResult<EventPropagation> {
        throws_with_return!({
            match action {
                AppSignal::ExecuteOperation {
                    operation_name,
                    table,
                    id_column: _id_column,
                    id_value,
                    field,
                    new_value,
                } => {
                    // Clone values for async task
                    let engine = global_data.state.engine.clone();
                    let operation_name = operation_name.clone(); // Use operation name from descriptor
                    let table_name = table.clone();
                    let id_val = id_value.clone();
                    let field_name = field.clone();
                    let value = new_value.clone();

                    // Get the signal sender to communicate results back to UI
                    let sender_opt = global_data
                        .state
                        .main_thread_sender_channel
                        .lock()
                        .unwrap()
                        .clone();

                    // Spawn async task to execute operation
                    tokio::spawn(async move {
                        // Get entity name from table mapping, fallback to table name if not mapped
                        let entity_name = engine
                            .get_entity_for_table(&table_name)
                            .await
                            .unwrap_or_else(|| table_name.clone());

                        // Build parameters for the operation
                        // UpdateField expects: id, field, value (and optionally table)
                        let mut params = std::collections::HashMap::new();
                        params.insert("id".to_string(), holon_api::Value::String(id_val));
                        params.insert("field".to_string(), holon_api::Value::String(field_name));
                        params.insert("value".to_string(), value);

                        // Execute the operation
                        let result = engine
                            .execute_operation(&entity_name, &operation_name, params)
                            .await;

                        // Send result back to UI thread via signal
                        if let Some(sender) = sender_opt {
                            let signal = AppSignal::OperationResult {
                                operation_name: operation_name.to_string(),
                                success: result.is_ok(),
                                error_message: result.err().map(|e| e.to_string()),
                            };

                            if sender
                                .send(r3bl_tui::TerminalWindowMainThreadSignal::ApplyAppSignal(
                                    signal,
                                ))
                                .await
                                .is_err()
                            {
                                tracing::error!(
                                    "Failed to send operation result signal for '{}'",
                                    operation_name
                                );
                            }
                        } else {
                            // Fallback: log errors if signal channel is not available
                            match result {
                                Ok(_) => {
                                    tracing::info!(
                                        "Operation {} executed successfully",
                                        operation_name
                                    );
                                }
                                Err(e) => {
                                    tracing::error!("Operation {} failed: {}", operation_name, e);
                                }
                            }
                        }
                    });

                    // Update status message immediately
                    global_data.state.status_message = "Operation sent to backend".to_string();
                }
                AppSignal::OperationResult {
                    operation_name,
                    success,
                    error_message,
                } => {
                    // Update status message based on operation result
                    if *success {
                        global_data.state.status_message = format!("{} succeeded", operation_name);
                    } else {
                        let error_msg = error_message.as_deref().unwrap_or("Unknown error");
                        global_data.state.status_message =
                            format!("{} failed: {}", operation_name, error_msg);
                    }
                }
                AppSignal::Noop => {}
            }

            EventPropagation::ConsumedRender
        });
    }

    fn app_render(
        &mut self,
        global_data: &mut GlobalData<State, AppSignal>,
        component_registry_map: &mut ComponentRegistryMap<State, AppSignal>,
        has_focus: &mut HasFocus,
    ) -> CommonResult<RenderPipeline> {
        throws_with_return!({
            // Poll CDC changes before rendering to ensure UI reflects latest state
            // This is called both on user input and when CDC watcher triggers a render
            global_data.state.poll_cdc_changes();

            let window_size = global_data.window_size;

            // Create surface and use SurfaceRender to handle layout
            let mut surface = {
                let mut it = surface!(stylesheet: stylesheet::create_stylesheet()?);

                it.surface_start(SurfaceProps {
                    pos: row(0) + col(0),
                    size: window_size.col_width + (window_size.row_height - height(2)),
                })?;

                // Title bar (2 rows) - using stylesheet
                {
                    let mut title_ops = RenderOpIRVec::new();
                    title_ops += RenderOpCommon::MoveCursorPositionAbs(Pos::from((col(2), row(0))));
                    let title_texts = tui_styled_texts! {
                        tui_styled_text! {
                            @style: new_style!(bold color_fg: {tui_color!(hex "#00AAFF")}),
                            @text: "Block Outliner (R3BL TUI)"
                        },
                    };
                    render_tui_styled_texts_into(&title_texts, &mut title_ops);
                    it.render_pipeline.push(ZOrder::Normal, title_ops);
                }

                // Render components using SurfaceRender trait
                ContainerSurfaceRender { _app: self }.render_in_surface(
                    &mut it,
                    global_data,
                    component_registry_map,
                    has_focus,
                )?;

                it.surface_end()?;

                it
            };

            // Render status bar (last row)
            render_status_bar(
                &mut surface.render_pipeline,
                window_size,
                &global_data.state.status_message,
            );

            surface.render_pipeline
        });
    }
}

// SurfaceRender implementation for layout
struct ContainerSurfaceRender<'a> {
    _app: &'a mut AppMain,
}

impl SurfaceRender<State, AppSignal> for ContainerSurfaceRender<'_> {
    fn render_in_surface(
        &mut self,
        surface: &mut Surface,
        global_data: &mut GlobalData<State, AppSignal>,
        component_registry_map: &mut ComponentRegistryMap<State, AppSignal>,
        has_focus: &mut HasFocus,
    ) -> CommonResult<()> {
        throws!({
            // Block list component (takes all available space)
            box_start!(
                in: surface,
                id: FlexBoxId::from(ComponentId::BlockList),
                dir: LayoutDirection::Vertical,
                requested_size_percent: req_size_pc!(width: 100, height: 100),
                styles: [StyleId::Default]
            );
            render_component_in_current_box!(
                in:             surface,
                component_id:   FlexBoxId::from(ComponentId::BlockList),
                from:           component_registry_map,
                global_data:    global_data,
                has_focus:      has_focus
            );
            box_end!(in: surface);
        });
    }
}

fn render_status_bar(pipeline: &mut RenderPipeline, size: Size, status_msg: &str) {
    let color_bg = tui_color!(hex "#076DEB");
    let color_fg = tui_color!(hex "#E9C940");

    let help_text = format!("Ctrl+q: Exit | ↑/↓: Navigate/Edit | Ctrl+x: Toggle | Ctrl+r: Sync | Ctrl+→/←: Indent/Outdent | Ctrl+↑/↓: Move | Alt+Enter: Split | {}", status_msg);

    // Use stylesheet for status bar styling
    let styled_texts = tui_styled_texts! {
        tui_styled_text! {
            @style: new_style!(color_fg:{color_fg} color_bg:{color_bg}),
            @text: &help_text
        },
    };

    let col_idx = col(2);
    let row_idx = row(size.row_height.convert_to_index());
    let cursor = Pos::from((col_idx, row_idx));

    let mut render_ops = RenderOpIRVec::new();
    render_ops += RenderOpIR::Common(RenderOpCommon::MoveCursorPositionAbs(Pos::from((
        col(0),
        row_idx,
    ))));
    render_ops += RenderOpCommon::ResetColor;
    render_ops += RenderOpCommon::SetBgColor(color_bg);
    render_ops += RenderOpIR::PaintTextWithAttributes(
        SPACER_GLYPH.repeat(size.col_width.as_usize()).into(),
        None,
    );
    render_ops += RenderOpCommon::ResetColor;
    render_ops += RenderOpIR::Common(RenderOpCommon::MoveCursorPositionAbs(cursor));
    render_tui_styled_texts_into(&styled_texts, &mut render_ops);
    pipeline.push(ZOrder::Normal, render_ops);
}

/// Helper function to save the currently editing block when exiting the app
/// This rebuilds the element tree and saves the block content synchronously
/// to ensure the save completes before the app exits
fn save_editing_block_on_exit(global_data: &mut GlobalData<State, AppSignal>) {
    // Check if we're editing a block
    if let Some(editing_idx) = global_data.state.editing_block_index {
        if let Some(buffer) = &global_data.state.editing_buffer {
            // Rebuild element tree to ensure it's current
            let element_tree = RenderInterpreter::build_element_tree(
                &global_data.state.render_spec,
                &global_data.state.data,
                global_data.state.selected_index,
            );

            // Extract operation info from the EditableText
            let operation_info = if let Some(element) = element_tree.get(editing_idx) {
                element.find_editable_text().and_then(|editable| {
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
            } else {
                None
            };

            // Get content from buffer - join all lines with \n
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

            // Exit edit mode and clear buffer first (release mutable borrow)
            global_data.state.editing_block_index = None;
            global_data.state.editing_buffer = None;

            // Now we can borrow state.data immutably and execute synchronously
            if let Some((operation_name, table, id_column, field)) = operation_info {
                if let Some(row_data) = global_data.state.data.get(editing_idx) {
                    if let Some(id_value) = row_data.get(&id_column) {
                        if let Some(id_str) = id_value.as_string() {
                            // Execute the operation synchronously to ensure it completes before exit
                            // We spawn a new thread with its own runtime since we can't create
                            // a runtime from within an existing runtime
                            let engine = global_data.state.engine.clone();
                            let operation_name = operation_name.clone(); // Use operation name from descriptor
                            let table_name = table.clone();
                            let id_val = id_str.to_string();
                            let field_name = field.clone();
                            let value = holon_api::Value::String(buffer_content);

                            // Build parameters for the operation
                            let mut params = std::collections::HashMap::new();
                            params.insert("id".to_string(), holon_api::Value::String(id_val));
                            params
                                .insert("field".to_string(), holon_api::Value::String(field_name));
                            params.insert("value".to_string(), value);

                            // Spawn a new thread with its own runtime to execute the blocking save
                            // This avoids the "runtime within runtime" issue
                            let result = std::thread::spawn(move || -> Result<(), String> {
                                let rt = match tokio::runtime::Runtime::new() {
                                    Ok(rt) => rt,
                                    Err(e) => {
                                        return Err(format!("Failed to create runtime: {}", e))
                                    }
                                };
                                rt.block_on(async {
                                    // Get entity name from table mapping, fallback to table name if not mapped
                                    let entity_name = engine
                                        .get_entity_for_table(&table_name)
                                        .await
                                        .unwrap_or_else(|| table_name.clone());
                                    engine
                                        .execute_operation(&entity_name, &operation_name, params)
                                        .await
                                })
                                .map_err(|e| format!("Operation error: {}", e))
                            })
                            .join()
                            .map_err(|e| format!("Thread join error: {:?}", e))
                            .and_then(|r| r);

                            match result {
                                Ok(_) => {
                                    global_data.state.status_message =
                                        "Saved before exit".to_string();
                                }
                                Err(e) => {
                                    eprintln!("Failed to save before exit: {}", e);
                                    global_data.state.status_message =
                                        format!("Save failed: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
