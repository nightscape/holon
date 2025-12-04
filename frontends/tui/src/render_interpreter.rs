use crate::ui_element::UIElement;
use holon_api::Value;
use query_render::{Arg, BinaryOperator, RenderExpr, RenderSpec};
use r3bl_tui::{
    col, new_style, render_tui_styled_texts_into, row, tui_color, tui_styled_text,
    tui_styled_texts, Pos, RenderOpCommon, RenderOpIRVec, TuiColor, DEFAULT_CURSOR_CHAR,
};
use std::collections::HashMap;

/// Interprets generic RenderExpr AST into R3BL TUI render operations.
///
/// This is the TUI-specific interpreter for the UI-agnostic backend.
/// The same RenderExpr can be interpreted differently by Ratatui, Flutter, Web, etc.
pub struct RenderInterpreter;

impl RenderInterpreter {
    /// Build element tree from RenderSpec with operations attached.
    /// This separates interpretation from rendering.
    pub fn build_element_tree(
        spec: &RenderSpec,
        data: &[HashMap<String, Value>],
        selected_index: usize,
    ) -> Vec<UIElement> {
        let mut elements = Vec::new();
        Self::build_elements_from_expr(&spec.root, data, selected_index, &mut elements, spec);
        elements
    }

    /// Build UIElements from a RenderExpr
    fn build_elements_from_expr(
        expr: &RenderExpr,
        data: &[HashMap<String, Value>],
        selected_index: usize,
        elements: &mut Vec<UIElement>,
        spec: &RenderSpec,
    ) {
        match expr {
            RenderExpr::FunctionCall {
                name,
                args,
                operations: _,
            } => {
                match name.as_str() {
                    "list" => Self::build_list_elements(args, data, selected_index, elements, spec),
                    _ => {
                        // For now, other function calls aren't converted to elements
                    }
                }
            }
            _ => {}
        }
    }

    /// Build list elements (one UIElement per data row)
    fn build_list_elements(
        args: &[Arg],
        data: &[HashMap<String, Value>],
        selected_index: usize,
        elements: &mut Vec<UIElement>,
        spec: &RenderSpec,
    ) {
        let item_template = args
            .iter()
            .find(|arg| arg.name.as_deref() == Some("item_template"))
            .map(|arg| &arg.value);

        // Sort data (reuse existing logic)
        let hierarchical_columns = args
            .iter()
            .find(|arg| arg.name.as_deref() == Some("hierarchical_sort"))
            .and_then(|arg| Self::extract_sort_columns(&arg.value));

        let sort_columns = args
            .iter()
            .find(|arg| arg.name.as_deref() == Some("sort_by"))
            .and_then(|arg| Self::extract_sort_columns(&arg.value))
            .unwrap_or_default();

        let sorted_data: Vec<&HashMap<String, Value>> =
            if let Some(hier_cols) = hierarchical_columns {
                Self::hierarchical_sort(data, &hier_cols)
            } else if !sort_columns.is_empty() {
                let mut data_refs: Vec<_> = data.iter().collect();
                data_refs.sort_by(|a, b| Self::compare_rows(a, b, &sort_columns));
                data_refs
            } else {
                data.iter().collect()
            };

        // Build element for each row
        for (idx, row_data) in sorted_data.iter().enumerate() {
            let is_selected = idx == selected_index;

            if let Some(template) = item_template {
                let element =
                    Self::build_element_from_template(template, row_data, is_selected, spec);
                elements.push(element);
            }
        }
    }

    /// Build a single UIElement from a template expression
    fn build_element_from_template(
        expr: &RenderExpr,
        row_data: &HashMap<String, Value>,
        is_selected: bool,
        spec: &RenderSpec,
    ) -> UIElement {
        match expr {
            RenderExpr::FunctionCall {
                name,
                args,
                operations,
            } => match name.as_str() {
                "row" => {
                    let mut children = Vec::new();
                    for arg in args {
                        let child = Self::build_element_from_template(
                            &arg.value,
                            row_data,
                            is_selected,
                            spec,
                        );
                        children.push(child);
                    }
                    UIElement::Row { children }
                }
                "text" => {
                    let content_expr = args
                        .iter()
                        .find(|arg| arg.name.as_deref() == Some("content"))
                        .map(|arg| &arg.value);

                    let content = if let Some(content) = content_expr {
                        Self::eval_expr(content, row_data)
                            .map(|v| Self::value_to_string(&v))
                            .unwrap_or_default()
                    } else {
                        String::new()
                    };

                    let bg_color = if is_selected {
                        Some(tui_color!(hex "#333333"))
                    } else {
                        None
                    };

                    UIElement::Text {
                        content,
                        fg_color: None,
                        bg_color,
                    }
                }
                "checkbox" => {
                    let checked_expr = args
                        .iter()
                        .find(|arg| arg.name.as_deref() == Some("checked"))
                        .map(|arg| &arg.value);

                    let is_checked = if let Some(checked) = checked_expr {
                        Self::eval_expr(checked, row_data)
                            .and_then(|v| Self::value_to_bool(&v))
                            .unwrap_or(false)
                    } else {
                        false
                    };

                    UIElement::Checkbox {
                        checked: is_checked,
                        operations: operations.clone(),
                    }
                }
                "editable_text" => {
                    let content_expr = args
                        .iter()
                        .find(|arg| arg.name.as_deref() == Some("content"))
                        .map(|arg| &arg.value);

                    let content = if let Some(content) = content_expr {
                        Self::eval_expr(content, row_data)
                            .map(|v| Self::value_to_string(&v))
                            .unwrap_or_default()
                    } else {
                        String::new()
                    };

                    let bg_color = if is_selected {
                        Some(tui_color!(hex "#333333"))
                    } else {
                        None
                    };

                    UIElement::EditableText {
                        content,
                        operations: operations.clone(),
                        fg_color: None,
                        bg_color,
                    }
                }
                "badge" => {
                    let content_expr = args
                        .iter()
                        .find(|arg| arg.name.as_deref() == Some("content"))
                        .map(|arg| &arg.value);

                    let content = if let Some(content) = content_expr {
                        Self::eval_expr(content, row_data)
                            .map(|v| format!(" [{}] ", Self::value_to_string(&v)))
                            .unwrap_or_default()
                    } else {
                        String::new()
                    };

                    UIElement::Badge {
                        content,
                        color: tui_color!(hex "#FFFF00"),
                    }
                }
                "icon" => {
                    let source_expr = args
                        .iter()
                        .find(|arg| arg.name.as_deref() == Some("source"))
                        .map(|arg| &arg.value);

                    let symbol = if let Some(source) = source_expr {
                        Self::eval_expr(source, row_data)
                            .and_then(|v| v.as_string().map(String::from))
                            .unwrap_or_else(|| "●".to_string())
                    } else {
                        "●".to_string()
                    };

                    UIElement::Icon { symbol }
                }
                _ => UIElement::Text {
                    content: format!("[{}]", name),
                    fg_color: Some(tui_color!(hex "#FF0000")),
                    bg_color: if is_selected {
                        Some(tui_color!(hex "#333333"))
                    } else {
                        None
                    },
                },
            },
            RenderExpr::Literal { value } => {
                let converted_value = value.clone();
                let text = Self::value_to_string(&converted_value);

                UIElement::Text {
                    content: text,
                    fg_color: None,
                    bg_color: if is_selected {
                        Some(tui_color!(hex "#333333"))
                    } else {
                        None
                    },
                }
            }
            RenderExpr::ColumnRef { name } => {
                let value = row_data.get(name).cloned().unwrap_or(Value::Null);
                let text = Self::value_to_string(&value);

                UIElement::Text {
                    content: text,
                    fg_color: None,
                    bg_color: if is_selected {
                        Some(tui_color!(hex "#333333"))
                    } else {
                        None
                    },
                }
            }
            _ => UIElement::Text {
                content: format!("{:?}", expr),
                fg_color: None,
                bg_color: if is_selected {
                    Some(tui_color!(hex "#333333"))
                } else {
                    None
                },
            },
        }
    }

    /// Render element tree to RenderOpIRVec with layout information
    pub fn render_element_tree(
        elements: &[UIElement],
        render_ops: &mut RenderOpIRVec,
        start_row: usize,
        data: &[HashMap<String, Value>],
        is_focused: bool,
        editing_block_index: Option<usize>,
        editing_buffer: Option<&r3bl_tui::EditorBuffer>,
    ) {
        let mut current_row = start_row;

        for (idx, element) in elements.iter().enumerate() {
            // Calculate indentation based on depth field from original data
            let depth = if idx < data.len() {
                data[idx].get("depth").and_then(|v| v.as_i64()).unwrap_or(0) as usize
            } else {
                0
            };
            let indent_spaces = depth * 2;
            let start_col = 2 + indent_spaces;

            // Move cursor to start of this row
            *render_ops += RenderOpCommon::MoveCursorPositionAbs(Pos::from((
                col(start_col),
                row(current_row),
            )));

            // Check if we're editing this block and get buffer content
            let is_editing = editing_block_index == Some(idx);
            let editing_buffer_ref = if is_editing { editing_buffer } else { None };

            // Render element - for multi-line text, this may consume multiple rows
            let (rows_consumed, _) = Self::render_element(
                element,
                render_ops,
                is_focused,
                is_editing,
                editing_buffer_ref,
                start_col,
                current_row,
            );
            current_row += rows_consumed;
        }
    }

    /// Render a single UIElement
    /// Returns (rows_consumed, ending_column)
    fn render_element(
        element: &UIElement,
        render_ops: &mut RenderOpIRVec,
        is_focused: bool,
        is_editing: bool,
        editing_buffer: Option<&r3bl_tui::EditorBuffer>,
        start_col: usize,
        start_row: usize,
    ) -> (usize, usize) {
        match element {
            UIElement::Text {
                content,
                fg_color,
                bg_color,
            } => {
                // Dim text if component doesn't have focus
                let adjusted_fg = if !is_focused {
                    Some(tui_color!(hex "#888888")) // Dimmed gray
                } else {
                    *fg_color
                };
                // Handle multi-line text
                let lines: Vec<&str> = content.split('\n').collect();
                for (line_idx, line) in lines.iter().enumerate() {
                    if line_idx > 0 {
                        // Move to next line with same indentation
                        *render_ops += RenderOpCommon::MoveCursorPositionAbs(Pos::from((
                            col(start_col),
                            row(start_row + line_idx),
                        )));
                    }
                    Self::render_text_simple(render_ops, line, adjusted_fg, *bg_color);
                }
                let ending_col = start_col + content.lines().next().map(|l| l.len()).unwrap_or(0);
                (lines.len().max(1), ending_col) // Return rows consumed and ending column
            }
            UIElement::Checkbox { checked, .. } => {
                let checkbox_text = if *checked { "[✓] " } else { "[ ] " };
                // Use dimmer green when not focused
                let fg_color = if is_focused {
                    tui_color!(hex "#00FF00") // Bright green when focused
                } else {
                    tui_color!(hex "#008800") // Dim green when not focused
                };
                Self::render_text_simple(render_ops, checkbox_text, Some(fg_color), None);
                (1, start_col + 4) // Return rows consumed and ending column (checkbox is 4 chars)
            }
            UIElement::Badge { content, color } => {
                // Badge color unchanged (or could dim if needed)
                Self::render_text_simple(render_ops, content, Some(*color), None);
                (1, start_col + content.len()) // Return rows consumed and ending column
            }
            UIElement::Icon { symbol } => {
                let text = format!("{} ", symbol);
                Self::render_text_simple(render_ops, &text, None, None);
                (1, start_col + text.len()) // Return rows consumed and ending column
            }
            UIElement::EditableText {
                content,
                fg_color,
                bg_color,
                ..
            } => {
                if is_editing {
                    // When editing, render multi-line text with cursor
                    if let Some(buf) = editing_buffer {
                        let lines = buf.get_lines();
                        let caret_raw = buf.get_caret_raw();
                        let caret_row = caret_raw.row_index.as_usize();
                        let caret_col = caret_raw.col_index.as_usize();

                        let adjusted_fg = if !is_focused {
                            Some(tui_color!(hex "#888888"))
                        } else {
                            Some(tui_color!(hex "#00FF00"))
                        };

                        use unicode_segmentation::UnicodeSegmentation;

                        // Render each line - iterate until get_line_content returns None
                        let mut line_idx = 0;
                        let mut num_lines = 0;
                        loop {
                            if let Some(line_content) = lines.get_line_content(row(line_idx)) {
                                if line_idx > 0 {
                                    // Move to next line with same indentation
                                    *render_ops += RenderOpCommon::MoveCursorPositionAbs(
                                        Pos::from((col(start_col), row(start_row + line_idx))),
                                    );
                                }

                                let graphemes: Vec<&str> = line_content.graphemes(true).collect();

                                if line_idx == caret_row {
                                    // This is the line with the cursor
                                    // Render graphemes before cursor
                                    for grapheme in
                                        graphemes.iter().take(caret_col.min(graphemes.len()))
                                    {
                                        Self::render_text_simple(
                                            render_ops,
                                            grapheme,
                                            adjusted_fg,
                                            *bg_color,
                                        );
                                    }

                                    // Render cursor character
                                    let cursor_char: String = graphemes
                                        .get(caret_col)
                                        .map(|g| (*g).to_string())
                                        .unwrap_or_else(|| DEFAULT_CURSOR_CHAR.to_string());
                                    let cursor_style = new_style!(reverse);
                                    let cursor_texts = tui_styled_texts! {
                                        tui_styled_text! {
                                            @style: cursor_style,
                                            @text: &cursor_char
                                        },
                                    };
                                    render_tui_styled_texts_into(&cursor_texts, render_ops);

                                    // Render graphemes after cursor
                                    let start_idx = if caret_col < graphemes.len() {
                                        caret_col + 1
                                    } else {
                                        graphemes.len()
                                    };
                                    for grapheme in graphemes.iter().skip(start_idx) {
                                        Self::render_text_simple(
                                            render_ops,
                                            grapheme,
                                            adjusted_fg,
                                            *bg_color,
                                        );
                                    }
                                } else {
                                    // Regular line without cursor - render normally
                                    Self::render_text_simple(
                                        render_ops,
                                        line_content,
                                        adjusted_fg,
                                        *bg_color,
                                    );
                                }

                                line_idx += 1;
                                num_lines += 1;
                            } else {
                                break;
                            }
                        }

                        let ending_col = start_col
                            + lines.get_line_content(row(0)).map(|l| l.len()).unwrap_or(0);
                        (num_lines.max(1), ending_col) // Return rows consumed and ending column
                    } else {
                        // Fallback if no buffer
                        let adjusted_fg = if !is_focused {
                            Some(tui_color!(hex "#888888"))
                        } else {
                            Some(tui_color!(hex "#00FF00"))
                        };
                        Self::render_text_simple(render_ops, content, adjusted_fg, *bg_color);
                        (
                            1,
                            start_col + content.lines().next().map(|l| l.len()).unwrap_or(0),
                        )
                    }
                } else {
                    // Not editing - render multi-line content with proper indentation
                    let adjusted_fg = if !is_focused {
                        Some(tui_color!(hex "#888888"))
                    } else {
                        *fg_color
                    };
                    let lines: Vec<&str> = content.split('\n').collect();
                    for (line_idx, line) in lines.iter().enumerate() {
                        if line_idx > 0 {
                            // Move to next line with same indentation
                            *render_ops += RenderOpCommon::MoveCursorPositionAbs(Pos::from((
                                col(start_col),
                                row(start_row + line_idx),
                            )));
                        }
                        Self::render_text_simple(render_ops, line, adjusted_fg, *bg_color);
                    }
                    let ending_col =
                        start_col + content.lines().next().map(|l| l.len()).unwrap_or(0);
                    (lines.len().max(1), ending_col) // Return rows consumed and ending column
                }
            }
            UIElement::Row { children } => {
                let mut max_rows = 1;
                let mut current_col = start_col;
                for child in children {
                    // Render child starting at current column position
                    let (rows, ending_col) = Self::render_element(
                        child,
                        render_ops,
                        is_focused,
                        is_editing,
                        editing_buffer,
                        current_col,
                        start_row,
                    );
                    max_rows = max_rows.max(rows);
                    // Advance column position for next child
                    current_col = ending_col;
                }
                (max_rows, current_col) // Return max rows consumed and ending column
            }
        }
    }

    /// Render styled text with optional foreground and background colors
    /// Note: Cursor position should be set before calling this
    fn render_text_simple(
        render_ops: &mut RenderOpIRVec,
        text: &str,
        fg_color: Option<TuiColor>,
        bg_color: Option<TuiColor>,
    ) {
        let fg = fg_color.unwrap_or_else(|| tui_color!(hex "#CCCCCC"));
        let bg = bg_color.unwrap_or_else(|| tui_color!(hex "#000000"));

        let styled_texts = tui_styled_texts! {
            tui_styled_text! {
                @style: new_style!(color_fg: {fg} color_bg: {bg}),
                @text: text
            },
        };

        render_tui_styled_texts_into(&styled_texts, render_ops);
    }

    /// Evaluate an expression against row data
    fn eval_expr(expr: &RenderExpr, row: &HashMap<String, Value>) -> Option<Value> {
        match expr {
            RenderExpr::ColumnRef { name } => {
                // Column names are normalized in the compiler (this. prefix is stripped)
                row.get(name).cloned()
            }
            RenderExpr::Literal { value } => Some(value.clone()),
            RenderExpr::BinaryOp { op, left, right } => {
                let left_val = Self::eval_expr(left, row)?;
                let right_val = Self::eval_expr(right, row)?;

                match op {
                    BinaryOperator::Eq => Some(Value::Boolean(left_val == right_val)),
                    BinaryOperator::Neq => Some(Value::Boolean(left_val != right_val)),
                    BinaryOperator::Gt => {
                        if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                            Some(Value::Boolean(l > r))
                        } else {
                            None
                        }
                    }
                    BinaryOperator::Lt => {
                        if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                            Some(Value::Boolean(l < r))
                        } else {
                            None
                        }
                    }
                    BinaryOperator::Gte => {
                        if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                            Some(Value::Boolean(l >= r))
                        } else {
                            None
                        }
                    }
                    BinaryOperator::Lte => {
                        if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                            Some(Value::Boolean(l <= r))
                        } else {
                            None
                        }
                    }
                    BinaryOperator::And => {
                        if let (Some(l), Some(r)) = (left_val.as_bool(), right_val.as_bool()) {
                            Some(Value::Boolean(l && r))
                        } else {
                            None
                        }
                    }
                    BinaryOperator::Or => {
                        if let (Some(l), Some(r)) = (left_val.as_bool(), right_val.as_bool()) {
                            Some(Value::Boolean(l || r))
                        } else {
                            None
                        }
                    }
                    BinaryOperator::Add => {
                        if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                            Some(Value::Integer(l + r))
                        } else {
                            None
                        }
                    }
                    BinaryOperator::Sub => {
                        if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                            Some(Value::Integer(l - r))
                        } else {
                            None
                        }
                    }
                    BinaryOperator::Mul => {
                        if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                            Some(Value::Integer(l * r))
                        } else {
                            None
                        }
                    }
                    BinaryOperator::Div => {
                        if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                            if r != 0 {
                                Some(Value::Integer(l / r))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                }
            }
            _ => None,
        }
    }

    /// Convert Value to String
    fn value_to_string(value: &Value) -> String {
        match value {
            Value::String(s) => s.clone(),
            Value::Integer(n) => n.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Null => "".to_string(),
            Value::Json(j) => j.to_string(),
            Value::DateTime(dt) => dt.clone(),
            Value::Reference(r) => r.clone(),
            Value::Float(f) => f.to_string(),
            Value::Array(arr) => serde_json::to_string(arr).unwrap_or_default(),
            Value::Object(obj) => serde_json::to_string(obj).unwrap_or_default(),
        }
    }

    /// Convert Value to bool, handling SQLite's integer representation (0=false, 1=true)
    fn value_to_bool(value: &Value) -> Option<bool> {
        match value {
            Value::Boolean(b) => Some(*b),
            Value::Integer(i) => Some(*i != 0),
            _ => None,
        }
    }

    /// Extract column names from sort_by array expression
    fn extract_sort_columns(expr: &RenderExpr) -> Option<Vec<String>> {
        match expr {
            RenderExpr::Array { items } => {
                let columns: Vec<String> = items
                    .iter()
                    .filter_map(|item| match item {
                        RenderExpr::ColumnRef { name } => Some(name.clone()),
                        _ => None,
                    })
                    .collect();
                Some(columns)
            }
            _ => None,
        }
    }

    /// Compare two rows by multiple columns for sorting
    /// NULL values are treated as less than any non-NULL value
    fn compare_rows(
        a: &HashMap<String, Value>,
        b: &HashMap<String, Value>,
        columns: &[String],
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        for col in columns {
            let a_val = a.get(col);
            let b_val = b.get(col);

            let cmp = match (a_val, b_val) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less, // NULL < any value
                (Some(_), None) => Ordering::Greater, // any value > NULL
                (Some(a_v), Some(b_v)) => Self::compare_values(a_v, b_v),
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }

        Ordering::Equal
    }

    /// Compare two Values for sorting
    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            (Value::String(a_s), Value::String(b_s)) => a_s.cmp(b_s),
            (Value::Integer(a_i), Value::Integer(b_i)) => a_i.cmp(b_i),
            (Value::Boolean(a_b), Value::Boolean(b_b)) => a_b.cmp(b_b),

            // Mixed types: convert to strings for comparison
            _ => Self::value_to_string(a).cmp(&Self::value_to_string(b)),
        }
    }

    /// Hierarchical tree sort using depth-first traversal
    ///
    /// columns: [parent_col, sort_col] - typically [parent_id, sort_key]
    /// Returns items in depth-first order: parent, then all children sorted by sort_col
    fn hierarchical_sort<'a>(
        data: &'a [HashMap<String, Value>],
        columns: &[String],
    ) -> Vec<&'a HashMap<String, Value>> {
        if columns.len() != 2 {
            // Fallback to unsorted if columns aren't [parent, sort]
            return data.iter().collect();
        }

        let parent_col = &columns[0];
        let sort_col = &columns[1];

        // Build parent -> children mapping
        let mut children_map: HashMap<Option<String>, Vec<&HashMap<String, Value>>> =
            HashMap::new();

        for row in data {
            let parent_id = row
                .get(parent_col)
                .and_then(|v| v.as_string())
                .map(|s| s.to_string());

            children_map.entry(parent_id).or_default().push(row);
        }

        // Sort each parent's children by sort_col
        for children in children_map.values_mut() {
            children.sort_by(|a, b| {
                let a_sort = a.get(sort_col);
                let b_sort = b.get(sort_col);

                match (a_sort, b_sort) {
                    (None, None) => std::cmp::Ordering::Equal,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (Some(a_v), Some(b_v)) => Self::compare_values(a_v, b_v),
                }
            });
        }

        // Depth-first traversal starting from roots (parent_id = NULL)
        let mut result = Vec::new();
        Self::visit_children(&mut result, &children_map, None, parent_col);

        result
    }

    /// Recursively visit children in depth-first order
    fn visit_children<'a>(
        result: &mut Vec<&'a HashMap<String, Value>>,
        children_map: &HashMap<Option<String>, Vec<&'a HashMap<String, Value>>>,
        parent_id: Option<String>,
        id_col: &str,
    ) {
        if let Some(children) = children_map.get(&parent_id) {
            for child in children {
                result.push(child);

                // Recursively visit this child's children
                let child_id = child
                    .get("id")
                    .and_then(|v| v.as_string())
                    .map(|s| s.to_string());

                Self::visit_children(result, children_map, child_id, id_col);
            }
        }
    }
}
