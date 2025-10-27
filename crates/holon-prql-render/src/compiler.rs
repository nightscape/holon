use crate::parser::ExtractedRender;
use crate::types::*;
use anyhow::{bail, Context, Result};
use holon_api::Value;
use prqlc::pr::*;
use std::collections::HashMap;

/// Compile ExtractedRender into a RenderSpec
pub fn compile_render_spec_from_extracted(extracted: &ExtractedRender) -> Result<RenderSpec> {
    match extracted {
        ExtractedRender::Single(expr) => {
            let json = crate::parser::prql_ast_to_json(expr)?;
            compile_render_spec(&json)
        }
        ExtractedRender::Named(named_views) => {
            let mut views = HashMap::new();
            let mut default_view = None;

            for (name, expr) in named_views {
                let view_json = crate::parser::prql_ast_to_json(expr)?;
                let view_spec = compile_view_spec(&view_json)?;

                if default_view.is_none() {
                    default_view = Some(name.clone());
                }
                views.insert(name.clone(), view_spec);
            }

            // For multi-view queries, set root to default view's structure for backward compatibility
            let root = if let Some(default_name) = &default_view {
                views
                    .get(default_name)
                    .map(|v| v.structure.clone())
                    .unwrap_or_else(|| RenderExpr::FunctionCall {
                        name: "list".to_string(),
                        args: vec![],
                        operations: vec![],
                    })
            } else {
                RenderExpr::FunctionCall {
                    name: "list".to_string(),
                    args: vec![],
                    operations: vec![],
                }
            };

            Ok(RenderSpec {
                views,
                default_view: default_view.unwrap_or_else(|| "default".to_string()),
                root,
                nested_queries: vec![],
                operations: HashMap::new(),
                row_templates: vec![],
            })
        }
    }
}

pub fn compile_render_spec(render_call: &Value) -> Result<RenderSpec> {
    let ui_expr = if let Some(obj) = render_call.as_object() {
        if obj.get("__fn").and_then(|v| v.as_string_owned()) == Some("render".to_string()) {
            obj.get("arg0")
                .context("render() requires at least one argument")?
        } else {
            render_call
        }
    } else {
        render_call
    };

    let root = compile_render_expr(ui_expr)?;

    // For single-view queries, keep views empty and use root field
    // This maintains backward compatibility
    Ok(RenderSpec {
        views: HashMap::new(),
        default_view: "default".to_string(),
        root,
        nested_queries: vec![],
        operations: HashMap::new(), // Removed - not used anymore
        row_templates: vec![],      // Populated by parser for derive { ui = (render ...) } queries
    })
}

/// Compile a view specification from JSON
/// A view spec is e.g. (list filter:(this.region == "sidebar") item_template:this.ui)
fn compile_view_spec(view_json: &Value) -> Result<ViewSpec> {
    if let Some(obj) = view_json.as_object() {
        // Extract filter if present
        let filter = obj
            .get("filter")
            .map(|f| compile_filter_expr(f))
            .transpose()?;

        // Compile the structure (the function call itself, e.g. list, tree, etc.)
        // We need to remove filter from the args before compiling
        // Reconstruct the object without the filter field
        let mut structure_obj = std::collections::HashMap::new();
        for (key, value) in obj.iter() {
            if key != "filter" {
                structure_obj.insert(key.clone(), value.clone());
            }
        }
        let structure_json = Value::Object(structure_obj);
        let structure = compile_render_expr(&structure_json)?;

        Ok(ViewSpec { filter, structure })
    } else {
        bail!("Expected object for view spec");
    }
}

/// Compile a filter expression from JSON
fn compile_filter_expr(filter_json: &Value) -> Result<FilterExpr> {
    // Check if it's a binary operation (e.g., this.region == "sidebar")
    if let Some(obj) = filter_json.as_object() {
        if let Some(op_str) = obj.get("__op").and_then(|v| v.as_string_owned()) {
            match op_str.as_str() {
                "Eq" => {
                    let left = obj.get("left").context("Binary operation missing 'left'")?;
                    let right = obj
                        .get("right")
                        .context("Binary operation missing 'right'")?;

                    let column = extract_column_name(left)?;
                    let value = extract_literal_value(right)?;

                    Ok(FilterExpr::Eq { column, value })
                }
                "Ne" | "Neq" => {
                    let left = obj.get("left").context("Binary operation missing 'left'")?;
                    let right = obj
                        .get("right")
                        .context("Binary operation missing 'right'")?;

                    let column = extract_column_name(left)?;
                    let value = extract_literal_value(right)?;

                    Ok(FilterExpr::Ne { column, value })
                }
                "And" => {
                    // For now, we'll handle simple cases
                    // Complex AND/OR can be added later
                    bail!("Complex filter expressions not yet supported")
                }
                "Or" => {
                    bail!("Complex filter expressions not yet supported")
                }
                _ => bail!("Unsupported filter operator: {}", op_str),
            }
        } else {
            // Check if it's a literal "true" (All filter)
            if let Some(b) = filter_json.as_bool() {
                if b {
                    return Ok(FilterExpr::All);
                }
            }
            bail!("Unsupported filter expression format")
        }
    } else {
        bail!("Expected object for filter expression")
    }
}

/// Extract column name from a column reference expression
fn extract_column_name(expr: &Value) -> Result<String> {
    if let Some(s) = expr.as_string() {
        if let Some(col_name) = s.strip_prefix("$col:") {
            // Strip "this." prefix if present
            let normalized = col_name.strip_prefix("this.").unwrap_or(col_name);
            return Ok(normalized.to_string());
        }
    }
    bail!("Expected column reference in filter")
}

/// Extract literal value from an expression
fn extract_literal_value(expr: &Value) -> Result<Value> {
    // Return holon_api::Value directly (no conversion needed)
    match expr {
        _ => Ok(expr.clone()),
    }
}

/// Compile a render expression from JSON, handling the render() wrapper if present.
///
/// This is used for compiling row templates extracted from derive { ui = (render ...) }.
pub fn compile_render_expr_from_json(render_call: &Value) -> Result<RenderExpr> {
    // Unwrap the render() call if present
    let ui_expr = if let Some(obj) = render_call.as_object() {
        if obj.get("__fn").and_then(|v| v.as_string_owned()) == Some("render".to_string()) {
            obj.get("arg0")
                .context("render() requires at least one argument")?
        } else {
            render_call
        }
    } else {
        render_call
    };

    compile_render_expr(ui_expr)
}

fn compile_render_expr(value: &Value) -> Result<RenderExpr> {
    match value {
        Value::String(s) => {
            if let Some(col_name) = s.strip_prefix("$col:") {
                // Strip "this." prefix if present (PRQL syntax for current row)
                // This normalizes column references so frontends don't need to handle it
                let normalized_name = col_name.strip_prefix("this.").unwrap_or(col_name);
                Ok(RenderExpr::ColumnRef {
                    name: normalized_name.to_string(),
                })
            } else {
                Ok(RenderExpr::Literal {
                    value: value.clone(),
                })
            }
        }
        Value::Integer(_)
        | Value::Float(_)
        | Value::Boolean(_)
        | Value::Null
        | Value::DateTime(_)
        | Value::Json(_) => Ok(RenderExpr::Literal {
            value: value.clone(),
        }),
        Value::Array(arr) => {
            let items: Result<Vec<_>> = arr.iter().map(compile_render_expr).collect();
            Ok(RenderExpr::Array { items: items? })
        }
        Value::Object(obj) => {
            if let Some(func_name) = obj.get("__fn").and_then(|v| v.as_string_owned()) {
                let mut args = vec![];

                for i in 0.. {
                    let key = format!("arg{}", i);
                    if let Some(arg_value) = obj.get(&key) {
                        args.push(Arg {
                            name: None,
                            value: compile_render_expr(arg_value)?,
                        });
                    } else {
                        break;
                    }
                }

                for (key, value) in obj.iter() {
                    if key != "__fn" && !key.starts_with("arg") {
                        args.push(Arg {
                            name: Some(key.clone()),
                            value: compile_render_expr(value)?,
                        });
                    }
                }

                Ok(RenderExpr::FunctionCall {
                    name: func_name,
                    args,
                    operations: vec![], // Filled in by lineage analysis
                })
            } else if let Some(op_name) = obj.get("__op").and_then(|v| v.as_string_owned()) {
                let left = obj.get("left").context("Binary operation missing 'left'")?;
                let right = obj
                    .get("right")
                    .context("Binary operation missing 'right'")?;

                let op = match op_name.as_str() {
                    "Eq" => BinaryOperator::Eq,
                    "Neq" | "Ne" => BinaryOperator::Neq,
                    "Gt" => BinaryOperator::Gt,
                    "Lt" => BinaryOperator::Lt,
                    "Gte" | "Ge" => BinaryOperator::Gte,
                    "Lte" | "Le" => BinaryOperator::Lte,
                    "Add" => BinaryOperator::Add,
                    "Sub" => BinaryOperator::Sub,
                    "Mul" => BinaryOperator::Mul,
                    "Div" => BinaryOperator::Div,
                    "And" => BinaryOperator::And,
                    "Or" => BinaryOperator::Or,
                    other => bail!("Unsupported binary operator: {}", other),
                };

                Ok(RenderExpr::BinaryOp {
                    op,
                    left: Box::new(compile_render_expr(left)?),
                    right: Box::new(compile_render_expr(right)?),
                })
            } else {
                let mut fields = HashMap::new();
                for (key, value) in obj.iter() {
                    fields.insert(key.clone(), compile_render_expr(value)?);
                }
                Ok(RenderExpr::Object { fields })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json_to_value(v: serde_json::Value) -> Value {
        Value::from_json_value(v)
    }

    #[test]
    fn test_compile_simple_text() {
        let json = serde_json::json!({
            "__fn": "text",
            "arg0": "Hello"
        });

        let spec = compile_render_spec(&json_to_value(serde_json::json!({
            "__fn": "render",
            "arg0": json
        })))
        .unwrap();

        match spec.root() {
            Some(RenderExpr::FunctionCall {
                name,
                args,
                operations,
            }) => {
                assert_eq!(name, "text");
                assert_eq!(args.len(), 1);
                assert!(operations.is_empty());
                match &args[0].value {
                    RenderExpr::Literal { value } => {
                        assert_eq!(value.as_string(), Some("Hello"));
                    }
                    _ => panic!("Expected literal"),
                }
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_compile_with_column_ref() {
        let json = serde_json::json!({
            "__fn": "text",
            "content": "$col:title"
        });

        let spec = compile_render_spec(&json_to_value(serde_json::json!({
            "__fn": "render",
            "arg0": json
        })))
        .unwrap();

        match spec.root() {
            Some(RenderExpr::FunctionCall { name, args, .. }) => {
                assert_eq!(name, "text");
                assert_eq!(args.len(), 1);
                assert_eq!(args[0].name, Some("content".to_string()));
                match &args[0].value {
                    RenderExpr::ColumnRef { name } => {
                        assert_eq!(name, "title");
                    }
                    _ => panic!("Expected column ref"),
                }
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_compile_nested_calls() {
        let json = serde_json::json!({
            "__fn": "row",
            "arg0": {
                "__fn": "text",
                "arg0": "A"
            },
            "arg1": {
                "__fn": "text",
                "arg0": "B"
            }
        });

        let spec = compile_render_spec(&json_to_value(serde_json::json!({
            "__fn": "render",
            "arg0": json
        })))
        .unwrap();

        match spec.root() {
            Some(RenderExpr::FunctionCall { name, args, .. }) => {
                assert_eq!(name, "row");
                assert_eq!(args.len(), 2);
                for arg in args {
                    match &arg.value {
                        RenderExpr::FunctionCall { name, .. } => {
                            assert_eq!(name, "text");
                        }
                        _ => panic!("Expected function call"),
                    }
                }
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_compile_binary_op() {
        let json = json_to_value(serde_json::json!({
            "__op": "Mul",
            "left": "$col:depth",
            "right": 24
        }));

        let expr = compile_render_expr(&json).unwrap();

        match expr {
            RenderExpr::BinaryOp { op, left, right } => {
                assert!(matches!(op, BinaryOperator::Mul));
                match *left {
                    RenderExpr::ColumnRef { ref name } => {
                        assert_eq!(name, "depth");
                    }
                    _ => panic!("Expected column ref"),
                }
                match *right {
                    RenderExpr::Literal { ref value } => {
                        assert_eq!(value.as_i64(), Some(24));
                    }
                    _ => panic!("Expected literal"),
                }
            }
            _ => panic!("Expected binary op"),
        }
    }

    #[test]
    fn test_compile_array() {
        let json = json_to_value(serde_json::json!(["A", "B", "C"]));

        let expr = compile_render_expr(&json).unwrap();

        match expr {
            RenderExpr::Array { items } => {
                assert_eq!(items.len(), 3);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_compile_object() {
        let json = json_to_value(serde_json::json!({
            "key1": "value1",
            "key2": 42
        }));

        let expr = compile_render_expr(&json).unwrap();

        match expr {
            RenderExpr::Object { fields } => {
                assert_eq!(fields.len(), 2);
                assert!(fields.contains_key("key1"));
                assert!(fields.contains_key("key2"));
            }
            _ => panic!("Expected object"),
        }
    }
}
