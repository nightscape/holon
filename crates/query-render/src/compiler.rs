use crate::types::*;
use anyhow::{bail, Context, Result};
use holon_api::Value;
use std::collections::HashMap;

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

    Ok(RenderSpec {
        root,
        nested_queries: vec![],
        operations: HashMap::new(), // Removed - not used anymore
        row_templates: vec![],      // Populated by parser for derive { ui = (render ...) } queries
    })
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
        | Value::Json(_)
        | Value::Reference(_) => Ok(RenderExpr::Literal {
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

        match spec.root {
            RenderExpr::FunctionCall {
                name,
                args,
                operations,
            } => {
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

        match spec.root {
            RenderExpr::FunctionCall { name, args, .. } => {
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

        match spec.root {
            RenderExpr::FunctionCall { name, args, .. } => {
                assert_eq!(name, "row");
                assert_eq!(args.len(), 2);
                for arg in &args {
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
