use std::collections::HashMap;

use blinc_app::prelude::*;
use blinc_theme::{ColorToken, ThemeState};
use holon_api::render_types::{Arg, BinaryOperator, RenderExpr};
use holon_api::Value;

use super::builders;
use super::context::RenderContext;

/// Interpret a RenderExpr into a Blinc Div element.
///
/// This is the core rendering function that recursively walks the RenderExpr tree
/// and produces Blinc UI elements. It mirrors the Flutter render interpreter pattern.
/// All builders return Div so they can be used as children of other Divs.
pub fn interpret(expr: &RenderExpr, ctx: &RenderContext) -> Div {
    let theme = ThemeState::get();
    match expr {
        RenderExpr::FunctionCall {
            name,
            args,
            operations,
        } => {
            let resolved = resolve_args(args, ctx);
            let child_ctx = RenderContext {
                data_rows: ctx.data_rows.clone(),
                operations: operations.clone(),
                session: ctx.session.clone(),
                runtime_handle: ctx.runtime_handle.clone(),
                depth: ctx.depth,
                query_depth: ctx.query_depth,
                is_screen_layout: ctx.is_screen_layout,
                sidebar_open: ctx.sidebar_open.clone(),
            };
            builders::build(name, &resolved, &child_ctx)
        }
        RenderExpr::ColumnRef { name } => {
            let value = ctx.row().get(name).cloned().unwrap_or(Value::Null);
            let display = value_to_string(&value);
            div().child(
                text(display)
                    .size(14.0)
                    .color(theme.color(ColorToken::TextPrimary)),
            )
        }
        RenderExpr::Literal { value } => {
            let display = value_to_string(value);
            div().child(
                text(display)
                    .size(14.0)
                    .color(theme.color(ColorToken::TextPrimary)),
            )
        }
        RenderExpr::BinaryOp { op, left, right } => {
            let l = eval_to_value(left, ctx);
            let r = eval_to_value(right, ctx);
            let result = eval_binary_op(op, &l, &r);
            let display = value_to_string(&result);
            div().child(
                text(display)
                    .size(14.0)
                    .color(theme.color(ColorToken::TextPrimary)),
            )
        }
        RenderExpr::Array { items } => {
            let mut container = div().flex_col();
            for item in items {
                container = container.child(interpret(item, ctx));
            }
            container
        }
        RenderExpr::Object { fields } => {
            let mut container = div().flex_col();
            for (_key, expr) in fields {
                container = container.child(interpret(expr, ctx));
            }
            container
        }
    }
}

/// Argument resolution result: named or positional values, plus template expressions.
pub struct ResolvedArgs {
    pub positional: Vec<Value>,
    /// Raw positional expressions (before evaluation) for builders that need
    /// to interpret children as UI elements rather than values.
    pub positional_exprs: Vec<RenderExpr>,
    pub named: HashMap<String, Value>,
    /// Template arguments are kept as expressions (not evaluated to Value)
    /// because they need to be interpreted per-row with different contexts.
    pub templates: HashMap<String, RenderExpr>,
}

impl ResolvedArgs {
    pub fn get_string(&self, name: &str) -> Option<&str> {
        self.named.get(name).and_then(|v| v.as_string())
    }

    #[allow(dead_code)]
    pub fn get_string_or(&self, name: &str, default: &str) -> String {
        self.get_string(name)
            .map(|s| s.to_string())
            .unwrap_or_else(|| default.to_string())
    }

    pub fn get_f64(&self, name: &str) -> Option<f64> {
        self.named.get(name).and_then(|v| match v {
            Value::Float(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            _ => None,
        })
    }

    pub fn get_bool(&self, name: &str) -> Option<bool> {
        self.named.get(name).and_then(|v| match v {
            Value::Boolean(b) => Some(*b),
            _ => None,
        })
    }

    pub fn get_positional_string(&self, index: usize) -> Option<&str> {
        self.positional.get(index).and_then(|v| v.as_string())
    }

    pub fn get_template(&self, name: &str) -> Option<&RenderExpr> {
        self.templates.get(name)
    }
}

/// Resolve function arguments into values and templates.
fn resolve_args(args: &[Arg], ctx: &RenderContext) -> ResolvedArgs {
    let mut positional = Vec::new();
    let mut positional_exprs = Vec::new();
    let mut named = HashMap::new();
    let mut templates = HashMap::new();

    for arg in args {
        match &arg.name {
            Some(name) if is_template_arg(name) => {
                templates.insert(name.clone(), arg.value.clone());
            }
            Some(name) => {
                named.insert(name.clone(), eval_to_value(&arg.value, ctx));
            }
            None => {
                positional_exprs.push(arg.value.clone());
                positional.push(eval_to_value(&arg.value, ctx));
            }
        }
    }

    ResolvedArgs {
        positional,
        positional_exprs,
        named,
        templates,
    }
}

fn is_template_arg(name: &str) -> bool {
    matches!(
        name,
        "item_template"
            | "item"
            | "header"
            | "header_template"
            | "child_template"
            | "action"
            | "parent_id"
            | "sortkey"
            | "sort_key"
            | "context"
    )
}

/// Evaluate a RenderExpr to a Value (for non-UI contexts like argument resolution).
pub fn eval_to_value(expr: &RenderExpr, ctx: &RenderContext) -> Value {
    match expr {
        RenderExpr::Literal { value } => value.clone(),
        RenderExpr::ColumnRef { name } => ctx.row().get(name).cloned().unwrap_or(Value::Null),
        RenderExpr::BinaryOp { op, left, right } => {
            let l = eval_to_value(left, ctx);
            let r = eval_to_value(right, ctx);
            eval_binary_op(op, &l, &r)
        }
        RenderExpr::FunctionCall { name, args, .. } => match name.as_str() {
            "concat" => {
                let resolved = resolve_args(args, ctx);
                let parts: Vec<String> = resolved.positional.iter().map(value_to_string).collect();
                Value::String(parts.join(""))
            }
            _ => {
                if let Some(first) = args.first() {
                    eval_to_value(&first.value, ctx)
                } else {
                    Value::Null
                }
            }
        },
        RenderExpr::Array { items } => {
            Value::Array(items.iter().map(|i| eval_to_value(i, ctx)).collect())
        }
        RenderExpr::Object { fields } => Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), eval_to_value(v, ctx)))
                .collect(),
        ),
    }
}

fn eval_binary_op(op: &BinaryOperator, left: &Value, right: &Value) -> Value {
    match op {
        BinaryOperator::Add => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a + b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
            (Value::String(a), Value::String(b)) => Value::String(format!("{a}{b}")),
            _ => Value::Null,
        },
        BinaryOperator::Sub => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a - b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
            _ => Value::Null,
        },
        BinaryOperator::Mul => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a * b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
            _ => Value::Null,
        },
        BinaryOperator::Div => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) if *b != 0 => Value::Integer(a / b),
            (Value::Float(a), Value::Float(b)) if *b != 0.0 => Value::Float(a / b),
            _ => Value::Null,
        },
        BinaryOperator::Eq => Value::Boolean(left == right),
        BinaryOperator::Neq => Value::Boolean(left != right),
        BinaryOperator::Gt => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Boolean(a > b),
            (Value::Float(a), Value::Float(b)) => Value::Boolean(a > b),
            _ => Value::Boolean(false),
        },
        BinaryOperator::Lt => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Boolean(a < b),
            (Value::Float(a), Value::Float(b)) => Value::Boolean(a < b),
            _ => Value::Boolean(false),
        },
        BinaryOperator::Gte => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Boolean(a >= b),
            (Value::Float(a), Value::Float(b)) => Value::Boolean(a >= b),
            _ => Value::Boolean(false),
        },
        BinaryOperator::Lte => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Value::Boolean(a <= b),
            (Value::Float(a), Value::Float(b)) => Value::Boolean(a <= b),
            _ => Value::Boolean(false),
        },
        BinaryOperator::And => match (left, right) {
            (Value::Boolean(a), Value::Boolean(b)) => Value::Boolean(*a && *b),
            _ => Value::Boolean(false),
        },
        BinaryOperator::Or => match (left, right) {
            (Value::Boolean(a), Value::Boolean(b)) => Value::Boolean(*a || *b),
            _ => Value::Boolean(false),
        },
    }
}

pub fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::DateTime(dt) => dt.clone(),
        Value::Null => String::new(),
        Value::Json(j) => j.clone(),
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(value_to_string).collect();
            parts.join(", ")
        }
        Value::Object(map) => serde_json::to_string(map).unwrap_or_default(),
    }
}
