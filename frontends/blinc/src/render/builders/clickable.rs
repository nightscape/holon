use std::collections::HashMap;

use blinc_app::prelude::*;

use crate::render::context::RenderContext;
use crate::render::interpreter::{eval_to_value, interpret, ResolvedArgs};

/// clickable(child_expr, action:(entity.operation param1:val1 ...))
///
/// Wraps a child element with an on_click handler that dispatches an operation.
pub fn build(args: &ResolvedArgs, ctx: &RenderContext) -> Div {
    // The first positional arg is a child expression (FunctionCall like `row(...)`)
    let child = if let Some(child_expr) = args.positional_exprs.first() {
        interpret(child_expr, ctx)
    } else {
        div()
    };

    let action_expr = match args.get_template("action") {
        Some(expr) => expr,
        None => return child,
    };

    if let holon_api::render_types::RenderExpr::FunctionCall {
        name,
        args: action_args,
        ..
    } = action_expr
    {
        let parts: Vec<&str> = name.split('.').collect();
        if parts.len() == 2 {
            let entity_name = parts[0].to_string();
            let op_name = parts[1].to_string();

            let mut params = HashMap::new();
            for arg in action_args {
                if let Some(ref param_name) = arg.name {
                    let value = eval_to_value(&arg.value, ctx);
                    params.insert(param_name.clone(), value);
                }
            }

            let session = ctx.session.clone();
            let handle = ctx.runtime_handle.clone();
            return child.on_click(move |_| {
                crate::operations::dispatch_operation(
                    &handle,
                    &session,
                    entity_name.clone(),
                    op_name.clone(),
                    params.clone(),
                );
            });
        }
    }

    child
}
