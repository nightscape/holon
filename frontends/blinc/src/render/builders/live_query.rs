use std::collections::HashMap;

use blinc_app::prelude::*;
use blinc_theme::{ColorToken, ThemeState};

use crate::render::context::RenderContext;
use crate::render::interpreter::{interpret, ResolvedArgs};

/// live_query builder invoked from PRQL render expressions.
pub fn build(args: &ResolvedArgs, ctx: &RenderContext) -> Div {
    let prql = args.get_string("prql").unwrap_or("").to_string();
    let context_id = args
        .get_string("context")
        .map(|s| s.to_string())
        .or_else(|| {
            ctx.row()
                .get("id")
                .and_then(|v| v.as_string())
                .map(|s| s.to_string())
        });

    build_prql(prql, context_id, ctx)
}

/// Execute a PRQL query synchronously and render the results.
///
/// Called from both `live_query` and `render_block` (for PRQL source blocks).
/// Runs on a separate OS thread to avoid blocking the tokio runtime.
const MAX_QUERY_DEPTH: usize = 10;

pub fn build_prql(prql: String, context_id: Option<String>, ctx: &RenderContext) -> Div {
    let theme = ThemeState::get();

    if ctx.query_depth >= MAX_QUERY_DEPTH {
        tracing::error!(
            query_depth = ctx.query_depth,
            prql = %prql,
            "Render query recursion depth exceeded {MAX_QUERY_DEPTH} — likely a cycle",
        );
        return div()
            .p(4.0)
            .rounded(4.0)
            .bg(theme.color(ColorToken::ErrorBg))
            .child(
                text(format!(
                    "[query recursion limit reached (depth {})]",
                    ctx.query_depth
                ))
                .size(12.0)
                .color(theme.color(ColorToken::Error)),
            );
    }

    if prql.is_empty() {
        return div().child(
            text("[empty query]")
                .size(12.0)
                .color(theme.color(ColorToken::TextSecondary)),
        );
    }

    let query_context = context_id.map(|id| holon_frontend::QueryContext {
        current_block_id: Some(id.clone()),
        context_parent_id: Some(id),
        context_path_prefix: None,
    });

    let session = ctx.session.clone();
    let handle = ctx.runtime_handle.clone();
    let result = std::thread::scope(|s| {
        s.spawn(|| handle.block_on(session.query_and_watch(prql, HashMap::new(), query_context)))
            .join()
            .unwrap()
    });

    let deeper_ctx = ctx.deeper_query();

    match result {
        Ok((widget_spec, _stream)) => {
            // TODO: spawn CDC listener for the stream to get reactive updates
            let root_expr = widget_spec.render_spec.root();
            match root_expr {
                Some(expr) => {
                    let query_ctx = deeper_ctx.with_data_rows(widget_spec.data.clone());
                    interpret(expr, &query_ctx)
                }
                None => div().child(
                    text("[no render spec]")
                        .size(12.0)
                        .color(theme.color(ColorToken::TextSecondary)),
                ),
            }
        }
        Err(e) => div()
            .p(4.0)
            .rounded(4.0)
            .bg(theme.color(ColorToken::ErrorBg))
            .child(
                text(format!("Query error: {e}"))
                    .size(12.0)
                    .color(theme.color(ColorToken::Error)),
            ),
    }
}
