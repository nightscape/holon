use blinc_app::prelude::*;
use blinc_theme::{ColorToken, ThemeState};

use crate::render::context::RenderContext;
use crate::render::interpreter::{interpret, ResolvedArgs};

/// tree(parent_id:parent_id sortkey:id item_template:(render_block this))
///
/// Renders data as a flat list for now (hierarchical tree rendering
/// requires row cache and parent-child indexing, which will come later).
pub fn build(args: &ResolvedArgs, ctx: &RenderContext) -> Div {
    let template = args
        .get_template("item_template")
        .or(args.get_template("item"));

    let mut container = div().flex_col().gap(2.0);

    match template {
        Some(tmpl) => {
            if ctx.data_rows.is_empty() {
                container = container.child(interpret(tmpl, ctx));
            } else {
                for row in &ctx.data_rows {
                    let row_ctx = ctx.with_row(row.clone());
                    container = container.child(interpret(tmpl, &row_ctx));
                }
            }
        }
        None => {
            container = container.child(
                text("[tree: no item_template]")
                    .size(12.0)
                    .color(ThemeState::get().color(ColorToken::TextSecondary)),
            );
        }
    }

    container
}
