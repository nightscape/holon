mod badge;
mod block;
mod checkbox;
mod clickable;
mod columns;
mod icon;
mod list;
pub(crate) mod live_query;
mod render_block;
mod row;
mod section;
mod spacer;
mod text;
mod tree;

use blinc_app::prelude::*;
use blinc_theme::{ColorToken, ThemeState};

use super::context::RenderContext;
use super::interpreter::ResolvedArgs;

/// Build a Blinc Div from a render function name and resolved arguments.
pub fn build(name: &str, args: &ResolvedArgs, ctx: &RenderContext) -> Div {
    match name {
        "text" => text::build(args, ctx),
        "row" => row::build(args, ctx),
        "block" => block::build(args, ctx),
        "list" => list::build(args, ctx),
        "columns" => columns::build(args, ctx),
        "section" => section::build(args, ctx),
        "spacer" => spacer::build(args, ctx),
        "icon" => icon::build(args, ctx),
        "checkbox" => checkbox::build(args, ctx),
        "badge" => badge::build(args, ctx),
        "live_query" => live_query::build(args, ctx),
        "render_block" => render_block::build(args, ctx),
        "clickable" => clickable::build(args, ctx),
        "tree" => tree::build(args, ctx),
        _ => {
            tracing::warn!("Unknown builder: {name}");
            div().flex_row().gap(4.0).child(
                blinc_app::prelude::text(format!("[unknown: {name}]"))
                    .size(12.0)
                    .color(ThemeState::get().color(ColorToken::TextSecondary)),
            )
        }
    }
}
