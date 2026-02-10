use blinc_app::prelude::*;
use blinc_theme::{ColorToken, ThemeState};

use crate::render::context::RenderContext;
use crate::render::interpreter::{interpret, ResolvedArgs};

pub fn build(args: &ResolvedArgs, ctx: &RenderContext) -> Div {
    let title = args
        .get_positional_string(0)
        .or(args.get_string("title"))
        .unwrap_or("Section")
        .to_string();

    let theme = ThemeState::get();
    let mut container = div()
        .flex_col()
        .gap(8.0)
        .p(16.0)
        .rounded(8.0)
        .bg(theme.color(ColorToken::Surface));

    container = container.child(
        text(title)
            .size(18.0)
            .semibold()
            .color(theme.color(ColorToken::TextPrimary)),
    );

    if let Some(tmpl) = args
        .get_template("item_template")
        .or(args.get_template("item"))
    {
        if ctx.data_rows.is_empty() {
            container = container.child(interpret(tmpl, ctx));
        } else {
            for row in &ctx.data_rows {
                let row_ctx = ctx.with_row(row.clone());
                container = container.child(interpret(tmpl, &row_ctx));
            }
        }
    }

    container
}
