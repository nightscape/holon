use blinc_app::prelude::*;
use blinc_theme::{ColorToken, ThemeState};

use crate::render::context::RenderContext;
use crate::render::interpreter::ResolvedArgs;

pub fn build(args: &ResolvedArgs, _ctx: &RenderContext) -> Div {
    let label = args
        .get_positional_string(0)
        .or(args.get_string("label"))
        .unwrap_or("")
        .to_string();

    let theme = ThemeState::get();
    div()
        .px(8.0)
        .py(2.0)
        .rounded(12.0)
        .bg(theme.color(ColorToken::SurfaceElevated))
        .child(
            text(label)
                .size(11.0)
                .color(theme.color(ColorToken::TextSecondary)),
        )
}
