use blinc_app::prelude::*;
use blinc_theme::{ColorToken, ThemeState};

use crate::render::context::RenderContext;
use crate::render::interpreter::ResolvedArgs;

pub fn build(args: &ResolvedArgs, _ctx: &RenderContext) -> Div {
    let checked = args.get_bool("checked").unwrap_or(false);

    let theme = ThemeState::get();
    let symbol = if checked { "[x]" } else { "[ ]" };
    let color = if checked {
        theme.color(ColorToken::Success)
    } else {
        theme.color(ColorToken::TextSecondary)
    };

    div().child(text(symbol).size(14.0).color(color))
}
