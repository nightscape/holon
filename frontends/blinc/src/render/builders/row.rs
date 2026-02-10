use blinc_app::prelude::*;
use blinc_theme::{ColorToken, ThemeState};

use crate::render::context::RenderContext;
use crate::render::interpreter::{interpret, ResolvedArgs};

pub fn build(args: &ResolvedArgs, ctx: &RenderContext) -> Div {
    let gap = args.get_f64("gap").unwrap_or(8.0) as f32;

    let mut container = div().flex_row().gap(gap).items_center();

    if let Some(template) = args
        .get_template("item_template")
        .or(args.get_template("item"))
    {
        container = container.child(interpret(template, ctx));
    }

    for val in &args.positional {
        if let holon_api::Value::String(s) = val {
            container = container.child(
                text(s.clone())
                    .size(14.0)
                    .color(ThemeState::get().color(ColorToken::TextPrimary)),
            );
        }
    }

    container
}
