use blinc_app::prelude::*;
use blinc_theme::{ColorToken, ThemeState};

use crate::render::context::RenderContext;
use crate::render::interpreter::ResolvedArgs;

static ICONS: &[(&str, &str)] = include!(concat!(env!("OUT_DIR"), "/embedded_icons.rs"));

pub fn build(args: &ResolvedArgs, _ctx: &RenderContext) -> Div {
    let name = args
        .get_positional_string(0)
        .or(args.get_string("name"))
        .unwrap_or("circle")
        .to_string();

    let size = args.get_f64("size").unwrap_or(20.0) as f32;

    match ICONS.iter().find(|(n, _)| *n == name) {
        Some((_, data_uri)) => div().child(img(*data_uri).size(size, size)),
        None => div().child(
            text(format!("[{name}]"))
                .size(size * 0.7)
                .color(ThemeState::get().color(ColorToken::TextSecondary)),
        ),
    }
}
