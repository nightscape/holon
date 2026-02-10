use blinc_app::prelude::*;
use blinc_core::Color;
use blinc_theme::{ColorToken, ThemeState};

use crate::render::context::RenderContext;
use crate::render::interpreter::{value_to_string, ResolvedArgs};

pub fn build(args: &ResolvedArgs, _ctx: &RenderContext) -> Div {
    let content = args
        .get_positional_string(0)
        .map(|s| s.to_string())
        .or_else(|| args.get_string("content").map(|s| s.to_string()))
        .unwrap_or_else(|| {
            args.positional
                .first()
                .map(value_to_string)
                .unwrap_or_default()
        });

    let size = args.get_f64("size").unwrap_or(14.0) as f32;
    let bold = args.get_bool("bold").unwrap_or(false);
    let color = if let Some(c) = args.get_string("color") {
        parse_color(c)
    } else {
        ThemeState::get().color(ColorToken::TextPrimary)
    };

    let mut t = text(content).size(size).color(color);
    if bold {
        t = t.bold();
    }
    div().child(t)
}

pub fn parse_color(s: &str) -> Color {
    match s {
        "red" => Color::RED,
        "green" => Color::GREEN,
        "blue" => Color::BLUE,
        "yellow" => Color::YELLOW,
        "white" => Color::WHITE,
        "gray" | "grey" => Color::GRAY,
        "muted" => Color::rgba(0.5, 0.5, 0.5, 1.0),
        _ => Color::WHITE,
    }
}
