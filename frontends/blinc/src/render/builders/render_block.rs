use blinc_app::prelude::*;
use blinc_theme::{ColorToken, ThemeState};

use crate::render::context::RenderContext;
use crate::render::interpreter::ResolvedArgs;

/// render_block(this) - dispatches based on block content_type.
///
/// - content_type: "source" + source_language: "prql" → execute query and render results
/// - content_type: "source" → source code display
/// - default → show content as text
pub fn build(_args: &ResolvedArgs, ctx: &RenderContext) -> Div {
    let content_type = ctx
        .row()
        .get("content_type")
        .and_then(|v| v.as_string())
        .unwrap_or("");
    let source_language = ctx
        .row()
        .get("source_language")
        .and_then(|v| v.as_string())
        .unwrap_or("");
    let content = ctx
        .row()
        .get("content")
        .and_then(|v| v.as_string())
        .unwrap_or("");

    let theme = ThemeState::get();
    match (content_type, source_language) {
        ("source", "prql") => {
            let context_id = ctx
                .row()
                .get("id")
                .and_then(|v| v.as_string())
                .map(|s| s.to_string());
            super::live_query::build_prql(content.to_string(), context_id, ctx)
        }
        ("source", lang) => div()
            .flex_col()
            .gap(2.0)
            .child(
                div().flex_row().gap(4.0).child(
                    text(format!("[{lang}]"))
                        .size(10.0)
                        .color(theme.color(ColorToken::TextTertiary)),
                ),
            )
            .child(
                div()
                    .p(8.0)
                    .rounded(4.0)
                    .bg(theme.color(ColorToken::SurfaceOverlay))
                    .child(
                        text(content)
                            .size(13.0)
                            .color(theme.color(ColorToken::TextPrimary)),
                    ),
            ),
        _ => {
            if content.is_empty() {
                div()
            } else {
                div().child(
                    text(content)
                        .size(14.0)
                        .color(theme.color(ColorToken::TextPrimary)),
                )
            }
        }
    }
}
