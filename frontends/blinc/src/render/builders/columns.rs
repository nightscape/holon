use blinc_app::prelude::*;
use blinc_theme::{ColorToken, ThemeState};

use crate::render::context::RenderContext;
use crate::render::interpreter::{interpret, ResolvedArgs};

const SIDEBAR_WIDTH: f32 = 280.0;

pub fn build(args: &ResolvedArgs, ctx: &RenderContext) -> Div {
    if ctx.is_screen_layout {
        return build_screen_layout(args, ctx);
    }

    let gap = args.get_f64("gap").unwrap_or(16.0) as f32;

    let mut container = div()
        .flex_row()
        .gap(gap)
        .border(1.0, ThemeState::get().color(ColorToken::Border))
        .rounded(4.0)
        .p(4.0);

    let template = args
        .get_template("item_template")
        .or(args.get_template("item"));

    if let Some(tmpl) = template {
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

/// Screen layout: first column becomes sidebar, rest becomes main content.
/// Mirrors Flutter's columns_builder.dart _buildScreenLayout.
fn build_screen_layout(args: &ResolvedArgs, ctx: &RenderContext) -> Div {
    let template = args
        .get_template("item_template")
        .or(args.get_template("item"));

    let tmpl = match template {
        Some(t) => t,
        None => return div(),
    };

    if ctx.data_rows.is_empty() {
        let child_ctx = ctx.with_row(Default::default());
        return interpret(tmpl, &child_ctx);
    }

    let theme = ThemeState::get();
    let is_open = ctx.sidebar_open.as_ref().map(|s| s.get()).unwrap_or(true);

    // First row -> sidebar, rest -> main content
    let mut rows = ctx.data_rows.iter();

    let sidebar_content = if let Some(first_row) = rows.next() {
        let row_ctx = ctx.with_row(first_row.clone());
        interpret(tmpl, &row_ctx)
    } else {
        div()
    };

    let main_children: Vec<Div> = rows
        .map(|row| {
            let row_ctx = ctx.with_row(row.clone());
            interpret(tmpl, &row_ctx)
        })
        .collect();

    let main_content = if main_children.len() == 1 {
        main_children.into_iter().next().unwrap()
    } else {
        let mut row_container = div().flex_row().flex_1();
        for child in main_children {
            row_container = row_container.child(child);
        }
        row_container
    };

    // Sidebar panel
    let mut sidebar = div()
        .flex_col()
        .h_full()
        .overflow_clip()
        .bg(theme.color(ColorToken::Surface))
        .border_right(1.0, theme.color(ColorToken::Border));

    if is_open {
        sidebar = sidebar.w(SIDEBAR_WIDTH).child(sidebar_content);
    } else {
        sidebar = sidebar.w(0.0);
    }

    // Main panel fills remaining space
    let main = div().flex_1().h_full().overflow_clip().child(main_content);

    div()
        .flex_row()
        .w_full()
        .h_full()
        .child(sidebar)
        .child(main)
}
