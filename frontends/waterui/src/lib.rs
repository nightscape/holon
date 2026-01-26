//! Holon WaterUI Frontend
//!
//! A native UI for the Holon outliner built with WaterUI framework.
//! Renders to native UIKit/AppKit on iOS/macOS and Android Views on Android.

use waterui::app::App;
use waterui::prelude::*;

/// Application root view
#[hot_reload]
fn root() -> impl View {
    scroll(
        vstack((
            header(),
            spacer().height(16.0),
            main_content(),
            spacer().height(16.0),
            footer(),
        ))
        .padding_with(EdgeInsets::all(16.0)),
    )
}

/// Header with app title
fn header() -> impl View {
    vstack((
        text("Holon")
            .size(32.0)
            .bold()
            .foreground(Color::srgb_hex("#1976D2")),
        text("Collaborative Outliner")
            .size(14.0)
            .foreground(Color::srgb_hex("#808080")),
    ))
}

/// Main content area
fn main_content() -> impl View {
    vstack((
        text("Welcome to Holon").size(24.0).bold(),
        spacer().height(8.0),
        text("Your collaborative outliner and knowledge management system")
            .size(16.0)
            .foreground(Color::srgb_hex("#404040")),
        spacer().height(16.0),
        quick_actions(),
    ))
    .spacing(12.0)
}

/// Quick action buttons
fn quick_actions() -> impl View {
    vstack((
        text("Quick Actions").size(18.0).bold(),
        spacer().height(8.0),
        action_button("New Note"),
        action_button("Browse Notes"),
        action_button("Search"),
        action_button("Settings"),
    ))
    .spacing(8.0)
}

/// Styled action button
fn action_button(label: &'static str) -> impl View {
    text(label)
        .size(16.0)
        .padding_with(EdgeInsets::symmetric(8.0, 16.0))
        .background(Color::srgb_hex("#1976D2"))
        .foreground(Color::srgb_hex("#FFFFFF"))
}

/// Footer with version info
fn footer() -> impl View {
    vstack((
        Divider,
        text("Holon v0.1.0")
            .size(12.0)
            .foreground(Color::srgb_hex("#808080")),
    ))
}

/// Application initialization and environment setup
pub fn app(env: Environment) -> App {
    // Create and return app with root view
    App::new(root, env)
}

// FFI export for native backends
waterui_ffi::export!();
