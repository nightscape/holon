// Library interface for tui-frontend
// Exposes modules for testing and reuse

pub mod app_main;
pub mod components;
pub mod config;
pub mod launcher;
pub mod render_interpreter;
pub mod state;
pub mod stylesheet;
pub mod ui_element;

// PBT testing modules
pub mod tui_pbt_backend;
pub mod tui_pbt_state_machine;

// Re-export commonly used types
pub use app_main::AppMain;
pub use state::{AppSignal, State};
pub use ui_element::UIElement;
