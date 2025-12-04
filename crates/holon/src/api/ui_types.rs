/// UI state types for frontend applications
///
/// These types represent UI-specific state that frontends (TUI, Flutter, etc.)
/// may need to pass to or retrieve from the render engine.

/// UI state containing cursor position and focused block
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone)]
pub struct UiState {
    pub cursor_pos: Option<CursorPosition>,
    pub focused_id: Option<String>,
}

/// Cursor position within a block
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone)]
pub struct CursorPosition {
    pub block_id: String,
    pub offset: u32,
}
