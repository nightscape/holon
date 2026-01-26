# TUI-R3BL Frontend

A Terminal User Interface implementation using the [R3BL TUI framework](https://github.com/r3bl-org/r3bl-open-core/tree/main/tui), providing a reactive, modern terminal interface inspired by React and Elm.

## Overview

This is an equivalent implementation of the ratatui-based TUI (`frontends/tui/`) using the R3BL framework. It demonstrates:

- **R3BL App/Component architecture**: Unidirectional data flow similar to React
- **Styled rendering**: Using R3BL's CSS-like styling system
- **Keyboard interaction**: Arrow keys for navigation, Space for toggling tasks
- **Task list display**: Shows tasks with checkboxes, content, and priority badges

## Features (MVP)

- ✅ Display task list with formatted items
- ✅ Navigate with arrow keys (↑/↓)
- ✅ Toggle task status with Space
- ✅ Color-coded priority badges
- ✅ Status bar with helpful hints
- ✅ Sample data for demonstration

## Not Yet Implemented

- ❌ Database integration (uses in-memory sample data)
- ❌ Query editor (PRQL editing)
- ❌ CDC polling for reactive updates
- ❌ Component-based architecture (currently single-app approach)

## How to Run

```bash
# From the project root
cargo run -p tui-frontend

# Or from this directory
cargo run
```

### Logging Configuration

The application **disables r3bl logging** by default to prevent log messages from breaking the TUI display.

If you need debug logging during development:

```rust
// In main.rs, replace the logging initialization with:
use r3bl_tui::log::WriterConfig;

let config = WriterConfig::File("debug.log".to_string());
try_initialize_logging_global(config).ok();
```

Then check `debug.log` for log output without breaking the TUI.

## Controls

- **↑/↓**: Navigate task list
- **Space**: Toggle task completion status
- **q**: Quit application
- **r**: Refresh (placeholder - not connected to DB)

## Architecture

### File Structure

```
src/
├── main.rs          # Entry point
├── launcher.rs      # Event loop setup with sample data
├── app_main.rs      # App trait implementation
├── state.rs         # State and AppSignal types
└── db.rs            # Database layer (unused in MVP)
```

### R3BL Integration

This implementation follows the R3BL pattern:

1. **State Management**: `State` struct holds application state
2. **App Trait**: `AppMain` implements the core `App` trait with:
   - `app_init()`: Initialize components (empty for MVP)
   - `app_handle_input_event()`: Handle keyboard input
   - `app_handle_signal()`: Handle out-of-band events (placeholder)
   - `app_render()`: Render UI with styled text and layout

3. **Rendering**: Uses R3BL's rendering pipeline:
   - `RenderPipeline`: Collection of render operations
   - `tui_styled_texts!`: Macro for creating styled text
   - `RenderOpIR`: Intermediate representation for rendering

## Comparison with Ratatui Version

| Feature | Ratatui TUI | R3BL TUI (MVP) |
|---------|-------------|----------------|
| Task display | ✅ | ✅ |
| Navigation | ✅ | ✅ |
| Toggle status | ✅ | ✅ (in-memory) |
| Database | ✅ | ❌ (planned) |
| Query editor | ✅ | ❌ (planned) |
| CDC polling | ✅ | ❌ (planned) |
| Styling | Widget-based | CSS-like macros |
| Architecture | Event loop | React-like App trait |

## Next Steps

To reach feature parity with the ratatui version:

1. **Phase 1**: Integrate database operations
   - Connect to libsql database
   - Implement query execution
   - Add task updates with CDC

2. **Phase 2**: Add query editor
   - Use R3BL's EditorComponent
   - Support PRQL editing
   - Compile and execute queries

3. **Phase 3**: Add CDC polling
   - Use AppSignal for background polling
   - Implement reactive UI updates
   - Add dirty flag tracking

4. **Phase 4**: Component architecture
   - Split into TaskListComponent, EditorComponent
   - Use ComponentRegistry
   - Improve modularity

## Dependencies

- **r3bl_tui**: TUI framework (local path to r3bl-open-core)
- **tokio**: Async runtime
- **serde/serde_json**: Data serialization
- **libsql**: Database client (prepared for future use)

## License

Same as parent project.
