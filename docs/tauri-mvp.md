# Tauri MVP - Task Management UI

## Overview

This is a minimal viable product (MVP) of the Rusty Knowledge application built with Tauri. It provides a basic task management interface similar to Todoist, allowing you to:

- Create top-level tasks
- Add subtasks (nested hierarchically)
- Mark tasks as completed
- Delete tasks

## Architecture

### Backend (Rust)
- **Location**: `src/tasks.rs` and `src-tauri/src/lib.rs`
- **Tech Stack**: Rust + Tauri
- **Features**:
  - In-memory task storage with default demo tasks
  - CRUD operations via Tauri commands
  - Support for hierarchical task structures (tasks can have children)

### Frontend
- **Location**: `src/main.ts`, `src/styles.css`, `index.html`
- **Tech Stack**: Vanilla TypeScript + Vite
- **Features**:
  - Clean, modern UI with a Todoist-inspired design
  - Real-time updates when tasks are modified
  - Support for nested subtasks
  - Keyboard shortcuts (Enter to add tasks)

## Project Structure

```
holon/
├── src/                      # Rust library code
│   ├── lib.rs
│   ├── tasks.rs             # Task data structures and store
│   └── sync.rs
├── src-tauri/               # Tauri application
│   ├── src/
│   │   ├── lib.rs          # Tauri commands and app setup
│   │   └── main.rs
│   ├── icons/              # App icons
│   ├── Cargo.toml
│   └── tauri.conf.json     # Tauri configuration
├── src/                     # Frontend source
│   ├── main.ts             # Frontend logic
│   └── styles.css          # Styling
├── index.html              # Entry HTML
├── package.json            # Node dependencies
├── vite.config.ts          # Vite configuration
└── tsconfig.json           # TypeScript configuration
```

## Running the Application

### Prerequisites

1. **Rust** (with Cargo)
2. **Node.js** and npm
3. **System dependencies** for Tauri (varies by platform)
   - macOS: No additional dependencies needed
   - Linux: See [Tauri prerequisites](https://v2.tauri.app/start/prerequisites/)
   - Windows: See [Tauri prerequisites](https://v2.tauri.app/start/prerequisites/)

### Development Mode

1. Install frontend dependencies:
   ```bash
   npm install
   ```

2. Run the application in development mode:
   ```bash
   npm run tauri dev
   ```

   This will:
   - Start the Vite dev server (hot reload for frontend)
   - Compile the Rust backend
   - Launch the Tauri application window

### Production Build

To create a production build:

```bash
npm run tauri build
```

This creates a native application bundle in `src-tauri/target/release/`.

## Features Demonstrated

### Task Operations

1. **Add Task**: Type in the input field and press Enter or click "Add Task"
2. **Add Subtask**: Click "Add Subtask" on any task, enter text, and press Enter or click "Add"
3. **Complete Task**: Click the checkbox next to any task
4. **Delete Task**: Click the "Delete" button (removes task and all subtasks)

### UI Features

- Tasks are styled differently when completed (strikethrough, faded)
- Subtasks are indented and have a lighter background
- Nested subtasks are supported (subtasks can have subtasks)
- Responsive layout

## Current Limitations (MVP)

- No data persistence (tasks reset on app restart)
- No task priorities
- No due dates or reminders
- No task editing (only add/delete)
- No drag-and-drop reordering
- No search or filtering
- No Loro CRDT integration yet (coming in next phase)

## Next Steps

Based on the architecture document (`docs/architecture.md`):

1. **Phase 1 Completion**:
   - Add data persistence with Loro CRDT
   - Implement Markdown export for tasks
   - Add block-based editor for task notes

2. **Phase 2**:
   - Integrate with external systems (Todoist)
   - Add SQLite caching layer
   - Implement sync mechanism

3. **Future Enhancements**:
   - Task priorities
   - Due dates and reminders
   - Tags and contexts
   - Kanban view
   - Search and filtering

## Technical Notes

### State Management

Currently, state is managed in Rust using a `Mutex<TaskStore>` wrapped in Tauri's state management. This provides thread-safe access to the task list from the frontend.

### Communication Pattern

Frontend ↔️ Backend communication uses Tauri's command pattern:

```typescript
// Frontend
await invoke('add_task', { title: 'New task', parentId: null });
```

```rust
// Backend
#[tauri::command]
fn add_task(title: String, parent_id: Option<String>, state: State<AppState>) -> Task {
    state.task_store.lock().unwrap().add_task(title, parent_id)
}
```

### UUID Generation

Tasks use UUID v4 for unique IDs, which will help when implementing CRDT sync later.

## Testing

Currently manual testing via the UI. To verify the build works:

```bash
# Check Rust compiles
cd src-tauri && cargo check

# Check frontend builds
npm run build
```

## Contributing

When adding features, follow the architecture principles from `docs/architecture.md`:

1. **Type Safety**: Use strong types in both Rust and TypeScript
2. **Separation of Concerns**: Keep data structures in Rust, UI in frontend
3. **Local-First**: Design for offline operation first
