# Flutter Frontend Development

## Mock Backend Mode

Run the Flutter UI without compiling Rust code. Useful for:
- UI-only development when Rust has compilation errors
- Faster iteration on Flutter code
- Testing UI components in isolation

### Prerequisites

Rust must compile successfully **once** to:
1. Generate Dart type bindings in `lib/src/rust/`
2. Build and cache `librust_lib_holon.a`

### Usage

```bash
SKIP_RUST_BUILD=1 flutter run --dart-define=USE_MOCK_BACKEND=true -d macos
```

| Flag | Purpose |
|------|---------|
| `SKIP_RUST_BUILD=1` | Skip Rust compilation, use cached native library |
| `USE_MOCK_BACKEND=true` | Use mock backend service instead of real Rust FFI |

### What Works in Mock Mode

- Full UI rendering and navigation
- Theme switching
- Settings screen
- Sidebar interactions
- Sample data tree (loaded from YAML)

### What Doesn't Work in Mock Mode

- Real sync operations (no-op)
- CDC/live updates (stream never emits)
- Undo/redo (always returns false)

### Mock Data

Mock data is defined in `assets/mock_data.yaml`. Edit this file to customize:
- **Row templates**: Define how each entity type renders (icons, checkboxes, text)
- **Tree configuration**: Set parent_id and sort_key columns
- **Sample data**: Hierarchical items with id, parent_id, content, etc.

Example structure:
```yaml
row_templates:
  - index: 0
    entity_name: mock_folders
    expr:
      function: row
      args:
        - function: icon
          args: ['folder']
        - function: text
          named_args:
            content: { column: content }

data:
  - id: folder-1
    parent_id: null
    content: My Folder
    ui: 0
```

### Implementation Details

Mock mode is implemented via:
- `MockRustLibApi` - Prevents native library loading in `RustLib.init()`
- `MockBackendService` - Provides stub implementations, loads data from YAML
- `assets/mock_data.yaml` - Defines mock UI templates and sample data
- `SKIP_RUST_BUILD` check in `rust_builder/cargokit/build_pod.sh`

## Normal Development

### Running the App

```bash
# macOS
flutter run -d macos

# With hot reload
flutter run -d macos --hot
```

### Building

```bash
# Debug build
flutter build macos --debug

# Release build
flutter build macos --release
```

### Code Generation

After modifying Rust API:

```bash
flutter_rust_bridge_codegen generate
```

### Analyzing Code

```bash
flutter analyze
```
