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

## Flutter Rust Bridge (FRB) Troubleshooting

### Detecting FRB Issues

Run the FRB codegen and check for errors:

```bash
cd frontends/flutter
flutter_rust_bridge_codegen generate 2>&1 | tee /tmp/frb_codegen.txt
```

Common error patterns to grep for:

```bash
# Duplicate class names (most common issue)
grep -i "duplicate" /tmp/frb_codegen.txt | grep -v "duplicate_field"

# General errors (filter out serde noise)
grep -i "error" /tmp/frb_codegen.txt | grep -v "duplicate_field\|invalid_length\|error_for_struct"
```

**Key error message:** `Will generate duplicated class names (["TypeName"]). This is often because the type is auto inferred as both opaque and non-opaque.`

### Diagnosing the Root Cause

1. **Check the generated Dart file** for duplicate class definitions:
   ```bash
   # Look for both opaque and non-opaque versions
   grep -n "class TypeName\|abstract class TypeName" \
     frontends/flutter/lib/src/rust/third_party/holon_api/typename.dart
   ```

2. **Compare with a working type** (e.g., Block works correctly):
   ```bash
   # Check what comments/derives Block has that the broken type doesn't
   diff <(grep -A5 "pub struct Block" crates/holon-api/src/block.rs) \
        <(grep -A5 "pub struct Document" crates/holon-api/src/document.rs)
   ```

3. **Check the generated Dart for clues** - look at the header comments:
   ```dart
   // These functions are ignored (category: IgnoreBecauseExplicitAttribute): `method1`, `method2`
   // These functions have error during generation: `problematic_method`
   ```

### Common Fixes

#### 1. Add `non_opaque` directive
For structs that should be passed by value (not as opaque references):

```rust
/// Description of the struct
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MyStruct { ... }
```

#### 2. Add `PartialEq` derive
FRB often requires `PartialEq` for non-opaque types:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]  // Add PartialEq
pub struct MyStruct { ... }
```

#### 3. Ignore methods that return references or use complex generics
Methods that return `&Self`, `&T`, or use generic type parameters often cause issues:

```rust
/// flutter_rust_bridge:ignore
pub fn get_reference(&self) -> &SomeType { ... }

/// flutter_rust_bridge:ignore
pub fn generic_method<T: SomeTrait>(&self, param: &T) -> Option<T> { ... }
```

#### 4. Hide traits from FRB using private modules
If a trait returns `Option<&YourType>`, FRB may treat `YourType` as opaque. Move it to a private module:

```rust
// BAD: FRB sees this trait and forces opaque treatment
/// flutter_rust_bridge:ignore  // This alone may NOT work for traits!
pub(crate) trait MyResolver {
    fn get_item(&self, id: &str) -> Option<&MyType>;
}

// GOOD: Private module hides it completely from FRB
mod private_impl {
    use super::MyType;

    pub trait MyResolver {
        fn get_item(&self, id: &str) -> Option<&MyType>;
    }

    impl MyType {
        /// flutter_rust_bridge:ignore
        pub fn method_using_trait<R: MyResolver>(&self, r: &R) { ... }
    }
}

// Re-export for internal use only
pub(crate) use private_impl::MyResolver;
```

#### 5. Fix Value conversions
When converting between `Value` and `serde_json::Value`, use the `From` impl:

```rust
// BAD: as_json_value() only works for Value::Json variant
map.into_iter().map(|(k, v)| (k, v.as_json_value().unwrap_or(serde_json::Value::Null)))

// GOOD: From<Value> for serde_json::Value handles all variants
map.into_iter().map(|(k, v)| (k, serde_json::Value::from(v)))
```

### Checklist for New Structs/Enums Exposed to Flutter

1. [ ] Add `/// flutter_rust_bridge:non_opaque` if it should be passed by value
2. [ ] Add `PartialEq` to derives
3. [ ] Mark methods returning references with `/// flutter_rust_bridge:ignore`
4. [ ] Mark methods with complex generics with `/// flutter_rust_bridge:ignore`
5. [ ] Move any traits that reference your type to private modules
6. [ ] Run codegen and check for duplicate class errors
7. [ ] Verify only ONE class definition in generated Dart

### Reference: Block (Working Example)

`Block` in `crates/holon-api/src/block.rs` is a good reference for a correctly configured FRB type:

```rust
/// Description...
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Entity)]
pub struct Block {
    // fields...
}

impl Block {
    // Public methods exposed to Dart
    pub fn new_text(...) -> Self { ... }

    // Methods hidden from Dart
    /// flutter_rust_bridge:ignore
    pub fn properties_map(&self) -> HashMap<String, Value> { ... }

    /// flutter_rust_bridge:ignore
    pub fn depth_from<'blk, F>(&self, get_block: F) -> usize
    where F: for<'a> FnMut(&'a str) -> Option<&'blk Block> { ... }
}
```
