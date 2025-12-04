# Plugin System Architecture

**Status**: Design Document
**Date**: 2025-01-05
**Related**: `codev/plans/0001-reactive-prql-rendering.md` (Phase 3.4)

---

## Overview

The plugin system allows users to add adapters for third-party systems (Todoist, JIRA, Linear, etc.) as plugins. Each plugin contains:

1. **API client** - Interacts with external system's REST API
2. **Simulator** - Contract-based fake implementation for optimistic updates
3. **ExternalSystem trait** - Unified interface for command execution

**Key Design Goal**: Users pick only the external systems they actually use.

---

## Platform Support

### ✅ Dynamic Loading (Runtime)

**macOS, Linux, Windows**
- Uses `libloading` crate
- Load `.dylib` / `.so` / `.dll` files at runtime
- Users install plugins as separate files
- Configuration file controls which plugins to load

**Android (with caveats)**
- Uses `.so` files like Linux
- Plugins must be bundled with APK (can't download at runtime)
- Each architecture needs separate `.so` (arm64-v8a, armeabi-v7a, x86_64)

### ✅ Static Linking (Compile-Time)

**iOS**
- Apple prohibits dynamic library loading in sandboxed apps
- All plugins compiled into binary
- Configuration controls which plugins to *enable* (not load)
- Uses `inventory` crate for static registration

**Android (alternative)**
- Can also use static linking approach for simpler deployment

---

## Architecture: Hybrid System

### Desktop: Dynamic Loading

```rust
#[cfg(not(target_os = "ios"))]
use libloading::Library;

pub struct PluginRegistry {
    plugins: HashMap<String, Box<dyn ExternalSystem>>,
    #[cfg(not(target_os = "ios"))]
    _libraries: Vec<Library>, // Keep loaded
}

#[cfg(not(target_os = "ios"))]
impl PluginRegistry {
    pub fn load_dynamic(paths: &[PathBuf]) -> Result<Self> {
        let mut registry = Self {
            plugins: HashMap::new(),
            _libraries: Vec::new(),
        };

        for path in paths {
            unsafe {
                let lib = Library::new(path)?;
                let create: Symbol<extern "C" fn() -> *mut dyn ExternalSystem> =
                    lib.get(b"create_plugin")?;

                let plugin = Box::from_raw(create());
                registry.plugins.insert(plugin.system_id().to_owned(), plugin);
                registry._libraries.push(lib);
            }
        }

        Ok(registry)
    }
}
```

### Mobile: Static Registration

```rust
#[cfg(target_os = "ios")]
use inventory;

pub struct PluginFactory {
    pub system_id: &'static str,
    pub create: fn() -> Box<dyn ExternalSystem>,
}

#[cfg(target_os = "ios")]
inventory::collect!(PluginFactory);

// Each plugin registers itself at compile time:
// (in plugin crate)
inventory::submit! {
    PluginFactory {
        system_id: "todoist",
        create: || Box::new(TodoistAdapter::new()),
    }
}

#[cfg(target_os = "ios")]
impl PluginRegistry {
    pub fn load_static(enabled: &[String]) -> Result<Self> {
        let mut registry = Self {
            plugins: HashMap::new(),
        };

        for factory in inventory::iter::<PluginFactory> {
            if enabled.contains(&factory.system_id.to_string()) {
                let plugin = (factory.create)();
                registry.plugins.insert(
                    plugin.system_id().to_owned(),
                    plugin
                );
            }
        }

        Ok(registry)
    }
}
```

### Unified API

```rust
impl PluginRegistry {
    pub fn load_from_config(config: &Config) -> Result<Self> {
        #[cfg(not(target_os = "ios"))]
        return Self::load_dynamic(&config.plugin_paths);

        #[cfg(target_os = "ios")]
        return Self::load_static(&config.enabled_plugins);
    }

    pub fn get(&self, system_id: &str) -> Option<&dyn ExternalSystem> {
        self.plugins.get(system_id).map(|b| &**b)
    }
}
```

---

## Plugin Interface: `ExternalSystem` Trait

All plugins implement this trait (from Phase 3.4):

```rust
#[async_trait]
pub trait ExternalSystem: Send + Sync {
    /// System identifier (e.g., "todoist", "jira", "linear")
    fn system_id(&self) -> &str;

    /// Apply a command, returning updated fields
    ///
    /// For optimistic updates (offline), uses contract-based simulator.
    /// For real sync, calls external API.
    async fn apply_command(
        &self,
        cmd: &Command,
        inputs: &HashMap<String, Value>,
    ) -> Result<HashMap<String, Value>>;
}
```

**Key Points:**
- **Async trait** - Supports both HTTP calls and local simulation
- **HashMap interface** - Generic, no fixed types (works with any entity)

---

## Creating a Plugin

### Plugin Crate Structure

```
crates/plugins/todoist-adapter/
├── Cargo.toml
├── src/
│   ├── lib.rs           # Plugin entry point
│   ├── client.rs        # Real HTTP client
│   ├── fake.rs          # Contract-based simulator
│   └── contracts.rs     # Contract definitions
```

### Example Plugin Implementation

**lib.rs** (entry point):
```rust
use holon::{ExternalSystem, Command, Value};
use async_trait::async_trait;

mod client;
mod fake;
mod contracts;

pub struct TodoistAdapter {
    client: client::TodoistClient,
    fake: fake::TodoistFake,
    mode: OperationMode,
}

enum OperationMode {
    Optimistic, // Use fake for offline
    RealTime,   // Use real API
}

impl TodoistAdapter {
    pub fn new() -> Self {
        Self {
            client: client::TodoistClient::new(),
            fake: fake::TodoistFake::new(),
            mode: OperationMode::Optimistic,
        }
    }
}

#[async_trait]
impl ExternalSystem for TodoistAdapter {
    fn system_id(&self) -> &str {
        "todoist"
    }

    async fn apply_command(
        &self,
        cmd: &Command,
        inputs: &HashMap<String, Value>,
    ) -> Result<HashMap<String, Value>> {
        match self.mode {
            OperationMode::Optimistic => {
                // Use contract-based simulator (Phase 3.4)
                self.fake.apply_command(cmd, inputs).await
            }
            OperationMode::RealTime => {
                // Real HTTP call
                let response = self.client.apply_command(cmd, inputs).await?;

                // Validate against contract (drift detection)
                self.fake.validate_response(cmd, inputs, &response)?;

                Ok(response)
            }
        }
    }
}

// Dynamic loading export (desktop only)
#[cfg(not(target_os = "ios"))]
#[no_mangle]
pub extern "C" fn create_plugin() -> *mut dyn ExternalSystem {
    Box::into_raw(Box::new(TodoistAdapter::new()))
}

// Static registration (iOS)
#[cfg(target_os = "ios")]
inventory::submit! {
    holon::PluginFactory {
        system_id: "todoist",
        create: || Box::new(TodoistAdapter::new()),
    }
}
```

---

## User Configuration

**~/.holon/config.toml**:
```toml
[plugins]
enabled = ["todoist", "jira"]  # Which plugins to enable

# Desktop: paths to .so/.dylib/.dll files
[plugins.paths]
todoist = "~/.holon/plugins/libtodoist_adapter.so"
jira = "~/.holon/plugins/libjira_adapter.so"

# Plugin-specific configuration
[plugins.todoist]
api_key = "..."
default_project = "Inbox"

[plugins.jira]
server_url = "https://company.atlassian.net"
username = "user@example.com"
api_token = "..."
```

**Same config works on all platforms:**
- Desktop: Reads `plugins.paths`, loads dynamically
- iOS: Ignores `plugins.paths`, only uses `enabled` list

---

## Building

### Desktop (Dynamic Plugins)

**Build main app:**
```bash
cargo build --release
```

**Build plugins separately:**
```bash
cd crates/plugins/todoist-adapter
cargo build --release --lib
# Output: target/release/libtodoist_adapter.so (or .dylib, .dll)
```

**Install plugins:**
```bash
mkdir -p ~/.holon/plugins
cp target/release/libtodoist_adapter.so ~/.holon/plugins/
```

### iOS (Static Plugins)

**Build with feature flags:**
```bash
# Include specific plugins at compile time
cargo build --release --target aarch64-apple-ios \
    --features plugin-todoist,plugin-jira
```

**Cargo.toml features:**
```toml
[features]
default = []
plugin-todoist = ["holon-todoist"]
plugin-jira = ["holon-jira"]
plugin-linear = ["holon-linear"]

# Only include plugin deps when feature is enabled
[dependencies]
holon-todoist = { path = "crates/plugins/todoist-adapter", optional = true }
holon-jira = { path = "crates/plugins/jira-adapter", optional = true }
holon-linear = { path = "crates/plugins/linear-adapter", optional = true }
```

### Android

**Option A: Dynamic (complex deployment):**
```bash
# Build for each architecture
cargo ndk build --release --target aarch64-linux-android
cargo ndk build --release --target armv7-linux-androideabi
cargo ndk build --release --target x86_64-linux-android

# Bundle .so files in APK jniLibs/
```

**Option B: Static (simpler, recommended):**
```bash
# Same as iOS approach
cargo ndk build --release --features plugin-todoist,plugin-jira
```

---

## Integration with Command Sourcing (Phase 3.4)

Plugins integrate seamlessly with the command sourcing system:

```rust
// CommandExecutor uses plugins for optimistic updates
pub async fn execute_command(
    cmd: Command,
    plugins: &PluginRegistry,
    db: &mut TursoBackend,
) -> Result<()> {
    let target_system = cmd.target_system; // "todoist", "jira", etc.

    // 1. Get plugin for target system
    let plugin = plugins.get(&target_system)
        .ok_or_else(|| anyhow!("Plugin not found: {}", target_system))?;

    // 2. Apply optimistically (uses contract-based fake)
    let response = plugin.apply_command(&cmd, &cmd.params).await?;

    // 3. Apply to Turso (local cache)
    apply_update(response, db)?;

    // 4. CDC triggers UI update

    // 5. Background worker syncs to real API later
    Ok(())
}
```

**Background sync worker:**
```rust
pub async fn sync_pending_commands(
    plugins: &PluginRegistry,
    db: &mut TursoBackend,
) -> Result<()> {
    let pending = db.query("SELECT * FROM commands WHERE status = 'pending'")?;

    for cmd in pending {
        let plugin = plugins.get(&cmd.target_system)?;

        // Switch to real-time mode
        plugin.set_mode(OperationMode::RealTime);

        match plugin.apply_command(&cmd, &cmd.params).await {
            Ok(response) => {
                // Update with real IDs from external system
                update_id_mappings(&response, db)?;
                db.execute("UPDATE commands SET status = 'synced' WHERE id = ?", [cmd.id])?;
            }
            Err(e) => {
                // Re-fetch canonical state
                db.execute("UPDATE commands SET status = 'failed', error = ? WHERE id = ?",
                          [e.to_string(), cmd.id])?;
            }
        }
    }

    Ok(())
}
```

---

## Plugin Discovery

### Desktop (Dynamic)

Plugins installed in standard location:
```
~/.holon/plugins/
├── libtodoist_adapter.so
├── libjira_adapter.so
└── liblinear_adapter.dylib
```

App scans directory on startup, reads plugin metadata via FFI:
```rust
#[no_mangle]
pub extern "C" fn plugin_metadata() -> PluginMetadata {
    PluginMetadata {
        id: "todoist",
        name: "Todoist Adapter",
        version: "1.0.0",
        author: "...",
        api_version: 1,
    }
}
```

### Mobile (Static)

Plugins registered at compile time via `inventory`. App shows available plugins in settings UI:
```rust
pub fn list_available_plugins() -> Vec<&'static str> {
    inventory::iter::<PluginFactory>()
        .map(|f| f.system_id)
        .collect()
}
```

User toggles plugins on/off in settings (no reinstall needed).

---

## Security Considerations

### Dynamic Loading (Desktop)

⚠️ **Risk**: Loading arbitrary `.so` files is dangerous
✅ **Mitigation**:
- Only load from trusted directory (`~/.holon/plugins/`)
- Verify plugin signatures (future work)
- Sandboxing via OS permissions (plugins can't access user files directly)

### API Credentials

Plugins need API keys/tokens. Storage options:

1. **Config file** (current): Plain text in `~/.holon/config.toml`
2. **OS keychain** (future): macOS Keychain, Windows Credential Manager, Linux Secret Service
3. **Environment variables**: `TODOIST_API_KEY`

---

## Why Not `abi_stable`?

We initially considered `abi_stable` crate for stable ABI, but decided against it:

**Downsides:**
- Type complexity: `HashMap<String, Value>` → `RHashMap<RString, RValue>`
- Async traits unsupported: Must manually return `RBoxFuture`
- Learning curve: Steep, macro-heavy
- Contract system friction: All types need `#[derive(StableAbi)]`
- Two FFI systems: Flutter ↔ Rust (FRB) + Rust ↔ Plugins (abi_stable)

**When to reconsider:**
- External plugin developers (not just internal)
- Version skew problems (core v2, plugin compiled for v1)
- ABI breakage between recompiles

For now, raw `libloading` is simpler. All plugins compile against same version of core crate (live in workspace).

---

## Why Not WebAssembly?

WASM would work on all platforms (including iOS), but:

❌ Requires marshaling all types across WASM boundary
❌ Complicates async (needs WASI or custom runtime)
❌ Plugins need native HTTP clients anyway (not sandboxed browser APIs)
❌ Performance overhead for high-frequency calls

✅ **When WASM makes sense:**
- User-provided plugins (untrusted code, need sandboxing)
- Plugin marketplace (download and run safely)

For our use case (internal adapters for known APIs), dynamic libraries are simpler.

---

## Future Work

### Near-Term
- [ ] Implement `PluginRegistry` with dynamic loading
- [ ] Create example Todoist adapter plugin
- [ ] Add plugin metadata system
- [ ] Document plugin development guide

### Medium-Term
- [ ] Plugin signature verification
- [ ] OS keychain integration for credentials
- [ ] Plugin update mechanism
- [ ] Error reporting from plugins

### Long-Term
- [ ] Plugin marketplace (WASM-based?)
- [ ] Plugin sandboxing (capabilities system)
- [ ] Hot-reloading for development
- [ ] Plugin analytics (usage stats)

---

## References

- **libloading**: https://crates.io/crates/libloading
- **inventory**: https://crates.io/crates/inventory (static registration)
- **abi_stable**: https://crates.io/crates/abi_stable (not using, but evaluated)
- **Phase 3.4**: Command Sourcing with Contract-Based Simulation

---

**Last Updated**: 2025-01-05
**Version**: 1.0
**Status**: Design Document
