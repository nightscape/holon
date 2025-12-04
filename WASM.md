# WASM/Web Compatibility Gaps

This project compiles most crates for `wasm32-unknown-unknown`, but the full Flutter + Rust stack does **not** run in a browser yet. The blockers fall into a few buckets:

## Build Pipeline / Artifacts
- `frontends/flutter/rust/Cargo.toml` still produces `cdylib`/`staticlib` outputs for `dart:ffi`. Flutter Web expects a `wasm32` artefact plus `wasm-bindgen` glue and the flutter_rust_bridge JS bindings. No wasm-friendly build pipeline (crate-type changes, bindgen, JS loader) exists yet.

## Tokio Runtime Assumptions
- Many modules rely on `tokio::task::block_in_place`, multi-thread runtimes, or APIs only available on native targets. Examples:
  - `crates/holon/src/api/pbt_infrastructure.rs`
  - `frontends/flutter/rust/src/api/flutter_pbt_state_machine.rs`
  - `frontends/tui/src/tui_pbt_backend.rs`
- The wasm runtime only supports current-thread executors and no blocking sections, so these call sites would panic even if the code compiles.

## Native-Only Dependencies
- Core crates always pull in `turso`, `turso_core`, `iroh`, `reqwest` (default TLS), etc., which bring in networking stacks (`mio`, sockets, threads) that simply do not build on wasm. Flutter’s crate compiles because it uses a subset of the workspace, but running the full app would require cfg-gating or replacing those dependencies with wasm-safe alternatives.
- Property-based testing now compiles on wasm, but the actual runner still needs Tokio handles and a MemoryBackend that depends on blocking helpers.

## Runtime Feature Parity
- Storage/sync layers assume OS networking, filesystem access, and background tasks (Todoist sync, Turso CDC, Iroh P2P). Browsers need fetch/WebSocket-based replacements.
- Flutter PBT bindings call back into Dart via FFI-style callbacks that have no wasm/web equivalent yet.

## Suggested Next Steps
1. Decide when (or if) a browser build is a product priority. Until then, continue prioritising desktop/mobile features.
2. When ready, design a wasm build pipeline:
   - Change crate types, add `wasm-bindgen`/FRB wasm support, generate JS loader glue, integrate with Flutter Web.
3. Refactor runtime code to avoid `block_in_place` and multithreaded Tokio assumptions. Consider using async traits + `spawn_local` or an executor-agnostic abstraction.
4. Gate native-only dependencies (`turso`, `iroh`, etc.) behind `cfg`s and provide wasm-friendly adapters (e.g., IndexedDB caches, `fetch` networking).
5. Replace or stub native storage/sync components with browser-safe implementations.

Until those items are complete, treat wasm support as “compiles only”; running the actual application in a browser will not work.
