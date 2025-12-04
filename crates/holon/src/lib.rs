pub mod adapter;
pub mod api;
pub mod core;
pub mod di;
pub mod operations;
pub mod references;
pub mod storage;
pub mod sync;
pub mod tasks;
#[cfg(not(target_arch = "wasm32"))]
pub mod testing;

// Re-export query-render types for FFI
pub use query_render::types::{Arg, BinaryOperator, RenderExpr, RenderSpec};

#[cfg(test)]
pub mod examples;
