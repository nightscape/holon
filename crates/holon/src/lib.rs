pub mod adapter;
pub mod api;
pub mod core;
pub mod di;
pub mod navigation;
pub mod operations;
pub mod references;
pub mod storage;
pub mod sync;
pub mod tasks;
#[cfg(not(target_arch = "wasm32"))]
pub mod testing;

// Re-export holon-prql-render types for FFI
pub use holon_prql_render::types::{Arg, BinaryOperator, RenderExpr, RenderSpec};

// Re-export macro-generated operation dispatch modules for HasCache trait
#[cfg(not(target_arch = "wasm32"))]
pub use core::datasource::__operations_has_cache;

#[cfg(test)]
pub mod examples;
