pub mod datasource;
pub mod operation_log;
pub mod operation_wrapper;
pub mod queryable_cache;
pub mod sql_operation_provider;
pub mod stream_cache;
pub mod traits;
pub mod transform;
pub mod unified_query;
pub mod updates;

#[cfg(test)]
mod test_macro;

pub use datasource::{DataSource, StreamProvider};
// Re-export DynamicEntity from holon_api (single source of truth)
pub use holon_api::DynamicEntity;
pub use operation_log::{OperationLogObserver, OperationLogStore};
pub use operation_wrapper::OperationWrapper;
pub use queryable_cache::QueryableCache;
pub use sql_operation_provider::SqlOperationProvider;
pub use stream_cache::QueryableCache as StreamCache;
pub use traits::{
    And, FieldSchema, HasSchema, Lens, Not, Or, Predicate, Queryable, Schema, SqlPredicate,
    value_to_turso,
};
pub use transform::{AstTransformer, ChangeOriginTransformer, TransformPhase, TransformPipeline};
pub use unified_query::UnifiedQuery;
pub use updates::{FieldChange, Updates};

// MaybeSendSync is now defined in holon-core and re-exported via datasource module
