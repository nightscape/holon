//! AST Transformation Pipeline for PRQL Query Modifications
//!
//! This module provides a composable, DI-integrated mechanism for modifying PRQL
//! queries at the AST level. This replaces brittle SQL string manipulation with
//! type-safe AST modifications.
//!
//! # Architecture
//!
//! The transformation pipeline operates at two levels of the PRQL compilation:
//! - **PL (Pipeline Language)**: Higher-level, closer to PRQL syntax
//! - **RQ (Relational Query)**: Lower-level, closer to SQL structure
//!
//! Transformers are ordered by phase and priority to ensure correct sequencing.
//!
//! # Example
//!
//! ```rust,ignore
//! use holon::core::transform::{AstTransformer, TransformPhase, TransformPipeline};
//!
//! // Create pipeline with transformers
//! let pipeline = TransformPipeline::new(vec![
//!     Arc::new(ChangeOriginTransformer),
//! ]);
//!
//! // Compile PRQL with transformations
//! let (sql, rq) = pipeline.compile("from tasks | select {id, content}")?;
//! ```

mod change_origin;
mod column_preservation;
mod entity_type_injector;
mod json_aggregation;
mod pipeline;
mod traits;

pub use change_origin::ChangeOriginTransformer;
pub use column_preservation::{ColumnPreservationTransformer, COLUMN_PRESERVATION_PRIORITY};
pub use entity_type_injector::{EntityTypeInjector, ENTITY_NAME_COLUMN};
pub use json_aggregation::{JsonAggregationTransformer, DATA_COLUMN, JSON_AGGREGATION_PRIORITY};
pub use pipeline::TransformPipeline;
pub use traits::{AstTransformer, TransformPhase};
