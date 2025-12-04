//! ChangeOriginTransformer - Injects `_change_origin` column for CDC trace propagation
//!
//! This transformer ensures that the `_change_origin` column is included in query results,
//! enabling CDC callbacks to read trace context from each row. This replaces the previous
//! brittle SQL string manipulation approach with type-safe AST modification.

use anyhow::Result;
use prqlc::ir::rq::{RelationColumn, RelationKind, RelationalQuery, Transform};
use tracing::debug;

use super::traits::{AstTransformer, TransformPhase};
use holon_api::CHANGE_ORIGIN_COLUMN;

/// Priority for the ChangeOriginTransformer within the Rq phase.
/// Run late (high number) so metadata columns are added after structural transforms.
pub const CHANGE_ORIGIN_PRIORITY: i32 = 100;

/// Transformer that injects `_change_origin` column into SELECT for CDC trace propagation.
///
/// The `_change_origin` column stores JSON-serialized `ChangeOrigin` data which includes:
/// - Whether the change originated locally or from sync
/// - OpenTelemetry trace context for distributed tracing
///
/// This transformer runs at `Rq(100)` - late in the RQ phase after all other
/// structural transformations are complete.
pub struct ChangeOriginTransformer;

impl AstTransformer for ChangeOriginTransformer {
    fn phase(&self) -> TransformPhase {
        TransformPhase::Rq(CHANGE_ORIGIN_PRIORITY)
    }

    fn name(&self) -> &'static str {
        "ChangeOriginTransformer"
    }

    fn transform_rq(&self, mut rq: RelationalQuery) -> Result<RelationalQuery> {
        // Add _change_origin to the main relation's columns
        add_change_origin_column(&mut rq.relation.columns);

        // Also add to any table declarations that have pipelines
        // This ensures CTEs and subqueries also include the column
        for table in &mut rq.tables {
            if let RelationKind::Pipeline(transforms) = &table.relation.kind {
                // Check if the pipeline has a From transform referencing an external table
                // that might have _change_origin
                if has_from_external_table(transforms) {
                    add_change_origin_column(&mut table.relation.columns);
                }
            }
        }

        debug!(
            "ChangeOriginTransformer: Added {} column to query",
            CHANGE_ORIGIN_COLUMN
        );

        Ok(rq)
    }
}

/// Add `_change_origin` column to a columns list if not already present.
fn add_change_origin_column(columns: &mut Vec<RelationColumn>) {
    let has_change_origin = columns.iter().any(
        |col| matches!(col, RelationColumn::Single(Some(name)) if name == CHANGE_ORIGIN_COLUMN),
    );

    // Also check for Wildcard which would already include all columns
    let has_wildcard = columns
        .iter()
        .any(|col| matches!(col, RelationColumn::Wildcard));

    if !has_change_origin && !has_wildcard {
        columns.push(RelationColumn::Single(Some(
            CHANGE_ORIGIN_COLUMN.to_string(),
        )));
    }
}

/// Check if a pipeline has a From transform referencing an external table.
fn has_from_external_table(transforms: &[Transform]) -> bool {
    transforms.iter().any(|t| matches!(t, Transform::From(_)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::transform::TransformPipeline;
    use std::sync::Arc;

    #[test]
    fn test_adds_change_origin_column() {
        let pipeline =
            TransformPipeline::empty().with_transformer(Arc::new(ChangeOriginTransformer));

        let result = pipeline.compile("from tasks | select {id, content}");
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

        let (sql, rq) = result.unwrap();

        // Check that _change_origin is in the relation columns
        let has_change_origin = rq.relation.columns.iter().any(
            |col| matches!(col, RelationColumn::Single(Some(name)) if name == CHANGE_ORIGIN_COLUMN),
        );
        assert!(
            has_change_origin,
            "RQ should have _change_origin column. Columns: {:?}",
            rq.relation.columns
        );

        // Check that SQL includes the column
        assert!(
            sql.contains(CHANGE_ORIGIN_COLUMN),
            "SQL should contain _change_origin: {}",
            sql
        );
    }

    #[test]
    fn test_does_not_duplicate_if_already_present() {
        let pipeline =
            TransformPipeline::empty().with_transformer(Arc::new(ChangeOriginTransformer));

        // Query that already selects _change_origin
        let result = pipeline.compile("from tasks | select {id, content, _change_origin}");
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

        let (_sql, rq) = result.unwrap();

        // Count occurrences of _change_origin
        let count = rq
            .relation
            .columns
            .iter()
            .filter(|col| {
                matches!(col, RelationColumn::Single(Some(name)) if name == CHANGE_ORIGIN_COLUMN)
            })
            .count();

        assert_eq!(
            count, 1,
            "Should have exactly one _change_origin column, found {}",
            count
        );
    }

    #[test]
    fn test_handles_select_star() {
        let pipeline =
            TransformPipeline::empty().with_transformer(Arc::new(ChangeOriginTransformer));

        // SELECT * already includes all columns
        let result = pipeline.compile("from tasks");
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

        let (_sql, rq) = result.unwrap();

        // With wildcard, we should not add another column
        let has_wildcard = rq
            .relation
            .columns
            .iter()
            .any(|col| matches!(col, RelationColumn::Wildcard));

        if has_wildcard {
            // If there's a wildcard, _change_origin should NOT be explicitly added
            let explicit_change_origin = rq.relation.columns.iter().any(|col| {
                matches!(col, RelationColumn::Single(Some(name)) if name == CHANGE_ORIGIN_COLUMN)
            });
            assert!(
                !explicit_change_origin,
                "Should not add explicit _change_origin when wildcard is present"
            );
        }
    }
}
