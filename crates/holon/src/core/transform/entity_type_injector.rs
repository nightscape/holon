//! EntityTypeInjector - Injects `entity_name` column for operation dispatch
//!
//! This transformer ensures that each row in query results includes an `entity_name`
//! column identifying which entity type the row represents. This enables:
//! - Correct operation dispatch in queries with UNIONs (where rows come from different tables)
//! - Frontend to look up available operations per row based on entity type
//!
//! The injection happens at the RQ (Relational Query) phase by adding a Compute transform
//! with a literal string value for the table name.

use std::collections::HashMap;

use anyhow::Result;
use prqlc::ir::pl::{Literal, TableExternRef};
use prqlc::ir::rq::{
    CId, Compute, Expr, ExprKind, RelationColumn, RelationKind, RelationalQuery, TId, TableDecl,
    Transform,
};
use tracing::debug;

use super::traits::{AstTransformer, TransformPhase};

/// Column name for entity type identification
pub const ENTITY_NAME_COLUMN: &str = "entity_name";

/// Priority for the EntityTypeInjector within the Rq phase.
/// Run early (low number) so entity_name is available for other transforms.
pub const ENTITY_TYPE_PRIORITY: i32 = 10;

/// Transformer that injects `entity_name` column into query results.
///
/// For each table source in the query, adds a computed column with the table name
/// as a literal string. This works correctly with UNIONs because each branch
/// gets its own entity_name value before being merged.
pub struct EntityTypeInjector;

impl AstTransformer for EntityTypeInjector {
    fn phase(&self) -> TransformPhase {
        TransformPhase::Rq(ENTITY_TYPE_PRIORITY)
    }

    fn name(&self) -> &'static str {
        "EntityTypeInjector"
    }

    fn transform_rq(&self, mut rq: RelationalQuery) -> Result<RelationalQuery> {
        // Build a map from TId to table name for quick lookup
        let table_names: HashMap<TId, String> = rq
            .tables
            .iter()
            .filter_map(|t| get_table_name_from_decl(t).map(|name| (t.id, name)))
            .collect();

        // Find the maximum CId in use so we can generate new unique ones
        let mut max_cid = find_max_cid(&rq);

        // Process each table declaration (for CTEs/subqueries)
        for table in &mut rq.tables {
            if let RelationKind::Pipeline(ref mut transforms) = table.relation.kind {
                max_cid = inject_entity_name_into_pipeline(
                    transforms,
                    &mut table.relation.columns,
                    &table_names,
                    max_cid,
                )?;
            }
        }

        // Process the main relation
        if let RelationKind::Pipeline(ref mut transforms) = rq.relation.kind {
            inject_entity_name_into_pipeline(
                transforms,
                &mut rq.relation.columns,
                &table_names,
                max_cid,
            )?;
        }

        debug!(
            "EntityTypeInjector: Processed query with {} tables",
            rq.tables.len()
        );

        Ok(rq)
    }
}

/// Extract table name from a TableDecl
fn get_table_name_from_decl(decl: &TableDecl) -> Option<String> {
    match &decl.relation.kind {
        RelationKind::ExternRef(TableExternRef::LocalTable(ident)) => Some(ident.name.clone()),
        _ => decl.name.clone(),
    }
}

/// Find the maximum CId used in the query
fn find_max_cid(rq: &RelationalQuery) -> usize {
    let mut max = 0;

    // Check all tables
    for table in &rq.tables {
        if let RelationKind::Pipeline(transforms) = &table.relation.kind {
            max = max.max(find_max_cid_in_transforms(transforms));
        }
    }

    // Check main relation
    if let RelationKind::Pipeline(transforms) = &rq.relation.kind {
        max = max.max(find_max_cid_in_transforms(transforms));
    }

    max
}

/// Find max CId in transforms
fn find_max_cid_in_transforms(transforms: &[Transform]) -> usize {
    let mut max = 0;

    for transform in transforms {
        match transform {
            Transform::From(table_ref) => {
                for (_, cid) in &table_ref.columns {
                    max = max.max(cid.get());
                }
            }
            Transform::Compute(compute) => {
                max = max.max(compute.id.get());
                max = max.max(find_max_cid_in_expr(&compute.expr));
            }
            Transform::Select(cids) => {
                for cid in cids {
                    max = max.max(cid.get());
                }
            }
            Transform::Filter(expr) => {
                max = max.max(find_max_cid_in_expr(expr));
            }
            Transform::Aggregate { partition, compute } => {
                for cid in partition {
                    max = max.max(cid.get());
                }
                for cid in compute {
                    max = max.max(cid.get());
                }
            }
            Transform::Sort(sorts) => {
                for sort in sorts {
                    max = max.max(sort.column.get());
                }
            }
            Transform::Take(take) => {
                for cid in &take.partition {
                    max = max.max(cid.get());
                }
                for sort in &take.sort {
                    max = max.max(sort.column.get());
                }
            }
            Transform::Join { with, filter, .. } => {
                for (_, cid) in &with.columns {
                    max = max.max(cid.get());
                }
                max = max.max(find_max_cid_in_expr(filter));
            }
            Transform::Append(table_ref) => {
                for (_, cid) in &table_ref.columns {
                    max = max.max(cid.get());
                }
            }
            Transform::Loop(inner_transforms) => {
                max = max.max(find_max_cid_in_transforms(inner_transforms));
            }
        }
    }

    max
}

/// Find max CId in an expression
fn find_max_cid_in_expr(expr: &Expr) -> usize {
    match &expr.kind {
        ExprKind::ColumnRef(cid) => cid.get(),
        ExprKind::Operator { args, .. } => args.iter().map(find_max_cid_in_expr).max().unwrap_or(0),
        ExprKind::Case(cases) => cases
            .iter()
            .flat_map(|c| {
                [
                    find_max_cid_in_expr(&c.condition),
                    find_max_cid_in_expr(&c.value),
                ]
            })
            .max()
            .unwrap_or(0),
        ExprKind::SString(_) => 0,
        ExprKind::Array(exprs) => exprs.iter().map(find_max_cid_in_expr).max().unwrap_or(0),
        ExprKind::Literal(_) | ExprKind::Param(_) => 0,
    }
}

/// Inject entity_name column into a pipeline's transforms and relation columns
fn inject_entity_name_into_pipeline(
    transforms: &mut Vec<Transform>,
    columns: &mut Vec<RelationColumn>,
    table_names: &HashMap<TId, String>,
    mut max_cid: usize,
) -> Result<usize> {
    // Find the From transform and get the table name
    let table_name = find_source_table_name(transforms, table_names);

    if let Some(name) = table_name {
        // Check if there's a Select transform - we need it to properly output the column
        let has_select = transforms.iter().any(|t| matches!(t, Transform::Select(_)));

        if has_select {
            // Generate new CId for the entity_name column
            max_cid += 1;
            let entity_name_cid = CId::from(max_cid);

            // Create a Compute transform with a literal string
            let compute = Transform::Compute(Compute {
                id: entity_name_cid,
                expr: Expr {
                    kind: ExprKind::Literal(Literal::String(name.clone())),
                    span: None,
                },
                window: None,
                is_aggregation: false,
            });

            // Find the position after From but before Select/Filter/etc
            let insert_pos = find_compute_insert_position(transforms);
            transforms.insert(insert_pos, compute);

            // Add CId to ALL Select transforms AND column to relation.columns
            // These must be updated together to maintain the invariant:
            // output_cids.len() == output_cols.len()
            // Note: We must update ALL Select transforms, especially the last one,
            // because prqlc's determine_select_columns looks at the last transform.
            let mut select_updated = false;
            for transform in transforms.iter_mut() {
                if let Transform::Select(cids) = transform {
                    if !cids.contains(&entity_name_cid) {
                        cids.push(entity_name_cid);
                        select_updated = true;
                    }
                }
            }

            // Only add to columns if we successfully added to Select
            if select_updated {
                columns.push(RelationColumn::Single(Some(ENTITY_NAME_COLUMN.to_string())));
            }

            debug!(
                "EntityTypeInjector: Injected entity_name='{}' with CId {}",
                name,
                entity_name_cid.get()
            );
        }
    }

    Ok(max_cid)
}

/// Find the source table name from the pipeline's From transform
fn find_source_table_name(
    transforms: &[Transform],
    table_names: &HashMap<TId, String>,
) -> Option<String> {
    for transform in transforms {
        if let Transform::From(table_ref) = transform {
            if let Some(name) = table_names.get(&table_ref.source) {
                return Some(name.clone());
            }
        }
    }
    None
}

/// Find the position to insert a Compute transform (after From, before most others)
fn find_compute_insert_position(transforms: &[Transform]) -> usize {
    let mut pos = 0;
    for (i, t) in transforms.iter().enumerate() {
        match t {
            Transform::From(_) | Transform::Join { .. } | Transform::Append(_) => {
                pos = i + 1;
            }
            Transform::Compute(_) => {
                pos = i + 1;
            }
            _ => break,
        }
    }
    pos
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::transform::TransformPipeline;
    use std::sync::Arc;

    #[test]
    fn test_injects_entity_name_simple_query() {
        let pipeline = TransformPipeline::empty().with_transformer(Arc::new(EntityTypeInjector));

        let result = pipeline.compile("from tasks | select {id, content}");
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

        let (sql, _rq) = result.unwrap();

        // Check that SQL includes the column with 'tasks' as the value
        let sql_lower = sql.to_lowercase();
        assert!(
            sql_lower.contains("'tasks'") || sql_lower.contains("\"tasks\""),
            "SQL should contain literal 'tasks': {}",
            sql
        );
    }

    #[test]
    fn test_injects_entity_name_union_query() {
        let pipeline = TransformPipeline::empty().with_transformer(Arc::new(EntityTypeInjector));

        let result = pipeline.compile(
            r#"
            from projects
            select {id, name}
            append (
                from tasks
                select {id, name = content}
            )
            "#,
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

        let (sql, _rq) = result.unwrap();

        // Both branches should have their respective entity_name values
        let sql_lower = sql.to_lowercase();

        let has_projects = sql_lower.contains("'projects'") || sql_lower.contains("\"projects\"");
        let has_tasks = sql_lower.contains("'tasks'") || sql_lower.contains("\"tasks\"");

        assert!(
            has_projects,
            "SQL should contain literal 'projects' for first branch: {}",
            sql
        );
        assert!(
            has_tasks,
            "SQL should contain literal 'tasks' for second branch: {}",
            sql
        );
    }

    #[test]
    fn test_find_max_cid() {
        let pipeline = TransformPipeline::empty();
        let result = pipeline.compile("from tasks | select {id, content}");
        assert!(result.is_ok());
        let (_, rq) = result.unwrap();

        let max = find_max_cid(&rq);
        assert!(max >= 2, "Should have found at least 2 CIds, got {}", max);
    }
}
