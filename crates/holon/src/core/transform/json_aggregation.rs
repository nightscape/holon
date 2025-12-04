//! JsonAggregationTransformer - Aggregates columns into JSON for heterogeneous UNIONs
//!
//! This transformer detects UNION queries (those using `append`) and wraps all columns
//! into a `json_object()` call, allowing different entity types to have different
//! columns while still being UNION-compatible.
//!
//! The transformation:
//! 1. Detects queries with `Transform::Append`
//! 2. For each branch, enumerates all available columns
//! 3. Creates a `json_object('col1', col1, 'col2', col2, ...)` expression
//! 4. Adds it as a computed `data` column
//!
//! On the Rust side, `turso.rs` flattens the `data` JSON back into the StorageEntity.

use std::collections::HashMap;

use anyhow::Result;
use prqlc::ir::rq::{
    CId, Compute, Expr, ExprKind, RelationColumn, RelationKind, RelationalQuery, TId, Transform,
};
use prqlc_parser::generic::InterpolateItem;
use tracing::debug;

use super::traits::{AstTransformer, TransformPhase};

/// Column name for the JSON aggregated data
pub const DATA_COLUMN: &str = "data";

/// Priority for the JsonAggregationTransformer within the Rq phase.
/// Run after EntityTypeInjector (10) but before ChangeOriginTransformer (100).
pub const JSON_AGGREGATION_PRIORITY: i32 = 50;

/// Transformer that aggregates columns into a JSON `data` column for UNION queries.
///
/// This enables heterogeneous entity types to be combined in a single query result,
/// with entity-specific fields stored in the JSON blob and flattened at runtime.
pub struct JsonAggregationTransformer;

impl AstTransformer for JsonAggregationTransformer {
    fn phase(&self) -> TransformPhase {
        TransformPhase::Rq(JSON_AGGREGATION_PRIORITY)
    }

    fn name(&self) -> &'static str {
        "JsonAggregationTransformer"
    }

    fn transform_rq(&self, mut rq: RelationalQuery) -> Result<RelationalQuery> {
        // Only transform queries with UNION (Append transforms)
        if !has_append_transforms(&rq) {
            tracing::info!("JsonAggregationTransformer: No append transforms found, skipping");
            return Ok(rq);
        }

        tracing::info!(
            "JsonAggregationTransformer: Found append transforms, processing {} tables",
            rq.tables.len()
        );

        // Find the maximum CId in use so we can generate new unique ones
        let mut max_cid = find_max_cid(&rq);

        // Build a map from TId to table name for column enumeration
        let table_decls: HashMap<TId, Vec<(String, CId)>> = rq
            .tables
            .iter()
            .filter_map(|t| {
                if let RelationKind::Pipeline(transforms) = &t.relation.kind {
                    Some((t.id, enumerate_columns_from_transforms(transforms)))
                } else {
                    None
                }
            })
            .collect();

        // Process each table declaration (for CTEs/subqueries)
        for table in &mut rq.tables {
            if let RelationKind::Pipeline(ref mut transforms) = table.relation.kind {
                max_cid = inject_json_object_into_pipeline(
                    transforms,
                    &mut table.relation.columns,
                    &table_decls,
                    max_cid,
                )?;
            }
        }

        // Process the main relation
        if let RelationKind::Pipeline(ref mut transforms) = rq.relation.kind {
            inject_json_object_into_pipeline(
                transforms,
                &mut rq.relation.columns,
                &table_decls,
                max_cid,
            )?;
        }

        Ok(rq)
    }
}

/// Check if the query has any Append transforms (indicating a UNION)
fn has_append_transforms(rq: &RelationalQuery) -> bool {
    // Check main relation
    if let RelationKind::Pipeline(transforms) = &rq.relation.kind {
        if transforms.iter().any(|t| matches!(t, Transform::Append(_))) {
            return true;
        }
    }

    // Check table declarations
    for table in &rq.tables {
        if let RelationKind::Pipeline(transforms) = &table.relation.kind {
            if transforms.iter().any(|t| matches!(t, Transform::Append(_))) {
                return true;
            }
        }
    }

    false
}

/// Find the maximum CId used in the query
fn find_max_cid(rq: &RelationalQuery) -> usize {
    let mut max = 0;

    // Check all tables
    for table in &rq.tables {
        if let RelationKind::Pipeline(transforms) = &table.relation.kind {
            max = max.max(find_max_cid_in_transforms(transforms));
        }
        // Also check relation columns
        for col in &table.relation.columns {
            if let RelationColumn::Single(Some(_)) = col {
                // Column exists but we can't get CId from RelationColumn directly
            }
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
        ExprKind::SString(items) => items
            .iter()
            .filter_map(|item| match item {
                InterpolateItem::Expr { expr, .. } => Some(find_max_cid_in_expr(expr)),
                InterpolateItem::String(_) => None,
            })
            .max()
            .unwrap_or(0),
        ExprKind::Array(exprs) => exprs.iter().map(find_max_cid_in_expr).max().unwrap_or(0),
        ExprKind::Literal(_) | ExprKind::Param(_) => 0,
    }
}

/// Enumerate all columns available in a pipeline's transforms
fn enumerate_columns_from_transforms(transforms: &[Transform]) -> Vec<(String, CId)> {
    let mut columns = Vec::new();

    for transform in transforms {
        match transform {
            Transform::From(table_ref) => {
                for (col, cid) in &table_ref.columns {
                    if let RelationColumn::Single(Some(name)) = col {
                        columns.push((name.clone(), *cid));
                    }
                }
            }
            Transform::Compute(_) => {
                // For computed columns, we'd need to track their names from relation.columns
                // This is handled by the caller looking at relation.columns
            }
            Transform::Select(_) => {
                // Select narrows down the columns - only these CIds are in the output
            }
            _ => {}
        }
    }

    columns
}

/// Inject json_object into a pipeline's transforms
fn inject_json_object_into_pipeline(
    transforms: &mut Vec<Transform>,
    columns: &mut Vec<RelationColumn>,
    _table_decls: &HashMap<TId, Vec<(String, CId)>>,
    mut max_cid: usize,
) -> Result<usize> {
    // Strategy: Include ALL columns in json_object (From + Compute), not just Select columns
    // This enables heterogeneous UNIONs where each branch has different source columns,
    // but they all get packed into the `data` blob for the frontend to use.

    // Step 1: Collect ALL columns from From transforms (base table columns)
    let mut all_columns: Vec<(String, CId)> = Vec::new();

    for transform in transforms.iter() {
        if let Transform::From(table_ref) = transform {
            for (col, cid) in &table_ref.columns {
                if let RelationColumn::Single(Some(name)) = col {
                    all_columns.push((name.clone(), *cid));
                }
            }
        }
    }

    // Step 2: Collect columns from Compute transforms (derived columns)
    // Match Compute CIds with relation.columns names using Select ordering
    let select_cids: Option<Vec<CId>> = transforms.iter().find_map(|t| {
        if let Transform::Select(cids) = t {
            Some(cids.clone())
        } else {
            None
        }
    });

    // Build CId -> name mapping from Select + relation.columns
    let mut cid_to_name: HashMap<CId, String> = HashMap::new();
    if let Some(cids) = &select_cids {
        for (i, cid) in cids.iter().enumerate() {
            if let Some(RelationColumn::Single(Some(name))) = columns.get(i) {
                cid_to_name.insert(*cid, name.clone());
            }
        }
    }

    // Add Compute columns using the CId -> name mapping
    for transform in transforms.iter() {
        if let Transform::Compute(compute) = transform {
            if let Some(name) = cid_to_name.get(&compute.id) {
                // Only add if not already present (From columns take precedence)
                if !all_columns.iter().any(|(n, _)| n == name) {
                    all_columns.push((name.clone(), compute.id));
                }
            }
        }
    }

    // If no columns found, skip
    if all_columns.is_empty() {
        return Ok(max_cid);
    }

    // Check if there's already a 'data' column (manual json_object already added)
    let has_data_column = all_columns.iter().any(|(name, _)| name == DATA_COLUMN)
        || columns
            .iter()
            .any(|col| matches!(col, RelationColumn::Single(Some(name)) if name == DATA_COLUMN));
    if has_data_column {
        debug!("JsonAggregationTransformer: 'data' column already exists, skipping");
        return Ok(max_cid);
    }

    // Build json_object expression using SString (s-string interpolation)
    // Format: json_object('col1', {col1}, 'col2', {col2}, ...)
    let mut items: Vec<InterpolateItem<Expr>> = Vec::new();
    items.push(InterpolateItem::String("json_object(".to_string()));

    for (i, (name, cid)) in all_columns.iter().enumerate() {
        if i > 0 {
            items.push(InterpolateItem::String(", ".to_string()));
        }
        // Add 'column_name', {column_ref}
        // Escape single quotes in column names to prevent SQL injection and syntax errors
        let escaped_name = name.replace("'", "''");
        items.push(InterpolateItem::String(format!("'{}', ", escaped_name)));
        items.push(InterpolateItem::Expr {
            expr: Box::new(Expr {
                kind: ExprKind::ColumnRef(*cid),
                span: None,
            }),
            format: None,
        });
    }

    items.push(InterpolateItem::String(")".to_string()));

    // Create json_object s-string expression
    let json_object_expr = Expr {
        kind: ExprKind::SString(items),
        span: None,
    };

    // Generate new CId for the data column
    max_cid += 1;
    let data_cid = CId::from(max_cid);

    // Create Compute transform for data column
    let compute = Transform::Compute(Compute {
        id: data_cid,
        expr: json_object_expr,
        window: None,
        is_aggregation: false,
    });

    // Find the position to insert (after all Compute transforms, before Select)
    let insert_pos = find_compute_insert_position(transforms);
    transforms.insert(insert_pos, compute);

    // Check if we have a Wildcard column (from `select { this.* }`)
    // If so, we need to replace SELECT * with just the essential columns + data
    // to maintain UNION compatibility (different tables have different columns)
    let has_wildcard = columns
        .iter()
        .any(|c| matches!(c, RelationColumn::Wildcard));

    if has_wildcard {
        debug!(
            "JsonAggregationTransformer: Detected Wildcard, replacing SELECT * with data column only"
        );

        // For UNION compatibility, we replace the Select to only include data column
        // This way both branches will have the same output schema: just `data`
        for transform in transforms.iter_mut() {
            if let Transform::Select(cids) = transform {
                // Replace the Select to only include the data column
                // All other data is packed into the json_object
                *cids = vec![data_cid];
            }
        }

        // Replace relation columns: remove Wildcard, keep only data
        columns.clear();
        columns.push(RelationColumn::Single(Some(DATA_COLUMN.to_string())));
    } else {
        // No wildcard - add data column to existing Select
        for transform in transforms.iter_mut() {
            if let Transform::Select(cids) = transform {
                if !cids.contains(&data_cid) {
                    cids.push(data_cid);
                }
            }
        }

        // Add to relation columns
        columns.push(RelationColumn::Single(Some(DATA_COLUMN.to_string())));
    }

    // Log the columns being included in json_object for debugging
    let column_names: Vec<&str> = all_columns.iter().map(|(name, _)| name.as_str()).collect();
    debug!(
        "JsonAggregationTransformer: Injected json_object with columns {:?}, data CId = {}",
        column_names,
        data_cid.get()
    );

    Ok(max_cid)
}

/// Find the position to insert a Compute transform (after From/Compute, before Select/Filter)
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
    fn test_skips_non_union_queries() {
        let pipeline =
            TransformPipeline::empty().with_transformer(Arc::new(JsonAggregationTransformer));

        let result = pipeline.compile("from tasks | select {id, content}");
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

        let (sql, _rq) = result.unwrap();

        // Non-UNION queries compile successfully and don't contain json_object
        let sql_lower = sql.to_lowercase();
        assert!(
            !sql_lower.contains("json_object"),
            "Non-UNION query should not contain json_object: {}",
            sql
        );
    }

    #[test]
    fn test_detects_union_queries() {
        let rq_result = TransformPipeline::empty().compile(
            r#"
            from projects
            select {id, name}
            append (
                from tasks
                select {id, name = content}
            )
            "#,
        );
        assert!(rq_result.is_ok());
        let (_, rq) = rq_result.unwrap();

        assert!(
            has_append_transforms(&rq),
            "Should detect append transforms in UNION query"
        );
    }

    #[test]
    fn test_does_not_detect_non_union_queries() {
        let rq_result = TransformPipeline::empty().compile("from tasks | select {id, content}");
        assert!(rq_result.is_ok());
        let (_, rq) = rq_result.unwrap();

        assert!(
            !has_append_transforms(&rq),
            "Should not detect append transforms in non-UNION query"
        );
    }

    #[test]
    fn test_enumerate_columns() {
        let rq_result = TransformPipeline::empty().compile("from tasks | select {id, content}");
        assert!(rq_result.is_ok());
        let (_, rq) = rq_result.unwrap();

        // Get the main relation transforms
        if let RelationKind::Pipeline(transforms) = &rq.relation.kind {
            let columns = enumerate_columns_from_transforms(transforms);
            assert!(
                !columns.is_empty(),
                "Should enumerate at least some columns"
            );
            // Check that common column names are found
            let column_names: Vec<&str> = columns.iter().map(|(name, _)| name.as_str()).collect();
            assert!(
                column_names.contains(&"id") || column_names.contains(&"content"),
                "Should find id or content column, found: {:?}",
                column_names
            );
        } else {
            panic!("Expected Pipeline relation kind");
        }
    }

    #[test]
    fn test_find_max_cid() {
        let rq_result = TransformPipeline::empty().compile("from tasks | select {id, content}");
        assert!(rq_result.is_ok());
        let (_, rq) = rq_result.unwrap();

        let max_cid = find_max_cid(&rq);
        assert!(max_cid >= 2, "Should find at least 2 CIds, got {}", max_cid);
    }

    #[test]
    fn test_injects_json_object_in_union_query() {
        let pipeline =
            TransformPipeline::empty().with_transformer(Arc::new(JsonAggregationTransformer));

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
        println!("Generated SQL for UNION with json_object:\n{}", sql);
        let sql_lower = sql.to_lowercase();
        assert!(
            sql_lower.contains("json_object"),
            "UNION query should contain json_object: {}",
            sql
        );
    }

    #[test]
    fn test_manual_json_object_in_prql_works() {
        // This test verifies that writing json_object() using s-strings in PRQL works
        // (the approach used in the actual queries until automatic injection is ready)
        // Note: PRQL function calls don't support comma-separated args, so we use s-strings
        let pipeline = TransformPipeline::empty();

        let result = pipeline.compile(
            r#"
            from projects
            derive { data = s"json_object('id', {id}, 'name', {name})" }
            select {id, name, data}
            append (
                from tasks
                derive { data = s"json_object('id', {id}, 'name', {content})" }
                select {id, name = content, data}
            )
            "#,
        );
        assert!(
            result.is_ok(),
            "Manual json_object compilation failed: {:?}",
            result.err()
        );

        let (sql, _rq) = result.unwrap();
        let sql_lower = sql.to_lowercase();

        assert!(
            sql_lower.contains("json_object"),
            "Manual PRQL should produce json_object in SQL: {}",
            sql
        );
        assert!(
            sql_lower.contains("union"),
            "Should produce UNION SQL: {}",
            sql
        );

        println!("Manual json_object SQL:\n{}", sql);
    }

    #[test]
    fn test_inspect_sstring_format() {
        // Debug test to understand what format value PRQL uses for s-string interpolation
        let pipeline = TransformPipeline::empty();

        let result = pipeline.compile(
            r#"
            from test
            derive { x = s"json_object('id', {id})" }
            select { id, x }
            "#,
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

        let (sql, rq) = result.unwrap();
        println!("SQL: {}", sql);

        // Inspect the RQ to find the s-string expression
        if let RelationKind::Pipeline(transforms) = &rq.relation.kind {
            for t in transforms {
                if let Transform::Compute(compute) = t {
                    if let ExprKind::SString(items) = &compute.expr.kind {
                        println!("Found SString with {} items:", items.len());
                        for (i, item) in items.iter().enumerate() {
                            match item {
                                InterpolateItem::String(s) => {
                                    println!("  Item {}: String({:?})", i, s);
                                }
                                InterpolateItem::Expr { expr, format } => {
                                    println!(
                                        "  Item {}: Expr {{ format: {:?}, kind: {:?} }}",
                                        i, format, expr.kind
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
