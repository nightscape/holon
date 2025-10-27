//! JsonAggregationTransformer - Aggregates columns into JSON for heterogeneous UNIONs
//!
//! This PL-phase transformer detects UNION queries (those using `append`) and:
//! 1. Extracts each branch into a CTE (`let` binding)
//! 2. Replaces each branch with `from cte | select { data = s"json_object(*)" }`
//!
//! This approach ensures derived columns are preserved in the CTE, making them
//! visible to `json_object(*)` which captures all columns from the CTE.
//!
//! On the Rust side, `turso.rs` flattens the `data` JSON back into the StorageEntity.

use anyhow::Result;
use prqlc::pr::{Expr, ExprKind, ModuleDef, Pipeline, Stmt, StmtKind, VarDef, VarDefKind};

use super::ast_builders::{
    create_append, create_expr, create_expr_with_alias, create_from, create_func_call,
    create_tuple, has_append_in_expr, is_append_call, is_ident_named,
};
use super::traits::{AstTransformer, TransformPhase};

/// Column name for the JSON aggregated data
pub const DATA_COLUMN: &str = "data";

/// Priority for the JsonAggregationTransformer within the PL phase.
/// Run AFTER ColumnPreservationTransformer (-100) to ensure columns are preserved first.
pub const JSON_AGGREGATION_PRIORITY: i32 = -50;

/// Transformer that aggregates columns into a JSON `data` column for UNION queries.
///
/// This enables heterogeneous entity types to be combined in a single query result,
/// with entity-specific fields stored in the JSON blob and flattened at runtime.
///
/// ## How it works
///
/// For queries with `append` (UNION), this transformer:
/// 1. Extracts each branch (main pipeline + append arguments) into a `let` binding (CTE)
/// 2. Replaces each branch with `from _branch_N | select { data = s"json_object(*)" }`
///
/// This ensures all derived columns are "materialized" in the CTE before json_object(*) runs.
pub struct JsonAggregationTransformer;

impl AstTransformer for JsonAggregationTransformer {
    fn phase(&self) -> TransformPhase {
        TransformPhase::Pl(JSON_AGGREGATION_PRIORITY)
    }

    fn name(&self) -> &'static str {
        "JsonAggregationTransformer"
    }

    fn transform_pl(&self, mut module: ModuleDef) -> Result<ModuleDef> {
        // Find the main query (VarDef with "main" name or the last VarDef)
        let main_stmt_idx = find_main_query_index(&module);

        if let Some(idx) = main_stmt_idx {
            // Extract info we need before mutating
            let transform_info = if let StmtKind::VarDef(var_def) = &module.stmts[idx].kind {
                if let Some(value) = &var_def.value {
                    if has_append_in_expr(value) {
                        // Clone value and compute cte count before mutating
                        let cte_count = count_ctes_needed(value);
                        let (new_stmts, new_main) = transform_to_ctes(value)?;
                        Some((new_stmts, new_main, cte_count))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            // Now apply the transformation
            if let Some((new_stmts, new_main, cte_count)) = transform_info {
                tracing::info!(
                    "JsonAggregationTransformer: Found append in main query, transforming to CTEs"
                );

                // Insert CTE definitions before the main query
                for (i, stmt) in new_stmts.into_iter().enumerate() {
                    module.stmts.insert(idx + i, stmt);
                }

                // Update the main query with the new expression
                let new_idx = idx + cte_count;
                if let StmtKind::VarDef(var_def) = &mut module.stmts[new_idx].kind {
                    var_def.value = Some(Box::new(new_main));
                }
            }
        }

        Ok(module)
    }
}

/// Find the index of the main query statement
fn find_main_query_index(module: &ModuleDef) -> Option<usize> {
    // Look for a VarDef named "main" or the last VarDef
    let mut last_vardef_idx = None;

    for (i, stmt) in module.stmts.iter().enumerate() {
        if let StmtKind::VarDef(var_def) = &stmt.kind {
            if var_def.name == "main" {
                return Some(i);
            }
            last_vardef_idx = Some(i);
        }
    }

    last_vardef_idx
}

/// Count how many CTEs we need to create (main branch + append branches)
fn count_ctes_needed(expr: &Expr) -> usize {
    let mut count = 1; // Main branch

    if let ExprKind::Pipeline(pipeline) = &expr.kind {
        for e in &pipeline.exprs {
            if let ExprKind::FuncCall(func_call) = &e.kind {
                if is_ident_named(&func_call.name, "append") {
                    count += func_call.args.len();
                }
            }
        }
    }

    count
}

/// Transform an append query into CTEs
fn transform_to_ctes(expr: &Expr) -> Result<(Vec<Stmt>, Expr)> {
    let mut cte_stmts = Vec::new();
    let mut branch_idx = 0;

    if let ExprKind::Pipeline(pipeline) = &expr.kind {
        // Extract the main branch (everything before the first append)
        let (main_branch_exprs, append_calls) = split_at_appends(pipeline);

        // Create CTE for main branch
        let main_cte_name = format!("_branch_{}", branch_idx);
        branch_idx += 1;

        let main_branch_expr = Expr {
            kind: ExprKind::Pipeline(Pipeline {
                exprs: main_branch_exprs,
            }),
            span: None,
            alias: None,
            doc_comment: None,
        };

        cte_stmts.push(create_cte_stmt(&main_cte_name, main_branch_expr));

        // Create CTEs for each append argument
        let mut append_cte_names = Vec::new();
        for append_call in &append_calls {
            if let ExprKind::FuncCall(func_call) = &append_call.kind {
                for arg in &func_call.args {
                    let cte_name = format!("_branch_{}", branch_idx);
                    branch_idx += 1;
                    cte_stmts.push(create_cte_stmt(&cte_name, arg.clone()));
                    append_cte_names.push(cte_name);
                }
            }
        }

        // Create the new main query: from _branch_0 | select { data = s"json_object(*)" } | append (...)
        let new_main = create_json_select_with_appends(&main_cte_name, &append_cte_names);

        return Ok((cte_stmts, new_main));
    }

    // If not a pipeline, just return as-is
    Ok((vec![], expr.clone()))
}

/// Split a pipeline into main expressions and append calls
fn split_at_appends(pipeline: &Pipeline) -> (Vec<Expr>, Vec<Expr>) {
    let mut main_exprs = Vec::new();
    let mut append_calls = Vec::new();

    for expr in &pipeline.exprs {
        if is_append_call(expr) {
            append_calls.push(expr.clone());
        } else {
            main_exprs.push(expr.clone());
        }
    }

    (main_exprs, append_calls)
}

/// Create a CTE statement: `let name = (pipeline)`
fn create_cte_stmt(name: &str, value: Expr) -> Stmt {
    Stmt {
        kind: StmtKind::VarDef(VarDef {
            kind: VarDefKind::Let,
            name: name.to_string(),
            value: Some(Box::new(value)),
            ty: None,
        }),
        span: None,
        doc_comment: None,
        annotations: vec![],
    }
}

/// Create `from cte_name | select { data = s"json_object(*)" }`
fn create_json_select_from_cte(cte_name: &str) -> Expr {
    let from_expr = create_from(cte_name);
    let select_expr = create_select_json_object();
    create_expr(ExprKind::Pipeline(Pipeline {
        exprs: vec![from_expr, select_expr],
    }))
}

/// Create `select { data = s"json_object(*)" }`
fn create_select_json_object() -> Expr {
    let sstring_expr = create_expr_with_alias(
        ExprKind::SString(vec![prqlc_parser::generic::InterpolateItem::String(
            "json_object(*)".to_string(),
        )]),
        "data",
    );
    create_func_call("select", vec![create_tuple(vec![sstring_expr])])
}

/// Create the final query with CTEs:
/// `from main_cte | select { data = s"json_object(*)" } | append ( from cte1 | select {...} ) | ...`
fn create_json_select_with_appends(main_cte: &str, append_ctes: &[String]) -> Expr {
    let mut pipeline_exprs = vec![create_from(main_cte), create_select_json_object()];

    for cte_name in append_ctes {
        let append_arg = create_json_select_from_cte(cte_name);
        pipeline_exprs.push(create_append(append_arg));
    }

    create_expr(ExprKind::Pipeline(Pipeline {
        exprs: pipeline_exprs,
    }))
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
    fn test_transforms_union_to_cte_with_json_object() {
        let pipeline =
            TransformPipeline::empty().with_transformer(Arc::new(JsonAggregationTransformer));

        let result = pipeline.compile(
            r#"
            from projects
            derive { entity_name = "projects" }
            select {id, name, entity_name}
            append (
                from tasks
                derive { entity_name = "tasks" }
                select {id, name = content, entity_name}
            )
            "#,
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

        let (sql, _rq) = result.unwrap();
        println!("Generated SQL:\n{}", sql);

        let sql_lower = sql.to_lowercase();

        // Should have CTEs
        assert!(
            sql_lower.contains("with"),
            "Should have WITH clause for CTEs: {}",
            sql
        );
        assert!(
            sql_lower.contains("_branch_0"),
            "Should have _branch_0 CTE: {}",
            sql
        );
        assert!(
            sql_lower.contains("_branch_1"),
            "Should have _branch_1 CTE: {}",
            sql
        );

        // Should have json_object(*)
        assert!(
            sql_lower.contains("json_object(*)"),
            "Should contain json_object(*): {}",
            sql
        );

        // Should still have UNION
        assert!(sql_lower.contains("union"), "Should contain UNION: {}", sql);
    }

    #[test]
    fn test_preserves_derived_columns_in_cte() {
        let pipeline =
            TransformPipeline::empty().with_transformer(Arc::new(JsonAggregationTransformer));

        let result = pipeline.compile(
            r#"
            from projects
            derive {
                content = name,
                entity_name = "projects",
                ui = s"'project_ui'"
            }
            select {id, parent_id, content, entity_name, ui}
            append (
                from tasks
                derive {
                    content = content,
                    entity_name = "tasks",
                    ui = s"'task_ui'"
                }
                select {id, parent_id, content, entity_name, ui}
            )
            "#,
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

        let (sql, _rq) = result.unwrap();
        println!("Generated SQL with derived columns:\n{}", sql);

        // CTEs should preserve derived columns
        assert!(
            sql.contains("entity_name"),
            "CTE should contain entity_name: {}",
            sql
        );
        assert!(
            sql.contains("'projects'") || sql.contains("\"projects\""),
            "CTE should contain 'projects': {}",
            sql
        );
    }

    #[test]
    fn test_count_ctes_needed() {
        let query = r#"
from projects
select {id, name}
append (
    from tasks
    select {id, name = content}
)
append (
    from issues
    select {id, name = title}
)
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");

        for stmt in &module.stmts {
            if let StmtKind::VarDef(var_def) = &stmt.kind {
                if let Some(value) = &var_def.value {
                    let count = count_ctes_needed(value);
                    assert_eq!(count, 3, "Should need 3 CTEs (main + 2 appends)");
                }
            }
        }
    }
}
