//! ColumnPreservationTransformer - Preserves all columns before PRQL optimization
//!
//! This PL-phase transformer runs BEFORE PRQL's optimization pass to ensure
//! all columns from source tables are preserved for heterogeneous UNIONs.
//!
//! Without this, PRQL's optimizer removes unused columns, which means the
//! JsonAggregationTransformer (RQ phase) can't include them in the `data` blob.
//!
//! ## How it works
//!
//! For queries with `append` (UNION), this transformer appends `select { this.* }`
//! at the end of every branch (pipeline), which translates to `SELECT *` in SQL.
//! This ensures all columns (base + derived) are preserved through optimization.
//!
//! We always append, regardless of existing selects, because:
//! - It never hurts: `this.*` selects everything available at that point
//! - It can hurt if we don't: derived columns after a select might be lost
//! - It respects user intent: if they already selected specific columns, `this.*`
//!   only sees those

use std::collections::HashMap;

use anyhow::Result;
use prqlc::pr::{Expr, ExprKind, FuncCall, Ident, ModuleDef, StmtKind};

use super::traits::{AstTransformer, TransformPhase};

/// Priority for the ColumnPreservationTransformer within the PL phase.
/// Run early to ensure columns are preserved before any other transformations.
pub const COLUMN_PRESERVATION_PRIORITY: i32 = -100;

/// Transformer that preserves all source columns for heterogeneous UNION queries.
///
/// This runs in the PL phase, before PRQL optimization, by appending
/// `select { this.* }` at the end of every branch to preserve all columns.
pub struct ColumnPreservationTransformer;

impl AstTransformer for ColumnPreservationTransformer {
    fn phase(&self) -> TransformPhase {
        TransformPhase::Pl(COLUMN_PRESERVATION_PRIORITY)
    }

    fn name(&self) -> &'static str {
        "ColumnPreservationTransformer"
    }

    fn transform_pl(&self, mut module: ModuleDef) -> Result<ModuleDef> {
        // Walk through all statements looking for pipelines with append
        for stmt in &mut module.stmts {
            if let StmtKind::VarDef(var_def) = &mut stmt.kind {
                if let Some(value) = &mut var_def.value {
                    if has_append_in_expr(value) {
                        tracing::debug!(
                            "ColumnPreservationTransformer: Found append, appending select {{ this.* }} to all branches"
                        );
                        append_select_star_to_branches(value, true);
                    }
                }
            }
        }

        Ok(module)
    }
}

/// Check if an expression contains an append (UNION) call
fn has_append_in_expr(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Pipeline(pipeline) => pipeline
            .exprs
            .iter()
            .any(|e| is_append_call(e) || has_append_in_expr(e)),
        ExprKind::FuncCall(func_call) => {
            // Check if this is an append call
            if is_ident_named(&func_call.name, "append") {
                return true;
            }
            // Check arguments
            func_call.args.iter().any(has_append_in_expr)
        }
        ExprKind::Tuple(items) | ExprKind::Array(items) => items.iter().any(has_append_in_expr),
        _ => false,
    }
}

/// Check if an expression is an append function call
fn is_append_call(expr: &Expr) -> bool {
    if let ExprKind::FuncCall(func_call) = &expr.kind {
        return is_ident_named(&func_call.name, "append");
    }
    false
}

/// Check if an expression is an identifier with a specific name
fn is_ident_named(expr: &Expr, name: &str) -> bool {
    if let ExprKind::Ident(ident) = &expr.kind {
        return ident.path.is_empty() && ident.name == name;
    }
    false
}

/// Create a `select { this.* }` expression
fn create_select_this_star() -> Expr {
    let this_star = Expr {
        kind: ExprKind::Ident(Ident::from_path(vec!["this".to_string(), "*".to_string()])),
        span: None,
        alias: None,
        doc_comment: None,
    };

    let tuple = Expr {
        kind: ExprKind::Tuple(vec![this_star]),
        span: None,
        alias: None,
        doc_comment: None,
    };

    let select_ident = Expr {
        kind: ExprKind::Ident(Ident {
            path: vec![],
            name: "select".to_string(),
        }),
        span: None,
        alias: None,
        doc_comment: None,
    };

    Expr {
        kind: ExprKind::FuncCall(FuncCall {
            name: Box::new(select_ident),
            args: vec![tuple],
            named_args: HashMap::new(),
        }),
        span: None,
        alias: None,
        doc_comment: None,
    }
}

/// Append `select { this.* }` to the end of every branch in an append query.
///
/// We always append, regardless of existing selects, because:
/// - It never hurts: `this.*` selects everything available at that point
/// - It can hurt if we don't: derived columns after a select might be lost
/// - It respects user intent: if they already selected specific columns, `this.*` only sees those
///
/// The `is_branch` parameter indicates whether this is a branch that should get
/// the select appended (true for the main pipeline and append arguments).
fn append_select_star_to_branches(expr: &mut Expr, is_branch: bool) {
    match &mut expr.kind {
        ExprKind::Pipeline(pipeline) => {
            // First, recursively process any nested append calls
            for e in &mut pipeline.exprs {
                if let ExprKind::FuncCall(func_call) = &mut e.kind {
                    if is_ident_named(&func_call.name, "append") {
                        // Process append arguments (inner pipelines)
                        for arg in &mut func_call.args {
                            append_select_star_to_branches(arg, true);
                        }
                    }
                }
            }

            // Now append select { this.* } to this branch
            if is_branch {
                pipeline.exprs.push(create_select_this_star());
                tracing::debug!(
                    "ColumnPreservationTransformer: Appended select {{ this.* }} to branch"
                );
            }
        }
        ExprKind::FuncCall(func_call) => {
            // If this is an append call, process its arguments as branches
            if is_ident_named(&func_call.name, "append") {
                for arg in &mut func_call.args {
                    append_select_star_to_branches(arg, true);
                }
            } else {
                // Process other function call arguments
                for arg in &mut func_call.args {
                    append_select_star_to_branches(arg, false);
                }
            }
        }
        ExprKind::Tuple(items) | ExprKind::Array(items) => {
            for item in items {
                append_select_star_to_branches(item, false);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detects_append() {
        let query = r#"
from projects
select { id, name }
append (
    from tasks
    select { id, name = content }
)
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");

        for stmt in &module.stmts {
            if let StmtKind::VarDef(var_def) = &stmt.kind {
                if let Some(value) = &var_def.value {
                    assert!(has_append_in_expr(value), "Should detect append");
                }
            }
        }
    }

    #[test]
    fn test_no_append_detection() {
        let query = r#"
from projects
select { id, name }
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");

        for stmt in &module.stmts {
            if let StmtKind::VarDef(var_def) = &stmt.kind {
                if let Some(value) = &var_def.value {
                    assert!(!has_append_in_expr(value), "Should not detect append");
                }
            }
        }
    }

    #[test]
    fn test_transform_appends_select_star_to_all_branches() {
        let query = r#"
from products
derive { entity_name = "products" }
select { id, name, entity_name }
append (
    from services
    derive { entity_name = "services" }
    select { id, name = title, entity_name }
)
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");
        let transformer = ColumnPreservationTransformer;

        let transformed = transformer.transform_pl(module).expect("Should transform");

        // Convert to SQL
        let rq = prqlc::pl_to_rq(transformed).expect("Should convert to RQ");
        let sql = prqlc::rq_to_sql(rq, &Default::default()).expect("Should generate SQL");

        println!("Generated SQL:\n{}", sql);

        // The SQL should contain SELECT * (from this.* appended to each branch)
        assert!(sql.contains("*"), "Should contain wildcard SELECT *");
        assert!(sql.contains("UNION"), "Should contain UNION");
    }

    #[test]
    fn test_transform_with_derive_after_select() {
        // Test that derive after select is preserved
        let query = r#"
from products
select { id, name }
derive { entity_name = "products" }
append (
    from services
    select { id, title }
    derive { entity_name = "services", name = title }
)
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");
        let transformer = ColumnPreservationTransformer;

        let transformed = transformer.transform_pl(module).expect("Should transform");

        // Convert to SQL
        let rq = prqlc::pl_to_rq(transformed).expect("Should convert to RQ");
        let sql = prqlc::rq_to_sql(rq, &Default::default()).expect("Should generate SQL");

        println!("Generated SQL (derive after select):\n{}", sql);

        // The SQL should contain SELECT * and include the derived columns
        assert!(sql.contains("*"), "Should contain wildcard SELECT *");
        assert!(
            sql.contains("entity_name"),
            "Should preserve derived entity_name column"
        );
    }

    #[test]
    fn test_transform_without_explicit_select() {
        // Test query with no explicit select - should still append select { this.* }
        let query = r#"
from products
derive { entity_name = "products", display_name = name }
append (
    from services
    derive { entity_name = "services", display_name = title }
)
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");
        let transformer = ColumnPreservationTransformer;

        let transformed = transformer.transform_pl(module).expect("Should transform");

        // Convert to SQL
        let rq = prqlc::pl_to_rq(transformed).expect("Should convert to RQ");
        let sql = prqlc::rq_to_sql(rq, &Default::default()).expect("Should generate SQL");

        println!("Generated SQL (no explicit select):\n{}", sql);

        // The SQL should contain SELECT *
        assert!(sql.contains("*"), "Should contain wildcard SELECT *");
        assert!(sql.contains("UNION"), "Should contain UNION");
    }

    #[test]
    fn test_non_union_query_unchanged() {
        let query = r#"
from products
derive { entity_name = "products" }
select { id, name, entity_name }
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");
        let transformer = ColumnPreservationTransformer;

        let transformed = transformer.transform_pl(module).expect("Should transform");

        // Convert to SQL
        let rq = prqlc::pl_to_rq(transformed).expect("Should convert to RQ");
        let sql = prqlc::rq_to_sql(rq, &Default::default()).expect("Should generate SQL");

        println!("Generated SQL (non-union):\n{}", sql);

        // Non-union queries should not be modified (no wildcard)
        // They should have explicit column selection
        assert!(
            sql.contains("id") && sql.contains("name"),
            "Should have explicit columns"
        );
    }
}
