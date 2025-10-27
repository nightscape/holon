//! Shared AST builder utilities for PRQL transformers
//!
//! This module provides helper functions for searching and constructing PRQL AST nodes,
//! reducing duplication across transformers like ColumnPreservationTransformer and
//! JsonAggregationTransformer.

use std::collections::HashMap;

use prqlc::pr::{Expr, ExprKind, FuncCall, Ident};

/// Check if an expression contains an append (UNION) call
pub fn has_append_in_expr(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Pipeline(pipeline) => pipeline
            .exprs
            .iter()
            .any(|e| is_append_call(e) || has_append_in_expr(e)),
        ExprKind::FuncCall(func_call) => {
            if is_ident_named(&func_call.name, "append") {
                return true;
            }
            func_call.args.iter().any(has_append_in_expr)
        }
        ExprKind::Tuple(items) | ExprKind::Array(items) => items.iter().any(has_append_in_expr),
        _ => false,
    }
}

/// Check if an expression is an append function call
pub fn is_append_call(expr: &Expr) -> bool {
    if let ExprKind::FuncCall(func_call) = &expr.kind {
        return is_ident_named(&func_call.name, "append");
    }
    false
}

/// Check if an expression is an identifier with a specific name
pub fn is_ident_named(expr: &Expr, name: &str) -> bool {
    if let ExprKind::Ident(ident) = &expr.kind {
        return ident.path.is_empty() && ident.name == name;
    }
    false
}

/// Create an expression with default span, alias, and doc_comment
pub fn create_expr(kind: ExprKind) -> Expr {
    Expr {
        kind,
        span: None,
        alias: None,
        doc_comment: None,
    }
}

/// Create an expression with an alias
pub fn create_expr_with_alias(kind: ExprKind, alias: &str) -> Expr {
    Expr {
        kind,
        span: None,
        alias: Some(alias.to_string()),
        doc_comment: None,
    }
}

/// Create a simple identifier expression (no path)
pub fn create_ident(name: &str) -> Expr {
    create_expr(ExprKind::Ident(Ident {
        path: vec![],
        name: name.to_string(),
    }))
}

/// Create an identifier expression with a path (e.g., "this.*")
pub fn create_ident_path(path: Vec<String>) -> Expr {
    create_expr(ExprKind::Ident(Ident::from_path(path)))
}

/// Create a tuple expression
pub fn create_tuple(items: Vec<Expr>) -> Expr {
    create_expr(ExprKind::Tuple(items))
}

/// Create a function call expression
pub fn create_func_call(func_name: &str, args: Vec<Expr>) -> Expr {
    create_expr(ExprKind::FuncCall(FuncCall {
        name: Box::new(create_ident(func_name)),
        args,
        named_args: HashMap::new(),
    }))
}

/// Create a `select { ... }` expression
pub fn create_select(tuple_items: Vec<Expr>) -> Expr {
    create_func_call("select", vec![create_tuple(tuple_items)])
}

/// Create a `from table_name` expression
pub fn create_from(table_name: &str) -> Expr {
    create_func_call("from", vec![create_ident(table_name)])
}

/// Create an `append ( ... )` expression
pub fn create_append(arg: Expr) -> Expr {
    create_func_call("append", vec![arg])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_ident() {
        let expr = create_ident("foo");
        if let ExprKind::Ident(ident) = &expr.kind {
            assert_eq!(ident.name, "foo");
            assert!(ident.path.is_empty());
        } else {
            panic!("Expected Ident");
        }
    }

    #[test]
    fn test_create_ident_path() {
        let expr = create_ident_path(vec!["this".to_string(), "*".to_string()]);
        if let ExprKind::Ident(ident) = &expr.kind {
            assert_eq!(ident.path, vec!["this".to_string()]);
            assert_eq!(ident.name, "*");
        } else {
            panic!("Expected Ident");
        }
    }

    #[test]
    fn test_create_func_call() {
        let expr = create_func_call("select", vec![create_ident("id")]);
        if let ExprKind::FuncCall(func_call) = &expr.kind {
            assert!(is_ident_named(&func_call.name, "select"));
            assert_eq!(func_call.args.len(), 1);
        } else {
            panic!("Expected FuncCall");
        }
    }

    #[test]
    fn test_is_ident_named() {
        let expr = create_ident("append");
        assert!(is_ident_named(&expr, "append"));
        assert!(!is_ident_named(&expr, "select"));
    }
}
