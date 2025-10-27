//! SQL dependency extraction using sqlparser.
//!
//! This module provides functions to extract table references from SQL statements,
//! enabling automatic dependency inference for the operation scheduler.

use std::collections::HashSet;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use super::resource::Resource;

/// Extract table references from a SQL statement.
///
/// Returns a set of table names that the statement reads from (FROM, JOIN clauses)
/// or writes to (INSERT INTO, UPDATE, DELETE FROM).
pub fn extract_table_refs(sql: &str) -> Vec<Resource> {
    let dialect = SQLiteDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) => stmts,
        Err(_) => return Vec::new(),
    };

    let mut refs = Vec::new();
    for stmt in statements {
        extract_refs_from_statement(&stmt, &mut refs);
    }

    refs.sort_by(|a, b| a.name().cmp(b.name()));
    refs.dedup();
    refs
}

/// Extract tables created by a SQL statement.
///
/// Returns table/view names from CREATE TABLE, CREATE VIEW, CREATE INDEX statements.
pub fn extract_created_tables(sql: &str) -> Vec<Resource> {
    let dialect = SQLiteDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) => stmts,
        Err(_) => {
            return extract_created_tables_regex(sql);
        }
    };

    let mut created = Vec::new();
    for stmt in statements {
        match &stmt {
            Statement::CreateTable { name, .. } => {
                created.push(Resource::schema(normalize_table_name(&name.to_string())));
            }
            Statement::CreateView { name, .. } => {
                created.push(Resource::schema(normalize_table_name(&name.to_string())));
            }
            Statement::CreateIndex { name, .. } => {
                if let Some(idx_name) = name {
                    created.push(Resource::schema(normalize_table_name(
                        &idx_name.to_string(),
                    )));
                }
            }
            _ => {}
        }
    }

    created
}

/// Fallback regex-based extraction for CREATE statements.
fn extract_created_tables_regex(sql: &str) -> Vec<Resource> {
    let mut created = Vec::new();
    let sql_upper = sql.to_uppercase();

    if sql_upper.contains("CREATE") {
        let words: Vec<&str> = sql.split_whitespace().collect();

        for (i, word) in words.iter().enumerate() {
            let upper = word.to_uppercase();
            if upper == "VIEW" || upper == "TABLE" {
                let mut name_idx = i + 1;
                while name_idx < words.len() {
                    let next_upper = words[name_idx].to_uppercase();
                    if next_upper == "IF" || next_upper == "NOT" || next_upper == "EXISTS" {
                        name_idx += 1;
                    } else {
                        break;
                    }
                }
                if name_idx < words.len() {
                    let name = normalize_table_name(words[name_idx]);
                    if !name.is_empty() && name != "AS" && name != "(" {
                        created.push(Resource::schema(name));
                        break;
                    }
                }
            }
        }
    }

    created
}

/// Normalize a table name by removing quotes and schema prefixes.
fn normalize_table_name(name: &str) -> String {
    name.trim()
        .trim_matches('"')
        .trim_matches('\'')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .split('.')
        .last()
        .unwrap_or(name)
        .to_string()
}

/// Extract table references from a Statement.
fn extract_refs_from_statement(stmt: &Statement, refs: &mut Vec<Resource>) {
    match stmt {
        Statement::Query(query) => {
            extract_refs_from_query(query, refs);
        }
        Statement::Insert {
            table_name, source, ..
        } => {
            refs.push(Resource::schema(normalize_table_name(
                &table_name.to_string(),
            )));
            if let Some(src) = source {
                extract_refs_from_query(src, refs);
            }
        }
        Statement::Update { table, from, .. } => {
            extract_refs_from_table_with_joins(table, refs);
            if let Some(from_table) = from {
                extract_refs_from_table_with_joins(from_table, refs);
            }
        }
        Statement::Delete { tables, from, .. } => {
            for table in tables {
                refs.push(Resource::schema(normalize_table_name(&table.to_string())));
            }
            for table in from {
                extract_refs_from_table_with_joins(table, refs);
            }
        }
        Statement::CreateView { query, .. } => {
            extract_refs_from_query(query, refs);
        }
        Statement::CreateTable { query, .. } => {
            if let Some(q) = query {
                extract_refs_from_query(q, refs);
            }
        }
        _ => {}
    }
}

/// Extract table references from a Query (SELECT statement).
///
/// CTEs (Common Table Expressions) defined in the WITH clause are NOT included
/// as external dependencies since they're defined within the same statement.
fn extract_refs_from_query(query: &Query, refs: &mut Vec<Resource>) {
    // Collect CTE names first - these are NOT external dependencies
    let mut cte_names: HashSet<String> = HashSet::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            cte_names.insert(cte.alias.name.value.to_lowercase());
        }
    }

    // Extract refs from CTE definitions (to find what external tables they reference)
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_refs_from_query_with_ctes(&cte.query, refs, &cte_names);
        }
    }

    // Extract refs from main query body, excluding CTE names
    extract_refs_from_set_expr_with_ctes(&query.body, refs, &cte_names);
}

/// Extract table references from a Query, excluding CTE-defined names.
fn extract_refs_from_query_with_ctes(
    query: &Query,
    refs: &mut Vec<Resource>,
    cte_names: &HashSet<String>,
) {
    // Collect additional CTE names from nested WITH clauses
    let mut all_cte_names = cte_names.clone();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            all_cte_names.insert(cte.alias.name.value.to_lowercase());
        }
    }

    // Extract refs from nested CTE definitions
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_refs_from_query_with_ctes(&cte.query, refs, &all_cte_names);
        }
    }

    // Extract refs from main query body
    extract_refs_from_set_expr_with_ctes(&query.body, refs, &all_cte_names);
}

/// Extract table references from a SetExpr.
fn extract_refs_from_set_expr(set_expr: &SetExpr, refs: &mut Vec<Resource>) {
    extract_refs_from_set_expr_with_ctes(set_expr, refs, &HashSet::new());
}

/// Extract table references from a SetExpr, excluding CTE-defined names.
fn extract_refs_from_set_expr_with_ctes(
    set_expr: &SetExpr,
    refs: &mut Vec<Resource>,
    cte_names: &HashSet<String>,
) {
    match set_expr {
        SetExpr::Select(select) => {
            extract_refs_from_select_with_ctes(select, refs, cte_names);
        }
        SetExpr::Query(query) => {
            extract_refs_from_query_with_ctes(query, refs, cte_names);
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_refs_from_set_expr_with_ctes(left, refs, cte_names);
            extract_refs_from_set_expr_with_ctes(right, refs, cte_names);
        }
        SetExpr::Values(_) => {}
        SetExpr::Insert(stmt) => {
            extract_refs_from_statement(stmt, refs);
        }
        SetExpr::Update(stmt) => {
            extract_refs_from_statement(stmt, refs);
        }
        SetExpr::Table(_) => {}
    }
}

/// Extract table references from a SELECT clause.
fn extract_refs_from_select(select: &Select, refs: &mut Vec<Resource>) {
    extract_refs_from_select_with_ctes(select, refs, &HashSet::new());
}

/// Extract table references from a SELECT clause, excluding CTE-defined names.
fn extract_refs_from_select_with_ctes(
    select: &Select,
    refs: &mut Vec<Resource>,
    cte_names: &HashSet<String>,
) {
    for table in &select.from {
        extract_refs_from_table_with_joins_with_ctes(table, refs, cte_names);
    }

    for item in &select.projection {
        if let SelectItem::ExprWithAlias { expr, .. } | SelectItem::UnnamedExpr(expr) = item {
            extract_refs_from_expr(expr, refs);
        }
    }

    if let Some(selection) = &select.selection {
        extract_refs_from_expr(selection, refs);
    }

    if let Some(having) = &select.having {
        extract_refs_from_expr(having, refs);
    }
}

/// Extract table references from a TableWithJoins.
fn extract_refs_from_table_with_joins(table: &TableWithJoins, refs: &mut Vec<Resource>) {
    extract_refs_from_table_with_joins_with_ctes(table, refs, &HashSet::new());
}

/// Extract table references from a TableWithJoins, excluding CTE-defined names.
fn extract_refs_from_table_with_joins_with_ctes(
    table: &TableWithJoins,
    refs: &mut Vec<Resource>,
    cte_names: &HashSet<String>,
) {
    extract_refs_from_table_factor_with_ctes(&table.relation, refs, cte_names);

    for join in &table.joins {
        extract_refs_from_table_factor_with_ctes(&join.relation, refs, cte_names);
    }
}

/// Extract table references from a TableFactor.
fn extract_refs_from_table_factor(factor: &TableFactor, refs: &mut Vec<Resource>) {
    extract_refs_from_table_factor_with_ctes(factor, refs, &HashSet::new());
}

/// Extract table references from a TableFactor, excluding CTE-defined names.
fn extract_refs_from_table_factor_with_ctes(
    factor: &TableFactor,
    refs: &mut Vec<Resource>,
    cte_names: &HashSet<String>,
) {
    match factor {
        TableFactor::Table { name, .. } => {
            let table_name = normalize_table_name(&name.to_string());
            // Skip if this is a CTE name (defined in the same query's WITH clause)
            if !cte_names.contains(&table_name.to_lowercase()) {
                refs.push(Resource::schema(table_name));
            }
        }
        TableFactor::Derived { subquery, .. } => {
            extract_refs_from_query_with_ctes(subquery, refs, cte_names);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            extract_refs_from_table_with_joins_with_ctes(table_with_joins, refs, cte_names);
        }
        _ => {}
    }
}

/// Extract table references from an expression (handles subqueries).
fn extract_refs_from_expr(expr: &Expr, refs: &mut Vec<Resource>) {
    match expr {
        Expr::Subquery(query) => {
            extract_refs_from_query(query, refs);
        }
        Expr::InSubquery { subquery, expr, .. } => {
            extract_refs_from_expr(expr, refs);
            extract_refs_from_query(subquery, refs);
        }
        Expr::Exists { subquery, .. } => {
            extract_refs_from_query(subquery, refs);
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_refs_from_expr(left, refs);
            extract_refs_from_expr(right, refs);
        }
        Expr::UnaryOp { expr, .. } => {
            extract_refs_from_expr(expr, refs);
        }
        Expr::Nested(inner) => {
            extract_refs_from_expr(inner, refs);
        }
        Expr::Function(func) => {
            for arg in &func.args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                    extract_refs_from_expr(e, refs);
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                extract_refs_from_expr(op, refs);
            }
            for cond in conditions {
                extract_refs_from_expr(cond, refs);
            }
            for result in results {
                extract_refs_from_expr(result, refs);
            }
            if let Some(else_res) = else_result {
                extract_refs_from_expr(else_res, refs);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_simple_select() {
        let refs = extract_table_refs("SELECT * FROM users");
        assert_eq!(refs, vec![Resource::schema("users")]);
    }

    #[test]
    fn test_extract_join() {
        let refs = extract_table_refs(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id LEFT JOIN items i ON o.id = i.order_id",
        );
        assert_eq!(
            refs,
            vec![
                Resource::schema("items"),
                Resource::schema("orders"),
                Resource::schema("users"),
            ]
        );
    }

    #[test]
    fn test_extract_subquery() {
        let refs = extract_table_refs(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)",
        );
        assert!(refs.contains(&Resource::schema("users")));
        assert!(refs.contains(&Resource::schema("active_users")));
    }

    #[test]
    fn test_extract_insert() {
        let refs = extract_table_refs("INSERT INTO users (name) VALUES ('test')");
        assert_eq!(refs, vec![Resource::schema("users")]);
    }

    #[test]
    fn test_extract_insert_select() {
        let refs =
            extract_table_refs("INSERT INTO users_backup SELECT * FROM users WHERE active = 1");
        assert!(refs.contains(&Resource::schema("users_backup")));
        assert!(refs.contains(&Resource::schema("users")));
    }

    #[test]
    fn test_extract_update() {
        let refs = extract_table_refs("UPDATE users SET name = 'test' WHERE id = 1");
        assert_eq!(refs, vec![Resource::schema("users")]);
    }

    #[test]
    fn test_extract_delete() {
        let refs = extract_table_refs("DELETE FROM users WHERE id = 1");
        assert_eq!(refs, vec![Resource::schema("users")]);
    }

    #[test]
    fn test_extract_create_table() {
        let created = extract_created_tables("CREATE TABLE users (id INTEGER PRIMARY KEY)");
        assert_eq!(created, vec![Resource::schema("users")]);
    }

    #[test]
    fn test_extract_create_table_if_not_exists() {
        let created =
            extract_created_tables("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY)");
        assert_eq!(created, vec![Resource::schema("users")]);
    }

    #[test]
    fn test_extract_create_view() {
        let created = extract_created_tables(
            "CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1",
        );
        assert_eq!(created, vec![Resource::schema("active_users")]);
    }

    #[test]
    fn test_extract_create_materialized_view_regex() {
        let created = extract_created_tables(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS watch_view_abc AS SELECT * FROM blocks",
        );
        assert_eq!(created, vec![Resource::schema("watch_view_abc")]);
    }

    #[test]
    fn test_view_definition_refs() {
        let refs =
            extract_table_refs("CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1");
        assert_eq!(refs, vec![Resource::schema("users")]);
    }

    #[test]
    fn test_normalize_quoted_names() {
        let refs = extract_table_refs(r#"SELECT * FROM "my table""#);
        assert_eq!(refs, vec![Resource::schema("my table")]);
    }

    #[test]
    fn test_extract_empty() {
        let refs = extract_table_refs("");
        assert!(refs.is_empty());

        let created = extract_created_tables("");
        assert!(created.is_empty());
    }

    #[test]
    fn test_extract_invalid_sql() {
        let refs = extract_table_refs("NOT VALID SQL AT ALL");
        assert!(refs.is_empty());
    }

    #[test]
    fn test_extract_cte() {
        // CTE names should NOT be included as external dependencies
        let refs = extract_table_refs(
            "WITH active AS (SELECT * FROM users WHERE active = 1) SELECT * FROM active JOIN orders ON active.id = orders.user_id",
        );
        assert!(refs.contains(&Resource::schema("users")));
        assert!(refs.contains(&Resource::schema("orders")));
        // CTE name 'active' should NOT be included - it's defined in the same query
        assert!(
            !refs.contains(&Resource::schema("active")),
            "CTE name should not be included as external dependency"
        );
    }

    #[test]
    fn test_extract_nested_cte() {
        // Multiple CTEs in the same WITH clause should all be excluded
        let refs = extract_table_refs(
            "WITH
                descendants AS (SELECT * FROM blocks WHERE parent_id = 'root'),
                grandchildren AS (SELECT * FROM descendants WHERE level > 1)
            SELECT * FROM grandchildren JOIN documents ON grandchildren.doc_id = documents.id",
        );
        // Real tables should be included
        assert!(refs.contains(&Resource::schema("blocks")));
        assert!(refs.contains(&Resource::schema("documents")));
        // CTE names should NOT be included
        assert!(
            !refs.contains(&Resource::schema("descendants")),
            "CTE name 'descendants' should not be included"
        );
        assert!(
            !refs.contains(&Resource::schema("grandchildren")),
            "CTE name 'grandchildren' should not be included"
        );
    }

    #[test]
    fn test_extract_union() {
        let refs = extract_table_refs("SELECT * FROM users UNION SELECT * FROM admins");
        assert!(refs.contains(&Resource::schema("users")));
        assert!(refs.contains(&Resource::schema("admins")));
    }
}
