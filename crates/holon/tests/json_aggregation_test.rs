//! Integration tests for JsonAggregationTransformer
//!
//! These tests verify:
//! 1. The transformer correctly detects UNION queries and creates CTEs
//! 2. Non-UNION queries are not affected
//! 3. Derived columns are preserved in the CTEs

use std::sync::Arc;

use holon::core::transform::{JsonAggregationTransformer, TransformPipeline};

#[test]
fn test_skips_non_union_queries() {
    let pipeline =
        TransformPipeline::empty().with_transformer(Arc::new(JsonAggregationTransformer));

    let result = pipeline.compile("from tasks | select {id, content}");
    assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

    let (sql, _rq) = result.unwrap();

    // Non-UNION queries should compile successfully without json_object
    let sql_lower = sql.to_lowercase();
    assert!(
        !sql_lower.contains("json_object"),
        "Non-UNION query should not contain json_object: {}",
        sql
    );
    assert!(
        !sql_lower.contains("with"),
        "Non-UNION query should not have CTEs: {}",
        sql
    );
}

#[test]
fn test_transforms_union_to_ctes_with_json_object() {
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

    // Derived entity_name should be in the CTE
    assert!(
        sql_lower.contains("'projects'"),
        "CTE should contain 'projects': {}",
        sql
    );
    assert!(
        sql_lower.contains("'tasks'"),
        "CTE should contain 'tasks': {}",
        sql
    );
}

#[test]
fn test_preserves_derived_columns_in_cte() {
    let pipeline =
        TransformPipeline::empty().with_transformer(Arc::new(JsonAggregationTransformer));

    let result = pipeline.compile(
        r#"
        from products
        derive {
            display_name = name,
            category = "product",
            price_formatted = s"printf('$%.2f', {price})"
        }
        select {id, display_name, category, price_formatted}
        append (
            from services
            derive {
                display_name = title,
                category = "service",
                price_formatted = s"printf('$%.2f', {hourly_rate})"
            }
            select {id, display_name, category, price_formatted}
        )
        "#,
    );
    assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

    let (sql, _rq) = result.unwrap();
    println!("Generated SQL with derived columns:\n{}", sql);

    // CTE should have the derived columns
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("category"),
        "CTE should contain derived category column: {}",
        sql
    );
    assert!(
        sql_lower.contains("'product'"),
        "CTE should contain 'product' literal: {}",
        sql
    );
    assert!(
        sql_lower.contains("'service'"),
        "CTE should contain 'service' literal: {}",
        sql
    );
}

#[test]
fn test_handles_multiple_appends() {
    let pipeline =
        TransformPipeline::empty().with_transformer(Arc::new(JsonAggregationTransformer));

    let result = pipeline.compile(
        r#"
        from directories
        derive { entity_name = "dir" }
        select {id, name, entity_name}
        append (
            from files
            derive { entity_name = "file" }
            select {id, name, entity_name}
        )
        append (
            from links
            derive { entity_name = "link" }
            select {id, name, entity_name}
        )
        "#,
    );
    assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

    let (sql, _rq) = result.unwrap();
    println!("Generated SQL with multiple appends:\n{}", sql);

    let sql_lower = sql.to_lowercase();

    // Should have 3 CTEs (one for each branch)
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
    assert!(
        sql_lower.contains("_branch_2"),
        "Should have _branch_2 CTE: {}",
        sql
    );

    // Should have 2 UNIONs
    let union_count = sql_lower.matches("union").count();
    assert_eq!(
        union_count, 2,
        "Should have 2 UNION clauses, got {}: {}",
        union_count, sql
    );
}

#[test]
fn test_works_with_filters() {
    let pipeline =
        TransformPipeline::empty().with_transformer(Arc::new(JsonAggregationTransformer));

    let result = pipeline.compile(
        r#"
        from active_users
        filter (status == "active")
        derive { user_type = "regular" }
        select {id, name, user_type}
        append (
            from admin_users
            filter (is_admin == true)
            derive { user_type = "admin" }
            select {id, name, user_type}
        )
        "#,
    );
    assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

    let (sql, _rq) = result.unwrap();
    println!("Generated SQL with filters:\n{}", sql);

    let sql_lower = sql.to_lowercase();

    // Should preserve filters in CTEs
    assert!(
        sql_lower.contains("where"),
        "CTEs should contain WHERE clauses: {}",
        sql
    );
    assert!(
        sql_lower.contains("'active'") || sql_lower.contains("\"active\""),
        "CTE should contain 'active' filter: {}",
        sql
    );
}
