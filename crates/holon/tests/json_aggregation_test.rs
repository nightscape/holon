//! Integration tests for JsonAggregationTransformer and manual json_object approach
//!
//! These tests verify:
//! 1. The transformer correctly detects UNION queries
//! 2. Manual json_object() via s-strings in PRQL works correctly (production approach)
//! 3. Non-UNION queries are not affected

use std::sync::Arc;

use holon::core::transform::{JsonAggregationTransformer, TransformPipeline};

#[test]
fn test_skips_non_union_queries() {
    let pipeline =
        TransformPipeline::empty().with_transformer(Arc::new(JsonAggregationTransformer));

    let result = pipeline.compile("from tasks | select {id, content}");
    assert!(result.is_ok(), "Compilation failed: {:?}", result.err());

    let (sql, _rq) = result.unwrap();

    // Non-UNION queries should compile successfully
    let sql_lower = sql.to_lowercase();
    assert!(
        !sql_lower.contains("json_object"),
        "Non-UNION query should not contain json_object: {}",
        sql
    );
}

#[test]
fn test_manual_json_object_using_query_render() {
    // This test uses query_render::parse_query_render_to_rq like production code
    // to verify that json_object() via s-strings works correctly
    // Note: PRQL doesn't support comma-separated function args, so we use s-strings
    let prql = r#"
        from projects
        derive { data = s"json_object('id', {id}, 'name', {name})" }
        select {id, name, data}
        append (
            from tasks
            derive { data = s"json_object('id', {id}, 'name', {content})" }
            select {id, name = content, data}
        )
        render (tree parent_id:parent_id sortkey:id item_template:this.data)
    "#;

    let result = query_render::parse_query_render_to_rq(prql);
    assert!(
        result.is_ok(),
        "Query render parsing failed: {:?}",
        result.err()
    );

    let parsed = result.unwrap();
    let sql_result = parsed.to_sql();
    assert!(
        sql_result.is_ok(),
        "SQL generation failed: {:?}",
        sql_result.err()
    );

    let sql = sql_result.unwrap();
    let sql_lower = sql.to_lowercase();

    assert!(
        sql_lower.contains("json_object"),
        "Should produce json_object in SQL: {}",
        sql
    );
    assert!(
        sql_lower.contains("union"),
        "Should produce UNION SQL: {}",
        sql
    );

    println!("json_object SQL via query_render:\n{}", sql);
}

#[test]
fn test_production_style_query_via_query_render() {
    // This test mimics the actual production query structure using query_render
    let prql = r#"
        from directories
        derive {
            content = name,
            entity_name = "directories",
            sort_key = name
        }
        derive { data = s"json_object('id', {id}, 'parent_id', {parent_id}, 'content', {content}, 'entity_name', {entity_name}, 'sort_key', {sort_key})" }
        select { id, parent_id, entity_name, sort_key, data }
        append (
            from files
            derive {
                content = title,
                entity_name = "files",
                sort_key = name
            }
            derive { data = s"json_object('id', {id}, 'parent_id', {parent_id}, 'content', {content}, 'entity_name', {entity_name}, 'sort_key', {sort_key})" }
            select { id, parent_id, entity_name, sort_key, data }
        )
        append (
            from headlines
            derive {
                content = title,
                entity_name = "headlines",
                sort_key = byte_start
            }
            derive { data = s"json_object('id', {id}, 'parent_id', {parent_id}, 'content', {content}, 'entity_name', {entity_name}, 'sort_key', {sort_key})" }
            select { id, parent_id, entity_name, sort_key, data }
        )
        render (tree parent_id:parent_id sortkey:sort_key item_template:this.data)
    "#;

    let result = query_render::parse_query_render_to_rq(prql);
    assert!(
        result.is_ok(),
        "Production-style query parsing failed: {:?}",
        result.err()
    );

    let parsed = result.unwrap();
    let sql_result = parsed.to_sql();
    assert!(
        sql_result.is_ok(),
        "Production-style SQL generation failed: {:?}",
        sql_result.err()
    );

    let sql = sql_result.unwrap();
    let sql_lower = sql.to_lowercase();

    // Verify structure
    assert!(
        sql_lower.contains("union"),
        "Should produce UNION SQL: {}",
        sql
    );
    assert!(
        sql_lower.contains("json_object"),
        "Should contain json_object: {}",
        sql
    );

    println!("Production-style SQL:\n{}", sql);
}
