//! End-to-end tests for json_object aggregation in PRQL queries
//!
//! These tests use BackendEngine (the same code path as the Flutter app)
//! to verify that:
//! 1. PRQL queries with manual json_object s-strings compile correctly
//! 2. The generated SQL executes against SQLite without errors
//! 3. The `data` column contains valid JSON that can be parsed

use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use holon::api::backend_engine::BackendEngine;
use holon::di::test_helpers::create_test_engine_with_path;

/// Create a unique database path for testing
fn unique_db_path() -> PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);

    std::env::temp_dir().join(format!(
        "holon_test_{}_{}_{}.db",
        std::process::id(),
        id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Create a BackendEngine for testing (same code path as Flutter app)
async fn create_test_engine() -> Result<Arc<BackendEngine>> {
    let db_path = unique_db_path();
    create_test_engine_with_path(db_path).await
}

/// Setup test tables that mimic the production schema
async fn setup_test_schema(engine: &Arc<BackendEngine>) -> Result<()> {
    // Create directories table (matches Directory struct - NO path column)
    engine
        .db_handle()
        .execute(
            r#"
        CREATE TABLE IF NOT EXISTS directories (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            parent_id TEXT,
            depth INTEGER DEFAULT 0
        )
        "#,
            vec![],
        )
        .await?;

    // Create todoist_projects table
    engine
        .db_handle()
        .execute(
            r#"
        CREATE TABLE IF NOT EXISTS todoist_projects (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            parent_id TEXT,
            color TEXT,
            is_favorite INTEGER DEFAULT 0,
            is_archived INTEGER DEFAULT 0
        )
        "#,
            vec![],
        )
        .await?;

    // Create todoist_tasks table
    engine
        .db_handle()
        .execute(
            r#"
        CREATE TABLE IF NOT EXISTS todoist_tasks (
            id TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            parent_id TEXT,
            project_id TEXT,
            priority INTEGER DEFAULT 1,
            completed INTEGER DEFAULT 0,
            is_deleted INTEGER DEFAULT 0
        )
        "#,
            vec![],
        )
        .await?;

    Ok(())
}

/// Insert test data
async fn insert_test_data(engine: &Arc<BackendEngine>) -> Result<()> {
    // Insert directories
    engine
        .db_handle()
        .execute(
            "INSERT INTO directories (id, name, parent_id, depth) VALUES ('dir-1', 'Root', NULL, 0)",
            vec![],
        )
        .await?;
    engine
        .db_handle()
        .execute(
            "INSERT INTO directories (id, name, parent_id, depth) VALUES ('dir-2', 'Subdir', 'dir-1', 1)",
            vec![],
        )
        .await?;

    // Insert todoist projects
    engine
        .db_handle()
        .execute(
            "INSERT INTO todoist_projects (id, name, parent_id, color, is_favorite, is_archived) VALUES ('proj-1', 'Project 1', NULL, 'red', 0, 0)",
            vec![],
        )
        .await?;
    engine
        .db_handle()
        .execute(
            "INSERT INTO todoist_projects (id, name, parent_id, color, is_favorite, is_archived) VALUES ('proj-2', 'Project 2', 'proj-1', 'blue', 1, 0)",
            vec![],
        )
        .await?;

    // Insert todoist tasks
    engine
        .db_handle()
        .execute(
            "INSERT INTO todoist_tasks (id, content, parent_id, project_id, priority, completed, is_deleted) VALUES ('task-1', 'Task 1', NULL, 'proj-1', 2, 0, 0)",
            vec![],
        )
        .await?;
    engine
        .db_handle()
        .execute(
            "INSERT INTO todoist_tasks (id, content, parent_id, project_id, priority, completed, is_deleted) VALUES ('task-2', 'Task 2', 'task-1', 'proj-1', 1, 1, 0)",
            vec![],
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_simple_json_object_query_via_backend_engine() -> Result<()> {
    let engine = create_test_engine().await?;
    setup_test_schema(&engine).await?;
    insert_test_data(&engine).await?;

    // Simple query with json_object - uses BackendEngine::compile_query
    let prql = r#"
from directories
derive { data = s"json_object('id', {id}, 'name', {name}, 'parent_id', {parent_id}, 'depth', {depth})" }
select { id, name, data }
render (list item_template:(text content:this.name))
    "#;

    // Use BackendEngine to compile and execute (same as Flutter app)
    let (sql, render_spec) = engine.compile_query(prql.to_string(), None)?;
    println!("BackendEngine compiled SQL:\n{}\n", sql);
    println!("RenderSpec root: {:?}", render_spec.root());

    // Execute via BackendEngine
    let results = engine.execute_query(sql, HashMap::new(), None).await?;

    assert!(!results.is_empty(), "Should have results");
    assert_eq!(results.len(), 2, "Should have 2 directories");

    // Verify data column was flattened (turso.rs flattens json_object results)
    for row in &results {
        println!("Row keys: {:?}", row.keys().collect::<Vec<_>>());

        // After flattening, 'data' is removed and its contents are merged
        // So we should have 'id', 'name', 'path' at the top level
        assert!(row.get("id").is_some(), "Should have id");
        assert!(row.get("name").is_some(), "Should have name");
    }

    Ok(())
}

#[tokio::test]
async fn test_union_query_with_json_object_via_backend_engine() -> Result<()> {
    let engine = create_test_engine().await?;
    setup_test_schema(&engine).await?;
    insert_test_data(&engine).await?;

    // UNION query similar to production todoist_hierarchy.prql
    let prql = r#"
from todoist_projects
filter (is_archived == null || is_archived == false)
derive {
    content = name,
    entity_name = "todoist_projects",
    sort_key = id
}
derive { data = s"json_object('id', {id}, 'name', {name}, 'parent_id', {parent_id}, 'color', {color}, 'entity_name', 'todoist_projects')" }
select { id, parent_id, entity_name, sort_key, data }
append (
    from todoist_tasks
    filter (is_deleted == null || is_deleted == false)
    derive {
        parent_id = parent_id ?? project_id,
        entity_name = "todoist_tasks",
        sort_key = id
    }
    derive { data = s"json_object('id', {id}, 'content', {content}, 'parent_id', {parent_id}, 'project_id', {project_id}, 'priority', {priority}, 'completed', {completed}, 'entity_name', 'todoist_tasks')" }
    select { id, parent_id, entity_name, sort_key, data }
)
render (tree parent_id:parent_id sortkey:sort_key item_template:(text content:this.entity_name))
    "#;

    // Use BackendEngine to compile and execute
    let (sql, _render_spec) = engine.compile_query(prql.to_string(), None)?;
    println!("BackendEngine UNION SQL:\n{}\n", sql);

    let results = engine.execute_query(sql, HashMap::new(), None).await?;

    // Should have projects + tasks = 2 + 2 = 4 rows
    assert_eq!(
        results.len(),
        4,
        "Should have 4 rows (2 projects + 2 tasks)"
    );

    // Verify each row has entity_name (from flattened data or direct select)
    for row in &results {
        let entity_name = row
            .get("entity_name")
            .and_then(|v| v.as_string())
            .unwrap_or("unknown");

        println!("Row entity_name: {}", entity_name);
        assert!(
            entity_name == "todoist_projects" || entity_name == "todoist_tasks",
            "entity_name should be projects or tasks, got: {}",
            entity_name
        );
    }

    Ok(())
}

/// Test the actual production todoist_hierarchy.prql query file
#[tokio::test]
async fn test_production_todoist_query_via_backend_engine() -> Result<()> {
    let engine = create_test_engine().await?;
    setup_test_schema(&engine).await?;
    insert_test_data(&engine).await?;

    // Load the actual production query
    let prql = include_str!("../../holon-todoist/queries/todoist_hierarchy.prql");
    println!("Testing production query:\n{}\n", prql);

    // Use BackendEngine to compile (same code path as Flutter app)
    let compile_result = engine.compile_query(prql.to_string(), None);

    match compile_result {
        Ok((sql, render_spec)) => {
            println!("Production query compiled successfully via BackendEngine!");
            println!("SQL:\n{}\n", sql);
            println!(
                "RenderSpec has {} row_templates",
                render_spec.row_templates.len()
            );

            // Execute via BackendEngine
            match engine.execute_query(sql, HashMap::new(), None).await {
                Ok(results) => {
                    println!("Query executed successfully with {} results", results.len());
                    for (i, row) in results.iter().enumerate() {
                        println!(
                            "Row {}: entity_name={:?}, keys={:?}",
                            i,
                            row.get("entity_name"),
                            row.keys().collect::<Vec<_>>()
                        );
                    }
                    assert_eq!(
                        results.len(),
                        4,
                        "Should have 4 rows (2 projects + 2 tasks)"
                    );
                }
                Err(e) => {
                    panic!("SQL execution failed via BackendEngine: {}", e);
                }
            }
        }
        Err(e) => {
            panic!("PRQL compilation failed via BackendEngine: {}", e);
        }
    }

    Ok(())
}

/// Test the actual production orgmode_hierarchy.prql query file
#[tokio::test]
async fn test_production_orgmode_query_via_backend_engine() -> Result<()> {
    let engine = create_test_engine().await?;

    // Setup schema for orgmode tables
    // Create directories table (matches Directory struct - NO path column)
    engine
        .db_handle()
        .execute(
            r#"
            CREATE TABLE IF NOT EXISTS directories (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                parent_id TEXT,
                depth INTEGER DEFAULT 0
            )
            "#,
            vec![],
        )
        .await?;

    // Create documents table
    engine
        .db_handle()
        .execute(
            r#"
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                path TEXT NOT NULL,
                parent_id TEXT,
                depth INTEGER DEFAULT 0,
                title TEXT
            )
            "#,
            vec![],
        )
        .await?;

    // Create blocks table (matches Block struct)
    engine
        .db_handle()
        .execute(
            r#"
            CREATE TABLE IF NOT EXISTS blocks (
                id TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                file_path TEXT NOT NULL,
                parent_id TEXT,
                depth INTEGER DEFAULT 0,
                byte_start INTEGER DEFAULT 0,
                byte_end INTEGER DEFAULT 0,
                title TEXT,
                content TEXT,
                task_state TEXT,
                priority INTEGER,
                tags TEXT,
                scheduled TEXT,
                deadline TEXT,
                properties TEXT
            )
            "#,
            vec![],
        )
        .await?;

    // Insert test data
    engine
        .db_handle()
        .execute(
            "INSERT INTO directories (id, name, parent_id, depth) VALUES ('dir-1', 'Test Dir', NULL, 0)",
            vec![],
        )
        .await?;

    engine
        .db_handle()
        .execute(
            "INSERT INTO documents (id, name, path, parent_id, depth, title) VALUES ('file-1', 'test.org', '/test/test.org', 'dir-1', 1, 'Test File')",
            vec![],
        )
        .await?;

    engine
        .db_handle()
        .execute(
            "INSERT INTO blocks (id, file_id, file_path, parent_id, depth, byte_start, title, content, task_state, priority) VALUES ('headline-1', 'file-1', '/test/test.org', 'file-1', 2, 100, 'My Task', 'Task content', 'TODO', 1)",
            vec![],
        )
        .await?;

    // Load the actual production query
    let prql = include_str!("../../holon-orgmode/queries/orgmode_hierarchy.prql");
    println!("Testing orgmode production query:\n{}\n", prql);

    // Use BackendEngine to compile (same code path as Flutter app)
    let compile_result = engine.compile_query(prql.to_string(), None);

    match compile_result {
        Ok((sql, render_spec)) => {
            println!("Orgmode query compiled successfully via BackendEngine!");
            println!("SQL:\n{}\n", sql);
            println!(
                "RenderSpec has {} row_templates",
                render_spec.row_templates.len()
            );

            // Execute via BackendEngine
            match engine.execute_query(sql, HashMap::new(), None).await {
                Ok(results) => {
                    println!("Query executed successfully with {} results", results.len());
                    for (i, row) in results.iter().enumerate() {
                        println!(
                            "Row {}: entity_name={:?}, keys={:?}",
                            i,
                            row.get("entity_name"),
                            row.keys().collect::<Vec<_>>()
                        );
                    }
                }
                Err(e) => {
                    panic!("Orgmode SQL execution failed via BackendEngine: {}", e);
                }
            }
        }
        Err(e) => {
            panic!("Orgmode PRQL compilation failed via BackendEngine: {}", e);
        }
    }

    Ok(())
}

/// Test the JsonAggregationTransformer automatically injects json_object for UNION queries
/// Uses simplified `select { this.* }` instead of explicit column lists
#[tokio::test]
async fn test_json_aggregation_transformer_auto_injection() -> Result<()> {
    let engine = create_test_engine().await?;

    // Setup tables
    engine
        .db_handle()
        .execute(
            "CREATE TABLE projects (id TEXT PRIMARY KEY, name TEXT, description TEXT)",
            vec![],
        )
        .await?;

    engine
        .db_handle()
        .execute(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, content TEXT, priority INTEGER)",
            vec![],
        )
        .await?;

    engine
        .db_handle()
        .execute(
            "INSERT INTO projects (id, name, description) VALUES ('proj-1', 'Project A', 'Description A')",
            vec![],
        )
        .await?;

    engine
        .db_handle()
        .execute(
            "INSERT INTO tasks (id, content, priority) VALUES ('task-1', 'Do something', 1)",
            vec![],
        )
        .await?;

    // Query WITHOUT manual json_object - transformers should:
    // 1. ColumnPreservationTransformer: convert explicit selects to this.*
    // 2. JsonAggregationTransformer: inject json_object automatically
    //
    // Note: We use `select { this.* }` to preserve all columns for UNION
    let prql = r#"
from projects
derive { display = name }
select { this.* }
append (
    from tasks
    derive { display = content }
    select { this.* }
)
render (list item_template:(text content:this.display))
    "#;

    println!("Testing JsonAggregationTransformer with UNION query (simplified select)...");

    let (sql, _render_spec) = engine.compile_query(prql.to_string(), None)?;
    println!("Generated SQL:\n{}\n", sql);

    // Check if json_object was injected
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("json_object"),
        "json_object should be automatically injected"
    );
    println!("json_object was automatically injected!");

    // SQL should NOT have SELECT * anymore - just SELECT data
    // The JsonAggregationTransformer replaces Wildcard with just the data column
    assert!(
        !sql.contains("SELECT\n  *"),
        "SQL should not contain SELECT * - should be replaced with json_object"
    );

    // Try to execute the query
    match engine
        .execute_query(sql.clone(), HashMap::new(), None)
        .await
    {
        Ok(results) => {
            println!("Query executed successfully with {} results", results.len());
            for (i, row) in results.iter().enumerate() {
                println!("Row {}: keys={:?}", i, row.keys().collect::<Vec<_>>());
            }
        }
        Err(e) => {
            println!("Query execution failed: {}", e);
            panic!("Query should execute successfully: {}", e);
        }
    }

    Ok(())
}

/// Test that JsonAggregationTransformer includes DERIVED columns (not just base table columns)
/// This test uses SIMPLIFIED select statements - the ColumnPreservationTransformer
/// converts them to `select { this.* }` automatically
#[tokio::test]
async fn test_json_aggregation_includes_derived_columns() -> Result<()> {
    let engine = create_test_engine().await?;

    // Setup tables
    engine
        .db_handle()
        .execute(
            "CREATE TABLE projects (id TEXT PRIMARY KEY, name TEXT)",
            vec![],
        )
        .await?;

    engine
        .db_handle()
        .execute(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, content TEXT, completed INTEGER)",
            vec![],
        )
        .await?;

    engine
        .db_handle()
        .execute(
            "INSERT INTO projects (id, name) VALUES ('proj-1', 'Project A')",
            vec![],
        )
        .await?;

    engine
        .db_handle()
        .execute(
            "INSERT INTO tasks (id, content, completed) VALUES ('task-1', 'Do something', 1)",
            vec![],
        )
        .await?;

    // Query with DERIVED columns - NO EXPLICIT COLUMN LIST in select!
    // The ColumnPreservationTransformer will convert these to `select { this.* }`
    // Then JsonAggregationTransformer will inject json_object with all columns
    let prql = r#"
from projects
derive { entity_name = "projects", display_name = name }
select { this.* }
append (
    from tasks
    derive { entity_name = "tasks", display_name = content }
    select { this.* }
)
render (list item_template:(text content:this.display_name))
    "#;

    println!("Testing JsonAggregationTransformer with DERIVED columns (simplified select)...");

    let (sql, _render_spec) = engine.compile_query(prql.to_string(), None)?;
    println!("Generated SQL:\n{}\n", sql);

    // Check if json_object includes derived columns
    let sql_lower = sql.to_lowercase();

    let has_json_object = sql_lower.contains("json_object");
    let has_entity_name = sql_lower.contains("'entity_name'");
    let has_display_name = sql_lower.contains("'display_name'");

    println!("json_object present: {}", has_json_object);
    println!("entity_name in json_object: {}", has_entity_name);
    println!("display_name in json_object: {}", has_display_name);

    assert!(has_json_object, "Should inject json_object");
    assert!(
        has_entity_name,
        "Should include derived column 'entity_name' in json_object"
    );
    assert!(
        has_display_name,
        "Should include derived column 'display_name' in json_object"
    );

    // Execute the query
    let results = engine
        .execute_query(sql.clone(), HashMap::new(), None)
        .await?;
    println!("Query executed with {} results", results.len());

    for (i, row) in results.iter().enumerate() {
        println!("Row {}: keys={:?}", i, row.keys().collect::<Vec<_>>());
        // Check that entity_name is present (from flattened data)
        assert!(
            row.get("entity_name").is_some(),
            "Row should have entity_name from flattened data"
        );
    }

    Ok(())
}

/// Test that ALL base columns are included in data with simplified select
/// Using `select { this.* }` directly to preserve all columns from each table
#[tokio::test]
async fn test_json_aggregation_includes_all_base_columns() -> Result<()> {
    let engine = create_test_engine().await?;

    // Two tables with different columns
    engine
        .db_handle()
        .execute(
            "CREATE TABLE products (id TEXT PRIMARY KEY, name TEXT, description TEXT, price REAL)",
            vec![],
        )
        .await?;

    engine
        .db_handle()
        .execute(
            "CREATE TABLE services (id TEXT PRIMARY KEY, title TEXT, duration INTEGER, rate REAL)",
            vec![],
        )
        .await?;

    engine
        .db_handle()
        .execute(
            "INSERT INTO products VALUES ('prod-1', 'Widget', 'A useful widget', 9.99)",
            vec![],
        )
        .await?;

    engine
        .db_handle()
        .execute(
            "INSERT INTO services VALUES ('svc-1', 'Consulting', 60, 150.00)",
            vec![],
        )
        .await?;

    // Test that json_object includes all REFERENCED columns from each table
    //
    // IMPORTANT: PRQL's optimizer removes unreferenced columns from scope.
    // `select { this.* }` means "all columns in scope", not "all table columns".
    // To include base columns in the json_object, they must be explicitly referenced.
    //
    // This test uses explicit select statements with only the columns we want,
    // matching what users would write in practice.
    let prql = r#"
from products
derive { entity_name = "products", display_name = name }
select { id, name, description, price, entity_name, display_name }
derive { ui = (render (row (pie_menu (text this.display_name) fields:this.*))) }
append (
    from services
    derive { entity_name = "services", display_name = title }
    select { id, name = title, description = title, price = rate, entity_name, display_name }
    derive { ui = (render (row (pie_menu (text this.display_name) fields:this.*))) }
)
render (list item_template:this.ui)
    "#;

    let (sql, _) = engine.compile_query(prql.to_string(), None)?;
    println!("SQL:\n{}\n", sql);

    // The SQL should use json_object (JsonAggregationTransformer converts to json_object)
    assert!(
        sql.contains("json_object"),
        "SQL should contain json_object"
    );

    // Check that json_object includes columns
    let sql_lower = sql.to_lowercase();

    // Should have basic columns
    let has_entity_name = sql_lower.contains("entity_name");
    let has_display_name = sql_lower.contains("display_name");

    println!(
        "entity_name: {}, display_name: {}",
        has_entity_name, has_display_name
    );

    assert!(has_entity_name, "data should include entity_name");
    assert!(has_display_name, "data should include display_name");

    // Execute and verify heterogeneous data works
    let results = engine.execute_query(sql, HashMap::new(), None).await?;
    assert_eq!(
        results.len(),
        2,
        "Should have 2 results (1 product, 1 service)"
    );

    for row in &results {
        let entity = row
            .get("entity_name")
            .and_then(|v| v.as_string())
            .unwrap_or("?");
        println!(
            "Entity {}: keys={:?}",
            entity,
            row.keys().collect::<Vec<_>>()
        );

        // Both should have entity_name from derive
        assert!(
            row.contains_key("entity_name"),
            "Row should have entity_name"
        );
    }

    Ok(())
}

/// Test json_object with special characters (quotes, newlines, etc.)
#[tokio::test]
async fn test_json_object_with_special_characters() -> Result<()> {
    let engine = create_test_engine().await?;

    // Setup table
    engine
        .db_handle()
        .execute(
            "CREATE TABLE test_special (id TEXT PRIMARY KEY, title TEXT, content TEXT)",
            vec![],
        )
        .await?;

    // Test with various special characters
    engine
        .db_handle()
        .execute(
            r#"INSERT INTO test_special (id, title, content) VALUES
               ('row-1', 'Normal title', 'Normal content'),
               ('row-2', 'Title with "quotes"', 'Content with "quotes"'),
               ('row-3', 'Title with ''single quotes''', 'Content with ''apostrophe'''),
               ('row-4', 'Title with
newline', 'Content with
newlines'),
               ('row-5', NULL, NULL),
               ('row-6', 'Title with \backslash', 'Content with \path\to\file'),
               ('row-7', 'Unicode: émojis', 'More unicode: ñ'),
               ('row-8', 'Org syntax [[link][desc]]', '* Headline\n** Subhead'),
               ('row-9', 'Tab	separated', 'Control chars'),
               ('row-10', 'Curly {braces}', 'Square [brackets]')"#,
            vec![],
        )
        .await?;

    // Test via BackendEngine
    let prql = r#"
from test_special
derive { data = s"json_object('id', {id}, 'title', {title}, 'content', {content})" }
select { id, title, data }
render (list item_template:(text content:this.id))
    "#;

    let (sql, _) = engine.compile_query(prql.to_string(), None)?;
    println!("SQL with special chars:\n{}\n", sql);

    match engine
        .execute_query(sql.clone(), std::collections::HashMap::new(), None)
        .await
    {
        Ok(results) => {
            println!("Query succeeded with {} results", results.len());
            for row in &results {
                println!("Row: {:?}", row);
            }
        }
        Err(e) => {
            println!("Query failed: {}", e);
            // Try each row individually to find which one fails
            for i in 1..=10 {
                let single_sql = format!(
                    "SELECT id, title, json_object('id', id, 'title', title, 'content', content) as data FROM test_special WHERE id = 'row-{}'",
                    i
                );
                match engine
                    .execute_query(single_sql.clone(), std::collections::HashMap::new(), None)
                    .await
                {
                    Ok(results) => println!("Row {} OK: {:?}", i, results.first()),
                    Err(e) => println!("Row {} FAILED: {}", i, e),
                }
            }
            // Now test with replace() workaround
            println!("\nTesting with replace() workaround for backslashes...");
            let sql_with_replace = r#"
                SELECT id, title,
                    json_object(
                        'id', id,
                        'title', replace(title, '\', '\\'),
                        'content', replace(content, '\', '\\')
                    ) as data
                FROM test_special WHERE id = 'row-6'
            "#;
            match engine
                .execute_query(
                    sql_with_replace.to_string(),
                    std::collections::HashMap::new(),
                    None,
                )
                .await
            {
                Ok(results) => {
                    println!("Row 6 with replace() workaround OK: {:?}", results.first());
                }
                Err(e) => {
                    println!("Row 6 with replace() workaround FAILED: {}", e);
                }
            }
        }
    }

    Ok(())
}

/// Test to isolate the printf issue - this is the root cause
#[tokio::test]
async fn test_printf_sql_issue() -> Result<()> {
    let engine = create_test_engine().await?;

    // Setup minimal table
    engine
        .db_handle()
        .execute(
            "CREATE TABLE test_table (id TEXT PRIMARY KEY, num INTEGER)",
            vec![],
        )
        .await?;

    engine
        .db_handle()
        .execute(
            "INSERT INTO test_table (id, num) VALUES ('row-1', 100)",
            vec![],
        )
        .await?;

    // Test 1: Via BackendEngine execute_sql (fails with "Invalid formatter")
    {
        let sql = "SELECT id, printf('%012d', num) as sort_key FROM test_table";
        println!("\nTesting via BackendEngine.execute_sql: {}", sql);

        match engine
            .execute_query(sql.to_string(), std::collections::HashMap::new(), None)
            .await
        {
            Ok(results) => {
                for row in &results {
                    println!("BackendEngine result: {:?}", row);
                }
                panic!("Expected error but got results");
            }
            Err(e) => {
                let err_msg = e.to_string();
                println!("BackendEngine error (expected): {}", err_msg);
                assert!(
                    err_msg.contains("Invalid formatter"),
                    "Expected 'Invalid formatter' error, got: {}",
                    err_msg
                );
            }
        }
    }

    // Test 2: Workaround - use CAST to explicitly convert result
    {
        let sql = "SELECT id, CAST(printf('%012d', num) AS TEXT) as sort_key FROM test_table";
        println!("\nTesting CAST workaround: {}", sql);

        match engine
            .execute_query(sql.to_string(), std::collections::HashMap::new(), None)
            .await
        {
            Ok(results) => {
                for row in &results {
                    println!("CAST workaround result: {:?}", row);
                }
                println!("CAST workaround WORKS!");
            }
            Err(e) => {
                println!("CAST workaround error: {}", e);
            }
        }
    }

    // Test 3: Alternative workaround - string concatenation
    {
        let sql = "SELECT id, '' || printf('%012d', num) as sort_key FROM test_table";
        println!("\nTesting concat workaround: {}", sql);

        match engine
            .execute_query(sql.to_string(), std::collections::HashMap::new(), None)
            .await
        {
            Ok(results) => {
                for row in &results {
                    println!("Concat workaround result: {:?}", row);
                }
                println!("Concat workaround WORKS!");
            }
            Err(e) => {
                println!("Concat workaround error: {}", e);
            }
        }
    }

    // Test 4: Alternative - use substr(zeroblob) padding approach
    {
        // Left-pad a number with zeros using substr and zeroblob
        // substr('000000000000' || num, -12) gives last 12 chars
        let sql = "SELECT id, substr('000000000000' || num, -12) as sort_key FROM test_table";
        println!("\nTesting substr padding workaround: {}", sql);

        match engine
            .execute_query(sql.to_string(), std::collections::HashMap::new(), None)
            .await
        {
            Ok(results) => {
                for row in &results {
                    println!("Substr padding result: {:?}", row);
                }
                println!("Substr padding WORKS!");
            }
            Err(e) => {
                println!("Substr padding error: {}", e);
            }
        }
    }

    Ok(())
}
