//! Minimal reproducer for Turso IVM JOIN panic
//!
//! This test reproduces the btree assertion failure when creating
//! a materialized view with a JOIN.
//!
//! Run with:
//!   cargo test -p holon turso_ivm_join_test::test_matview_with_join -- --nocapture
//!
//! The panic occurs at:
//!   turso_core::storage::btree::PageStack::top
//!   assertion failed: self.current_page >= 0

use crate::storage::test_helpers::create_test_backend_at_path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::RwLock;

/// Minimal reproducer: CREATE MATERIALIZED VIEW with JOIN panics
#[tokio::test]
async fn test_matview_with_join_panics() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    // Create base tables
    db.execute(
        "CREATE TABLE IF NOT EXISTS navigation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT NOT NULL,
            block_id TEXT,
            timestamp TEXT DEFAULT (datetime('now'))
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE IF NOT EXISTS navigation_cursor (
            region TEXT PRIMARY KEY,
            history_id INTEGER REFERENCES navigation_history(id)
        )",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[test] Tables created");

    // Insert some data (tables being empty might be relevant)
    db.execute(
        "INSERT INTO navigation_cursor (region, history_id) VALUES ('main', NULL)",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[test] Data inserted");

    // This panics with: assertion failed: self.current_page >= 0
    eprintln!("[test] About to create materialized view with JOIN...");
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW current_focus AS
        SELECT
            nc.region,
            nh.block_id,
            nh.timestamp
        FROM navigation_cursor nc
        JOIN navigation_history nh ON nc.history_id = nh.id",
    )
    .await
    .unwrap();

    eprintln!("[test] Materialized view created successfully!");
}

/// Test if the issue is related to NULL foreign keys
#[tokio::test]
async fn test_matview_with_join_and_data() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    // Create base tables
    db.execute(
        "CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY,
            data TEXT
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE IF NOT EXISTS cursor (
            id TEXT PRIMARY KEY,
            history_id INTEGER
        )",
        vec![],
    )
    .await
    .unwrap();

    // Insert matching data (no NULLs)
    db.execute("INSERT INTO history (id, data) VALUES (1, 'test')", vec![])
        .await
        .unwrap();

    db.execute(
        "INSERT INTO cursor (id, history_id) VALUES ('main', 1)",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[test_with_data] Tables created and data inserted");

    // Try with matching data
    eprintln!("[test_with_data] About to create materialized view with JOIN...");
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW test_view AS
        SELECT c.id, h.data
        FROM cursor c
        JOIN history h ON c.history_id = h.id",
    )
    .await
    .unwrap();

    eprintln!("[test_with_data] Materialized view created successfully!");
}

/// Simplest possible JOIN materialized view
#[tokio::test]
async fn test_simplest_join_matview() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)", vec![])
        .await
        .unwrap();

    db.execute(
        "CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[simplest] Tables created (empty)");

    // Try with empty tables
    eprintln!("[simplest] Creating materialized view with JOIN on empty tables...");
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW ab_view AS
        SELECT a.id, a.val, b.id as b_id
        FROM a
        JOIN b ON b.a_id = a.id",
    )
    .await
    .unwrap();

    eprintln!("[simplest] Success!");
}

/// Reproduce: Create matview with JOIN in block_in_place context
/// This simulates the DI initialization pattern
#[tokio::test(flavor = "multi_thread")]
async fn test_matview_join_block_in_place() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = Arc::new(RwLock::new(create_test_backend_at_path(&db_path).await));

    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            let backend_guard = backend.read().await;
            let db = backend_guard.handle();

            // Create tables
            db.execute(
                "CREATE TABLE IF NOT EXISTS navigation_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    region TEXT NOT NULL,
                    block_id TEXT
                )",
                vec![],
            )
            .await
            .unwrap();

            db.execute(
                "CREATE TABLE IF NOT EXISTS navigation_cursor (
                    region TEXT PRIMARY KEY,
                    history_id INTEGER
                )",
                vec![],
            )
            .await
            .unwrap();

            eprintln!("[block_in_place] Tables created");

            // Insert data
            db.execute(
                "INSERT INTO navigation_cursor (region, history_id) VALUES ('main', NULL)",
                vec![],
            )
            .await
            .unwrap();

            eprintln!("[block_in_place] About to create JOIN materialized view...");
            db.execute_ddl(
                "CREATE MATERIALIZED VIEW current_focus AS
                SELECT nc.region, nh.block_id
                FROM navigation_cursor nc
                JOIN navigation_history nh ON nc.history_id = nh.id",
            )
            .await
            .unwrap();

            eprintln!("[block_in_place] Success!");
        });
    });
}

/// Reproduce: Create multiple matviews, then one with JOIN
/// This tests if previous views affect JOIN view creation
#[tokio::test]
async fn test_multiple_matviews_then_join() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    // Create events table and matview (like TursoEventBus does)
    db.execute(
        "CREATE TABLE events (
            id TEXT PRIMARY KEY,
            event_type TEXT,
            status TEXT DEFAULT 'confirmed'
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute_ddl(
        "CREATE MATERIALIZED VIEW events_view AS
        SELECT * FROM events WHERE status = 'confirmed'",
    )
    .await
    .unwrap();

    eprintln!("[multi] First matview created (no join)");

    // Create navigation tables
    db.execute(
        "CREATE TABLE navigation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT NOT NULL,
            block_id TEXT
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE navigation_cursor (
            region TEXT PRIMARY KEY,
            history_id INTEGER
        )",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[multi] Navigation tables created");

    // Now try to create JOIN matview
    eprintln!("[multi] About to create JOIN materialized view...");
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW current_focus AS
        SELECT nc.region, nh.block_id
        FROM navigation_cursor nc
        JOIN navigation_history nh ON nc.history_id = nh.id",
    )
    .await
    .unwrap();

    eprintln!("[multi] Success!");
}

/// Reproduce: Failed view creation followed by JOIN view
/// This tests if a previous failure corrupts state
#[tokio::test]
async fn test_failed_view_then_join_view() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    // Create tables
    db.execute("CREATE TABLE items (id TEXT PRIMARY KEY, val TEXT)", vec![])
        .await
        .unwrap();

    db.execute(
        "CREATE TABLE cursors (id TEXT PRIMARY KEY, item_id TEXT)",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[failed_then_join] Tables created");

    // Try to create a view with a literal in JOIN (should fail)
    let result = db
        .execute_ddl(
            "CREATE MATERIALIZED VIEW bad_view AS
            SELECT i.id FROM items i
            LEFT JOIN cursors c ON c.id = 'main'
            WHERE i.id = c.item_id",
        )
        .await;

    if let Err(e) = result {
        eprintln!("[failed_then_join] Expected failure: {}", e);
    }

    // Now try to create a valid JOIN view
    eprintln!("[failed_then_join] About to create valid JOIN materialized view...");
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW good_view AS
        SELECT i.id, i.val, c.id as cursor_id
        FROM items i
        JOIN cursors c ON c.item_id = i.id",
    )
    .await
    .unwrap();

    eprintln!("[failed_then_join] Success!");
}

/// MINIMAL REPRODUCER: JOIN materialized view panics on INSERT
///
/// This is the bug we need to fix in Turso.
/// Run with: cargo test -p holon turso_ivm_join_test::test_join_matview_insert_panics -- --nocapture
#[tokio::test]
async fn test_join_matview_insert_panics() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    // Step 1: Create two tables
    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)", vec![])
        .await
        .unwrap();

    db.execute(
        "CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[REPRODUCER] Tables created");

    // Step 2: Create JOIN materialized view (succeeds)
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW ab_view AS
        SELECT a.id, a.val, b.id as b_id
        FROM a
        JOIN b ON b.a_id = a.id",
    )
    .await
    .unwrap();

    eprintln!("[REPRODUCER] Materialized view created");

    // Step 3: Insert data into base table - THIS PANICS!
    eprintln!("[REPRODUCER] About to INSERT into table 'a'...");
    db.execute("INSERT INTO a VALUES (1, 'test')", vec![])
        .await
        .unwrap();

    eprintln!("[REPRODUCER] Insert succeeded!");
}

/// REPRODUCER: Exact navigation schema that panics
///
/// Run with: cargo test -p holon turso_ivm_join_test::test_navigation_schema_panics -- --nocapture
#[tokio::test]
async fn test_navigation_schema_panics() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    // Exact schema from navigation/schema.rs
    db.execute(
        "CREATE TABLE IF NOT EXISTS navigation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT NOT NULL,
            block_id TEXT,
            timestamp TEXT DEFAULT (datetime('now'))
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_navigation_history_region
        ON navigation_history(\"region\")",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE IF NOT EXISTS navigation_cursor (
            region TEXT PRIMARY KEY,
            history_id INTEGER REFERENCES navigation_history(id)
        )",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[NAV] Tables created");

    // Insert cursor with NULL history_id (like schema.rs does)
    db.execute(
        "INSERT OR IGNORE INTO navigation_cursor (region, history_id) VALUES ('main', NULL)",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[NAV] Cursor inserted with NULL history_id");

    // Create the JOIN materialized view
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW current_focus AS
        SELECT
            nc.region,
            nh.block_id,
            nh.timestamp
        FROM navigation_cursor nc
        JOIN navigation_history nh ON nc.history_id = nh.id",
    )
    .await
    .unwrap();

    eprintln!("[NAV] Materialized view created");

    // Now insert into navigation_history - does this panic?
    eprintln!("[NAV] About to INSERT into navigation_history...");
    db.execute(
        "INSERT INTO navigation_history (region, block_id) VALUES ('main', 'block-123')",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[NAV] Insert into navigation_history succeeded!");

    // Update cursor to point to the new history entry
    eprintln!("[NAV] About to UPDATE navigation_cursor...");
    db.execute(
        "UPDATE navigation_cursor SET history_id = 1 WHERE region = 'main'",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[NAV] Update succeeded!");
}

/// REPRODUCER: Watch query with JOIN panics on base table insert (with transaction)
///
/// Same as test_watch_query_join_insert_panics but uses explicit transaction
/// like QueryableCache::execute_batch_transaction does.
///
/// Run with: cargo test -p holon turso_ivm_join_test::test_watch_query_join_insert_in_transaction -- --nocapture
#[tokio::test]
async fn test_watch_query_join_insert_in_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    // Setup tables and views
    db.execute(
        "CREATE TABLE blocks (id TEXT PRIMARY KEY, parent_id TEXT, content TEXT)",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE navigation_history (id INTEGER PRIMARY KEY AUTOINCREMENT, region TEXT, block_id TEXT)",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE navigation_cursor (region TEXT PRIMARY KEY, history_id INTEGER)",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "INSERT INTO navigation_cursor (region, history_id) VALUES ('main', NULL)",
        vec![],
    )
    .await
    .unwrap();

    db.execute_ddl(
        "CREATE MATERIALIZED VIEW current_focus AS
        SELECT nc.region, nh.block_id
        FROM navigation_cursor nc
        JOIN navigation_history nh ON nc.history_id = nh.id",
    )
    .await
    .unwrap();

    db.execute_ddl(
        "CREATE MATERIALIZED VIEW watch_view_main AS
        SELECT blocks.id, blocks.parent_id, blocks.content
        FROM blocks
        JOIN current_focus cf ON blocks.parent_id = cf.block_id
        WHERE cf.region = 'main'",
    )
    .await
    .unwrap();

    eprintln!("[TX_JOIN] Setup complete, starting transaction");

    // Now insert in a transaction (like QueryableCache does)
    db.execute("BEGIN IMMEDIATE", vec![]).await.unwrap();
    eprintln!("[TX_JOIN] Transaction started");

    eprintln!("[TX_JOIN] About to INSERT into blocks...");
    db.execute(
        "INSERT INTO blocks (id, parent_id, content) VALUES ('block-1', 'root', 'Hello')",
        vec![],
    )
    .await
    .unwrap();
    eprintln!("[TX_JOIN] Insert completed");

    db.execute("COMMIT", vec![]).await.unwrap();
    eprintln!("[TX_JOIN] Commit succeeded!");
}

/// REPRODUCER: Watch query with JOIN panics on base table insert
///
/// This is the EXACT bug in holon. The sequence:
/// 1. Create base tables (blocks, navigation tables)
/// 2. Create current_focus matview (JOIN between nav tables)
/// 3. Create watch query matview that JOINs blocks with current_focus
/// 4. Insert into blocks -> PANIC in JoinOperator::commit
///
/// Run with: cargo test -p holon turso_ivm_join_test::test_watch_query_join_insert_panics -- --nocapture
#[tokio::test]
async fn test_watch_query_join_insert_panics() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    // Step 1: Create blocks table
    db.execute(
        "CREATE TABLE blocks (
            id TEXT PRIMARY KEY,
            parent_id TEXT,
            content TEXT
        )",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[WATCH_JOIN] blocks table created");

    // Step 2: Create navigation tables
    db.execute(
        "CREATE TABLE navigation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT NOT NULL,
            block_id TEXT
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE navigation_cursor (
            region TEXT PRIMARY KEY,
            history_id INTEGER
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "INSERT INTO navigation_cursor (region, history_id) VALUES ('main', NULL)",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[WATCH_JOIN] navigation tables created");

    // Step 3: Create current_focus matview (JOIN between nav tables)
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW current_focus AS
        SELECT nc.region, nh.block_id
        FROM navigation_cursor nc
        JOIN navigation_history nh ON nc.history_id = nh.id",
    )
    .await
    .unwrap();

    eprintln!("[WATCH_JOIN] current_focus matview created");

    // Step 4: Create watch query matview (like index.org query)
    // This JOINs blocks with current_focus
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW watch_view_main AS
        SELECT blocks.id, blocks.parent_id, blocks.content
        FROM blocks
        JOIN current_focus cf ON blocks.parent_id = cf.block_id
        WHERE cf.region = 'main'",
    )
    .await
    .unwrap();

    eprintln!("[WATCH_JOIN] watch_view matview created");

    // Step 5: Insert into blocks -> THIS SHOULD PANIC
    eprintln!("[WATCH_JOIN] About to INSERT into blocks...");
    db.execute(
        "INSERT INTO blocks (id, parent_id, content) VALUES ('block-1', 'root', 'Hello')",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[WATCH_JOIN] Insert succeeded!");
}

/// REPRODUCER: Full holon schema - exact reproduction attempt
///
/// Uses the exact schema from the production crash.
///
/// Run with: cargo test -p holon turso_ivm_join_test::test_full_holon_schema_panic -- --nocapture
#[tokio::test]
async fn test_full_holon_schema_panic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    // Full blocks schema from holon
    db.execute(
        "CREATE TABLE blocks (
            id TEXT PRIMARY KEY,
            parent_id TEXT,
            content TEXT,
            content_type TEXT,
            source_language TEXT,
            source_name TEXT,
            source_header_args TEXT,
            source_results TEXT,
            properties TEXT,
            children TEXT,
            created_at TEXT,
            updated_at TEXT,
            _change_origin TEXT
        )",
        vec![],
    )
    .await
    .unwrap();

    // Events table and matview (like TursoEventBus)
    db.execute(
        "CREATE TABLE events (
            id TEXT PRIMARY KEY,
            event_type TEXT,
            aggregate_type TEXT,
            status TEXT DEFAULT 'confirmed'
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute_ddl(
        "CREATE MATERIALIZED VIEW events_view_block AS
        SELECT * FROM events WHERE status = 'confirmed' AND aggregate_type = 'block'",
    )
    .await
    .unwrap();

    // Navigation schema
    db.execute(
        "CREATE TABLE navigation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT NOT NULL,
            block_id TEXT,
            timestamp TEXT DEFAULT (datetime('now'))
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE navigation_cursor (
            region TEXT PRIMARY KEY,
            history_id INTEGER REFERENCES navigation_history(id)
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "INSERT INTO navigation_cursor (region, history_id) VALUES ('main', NULL)",
        vec![],
    )
    .await
    .unwrap();

    db.execute_ddl(
        "CREATE MATERIALIZED VIEW current_focus AS
        SELECT nc.region, nh.block_id, nh.timestamp
        FROM navigation_cursor nc
        JOIN navigation_history nh ON nc.history_id = nh.id",
    )
    .await
    .unwrap();

    eprintln!("[FULL_SCHEMA] Base schema created");

    // First watch view (no JOIN)
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW watch_view_1 AS
        SELECT * FROM blocks WHERE properties LIKE '%REGION%'",
    )
    .await
    .unwrap();

    eprintln!("[FULL_SCHEMA] watch_view_1 created (no JOIN)");

    // Second watch view (WITH JOIN to current_focus)
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW watch_view_2 AS
        SELECT blocks.id, blocks.parent_id, blocks.content, blocks.content_type, blocks.id AS sort_key
        FROM blocks
        INNER JOIN current_focus AS cf ON blocks.parent_id = cf.block_id
        WHERE cf.region = 'main'",
    )
    .await
    .unwrap();

    eprintln!("[FULL_SCHEMA] watch_view_2 created (WITH JOIN)");

    // Now insert using a transaction (like QueryableCache does)
    db.execute("BEGIN IMMEDIATE", vec![]).await.unwrap();

    eprintln!("[FULL_SCHEMA] Transaction started, about to INSERT...");
    db.execute(
        "INSERT INTO blocks (id, parent_id, content, content_type, _change_origin)
        VALUES ('block-1', 'root', 'Hello', 'paragraph', 'local')",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[FULL_SCHEMA] Insert done, about to COMMIT...");
    db.execute("COMMIT", vec![]).await.unwrap();

    eprintln!("[FULL_SCHEMA] Commit succeeded!");
}

/// REPRODUCER: NESTED MATVIEW JOINS - THE ACTUAL BUG!
///
/// The bug is: when a matview (watch_view) JOINs with another matview (current_focus),
/// and that other matview also has a JOIN internally, inserting into the base table panics.
///
/// Scenario:
/// 1. current_focus = MATVIEW with JOIN (navigation_cursor JOIN navigation_history)
/// 2. watch_view = MATVIEW with JOIN (blocks JOIN current_focus)
/// 3. INSERT into blocks -> PANIC!
///
/// Run with: cargo test -p holon turso_ivm_join_test::test_nested_matview_joins_panic -- --nocapture
#[tokio::test]
async fn test_nested_matview_joins_panic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    // Step 1: Create base tables
    db.execute(
        "CREATE TABLE blocks (id TEXT PRIMARY KEY, parent_id TEXT, content TEXT)",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE navigation_history (id INTEGER PRIMARY KEY AUTOINCREMENT, region TEXT, block_id TEXT)",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE navigation_cursor (region TEXT PRIMARY KEY, history_id INTEGER)",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[NESTED] Base tables created");

    // Step 2: Create first matview with JOIN (current_focus)
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW current_focus AS
        SELECT nc.region, nh.block_id
        FROM navigation_cursor nc
        JOIN navigation_history nh ON nc.history_id = nh.id",
    )
    .await
    .unwrap();

    eprintln!("[NESTED] current_focus matview created (has JOIN)");

    // Step 3: Create second matview that JOINs with the first matview
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW watch_view AS
        SELECT blocks.id, blocks.parent_id, blocks.content
        FROM blocks
        INNER JOIN current_focus AS cf ON blocks.parent_id = cf.block_id
        WHERE cf.region = 'main'",
    )
    .await
    .unwrap();

    eprintln!("[NESTED] watch_view matview created (JOINs with current_focus matview)");

    // Step 4: Insert into blocks - THIS SHOULD TRIGGER THE PANIC
    eprintln!("[NESTED] About to INSERT into blocks...");
    db.execute(
        "INSERT INTO blocks (id, parent_id, content) VALUES ('block-1', 'some-parent', 'Hello')",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[NESTED] Insert succeeded!");
}

/// REPRODUCER: Multiple matviews + JOIN matview
///
/// Run with: cargo test -p holon turso_ivm_join_test::test_multiple_matviews_with_join -- --nocapture
#[tokio::test]
async fn test_multiple_matviews_with_join() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    // Create events table and matview (like TursoEventBus)
    db.execute(
        "CREATE TABLE events (
            id TEXT PRIMARY KEY,
            event_type TEXT,
            aggregate_type TEXT,
            status TEXT DEFAULT 'confirmed'
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute_ddl(
        "CREATE MATERIALIZED VIEW events_view_block AS
        SELECT * FROM events WHERE status = 'confirmed' AND aggregate_type = 'block'",
    )
    .await
    .unwrap();

    eprintln!("[MULTI] First matview created");

    // Create navigation tables
    db.execute(
        "CREATE TABLE navigation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT NOT NULL,
            block_id TEXT
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE navigation_cursor (
            region TEXT PRIMARY KEY,
            history_id INTEGER
        )",
        vec![],
    )
    .await
    .unwrap();

    db.execute(
        "INSERT INTO navigation_cursor (region, history_id) VALUES ('main', NULL)",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[MULTI] Navigation tables created");

    // Create JOIN matview
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW current_focus AS
        SELECT nc.region, nh.block_id
        FROM navigation_cursor nc
        JOIN navigation_history nh ON nc.history_id = nh.id",
    )
    .await
    .unwrap();

    eprintln!("[MULTI] JOIN matview created");

    // Insert event - this triggers the first matview
    eprintln!("[MULTI] Inserting event...");
    db.execute(
        "INSERT INTO events (id, event_type, aggregate_type) VALUES ('evt-1', 'created', 'block')",
        vec![],
    )
    .await
    .unwrap();

    eprintln!("[MULTI] Event inserted successfully!");
}
