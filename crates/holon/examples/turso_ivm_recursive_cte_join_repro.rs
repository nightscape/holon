//! Minimal reproducer for Turso IVM "Invalid join commit state" error
//!
//! This bug occurs when:
//! 1. Multiple materialized views with JOINs exist
//! 2. A materialized view uses a recursive CTE with INNER JOIN
//! 3. CDC callbacks are active via set_view_change_callback()
//! 4. Data is inserted into the base table, triggering IVM update
//!
//! The error manifests as "Invalid join commit state" during the IVM commit phase.
//!
//! Run with: cargo run --example turso_ivm_recursive_cte_join_repro
//!
//! Expected: Panic with "Invalid join commit state" or similar JoinOperator error

use std::path::Path;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Use a fresh database each time
    let db_path = "/tmp/turso-ivm-recursive-cte-test.db";
    if Path::new(db_path).exists() {
        std::fs::remove_file(db_path)?;
    }
    let _ = std::fs::remove_file(format!("{}-wal", db_path));
    let _ = std::fs::remove_file(format!("{}-shm", db_path));

    println!("Creating database at {}", db_path);
    let db = turso::Builder::new_local(db_path)
        .with_views(true) // Enable experimental materialized views
        .build()
        .await?;
    let conn = db.connect()?;

    // =====================================================================
    // STEP 1: Create events table with materialized views (from TursoEventBus)
    // =====================================================================
    println!("\n[STEP 1] Creating events table and matviews...");

    conn.execute(
        r#"CREATE TABLE events (
            id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            aggregate_type TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            origin TEXT NOT NULL,
            status TEXT DEFAULT 'confirmed',
            payload TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )"#,
        (),
    )
    .await?;

    // Materialized view on events (NO JOIN)
    conn.execute(
        r#"CREATE MATERIALIZED VIEW events_view_block AS
           SELECT * FROM events
           WHERE status = 'confirmed' AND aggregate_type = 'block'"#,
        (),
    )
    .await?;

    println!("  events_view_block created");

    // =====================================================================
    // STEP 2: Create navigation tables with JOIN matview (from NavigationSchema)
    // =====================================================================
    println!("\n[STEP 2] Creating navigation tables with JOIN matview...");

    conn.execute(
        r#"CREATE TABLE navigation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT NOT NULL,
            block_id TEXT,
            timestamp TEXT DEFAULT (datetime('now'))
        )"#,
        (),
    )
    .await?;

    conn.execute(
        r#"CREATE TABLE navigation_cursor (
            region TEXT PRIMARY KEY,
            history_id INTEGER REFERENCES navigation_history(id)
        )"#,
        (),
    )
    .await?;

    // Initialize cursor with NULL
    conn.execute(
        "INSERT INTO navigation_cursor (region, history_id) VALUES ('main', NULL)",
        (),
    )
    .await?;

    // DROP first to ensure clean state
    let _ = conn.execute("DROP VIEW IF EXISTS current_focus", ()).await;

    // Create current_focus matview WITH JOIN
    conn.execute(
        r#"CREATE MATERIALIZED VIEW current_focus AS
           SELECT nc.region, nh.block_id, nh.timestamp
           FROM navigation_cursor nc
           JOIN navigation_history nh ON nc.history_id = nh.id"#,
        (),
    )
    .await?;

    println!("  current_focus created (has JOIN)");

    // =====================================================================
    // STEP 3: Create blocks table with recursive CTE matview
    // =====================================================================
    println!("\n[STEP 3] Creating blocks table with recursive CTE matview...");

    conn.execute(
        r#"CREATE TABLE blocks (
            id TEXT PRIMARY KEY,
            parent_id TEXT NOT NULL,
            content TEXT DEFAULT '',
            content_type TEXT DEFAULT 'text',
            source_language TEXT,
            source_name TEXT,
            properties TEXT DEFAULT '{}',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )"#,
        (),
    )
    .await?;

    // Create blocks_with_paths - recursive CTE with INNER JOIN
    conn.execute(
        r#"CREATE MATERIALIZED VIEW blocks_with_paths AS
        WITH RECURSIVE paths AS (
            SELECT
                id, parent_id, content, content_type,
                source_language, source_name, properties,
                created_at, updated_at,
                '/' || id as path
            FROM blocks
            WHERE parent_id LIKE 'holon-doc://%'
               OR parent_id = '__no_parent__'

            UNION ALL

            SELECT
                b.id, b.parent_id, b.content, b.content_type,
                b.source_language, b.source_name, b.properties,
                b.created_at, b.updated_at,
                p.path || '/' || b.id as path
            FROM blocks b
            INNER JOIN paths p ON b.parent_id = p.id
        )
        SELECT * FROM paths"#,
        (),
    )
    .await?;

    println!("  blocks_with_paths created (recursive CTE with JOIN)");

    // =====================================================================
    // STEP 4: Set up CDC callback
    // =====================================================================
    println!("\n[STEP 4] Setting up CDC callback...");

    conn.set_view_change_callback(|event| {
        println!(
            "  CDC: {} changes to {}",
            event.changes.len(),
            event.relation_name
        );
    })?;

    // =====================================================================
    // STEP 5: Insert some initial data
    // =====================================================================
    println!("\n[STEP 5] Inserting initial blocks (before navigation update)...");

    conn.execute(
        r#"INSERT INTO blocks (id, parent_id, content, content_type)
           VALUES ('root-1', 'holon-doc://test.org', 'Root content', 'text')"#,
        (),
    )
    .await?;

    conn.execute(
        r#"INSERT INTO blocks (id, parent_id, content, content_type)
           VALUES ('child-1', 'root-1', 'Child content', 'text')"#,
        (),
    )
    .await?;

    println!("  Initial blocks inserted");

    // =====================================================================
    // STEP 6: Update navigation (creates a JOIN scenario)
    // =====================================================================
    println!("\n[STEP 6] Updating navigation (creates data in JOIN view)...");

    // Insert navigation history
    conn.execute(
        "INSERT INTO navigation_history (region, block_id) VALUES ('main', 'root-1')",
        (),
    )
    .await?;

    // Update cursor to point to the history entry
    conn.execute(
        "UPDATE navigation_cursor SET history_id = 1 WHERE region = 'main'",
        (),
    )
    .await?;

    println!("  Navigation updated");

    // =====================================================================
    // STEP 7: Insert more blocks - this is where the bug typically manifests
    // =====================================================================
    println!("\n[STEP 7] Inserting more blocks (this triggers IVM on multiple views)...");
    println!("  If bug exists, panic should occur here...\n");

    // This insert should trigger:
    // 1. IVM update on blocks_with_paths (recursive CTE with JOIN)
    // 2. CDC callback fires
    // The combination with existing current_focus JOIN view can cause "Invalid join commit state"

    conn.execute(
        r#"INSERT INTO blocks (id, parent_id, content, content_type)
           VALUES ('grandchild-1', 'child-1', 'Grandchild content', 'text')"#,
        (),
    )
    .await?;

    println!("  grandchild-1 inserted");

    // Insert a few more to stress test
    for i in 2..=5 {
        conn.execute(
            &format!(
                r#"INSERT INTO blocks (id, parent_id, content, content_type)
                   VALUES ('child-{}', 'root-1', 'Child {} content', 'text')"#,
                i, i
            ),
            (),
        )
        .await?;
        println!("  child-{} inserted", i);
    }

    // =====================================================================
    // STEP 8: Verify data
    // =====================================================================
    println!("\n[STEP 8] Querying blocks_with_paths...");

    let mut rows = conn
        .query("SELECT id, path FROM blocks_with_paths ORDER BY path", ())
        .await?;

    println!("  blocks_with_paths contents:");
    while let Some(row) = rows.next().await? {
        let id: String = row.get(0)?;
        let path: String = row.get(1)?;
        println!("    {} -> {}", id, path);
    }

    println!("\n=== SUCCESS ===");
    println!("No panic occurred. The bug may:");
    println!("  - Be fixed in this Turso version");
    println!("  - Require more complex concurrent operations");
    println!("  - Depend on specific timing conditions");

    Ok(())
}
