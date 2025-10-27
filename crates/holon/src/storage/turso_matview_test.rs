//! Minimal reproducer for CREATE MATERIALIZED VIEW hang
//!
//! This test isolates the issue where CREATE MATERIALIZED VIEW hangs
//! when executed inside block_in_place + block_on context.
//!
//! Run individual tests with:
//!   cargo test -p holon turso_matview_test::test_create_matview_normal_async -- --nocapture
//!   cargo test -p holon turso_matview_test::test_create_matview_block_in_place -- --nocapture
//!
//! Run with timeout to prevent hang:
//!   timeout 30 cargo test -p holon turso_matview_test::test_create_matview_block_in_place -- --nocapture

use crate::storage::test_helpers::create_test_backend_at_path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::RwLock;

/// Test CREATE MATERIALIZED VIEW in normal async context (should work)
#[tokio::test]
async fn test_create_matview_normal_async() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = create_test_backend_at_path(&db_path).await;
    let db = backend.handle();

    // Create the base table
    db.execute(
        "CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            aggregate_type TEXT NOT NULL,
            status TEXT DEFAULT 'confirmed'
        )",
        vec![],
    )
    .await
    .unwrap();
    eprintln!("[test_normal_async] Created events table");

    // Try to create materialized view
    eprintln!("[test_normal_async] About to CREATE MATERIALIZED VIEW...");
    db.execute_ddl(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS events_view_block AS SELECT * FROM events WHERE status = 'confirmed'",
    )
    .await
    .unwrap();
    eprintln!("[test_normal_async] CREATE MATERIALIZED VIEW succeeded!");
}

/// Test CREATE MATERIALIZED VIEW in block_in_place context (reproduces hang)
#[tokio::test(flavor = "multi_thread")]
async fn test_create_matview_block_in_place() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = Arc::new(RwLock::new(create_test_backend_at_path(&db_path).await));

    // Simulate what happens during DI initialization
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            let backend_guard = backend.read().await;
            let db = backend_guard.handle();

            // Create the base table
            db.execute(
                "CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    aggregate_type TEXT NOT NULL,
                    status TEXT DEFAULT 'confirmed'
                )",
                vec![],
            )
            .await
            .unwrap();
            eprintln!("[test_block_in_place] Created events table");

            // Test SELECT first
            let result = db
                .query_positional("SELECT COUNT(*) FROM events", vec![])
                .await
                .unwrap();
            eprintln!(
                "[test_block_in_place] SELECT succeeded with {} rows",
                result.len()
            );

            // Try to create materialized view - this should hang
            eprintln!("[test_block_in_place] About to CREATE MATERIALIZED VIEW...");
            db.execute_ddl(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS events_view_block AS SELECT * FROM events WHERE status = 'confirmed'",
            )
            .await
            .unwrap();
            eprintln!("[test_block_in_place] CREATE MATERIALIZED VIEW succeeded!");
        });
    });
}

/// Test CREATE MATERIALIZED VIEW with spawn_blocking (alternative approach)
#[tokio::test]
async fn test_create_matview_spawn_blocking() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = Arc::new(RwLock::new(create_test_backend_at_path(&db_path).await));

    // Use spawn_blocking instead of block_in_place
    let backend_clone = Arc::clone(&backend);
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let backend_guard = backend_clone.read().await;
            let db = backend_guard.handle();

            // Create the base table
            db.execute(
                "CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    aggregate_type TEXT NOT NULL,
                    status TEXT DEFAULT 'confirmed'
                )",
                vec![],
            )
            .await
            .unwrap();
            eprintln!("[test_spawn_blocking] Created events table");

            // Try to create materialized view
            eprintln!("[test_spawn_blocking] About to CREATE MATERIALIZED VIEW...");
            db.execute_ddl(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS events_view_block AS SELECT * FROM events WHERE status = 'confirmed'",
            )
            .await
            .unwrap();
            eprintln!("[test_spawn_blocking] CREATE MATERIALIZED VIEW succeeded!");
        });
    })
    .await
    .unwrap();
}

/// Test CREATE MATERIALIZED VIEW where table is created in one block_in_place
/// and view is created in another (simulates init_schema + subscribe pattern)
#[tokio::test(flavor = "multi_thread")]
async fn test_create_matview_separate_block_in_place() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = Arc::new(RwLock::new(create_test_backend_at_path(&db_path).await));

    // First block_in_place: Create table (simulates TursoEventBus::init_schema)
    {
        let backend_clone = Arc::clone(&backend);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let backend_guard = backend_clone.read().await;
                let db = backend_guard.handle();
                db.execute(
                    "CREATE TABLE IF NOT EXISTS events (
                        id TEXT PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        aggregate_type TEXT NOT NULL,
                        status TEXT DEFAULT 'confirmed'
                    )",
                    vec![],
                )
                .await
                .unwrap();
                eprintln!("[test_separate_block_in_place] Created events table");
            });
        });
    }

    // Second block_in_place: Create view (simulates CacheEventSubscriber::start -> subscribe)
    {
        let backend_clone = Arc::clone(&backend);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let backend_guard = backend_clone.read().await;
                let db = backend_guard.handle();

                eprintln!("[test_separate_block_in_place] About to CREATE MATERIALIZED VIEW...");
                db.execute_ddl(
                    "CREATE MATERIALIZED VIEW IF NOT EXISTS events_view_block AS SELECT * FROM events WHERE status = 'confirmed'",
                )
                .await
                .unwrap();
                eprintln!("[test_separate_block_in_place] CREATE MATERIALIZED VIEW succeeded!");
            });
        });
    }
}

/// Test that simulates the full TursoEventBus::init_schema + subscribe pattern
/// including all indexes that are created in production
#[tokio::test(flavor = "multi_thread")]
async fn test_create_matview_full_init_schema_pattern() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = Arc::new(RwLock::new(create_test_backend_at_path(&db_path).await));

    // First block_in_place: Full init_schema (exactly as TursoEventBus does it)
    {
        let backend_clone = Arc::clone(&backend);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let backend_guard = backend_clone.read().await;
                let db = backend_guard.handle();

                // Create events table
                db.execute(
                    "CREATE TABLE IF NOT EXISTS events (
                        id TEXT PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        aggregate_type TEXT NOT NULL,
                        aggregate_id TEXT NOT NULL,
                        origin TEXT NOT NULL,
                        status TEXT DEFAULT 'confirmed',
                        payload TEXT NOT NULL,
                        trace_id TEXT,
                        command_id TEXT,
                        created_at INTEGER NOT NULL,
                        processed_by_loro INTEGER DEFAULT 0,
                        processed_by_org INTEGER DEFAULT 0,
                        processed_by_cache INTEGER DEFAULT 0,
                        speculative_id TEXT,
                        rejection_reason TEXT
                    )",
                    vec![],
                )
                .await
                .unwrap();
                eprintln!("[test_full_init_schema] Created events table");

                // Create indexes
                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_events_loro_pending
                     ON events(\"created_at\")
                     WHERE processed_by_loro = 0 AND origin != 'loro' AND status = 'confirmed'",
                    vec![],
                )
                .await
                .unwrap();
                eprintln!("[test_full_init_schema] Created loro pending index");

                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_events_org_pending
                     ON events(\"created_at\")
                     WHERE processed_by_org = 0 AND origin != 'org' AND status = 'confirmed'",
                    vec![],
                )
                .await
                .unwrap();
                eprintln!("[test_full_init_schema] Created org pending index");

                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_events_cache_pending
                     ON events(\"created_at\")
                     WHERE processed_by_cache = 0 AND status = 'confirmed'",
                    vec![],
                )
                .await
                .unwrap();
                eprintln!("[test_full_init_schema] Created cache pending index");

                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_events_aggregate
                     ON events(\"aggregate_type\", \"aggregate_id\", \"created_at\")",
                    vec![],
                )
                .await
                .unwrap();
                eprintln!("[test_full_init_schema] Created aggregate index");

                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_events_command
                     ON events(\"command_id\")
                     WHERE command_id IS NOT NULL",
                    vec![],
                )
                .await
                .unwrap();
                eprintln!("[test_full_init_schema] Created command index");
            });
        });
    }

    // Second block_in_place: Create view (simulates CacheEventSubscriber::start -> subscribe)
    {
        let backend_clone = Arc::clone(&backend);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let backend_guard = backend_clone.read().await;
                let db = backend_guard.handle();

                eprintln!("[test_full_init_schema] About to CREATE MATERIALIZED VIEW...");
                db.execute_ddl(
                    "CREATE MATERIALIZED VIEW IF NOT EXISTS events_view_block AS SELECT * FROM events WHERE 1=1 AND status IN ('confirmed') AND aggregate_type IN ('block')",
                )
                .await
                .unwrap();
                eprintln!("[test_full_init_schema] CREATE MATERIALIZED VIEW succeeded!");
            });
        });
    }
}

/// Test CREATE MATERIALIZED VIEW with spawned tasks before block_in_place
/// This simulates the DI scenario where tasks are spawned before subscribe() is called
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_create_matview_with_spawned_tasks() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = Arc::new(RwLock::new(create_test_backend_at_path(&db_path).await));

    // Create table first
    {
        let backend_guard = backend.read().await;
        let db = backend_guard.handle();
        db.execute(
            "CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                status TEXT DEFAULT 'confirmed'
            )",
            vec![],
        )
        .await
        .unwrap();
        eprintln!("[test_with_spawned_tasks] Created events table");
    }

    // Spawn some background tasks (simulating OrgModeModule DI pattern)
    let backend_clone1 = Arc::clone(&backend);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            // Simulate periodic work that uses the backend
            let _ = backend_clone1.read().await;
        }
    });

    let backend_clone2 = Arc::clone(&backend);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            // Another background task
            let _ = backend_clone2.read().await;
        }
    });

    // Give spawned tasks a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Now try to create materialized view in block_in_place (simulating CacheEventSubscriber::start)
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            let backend_guard = backend.read().await;
            let db = backend_guard.handle();

            eprintln!("[test_with_spawned_tasks] About to CREATE MATERIALIZED VIEW...");
            db.execute_ddl(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS events_view_block AS SELECT * FROM events WHERE status = 'confirmed'",
            )
            .await
            .unwrap();
            eprintln!("[test_with_spawned_tasks] CREATE MATERIALIZED VIEW succeeded!");
        });
    });
}

/// Test nested block_in_place (simulates DI factory calling another factory)
#[tokio::test(flavor = "multi_thread")]
async fn test_create_matview_nested_block_in_place() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let backend = Arc::new(RwLock::new(create_test_backend_at_path(&db_path).await));

    // Outer block_in_place (simulates DI initialization)
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            // Create table in outer block
            {
                let backend_guard = backend.read().await;
                let db = backend_guard.handle();
                db.execute(
                    "CREATE TABLE IF NOT EXISTS events (
                        id TEXT PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        aggregate_type TEXT NOT NULL,
                        status TEXT DEFAULT 'confirmed'
                    )",
                    vec![],
                )
                .await
                .unwrap();
                eprintln!("[test_nested] Created events table");
            }

            // Inner block_in_place (simulates subscriber setup)
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let backend_guard = backend.read().await;
                    let db = backend_guard.handle();

                    eprintln!("[test_nested] About to CREATE MATERIALIZED VIEW (nested)...");
                    db.execute_ddl(
                        "CREATE MATERIALIZED VIEW IF NOT EXISTS events_view_block AS SELECT * FROM events WHERE status = 'confirmed'",
                    )
                    .await
                    .unwrap();
                    eprintln!("[test_nested] CREATE MATERIALIZED VIEW succeeded (nested)!");
                });
            });
        });
    });
}
