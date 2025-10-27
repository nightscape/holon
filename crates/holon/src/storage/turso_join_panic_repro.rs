//! Minimal reproduction for Turso JoinOperator panic
//!
//! This test demonstrates a panic in Turso's incremental view maintenance (IVM)
//! when using explicit transactions with materialized views that contain recursive
//! CTEs with JOINs.
//!
//! ## Bug Description
//!
//! When a transaction commits changes to a table that has a materialized view with
//! a recursive CTE containing a self-JOIN, Turso panics with:
//!
//! ```
//! thread 'tokio-runtime-worker' panicked at join_operator.rs:731:21:
//! Invalid join commit state
//! ```
//!
//! ## Stack Trace (abbreviated)
//!
//! ```
//! turso_core::incremental::join_operator::JoinOperator::commit
//! turso_core::incremental::compiler::DbspCircuit::commit
//! turso_core::incremental::view::IncrementalView::merge_delta
//! turso_core::vdbe::Program::commit_txn
//! ```
//!
//! ## Conditions to Trigger
//!
//! 1. Create a table with self-referencing parent_id
//! 2. Create a materialized view with recursive CTE that JOINs the table to itself
//! 3. Use explicit BEGIN IMMEDIATE TRANSACTION
//! 4. Insert/update rows within the transaction
//! 5. COMMIT (or auto-commit triggers the bug)
//!
//! ## Workaround
//!
//! Using individual inserts with auto-commit (no explicit transaction) works fine.
//! The bug only manifests with explicit transactions.

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Create a temporary database path
    fn temp_db_path() -> (TempDir, PathBuf) {
        let dir = TempDir::new().expect("Failed to create temp dir");
        let path = dir.path().join("test.db");
        (dir, path)
    }

    /// Minimal reproduction of JoinOperator panic
    ///
    /// This test will panic with "Invalid join commit state" in Turso's JoinOperator.
    #[tokio::test]
    async fn test_join_operator_panic_with_recursive_view() {
        let (_dir, db_path) = temp_db_path();

        // Create database
        let db = turso::Database::open(db_path.to_str().unwrap())
            .await
            .expect("Failed to open database");
        let conn = db.connect().expect("Failed to connect");

        // Step 1: Create table with self-referencing parent_id
        conn.execute(
            r#"
            CREATE TABLE blocks (
                id TEXT PRIMARY KEY,
                parent_id TEXT,
                content TEXT
            )
            "#,
            (),
        )
        .await
        .expect("Failed to create table");

        // Step 2: Create materialized view with recursive CTE containing JOIN
        // This is the key - the JOIN in the recursive part triggers the bug
        conn.execute(
            r#"
            CREATE MATERIALIZED VIEW blocks_with_paths AS
            WITH RECURSIVE paths AS (
                -- Base case: root blocks
                SELECT id, parent_id, content, '/' || id as path
                FROM blocks
                WHERE parent_id IS NULL OR parent_id = ''

                UNION ALL

                -- Recursive case: JOIN to build path
                SELECT b.id, b.parent_id, b.content, p.path || '/' || b.id as path
                FROM blocks b
                INNER JOIN paths p ON b.parent_id = p.id
            )
            SELECT * FROM paths
            "#,
            (),
        )
        .await
        .expect("Failed to create materialized view");

        // Step 3: Insert some initial data (this works fine with auto-commit)
        conn.execute(
            "INSERT INTO blocks (id, parent_id, content) VALUES ('root1', '', 'Root block')",
            (),
        )
        .await
        .expect("Initial insert should work");

        conn.execute(
            "INSERT INTO blocks (id, parent_id, content) VALUES ('child1', 'root1', 'Child block')",
            (),
        )
        .await
        .expect("Child insert should work");

        // Step 4: Start explicit transaction - THIS TRIGGERS THE BUG
        conn.execute("BEGIN IMMEDIATE TRANSACTION", ())
            .await
            .expect("BEGIN should work");

        // Step 5: Insert within transaction
        // The INSERT succeeds, but triggers IVM on the materialized view
        let result = conn
            .execute(
                "INSERT INTO blocks (id, parent_id, content) VALUES ('child2', 'root1', 'Another child')",
                (),
            )
            .await;

        // At this point, or during commit, Turso may panic with:
        // "Invalid join commit state" in JoinOperator::commit
        match result {
            Ok(_) => {
                // Try to commit - this is where the panic often happens
                let commit_result = conn.execute("COMMIT", ()).await;
                match commit_result {
                    Ok(_) => println!("Transaction committed successfully (bug not triggered)"),
                    Err(e) => panic!("COMMIT failed: {}", e),
                }
            }
            Err(e) => panic!("INSERT within transaction failed: {}", e),
        }
    }

    /// Simpler variant: multiple inserts in transaction with recursive view
    #[tokio::test]
    async fn test_join_operator_panic_batch_inserts() {
        let (_dir, db_path) = temp_db_path();

        let db = turso::Database::open(db_path.to_str().unwrap())
            .await
            .expect("Failed to open database");
        let conn = db.connect().expect("Failed to connect");

        // Create table
        conn.execute(
            "CREATE TABLE items (id TEXT PRIMARY KEY, parent_id TEXT, value TEXT)",
            (),
        )
        .await
        .unwrap();

        // Create recursive view with JOIN
        conn.execute(
            r#"
            CREATE MATERIALIZED VIEW item_tree AS
            WITH RECURSIVE tree AS (
                SELECT id, parent_id, value, 1 as depth
                FROM items WHERE parent_id IS NULL
                UNION ALL
                SELECT i.id, i.parent_id, i.value, t.depth + 1
                FROM items i
                INNER JOIN tree t ON i.parent_id = t.id
            )
            SELECT * FROM tree
            "#,
            (),
        )
        .await
        .unwrap();

        // Batch insert in transaction - triggers panic
        conn.execute("BEGIN IMMEDIATE", ()).await.unwrap();

        for i in 0..5 {
            let parent = if i == 0 {
                "NULL".to_string()
            } else {
                format!("'item_{}'", i - 1)
            };
            let sql = format!(
                "INSERT INTO items (id, parent_id, value) VALUES ('item_{}', {}, 'value_{}')",
                i, parent, i
            );
            conn.execute(&sql, ()).await.expect(&format!(
                "Insert {} failed - JoinOperator likely panicked",
                i
            ));
        }

        conn.execute("COMMIT", ()).await.expect("COMMIT failed");
    }

    /// Control test: same operations WITHOUT explicit transaction work fine
    #[tokio::test]
    async fn test_auto_commit_works_fine() {
        let (_dir, db_path) = temp_db_path();

        let db = turso::Database::open(db_path.to_str().unwrap())
            .await
            .expect("Failed to open database");
        let conn = db.connect().expect("Failed to connect");

        conn.execute(
            "CREATE TABLE items (id TEXT PRIMARY KEY, parent_id TEXT, value TEXT)",
            (),
        )
        .await
        .unwrap();

        conn.execute(
            r#"
            CREATE MATERIALIZED VIEW item_tree AS
            WITH RECURSIVE tree AS (
                SELECT id, parent_id, value, 1 as depth
                FROM items WHERE parent_id IS NULL
                UNION ALL
                SELECT i.id, i.parent_id, i.value, t.depth + 1
                FROM items i
                INNER JOIN tree t ON i.parent_id = t.id
            )
            SELECT * FROM tree
            "#,
            (),
        )
        .await
        .unwrap();

        // NO explicit transaction - each insert auto-commits
        for i in 0..5 {
            let parent = if i == 0 {
                "NULL".to_string()
            } else {
                format!("'item_{}'", i - 1)
            };
            let sql = format!(
                "INSERT INTO items (id, parent_id, value) VALUES ('item_{}', {}, 'value_{}')",
                i, parent, i
            );
            conn.execute(&sql, ())
                .await
                .expect("Auto-commit insert should work");
        }

        // Verify data
        let mut stmt = conn
            .prepare("SELECT COUNT(*) as cnt FROM item_tree")
            .await
            .unwrap();
        let rows = stmt.query(()).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let count: i64 = row.get(0).unwrap();
        assert_eq!(count, 5, "All items should be in the tree view");
    }
}
