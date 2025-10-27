//! Minimal test to verify if turso supports file-based storage
//!
//! This script tests whether the turso fork actually supports file-based storage
//! by attempting to:
//! 1. Create a database file
//! 2. Write data to it
//! 3. Close and reopen the database
//! 4. Verify the data persists

use std::sync::Arc;
use turso_core::{Database, DatabaseOpts, UnixIO};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_path = "/tmp/turso_test.db";

    // Clean up any existing test file
    let _ = std::fs::remove_file(test_path);

    println!("Testing file-based storage...\n");

    // Test 1: Create database with UnixIO for file-based storage
    println!("1. Creating database with file path and UnixIO...");
    let io = Arc::new(UnixIO::new()?);
    let db = Database::open_file_with_flags(
        io.clone(),
        test_path,
        turso_core::OpenFlags::default(),
        DatabaseOpts::default().with_views(true),
        None,
    )?;
    println!("   ✓ Database created successfully");

    // Create a table and insert data
    let conn = db.connect()?;
    let turso_conn = turso::Connection::create(conn);

    println!("2. Creating table and inserting data...");
    turso_conn
        .execute("CREATE TABLE test (id TEXT PRIMARY KEY, value TEXT)", ())
        .await?;

    turso_conn
        .execute(
            "INSERT INTO test (id, value) VALUES ('test1', 'hello world')",
            (),
        )
        .await?;
    println!("   ✓ Data inserted");

    drop(turso_conn);
    drop(db);

    // Test 2: Reopen the database
    println!("3. Reopening database...");
    let io2 = Arc::new(UnixIO::new()?);
    let db2 = Database::open_file_with_flags(
        io2,
        test_path,
        turso_core::OpenFlags::default(),
        DatabaseOpts::default().with_views(true),
        None,
    )?;
    let conn2 = db2.connect()?;
    let turso_conn2 = turso::Connection::create(conn2);

    println!("4. Querying data...");
    let mut stmt = turso_conn2
        .prepare("SELECT * FROM test WHERE id = ?")
        .await?;
    let mut rows = stmt
        .query([turso::Value::Text("test1".to_string())])
        .await?;

    if let Some(row) = rows.next().await? {
        let value = row.get_value(1)?;
        match value {
            turso::Value::Text(s) => {
                println!("   ✓ Data retrieved: {}", s);
                assert_eq!(s, "hello world", "Data should persist");
                println!("\n✅ FILE-BASED STORAGE WORKS!");
            }
            _ => {
                println!("   ✗ Unexpected value type");
                return Err("Unexpected value type".into());
            }
        }
    } else {
        println!("   ✗ No data found");
        return Err("Data not found after reopening".into());
    }

    // Clean up
    std::fs::remove_file(test_path)?;

    Ok(())
}
