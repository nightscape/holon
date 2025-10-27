use super::*;
use crate::storage::schema::{EntitySchema, FieldSchema, FieldType};
use crate::storage::turso::TursoBackend;
use crate::storage::types::StorageEntity;
use chrono::Utc;
use holon_api::Value;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::broadcast;

/// Create a test backend for unit testing.
///
/// This function creates an in-memory-like test setup using a temporary file.
/// Returns the TursoBackend ready for use.
async fn create_test_backend() -> TursoBackend {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    // Open database
    let db = TursoBackend::open_database(&db_path).expect("Failed to open database");

    // Create CDC broadcast channel
    let (cdc_tx, _cdc_rx) = broadcast::channel(1024);

    // Create backend (which internally spawns the actor)
    let (backend, _handle) = TursoBackend::new(db, cdc_tx).expect("Failed to create TursoBackend");

    // Keep the temp dir alive - this leaks memory but is acceptable for tests
    std::mem::forget(temp_dir);

    backend
}

// ============================================================================
// TESTS REMOVED - NOW COVERED BY PROPERTY-BASED TESTS
// ============================================================================
//
// The following test modules have been removed because they are now covered by
// comprehensive property-based tests in turso_pbt_tests.rs:
//
// - `cdc_tests` - Basic CDC insert/update/delete tracking
//   Now covered by PBT with CDC operations and verification
//
// - `incremental_view_maintenance_tests` - Materialized view operations
//   Now covered by PBT with CreateMaterializedView transitions
//
// - `turso_materialized_view_tests` - More materialized view tests
//   Now covered by PBT with random operation sequences on views
//
// - `turso_cdc_with_materialized_views_tests` - CDC integration with views
//   Now covered by PBT which tests CDC alongside view operations
//
// - `view_change_stream_tests` - View change notifications
//   Now covered by PBT with CreateViewStream transitions
//
// - `filter_building_tests` - All filter types (Eq, In, And, Or, IsNull, IsNotNull)
//   Now covered by PBT through random query generation
//
// The PBT suite runs 30 test cases with random sequences of 1-50 operations each,
// providing much more comprehensive coverage than the individual unit tests.
//
// ============================================================================
// REMAINING TESTS - EDGE CASES AND SPECIALIZED TESTING
// ============================================================================
//
// The tests below are kept because they test edge cases or specialized features
// that are not appropriate for property-based testing:
//
// - `sql_injection_tests` - SQL injection prevention via string escaping
// - `value_conversion_tests` - Value type conversion edge cases with property tests
// - `view_cdc_tests` - Basic view creation (non-materialized views)
//
//

#[cfg(test)]
mod sql_injection_tests {
    use super::*;

    #[tokio::test]
    async fn test_value_to_sql_param_escapes_single_quotes() {
        let backend = create_test_backend().await;
        let input = Value::String("O'Reilly".to_string());
        let escaped = backend.value_to_sql_param(&input);
        assert_eq!(escaped, "'O''Reilly'");
    }

    #[tokio::test]
    async fn test_value_to_sql_param_handles_multiple_quotes() {
        let backend = create_test_backend().await;
        let input = Value::String("It's a'test'case".to_string());
        let escaped = backend.value_to_sql_param(&input);
        assert_eq!(escaped, "'It''s a''test''case'");
    }

    #[tokio::test]
    async fn test_value_to_sql_param_handles_empty_string() {
        let backend = create_test_backend().await;
        let input = Value::String("".to_string());
        let escaped = backend.value_to_sql_param(&input);
        assert_eq!(escaped, "''");
    }
}

#[cfg(test)]
mod value_conversion_tests {
    use super::*;
    use proptest::prelude::*;

    #[tokio::test]
    async fn test_value_to_sql_param_null() {
        let backend = create_test_backend().await;
        assert_eq!(backend.value_to_sql_param(&Value::Null), "NULL");
    }

    #[tokio::test]
    async fn test_value_to_sql_param_datetime() {
        let backend = create_test_backend().await;
        let dt = Utc::now();
        let result = backend.value_to_sql_param(&Value::DateTime(dt.to_rfc3339()));
        assert!(result.starts_with('\''));
        assert!(result.ends_with('\''));
    }

    #[tokio::test]
    async fn test_value_to_sql_param_json() {
        let backend = create_test_backend().await;
        let json = serde_json::json!({"key": "value"});
        let result =
            backend.value_to_sql_param(&Value::Json(serde_json::to_string(&json).unwrap()));
        assert!(result.contains("key"));
        assert!(result.contains("value"));
    }

    proptest! {
        #[test]
        fn prop_integer_roundtrip(i in any::<i64>()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let backend = rt.block_on(create_test_backend());
            let value = Value::Integer(i);
            let sql_param = backend.value_to_sql_param(&value);
            assert_eq!(sql_param, i.to_string());
        }

        #[test]
        fn prop_boolean_conversion(b in any::<bool>()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let backend = rt.block_on(create_test_backend());
            let value = Value::Boolean(b);
            let sql_param = backend.value_to_sql_param(&value);
            assert_eq!(sql_param, if b { "1" } else { "0" });
        }

        #[test]
        fn prop_string_escaping_prevents_injection(s in "\\PC*") {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let backend = rt.block_on(create_test_backend());
            let value = Value::String(s.clone());
            let sql_param = backend.value_to_sql_param(&value);
            assert!(sql_param.starts_with('\''));
            assert!(sql_param.ends_with('\''));
            let quote_count_input = s.matches('\'').count();
            let quote_count_output = sql_param[1..sql_param.len()-1].matches("''").count();
            assert_eq!(quote_count_input, quote_count_output);
        }

    }
}

#[cfg(test)]
mod view_cdc_tests {
    use super::*;

    #[tokio::test]
    async fn test_view_cdc_basic_view_creation() {
        let mut backend = create_test_backend().await;

        let schema = EntitySchema {
            name: "view_base".to_string(),
            primary_key: "id".to_string(),
            fields: vec![
                FieldSchema {
                    name: "id".to_string(),
                    field_type: FieldType::String,
                    required: true,
                    indexed: true,
                },
                FieldSchema {
                    name: "value".to_string(),
                    field_type: FieldType::String,
                    required: true,
                    indexed: false,
                },
            ],
        };

        backend.create_entity(&schema).await.unwrap();

        let mut entity = StorageEntity::new();
        entity.insert("id".to_string(), Value::String("item-1".to_string()));
        entity.insert("value".to_string(), Value::String("test".to_string()));
        backend
            .insert(&holon_api::Schema::from_table_name("view_base"), entity)
            .await
            .unwrap();

        // Create view via DDL
        backend
            .execute_ddl("CREATE VIEW basic_view AS SELECT * FROM view_base")
            .await
            .unwrap();

        // Query via handle
        let rows = backend
            .handle()
            .query_positional("SELECT COUNT(*) as cnt FROM basic_view", vec![])
            .await
            .unwrap();

        assert!(!rows.is_empty());
        let count = rows[0]
            .get("cnt")
            .and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                _ => None,
            })
            .unwrap_or(0);
        assert_eq!(count, 1);
    }

    #[tokio::test]
    #[ignore = "Flaky test - view refresh timing issue"]
    async fn test_view_cdc_filtered_view_with_trigger() {
        let mut backend = create_test_backend().await;

        let schema = EntitySchema {
            name: "view_filter_base".to_string(),
            primary_key: "id".to_string(),
            fields: vec![
                FieldSchema {
                    name: "id".to_string(),
                    field_type: FieldType::String,
                    required: true,
                    indexed: true,
                },
                FieldSchema {
                    name: "category".to_string(),
                    field_type: FieldType::String,
                    required: true,
                    indexed: true,
                },
                FieldSchema {
                    name: "priority".to_string(),
                    field_type: FieldType::Integer,
                    required: true,
                    indexed: false,
                },
            ],
        };

        backend.create_entity(&schema).await.unwrap();

        for i in 1..=5 {
            let mut entity = StorageEntity::new();
            entity.insert("id".to_string(), Value::String(format!("item-{}", i)));
            entity.insert(
                "category".to_string(),
                Value::String(if i <= 3 { "bugs" } else { "features" }.to_string()),
            );
            entity.insert("priority".to_string(), Value::Integer(i));
            backend
                .insert(
                    &holon_api::Schema::from_table_name("view_filter_base"),
                    entity,
                )
                .await
                .unwrap();
        }

        backend
            .execute_ddl(
                "CREATE VIEW high_priority_bugs AS \
                 SELECT * FROM view_filter_base \
                 WHERE category = 'bugs' AND priority >= 3",
            )
            .await
            .unwrap();

        let rows = backend
            .handle()
            .query_positional("SELECT COUNT(*) as cnt FROM high_priority_bugs", vec![])
            .await
            .unwrap();
        let count = rows[0]
            .get("cnt")
            .and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                _ => None,
            })
            .unwrap_or(0);
        assert_eq!(count, 1);

        let mut updates = StorageEntity::new();
        updates.insert("priority".to_string(), Value::Integer(5));
        backend
            .update(
                &holon_api::Schema::from_table_name("view_filter_base"),
                "item-2",
                updates,
            )
            .await
            .unwrap();

        let rows = backend
            .handle()
            .query_positional("SELECT COUNT(*) as cnt FROM high_priority_bugs", vec![])
            .await
            .unwrap();
        let count = rows[0]
            .get("cnt")
            .and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                _ => None,
            })
            .unwrap_or(0);
        assert_eq!(count, 2);
    }
}

#[cfg(test)]
mod concurrent_ddl_dml_tests {
    use super::*;

    #[tokio::test]
    async fn test_concurrent_inserts_serialized_by_actor() {
        let backend = create_test_backend().await;

        let schema = EntitySchema {
            name: "concurrent_test".to_string(),
            primary_key: "id".to_string(),
            fields: vec![
                FieldSchema {
                    name: "id".to_string(),
                    field_type: FieldType::String,
                    required: true,
                    indexed: true,
                },
                FieldSchema {
                    name: "value".to_string(),
                    field_type: FieldType::String,
                    required: true,
                    indexed: false,
                },
            ],
        };
        backend.create_entity(&schema).await.unwrap();

        let handle = backend.handle();

        // Spawn 20 concurrent insert tasks
        let mut tasks = Vec::new();
        for i in 0..20 {
            let h = handle.clone();
            tasks.push(tokio::spawn(async move {
                let sql = format!(
                    "INSERT INTO concurrent_test (id, value) VALUES ('item-{}', 'val-{}')",
                    i, i
                );
                h.execute(&sql, vec![]).await
            }));
        }

        // All tasks must succeed
        for (i, task) in tasks.into_iter().enumerate() {
            task.await
                .unwrap_or_else(|e| panic!("Task {} panicked: {:?}", i, e))
                .unwrap_or_else(|e| panic!("Task {} insert failed: {:?}", i, e));
        }

        // Verify all 20 rows exist
        let rows = handle
            .query_positional("SELECT COUNT(*) as cnt FROM concurrent_test", vec![])
            .await
            .unwrap();
        let count = rows[0]
            .get("cnt")
            .and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                _ => None,
            })
            .unwrap_or(0);
        assert_eq!(count, 20, "All concurrent inserts must succeed");
    }

    #[tokio::test]
    async fn test_dml_before_table_created_fails() {
        let backend = create_test_backend().await;
        let handle = backend.handle();

        // Insert into non-existent table should fail
        let result = handle
            .execute(
                "INSERT INTO nonexistent_table (id, value) VALUES ('a', 'b')",
                vec![],
            )
            .await;

        assert!(result.is_err(), "INSERT into non-existent table must fail");
    }

    #[tokio::test]
    async fn test_ddl_then_dml_sequence() {
        let backend = create_test_backend().await;
        let handle = backend.handle();

        // Create table via DDL
        backend
            .execute_ddl("CREATE TABLE IF NOT EXISTS seq_test (id TEXT PRIMARY KEY, data TEXT)")
            .await
            .unwrap();

        // Immediately insert data
        handle
            .execute(
                "INSERT INTO seq_test (id, data) VALUES ('row-1', 'hello')",
                vec![],
            )
            .await
            .unwrap();

        // Insert second row to verify multiple DML ops after DDL
        handle
            .execute(
                "INSERT INTO seq_test (id, data) VALUES ('row-2', 'world')",
                vec![],
            )
            .await
            .unwrap();

        // Verify via count
        let rows = handle
            .query_positional("SELECT COUNT(*) as cnt FROM seq_test", vec![])
            .await
            .unwrap();
        let count = rows[0]
            .get("cnt")
            .and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                _ => None,
            })
            .unwrap_or(0);
        assert_eq!(count, 2, "Both inserts after DDL must succeed");
    }

    #[tokio::test]
    async fn test_concurrent_ddl_and_dml_via_schema_registry() {
        let mut backend = create_test_backend().await;

        // Create base table first
        let schema = EntitySchema {
            name: "base_concurrent".to_string(),
            primary_key: "id".to_string(),
            fields: vec![
                FieldSchema {
                    name: "id".to_string(),
                    field_type: FieldType::String,
                    required: true,
                    indexed: true,
                },
                FieldSchema {
                    name: "parent_id".to_string(),
                    field_type: FieldType::String,
                    required: false,
                    indexed: true,
                },
                FieldSchema {
                    name: "content".to_string(),
                    field_type: FieldType::String,
                    required: true,
                    indexed: false,
                },
            ],
        };
        backend.create_entity(&schema).await.unwrap();

        let handle = backend.handle();

        // Spawn concurrent: DDL (create index) and DML (inserts) at the same time
        let h1 = handle.clone();
        let ddl_task = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            h1.execute_ddl("CREATE INDEX IF NOT EXISTS idx_bc_parent ON base_concurrent(parent_id)")
                .await
        });

        let mut dml_tasks = Vec::new();
        for i in 0..10 {
            let h = handle.clone();
            dml_tasks.push(tokio::spawn(async move {
                let sql = format!(
                    "INSERT INTO base_concurrent (id, parent_id, content) VALUES ('item-{}', 'root', 'content-{}')",
                    i, i
                );
                h.execute(&sql, vec![]).await
            }));
        }

        // DDL must succeed
        ddl_task.await.unwrap().unwrap();

        // All DML must succeed
        for task in dml_tasks {
            task.await.unwrap().unwrap();
        }

        // Verify all data
        let rows = handle
            .query_positional("SELECT COUNT(*) as cnt FROM base_concurrent", vec![])
            .await
            .unwrap();
        let count = rows[0]
            .get("cnt")
            .and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                _ => None,
            })
            .unwrap_or(0);
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_concurrent_updates_on_same_row() {
        let mut backend = create_test_backend().await;

        let schema = EntitySchema {
            name: "update_race".to_string(),
            primary_key: "id".to_string(),
            fields: vec![
                FieldSchema {
                    name: "id".to_string(),
                    field_type: FieldType::String,
                    required: true,
                    indexed: true,
                },
                FieldSchema {
                    name: "counter".to_string(),
                    field_type: FieldType::Integer,
                    required: true,
                    indexed: false,
                },
            ],
        };
        backend.create_entity(&schema).await.unwrap();

        let handle = backend.handle();

        // Insert initial row
        handle
            .execute(
                "INSERT INTO update_race (id, counter) VALUES ('row-1', 0)",
                vec![],
            )
            .await
            .unwrap();

        // Spawn 50 concurrent increments
        // Since actor serializes, final value should be 50
        let mut tasks = Vec::new();
        for _ in 0..50 {
            let h = handle.clone();
            tasks.push(tokio::spawn(async move {
                h.execute(
                    "UPDATE update_race SET counter = counter + 1 WHERE id = 'row-1'",
                    vec![],
                )
                .await
            }));
        }

        for task in tasks {
            task.await.unwrap().unwrap();
        }

        let rows = handle
            .query_positional("SELECT counter FROM update_race WHERE id = 'row-1'", vec![])
            .await
            .unwrap();
        let counter = rows[0]
            .get("counter")
            .and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                _ => None,
            })
            .unwrap_or(-1);
        assert_eq!(
            counter, 50,
            "Actor serialization must ensure all 50 increments apply"
        );
    }

    #[tokio::test]
    async fn test_materialized_view_after_concurrent_writes() {
        let backend = create_test_backend().await;
        let handle = backend.handle();

        // Create base table
        backend
            .execute_ddl(
                "CREATE TABLE IF NOT EXISTS mv_base (
                    id TEXT PRIMARY KEY,
                    parent_id TEXT,
                    content TEXT NOT NULL
                )",
            )
            .await
            .unwrap();

        // Create materialized view
        backend
            .execute_ddl(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_view AS
                 SELECT id, parent_id, content FROM mv_base WHERE parent_id = 'root'",
            )
            .await
            .unwrap();

        // Concurrent inserts with mix of matching/non-matching rows
        let mut tasks = Vec::new();
        for i in 0..20 {
            let h = handle.clone();
            let parent = if i % 2 == 0 { "root" } else { "other" };
            let parent_owned = parent.to_string();
            tasks.push(tokio::spawn(async move {
                let sql = format!(
                    "INSERT INTO mv_base (id, parent_id, content) VALUES ('item-{}', '{}', 'data-{}')",
                    i, parent_owned, i
                );
                h.execute(&sql, vec![]).await
            }));
        }

        for task in tasks {
            task.await.unwrap().unwrap();
        }

        // Materialized view should reflect only root-parented rows
        let rows = handle
            .query_positional("SELECT COUNT(*) as cnt FROM mv_view", vec![])
            .await
            .unwrap();
        let count = rows[0]
            .get("cnt")
            .and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                _ => None,
            })
            .unwrap_or(0);
        assert_eq!(
            count, 10,
            "Materialized view must reflect all 10 root-parented rows after concurrent writes"
        );
    }
}
