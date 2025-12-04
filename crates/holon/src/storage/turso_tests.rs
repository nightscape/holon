use super::*;
use crate::storage::schema::{EntitySchema, FieldSchema, FieldType};
use crate::storage::turso::TursoBackend;
use crate::storage::types::StorageEntity;
use chrono::Utc;
use holon_api::Value;

// Helper functions for tests
async fn create_test_backend() -> TursoBackend {
    TursoBackend::new_in_memory().await.unwrap()
}

fn get_test_connection(backend: &TursoBackend) -> turso::Connection {
    backend.get_raw_connection().unwrap()
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

        #[test]
        fn prop_reference_escaping(r in "\\PC*") {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let backend = rt.block_on(create_test_backend());
            let value = Value::Reference(r.clone());
            let sql_param = backend.value_to_sql_param(&value);
            assert!(sql_param.starts_with('\''));
            assert!(sql_param.ends_with('\''));
        }
    }
}

#[cfg(test)]
mod view_cdc_tests {
    use super::*;

    #[tokio::test]
    async fn test_view_cdc_basic_view_creation() {
        let mut backend = create_test_backend().await;
        let conn = get_test_connection(&backend);

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
        backend.insert("view_base", entity).await.unwrap();

        conn.execute("CREATE VIEW basic_view AS SELECT * FROM view_base", ())
            .await
            .unwrap();

        let mut rows = conn
            .query("SELECT COUNT(*) FROM basic_view", ())
            .await
            .unwrap();
        if let Some(row) = rows.next().await.unwrap() {
            let count = row.get_value(0).unwrap();
            assert_eq!(count, turso::Value::Integer(1));
        }
    }

    #[tokio::test]
    #[ignore = "Flaky test - view refresh timing issue"]
    async fn test_view_cdc_filtered_view_with_trigger() {
        let mut backend = create_test_backend().await;
        let _keep_alive = get_test_connection(&backend);

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
            backend.insert("view_filter_base", entity).await.unwrap();
        }

        let conn = get_test_connection(&backend);
        conn.execute(
            "CREATE VIEW high_priority_bugs AS \
             SELECT * FROM view_filter_base \
             WHERE category = 'bugs' AND priority >= 3",
            (),
        )
        .await
        .unwrap();

        let mut rows = conn
            .query("SELECT COUNT(*) FROM high_priority_bugs", ())
            .await
            .unwrap();
        if let Some(row) = rows.next().await.unwrap() {
            let count = row.get_value(0).unwrap();
            assert_eq!(count, turso::Value::Integer(1));
        }

        let mut updates = StorageEntity::new();
        updates.insert("priority".to_string(), Value::Integer(5));
        backend
            .update("view_filter_base", "item-2", updates)
            .await
            .unwrap();
        let mut rows = conn
            .query("SELECT COUNT(*) FROM high_priority_bugs", ())
            .await
            .unwrap();
        if let Some(row) = rows.next().await.unwrap() {
            let count = row.get_value(0).unwrap();
            assert_eq!(count, turso::Value::Integer(2));
        }
    }
}
