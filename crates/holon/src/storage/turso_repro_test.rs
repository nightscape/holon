#[cfg(test)]
mod tests {
    use crate::storage::turso::TursoBackend;
    use holon_api::Value;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::time::timeout;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_cdc_cross_connection() -> anyhow::Result<()> {
        // 1. Create Backend
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let backend = TursoBackend::new(&db_path).await?;

        // 2. Create a table
        let create_sql = "CREATE TABLE test_table (id TEXT PRIMARY KEY, value TEXT)";
        backend.execute_sql(create_sql, HashMap::new()).await?;

        // 3. Create a materialized view to watch
        let view_sql = "CREATE MATERIALIZED VIEW test_view AS SELECT * FROM test_table";
        backend.execute_sql(view_sql, HashMap::new()).await?;

        // 4. Set up CDC on a separate connection (row_changes creates a new conn)
        let (_cdc_conn, mut stream) = backend.row_changes()?;

        // 5. Insert data using the backend's main mechanism (which creates a NEW connection)
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String("1".to_string()));
        params.insert("value".to_string(), Value::String("hello".to_string()));

        let insert_sql = "INSERT INTO test_table (id, value) VALUES ($id, $value)";
        backend.execute_sql(insert_sql, params).await?;

        // 6. Wait for event
        let event = timeout(Duration::from_secs(2), stream.next()).await;

        match event {
            Ok(Some(batch)) => {
                println!(
                    "Received batch: {} changes, relation={}",
                    batch.inner.items.len(),
                    batch.metadata.relation_name
                );
                assert!(
                    !batch.inner.items.is_empty(),
                    "Batch should contain at least one change"
                );
                // Check first change in batch
                assert_eq!(batch.inner.items[0].relation_name, "test_view");
                // Verify metadata
                assert_eq!(batch.metadata.relation_name, "test_view");
            }
            Ok(None) => panic!("Stream closed unexpectedly"),
            Err(_) => {
                panic!("Timed out waiting for CDC event - Cross-connection notification failed!")
            }
        }

        Ok(())
    }
}
