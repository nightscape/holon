#[cfg(test)]
mod tests {
    use crate::storage::test_helpers::create_test_backend;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_cdc_cross_connection() -> anyhow::Result<()> {
        // 1. Create Backend with DatabaseActor
        let backend = create_test_backend().await;

        // 2. Create a table
        backend
            .handle()
            .execute(
                "CREATE TABLE test_table (id TEXT PRIMARY KEY, value TEXT)",
                vec![],
            )
            .await?;

        // 3. Create a materialized view to watch
        backend
            .handle()
            .execute_ddl("CREATE MATERIALIZED VIEW test_view AS SELECT * FROM test_table")
            .await?;

        // 4. Subscribe to CDC changes via the backend's cdc_broadcast
        let mut rx = backend.cdc_broadcast().subscribe();

        // 5. Insert data using the backend's db_handle
        backend
            .handle()
            .execute(
                "INSERT INTO test_table (id, value) VALUES ('1', 'hello')",
                vec![],
            )
            .await?;

        // 6. Wait for event from the CDC broadcast channel
        let event = timeout(Duration::from_secs(2), rx.recv()).await;

        match event {
            Ok(Ok(batch)) => {
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
            Ok(Err(e)) => panic!("Channel error: {}", e),
            Err(_) => {
                panic!("Timed out waiting for CDC event - Cross-connection notification failed!")
            }
        }

        Ok(())
    }
}
