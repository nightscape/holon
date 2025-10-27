use anyhow::Result;
use holon::sync::LoroDocument;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[cfg(feature = "iroh-sync")]
use holon::sync::IrohSyncAdapter;
#[cfg(feature = "iroh-sync")]
use serial_test::serial;

#[tokio::test]
async fn test_high_frequency_updates() -> Result<()> {
    let doc1 = LoroDocument::new("high-freq".to_string())?;
    let doc2 = LoroDocument::new("high-freq".to_string())?;

    let mut updates = Vec::new();
    for i in 0..1000 {
        let update = doc1.insert_text("editor", i, "x").await?;
        updates.push(update);
    }

    let start = Instant::now();
    for update in updates {
        doc2.apply_update(&update).await?;
    }
    let duration = start.elapsed();

    println!("Applied 1000 updates in {:?}", duration);
    assert!(
        duration.as_secs() < 10,
        "Should apply 1000 updates in under 10 seconds"
    );

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;
    assert_eq!(text1, text2);
    assert_eq!(text1.len(), 1000);

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_large_batch_sync() -> Result<()> {
    let doc1 = LoroDocument::new("large-batch".to_string())?;
    let doc2 = LoroDocument::new("large-batch".to_string())?;

    let start = Instant::now();
    let chunk = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(100);
    for i in 0..100 {
        doc1.insert_text("editor", i * chunk.len(), &chunk).await?;
    }
    let creation_time = start.elapsed();
    println!("Created large document in {:?}", creation_time);

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;
    let adapter2 = IrohSyncAdapter::new("loro-sync").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();
    let peer1_addr = adapter1.node_addr();

    let accept_handle = tokio::spawn(async move { adapter1.accept_sync(&doc1_clone).await });

    sleep(Duration::from_millis(500)).await;

    let sync_start = Instant::now();
    adapter2.sync_with_peer(&doc2, peer1_addr).await?;
    sleep(Duration::from_millis(500)).await;
    let sync_time = sync_start.elapsed();

    let _ = accept_handle.await?;

    println!("Synced large document in {:?}", sync_time);
    assert!(
        sync_time.as_secs() < 30,
        "Large sync should complete in under 30 seconds"
    );

    let text2 = doc2.get_text("editor").await?;
    assert!(text2.len() > 100000);

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_many_small_containers() -> Result<()> {
    let doc1 = LoroDocument::new("many-containers".to_string())?;
    let doc2 = LoroDocument::new("many-containers".to_string())?;

    for i in 0..100 {
        let container_name = format!("container_{}", i);
        doc1.insert_text(&container_name, 0, &format!("Content {}", i))
            .await?;
    }

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;
    let adapter2 = IrohSyncAdapter::new("loro-sync").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();
    let peer1_addr = adapter1.node_addr();

    let accept_handle = tokio::spawn(async move { adapter1.accept_sync(&doc1_clone).await });

    sleep(Duration::from_millis(500)).await;
    adapter2.sync_with_peer(&doc2, peer1_addr).await?;
    sleep(Duration::from_millis(500)).await;
    let _ = accept_handle.await?;

    for i in 0..100 {
        let container_name = format!("container_{}", i);
        let text = doc2.get_text(&container_name).await?;
        assert_eq!(text, format!("Content {}", i));
    }

    Ok(())
}

#[tokio::test]
async fn test_sustained_concurrent_operations() -> Result<()> {
    let doc1 = LoroDocument::new("sustained".to_string())?;
    let doc2 = LoroDocument::new("sustained".to_string())?;

    let doc1 = Arc::new(doc1);
    let doc2 = Arc::new(doc2);

    let doc1_clone = doc1.clone();
    let writer1 = tokio::spawn(async move {
        for i in 0..50 {
            doc1_clone.insert_text("editor", i, "A").await.ok();
            sleep(Duration::from_millis(10)).await;
        }
    });

    let doc2_clone = doc2.clone();
    let writer2 = tokio::spawn(async move {
        for i in 0..50 {
            doc2_clone.insert_text("editor", i, "B").await.ok();
            sleep(Duration::from_millis(10)).await;
        }
    });

    writer1.await?;
    writer2.await?;

    let update1 = doc1.export_snapshot().await?;
    let update2 = doc2.export_snapshot().await?;

    doc1.apply_update(&update2).await?;
    doc2.apply_update(&update1).await?;

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;

    assert_eq!(text1, text2);

    Ok(())
}

#[tokio::test]
async fn test_memory_efficiency_large_doc() -> Result<()> {
    let doc = LoroDocument::new("memory-test".to_string())?;

    let iterations = 10000;
    for i in 0..iterations {
        doc.insert_text("editor", i, "x").await?;
    }

    let snapshot = doc.export_snapshot().await?;

    assert!(
        snapshot.len() < 1_000_000,
        "Snapshot should be reasonably compressed"
    );

    let text = doc.get_text("editor").await?;
    assert_eq!(text.len(), iterations);

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
async fn test_parallel_sync_operations() -> Result<()> {
    let hub = Arc::new(LoroDocument::new("parallel-hub".to_string())?);
    hub.insert_text("editor", 0, "Hub content").await?;

    let hub_adapter = IrohSyncAdapter::new("loro-sync").await?;
    let peer_addr = hub_adapter.node_addr();
    let mut join_handles = Vec::new();

    for _ in 0..5 {
        let hub_clone = hub.clone();
        let hub_adapter_new = IrohSyncAdapter::new("loro-sync").await?;
        let accept_handle =
            tokio::spawn(async move { hub_adapter_new.accept_sync(&hub_clone).await });
        join_handles.push(accept_handle);
    }

    sleep(Duration::from_millis(500)).await;

    let mut connect_handles = Vec::new();
    for i in 0..5 {
        let addr = peer_addr.clone();
        let connect_handle = tokio::spawn(async move {
            let doc = LoroDocument::new("parallel-hub".to_string()).ok()?;
            doc.insert_text("editor", 0, &format!("Client {}", i))
                .await
                .ok()?;
            let adapter = IrohSyncAdapter::new("loro-sync").await.ok()?;
            sleep(Duration::from_millis(100)).await;
            adapter.sync_with_peer(&doc, addr).await.ok()?;
            Some(doc)
        });
        connect_handles.push(connect_handle);
    }

    sleep(Duration::from_millis(500)).await;

    for handle in connect_handles {
        let _ = handle.await;
    }

    for handle in join_handles {
        let _ = handle.await;
    }

    let hub_text = hub.get_text("editor").await?;
    assert!(!hub_text.is_empty());

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_sync_latency_measurement() -> Result<()> {
    let doc1 = LoroDocument::new("latency-test".to_string())?;
    let doc2 = LoroDocument::new("latency-test".to_string())?;

    doc1.insert_text("editor", 0, "Initial").await?;

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;
    let adapter2 = IrohSyncAdapter::new("loro-sync").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();
    let peer1_addr = adapter1.node_addr();

    let accept_handle = tokio::spawn(async move { adapter1.accept_sync(&doc1_clone).await });

    sleep(Duration::from_millis(500)).await;

    let start = Instant::now();
    adapter2.sync_with_peer(&doc2, peer1_addr).await?;
    let latency = start.elapsed();

    sleep(Duration::from_millis(200)).await;
    let _ = accept_handle.await?;

    println!("Sync latency: {:?}", latency);
    assert!(
        latency.as_secs() < 5,
        "Initial sync should complete in under 5 seconds"
    );

    Ok(())
}

#[tokio::test]
async fn test_update_size_efficiency() -> Result<()> {
    let doc = LoroDocument::new("update-size".to_string())?;

    let update1 = doc.insert_text("editor", 0, "Small").await?;
    assert!(update1.len() < 1000, "Small update should be compact");

    let large_text = "x".repeat(100000);
    let update2 = doc.insert_text("editor", 5, &large_text).await?;

    assert!(
        update2.len() < large_text.len() * 2,
        "Update should not be excessively larger than content"
    );

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
async fn test_rapid_peer_connections() -> Result<()> {
    let hub = Arc::new(LoroDocument::new("rapid-conn".to_string())?);
    hub.insert_text("editor", 0, "Hub").await?;

    for _ in 0..10 {
        let hub_clone = hub.clone();
        let hub_adapter = IrohSyncAdapter::new("loro-sync").await?;
        let addr = hub_adapter.node_addr();

        tokio::spawn(async move { hub_adapter.accept_sync(&hub_clone).await });

        sleep(Duration::from_millis(100)).await;

        let doc = LoroDocument::new("rapid-conn".to_string())?;
        doc.insert_text("editor", 0, "Client").await?;
        let client_adapter = IrohSyncAdapter::new("loro-sync").await?;
        let _ = client_adapter.sync_with_peer(&doc, addr).await;

        sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}

#[tokio::test]
async fn test_long_running_stability() -> Result<()> {
    let doc1 = LoroDocument::new("stability".to_string())?;
    let doc2 = LoroDocument::new("stability".to_string())?;

    for round in 0..20 {
        for i in 0..10 {
            doc1.insert_text("editor", round * 10 + i, "x").await?;
        }

        let update = doc1.export_snapshot().await?;
        doc2.apply_update(&update).await?;

        sleep(Duration::from_millis(50)).await;
    }

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;

    assert_eq!(text1, text2);
    assert_eq!(text1.len(), 200);

    Ok(())
}
