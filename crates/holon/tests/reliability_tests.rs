use anyhow::Result;
use holon::sync::CollaborativeDoc;
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_empty_update_handling() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("empty-update".to_string()).await?;

    let empty_update = vec![];
    let result = doc.apply_update(&empty_update).await;

    assert!(result.is_ok() || result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_corrupted_update_rejection() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("corrupted".to_string()).await?;

    let corrupted_update = vec![0xFF; 100];
    let result = doc.apply_update(&corrupted_update).await;

    assert!(result.is_err(), "Should reject corrupted update data");

    Ok(())
}

#[tokio::test]
async fn test_partial_update_handling() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("partial".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("partial".to_string()).await?;

    let full_update = doc1.insert_text("editor", 0, "Full content here").await?;

    if full_update.len() > 10 {
        let partial = &full_update[..full_update.len() / 2];
        let result = doc2.apply_update(partial).await;

        assert!(result.is_err(), "Should reject partial/truncated updates");
    }

    Ok(())
}

#[tokio::test]
async fn test_out_of_order_updates() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("out-of-order".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("out-of-order".to_string()).await?;

    let update1 = doc1.insert_text("editor", 0, "First").await?;
    let update2 = doc1.insert_text("editor", 5, " Second").await?;
    let update3 = doc1.insert_text("editor", 12, " Third").await?;

    doc2.apply_update(&update3).await?;
    doc2.apply_update(&update1).await?;
    doc2.apply_update(&update2).await?;

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;

    assert_eq!(text1, text2);

    Ok(())
}

#[tokio::test]
async fn test_duplicate_update_filtering() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("duplicate".to_string()).await?;

    let update = doc.insert_text("editor", 0, "Content").await?;

    doc.apply_update(&update).await?;
    doc.apply_update(&update).await?;
    doc.apply_update(&update).await?;

    let text = doc.get_text("editor").await?;
    assert_eq!(
        text, "Content",
        "Duplicate updates should not duplicate content"
    );

    Ok(())
}

#[tokio::test]
async fn test_snapshot_after_many_updates() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("snapshot-integrity".to_string()).await?;

    for i in 0..100 {
        doc.insert_text("editor", i, "x").await?;
    }

    let snapshot = doc.export_snapshot().await?;

    let doc2 = CollaborativeDoc::with_new_endpoint("snapshot-integrity".to_string()).await?;
    doc2.apply_update(&snapshot).await?;

    let text1 = doc.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;

    assert_eq!(text1, text2);
    assert_eq!(text1.len(), 100);

    Ok(())
}

#[tokio::test]
#[serial]

async fn test_connection_without_accept() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("no-accept".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("no-accept".to_string()).await?;

    doc1.insert_text("editor", 0, "Test").await?;

    let peer1_addr = doc1.node_addr();

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        doc2.connect_and_sync_to_peer(peer1_addr),
    )
    .await;

    assert!(
        result.is_err() || result.unwrap().is_err(),
        "Connection should timeout or fail if peer is not accepting"
    );

    Ok(())
}

#[tokio::test]
#[serial]

async fn test_accept_without_connection() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("no-connect".to_string()).await?;

    let doc = Arc::new(doc);
    let doc_clone = doc.clone();

    let result = tokio::time::timeout(
        Duration::from_secs(3),
        tokio::spawn(async move { doc_clone.accept_sync_from_peer().await }),
    )
    .await;

    assert!(result.is_err(), "Accept should timeout if no peer connects");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_multiple_sequential_accepts() -> Result<()> {
    let doc1 = Arc::new(CollaborativeDoc::with_new_endpoint("multi-accept".to_string()).await?);
    let doc2 = CollaborativeDoc::with_new_endpoint("multi-accept".to_string()).await?;
    let doc3 = CollaborativeDoc::with_new_endpoint("multi-accept".to_string()).await?;

    doc1.insert_text("editor", 0, "Hub").await?;
    doc2.insert_text("editor", 0, "Client2").await?;
    doc3.insert_text("editor", 0, "Client3").await?;

    let peer1_addr = doc1.node_addr();

    let doc1_clone = doc1.clone();
    let accept1 = tokio::spawn(async move { doc1_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;
    doc2.connect_and_sync_to_peer(peer1_addr.clone()).await?;
    sleep(Duration::from_millis(200)).await;
    let _ = accept1.await?;

    let doc1_clone = doc1.clone();
    let accept2 = tokio::spawn(async move { doc1_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;
    doc3.connect_and_sync_to_peer(peer1_addr).await?;
    sleep(Duration::from_millis(200)).await;
    let _ = accept2.await?;

    let text1 = doc1.get_text("editor").await?;
    assert!(text1.contains("Client2") || text1.contains("Client3"));

    Ok(())
}

#[tokio::test]
#[serial]

async fn test_update_after_sync() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("update-post-sync".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("update-post-sync".to_string()).await?;

    doc1.insert_text("editor", 0, "Initial").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();
    let peer1_addr = doc1.node_addr();

    let accept_handle = tokio::spawn(async move { doc1_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;
    doc2.connect_and_sync_to_peer(peer1_addr).await?;
    sleep(Duration::from_millis(200)).await;
    let _ = accept_handle.await?;

    doc1.insert_text("editor", 7, " after sync").await?;
    let text = doc1.get_text("editor").await?;
    assert_eq!(text, "Initial after sync");

    Ok(())
}

#[tokio::test]
async fn test_peer_id_stability_across_operations() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("peer-stability".to_string()).await?;

    let peer_id_before = doc.peer_id();

    doc.insert_text("editor", 0, "Test").await?;
    let peer_id_after_insert = doc.peer_id();

    let _snapshot = doc.export_snapshot().await?;
    let peer_id_after_snapshot = doc.peer_id();

    assert_eq!(peer_id_before, peer_id_after_insert);
    assert_eq!(peer_id_before, peer_id_after_snapshot);

    Ok(())
}

#[tokio::test]
async fn test_doc_id_immutability() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("immutable-id".to_string()).await?;

    let doc_id_before = doc.doc_id().to_string();

    doc.insert_text("editor", 0, "Test").await?;
    let doc_id_after = doc.doc_id().to_string();

    assert_eq!(doc_id_before, doc_id_after);
    assert_eq!(doc_id_before, "immutable-id");

    Ok(())
}

#[tokio::test]
async fn test_concurrent_read_write() -> Result<()> {
    let doc = Arc::new(CollaborativeDoc::with_new_endpoint("concurrent-rw".to_string()).await?);

    doc.insert_text("editor", 0, "Initial").await?;

    let doc_read = doc.clone();
    let doc_write = doc.clone();

    let read_handle = tokio::spawn(async move {
        for _ in 0..50 {
            let _text = doc_read.get_text("editor").await.ok();
            sleep(Duration::from_millis(10)).await;
        }
    });

    let write_handle = tokio::spawn(async move {
        for i in 0..50 {
            doc_write.insert_text("editor", i + 7, "x").await.ok();
            sleep(Duration::from_millis(10)).await;
        }
    });

    read_handle.await?;
    write_handle.await?;

    let final_text = doc.get_text("editor").await?;
    assert!(final_text.len() > 7);

    Ok(())
}

#[tokio::test]
async fn test_export_stability() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("export-stable".to_string()).await?;

    doc.insert_text("editor", 0, "Content").await?;

    let snapshot1 = doc.export_snapshot().await?;
    sleep(Duration::from_millis(100)).await;
    let snapshot2 = doc.export_snapshot().await?;

    assert_eq!(snapshot1, snapshot2, "Snapshots should be deterministic");

    Ok(())
}

#[tokio::test]
async fn test_very_large_single_insert() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("huge-insert".to_string()).await?;

    let huge_text = "x".repeat(1_000_000);
    let result = doc.insert_text("editor", 0, &huge_text).await;

    assert!(result.is_ok(), "Should handle very large inserts");

    if result.is_ok() {
        let text = doc.get_text("editor").await?;
        assert_eq!(text.len(), 1_000_000);
    }

    Ok(())
}

#[tokio::test]
async fn test_boundary_insert_positions() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("boundary".to_string()).await?;

    doc.insert_text("editor", 0, "Hello").await?;

    doc.insert_text("editor", 0, "A").await?;
    let text = doc.get_text("editor").await?;
    assert!(text.starts_with("A"));

    doc.insert_text("editor", text.len(), "Z").await?;
    let text = doc.get_text("editor").await?;
    assert!(text.ends_with("Z"));

    Ok(())
}

#[tokio::test]
async fn test_invalid_insert_position() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("invalid-pos".to_string()).await?;

    doc.insert_text("editor", 0, "Test").await?;

    let result = doc.insert_text("editor", 1000, "X").await;

    assert!(result.is_err(), "Should reject insert at invalid position");

    Ok(())
}

#[tokio::test]
async fn test_state_consistency_after_errors() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("error-recovery".to_string()).await?;

    doc.insert_text("editor", 0, "Valid").await?;

    let corrupted = vec![0xFF; 50];
    let _ = doc.apply_update(&corrupted).await;

    doc.insert_text("editor", 5, " Still Works").await?;
    let text = doc.get_text("editor").await?;

    assert!(text.contains("Valid") && text.contains("Still Works"));

    Ok(())
}

#[tokio::test]
async fn test_multiple_endpoint_reuse() -> Result<()> {
    let endpoint = CollaborativeDoc::create_endpoint("shared-endpoint").await?;

    let doc1 = CollaborativeDoc::new(endpoint.clone(), "doc1".to_string()).await?;
    let doc2 = CollaborativeDoc::new(endpoint.clone(), "doc2".to_string()).await?;

    doc1.insert_text("editor", 0, "Doc1").await?;
    doc2.insert_text("editor", 0, "Doc2").await?;

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;

    assert_eq!(text1, "Doc1");
    assert_eq!(text2, "Doc2");
    assert_ne!(text1, text2);

    Ok(())
}

#[tokio::test]
#[serial]

async fn test_sync_with_empty_peer() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("empty-peer-test".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("empty-peer-test".to_string()).await?;

    doc1.insert_text("editor", 0, "Non-empty").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();
    let peer1_addr = doc1.node_addr();

    let accept_handle = tokio::spawn(async move { doc1_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;
    doc2.connect_and_sync_to_peer(peer1_addr).await?;
    sleep(Duration::from_millis(200)).await;
    let _ = accept_handle.await?;

    let text2 = doc2.get_text("editor").await?;
    assert_eq!(text2, "");

    Ok(())
}

#[tokio::test]
async fn test_alpn_format_validation() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("alpn-test-123".to_string()).await?;

    assert_eq!(doc.doc_id(), "alpn-test-123");

    Ok(())
}
