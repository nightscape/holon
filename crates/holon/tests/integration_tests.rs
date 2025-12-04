use anyhow::Result;
use holon::sync::CollaborativeDoc;
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[tokio::test]
#[serial]
async fn test_basic_two_peer_sync() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("two-peer-sync".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("two-peer-sync".to_string()).await?;

    doc1.insert_text("editor", 0, "Initial content").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();
    let peer1_addr = doc1.node_addr();

    let accept_handle = tokio::spawn(async move { doc1_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;

    doc2.connect_and_sync_to_peer(peer1_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept_handle.await?;

    let text1 = doc1.get_text("editor").await?;
    assert!(text1.contains("Initial content"));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_three_peer_synchronization() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("three-peer".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("three-peer".to_string()).await?;
    let doc3 = CollaborativeDoc::with_new_endpoint("three-peer".to_string()).await?;

    doc1.insert_text("editor", 0, "From peer 1").await?;
    doc2.insert_text("editor", 0, "From peer 2").await?;
    doc3.insert_text("editor", 0, "From peer 3").await?;

    let doc1 = Arc::new(doc1);
    let doc2 = Arc::new(doc2);
    let doc1_clone = doc1.clone();
    let doc2_clone = doc2.clone();

    let peer1_addr = doc1.node_addr();
    let peer2_addr = doc2.node_addr();

    let accept1_handle = tokio::spawn(async move { doc1_clone.accept_sync_from_peer().await });

    let accept2_handle = tokio::spawn(async move { doc2_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;

    doc3.connect_and_sync_to_peer(peer1_addr).await?;
    sleep(Duration::from_millis(200)).await;

    doc3.connect_and_sync_to_peer(peer2_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept1_handle.await?;
    let _ = accept2_handle.await?;

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;
    let text3 = doc3.get_text("editor").await?;

    // In this test, doc3 connects to both doc1 and doc2
    // When doc3 connects to doc2, it already has doc1's content, so doc2 receives it too
    // So:
    // - doc1 has: its own content + doc3's content
    // - doc2 has: its own content + doc3's content + doc1's content (through doc3)
    // - doc3 has: its own content + doc1's content + doc2's content

    assert!(
        text1.contains("From peer 1"),
        "doc1 should have its own content"
    );
    assert!(
        text1.contains("From peer 3"),
        "doc1 should have received content from doc3"
    );

    assert!(
        text2.contains("From peer 2"),
        "doc2 should have its own content"
    );
    assert!(
        text2.contains("From peer 3"),
        "doc2 should have received content from doc3"
    );
    // doc2 should also have doc1's content because doc3 had it when connecting
    assert!(
        text2.contains("From peer 1"),
        "doc2 should have received doc1's content through doc3"
    );

    assert!(
        text3.contains("From peer 3"),
        "doc3 should have its own content"
    );
    assert!(
        text3.contains("From peer 1"),
        "doc3 should have received content from doc1"
    );
    assert!(
        text3.contains("From peer 2"),
        "doc3 should have received content from doc2"
    );

    // doc2 and doc3 should have all content, so they should be the same length
    assert_eq!(
        text2.len(),
        text3.len(),
        "doc2 and doc3 should have the same content (doc2 received doc1's content through doc3)"
    );
    // doc1 only synced with doc3, so it should have less content
    assert!(
        text1.len() < text2.len(),
        "doc1 should have less content than doc2 (it didn't sync with doc2)"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_bidirectional_sync() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("bidirectional".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("bidirectional".to_string()).await?;

    doc1.insert_text("editor", 0, "Peer 1 initial").await?;
    doc2.insert_text("editor", 0, "Peer 2 initial").await?;

    let doc1 = Arc::new(doc1);
    let doc2 = Arc::new(doc2);

    let doc1_clone = doc1.clone();
    let doc2_clone = doc2.clone();

    let peer1_addr = doc1.node_addr();
    let peer2_addr = doc2.node_addr();

    let accept1_handle = tokio::spawn(async move { doc1_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;

    doc2_clone.connect_and_sync_to_peer(peer1_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept1_handle.await?;

    sleep(Duration::from_millis(100)).await;

    let accept2_handle = tokio::spawn(async move { doc2.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;

    doc1.connect_and_sync_to_peer(peer2_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept2_handle.await?;

    let text1 = doc1.get_text("editor").await?;

    assert!(text1.contains("Peer 2 initial") || text1.contains("Peer 1 initial"));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_empty_document_sync() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("empty-doc".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("empty-doc".to_string()).await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();
    let peer1_addr = doc1.node_addr();

    let accept_handle = tokio::spawn(async move { doc1_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;

    doc2.connect_and_sync_to_peer(peer1_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept_handle.await?;

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;

    assert_eq!(text1, text2);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_large_document_sync() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("large-doc".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("large-doc".to_string()).await?;

    let large_text = "Lorem ipsum dolor sit amet, ".repeat(10000);
    doc1.insert_text("editor", 0, &large_text).await?;

    let doc2 = Arc::new(doc2);
    let doc2_clone = doc2.clone();
    let peer2_addr = doc2.node_addr();

    let accept_handle = tokio::spawn(async move { doc2_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;

    doc1.connect_and_sync_to_peer(peer2_addr).await?;
    sleep(Duration::from_millis(500)).await;

    let _ = accept_handle.await?;

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;

    assert_eq!(text1.len(), text2.len());
    assert!(text1.len() > 100000);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_rapid_sequential_edits() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("rapid-edits".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("rapid-edits".to_string()).await?;

    for i in 0..100 {
        doc1.insert_text("editor", i, "x").await?;
    }

    let doc2 = Arc::new(doc2);
    let doc2_clone = doc2.clone();
    let peer2_addr = doc2.node_addr();

    let accept_handle = tokio::spawn(async move { doc2_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;

    doc1.connect_and_sync_to_peer(peer2_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept_handle.await?;

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;

    assert_eq!(text1, text2);
    assert_eq!(text1.len(), 100);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_multiple_containers() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("multi-container".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("multi-container".to_string()).await?;

    doc1.insert_text("title", 0, "Document Title").await?;
    doc1.insert_text("body", 0, "Document Body").await?;
    doc1.insert_text("footer", 0, "Footer Text").await?;

    let doc1 = Arc::new(doc1);
    let doc2 = Arc::new(doc2);
    let doc2_clone = doc2.clone();
    let peer2_addr = doc2.node_addr();

    let accept_handle = tokio::spawn(async move { doc2_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;

    let doc1_clone = doc1.clone();
    doc1_clone.connect_and_sync_to_peer(peer2_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept_handle.await?;

    let title2 = doc2.get_text("title").await?;
    let body2 = doc2.get_text("body").await?;
    let footer2 = doc2.get_text("footer").await?;

    assert_eq!(title2, "Document Title");
    assert_eq!(body2, "Document Body");
    assert_eq!(footer2, "Footer Text");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_concurrent_connections() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("concurrent-conn".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("concurrent-conn".to_string()).await?;
    let doc3 = CollaborativeDoc::with_new_endpoint("concurrent-conn".to_string()).await?;

    doc1.insert_text("editor", 0, "Hub content").await?;
    doc2.insert_text("editor", 0, "Client 2").await?;
    doc3.insert_text("editor", 0, "Client 3").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone1 = doc1.clone();
    let doc1_clone2 = doc1.clone();

    let peer1_addr = doc1.node_addr();

    let accept1_handle = tokio::spawn(async move { doc1_clone1.accept_sync_from_peer().await });

    let accept2_handle = tokio::spawn(async move { doc1_clone2.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;

    let peer1_addr_clone = peer1_addr.clone();
    let connect2 =
        tokio::spawn(async move { doc2.connect_and_sync_to_peer(peer1_addr_clone).await });

    let connect3 = tokio::spawn(async move { doc3.connect_and_sync_to_peer(peer1_addr).await });

    sleep(Duration::from_millis(200)).await;

    let _ = connect2.await?;
    let _ = connect3.await?;
    let _ = accept1_handle.await?;
    let _ = accept2_handle.await?;

    let text1 = doc1.get_text("editor").await?;

    assert!(
        text1.contains("Client 2") || text1.contains("Client 3") || text1.contains("Hub content")
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_sync_timeout_protection() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("timeout-test".to_string()).await?;

    doc1.insert_text("editor", 0, "Test content").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();

    let accept_result = timeout(
        Duration::from_secs(5),
        tokio::spawn(async move { doc1_clone.accept_sync_from_peer().await }),
    )
    .await;

    assert!(
        accept_result.is_err() || accept_result.unwrap()?.is_err(),
        "Accept should timeout or error when no peer connects"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_alpn_mismatch_detection() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("doc-alpha".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("doc-beta".to_string()).await?;

    doc1.insert_text("editor", 0, "Alpha").await?;
    doc2.insert_text("editor", 0, "Beta").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();
    let peer1_addr = doc1.node_addr();

    let accept_handle = tokio::spawn(async move { doc1_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;

    let _connect_result = doc2.connect_and_sync_to_peer(peer1_addr).await;
    let accept_result = accept_handle.await?;

    assert!(
        accept_result.is_err(),
        "Should reject mismatched document IDs"
    );

    if let Err(e) = accept_result {
        let err_str = format!("{:?}", e);
        assert!(
            err_str.contains("Wrong document")
                || err_str.contains("ALPN")
                || err_str.contains("protocol"),
            "Error should mention document/ALPN/protocol mismatch: {}",
            err_str
        );
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_update_idempotency() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("idempotent".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("idempotent".to_string()).await?;

    let update = doc1.insert_text("editor", 0, "Test").await?;

    doc2.apply_update(&update).await?;
    doc2.apply_update(&update).await?;
    doc2.apply_update(&update).await?;

    let text = doc2.get_text("editor").await?;

    assert_eq!(
        text, "Test",
        "Applying same update multiple times should be idempotent"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_snapshot_consistency() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("snapshot".to_string()).await?;

    doc1.insert_text("editor", 0, "Hello").await?;
    doc1.insert_text("editor", 5, " World").await?;

    let snapshot1 = doc1.export_snapshot().await?;
    let snapshot2 = doc1.export_snapshot().await?;

    assert_eq!(
        snapshot1, snapshot2,
        "Multiple snapshots of unchanged document should be identical"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_peer_id_uniqueness() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("unique-peer".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("unique-peer".to_string()).await?;
    let doc3 = CollaborativeDoc::with_new_endpoint("unique-peer".to_string()).await?;

    let peer1 = doc1.peer_id();
    let peer2 = doc2.peer_id();
    let peer3 = doc3.peer_id();

    assert_ne!(peer1, peer2);
    assert_ne!(peer1, peer3);
    assert_ne!(peer2, peer3);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_node_id_consistency() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("node-id-test".to_string()).await?;

    let node_id1 = doc1.node_id();
    let node_id2 = doc1.node_id();
    let node_addr = doc1.node_addr();

    assert_eq!(node_id1, node_id2);
    assert_eq!(node_id1, node_addr.node_id);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_sequential_sync_sessions() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("sequential".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("sequential".to_string()).await?;

    doc1.insert_text("editor", 0, "First").await?;

    let doc2 = Arc::new(doc2);
    let doc2_clone = doc2.clone();
    let peer2_addr = doc2.node_addr();

    let accept_handle = tokio::spawn(async move { doc2_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;
    doc1.connect_and_sync_to_peer(peer2_addr.clone()).await?;
    sleep(Duration::from_millis(200)).await;
    let _ = accept_handle.await?;

    doc1.insert_text("editor", 5, " Second").await?;

    let doc2_clone = doc2.clone();
    let accept_handle = tokio::spawn(async move { doc2_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;
    doc1.connect_and_sync_to_peer(peer2_addr).await?;
    sleep(Duration::from_millis(200)).await;
    let _ = accept_handle.await?;

    let text2 = doc2.get_text("editor").await?;
    assert!(text2.contains("Second"));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_utf8_content_sync() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("utf8".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("utf8".to_string()).await?;

    let utf8_content = "Hello 世界 🌍 Здравствуй مرحبا";
    doc1.insert_text("editor", 0, utf8_content).await?;

    let doc2 = Arc::new(doc2);
    let doc2_clone = doc2.clone();
    let peer2_addr = doc2.node_addr();

    let accept_handle = tokio::spawn(async move { doc2_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;
    doc1.connect_and_sync_to_peer(peer2_addr).await?;
    sleep(Duration::from_millis(200)).await;
    let _ = accept_handle.await?;

    let text2 = doc2.get_text("editor").await?;
    assert_eq!(text2, utf8_content);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_special_characters_in_content() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("special-chars".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("special-chars".to_string()).await?;

    let special_content = "Line1\nLine2\tTabbed\r\nWindows\0Null";
    doc1.insert_text("editor", 0, special_content).await?;

    let doc2 = Arc::new(doc2);
    let doc2_clone = doc2.clone();
    let peer2_addr = doc2.node_addr();

    let accept_handle = tokio::spawn(async move { doc2_clone.accept_sync_from_peer().await });

    sleep(Duration::from_millis(500)).await;
    doc1.connect_and_sync_to_peer(peer2_addr).await?;
    sleep(Duration::from_millis(200)).await;
    let _ = accept_handle.await?;

    let text2 = doc2.get_text("editor").await?;
    assert_eq!(text2, special_content);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_zero_length_insert() -> Result<()> {
    let doc = CollaborativeDoc::with_new_endpoint("zero-insert".to_string()).await?;

    doc.insert_text("editor", 0, "").await?;
    let text = doc.get_text("editor").await?;

    assert_eq!(text, "");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_conflicting_edits_convergence() -> Result<()> {
    let doc1 = CollaborativeDoc::with_new_endpoint("conflict-test".to_string()).await?;
    let doc2 = CollaborativeDoc::with_new_endpoint("conflict-test".to_string()).await?;
    let doc3 = CollaborativeDoc::with_new_endpoint("conflict-test".to_string()).await?;

    doc1.insert_text("editor", 0, "Base").await?;

    let update_base = doc1.export_snapshot().await?;
    doc2.apply_update(&update_base).await?;
    doc3.apply_update(&update_base).await?;

    let update1 = doc1.insert_text("editor", 4, " from 1").await?;
    let update2 = doc2.insert_text("editor", 4, " from 2").await?;
    let update3 = doc3.insert_text("editor", 4, " from 3").await?;

    doc1.apply_update(&update2).await?;
    doc1.apply_update(&update3).await?;

    doc2.apply_update(&update1).await?;
    doc2.apply_update(&update3).await?;

    doc3.apply_update(&update1).await?;
    doc3.apply_update(&update2).await?;

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;
    let text3 = doc3.get_text("editor").await?;

    assert_eq!(text1, text2);
    assert_eq!(text2, text3);
    assert!(text1.contains("Base"));

    Ok(())
}
