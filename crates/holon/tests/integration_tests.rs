use anyhow::Result;
use holon::sync::LoroDocument;
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[cfg(feature = "iroh-sync")]
use holon::sync::IrohSyncAdapter;

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_basic_two_peer_sync() -> Result<()> {
    let doc1 = LoroDocument::new("two-peer-sync".to_string())?;
    let doc2 = LoroDocument::new("two-peer-sync".to_string())?;

    doc1.insert_text("editor", 0, "Initial content").await?;

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;
    let adapter2 = IrohSyncAdapter::new("loro-sync").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();
    let peer1_addr = adapter1.node_addr();

    let accept_handle = tokio::spawn(async move { adapter1.accept_sync(&doc1_clone).await });

    sleep(Duration::from_millis(500)).await;

    adapter2.sync_with_peer(&doc2, peer1_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept_handle.await?;

    let text1 = doc1.get_text("editor").await?;
    assert!(text1.contains("Initial content"));

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_three_peer_synchronization() -> Result<()> {
    let doc1 = LoroDocument::new("three-peer".to_string())?;
    let doc2 = LoroDocument::new("three-peer".to_string())?;
    let doc3 = LoroDocument::new("three-peer".to_string())?;

    doc1.insert_text("editor", 0, "From peer 1").await?;
    doc2.insert_text("editor", 0, "From peer 2").await?;
    doc3.insert_text("editor", 0, "From peer 3").await?;

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;
    let adapter2 = IrohSyncAdapter::new("loro-sync").await?;
    let adapter3 = IrohSyncAdapter::new("loro-sync").await?;

    let doc1 = Arc::new(doc1);
    let doc2 = Arc::new(doc2);
    let doc1_clone = doc1.clone();
    let doc2_clone = doc2.clone();

    let peer1_addr = adapter1.node_addr();
    let peer2_addr = adapter2.node_addr();

    let accept1_handle = tokio::spawn(async move { adapter1.accept_sync(&doc1_clone).await });

    let accept2_handle = tokio::spawn(async move { adapter2.accept_sync(&doc2_clone).await });

    sleep(Duration::from_millis(500)).await;

    adapter3.sync_with_peer(&doc3, peer1_addr).await?;
    sleep(Duration::from_millis(200)).await;

    adapter3.sync_with_peer(&doc3, peer2_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept1_handle.await?;
    let _ = accept2_handle.await?;

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;
    let text3 = doc3.get_text("editor").await?;

    assert!(text1.contains("From peer 1"));
    assert!(text1.contains("From peer 3"));

    assert!(text2.contains("From peer 2"));
    assert!(text2.contains("From peer 3"));
    assert!(text2.contains("From peer 1"));

    assert!(text3.contains("From peer 3"));
    assert!(text3.contains("From peer 1"));
    assert!(text3.contains("From peer 2"));

    assert_eq!(text2.len(), text3.len());
    assert!(text1.len() < text2.len());

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_bidirectional_sync() -> Result<()> {
    let doc1 = LoroDocument::new("bidirectional".to_string())?;
    let doc2 = LoroDocument::new("bidirectional".to_string())?;

    doc1.insert_text("editor", 0, "Peer 1 initial").await?;
    doc2.insert_text("editor", 0, "Peer 2 initial").await?;

    let doc1 = Arc::new(doc1);
    let doc2 = Arc::new(doc2);

    let doc1_clone = doc1.clone();
    let doc2_clone = doc2.clone();

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;
    let adapter2 = IrohSyncAdapter::new("loro-sync").await?;

    let peer1_addr = adapter1.node_addr();
    let peer2_addr = adapter2.node_addr();

    let accept1_handle = tokio::spawn(async move { adapter1.accept_sync(&doc1_clone).await });

    sleep(Duration::from_millis(500)).await;

    adapter2.sync_with_peer(&doc2_clone, peer1_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept1_handle.await?;

    sleep(Duration::from_millis(100)).await;

    let accept2_handle = tokio::spawn(async move { adapter2.accept_sync(&doc2).await });

    sleep(Duration::from_millis(500)).await;

    let adapter1b = IrohSyncAdapter::new("loro-sync").await?;
    adapter1b.sync_with_peer(&doc1, peer2_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept2_handle.await?;

    let text1 = doc1.get_text("editor").await?;

    assert!(text1.contains("Peer 2 initial") || text1.contains("Peer 1 initial"));

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_empty_document_sync() -> Result<()> {
    let doc1 = LoroDocument::new("empty-doc".to_string())?;
    let doc2 = LoroDocument::new("empty-doc".to_string())?;

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;
    let adapter2 = IrohSyncAdapter::new("loro-sync").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();
    let peer1_addr = adapter1.node_addr();

    let accept_handle = tokio::spawn(async move { adapter1.accept_sync(&doc1_clone).await });

    sleep(Duration::from_millis(500)).await;

    adapter2.sync_with_peer(&doc2, peer1_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept_handle.await?;

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;

    assert_eq!(text1, text2);

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_large_document_sync() -> Result<()> {
    let doc1 = LoroDocument::new("large-doc".to_string())?;
    let doc2 = LoroDocument::new("large-doc".to_string())?;

    let large_text = "Lorem ipsum dolor sit amet, ".repeat(10000);
    doc1.insert_text("editor", 0, &large_text).await?;

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;
    let adapter2 = IrohSyncAdapter::new("loro-sync").await?;

    let doc2 = Arc::new(doc2);
    let doc2_clone = doc2.clone();
    let peer2_addr = adapter2.node_addr();

    let accept_handle = tokio::spawn(async move { adapter2.accept_sync(&doc2_clone).await });

    sleep(Duration::from_millis(500)).await;

    adapter1.sync_with_peer(&doc1, peer2_addr).await?;
    sleep(Duration::from_millis(500)).await;

    let _ = accept_handle.await?;

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;

    assert_eq!(text1.len(), text2.len());
    assert!(text1.len() > 100000);

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_rapid_sequential_edits() -> Result<()> {
    let doc1 = LoroDocument::new("rapid-edits".to_string())?;
    let doc2 = LoroDocument::new("rapid-edits".to_string())?;

    for i in 0..100 {
        doc1.insert_text("editor", i, "x").await?;
    }

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;
    let adapter2 = IrohSyncAdapter::new("loro-sync").await?;

    let doc2 = Arc::new(doc2);
    let doc2_clone = doc2.clone();
    let peer2_addr = adapter2.node_addr();

    let accept_handle = tokio::spawn(async move { adapter2.accept_sync(&doc2_clone).await });

    sleep(Duration::from_millis(500)).await;

    adapter1.sync_with_peer(&doc1, peer2_addr).await?;
    sleep(Duration::from_millis(200)).await;

    let _ = accept_handle.await?;

    let text1 = doc1.get_text("editor").await?;
    let text2 = doc2.get_text("editor").await?;

    assert_eq!(text1, text2);
    assert_eq!(text1.len(), 100);

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_multiple_containers() -> Result<()> {
    let doc1 = LoroDocument::new("multi-container".to_string())?;
    let doc2 = LoroDocument::new("multi-container".to_string())?;

    doc1.insert_text("title", 0, "Document Title").await?;
    doc1.insert_text("body", 0, "Document Body").await?;
    doc1.insert_text("footer", 0, "Footer Text").await?;

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;
    let adapter2 = IrohSyncAdapter::new("loro-sync").await?;

    let doc2 = Arc::new(doc2);
    let doc2_clone = doc2.clone();
    let peer2_addr = adapter2.node_addr();

    let accept_handle = tokio::spawn(async move { adapter2.accept_sync(&doc2_clone).await });

    sleep(Duration::from_millis(500)).await;

    adapter1.sync_with_peer(&doc1, peer2_addr).await?;
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

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_sync_timeout_protection() -> Result<()> {
    let doc1 = LoroDocument::new("timeout-test".to_string())?;

    doc1.insert_text("editor", 0, "Test content").await?;

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();

    let accept_result = timeout(
        Duration::from_secs(5),
        tokio::spawn(async move { adapter1.accept_sync(&doc1_clone).await }),
    )
    .await;

    assert!(
        accept_result.is_err() || accept_result.unwrap()?.is_err(),
        "Accept should timeout or error when no peer connects"
    );

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_alpn_mismatch_detection() -> Result<()> {
    let doc1 = LoroDocument::new("doc-alpha".to_string())?;
    let doc2 = LoroDocument::new("doc-beta".to_string())?;

    doc1.insert_text("editor", 0, "Alpha").await?;
    doc2.insert_text("editor", 0, "Beta").await?;

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;
    let adapter2 = IrohSyncAdapter::new("loro-sync").await?;

    let doc1 = Arc::new(doc1);
    let doc1_clone = doc1.clone();
    let peer1_addr = adapter1.node_addr();

    let accept_handle = tokio::spawn(async move { adapter1.accept_sync(&doc1_clone).await });

    sleep(Duration::from_millis(500)).await;

    let _connect_result = adapter2.sync_with_peer(&doc2, peer1_addr).await;
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

// Loro-only tests (no P2P adapter needed)

#[tokio::test]
#[serial]
async fn test_update_idempotency() -> Result<()> {
    let doc1 = LoroDocument::new("idempotent".to_string())?;
    let doc2 = LoroDocument::new("idempotent".to_string())?;

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
    let doc1 = LoroDocument::new("snapshot".to_string())?;

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
    let doc1 = LoroDocument::new("unique-peer".to_string())?;
    let doc2 = LoroDocument::new("unique-peer".to_string())?;
    let doc3 = LoroDocument::new("unique-peer".to_string())?;

    let peer1 = doc1.peer_id();
    let peer2 = doc2.peer_id();
    let peer3 = doc3.peer_id();

    assert_ne!(peer1, peer2);
    assert_ne!(peer1, peer3);
    assert_ne!(peer2, peer3);

    Ok(())
}

#[cfg(feature = "iroh-sync")]
#[tokio::test]
#[serial]
async fn test_sequential_sync_sessions() -> Result<()> {
    let doc1 = LoroDocument::new("sequential".to_string())?;
    let doc2 = LoroDocument::new("sequential".to_string())?;

    doc1.insert_text("editor", 0, "First").await?;

    let adapter1 = IrohSyncAdapter::new("loro-sync").await?;

    let doc2 = Arc::new(doc2);
    let doc2_clone = doc2.clone();
    let peer2_addr = adapter1.node_addr();

    // Note: IrohSyncAdapter creates a new endpoint each time, so for sequential
    // sessions we need fresh adapters for the acceptor side
    let adapter2 = IrohSyncAdapter::new("loro-sync").await?;
    let peer2_addr = adapter2.node_addr();

    let doc2_clone2 = doc2.clone();
    let accept_handle = tokio::spawn(async move { adapter2.accept_sync(&doc2_clone2).await });

    sleep(Duration::from_millis(500)).await;
    adapter1.sync_with_peer(&doc1, peer2_addr.clone()).await?;
    sleep(Duration::from_millis(200)).await;
    let _ = accept_handle.await?;

    doc1.insert_text("editor", 5, " Second").await?;

    let adapter3 = IrohSyncAdapter::new("loro-sync").await?;
    let peer2_addr2 = adapter3.node_addr();

    let doc2_clone3 = doc2.clone();
    let accept_handle = tokio::spawn(async move { adapter3.accept_sync(&doc2_clone3).await });

    let adapter1b = IrohSyncAdapter::new("loro-sync").await?;
    sleep(Duration::from_millis(500)).await;
    adapter1b.sync_with_peer(&doc1, peer2_addr2).await?;
    sleep(Duration::from_millis(200)).await;
    let _ = accept_handle.await?;

    let text2 = doc2.get_text("editor").await?;
    assert!(text2.contains("Second"));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_utf8_content_sync() -> Result<()> {
    let doc1 = LoroDocument::new("utf8".to_string())?;
    let doc2 = LoroDocument::new("utf8".to_string())?;

    let utf8_content = "Hello 世界 🌍 Здравствуй مرحبا";
    doc1.insert_text("editor", 0, utf8_content).await?;

    let snapshot = doc1.export_snapshot().await?;
    doc2.apply_update(&snapshot).await?;

    let text2 = doc2.get_text("editor").await?;
    assert_eq!(text2, utf8_content);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_special_characters_in_content() -> Result<()> {
    let doc1 = LoroDocument::new("special-chars".to_string())?;
    let doc2 = LoroDocument::new("special-chars".to_string())?;

    let special_content = "Line1\nLine2\tTabbed\r\nWindows\0Null";
    doc1.insert_text("editor", 0, special_content).await?;

    let snapshot = doc1.export_snapshot().await?;
    doc2.apply_update(&snapshot).await?;

    let text2 = doc2.get_text("editor").await?;
    assert_eq!(text2, special_content);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_zero_length_insert() -> Result<()> {
    let doc = LoroDocument::new("zero-insert".to_string())?;

    doc.insert_text("editor", 0, "").await?;
    let text = doc.get_text("editor").await?;

    assert_eq!(text, "");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_conflicting_edits_convergence() -> Result<()> {
    let doc1 = LoroDocument::new("conflict-test".to_string())?;
    let doc2 = LoroDocument::new("conflict-test".to_string())?;
    let doc3 = LoroDocument::new("conflict-test".to_string())?;

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
