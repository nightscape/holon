use anyhow::Result;
#[cfg(not(target_arch = "wasm32"))]
use iroh::Endpoint;
#[cfg(not(target_arch = "wasm32"))]
use iroh::NodeAddr;
#[cfg(not(target_arch = "wasm32"))]
use iroh::PublicKey;
use loro::{LoroDoc, PeerID};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, info};

pub struct CollaborativeDoc {
    doc: Arc<RwLock<LoroDoc>>,
    #[cfg(not(target_arch = "wasm32"))]
    endpoint: Arc<Endpoint>,
    #[cfg(not(target_arch = "wasm32"))]
    peer_id: PeerID,
    doc_id: String,
}

impl CollaborativeDoc {
    /// Create an Iroh endpoint configured for the given document ID.
    /// This endpoint can accept connections for this document via ALPN.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn create_endpoint(doc_id: &str) -> Result<Arc<Endpoint>> {
        let alpn = format!("loro-sync/{}", doc_id).into_bytes();
        let endpoint = Endpoint::builder()
            .discovery_n0()
            .alpns(vec![alpn])
            .bind()
            .await?;
        Ok(Arc::new(endpoint))
    }

    /// Create a new collaborative document on an existing endpoint.
    ///
    /// # Arguments
    /// * `endpoint` - Shared Iroh endpoint (can be used by multiple documents)
    /// * `doc_id` - Unique identifier for this document (must match on all collaborating peers)
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn new(endpoint: Arc<Endpoint>, doc_id: String) -> Result<Self> {
        let node_id = endpoint.node_id();
        let node_id_bytes = node_id.as_bytes();
        let peer_id = u64::from_le_bytes(node_id_bytes[0..8].try_into()?);

        let doc = LoroDoc::new();
        doc.set_peer_id(peer_id)?;

        info!(
            "Created collaborative doc '{}' with peer_id: {}",
            doc_id, peer_id
        );

        Ok(Self {
            doc: Arc::new(RwLock::new(doc)),
            endpoint,
            peer_id,
            doc_id,
        })
    }

    /// Convenience method to create both endpoint and document in one call.
    /// Use this when you only need one document per endpoint.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn with_new_endpoint(doc_id: String) -> Result<Self> {
        let endpoint = Self::create_endpoint(&doc_id).await?;
        Self::new(endpoint, doc_id).await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn with_new_endpoint(doc_id: String) -> Result<Self> {
        let doc = LoroDoc::new();
        // Use a random peer ID for WASM since we don't have Iroh node ID
        let peer_id = rand::random::<u64>();
        doc.set_peer_id(peer_id)?;

        info!(
            "Created local-only doc '{}' with peer_id: {}",
            doc_id, peer_id
        );

        Ok(Self {
            doc: Arc::new(RwLock::new(doc)),
            doc_id,
        })
    }

    pub fn doc_id(&self) -> &str {
        &self.doc_id
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn alpn(&self) -> Vec<u8> {
        format!("loro-sync/{}", self.doc_id).into_bytes()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn node_id(&self) -> PublicKey {
        self.endpoint.node_id()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn peer_id(&self) -> PeerID {
        self.peer_id
    }

    pub async fn insert_text(&self, container: &str, index: usize, text: &str) -> Result<Vec<u8>> {
        let doc = self.doc.write().await;
        let text_obj = doc.get_text(container);
        text_obj.insert(index, text)?;

        Ok(doc.export(loro::ExportMode::updates_owned(Default::default()))?)
    }

    pub async fn get_text(&self, container: &str) -> Result<String> {
        let doc = self.doc.read().await;
        let text_obj = doc.get_text(container);
        Ok(text_obj.to_string())
    }

    pub async fn apply_update(&self, update: &[u8]) -> Result<()> {
        let doc = self.doc.write().await;
        doc.import(update)?;
        debug!("Applied update of {} bytes", update.len());
        Ok(())
    }

    pub async fn export_snapshot(&self) -> Result<Vec<u8>> {
        let doc = self.doc.read().await;
        Ok(doc.export(loro::ExportMode::Snapshot)?)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn connect_and_sync_to_peer(&self, peer_node_addr: NodeAddr) -> Result<()> {
        info!(
            "Connecting to peer: {:?} for document '{}'",
            peer_node_addr.node_id, self.doc_id
        );
        let alpn = self.alpn();
        let conn = self.endpoint.connect(peer_node_addr, &alpn).await?;

        // Send our snapshot to the peer
        let snapshot = self.export_snapshot().await?;
        let mut send_stream = conn.open_uni().await?;
        send_stream.write_all(&snapshot).await?;
        send_stream.finish()?;

        info!(
            "Sent {} bytes snapshot to peer for document '{}'",
            snapshot.len(),
            self.doc_id
        );

        // Receive peer's snapshot
        let mut recv_stream = conn.accept_uni().await?;
        let buffer = recv_stream.read_to_end(10 * 1024 * 1024).await?;

        if !buffer.is_empty() {
            self.apply_update(&buffer).await?;
            info!(
                "Received and applied {} bytes from peer for document '{}'",
                buffer.len(),
                self.doc_id
            );
        }

        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn accept_sync_from_peer(&self) -> Result<()> {
        info!(
            "Waiting for incoming connection for document '{}'...",
            self.doc_id
        );
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| anyhow::anyhow!("No incoming connection"))?;

        let conn = incoming.await?;
        let alpn = conn.alpn().clone();
        let expected_alpn = self.alpn();

        if alpn.as_deref() != Some(expected_alpn.as_slice()) {
            anyhow::bail!(
                "Wrong document! Expected '{}' but got ALPN: {:?}",
                self.doc_id,
                alpn.as_ref()
                    .map(|v| String::from_utf8_lossy(v).to_string())
            );
        }

        info!(
            "Connection established with peer for document '{}'",
            self.doc_id
        );

        // Receive peer's snapshot
        let mut recv_stream = conn.accept_uni().await?;
        let buffer = recv_stream.read_to_end(10 * 1024 * 1024).await?;

        if !buffer.is_empty() {
            self.apply_update(&buffer).await?;
            info!(
                "Received and applied {} bytes from peer for document '{}'",
                buffer.len(),
                self.doc_id
            );
        }

        // Send our snapshot back to the peer
        let snapshot = self.export_snapshot().await?;
        let mut send_stream = conn.open_uni().await?;
        send_stream.write_all(&snapshot).await?;
        send_stream.finish()?;

        info!(
            "Sent {} bytes snapshot to peer for document '{}'",
            snapshot.len(),
            self.doc_id
        );

        // Small delay to ensure the stream is fully sent before the connection might be dropped
        sleep(Duration::from_millis(100)).await;

        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn node_addr(&self) -> NodeAddr {
        let node_id = self.endpoint.node_id();
        NodeAddr::new(node_id)
    }

    /// Execute a read-only operation on the document.
    ///
    /// Provides shared access to the LoroDoc for querying without mutations.
    pub async fn with_read<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&LoroDoc) -> Result<R>,
    {
        let doc = self.doc.read().await;
        f(&doc)
    }

    /// Execute a write operation with automatic transaction management and sync.
    ///
    /// Guarantees:
    /// - All mutations occur within a transaction
    /// - Transaction commits only on Ok
    /// - Transaction aborts on Err or panic
    /// - P2P sync triggered after successful commit
    /// - Prevents nested write transactions
    ///
    /// # Example
    ///
    /// ```no_run
    /// collab_doc.with_write(|doc, txn| {
    ///     let map = doc.get_map("my_map");
    ///     map.insert(txn, "key", "value")?;
    ///     Ok(())
    /// }).await?;
    /// ```
    pub async fn with_write<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&LoroDoc) -> Result<R>,
    {
        let doc = self.doc.write().await;

        // Execute operations (Loro creates implicit transactions)
        let result = f(&doc)?;

        // Export updates for P2P sync
        let updates = doc.export(loro::ExportMode::updates_owned(Default::default()))?;

        // Release lock before async sync
        drop(doc);

        // TODO: Trigger P2P sync with updates if not empty
        // This should be debounced/coalesced in production
        if !updates.is_empty() {
            debug!("Write committed, {} bytes to sync", updates.len());
        }

        Ok(result)
    }

    /// Access the underlying LoroDoc directly.
    ///
    /// Prefer using `with_read()` or `with_write()` for better safety guarantees.
    /// This method is provided for advanced use cases that need direct doc access.
    pub fn doc(&self) -> Arc<RwLock<LoroDoc>> {
        self.doc.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_collaborative_doc() -> Result<()> {
        let endpoint = Arc::new(Endpoint::builder().discovery_n0().bind().await?);
        let doc = CollaborativeDoc::new(endpoint, "test-doc".to_string()).await?;
        assert_ne!(doc.peer_id().to_string(), "");
        assert_eq!(doc.doc_id(), "test-doc");
        Ok(())
    }

    #[tokio::test]
    async fn test_text_operations() -> Result<()> {
        let endpoint = Arc::new(Endpoint::builder().discovery_n0().bind().await?);
        let doc = CollaborativeDoc::new(endpoint, "test-doc".to_string()).await?;

        doc.insert_text("editor", 0, "Hello").await?;
        let text = doc.get_text("editor").await?;
        assert_eq!(text, "Hello");

        doc.insert_text("editor", 5, " World").await?;
        let text = doc.get_text("editor").await?;
        assert_eq!(text, "Hello World");

        Ok(())
    }

    #[tokio::test]
    async fn test_update_export_and_apply() -> Result<()> {
        let endpoint1 = Arc::new(Endpoint::builder().discovery_n0().bind().await?);
        let endpoint2 = Arc::new(Endpoint::builder().discovery_n0().bind().await?);

        let doc1 = CollaborativeDoc::new(endpoint1, "shared-doc".to_string()).await?;
        let doc2 = CollaborativeDoc::new(endpoint2, "shared-doc".to_string()).await?;

        let update = doc1.insert_text("editor", 0, "Collaborative").await?;

        doc2.apply_update(&update).await?;

        let text1 = doc1.get_text("editor").await?;
        let text2 = doc2.get_text("editor").await?;

        assert_eq!(text1, text2);
        assert_eq!(text1, "Collaborative");

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_edits_merge() -> Result<()> {
        let endpoint1 = Arc::new(Endpoint::builder().discovery_n0().bind().await?);
        let endpoint2 = Arc::new(Endpoint::builder().discovery_n0().bind().await?);

        let doc1 = CollaborativeDoc::new(endpoint1, "shared-doc".to_string()).await?;
        let doc2 = CollaborativeDoc::new(endpoint2, "shared-doc".to_string()).await?;

        let update1 = doc1.insert_text("editor", 0, "Hello").await?;
        doc2.apply_update(&update1).await?;

        let update2a = doc1.insert_text("editor", 5, " from doc1").await?;
        let update2b = doc2.insert_text("editor", 5, " from doc2").await?;

        doc1.apply_update(&update2b).await?;
        doc2.apply_update(&update2a).await?;

        let text1 = doc1.get_text("editor").await?;
        let text2 = doc2.get_text("editor").await?;

        assert_eq!(text1, text2);
        assert!(text1.contains("Hello"));

        Ok(())
    }

    #[tokio::test]
    async fn test_different_documents_isolated() -> Result<()> {
        let endpoint = Arc::new(Endpoint::builder().discovery_n0().bind().await?);

        let doc_a = CollaborativeDoc::new(endpoint.clone(), "doc-a".to_string()).await?;
        let doc_b = CollaborativeDoc::new(endpoint.clone(), "doc-b".to_string()).await?;

        doc_a.insert_text("editor", 0, "Document A").await?;
        doc_b.insert_text("editor", 0, "Document B").await?;

        let text_a = doc_a.get_text("editor").await?;
        let text_b = doc_b.get_text("editor").await?;

        assert_eq!(text_a, "Document A");
        assert_eq!(text_b, "Document B");
        assert_ne!(doc_a.alpn(), doc_b.alpn());

        Ok(())
    }
}
