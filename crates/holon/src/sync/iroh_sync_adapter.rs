#[cfg(not(target_arch = "wasm32"))]
mod adapter {
    use crate::sync::loro_document::LoroDocument;
    use anyhow::Result;
    use iroh::{Endpoint, NodeAddr, PublicKey};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;
    use tracing::info;

    pub struct IrohSyncAdapter {
        endpoint: Arc<Endpoint>,
        alpn_prefix: String,
    }

    impl IrohSyncAdapter {
        pub async fn new(alpn_prefix: &str) -> Result<Self> {
            let endpoint = Endpoint::builder().discovery_n0().bind().await?;
            Ok(Self {
                endpoint: Arc::new(endpoint),
                alpn_prefix: alpn_prefix.to_string(),
            })
        }

        fn alpn(&self, doc_id: &str) -> Vec<u8> {
            format!("{}/{}", self.alpn_prefix, doc_id).into_bytes()
        }

        /// Derive peer_id from the Iroh node_id and set it on the document.
        pub fn set_peer_id_from_node(&self, doc: &mut LoroDocument) -> Result<()> {
            let node_id = self.endpoint.node_id();
            let node_id_bytes = node_id.as_bytes();
            let peer_id = u64::from_le_bytes(node_id_bytes[0..8].try_into()?);
            doc.set_peer_id(peer_id)?;
            Ok(())
        }

        pub fn node_id(&self) -> PublicKey {
            self.endpoint.node_id()
        }

        pub fn node_addr(&self) -> NodeAddr {
            NodeAddr::new(self.endpoint.node_id())
        }

        pub fn endpoint(&self) -> &Arc<Endpoint> {
            &self.endpoint
        }

        /// Connect to a peer and exchange snapshots for the given document.
        pub async fn sync_with_peer(
            &self,
            doc: &LoroDocument,
            peer_node_addr: NodeAddr,
        ) -> Result<()> {
            let doc_id = doc.doc_id();
            info!(
                "Connecting to peer: {:?} for document '{}'",
                peer_node_addr.node_id, doc_id
            );
            let alpn = self.alpn(doc_id);
            let conn = self.endpoint.connect(peer_node_addr, &alpn).await?;

            // Send our snapshot to the peer
            let snapshot = doc.export_snapshot().await?;
            let mut send_stream = conn.open_uni().await?;
            send_stream.write_all(&snapshot).await?;
            send_stream.finish()?;

            info!(
                "Sent {} bytes snapshot to peer for document '{}'",
                snapshot.len(),
                doc_id
            );

            // Receive peer's snapshot
            let mut recv_stream = conn.accept_uni().await?;
            let buffer = recv_stream.read_to_end(10 * 1024 * 1024).await?;

            if !buffer.is_empty() {
                doc.apply_update(&buffer).await?;
                info!(
                    "Received and applied {} bytes from peer for document '{}'",
                    buffer.len(),
                    doc_id
                );
            }

            Ok(())
        }

        /// Accept an incoming sync connection for the given document.
        pub async fn accept_sync(&self, doc: &LoroDocument) -> Result<()> {
            let doc_id = doc.doc_id();
            info!(
                "Waiting for incoming connection for document '{}'...",
                doc_id
            );
            let incoming = self
                .endpoint
                .accept()
                .await
                .ok_or_else(|| anyhow::anyhow!("No incoming connection"))?;

            let conn = incoming.await?;
            let alpn = conn.alpn().clone();
            let expected_alpn = self.alpn(doc_id);

            if alpn.as_deref() != Some(expected_alpn.as_slice()) {
                anyhow::bail!(
                    "Wrong document! Expected '{}' but got ALPN: {:?}",
                    doc_id,
                    alpn.as_ref()
                        .map(|v| String::from_utf8_lossy(v).to_string())
                );
            }

            info!("Connection established with peer for document '{}'", doc_id);

            // Receive peer's snapshot
            let mut recv_stream = conn.accept_uni().await?;
            let buffer = recv_stream.read_to_end(10 * 1024 * 1024).await?;

            if !buffer.is_empty() {
                doc.apply_update(&buffer).await?;
                info!(
                    "Received and applied {} bytes from peer for document '{}'",
                    buffer.len(),
                    doc_id
                );
            }

            // Send our snapshot back to the peer
            let snapshot = doc.export_snapshot().await?;
            let mut send_stream = conn.open_uni().await?;
            send_stream.write_all(&snapshot).await?;
            send_stream.finish()?;

            info!(
                "Sent {} bytes snapshot to peer for document '{}'",
                snapshot.len(),
                doc_id
            );

            sleep(Duration::from_millis(100)).await;

            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn test_create_adapter() -> Result<()> {
            let adapter = IrohSyncAdapter::new("loro-sync").await?;
            let _node_id = adapter.node_id();
            let _node_addr = adapter.node_addr();
            Ok(())
        }

        #[tokio::test]
        async fn test_set_peer_id_from_node() -> Result<()> {
            let adapter = IrohSyncAdapter::new("loro-sync").await?;
            let mut doc = LoroDocument::new("test".to_string())?;
            let original_peer_id = doc.peer_id();
            adapter.set_peer_id_from_node(&mut doc)?;
            assert_ne!(doc.peer_id(), original_peer_id);
            Ok(())
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub use adapter::IrohSyncAdapter;
