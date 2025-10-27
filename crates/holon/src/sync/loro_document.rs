use anyhow::Result;
use loro::{LoroDoc, PeerID};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

pub struct LoroDocument {
    doc: Arc<RwLock<LoroDoc>>,
    peer_id: PeerID,
    doc_id: String,
}

impl LoroDocument {
    pub fn new(doc_id: String) -> Result<Self> {
        let peer_id = rand::random::<u64>();
        let doc = LoroDoc::new();
        doc.set_peer_id(peer_id)?;

        info!(
            "Created LoroDocument '{}' with peer_id: {}",
            doc_id, peer_id
        );

        Ok(Self {
            doc: Arc::new(RwLock::new(doc)),
            peer_id,
            doc_id,
        })
    }

    pub fn doc_id(&self) -> &str {
        &self.doc_id
    }

    pub fn peer_id(&self) -> PeerID {
        self.peer_id
    }

    /// Override the peer_id (used by IrohSyncAdapter to set Iroh-derived ID).
    pub fn set_peer_id(&mut self, peer_id: PeerID) -> Result<()> {
        self.peer_id = peer_id;
        // We need write access to set peer_id on the inner doc, but this is
        // a sync method. Use try_write since this should only be called before
        // any concurrent access.
        let doc = self
            .doc
            .try_write()
            .map_err(|_| anyhow::anyhow!("Cannot set_peer_id while document is in use"))?;
        doc.set_peer_id(peer_id)?;
        Ok(())
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

    pub async fn with_read<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&LoroDoc) -> Result<R>,
    {
        let doc = self.doc.read().await;
        f(&doc)
    }

    pub async fn with_write<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&LoroDoc) -> Result<R>,
    {
        let doc = self.doc.write().await;

        let result = f(&doc)?;

        let updates = doc.export(loro::ExportMode::updates_owned(Default::default()))?;

        drop(doc);

        if !updates.is_empty() {
            debug!("Write committed, {} bytes to sync", updates.len());
        }

        Ok(result)
    }

    pub fn doc(&self) -> Arc<RwLock<LoroDoc>> {
        self.doc.clone()
    }

    pub async fn save_to_file(&self, path: &Path) -> Result<()> {
        let snapshot = self.export_snapshot().await?;
        tokio::fs::write(path, snapshot).await?;
        debug!("Saved LoroDoc snapshot to {}", path.display());
        Ok(())
    }

    pub async fn load_from_file(path: &Path, doc_id: String) -> Result<Self> {
        let bytes = tokio::fs::read(path).await?;
        let peer_id = rand::random::<u64>();

        let doc = LoroDoc::new();
        doc.set_peer_id(peer_id)?;
        doc.import(&bytes)?;

        info!(
            "Loaded LoroDocument '{}' from {} with peer_id: {}",
            doc_id,
            path.display(),
            peer_id
        );

        Ok(Self {
            doc: Arc::new(RwLock::new(doc)),
            peer_id,
            doc_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_loro_document() -> Result<()> {
        let doc = LoroDocument::new("test-doc".to_string())?;
        assert_ne!(doc.peer_id().to_string(), "");
        assert_eq!(doc.doc_id(), "test-doc");
        Ok(())
    }

    #[tokio::test]
    async fn test_text_operations() -> Result<()> {
        let doc = LoroDocument::new("test-doc".to_string())?;

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
        let doc1 = LoroDocument::new("shared-doc".to_string())?;
        let doc2 = LoroDocument::new("shared-doc".to_string())?;

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
        let doc1 = LoroDocument::new("shared-doc".to_string())?;
        let doc2 = LoroDocument::new("shared-doc".to_string())?;

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
        let doc_a = LoroDocument::new("doc-a".to_string())?;
        let doc_b = LoroDocument::new("doc-b".to_string())?;

        doc_a.insert_text("editor", 0, "Document A").await?;
        doc_b.insert_text("editor", 0, "Document B").await?;

        let text_a = doc_a.get_text("editor").await?;
        let text_b = doc_b.get_text("editor").await?;

        assert_eq!(text_a, "Document A");
        assert_eq!(text_b, "Document B");

        Ok(())
    }
}
