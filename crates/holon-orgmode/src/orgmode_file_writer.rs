//! Event subscriber that writes Loro blocks to Org files
//!
//! Subscribes to block events from EventBus and serializes them to Org files.
//! This completes the forward path: Loro → EventBus → Org file.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

use holon::api::types::Traversal;
use holon::api::{CoreOperations, LoroBackend};
use holon::storage::types::Result;
use holon::sync::event_bus::{EventBus, EventFilter, EventOrigin, EventStatus};
use holon::sync::{CanonicalPath, LoroDocumentStore};
use holon_api::block::Block;

use crate::file_watcher::OrgFileWatcher;
use crate::org_renderer::OrgRenderer;
use crate::write_tracker::WriteTracker;

/// Subscriber that writes Loro blocks to Org files when block events occur
pub struct OrgFileWriter {
    doc_store: Arc<RwLock<LoroDocumentStore>>,
    write_tracker: Arc<RwLock<WriteTracker>>,
    /// Known file hashes for preventing echo events
    known_hashes: Option<Arc<RwLock<HashMap<CanonicalPath, String>>>>,
    /// Debounce window in milliseconds
    debounce_ms: u64,
}

impl OrgFileWriter {
    /// Create a new OrgFileWriter
    pub fn new(
        doc_store: Arc<RwLock<LoroDocumentStore>>,
        write_tracker: Arc<RwLock<WriteTracker>>,
    ) -> Self {
        Self {
            doc_store,
            write_tracker,
            known_hashes: None,
            debounce_ms: 500,
        }
    }

    /// Create a new OrgFileWriter with hash tracking
    pub fn with_hash_tracking(
        doc_store: Arc<RwLock<LoroDocumentStore>>,
        write_tracker: Arc<RwLock<WriteTracker>>,
        known_hashes: Arc<RwLock<HashMap<CanonicalPath, String>>>,
        debounce_ms: u64,
    ) -> Self {
        Self {
            doc_store,
            write_tracker,
            known_hashes: Some(known_hashes),
            debounce_ms,
        }
    }

    /// Start subscribing to block events and writing to Org files
    ///
    /// This spawns a background task that listens to block events from the EventBus
    /// and writes the corresponding Org file when blocks are modified.
    pub async fn start(&self, event_bus: Arc<dyn EventBus>) -> Result<()> {
        let doc_store = Arc::clone(&self.doc_store);
        let write_tracker = Arc::clone(&self.write_tracker);
        let known_hashes = self.known_hashes.clone();
        let debounce_ms = self.debounce_ms;

        // Subscribe to confirmed block events from Loro origin
        // (we only write to Org files when Loro is the source of truth)
        let filter = EventFilter::new()
            .with_status(EventStatus::Confirmed)
            .with_aggregate_type("block")
            .with_origin(EventOrigin::Loro);

        let mut event_stream = event_bus.subscribe(filter).await?;

        tokio::spawn(async move {
            info!(
                "[OrgFileWriter] Started listening to block events (debounce={}ms)",
                debounce_ms
            );
            eprintln!(
                "[OrgFileWriter] Started listening to block events (debounce={}ms)",
                debounce_ms
            );

            // Track pending render request
            let mut pending_render = false;
            let mut last_event_time = std::time::Instant::now();

            loop {
                // Use a short timeout to check for debounce expiry
                let timeout = tokio::time::sleep(tokio::time::Duration::from_millis(100));
                tokio::pin!(timeout);

                tokio::select! {
                    // Check for new events
                    event = event_stream.next() => {
                        match event {
                            Some(event) => {
                                eprintln!(
                                    "[OrgFileWriter] Received event: {} for block {} (debounce reset)",
                                    event.event_type, event.aggregate_id
                                );
                                // Mark that we need to render, reset debounce timer
                                pending_render = true;
                                last_event_time = std::time::Instant::now();
                            }
                            None => {
                                // Stream closed
                                break;
                            }
                        }
                    }
                    // Debounce timeout check
                    _ = &mut timeout => {
                        if pending_render && last_event_time.elapsed().as_millis() >= debounce_ms as u128 {
                            eprintln!("[OrgFileWriter] Debounce period elapsed ({}ms), rendering documents", debounce_ms);
                            // Re-render all loaded documents
                            match Self::render_all_documents(&doc_store, &write_tracker, known_hashes.as_ref()).await {
                                Ok(any_skipped) => {
                                    // Only clear pending_render if all documents were rendered.
                                    // If some were skipped (OrgAdapter still processing), retry later.
                                    if any_skipped {
                                        eprintln!("[OrgFileWriter] Some documents skipped, will retry");
                                        last_event_time = std::time::Instant::now();
                                    } else {
                                        pending_render = false;
                                    }
                                }
                                Err(e) => {
                                    error!("[OrgFileWriter] Failed to render documents: {}", e);
                                    pending_render = false;
                                }
                            }
                        }
                    }
                }
            }

            info!("[OrgFileWriter] Event stream closed");
            eprintln!("[OrgFileWriter] Event stream closed");
        });

        Ok(())
    }

    /// Render all loaded documents to their Org files.
    /// Returns `Ok(true)` if any documents were skipped (caller should retry later).
    async fn render_all_documents(
        doc_store: &Arc<RwLock<LoroDocumentStore>>,
        write_tracker: &Arc<RwLock<WriteTracker>>,
        known_hashes: Option<&Arc<RwLock<HashMap<CanonicalPath, String>>>>,
    ) -> Result<bool> {
        let store = doc_store.read().await;
        let documents: Vec<_> = store.iter().await;
        drop(store);

        eprintln!(
            "[OrgFileWriter] Rendering {} documents: {:?}",
            documents.len(),
            documents
                .iter()
                .map(|(p, _)| p.display().to_string())
                .collect::<Vec<_>>()
        );

        let mut any_skipped = false;

        for (file_path, collab_doc) in documents {
            // Skip non-.org files
            if !file_path
                .extension()
                .map(|ext| ext == "org")
                .unwrap_or(false)
            {
                continue;
            }

            // Skip if OrgAdapter is currently processing external changes for this file.
            // This prevents the sync loop where OrgFileWriter re-renders with stale
            // Loro state before OrgAdapter has finished creating all blocks.
            {
                let tracker = write_tracker.read().await;
                if tracker.should_skip_render(&file_path) {
                    eprintln!(
                        "[OrgFileWriter] Skipping render for {} - OrgAdapter is processing",
                        file_path.display()
                    );
                    any_skipped = true;
                    continue;
                }
            }

            // Create a backend to read blocks
            let backend = LoroBackend::from_document(collab_doc);
            let file_id = backend.doc_id().to_string();

            // Get all blocks from the document
            let blocks: Vec<Block> = match backend.get_all_blocks(Traversal::ALL_BUT_ROOT).await {
                Ok(blocks) => blocks,
                Err(e) => {
                    warn!(
                        "[OrgFileWriter] Failed to get blocks from {}: {}",
                        file_path.display(),
                        e
                    );
                    continue;
                }
            };

            eprintln!(
                "[OrgFileWriter] Got {} blocks for {}",
                blocks.len(),
                file_path.display()
            );

            // Debug: show each block's content_type to diagnose source block corruption
            for block in &blocks {
                eprintln!(
                    "[OrgFileWriter]   Block id={}, content_type={}, source_language={:?}",
                    block.id, block.content_type, block.source_language
                );
            }

            // Render blocks to Org format
            let org_content = OrgRenderer::render_blocks(&blocks, &file_path, &file_id);

            // Mark write BEFORE writing: hash the content we're about to write so the
            // file watcher can't race between write and mark_write.
            {
                let content_hash = crate::file_utils::hash_bytes(org_content.as_bytes());
                let mut tracker = write_tracker.write().await;
                tracker.mark_write_with_hash(&file_path, content_hash);
            }

            // Write to file
            if let Err(e) = tokio::fs::write(&file_path, &org_content).await {
                error!(
                    "[OrgFileWriter] Failed to write Org file {}: {}",
                    file_path.display(),
                    e
                );
                continue;
            }

            // Update hash after writing to prevent echo events
            if let Some(known_hashes) = known_hashes {
                if let Ok(hash) = OrgFileWatcher::hash_file(&file_path) {
                    known_hashes.write().await.insert(file_path.clone(), hash);
                }
            }

            eprintln!(
                "[OrgFileWriter] Wrote {} bytes to {}",
                org_content.len(),
                file_path.display()
            );
        }

        Ok(any_skipped)
    }
}
