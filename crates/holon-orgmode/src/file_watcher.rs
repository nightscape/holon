//! File watcher for Org files
//!
//! Watches for changes to .org files and notifies the OrgAdapter.

use anyhow::Result;
use holon::sync::CanonicalPath;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

/// File watcher for Org files
pub struct OrgFileWatcher {
    watcher: RecommendedWatcher,
    /// Channel sender for file change events
    #[allow(dead_code)]
    change_tx: mpsc::UnboundedSender<PathBuf>,
    /// Channel receiver for file change events
    change_rx: mpsc::UnboundedReceiver<PathBuf>,
    /// Known file hashes for content-based change detection
    known_hashes: Arc<RwLock<HashMap<CanonicalPath, String>>>,
}

impl OrgFileWatcher {
    /// Create a new file watcher
    ///
    /// # Arguments
    /// * `watch_dir` - Directory to watch for .org files
    /// * `known_hashes` - Optional shared hash map for content-based change detection
    pub fn new(watch_dir: &Path) -> Result<Self> {
        Self::with_hashes(watch_dir, Arc::new(RwLock::new(HashMap::new())))
    }

    /// Create a new file watcher with shared hash tracking
    ///
    /// # Arguments
    /// * `watch_dir` - Directory to watch for .org files
    /// * `known_hashes` - Shared hash map for content-based change detection
    pub fn with_hashes(
        watch_dir: &Path,
        known_hashes: Arc<RwLock<HashMap<CanonicalPath, String>>>,
    ) -> Result<Self> {
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let change_tx_clone = change_tx.clone();

        let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                match event.kind {
                    EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_) => {
                        for path in event.paths {
                            if path.extension().map(|e| e == "org").unwrap_or(false) {
                                if let Err(e) = change_tx_clone.send(path) {
                                    warn!("Failed to send file change event: {}", e);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            } else if let Err(e) = res {
                error!("File watcher error: {}", e);
            }
        })?;

        watcher.watch(watch_dir, RecursiveMode::Recursive)?;
        info!("Started watching Org files in: {}", watch_dir.display());

        Ok(Self {
            watcher,
            change_tx,
            change_rx,
            known_hashes,
        })
    }

    /// Compute SHA256 hash of file contents.
    pub fn hash_file(path: &Path) -> std::io::Result<String> {
        crate::file_utils::hash_file(path)
    }

    /// Check if file content actually changed (not just metadata/touch).
    pub async fn content_changed(&self, path: &Path) -> bool {
        let current_hash = match Self::hash_file(path) {
            Ok(h) => h,
            Err(_) => return true, // Assume changed if we can't read
        };

        let canonical_path = CanonicalPath::new(path);
        let known = self.known_hashes.read().await.get(&canonical_path).cloned();

        if Some(&current_hash) != known.as_ref() {
            self.known_hashes
                .write()
                .await
                .insert(canonical_path, current_hash);
            true
        } else {
            false
        }
    }

    /// Update known hash after writing a file (to prevent echo events).
    pub async fn update_hash(&self, path: &Path) {
        if let Ok(hash) = Self::hash_file(path) {
            let canonical_path = CanonicalPath::new(path);
            self.known_hashes.write().await.insert(canonical_path, hash);
        }
    }

    /// Get a receiver for file change events
    ///
    /// Note: This consumes the watcher. Use `into_receiver()` if you need to move the receiver.
    pub fn receiver(&mut self) -> &mut mpsc::UnboundedReceiver<PathBuf> {
        &mut self.change_rx
    }

    /// Consume the watcher and return the receiver
    ///
    /// WARNING: This drops the watcher, which stops file watching!
    /// Use `into_parts()` instead to keep the watcher alive.
    #[deprecated(note = "This drops the watcher. Use into_parts() instead.")]
    pub fn into_receiver(self) -> mpsc::UnboundedReceiver<PathBuf> {
        self.change_rx
    }

    /// Consume the watcher and return both the watcher and receiver.
    ///
    /// The caller MUST keep the watcher alive for file watching to work.
    pub fn into_parts(
        self,
    ) -> (
        RecommendedWatcher,
        mpsc::UnboundedReceiver<PathBuf>,
        Arc<RwLock<HashMap<CanonicalPath, String>>>,
    ) {
        (self.watcher, self.change_rx, self.known_hashes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_file_watcher_detects_changes() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.org");

        let mut watcher = OrgFileWatcher::new(temp_dir.path()).unwrap();

        // Wait a bit for watcher to initialize
        sleep(Duration::from_millis(100)).await;

        // Create a file
        tokio::fs::write(&test_file, "* Test").await.unwrap();
        sleep(Duration::from_millis(100)).await;

        // Should receive a change event
        let mut receiver = watcher.into_receiver();
        let received = receiver.try_recv();
        assert!(received.is_ok(), "Should receive file change event");
    }
}
