//! Stream-based OrgModeSyncProvider
//!
//! This sync provider scans an org-mode directory and emits changes on typed streams.
//! Architecture:
//! - ONE sync() call → multiple typed streams (directories, files, headlines)
//! - Uses file content hashes for change detection
//! - Fire-and-forget operations - updates arrive via streams

use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
use walkdir::WalkDir;

use holon::core::datasource::{
    generate_sync_operation, Change, ChangeOrigin, OperationDescriptor, OperationProvider, Result,
    StreamPosition, SyncTokenStore, SyncableProvider, UndoAction,
};
use holon::storage::types::StorageEntity;
use holon_api::{BatchMetadata, Operation, SyncTokenUpdate, WithMetadata};

use holon_filesystem::{
    directory::{ChangesWithMetadata, DirectoryChangeProvider},
    directory::{Directory, ROOT_ID},
};

use crate::models::{OrgFile, OrgHeadline};
use crate::parser::{
    compute_content_hash, generate_directory_id, generate_file_id, parse_org_file,
};
use crate::writer::write_id_properties;

/// Sync state stored as JSON in token store
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
struct SyncState {
    /// Map of file paths to their content hashes
    file_hashes: HashMap<String, String>,
    /// Map of directory paths
    known_dirs: HashMap<String, bool>,
}

/// Stream-based OrgModeSyncProvider that scans directories and emits changes on typed streams
pub struct OrgModeSyncProvider {
    root_directory: PathBuf,
    token_store: Arc<dyn SyncTokenStore>,
    directory_tx: broadcast::Sender<ChangesWithMetadata<Directory>>,
    file_tx: broadcast::Sender<ChangesWithMetadata<OrgFile>>,
    headline_tx: broadcast::Sender<ChangesWithMetadata<OrgHeadline>>,
}

impl OrgModeSyncProvider {
    pub fn new(root_directory: PathBuf, token_store: Arc<dyn SyncTokenStore>) -> Self {
        Self {
            root_directory,
            token_store,
            directory_tx: broadcast::channel(1000).0,
            file_tx: broadcast::channel(1000).0,
            headline_tx: broadcast::channel(1000).0,
        }
    }

    pub fn subscribe_directories(&self) -> broadcast::Receiver<ChangesWithMetadata<Directory>> {
        self.directory_tx.subscribe()
    }

    pub fn subscribe_files(&self) -> broadcast::Receiver<ChangesWithMetadata<OrgFile>> {
        self.file_tx.subscribe()
    }

    pub fn subscribe_headlines(&self) -> broadcast::Receiver<ChangesWithMetadata<OrgHeadline>> {
        self.headline_tx.subscribe()
    }

    /// Load sync state from token store
    async fn load_state(&self) -> Result<SyncState> {
        let position = self
            .token_store
            .load_token(self.provider_name())
            .await?
            .unwrap_or(StreamPosition::Beginning);

        match position {
            StreamPosition::Beginning => Ok(SyncState::default()),
            StreamPosition::Version(bytes) => {
                let state: SyncState = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("Failed to parse sync state: {}", e))?;
                Ok(state)
            }
        }
    }

    /// Perform directory scan and compute changes
    async fn scan_and_compute_changes(
        &self,
        old_state: &SyncState,
    ) -> Result<(
        SyncState,
        Vec<Change<Directory>>,
        Vec<Change<OrgFile>>,
        Vec<Change<OrgHeadline>>,
    )> {
        let origin = ChangeOrigin::remote_with_current_span();
        let mut new_state = SyncState::default();
        let mut dir_changes = Vec::new();
        let mut file_changes = Vec::new();
        let mut headline_changes = Vec::new();

        // Track what we've seen to detect deletions
        let mut seen_dirs: HashMap<String, bool> = HashMap::new();
        let mut seen_files: HashMap<String, bool> = HashMap::new();

        // Walk the directory tree
        let mut entry_count = 0;
        let mut org_file_count = 0;
        for entry in WalkDir::new(&self.root_directory)
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            entry_count += 1;
            let path = entry.path();

            if entry.file_type().is_dir() && path != self.root_directory {
                // Process directory
                let dir_id = generate_directory_id(path, &self.root_directory);
                seen_dirs.insert(dir_id.clone(), true);

                let parent_id = path
                    .parent()
                    .map(|p| {
                        if p == self.root_directory {
                            ROOT_ID.to_string()
                        } else {
                            generate_directory_id(p, &self.root_directory)
                        }
                    })
                    .unwrap_or_else(|| ROOT_ID.to_string());

                let depth = path
                    .strip_prefix(&self.root_directory)
                    .map(|p| p.components().count() as i64)
                    .unwrap_or(1);

                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                // Check if this is a new directory
                if !old_state.known_dirs.contains_key(&dir_id) {
                    let dir = Directory::new(dir_id.clone(), name, parent_id, depth);
                    dir_changes.push(Change::Created {
                        data: dir,
                        origin: origin.clone(),
                    });
                }

                new_state.known_dirs.insert(dir_id, true);
            } else if path.extension().map(|e| e == "org").unwrap_or(false) {
                // Process .org file
                org_file_count += 1;
                tracing::debug!("[OrgModeSyncProvider] Found .org file: {}", path.display());
                let file_id = generate_file_id(path);
                seen_files.insert(file_id.clone(), true);

                let content = match std::fs::read_to_string(path) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!("Failed to read {}: {}", path.display(), e);
                        continue;
                    }
                };

                let content_hash = compute_content_hash(&content);

                // Check if file has changed
                let file_changed = old_state
                    .file_hashes
                    .get(&file_id)
                    .map(|old_hash| old_hash != &content_hash)
                    .unwrap_or(true); // New file = changed

                if file_changed {
                    let parent_id = path
                        .parent()
                        .map(|p| {
                            if p == self.root_directory {
                                ROOT_ID.to_string()
                            } else {
                                generate_directory_id(p, &self.root_directory)
                            }
                        })
                        .unwrap_or_else(|| ROOT_ID.to_string());

                    let parent_depth = path
                        .strip_prefix(&self.root_directory)
                        .map(|p| p.components().count() as i64 - 1)
                        .unwrap_or(0);

                    let parse_result = parse_org_file(path, &content, &parent_id, parent_depth)?;

                    // Write back IDs for headlines that need them
                    if !parse_result.headlines_needing_ids.is_empty() {
                        write_id_properties(path, &parse_result.headlines_needing_ids)?;
                    }

                    // Emit file change
                    let is_new = !old_state.file_hashes.contains_key(&file_id);
                    if is_new {
                        file_changes.push(Change::Created {
                            data: parse_result.file,
                            origin: origin.clone(),
                        });
                    } else {
                        file_changes.push(Change::Updated {
                            id: file_id.clone(),
                            data: parse_result.file,
                            origin: origin.clone(),
                        });
                    }

                    // Emit headline changes (for simplicity, treat all as Updated)
                    for headline in parse_result.headlines {
                        headline_changes.push(Change::Updated {
                            id: headline.id.clone(),
                            data: headline,
                            origin: origin.clone(),
                        });
                    }
                }

                new_state.file_hashes.insert(file_id, content_hash);
            }
        }

        tracing::info!(
            "[OrgModeSyncProvider] Scan complete: {} total entries, {} .org files found",
            entry_count,
            org_file_count
        );

        // Detect deleted directories
        for old_dir_id in old_state.known_dirs.keys() {
            if !seen_dirs.contains_key(old_dir_id) {
                dir_changes.push(Change::Deleted {
                    id: old_dir_id.clone(),
                    origin: origin.clone(),
                });
            }
        }

        // Detect deleted files (and their headlines)
        for old_file_id in old_state.file_hashes.keys() {
            if !seen_files.contains_key(old_file_id) {
                file_changes.push(Change::Deleted {
                    id: old_file_id.clone(),
                    origin: origin.clone(),
                });
                // Note: Headlines from deleted files should be cleaned up
                // In production, we'd track headline IDs per file
            }
        }

        Ok((new_state, dir_changes, file_changes, headline_changes))
    }
}

impl DirectoryChangeProvider for OrgModeSyncProvider {
    fn subscribe_directories(&self) -> broadcast::Receiver<ChangesWithMetadata<Directory>> {
        self.directory_tx.subscribe()
    }

    fn root_directory(&self) -> std::path::PathBuf {
        self.root_directory.clone()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl SyncableProvider for OrgModeSyncProvider {
    fn provider_name(&self) -> &str {
        "orgmode"
    }

    #[tracing::instrument(name = "provider.orgmode.sync", skip(self, _position))]
    async fn sync(&self, _position: StreamPosition) -> Result<StreamPosition> {
        use tracing::{debug, info};

        info!(
            "[OrgModeSyncProvider] Starting sync for directory: {}",
            self.root_directory.display()
        );

        // Check if directory exists
        if !self.root_directory.exists() {
            info!(
                "[OrgModeSyncProvider] WARNING: Root directory does not exist: {}",
                self.root_directory.display()
            );
        }

        // Load current state
        let old_state = self.load_state().await?;

        // Scan directory and compute changes
        let (new_state, dir_changes, file_changes, headline_changes) =
            self.scan_and_compute_changes(&old_state).await?;

        // Serialize new state for position
        let state_bytes = serde_json::to_vec(&new_state)
            .map_err(|e| format!("Failed to serialize sync state: {}", e))?;
        let new_position = StreamPosition::Version(state_bytes);

        // Create sync token update
        let sync_token_update = SyncTokenUpdate {
            provider_name: self.provider_name().to_string(),
            position: new_position.clone(),
        };

        let trace_context = holon_api::BatchTraceContext::from_current_span();

        // Create metadata for each stream
        let dir_metadata = BatchMetadata {
            relation_name: "directories".to_string(),
            trace_context: trace_context.clone(),
            sync_token: Some(sync_token_update.clone()),
        };

        let file_metadata = BatchMetadata {
            relation_name: "org_files".to_string(),
            trace_context: trace_context.clone(),
            sync_token: Some(sync_token_update.clone()),
        };

        let headline_metadata = BatchMetadata {
            relation_name: "org_headlines".to_string(),
            trace_context,
            sync_token: Some(sync_token_update),
        };

        // Log stats
        info!(
            "[OrgModeSyncProvider] Emitting {} directory, {} file, {} headline changes",
            dir_changes.len(),
            file_changes.len(),
            headline_changes.len()
        );

        // Emit changes on streams
        let _ = self.directory_tx.send(WithMetadata {
            inner: dir_changes,
            metadata: dir_metadata,
        });

        let _ = self.file_tx.send(WithMetadata {
            inner: file_changes,
            metadata: file_metadata,
        });

        let _ = self.headline_tx.send(WithMetadata {
            inner: headline_changes,
            metadata: headline_metadata,
        });

        Ok(new_position)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OperationProvider for OrgModeSyncProvider {
    fn operations(&self) -> Vec<OperationDescriptor> {
        vec![generate_sync_operation(self.provider_name())]
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        _params: StorageEntity,
    ) -> Result<UndoAction> {
        let expected_entity_name = format!("{}.sync", self.provider_name());
        if entity_name != expected_entity_name {
            return Err(format!(
                "Expected entity_name '{}', got '{}'",
                expected_entity_name, entity_name
            )
            .into());
        }

        if op_name != "sync" {
            return Err(format!("Expected op_name 'sync', got '{}'", op_name).into());
        }

        self.sync(StreamPosition::Beginning).await?;
        Ok(UndoAction::Irreversible)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::RwLock;
    use tempfile::tempdir;

    /// Simple in-memory mock for SyncTokenStore
    struct MockSyncTokenStore {
        tokens: RwLock<HashMap<String, StreamPosition>>,
    }

    impl MockSyncTokenStore {
        fn new() -> Self {
            Self {
                tokens: RwLock::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl SyncTokenStore for MockSyncTokenStore {
        async fn load_token(&self, provider_name: &str) -> Result<Option<StreamPosition>> {
            Ok(self.tokens.read().unwrap().get(provider_name).cloned())
        }
        async fn save_token(&self, provider_name: &str, position: StreamPosition) -> Result<()> {
            self.tokens
                .write()
                .unwrap()
                .insert(provider_name.to_string(), position);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_sync_empty_directory() {
        let dir = tempdir().unwrap();
        let token_store = Arc::new(MockSyncTokenStore::new());
        let provider = OrgModeSyncProvider::new(dir.path().to_path_buf(), token_store);

        let result = provider.sync(StreamPosition::Beginning).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sync_with_org_file() {
        let dir = tempdir().unwrap();
        let org_file = dir.path().join("test.org");
        std::fs::write(&org_file, "* Headline 1\n** Nested headline\n").unwrap();

        let token_store = Arc::new(MockSyncTokenStore::new());
        let provider = OrgModeSyncProvider::new(dir.path().to_path_buf(), token_store);

        let mut headline_rx = provider.subscribe_headlines();
        let mut file_rx = provider.subscribe_files();

        let result = provider.sync(StreamPosition::Beginning).await;
        assert!(result.is_ok());

        // Check that we received file changes
        let file_batch = file_rx.try_recv().unwrap();
        assert_eq!(file_batch.inner.len(), 1);

        // Check that we received headline changes
        let headline_batch = headline_rx.try_recv().unwrap();
        assert_eq!(headline_batch.inner.len(), 2);
    }
}
