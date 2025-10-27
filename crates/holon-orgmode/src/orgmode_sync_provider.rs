//! Stream-based OrgModeSyncProvider
//!
//! This sync provider scans an org-mode directory and emits changes on typed streams.
//! Architecture:
//! - ONE sync() call → multiple typed streams (directories, files, blocks)
//! - Uses file content hashes for change detection
//! - Fire-and-forget operations - updates arrive via streams

use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
use walkdir::WalkDir;

use holon::core::datasource::{
    generate_sync_operation, Change, ChangeOrigin, FieldDelta, OperationDescriptor,
    OperationProvider, OperationResult, Result, StreamPosition, SyncTokenStore, SyncableProvider,
};
use holon::storage::types::StorageEntity;
use holon_api::block::Block;
use holon_api::Value;
use holon_api::{BatchMetadata, SyncTokenUpdate, WithMetadata};

use holon_filesystem::{
    directory::{ChangesWithMetadata, DirectoryChangeProvider},
    directory::{Directory, ROOT_ID},
    File,
};

use crate::file_io::write_id_properties;
use crate::parser::{
    compute_content_hash, generate_directory_id, generate_file_id, parse_org_file,
};

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
    file_tx: broadcast::Sender<ChangesWithMetadata<File>>,
    block_tx: broadcast::Sender<ChangesWithMetadata<Block>>,
}

impl OrgModeSyncProvider {
    pub fn new(root_directory: PathBuf, token_store: Arc<dyn SyncTokenStore>) -> Self {
        Self {
            root_directory,
            token_store,
            directory_tx: broadcast::channel(1000).0,
            file_tx: broadcast::channel(1000).0,
            block_tx: broadcast::channel(1000).0,
        }
    }

    pub fn subscribe_directories(&self) -> broadcast::Receiver<ChangesWithMetadata<Directory>> {
        self.directory_tx.subscribe()
    }

    pub fn subscribe_files(&self) -> broadcast::Receiver<ChangesWithMetadata<File>> {
        self.file_tx.subscribe()
    }

    pub fn subscribe_blocks(&self) -> broadcast::Receiver<ChangesWithMetadata<Block>> {
        self.block_tx.subscribe()
    }

    /// Legacy alias for subscribe_blocks
    #[deprecated(note = "Use subscribe_blocks instead")]
    pub fn subscribe_headlines(&self) -> broadcast::Receiver<ChangesWithMetadata<Block>> {
        self.subscribe_blocks()
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
        Vec<Change<File>>,
        Vec<Change<Block>>,
    )> {
        let origin = ChangeOrigin::remote_with_current_span();
        let mut new_state = SyncState::default();
        let mut dir_changes = Vec::new();
        let mut file_changes = Vec::new();
        let mut block_changes = Vec::new();

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
                let file_id = generate_file_id(path, &self.root_directory);
                seen_files.insert(file_id.clone(), true);

                let content = match std::fs::read_to_string(path) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!("Failed to read {}: {}", path.display(), e);
                        continue;
                    }
                };

                let mut content_hash = compute_content_hash(&content);
                let file_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

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

                    let mut parse_result = parse_org_file(
                        path,
                        &content,
                        &parent_id,
                        parent_depth,
                        &self.root_directory,
                    )?;

                    // Write back IDs for blocks that need them
                    if !parse_result.headlines_needing_ids.is_empty() {
                        write_id_properties(
                            path,
                            &parse_result.headlines_needing_ids,
                            &parse_result.document,
                            &parse_result.blocks,
                        )?;

                        // Re-read and re-parse the file after writing IDs to get the updated content
                        let updated_content = std::fs::read_to_string(path).map_err(|e| {
                            format!("Failed to re-read file after writing IDs: {}", e)
                        })?;
                        parse_result = parse_org_file(
                            path,
                            &updated_content,
                            &parent_id,
                            parent_depth,
                            &self.root_directory,
                        )?;

                        // Update content_hash to reflect the written file
                        content_hash = compute_content_hash(&updated_content);
                    }

                    // Emit file change
                    let file = File::new(
                        file_id.clone(),
                        file_name.clone(),
                        parent_id.clone(),
                        content_hash.clone(),
                        None, // document_id set later by OrgAdapter
                    );
                    let is_new = !old_state.file_hashes.contains_key(&file_id);
                    if is_new {
                        file_changes.push(Change::Created {
                            data: file,
                            origin: origin.clone(),
                        });
                    } else {
                        file_changes.push(Change::Updated {
                            id: file_id.clone(),
                            data: file,
                            origin: origin.clone(),
                        });
                    }

                    // Emit block changes (for simplicity, treat all as Updated)
                    for block in parse_result.blocks {
                        block_changes.push(Change::Updated {
                            id: block.id.clone(),
                            data: block,
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

        // Detect deleted files (and their blocks)
        for old_file_id in old_state.file_hashes.keys() {
            if !seen_files.contains_key(old_file_id) {
                file_changes.push(Change::Deleted {
                    id: old_file_id.clone(),
                    origin: origin.clone(),
                });
                // Note: Blocks from deleted files should be cleaned up
                // In production, we'd track block IDs per file
            }
        }

        Ok((new_state, dir_changes, file_changes, block_changes))
    }

    /// Sync a single file - parses and emits changes for just this file
    ///
    /// This is an optimization for file-specific sync after operations.
    /// Instead of scanning the entire directory tree, we just re-parse the modified file.
    pub async fn sync_file(&self, file_path: &std::path::Path) -> Result<()> {
        use tracing::info;

        info!(
            "[OrgModeSyncProvider] Syncing single file: {}",
            file_path.display()
        );

        // Load current state
        let old_state = self.load_state().await?;
        let origin = ChangeOrigin::remote_with_current_span();

        // Read and parse the file
        let content = std::fs::read_to_string(file_path)
            .map_err(|e| format!("Failed to read file {}: {}", file_path.display(), e))?;

        let file_id = generate_file_id(file_path, &self.root_directory);
        let mut content_hash = compute_content_hash(&content);

        // Determine parent info
        let parent_id = file_path
            .parent()
            .map(|p| {
                if p == self.root_directory {
                    ROOT_ID.to_string()
                } else {
                    generate_directory_id(p, &self.root_directory)
                }
            })
            .unwrap_or_else(|| ROOT_ID.to_string());

        let parent_depth = file_path
            .strip_prefix(&self.root_directory)
            .map(|p| p.components().count() as i64 - 1)
            .unwrap_or(0);

        // Parse the file
        let mut parse_result = parse_org_file(
            file_path,
            &content,
            &parent_id,
            parent_depth,
            &self.root_directory,
        )?;

        // Write back IDs for blocks that need them
        if !parse_result.headlines_needing_ids.is_empty() {
            write_id_properties(
                file_path,
                &parse_result.headlines_needing_ids,
                &parse_result.document,
                &parse_result.blocks,
            )?;

            // Re-read and re-parse the file after writing IDs to get the updated content
            let updated_content = std::fs::read_to_string(file_path)
                .map_err(|e| format!("Failed to re-read file after writing IDs: {}", e))?;
            parse_result = parse_org_file(
                file_path,
                &updated_content,
                &parent_id,
                parent_depth,
                &self.root_directory,
            )?;

            // Update content_hash to reflect the written file
            content_hash = compute_content_hash(&updated_content);
        }

        // Check if file has changed (after writing IDs if needed)
        let file_changed = old_state
            .file_hashes
            .get(&file_id)
            .map(|old_hash| old_hash != &content_hash)
            .unwrap_or(true); // New file = changed

        // Update state with new file hash (even if file hasn't changed, to keep state in sync)
        let mut new_state = old_state.clone();
        new_state
            .file_hashes
            .insert(file_id.clone(), content_hash.clone());

        if !file_changed {
            // File hasn't changed, but we still need to save the state in case it was updated by an operation
            // Serialize new state for position
            let state_bytes = serde_json::to_vec(&new_state)
                .map_err(|e| format!("Failed to serialize sync state: {}", e))?;
            let new_position = StreamPosition::Version(state_bytes);

            // Save state to token store
            self.token_store
                .save_token(self.provider_name(), new_position)
                .await?;

            info!(
                "[OrgModeSyncProvider] File {} has not changed, skipping sync but updating state",
                file_path.display()
            );
            return Ok(());
        }

        // Determine if this is a new file or update
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();
        let file = File::new(
            file_id.clone(),
            file_name,
            parent_id,
            content_hash.clone(),
            None,
        );
        let is_new = !old_state.file_hashes.contains_key(&file_id);
        let file_change = if is_new {
            Change::Created {
                data: file,
                origin: origin.clone(),
            }
        } else {
            Change::Updated {
                id: file_id.clone(),
                data: file,
                origin: origin.clone(),
            }
        };

        // Collect block changes
        let block_changes: Vec<Change<Block>> = parse_result
            .blocks
            .into_iter()
            .map(|block| Change::Updated {
                id: block.id.clone(),
                data: block,
                origin: origin.clone(),
            })
            .collect();

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

        // Emit file change
        let file_metadata = BatchMetadata {
            relation_name: "files".to_string(),
            trace_context: trace_context.clone(),
            sync_token: Some(sync_token_update.clone()),
        };

        let _ = self.file_tx.send(WithMetadata {
            inner: vec![file_change],
            metadata: file_metadata,
        });

        // Emit block changes
        let block_metadata = BatchMetadata {
            relation_name: "blocks".to_string(),
            trace_context,
            sync_token: Some(sync_token_update),
        };

        info!(
            "[OrgModeSyncProvider] Emitting 1 file, {} block changes for {}",
            block_changes.len(),
            file_path.display()
        );

        let _ = self.block_tx.send(WithMetadata {
            inner: block_changes,
            metadata: block_metadata,
        });

        // Save state to token store
        self.token_store
            .save_token(self.provider_name(), new_position)
            .await?;

        Ok(())
    }

    /// Extract file paths from FieldDeltas
    ///
    /// FieldDeltas now include a "file_path" field for operations that modify files.
    /// This extracts unique file paths from those FieldDeltas.
    fn extract_file_paths_from_deltas(
        &self,
        changes: &[FieldDelta],
    ) -> std::collections::HashSet<PathBuf> {
        let mut file_paths = std::collections::HashSet::new();

        for delta in changes {
            // Look for FieldDeltas with field name "file_path"
            if delta.field == "file_path" {
                // Extract file path from new_value (or old_value if new_value is null)
                if let Value::String(path_str) = &delta.new_value {
                    if !path_str.is_empty() {
                        file_paths.insert(PathBuf::from(path_str));
                    }
                } else if let Value::String(path_str) = &delta.old_value {
                    if !path_str.is_empty() {
                        file_paths.insert(PathBuf::from(path_str));
                    }
                }
            }
        }

        file_paths
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

    #[tracing::instrument(name = "provider.orgmode.sync", skip(self, position))]
    async fn sync(&self, position: StreamPosition) -> Result<StreamPosition> {
        use tracing::info;

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

        // Load current state based on position
        // StreamPosition::Beginning means start fresh (ignore stored state)
        let old_state = match position {
            StreamPosition::Beginning => {
                info!("[OrgModeSyncProvider] Starting fresh sync (Beginning position)");
                SyncState::default()
            }
            StreamPosition::Version(_) => self.load_state().await?,
        };

        // Scan directory and compute changes
        let (new_state, dir_changes, file_changes, block_changes) =
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
            relation_name: "files".to_string(),
            trace_context: trace_context.clone(),
            sync_token: Some(sync_token_update.clone()),
        };

        let block_metadata = BatchMetadata {
            relation_name: "blocks".to_string(),
            trace_context,
            sync_token: Some(sync_token_update),
        };

        // Log stats
        info!(
            "[OrgModeSyncProvider] Emitting {} directory, {} file, {} block changes",
            dir_changes.len(),
            file_changes.len(),
            block_changes.len()
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

        let _ = self.block_tx.send(WithMetadata {
            inner: block_changes,
            metadata: block_metadata,
        });

        Ok(new_position)
    }

    /// Optimized sync for post-operation changes
    ///
    /// IMPORTANT: Operations write files directly but don't return FieldDeltas yet (see TODO in OperationWrapper).
    /// Since operations already wrote files, we should NOT re-read and re-sync from files (causes duplicates).
    /// Instead, we just update the sync state hash to reflect that files are now in sync.
    ///
    /// Once operations return OperationResult with FieldDeltas, we can:
    /// 1. Extract file paths from FieldDeltas
    /// 2. Update sync state hash for those files
    /// 3. Optionally emit changes based on FieldDeltas (if needed for cache updates)
    #[tracing::instrument(name = "provider.orgmode.sync_changes", skip(self, changes))]
    async fn sync_changes(&self, changes: &[FieldDelta]) -> Result<()> {
        use tracing::info;

        // TODO: Once operations return OperationResult with FieldDeltas, extract file paths from changes
        // For now, operations don't return FieldDeltas, so changes is always empty
        // Since operations write files directly, we should NOT sync from files (would cause duplicates)
        // Instead, we need to update sync state hash for affected files

        if changes.is_empty() {
            // No FieldDeltas available - operations wrote files but didn't tell us which ones
            // We can't safely update sync state without knowing which files changed
            // For now, skip sync entirely - operations already wrote files
            // TODO: Once operations return FieldDeltas, extract file paths and update sync state
            info!(
                "[OrgModeSyncProvider] sync_changes: No FieldDeltas available (operations don't return them yet), skipping sync to avoid duplicates"
            );
            return Ok(());
        }

        // Try to extract file paths from the changes
        let file_paths = self.extract_file_paths_from_deltas(changes);

        if file_paths.is_empty() {
            // FieldDeltas available but can't extract file paths
            // This shouldn't happen once FieldDeltas include file_path
            info!(
                "[OrgModeSyncProvider] sync_changes: FieldDeltas available but no file paths extracted"
            );
            return Ok(());
        }

        // Update sync state hash for affected files without emitting changes
        // (operations already updated database and wrote files)
        info!(
            "[OrgModeSyncProvider] sync_changes: Updating sync state for {} files",
            file_paths.len()
        );

        let old_state = self.load_state().await?;
        let mut new_state = old_state.clone();

        for file_path in file_paths {
            let file_id = generate_file_id(&file_path, &self.root_directory);
            let content = std::fs::read_to_string(&file_path)
                .map_err(|e| format!("Failed to read file {}: {}", file_path.display(), e))?;
            let content_hash = compute_content_hash(&content);
            new_state.file_hashes.insert(file_id, content_hash);
        }

        let state_bytes = serde_json::to_vec(&new_state)
            .map_err(|e| format!("Failed to serialize sync state: {}", e))?;
        let new_position = StreamPosition::Version(state_bytes);

        self.token_store
            .save_token(self.provider_name(), new_position)
            .await?;

        Ok(())
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
    ) -> Result<OperationResult> {
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
        // Sync operations don't have FieldDeltas - they scan everything
        Ok(OperationResult::irreversible(Vec::new()))
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
        async fn clear_all_tokens(&self) -> Result<()> {
            self.tokens.write().unwrap().clear();
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

        let mut block_rx = provider.subscribe_blocks();
        let mut file_rx = provider.subscribe_files();

        let result = provider.sync(StreamPosition::Beginning).await;
        assert!(result.is_ok());

        // Check that we received file changes
        let file_batch = file_rx.try_recv().unwrap();
        assert_eq!(file_batch.inner.len(), 1);

        // Check that we received block changes
        let block_batch = block_rx.try_recv().unwrap();
        assert_eq!(block_batch.inner.len(), 2);
    }
}
