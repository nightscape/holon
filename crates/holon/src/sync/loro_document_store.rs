//! LoroDocumentStore - manages multiple Loro documents, one per org file
//!
//! Maps org file paths to LoroDocument instances and handles persistence.
//! Uses CanonicalPath to ensure symlinks are resolved, preventing duplicate
//! documents for the same file accessed via different paths.

use crate::api::LoroBackend;
use crate::sync::LoroDocument;
use crate::sync::canonical_path::CanonicalPath;
use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Manages multiple Loro documents, one per org file.
///
/// Each org file gets its own LoroDocument instance, stored in `.loro` files
/// alongside the org files.
///
/// Uses `CanonicalPath` internally to ensure that symlinks are resolved,
/// preventing duplicate documents for the same file accessed via different paths
/// (e.g., `/var/...` vs `/private/var/...` on macOS).
#[derive(Clone)]
pub struct LoroDocumentStore {
    /// Map from canonical org file path to LoroDocument
    docs: Arc<RwLock<HashMap<CanonicalPath, Arc<LoroDocument>>>>,
    /// Directory where .loro files are stored (typically alongside org files)
    storage_dir: PathBuf,
    /// Aliases mapping alternate doc_ids (e.g. UUIDs) to canonical file paths.
    /// This allows LoroBlockOperations to resolve UUID-based document references
    /// back to the file-path-keyed LoroDocument.
    doc_id_aliases: Arc<RwLock<HashMap<String, CanonicalPath>>>,
}

impl LoroDocumentStore {
    /// Create a new document store.
    ///
    /// # Arguments
    /// * `storage_dir` - Directory where .loro snapshot files are stored
    pub fn new(storage_dir: PathBuf) -> Self {
        Self {
            docs: Arc::new(RwLock::new(HashMap::new())),
            storage_dir,
            doc_id_aliases: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the storage directory for this document store.
    pub fn storage_dir(&self) -> &Path {
        &self.storage_dir
    }

    /// Get the path to the .loro file for a given document path.
    ///
    /// If the path's parent is "." or empty (relative path), uses storage_dir.
    /// Otherwise uses the path's parent directory.
    fn loro_file_path(&self, doc_path: &Path) -> PathBuf {
        let file_name = doc_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");
        let doc_dir = doc_path.parent().unwrap_or(Path::new("."));

        // Use storage_dir for relative paths to avoid polluting CWD
        let target_dir = if doc_dir == Path::new(".") || doc_dir == Path::new("") {
            &self.storage_dir
        } else {
            doc_dir
        };

        target_dir.join(format!("{}.loro", file_name))
    }

    /// Generate a document ID from a file path.
    ///
    /// Uses relative paths from storage_dir for portability.
    /// Note: This is a generic path→ID mapping. Format-specific logic
    /// (like Org mode) should determine the doc_id at a higher layer.
    pub(crate) fn doc_id_from_path(&self, path: &Path) -> String {
        // Canonicalize both paths to handle symlinks (e.g., /var -> /private/var on macOS)
        let canonical_path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let canonical_root =
            std::fs::canonicalize(&self.storage_dir).unwrap_or_else(|_| self.storage_dir.clone());

        // Make path relative to storage_dir
        canonical_path
            .strip_prefix(&canonical_root)
            .map(|p| p.to_string_lossy().replace(std::path::MAIN_SEPARATOR, "/"))
            .unwrap_or_else(|_| {
                // Fallback: use just the filename if not under storage_dir
                path.file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| path.to_string_lossy().to_string())
            })
    }

    /// Register an alias doc_id (e.g. a UUID) that maps to a canonical file path.
    pub async fn register_alias(&self, alias_doc_id: &str, file_path: &Path) {
        let canonical = CanonicalPath::new(file_path);
        eprintln!(
            "[LoroDocumentStore] Registering alias: {} -> {}",
            alias_doc_id, canonical
        );
        self.doc_id_aliases
            .write()
            .await
            .insert(alias_doc_id.to_string(), canonical);
    }

    /// Resolve a doc_id that might be an alias (UUID) to the actual LoroDocument.
    pub async fn resolve_by_doc_id(&self, doc_id: &str) -> Option<Arc<LoroDocument>> {
        let docs = self.docs.read().await;

        // Direct match: check if any loaded doc has this doc_id
        for (_path, doc) in docs.iter() {
            if doc.doc_id() == doc_id {
                return Some(doc.clone());
            }
        }

        // Alias match: check if this doc_id is an alias for a file path
        let aliases = self.doc_id_aliases.read().await;
        if let Some(canonical_path) = aliases.get(doc_id) {
            if let Some(doc) = docs.get(canonical_path) {
                return Some(doc.clone());
            }
        }

        None
    }

    /// Resolve an alias doc_id to its canonical file path (without loading the document).
    pub async fn resolve_alias_to_path(&self, doc_id: &str) -> Option<PathBuf> {
        let aliases = self.doc_id_aliases.read().await;
        aliases.get(doc_id).map(|cp| cp.to_path_buf())
    }

    /// Get or load a LoroDocument for the given file path.
    ///
    /// If the document is already loaded, returns it.
    /// Otherwise, attempts to load from disk, or creates a new one if not found.
    ///
    /// The path is automatically canonicalized to handle symlinks.
    pub async fn get_or_load(&mut self, path: &Path) -> Result<Arc<LoroDocument>> {
        // Convert relative paths to absolute using storage_dir
        // This ensures OrgFileWriter writes to the correct directory
        let abs_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.storage_dir.join(path)
        };
        // Canonicalize path to handle symlinks (e.g., /var -> /private/var on macOS)
        // This prevents duplicate documents for the same file via different paths
        let canonical_path = CanonicalPath::new(&abs_path);
        let loro_path = self.loro_file_path(&canonical_path);
        let doc_id = self.doc_id_from_path(&canonical_path);

        // Check if already loaded
        {
            let docs = self.docs.read().await;
            if let Some(doc) = docs.get(&canonical_path) {
                return Ok(doc.clone());
            }
        }

        // Try to load from disk, handling corrupted files gracefully
        let doc = if loro_path.exists() {
            info!(
                "Loading LoroDoc from {} for file {}",
                loro_path.display(),
                canonical_path
            );
            match LoroDocument::load_from_file(&loro_path, doc_id.clone()).await {
                Ok(loaded_doc) => Arc::new(loaded_doc),
                Err(e) => {
                    // Handle corrupted .loro files by deleting and recreating
                    let error_str = e.to_string();
                    if error_str.contains("Decode error")
                        || error_str.contains("Invalid import data")
                    {
                        tracing::warn!(
                            "Corrupted .loro file detected at {}: {}. Deleting and creating fresh document.",
                            loro_path.display(),
                            e
                        );
                        eprintln!(
                            "[LoroDocumentStore] Corrupted .loro file at {}: {}. Recreating.",
                            loro_path.display(),
                            e
                        );

                        // Delete the corrupted file
                        if let Err(del_err) = std::fs::remove_file(&loro_path) {
                            tracing::warn!(
                                "Failed to delete corrupted .loro file {}: {}",
                                loro_path.display(),
                                del_err
                            );
                        }

                        // Create a fresh document
                        let fresh_doc = Arc::new(LoroDocument::new(doc_id)?);
                        LoroBackend::initialize_schema_minimal(&fresh_doc)
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to initialize schema: {}", e))?;
                        fresh_doc
                    } else {
                        // For other errors, propagate them
                        return Err(e);
                    }
                }
            }
        } else {
            info!("Creating new LoroDoc for file {}", canonical_path);
            let doc = Arc::new(LoroDocument::new(doc_id)?);
            // Initialize the block schema for new documents
            LoroBackend::initialize_schema_minimal(&doc)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to initialize schema: {}", e))?;
            doc
        };

        // Store in memory
        {
            let mut docs = self.docs.write().await;
            docs.insert(canonical_path, doc.clone());
        }

        Ok(doc)
    }

    /// Save all loaded documents to disk.
    pub async fn save_all(&self) -> Result<()> {
        let docs = self.docs.read().await;
        for (canonical_path, doc) in docs.iter() {
            let loro_path = self.loro_file_path(canonical_path);
            if let Err(e) = doc.save_to_file(&loro_path).await {
                debug!("Failed to save LoroDoc for {}: {}", canonical_path, e);
            }
        }
        Ok(())
    }

    /// Save a specific document to disk.
    pub async fn save(&self, path: &Path) -> Result<()> {
        let canonical_path = CanonicalPath::new(path);
        let docs = self.docs.read().await;
        if let Some(doc) = docs.get(&canonical_path) {
            let loro_path = self.loro_file_path(&canonical_path);
            doc.save_to_file(&loro_path).await?;
        }
        Ok(())
    }

    /// Remove a document from memory (does not delete from disk).
    pub async fn remove(&mut self, path: &Path) {
        let canonical_path = CanonicalPath::new(path);
        let mut docs = self.docs.write().await;
        docs.remove(&canonical_path);
    }

    /// Get a document if it's already loaded (does not load from disk).
    pub async fn get(&self, path: &Path) -> Option<Arc<LoroDocument>> {
        let canonical_path = CanonicalPath::new(path);
        let docs = self.docs.read().await;
        docs.get(&canonical_path).cloned()
    }

    /// Get all loaded file paths (for iteration).
    pub async fn get_loaded_paths(&self) -> Vec<CanonicalPath> {
        let docs = self.docs.read().await;
        docs.keys().cloned().collect()
    }

    /// Iterate over all loaded documents (for subscription).
    pub async fn iter(&self) -> Vec<(CanonicalPath, Arc<LoroDocument>)> {
        let docs = self.docs.read().await;
        docs.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Load all existing .loro files from a root directory (recursively).
    ///
    /// Scans the root directory recursively for .loro files (stored alongside .org files)
    /// and loads them, returning the list of corresponding org file paths that were loaded.
    pub async fn load_all_existing(
        &mut self,
        root_dir: &Path,
    ) -> Result<Vec<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
        let mut loaded = Vec::new();

        // Recursively scan root_dir for .loro files (they are stored alongside .org files)
        if root_dir.exists() {
            fn walk_dir(dir: &Path, loro_files: &mut Vec<PathBuf>) -> std::io::Result<()> {
                for entry in std::fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        // Skip hidden directories
                        if !path
                            .file_name()
                            .map(|n| n.to_string_lossy().starts_with('.'))
                            .unwrap_or(false)
                        {
                            walk_dir(&path, loro_files)?;
                        }
                    } else if path.extension().map(|e| e == "loro").unwrap_or(false) {
                        loro_files.push(path);
                    }
                }
                Ok(())
            }

            let mut loro_files = Vec::new();
            walk_dir(root_dir, &mut loro_files)?;

            for loro_path in loro_files {
                // Derive org path from loro path (just change extension)
                let org_path = loro_path.with_extension("org");
                if self.get_or_load(&org_path).await.is_ok() {
                    info!("Loaded existing Loro document for {}", org_path.display());
                    loaded.push(org_path);
                }
            }
        }

        Ok(loaded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_get_or_load_creates_new() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let store_dir = temp_dir.path().to_path_buf();
        let mut store = LoroDocumentStore::new(store_dir);

        let org_path = temp_dir.path().join("test.org");
        let doc = store.get_or_load(&org_path).await?;

        assert_eq!(doc.doc_id(), "test.org");
        Ok(())
    }

    #[tokio::test]
    async fn test_get_or_load_reuses_existing() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let store_dir = temp_dir.path().to_path_buf();
        let mut store = LoroDocumentStore::new(store_dir);

        let org_path = temp_dir.path().join("test.org");
        let doc1 = store.get_or_load(&org_path).await?;
        let doc2 = store.get_or_load(&org_path).await?;

        // Should return the same Arc
        assert!(Arc::ptr_eq(&doc1, &doc2));
        Ok(())
    }

    #[tokio::test]
    async fn test_save_and_load() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let store_dir = temp_dir.path().to_path_buf();
        let mut store = LoroDocumentStore::new(store_dir.clone());

        let org_path = temp_dir.path().join("test.org");
        let doc1 = store.get_or_load(&org_path).await?;

        // Insert some data
        doc1.insert_text("test", 0, "Hello").await?;

        // Save
        store.save(&org_path).await?;

        // Create a new store and load
        let mut store2 = LoroDocumentStore::new(store_dir);
        let doc2 = store2.get_or_load(&org_path).await?;

        // Verify data persisted
        let text = doc2.get_text("test").await?;
        assert_eq!(text, "Hello");
        Ok(())
    }
}
