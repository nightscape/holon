//! Org Adapter for handling file changes
//!
//! When Org files are modified externally, this adapter:
//! 1. Parses the Org file
//! 2. Compares with known Loro state
//! 3. Sends Commands (Operations) via OperationProvider (Command Bus)
//!
//! This keeps Org and Loro decoupled - they only communicate via Command Bus and Event Bus.
//!
//! The adapter also subscribes to block events from the EventBus to keep its known_state
//! in sync with blocks created/updated via UI mutations (not through Org files).

use anyhow::Result;
use holon::core::datasource::OperationProvider;
use holon::sync::event_bus::{EventBus, EventFilter, EventOrigin};
use holon::sync::{Document, DocumentOperations};
use holon_api::block::{Block, ROOT_DOC_ID};
use holon_api::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tracing::{debug, info};

use crate::models::OrgBlockExt;
use crate::parser::{generate_file_id, parse_org_file};
use crate::write_tracker::WriteTracker;

/// Adapter that handles external Org file changes
///
/// Sends Commands (Operations) via OperationProvider (Command Bus) instead of directly calling Loro.
/// This keeps Org and Loro decoupled - they only communicate via Command Bus and Event Bus.
pub struct OrgAdapter {
    /// OperationProvider (Command Bus) for executing commands
    command_bus: Arc<dyn OperationProvider>,
    /// Document operations for managing documents
    doc_ops: Arc<DocumentOperations>,
    /// Root directory for resolving relative paths
    root_dir: PathBuf,
    /// Write tracker to prevent sync loops
    write_tracker: Arc<RwLock<WriteTracker>>,
    /// Cache of known state per file (block_id -> Block)
    known_state: Arc<RwLock<HashMap<PathBuf, HashMap<String, Block>>>>,
    /// Loro document store for registering doc_id aliases (UUID → file path)
    loro_doc_store: Option<Arc<holon::sync::LoroDocumentStore>>,
}

impl OrgAdapter {
    /// Create a new OrgAdapter
    pub fn new(
        command_bus: Arc<dyn OperationProvider>,
        doc_ops: Arc<DocumentOperations>,
        root_dir: PathBuf,
    ) -> Self {
        Self {
            command_bus,
            doc_ops,
            root_dir,
            write_tracker: Arc::new(RwLock::new(WriteTracker::new())),
            known_state: Arc::new(RwLock::new(HashMap::new())),
            loro_doc_store: None,
        }
    }

    /// Create a new OrgAdapter with a shared WriteTracker
    pub fn with_write_tracker(
        command_bus: Arc<dyn OperationProvider>,
        doc_ops: Arc<DocumentOperations>,
        root_dir: PathBuf,
        write_tracker: Arc<RwLock<WriteTracker>>,
    ) -> Self {
        Self {
            command_bus,
            doc_ops,
            root_dir,
            write_tracker,
            known_state: Arc::new(RwLock::new(HashMap::new())),
            loro_doc_store: None,
        }
    }

    /// Set the Loro document store for registering doc_id aliases.
    pub fn with_loro_doc_store(mut self, store: Arc<holon::sync::LoroDocumentStore>) -> Self {
        self.loro_doc_store = Some(store);
        self
    }

    /// Start subscribing to block events from EventBus to keep known_state in sync.
    ///
    /// This is important because blocks can be created/updated via UI mutations
    /// (not through Org files), and we need to know about them to detect deletes
    /// when an Org file is externally modified.
    pub async fn start_event_subscription(
        self: Arc<Self>,
        event_bus: Arc<dyn EventBus>,
    ) -> Result<()> {
        let filter = EventFilter::new()
            .with_aggregate_type("block")
            .with_origin(EventOrigin::Loro);

        let mut event_stream = event_bus
            .subscribe(filter)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to subscribe to events: {}", e))?;

        let adapter = self.clone();
        tokio::spawn(async move {
            info!("[OrgAdapter] Started event subscription for known_state sync");

            while let Some(event) = event_stream.next().await {
                // Extract block data from event payload
                if let Some(data) = event.payload.get("data") {
                    if let Ok(block) = serde_json::from_value::<Block>(data.clone()) {
                        // Determine file path from parent_id (document URI)
                        let file_path = adapter.doc_uri_to_file_path(&block.parent_id);

                        let mut known_state = adapter.known_state.write().await;
                        let file_blocks = known_state
                            .entry(file_path.clone())
                            .or_insert_with(HashMap::new);

                        match event.event_type.as_str() {
                            "block.created" | "block.updated" => {
                                file_blocks.insert(block.id.clone(), block);
                            }
                            "block.deleted" => {
                                file_blocks.remove(&event.aggregate_id);
                            }
                            _ => {}
                        }
                    }
                } else if event.event_type == "block.deleted" {
                    // For deletes, we might not have data, just aggregate_id
                    // We need to find and remove from known_state
                    let mut known_state = adapter.known_state.write().await;
                    for (_, file_blocks) in known_state.iter_mut() {
                        file_blocks.remove(&event.aggregate_id);
                    }
                }
            }

            info!("[OrgAdapter] Event subscription ended");
        });

        Ok(())
    }

    /// Convert a document URI to a file path.
    fn doc_uri_to_file_path(&self, doc_uri: &str) -> PathBuf {
        // Strip "holon-doc://" prefix and add .org extension
        let rel_path = doc_uri.strip_prefix("holon-doc://").unwrap_or(doc_uri);
        self.root_dir.join(rel_path)
    }

    /// Get or create a document for the given file path.
    ///
    /// Path is relative to root (e.g., "projects/todo.org").
    /// Creates parent documents as needed.
    async fn get_or_create_document(&self, rel_path: &Path) -> Result<Document> {
        // Strip .org extension to get the document path
        let doc_path = rel_path.with_extension("");
        let segments: Vec<&str> = doc_path
            .components()
            .filter_map(|c| c.as_os_str().to_str())
            .collect();

        if segments.is_empty() {
            // Root document
            return self
                .doc_ops
                .get_by_id(ROOT_DOC_ID)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get root document: {}", e))?
                .ok_or_else(|| anyhow::anyhow!("Root document not found"));
        }

        let mut current_parent_id = ROOT_DOC_ID.to_string();
        let mut current_doc: Option<Document> = None;

        for segment in &segments {
            // Check if document exists under current parent
            match self
                .doc_ops
                .find_by_parent_and_name(&current_parent_id, segment)
                .await
            {
                Ok(Some(existing)) => {
                    current_parent_id = existing.id.clone();
                    current_doc = Some(existing);
                }
                Ok(None) => {
                    // Create new document
                    let new_doc = Document::new(
                        uuid::Uuid::new_v4().to_string(),
                        current_parent_id.clone(),
                        segment.to_string(),
                    );
                    let created = self
                        .doc_ops
                        .create(new_doc)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to create document: {}", e))?;
                    current_parent_id = created.id.clone();
                    current_doc = Some(created);
                }
                Err(e) => return Err(anyhow::anyhow!("Failed to find document: {}", e)),
            }
        }

        current_doc.ok_or_else(|| anyhow::anyhow!("Failed to resolve document"))
    }

    /// Handle a file change event
    ///
    /// This method is called by the file watcher when an Org file is modified.
    /// It parses the file, compares with known state, and applies changes to Loro.
    pub async fn on_file_changed(&self, path: &Path) -> Result<()> {
        // Skip if this is our own write
        {
            let tracker = self.write_tracker.read().await;
            if tracker.is_our_write(&path.to_path_buf()) {
                debug!("Skipping file change - our own write: {}", path.display());
                return Ok(());
            }
        }

        // Mark that we're processing this file - OrgFileWriter should not render it
        {
            let mut tracker = self.write_tracker.write().await;
            tracker.mark_external_processing(&path.to_path_buf());
        }

        info!("Processing external file change: {}", path.display());
        eprintln!(
            "[OrgAdapter] Processing external file change: {}",
            path.display()
        );

        // Get or create document for this file
        let rel_path = path.strip_prefix(&self.root_dir).map_err(|e| {
            anyhow::anyhow!(
                "File path {} is not under root {}: {}",
                path.display(),
                self.root_dir.display(),
                e
            )
        })?;
        let document = self.get_or_create_document(rel_path).await?;
        // Compute the document URI using the document's UUID (not the file path)
        let document_uri = format!("holon-doc://{}", document.id);

        // Register UUID → file path alias so LoroBlockOperations can find the right document
        if let Some(ref store) = self.loro_doc_store {
            store.register_alias(&document.id, path).await;
        }

        // Parse the Org file
        let content = tokio::fs::read_to_string(path).await?;
        let file_id = generate_file_id(path, &self.root_dir);
        let parse_result = parse_org_file(path, &content, "root", 0, &self.root_dir)?;

        eprintln!(
            "[OrgAdapter] Parsed file: {} blocks found, file_id={}",
            parse_result.blocks.len(),
            file_id
        );

        // Get current known state for this file
        let mut known_state = self.known_state.write().await;
        let known_blocks = known_state
            .entry(path.to_path_buf())
            .or_insert_with(HashMap::new);

        eprintln!(
            "[OrgAdapter] Known blocks: {} for {}",
            known_blocks.len(),
            path.display()
        );

        // Keep blocks in document order (parser returns them top-to-bottom)
        let current_blocks_vec = parse_result.blocks;
        // Also create a HashMap for fast lookups
        let current_blocks: HashMap<String, Block> = current_blocks_vec
            .iter()
            .map(|b| (b.id.clone(), b.clone()))
            .collect();

        eprintln!(
            "[OrgAdapter] Current blocks: {} (from parsed file)",
            current_blocks.len()
        );

        // Find new blocks (created) - iterate in document order so parents are created before children
        for block in &current_blocks_vec {
            if !known_blocks.contains_key(&block.id) {
                eprintln!("[OrgAdapter] Detected NEW block: {}", block.id);
                info!(
                    "Detected new block: {} in file {}",
                    block.id,
                    path.display()
                );
                if let Err(e) = self
                    .create_block_from_block(block, &file_id, &document_uri)
                    .await
                {
                    eprintln!("[OrgAdapter] ERROR creating block {}: {}", block.id, e);
                    return Err(e);
                }
            }
        }

        // Find updated blocks
        for (id, current_block) in &current_blocks {
            if let Some(known_block) = known_blocks.get(id) {
                if self.blocks_differ(known_block, current_block) {
                    info!("Detected updated block: {} in file {}", id, path.display());
                    self.update_block_from_block(current_block, &file_id, &document_uri)
                        .await?;
                }
            }
        }

        // Find deleted blocks
        for (id, _) in known_blocks.iter() {
            if !current_blocks.contains_key(id) {
                info!("Detected deleted block: {} in file {}", id, path.display());
                self.delete_block(id).await?;
            }
        }

        // Update known state
        *known_blocks = current_blocks;

        // Clear the external_processing flag now that all block operations are complete.
        // By this point, all creates/updates/deletes have been awaited synchronously,
        // so the Loro documents contain the final state. OrgFileWriter reads from Loro
        // directly (not Turso), so it will see the correct blocks when it renders.
        {
            let mut tracker = self.write_tracker.write().await;
            tracker.clear_external_processing(&path.to_path_buf());
        }

        Ok(())
    }

    /// Create a block in Loro from a Block
    ///
    /// Sends a "create" command via OperationProvider (Command Bus).
    ///
    /// # Arguments
    /// * `block` - The block to create
    /// * `file_id` - The file-based URI (e.g., "holon-doc://path.org") for matching top-level blocks
    /// * `document_uri` - The document UUID-based URI (e.g., "holon-doc://{uuid}") to use instead
    async fn create_block_from_block(
        &self,
        block: &Block,
        file_id: &str,
        document_uri: &str,
    ) -> Result<()> {
        // Convert properties
        let properties = self.block_to_properties(block);

        // Rewrite parent_id: if it's the file-based URI, replace with document UUID-based URI
        let parent_id = if block.parent_id == file_id {
            document_uri.to_string()
        } else {
            block.parent_id.clone()
        };

        // Build command parameters - parent_id is used to derive the document
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(block.id.clone()));
        params.insert("parent_id".to_string(), Value::String(parent_id));
        params.insert("content".to_string(), Value::String(block.content.clone()));
        params.insert(
            "content_type".to_string(),
            Value::String(block.content_type.clone()),
        );

        // Source block fields (only for source blocks)
        if block.content_type == "source" {
            if let Some(ref lang) = block.source_language {
                params.insert("source_language".to_string(), Value::String(lang.clone()));
            }
            if let Some(ref name) = block.source_name {
                params.insert("source_name".to_string(), Value::String(name.clone()));
            }
            let header_args = block.get_source_header_args();
            if !header_args.is_empty() {
                if let Ok(json) = serde_json::to_string(&header_args) {
                    params.insert("source_header_args".to_string(), Value::String(json));
                }
            }
        }

        // Add Org-specific fields (using extension trait)
        if let Some(task_state) = block.task_state() {
            params.insert("task_state".to_string(), Value::String(task_state));
        }
        if let Some(priority) = block.priority() {
            params.insert("priority".to_string(), Value::Integer(priority as i64));
        }
        if let Some(tags) = block.tags() {
            params.insert("tags".to_string(), Value::String(tags));
        }
        if let Some(scheduled) = block.scheduled() {
            params.insert("scheduled".to_string(), Value::String(scheduled));
        }
        if let Some(deadline) = block.deadline() {
            params.insert("deadline".to_string(), Value::String(deadline));
        }

        // Add properties
        for (k, v) in &properties {
            params.insert(k.clone(), v.clone());
        }

        // Execute command via Command Bus (OperationProvider)
        self.command_bus
            .execute_operation("blocks", "create", params)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute create command: {}", e))?;

        info!("Sent create command for block {}", block.id);
        Ok(())
    }

    /// Update a block in Loro from a Block
    ///
    /// Sends an "update" command via OperationProvider (Command Bus).
    ///
    /// # Arguments
    /// * `block` - The block to update
    /// * `file_id` - The file-based URI (e.g., "holon-doc://path.org") for matching top-level blocks
    /// * `document_uri` - The document UUID-based URI (e.g., "holon-doc://{uuid}") to use instead
    async fn update_block_from_block(
        &self,
        block: &Block,
        file_id: &str,
        document_uri: &str,
    ) -> Result<()> {
        // Convert properties
        let properties = self.block_to_properties(block);

        // Rewrite parent_id: if it's the file-based URI, replace with document UUID-based URI
        let parent_id = if block.parent_id == file_id {
            document_uri.to_string()
        } else {
            block.parent_id.clone()
        };

        // Build command parameters - parent_id is used to derive the document
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(block.id.clone()));
        params.insert("parent_id".to_string(), Value::String(parent_id));
        params.insert("content".to_string(), Value::String(block.content.clone()));
        params.insert(
            "content_type".to_string(),
            Value::String(block.content_type.clone()),
        );

        // Source block fields (only for source blocks)
        if block.content_type == "source" {
            if let Some(ref lang) = block.source_language {
                params.insert("source_language".to_string(), Value::String(lang.clone()));
            }
            if let Some(ref name) = block.source_name {
                params.insert("source_name".to_string(), Value::String(name.clone()));
            }
            let header_args = block.get_source_header_args();
            if !header_args.is_empty() {
                if let Ok(json) = serde_json::to_string(&header_args) {
                    params.insert("source_header_args".to_string(), Value::String(json));
                }
            }
        }

        // Add Org-specific fields (using extension trait)
        if let Some(task_state) = block.task_state() {
            params.insert("task_state".to_string(), Value::String(task_state));
        }
        if let Some(priority) = block.priority() {
            params.insert("priority".to_string(), Value::Integer(priority as i64));
        }
        if let Some(tags) = block.tags() {
            params.insert("tags".to_string(), Value::String(tags));
        }
        if let Some(scheduled) = block.scheduled() {
            params.insert("scheduled".to_string(), Value::String(scheduled));
        }
        if let Some(deadline) = block.deadline() {
            params.insert("deadline".to_string(), Value::String(deadline));
        }

        // Add properties
        for (k, v) in &properties {
            params.insert(k.clone(), v.clone());
        }

        // Execute command via Command Bus (OperationProvider)
        self.command_bus
            .execute_operation("blocks", "update", params)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute update command: {}", e))?;

        info!("Sent update command for block {}", block.id);
        Ok(())
    }

    /// Delete a block in Loro
    ///
    /// Sends a "delete" command via OperationProvider (Command Bus).
    async fn delete_block(&self, block_id: &str) -> Result<()> {
        // Build command parameters - block ID is enough, document is found automatically
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(block_id.to_string()));

        // Execute command via Command Bus (OperationProvider)
        self.command_bus
            .execute_operation("blocks", "delete", params)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute delete command: {}", e))?;

        info!("Sent delete command for block {}", block_id);
        Ok(())
    }

    /// Convert Block properties to HashMap
    fn block_to_properties(&self, block: &Block) -> HashMap<String, Value> {
        let mut properties = HashMap::new();

        // Ensure ID is in properties
        let id = block.get_block_id().unwrap_or_else(|| block.id.clone());
        properties.insert("ID".to_string(), Value::String(id));

        // Parse org_properties JSON if present
        if let Some(props_json) = block.org_properties() {
            if let Ok(props_map) =
                serde_json::from_str::<HashMap<String, serde_json::Value>>(&props_json)
            {
                for (k, v) in props_map {
                    let value = match v {
                        serde_json::Value::String(s) => Value::String(s),
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                Value::Integer(i)
                            } else if let Some(f) = n.as_f64() {
                                Value::Float(f)
                            } else {
                                Value::String(n.to_string())
                            }
                        }
                        serde_json::Value::Bool(b) => Value::Boolean(b),
                        _ => Value::String(v.to_string()),
                    };
                    properties.insert(k, value);
                }
            }
        }

        // Add Org-specific fields as properties (using extension trait)
        if let Some(task_state) = block.task_state() {
            properties.insert("TODO".to_string(), Value::String(task_state));
        }
        if let Some(priority) = block.priority() {
            properties.insert("PRIORITY".to_string(), Value::Integer(priority as i64));
        }
        if let Some(tags) = block.tags() {
            properties.insert("TAGS".to_string(), Value::String(tags));
        }
        if let Some(scheduled) = block.scheduled() {
            properties.insert("SCHEDULED".to_string(), Value::String(scheduled));
        }
        if let Some(deadline) = block.deadline() {
            properties.insert("DEADLINE".to_string(), Value::String(deadline));
        }

        properties
    }

    /// Check if two blocks differ
    fn blocks_differ(&self, a: &Block, b: &Block) -> bool {
        a.content != b.content
            || a.task_state() != b.task_state()
            || a.priority() != b.priority()
            || a.tags() != b.tags()
            || a.scheduled() != b.scheduled()
            || a.deadline() != b.deadline()
            || a.org_properties() != b.org_properties()
    }
}
