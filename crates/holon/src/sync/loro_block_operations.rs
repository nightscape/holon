//! Generic operations on Loro blocks.
//!
//! This is the primary operations layer for Loro. It's independent of any
//! specific persistence format (org-mode, JSON, etc.).
//!
//! ## Change Notifications
//!
//! `LoroBlockOperations` emits change notifications when blocks are created,
//! updated, or deleted. Subscribe via `subscribe()` to receive these changes.
//! The `QueryableCache` should be wired to receive these changes to stay in sync.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};

use holon_api::Value;
use holon_api::block::{Block, BlockContent};
use holon_api::streaming::{Change, ChangeOrigin};

use crate::api::{CoreOperations, LoroBackend};
use crate::core::datasource::{
    BlockDataSourceHelpers, BlockMaintenanceHelpers, BlockOperations, BlockQueryHelpers,
    CompletionStateInfo, CrudOperations, DataSource, HasCache, OperationDescriptor,
    OperationProvider, OperationRegistry, OperationResult, Result, TaskOperations,
    UnknownOperationError,
};
use crate::core::queryable_cache::QueryableCache;
use crate::storage::types::StorageEntity;
use crate::sync::LoroDocumentStore;
use crate::sync::document_entity::parse_document_uri;

/// Generic operations on Loro blocks.
///
/// Implements standard operation traits, delegating to LoroBackend.
/// Independent of persistence format (org-mode is just one adapter).
///
/// Emits change notifications via a broadcast channel when blocks are modified.
pub struct LoroBlockOperations {
    doc_store: Arc<RwLock<LoroDocumentStore>>,
    cache: Arc<QueryableCache<Block>>,
    /// Broadcast channel for change notifications
    change_tx: broadcast::Sender<Vec<Change<Block>>>,
}

impl LoroBlockOperations {
    pub fn new(
        doc_store: Arc<RwLock<LoroDocumentStore>>,
        cache: Arc<QueryableCache<Block>>,
    ) -> Self {
        let (change_tx, _) = broadcast::channel(100);
        Self {
            doc_store,
            cache,
            change_tx,
        }
    }

    /// Subscribe to change notifications.
    ///
    /// Returns a receiver that will receive batches of changes whenever
    /// blocks are created, updated, or deleted.
    pub fn subscribe(&self) -> broadcast::Receiver<Vec<Change<Block>>> {
        self.change_tx.subscribe()
    }

    /// Emit a change notification.
    fn emit_change(&self, change: Change<Block>) {
        eprintln!(
            "[LoroBlockOperations::emit_change] Sending change, receiver_count={}",
            self.change_tx.receiver_count()
        );
        let result = self.change_tx.send(vec![change]);
        match result {
            Ok(num_receivers) => eprintln!(
                "[LoroBlockOperations::emit_change] Sent to {} receivers",
                num_receivers
            ),
            Err(e) => eprintln!("[LoroBlockOperations::emit_change] Send failed: {:?}", e),
        }
    }

    /// Extract file path from doc_id (document URI).
    fn doc_id_to_path(doc_id: &str) -> String {
        parse_document_uri(doc_id)
            .map(|s| s.to_string())
            .unwrap_or_else(|| doc_id.to_string())
    }

    /// Get backend for a specific document.
    ///
    /// If `doc_id` is empty and no documents exist, creates a default document.
    /// If `doc_id` is provided but doesn't exist, creates the document.
    async fn get_backend(&self, doc_id: &str) -> Result<LoroBackend> {
        // Convert doc_id to file path for comparison
        let doc_path = Self::doc_id_to_path(doc_id);

        // First try with read lock
        let alias_path = {
            let store = self.doc_store.read().await;
            let loaded_docs: Vec<_> = store.iter().await;
            eprintln!(
                "[LoroBlockOperations::get_backend] doc_id={:?}, doc_path={:?}, loaded_docs={}",
                doc_id,
                doc_path,
                loaded_docs.len()
            );

            // Direct match by doc_id
            for (_path, collab_doc) in &loaded_docs {
                let collab_doc_id = collab_doc.doc_id();
                if collab_doc_id == doc_path || doc_id.is_empty() {
                    eprintln!(
                        "[LoroBlockOperations::get_backend] Found existing doc: {:?}",
                        collab_doc_id
                    );
                    return Ok(LoroBackend::from_document(collab_doc.clone()));
                }
            }

            // Try alias resolution (e.g. UUID → file path)
            if let Some(doc) = store.resolve_by_doc_id(&doc_path).await {
                eprintln!(
                    "[LoroBlockOperations::get_backend] Found doc via alias: {:?} -> {:?}",
                    doc_path,
                    doc.doc_id()
                );
                return Ok(LoroBackend::from_document(doc));
            }

            // Check if an alias exists but doc isn't loaded yet — use the alias path
            // so the document gets stored under its .org path (not the UUID path)
            store.resolve_alias_to_path(&doc_path).await
        };

        // No document found - create one
        let path = if doc_id.is_empty() {
            std::path::PathBuf::from("default.loro")
        } else if let Some(alias_path) = alias_path {
            eprintln!(
                "[LoroBlockOperations::get_backend] Using alias path: {:?} for doc_id {:?}",
                alias_path, doc_path
            );
            alias_path
        } else {
            std::path::PathBuf::from(&doc_path)
        };

        eprintln!(
            "[LoroBlockOperations::get_backend] Creating new document: {:?}",
            path
        );

        let mut store = self.doc_store.write().await;
        let collab_doc = store
            .get_or_load(&path)
            .await
            .map_err(|e| format!("Failed to create document: {}", e))?;

        eprintln!(
            "[LoroBlockOperations::get_backend] Document created/loaded, checking existing blocks"
        );
        // Check what blocks exist BEFORE initialization
        let backend_before = LoroBackend::from_document(collab_doc.clone());
        if let Ok(blocks_before) = backend_before
            .get_all_blocks(crate::api::types::Traversal::ALL_BUT_ROOT)
            .await
        {
            eprintln!(
                "[LoroBlockOperations::get_backend] Before initialize_schema_minimal: {} blocks exist: {:?}",
                blocks_before.len(),
                blocks_before.iter().map(|b| &b.id).collect::<Vec<_>>()
            );
        }

        // Initialize schema
        eprintln!("[LoroBlockOperations::get_backend] Calling initialize_schema_minimal");
        LoroBackend::initialize_schema_minimal(&collab_doc)
            .await
            .map_err(|e| format!("Failed to initialize schema: {}", e))?;

        eprintln!("[LoroBlockOperations::get_backend] Schema initialized");

        Ok(LoroBackend::from_document(collab_doc))
    }

    /// Find which document contains a block by ID.
    /// Returns (doc_path, backend) where doc_path is used for saving.
    async fn find_doc_for_block(&self, block_id: &str) -> Result<(String, LoroBackend)> {
        let store = self.doc_store.read().await;

        for (_path, collab_doc) in store.iter().await {
            let backend = LoroBackend::from_document(collab_doc);

            if let Ok(Some(_)) = backend.find_block_by_uuid(block_id).await {
                return Ok((backend.doc_id().to_string(), backend));
            }
            // Also check direct block ID
            if backend.get_block(block_id).await.is_ok() {
                return Ok((backend.doc_id().to_string(), backend));
            }
        }

        Err(format!("Block not found: {}", block_id).into())
    }

    /// Save a document after modification.
    async fn save_doc(&self, doc_path: &str) -> Result<()> {
        let store = self.doc_store.write().await;
        let path = std::path::Path::new(doc_path);
        store.save(path).await?;
        Ok(())
    }
}

#[async_trait]
impl DataSource<Block> for LoroBlockOperations {
    async fn get_all(&self) -> Result<Vec<Block>> {
        self.cache.get_all().await
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<Block>> {
        self.cache.get_by_id(id).await
    }
}

#[async_trait]
impl HasCache<Block> for LoroBlockOperations {
    fn get_cache(&self) -> &QueryableCache<Block> {
        &self.cache
    }
}

impl BlockQueryHelpers<Block> for LoroBlockOperations {}
impl BlockMaintenanceHelpers<Block> for LoroBlockOperations {}
impl BlockDataSourceHelpers<Block> for LoroBlockOperations {}
impl BlockOperations<Block> for LoroBlockOperations {}

#[async_trait]
impl CrudOperations<Block> for LoroBlockOperations {
    async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<OperationResult> {
        let (doc_path, backend) = self.find_doc_for_block(id).await?;

        match field {
            "content" => {
                if let Value::String(s) = &value {
                    backend
                        .update_block_text(id, s)
                        .await
                        .map_err(|e| format!("Failed to update content: {}", e))?;
                }
            }
            _ => {
                // Store in properties
                let mut props = HashMap::new();
                props.insert(field.to_string(), value);
                backend
                    .update_block_properties(id, &props)
                    .await
                    .map_err(|e| format!("Failed to update property: {}", e))?;
            }
        }

        self.save_doc(&doc_path).await?;

        // Emit change notification with updated block data
        if let Ok(block) = backend.get_block(id).await {
            self.emit_change(Change::Updated {
                id: id.to_string(),
                data: block,
                origin: ChangeOrigin::Local {
                    operation_id: None,
                    trace_id: None,
                },
            });
        }

        Ok(OperationResult::irreversible(vec![]))
    }

    async fn create(&self, fields: HashMap<String, Value>) -> Result<(String, OperationResult)> {
        // parent_id is required - it's either a document URI or a block ID
        let parent_id = fields
            .get("parent_id")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string())
            .ok_or_else(|| "parent_id is required for block creation")?;

        // Derive doc_id from parent_id
        let doc_id = if let Some(doc_path) = parse_document_uri(&parent_id) {
            // parent_id is a document URI like "holon-doc://test.org"
            doc_path.to_string()
        } else {
            // parent_id is a block ID - find its document
            let (doc_path, _) = self.find_doc_for_block(&parent_id).await?;
            doc_path
        };

        let content = fields
            .get("content")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string())
            .unwrap_or_default();

        let content_type = fields
            .get("content_type")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string());

        let source_language = fields
            .get("source_language")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string());

        let block_id = fields
            .get("id")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string());

        eprintln!(
            "[LoroBlockOperations::create] doc_id={:?}, block_id={:?}, parent_id={:?}, content_type={:?}, source_language={:?}",
            doc_id, block_id, parent_id, content_type, source_language
        );

        // Build the appropriate BlockContent based on content_type
        let block_content = if content_type.as_deref() == Some("source") {
            let lang = source_language.as_deref().unwrap_or("text");
            BlockContent::source(lang, content.clone())
        } else {
            BlockContent::text(content.clone())
        };

        let backend = self.get_backend(&doc_id).await?;

        // Check if block already exists (upsert behavior)
        let existing_block = if let Some(ref id) = block_id {
            backend.get_block(id).await.ok()
        } else {
            None
        };

        let block = if let Some(existing) = existing_block {
            // Block exists - update it instead of creating
            eprintln!(
                "[LoroBlockOperations::create] Block {} exists, updating instead",
                existing.id
            );

            // Check if parent_id has changed - if so, move the block first
            // This is critical: without this, the children_by_parent mapping stays stale
            // and the block gets rendered under its old parent
            if existing.parent_id != parent_id {
                use holon_api::document::is_document_uri;
                if is_document_uri(&existing.parent_id) && is_document_uri(&parent_id) {
                    // Both are document URIs (e.g., holon-doc://file.org vs holon-doc://{uuid}).
                    // The block is already at the document root — just update the stored parent_id.
                    eprintln!(
                        "[LoroBlockOperations::create] Parent URI alias change: {} -> {}, updating field only",
                        existing.parent_id, parent_id
                    );
                    backend
                        .update_parent_id(&existing.id, parent_id.clone())
                        .await
                        .map_err(|e| format!("Failed to update parent_id: {}", e))?;
                } else {
                    eprintln!(
                        "[LoroBlockOperations::create] Parent changed: {} -> {}, moving block",
                        existing.parent_id, parent_id
                    );
                    backend
                        .move_block(&existing.id, parent_id.clone(), None)
                        .await
                        .map_err(|e| format!("Failed to move block to new parent: {}", e))?;
                }
            }

            backend
                .update_block(&existing.id, block_content.clone())
                .await
                .map_err(|e| format!("Failed to update existing block: {}", e))?;
            backend
                .get_block(&existing.id)
                .await
                .map_err(|e| format!("Failed to get updated block: {}", e))?
        } else {
            // Block doesn't exist - create it
            backend
                .create_block(parent_id, block_content, block_id)
                .await
                .map_err(|e| format!("Failed to create block: {}", e))?
        };

        // Set additional properties (excluding fields handled above and source block fields)
        let mut props = HashMap::new();
        let handled_fields = [
            "parent_id",
            "content",
            "id",
            "content_type",
            "source_language",
            "source_name",
            "source_header_args",
            "source_results",
        ];
        for (key, value) in &fields {
            if !handled_fields.contains(&key.as_str()) {
                props.insert(key.clone(), value.clone());
            }
        }
        if !props.is_empty() {
            backend
                .update_block_properties(&block.id, &props)
                .await
                .map_err(|e| format!("Failed to set properties: {}", e))?;
        }

        // Save
        self.save_doc(&doc_id).await?;

        // Re-fetch the block to get updated properties
        let block_with_props = backend
            .get_block(&block.id)
            .await
            .map_err(|e| format!("Failed to get block after property update: {}", e))?;

        // Emit change notification
        self.emit_change(Change::Created {
            data: block_with_props.clone(),
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        Ok((block_with_props.id, OperationResult::irreversible(vec![])))
    }

    async fn delete(&self, id: &str) -> Result<OperationResult> {
        let (doc_path, backend) = self.find_doc_for_block(id).await?;

        backend
            .delete_block(id)
            .await
            .map_err(|e| format!("Failed to delete block: {}", e))?;

        self.save_doc(&doc_path).await?;

        // Emit change notification
        self.emit_change(Change::Deleted {
            id: id.to_string(),
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        Ok(OperationResult::irreversible(vec![]))
    }
}

impl LoroBlockOperations {
    /// Update a block with the given fields.
    ///
    /// Forwards to `create` which does upsert (create if not exists, update if exists).
    async fn update_block(&self, fields: HashMap<String, Value>) -> Result<OperationResult> {
        let (_block_id, result) = self.create(fields).await?;
        Ok(result)
    }
}

#[async_trait]
impl TaskOperations<Block> for LoroBlockOperations {
    async fn set_title(&self, id: &str, title: &str) -> Result<OperationResult> {
        // Get current content, replace first line
        if let Some(block) = self.cache.get_by_id(id).await? {
            let body: String = block.content.lines().skip(1).collect::<Vec<_>>().join("\n");
            let new_content = if body.is_empty() {
                title.to_string()
            } else {
                format!("{}\n{}", title, body)
            };
            self.set_field(id, "content", Value::String(new_content))
                .await
        } else {
            Err(format!("Block not found: {}", id).into())
        }
    }

    fn completion_states_with_progress(&self) -> Vec<CompletionStateInfo> {
        vec![
            CompletionStateInfo {
                state: "TODO".into(),
                progress: 0.0,
                is_done: false,
                is_active: true,
            },
            CompletionStateInfo {
                state: "DOING".into(),
                progress: 0.5,
                is_done: false,
                is_active: true,
            },
            CompletionStateInfo {
                state: "DONE".into(),
                progress: 1.0,
                is_done: true,
                is_active: false,
            },
        ]
    }

    async fn set_state(&self, id: &str, state: String) -> Result<OperationResult> {
        self.set_field(id, "TODO", Value::String(state)).await
    }

    async fn set_due_date(
        &self,
        id: &str,
        date: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<OperationResult> {
        match date {
            Some(dt) => {
                self.set_field(id, "DEADLINE", Value::String(dt.to_rfc3339()))
                    .await
            }
            None => self.set_field(id, "DEADLINE", Value::Null).await,
        }
    }

    async fn set_priority(&self, id: &str, priority: i64) -> Result<OperationResult> {
        self.set_field(id, "PRIORITY", Value::Integer(priority))
            .await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OperationProvider for LoroBlockOperations {
    fn operations(&self) -> Vec<OperationDescriptor> {
        use crate::__operations_has_cache;
        use crate::core::datasource::{
            __operations_crud_operation_provider, __operations_mutable_block_data_source,
            __operations_mutable_task_data_source,
        };

        let entity_name = Block::entity_name();
        let short_name = Block::short_name().expect("Block must have short_name");
        let id_column = "id";

        // Use resolver function for task_operations to resolve enum_from annotations
        let mut ops = __operations_mutable_task_data_source::task_operations_with_resolver(
            self,
            entity_name,
            short_name,
            entity_name,
            id_column,
        );

        // Add operations from other trait sources
        ops.extend(
            __operations_crud_operation_provider::crud_operations(
                entity_name,
                short_name,
                entity_name,
                id_column,
            )
            .into_iter(),
        );
        ops.extend(
            __operations_mutable_block_data_source::block_operations(
                entity_name,
                short_name,
                entity_name,
                id_column,
            )
            .into_iter(),
        );
        ops.extend(
            __operations_has_cache::has_cache(entity_name, short_name, entity_name, id_column)
                .into_iter(),
        );

        ops
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<OperationResult> {
        use crate::__operations_has_cache;
        use crate::core::datasource::{
            __operations_crud_operation_provider, __operations_mutable_block_data_source,
            __operations_mutable_task_data_source,
        };

        eprintln!(
            "[LoroBlockOperations::execute_operation] entity={}, op={}",
            entity_name, op_name
        );

        if entity_name != "blocks" {
            return Err(format!("Expected entity_name 'blocks', got '{}'", entity_name).into());
        }

        // Try HasCache operations (clear_cache)
        eprintln!("[LoroBlockOperations::execute_operation] Trying HasCache operations");
        match __operations_has_cache::dispatch_operation::<_, Block>(self, op_name, &params).await {
            Ok(op) => {
                eprintln!("[LoroBlockOperations::execute_operation] HasCache matched!");
                return Ok(op);
            }
            Err(err) => {
                if !UnknownOperationError::is_unknown(err.as_ref()) {
                    eprintln!(
                        "[LoroBlockOperations::execute_operation] HasCache error: {}",
                        err
                    );
                    return Err(err);
                }
            }
        }

        // Try CRUD operations
        eprintln!("[LoroBlockOperations::execute_operation] Trying CRUD operations");
        match __operations_crud_operation_provider::dispatch_operation::<_, Block>(
            self, op_name, &params,
        )
        .await
        {
            Ok(op) => {
                eprintln!("[LoroBlockOperations::execute_operation] CRUD matched!");
                return Ok(op);
            }
            Err(err) => {
                if !UnknownOperationError::is_unknown(err.as_ref()) {
                    eprintln!(
                        "[LoroBlockOperations::execute_operation] CRUD error: {}",
                        err
                    );
                    return Err(err);
                }
            }
        }

        // Handle "update" operation (forwards to create which does upsert)
        if op_name == "update" {
            eprintln!("[LoroBlockOperations::execute_operation] Handling update operation");
            return self.update_block(params).await;
        }

        // Try block operations
        match __operations_mutable_block_data_source::dispatch_operation::<_, Block>(
            self, op_name, &params,
        )
        .await
        {
            Ok(op) => return Ok(op),
            Err(err) => {
                if !UnknownOperationError::is_unknown(err.as_ref()) {
                    return Err(err);
                }
            }
        }

        // Try task operations
        __operations_mutable_task_data_source::dispatch_operation::<_, Block>(
            self, op_name, &params,
        )
        .await
    }
}
