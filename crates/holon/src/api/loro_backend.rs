//! Loro-based implementation of DocumentRepository
//!
//! This module provides a CRDT-backed implementation using Loro with a normalized
//! adjacency-list data model for hierarchical blocks.

use super::repository::{CoreOperations, Lifecycle, P2POperations};
use super::types::NewBlock;
use crate::sync::LoroDocument;
use crate::sync::document_entity::{document_uri, is_document_uri};
use async_trait::async_trait;
use holon_api::block::NO_PARENT_ID;
use holon_api::streaming::{ChangeNotifications, ChangeSubscribers};
use holon_api::{
    ApiError, Block, BlockContent, Change, ChangeOrigin, SourceBlock, StreamPosition, Value,
};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use uuid::Uuid;

/// Helper trait for collecting and searching values in Loro containers
trait LoroListExt {
    /// Collect values by applying a function to each element, keeping only Some results
    fn collect_map<T, F>(&self, f: F) -> Vec<T>
    where
        F: FnMut(loro::ValueOrContainer) -> Option<T>;

    /// Find the index of the first element where the function returns Some(true)
    fn find_index<F>(&self, f: F) -> Option<usize>
    where
        F: FnMut(loro::ValueOrContainer) -> Option<bool>;
}

impl LoroListExt for loro::LoroList {
    fn collect_map<T, F>(&self, mut f: F) -> Vec<T>
    where
        F: FnMut(loro::ValueOrContainer) -> Option<T>,
    {
        let mut result = Vec::new();
        self.for_each(|v| {
            if let Some(value) = f(v) {
                result.push(value);
            }
        });
        result
    }

    fn find_index<F>(&self, mut f: F) -> Option<usize>
    where
        F: FnMut(loro::ValueOrContainer) -> Option<bool>,
    {
        let mut index = 0;
        let mut found = None;
        self.for_each(|v| {
            if found.is_none()
                && let Some(true) = f(v)
            {
                found = Some(index);
            }
            index += 1;
        });
        found
    }
}

/// Helper trait for extracting typed values from Loro maps
trait LoroMapExt {
    /// Get a value from the map and apply a function to the LoroValue
    /// Automatically unwraps the ValueOrContainer::Value variant
    fn get_typed<T, F>(&self, key: &str, f: F) -> Option<T>
    where
        F: FnOnce(&loro::LoroValue) -> Option<T>;
}

impl LoroMapExt for loro::LoroMap {
    fn get_typed<T, F>(&self, key: &str, f: F) -> Option<T>
    where
        F: FnOnce(&loro::LoroValue) -> Option<T>,
    {
        self.get(key).and_then(|v| match v {
            loro::ValueOrContainer::Value(val) => f(&val),
            _ => None,
        })
    }
}

// BlockContent field names (module-level constants)
const CONTENT_TYPE: &str = "content_type";
const CONTENT_RAW: &str = "content_raw";
const SOURCE_LANGUAGE: &str = "source_language";
const SOURCE_CODE: &str = "source_code";
const SOURCE_NAME: &str = "source_name";
const SOURCE_HEADER_ARGS: &str = "source_header_args";
#[allow(dead_code)]
const SOURCE_RESULTS: &str = "source_results";
const PROPERTIES: &str = "properties";

/// Read text content from a block map, supporting both LoroText (CRDT) and LWW string.
///
/// New blocks use a LoroText sub-container under CONTENT_RAW for CRDT text merging.
/// Old blocks may have a plain string value — read it for backward compatibility.
fn read_text_content(block_map: &loro::LoroMap) -> String {
    match block_map.get(CONTENT_RAW) {
        Some(loro::ValueOrContainer::Container(loro::Container::Text(text))) => text.to_string(),
        Some(loro::ValueOrContainer::Value(val)) => {
            val.as_string().map(|s| s.to_string()).unwrap_or_default()
        }
        _ => String::new(),
    }
}

/// Read source code from a block map, supporting both LoroText (CRDT) and LWW string.
fn read_source_code(block_map: &loro::LoroMap) -> String {
    match block_map.get(SOURCE_CODE) {
        Some(loro::ValueOrContainer::Container(loro::Container::Text(text))) => text.to_string(),
        Some(loro::ValueOrContainer::Value(val)) => {
            val.as_string().map(|s| s.to_string()).unwrap_or_default()
        }
        _ => String::new(),
    }
}

/// Read BlockContent from a Loro block map.
/// Handles backward compatibility with old string-only content.
fn read_content_from_map(block_map: &loro::LoroMap) -> BlockContent {
    let content_type =
        block_map.get_typed(CONTENT_TYPE, |val| val.as_string().map(|s| s.to_string()));

    match content_type.as_deref() {
        Some("source") => {
            let language = block_map.get_typed(SOURCE_LANGUAGE, |val| {
                val.as_string().map(|s| s.to_string())
            });
            let source = read_source_code(block_map);
            let name =
                block_map.get_typed(SOURCE_NAME, |val| val.as_string().map(|s| s.to_string()));

            let header_args: HashMap<String, Value> = block_map
                .get_typed(SOURCE_HEADER_ARGS, |val| {
                    val.as_string()
                        .and_then(|s| serde_json::from_str(s.as_ref()).ok())
                })
                .unwrap_or_default();

            BlockContent::Source(SourceBlock {
                language,
                source,
                name,
                header_args,
            })
        }
        Some("text") => {
            let raw = read_text_content(block_map);
            BlockContent::Text { raw }
        }
        _ => {
            // Backward compatibility: old format stored content as plain string
            let raw = block_map
                .get_typed("content", |val| val.as_string().map(|s| s.to_string()))
                .unwrap_or_default();
            BlockContent::Text { raw }
        }
    }
}

/// Read properties from a Loro block map.
fn read_properties_from_map(block_map: &loro::LoroMap) -> HashMap<String, Value> {
    block_map
        .get_typed(PROPERTIES, |val| {
            val.as_string()
                .and_then(|s| serde_json::from_str(s.as_ref()).ok())
        })
        .unwrap_or_default()
}

/// Read a complete Block from a Loro block map.
///
/// This creates a Block with the new flattened structure, using
/// Block::from_block_content() and setting properties/children.
fn read_block_from_map(id: String, block_map: &loro::LoroMap, children: Vec<String>) -> Block {
    let content = read_content_from_map(block_map);
    let properties = read_properties_from_map(block_map);

    let parent_id = block_map
        .get_typed("parent_id", |val| val.as_string().map(|s| s.to_string()))
        .unwrap_or_else(|| NO_PARENT_ID.to_string());

    let created_at = block_map
        .get_typed("created_at", |val| val.as_i64().copied())
        .unwrap_or(0);

    let updated_at = block_map
        .get_typed("updated_at", |val| val.as_i64().copied())
        .unwrap_or(0);

    let mut block = Block::from_block_content(id, parent_id, content);
    block.set_properties_map(properties);
    // Children relationship is established via parent_id and managed separately in Loro
    block.created_at = created_at;
    block.updated_at = updated_at;
    block
}

/// Update a LoroText sub-container within a map using CRDT diff-based update.
///
/// Uses `get_or_create_container` to get/create the LoroText, then `update()` which
/// computes a Myers' diff and applies character-level CRDT operations. This means
/// concurrent edits from different sources (Org file changes, P2P sync, UI) merge
/// properly instead of overwriting each other.
fn update_text_field(block_map: &loro::LoroMap, key: &str, new_text: &str) -> anyhow::Result<()> {
    let text = block_map.get_or_create_container(key, loro::LoroText::new())?;
    text.update(new_text, Default::default())
        .map_err(|e| anyhow::anyhow!("LoroText update failed: {:?}", e))?;
    Ok(())
}

/// Write BlockContent fields to a Loro block map.
///
/// Text content is stored in LoroText sub-containers for CRDT merging.
/// Metadata (content_type, language, etc.) remains as LWW values.
fn write_content_to_map(block_map: &loro::LoroMap, content: &BlockContent) -> anyhow::Result<()> {
    match content {
        BlockContent::Text { raw } => {
            block_map.insert(CONTENT_TYPE, loro::LoroValue::from("text"))?;
            update_text_field(block_map, CONTENT_RAW, raw)?;
        }
        BlockContent::Source(source) => {
            block_map.insert(CONTENT_TYPE, loro::LoroValue::from("source"))?;
            if let Some(lang) = &source.language {
                block_map.insert(SOURCE_LANGUAGE, loro::LoroValue::from(lang.as_str()))?;
            }
            update_text_field(block_map, SOURCE_CODE, &source.source)?;

            if let Some(name) = &source.name {
                block_map.insert(SOURCE_NAME, loro::LoroValue::from(name.as_str()))?;
            }

            if !source.header_args.is_empty() {
                let json = serde_json::to_string(&source.header_args)?;
                block_map.insert(SOURCE_HEADER_ARGS, loro::LoroValue::from(json.as_str()))?;
            }
        }
    }
    Ok(())
}

/// Write properties to a Loro block map.
fn write_properties_to_map(
    block_map: &loro::LoroMap,
    properties: &HashMap<String, Value>,
) -> anyhow::Result<()> {
    if !properties.is_empty() {
        let json = serde_json::to_string(properties)?;
        block_map.insert(PROPERTIES, loro::LoroValue::from(json.as_str()))?;
    }
    Ok(())
}

/// Loro-backed document repository implementation.
///
/// Uses a normalized data model:
/// - `blocks_by_id`: LoroMap<String, BlockData> - O(1) block lookup
/// - `children_by_parent`: LoroMap<String, LoroList<String>> - Parent → children mapping
///
/// Top-level blocks have `parent_id` set to the document URI (e.g., `holon-doc://my-doc.org`).
/// All other blocks nest under these top-level blocks transitively.
///
/// # Data Model
///
/// Each block in `blocks_by_id` contains:
/// - `content`: LoroText - CRDT text content
/// - `parent_id`: String - Parent block ID
/// - `created_at`: i64 - Unix timestamp (milliseconds)
/// - `updated_at`: i64 - Unix timestamp (milliseconds)
/// - `deleted_at`: i64 or null - Tombstone timestamp (null = not deleted)
pub struct LoroBackend {
    /// Loro CRDT document (storage only, transport decoupled)
    collab_doc: Arc<LoroDocument>,
    /// Active change notification subscribers
    subscribers: ChangeSubscribers<Block>,
    /// In-memory log of emitted changes for late subscribers
    event_log: Arc<Mutex<Vec<Change<Block>>>>,
}

impl Clone for LoroBackend {
    fn clone(&self) -> Self {
        Self {
            collab_doc: self.collab_doc.clone(),
            subscribers: self.subscribers.clone(),
            event_log: self.event_log.clone(),
        }
    }
}

impl LoroBackend {
    /// Create a LoroBackend from an existing LoroDocument.
    ///
    /// This is useful when you already have a LoroDocument (e.g., from LoroDocumentStore)
    /// and want to use LoroBackend's block operations on it.
    pub fn from_document(collab_doc: Arc<LoroDocument>) -> Self {
        Self {
            collab_doc,
            subscribers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            event_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get the document ID (delegates to LoroDocument).
    pub fn doc_id(&self) -> &str {
        self.collab_doc.doc_id()
    }

    /// Update a block's parent_id field without moving it in the tree.
    /// Used when both old and new parent_ids are document URIs (alias change).
    pub async fn update_parent_id(&self, id: &str, new_parent_id: String) -> Result<(), ApiError> {
        self.collab_doc
            .with_write(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);
                let block_map = Self::get_block_map(&blocks_map, id)?;
                block_map.insert("parent_id", loro::LoroValue::from(new_parent_id.as_str()))?;
                block_map.insert("updated_at", loro::LoroValue::from(Self::now_millis()))?;
                doc.commit();
                Ok(())
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to update parent_id: {}", e),
            })?;
        Ok(())
    }
}

impl LoroBackend {
    // Container name constants (our "schema")
    const BLOCKS_BY_ID: &'static str = "blocks_by_id";
    const CHILDREN_BY_PARENT: &'static str = "children_by_parent";
    const SCHEMA_VERSION: &'static str = "_schema_version";

    /// Get current Unix timestamp in milliseconds.
    fn now_millis() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// Generate a local URI-based block ID.
    fn generate_block_id() -> String {
        format!("local://{}", Uuid::new_v4())
    }

    /// Find a block ID by UUID (from properties).
    ///
    /// Searches through all blocks to find one with the given UUID in its properties.
    /// Returns the block ID if found, None otherwise.
    pub async fn find_block_by_uuid(&self, uuid: &str) -> Result<Option<String>, ApiError> {
        self.collab_doc
            .with_read(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);
                let mut found_id = None;

                blocks_map.for_each(|block_id, block_data| {
                    if found_id.is_some() {
                        return;
                    }

                    if let loro::ValueOrContainer::Container(loro::Container::Map(block_map)) =
                        block_data
                    {
                        // Check if this block has the UUID in properties
                        let properties = read_properties_from_map(&block_map);
                        if let Some(Value::String(prop_uuid)) = properties.get("ID") {
                            if prop_uuid == uuid {
                                found_id = Some(block_id.to_string());
                            }
                        }
                    }
                });

                Ok(found_id)
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to find block by UUID: {}", e),
            })
    }

    /// Update block text content using LoroText CRDT diff-based update.
    ///
    /// Uses Myers' diff algorithm to compute character-level changes, enabling
    /// proper CRDT merging with concurrent edits from other sources.
    pub async fn update_block_text(&self, id: &str, new_text: &str) -> Result<(), ApiError> {
        self.collab_doc
            .with_write(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);
                let block_map = Self::get_block_map(&blocks_map, id)?;

                // Determine the right field based on current content_type
                let content_type = block_map
                    .get(CONTENT_TYPE)
                    .and_then(|v| match v {
                        loro::ValueOrContainer::Value(val) => {
                            val.as_string().map(|s| s.to_string())
                        }
                        _ => None,
                    })
                    .unwrap_or_else(|| "text".to_string());

                let field = if content_type == "source" {
                    SOURCE_CODE
                } else {
                    CONTENT_RAW
                };
                update_text_field(&block_map, field, new_text)?;
                block_map.insert("updated_at", loro::LoroValue::from(Self::now_millis()))?;

                doc.commit();
                Ok(())
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to update block text: {}", e),
            })?;

        // Emit change notification
        let block = self.get_block(id).await?;
        self.emit_change(Change::Updated {
            id: id.to_string(),
            data: block,
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        Ok(())
    }

    /// Update block properties.
    ///
    /// Merges the provided properties with existing ones.
    pub async fn update_block_properties(
        &self,
        id: &str,
        properties: &HashMap<String, Value>,
    ) -> Result<(), ApiError> {
        self.collab_doc
            .with_write(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);
                let block_map = Self::get_block_map(&blocks_map, id)?;

                // Read existing properties
                let mut existing_props = read_properties_from_map(&block_map);

                // Merge new properties
                existing_props.extend(properties.clone());

                // Write back merged properties
                write_properties_to_map(&block_map, &existing_props)?;
                block_map.insert("updated_at", loro::LoroValue::from(Self::now_millis()))?;

                doc.commit();
                Ok(())
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to update block properties: {}", e),
            })?;

        // Emit change notification
        let block = self.get_block(id).await?;
        self.emit_change(Change::Updated {
            id: id.to_string(),
            data: block,
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        Ok(())
    }

    /// Update specific fields of a block.
    ///
    /// Takes a list of (field_name, old_value, new_value) tuples and applies them.
    pub async fn update_block_fields(
        &self,
        id: &str,
        fields: &[(String, Value, Value)],
    ) -> Result<(), ApiError> {
        self.collab_doc
            .with_write(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);
                let block_map = Self::get_block_map(&blocks_map, id)?;

                // Read existing properties
                let mut properties = read_properties_from_map(&block_map);

                // Apply field changes
                for (field_name, _old_value, new_value) in fields {
                    if new_value == &Value::Null {
                        properties.remove(field_name);
                    } else {
                        properties.insert(field_name.clone(), new_value.clone());
                    }
                }

                // Write back updated properties
                write_properties_to_map(&block_map, &properties)?;
                block_map.insert("updated_at", loro::LoroValue::from(Self::now_millis()))?;

                doc.commit();
                Ok(())
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to update block fields: {}", e),
            })?;

        // Emit change notification
        let block = self.get_block(id).await?;
        self.emit_change(Change::Updated {
            id: id.to_string(),
            data: block,
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        Ok(())
    }

    /// Emit a change to all subscribers and record it for late listeners.
    /// Sends the change as a single-item batch.
    /// Note: This spawns a task to avoid blocking on async lock.
    pub(crate) fn emit_change(&self, change: Change<Block>) {
        self.event_log.lock().unwrap().push(change.clone());
        let batch = vec![change];
        let subscribers = self.subscribers.clone();
        tokio::spawn(async move {
            let mut subscribers = subscribers.lock().await;
            subscribers.retain(|sender| sender.try_send(Ok(batch.clone())).is_ok());
        });
    }

    /// Helper to get a block's LoroMap from the blocks_by_id map
    fn get_block_map(blocks_map: &loro::LoroMap, id: &str) -> anyhow::Result<loro::LoroMap> {
        let block_data = blocks_map
            .get(id)
            .ok_or_else(|| anyhow::anyhow!(ApiError::BlockNotFound { id: id.to_string() }))?;

        match block_data {
            loro::ValueOrContainer::Container(loro::Container::Map(m)) => Ok(m),
            _ => Err(anyhow::anyhow!("Block {} is not a map", id)),
        }
    }

    /// Helper to get or create a children list for a parent
    fn get_or_create_children_list(
        doc: &loro::LoroDoc,
        parent_id: &str,
    ) -> anyhow::Result<loro::LoroList> {
        let children_map = doc.get_map(Self::CHILDREN_BY_PARENT);
        match children_map.get(parent_id) {
            Some(loro::ValueOrContainer::Container(loro::Container::List(list))) => Ok(list),
            Some(_) => Err(anyhow::anyhow!("Children container is not a list")),
            None => Ok(children_map.insert_container(parent_id, loro::LoroList::new())?),
        }
    }

    /// Helper to remove a block ID from a list
    fn remove_from_list(list: &loro::LoroList, block_id: &str) -> anyhow::Result<()> {
        if let Some(index) = list.find_index(|v| match v {
            loro::ValueOrContainer::Value(val) => val.as_string().map(|s| s.as_ref() == block_id),
            _ => None,
        }) {
            list.delete(index, 1)?;
        }
        Ok(())
    }

    /// Helper to insert a block ID into a list, optionally after a specific block
    fn insert_into_list(
        list: &loro::LoroList,
        block_id: &str,
        after: Option<&str>,
    ) -> anyhow::Result<()> {
        if let Some(after_id) = after {
            if let Some(index) = list.find_index(|v| match v {
                loro::ValueOrContainer::Value(val) => {
                    val.as_string().map(|s| s.as_ref() == after_id)
                }
                _ => None,
            }) {
                list.insert(index + 1, loro::LoroValue::from(block_id))?;
            } else {
                // If 'after' block not found, append to end
                list.push(loro::LoroValue::from(block_id))?;
            }
        } else {
            list.push(loro::LoroValue::from(block_id))?;
        }
        Ok(())
    }

    /// Check if `ancestor_id` is an ancestor of `descendant_id` (cycle detection helper)
    fn is_ancestor(
        ancestor_id: &str,
        descendant_id: &str,
        doc: &loro::LoroDoc,
    ) -> anyhow::Result<bool> {
        let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);
        let mut current_id = Some(descendant_id.to_string());

        while let Some(id) = current_id {
            if id == ancestor_id {
                return Ok(true);
            }

            // Stop if we reached a document URI (virtual parent, not in blocks_by_id)
            if is_document_uri(&id) {
                return Ok(false);
            }

            // Get parent of current block
            let parent_id = Self::get_block_map(&blocks_map, &id)?
                .get_typed("parent_id", |val| val.as_string().map(|s| s.to_string()));

            // Stop if we reached the root (NO_PARENT_ID sentinel)
            current_id = if parent_id.as_deref() == Some(NO_PARENT_ID) {
                None
            } else {
                parent_id
            };
        }

        Ok(false)
    }

    /// Initialize schema containers in the document.
    ///
    /// Called once during create_new() to set up the data model.
    /// Creates a default first child block with parent_id = document URI.
    pub async fn initialize_schema(collab_doc: &LoroDocument) -> Result<(), ApiError> {
        Self::initialize_schema_internal(collab_doc, true).await
    }

    /// Initialize schema without a default first child.
    ///
    /// Used by startup sync when populating from external sources that provide their own content.
    pub async fn initialize_schema_minimal(collab_doc: &LoroDocument) -> Result<(), ApiError> {
        Self::initialize_schema_internal(collab_doc, false).await
    }

    /// Internal schema initialization with control over default first child creation.
    async fn initialize_schema_internal(
        collab_doc: &LoroDocument,
        create_first_child: bool,
    ) -> Result<(), ApiError> {
        let doc_uri = document_uri(collab_doc.doc_id());

        collab_doc
            .with_write(|doc| {
                // Initialize containers (Loro creates them if they don't exist)
                doc.get_map(Self::BLOCKS_BY_ID);
                doc.get_map(Self::CHILDREN_BY_PARENT);

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                if create_first_child {
                    let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);
                    let children_map = doc.get_map(Self::CHILDREN_BY_PARENT);

                    // Create a default first child block with parent_id = document URI
                    let first_block_id = Self::generate_block_id();
                    let first_block_map =
                        blocks_map.insert_container(&first_block_id, loro::LoroMap::new())?;

                    // First block uses empty text content
                    write_content_to_map(&first_block_map, &BlockContent::text(""))?;
                    first_block_map.insert("parent_id", loro::LoroValue::from(doc_uri.as_str()))?;
                    first_block_map.insert("created_at", loro::LoroValue::from(now))?;
                    first_block_map.insert("updated_at", loro::LoroValue::from(now))?;

                    // Add the first block as a child of the document root
                    let doc_children =
                        children_map.insert_container(&doc_uri, loro::LoroList::new())?;
                    doc_children.push(loro::LoroValue::from(first_block_id.as_str()))?;
                }

                // Set schema version for future migrations
                let meta = doc.get_map("_meta");
                meta.insert(Self::SCHEMA_VERSION, loro::LoroValue::from(1i64))?;

                Ok(())
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to initialize schema: {}", e),
            })
    }
}

// Lifecycle trait implementation
#[async_trait]
impl Lifecycle for LoroBackend {
    async fn create_new(doc_id: String) -> Result<Self, ApiError>
    where
        Self: Sized,
    {
        let collab_doc = LoroDocument::new(doc_id).map_err(|e| ApiError::InternalError {
            message: format!("Failed to create document: {}", e),
        })?;

        let collab_doc = Arc::new(collab_doc);

        // Initialize schema
        Self::initialize_schema(&collab_doc).await?;

        Ok(Self {
            collab_doc,
            subscribers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            event_log: Arc::new(Mutex::new(Vec::new())),
        })
    }

    async fn open_existing(doc_id: String) -> Result<Self, ApiError>
    where
        Self: Sized,
    {
        // For now, same as create_new (will need persistence layer later)
        // TODO: Load from disk and validate schema version
        Self::create_new(doc_id).await
    }

    async fn dispose(&self) -> Result<(), ApiError> {
        // Release resources (LoroDocument drops automatically)
        Ok(())
    }
}

// ChangeNotifications trait implementation
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ChangeNotifications<Block> for LoroBackend {
    async fn watch_changes_since(
        &self,
        position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<Vec<Change<Block>>, ApiError>> + Send>> {
        // Collect replay items synchronously
        let mut replay_items = Vec::new();

        // If position is Beginning, collect all current blocks as Created events first
        if matches!(position, StreamPosition::Beginning) {
            match self
                .collab_doc
                .with_read(|doc| {
                    let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);
                    let children_map = doc.get_map(Self::CHILDREN_BY_PARENT);

                    // Collect all non-deleted blocks
                    let mut blocks = Vec::new();
                    blocks_map.for_each(|k, v| {
                        if let loro::ValueOrContainer::Container(loro::Container::Map(block_map)) =
                            v
                        {
                            // Skip deleted blocks
                            let is_deleted = block_map
                                .get_typed("deleted_at", |val| {
                                    Some(!matches!(val, loro::LoroValue::Null))
                                })
                                .unwrap_or(false);

                            if !is_deleted {
                                // Get children IDs for this block
                                let children = if let Some(loro::ValueOrContainer::Container(
                                    loro::Container::List(children_list),
                                )) = children_map.get(k.as_ref())
                                {
                                    children_list.collect_map(|v| match v {
                                        loro::ValueOrContainer::Value(val) => {
                                            val.as_string().map(|s| s.to_string())
                                        }
                                        _ => None,
                                    })
                                } else {
                                    Vec::new()
                                };

                                let block =
                                    read_block_from_map(k.to_string(), &block_map, children);
                                blocks.push(block);
                            }
                        }
                    });

                    anyhow::Ok(blocks)
                })
                .await
                .map_err(|e| ApiError::InternalError {
                    message: format!("Failed to get current blocks: {}", e),
                }) {
                Ok(current_blocks) => {
                    // Collect current blocks as replay items
                    for block in current_blocks {
                        replay_items.push(Change::Created {
                            data: block,
                            origin: ChangeOrigin::Remote {
                                operation_id: None,
                                trace_id: None,
                            },
                        });
                    }
                }
                Err(e) => {
                    // Return error as first item in stream
                    let error_stream = tokio_stream::iter(vec![Err(e)]);
                    let (_tx, rx) =
                        mpsc::channel::<std::result::Result<Vec<Change<Block>>, ApiError>>(100);
                    let live_stream = ReceiverStream::new(rx);
                    return Box::pin(error_stream.chain(live_stream));
                }
            }
        }

        // Collect buffered changes from event log
        let backlog = self.event_log.lock().unwrap().clone();
        replay_items.extend(backlog);

        // Create channel for live updates
        let (tx, rx) = mpsc::channel::<std::result::Result<Vec<Change<Block>>, ApiError>>(100);

        // Subscribe to future changes
        {
            let mut subscribers = self.subscribers.lock().await;
            subscribers.push(tx);
        }

        // Create a stream that first yields replay items as a batch, then live updates
        // This avoids spawning tasks which can cause runtime deadlocks when used with block_on
        let replay_batch = if replay_items.is_empty() {
            vec![]
        } else {
            vec![replay_items]
        };
        let replay_stream = tokio_stream::iter(replay_batch.into_iter().map(Ok));
        let live_stream = ReceiverStream::new(rx);
        let combined = replay_stream.chain(live_stream);

        Box::pin(combined)
    }

    async fn get_current_version(&self) -> std::result::Result<Vec<u8>, ApiError> {
        self.collab_doc
            .with_read(|doc| Ok(doc.export(loro::ExportMode::Snapshot)?))
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to get current version: {}", e),
            })
    }
}

// CoreOperations trait implementation
#[async_trait]
impl CoreOperations for LoroBackend {
    async fn get_block(&self, id: &str) -> Result<Block, ApiError> {
        self.collab_doc
            .with_read(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);

                // Get block data
                let block_data = blocks_map.get(id).ok_or_else(|| {
                    anyhow::anyhow!(ApiError::BlockNotFound { id: id.to_string() })
                })?;

                let block_map = match block_data {
                    loro::ValueOrContainer::Container(loro::Container::Map(m)) => m,
                    _ => {
                        return Err(ApiError::InternalError {
                            message: format!("Block {} is not a map", id),
                        }
                        .into());
                    }
                };

                // Get children from children_by_parent map
                let children_map = doc.get_map(Self::CHILDREN_BY_PARENT);
                let children = if let Some(loro::ValueOrContainer::Container(
                    loro::Container::List(children_list),
                )) = children_map.get(id)
                {
                    children_list.collect_map(|v| match v {
                        loro::ValueOrContainer::Value(val) => {
                            val.as_string().map(|s| s.to_string())
                        }
                        _ => None,
                    })
                } else {
                    Vec::new()
                };

                Ok(read_block_from_map(id.to_string(), &block_map, children))
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to get block: {}", e),
            })
    }

    async fn get_all_blocks(
        &self,
        traversal: super::types::Traversal,
    ) -> Result<Vec<Block>, ApiError> {
        self.collab_doc
            .with_read(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);
                let children_map = doc.get_map(Self::CHILDREN_BY_PARENT);
                let mut result = Vec::new();

                // Scan all blocks in blocks_map directly (more robust than tree traversal)
                // This handles cases where children_map is inconsistent with parent_id properties
                blocks_map.for_each(|block_id, value| {
                    if let loro::ValueOrContainer::Container(loro::Container::Map(block_map)) =
                        value
                    {
                        // Skip deleted blocks
                        let is_deleted = block_map
                            .get("deleted_at")
                            .map(|v| {
                                !matches!(v, loro::ValueOrContainer::Value(loro::LoroValue::Null))
                            })
                            .unwrap_or(false);

                        if is_deleted {
                            return;
                        }

                        // Get parent_id to determine level
                        let parent_id = block_map
                            .get("parent_id")
                            .and_then(|v| match v {
                                loro::ValueOrContainer::Value(val) => {
                                    val.as_string().map(|s| s.to_string())
                                }
                                _ => None,
                            })
                            .unwrap_or_else(|| NO_PARENT_ID.to_string());

                        // Determine level based on parent_id
                        // Level 1 = top-level blocks (parent_id is document URI), Level 2+ = nested
                        let level = if is_document_uri(&parent_id) {
                            1 // Top-level blocks under document root
                        } else {
                            // Count depth by looking up parent chain (simplified: assume level 2+ for nested)
                            2 // For now, treat all nested blocks as level 2
                        };

                        // Check if this level is included in traversal
                        if !traversal.includes_level(level) {
                            return;
                        }

                        // Get children IDs from children_map
                        let children = if let Some(loro::ValueOrContainer::Container(
                            loro::Container::List(children_list),
                        )) = children_map.get(&block_id)
                        {
                            children_list.collect_map(|v| match v {
                                loro::ValueOrContainer::Value(val) => {
                                    val.as_string().map(|s| s.to_string())
                                }
                                _ => None,
                            })
                        } else {
                            Vec::new()
                        };

                        let block = read_block_from_map(block_id.to_string(), &block_map, children);
                        result.push(block);
                    }
                });

                Ok(result)
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to get all blocks: {}", e),
            })
    }

    async fn list_children(&self, parent_id: &str) -> Result<Vec<String>, ApiError> {
        self.collab_doc
            .with_read(|doc| {
                let children_map = doc.get_map(Self::CHILDREN_BY_PARENT);

                let children = match children_map.get(parent_id) {
                    Some(loro::ValueOrContainer::Container(loro::Container::List(list))) => list
                        .collect_map(|v| match v {
                            loro::ValueOrContainer::Value(val) => {
                                val.as_string().map(|s| s.to_string())
                            }
                            _ => None,
                        }),
                    _ => Vec::new(),
                };

                Ok(children)
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to list children: {}", e),
            })
    }

    async fn create_block(
        &self,
        parent_id: String,
        content: BlockContent,
        id: Option<String>,
    ) -> Result<Block, ApiError> {
        let block_id = id.unwrap_or_else(Self::generate_block_id);
        let now = Self::now_millis();
        let parent_id_clone = parent_id.clone();

        let created_block = self
            .collab_doc
            .with_write(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);

                // Verify parent exists (and is not deleted)
                // Document URIs (holon-doc://) are virtual parents that represent documents
                if !is_document_uri(&parent_id) {
                    match blocks_map.get(&parent_id) {
                        Some(loro::ValueOrContainer::Container(loro::Container::Map(
                            parent_map,
                        ))) => {
                            // Check if parent is tombstoned
                            if let Some(loro::ValueOrContainer::Value(loro::LoroValue::I64(_))) =
                                parent_map.get("deleted_at")
                            {
                                return Err(anyhow::anyhow!(
                                    "Parent block is deleted: {}",
                                    parent_id
                                ));
                            }
                        }
                        _ => {
                            return Err(anyhow::anyhow!("Parent block not found: {}", parent_id));
                        }
                    }
                }

                // Check if block with this ID already exists (including tombstoned)
                let (block_map, is_resurrection) = if let Some(existing) = blocks_map.get(&block_id)
                {
                    match existing {
                        loro::ValueOrContainer::Container(loro::Container::Map(existing_map)) => {
                            // Block exists - check if it's tombstoned
                            let is_tombstoned = matches!(
                                existing_map.get("deleted_at"),
                                Some(loro::ValueOrContainer::Value(loro::LoroValue::I64(_)))
                            );

                            if is_tombstoned {
                                // Resurrect the tombstoned block
                                (existing_map, true)
                            } else {
                                // Block exists and is not deleted - this is an error
                                return Err(anyhow::anyhow!("Block already exists: {}", block_id));
                            }
                        }
                        _ => {
                            return Err(anyhow::anyhow!(
                                "Block {} exists but is not a map",
                                block_id
                            ));
                        }
                    }
                } else {
                    // Create new block
                    (
                        blocks_map.insert_container(&block_id, loro::LoroMap::new())?,
                        false,
                    )
                };

                // Update block content (works for both new and resurrected blocks)
                write_content_to_map(&block_map, &content)?;
                block_map.insert("parent_id", loro::LoroValue::from(parent_id.as_str()))?;
                block_map.insert("created_at", loro::LoroValue::from(now))?;
                block_map.insert("updated_at", loro::LoroValue::from(now))?;
                block_map.insert("deleted_at", loro::LoroValue::Null)?; // Clear tombstone

                // Add to parent's children list
                let children_map = doc.get_map(Self::CHILDREN_BY_PARENT);
                let children_list = match children_map.get(&parent_id) {
                    Some(loro::ValueOrContainer::Container(loro::Container::List(list))) => list,
                    Some(_) => {
                        return Err(anyhow::anyhow!("Children container is not a list"));
                    }
                    None => children_map.insert_container(&parent_id, loro::LoroList::new())?,
                };

                // Only add to children list if this is a new block (not resurrection)
                // Resurrected blocks should already be in the list
                if !is_resurrection {
                    children_list.push(loro::LoroValue::from(block_id.as_str()))?;
                }

                // Commit to trigger event subscribers
                doc.commit();

                let mut block = Block::from_block_content(block_id, parent_id, content);
                block.created_at = now;
                block.updated_at = now;
                Ok(block)
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("Parent block not found") {
                    ApiError::BlockNotFound {
                        id: parent_id_clone,
                    }
                } else {
                    ApiError::InternalError {
                        message: format!("Failed to create block: {}", e),
                    }
                }
            })?;

        self.emit_change(Change::Created {
            data: created_block.clone(),
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        Ok(created_block)
    }

    async fn update_block(&self, id: &str, content: BlockContent) -> Result<(), ApiError> {
        let content_clone = content.clone();
        // Read block data before updating to get parent_id and children
        let block_before = self.get_block(id).await?;

        self.collab_doc
            .with_write(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);

                // Get the block
                let block_data = blocks_map.get(id).ok_or_else(|| {
                    anyhow::anyhow!(ApiError::BlockNotFound { id: id.to_string() })
                })?;

                let block_map = match block_data {
                    loro::ValueOrContainer::Container(loro::Container::Map(m)) => m,
                    _ => {
                        return Err(anyhow::anyhow!("Block {} is not a map", id));
                    }
                };

                // Update content and timestamp
                write_content_to_map(&block_map, &content)?;
                block_map.insert("updated_at", loro::LoroValue::from(Self::now_millis()))?;

                // Commit to trigger event subscribers
                doc.commit();

                Ok(())
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to update block: {}", e),
            })?;

        let mut updated_block = block_before.clone();
        updated_block.set_block_content(content_clone);

        self.emit_change(Change::Updated {
            id: id.to_string(),
            data: updated_block,
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        Ok(())
    }

    async fn delete_block(&self, id: &str) -> Result<(), ApiError> {
        self.collab_doc
            .with_write(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);

                // Get the block
                let block_data = blocks_map.get(id).ok_or_else(|| {
                    anyhow::anyhow!(ApiError::BlockNotFound { id: id.to_string() })
                })?;

                let block_map = match block_data {
                    loro::ValueOrContainer::Container(loro::Container::Map(m)) => m,
                    _ => {
                        return Err(anyhow::anyhow!("Block {} is not a map", id));
                    }
                };

                // Set tombstone timestamp
                block_map.insert("deleted_at", loro::LoroValue::from(Self::now_millis()))?;
                block_map.insert("updated_at", loro::LoroValue::from(Self::now_millis()))?;

                // Commit to trigger event subscribers
                doc.commit();

                Ok(())
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to delete block: {}", e),
            })?;

        self.emit_change(Change::Deleted {
            id: id.to_string(),
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        Ok(())
    }

    async fn move_block(
        &self,
        id: &str,
        new_parent: String,
        after: Option<String>,
    ) -> Result<(), ApiError> {
        let new_parent_for_notify = new_parent.clone();
        let _after_for_notify = after.clone();

        // Read block data before moving to get content and children
        let block_before = self.get_block(id).await?;

        self.collab_doc
            .with_write(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);
                let block_map = Self::get_block_map(&blocks_map, id)?;

                // Get old parent_id
                let old_parent = block_map
                    .get_typed("parent_id", |val| val.as_string().map(|s| s.to_string()))
                    .ok_or_else(|| anyhow::anyhow!("Block {} has no parent_id", id))?;

                // Cycle detection
                if Self::is_ancestor(id, &new_parent, doc)? {
                    return Err(anyhow::anyhow!(
                        "Cannot move block {} under its descendant {}",
                        id,
                        new_parent
                    ));
                }

                // Verify new parent exists
                Self::get_block_map(&blocks_map, &new_parent)?;

                // Remove from old location
                let old_children_list = Self::get_or_create_children_list(doc, &old_parent)?;
                Self::remove_from_list(&old_children_list, id)?;

                // Add to new location
                let new_children_list = Self::get_or_create_children_list(doc, &new_parent)?;
                Self::insert_into_list(&new_children_list, id, after.as_deref())?;

                // Update block's parent_id
                block_map.insert("parent_id", loro::LoroValue::from(new_parent.as_str()))?;
                block_map.insert("updated_at", loro::LoroValue::from(Self::now_millis()))?;

                doc.commit();

                Ok(())
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to move block: {}", e),
            })?;

        let mut moved_block = block_before.clone();
        moved_block.parent_id = new_parent_for_notify;

        self.emit_change(Change::Updated {
            id: id.to_string(),
            data: moved_block,
            origin: ChangeOrigin::Local {
                operation_id: None,
                trace_id: None,
            },
        });

        Ok(())
    }

    async fn get_blocks(&self, ids: Vec<String>) -> Result<Vec<Block>, ApiError> {
        self.collab_doc
            .with_read(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);
                let mut blocks = Vec::new();

                // Collect successful results, skip blocks that don't exist
                for id in ids {
                    if let Some(loro::ValueOrContainer::Container(loro::Container::Map(
                        block_map,
                    ))) = blocks_map.get(&id)
                    {
                        // Note: children are not populated here since we don't have access to children_map
                        // For full children, use get_block() which looks up children_by_parent
                        let block = read_block_from_map(id.clone(), &block_map, vec![]);
                        blocks.push(block);
                    }
                    // Silently skip blocks that don't exist (partial success pattern)
                }

                Ok(blocks)
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to get blocks: {}", e),
            })
    }

    async fn create_blocks(&self, blocks: Vec<NewBlock>) -> Result<Vec<Block>, ApiError> {
        let now = Self::now_millis();

        let created_blocks = self
            .collab_doc
            .with_write(|doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);
                let mut created_blocks = Vec::new();

                // Single transaction for entire batch
                for new_block in blocks {
                    let block_id = new_block.id.unwrap_or_else(Self::generate_block_id);
                    let parent_id = new_block.parent_id.clone();

                    // Validate parent exists (and is not deleted)
                    match blocks_map.get(&parent_id) {
                        Some(loro::ValueOrContainer::Container(loro::Container::Map(
                            parent_map,
                        ))) => {
                            if let Some(loro::ValueOrContainer::Value(loro::LoroValue::I64(_))) =
                                parent_map.get("deleted_at")
                            {
                                return Err(anyhow::anyhow!(
                                    "Parent block is deleted: {}",
                                    parent_id
                                ));
                            }
                        }
                        _ => {
                            return Err(anyhow::anyhow!("Parent block not found: {}", parent_id));
                        }
                    }

                    // Check if block with this ID already exists (including tombstoned)
                    let (block_map, is_resurrection) = if let Some(existing) =
                        blocks_map.get(&block_id)
                    {
                        match existing {
                            loro::ValueOrContainer::Container(loro::Container::Map(
                                existing_map,
                            )) => {
                                let is_tombstoned = matches!(
                                    existing_map.get("deleted_at"),
                                    Some(loro::ValueOrContainer::Value(loro::LoroValue::I64(_)))
                                );

                                if is_tombstoned {
                                    (existing_map, true)
                                } else {
                                    return Err(anyhow::anyhow!(
                                        "Block already exists: {}",
                                        block_id
                                    ));
                                }
                            }
                            _ => {
                                return Err(anyhow::anyhow!(
                                    "Block {} exists but is not a map",
                                    block_id
                                ));
                            }
                        }
                    } else {
                        (
                            blocks_map.insert_container(&block_id, loro::LoroMap::new())?,
                            false,
                        )
                    };

                    // Update block content
                    write_content_to_map(&block_map, &new_block.content)?;
                    block_map.insert("parent_id", loro::LoroValue::from(parent_id.as_str()))?;
                    block_map.insert("created_at", loro::LoroValue::from(now))?;
                    block_map.insert("updated_at", loro::LoroValue::from(now))?;
                    block_map.insert("deleted_at", loro::LoroValue::Null)?;

                    // Add to parent's children list (only for new blocks)
                    if !is_resurrection {
                        let children_list = Self::get_or_create_children_list(doc, &parent_id)?;
                        Self::insert_into_list(
                            &children_list,
                            &block_id,
                            new_block.after.as_deref(),
                        )?;
                    }

                    let mut block =
                        Block::from_block_content(block_id, parent_id, new_block.content);
                    block.created_at = now;
                    block.updated_at = now;
                    created_blocks.push(block);
                }

                doc.commit();

                Ok(created_blocks)
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("Parent block not found") {
                    ApiError::BlockNotFound { id: e.to_string() }
                } else {
                    ApiError::InternalError {
                        message: format!("Failed to create blocks: {}", e),
                    }
                }
            })?;

        for block in &created_blocks {
            self.emit_change(Change::Created {
                data: block.clone(),
                origin: ChangeOrigin::Local {
                    operation_id: None,
                    trace_id: None,
                },
            });
        }

        Ok(created_blocks)
    }

    async fn delete_blocks(&self, ids: Vec<String>) -> Result<(), ApiError> {
        let now = Self::now_millis();

        // Deduplicate IDs to handle cases where the same ID appears multiple times
        let mut seen = std::collections::HashSet::new();
        let unique_ids: Vec<_> = ids
            .into_iter()
            .filter(|id| seen.insert(id.clone()))
            .collect();
        let ids_for_doc = unique_ids.clone();

        self.collab_doc
            .with_write(move |doc| {
                let blocks_map = doc.get_map(Self::BLOCKS_BY_ID);

                // Single transaction for entire batch
                for id in &ids_for_doc {
                    // Get the block - error if doesn't exist
                    let block_map = match blocks_map.get(id) {
                        Some(loro::ValueOrContainer::Container(loro::Container::Map(m))) => m,
                        _ => return Err(anyhow::anyhow!("Block not found: {}", id)),
                    };

                    // Set tombstone timestamp
                    block_map.insert("deleted_at", loro::LoroValue::from(now))?;
                    block_map.insert("updated_at", loro::LoroValue::from(now))?;
                }

                doc.commit();

                Ok(())
            })
            .await
            .map_err(|e| ApiError::InternalError {
                message: format!("Failed to delete blocks: {}", e),
            })?;

        for id in unique_ids {
            self.emit_change(Change::Deleted {
                id,
                origin: ChangeOrigin::Local {
                    operation_id: None,
                    trace_id: None,
                },
            });
        }

        Ok(())
    }
}

// P2POperations trait implementation
// P2P is now handled by IrohSyncAdapter, not by LoroDocument directly.
// These stubs remain for API compatibility; wire IrohSyncAdapter at a higher layer.
#[async_trait]
impl P2POperations for LoroBackend {
    async fn get_node_id(&self) -> String {
        "local-only".to_string()
    }

    async fn connect_to_peer(&self, _peer_node_id: String) -> Result<(), ApiError> {
        Err(ApiError::NetworkError {
            message: "P2P sync requires IrohSyncAdapter (not wired to LoroBackend)".to_string(),
        })
    }

    async fn accept_connections(&self) -> Result<(), ApiError> {
        Err(ApiError::NetworkError {
            message: "P2P sync requires IrohSyncAdapter (not wired to LoroBackend)".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::repository::{CoreOperations, Lifecycle};

    const DOC_ID: &str = "test-cycle";

    async fn create_test_backend() -> LoroBackend {
        LoroBackend::create_new(DOC_ID.to_string()).await.unwrap()
    }

    fn doc_uri() -> String {
        document_uri(DOC_ID)
    }

    /// Build a chain: doc_uri → root → A → B → C
    async fn build_chain(backend: &LoroBackend) -> (Block, Block, Block, Block) {
        let root = backend
            .create_block(doc_uri(), BlockContent::Text { raw: "root".into() }, None)
            .await
            .unwrap();

        let a = backend
            .create_block(
                root.id.clone(),
                BlockContent::Text { raw: "A".into() },
                None,
            )
            .await
            .unwrap();

        let b = backend
            .create_block(a.id.clone(), BlockContent::Text { raw: "B".into() }, None)
            .await
            .unwrap();

        let c = backend
            .create_block(b.id.clone(), BlockContent::Text { raw: "C".into() }, None)
            .await
            .unwrap();

        (root, a, b, c)
    }

    #[tokio::test]
    async fn move_block_self_cycle_rejected() {
        let backend = create_test_backend().await;
        let (_, a, _, _) = build_chain(&backend).await;

        let result = backend.move_block(&a.id, a.id.clone(), None).await;
        assert!(result.is_err(), "Moving a block under itself should fail");
    }

    #[tokio::test]
    async fn move_block_direct_child_cycle_rejected() {
        let backend = create_test_backend().await;
        let (_, a, b, _) = build_chain(&backend).await;

        let result = backend.move_block(&a.id, b.id.clone(), None).await;
        assert!(result.is_err(), "Moving A under its child B should fail");
    }

    #[tokio::test]
    async fn move_block_deep_descendant_cycle_rejected() {
        let backend = create_test_backend().await;
        let (_, a, _, c) = build_chain(&backend).await;

        let result = backend.move_block(&a.id, c.id.clone(), None).await;
        assert!(
            result.is_err(),
            "Moving A under its grandchild C should fail"
        );
    }

    #[tokio::test]
    async fn move_block_valid_sibling_move_succeeds() {
        let backend = create_test_backend().await;
        let root = backend
            .create_block(doc_uri(), BlockContent::Text { raw: "root".into() }, None)
            .await
            .unwrap();

        let a = backend
            .create_block(
                root.id.clone(),
                BlockContent::Text { raw: "A".into() },
                None,
            )
            .await
            .unwrap();

        let b = backend
            .create_block(
                root.id.clone(),
                BlockContent::Text { raw: "B".into() },
                None,
            )
            .await
            .unwrap();

        let result = backend.move_block(&b.id, a.id.clone(), None).await;
        assert!(
            result.is_ok(),
            "Moving sibling under sibling should succeed: {:?}",
            result.err()
        );

        let moved_b = backend.get_block(&b.id).await.unwrap();
        assert_eq!(moved_b.parent_id, a.id);
    }

    #[tokio::test]
    async fn move_block_to_nonexistent_parent_fails() {
        let backend = create_test_backend().await;
        let (_, a, _, _) = build_chain(&backend).await;

        let result = backend
            .move_block(&a.id, "nonexistent-id".to_string(), None)
            .await;
        assert!(result.is_err(), "Moving to nonexistent parent should fail");
    }
}
