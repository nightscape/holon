use holon_macros::Entity;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::Value;

/// Sentinel value indicating a block has no parent (used for root block's parent_id).
/// This prevents the root block from forming a cycle with itself.
pub const NO_PARENT_ID: &str = "__no_parent__";

/// ID of the root document in the document tree.
/// The root document represents the main layout/index document.
pub const ROOT_DOC_ID: &str = "index.org";

/// Sentinel value indicating a document has no parent (used for root document's parent_id).
/// This prevents the root document from forming a cycle with itself.
pub const NO_PARENT_DOC_ID: &str = "__no_parent__"; // Reuse same sentinel

/// Content type discriminator for flattened Block storage.
pub const CONTENT_TYPE_TEXT: &str = "text";
pub const CONTENT_TYPE_SOURCE: &str = "source";

// =============================================================================
// BlockContent - Discriminated union for block content types
// =============================================================================

/// Content of a block - discriminated union for different content types.
///
/// This enables a unified data model across Org Mode, Markdown, and Loro:
/// - Tier 1 (all formats): Text and basic Source blocks
/// - Tier 2 (Org + Loro): Full SourceBlock with name, header_args, results
/// - Tier 3 (Loro only): CRDT history, real-time sync
///
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum BlockContent {
    /// Plain text content (paragraphs, prose)
    Text {
        /// Raw text content
        raw: String,
    },

    /// Source code block (language-agnostic)
    Source(SourceBlock),
}

impl Default for BlockContent {
    fn default() -> Self {
        BlockContent::Text { raw: String::new() }
    }
}

impl std::fmt::Display for BlockContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockContent::Text { raw } => write!(f, "{}", raw),
            BlockContent::Source(sb) => {
                let lang = sb.language.as_deref().unwrap_or("unknown");
                write!(f, "[{}] {}", lang, sb.source)
            }
        }
    }
}

impl BlockContent {
    /// Create a text content block
    pub fn text(raw: impl Into<String>) -> Self {
        BlockContent::Text { raw: raw.into() }
    }

    /// Create a source block with minimal fields (Tier 1)
    pub fn source(language: impl Into<String>, source: impl Into<String>) -> Self {
        BlockContent::Source(SourceBlock::new(language, source))
    }

    /// Get the raw text if this is a Text variant
    /// flutter_rust_bridge:ignore
    pub fn as_text(&self) -> Option<&str> {
        match self {
            BlockContent::Text { raw } => Some(raw),
            _ => None,
        }
    }

    /// Get the source block if this is a Source variant
    /// flutter_rust_bridge:ignore
    pub fn as_source(&self) -> Option<&SourceBlock> {
        match self {
            BlockContent::Source(sb) => Some(sb),
            _ => None,
        }
    }

    /// Get a plain text representation (for search, display, etc.)
    /// flutter_rust_bridge:ignore
    pub fn to_plain_text(&self) -> &str {
        match self {
            BlockContent::Text { raw } => raw,
            BlockContent::Source(sb) => &sb.source,
        }
    }
}

/// A source code block with optional metadata.
///
/// Supports three tiers of features:
/// - Tier 1 (all formats): language + source code
/// - Tier 2 (Org + Loro): name, header_args, results
/// - Tier 3 (Loro only): inherited from Block's CRDT features
///
/// In Org Mode: `#+BEGIN_SRC language :arg1 val1 ... #+END_SRC`
/// In Markdown: ` ```language ... ``` `
/// In Loro: Native storage with full fidelity
///
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceBlock {
    /// Language identifier (e.g., "prql", "sql", "python", "rust")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,

    /// The source code itself
    pub source: String,

    /// Optional block name for references (#+NAME: in Org Mode)
    /// Tier 2: Supported in Org Mode and Loro, lost in Markdown
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Header arguments / parameters
    /// Tier 2: Supported in Org Mode (`:var x=1 :results table`) and Loro
    /// Examples for PRQL: { "connection": "main", "results": "table" }
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub header_args: HashMap<String, Value>,
}

impl SourceBlock {
    /// Create a new source block with minimal fields (Tier 1)
    pub fn new(language: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            language: Some(language.into()),
            source: source.into(),
            name: None,
            header_args: HashMap::new(),
        }
    }

    /// Builder: set the block name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Builder: add a header argument
    pub fn with_header_arg(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.header_args.insert(key.into(), value.into());
        self
    }

    /// Check if this is a PRQL source block
    pub fn is_prql(&self) -> bool {
        self.language
            .as_ref()
            .map(|l| l.eq_ignore_ascii_case("prql"))
            .unwrap_or(false)
    }

    /// Get a header argument by key
    /// flutter_rust_bridge:ignore
    pub fn get_header_arg(&self, key: &str) -> Option<&Value> {
        self.header_args.get(key)
    }
}

/// Results from executing a source block.
///
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockResult {
    /// The output content
    pub output: ResultOutput,

    /// Unix timestamp (milliseconds) when the block was executed
    pub executed_at: i64,
}

impl BlockResult {
    /// Create a text result
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            output: ResultOutput::Text {
                content: content.into(),
            },
            executed_at: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Create a table result
    pub fn table(headers: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            output: ResultOutput::Table { headers, rows },
            executed_at: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Create an error result
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            output: ResultOutput::Error {
                message: message.into(),
            },
            executed_at: chrono::Utc::now().timestamp_millis(),
        }
    }
}

/// Output types for block execution results.
///
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ResultOutput {
    /// Plain text output
    Text { content: String },

    /// Tabular output (from queries)
    Table {
        headers: Vec<String>,
        rows: Vec<Vec<Value>>,
    },

    /// Error output
    Error { message: String },
}

// =============================================================================
// Block - The main block structure (flattened for database storage)
// =============================================================================

/// A block in the hierarchical document structure.
///
/// This struct is flattened for efficient database storage while maintaining
/// a rich API through helper methods. Complex types (properties, children,
/// source block metadata) are stored as JSON strings.
///
/// Blocks use URI-based IDs to support integration with external systems:
/// - Local blocks: `local://<uuid-v4>` (e.g., `local://550e8400-e29b-41d4-a716-446655440000`)
/// - External systems: `todoist://task/12345`, `logseq://page/abc123`
///
/// # Example
///
/// ```rust
/// use holon_api::Block;
///
/// // Text block
/// let block = Block::new_text("block-1", "__root_parent__", "My first block");
///
/// // PRQL source block
/// let query_block = Block::new_source("query-1", "__root_parent__", "prql", "from tasks");
/// ```
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Entity)]
#[entity(name = "blocks", short_name = "block", api_crate = "crate")]
pub struct Block {
    /// URI-based unique identifier
    #[primary_key]
    #[indexed]
    pub id: String,

    /// Parent block ID
    #[indexed]
    pub parent_id: String,

    // --- Content fields (flattened from BlockContent) ---
    /// Text content (raw text or source code)
    pub content: String,

    /// Content type: "text" or "source"
    pub content_type: String,

    /// For source blocks: programming language (e.g., "prql", "python")
    pub source_language: Option<String>,

    /// For source blocks: optional block name for references (#+NAME: in Org Mode)
    /// Tier 2: Supported in Org Mode and Loro, lost in Markdown
    pub source_name: Option<String>,

    // --- Properties (JSON strings) ---
    /// Key-value properties (TODO, PRIORITY, TAGS, dates, etc.)
    /// Stored as JSON object for native JSON support in Turso.
    /// Tier 2: works fully in Org + Loro
    #[serde(default)]
    #[jsonb]
    pub properties: HashMap<String, Value>,

    // --- Timestamps (flattened from BlockMetadata) ---
    /// Unix timestamp (milliseconds) when block was created
    pub created_at: i64,

    /// Unix timestamp (milliseconds) when block was last updated
    pub updated_at: i64,
}

impl Default for Block {
    fn default() -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id: String::new(),
            parent_id: NO_PARENT_ID.to_string(),
            content: String::new(),
            content_type: CONTENT_TYPE_TEXT.to_string(),
            source_language: None,
            source_name: None,
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }
}

impl Block {
    /// Create a new text block with sensible defaults
    pub fn new_text(
        id: impl Into<String>,
        parent_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id: id.into(),
            parent_id: parent_id.into(),
            content: text.into(),
            content_type: CONTENT_TYPE_TEXT.to_string(),
            source_language: None,
            source_name: None,
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new source block with sensible defaults
    pub fn new_source(
        id: impl Into<String>,
        parent_id: impl Into<String>,
        language: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id: id.into(),
            parent_id: parent_id.into(),
            content: source.into(),
            content_type: CONTENT_TYPE_SOURCE.to_string(),
            source_language: Some(language.into()),
            source_name: None,
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a Block from a BlockContent (for API compatibility)
    pub fn from_block_content(
        id: impl Into<String>,
        parent_id: impl Into<String>,
        content: BlockContent,
    ) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        let (text, content_type, lang, name) = match content {
            BlockContent::Text { raw } => (raw, CONTENT_TYPE_TEXT.to_string(), None, None),
            BlockContent::Source(sb) => (
                sb.source,
                CONTENT_TYPE_SOURCE.to_string(),
                sb.language,
                sb.name,
            ),
        };

        Self {
            id: id.into(),
            parent_id: parent_id.into(),
            content: text,
            content_type,
            source_language: lang,
            source_name: name,
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Get the content as a BlockContent enum (for API compatibility)
    /// flutter_rust_bridge:ignore
    pub fn to_block_content(&self) -> BlockContent {
        if self.content_type == CONTENT_TYPE_SOURCE {
            BlockContent::Source(SourceBlock {
                language: self.source_language.clone(),
                source: self.content.clone(),
                name: self.source_name.clone(),
                header_args: HashMap::new(),
            })
        } else {
            BlockContent::Text {
                raw: self.content.clone(),
            }
        }
    }

    /// Set the content from a BlockContent enum
    /// flutter_rust_bridge:ignore
    pub fn set_block_content(&mut self, content: BlockContent) {
        match content {
            BlockContent::Text { raw } => {
                self.content = raw;
                self.content_type = CONTENT_TYPE_TEXT.to_string();
                self.source_language = None;
                self.source_name = None;
            }
            BlockContent::Source(sb) => {
                self.content = sb.source;
                self.content_type = CONTENT_TYPE_SOURCE.to_string();
                self.source_language = sb.language;
                self.source_name = sb.name;
            }
        }
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    /// Get the plain text content of this block.
    /// For text blocks, returns the raw text.
    /// For source blocks, returns the source code.
    /// flutter_rust_bridge:ignore
    pub fn content_text(&self) -> &str {
        &self.content
    }

    /// Get title (first line of content)
    /// flutter_rust_bridge:ignore
    pub fn title(&self) -> String {
        self.content.lines().next().unwrap_or("").to_string()
    }

    /// Check if this block contains a source block
    /// flutter_rust_bridge:ignore
    pub fn is_source_block(&self) -> bool {
        self.content_type == CONTENT_TYPE_SOURCE
    }

    /// Check if this block contains a PRQL source block
    /// flutter_rust_bridge:ignore
    pub fn is_prql_block(&self) -> bool {
        self.is_source_block()
            && self
                .source_language
                .as_ref()
                .map(|l| l.eq_ignore_ascii_case("prql"))
                .unwrap_or(false)
    }

    /// Get properties as a HashMap (returns a clone)
    /// flutter_rust_bridge:ignore
    pub fn properties_map(&self) -> HashMap<String, Value> {
        self.properties.clone()
    }

    /// Set properties from a HashMap
    /// flutter_rust_bridge:ignore
    pub fn set_properties_map(&mut self, props: HashMap<String, Value>) {
        self.properties = props;
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    /// Get a property value by key
    /// flutter_rust_bridge:ignore
    pub fn get_property(&self, key: &str) -> Option<Value> {
        self.properties.get(key).cloned()
    }

    /// Get a property value as string
    /// flutter_rust_bridge:ignore
    pub fn get_property_str(&self, key: &str) -> Option<String> {
        self.properties
            .get(key)
            .and_then(|v| v.as_string().map(|s| s.to_string()))
    }

    /// Set a property value
    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.properties.insert(key.into(), value.into());
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    /// Get source header arguments from properties (for Org Mode compatibility)
    /// flutter_rust_bridge:ignore
    pub fn get_source_header_args(&self) -> HashMap<String, Value> {
        self.properties
            .get("_source_header_args")
            .and_then(|v| {
                if let Value::String(s) = v {
                    serde_json::from_str(s).ok()
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    /// Set source header arguments in properties (for Org Mode compatibility)
    /// flutter_rust_bridge:ignore
    pub fn set_source_header_args(&mut self, header_args: HashMap<String, Value>) {
        if !header_args.is_empty() {
            if let Ok(json) = serde_json::to_string(&header_args) {
                self.properties
                    .insert("_source_header_args".to_string(), Value::String(json));
                self.updated_at = chrono::Utc::now().timestamp_millis();
            }
        }
    }

    /// Get source results from properties (for Org Mode compatibility)
    /// flutter_rust_bridge:ignore
    pub fn get_source_results(&self) -> Option<String> {
        self.properties
            .get("_source_results")
            .and_then(|v| v.as_string().map(|s| s.to_string()))
    }

    /// Set source results in properties (for Org Mode compatibility)
    /// flutter_rust_bridge:ignore
    pub fn set_source_results(&mut self, results: Option<String>) {
        if let Some(r) = results {
            self.properties
                .insert("_source_results".to_string(), Value::String(r));
            self.updated_at = chrono::Utc::now().timestamp_millis();
        }
    }

    /// Get metadata as BlockMetadata
    /// flutter_rust_bridge:ignore
    pub fn metadata(&self) -> BlockMetadata {
        BlockMetadata {
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    /// Set metadata from BlockMetadata
    /// flutter_rust_bridge:ignore
    pub fn set_metadata(&mut self, metadata: BlockMetadata) {
        self.created_at = metadata.created_at;
        self.updated_at = metadata.updated_at;
    }

    /// Get the depth/nesting level of this block by following parent chain.
    ///
    /// This requires a lookup function to resolve parent IDs to blocks.
    /// Returns 0 for root blocks, 1 for children of roots, etc.
    ///
    /// # Arguments
    ///
    /// * `get_block` - Function to look up a block by ID
    ///
    /// flutter_rust_bridge:ignore
    pub fn depth_from<'blk, F>(&self, mut get_block: F) -> usize
    where
        F: for<'a> FnMut(&'a str) -> Option<&'blk Block>,
    {
        use crate::document::is_document_uri;

        let mut depth = 0;
        let mut current_parent = Some(self.parent_id.as_str());

        while let Some(parent_id) = current_parent {
            // Stop at document roots (document URI or no parent)
            if parent_id == NO_PARENT_ID || is_document_uri(parent_id) {
                break;
            }
            depth += 1;
            current_parent = get_block(parent_id).map(|b| b.parent_id.as_str());
        }

        depth
    }
}

/// A block with its tree depth/nesting level.
///
/// Used for tree-ordered iteration and diffing. The depth indicates
/// how deeply nested the block is (0 = root, 1 = child of root, etc.).
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockWithDepth {
    /// The block data
    pub block: Block,
    /// Nesting depth (0 = root level)
    pub depth: usize,
}

/// Metadata associated with a block.
///
/// Note: UI state like `collapsed` is NOT stored here - it's kept locally
/// in the frontend to avoid cross-user UI churn in collaborative sessions.
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct BlockMetadata {
    /// Unix timestamp (milliseconds) when block was created
    pub created_at: i64,
    /// Unix timestamp (milliseconds) when block was last updated
    pub updated_at: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::HasSchema;

    #[test]
    fn block_schema_has_correct_jsonb_fields() {
        let schema = Block::schema();

        // These fields should be JSONB
        assert!(
            schema.field_is_jsonb("properties"),
            "properties should be JSONB"
        );

        // These fields should NOT be JSONB
        assert!(!schema.field_is_jsonb("id"), "id should NOT be JSONB");
        assert!(
            !schema.field_is_jsonb("content"),
            "content should NOT be JSONB"
        );
        assert!(
            !schema.field_is_jsonb("parent_id"),
            "parent_id should NOT be JSONB"
        );
    }
}
