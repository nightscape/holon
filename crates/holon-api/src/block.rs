use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::Value;

/// ID of the root block in the document tree.
/// The root block is a synthetic container for all top-level blocks.
pub const ROOT_PARENT_ID: &str = "__root_parent__";

/// Sentinel value indicating a block has no parent (used for root block's parent_id).
/// This prevents the root block from forming a cycle with itself.
pub const NO_PARENT_ID: &str = "__no_parent__";

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
            BlockContent::Source(sb) => write!(f, "[{}] {}", sb.language, sb.source),
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
    pub fn as_text(&self) -> Option<&str> {
        match self {
            BlockContent::Text { raw } => Some(raw),
            _ => None,
        }
    }

    /// Get the source block if this is a Source variant
    pub fn as_source(&self) -> Option<&SourceBlock> {
        match self {
            BlockContent::Source(sb) => Some(sb),
            _ => None,
        }
    }

    /// Get a plain text representation (for search, display, etc.)
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
    pub language: String,

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

    /// Cached execution results
    /// Tier 2: Supported in Org Mode (#+RESULTS:) and Loro
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<BlockResult>,
}

impl SourceBlock {
    /// Create a new source block with minimal fields (Tier 1)
    pub fn new(language: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            language: language.into(),
            source: source.into(),
            name: None,
            header_args: HashMap::new(),
            results: None,
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

    /// Builder: set results
    pub fn with_results(mut self, results: BlockResult) -> Self {
        self.results = Some(results);
        self
    }

    /// Check if this is a PRQL source block
    pub fn is_prql(&self) -> bool {
        self.language.eq_ignore_ascii_case("prql")
    }

    /// Get a header argument by key
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
// Block - The main block structure
// =============================================================================

/// A block in the hierarchical document structure.
///
/// Blocks use URI-based IDs to support integration with external systems:
/// - Local blocks: `local://<uuid-v4>` (e.g., `local://550e8400-e29b-41d4-a716-446655440000`)
/// - External systems: `todoist://task/12345`, `logseq://page/abc123`
///
/// # Example
///
/// ```rust
/// use holon_api::{Block, BlockContent};
///
/// // Text block
/// let block = Block {
///     id: "local://550e8400-e29b-41d4-a716-446655440000".to_string(),
///     parent_id: "parent_id".to_string(),
///     content: BlockContent::text("My first block"),
///     properties: Default::default(),
///     children: vec![],
///     metadata: Default::default(),
/// };
///
/// // PRQL source block
/// let query_block = Block {
///     id: "local://query-1".to_string(),
///     parent_id: "parent_id".to_string(),
///     content: BlockContent::source("prql", "from tasks | filter completed == false"),
///     properties: Default::default(),
///     children: vec![],
///     metadata: Default::default(),
/// };
/// ```
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Block {
    /// URI-based unique identifier
    pub id: String,
    /// Parent block ID
    pub parent_id: String,
    /// Typed content (text, source block, etc.)
    pub content: BlockContent,
    /// Key-value properties (Tier 2: works fully in Org + Loro)
    /// Supports arbitrary metadata like tags, priorities, dates
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, Value>,
    /// IDs of child blocks in display order
    pub children: Vec<String>,
    /// Block metadata (timestamps, etc.)
    pub metadata: BlockMetadata,
}

impl Block {
    /// Create a new text block with sensible defaults
    pub fn new_text(
        id: impl Into<String>,
        parent_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            parent_id: parent_id.into(),
            content: BlockContent::text(text),
            properties: HashMap::new(),
            children: Vec::new(),
            metadata: BlockMetadata::default(),
        }
    }

    /// Create a new source block with sensible defaults
    pub fn new_source(
        id: impl Into<String>,
        parent_id: impl Into<String>,
        language: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            parent_id: parent_id.into(),
            content: BlockContent::source(language, source),
            properties: HashMap::new(),
            children: Vec::new(),
            metadata: BlockMetadata::default(),
        }
    }

    /// Get the plain text content of this block.
    /// For text blocks, returns the raw text.
    /// For source blocks, returns the source code.
    pub fn content_text(&self) -> &str {
        self.content.to_plain_text()
    }

    /// Check if this block contains a source block
    pub fn is_source_block(&self) -> bool {
        matches!(self.content, BlockContent::Source(_))
    }

    /// Check if this block contains a PRQL source block
    pub fn is_prql_block(&self) -> bool {
        self.content
            .as_source()
            .map(|s| s.is_prql())
            .unwrap_or(false)
    }

    /// Get a property value by key
    pub fn get_property(&self, key: &str) -> Option<&Value> {
        self.properties.get(key)
    }

    /// Set a property value
    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.properties.insert(key.into(), value.into());
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
    /// # Example
    ///
    /// ```rust,no_run
    /// use holon_api::Block;
    /// use std::collections::HashMap;
    ///
    /// let mut blocks = HashMap::new();
    /// // ... populate blocks ...
    ///
    /// let block = blocks.get("some-id").unwrap();
    /// let depth = block.depth(&|id| blocks.get(id));
    /// ```
    ///
    /// flutter_rust_bridge:ignore
    pub fn depth<'blk, F>(&self, mut get_block: F) -> usize
    where
        F: for<'a> FnMut(&'a str) -> Option<&'blk Block>,
    {
        let mut depth = 0;
        let mut current_parent = Some(self.parent_id.as_str());

        while let Some(parent_id) = current_parent {
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
