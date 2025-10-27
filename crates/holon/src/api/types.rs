//! Core data types for holon API
//!
//! This module defines the types used across all frontends (Tauri, Flutter, etc.)
//! to interact with the holon backend.

use serde::{Deserialize, Serialize};

// Block types are now in holon-api, re-export for convenience
use holon_api::BlockContent;

/// Configuration for filtering blocks by tree depth when traversing.
///
/// Depth levels:
/// - Level 0: Root of the current document
/// - Level 1: Top-level user blocks (direct children of root)
/// - Level 2+: Nested blocks
///
/// # Examples
///
/// ```rust
/// use holon::api::Traversal;
///
/// // Get only top-level blocks
/// let top_level = Traversal::TOP_LEVEL;
///
/// // Get all blocks including root
/// let all = Traversal::ALL;
///
/// // Get outline headers only (levels 1-2)
/// let headers = Traversal::new(1, 2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Traversal {
    /// Minimum depth level to include (inclusive)
    pub min_level: usize,
    /// Maximum depth level to include (inclusive)
    pub max_level: usize,
}

impl Traversal {
    /// Only top-level user blocks (level 1)
    pub const TOP_LEVEL: Self = Self {
        min_level: 1,
        max_level: 1,
    };

    /// All blocks including the synthetic root (levels 0 to MAX)
    pub const ALL: Self = Self {
        min_level: 0,
        max_level: usize::MAX,
    };

    /// All blocks except the synthetic root (levels 1 to MAX)
    pub const ALL_BUT_ROOT: Self = Self {
        min_level: 1,
        max_level: usize::MAX,
    };

    /// Create a custom depth range filter
    pub const fn new(min_level: usize, max_level: usize) -> Self {
        Self {
            min_level,
            max_level,
        }
    }

    /// Check if a given depth level should be included
    pub const fn includes_level(&self, level: usize) -> bool {
        level >= self.min_level && level <= self.max_level
    }
}

/// Template for creating a new block in a batch operation.
///
/// # ID Generation
///
/// - `id = None`: Generate `local://<uuid-v4>`
/// - `id = Some(uri)`: Use provided URI (e.g., `todoist://task/123`)
///
/// # Positioning
///
/// - `after = None`: Insert at start of parent's children
/// - `after = Some(sibling_id)`: Insert after specified sibling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewBlock {
    /// Parent block ID
    pub parent_id: String,
    /// Initial content (text, source block, etc.)
    pub content: BlockContent,
    /// Position anchor: insert after this sibling (None = insert at start)
    pub after: Option<String>,
    /// Optional custom ID (None = generate local URI)
    pub id: Option<String>,
}

impl NewBlock {
    /// Create a new text block
    pub fn text(parent_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            parent_id: parent_id.into(),
            content: BlockContent::text(text),
            after: None,
            id: None,
        }
    }

    /// Create a new source block
    pub fn source(
        parent_id: impl Into<String>,
        language: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            parent_id: parent_id.into(),
            content: BlockContent::source(language, source),
            after: None,
            id: None,
        }
    }

    /// Builder: set position after a sibling
    pub fn after(mut self, sibling_id: impl Into<String>) -> Self {
        self.after = Some(sibling_id.into());
        self
    }

    /// Builder: set custom ID
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }
}
