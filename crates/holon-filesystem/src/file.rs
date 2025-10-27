use holon_macros::Entity;
use serde::{Deserialize, Serialize};

/// File - represents a file in the filesystem that maps to a logical Document
#[derive(Debug, Clone, Serialize, Deserialize, Entity)]
#[entity(name = "files", short_name = "file")]
pub struct File {
    #[primary_key]
    #[indexed]
    pub id: String,

    /// Filename (e.g. "index.org")
    pub name: String,

    /// Directory ID (e.g. "Projects" or "null" for root)
    #[indexed]
    pub parent_id: String,

    /// SHA256 for change detection
    pub content_hash: String,

    /// FK to Document.id (UUID), None until adapter creates the Document
    #[indexed]
    pub document_id: Option<String>,
}

impl File {
    pub fn new(
        id: String,
        name: String,
        parent_id: String,
        content_hash: String,
        document_id: Option<String>,
    ) -> Self {
        Self {
            id,
            name,
            parent_id,
            content_hash,
            document_id,
        }
    }
}
