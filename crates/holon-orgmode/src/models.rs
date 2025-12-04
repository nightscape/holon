use holon_macros::Entity;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Re-export Directory and ROOT_ID from holon-filesystem
pub use holon_filesystem::directory::{Directory, ROOT_ID};

/// Default done keywords when file doesn't specify custom TODO config
pub const DEFAULT_DONE_KEYWORDS: &[&str] = &["DONE", "CANCELLED", "CLOSED"];

/// Check if a keyword is considered "done" using default keywords
pub fn is_done_keyword(keyword: &str) -> bool {
    DEFAULT_DONE_KEYWORDS.contains(&keyword)
}

/// OrgFile - represents a .org file
#[derive(Debug, Clone, Serialize, Deserialize, Entity)]
#[entity(name = "org_files", short_name = "file")]
pub struct OrgFile {
    #[primary_key]
    #[indexed]
    pub id: String,

    /// Filename with extension (relative to parent directory)
    pub name: String,

    /// Full absolute path to the file (for write-back operations)
    pub path: String,

    /// Parent directory ID
    #[indexed]
    pub parent_id: String,

    /// parent.depth + 1
    pub depth: i64,

    /// #+TITLE: value if present
    pub title: Option<String>,

    /// Custom TODO keywords, comma-separated with pipe separator
    /// Format: "TODO,INPROGRESS|DONE,CANCELLED"
    pub todo_keywords: Option<String>,

    /// Content hash for change detection
    pub file_hash: String,

    /// File modification time (ISO 8601)
    pub updated_at: String,
}

impl OrgFile {
    pub fn new(
        id: String,
        name: String,
        path: String,
        parent_id: String,
        depth: i64,
        title: Option<String>,
        file_hash: String,
        updated_at: String,
    ) -> Self {
        Self {
            id,
            name,
            path,
            parent_id,
            depth,
            title,
            todo_keywords: None,
            file_hash,
            updated_at,
        }
    }

    /// Parse TODO keywords configuration
    /// Returns (active_keywords, done_keywords)
    pub fn parse_todo_keywords(&self) -> (Vec<String>, Vec<String>) {
        if let Some(ref config) = self.todo_keywords {
            let parts: Vec<&str> = config.split('|').collect();
            let active = parts
                .first()
                .map(|s| s.split(',').map(|k| k.trim().to_string()).collect())
                .unwrap_or_else(|| vec!["TODO".to_string()]);
            let done = parts
                .get(1)
                .map(|s| s.split(',').map(|k| k.trim().to_string()).collect())
                .unwrap_or_else(|| vec!["DONE".to_string()]);
            (active, done)
        } else {
            (vec!["TODO".to_string()], vec!["DONE".to_string()])
        }
    }

    /// Check if a keyword is "done" for this file's config
    pub fn is_done(&self, keyword: &str) -> bool {
        let (_, done_keywords) = self.parse_todo_keywords();
        done_keywords.contains(&keyword.to_string())
    }
}

impl holon::core::datasource::BlockEntity for OrgFile {
    fn id(&self) -> &str {
        &self.id
    }

    fn parent_id(&self) -> Option<&str> {
        Some(&self.parent_id)
    }

    fn sort_key(&self) -> &str {
        &self.name
    }

    fn depth(&self) -> i64 {
        self.depth
    }

    fn content(&self) -> &str {
        self.title.as_deref().unwrap_or(&self.name)
    }
}

impl holon::core::datasource::OperationRegistry for OrgFile {
    fn all_operations() -> Vec<holon::core::datasource::OperationDescriptor> {
        let entity_name = Self::entity_name();
        let short_name = Self::short_name().expect("OrgFile must have short_name");
        let table = entity_name;
        let id_column = "id";

        #[cfg(not(target_arch = "wasm32"))]
        {
            use holon::core::datasource::{
                __operations_crud_operation_provider, __operations_mutable_block_data_source,
            };
            __operations_crud_operation_provider::crud_operations(
                entity_name,
                short_name,
                table,
                id_column,
            )
            .into_iter()
            .chain(
                __operations_mutable_block_data_source::block_operations(
                    entity_name,
                    short_name,
                    table,
                    id_column,
                )
                .into_iter(),
            )
            .collect()
        }
        #[cfg(target_arch = "wasm32")]
        {
            Vec::new()
        }
    }

    fn entity_name() -> &'static str {
        "org_files"
    }

    fn short_name() -> Option<&'static str> {
        OrgFile::short_name()
    }
}

/// OrgHeadline - represents a headline within an org file
#[derive(Debug, Clone, Serialize, Deserialize, Entity)]
#[entity(name = "org_headlines", short_name = "headline")]
pub struct OrgHeadline {
    #[primary_key]
    #[indexed]
    pub id: String,

    /// Denormalized file ID for efficient queries
    #[indexed]
    pub file_id: String,

    /// Full path to the containing file (for write-back operations)
    pub file_path: String,

    /// Parent headline ID or file_id for top-level headlines
    #[indexed]
    pub parent_id: String,

    /// parent.depth + 1
    pub depth: i64,

    /// Position in file (for ordering)
    pub byte_start: i64,

    /// End position in file
    pub byte_end: i64,

    /// Headline title text (without TODO/priority/tags)
    pub title: String,

    /// Section body text (paragraph content)
    pub content: Option<String>,

    /// TODO keyword (e.g., "TODO", "DONE", custom keywords)
    pub todo_keyword: Option<String>,

    /// Priority: A=3, B=2, C=1
    pub priority: Option<i32>,

    /// Comma-separated tags
    pub tags: Option<String>,

    /// SCHEDULED timestamp (ISO 8601)
    pub scheduled: Option<String>,

    /// DEADLINE timestamp (ISO 8601)
    pub deadline: Option<String>,

    /// JSON-serialized property drawer
    pub properties: Option<String>,

    /// JSON-serialized source blocks found in the section
    /// Contains Vec<OrgSourceBlock> serialized as JSON
    pub source_blocks: Option<String>,
}

impl OrgHeadline {
    /// Create a new headline
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        file_id: String,
        file_path: String,
        parent_id: String,
        depth: i64,
        byte_start: i64,
        byte_end: i64,
        title: String,
    ) -> Self {
        Self {
            id,
            file_id,
            file_path,
            parent_id,
            depth,
            byte_start,
            byte_end,
            title,
            content: None,
            todo_keyword: None,
            priority: None,
            tags: None,
            scheduled: None,
            deadline: None,
            properties: None,
            source_blocks: None,
        }
    }

    /// Get parsed source blocks from the serialized JSON
    pub fn get_source_blocks(&self) -> Vec<OrgSourceBlock> {
        self.source_blocks
            .as_ref()
            .and_then(|json| serde_json::from_str(json).ok())
            .unwrap_or_default()
    }

    /// Set source blocks by serializing to JSON
    pub fn set_source_blocks(&mut self, blocks: Vec<OrgSourceBlock>) {
        if blocks.is_empty() {
            self.source_blocks = None;
        } else {
            self.source_blocks = serde_json::to_string(&blocks).ok();
        }
    }

    /// Check if this headline has any source blocks
    pub fn has_source_blocks(&self) -> bool {
        self.source_blocks.is_some()
    }

    /// Get all PRQL source blocks
    pub fn prql_blocks(&self) -> Vec<OrgSourceBlock> {
        self.get_source_blocks()
            .into_iter()
            .filter(|b| b.is_prql())
            .collect()
    }

    /// Get sort key as zero-padded byte_start
    pub fn computed_sort_key(&self) -> String {
        format!("{:012}", self.byte_start)
    }

    /// Parse tags from comma-separated string
    pub fn get_tags(&self) -> Vec<String> {
        self.tags
            .as_ref()
            .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default()
    }

    /// Check if this headline is completed (using default keywords)
    pub fn is_completed(&self) -> bool {
        self.todo_keyword
            .as_ref()
            .map(|kw| is_done_keyword(kw))
            .unwrap_or(false)
    }
}

/// OrgSourceBlock - represents a source block within a headline section.
///
/// This is a unified representation that can be serialized to/from:
/// - Org Mode: `#+BEGIN_SRC language :args ... #+END_SRC`
/// - Markdown: ` ```language ... ``` `
/// - Loro/JSON: Native storage
///
/// Tier 1 (all formats): language, source
/// Tier 2 (Org + Loro): name, header_args, results
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrgSourceBlock {
    /// Language identifier (e.g., "prql", "sql", "python", "rust")
    pub language: Option<String>,

    /// The source code content
    pub source: String,

    /// Optional block name for references (#+NAME: in Org Mode)
    pub name: Option<String>,

    /// Header arguments as key-value pairs (`:var x=1 :results table`)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub header_args: HashMap<String, String>,

    /// Byte offset within the section where this block starts
    pub byte_start: i64,

    /// Byte offset where this block ends
    pub byte_end: i64,
}

impl OrgSourceBlock {
    /// Create a new source block with minimal fields
    pub fn new(language: Option<String>, source: String, byte_start: i64, byte_end: i64) -> Self {
        Self {
            language,
            source,
            name: None,
            header_args: HashMap::new(),
            byte_start,
            byte_end,
        }
    }

    /// Check if this is a PRQL source block
    pub fn is_prql(&self) -> bool {
        self.language
            .as_ref()
            .map(|l| l.eq_ignore_ascii_case("prql"))
            .unwrap_or(false)
    }

    /// Parse header arguments string into key-value pairs
    /// Format: `:key1 value1 :key2 value2` or `:key1 :key2`
    pub fn parse_header_args(params: &str) -> HashMap<String, String> {
        let mut args = HashMap::new();
        let mut current_key: Option<String> = None;
        let mut current_value = String::new();

        for token in params.split_whitespace() {
            if token.starts_with(':') {
                // Save previous key-value pair
                if let Some(key) = current_key.take() {
                    args.insert(key, current_value.trim().to_string());
                    current_value.clear();
                }
                current_key = Some(token[1..].to_string());
            } else if current_key.is_some() {
                if !current_value.is_empty() {
                    current_value.push(' ');
                }
                current_value.push_str(token);
            }
        }

        // Save last key-value pair
        if let Some(key) = current_key {
            args.insert(key, current_value.trim().to_string());
        }

        args
    }

    /// Convert to holon_api::SourceBlock
    pub fn to_api_source_block(&self) -> holon_api::SourceBlock {
        let mut header_args = HashMap::new();
        for (k, v) in &self.header_args {
            header_args.insert(k.clone(), holon_api::Value::String(v.clone()));
        }

        holon_api::SourceBlock {
            language: self.language.clone().unwrap_or_default(),
            source: self.source.clone(),
            name: self.name.clone(),
            header_args,
            results: None,
        }
    }
}

/// Parsed section content with both text and source blocks
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ParsedSectionContent {
    /// Plain text content (paragraphs outside of source blocks)
    pub text: String,

    /// Source blocks found in this section
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_blocks: Vec<OrgSourceBlock>,
}

impl ParsedSectionContent {
    /// Check if there are any source blocks
    pub fn has_source_blocks(&self) -> bool {
        !self.source_blocks.is_empty()
    }

    /// Get all PRQL source blocks
    pub fn prql_blocks(&self) -> impl Iterator<Item = &OrgSourceBlock> {
        self.source_blocks.iter().filter(|b| b.is_prql())
    }
}

impl holon::core::datasource::BlockEntity for OrgHeadline {
    fn id(&self) -> &str {
        &self.id
    }

    fn parent_id(&self) -> Option<&str> {
        Some(&self.parent_id)
    }

    fn sort_key(&self) -> &str {
        // This is a limitation: we can't return a computed String from &str
        // The plan noted we may need to store sort_key as a field
        // For now, use a static placeholder - proper implementation would
        // require storing the computed key
        "a0"
    }

    fn depth(&self) -> i64 {
        self.depth
    }

    fn content(&self) -> &str {
        &self.title
    }
}

impl holon::core::datasource::TaskEntity for OrgHeadline {
    fn completed(&self) -> bool {
        self.is_completed()
    }

    fn priority(&self) -> Option<i64> {
        self.priority.map(|p| p as i64)
    }

    fn due_date(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.deadline.as_ref().and_then(|d| {
            chrono::DateTime::parse_from_rfc3339(d)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        })
    }
}

impl holon::core::datasource::OperationRegistry for OrgHeadline {
    fn all_operations() -> Vec<holon::core::datasource::OperationDescriptor> {
        let entity_name = Self::entity_name();
        let short_name = Self::short_name().expect("OrgHeadline must have short_name");
        let table = entity_name;
        let id_column = "id";

        #[cfg(not(target_arch = "wasm32"))]
        {
            use holon::core::datasource::{
                __operations_crud_operation_provider, __operations_mutable_block_data_source,
                __operations_mutable_task_data_source,
            };
            __operations_crud_operation_provider::crud_operations(
                entity_name,
                short_name,
                table,
                id_column,
            )
            .into_iter()
            .chain(
                __operations_mutable_block_data_source::block_operations(
                    entity_name,
                    short_name,
                    table,
                    id_column,
                )
                .into_iter(),
            )
            .chain(
                __operations_mutable_task_data_source::task_operations(
                    entity_name,
                    short_name,
                    table,
                    id_column,
                )
                .into_iter(),
            )
            .collect()
        }
        #[cfg(target_arch = "wasm32")]
        {
            Vec::new()
        }
    }

    fn entity_name() -> &'static str {
        "org_headlines"
    }

    fn short_name() -> Option<&'static str> {
        OrgHeadline::short_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_done_keyword() {
        assert!(is_done_keyword("DONE"));
        assert!(is_done_keyword("CANCELLED"));
        assert!(is_done_keyword("CLOSED"));
        assert!(!is_done_keyword("TODO"));
        assert!(!is_done_keyword("INPROGRESS"));
    }

    #[test]
    fn test_org_file_parse_todo_keywords() {
        let file = OrgFile {
            id: "test".to_string(),
            name: "test.org".to_string(),
            path: "/test/test.org".to_string(),
            parent_id: ROOT_ID.to_string(),
            depth: 1,
            title: None,
            todo_keywords: Some("TODO,INPROGRESS|DONE,CANCELLED".to_string()),
            file_hash: "abc".to_string(),
            updated_at: "2024-01-01".to_string(),
        };

        let (active, done) = file.parse_todo_keywords();
        assert_eq!(active, vec!["TODO", "INPROGRESS"]);
        assert_eq!(done, vec!["DONE", "CANCELLED"]);
        assert!(file.is_done("DONE"));
        assert!(file.is_done("CANCELLED"));
        assert!(!file.is_done("TODO"));
    }

    #[test]
    fn test_org_headline_computed_sort_key() {
        let headline = OrgHeadline::new(
            "id1".to_string(),
            "file1".to_string(),
            "/test/file1.org".to_string(),
            "file1".to_string(),
            2,
            42,
            100,
            "Test headline".to_string(),
        );

        assert_eq!(headline.computed_sort_key(), "000000000042");
    }
}
