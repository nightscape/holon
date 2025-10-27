//! Org-mode specific extensions for Document and Block types.
//!
//! This module provides extension traits that add org-mode specific functionality
//! to the generic Document and Block types. Org-specific fields are stored in the
//! `properties` JSON field.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Re-export the generic types
// Note: Block is NOT re-exported here to avoid duplicate type issues with flutter_rust_bridge
// Use holon_api::block::Block directly instead
pub use holon::sync::Document;

// Import Block for use in extension traits (not re-exported to avoid FRB issues)
use holon_api::block::{Block, CONTENT_TYPE_SOURCE};

// Re-export document URI functions from centralized location
pub use holon::sync::{is_document_uri, parse_document_uri, DOCUMENT_URI_SCHEME};

// Re-export Directory and ROOT_ID from holon-filesystem
pub use holon_filesystem::directory::{Directory, ROOT_ID};

/// Property keys for org-specific fields stored in properties JSON.
pub mod org_props {
    pub const TITLE: &str = "title";
    pub const TODO_KEYWORDS: &str = "todo_keywords";
    pub const TASK_STATE: &str = "task_state";
    pub const PRIORITY: &str = "priority";
    pub const TAGS: &str = "tags";
    pub const LEVEL: &str = "level";
    pub const SEQUENCE: &str = "sequence";
    pub const SCHEDULED: &str = "scheduled";
    pub const DEADLINE: &str = "deadline";
    pub const ORG_PROPERTIES: &str = "org_properties";
}

// =============================================================================
// Path derivation utilities for org-mode
// =============================================================================

/// Trait for resolving blocks by ID (used for parent chain walking)
pub trait BlockResolver {
    /// Get a block by its ID
    fn get_block(&self, id: &str) -> Option<Block>;
}

/// Find the document ID for a block by walking up the parent chain
///
/// For org-mode blocks:
/// - Top-level blocks have parent_id == document_id (a document URI)
/// - Nested blocks have parent_id pointing to another block
///
/// This function walks up the parent chain until it finds a document ID.
pub fn find_document_id<R: BlockResolver>(block: &Block, resolver: &R) -> Option<String> {
    // Check if parent is already a document
    if is_document_uri(&block.parent_id) {
        return Some(block.parent_id.clone());
    }

    // Walk up the parent chain
    let mut current_parent_id = block.parent_id.clone();
    let mut visited = std::collections::HashSet::new();

    while !is_document_uri(&current_parent_id) {
        // Prevent infinite loops
        if visited.contains(&current_parent_id) {
            return None;
        }
        visited.insert(current_parent_id.clone());

        // Look up the parent block
        let parent = resolver.get_block(&current_parent_id)?;
        current_parent_id = parent.parent_id.clone();
    }

    Some(current_parent_id)
}

/// Get the file path for a block by finding its document and extracting the path
pub fn get_block_file_path<R: BlockResolver>(block: &Block, resolver: &R) -> Option<String> {
    let doc_id = find_document_id(block, resolver)?;
    parse_document_uri(&doc_id).map(|s| s.to_string())
}

/// Simple in-memory block resolver using a HashMap
pub struct HashMapBlockResolver {
    blocks: HashMap<String, Block>,
}

impl HashMapBlockResolver {
    pub fn new() -> Self {
        Self {
            blocks: HashMap::new(),
        }
    }

    pub fn insert(&mut self, block: Block) {
        self.blocks.insert(block.id.clone(), block);
    }

    pub fn from_blocks(blocks: Vec<Block>) -> Self {
        let mut resolver = Self::new();
        for block in blocks {
            resolver.insert(block);
        }
        resolver
    }
}

impl Default for HashMapBlockResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockResolver for HashMapBlockResolver {
    fn get_block(&self, id: &str) -> Option<Block> {
        self.blocks.get(id).cloned()
    }
}

/// Default active keywords when file doesn't specify custom TODO config
pub const DEFAULT_ACTIVE_KEYWORDS: &[&str] = &["TODO", "DOING"];

/// Default done keywords when file doesn't specify custom TODO config
pub const DEFAULT_DONE_KEYWORDS: &[&str] = &["DONE", "CANCELLED", "CLOSED"];

/// Check if a keyword is considered "done" using default keywords
pub fn is_done_keyword(keyword: &str) -> bool {
    DEFAULT_DONE_KEYWORDS.contains(&keyword)
}

/// Trait for converting entities to org-mode formatted strings
pub trait ToOrg {
    fn to_org(&self) -> String;
}

/// Convert priority integer to string (3=A, 2=B, 1=C)
fn priority_int_to_str(priority: i32) -> String {
    match priority {
        3 => "A".to_string(),
        2 => "B".to_string(),
        1 => "C".to_string(),
        _ => "A".to_string(),
    }
}

/// Format tags from comma-separated string to org-mode format
/// Input: "tag1,tag2" -> Output: ":tag1:tag2:"
fn format_tags(tags: &str) -> String {
    if tags.is_empty() {
        return String::new();
    }
    let tag_list: Vec<&str> = tags
        .split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .collect();
    if tag_list.is_empty() {
        return String::new();
    }
    format!(":{}:", tag_list.join(":"))
}

/// Format properties drawer from JSON
/// Input: JSON string -> Output: ":PROPERTIES:\n:KEY: VALUE\n:END:"
/// Ensures :ID: property is rendered first.
fn format_properties_drawer(properties_json: &str) -> String {
    let props: serde_json::Map<String, serde_json::Value> =
        match serde_json::from_str(properties_json) {
            Ok(map) => map,
            Err(_) => return String::new(),
        };

    if props.is_empty() {
        return String::new();
    }

    let mut result = String::from(":PROPERTIES:\n");

    // Render :ID: first if present
    if let Some(id_value) = props.get("ID") {
        let value_str = match id_value {
            serde_json::Value::String(s) => s.clone(),
            _ => id_value.to_string(),
        };
        result.push_str(&format!(":ID: {}\n", value_str));
    }

    // Render other properties (excluding ID which we already rendered)
    for (key, value) in &props {
        if key == "ID" {
            continue;
        }
        let value_str = match value {
            serde_json::Value::String(s) => s.clone(),
            _ => value.to_string(),
        };
        result.push_str(&format!(":{}: {}\n", key, value_str));
    }
    result.push_str(":END:");
    result
}

/// Format planning lines (SCHEDULED/DEADLINE)
fn format_planning(scheduled: Option<&str>, deadline: Option<&str>) -> String {
    let mut result = String::new();
    if let Some(sched) = scheduled {
        result.push_str(&format!("SCHEDULED: {}\n", sched.trim()));
    }
    if let Some(dead) = deadline {
        result.push_str(&format!("DEADLINE: {}\n", dead.trim()));
    }
    result
}

/// Format header arguments as Org Mode inline parameters.
/// Input: `{ "connection": "main", "results": "table" }`
/// Output: `:connection main :results table`
#[allow(dead_code)]
fn format_header_args(args: &HashMap<String, String>) -> String {
    if args.is_empty() {
        return String::new();
    }

    let mut parts: Vec<String> = args
        .iter()
        .map(|(k, v)| {
            if v.is_empty() {
                format!(":{}", k)
            } else {
                format!(":{} {}", k, v)
            }
        })
        .collect();

    parts.sort();
    parts.join(" ")
}

/// Format header arguments with Value types as Org Mode inline parameters.
/// Input: `{ "connection": String("main"), "results": String("table") }`
/// Output: `:connection main :results table`
fn format_header_args_value(args: &HashMap<String, holon_api::Value>) -> String {
    if args.is_empty() {
        return String::new();
    }

    let mut parts: Vec<String> = args
        .iter()
        .map(|(k, v)| {
            let v_str = match v {
                holon_api::Value::String(s) => s.clone(),
                holon_api::Value::Integer(i) => i.to_string(),
                holon_api::Value::Float(f) => f.to_string(),
                holon_api::Value::Boolean(b) => b.to_string(),
                holon_api::Value::Null => String::new(),
                holon_api::Value::Json(j) => j.to_string(),
                holon_api::Value::DateTime(dt) => dt.to_string(),
                holon_api::Value::Array(_) => "[array]".to_string(),
                holon_api::Value::Object(_) => "[object]".to_string(),
            };
            if v_str.is_empty() {
                format!(":{}", k)
            } else {
                format!(":{} {}", k, v_str)
            }
        })
        .collect();

    parts.sort();
    parts.join(" ")
}

// =============================================================================
// OrgDocumentExt - Extension trait for Document with org-specific functionality
// =============================================================================

/// Extension trait for Document with org-mode specific functionality.
///
/// Provides accessors for org-specific fields stored in the properties JSON:
/// - title: #+TITLE value
/// - todo_keywords: Custom TODO keyword configuration
pub trait OrgDocumentExt {
    /// Get the org title (#+TITLE value)
    fn org_title(&self) -> Option<String>;

    /// Set the org title
    fn set_org_title(&mut self, title: Option<String>);

    /// Get the TODO keywords configuration (format: "TODO,DOING|DONE,CANCELLED")
    fn todo_keywords(&self) -> Option<String>;

    /// Set the TODO keywords configuration
    fn set_todo_keywords(&mut self, keywords: Option<String>);

    /// Parse TODO keywords configuration into (active, done) keyword lists
    fn parse_todo_keywords(&self) -> (Vec<String>, Vec<String>);

    /// Check if a keyword is "done" according to this document's configuration
    fn is_done(&self, keyword: &str) -> bool;
}

impl OrgDocumentExt for Document {
    fn org_title(&self) -> Option<String> {
        self.get_property(org_props::TITLE)
            .and_then(|v| v.as_string().map(|s| s.to_string()))
    }

    fn set_org_title(&mut self, title: Option<String>) {
        if let Some(t) = title {
            self.set_property(org_props::TITLE, t);
        } else {
            let mut props = self.properties_map();
            props.remove(org_props::TITLE);
            let json_props: std::collections::HashMap<String, serde_json::Value> = props
                .into_iter()
                .map(|(k, v)| (k, v.as_json_value().unwrap_or(serde_json::Value::Null)))
                .collect();
            self.properties =
                serde_json::to_string(&json_props).unwrap_or_else(|_| "{}".to_string());
        }
    }

    fn todo_keywords(&self) -> Option<String> {
        self.get_property(org_props::TODO_KEYWORDS)
            .and_then(|v| v.as_string().map(|s| s.to_string()))
    }

    fn set_todo_keywords(&mut self, keywords: Option<String>) {
        if let Some(kw) = keywords {
            self.set_property(org_props::TODO_KEYWORDS, kw);
        } else {
            let mut props = self.properties_map();
            props.remove(org_props::TODO_KEYWORDS);
            let json_props: std::collections::HashMap<String, serde_json::Value> = props
                .into_iter()
                .map(|(k, v)| (k, v.as_json_value().unwrap_or(serde_json::Value::Null)))
                .collect();
            self.properties =
                serde_json::to_string(&json_props).unwrap_or_else(|_| "{}".to_string());
        }
    }

    fn parse_todo_keywords(&self) -> (Vec<String>, Vec<String>) {
        if let Some(ref config) = self.todo_keywords() {
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

    fn is_done(&self, keyword: &str) -> bool {
        let (_, done_keywords) = self.parse_todo_keywords();
        done_keywords.contains(&keyword.to_string())
    }
}

impl ToOrg for Document {
    fn to_org(&self) -> String {
        let mut result = String::new();

        // File title
        if let Some(title) = self.org_title() {
            result.push_str(&format!("#+TITLE: {}\n", title));
        }

        // TODO keywords configuration
        if let Some(todo_config) = self.todo_keywords() {
            let parts: Vec<&str> = todo_config.split('|').collect();
            let active = parts.first().map(|s| s.trim()).unwrap_or("");
            let done = parts.get(1).map(|s| s.trim()).unwrap_or("");

            if !active.is_empty() || !done.is_empty() {
                result.push_str("#+TODO:");
                if !active.is_empty() {
                    let active_formatted = active.replace(',', " ");
                    result.push_str(&format!(" {}", active_formatted));
                }
                if !done.is_empty() {
                    let done_formatted = done.replace(',', " ");
                    result.push_str(&format!(" | {}", done_formatted));
                }
                result.push('\n');
            }
        }

        // Ensure result ends with newline if non-empty
        if !result.is_empty() && !result.ends_with('\n') {
            result.push('\n');
        }

        result
    }
}

// =============================================================================
// OrgBlockExt - Extension trait for Block with org-specific functionality
// =============================================================================

/// Extension trait for Block with org-mode specific functionality.
///
/// Provides accessors for org-specific fields stored in properties JSON:
/// - level: Headline level (number of stars)
/// - sequence: Ordering within file
/// - task_state: TODO keyword
/// - priority: A=3, B=2, C=1
/// - tags: Comma-separated tag list
/// - scheduled/deadline: Planning timestamps
/// - source_blocks: Embedded source blocks
pub trait OrgBlockExt {
    /// Get the headline level (number of stars: 1-6)
    fn level(&self) -> i64;

    /// Set the headline level
    fn set_level(&mut self, level: i64);

    /// Get the sequence number for ordering
    fn sequence(&self) -> i64;

    /// Set the sequence number
    fn set_sequence(&mut self, sequence: i64);

    /// Get the headline title (first line of content)
    fn org_title(&self) -> String;

    /// Get the body text (content after first line)
    fn body(&self) -> Option<String>;

    /// Set content from title and body
    fn set_title_and_body(&mut self, title: String, body: Option<String>);

    /// Get the task state (TODO keyword)
    fn task_state(&self) -> Option<String>;

    /// Set the task state
    fn set_task_state(&mut self, state: Option<String>);

    /// Get the priority (A=3, B=2, C=1)
    fn priority(&self) -> Option<i32>;

    /// Set the priority
    fn set_priority(&mut self, priority: Option<i32>);

    /// Get the tags (comma-separated)
    fn tags(&self) -> Option<String>;

    /// Set the tags
    fn set_tags(&mut self, tags: Option<String>);

    /// Get the scheduled timestamp
    fn scheduled(&self) -> Option<String>;

    /// Set the scheduled timestamp
    fn set_scheduled(&mut self, scheduled: Option<String>);

    /// Get the deadline timestamp
    fn deadline(&self) -> Option<String>;

    /// Set the deadline timestamp
    fn set_deadline(&mut self, deadline: Option<String>);

    /// Get the org properties drawer as JSON
    fn org_properties(&self) -> Option<String>;

    /// Set the org properties drawer
    fn set_org_properties(&mut self, properties: Option<String>);

    /// Get sort key as zero-padded sequence
    fn computed_sort_key(&self) -> String;

    /// Parse tags from comma-separated string
    fn get_tags(&self) -> Vec<String>;

    /// Check if this block is completed (using default keywords)
    fn is_completed(&self) -> bool;

    /// Get the block ID from the properties drawer
    fn get_block_id(&self) -> Option<String>;
}

impl OrgBlockExt for Block {
    fn level(&self) -> i64 {
        self.get_property(org_props::LEVEL)
            .and_then(|v| v.as_i64())
            .unwrap_or(1)
    }

    fn set_level(&mut self, level: i64) {
        self.set_property(org_props::LEVEL, holon_api::Value::Integer(level));
    }

    fn sequence(&self) -> i64 {
        self.get_property(org_props::SEQUENCE)
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
    }

    fn set_sequence(&mut self, sequence: i64) {
        self.set_property(org_props::SEQUENCE, holon_api::Value::Integer(sequence));
    }

    fn org_title(&self) -> String {
        self.content.lines().next().unwrap_or("").to_string()
    }

    fn body(&self) -> Option<String> {
        let lines: Vec<&str> = self.content.lines().collect();
        if lines.len() > 1 {
            Some(lines[1..].join("\n"))
        } else {
            None
        }
    }

    fn set_title_and_body(&mut self, title: String, body: Option<String>) {
        if let Some(b) = body {
            self.content = format!("{}\n{}", title, b);
        } else {
            self.content = title;
        }
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    fn task_state(&self) -> Option<String> {
        self.get_property(org_props::TASK_STATE)
            .and_then(|v| v.as_string().map(|s| s.to_string()))
    }

    fn set_task_state(&mut self, state: Option<String>) {
        if let Some(s) = state {
            self.set_property(org_props::TASK_STATE, holon_api::Value::String(s));
        } else {
            let mut props = self.properties_map();
            props.remove(org_props::TASK_STATE);
            self.set_properties_map(props);
        }
    }

    fn priority(&self) -> Option<i32> {
        self.get_property(org_props::PRIORITY)
            .and_then(|v| v.as_i64())
            .map(|i| i as i32)
    }

    fn set_priority(&mut self, priority: Option<i32>) {
        if let Some(p) = priority {
            self.set_property(org_props::PRIORITY, holon_api::Value::Integer(p as i64));
        } else {
            let mut props = self.properties_map();
            props.remove(org_props::PRIORITY);
            self.set_properties_map(props);
        }
    }

    fn tags(&self) -> Option<String> {
        self.get_property(org_props::TAGS)
            .and_then(|v| v.as_string().map(|s| s.to_string()))
    }

    fn set_tags(&mut self, tags: Option<String>) {
        if let Some(t) = tags {
            self.set_property(org_props::TAGS, holon_api::Value::String(t));
        } else {
            let mut props = self.properties_map();
            props.remove(org_props::TAGS);
            self.set_properties_map(props);
        }
    }

    fn scheduled(&self) -> Option<String> {
        self.get_property(org_props::SCHEDULED)
            .and_then(|v| v.as_string().map(|s| s.to_string()))
    }

    fn set_scheduled(&mut self, scheduled: Option<String>) {
        if let Some(s) = scheduled {
            self.set_property(org_props::SCHEDULED, holon_api::Value::String(s));
        } else {
            let mut props = self.properties_map();
            props.remove(org_props::SCHEDULED);
            self.set_properties_map(props);
        }
    }

    fn deadline(&self) -> Option<String> {
        self.get_property(org_props::DEADLINE)
            .and_then(|v| v.as_string().map(|s| s.to_string()))
    }

    fn set_deadline(&mut self, deadline: Option<String>) {
        if let Some(d) = deadline {
            self.set_property(org_props::DEADLINE, holon_api::Value::String(d));
        } else {
            let mut props = self.properties_map();
            props.remove(org_props::DEADLINE);
            self.set_properties_map(props);
        }
    }

    fn org_properties(&self) -> Option<String> {
        self.get_property(org_props::ORG_PROPERTIES)
            .and_then(|v| v.as_string().map(|s| s.to_string()))
    }

    fn set_org_properties(&mut self, properties: Option<String>) {
        if let Some(p) = properties {
            self.set_property(org_props::ORG_PROPERTIES, holon_api::Value::String(p));
        } else {
            let mut props = self.properties_map();
            props.remove(org_props::ORG_PROPERTIES);
            self.set_properties_map(props);
        }
    }

    fn computed_sort_key(&self) -> String {
        format!("{:012}", self.sequence())
    }

    fn get_tags(&self) -> Vec<String> {
        self.tags()
            .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default()
    }

    fn is_completed(&self) -> bool {
        self.task_state()
            .map(|kw| is_done_keyword(&kw))
            .unwrap_or(false)
    }

    fn get_block_id(&self) -> Option<String> {
        self.org_properties()
            .and_then(|json| serde_json::from_str::<HashMap<String, String>>(&json).ok())
            .and_then(|props| props.get("ID").cloned())
            .or_else(|| {
                self.org_properties()
                    .and_then(|json| {
                        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&json)
                            .ok()
                    })
                    .and_then(|props| {
                        props
                            .get("ID")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
            })
    }
}

impl ToOrg for Block {
    fn to_org(&self) -> String {
        // Source blocks render as #+BEGIN_SRC ... #+END_SRC
        if self.content_type == CONTENT_TYPE_SOURCE {
            return source_block_to_org(self);
        }

        // Text blocks (headlines) render with stars, TODO, etc.
        let mut result = String::new();

        // Headline level (stars)
        result.push_str(&"*".repeat(self.level() as usize));
        result.push(' ');

        // TODO keyword
        if let Some(todo) = self.task_state() {
            result.push_str(&todo);
            result.push(' ');
        }

        // Priority
        if let Some(priority) = self.priority() {
            result.push_str(&format!("[#{}] ", priority_int_to_str(priority)));
        }

        // Title
        result.push_str(&self.org_title());

        // Tags
        if let Some(tags) = self.tags() {
            let formatted_tags = format_tags(&tags);
            if !formatted_tags.is_empty() {
                result.push(' ');
                result.push_str(&formatted_tags);
            }
        }

        result.push('\n');

        // Properties drawer
        if let Some(props_json) = self.org_properties() {
            let props_drawer = format_properties_drawer(&props_json);
            if !props_drawer.is_empty() {
                result.push_str(&props_drawer);
                result.push('\n');
            }
        }

        // Planning (SCHEDULED/DEADLINE)
        let planning = format_planning(self.scheduled().as_deref(), self.deadline().as_deref());
        if !planning.is_empty() {
            result.push_str(&planning);
        }

        // Body text (source blocks are child Block entities, rendered via tree traversal)
        if let Some(body) = self.body() {
            let trimmed_body = body.trim();
            if !trimmed_body.is_empty() {
                result.push_str(trimmed_body);
                if !trimmed_body.ends_with('\n') {
                    result.push('\n');
                }
                result.push('\n');
            }
        }

        // Ensure result ends with newline if non-empty
        if !result.is_empty() && !result.ends_with('\n') {
            result.push('\n');
        }

        result
    }
}

/// Render a source-type Block as Org Mode #+BEGIN_SRC ... #+END_SRC
fn source_block_to_org(block: &Block) -> String {
    let mut result = String::new();

    // #+NAME: if present
    if let Some(ref name) = block.source_name {
        result.push_str("#+NAME: ");
        result.push_str(name);
        result.push('\n');
    }

    result.push_str("#+BEGIN_SRC");

    // Language
    if let Some(ref lang) = block.source_language {
        result.push(' ');
        result.push_str(lang);
    }

    // Include block ID in header arguments so it survives round-trips
    // This is critical for preventing orphan blocks when Org files are re-parsed
    result.push_str(" :id ");
    result.push_str(&block.id);

    // Header arguments
    let header_args = block.get_source_header_args();
    let header_args_str = format_header_args_value(&header_args);
    if !header_args_str.is_empty() {
        result.push(' ');
        result.push_str(&header_args_str);
    }

    result.push('\n');

    // Source code
    result.push_str(&block.content);
    if !block.content.ends_with('\n') {
        result.push('\n');
    }

    result.push_str("#+END_SRC\n");

    result
}

// Note: We re-export SourceBlock from holon_api to use it directly
pub use holon_api::SourceBlock;

/// Parse header arguments string into key-value pairs
/// Format: `:key1 value1 :key2 value2` or `:key1 :key2`
pub fn parse_header_args_from_str(params: &str) -> HashMap<String, String> {
    let mut args = HashMap::new();
    let mut current_key: Option<String> = None;
    let mut current_value = String::new();

    for token in params.split_whitespace() {
        if token.starts_with(':') {
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

    if let Some(key) = current_key {
        args.insert(key, current_value.trim().to_string());
    }

    args
}

impl ToOrg for SourceBlock {
    fn to_org(&self) -> String {
        let mut result = String::new();

        if let Some(ref name) = self.name {
            result.push_str("#+NAME: ");
            result.push_str(name);
            result.push('\n');
        }

        result.push_str("#+BEGIN_SRC");

        if let Some(ref lang) = self.language {
            result.push(' ');
            result.push_str(lang);
        }

        let header_args_str = format_header_args_value(&self.header_args);
        if !header_args_str.is_empty() {
            result.push(' ');
            result.push_str(&header_args_str);
        }

        result.push('\n');
        result.push_str(&self.source);

        if !self.source.ends_with('\n') {
            result.push('\n');
        }

        result.push_str("#+END_SRC");

        // Ensure trailing newline
        if !result.ends_with('\n') {
            result.push('\n');
        }

        result
    }
}

// =============================================================================
// ParsedSectionContent - Helper for parsed section data
// =============================================================================

/// Parsed section content with both text and source blocks
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ParsedSectionContent {
    /// Plain text content (paragraphs outside of source blocks)
    pub text: String,

    /// Source blocks found in this section
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_blocks: Vec<SourceBlock>,
}

impl ParsedSectionContent {
    /// Check if there are any source blocks
    pub fn has_source_blocks(&self) -> bool {
        !self.source_blocks.is_empty()
    }

    /// Get all PRQL source blocks
    pub fn prql_blocks(&self) -> impl Iterator<Item = &SourceBlock> {
        self.source_blocks.iter().filter(|b| b.is_prql())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use holon_api::block::ROOT_DOC_ID;

    #[test]
    fn test_is_org_document_id() {
        assert!(is_document_uri("holon-doc:///path/to/file.org"));
        assert!(is_document_uri("holon-doc://relative/path.org"));
        assert!(!is_document_uri("some-block-id"));
        assert!(!is_document_uri("doc:123:root"));
    }

    #[test]
    fn test_path_from_org_document_id() {
        assert_eq!(
            parse_document_uri("holon-doc:///path/to/file.org"),
            Some("/path/to/file.org")
        );
        assert_eq!(
            parse_document_uri("holon-doc://relative/path.org"),
            Some("relative/path.org")
        );
        assert_eq!(parse_document_uri("some-block-id"), None);
    }

    #[test]
    fn test_find_document_id_top_level() {
        // Top-level block has document as parent
        let block = Block::new_text("block1", "holon-doc:///test.org", "Test headline");
        let resolver = HashMapBlockResolver::new();

        let doc_id = find_document_id(&block, &resolver);
        assert_eq!(doc_id, Some("holon-doc:///test.org".to_string()));
    }

    #[test]
    fn test_find_document_id_nested() {
        // Nested block hierarchy: doc -> block1 -> block2
        let block1 = Block::new_text("block1", "holon-doc:///test.org", "Parent headline");
        let block2 = Block::new_text("block2", "block1", "Child headline");

        let resolver = HashMapBlockResolver::from_blocks(vec![block1.clone(), block2.clone()]);

        let doc_id = find_document_id(&block2, &resolver);
        assert_eq!(doc_id, Some("holon-doc:///test.org".to_string()));
    }

    #[test]
    fn test_find_document_id_deeply_nested() {
        // doc -> block1 -> block2 -> block3
        let block1 = Block::new_text("block1", "holon-doc:///test.org", "Level 1");
        let block2 = Block::new_text("block2", "block1", "Level 2");
        let block3 = Block::new_text("block3", "block2", "Level 3");

        let resolver =
            HashMapBlockResolver::from_blocks(vec![block1.clone(), block2.clone(), block3.clone()]);

        let doc_id = find_document_id(&block3, &resolver);
        assert_eq!(doc_id, Some("holon-doc:///test.org".to_string()));
    }

    #[test]
    fn test_get_block_file_path() {
        let block = Block::new_text("block1", "holon-doc:///path/to/notes.org", "Test");
        let resolver = HashMapBlockResolver::new();

        let path = get_block_file_path(&block, &resolver);
        assert_eq!(path, Some("/path/to/notes.org".to_string()));
    }

    #[test]
    fn test_is_done_keyword() {
        assert!(is_done_keyword("DONE"));
        assert!(is_done_keyword("CANCELLED"));
        assert!(is_done_keyword("CLOSED"));
        assert!(!is_done_keyword("TODO"));
        assert!(!is_done_keyword("INPROGRESS"));
    }

    #[test]
    fn test_document_todo_keywords() {
        let mut doc = Document::new(
            "test".to_string(),
            ROOT_DOC_ID.to_string(),
            "test.org".to_string(),
        );
        doc.set_todo_keywords(Some("TODO,INPROGRESS|DONE,CANCELLED".to_string()));

        let (active, done) = doc.parse_todo_keywords();
        assert_eq!(active, vec!["TODO", "INPROGRESS"]);
        assert_eq!(done, vec!["DONE", "CANCELLED"]);
        assert!(doc.is_done("DONE"));
        assert!(doc.is_done("CANCELLED"));
        assert!(!doc.is_done("TODO"));
    }

    #[test]
    fn test_block_computed_sort_key() {
        let mut block = Block::new_text("id1", "parent1", "Test headline");
        block.set_sequence(42);

        assert_eq!(block.computed_sort_key(), "000000000042");
    }

    #[test]
    fn test_block_title_and_body() {
        let mut block = Block::new_text("id1", "parent1", "Title line\nBody line 1\nBody line 2");

        assert_eq!(block.org_title(), "Title line");
        assert_eq!(block.body(), Some("Body line 1\nBody line 2".to_string()));

        block.set_title_and_body("New title".to_string(), Some("New body".to_string()));
        assert_eq!(block.org_title(), "New title");
        assert_eq!(block.body(), Some("New body".to_string()));
    }

    #[test]
    fn test_block_org_properties() {
        let mut block = Block::new_text("id1", "parent1", "Test");
        block.set_level(2);
        block.set_task_state(Some("TODO".to_string()));
        block.set_priority(Some(2));
        block.set_tags(Some("work,urgent".to_string()));

        assert_eq!(block.level(), 2);
        assert_eq!(block.task_state(), Some("TODO".to_string()));
        assert_eq!(block.priority(), Some(2));
        assert_eq!(block.tags(), Some("work,urgent".to_string()));
    }

    #[test]
    fn test_document_to_org() {
        let mut doc = Document::new(
            "test".to_string(),
            ROOT_DOC_ID.to_string(),
            "test.org".to_string(),
        );
        doc.set_org_title(Some("My Document".to_string()));
        doc.set_todo_keywords(Some("TODO,DOING|DONE".to_string()));

        let org = doc.to_org();
        assert!(org.contains("#+TITLE: My Document"));
        assert!(org.contains("#+TODO: TODO DOING | DONE"));
    }

    #[test]
    fn test_block_to_org() {
        let mut block = Block::new_text("id1", "parent1", "Test headline");
        block.set_level(2);
        block.set_task_state(Some("TODO".to_string()));
        block.set_priority(Some(3));
        block.set_tags(Some("work,urgent".to_string()));

        let org = block.to_org();
        assert!(org.starts_with("** TODO [#A] Test headline :work:urgent:"));
    }
}
