use crate::models::{
    parse_header_args_from_str, Document, OrgBlockExt, OrgDocumentExt, SourceBlock,
};
use anyhow::Result;
use chrono::Utc;
use holon::sync::document_entity::DOCUMENT_URI_SCHEME;
use holon_api::block::Block;
use holon_api::block::{CONTENT_TYPE_SOURCE, CONTENT_TYPE_TEXT};
use orgize::ast::{Headline, SourceBlock as OrgizeSourceBlock};
use orgize::rowan::ast::AstNode;
use orgize::{Org, ParseConfig, SyntaxKind};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use uuid::Uuid;

/// Generate a directory ID from its path (ID is the relative path from root)
pub fn generate_directory_id(path: &Path, root_directory: &Path) -> String {
    path.strip_prefix(root_directory)
        .map(|rel_path| rel_path.to_string_lossy().to_string())
        .unwrap_or_else(|_| path.to_string_lossy().to_string())
}

/// Generate a deterministic document URI for a file based on its path relative to a root.
///
/// The root is canonicalized to handle symlinks (e.g., /var -> /private/var on macOS),
/// and the path is made relative to produce portable, sync-friendly URIs like:
/// - `holon-doc://doc.org` for files in the root
/// - `holon-doc://projects/todo.org` for nested files
pub fn generate_file_id(path: &Path, root: &Path) -> String {
    // Canonicalize both paths to handle symlinks consistently
    let canonical_path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let canonical_root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());

    // Make path relative to root
    let relative = canonical_path
        .strip_prefix(&canonical_root)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| canonical_path.to_string_lossy().to_string());

    format!("{DOCUMENT_URI_SCHEME}{relative}")
}

/// Generate a document URI from a path string (already relative to root).
pub fn generate_file_id_from_relative_path(relative_path: &str) -> String {
    format!("{DOCUMENT_URI_SCHEME}{relative_path}")
}

/// Compute content hash for change detection
pub fn compute_content_hash(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}

/// Result of parsing an org file
pub struct ParseResult {
    pub document: Document,
    pub blocks: Vec<Block>,
    /// Block IDs that need :ID: property added (for write-back)
    pub headlines_needing_ids: Vec<String>,
}

/// Parse TODO keywords from file content (#+TODO: or #+SEQ_TODO: lines)
fn parse_todo_keywords_config(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("#+TODO:") || trimmed.starts_with("#+SEQ_TODO:") {
            let spec = trimmed
                .split_once(':')
                .map(|(_, rest)| rest.trim())
                .unwrap_or("");
            if !spec.is_empty() {
                return Some(spec.replace(" | ", "|").replace(' ', ","));
            }
        }
    }
    None
}

/// Parse #+TITLE: from file content
fn parse_title(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("#+TITLE:") {
            return trimmed
                .split_once(':')
                .map(|(_, rest)| rest.trim().to_string());
        }
    }
    None
}

/// Convert priority string to integer (A=3, B=2, C=1)
fn priority_str_to_int(priority: &str) -> i32 {
    match priority.trim() {
        "A" => 3,
        "B" => 2,
        "C" => 1,
        _ => 0,
    }
}

/// Parse an org file and return Document + Block entities
pub fn parse_org_file(
    path: &Path,
    content: &str,
    parent_dir_id: &str,
    _parent_depth: i64,
    root: &Path,
) -> Result<ParseResult> {
    let file_id = generate_file_id(path, root);
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    // Parse file-level metadata
    let title = parse_title(content);
    let todo_keywords = parse_todo_keywords_config(content);

    // Create Document entity
    let mut document = Document::new(file_id.clone(), parent_dir_id.to_string(), file_name);

    // Set org-specific properties using extension trait
    document.set_org_title(title);
    document.set_todo_keywords(todo_keywords.clone());

    // Parse org content
    let org = if let Some(ref kw) = todo_keywords {
        let (active, done) = parse_keywords_from_config(kw);
        let config = ParseConfig {
            todo_keywords: (active, done),
            ..Default::default()
        };
        config.parse(content)
    } else {
        Org::parse(content)
    };

    // Extract blocks (headlines)
    let mut blocks = Vec::new();
    let mut headlines_needing_ids = Vec::new();
    let mut sequence_counter = 0i64;

    // Process document headlines recursively
    let doc = org.document();
    process_headlines(
        doc.headlines(),
        &file_id, // Top-level headlines have document as parent
        &mut sequence_counter,
        &mut blocks,
        &mut headlines_needing_ids,
    )?;

    Ok(ParseResult {
        document,
        blocks,
        headlines_needing_ids,
    })
}

/// Parse keywords config string "TODO,INPROGRESS|DONE,CANCELLED" into (Vec<String>, Vec<String>)
fn parse_keywords_from_config(config: &str) -> (Vec<String>, Vec<String>) {
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
}

/// Recursively process headlines and their children
fn process_headlines(
    headlines: impl Iterator<Item = Headline>,
    parent_id: &str,
    sequence_counter: &mut i64,
    output: &mut Vec<Block>,
    needs_id: &mut Vec<String>,
) -> Result<()> {
    for headline in headlines {
        // Extract headline level (number of stars)
        let level = headline.level() as i64;

        // Assign sequence number
        let sequence = *sequence_counter;
        *sequence_counter += 1;

        // Extract :ID: property if exists
        let (id, needs_write) = extract_or_generate_id(&headline);
        if needs_write {
            needs_id.push(id.clone());
        }

        // Extract TODO keyword first
        let task_state = headline.todo_keyword().map(|t| t.to_string());

        // Extract title using title_raw() and remove TODO keyword if present
        let mut title = headline.title_raw().trim().to_string();
        if let Some(ref todo) = task_state {
            if title.starts_with(todo) {
                title = title[todo.len()..].trim_start().to_string();
            }
        }

        // Extract priority (Token contains just the letter like "A")
        let priority = headline
            .priority()
            .map(|t| priority_str_to_int(&t.to_string()));

        // Extract tags
        let tags: Option<String> = {
            let tag_list: Vec<String> = headline.tags().map(|t| t.to_string()).collect();
            if tag_list.is_empty() {
                None
            } else {
                Some(tag_list.join(","))
            }
        };

        // Extract section content with source blocks
        let (body, mut source_blocks) = extract_section_content(&headline);

        // Look for #+NAME: directives for each source block that doesn't have a name yet
        let source_blocks_for_name_lookup = source_blocks.clone();
        for source_block in &mut source_blocks {
            if source_block.name.is_none() {
                source_block.name =
                    find_block_name_in_section(&headline, &source_blocks_for_name_lookup);
            }
        }

        // Extract planning (SCHEDULED, DEADLINE)
        let (scheduled, deadline) = extract_planning(&headline);

        // Extract properties as JSON
        let org_properties = extract_properties(&headline);

        // Create Block entity - content is title + body combined
        let content = if let Some(ref b) = body {
            format!("{}\n{}", title, b)
        } else {
            title.clone()
        };

        let now = Utc::now().timestamp_millis();
        let mut block = Block {
            id: id.clone(),
            parent_id: parent_id.to_string(),
            content,
            content_type: CONTENT_TYPE_TEXT.to_string(),
            source_language: None,
            source_name: None,
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        };

        // Set org-specific properties using extension trait
        block.set_level(level);
        block.set_sequence(sequence);
        block.set_task_state(task_state);
        block.set_priority(priority);
        block.set_tags(tags);
        block.set_scheduled(scheduled);
        block.set_deadline(deadline);

        // Flatten org_properties directly into block properties (not wrapped)
        if let Some(ref props_json) = org_properties {
            if let Ok(props_map) = serde_json::from_str::<
                std::collections::HashMap<String, serde_json::Value>,
            >(props_json)
            {
                for (key, value) in props_map {
                    let api_value = match value {
                        serde_json::Value::String(s) => holon_api::Value::String(s),
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                holon_api::Value::Integer(i)
                            } else if let Some(f) = n.as_f64() {
                                holon_api::Value::Float(f)
                            } else {
                                holon_api::Value::String(n.to_string())
                            }
                        }
                        serde_json::Value::Bool(b) => holon_api::Value::Boolean(b),
                        _ => holon_api::Value::String(value.to_string()),
                    };
                    block.set_property(&key, api_value);
                }
            }
        }
        // Keep org_properties for round-trip serialization back to .org files
        block.set_org_properties(org_properties);

        output.push(block);

        // Create child Block entities for each source block
        for (src_index, mut source_block) in source_blocks.into_iter().enumerate() {
            // Extract :id from header args if present (preserves ID across round-trips)
            // Otherwise fall back to stable ID based on parent + index
            let src_id = source_block
                .header_args
                .remove("id")
                .and_then(|v| v.as_string().map(|s| s.to_string()))
                .unwrap_or_else(|| format!("{}::src::{}", id, src_index));

            let src_sequence = *sequence_counter;
            *sequence_counter += 1;

            let mut src_block = Block {
                id: src_id,
                parent_id: id.clone(),
                content: source_block.source,
                content_type: CONTENT_TYPE_SOURCE.to_string(),
                source_language: source_block.language,
                source_name: source_block.name,
                properties: HashMap::new(),
                created_at: now,
                updated_at: now,
            };
            src_block.set_sequence(src_sequence);

            // Store header args in properties if present
            if !source_block.header_args.is_empty() {
                src_block.set_source_header_args(source_block.header_args);
            }

            output.push(src_block);
        }

        // Recursively process children
        process_headlines(
            headline.headlines(),
            &id,
            sequence_counter,
            output,
            needs_id,
        )?;
    }

    Ok(())
}

/// Extract :ID: property from headline, or generate a new UUID
/// Returns (id, needs_write_back)
fn extract_or_generate_id(headline: &Headline) -> (String, bool) {
    if let Some(drawer) = headline.properties() {
        if let Some(id_token) = drawer.get("ID") {
            let value = id_token.to_string().trim().to_string();
            if !value.is_empty() {
                return (value, false);
            }
        }
    }
    (Uuid::new_v4().to_string(), true)
}

/// Extract SCHEDULED and DEADLINE timestamps from headline
fn extract_planning(headline: &Headline) -> (Option<String>, Option<String>) {
    let mut scheduled = None;
    let mut deadline = None;

    if let Some(planning) = headline.planning() {
        if let Some(s) = planning.scheduled() {
            scheduled = Some(s.syntax().to_string());
        }
        if let Some(d) = planning.deadline() {
            deadline = Some(d.syntax().to_string());
        }
    }

    (scheduled, deadline)
}

/// Extract properties from property drawer as JSON
fn extract_properties(headline: &Headline) -> Option<String> {
    let drawer = headline.properties()?;
    let mut props = serde_json::Map::new();

    for (key_token, value_token) in drawer.iter() {
        let key = key_token.to_string().trim().to_string();
        let value = value_token.to_string().trim().to_string();
        // Skip ID property (handled separately)
        if !key.eq_ignore_ascii_case("ID") {
            props.insert(key, serde_json::Value::String(value));
        }
    }

    if props.is_empty() {
        None
    } else {
        serde_json::to_string(&props).ok()
    }
}

/// Extract source blocks from a headline's section.
/// Returns (plain_text_content, source_blocks)
fn extract_section_content(headline: &Headline) -> (Option<String>, Vec<SourceBlock>) {
    let section = match headline.section() {
        Some(s) => s,
        None => return (None, Vec::new()),
    };

    let section_syntax = section.syntax();
    let section_text = section_syntax.to_string();
    let mut source_blocks = Vec::new();
    let mut text_parts = Vec::new();

    let mut pending_name: Option<String> = None;

    for child in section_syntax.children() {
        if child.kind() == SyntaxKind::KEYWORD {
            let keyword_text = child.text().to_string();
            let trimmed = keyword_text.trim();
            if trimmed.starts_with("#+NAME:") || trimmed.starts_with("#+name:") {
                if let Some((_, name)) = trimmed.split_once(':') {
                    pending_name = Some(name.trim().to_string());
                }
                continue;
            }
        }

        if child.kind() == SyntaxKind::SOURCE_BLOCK {
            if let Some(src_block) = OrgizeSourceBlock::cast(child.clone()) {
                let language = src_block
                    .language()
                    .map(|t| t.to_string().trim().to_string());
                let source = src_block.value();
                let parameters = src_block.parameters().map(|t| t.to_string());

                let mut source_block =
                    SourceBlock::new(language.clone().unwrap_or_default(), source);

                // Check for #+NAME: in the block text (orgize includes it in SOURCE_BLOCK)
                let block_text = child.text().to_string();
                if let Some(name) = extract_name_from_block_text(&block_text) {
                    source_block.name = Some(name);
                } else if let Some(name) = pending_name.take() {
                    source_block.name = Some(name);
                }

                if let Some(params) = parameters {
                    let header_args_str = parse_header_args_from_str(&params);
                    for (k, v) in header_args_str {
                        source_block
                            .header_args
                            .insert(k, holon_api::Value::String(v));
                    }
                }

                source_blocks.push(source_block);
                pending_name = None;
            }
        } else if !child.text().to_string().trim().is_empty() {
            pending_name = None;
        }
    }

    for child in section_syntax.children() {
        match child.kind() {
            SyntaxKind::SOURCE_BLOCK | SyntaxKind::KEYWORD => {
                continue;
            }
            _ => {
                let child_text = child.text().to_string();
                let trimmed = child_text.trim();
                if !trimmed.is_empty() {
                    text_parts.push(trimmed.to_string());
                }
            }
        }
    }

    if source_blocks.is_empty() {
        let full_text = section_text.trim().to_string();
        return (
            if full_text.is_empty() {
                None
            } else {
                Some(full_text)
            },
            Vec::new(),
        );
    }

    let plain_text = if text_parts.is_empty() {
        None
    } else {
        Some(text_parts.join("\n\n"))
    };

    (plain_text, source_blocks)
}

/// Extract #+NAME: from block text (orgize includes it in SOURCE_BLOCK node)
fn extract_name_from_block_text(text: &str) -> Option<String> {
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("#+NAME:") || trimmed.starts_with("#+name:") {
            if let Some((_, name)) = trimmed.split_once(':') {
                return Some(name.trim().to_string());
            }
        }
        // Stop looking once we hit BEGIN_SRC
        if trimmed.starts_with("#+BEGIN_SRC") || trimmed.starts_with("#+begin_src") {
            break;
        }
    }
    None
}

/// Look for #+NAME: directive for a source block in the section
fn find_block_name_in_section(
    _headline: &Headline,
    _source_blocks: &[SourceBlock],
) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use holon_filesystem::directory::ROOT_ID;
    use std::path::PathBuf;

    #[test]
    fn test_parse_simple_headlines() {
        let content = "* First headline\n** Nested headline\n* Second headline";
        let path = PathBuf::from("/test/file.org");
        let root = PathBuf::from("/test");

        let result = parse_org_file(&path, content, ROOT_ID, 0, &root).unwrap();

        assert_eq!(result.blocks.len(), 3);
        assert_eq!(result.blocks[0].org_title(), "First headline");
        assert_eq!(result.blocks[1].org_title(), "Nested headline");
        assert_eq!(result.blocks[1].parent_id, result.blocks[0].id);
        assert_eq!(result.blocks[2].org_title(), "Second headline");
    }

    #[test]
    fn test_parse_todo_and_priority() {
        let content = "* TODO [#A] Important task :work:urgent:";
        let path = PathBuf::from("/test/file.org");
        let root = PathBuf::from("/test");

        let result = parse_org_file(&path, content, ROOT_ID, 0, &root).unwrap();

        assert_eq!(result.blocks.len(), 1);
        let h = &result.blocks[0];
        assert_eq!(h.task_state(), Some("TODO".to_string()));
        assert_eq!(h.priority(), Some(3)); // A = 3
        assert_eq!(h.tags(), Some("work,urgent".to_string()));
    }

    #[test]
    fn test_parse_title_and_todo_keywords() {
        let content = "#+TITLE: My Document\n#+TODO: TODO INPROGRESS | DONE CANCELLED\n* Task";
        let path = PathBuf::from("/test/file.org");
        let root = PathBuf::from("/test");

        let result = parse_org_file(&path, content, ROOT_ID, 0, &root).unwrap();

        assert_eq!(result.document.org_title(), Some("My Document".to_string()));
        assert_eq!(
            result.document.todo_keywords(),
            Some("TODO,INPROGRESS|DONE,CANCELLED".to_string())
        );
    }

    #[test]
    fn test_generate_ids() {
        let root = Path::new("/path/to");
        let path1 = Path::new("/path/to/file1.org");
        let path2 = Path::new("/path/to/file2.org");

        let id1 = generate_file_id(path1, root);
        let id2 = generate_file_id(path2, root);

        assert_ne!(id1, id2);
        assert!(id1.starts_with(DOCUMENT_URI_SCHEME));
        // Should be relative paths
        assert_eq!(id1, "holon-doc://file1.org");
        assert_eq!(id2, "holon-doc://file2.org");

        let id1_again = generate_file_id(path1, root);
        assert_eq!(id1, id1_again);
    }

    #[test]
    fn test_parse_existing_id_property() {
        let content = "* Headline\n:PROPERTIES:\n:ID: existing-uuid-here\n:END:";
        let path = PathBuf::from("/test/file.org");
        let root = PathBuf::from("/test");

        let result = parse_org_file(&path, content, ROOT_ID, 0, &root).unwrap();

        assert_eq!(result.blocks.len(), 1);
        assert_eq!(result.blocks[0].id, "existing-uuid-here");
        assert!(result.headlines_needing_ids.is_empty());
    }

    #[test]
    fn test_headlines_without_id_need_writeback() {
        let content = "* Headline without ID";
        let path = PathBuf::from("/test/file.org");
        let root = PathBuf::from("/test");

        let result = parse_org_file(&path, content, ROOT_ID, 0, &root).unwrap();

        assert_eq!(result.blocks.len(), 1);
        assert!(!result.headlines_needing_ids.is_empty());
    }

    #[test]
    fn test_parse_source_block_basic() {
        let content = r#"* Headline with code
#+BEGIN_SRC python
def hello():
    print("Hello, world!")
#+END_SRC
"#;
        let path = PathBuf::from("/test/file.org");
        let root = PathBuf::from("/test");

        let result = parse_org_file(&path, content, ROOT_ID, 0, &root).unwrap();

        // Should have 2 blocks: headline + source block
        assert_eq!(result.blocks.len(), 2);

        let headline = &result.blocks[0];
        assert_eq!(headline.content_type, CONTENT_TYPE_TEXT);

        // Source block is a separate child block
        let source_block = &result.blocks[1];
        assert_eq!(source_block.content_type, CONTENT_TYPE_SOURCE);
        assert_eq!(source_block.parent_id, headline.id);
        assert_eq!(source_block.source_language, Some("python".to_string()));
        assert!(source_block.content.contains("def hello():"));
        assert!(source_block.content.contains("print(\"Hello, world!\")"));
    }

    #[test]
    fn test_parse_source_block_with_header_args() {
        let content = r#"* Headline with PRQL
#+BEGIN_SRC prql :connection main :results table
from tasks
filter completed == false
#+END_SRC
"#;
        let path = PathBuf::from("/test/file.org");
        let root = PathBuf::from("/test");

        let result = parse_org_file(&path, content, ROOT_ID, 0, &root).unwrap();

        // Should have 2 blocks: headline + source block
        assert_eq!(result.blocks.len(), 2);

        let source_block = &result.blocks[1];
        assert_eq!(source_block.content_type, CONTENT_TYPE_SOURCE);
        assert_eq!(source_block.source_language, Some("prql".to_string()));
        assert!(source_block.is_prql_block());

        // Parse header args from JSON
        let header_args = source_block.get_source_header_args();
        assert_eq!(
            header_args.get("connection"),
            Some(&holon_api::Value::String("main".to_string()))
        );
        assert_eq!(
            header_args.get("results"),
            Some(&holon_api::Value::String("table".to_string()))
        );
    }

    #[test]
    fn test_parse_multiple_source_blocks() {
        let content = r#"* Multiple blocks
Some intro text.

#+BEGIN_SRC sql
SELECT * FROM users;
#+END_SRC

Middle text.

#+BEGIN_SRC prql
from users | take 10
#+END_SRC

Outro text.
"#;
        let path = PathBuf::from("/test/file.org");
        let root = PathBuf::from("/test");

        let result = parse_org_file(&path, content, ROOT_ID, 0, &root).unwrap();

        // Should have 3 blocks: headline + 2 source blocks
        assert_eq!(result.blocks.len(), 3);

        let headline = &result.blocks[0];
        assert_eq!(headline.content_type, CONTENT_TYPE_TEXT);

        // First source block (sql)
        let sql_block = &result.blocks[1];
        assert_eq!(sql_block.content_type, CONTENT_TYPE_SOURCE);
        assert_eq!(sql_block.source_language, Some("sql".to_string()));
        assert_eq!(sql_block.parent_id, headline.id);

        // Second source block (prql)
        let prql_block = &result.blocks[2];
        assert_eq!(prql_block.content_type, CONTENT_TYPE_SOURCE);
        assert_eq!(prql_block.source_language, Some("prql".to_string()));
        assert_eq!(prql_block.parent_id, headline.id);

        // Text content should be preserved in headline
        assert!(headline.body().is_some());
    }

    #[test]
    fn test_parse_named_source_block() {
        let content = r#"* Named block
#+NAME: my-query
#+BEGIN_SRC prql
from tasks
#+END_SRC
"#;
        let path = PathBuf::from("/test/file.org");
        let root = PathBuf::from("/test");

        let result = parse_org_file(&path, content, ROOT_ID, 0, &root).unwrap();

        // Should have 2 blocks: headline + source block
        assert_eq!(result.blocks.len(), 2);

        let source_block = &result.blocks[1];
        assert_eq!(source_block.content_type, CONTENT_TYPE_SOURCE);
        assert_eq!(source_block.source_name, Some("my-query".to_string()));
    }

    #[test]
    fn test_parse_header_args() {
        let params = ":var x=1 :results table :tangle yes";
        let args = parse_header_args_from_str(params);

        assert_eq!(args.get("var"), Some(&"x=1".to_string()));
        assert_eq!(args.get("results"), Some(&"table".to_string()));
        assert_eq!(args.get("tangle"), Some(&"yes".to_string()));
    }

    #[test]
    fn test_parse_header_args_flags_only() {
        let params = ":noweb :tangle";
        let args = parse_header_args_from_str(params);

        assert_eq!(args.get("noweb"), Some(&"".to_string()));
        assert_eq!(args.get("tangle"), Some(&"".to_string()));
    }

    #[test]
    fn test_prql_blocks_filter() {
        let content = r#"* Mixed blocks
#+BEGIN_SRC sql
SELECT 1;
#+END_SRC

#+BEGIN_SRC prql
from users
#+END_SRC

#+BEGIN_SRC python
print("hello")
#+END_SRC
"#;
        let path = PathBuf::from("/test/file.org");
        let root = PathBuf::from("/test");

        let result = parse_org_file(&path, content, ROOT_ID, 0, &root).unwrap();

        // Should have 4 blocks: headline + 3 source blocks
        assert_eq!(result.blocks.len(), 4);

        // Filter to find PRQL blocks
        let prql_blocks: Vec<_> = result.blocks.iter().filter(|b| b.is_prql_block()).collect();

        assert_eq!(prql_blocks.len(), 1);
        assert!(prql_blocks[0].content.contains("from users"));
    }

    #[test]
    fn test_parse_real_index_org() {
        let content = r#"* Today's Tasks
:PROPERTIES:
:ID: 39471ed2-64b6-4b98-9782-30c6caf8f061
:VIEW: query
:END:

#+BEGIN_SRC prql
from blocks
select {id, parent_id, content, content_type}
#+END_SRC
"#;
        let path = PathBuf::from("/test/index.org");
        let root = PathBuf::from("/test");
        let result = parse_org_file(&path, content, ROOT_ID, 0, &root).unwrap();

        // Should have 2 blocks: headline + source block
        assert_eq!(result.blocks.len(), 2, "Expected 2 blocks");

        let headline = &result.blocks[0];
        assert_eq!(headline.content_type, CONTENT_TYPE_TEXT);
        assert!(headline.org_title().contains("Today's Tasks"));

        let source = &result.blocks[1];
        assert_eq!(source.content_type, CONTENT_TYPE_SOURCE);
        assert_eq!(source.source_language, Some("prql".to_string()));
        assert!(source.content.contains("from blocks"));
        assert_eq!(source.parent_id, headline.id);

        println!("\n=== Parse Test Results ===");
        println!("Headline ID: {}", headline.id);
        println!("Headline content_type: {}", headline.content_type);
        println!("Source block ID: {}", source.id);
        println!("Source block content_type: {}", source.content_type);
        println!("Source block parent_id: {}", source.parent_id);
        println!("Source block language: {:?}", source.source_language);
        println!("Source block content:\n{}", source.content);
    }
}
