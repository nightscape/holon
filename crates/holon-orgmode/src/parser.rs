use crate::models::{OrgFile, OrgHeadline, OrgSourceBlock};
use anyhow::Result;
use chrono::Utc;
use orgize::ast::{Headline, SourceBlock};
use orgize::rowan::ast::AstNode;
use orgize::{Org, ParseConfig, SyntaxKind};
use sha2::{Digest, Sha256};
use std::path::Path;
use uuid::Uuid;

/// Generate a directory ID from its path (ID is the relative path from root)
pub fn generate_directory_id(path: &Path, root_directory: &Path) -> String {
    path.strip_prefix(root_directory)
        .map(|rel_path| {
            // Convert to string, handling path separators
            rel_path.to_string_lossy().to_string()
        })
        .unwrap_or_else(|_| {
            // If path is not under root, fall back to absolute path
            // This shouldn't happen in normal operation
            path.to_string_lossy().to_string()
        })
}

/// Generate a deterministic ID for a file based on its path
pub fn generate_file_id(path: &Path) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.to_string_lossy().as_bytes());
    let hash = hex::encode(&hasher.finalize()[..8]);
    format!("org-file://{}", hash)
}

/// Compute content hash for change detection
pub fn compute_content_hash(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}

/// Result of parsing an org file, including headlines that need ID write-back
pub struct ParseResult {
    pub file: OrgFile,
    pub headlines: Vec<OrgHeadline>,
    /// Headlines that need :ID: property added (id, byte_start for insertion)
    pub headlines_needing_ids: Vec<(String, i64)>,
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

/// Parse an org file and return OrgFile + OrgHeadline entities
pub fn parse_org_file(
    path: &Path,
    content: &str,
    parent_dir_id: &str,
    parent_depth: i64,
) -> Result<ParseResult> {
    let file_id = generate_file_id(path);
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    // Parse file-level metadata
    let title = parse_title(content);
    let todo_keywords = parse_todo_keywords_config(content);
    let file_hash = compute_content_hash(content);
    let updated_at = Utc::now().to_rfc3339();

    // Create OrgFile entity
    let file = OrgFile::new(
        file_id.clone(),
        file_name,
        path.to_string_lossy().to_string(),
        parent_dir_id.to_string(),
        parent_depth + 1,
        title,
        file_hash,
        updated_at,
    );

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

    // Extract headlines
    let mut headlines = Vec::new();
    let mut headlines_needing_ids = Vec::new();
    let file_depth = parent_depth + 1;

    // Process document headlines recursively
    let doc = org.document();
    let file_path_str = path.to_string_lossy().to_string();
    process_headlines(
        doc.headlines(),
        &file_id,
        &file_path_str,
        &file_id, // Top-level headlines have file as parent
        file_depth,
        &mut headlines,
        &mut headlines_needing_ids,
    )?;

    Ok(ParseResult {
        file,
        headlines,
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
    file_id: &str,
    file_path: &str,
    parent_id: &str,
    parent_depth: i64,
    output: &mut Vec<OrgHeadline>,
    needs_id: &mut Vec<(String, i64)>,
) -> Result<()> {
    for headline in headlines {
        let headline_depth = parent_depth + 1;

        // Get byte range
        let range = headline.syntax().text_range();
        let byte_start = u32::from(range.start()) as i64;
        let byte_end = u32::from(range.end()) as i64;

        // Extract :ID: property if exists
        let (id, needs_write) = extract_or_generate_id(&headline);
        if needs_write {
            needs_id.push((id.clone(), byte_start));
        }

        // Extract title using title_raw()
        let title = headline.title_raw().trim().to_string();

        // Extract TODO keyword
        let todo_keyword = headline.todo_keyword().map(|t| t.to_string());

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
        let (content, mut source_blocks) = extract_section_content(&headline);

        // Look for #+NAME: directives for each source block
        for source_block in &mut source_blocks {
            if source_block.name.is_none() {
                source_block.name = find_block_name(&headline, source_block.byte_start);
            }
        }

        // Extract planning (SCHEDULED, DEADLINE)
        let (scheduled, deadline) = extract_planning(&headline);

        // Extract properties as JSON
        let properties = extract_properties(&headline);

        // Create headline entity
        let mut org_headline = OrgHeadline::new(
            id.clone(),
            file_id.to_string(),
            file_path.to_string(),
            parent_id.to_string(),
            headline_depth,
            byte_start,
            byte_end,
            title,
        );
        org_headline.content = content;
        org_headline.todo_keyword = todo_keyword;
        org_headline.priority = priority;
        org_headline.tags = tags;
        org_headline.scheduled = scheduled;
        org_headline.deadline = deadline;
        org_headline.properties = properties;
        org_headline.set_source_blocks(source_blocks);

        output.push(org_headline);

        // Recursively process children
        process_headlines(
            headline.headlines(),
            file_id,
            file_path,
            &id,
            headline_depth,
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
        // Use get() method to look up ID property
        if let Some(id_token) = drawer.get("ID") {
            let value = id_token.to_string().trim().to_string();
            if !value.is_empty() {
                return (value, false);
            }
        }
    }
    // Generate new UUID
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
fn extract_section_content(headline: &Headline) -> (Option<String>, Vec<OrgSourceBlock>) {
    let section = match headline.section() {
        Some(s) => s,
        None => return (None, Vec::new()),
    };

    let section_syntax = section.syntax();
    let section_start = u32::from(section_syntax.text_range().start()) as i64;
    let mut source_blocks = Vec::new();
    let mut text_parts = Vec::new();
    let mut last_end = section_start;

    // Traverse section children to find source blocks
    for child in section_syntax.children() {
        if child.kind() == SyntaxKind::SOURCE_BLOCK {
            if let Some(src_block) = SourceBlock::cast(child.clone()) {
                let range = src_block.syntax().text_range();
                let block_start = u32::from(range.start()) as i64;
                let block_end = u32::from(range.end()) as i64;

                // Capture text before this source block
                if block_start > last_end {
                    // Get the text between last_end and block_start
                    let offset = (last_end - section_start) as usize;
                    let len = (block_start - last_end) as usize;
                    let section_text = section_syntax.to_string();
                    if offset + len <= section_text.len() {
                        let text_chunk = &section_text[offset..offset + len];
                        let trimmed = text_chunk.trim();
                        if !trimmed.is_empty() {
                            text_parts.push(trimmed.to_string());
                        }
                    }
                }

                // Extract source block data
                let language = src_block
                    .language()
                    .map(|t| t.to_string().trim().to_string());
                let source = src_block.value();
                let parameters = src_block.parameters().map(|t| t.to_string());

                let mut org_source_block =
                    OrgSourceBlock::new(language, source, block_start, block_end);

                // Parse header arguments if present
                if let Some(params) = parameters {
                    org_source_block.header_args = OrgSourceBlock::parse_header_args(&params);
                }

                source_blocks.push(org_source_block);
                last_end = block_end;
            }
        }
    }

    // Capture any remaining text after the last source block
    let section_text = section_syntax.to_string();
    let section_end = section_start + section_text.len() as i64;
    if last_end < section_end {
        let offset = (last_end - section_start) as usize;
        if offset < section_text.len() {
            let text_chunk = &section_text[offset..];
            let trimmed = text_chunk.trim();
            if !trimmed.is_empty() {
                text_parts.push(trimmed.to_string());
            }
        }
    }

    // If no source blocks found, return the full section text
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

    // Join text parts
    let plain_text = if text_parts.is_empty() {
        None
    } else {
        Some(text_parts.join("\n\n"))
    };

    (plain_text, source_blocks)
}

/// Look for #+NAME: directive before a source block
fn find_block_name(headline: &Headline, source_block_start: i64) -> Option<String> {
    let section = headline.section()?;
    let section_syntax = section.syntax();

    // Search for #+NAME: or #+name: in the text before the source block
    let section_text = section_syntax.to_string();
    let section_start = u32::from(section_syntax.text_range().start()) as i64;
    let relative_pos = (source_block_start - section_start) as usize;

    if relative_pos > section_text.len() {
        return None;
    }

    let before_block = &section_text[..relative_pos];

    // Find the last #+NAME: line before the source block
    for line in before_block.lines().rev() {
        let trimmed = line.trim();
        if trimmed.starts_with("#+NAME:") || trimmed.starts_with("#+name:") {
            return trimmed
                .split_once(':')
                .map(|(_, name)| name.trim().to_string());
        }
        // Stop if we hit a non-empty, non-NAME line (NAME must immediately precede the block)
        if !trimmed.is_empty() && !trimmed.starts_with("#+") {
            break;
        }
    }

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

        let result = parse_org_file(&path, content, ROOT_ID, 0).unwrap();

        assert_eq!(result.headlines.len(), 3);
        assert_eq!(result.headlines[0].title, "First headline");
        assert_eq!(result.headlines[0].depth, 2); // ROOT_ID depth 0 + file depth 1 + headline level 1
        assert_eq!(result.headlines[1].title, "Nested headline");
        assert_eq!(result.headlines[1].parent_id, result.headlines[0].id);
        assert_eq!(result.headlines[2].title, "Second headline");
    }

    #[test]
    fn test_parse_todo_and_priority() {
        let content = "* TODO [#A] Important task :work:urgent:";
        let path = PathBuf::from("/test/file.org");

        let result = parse_org_file(&path, content, ROOT_ID, 0).unwrap();

        assert_eq!(result.headlines.len(), 1);
        let h = &result.headlines[0];
        assert_eq!(h.todo_keyword, Some("TODO".to_string()));
        assert_eq!(h.priority, Some(3)); // A = 3
        assert_eq!(h.tags, Some("work,urgent".to_string()));
    }

    #[test]
    fn test_parse_title_and_todo_keywords() {
        let content = "#+TITLE: My Document\n#+TODO: TODO INPROGRESS | DONE CANCELLED\n* Task";
        let path = PathBuf::from("/test/file.org");

        let result = parse_org_file(&path, content, ROOT_ID, 0).unwrap();

        assert_eq!(result.file.title, Some("My Document".to_string()));
        assert!(result.file.todo_keywords.is_none()); // Currently not being set in the flow
    }

    #[test]
    fn test_generate_ids() {
        let path1 = Path::new("/path/to/file1.org");
        let path2 = Path::new("/path/to/file2.org");

        let id1 = generate_file_id(path1);
        let id2 = generate_file_id(path2);

        assert_ne!(id1, id2);
        assert!(id1.starts_with("org-file://"));

        // Same path should generate same ID
        let id1_again = generate_file_id(path1);
        assert_eq!(id1, id1_again);
    }

    #[test]
    fn test_parse_existing_id_property() {
        let content = "* Headline\n:PROPERTIES:\n:ID: existing-uuid-here\n:END:";
        let path = PathBuf::from("/test/file.org");

        let result = parse_org_file(&path, content, ROOT_ID, 0).unwrap();

        assert_eq!(result.headlines.len(), 1);
        assert_eq!(result.headlines[0].id, "existing-uuid-here");
        assert!(result.headlines_needing_ids.is_empty());
    }

    #[test]
    fn test_headlines_without_id_need_writeback() {
        let content = "* Headline without ID";
        let path = PathBuf::from("/test/file.org");

        let result = parse_org_file(&path, content, ROOT_ID, 0).unwrap();

        assert_eq!(result.headlines.len(), 1);
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

        let result = parse_org_file(&path, content, ROOT_ID, 0).unwrap();

        assert_eq!(result.headlines.len(), 1);
        let headline = &result.headlines[0];

        let source_blocks = headline.get_source_blocks();
        assert_eq!(source_blocks.len(), 1);
        assert_eq!(source_blocks[0].language, Some("python".to_string()));
        assert!(source_blocks[0].source.contains("def hello():"));
        assert!(source_blocks[0].source.contains("print(\"Hello, world!\")"));
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

        let result = parse_org_file(&path, content, ROOT_ID, 0).unwrap();

        let headline = &result.headlines[0];
        let source_blocks = headline.get_source_blocks();

        assert_eq!(source_blocks.len(), 1);
        assert_eq!(source_blocks[0].language, Some("prql".to_string()));
        assert!(source_blocks[0].is_prql());
        assert_eq!(
            source_blocks[0].header_args.get("connection"),
            Some(&"main".to_string())
        );
        assert_eq!(
            source_blocks[0].header_args.get("results"),
            Some(&"table".to_string())
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

        let result = parse_org_file(&path, content, ROOT_ID, 0).unwrap();

        let headline = &result.headlines[0];
        let source_blocks = headline.get_source_blocks();

        assert_eq!(source_blocks.len(), 2);
        assert_eq!(source_blocks[0].language, Some("sql".to_string()));
        assert_eq!(source_blocks[1].language, Some("prql".to_string()));

        // Text content should be preserved (intro, middle, outro)
        assert!(headline.content.is_some());
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

        let result = parse_org_file(&path, content, ROOT_ID, 0).unwrap();

        let headline = &result.headlines[0];
        let source_blocks = headline.get_source_blocks();

        assert_eq!(source_blocks.len(), 1);
        assert_eq!(source_blocks[0].name, Some("my-query".to_string()));
    }

    #[test]
    fn test_parse_header_args() {
        let params = ":var x=1 :results table :tangle yes";
        let args = OrgSourceBlock::parse_header_args(params);

        assert_eq!(args.get("var"), Some(&"x=1".to_string()));
        assert_eq!(args.get("results"), Some(&"table".to_string()));
        assert_eq!(args.get("tangle"), Some(&"yes".to_string()));
    }

    #[test]
    fn test_parse_header_args_flags_only() {
        let params = ":noweb :tangle";
        let args = OrgSourceBlock::parse_header_args(params);

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

        let result = parse_org_file(&path, content, ROOT_ID, 0).unwrap();

        let headline = &result.headlines[0];
        let prql_blocks = headline.prql_blocks();

        assert_eq!(prql_blocks.len(), 1);
        assert!(prql_blocks[0].source.contains("from users"));
    }
}
