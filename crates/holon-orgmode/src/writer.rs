//! Write-back support for org-mode files
//!
//! Handles modifications to .org files including:
//! - Adding :ID: properties to headlines
//! - Updating headline content
//! - Creating and deleting headlines
//! - Writing and updating source blocks (#+BEGIN_SRC ... #+END_SRC)

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::models::OrgSourceBlock;
use holon_api::{BlockResult, ResultOutput, SourceBlock, Value};

/// Write :ID: property to headlines that need it.
/// Takes a list of (headline_id, byte_start) pairs and inserts :ID: properties.
pub fn write_id_properties(path: &Path, ids_to_write: &[(String, i64)]) -> Result<()> {
    if ids_to_write.is_empty() {
        return Ok(());
    }

    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read file: {}", path.display()))?;

    let mut result = content.clone();
    // Process in reverse order so byte offsets remain valid
    let mut sorted_ids: Vec<_> = ids_to_write.to_vec();
    sorted_ids.sort_by(|a, b| b.1.cmp(&a.1)); // Reverse sort by byte_start

    for (id, byte_start) in sorted_ids {
        result = insert_id_property(&result, byte_start as usize, &id)?;
    }

    fs::write(path, result).with_context(|| format!("Failed to write file: {}", path.display()))?;

    Ok(())
}

/// Insert an :ID: property into a headline at the given byte offset.
/// If a property drawer exists, adds to it. Otherwise creates one.
fn insert_id_property(content: &str, headline_start: usize, id: &str) -> Result<String> {
    // Find the end of the headline line
    let headline_line_end = content[headline_start..]
        .find('\n')
        .map(|i| headline_start + i)
        .unwrap_or(content.len());

    // Check if there's already a property drawer after the headline
    let after_headline = &content[headline_line_end..];
    let trimmed = after_headline.trim_start_matches('\n');

    if trimmed.starts_with(":PROPERTIES:") {
        // Property drawer exists - insert ID as first property
        let properties_start = headline_line_end + (after_headline.len() - trimmed.len());
        let properties_line_end = content[properties_start..]
            .find('\n')
            .map(|i| properties_start + i + 1)
            .unwrap_or(content.len());

        let mut result = String::with_capacity(content.len() + 50);
        result.push_str(&content[..properties_line_end]);
        result.push_str(&format!(":ID: {}\n", id));
        result.push_str(&content[properties_line_end..]);
        Ok(result)
    } else {
        // No property drawer - create one
        let mut result = String::with_capacity(content.len() + 100);
        result.push_str(&content[..headline_line_end]);
        result.push('\n');
        result.push_str(":PROPERTIES:\n");
        result.push_str(&format!(":ID: {}\n", id));
        result.push_str(":END:");
        result.push_str(&content[headline_line_end..]);
        Ok(result)
    }
}

/// Update a headline's TODO keyword
pub fn update_todo_keyword(
    content: &str,
    headline_start: usize,
    new_keyword: Option<&str>,
) -> Result<String> {
    let headline_line_end = content[headline_start..]
        .find('\n')
        .map(|i| headline_start + i)
        .unwrap_or(content.len());

    let headline_line = &content[headline_start..headline_line_end];

    // Parse the headline to find the stars and any existing keyword
    let (stars, rest) = parse_headline_parts(headline_line);

    // Reconstruct the headline with new keyword
    let new_headline = if let Some(kw) = new_keyword {
        format!("{} {} {}", stars, kw, rest.trim())
    } else {
        format!("{} {}", stars, rest.trim())
    };

    let mut result = String::with_capacity(content.len());
    result.push_str(&content[..headline_start]);
    result.push_str(&new_headline);
    result.push_str(&content[headline_line_end..]);
    Ok(result)
}

/// Parse headline into (stars, rest after any TODO keyword)
fn parse_headline_parts(line: &str) -> (&str, &str) {
    // Find the stars
    let stars_end = line.find(|c: char| c != '*').unwrap_or(line.len());
    let stars = &line[..stars_end];
    let after_stars = line[stars_end..].trim_start();

    // Check for TODO keyword (uppercase word at start)
    let todo_keywords = [
        "TODO",
        "DONE",
        "CANCELLED",
        "WAITING",
        "HOLD",
        "NEXT",
        "INPROGRESS",
        "CLOSED",
    ];

    for kw in todo_keywords {
        if after_stars.starts_with(kw) {
            let after_kw = &after_stars[kw.len()..];
            if after_kw.is_empty() || after_kw.starts_with(' ') || after_kw.starts_with('\t') {
                return (stars, after_kw.trim_start());
            }
        }
    }

    (stars, after_stars)
}

/// Update a headline's priority
pub fn update_priority(
    content: &str,
    headline_start: usize,
    new_priority: Option<char>,
) -> Result<String> {
    let headline_line_end = content[headline_start..]
        .find('\n')
        .map(|i| headline_start + i)
        .unwrap_or(content.len());

    let headline_line = &content[headline_start..headline_line_end];
    let (stars, rest) = parse_headline_parts(headline_line);

    // Check for existing priority like [#A]
    let (todo, after_todo) = extract_todo_keyword(rest);
    let (_, title) = extract_priority(after_todo);

    // Reconstruct headline
    let mut new_headline = stars.to_string();
    if let Some(kw) = todo {
        new_headline.push(' ');
        new_headline.push_str(kw);
    }
    if let Some(p) = new_priority {
        new_headline.push_str(" [#");
        new_headline.push(p);
        new_headline.push(']');
    }
    new_headline.push(' ');
    new_headline.push_str(title.trim());

    let mut result = String::with_capacity(content.len());
    result.push_str(&content[..headline_start]);
    result.push_str(&new_headline);
    result.push_str(&content[headline_line_end..]);
    Ok(result)
}

fn extract_todo_keyword(s: &str) -> (Option<&str>, &str) {
    let todo_keywords = [
        "TODO",
        "DONE",
        "CANCELLED",
        "WAITING",
        "HOLD",
        "NEXT",
        "INPROGRESS",
        "CLOSED",
    ];

    let trimmed = s.trim_start();
    for kw in todo_keywords {
        if trimmed.starts_with(kw) {
            let after = &trimmed[kw.len()..];
            if after.is_empty() || after.starts_with(' ') || after.starts_with('\t') {
                return (Some(kw), after.trim_start());
            }
        }
    }
    (None, trimmed)
}

fn extract_priority(s: &str) -> (Option<char>, &str) {
    let trimmed = s.trim_start();
    if trimmed.starts_with("[#") && trimmed.len() >= 4 && trimmed.chars().nth(3) == Some(']') {
        let priority = trimmed.chars().nth(2);
        (priority, &trimmed[4..])
    } else {
        (None, trimmed)
    }
}

/// Update a headline's section content (body text after property drawer and planning)
///
/// Takes a transformation function that receives the current body content and returns the new content.
/// This allows both replacement (`|_| new_text`) and modification (`|old| old + " appended"`).
pub fn update_content<F>(
    content: &str,
    byte_start: usize,
    byte_end: usize,
    transform: F,
) -> Result<String>
where
    F: FnOnce(&str) -> String,
{
    let headline_line_end = content[byte_start..]
        .find('\n')
        .map(|i| byte_start + i + 1)
        .unwrap_or(content.len());

    // Find where the section body starts (after property drawer and planning)
    let section_start = find_section_body_start(content, headline_line_end);

    // The section ends at byte_end (which is the start of next headline or EOF)
    let section_end = byte_end;

    // Get current body content
    let current_body = &content[section_start..section_end];

    // Transform the content
    let new_body = transform(current_body);

    // Build the new content
    let mut result = String::with_capacity(content.len() + new_body.len());
    result.push_str(&content[..section_start]);
    if !new_body.is_empty() {
        result.push_str(new_body.trim());
        result.push('\n');
    }
    result.push_str(&content[section_end..]);

    Ok(result)
}

/// Find where the actual section body starts (after property drawer and planning lines)
fn find_section_body_start(content: &str, after_headline: usize) -> usize {
    let remaining = &content[after_headline..];
    let mut pos = after_headline;

    // Skip property drawer if present
    let trimmed = remaining.trim_start_matches('\n');
    let newlines_skipped = remaining.len() - trimmed.len();
    pos += newlines_skipped;

    if trimmed.starts_with(":PROPERTIES:") {
        // Find :END:
        if let Some(end_pos) = trimmed.find(":END:") {
            let end_line_end = trimmed[end_pos..]
                .find('\n')
                .map(|i| end_pos + i + 1)
                .unwrap_or(trimmed.len());
            pos += end_line_end;
        }
    }

    // Skip planning lines (SCHEDULED, DEADLINE, CLOSED)
    let remaining_after_props = &content[pos..];
    for line in remaining_after_props.lines() {
        let trimmed_line = line.trim();
        if trimmed_line.starts_with("SCHEDULED:")
            || trimmed_line.starts_with("DEADLINE:")
            || trimmed_line.starts_with("CLOSED:")
        {
            pos += line.len() + 1; // +1 for newline
        } else {
            break;
        }
    }

    pos
}

// =============================================================================
// Source Block Writing
// =============================================================================

/// Format header arguments as Org Mode inline parameters.
/// Input: `{ "connection": "main", "results": "table" }`
/// Output: `:connection main :results table`
pub fn format_header_args(args: &HashMap<String, String>) -> String {
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

/// Convert a holon_api::Value to a string suitable for Org Mode header arguments.
pub fn value_to_header_arg_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Boolean(b) => if *b { "yes" } else { "no" }.to_string(),
        Value::Null => String::new(),
        Value::DateTime(dt) => dt.clone(),
        Value::Reference(r) => r.clone(),
        Value::Array(arr) => arr
            .iter()
            .map(value_to_header_arg_string)
            .collect::<Vec<_>>()
            .join(" "),
        Value::Object(_) | Value::Json(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

/// Format header arguments from holon_api::Value HashMap.
pub fn format_header_args_from_values(args: &HashMap<String, Value>) -> String {
    if args.is_empty() {
        return String::new();
    }

    let string_args: HashMap<String, String> = args
        .iter()
        .map(|(k, v)| (k.clone(), value_to_header_arg_string(v)))
        .collect();

    format_header_args(&string_args)
}

/// Format an OrgSourceBlock as Org Mode text.
///
/// Output format:
/// ```org
/// #+NAME: block-name
/// #+BEGIN_SRC language :arg1 val1 :arg2 val2
/// source code here
/// #+END_SRC
/// ```
pub fn format_org_source_block(block: &OrgSourceBlock) -> String {
    let mut result = String::new();

    if let Some(ref name) = block.name {
        result.push_str("#+NAME: ");
        result.push_str(name);
        result.push('\n');
    }

    result.push_str("#+BEGIN_SRC");

    if let Some(ref lang) = block.language {
        result.push(' ');
        result.push_str(lang);
    }

    let header_args = format_header_args(&block.header_args);
    if !header_args.is_empty() {
        result.push(' ');
        result.push_str(&header_args);
    }

    result.push('\n');
    result.push_str(&block.source);

    if !block.source.ends_with('\n') {
        result.push('\n');
    }

    result.push_str("#+END_SRC");

    result
}

/// Format a holon_api::SourceBlock as Org Mode text.
///
/// This includes the optional #+NAME: directive and #+RESULTS: block.
pub fn format_api_source_block(block: &SourceBlock) -> String {
    let mut result = String::new();

    if let Some(ref name) = block.name {
        result.push_str("#+NAME: ");
        result.push_str(name);
        result.push('\n');
    }

    result.push_str("#+BEGIN_SRC");

    if !block.language.is_empty() {
        result.push(' ');
        result.push_str(&block.language);
    }

    let header_args = format_header_args_from_values(&block.header_args);
    if !header_args.is_empty() {
        result.push(' ');
        result.push_str(&header_args);
    }

    result.push('\n');
    result.push_str(&block.source);

    if !block.source.ends_with('\n') {
        result.push('\n');
    }

    result.push_str("#+END_SRC");

    if let Some(ref results) = block.results {
        result.push('\n');
        result.push_str(&format_block_result(results, block.name.as_deref()));
    }

    result
}

/// Format a BlockResult as an Org Mode #+RESULTS: block.
pub fn format_block_result(result: &BlockResult, name: Option<&str>) -> String {
    let mut output = String::from("#+RESULTS:");

    if let Some(n) = name {
        output.push(' ');
        output.push_str(n);
    }

    output.push('\n');

    match &result.output {
        ResultOutput::Text { content } => {
            for line in content.lines() {
                output.push_str(": ");
                output.push_str(line);
                output.push('\n');
            }
        }
        ResultOutput::Table { headers, rows } => {
            output.push('|');
            for header in headers {
                output.push(' ');
                output.push_str(header);
                output.push_str(" |");
            }
            output.push('\n');

            output.push('|');
            for _ in headers {
                output.push_str("---+");
            }
            output.pop();
            output.push('|');
            output.push('\n');

            for row in rows {
                output.push('|');
                for cell in row {
                    output.push(' ');
                    output.push_str(&value_to_header_arg_string(cell));
                    output.push_str(" |");
                }
                output.push('\n');
            }
        }
        ResultOutput::Error { message } => {
            output.push_str("#+begin_error\n");
            output.push_str(message);
            if !message.ends_with('\n') {
                output.push('\n');
            }
            output.push_str("#+end_error\n");
        }
    }

    output.trim_end().to_string()
}

/// Insert a source block at the specified position in the content.
pub fn insert_source_block(
    content: &str,
    insert_pos: usize,
    block: &OrgSourceBlock,
) -> Result<String> {
    assert!(insert_pos <= content.len(), "insert_pos out of bounds");

    let formatted = format_org_source_block(block);
    let mut result = String::with_capacity(content.len() + formatted.len() + 2);

    result.push_str(&content[..insert_pos]);

    if insert_pos > 0 && !content[..insert_pos].ends_with('\n') {
        result.push('\n');
    }

    result.push_str(&formatted);

    if insert_pos < content.len() && !content[insert_pos..].starts_with('\n') {
        result.push('\n');
    }

    result.push_str(&content[insert_pos..]);

    Ok(result)
}

/// Update a source block at the specified byte range.
/// The new block replaces everything from byte_start to byte_end.
pub fn update_source_block(
    content: &str,
    byte_start: usize,
    byte_end: usize,
    new_block: &OrgSourceBlock,
) -> Result<String> {
    assert!(byte_start <= byte_end, "byte_start must be <= byte_end");
    assert!(byte_end <= content.len(), "byte_end out of bounds");

    let formatted = format_org_source_block(new_block);

    let before = &content[..byte_start];
    let name_prefix = find_and_strip_name_before_block(before);
    let actual_start = byte_start - name_prefix.len();

    let mut result = String::with_capacity(content.len() + formatted.len());
    result.push_str(&content[..actual_start]);
    result.push_str(&formatted);
    result.push_str(&content[byte_end..]);

    Ok(result)
}

/// Find #+NAME: directive immediately before a source block and return it.
fn find_and_strip_name_before_block(before: &str) -> &str {
    let trimmed = before.trim_end_matches('\n');
    if let Some(last_newline) = trimmed.rfind('\n') {
        let last_line = &trimmed[last_newline + 1..];
        let stripped = last_line.trim();
        if stripped.starts_with("#+NAME:") || stripped.starts_with("#+name:") {
            return &before[last_newline + 1..];
        }
    } else {
        let stripped = trimmed.trim();
        if stripped.starts_with("#+NAME:") || stripped.starts_with("#+name:") {
            return before;
        }
    }
    ""
}

/// Delete a source block at the specified byte range.
/// Also removes any #+NAME: directive immediately before it.
pub fn delete_source_block(content: &str, byte_start: usize, byte_end: usize) -> Result<String> {
    assert!(byte_start <= byte_end, "byte_start must be <= byte_end");
    assert!(byte_end <= content.len(), "byte_end out of bounds");

    let before = &content[..byte_start];
    let name_prefix = find_and_strip_name_before_block(before);
    let actual_start = byte_start - name_prefix.len();

    let mut result = String::with_capacity(content.len());
    result.push_str(&content[..actual_start]);

    let after = &content[byte_end..];
    let after_trimmed = after.trim_start_matches('\n');
    result.push_str(after_trimmed);

    Ok(result)
}

/// Insert a source block (from holon_api) at the specified position.
pub fn insert_api_source_block(
    content: &str,
    insert_pos: usize,
    block: &SourceBlock,
) -> Result<String> {
    assert!(insert_pos <= content.len(), "insert_pos out of bounds");

    let formatted = format_api_source_block(block);
    let mut result = String::with_capacity(content.len() + formatted.len() + 2);

    result.push_str(&content[..insert_pos]);

    if insert_pos > 0 && !content[..insert_pos].ends_with('\n') {
        result.push('\n');
    }

    result.push_str(&formatted);

    if insert_pos < content.len() && !content[insert_pos..].starts_with('\n') {
        result.push('\n');
    }

    result.push_str(&content[insert_pos..]);

    Ok(result)
}

/// Update a source block with a holon_api::SourceBlock.
pub fn update_api_source_block(
    content: &str,
    byte_start: usize,
    byte_end: usize,
    new_block: &SourceBlock,
) -> Result<String> {
    assert!(byte_start <= byte_end, "byte_start must be <= byte_end");
    assert!(byte_end <= content.len(), "byte_end out of bounds");

    let formatted = format_api_source_block(new_block);

    let before = &content[..byte_start];
    let name_prefix = find_and_strip_name_before_block(before);
    let actual_start = byte_start - name_prefix.len();

    let mut result = String::with_capacity(content.len() + formatted.len());
    result.push_str(&content[..actual_start]);
    result.push_str(&formatted);
    result.push_str(&content[byte_end..]);

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_id_no_property_drawer() {
        let content = "* Headline\nSome content";
        let result = insert_id_property(content, 0, "test-uuid").unwrap();
        assert!(result.contains(":PROPERTIES:"));
        assert!(result.contains(":ID: test-uuid"));
        assert!(result.contains(":END:"));
    }

    #[test]
    fn test_insert_id_existing_property_drawer() {
        let content = "* Headline\n:PROPERTIES:\n:CUSTOM: value\n:END:\nContent";
        let result = insert_id_property(content, 0, "test-uuid").unwrap();
        assert!(result.contains(":ID: test-uuid"));
        // ID should come after :PROPERTIES: line
        let id_pos = result.find(":ID:").unwrap();
        let props_pos = result.find(":PROPERTIES:").unwrap();
        assert!(id_pos > props_pos);
    }

    #[test]
    fn test_parse_headline_parts() {
        let (stars, rest) = parse_headline_parts("** TODO Important task");
        assert_eq!(stars, "**");
        assert_eq!(rest, "Important task");

        let (stars, rest) = parse_headline_parts("* Just a headline");
        assert_eq!(stars, "*");
        assert_eq!(rest, "Just a headline");
    }

    #[test]
    fn test_update_todo_keyword() {
        let content = "* TODO Task\nContent";
        let result = update_todo_keyword(content, 0, Some("DONE")).unwrap();
        assert_eq!(result, "* DONE Task\nContent");

        let result = update_todo_keyword(content, 0, None).unwrap();
        assert_eq!(result, "* Task\nContent");
    }

    #[test]
    fn test_update_priority() {
        let content = "* TODO [#A] Task\nContent";
        let result = update_priority(content, 0, Some('B')).unwrap();
        assert!(result.contains("[#B]"));
        assert!(!result.contains("[#A]"));
    }

    #[test]
    fn test_extract_priority() {
        let (p, rest) = extract_priority("[#A] Title");
        assert_eq!(p, Some('A'));
        assert_eq!(rest, " Title");

        let (p, rest) = extract_priority("No priority");
        assert_eq!(p, None);
        assert_eq!(rest, "No priority");
    }

    #[test]
    fn test_update_content_simple() {
        let content = "* Headline\nOld body text\n* Next headline";
        let result = update_content(content, 0, 25, |_| "New body".to_string()).unwrap();
        assert!(result.contains("New body"));
        assert!(!result.contains("Old body"));
        assert!(result.contains("* Next headline"));
    }

    #[test]
    fn test_update_content_with_property_drawer() {
        let content = "* Headline\n:PROPERTIES:\n:ID: abc\n:END:\nOld body\n* Next";
        let byte_end = content.find("* Next").unwrap();
        let result = update_content(content, 0, byte_end, |_| "New body".to_string()).unwrap();
        assert!(result.contains(":PROPERTIES:"));
        assert!(result.contains(":ID: abc"));
        assert!(result.contains("New body"));
        assert!(!result.contains("Old body"));
    }

    #[test]
    fn test_update_content_transform() {
        let content = "* Headline\nOriginal text\n";
        let result = update_content(content, 0, content.len(), |old| {
            format!("{} - appended", old.trim())
        })
        .unwrap();
        assert!(result.contains("Original text - appended"));
    }

    #[test]
    fn test_format_header_args_empty() {
        let args: HashMap<String, String> = HashMap::new();
        assert_eq!(format_header_args(&args), "");
    }

    #[test]
    fn test_format_header_args_single() {
        let mut args = HashMap::new();
        args.insert("connection".to_string(), "main".to_string());
        assert_eq!(format_header_args(&args), ":connection main");
    }

    #[test]
    fn test_format_header_args_multiple() {
        let mut args = HashMap::new();
        args.insert("connection".to_string(), "main".to_string());
        args.insert("results".to_string(), "table".to_string());
        let result = format_header_args(&args);
        assert!(result.contains(":connection main"));
        assert!(result.contains(":results table"));
    }

    #[test]
    fn test_format_header_args_empty_value() {
        let mut args = HashMap::new();
        args.insert("noweb".to_string(), "".to_string());
        assert_eq!(format_header_args(&args), ":noweb");
    }

    #[test]
    fn test_format_org_source_block_minimal() {
        let block = OrgSourceBlock::new(
            Some("python".to_string()),
            "print('hello')".to_string(),
            0,
            0,
        );
        let result = format_org_source_block(&block);
        assert_eq!(result, "#+BEGIN_SRC python\nprint('hello')\n#+END_SRC");
    }

    #[test]
    fn test_format_org_source_block_with_name() {
        let mut block =
            OrgSourceBlock::new(Some("prql".to_string()), "from tasks".to_string(), 0, 0);
        block.name = Some("my-query".to_string());
        let result = format_org_source_block(&block);
        assert!(result.starts_with("#+NAME: my-query\n"));
        assert!(result.contains("#+BEGIN_SRC prql"));
    }

    #[test]
    fn test_format_org_source_block_with_header_args() {
        let mut block =
            OrgSourceBlock::new(Some("prql".to_string()), "from tasks".to_string(), 0, 0);
        block
            .header_args
            .insert("connection".to_string(), "main".to_string());
        let result = format_org_source_block(&block);
        assert!(result.contains("#+BEGIN_SRC prql :connection main"));
    }

    #[test]
    fn test_format_org_source_block_no_language() {
        let block = OrgSourceBlock::new(None, "some text".to_string(), 0, 0);
        let result = format_org_source_block(&block);
        assert!(result.starts_with("#+BEGIN_SRC\n"));
    }

    #[test]
    fn test_format_api_source_block() {
        let block = SourceBlock::new("rust", "fn main() {}")
            .with_name("example")
            .with_header_arg("results", Value::String("output".to_string()));
        let result = format_api_source_block(&block);
        assert!(result.contains("#+NAME: example"));
        assert!(result.contains("#+BEGIN_SRC rust"));
        assert!(result.contains(":results output"));
    }

    #[test]
    fn test_format_api_source_block_with_results() {
        let block = SourceBlock::new("prql", "from tasks")
            .with_name("query")
            .with_results(BlockResult::text("output line 1\noutput line 2"));
        let result = format_api_source_block(&block);
        assert!(result.contains("#+RESULTS: query"));
        assert!(result.contains(": output line 1"));
        assert!(result.contains(": output line 2"));
    }

    #[test]
    fn test_format_block_result_text() {
        let result = BlockResult::text("Hello\nWorld");
        let output = format_block_result(&result, Some("my-block"));
        assert_eq!(output, "#+RESULTS: my-block\n: Hello\n: World");
    }

    #[test]
    fn test_format_block_result_table() {
        let result = BlockResult::table(
            vec!["id".to_string(), "name".to_string()],
            vec![
                vec![Value::Integer(1), Value::String("Alice".to_string())],
                vec![Value::Integer(2), Value::String("Bob".to_string())],
            ],
        );
        let output = format_block_result(&result, None);
        assert!(output.contains("| id | name |"));
        assert!(output.contains("|---+---|"));
        assert!(output.contains("| 1 | Alice |"));
        assert!(output.contains("| 2 | Bob |"));
    }

    #[test]
    fn test_format_block_result_error() {
        let result = BlockResult::error("Something went wrong");
        let output = format_block_result(&result, None);
        assert!(output.contains("#+begin_error"));
        assert!(output.contains("Something went wrong"));
        assert!(output.contains("#+end_error"));
    }

    #[test]
    fn test_insert_source_block() {
        let content = "* Headline\nSome text\n";
        let block =
            OrgSourceBlock::new(Some("python".to_string()), "print('hi')".to_string(), 0, 0);
        let result = insert_source_block(content, content.len(), &block).unwrap();
        assert!(result.contains("Some text"));
        assert!(result.contains("#+BEGIN_SRC python"));
        assert!(result.contains("print('hi')"));
    }

    #[test]
    fn test_update_source_block() {
        let content = "* Headline\n#+BEGIN_SRC python\nold code\n#+END_SRC\n";
        let block_start = content.find("#+BEGIN_SRC").unwrap();
        let block_end = content.find("#+END_SRC").unwrap() + "#+END_SRC".len();

        let new_block = OrgSourceBlock::new(Some("rust".to_string()), "new code".to_string(), 0, 0);
        let result = update_source_block(content, block_start, block_end, &new_block).unwrap();

        assert!(result.contains("#+BEGIN_SRC rust"));
        assert!(result.contains("new code"));
        assert!(!result.contains("python"));
        assert!(!result.contains("old code"));
    }

    #[test]
    fn test_update_source_block_with_name() {
        let content = "* Headline\n#+NAME: old-name\n#+BEGIN_SRC python\nold code\n#+END_SRC\n";
        let block_start = content.find("#+BEGIN_SRC").unwrap();
        let block_end = content.find("#+END_SRC").unwrap() + "#+END_SRC".len();

        let mut new_block =
            OrgSourceBlock::new(Some("rust".to_string()), "new code".to_string(), 0, 0);
        new_block.name = Some("new-name".to_string());
        let result = update_source_block(content, block_start, block_end, &new_block).unwrap();

        assert!(result.contains("#+NAME: new-name"));
        assert!(!result.contains("old-name"));
    }

    #[test]
    fn test_delete_source_block() {
        let content = "* Headline\nBefore\n#+BEGIN_SRC python\ncode\n#+END_SRC\nAfter\n";
        let block_start = content.find("#+BEGIN_SRC").unwrap();
        let block_end = content.find("#+END_SRC").unwrap() + "#+END_SRC\n".len();

        let result = delete_source_block(content, block_start, block_end).unwrap();

        assert!(result.contains("Before"));
        assert!(result.contains("After"));
        assert!(!result.contains("#+BEGIN_SRC"));
        assert!(!result.contains("code"));
    }

    #[test]
    fn test_delete_source_block_with_name() {
        let content = "* Headline\n#+NAME: my-block\n#+BEGIN_SRC python\ncode\n#+END_SRC\n";
        let block_start = content.find("#+BEGIN_SRC").unwrap();
        let block_end = content.find("#+END_SRC").unwrap() + "#+END_SRC\n".len();

        let result = delete_source_block(content, block_start, block_end).unwrap();

        assert!(!result.contains("#+NAME:"));
        assert!(!result.contains("#+BEGIN_SRC"));
    }

    #[test]
    fn test_value_to_header_arg_string() {
        assert_eq!(
            value_to_header_arg_string(&Value::String("test".into())),
            "test"
        );
        assert_eq!(value_to_header_arg_string(&Value::Integer(42)), "42");
        assert_eq!(value_to_header_arg_string(&Value::Float(3.14)), "3.14");
        assert_eq!(value_to_header_arg_string(&Value::Boolean(true)), "yes");
        assert_eq!(value_to_header_arg_string(&Value::Boolean(false)), "no");
        assert_eq!(value_to_header_arg_string(&Value::Null), "");
    }
}
