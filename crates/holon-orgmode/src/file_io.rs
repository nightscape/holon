//! File I/O utilities for Org Mode files
//!
//! This module provides utilities for reading and writing Org Mode files,
//! including file reconstruction, source block formatting, and ID property writing.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Context;
use holon::sync::Document;
use holon_api::block::Block;
use holon_api::{BlockResult, ResultOutput, SourceBlock, Value};

use crate::models::{OrgBlockExt, ToOrg};

/// Write :ID: property to headlines that need it.
///
/// This function modifies the org file to add :ID: properties to headlines
/// that don't have them, ensuring all blocks have persistent IDs.
pub fn write_id_properties(
    path: &Path,
    ids_to_write: &[String],
    file: &Document,
    blocks: &[Block],
) -> anyhow::Result<()> {
    if ids_to_write.is_empty() {
        return Ok(());
    }

    let ids_to_add: std::collections::HashSet<String> = ids_to_write.iter().cloned().collect();

    let mut updated_blocks: Vec<Block> = blocks.iter().cloned().collect();
    for block in &mut updated_blocks {
        if ids_to_add.contains(&block.id) {
            // Add ID to org properties
            let mut props: serde_json::Map<String, serde_json::Value> = block
                .org_properties()
                .as_ref()
                .and_then(|p| serde_json::from_str(p).ok())
                .unwrap_or_default();
            props.insert(
                "ID".to_string(),
                serde_json::Value::String(block.id.clone()),
            );
            block.set_org_properties(Some(serde_json::to_string(&props).unwrap()));
        }
    }

    let reconstructed = reconstruct_file(file, &updated_blocks)?;

    std::fs::write(path, reconstructed)
        .with_context(|| format!("Failed to write file: {}", path.display()))?;

    Ok(())
}

/// Reconstruct an org file from Document and Block entities.
///
/// This renders the document header and all blocks in tree order,
/// producing a complete org file as a string.
pub fn reconstruct_file(file: &Document, blocks: &[Block]) -> anyhow::Result<String> {
    let mut sorted_blocks = blocks.to_vec();
    sorted_blocks.sort_by_key(|h| h.sequence());

    let mut children_map: HashMap<String, Vec<Block>> = HashMap::new();
    for block in sorted_blocks {
        children_map
            .entry(block.parent_id.clone())
            .or_default()
            .push(block);
    }

    for children in children_map.values_mut() {
        children.sort_by_key(|h| h.sequence());
    }

    let mut result = String::new();

    // Add document header (title, TODO keywords)
    result.push_str(&file.to_org());

    fn render_block_tree(
        parent_id: &str,
        children_map: &HashMap<String, Vec<Block>>,
        result: &mut String,
    ) {
        if let Some(children) = children_map.get(parent_id) {
            for block in children {
                // Block::to_org() guarantees trailing newline for non-empty output
                result.push_str(&block.to_org());
                render_block_tree(&block.id, children_map, result);
            }
        }
    }

    render_block_tree(&file.id, &children_map, &mut result);

    Ok(result)
}

/// Format header arguments as Org Mode inline parameters.
///
/// Example: `{ "connection": "main", "results": "table" }` -> `:connection main :results table`
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

/// Format a SourceBlock as Org Mode text.
pub fn format_org_source_block(block: &SourceBlock) -> String {
    block.to_org()
}

/// Format a holon_api::SourceBlock as Org Mode text.
pub fn format_api_source_block(block: &SourceBlock) -> String {
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

    // Ensure trailing newline
    if !result.ends_with('\n') {
        result.push('\n');
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
    block: &SourceBlock,
) -> anyhow::Result<String> {
    assert!(insert_pos <= content.len(), "insert_pos out of bounds");

    let formatted = block.to_org();
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
pub fn update_source_block(
    content: &str,
    byte_start: usize,
    byte_end: usize,
    new_block: &SourceBlock,
) -> anyhow::Result<String> {
    assert!(byte_start <= byte_end, "byte_start must be <= byte_end");
    assert!(byte_end <= content.len(), "byte_end out of bounds");

    let formatted = new_block.to_org();

    let before = &content[..byte_start];
    let name_prefix = find_and_strip_name_before_block(before);
    let actual_start = byte_start - name_prefix.len();

    let mut result = String::with_capacity(content.len() + formatted.len());
    result.push_str(&content[..actual_start]);
    result.push_str(&formatted);
    result.push_str(&content[byte_end..]);

    Ok(result)
}

/// Delete a source block at the specified byte range.
pub fn delete_source_block(
    content: &str,
    byte_start: usize,
    byte_end: usize,
) -> anyhow::Result<String> {
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

/// Find and strip #+NAME: prefix before a source block.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_header_args() {
        let mut args = HashMap::new();
        args.insert("connection".to_string(), "main".to_string());
        args.insert("results".to_string(), "table".to_string());

        let result = format_header_args(&args);
        assert!(result.contains(":connection main"));
        assert!(result.contains(":results table"));
    }

    #[test]
    fn test_format_header_args_empty() {
        let args = HashMap::new();
        assert_eq!(format_header_args(&args), "");
    }

    #[test]
    fn test_value_to_header_arg_string() {
        assert_eq!(
            value_to_header_arg_string(&Value::String("hello".to_string())),
            "hello"
        );
        assert_eq!(value_to_header_arg_string(&Value::Integer(42)), "42");
        assert_eq!(value_to_header_arg_string(&Value::Boolean(true)), "yes");
        assert_eq!(value_to_header_arg_string(&Value::Boolean(false)), "no");
    }
}
