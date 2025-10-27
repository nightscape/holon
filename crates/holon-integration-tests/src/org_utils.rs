//! Org file serialization utilities

use holon_api::Value;
use holon_api::block::{Block, CONTENT_TYPE_SOURCE};
use holon_orgmode::models::OrgBlockExt;

/// Internal properties that Loro/Org adds but reference model doesn't track
pub const INTERNAL_PROPS: &[&str] = &["sequence", "level", "ID", "id"];

/// Extract the first :ID: property value from org content.
///
/// This is useful for waiting on a specific block to sync after writing an org file.
pub fn extract_first_block_id(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with(":ID:") {
            return Some(trimmed.strip_prefix(":ID:")?.trim().to_string());
        }
    }
    None
}

/// Serialize Blocks to Org file format
pub fn serialize_blocks_to_org(blocks: &[&Block], doc_uri: &str) -> String {
    let mut root_blocks: Vec<&&Block> = blocks.iter().filter(|b| b.parent_id == doc_uri).collect();
    root_blocks.sort_by(|a, b| {
        let a_is_source = (a.content_type == CONTENT_TYPE_SOURCE) as u8;
        let b_is_source = (b.content_type == CONTENT_TYPE_SOURCE) as u8;
        b_is_source.cmp(&a_is_source).then_with(|| a.id.cmp(&b.id))
    });

    let mut result = String::new();

    for block in root_blocks {
        serialize_block_recursive(block, blocks, &mut result, 1);
    }

    result
}

/// Recursively serialize a block and its children
pub fn serialize_block_recursive(
    block: &Block,
    all_blocks: &[&Block],
    result: &mut String,
    level: usize,
) {
    if block.content_type == CONTENT_TYPE_SOURCE {
        let language = block.source_language.as_deref().unwrap_or("text");
        result.push_str(&format!("#+BEGIN_SRC {} :id {}\n", language, block.id));
        result.push_str(&block.content);
        if !block.content.ends_with('\n') {
            result.push('\n');
        }
        result.push_str("#+END_SRC\n");
        return;
    }

    let mut headline = String::new();
    headline.push_str(&"*".repeat(level));
    headline.push(' ');

    if let Some(ref task_state) = block.task_state() {
        headline.push_str(task_state);
        headline.push(' ');
    }

    if let Some(priority) = block.priority() {
        let priority_char = match priority {
            3 => "A",
            2 => "B",
            1 => "C",
            _ => "",
        };
        if !priority_char.is_empty() {
            headline.push_str(&format!("[#{}] ", priority_char));
        }
    }

    headline.push_str(&block.content);

    if let Some(ref tags) = block.tags() {
        let tag_list: Vec<&str> = tags
            .split(',')
            .map(|t| t.trim())
            .filter(|t| !t.is_empty())
            .collect();
        if !tag_list.is_empty() {
            headline.push_str(&format!(" :{}:", tag_list.join(":")));
        }
    }

    result.push_str(&headline);
    result.push('\n');

    if block.scheduled().is_some() || block.deadline().is_some() {
        if let Some(ref scheduled) = block.scheduled() {
            result.push_str(&format!("SCHEDULED: {}\n", scheduled));
        }
        if let Some(ref deadline) = block.deadline() {
            result.push_str(&format!("DEADLINE: {}\n", deadline));
        }
    }

    result.push_str(":PROPERTIES:\n");
    result.push_str(&format!(":ID: {}\n", block.id));

    for (k, v) in &block.properties {
        if k != "ID" && k != "id" && !INTERNAL_PROPS.contains(&k.as_str()) {
            if matches!(
                k.as_str(),
                "task_state" | "priority" | "tags" | "scheduled" | "deadline"
            ) {
                continue;
            }
            let value_str = match v {
                Value::String(s) => s.clone(),
                Value::Integer(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::Boolean(b) => b.to_string(),
                Value::DateTime(s) => s.clone(),
                Value::Json(s) => s.clone(),
                Value::Array(_) => "[array]".to_string(),
                Value::Object(_) => "[object]".to_string(),
                Value::Null => "".to_string(),
            };
            result.push_str(&format!(":{}: {}\n", k, value_str));
        }
    }
    result.push_str(":END:\n");

    let mut children: Vec<&&Block> = all_blocks
        .iter()
        .filter(|b| b.parent_id == block.id)
        .collect();
    // Source blocks must come before text children so that the org parser assigns
    // them to the correct parent heading (not to a sub-heading that follows).
    children.sort_by(|a, b| {
        let a_is_source = (a.content_type == CONTENT_TYPE_SOURCE) as u8;
        let b_is_source = (b.content_type == CONTENT_TYPE_SOURCE) as u8;
        b_is_source.cmp(&a_is_source).then_with(|| a.id.cmp(&b.id))
    });

    for child in children {
        serialize_block_recursive(child, all_blocks, result, level + 1);
    }
}
