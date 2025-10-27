//! Loro → Org-mode rendering
//!
//! Converts Loro document blocks to org-mode format using Block with OrgBlockExt.

use crate::models::{OrgBlockExt, ToOrg};
use holon::sync::document_entity::is_document_uri;
use holon_api::block::Block;
use holon_api::Value;
use std::collections::HashMap;
use std::path::Path;

/// Render a Loro document (represented as blocks) to org-mode format.
///
/// Takes a list of blocks in tree order and converts them to org-mode text.
pub struct OrgRenderer;

impl OrgRenderer {
    /// Render blocks to org-mode format.
    ///
    /// # Arguments
    /// * `blocks` - Blocks in tree order (parent before children)
    /// * `file_path` - Path to the org file (for OrgBlock metadata)
    /// * `file_id` - ID of the org file
    ///
    /// # Returns
    /// Org-mode formatted string
    pub fn render_blocks(blocks: &[Block], file_path: &Path, file_id: &str) -> String {
        let mut result = String::new();

        // Build a map of block ID to block for quick lookup
        let block_map: HashMap<&str, &Block> = blocks.iter().map(|b| (b.id.as_str(), b)).collect();

        // Find content root blocks - blocks that should be rendered as top-level content.
        // These are blocks whose parent is a document URI.
        let root_blocks: Vec<&Block> = blocks
            .iter()
            .filter(|b| is_document_uri(&b.parent_id))
            .collect();

        // Render each root block and its children recursively
        for root_block in root_blocks {
            Self::render_block_tree(root_block, &block_map, file_path, file_id, &mut result, 0);
        }

        result
    }

    /// Render a block and its children recursively.
    fn render_block_tree(
        block: &Block,
        block_map: &HashMap<&str, &Block>,
        _file_path: &Path,
        _file_id: &str,
        result: &mut String,
        depth: usize,
    ) {
        // Prepare block for org rendering - transfer Loro properties to org_props format
        let mut prepared_block = block.clone();
        Self::prepare_block_for_org(&mut prepared_block, depth);

        // Render using Block::to_org() which guarantees trailing newline
        result.push_str(&prepared_block.to_org());

        // Render children (find blocks where parent_id matches this block's id)
        // Source blocks must come BEFORE text children (sub-headings) so that
        // when the org file is re-parsed, the source block is assigned to this
        // parent heading, not to the first sub-heading that follows it.
        let mut child_blocks: Vec<_> = block_map
            .values()
            .filter(|b| b.parent_id == block.id)
            .collect();
        child_blocks.sort_by_key(|b| if b.content_type == "source" { 0 } else { 1 });
        for child_block in child_blocks {
            Self::render_block_tree(
                child_block,
                block_map,
                _file_path,
                _file_id,
                result,
                depth + 1,
            );
        }
    }

    /// Prepare a block for org rendering by transferring Loro properties to org_props format.
    fn prepare_block_for_org(block: &mut Block, depth: usize) {
        use crate::models::org_props;

        let properties = block.properties_map();

        // Set level from depth (level = depth + 1)
        block.set_level((depth + 1) as i64);

        // Transfer TODO to task_state if not already set
        if block.task_state().is_none() {
            if let Some(todo) = properties.get("TODO").and_then(|v| v.as_string()) {
                block.set_task_state(Some(todo.to_string()));
            }
        }

        // Transfer PRIORITY to priority if not already set
        if block.priority().is_none() {
            if let Some(priority_val) = properties.get("PRIORITY") {
                let priority = match priority_val {
                    Value::String(s) => match s.as_str() {
                        "A" => Some(3),
                        "B" => Some(2),
                        "C" => Some(1),
                        _ => None,
                    },
                    Value::Integer(n) => Some(*n as i32),
                    Value::Float(f) => Some(*f as i32),
                    _ => None,
                };
                if let Some(p) = priority {
                    block.set_priority(Some(p));
                }
            }
        }

        // Transfer TAGS to tags if not already set
        if block.tags().is_none() {
            if let Some(tags) = properties.get("TAGS").and_then(|v| v.as_string()) {
                block.set_tags(Some(tags.to_string()));
            }
        }

        // Transfer SCHEDULED if not already set
        if block.scheduled().is_none() {
            if let Some(sched) = properties.get("SCHEDULED").and_then(|v| v.as_string()) {
                block.set_scheduled(Some(sched.to_string()));
            }
        }

        // Transfer DEADLINE if not already set
        if block.deadline().is_none() {
            if let Some(dead) = properties.get("DEADLINE").and_then(|v| v.as_string()) {
                block.set_deadline(Some(dead.to_string()));
            }
        }

        // Build org properties drawer with block ID
        // Use ID from properties if present, otherwise use block.id
        let id = properties
            .get("ID")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string())
            .unwrap_or_else(|| block.id.clone());

        let mut org_props_map: HashMap<String, String> = HashMap::new();
        org_props_map.insert("ID".to_string(), id);

        // Include other org-drawer properties (exclude our internal ones)
        let internal_keys = [
            "TODO",
            "PRIORITY",
            "TAGS",
            "SCHEDULED",
            "DEADLINE",
            org_props::TASK_STATE,
            org_props::PRIORITY,
            org_props::TAGS,
            org_props::SCHEDULED,
            org_props::DEADLINE,
            org_props::LEVEL,
            org_props::SEQUENCE,
            org_props::ORG_PROPERTIES,
            "ID",
        ];
        for (k, v) in &properties {
            if !internal_keys.contains(&k.as_str()) {
                if let Some(s) = v.as_string() {
                    org_props_map.insert(k.clone(), s.to_string());
                }
            }
        }

        if let Ok(json) = serde_json::to_string(&org_props_map) {
            block.set_org_properties(Some(json));
        }

        // Note: Source blocks are now separate Block entities (content_type = "source"),
        // not embedded in headline properties. No conversion needed here.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_simple_block() {
        let mut block = Block::new_text(
            "local://test-uuid",
            "holon-doc:///test/file.org",
            "Test Title\nBody content here",
        );
        block.set_property("ID", Value::String("local://test-uuid".to_string()));

        let file_path = Path::new("/test/file.org");
        let file_id = "holon-doc:///test/file.org";
        let org_text = OrgRenderer::render_blocks(&[block], file_path, file_id);

        assert!(org_text.contains("* Test Title"));
        assert!(org_text.contains("Body content here"));
        assert!(org_text.contains(":ID: local://test-uuid"));
    }

    #[test]
    fn test_render_block_with_todo_and_priority() {
        let mut block = Block::new_text("test-id", "holon-doc:///test/file.org", "Task headline");
        block.set_property("ID", Value::String("test-id".to_string()));
        block.set_property("TODO", Value::String("TODO".to_string()));
        block.set_property("PRIORITY", Value::String("A".to_string()));

        let file_path = Path::new("/test/file.org");
        let file_id = "holon-doc:///test/file.org";
        let org_text = OrgRenderer::render_blocks(&[block], file_path, file_id);

        assert!(org_text.contains("* TODO [#A] Task headline"));
    }

    #[test]
    fn test_source_blocks_render_before_child_headlines() {
        // Parent headline with both a source block child and a text child (sub-heading).
        // Source block MUST appear before the sub-heading in the output,
        // otherwise the org parser would assign it to the sub-heading instead.
        let file_id = "holon-doc:///test/file.org";

        let mut parent = Block::new_text("parent-id", file_id, "Parent Heading");
        parent.set_property("ID", Value::String("parent-id".to_string()));

        let mut child_heading = Block::new_text("child-heading-id", "parent-id", "Child Heading");
        child_heading.set_property("ID", Value::String("child-heading-id".to_string()));

        let mut source_block = Block {
            id: "src-id".to_string(),
            parent_id: "parent-id".to_string(),
            content: "from tasks\n".to_string(),
            content_type: "source".to_string(),
            source_language: Some("prql".to_string()),
            source_name: None,
            properties: HashMap::new(),
            created_at: 0,
            updated_at: 0,
        };
        source_block.set_sequence(1);

        let file_path = Path::new("/test/file.org");
        let blocks = vec![parent, child_heading, source_block];
        let org_text = OrgRenderer::render_blocks(&blocks, file_path, file_id);

        let src_pos = org_text
            .find("#+BEGIN_SRC")
            .expect("source block must be present");
        let child_pos = org_text
            .find("** Child Heading")
            .expect("child heading must be present");

        assert!(
            src_pos < child_pos,
            "Source block must render BEFORE child heading.\nOutput:\n{}",
            org_text
        );
    }

    #[test]
    fn test_multiple_source_blocks_all_before_children() {
        let file_id = "holon-doc:///test/file.org";

        let mut parent = Block::new_text("parent-id", file_id, "Parent");
        parent.set_property("ID", Value::String("parent-id".to_string()));

        let mut src1 = Block {
            id: "src1".to_string(),
            parent_id: "parent-id".to_string(),
            content: "SELECT 1;\n".to_string(),
            content_type: "source".to_string(),
            source_language: Some("sql".to_string()),
            source_name: None,
            properties: HashMap::new(),
            created_at: 0,
            updated_at: 0,
        };
        src1.set_sequence(1);

        let mut src2 = Block {
            id: "src2".to_string(),
            parent_id: "parent-id".to_string(),
            content: "from users\n".to_string(),
            content_type: "source".to_string(),
            source_language: Some("prql".to_string()),
            source_name: None,
            properties: HashMap::new(),
            created_at: 0,
            updated_at: 0,
        };
        src2.set_sequence(2);

        let mut child = Block::new_text("child-id", "parent-id", "Child");
        child.set_property("ID", Value::String("child-id".to_string()));

        let file_path = Path::new("/test/file.org");
        let blocks = vec![parent, child, src1, src2];
        let org_text = OrgRenderer::render_blocks(&blocks, file_path, file_id);

        let src1_pos = org_text.find("#+BEGIN_SRC sql").expect("sql block");
        let src2_pos = org_text.find("#+BEGIN_SRC prql").expect("prql block");
        let child_pos = org_text.find("** Child").expect("child heading");

        assert!(
            src1_pos < child_pos && src2_pos < child_pos,
            "All source blocks must render before child heading.\nOutput:\n{}",
            org_text
        );
    }

    #[test]
    fn test_source_block_ordering_with_interleaved_input() {
        // Even when blocks arrive in wrong order (child text before source),
        // the renderer must sort source blocks first.
        let file_id = "holon-doc:///test/file.org";

        let mut parent = Block::new_text("p", file_id, "Root");
        parent.set_property("ID", Value::String("p".to_string()));

        let mut text_child = Block::new_text("t1", "p", "Sub Heading");
        text_child.set_property("ID", Value::String("t1".to_string()));

        let mut src_child = Block {
            id: "s1".to_string(),
            parent_id: "p".to_string(),
            content: "print('hi')\n".to_string(),
            content_type: "source".to_string(),
            source_language: Some("python".to_string()),
            source_name: None,
            properties: HashMap::new(),
            created_at: 0,
            updated_at: 0,
        };
        src_child.set_sequence(10);

        // Deliberately put text_child before src_child in the input vec
        let file_path = Path::new("/test/file.org");
        let blocks = vec![parent, text_child, src_child];
        let org_text = OrgRenderer::render_blocks(&blocks, file_path, file_id);

        let src_pos = org_text.find("#+BEGIN_SRC python").expect("source block");
        let sub_pos = org_text.find("** Sub Heading").expect("sub heading");

        assert!(
            src_pos < sub_pos,
            "Source block must come first regardless of input order.\nOutput:\n{}",
            org_text
        );
    }
}
