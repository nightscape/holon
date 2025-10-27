//! Comprehensive Property-Based Tests for org-mode round-tripping.
//!
//! Uses a simple normalize-and-compare approach:
//! 1. Generate valid Block hierarchies
//! 2. Serialize to Org format
//! 3. Parse back
//! 4. Normalize both and compare for equality

use holon::sync::document_entity::DOCUMENT_URI_SCHEME;
use holon_api::block::ROOT_DOC_ID;
use holon_api::block::{Block, CONTENT_TYPE_SOURCE, CONTENT_TYPE_TEXT};
use holon_api::Value;
use holon_filesystem::directory::ROOT_ID;
use holon_orgmode::models::{OrgBlockExt, OrgDocumentExt, ToOrg};
use holon_orgmode::org_renderer::OrgRenderer;
use holon_orgmode::parser::parse_org_file;
use holon_orgmode::Document;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use uuid::Uuid;

// ============================================================================
// Normalized representation for comparison
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct NormalizedBlock {
    id: String,
    parent_id: String,
    content_type: String,
    // Headline fields
    level: i64,
    title: String,
    task_state: Option<String>,
    priority: Option<i32>,
    tags: BTreeSet<String>,
    // Source block fields
    source_language: Option<String>,
    source_name: Option<String>,
    header_args: BTreeMap<String, String>,
}

impl NormalizedBlock {
    fn from_block(block: &Block) -> Self {
        let title = block.org_title().trim().to_string();

        let tags: BTreeSet<String> = block
            .tags()
            .map(|t| {
                t.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        let header_args: BTreeMap<String, String> = block
            .get_source_header_args()
            .into_iter()
            .filter(|(k, _)| k != "id") // Skip 'id' as it's auto-added
            .map(|(k, v)| (k, v.as_string().unwrap_or_default().to_string()))
            .collect();

        NormalizedBlock {
            id: block.id.clone(),
            parent_id: block.parent_id.clone(),
            content_type: block.content_type.clone(),
            level: block.level(),
            title,
            task_state: block.task_state(),
            priority: block.priority(),
            tags,
            source_language: block.source_language.clone(),
            source_name: block.source_name.clone(),
            header_args,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedDocument {
    title: Option<String>,
    blocks: Vec<NormalizedBlock>,
}

impl NormalizedDocument {
    fn from_doc_and_blocks(doc: &Document, blocks: &[Block]) -> Self {
        let mut normalized_blocks: Vec<NormalizedBlock> =
            blocks.iter().map(NormalizedBlock::from_block).collect();
        normalized_blocks.sort_by(|a, b| a.id.cmp(&b.id));

        NormalizedDocument {
            title: doc.org_title().map(|t| t.trim().to_string()),
            blocks: normalized_blocks,
        }
    }
}

// ============================================================================
// Strategy: Valid identifiers and text
// ============================================================================

fn valid_identifier() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_]{0,19}[a-zA-Z0-9]?"
}

fn valid_tag() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_]{0,14}"
}

fn valid_title() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9][a-zA-Z0-9 ]{0,48}[a-zA-Z0-9]"
}

fn valid_body() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9 .,!?\n]{10,200}"
}

fn valid_source_code() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_ =(){}\\[\\];,.\n]{10,100}"
}

fn valid_property_value() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_-]{1,30}"
}

fn valid_timestamp() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("<2024-01-15 Mon>".to_string()),
        Just("<2024-06-20 Thu 14:00>".to_string()),
        Just("<2024-12-31 Tue 09:30>".to_string()),
    ]
}

// ============================================================================
// Strategy: Document
// ============================================================================

fn document_strategy() -> impl Strategy<Value = Document> {
    (
        prop::option::of(valid_title()),
        prop::option::of(prop_oneof![
            Just("TODO,INPROGRESS|DONE,CANCELLED".to_string()),
            Just("TODO|DONE".to_string()),
            Just("TASK,WORK|COMPLETE".to_string()),
        ]),
    )
        .prop_map(|(title, todo_keywords)| {
            let id = format!("{}test-{}", DOCUMENT_URI_SCHEME, Uuid::new_v4());
            let mut doc =
                Document::new(id.clone(), ROOT_DOC_ID.to_string(), "test.org".to_string());
            doc.set_org_title(title);
            doc.set_todo_keywords(todo_keywords);
            doc
        })
}

// ============================================================================
// Strategy: Properties drawer (with explicit :ID:)
// ============================================================================

#[derive(Debug, Clone)]
struct PropertiesDrawer {
    id: String,
    other_props: HashMap<String, String>,
}

fn properties_drawer_strategy() -> impl Strategy<Value = PropertiesDrawer> {
    (
        "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}",
        prop::option::of(prop::collection::hash_map(
            prop_oneof![
                Just("VIEW".to_string()),
                Just("REGION".to_string()),
                Just("CUSTOM".to_string()),
            ],
            valid_property_value(),
            1..=2,
        )),
    )
        .prop_map(|(id, other_props)| PropertiesDrawer {
            id,
            other_props: other_props.unwrap_or_default(),
        })
}

// ============================================================================
// Strategy: Source Block
// ============================================================================

#[derive(Debug, Clone)]
struct SourceBlockSpec {
    id: String,
    language: String,
    source: String,
    name: Option<String>,
    header_args: HashMap<String, String>,
}

fn source_block_spec_strategy() -> impl Strategy<Value = SourceBlockSpec> {
    (
        prop_oneof![
            Just("prql".to_string()),
            Just("python".to_string()),
            Just("rust".to_string()),
            Just("sql".to_string()),
        ],
        valid_source_code(),
        prop::option::of(valid_identifier()),
        prop::collection::hash_map(
            prop_oneof![
                Just("results".to_string()),
                Just("session".to_string()),
                Just("connection".to_string()),
            ],
            valid_identifier(),
            0..=2,
        ),
    )
        .prop_map(|(language, source, name, header_args)| SourceBlockSpec {
            id: Uuid::new_v4().to_string(),
            language,
            source,
            name,
            header_args,
        })
}

// ============================================================================
// Strategy: Headline Block
// ============================================================================

#[derive(Debug, Clone)]
struct HeadlineSpec {
    properties_drawer: PropertiesDrawer,
    level: i64,
    task_state: Option<String>,
    priority: Option<i32>,
    title: String,
    tags: Option<Vec<String>>,
    body: Option<String>,
    scheduled: Option<String>,
    deadline: Option<String>,
    source_blocks: Vec<SourceBlockSpec>,
    child_headlines: Vec<HeadlineSpec>,
}

impl HeadlineSpec {
    fn id(&self) -> &str {
        &self.properties_drawer.id
    }

    fn to_block(&self, parent_id: &str, sequence: &mut i64) -> Vec<Block> {
        let mut blocks = Vec::new();

        let content = match &self.body {
            Some(b) => format!("{}\n{}", self.title, b),
            None => self.title.clone(),
        };

        let mut block = Block::new_text(self.id().to_string(), parent_id.to_string(), &content);
        block.set_level(self.level);
        block.set_sequence(*sequence);
        *sequence += 1;

        block.set_task_state(self.task_state.clone());
        block.set_priority(self.priority);

        if let Some(ref tags) = self.tags {
            if !tags.is_empty() {
                block.set_tags(Some(tags.join(",")));
            }
        }

        block.set_scheduled(self.scheduled.clone());
        block.set_deadline(self.deadline.clone());

        // Set org properties (including ID)
        let mut props_map: HashMap<String, serde_json::Value> = HashMap::new();
        props_map.insert(
            "ID".to_string(),
            serde_json::Value::String(self.id().to_string()),
        );
        for (k, v) in &self.properties_drawer.other_props {
            props_map.insert(k.clone(), serde_json::Value::String(v.clone()));
        }
        if !props_map.is_empty() {
            let props_json = serde_json::to_string(&props_map).unwrap_or_else(|_| "{}".to_string());
            block.set_org_properties(Some(props_json));
        }

        // Build children list
        let mut child_ids: Vec<String> = Vec::new();
        for sb_spec in &self.source_blocks {
            child_ids.push(sb_spec.id.clone());
        }
        for child in &self.child_headlines {
            child_ids.push(child.id().to_string());
        }
        // Children relationship is established via parent_id
        blocks.push(block);

        // Create source block entities
        for sb_spec in &self.source_blocks {
            let header_args: HashMap<String, Value> = sb_spec
                .header_args
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect();

            let mut src_block = Block {
                id: sb_spec.id.clone(),
                parent_id: self.id().to_string(),
                content: sb_spec.source.clone(),
                content_type: CONTENT_TYPE_SOURCE.to_string(),
                source_language: Some(sb_spec.language.clone()),
                source_name: sb_spec.name.clone(),
                properties: HashMap::new(),
                created_at: chrono::Utc::now().timestamp_millis(),
                updated_at: chrono::Utc::now().timestamp_millis(),
            };
            if !sb_spec.header_args.is_empty() {
                src_block.set_source_header_args(header_args);
            }
            src_block.set_sequence(*sequence);
            *sequence += 1;
            blocks.push(src_block);
        }

        // Recursively create child headline blocks
        for child in &self.child_headlines {
            blocks.extend(child.to_block(self.id(), sequence));
        }

        blocks
    }
}

fn headline_spec_strategy(
    level: i64,
    max_children: usize,
    max_depth: usize,
) -> impl Strategy<Value = HeadlineSpec> {
    (
        properties_drawer_strategy(),
        prop::option::of(prop_oneof![
            Just("TODO".to_string()),
            Just("DONE".to_string()),
            Just("INPROGRESS".to_string()),
        ]),
        prop::option::of(1..=3i32),
        valid_title(),
        prop::option::of(prop::collection::vec(valid_tag(), 1..=3)),
        prop::option::of(valid_body()),
        prop::option::of(valid_timestamp()),
        prop::option::of(valid_timestamp()),
        prop::collection::vec(source_block_spec_strategy(), 0..=3),
    )
        .prop_flat_map(
            move |(
                props,
                task_state,
                priority,
                title,
                tags,
                body,
                scheduled,
                deadline,
                source_blocks,
            )| {
                let headline = HeadlineSpec {
                    properties_drawer: props,
                    level,
                    task_state,
                    priority,
                    title,
                    tags,
                    body,
                    scheduled,
                    deadline,
                    source_blocks,
                    child_headlines: Vec::new(),
                };

                if max_depth == 0 || max_children == 0 {
                    Just(headline).boxed()
                } else {
                    let child_level = level + 1;
                    let child_max_children = max_children.saturating_sub(1);
                    let child_max_depth = max_depth - 1;

                    prop::collection::vec(
                        headline_spec_strategy(child_level, child_max_children, child_max_depth),
                        0..=max_children,
                    )
                    .prop_map(move |children| {
                        let mut h = headline.clone();
                        h.child_headlines = children;
                        h
                    })
                    .boxed()
                }
            },
        )
}

// ============================================================================
// Strategy: Complete Document
// ============================================================================

#[derive(Debug, Clone)]
struct CompleteDocument {
    document: Document,
    root_headlines: Vec<HeadlineSpec>,
}

impl CompleteDocument {
    fn all_blocks(&self) -> Vec<Block> {
        let mut blocks = Vec::new();
        let mut sequence = 0i64;

        for headline in &self.root_headlines {
            blocks.extend(headline.to_block(&self.document.id, &mut sequence));
        }

        blocks
    }

    fn ensure_todo_keywords_configured(&mut self) {
        let mut all_todos = std::collections::HashSet::new();
        all_todos.insert("TODO".to_string());
        all_todos.insert("DONE".to_string());

        fn collect_todos(headline: &HeadlineSpec, todos: &mut std::collections::HashSet<String>) {
            if let Some(ref todo) = headline.task_state {
                todos.insert(todo.clone());
            }
            for child in &headline.child_headlines {
                collect_todos(child, todos);
            }
        }

        for headline in &self.root_headlines {
            collect_todos(headline, &mut all_todos);
        }

        let active_todos: Vec<String> = all_todos
            .iter()
            .filter(|t| *t != "DONE" && *t != "CANCELLED" && *t != "COMPLETE")
            .cloned()
            .collect();
        let done_todos: Vec<String> = all_todos
            .iter()
            .filter(|t| *t == "DONE" || *t == "CANCELLED" || *t == "COMPLETE")
            .cloned()
            .collect();

        if !active_todos.is_empty() || !done_todos.is_empty() {
            self.document.set_todo_keywords(Some(format!(
                "{}|{}",
                active_todos.join(","),
                done_todos.join(",")
            )));
        }
    }
}

fn complete_document_strategy() -> impl Strategy<Value = CompleteDocument> {
    document_strategy().prop_flat_map(|doc| {
        let doc_clone = doc.clone();
        prop::collection::vec(headline_spec_strategy(1, 2, 2), 1..=4).prop_map(
            move |root_headlines| {
                let mut cd = CompleteDocument {
                    document: doc_clone.clone(),
                    root_headlines,
                };
                cd.ensure_todo_keywords_configured();
                cd
            },
        )
    })
}

// ============================================================================
// Helpers
// ============================================================================

fn build_org_text(doc: &Document, blocks: &[Block]) -> String {
    let file_path = PathBuf::from("/test/test.org");
    let file_id = &doc.id;

    let mut org_text = doc.to_org();
    if !org_text.is_empty() && !org_text.ends_with('\n') {
        org_text.push('\n');
    }

    let rendered = OrgRenderer::render_blocks(blocks, &file_path, file_id);
    org_text.push_str(&rendered);

    org_text
}

fn parse_org(org_text: &str) -> Result<holon_orgmode::parser::ParseResult, String> {
    let path = PathBuf::from("/test/test.org");
    let root = PathBuf::from("/test");
    parse_org_file(&path, org_text, ROOT_ID, 0, &root).map_err(|e| e.to_string())
}

// ============================================================================
// PBT: Comprehensive round-trip test
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 100,
        ..ProptestConfig::default()
    })]

    #[test]
    fn test_round_trip(mut complete_doc in complete_document_strategy()) {
        complete_doc.ensure_todo_keywords_configured();

        let blocks = complete_doc.all_blocks();
        let org_text = build_org_text(&complete_doc.document, &blocks);

        let parse_result = match parse_org(&org_text) {
            Ok(r) => r,
            Err(e) => {
                prop_assert!(false, "Parsing failed: {}\n\nOrg text:\n{}", e, org_text);
                return Ok(());
            }
        };

        let expected = NormalizedDocument::from_doc_and_blocks(&complete_doc.document, &blocks);
        let actual = NormalizedDocument::from_doc_and_blocks(&parse_result.document, &parse_result.blocks);

        // Compare document title
        prop_assert_eq!(
            &expected.title,
            &actual.title,
            "Document titles must match"
        );

        // Compare block counts
        prop_assert_eq!(
            expected.blocks.len(),
            actual.blocks.len(),
            "Block counts must match"
        );

        // Compare each block by ID
        for expected_block in &expected.blocks {
            let actual_block = actual.blocks.iter().find(|b| b.id == expected_block.id);

            prop_assert!(
                actual_block.is_some(),
                "Block with ID '{}' must exist after round-trip",
                expected_block.id
            );

            let actual_block = actual_block.unwrap();

            // Compare fields
            // For root-level blocks, parent is the document which has different ID formats
            let expected_is_root = expected_block.parent_id.starts_with("holon-doc://");
            let actual_is_root = actual_block.parent_id.starts_with("holon-doc://");
            if expected_is_root || actual_is_root {
                prop_assert_eq!(
                    expected_is_root,
                    actual_is_root,
                    "Root status must match for block '{}'",
                    expected_block.id
                );
            } else {
                prop_assert_eq!(
                    &expected_block.parent_id,
                    &actual_block.parent_id,
                    "Parent ID must match for block '{}'",
                    expected_block.id
                );
            }

            prop_assert_eq!(
                &expected_block.content_type,
                &actual_block.content_type,
                "Content type must match for block '{}'",
                expected_block.id
            );

            if expected_block.content_type == CONTENT_TYPE_TEXT {
                prop_assert_eq!(
                    expected_block.level,
                    actual_block.level,
                    "Level must match for headline '{}'",
                    expected_block.id
                );

                prop_assert_eq!(
                    &expected_block.task_state,
                    &actual_block.task_state,
                    "Task state must match for headline '{}'",
                    expected_block.id
                );

                prop_assert_eq!(
                    expected_block.priority,
                    actual_block.priority,
                    "Priority must match for headline '{}'",
                    expected_block.id
                );

                prop_assert_eq!(
                    &expected_block.tags,
                    &actual_block.tags,
                    "Tags must match for headline '{}'",
                    expected_block.id
                );
            }

            if expected_block.content_type == CONTENT_TYPE_SOURCE {
                prop_assert_eq!(
                    &expected_block.source_language,
                    &actual_block.source_language,
                    "Source language must match for source block '{}'",
                    expected_block.id
                );

                prop_assert_eq!(
                    &expected_block.source_name,
                    &actual_block.source_name,
                    "Source name must match for source block '{}'",
                    expected_block.id
                );

                prop_assert_eq!(
                    &expected_block.header_args,
                    &actual_block.header_args,
                    "Header args must match for source block '{}'",
                    expected_block.id
                );
            }
        }
    }

    /// Test round-trip fidelity: parse org → render → parse again → compare
    #[test]
    fn test_parse_render_parse_fidelity(mut complete_doc in complete_document_strategy()) {
        complete_doc.ensure_todo_keywords_configured();

        let blocks = complete_doc.all_blocks();
        let org_text_original = build_org_text(&complete_doc.document, &blocks);

        // Parse the org text (simulates reading from disk)
        let parse_1 = match parse_org(&org_text_original) {
            Ok(r) => r,
            Err(_) => return Ok(()),
        };

        // Render parsed blocks back to org (simulates OrgFileWriter)
        let org_text_rendered = build_org_text(&parse_1.document, &parse_1.blocks);

        // Parse the rendered text again (simulates OrgAdapter re-reading)
        let parse_2 = match parse_org(&org_text_rendered) {
            Ok(r) => r,
            Err(e) => {
                prop_assert!(false,
                    "Second parse failed: {}\n\nRendered text:\n{}\n\nOriginal text:\n{}",
                    e, org_text_rendered, org_text_original
                );
                return Ok(());
            }
        };

        let norm_1 = NormalizedDocument::from_doc_and_blocks(&parse_1.document, &parse_1.blocks);
        let norm_2 = NormalizedDocument::from_doc_and_blocks(&parse_2.document, &parse_2.blocks);

        // Document title must be stable
        prop_assert_eq!(&norm_1.title, &norm_2.title, "Title unstable across round-trip");

        // Block count must be stable
        prop_assert_eq!(
            norm_1.blocks.len(),
            norm_2.blocks.len(),
            "Block count changed: {} -> {}\nRendered:\n{}",
            norm_1.blocks.len(), norm_2.blocks.len(), org_text_rendered
        );

        // Each block's content must be stable
        for b1 in &norm_1.blocks {
            let b2 = norm_2.blocks.iter().find(|b| b.id == b1.id);
            prop_assert!(b2.is_some(), "Block '{}' lost in round-trip", b1.id);
            let b2 = b2.unwrap();

            prop_assert_eq!(&b1.content_type, &b2.content_type, "Content type changed for '{}'", b1.id);
            prop_assert_eq!(b1.level, b2.level, "Level changed for '{}'", b1.id);
            prop_assert_eq!(&b1.title, &b2.title, "Title changed for '{}'", b1.id);
            prop_assert_eq!(&b1.task_state, &b2.task_state, "Task state changed for '{}'", b1.id);
            prop_assert_eq!(b1.priority, b2.priority, "Priority changed for '{}'", b1.id);
            prop_assert_eq!(&b1.tags, &b2.tags, "Tags changed for '{}'", b1.id);
            prop_assert_eq!(&b1.source_language, &b2.source_language, "Language changed for '{}'", b1.id);
            prop_assert_eq!(&b1.source_name, &b2.source_name, "Name changed for '{}'", b1.id);
            prop_assert_eq!(&b1.header_args, &b2.header_args, "Header args changed for '{}'", b1.id);
        }
    }

    /// Test idempotency: serialize → parse → serialize → parse should be stable
    #[test]
    fn test_idempotent_round_trip(mut complete_doc in complete_document_strategy()) {
        complete_doc.ensure_todo_keywords_configured();

        let blocks = complete_doc.all_blocks();
        let org_text_1 = build_org_text(&complete_doc.document, &blocks);

        let parse_result_1 = match parse_org(&org_text_1) {
            Ok(r) => r,
            Err(_) => return Ok(()),
        };

        // Rebuild children for re-serialization
        let mut blocks_1 = parse_result_1.blocks.clone();
        let mut parent_to_children: HashMap<String, Vec<String>> = HashMap::new();
        for block in &blocks_1 {
            parent_to_children.entry(block.parent_id.clone()).or_default().push(block.id.clone());
        }
        // Children relationship is established via parent_id, no need to set children vec
        let _ = blocks_1; // Keep the variable to avoid unused warning

        let org_text_2 = build_org_text(&parse_result_1.document, &blocks_1);

        let parse_result_2 = match parse_org(&org_text_2) {
            Ok(r) => r,
            Err(_) => return Ok(()),
        };

        let normalized_1 = NormalizedDocument::from_doc_and_blocks(&parse_result_1.document, &parse_result_1.blocks);
        let normalized_2 = NormalizedDocument::from_doc_and_blocks(&parse_result_2.document, &parse_result_2.blocks);

        prop_assert_eq!(
            normalized_1.blocks.len(),
            normalized_2.blocks.len(),
            "Block count must be stable across round-trips"
        );

        for block_1 in &normalized_1.blocks {
            let block_2 = normalized_2.blocks.iter().find(|b| b.id == block_1.id);
            prop_assert!(
                block_2.is_some(),
                "Block '{}' must persist across round-trips",
                block_1.id
            );
        }
    }
}

// ============================================================================
// Deterministic round-trip fidelity tests with hand-crafted org files
// ============================================================================

#[cfg(test)]
mod deterministic_round_trip {
    use super::*;

    /// Parse org text, render back, parse again, and assert block-level equivalence.
    fn assert_round_trip_stable(org_text: &str) {
        let parse_1 = parse_org(org_text).expect("First parse must succeed");

        let org_rendered = build_org_text(&parse_1.document, &parse_1.blocks);

        let parse_2 = parse_org(&org_rendered)
            .unwrap_or_else(|e| panic!("Second parse failed: {}\nRendered:\n{}", e, org_rendered));

        let norm_1 = NormalizedDocument::from_doc_and_blocks(&parse_1.document, &parse_1.blocks);
        let norm_2 = NormalizedDocument::from_doc_and_blocks(&parse_2.document, &parse_2.blocks);

        assert_eq!(norm_1.title, norm_2.title, "Document title must be stable");
        assert_eq!(
            norm_1.blocks.len(),
            norm_2.blocks.len(),
            "Block count must be stable. Rendered:\n{}",
            org_rendered
        );

        for b1 in &norm_1.blocks {
            let b2 = norm_2
                .blocks
                .iter()
                .find(|b| b.id == b1.id)
                .unwrap_or_else(|| {
                    panic!(
                        "Block '{}' lost in round-trip. Rendered:\n{}",
                        b1.id, org_rendered
                    )
                });
            assert_eq!(
                b1.content_type, b2.content_type,
                "content_type for '{}'",
                b1.id
            );
            assert_eq!(b1.level, b2.level, "level for '{}'", b1.id);
            assert_eq!(b1.title, b2.title, "title for '{}'", b1.id);
            assert_eq!(b1.task_state, b2.task_state, "task_state for '{}'", b1.id);
            assert_eq!(b1.priority, b2.priority, "priority for '{}'", b1.id);
            assert_eq!(b1.tags, b2.tags, "tags for '{}'", b1.id);
            assert_eq!(
                b1.source_language, b2.source_language,
                "language for '{}'",
                b1.id
            );
            assert_eq!(b1.source_name, b2.source_name, "name for '{}'", b1.id);
        }
    }

    #[test]
    fn test_simple_headlines() {
        assert_round_trip_stable(
            "* First headline\n:PROPERTIES:\n:ID: id-1\n:END:\n\
             ** Nested headline\n:PROPERTIES:\n:ID: id-2\n:END:\n\
             * Second headline\n:PROPERTIES:\n:ID: id-3\n:END:\n",
        );
    }

    #[test]
    fn test_todo_priority_tags() {
        assert_round_trip_stable(
            "#+TODO: TODO INPROGRESS | DONE CANCELLED\n\
             * TODO [#A] Important task :work:urgent:\n\
             :PROPERTIES:\n:ID: id-task-1\n:END:\n",
        );
    }

    #[test]
    fn test_headline_with_body() {
        assert_round_trip_stable(
            "* Notes\n:PROPERTIES:\n:ID: id-notes\n:END:\n\
             Some body text here.\nWith multiple lines.\n\n",
        );
    }

    #[test]
    fn test_headline_with_source_block() {
        assert_round_trip_stable(
            "* Query\n:PROPERTIES:\n:ID: id-query\n:END:\n\
             #+BEGIN_SRC prql :id src-1\n\
             from tasks\n\
             select {id, content}\n\
             #+END_SRC\n",
        );
    }

    #[test]
    fn test_named_source_block_with_header_args() {
        assert_round_trip_stable(
            "* Analysis\n:PROPERTIES:\n:ID: id-analysis\n:END:\n\
             #+NAME: my-query\n\
             #+BEGIN_SRC sql :id src-2 :connection main :results table\n\
             SELECT * FROM users;\n\
             #+END_SRC\n",
        );
    }

    #[test]
    fn test_multiple_source_blocks_under_one_heading() {
        assert_round_trip_stable(
            "* Multiple\n:PROPERTIES:\n:ID: id-multi\n:END:\n\
             #+BEGIN_SRC sql :id src-a\n\
             SELECT 1;\n\
             #+END_SRC\n\
             #+BEGIN_SRC prql :id src-b\n\
             from users | take 10\n\
             #+END_SRC\n",
        );
    }

    #[test]
    fn test_deep_nesting() {
        assert_round_trip_stable(
            "* Level 1\n:PROPERTIES:\n:ID: l1\n:END:\n\
             ** Level 2\n:PROPERTIES:\n:ID: l2\n:END:\n\
             *** Level 3\n:PROPERTIES:\n:ID: l3\n:END:\n\
             **** Level 4\n:PROPERTIES:\n:ID: l4\n:END:\n",
        );
    }

    #[test]
    fn test_document_with_title() {
        assert_round_trip_stable(
            "#+TITLE: My Document\n\
             * Heading\n:PROPERTIES:\n:ID: id-h\n:END:\n",
        );
    }

    #[test]
    fn test_custom_properties_preserved() {
        assert_round_trip_stable(
            "* Heading\n:PROPERTIES:\n:ID: id-props\n:VIEW: query\n:REGION: main\n:END:\n",
        );
    }

    #[test]
    fn test_mixed_todo_states() {
        assert_round_trip_stable(
            "#+TODO: TODO DOING | DONE CANCELLED\n\
             * TODO Task one\n:PROPERTIES:\n:ID: t1\n:END:\n\
             * DONE Task two\n:PROPERTIES:\n:ID: t2\n:END:\n\
             * DOING Task three\n:PROPERTIES:\n:ID: t3\n:END:\n",
        );
    }
}
