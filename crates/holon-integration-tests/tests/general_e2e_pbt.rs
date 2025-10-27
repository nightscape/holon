//! General-Purpose Property-Based E2E Test
//!
//! This module provides a stateful property-based test that verifies:
//! 1. **Convergence**: All persistence formats (Loro, Org files, future formats) converge to the same state
//! 2. **CDC correctness**: Change streams accurately reflect mutations
//! 3. **Multi-source consistency**: Mutations from UI, external files, and Org sync all produce consistent states
//!
//! ## Architecture
//!
//! The test uses the real Loro-based system:
//! - `LoroDocumentStore` manages Loro documents (Org files as backing store)
//! - `LoroBlockOperations` provides CRUD operations on blocks
//! - UI mutations go through `ctx.execute_op()` → `LoroBlockOperations` → Loro → Org file
//! - External mutations modify Org files directly, which are then synced to Loro
//!
//! ## No Direct Database Access
//!
//! This test does NOT do any DDL or DML operations on the Turso DB directly.
//! All mutations go through the proper operation providers.
//!
//! ## Why This Crate Exists
//!
//! This test is in a separate crate to avoid circular dev-dependencies:
//! - holon → (dev-dep) → holon-orgmode → (dep) → holon would cause TypeId mismatches
//! - This crate depends on both as regular dev-dependencies, avoiding the issue
//!
//! ## Reference Model
//!
//! This test uses the production `Block` struct directly as the reference model,
//! leveraging `OrgBlockExt` for org-specific field access. This eliminates the
//! need for a separate test-only struct and ensures the test validates against
//! the actual production data model.

use anyhow::Result;
use loro::{ExportMode, LoroDoc};
use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use holon::api::types::Traversal;
use holon::api::{CoreOperations, LoroBackend};
use holon::sync::document_entity::is_document_uri;
use holon::testing::e2e_test_helpers::ChangeType;
use holon_api::Value;
use holon_api::block::{Block, CONTENT_TYPE_SOURCE, CONTENT_TYPE_TEXT};
use holon_orgmode::models::OrgBlockExt;
#[cfg(test)]
use similar_asserts::assert_eq;
use std::time::{Duration, Instant};

// Import shared utilities from the crate's lib
use holon_integration_tests::{
    LoroCorruptionType, TestContext, assert_blocks_equivalent, block_belongs_to_document,
    serialize_blocks_to_org, wait_for_file_condition,
};

// =============================================================================
// Polling Utilities
// =============================================================================

// Note: `wait_until` and `wait_for_file_condition` are imported from the shared crate.
// `wait_for_block_count` and `wait_for_block` are methods on TestContext.

// =============================================================================
// CRDT Merge Simulation
// =============================================================================

/// Simulate Loro CRDT merge for concurrent text updates.
///
/// Creates two Loro peers from a common ancestor, applies one update on each,
/// then merges them. Returns the CRDT-merged content.
///
/// This mirrors what the real system does: the UI mutation applies to Loro directly,
/// while the External mutation writes to org → OrgAdapter → Loro. Both use
/// `LoroText::update()` which does diff-based CRDT edits.
fn loro_merge_text(original: &str, update_a: &str, update_b: &str) -> String {
    // Common ancestor
    let ancestor = LoroDoc::new();
    ancestor.set_peer_id(0).unwrap();
    let text = ancestor.get_text("content");
    text.update(original, Default::default()).unwrap();
    ancestor.commit();
    let snapshot = ancestor.export(ExportMode::Snapshot).unwrap();

    // Peer A (UI) — applies update_a concurrently
    let peer_a = LoroDoc::new();
    peer_a.set_peer_id(1).unwrap();
    peer_a.import(&snapshot).unwrap();
    peer_a
        .get_text("content")
        .update(update_a, Default::default())
        .unwrap();
    peer_a.commit();

    // Peer B (External) — applies update_b concurrently
    let peer_b = LoroDoc::new();
    peer_b.set_peer_id(2).unwrap();
    peer_b.import(&snapshot).unwrap();
    peer_b
        .get_text("content")
        .update(update_b, Default::default())
        .unwrap();
    peer_b.commit();

    // Merge: A imports B's changes
    let b_updates = peer_b.export(ExportMode::all_updates()).unwrap();
    peer_a.import(&b_updates).unwrap();

    peer_a.get_text("content").to_string()
}

// =============================================================================
// Unified Mutation Model
// =============================================================================

/// Source of a mutation
#[derive(Debug, Clone, PartialEq)]
pub enum MutationSource {
    /// User action via BackendEngine operations (through ctx.execute_op)
    UI,
    /// External change to an Org file (simulates file edit)
    External,
}

/// A mutation to the data model
#[derive(Debug, Clone)]
pub enum Mutation {
    Create {
        entity: String,
        id: String,
        parent_id: String,
        fields: HashMap<String, Value>,
    },
    Update {
        entity: String,
        id: String,
        fields: HashMap<String, Value>,
    },
    Delete {
        entity: String,
        id: String,
    },
    Move {
        entity: String,
        id: String,
        new_parent_id: String,
    },
    /// Simulate app restart: clears OrgAdapter's known_state.
    /// This tests that re-parsing org files doesn't create orphan blocks in Loro.
    RestartApp,
}

impl Mutation {
    /// Returns the block ID targeted by this mutation, if any.
    pub fn target_block_id(&self) -> Option<String> {
        match self {
            Mutation::Create { id, .. }
            | Mutation::Update { id, .. }
            | Mutation::Delete { id, .. }
            | Mutation::Move { id, .. } => Some(id.clone()),
            Mutation::RestartApp => None,
        }
    }

    /// Convert mutation to BackendEngine operation parameters
    pub fn to_operation(&self) -> (String, String, HashMap<String, Value>) {
        match self {
            Mutation::Create {
                entity,
                id,
                parent_id,
                fields,
            } => {
                let mut params = fields.clone();
                params.insert("id".to_string(), Value::String(id.clone()));
                params.insert("parent_id".to_string(), Value::String(parent_id.clone()));
                (entity.clone(), "create".to_string(), params)
            }
            Mutation::Update { entity, id, fields } => {
                let mut params = HashMap::new();
                params.insert("id".to_string(), Value::String(id.clone()));

                if let Some((field_name, field_value)) = fields
                    .iter()
                    .find(|(k, _)| *k != "id" && *k != "parent_id")
                    .map(|(k, v)| (k.clone(), v.clone()))
                {
                    params.insert("field".to_string(), Value::String(field_name));
                    params.insert("value".to_string(), field_value);
                } else {
                    params.insert("field".to_string(), Value::String("content".to_string()));
                    params.insert("value".to_string(), Value::String(String::new()));
                }
                (entity.clone(), "set_field".to_string(), params)
            }
            Mutation::Delete { entity, id } => {
                let mut params = HashMap::new();
                params.insert("id".to_string(), Value::String(id.clone()));
                (entity.clone(), "delete".to_string(), params)
            }
            Mutation::Move {
                entity,
                id,
                new_parent_id,
            } => {
                let mut params = HashMap::new();
                params.insert("id".to_string(), Value::String(id.clone()));
                params.insert(
                    "parent_id".to_string(),
                    Value::String(new_parent_id.clone()),
                );
                (entity.clone(), "set_field".to_string(), params)
            }
            Mutation::RestartApp => {
                // RestartApp doesn't use backend operations - handled specially
                (
                    "_restart".to_string(),
                    "restart".to_string(),
                    HashMap::new(),
                )
            }
        }
    }

    /// Apply mutation to a vector of blocks (for reference model)
    pub fn apply_to(&self, blocks: &mut Vec<Block>) {
        match self {
            Mutation::Create {
                id,
                parent_id,
                fields,
                ..
            } => {
                let content = fields
                    .get("content")
                    .and_then(|v| v.as_string())
                    .unwrap_or_default()
                    .to_string();

                let content_type = fields
                    .get("content_type")
                    .and_then(|v| v.as_string())
                    .unwrap_or(CONTENT_TYPE_TEXT);

                let source_language = fields
                    .get("source_language")
                    .and_then(|v| v.as_string())
                    .map(|s| s.to_string());

                // Create block with appropriate content type
                let mut block = if content_type == CONTENT_TYPE_SOURCE {
                    let mut b = Block::new_text(id.clone(), parent_id.clone(), content);
                    b.content_type = CONTENT_TYPE_SOURCE.to_string();
                    b.source_language = source_language;
                    b
                } else {
                    Block::new_text(id.clone(), parent_id.clone(), content)
                };

                // Set Org-specific fields via OrgBlockExt
                if let Some(task_state) = fields
                    .get("task_state")
                    .or_else(|| fields.get("TODO"))
                    .and_then(|v| v.as_string())
                {
                    block.set_task_state(Some(task_state.to_string()));
                }
                if let Some(priority) = fields
                    .get("priority")
                    .or_else(|| fields.get("PRIORITY"))
                    .and_then(|v| v.as_i64())
                {
                    block.set_priority(Some(priority as i32));
                }
                if let Some(tags) = fields
                    .get("tags")
                    .or_else(|| fields.get("TAGS"))
                    .and_then(|v| v.as_string())
                {
                    block.set_tags(Some(tags.to_string()));
                }
                if let Some(scheduled) = fields
                    .get("scheduled")
                    .or_else(|| fields.get("SCHEDULED"))
                    .and_then(|v| v.as_string())
                {
                    block.set_scheduled(Some(scheduled.to_string()));
                }
                if let Some(deadline) = fields
                    .get("deadline")
                    .or_else(|| fields.get("DEADLINE"))
                    .and_then(|v| v.as_string())
                {
                    block.set_deadline(Some(deadline.to_string()));
                }

                // Set other properties (excluding Org-specific, standard, and source block fields)
                for (k, v) in fields.iter() {
                    if !matches!(
                        k.as_str(),
                        "content"
                            | "content_type"
                            | "source_language"
                            | "id"
                            | "parent_id"
                            | "task_state"
                            | "TODO"
                            | "priority"
                            | "PRIORITY"
                            | "tags"
                            | "TAGS"
                            | "scheduled"
                            | "SCHEDULED"
                            | "deadline"
                            | "DEADLINE"
                    ) {
                        block.properties.insert(k.clone(), v.clone());
                    }
                }

                blocks.push(block);
            }
            Mutation::Update { id, fields, .. } => {
                if let Some(block) = blocks.iter_mut().find(|b| b.id == *id) {
                    if let Some(content) = fields.get("content").and_then(|v| v.as_string()) {
                        block.content = content.to_string();
                    }

                    // Update Org-specific fields via OrgBlockExt
                    if let Some(task_state) = fields
                        .get("task_state")
                        .or_else(|| fields.get("TODO"))
                        .and_then(|v| v.as_string())
                    {
                        block.set_task_state(Some(task_state.to_string()));
                    }
                    if let Some(priority) = fields
                        .get("priority")
                        .or_else(|| fields.get("PRIORITY"))
                        .and_then(|v| v.as_i64())
                    {
                        block.set_priority(Some(priority as i32));
                    }
                    if let Some(tags) = fields
                        .get("tags")
                        .or_else(|| fields.get("TAGS"))
                        .and_then(|v| v.as_string())
                    {
                        block.set_tags(Some(tags.to_string()));
                    }
                    if let Some(scheduled) = fields
                        .get("scheduled")
                        .or_else(|| fields.get("SCHEDULED"))
                        .and_then(|v| v.as_string())
                    {
                        block.set_scheduled(Some(scheduled.to_string()));
                    }
                    if let Some(deadline) = fields
                        .get("deadline")
                        .or_else(|| fields.get("DEADLINE"))
                        .and_then(|v| v.as_string())
                    {
                        block.set_deadline(Some(deadline.to_string()));
                    }

                    // Update other properties
                    for (k, v) in fields.iter() {
                        if !matches!(
                            k.as_str(),
                            "content"
                                | "task_state"
                                | "TODO"
                                | "priority"
                                | "PRIORITY"
                                | "tags"
                                | "TAGS"
                                | "scheduled"
                                | "SCHEDULED"
                                | "deadline"
                                | "DEADLINE"
                        ) {
                            block.properties.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
            Mutation::Delete { id, .. } => {
                // Cascade delete: remove the block and all descendants
                let mut to_delete: Vec<String> = vec![id.clone()];
                let mut i = 0;
                while i < to_delete.len() {
                    let parent_id = &to_delete[i];
                    let children: Vec<String> = blocks
                        .iter()
                        .filter(|b| &b.parent_id == parent_id)
                        .map(|b| b.id.clone())
                        .collect();
                    to_delete.extend(children);
                    i += 1;
                }
                blocks.retain(|b| !to_delete.contains(&b.id));
            }
            Mutation::Move {
                id, new_parent_id, ..
            } => {
                if let Some(block) = blocks.iter_mut().find(|b| b.id == *id) {
                    block.parent_id = new_parent_id.clone();
                }
            }
            Mutation::RestartApp => {
                // RestartApp doesn't change the reference model - blocks should remain the same.
                // The test verifies that the SUT also preserves blocks correctly after restart.
            }
        }
    }
}

/// A mutation event with source information
#[derive(Debug, Clone)]
pub struct MutationEvent {
    pub source: MutationSource,
    pub mutation: Mutation,
}

// =============================================================================
// Reference Model
// =============================================================================

/// Reference state tracking all expected data (uses production Block struct)
#[derive(Debug, Clone)]
pub struct ReferenceState {
    /// Whether the application has been started
    pub app_started: bool,

    /// Canonical block state (using production Block struct)
    blocks: HashMap<String, Block>,

    /// Created documents (doc_uri -> file_name)
    documents: HashMap<String, String>,

    /// Expected CDC events not yet observed
    pending_cdc_events: VecDeque<ExpectedCDCEvent>,

    /// Active query watches (query_id -> filter predicate)
    active_watches: HashMap<String, WatchSpec>,

    /// ID counter for generating unique block IDs
    next_id: usize,

    /// ID counter for generating unique document IDs
    next_doc_id: usize,

    /// Current view filter ("all", "main", "sidebar")
    current_view: String,

    /// Navigation history per region (for back/forward navigation)
    navigation_history: HashMap<String, NavigationHistory>,

    /// Mapping of block_id → doc_uri (persists even after blocks are deleted)
    block_documents: HashMap<String, String>,

    /// Runtime for async operations
    runtime: Arc<tokio::runtime::Runtime>,

    /// Pre-startup directories created (relative paths)
    pre_startup_directories: Vec<String>,

    /// Whether git has been initialized
    git_initialized: bool,

    /// Whether jj has been initialized
    jj_initialized: bool,

    /// Number of pre-startup org files created (for weighting StartApp)
    pre_startup_file_count: usize,

    /// Block IDs that are part of the root layout structure (index.org headline + PRQL source).
    /// These must not be deleted or have their content mutated, as `initial_widget()` depends on them.
    layout_block_ids: HashSet<String>,
}

/// Expected CDC event
#[derive(Debug, Clone)]
pub struct ExpectedCDCEvent {
    pub query_id: String,
    pub change_type: ChangeType,
    pub entity_id: String,
}

/// Specification for a watch
#[derive(Debug, Clone)]
pub struct WatchSpec {
    pub prql: String,
    pub filter: Option<FilterPredicate>,
}

/// Filter predicate (simplified for now)
#[derive(Debug, Clone)]
pub struct FilterPredicate {
    pub field: String,
    pub value: Value,
}

/// Navigation history for a region (for back/forward navigation)
#[derive(Debug, Clone)]
pub struct NavigationHistory {
    /// History entries: None = home view, Some(id) = focused on block
    pub entries: Vec<Option<String>>,
    /// Current cursor position in history
    pub cursor: usize,
}

impl NavigationHistory {
    pub fn new() -> Self {
        Self {
            entries: vec![None],
            cursor: 0,
        }
    }

    pub fn can_go_back(&self) -> bool {
        self.cursor > 0
    }

    pub fn can_go_forward(&self) -> bool {
        self.cursor < self.entries.len().saturating_sub(1)
    }

    pub fn current_focus(&self) -> Option<String> {
        self.entries.get(self.cursor).cloned().flatten()
    }
}

impl ReferenceState {
    pub fn empty() -> Self {
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        Self {
            app_started: false,
            blocks: HashMap::new(),
            documents: HashMap::new(),
            pending_cdc_events: VecDeque::new(),
            active_watches: HashMap::new(),
            next_id: 0,
            next_doc_id: 0,
            current_view: "all".to_string(),
            navigation_history: HashMap::new(),
            block_documents: HashMap::new(),
            runtime,
            pre_startup_directories: Vec::new(),
            git_initialized: false,
            jj_initialized: false,
            pre_startup_file_count: 0,
            layout_block_ids: HashSet::new(),
        }
    }

    pub fn with_blocks(blocks: Vec<Block>) -> Self {
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let blocks_map: HashMap<String, Block> =
            blocks.iter().map(|b| (b.id.clone(), b.clone())).collect();
        let block_documents: HashMap<String, String> = blocks
            .iter()
            .filter_map(|b| {
                if is_document_uri(&b.parent_id) {
                    Some((b.id.clone(), b.parent_id.clone()))
                } else {
                    None
                }
            })
            .collect();
        Self {
            app_started: true,
            blocks: blocks_map,
            documents: HashMap::new(),
            pending_cdc_events: VecDeque::new(),
            active_watches: HashMap::new(),
            next_id: 0,
            next_doc_id: 0,
            current_view: "all".to_string(),
            navigation_history: HashMap::new(),
            block_documents,
            runtime,
            pre_startup_directories: Vec::new(),
            git_initialized: false,
            jj_initialized: false,
            pre_startup_file_count: 0,
            layout_block_ids: HashSet::new(),
        }
    }

    pub fn current_focus(&self, region: &str) -> Option<String> {
        self.navigation_history
            .get(region)
            .and_then(|h| h.current_focus())
    }

    pub fn can_go_back(&self, region: &str) -> bool {
        self.navigation_history
            .get(region)
            .map(|h| h.can_go_back())
            .unwrap_or(false)
    }

    pub fn can_go_forward(&self, region: &str) -> bool {
        self.navigation_history
            .get(region)
            .map(|h| h.can_go_forward())
            .unwrap_or(false)
    }

    pub fn from_structure(_structure: Vec<Block>) -> Self {
        Self::empty()
    }

    pub fn current_view(&self) -> String {
        self.current_view.clone()
    }

    pub fn query_results(&self, _watch_spec: &WatchSpec) -> Vec<HashMap<String, Value>> {
        self.blocks
            .values()
            .map(|b| {
                let mut row = HashMap::new();
                row.insert("id".to_string(), Value::String(b.id.clone()));
                row.insert("content".to_string(), Value::String(b.content.clone()));
                row.insert(
                    "content_type".to_string(),
                    Value::String(b.content_type.clone()),
                );
                row.insert("parent_id".to_string(), Value::String(b.parent_id.clone()));
                if let Some(ref lang) = b.source_language {
                    row.insert("source_language".to_string(), Value::String(lang.clone()));
                }
                if let Some(ref name) = b.source_name {
                    row.insert("source_name".to_string(), Value::String(name.clone()));
                }
                for (k, v) in &b.properties {
                    row.insert(k.clone(), v.clone());
                }
                row
            })
            .collect()
    }

    /// Check if index.org exists with the structure required by initial_widget().
    ///
    /// initial_widget() requires:
    /// 1. An index.org document to exist
    /// 2. A root headline block (direct child of index.org)
    /// 3. That headline has a PRQL source block child
    pub fn has_valid_index_org(&self) -> bool {
        use holon::sync::document_entity::DOCUMENT_URI_SCHEME;

        // Check if index.org exists
        let index_doc_uri = format!("{}index.org", DOCUMENT_URI_SCHEME);
        if !self.documents.contains_key(&index_doc_uri) {
            return false;
        }

        // Find root-level blocks in index.org (direct children of the document)
        let root_blocks: Vec<&Block> = self
            .blocks
            .values()
            .filter(|b| b.parent_id == index_doc_uri)
            .collect();

        // Check if any root block has a PRQL source child
        root_blocks.iter().any(|root_block| {
            self.blocks.values().any(|child| {
                child.parent_id == root_block.id
                    && child.content_type == CONTENT_TYPE_SOURCE
                    && child.source_language.as_deref() == Some("prql")
            })
        })
    }

    /// Get IDs of text blocks only (not source blocks).
    ///
    /// In org-mode, source blocks can only be children of headlines (text blocks),
    /// not children of other source blocks. This method returns only text block IDs
    /// that are valid parents for source blocks.
    pub fn text_block_ids(&self) -> Vec<String> {
        self.blocks
            .iter()
            .filter(|(_, b)| b.content_type == CONTENT_TYPE_TEXT)
            .map(|(id, _)| id.clone())
            .collect()
    }
}

// =============================================================================
// Transitions
// =============================================================================

#[derive(Debug, Clone)]
pub enum E2ETransition {
    // === Pre-startup transitions ===
    /// Write an org file to temp directory (before app starts)
    ///
    /// Only valid when app is not running.
    WriteOrgFile { filename: String, content: String },

    /// Create a directory (possibly nested) before app starts
    ///
    /// This simulates having nested directory structures that the sync provider
    /// will scan, increasing processing time and widening the race window.
    CreateDirectory { path: String },

    /// Initialize git repository (runs `git init`)
    ///
    /// This creates a .git directory with many subdirectories that the sync
    /// provider currently scans (a bug), contributing to the race condition.
    GitInit,

    /// Initialize jj repository (runs `jj git init`)
    ///
    /// This creates both .git and .jj directories with many subdirectories.
    /// Real org-mode users often use version control, so this is realistic.
    JjGitInit,

    /// Create a stale/corrupted .loro file BEFORE the system starts.
    ///
    /// This tests the scenario where a .loro file exists from a previous run
    /// but is corrupted (empty, truncated, or invalid). The system should
    /// detect this and recover by recreating the document from the .org file.
    CreateStaleLoro {
        /// The org filename this .loro file corresponds to (e.g., "test.org")
        org_filename: String,
        /// Type of corruption to simulate
        corruption_type: LoroCorruptionType,
    },

    /// Start the application (triggers sync, may race with DDL)
    ///
    /// Only valid when app is not running.
    StartApp {
        wait_for_ready: bool,
        /// Enable Todoist fake mode (adds concurrent DDL during startup)
        enable_todoist: bool,
    },

    // === Post-startup transitions ===
    /// Create a new document (Org file)
    CreateDocument { file_name: String },

    /// Apply a mutation from any source (UI, external file, Loro sync)
    ApplyMutation(MutationEvent),

    /// Set up a CDC watch for a query
    SetupWatch {
        query_id: String,
        prql: String,
        filter: Option<FilterPredicate>,
    },

    /// Remove a watch
    RemoveWatch { query_id: String },

    /// Switch the active view filter
    SwitchView { view_name: String },

    /// Navigate to focus on a specific block in a region
    NavigateFocus { region: String, block_id: String },

    /// Navigate back in history for a region
    NavigateBack { region: String },

    /// Navigate forward in history for a region
    NavigateForward { region: String },

    /// Navigate to home (root view) for a region
    NavigateHome { region: String },

    /// Simulate app restart: clears OrgAdapter's known_state.
    /// This tests that re-parsing org files doesn't create orphan blocks in Loro.
    /// After restart, the org file should be re-processed and blocks should match.
    SimulateRestart,

    /// Bulk external add: adds multiple blocks at once via external file modification.
    /// This tests the sync loop where OrgFileWriter might re-render before all blocks are synced.
    /// The test verifies that adding N blocks externally results in N new blocks, not fewer.
    BulkExternalAdd {
        /// Target document URI
        doc_uri: String,
        /// Blocks to add (fully specified for deterministic state)
        blocks: Vec<Block>,
    },

    /// Concurrent schema init: triggers schema re-initialization while other operations are running.
    /// This tests the bug where calling ensure_navigation_schema() a second time while
    /// CDC/IVM operations are active causes "database is locked" errors.
    /// The bug occurs because:
    /// 1. ensure_navigation_schema() is called during DI init
    /// 2. initial_widget() or query_and_watch() calls it again
    /// 3. The DROP VIEW + CREATE MATERIALIZED VIEW sequence conflicts with ongoing IVM
    ConcurrentSchemaInit,

    /// Concurrent mutations: fires a UI mutation and an External mutation without waiting
    /// for sync between them. This simulates real-world concurrent edits (e.g., user edits
    /// in the UI while an external tool modifies the org file simultaneously).
    /// Validates that the Loro CRDT correctly resolves concurrent writes.
    ConcurrentMutations {
        ui_mutation: MutationEvent,
        external_mutation: MutationEvent,
    },
}

// =============================================================================
// System Under Test - thin newtype wrapper around TestContext for orphan rules
// =============================================================================

/// E2ESut is a newtype wrapper around TestContext to satisfy orphan rules.
/// This allows implementing StateMachineTest for it.
/// Use Deref/DerefMut for transparent access to TestContext methods.
pub struct E2ESut(pub TestContext);

impl std::ops::Deref for E2ESut {
    type Target = TestContext;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for E2ESut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::fmt::Debug for E2ESut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl E2ESut {
    /// Create a new test environment (not started yet).
    ///
    /// The test environment starts in pre-startup state - use `start_app()` to start the app.
    pub fn new(runtime: Arc<tokio::runtime::Runtime>) -> Result<Self> {
        Ok(Self(TestContext::new(runtime)?))
    }
}

// =============================================================================
// Reference State Machine Implementation
// =============================================================================

impl ReferenceStateMachine for ReferenceState {
    type State = Self;
    type Transition = E2ETransition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(ReferenceState::empty()).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        // Pre-startup phase: generate various pre-startup transitions
        if !state.app_started {
            let pre_startup_file_count = state.documents.len();
            let dir_count = state.pre_startup_directories.len();

            // Weight towards starting app after some setup
            let file_weight = if pre_startup_file_count < 3 { 3 } else { 1 };
            let dir_weight = if dir_count < 10 { 2 } else { 0 }; // Create up to ~10 directories
            let vcs_weight = if !state.git_initialized && !state.jj_initialized {
                1
            } else {
                0
            };

            // Build strategy list dynamically based on state
            let mut strategies: Vec<(u32, BoxedStrategy<E2ETransition>)> = vec![
                (
                    file_weight,
                    generate_org_file_content()
                        .prop_map(|(filename, content)| E2ETransition::WriteOrgFile {
                            filename,
                            content,
                        })
                        .boxed(),
                ),
                (
                    2,
                    // Always wait for OrgAdapter to finish initial file processing.
                    // Without this, UI mutations race with OrgAdapter's initial processing,
                    // creating duplicate Loro documents for the same file path (one from
                    // the mutation, one from OrgAdapter), causing block deletion on sync.
                    Just(E2ETransition::StartApp {
                        wait_for_ready: true,
                        enable_todoist: true,
                    })
                    .boxed(),
                ),
            ];

            if dir_weight > 0 {
                strategies.push((
                    dir_weight,
                    generate_directory_path()
                        .prop_map(|path| E2ETransition::CreateDirectory { path })
                        .boxed(),
                ));
            }

            if vcs_weight > 0 && !state.git_initialized {
                strategies.push((vcs_weight, Just(E2ETransition::GitInit).boxed()));
            }

            if vcs_weight > 0 && !state.jj_initialized {
                strategies.push((vcs_weight, Just(E2ETransition::JjGitInit).boxed()));
            }

            // Add CreateStaleLoro transition if there are org files to corrupt
            // This tests recovery from stale/corrupted .loro files
            let org_filenames: Vec<String> = state.documents.values().map(|f| f.clone()).collect();
            if !org_filenames.is_empty() {
                strategies.push((
                    1, // Lower weight - only occasionally create stale loro files
                    (
                        prop::sample::select(org_filenames),
                        prop::sample::select(vec![
                            LoroCorruptionType::Empty,
                            LoroCorruptionType::Truncated,
                            LoroCorruptionType::InvalidHeader,
                        ]),
                    )
                        .prop_map(|(org_filename, corruption_type)| {
                            E2ETransition::CreateStaleLoro {
                                org_filename,
                                corruption_type,
                            }
                        })
                        .boxed(),
                ));
            }

            return prop::strategy::Union::new_weighted(strategies).boxed();
        }

        // Post-startup phase: generate normal transitions
        let block_ids: Vec<String> = state.blocks.keys().cloned().collect();
        let text_block_ids = state.text_block_ids();
        let doc_uris: Vec<String> = state.documents.keys().cloned().collect();
        let next_id = state.next_id;
        let next_doc_id = state.next_doc_id;

        let mut strategies: Vec<BoxedStrategy<E2ETransition>> = Vec::new();

        strategies.push(
            Just(E2ETransition::CreateDocument {
                file_name: format!("doc_{}.org", next_doc_id),
            })
            .boxed(),
        );

        if !doc_uris.is_empty() {
            strategies.push(
                generate_mutation(
                    next_id,
                    block_ids.clone(),
                    text_block_ids.clone(),
                    doc_uris.clone(),
                )
                .prop_map(|mutation| {
                    E2ETransition::ApplyMutation(MutationEvent {
                        source: MutationSource::UI,
                        mutation,
                    })
                })
                .boxed(),
            );

            strategies.push(
                generate_mutation(
                    next_id,
                    block_ids.clone(),
                    text_block_ids.clone(),
                    doc_uris.clone(),
                )
                .prop_map(|mutation| {
                    E2ETransition::ApplyMutation(MutationEvent {
                        source: MutationSource::External,
                        mutation,
                    })
                })
                .boxed(),
            );
        }

        strategies.push(
            generate_watch_setup()
                .prop_map(|(query_id, prql)| E2ETransition::SetupWatch {
                    query_id,
                    prql,
                    filter: None,
                })
                .boxed(),
        );

        if !state.active_watches.is_empty() {
            let watch_ids: Vec<String> = state.active_watches.keys().cloned().collect();
            strategies.push(
                prop::sample::select(watch_ids)
                    .prop_map(|query_id| E2ETransition::RemoveWatch { query_id })
                    .boxed(),
            );
        }

        strategies.push(
            prop::sample::select(vec![
                "all".to_string(),
                "sidebar".to_string(),
                "main".to_string(),
            ])
            .prop_map(|view_name| E2ETransition::SwitchView { view_name })
            .boxed(),
        );

        let regions = vec![
            "main".to_string(),
            "left_sidebar".to_string(),
            "right_sidebar".to_string(),
        ];

        if !block_ids.is_empty() {
            let block_ids_clone = block_ids.clone();
            strategies.push(
                (
                    prop::sample::select(regions.clone()),
                    prop::sample::select(block_ids_clone),
                )
                    .prop_map(|(region, block_id)| E2ETransition::NavigateFocus {
                        region,
                        block_id,
                    })
                    .boxed(),
            );
        }

        for region in &regions {
            if state.can_go_back(region) {
                let region_clone = region.clone();
                strategies.push(
                    Just(E2ETransition::NavigateBack {
                        region: region_clone,
                    })
                    .boxed(),
                );
            }
        }

        for region in &regions {
            if state.can_go_forward(region) {
                let region_clone = region.clone();
                strategies.push(
                    Just(E2ETransition::NavigateForward {
                        region: region_clone,
                    })
                    .boxed(),
                );
            }
        }

        strategies.push(
            prop::sample::select(regions)
                .prop_map(|region| E2ETransition::NavigateHome { region })
                .boxed(),
        );

        // Add SimulateRestart - only useful if there are blocks to test with
        if !block_ids.is_empty() {
            strategies.push(Just(E2ETransition::SimulateRestart).boxed());
        }

        // Add BulkExternalAdd - tests sync loop by adding multiple blocks via external file
        // Only if there's at least one document to add blocks to
        if !doc_uris.is_empty() {
            let doc_uris_clone = doc_uris.clone();
            strategies.push(
                (
                    prop::sample::select(doc_uris_clone),
                    prop::collection::vec(
                        "[a-zA-Z][a-zA-Z0-9 ]{0,20}",
                        3..=10, // Add 3-10 blocks at once
                    ),
                )
                    .prop_map(move |(doc_uri, contents)| {
                        let blocks: Vec<Block> = contents
                            .into_iter()
                            .enumerate()
                            .map(|(i, content)| {
                                Block::new_text(
                                    format!("bulk-{}-{}", next_id, i),
                                    doc_uri.clone(),
                                    content,
                                )
                            })
                            .collect();
                        E2ETransition::BulkExternalAdd { doc_uri, blocks }
                    })
                    .boxed(),
            );
        }

        // Add ConcurrentSchemaInit - tests database lock bug from concurrent schema initialization
        // Only useful if there are blocks and active watches (IVM operations running)
        if !block_ids.is_empty() && !state.active_watches.is_empty() {
            strategies.push(Just(E2ETransition::ConcurrentSchemaInit).boxed());
        }

        // Add ConcurrentMutations - fires UI + External mutations without sync barriers
        // Only useful when there are documents (for creates) and blocks (for updates/deletes)
        if !doc_uris.is_empty() {
            // Independent mutations (may target different blocks)
            let ui_next_id = next_id;
            let ext_next_id = next_id + 1;
            strategies.push(
                (
                    generate_mutation(
                        ui_next_id,
                        block_ids.clone(),
                        text_block_ids.clone(),
                        doc_uris.clone(),
                    ),
                    generate_mutation(
                        ext_next_id,
                        block_ids.clone(),
                        text_block_ids.clone(),
                        doc_uris.clone(),
                    ),
                )
                    .prop_map(|(ui_mut, ext_mut)| E2ETransition::ConcurrentMutations {
                        ui_mutation: MutationEvent {
                            source: MutationSource::UI,
                            mutation: ui_mut,
                        },
                        external_mutation: MutationEvent {
                            source: MutationSource::External,
                            mutation: ext_mut,
                        },
                    })
                    .boxed(),
            );

            // Same-block concurrent partial edits — the highest-conflict CRDT scenario.
            // UI appends a suffix, External prepends a prefix to the same block.
            // After CRDT merge, the result should contain both:
            //   "{prefix} {original} {suffix}"
            // This makes the merge clearly observable in the test output.
            if !block_ids.is_empty() {
                let blocks_snapshot: Vec<(String, String)> = block_ids
                    .iter()
                    .filter_map(|id| {
                        let block = state.blocks.get(id)?;
                        // Only text blocks — source blocks have different content semantics
                        if block.content_type == CONTENT_TYPE_TEXT {
                            Some((id.clone(), block.content.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();

                if !blocks_snapshot.is_empty() {
                    strategies.push(
                        (
                            prop::sample::select(blocks_snapshot),
                            "[A-Z][a-z]{2,8}", // suffix (UI appends)
                            "[A-Z][a-z]{2,8}", // prefix (External prepends)
                        )
                            .prop_map(|((id, original), suffix, prefix)| {
                                let ui_content = format!("{} {}", original, suffix);
                                let ext_content = format!("{} {}", prefix, original);
                                E2ETransition::ConcurrentMutations {
                                    ui_mutation: MutationEvent {
                                        source: MutationSource::UI,
                                        mutation: Mutation::Update {
                                            entity: "blocks".to_string(),
                                            id: id.clone(),
                                            fields: [(
                                                "content".to_string(),
                                                Value::String(ui_content),
                                            )]
                                            .into_iter()
                                            .collect(),
                                        },
                                    },
                                    external_mutation: MutationEvent {
                                        source: MutationSource::External,
                                        mutation: Mutation::Update {
                                            entity: "blocks".to_string(),
                                            id,
                                            fields: [(
                                                "content".to_string(),
                                                Value::String(ext_content),
                                            )]
                                            .into_iter()
                                            .collect(),
                                        },
                                    },
                                }
                            })
                            .boxed(),
                    );
                }
            }
        }

        prop::strategy::Union::new(strategies).boxed()
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        // WriteOrgFile: always valid (can write files before or after startup)
        // StartApp: only valid when app is not started
        // All other transitions: only valid after startup
        match transition {
            // Pre-startup transitions
            E2ETransition::WriteOrgFile { .. } => !state.app_started, // Only before startup
            E2ETransition::CreateDirectory { .. } => !state.app_started, // Only before startup
            E2ETransition::GitInit => !state.app_started && !state.git_initialized, // Once only
            E2ETransition::JjGitInit => !state.app_started && !state.jj_initialized, // Once only
            E2ETransition::CreateStaleLoro { org_filename, .. } => {
                // Only valid before startup, and org file must exist
                !state.app_started && state.documents.values().any(|f| f == org_filename)
            }
            E2ETransition::StartApp { .. } => !state.app_started, // Only valid when not started

            // Post-startup transitions (require app to be running)
            E2ETransition::CreateDocument { .. } => state.app_started,
            E2ETransition::ApplyMutation(event) => {
                if !state.app_started {
                    return false;
                }
                match &event.mutation {
                    Mutation::Update { id, .. } | Mutation::Delete { id, .. } => {
                        state.blocks.contains_key(id) && !state.layout_block_ids.contains(id)
                    }
                    Mutation::Move {
                        id, new_parent_id, ..
                    } => {
                        state.blocks.contains_key(id)
                            // Don't move source blocks — Org format determines their parent
                            // by heading position, so moves can't round-trip correctly.
                            && state
                                .blocks
                                .get(id)
                                .map_or(false, |b| b.content_type != "source")
                            && state
                                .blocks
                                .get(new_parent_id)
                                .map_or(state.documents.contains_key(new_parent_id), |b| {
                                    b.content_type != "source"
                                })
                    }
                    Mutation::Create { parent_id, .. } => {
                        state.documents.contains_key(parent_id)
                            || state.blocks.get(parent_id).map_or(false, |b| {
                                // Don't create children under source blocks — Org format
                                // can't represent children inside #+begin_src blocks, so
                                // the Org round-trip would flatten the hierarchy.
                                b.content_type != "source"
                            })
                    }
                    Mutation::RestartApp => true,
                }
            }
            E2ETransition::SetupWatch { .. } => state.app_started,
            E2ETransition::RemoveWatch { query_id } => {
                state.app_started && state.active_watches.contains_key(query_id)
            }
            E2ETransition::SwitchView { .. } => state.app_started,
            E2ETransition::NavigateFocus { block_id, .. } => {
                state.app_started && state.blocks.contains_key(block_id)
            }
            E2ETransition::NavigateBack { region } => {
                state.app_started && state.can_go_back(region)
            }
            E2ETransition::NavigateForward { region } => {
                state.app_started && state.can_go_forward(region)
            }
            E2ETransition::NavigateHome { .. } => state.app_started,
            E2ETransition::SimulateRestart => state.app_started && !state.blocks.is_empty(),
            E2ETransition::BulkExternalAdd { doc_uri, .. } => {
                state.app_started && state.documents.contains_key(doc_uri)
            }
            E2ETransition::ConcurrentSchemaInit => {
                state.app_started && !state.blocks.is_empty() && !state.active_watches.is_empty()
            }
            E2ETransition::ConcurrentMutations {
                ui_mutation,
                external_mutation,
            } => {
                if !state.app_started {
                    return false;
                }
                // Both sub-mutations must pass preconditions individually
                let ui_ok =
                    Self::preconditions(state, &E2ETransition::ApplyMutation(ui_mutation.clone()));
                let ext_ok = Self::preconditions(
                    state,
                    &E2ETransition::ApplyMutation(external_mutation.clone()),
                );
                // Reject if both are creates with the same ID (impossible in practice)
                let same_create_id = matches!(
                    (&ui_mutation.mutation, &external_mutation.mutation),
                    (Mutation::Create { id: id1, .. }, Mutation::Create { id: id2, .. }) if id1 == id2
                );
                ui_ok && ext_ok && !same_create_id
            }
        }
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            // Pre-startup transitions
            E2ETransition::WriteOrgFile { filename, content } => {
                use holon::sync::document_entity::DOCUMENT_URI_SCHEME;
                use holon_api::block::CONTENT_TYPE_SOURCE;
                use regex::Regex;

                let doc_uri = format!("{}{}", DOCUMENT_URI_SCHEME, filename);
                state.documents.insert(doc_uri.clone(), filename.clone());

                // Remove old blocks from this document (handles re-writing the same file)
                let old_block_ids: Vec<String> = state
                    .block_documents
                    .iter()
                    .filter(|(_, uri)| *uri == &doc_uri)
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in &old_block_ids {
                    state.blocks.remove(id);
                    state.block_documents.remove(id);
                    state.layout_block_ids.remove(id);
                }

                // Parse block IDs from content and add to reference state
                // This tracks what blocks will exist after the app starts and syncs the file
                let id_regex = Regex::new(r":ID:\s*(\S+)").unwrap();
                let headline_regex = Regex::new(r"^\*+\s+(.+)$").unwrap();
                let src_begin_regex = Regex::new(r"#\+begin_src\s+(\w+)").unwrap();
                let src_end_regex = Regex::new(r"#\+end_src").unwrap();

                let mut current_headline: Option<String> = None;
                let mut current_block_id: Option<String> = None;
                let mut in_source_block = false;
                let mut source_language: Option<String> = None;
                let mut source_content = String::new();
                let mut source_block_index = 0;

                for line in content.lines() {
                    if let Some(caps) = headline_regex.captures(line) {
                        current_headline = Some(caps.get(1).unwrap().as_str().trim().to_string());
                        source_block_index = 0; // Reset source block counter for each headline
                    } else if let Some(caps) = id_regex.captures(line) {
                        let block_id = caps.get(1).unwrap().as_str().to_string();
                        let content = current_headline.clone().unwrap_or_default();
                        let block = Block::new_text(block_id.clone(), doc_uri.clone(), content);
                        state.blocks.insert(block_id.clone(), block);
                        state
                            .block_documents
                            .insert(block_id.clone(), doc_uri.clone());
                        current_block_id = Some(block_id);
                    } else if let Some(caps) = src_begin_regex.captures(line) {
                        in_source_block = true;
                        source_language = Some(caps.get(1).unwrap().as_str().to_string());
                        source_content.clear();
                    } else if src_end_regex.is_match(line) && in_source_block {
                        // Create source block as child of current headline block
                        if let Some(parent_id) = &current_block_id {
                            let src_id = format!("{}::src::{}", parent_id, source_block_index);
                            let src_block = Block {
                                id: src_id.clone(),
                                parent_id: parent_id.clone(),
                                content: source_content.trim().to_string(),
                                content_type: CONTENT_TYPE_SOURCE.to_string(),
                                source_language: source_language.clone(),
                                source_name: None,
                                properties: HashMap::new(),
                                created_at: 0,
                                updated_at: 0,
                            };
                            // Mark PRQL source blocks in index.org as layout blocks
                            if filename == "index.org" && source_language.as_deref() == Some("prql")
                            {
                                state.layout_block_ids.insert(parent_id.clone());
                                state.layout_block_ids.insert(src_id.clone());
                            }
                            state.blocks.insert(src_id.clone(), src_block);
                            state.block_documents.insert(src_id, doc_uri.clone());
                            source_block_index += 1;
                        }
                        in_source_block = false;
                        source_language = None;
                        source_content.clear();
                    } else if in_source_block {
                        if !source_content.is_empty() {
                            source_content.push('\n');
                        }
                        source_content.push_str(line);
                    }
                }
                state.pre_startup_file_count += 1;
            }
            E2ETransition::CreateDirectory { path } => {
                state.pre_startup_directories.push(path.clone());
            }
            E2ETransition::GitInit => {
                state.git_initialized = true;
            }
            E2ETransition::JjGitInit => {
                state.jj_initialized = true;
                state.git_initialized = true; // jj git init also creates .git
            }
            E2ETransition::CreateStaleLoro { .. } => {
                // CreateStaleLoro doesn't change reference state - the blocks from the
                // corresponding org file should still exist after startup. The system
                // should detect the corrupted .loro file and recover from the .org file.
            }
            E2ETransition::StartApp { .. } => {
                state.app_started = true;
            }

            // Post-startup transitions
            E2ETransition::CreateDocument { file_name } => {
                use holon::sync::document_entity::DOCUMENT_URI_SCHEME;
                let doc_uri = format!("{}{}", DOCUMENT_URI_SCHEME, file_name);
                state.documents.insert(doc_uri, file_name.clone());
                state.next_doc_id += 1;
            }
            E2ETransition::ApplyMutation(event) => {
                if let Mutation::Create { id, parent_id, .. } = &event.mutation {
                    let doc_uri = if is_document_uri(parent_id) {
                        parent_id.clone()
                    } else {
                        fn find_doc(block_id: &str, state: &ReferenceState) -> Option<String> {
                            let block = state.blocks.get(block_id)?;
                            if is_document_uri(&block.parent_id) {
                                Some(block.parent_id.clone())
                            } else {
                                find_doc(&block.parent_id, state)
                            }
                        }
                        find_doc(parent_id, &state).unwrap_or_else(|| parent_id.clone())
                    };
                    state.block_documents.insert(id.clone(), doc_uri);
                }

                let mut blocks: Vec<Block> = state.blocks.values().cloned().collect();
                event.mutation.apply_to(&mut blocks);
                state.blocks = blocks.into_iter().map(|b| (b.id.clone(), b)).collect();
                state.next_id += 1;
            }
            E2ETransition::SetupWatch {
                query_id,
                prql,
                filter,
            } => {
                state.active_watches.insert(
                    query_id.clone(),
                    WatchSpec {
                        prql: prql.clone(),
                        filter: filter.clone(),
                    },
                );
            }
            E2ETransition::RemoveWatch { query_id } => {
                state.active_watches.remove(query_id);
            }
            E2ETransition::SwitchView { view_name } => {
                state.current_view = view_name.clone();
            }
            E2ETransition::NavigateFocus { region, block_id } => {
                let history = state
                    .navigation_history
                    .entry(region.clone())
                    .or_insert_with(NavigationHistory::new);

                history.entries.truncate(history.cursor + 1);
                history.entries.push(Some(block_id.clone()));
                history.cursor = history.entries.len() - 1;
            }
            E2ETransition::NavigateBack { region } => {
                if let Some(history) = state.navigation_history.get_mut(region) {
                    if history.cursor > 0 {
                        history.cursor -= 1;
                    }
                }
            }
            E2ETransition::NavigateForward { region } => {
                if let Some(history) = state.navigation_history.get_mut(region) {
                    if history.cursor < history.entries.len() - 1 {
                        history.cursor += 1;
                    }
                }
            }
            E2ETransition::NavigateHome { region } => {
                let history = state
                    .navigation_history
                    .entry(region.clone())
                    .or_insert_with(NavigationHistory::new);

                history.entries.truncate(history.cursor + 1);
                history.entries.push(None);
                history.cursor = history.entries.len() - 1;
            }
            E2ETransition::SimulateRestart => {
                // SimulateRestart doesn't change reference state - blocks should be preserved.
                // The SUT will clear known_state and trigger file re-processing.
            }
            E2ETransition::BulkExternalAdd { blocks, .. } => {
                // Add all blocks to the reference state
                for block in blocks {
                    state.blocks.insert(block.id.clone(), block.clone());
                }
                state.next_id += blocks.len();
            }
            E2ETransition::ConcurrentSchemaInit => {
                // ConcurrentSchemaInit doesn't change reference state - it only tests
                // that the database doesn't get locked when schema init runs concurrently.
            }
            E2ETransition::ConcurrentMutations {
                ui_mutation,
                external_mutation,
            } => {
                // Detect same-block concurrent content updates — use Loro CRDT merge
                // to determine the expected merged content rather than naive LWW.
                let same_block_update = match (&ui_mutation.mutation, &external_mutation.mutation) {
                    (
                        Mutation::Update {
                            id: ui_id,
                            fields: ui_fields,
                            ..
                        },
                        Mutation::Update {
                            id: ext_id,
                            fields: ext_fields,
                            ..
                        },
                    ) if ui_id == ext_id => {
                        let ui_content = ui_fields.get("content").and_then(|v| v.as_string());
                        let ext_content = ext_fields.get("content").and_then(|v| v.as_string());
                        match (ui_content, ext_content) {
                            (Some(ui_c), Some(ext_c)) => {
                                Some((ui_id.clone(), ui_c.to_string(), ext_c.to_string()))
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                };

                if let Some((block_id, ui_content, ext_content)) = same_block_update {
                    // Use Loro CRDT merge to determine expected content
                    let original = state
                        .blocks
                        .get(&block_id)
                        .map(|b| b.content.as_str())
                        .unwrap_or("");
                    let merged = loro_merge_text(original, &ui_content, &ext_content);
                    if let Some(block) = state.blocks.get_mut(&block_id) {
                        block.content = merged;
                    }
                } else {
                    // Non-overlapping mutations: apply both sequentially
                    for event in [ui_mutation, external_mutation] {
                        if let Mutation::Create { id, parent_id, .. } = &event.mutation {
                            let doc_uri = if is_document_uri(parent_id) {
                                parent_id.clone()
                            } else {
                                fn find_doc(
                                    block_id: &str,
                                    state: &ReferenceState,
                                ) -> Option<String> {
                                    let block = state.blocks.get(block_id)?;
                                    if is_document_uri(&block.parent_id) {
                                        Some(block.parent_id.clone())
                                    } else {
                                        find_doc(&block.parent_id, state)
                                    }
                                }
                                find_doc(parent_id, &state).unwrap_or_else(|| parent_id.clone())
                            };
                            state.block_documents.insert(id.clone(), doc_uri);
                        }

                        let mut blocks: Vec<Block> = state.blocks.values().cloned().collect();
                        event.mutation.apply_to(&mut blocks);
                        state.blocks = blocks.into_iter().map(|b| (b.id.clone(), b)).collect();
                    }
                }
                // UI used next_id, External used next_id+1
                state.next_id += 2;
            }
        }
        state
    }
}

/// Generate an org file with some blocks for pre-startup testing.
///
/// Sometimes generates `index.org` which is required for `initial_widget()` to work.
/// When generating `index.org`, includes a root layout block with a PRQL source child
/// (the structure required by `load_root_layout_block()`).
fn generate_org_file_content() -> impl Strategy<Value = (String, String)> {
    use proptest::collection::vec as prop_vec;

    // Strategy for regular org files
    let regular_file = (
        // Filename (e.g., "test_0.org", "notes_1.org")
        "[a-z_]+_[0-9]+\\.org",
        // Number of blocks (1-5)
        prop_vec(
            (
                "[A-Z][a-zA-Z0-9 ]{0,20}", // Headline content
                "[a-z0-9-]+",              // Block ID
            ),
            1..=5,
        ),
    )
        .prop_map(|(filename, blocks)| {
            let mut content = String::new();
            for (headline, id) in blocks {
                content.push_str(&format!(
                    "* {}\n:PROPERTIES:\n:ID: {}\n:END:\n\n",
                    headline, id
                ));
            }
            (filename, content)
        });

    // Strategy for index.org with proper root layout block structure.
    // This is the structure required by initial_widget():
    // - A root headline block (direct child of document)
    // - That headline has a PRQL source block child
    let index_file = (
        "[A-Z][a-zA-Z0-9 ]{0,15}", // Headline content for layout block
        "[a-z0-9-]+",              // Block ID for headline
    )
        .prop_map(|(headline, id)| {
            // Create index.org with a root layout block and PRQL source child
            // The PRQL query uses "from children" which will be resolved relative to the layout block
            let content = format!(
                "* {headline}\n\
                 :PROPERTIES:\n\
                 :ID: {id}\n\
                 :END:\n\
                 \n\
                 #+begin_src prql\n\
                 from children\n\
                 select {{id, content, parent_id}}\n\
                 render (list item_template:(row (text content:this.content)))\n\
                 #+end_src\n\
                 \n",
                headline = headline,
                id = id,
            );
            ("index.org".to_string(), content)
        });

    // Weight towards regular files but sometimes generate index.org
    // This ensures we test both the happy path (index.org exists) and
    // the error path (index.org doesn't exist or is malformed)
    prop_oneof![
        3 => regular_file,
        1 => index_file,
    ]
}

/// Generate a directory path (possibly nested) for pre-startup testing
///
/// This creates paths like "dir_0", "projects/subdir", "deep/nested/path"
/// to simulate real-world directory structures that the sync provider scans.
fn generate_directory_path() -> impl Strategy<Value = String> {
    prop_oneof![
        // Simple directory at root
        "[a-z_]+_[0-9]+".prop_map(|name| name),
        // Nested directory (2 levels)
        ("[a-z_]+", "[a-z_]+_[0-9]+").prop_map(|(parent, child)| format!("{}/{}", parent, child)),
        // Deep nested directory (3 levels)
        ("[a-z_]+", "[a-z_]+", "[a-z_]+_[0-9]+").prop_map(|(a, b, c)| format!("{}/{}/{}", a, b, c)),
    ]
}

fn generate_mutation(
    next_id: usize,
    existing_block_ids: Vec<String>,
    text_block_ids: Vec<String>,
    doc_uris: Vec<String>,
) -> impl Strategy<Value = Mutation> {
    // Text blocks can have document URIs or existing blocks as parents
    let mut valid_parent_ids_for_text = doc_uris.clone();
    valid_parent_ids_for_text.extend(existing_block_ids.iter().cloned());

    // Source blocks can ONLY have text blocks (headlines) as parents.
    // In org-mode, source blocks must be inside headline sections - they can't
    // exist at the document root level or be nested inside other source blocks.
    // Using doc_uri as parent would create an invalid org structure that can't
    // round-trip through parse/serialize, and using a source block as parent
    // creates nested source blocks which org-mode doesn't support.
    let valid_parent_ids_for_source = text_block_ids;

    // Strategy for creating text blocks
    let create_text = (
        "[a-zA-Z][a-zA-Z0-9 ]{0,20}",
        prop::sample::select(valid_parent_ids_for_text),
    )
        .prop_map(move |(content, parent_id)| Mutation::Create {
            entity: "blocks".to_string(),
            id: format!("block-{}", next_id),
            parent_id,
            fields: [
                ("content".to_string(), Value::String(content)),
                (
                    "content_type".to_string(),
                    Value::String(CONTENT_TYPE_TEXT.to_string()),
                ),
            ]
            .into_iter()
            .collect(),
        });

    // If there are no existing blocks (no headlines), we can only create text blocks
    if valid_parent_ids_for_source.is_empty() {
        return create_text.boxed();
    }

    // Strategy for creating source blocks (critical for catching source block corruption bugs)
    let create_source = (
        // No "prql" — only the controlled layout block in index.org should have PRQL.
        // Random PRQL content would be picked up by load_root_layout_block() and fail parsing.
        prop::sample::select(vec!["python", "rust", "sql", "elisp"]),
        "[a-zA-Z_][a-zA-Z0-9_ \n]{5,50}", // Source code content
        prop::sample::select(valid_parent_ids_for_source),
    )
        .prop_map(
            move |(language, source_content, parent_id)| Mutation::Create {
                entity: "blocks".to_string(),
                id: format!("block-{}", next_id),
                parent_id,
                fields: [
                    ("content".to_string(), Value::String(source_content)),
                    (
                        "content_type".to_string(),
                        Value::String(CONTENT_TYPE_SOURCE.to_string()),
                    ),
                    (
                        "source_language".to_string(),
                        Value::String(language.to_string()),
                    ),
                ]
                .into_iter()
                .collect(),
            },
        );

    // Combine text and source block creation (source blocks are less common)
    let create = prop_oneof![3 => create_text, 1 => create_source];

    // Note: We already returned early if existing_block_ids is empty (line 1223-1225),
    // so we can safely use ids for update/delete strategies below.
    let ids = existing_block_ids;
    let update = (
        prop::sample::select(ids.clone()),
        "[a-zA-Z][a-zA-Z0-9 ]{0,20}",
    )
        .prop_map(|(id, new_content)| Mutation::Update {
            entity: "blocks".to_string(),
            id,
            fields: [("content".to_string(), Value::String(new_content))]
                .into_iter()
                .collect(),
        });

    let delete = prop::sample::select(ids).prop_map(|id| Mutation::Delete {
        entity: "blocks".to_string(),
        id,
    });

    prop_oneof![3 => create, 2 => update, 1 => delete].boxed()
}

fn generate_watch_setup() -> impl Strategy<Value = (String, String)> {
    (
        "[a-z]{1,10}",
        prop::sample::select(vec![
            "from blocks | select {id, content, parent_id} | render (list item_template:(row (text content:this.content)))".to_string(),
            "from blocks | select {id, content, parent_id} | filter content != null | render (list item_template:(row (text content:this.content)))".to_string(),
        ]),
    )
        .prop_map(|(id, prql)| (format!("query-{}", id), prql))
}

// =============================================================================
// State Machine Test Implementation
// =============================================================================

impl StateMachineTest for E2ESut {
    type SystemUnderTest = Self;
    type Reference = ReferenceState;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        eprintln!(
            "[init_test] Starting, ref_state has {} blocks, app_started: {}",
            _ref_state.blocks.len(),
            _ref_state.app_started
        );
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let result = E2ESut::new(runtime).unwrap();
        eprintln!("[init_test] Completed (app not started yet - pre-startup phase)");
        result
    }

    fn apply(
        mut state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: E2ETransition,
    ) -> Self::SystemUnderTest {
        eprintln!(
            "[apply] ref_state has {} blocks, transition: {:?}",
            ref_state.blocks.len(),
            std::mem::discriminant(&transition)
        );
        let runtime = state.runtime.clone();
        let ref_state_clone = ref_state.clone();

        runtime.block_on(async {
            match &transition {
                // Pre-startup transitions
                E2ETransition::WriteOrgFile { filename, content } => {
                    eprintln!("[apply] WriteOrgFile: {} ({} bytes)", filename, content.len());
                    state
                        .write_org_file(filename, content)
                        .await
                        .expect("Failed to write org file");
                }

                E2ETransition::CreateDirectory { path } => {
                    eprintln!("[apply] CreateDirectory: {}", path);
                    let full_path = state.temp_dir.path().join(path);
                    tokio::fs::create_dir_all(&full_path)
                        .await
                        .expect("Failed to create directory");
                }

                E2ETransition::GitInit => {
                    eprintln!("[apply] GitInit");
                    let output = tokio::process::Command::new("git")
                        .args(["init"])
                        .current_dir(state.temp_dir.path())
                        .output()
                        .await
                        .expect("Failed to run git init");
                    assert!(output.status.success(), "git init failed: {:?}", output);
                }

                E2ETransition::JjGitInit => {
                    eprintln!("[apply] JjGitInit");
                    let output = tokio::process::Command::new("jj")
                        .args(["git", "init"])
                        .current_dir(state.temp_dir.path())
                        .output()
                        .await
                        .expect("Failed to run jj git init");
                    assert!(output.status.success(), "jj git init failed: {:?}", output);
                }

                E2ETransition::CreateStaleLoro {
                    org_filename,
                    corruption_type,
                } => {
                    eprintln!(
                        "[apply] CreateStaleLoro: {} ({:?})",
                        org_filename, corruption_type
                    );
                    state
                        .write_stale_loro_file(org_filename, *corruption_type)
                        .await
                        .expect("Failed to create stale loro file");
                }

                E2ETransition::StartApp {
                    wait_for_ready,
                    enable_todoist,
                } => {
                    eprintln!(
                        "[apply] StartApp (wait_for_ready={}, enable_todoist={})",
                        wait_for_ready, enable_todoist
                    );
                    state.set_enable_todoist(*enable_todoist);
                    state
                        .start_app(*wait_for_ready)
                        .await
                        .expect("Failed to start app");

                    // Mirror Flutter startup: call initial_widget() after engine ready.
                    // This is the same code path Flutter uses via FrontendSession.
                    //
                    // The "Actor channel closed" bug that previously occurred here was caused
                    // by the DI ServiceProvider being dropped (after create_backend_engine_with_extras
                    // returns), which dropped TursoBackend and its sender. Now that BackendEngine
                    // holds a reference to TursoBackend (_backend_keepalive), the actor survives.
                    let expects_valid_index = ref_state_clone.has_valid_index_org();
                    eprintln!(
                        "[apply] Calling initial_widget() (expects valid index.org: {})",
                        expects_valid_index
                    );

                    let initial_widget_result = state.session().initial_widget().await;

                    match (expects_valid_index, initial_widget_result) {
                        (true, Ok((widget_spec, _stream))) => {
                            eprintln!(
                                "[apply] initial_widget() succeeded with {} rows",
                                widget_spec.data.len()
                            );
                        }
                        (true, Err(e)) => {
                            panic!(
                                "initial_widget() failed but reference state has valid index.org: {}",
                                e
                            );
                        }
                        (false, Ok(_)) => {
                            panic!(
                                "initial_widget() succeeded but reference state has no valid index.org"
                            );
                        }
                        (false, Err(e)) => {
                            eprintln!(
                                "[apply] initial_widget() correctly failed (no valid index.org): {}",
                                e
                            );
                        }
                    }
                }

                // Post-startup transitions
                E2ETransition::CreateDocument { file_name } => {
                    eprintln!("[apply] Creating document: {}", file_name);
                    match state.create_document(file_name).await {
                        Ok(doc_uri) => eprintln!("[apply] Created document: {}", doc_uri),
                        Err(e) => panic!("Failed to create document: {}", e),
                    }
                }

                E2ETransition::ApplyMutation(event) => {
                    eprintln!("[apply] Applying mutation: {:?}", event.mutation);
                    state.apply_mutation(event.clone(), &ref_state_clone).await;
                }

                E2ETransition::SetupWatch { query_id, prql, .. } => {
                    state.setup_watch(query_id, prql).await.expect("Watch setup failed");
                }

                E2ETransition::RemoveWatch { query_id } => {
                    state.remove_watch(query_id);
                }

                E2ETransition::SwitchView { view_name } => {
                    state.switch_view(view_name);
                }

                E2ETransition::NavigateFocus { region, block_id } => {
                    state.navigate_focus(region, block_id).await.expect("Navigation failed");
                }

                E2ETransition::NavigateBack { region } => {
                    state.navigate_back(region).await.expect("Navigation failed");
                }

                E2ETransition::NavigateForward { region } => {
                    state.navigate_forward(region).await.expect("Navigation failed");
                }

                E2ETransition::NavigateHome { region } => {
                    state.navigate_home(region).await.expect("Navigation failed");
                }

                E2ETransition::SimulateRestart => {
                    let expected_count = ref_state_clone.blocks.len();
                    state
                        .simulate_restart(expected_count)
                        .await
                        .expect("SimulateRestart failed");
                }

                E2ETransition::BulkExternalAdd { doc_uri, blocks } => {
                    eprintln!("[apply] BulkExternalAdd: adding {} blocks to {}", blocks.len(), doc_uri);

                    // Get file path for document
                    let file_path = state.documents.get(doc_uri)
                        .expect("Document not found for BulkExternalAdd");

                    // Get all blocks for this document from reference state.
                    // Note: ref_state already includes the new blocks (from apply_reference).
                    let all_blocks: Vec<Block> = ref_state_clone.blocks.values()
                        .filter(|b| block_belongs_to_document(b, &ref_state_clone.blocks.values().cloned().collect::<Vec<_>>(), doc_uri))
                        .cloned()
                        .collect();
                    let existing_count = all_blocks.len().saturating_sub(blocks.len());

                    // Serialize to org file
                    let block_refs: Vec<&Block> = all_blocks.iter().collect();
                    let org_content = serialize_blocks_to_org(&block_refs, doc_uri);

                    eprintln!("[BulkExternalAdd] Writing {} total blocks ({} new) to {:?}",
                        all_blocks.len(), blocks.len(), file_path);
                    tokio::fs::write(file_path, &org_content).await
                        .expect("Failed to write bulk external add");

                    // =========================================================================
                    // FLUTTER STARTUP BUG REPRODUCTION:
                    // Immediately after writing bulk data, spawn concurrent query_and_watch calls
                    // while IVM is still processing the blocks_with_paths materialized view.
                    // This simulates what Flutter does: UI requests reactive queries while
                    // the backend is still processing the initial data sync.
                    // =========================================================================
                    let engine = state.test_ctx().engine();
                    let num_concurrent_watches = 3; // Simulate multiple UI components requesting data
                    let mut watch_tasks = Vec::new();

                    // Timeout for query_and_watch calls.
                    // If the OperationScheduler's mark_available bug is present, these calls
                    // will hang forever because:
                    // 1. query_and_watch creates a materialized view via execute_ddl_with_deps
                    // 2. The DDL requires Schema("blocks") dependency
                    // 3. OperationScheduler checks if "blocks" is in available set - it's NOT
                    // 4. Operation is queued in pending, response_rx.await hangs forever
                    // 5. mark_available() was never called for core tables during DI init
                    let query_timeout = Duration::from_secs(10);

                    for i in 0..num_concurrent_watches {
                        let engine_clone = engine.clone();
                        let task = tokio::spawn(async move {
                            let prql = format!(
                                "from blocks | select {{id, content}} | filter id != \"bulk-race-{}\" | render (list item_template:(row (text content:this.content)))",
                                i
                            );
                            let start = Instant::now();
                            // Use timeout to detect scheduler hangs
                            let result = tokio::time::timeout(
                                query_timeout,
                                engine_clone.query_and_watch(prql.clone(), HashMap::new(), None),
                            ).await;
                            (i, start.elapsed(), prql, result)
                        });
                        watch_tasks.push(task);
                    }

                    // Note: Schema initialization happens during app startup via SchemaRegistry.
                    // We don't need to test concurrent schema init here - the query_and_watch
                    // calls above already test the critical concurrency path.

                    // Check results - database lock/schema change errors indicate the Flutter bug
                    // These manifest as various error messages:
                    // - "database is locked" - SQLite busy timeout expired
                    // - "Database schema changed" - IVM detected concurrent schema modifications
                    // - "Failed to lock connection pool" - Connection pool contention
                    fn is_concurrency_error(error_str: &str) -> bool {
                        error_str.contains("database is locked")
                            || error_str.contains("Database schema changed")
                            || error_str.contains("Failed to lock connection pool")
                    }

                    for task in watch_tasks {
                        match task.await {
                            Ok((i, elapsed, _prql, Ok(Ok(_)))) => {
                                eprintln!(
                                    "[BulkExternalAdd] Concurrent query_and_watch {} succeeded in {:?}",
                                    i, elapsed
                                );
                            }
                            Ok((i, elapsed, prql, Ok(Err(e)))) => {
                                let error_str = format!("{:?}", e);
                                if is_concurrency_error(&error_str) {
                                    panic!(
                                        "FLUTTER STARTUP BUG REPRODUCED: query_and_watch {} failed with concurrency error \
                                         after {:?} while bulk data ({} blocks) was being synced!\n\
                                         This is the exact bug that causes Flutter app to get stuck during startup.\n\
                                         Query: {}\n\
                                         Error: {}",
                                        i, elapsed, blocks.len(), prql, error_str
                                    );
                                } else {
                                    panic!(
                                        "Concurrent query_and_watch {} failed after {:?}: {}\nQuery: {}",
                                        i, elapsed, error_str, prql
                                    );
                                }
                            }
                            Ok((i, elapsed, prql, Err(_timeout))) => {
                                // Timeout occurred - this indicates the scheduler bug
                                panic!(
                                    "SCHEDULER BUG: query_and_watch {} timed out after {:?}!\n\n\
                                     Root cause: OperationScheduler's mark_available() was never called for 'blocks' table.\n\n\
                                     The materialized view creation is stuck in the scheduler's pending queue:\n\
                                     - execute_ddl_with_deps submitted with requires=[Schema(\"blocks\")]\n\
                                     - can_execute() returned false (blocks not in available set)\n\
                                     - Operation queued in pending, response_rx.await blocks forever\n\n\
                                     Query: {}\n\n\
                                     Fix required:\n\
                                     1. Call scheduler_handle.mark_available() for core tables after schema creation in DI\n\
                                     2. Ensure MarkAvailable command calls process_pending_queue() to wake pending ops",
                                    i, elapsed, prql
                                );
                            }
                            Err(e) => {
                                panic!("Query task panicked: {:?}", e);
                            }
                        }
                    }


                    // Poll until file contains expected block count (with timeout)
                    let expected_block_count = all_blocks.len();
                    let file_path_clone = file_path.clone();
                    let start = Instant::now();
                    let timeout = Duration::from_millis(5000);

                    let condition_met = wait_for_file_condition(
                        &file_path_clone,
                        |content| {
                            let text_count = content.matches(":ID:").count();
                            let src_count = content.to_lowercase().matches("#+begin_src").count();
                            text_count + src_count >= expected_block_count
                        },
                        timeout,
                    ).await;

                    let elapsed = start.elapsed();
                    let final_content = tokio::fs::read_to_string(file_path).await
                        .expect("Failed to read file after bulk add");
                    let text_block_count = final_content.matches(":ID:").count();
                    let source_block_count = final_content.to_lowercase().matches("#+begin_src").count();
                    let actual_block_count = text_block_count + source_block_count;

                    if !condition_met || actual_block_count < expected_block_count {
                        panic!(
                            "SYNC LOOP BUG: BulkExternalAdd wrote {} blocks but only {} remain after {:?}!\n\
                             Expected {} blocks total ({} existing + {} new).\n\
                             File content:\n{}",
                            expected_block_count,
                            actual_block_count,
                            elapsed,
                            expected_block_count,
                            existing_count,
                            blocks.len(),
                            final_content
                        );
                    }
                    eprintln!("[BulkExternalAdd] File verified with {} blocks after {:?}", actual_block_count, elapsed);

                    // Now wait for the blocks to sync to the DATABASE
                    // The chain is: File → FileWatcher → OrgAdapter → Loro → EventBus → CacheEventSubscriber → Database
                    let expected_db_count = ref_state_clone.blocks.len();
                    let db_timeout = Duration::from_millis(10000);
                    let db_start = Instant::now();

                    let actual_rows = state.wait_for_block_count(expected_db_count, db_timeout).await;
                    let db_elapsed = db_start.elapsed();

                    if actual_rows.len() == expected_db_count {
                        eprintln!(
                            "[BulkExternalAdd] Database synced ({} blocks) in {:?}",
                            expected_db_count, db_elapsed
                        );
                    } else {
                        eprintln!(
                            "[BulkExternalAdd] WARNING: Database has {} blocks, expected {} after {:?}",
                            actual_rows.len(), expected_db_count, db_elapsed
                        );
                    }

                    // Wait for WriteTracker's external_processing window to expire (3000ms)
                    // This is needed so that subsequent UI mutations can have their changes
                    // written to the Org file by OrgFileWriter.
                    tokio::time::sleep(tokio::time::Duration::from_millis(3100)).await;
                    eprintln!("[BulkExternalAdd] External processing window expired");
                }

                E2ETransition::ConcurrentSchemaInit => {
                    eprintln!("[apply] ConcurrentSchemaInit: testing sequential operations don't cause database lock");

                    // This test verifies that normal sequential operations don't cause
                    // "database is locked" errors. The original bug was:
                    // 1. ensure_navigation_schema() called during DI init
                    // 2. initial_widget() called it AGAIN while IVM was still processing
                    // 3. This caused persistent "database is locked" errors
                    //
                    // After the fix, sequential operations should work without locking issues.
                    let engine = state.engine();

                    // Run several query_and_watch operations SEQUENTIALLY (not concurrently)
                    // Each creates a materialized view, which should work fine when done one at a time
                    for i in 0..3 {
                        let prql = format!(
                            "from blocks | select {{id, content}} | filter id != \"dummy-{}\" | render (list item_template:(row (text content:this.content)))",
                            i
                        );
                        let start = Instant::now();
                        match engine.query_and_watch(prql, HashMap::new(), None).await {
                            Ok(_) => {
                                eprintln!(
                                    "[ConcurrentSchemaInit] query_and_watch {} succeeded in {:?}",
                                    i, start.elapsed()
                                );
                            }
                            Err(e) => {
                                let error_str = format!("{:?}", e);
                                let elapsed = start.elapsed();
                                eprintln!(
                                    "[ConcurrentSchemaInit] query_and_watch {} FAILED in {:?}: {}",
                                    i, elapsed, error_str
                                );
                                // Check for the specific "database is locked" error that indicates
                                // the double-schema-init bug
                                if error_str.contains("database is locked") {
                                    panic!(
                                        "DATABASE LOCK BUG: Sequential query_and_watch {} failed with 'database is locked' after {:?}!\n\
                                         This indicates the ensure_navigation_schema() is still being called multiple times.\n\
                                         Error: {}",
                                        i, elapsed, error_str
                                    );
                                }
                                // Other errors (like "Database schema changed") might occur due to
                                // other concurrent activity and are not necessarily the double-init bug
                            }
                        }
                        // Small delay between operations to ensure IVM has time to settle
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }

                    // Also run some simple queries to verify basic operations work
                    for i in 0..2 {
                        let sql = "SELECT id FROM blocks LIMIT 1".to_string();
                        let start = Instant::now();
                        match engine.execute_query(sql, HashMap::new(), None).await {
                            Ok(_) => {
                                eprintln!(
                                    "[ConcurrentSchemaInit] simple query {} succeeded in {:?}",
                                    i, start.elapsed()
                                );
                            }
                            Err(e) => {
                                let error_str = format!("{:?}", e);
                                let elapsed = start.elapsed();
                                eprintln!(
                                    "[ConcurrentSchemaInit] simple query {} FAILED in {:?}: {}",
                                    i, elapsed, error_str
                                );
                                if error_str.contains("database is locked") {
                                    panic!(
                                        "DATABASE LOCK BUG: Sequential simple query {} failed with 'database is locked' after {:?}!\n\
                                         Error: {}",
                                        i, elapsed, error_str
                                    );
                                }
                            }
                        }
                    }

                    eprintln!("[ConcurrentSchemaInit] All sequential operations completed successfully");

                    eprintln!("[ConcurrentSchemaInit] Test completed successfully");
                }

                E2ETransition::ConcurrentMutations {
                    ui_mutation,
                    external_mutation,
                } => {
                    eprintln!(
                        "[apply] ConcurrentMutations: UI={:?}, External={:?}",
                        ui_mutation.mutation, external_mutation.mutation
                    );
                    state
                        .apply_concurrent_mutations(
                            ui_mutation.clone(),
                            external_mutation.clone(),
                            &ref_state_clone,
                        )
                        .await;
                }
            }

            state.drain_cdc_events().await;
            state.drain_region_cdc_events().await;
        });

        state
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        let runtime = state.runtime.clone();
        eprintln!(
            "[check_invariants] ref_state has {} blocks, app_started: {}",
            ref_state.blocks.len(),
            ref_state.app_started
        );

        // Skip invariant checks if app is not started
        if !ref_state.app_started {
            return;
        }

        runtime.block_on(async {
            // 0. Check for startup errors (Flutter bug: DDL/sync race)
            assert!(
                !state.has_startup_errors(),
                "FLUTTER STARTUP BUG: {} publish errors during startup.\n\
                 This indicates DDL/sync race condition when {} pre-existing files were synced.\n\
                 Files: {:?}",
                state.startup_error_count(),
                state.documents.len(),
                state.documents.keys().collect::<Vec<_>>()
            );

            // 1. Backend storage matches reference model
            let prql = r#"
                from blocks
                select {id, content, content_type, source_language, parent_id, properties}
                render (list item_template:(row (text content:this.content)))
            "#;
            let widget_spec = state
                .test_ctx()
                .query(prql.to_string(), HashMap::new())
                .await
                .expect("Failed to query blocks");

            // Convert query results to Blocks for comparison
            let backend_blocks: Vec<Block> = widget_spec.data
                .into_iter()
                .filter_map(|row: HashMap<String, Value>| {
                    let id = row.get("id")?.as_string()?.to_string();
                    let parent_id = row.get("parent_id")?.as_string()?.to_string();
                    let content = row
                        .get("content")
                        .and_then(|v| v.as_string())
                        .unwrap_or("")
                        .to_string();

                    let mut block = Block::new_text(id, parent_id, content);

                    // Set content_type and source_language (critical for source block round-trip)
                    if let Some(content_type) = row.get("content_type").and_then(|v| v.as_string()) {
                        block.content_type = content_type.to_string();
                    }
                    if let Some(source_language) = row.get("source_language").and_then(|v| v.as_string()) {
                        block.source_language = Some(source_language.to_string());
                    }

                    // Extract properties from the row
                    if let Some(Value::Object(props)) = row.get("properties") {
                        for (k, v) in props {
                            block.properties.insert(k.clone(), v.clone());
                        }
                    }

                    // Also check for top-level org fields (in case they're returned directly)
                    if let Some(task_state) = row
                        .get("task_state")
                        .or_else(|| row.get("TODO"))
                        .and_then(|v| v.as_string())
                    {
                        block.set_task_state(Some(task_state.to_string()));
                    }
                    if let Some(priority) = row
                        .get("priority")
                        .or_else(|| row.get("PRIORITY"))
                        .and_then(|v| v.as_i64())
                    {
                        block.set_priority(Some(priority as i32));
                    }
                    if let Some(tags) = row
                        .get("tags")
                        .or_else(|| row.get("TAGS"))
                        .and_then(|v| v.as_string())
                    {
                        block.set_tags(Some(tags.to_string()));
                    }
                    if let Some(scheduled) = row
                        .get("scheduled")
                        .or_else(|| row.get("SCHEDULED"))
                        .and_then(|v| v.as_string())
                    {
                        block.set_scheduled(Some(scheduled.to_string()));
                    }
                    if let Some(deadline) = row
                        .get("deadline")
                        .or_else(|| row.get("DEADLINE"))
                        .and_then(|v| v.as_string())
                    {
                        block.set_deadline(Some(deadline.to_string()));
                    }

                    Some(block)
                })
                .collect();

            let ref_blocks: Vec<_> = ref_state.blocks.values().cloned().collect();
            assert_blocks_equivalent(&backend_blocks, &ref_blocks, "Backend diverged from reference");

            // 2. Org file matches reference model
            let org_blocks = state
                .parse_org_file_blocks()
                .await
                .expect("Failed to parse Org file");
            let ref_blocks: Vec<_> = ref_state.blocks.values().cloned().collect();
            assert_blocks_equivalent(&org_blocks, &ref_blocks, "Org file diverged from reference");

            // 3. UI model (built from CDC) matches reference — verify all fields, not just IDs
            for (query_id, ui_data) in &state.ui_model {
                if let Some(watch_spec) = ref_state.active_watches.get(query_id) {
                    let expected = ref_state.query_results(watch_spec);

                    let ui_ids: HashSet<&str> = ui_data.iter()
                        .filter_map(|row| row.get("id").and_then(|v| v.as_string()))
                        .collect();
                    let expected_ids: HashSet<&str> = expected.iter()
                        .filter_map(|row| row.get("id").and_then(|v| v.as_string()))
                        .collect();

                    assert_eq!(
                        ui_ids, expected_ids,
                        "CDC UI model for watch '{}' has wrong block IDs.\n\
                         Expected {} blocks: {:?}\n\
                         Got {} blocks: {:?}",
                        query_id, expected_ids.len(), expected_ids,
                        ui_ids.len(), ui_ids
                    );

                    // Verify all fields per block, not just content
                    let fields_to_check = ["content", "content_type", "source_language", "source_name"];
                    for expected_row in &expected {
                        let expected_id = expected_row.get("id").and_then(|v| v.as_string()).unwrap();

                        if let Some(ui_row) = ui_data.iter().find(|r| {
                            r.get("id").and_then(|v| v.as_string()) == Some(expected_id)
                        }) {
                            for field in &fields_to_check {
                                let expected_val = expected_row.get(*field)
                                    .and_then(|v| v.as_string())
                                    .map(|s| s.trim());
                                let actual_val = ui_row.get(*field)
                                    .and_then(|v| v.as_string())
                                    .map(|s| s.trim());
                                assert_eq!(
                                    actual_val, expected_val,
                                    "CDC field '{}' mismatch for block '{}' in watch '{}'",
                                    field, expected_id, query_id
                                );
                            }

                            // parent_id: normalize document URIs before comparing
                            let normalize_parent = |v: Option<&Value>| -> Option<String> {
                                v.and_then(|v| v.as_string()).map(|s| {
                                    if is_document_uri(s) { "__document_root__".to_string() }
                                    else { s.trim().to_string() }
                                })
                            };
                            assert_eq!(
                                normalize_parent(ui_row.get("parent_id")),
                                normalize_parent(expected_row.get("parent_id")),
                                "CDC parent_id mismatch for block '{}' in watch '{}'",
                                expected_id, query_id
                            );
                        }
                    }
                }
            }

            // 4. View selection synchronized
            assert_eq!(state.current_view, ref_state.current_view());

            // 5. Active watches match
            assert_eq!(
                state.active_watches.keys().collect::<HashSet<_>>(),
                ref_state.active_watches.keys().collect::<HashSet<_>>(),
                "Watch sets diverged"
            );

            // 6. Structural integrity: no orphan blocks
            for block in &backend_blocks {
                if is_document_uri(&block.parent_id) {
                    continue;
                }
                assert!(
                    backend_blocks.iter().any(|b| b.id == block.parent_id),
                    "Orphan block: {} has invalid parent {}",
                    block.id,
                    block.parent_id
                );
            }

            // 7. Navigation state verification
            let focus_rows = state
                .engine()
                .execute_query("SELECT region, block_id FROM current_focus".to_string(), HashMap::new(), None)
                .await
                .expect("Failed to query current_focus - this may indicate a Turso IVM bug");

            for (region, history) in &ref_state.navigation_history {
                    let expected_focus = history.current_focus();
                    let actual = focus_rows
                        .iter()
                        .find(|r| r.get("region").and_then(|v| v.as_string()) == Some(region));

                    match (actual, &expected_focus) {
                        (Some(row), Some(expected_id)) => {
                            let actual_block_id = row
                                .get("block_id")
                                .and_then(|v| v.as_string())
                                .map(|s| s.to_string());
                            assert_eq!(
                                actual_block_id.as_deref(),
                                Some(expected_id.as_str()),
                                "Navigation focus mismatch for region '{}': expected {:?}, got {:?}",
                                region,
                                expected_focus,
                                actual_block_id
                            );
                        }
                        (Some(row), None) => {
                            let actual_block_id = row.get("block_id");
                            assert!(
                                actual_block_id.is_none()
                                    || actual_block_id.and_then(|v| v.as_string()).is_none()
                                    || matches!(actual_block_id, Some(Value::Null)),
                                "Navigation focus mismatch for region '{}': expected home (None), got {:?}",
                                region,
                                actual_block_id
                            );
                        }
                        (None, None) => {}
                        (None, Some(expected_id)) => {
                            eprintln!(
                                "[check_invariants] Warning: Region '{}' should have focus on '{}' but not found in DB",
                                region, expected_id
                            );
                        }
                    }
                }

            // 8. Region data verification - main region should show document-level blocks
            // Skip this check if no regions are configured (init_app_frame failed due to no root layout block)
            if !state.region_data.is_empty() {
                // The default main region query filters for blocks where parent_id starts with 'holon-doc://'
                let mut expected_main_block_ids: Vec<String> = ref_state
                    .blocks
                    .values()
                    .filter(|b| is_document_uri(&b.parent_id))
                    .map(|b| b.id.clone())
                    .collect();
                expected_main_block_ids.sort();

                let mut actual_main_block_ids: Vec<String> = state
                    .region_data
                    .get("main")
                    .map(|data| {
                        data.iter()
                            .filter_map(|row| row.get("id").and_then(|v| v.as_string()).map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                actual_main_block_ids.sort();

                assert_eq!(
                    actual_main_block_ids,
                    expected_main_block_ids,
                    "Main region should show document-level blocks (parent_id starts with 'holon-doc://')"
                );
            }

            // 9. Verify blocks with properties HashMap are correctly stored in cache
            // This catches the Value::Object -> NULL serialization bug
            // Collect all (block_id, expected_props, actual_props) for comparison
            #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
            struct PropsCheck {
                block_id: String,
                expected_non_empty: bool,
                actual_is_valid: bool,  // true if not NULL and present
            }

            let mut props_checks: Vec<PropsCheck> = Vec::new();
            for block in &backend_blocks {
                let has_props = !block.properties.is_empty();
                if has_props {
                    let prql = format!(
                        "from blocks | filter id == \"{}\" | select {{id, properties}} | render table",
                        block.id
                    );
                    let query_result = state.test_ctx().query(prql.clone(), HashMap::new()).await;
                    let actual_is_valid = if let Ok(widget_spec) = &query_result {
                        let props_value = widget_spec.data.first().and_then(|row| row.get("properties"));
                        eprintln!(
                            "[props_check] block={}, has_props=true, properties_in_block={:?}, query_result_properties={:?}",
                            block.id,
                            block.properties,
                            props_value
                        );
                        props_value.map(|v| !matches!(v, Value::Null)).unwrap_or(false)
                    } else {
                        eprintln!(
                            "[props_check] block={}, has_props=true, query FAILED: {:?}",
                            block.id,
                            query_result.err()
                        );
                        false
                    };
                    props_checks.push(PropsCheck {
                        block_id: block.id.clone(),
                        expected_non_empty: true,
                        actual_is_valid,
                    });
                }
            }
            props_checks.sort();

            let expected_props: Vec<_> = props_checks
                .iter()
                .map(|c| PropsCheck {
                    block_id: c.block_id.clone(),
                    expected_non_empty: c.expected_non_empty,
                    actual_is_valid: true, // All should be valid
                })
                .collect();

            assert_eq!(
                props_checks,
                expected_props,
                "Block properties should not be NULL in cache (Value::Object serialization)"
            );

            // 10. Loro blocks match Org file blocks (catches orphan blocks from re-parsing)
            // This is critical for detecting the bug where source blocks get new random UUIDs
            // on every parse, creating orphan blocks in Loro.
            let store = state.doc_store().read().await;
            let documents: Vec<_> = store.iter().await;
            drop(store);

            let root = state.temp_dir.path();
            for (file_path, collab_doc) in documents {
                // Skip non-.org files
                if !file_path
                    .extension()
                    .map(|ext| ext == "org")
                    .unwrap_or(false)
                {
                    continue;
                }

                // Get blocks from Loro directly
                let loro_backend = LoroBackend::from_document(collab_doc);
                let loro_blocks: Vec<Block> = loro_backend
                    .get_all_blocks(Traversal::ALL_BUT_ROOT)
                    .await
                    .expect("Failed to get blocks from Loro");

                // Parse blocks from Org file
                let org_content = tokio::fs::read_to_string(&file_path)
                    .await
                    .expect("Failed to read Org file");
                let parse_result = holon_orgmode::parser::parse_org_file(
                    &file_path,
                    &org_content,
                    "root",
                    0,
                    root,
                ).expect("Failed to parse Org file");
                let org_blocks_from_file = parse_result.blocks;

                eprintln!(
                    "[check_invariants] Loro vs Org for {}: Loro has {} blocks, Org has {} blocks",
                    file_path.display(),
                    loro_blocks.len(),
                    org_blocks_from_file.len()
                );

                // If Loro has 0 blocks, it hasn't synced yet - skip check (timing issue)
                // The critical check is: Loro should NOT have MORE blocks than Org (orphan blocks)
                if loro_blocks.is_empty() && !org_blocks_from_file.is_empty() {
                    eprintln!(
                        "[check_invariants] Skipping Loro vs Org check - Loro hasn't synced yet"
                    );
                    continue;
                }

                // Normalize and compare - Loro should match Org exactly (no orphan blocks)
                assert_blocks_equivalent(
                    &loro_blocks,
                    &org_blocks_from_file,
                    &format!("Loro diverged from Org file at {}", file_path.display()),
                );
            }
        });
    }
}

impl E2ESut {
    /// Apply a mutation (UI or External) and wait for sync to complete.
    ///
    /// This method delegates to TestContext methods for the actual work,
    /// keeping the PBT layer thin.
    async fn apply_mutation(&mut self, event: MutationEvent, ref_state: &ReferenceState) {
        match event.source {
            MutationSource::UI => {
                let (entity, op, params) = event.mutation.to_operation();
                eprintln!(
                    "[E2ESut::apply_mutation] About to call execute_op: entity={}, op={}",
                    entity, op
                );
                match self.test_ctx().execute_op(&entity, &op, params).await {
                    Ok(()) => eprintln!("[E2ESut::apply_mutation] execute_op returned Ok"),
                    Err(e) => panic!("Operation {}.{} failed: {:?}", entity, op, e),
                }
            }

            MutationSource::External => {
                eprintln!("[E2ESut::apply_mutation] External mutation - writing to Org file");
                let expected_blocks: Vec<Block> = ref_state.blocks.values().cloned().collect();
                if let Err(e) = self.0.apply_external_mutation(&expected_blocks).await {
                    eprintln!("[E2ESut::apply_mutation] External mutation failed: {:?}", e);
                } else {
                    eprintln!(
                        "[E2ESut::apply_mutation] External mutation wrote to file, waiting for file watcher"
                    );
                }
            }
        }

        // Wait until block count matches expected (with timeout)
        let expected_count = ref_state.blocks.len();
        let timeout = Duration::from_millis(10000);
        let start = Instant::now();

        let actual_rows = self.wait_for_block_count(expected_count, timeout).await;
        let elapsed = start.elapsed();

        if actual_rows.len() == expected_count {
            eprintln!(
                "[E2ESut::apply_mutation] Block count matched ({}) in {:?}",
                expected_count, elapsed
            );
        } else {
            eprintln!(
                "[E2ESut::apply_mutation] Timeout waiting for {} blocks, got {} after {:?}",
                expected_count,
                actual_rows.len(),
                elapsed
            );
        }

        // Spot-check: verify the mutated block has correct data in the DB
        if let Some(block_id) = event.mutation.target_block_id() {
            if let Some(expected_block) = ref_state.blocks.get(&block_id) {
                let prql = format!(
                    "from blocks | filter id == \"{}\" | select {{id, content, content_type, parent_id}} | render table",
                    block_id
                );
                if let Ok(spec) = self.test_ctx().query(prql, HashMap::new()).await {
                    if let Some(row) = spec.data.first() {
                        let actual_content = row
                            .get("content")
                            .and_then(|v| v.as_string())
                            .unwrap_or("")
                            .trim();
                        let expected_content = expected_block.content.trim();
                        assert_eq!(
                            actual_content, expected_content,
                            "Post-mutation spot-check: content mismatch for block '{}'",
                            block_id
                        );
                        let actual_ct = row
                            .get("content_type")
                            .and_then(|v| v.as_string())
                            .unwrap_or("");
                        assert_eq!(
                            actual_ct, &expected_block.content_type,
                            "Post-mutation spot-check: content_type mismatch for block '{}'",
                            block_id
                        );
                    }
                }
            }
        }

        // For External mutations, wait for external_processing to expire
        if matches!(event.source, MutationSource::External) {
            self.0.wait_for_external_processing_expiry().await;
        }

        // For UI mutations, wait for OrgFileWriter to write to Org files, then for write window
        if matches!(event.source, MutationSource::UI) {
            let expected_blocks: Vec<Block> = ref_state.blocks.values().cloned().collect();
            let org_timeout = Duration::from_millis(3000);
            self.0
                .wait_for_org_file_sync(&expected_blocks, org_timeout)
                .await;
            self.0.wait_for_write_window_expiry().await;
        }
    }

    /// Apply two mutations concurrently (UI + External) without sync barriers between them.
    /// Waits only once at the end for final convergence.
    async fn apply_concurrent_mutations(
        &mut self,
        ui_event: MutationEvent,
        _ext_event: MutationEvent,
        ref_state: &ReferenceState,
    ) {
        // Fire UI mutation (no sync wait)
        let (entity, op, params) = ui_event.mutation.to_operation();
        eprintln!(
            "[ConcurrentMutations] Firing UI mutation: {}.{}",
            entity, op
        );
        match self.test_ctx().execute_op(&entity, &op, params).await {
            Ok(()) => {}
            Err(e) => panic!("Concurrent UI mutation {}.{} failed: {:?}", entity, op, e),
        }

        // Fire External mutation immediately (no sync wait)
        eprintln!("[ConcurrentMutations] Firing External mutation (no barrier)");
        let expected_blocks: Vec<Block> = ref_state.blocks.values().cloned().collect();
        if let Err(e) = self.0.apply_external_mutation(&expected_blocks).await {
            eprintln!("[ConcurrentMutations] External mutation failed: {:?}", e);
        }

        // Single sync barrier: wait for final expected block count
        let expected_count = ref_state.blocks.len();
        let timeout = Duration::from_millis(15000);
        let start = Instant::now();

        let actual_rows = self.wait_for_block_count(expected_count, timeout).await;
        let elapsed = start.elapsed();

        if actual_rows.len() == expected_count {
            eprintln!(
                "[ConcurrentMutations] Converged ({} blocks) in {:?}",
                expected_count, elapsed
            );
        } else {
            eprintln!(
                "[ConcurrentMutations] Timeout: expected {} blocks, got {} after {:?}",
                expected_count,
                actual_rows.len(),
                elapsed
            );
        }

        // Wait for both external_processing and write windows to expire
        self.0.wait_for_external_processing_expiry().await;
        let expected_blocks: Vec<Block> = ref_state.blocks.values().cloned().collect();
        let org_timeout = Duration::from_millis(5000);
        self.0
            .wait_for_org_file_sync(&expected_blocks, org_timeout)
            .await;
        self.0.wait_for_write_window_expiry().await;
    }
}

// =============================================================================
// Invariant Checking
// =============================================================================

// Note: `assert_blocks_equivalent` and `block_belongs_to_document` are imported
// from the shared crate. `find_document_for_block` below uses PBT's ReferenceState.

/// Find the document URI that a block belongs to (uses PBT's ReferenceState)
#[allow(dead_code)]
fn find_document_for_block(block_id: &str, ref_state: &ReferenceState) -> Option<String> {
    let block = ref_state.blocks.get(block_id)?;

    if is_document_uri(&block.parent_id) {
        return Some(block.parent_id.clone());
    }

    find_document_for_block(&block.parent_id, ref_state)
}

// Note: `serialize_blocks_to_org` and `serialize_block_recursive` are imported
// from the shared crate.

// =============================================================================
// Main Test
// =============================================================================

proptest_state_machine::prop_state_machine! {
    #![proptest_config(ProptestConfig {
        cases: 8,
        max_shrink_iters: 0,
        .. ProptestConfig::default()
    })]

    #[test]
    fn general_e2e_pbt(sequential 3..20 => E2ESut);
}
