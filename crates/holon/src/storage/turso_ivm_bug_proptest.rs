//! Property-based test to reproduce Turso IVM "Invalid join commit state" bug
//!
//! This test attempts to reproduce the bug through concurrent operations on
//! materialized views with JOINs. The bug occurs when:
//! 1. Multiple matviews with JOINs exist (current_focus, blocks_with_paths)
//! 2. CDC callback is registered
//! 3. Concurrent tokio::spawn tasks perform COMMIT operations
//! 4. JoinOperator state becomes corrupted during IVM cascade
//!
//! Run with:
//!   cargo test -p holon turso_ivm_bug_proptest -- --ignored --nocapture
//!   PROPTEST_CASES=1000 cargo test -p holon turso_ivm_bug_proptest -- --ignored --nocapture

use super::TursoBackend;
use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

/// Tracks whether the IVM bug was triggered
#[derive(Debug, Clone, Default)]
pub struct BugDetection {
    pub panic_message: Arc<Mutex<Option<String>>>,
    pub hang_detected: Arc<AtomicBool>,
}

/// Reference state tracking schema and data
#[derive(Debug)]
pub struct IvmBugReferenceState {
    // Schema state
    pub blocks_table_created: bool,
    pub navigation_tables_created: bool,
    pub current_focus_matview: bool,
    pub blocks_with_paths_matview: bool,
    pub cdc_registered: bool,

    // Data state
    pub blocks: HashMap<String, (String, String)>, // id -> (parent_id, content)
    pub nav_history: Vec<(i64, String, String)>,   // (id, region, block_id)
    pub nav_cursor: HashMap<String, i64>,          // region -> history_id

    // Concurrency tracking
    pub spawned_task_count: usize,

    // Bug detection (shared across clones)
    pub bug_detection: BugDetection,

    // Runtime
    pub handle: tokio::runtime::Handle,
    pub _runtime: Option<Arc<tokio::runtime::Runtime>>,
}

impl Clone for IvmBugReferenceState {
    fn clone(&self) -> Self {
        Self {
            blocks_table_created: self.blocks_table_created,
            navigation_tables_created: self.navigation_tables_created,
            current_focus_matview: self.current_focus_matview,
            blocks_with_paths_matview: self.blocks_with_paths_matview,
            cdc_registered: self.cdc_registered,
            blocks: self.blocks.clone(),
            nav_history: self.nav_history.clone(),
            nav_cursor: self.nav_cursor.clone(),
            spawned_task_count: self.spawned_task_count,
            bug_detection: self.bug_detection.clone(),
            handle: self.handle.clone(),
            _runtime: self._runtime.clone(),
        }
    }
}

impl Default for IvmBugReferenceState {
    fn default() -> Self {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => Self {
                blocks_table_created: false,
                navigation_tables_created: false,
                current_focus_matview: false,
                blocks_with_paths_matview: false,
                cdc_registered: false,
                blocks: HashMap::new(),
                nav_history: Vec::new(),
                nav_cursor: HashMap::new(),
                spawned_task_count: 0,
                bug_detection: BugDetection::default(),
                handle,
                _runtime: None,
            },
            Err(_) => {
                let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
                let handle = runtime.handle().clone();
                Self {
                    blocks_table_created: false,
                    navigation_tables_created: false,
                    current_focus_matview: false,
                    blocks_with_paths_matview: false,
                    cdc_registered: false,
                    blocks: HashMap::new(),
                    nav_history: Vec::new(),
                    nav_cursor: HashMap::new(),
                    spawned_task_count: 0,
                    bug_detection: BugDetection::default(),
                    handle,
                    _runtime: Some(runtime),
                }
            }
        }
    }
}

/// Transitions for the IVM bug state machine
#[derive(Clone, Debug)]
pub enum IvmBugTransition {
    // Schema setup (phase 1)
    CreateBlocksTable,
    CreateNavigationTables,
    CreateCurrentFocusMatview,
    CreateBlocksWithPathsMatview,

    // CDC setup (phase 2)
    RegisterCdcCallback,

    // Data operations (phase 3)
    InsertRootBlock {
        id: String,
        content: String,
    },
    InsertChildBlock {
        id: String,
        parent_id: String,
        content: String,
    },
    InsertNavHistory {
        region: String,
        block_id: String,
    },
    UpdateNavCursor {
        region: String,
    },

    // Concurrent operations (phase 4 - bug triggers here)
    SpawnConcurrentInsert {
        id: String,
        parent_id: String,
        content: String,
    },
    SpawnConcurrentBatch {
        count: usize,
    },

    // Yield to allow spawned tasks to execute
    YieldToTasks,
}

/// Counter for unique temp directories
static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// System under test
pub struct IvmBugTest {
    pub backend: TursoBackend,
    pub cdc_connection: Option<turso::Connection>,
    pub bug_detection: BugDetection,
    #[allow(dead_code)]
    pub temp_dir: TempDir, // Keep alive for the duration of the test
}

impl IvmBugTest {
    pub fn new(handle: &tokio::runtime::Handle, bug_detection: BugDetection) -> Self {
        let test_id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join(format!("test_{}.db", test_id));

        let backend =
            tokio::task::block_in_place(|| handle.block_on(TursoBackend::new(&db_path))).unwrap();
        Self {
            backend,
            cdc_connection: None,
            bug_detection,
            temp_dir,
        }
    }

    /// Execute an operation with panic capture and timeout detection
    async fn execute_with_detection<F, T>(&self, op: F, timeout_ms: u64) -> Result<T, String>
    where
        F: std::future::Future<Output = Result<T, turso::Error>> + Send,
        T: Send,
    {
        let panic_captured = self.bug_detection.panic_message.clone();
        let hang_detected = self.bug_detection.hang_detected.clone();

        // Execute with timeout
        match tokio::time::timeout(Duration::from_millis(timeout_ms), op).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => {
                let msg = e.to_string();
                // Check for IVM bug signatures
                if msg.contains("Invalid join commit state")
                    || msg.contains("PageStack")
                    || msg.contains("BTreeCursor")
                    || msg.contains("assertion failed")
                {
                    *panic_captured.lock().unwrap() = Some(msg.clone());
                }
                Err(msg)
            }
            Err(_timeout) => {
                hang_detected.store(true, Ordering::SeqCst);
                Err("Operation timed out - possible IVM hang".to_string())
            }
        }
    }
}

/// SQL for blocks table
const BLOCKS_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS blocks (
    id TEXT PRIMARY KEY,
    parent_id TEXT NOT NULL,
    content TEXT,
    content_type TEXT DEFAULT 'text',
    source_language TEXT,
    source_name TEXT,
    properties TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
)
"#;

/// SQL for navigation tables
const NAVIGATION_TABLES_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS navigation_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region TEXT NOT NULL,
    block_id TEXT,
    timestamp TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS navigation_cursor (
    region TEXT PRIMARY KEY,
    history_id INTEGER REFERENCES navigation_history(id)
)
"#;

/// SQL for current_focus matview (simple JOIN)
const CURRENT_FOCUS_SQL: &str = r#"
CREATE MATERIALIZED VIEW current_focus AS
SELECT
    nc.region,
    nh.block_id,
    nh.timestamp
FROM navigation_cursor nc
JOIN navigation_history nh ON nc.history_id = nh.id
"#;

/// SQL for blocks_with_paths matview (recursive CTE + INNER JOIN)
const BLOCKS_WITH_PATHS_SQL: &str = r#"
CREATE MATERIALIZED VIEW IF NOT EXISTS blocks_with_paths AS
WITH RECURSIVE paths AS (
    SELECT
        id,
        parent_id,
        content,
        content_type,
        source_language,
        source_name,
        properties,
        created_at,
        updated_at,
        '/' || id as path
    FROM blocks
    WHERE parent_id LIKE 'holon-doc://%'
       OR parent_id = '__no_parent__'

    UNION ALL

    SELECT
        b.id,
        b.parent_id,
        b.content,
        b.content_type,
        b.source_language,
        b.source_name,
        b.properties,
        b.created_at,
        b.updated_at,
        p.path || '/' || b.id as path
    FROM blocks b
    INNER JOIN paths p ON b.parent_id = p.id
)
SELECT * FROM paths
"#;

/// Apply transition to reference state
fn apply_to_reference(state: &mut IvmBugReferenceState, transition: &IvmBugTransition) {
    match transition {
        IvmBugTransition::CreateBlocksTable => {
            state.blocks_table_created = true;
        }
        IvmBugTransition::CreateNavigationTables => {
            state.navigation_tables_created = true;
        }
        IvmBugTransition::CreateCurrentFocusMatview => {
            state.current_focus_matview = true;
        }
        IvmBugTransition::CreateBlocksWithPathsMatview => {
            state.blocks_with_paths_matview = true;
        }
        IvmBugTransition::RegisterCdcCallback => {
            state.cdc_registered = true;
        }
        IvmBugTransition::InsertRootBlock { id, content } => {
            state.blocks.insert(
                id.clone(),
                ("holon-doc://test".to_string(), content.clone()),
            );
        }
        IvmBugTransition::InsertChildBlock {
            id,
            parent_id,
            content,
        } => {
            state
                .blocks
                .insert(id.clone(), (parent_id.clone(), content.clone()));
        }
        IvmBugTransition::InsertNavHistory { region, block_id } => {
            let id = state.nav_history.len() as i64 + 1;
            state
                .nav_history
                .push((id, region.clone(), block_id.clone()));
        }
        IvmBugTransition::UpdateNavCursor { region } => {
            if let Some((id, _, _)) = state.nav_history.iter().rev().find(|(_, r, _)| r == region) {
                state.nav_cursor.insert(region.clone(), *id);
            }
        }
        IvmBugTransition::SpawnConcurrentInsert {
            id,
            parent_id,
            content,
        } => {
            state
                .blocks
                .insert(id.clone(), (parent_id.clone(), content.clone()));
            state.spawned_task_count += 1;
        }
        IvmBugTransition::SpawnConcurrentBatch { count } => {
            state.spawned_task_count += count;
        }
        IvmBugTransition::YieldToTasks => {
            // No-op for reference state
        }
    }
}

/// Apply transition to the system under test
async fn apply_to_turso(
    test: &mut IvmBugTest,
    transition: &IvmBugTransition,
    handle: &tokio::runtime::Handle,
) -> Result<(), String> {
    let conn = test.backend.get_connection().map_err(|e| e.to_string())?;

    match transition {
        IvmBugTransition::CreateBlocksTable => {
            test.execute_with_detection(conn.execute(BLOCKS_TABLE_SQL, ()), 5000)
                .await?;
        }
        IvmBugTransition::CreateNavigationTables => {
            for stmt in NAVIGATION_TABLES_SQL.split(';') {
                let trimmed = stmt.trim();
                if !trimmed.is_empty() {
                    test.execute_with_detection(conn.execute(trimmed, ()), 5000)
                        .await?;
                }
            }
        }
        IvmBugTransition::CreateCurrentFocusMatview => {
            test.execute_with_detection(conn.execute(CURRENT_FOCUS_SQL, ()), 5000)
                .await?;
        }
        IvmBugTransition::CreateBlocksWithPathsMatview => {
            test.execute_with_detection(conn.execute(BLOCKS_WITH_PATHS_SQL, ()), 5000)
                .await?;
        }
        IvmBugTransition::RegisterCdcCallback => {
            // Get a raw connection and enable CDC
            let cdc_conn = test
                .backend
                .get_raw_connection()
                .map_err(|e| e.to_string())?;
            cdc_conn
                .execute("PRAGMA unstable_capture_data_changes_conn('full')", ())
                .await
                .map_err(|e| e.to_string())?;
            test.cdc_connection = Some(cdc_conn);
        }
        IvmBugTransition::InsertRootBlock { id, content } => {
            let sql = format!(
                "INSERT INTO blocks (id, parent_id, content) VALUES ('{}', 'holon-doc://test', '{}')",
                id.replace('\'', "''"),
                content.replace('\'', "''")
            );
            let conn_to_use = test.cdc_connection.as_ref().unwrap_or(&conn);
            test.execute_with_detection(conn_to_use.execute(&sql, ()), 5000)
                .await?;
        }
        IvmBugTransition::InsertChildBlock {
            id,
            parent_id,
            content,
        } => {
            let sql = format!(
                "INSERT INTO blocks (id, parent_id, content) VALUES ('{}', '{}', '{}')",
                id.replace('\'', "''"),
                parent_id.replace('\'', "''"),
                content.replace('\'', "''")
            );
            let conn_to_use = test.cdc_connection.as_ref().unwrap_or(&conn);
            test.execute_with_detection(conn_to_use.execute(&sql, ()), 5000)
                .await?;
        }
        IvmBugTransition::InsertNavHistory { region, block_id } => {
            let sql = format!(
                "INSERT INTO navigation_history (region, block_id) VALUES ('{}', '{}')",
                region.replace('\'', "''"),
                block_id.replace('\'', "''")
            );
            let conn_to_use = test.cdc_connection.as_ref().unwrap_or(&conn);
            test.execute_with_detection(conn_to_use.execute(&sql, ()), 5000)
                .await?;
        }
        IvmBugTransition::UpdateNavCursor { region } => {
            // Get the latest history_id for this region
            let sql = format!(
                "INSERT OR REPLACE INTO navigation_cursor (region, history_id)
                 SELECT '{}', MAX(id) FROM navigation_history WHERE region = '{}'",
                region.replace('\'', "''"),
                region.replace('\'', "''")
            );
            let conn_to_use = test.cdc_connection.as_ref().unwrap_or(&conn);
            test.execute_with_detection(conn_to_use.execute(&sql, ()), 5000)
                .await?;
        }
        IvmBugTransition::SpawnConcurrentInsert {
            id,
            parent_id,
            content,
        } => {
            // Simulate concurrent insert using a separate connection with transaction
            // This mimics the CacheEventSubscriber pattern: BEGIN -> INSERT -> COMMIT
            let conn = test
                .backend
                .get_raw_connection()
                .map_err(|e| e.to_string())?;

            conn.execute("BEGIN IMMEDIATE TRANSACTION", ())
                .await
                .map_err(|e| {
                    let msg = e.to_string();
                    if msg.contains("Invalid join commit state") {
                        *test.bug_detection.panic_message.lock().unwrap() = Some(msg.clone());
                    }
                    msg
                })?;

            let sql = format!(
                "INSERT INTO blocks (id, parent_id, content) VALUES ('{}', '{}', '{}')",
                id.replace('\'', "''"),
                parent_id.replace('\'', "''"),
                content.replace('\'', "''")
            );
            conn.execute(&sql, ()).await.map_err(|e| {
                let msg = e.to_string();
                if msg.contains("Invalid join commit state") {
                    *test.bug_detection.panic_message.lock().unwrap() = Some(msg.clone());
                }
                msg
            })?;

            // This COMMIT triggers IVM update on blocks_with_paths - this is where bug occurs
            conn.execute("COMMIT", ()).await.map_err(|e| {
                let msg = e.to_string();
                if msg.contains("Invalid join commit state")
                    || msg.contains("PageStack")
                    || msg.contains("assertion failed")
                {
                    *test.bug_detection.panic_message.lock().unwrap() = Some(msg.clone());
                }
                msg
            })?;
        }
        IvmBugTransition::SpawnConcurrentBatch { count } => {
            // Execute multiple inserts in sequence using separate connections
            for i in 0..*count {
                let conn = test
                    .backend
                    .get_raw_connection()
                    .map_err(|e| e.to_string())?;
                let block_id = format!("batch_block_{}", i);

                conn.execute("BEGIN IMMEDIATE TRANSACTION", ())
                    .await
                    .map_err(|e| e.to_string())?;

                let sql = format!(
                    "INSERT INTO blocks (id, parent_id, content) VALUES ('{}', 'holon-doc://batch', 'batch content')",
                    block_id
                );
                conn.execute(&sql, ()).await.map_err(|e| e.to_string())?;

                // This COMMIT triggers IVM - potential bug location
                conn.execute("COMMIT", ()).await.map_err(|e| {
                    let msg = e.to_string();
                    if msg.contains("Invalid join commit state")
                        || msg.contains("PageStack")
                        || msg.contains("assertion failed")
                    {
                        *test.bug_detection.panic_message.lock().unwrap() = Some(msg.clone());
                    }
                    msg
                })?;
            }
        }
        IvmBugTransition::YieldToTasks => {
            // No-op since we're now using synchronous operations
            // Keep this transition for state machine compatibility
        }
    }

    Ok(())
}

/// Check preconditions for transitions
fn check_preconditions(state: &IvmBugReferenceState, transition: &IvmBugTransition) -> bool {
    match transition {
        IvmBugTransition::CreateBlocksTable => !state.blocks_table_created,
        IvmBugTransition::CreateNavigationTables => !state.navigation_tables_created,
        IvmBugTransition::CreateCurrentFocusMatview => {
            state.navigation_tables_created && !state.current_focus_matview
        }
        IvmBugTransition::CreateBlocksWithPathsMatview => {
            state.blocks_table_created && !state.blocks_with_paths_matview
        }
        IvmBugTransition::RegisterCdcCallback => {
            // Only register CDC after matviews exist
            state.current_focus_matview && state.blocks_with_paths_matview && !state.cdc_registered
        }
        IvmBugTransition::InsertRootBlock { id, .. } => {
            state.blocks_table_created && !state.blocks.contains_key(id)
        }
        IvmBugTransition::InsertChildBlock { id, parent_id, .. } => {
            state.blocks_table_created
                && !state.blocks.contains_key(id)
                && (state.blocks.contains_key(parent_id) || parent_id.starts_with("holon-doc://"))
        }
        IvmBugTransition::InsertNavHistory { .. } => state.navigation_tables_created,
        IvmBugTransition::UpdateNavCursor { region } => {
            state.navigation_tables_created && state.nav_history.iter().any(|(_, r, _)| r == region)
        }
        IvmBugTransition::SpawnConcurrentInsert { id, parent_id, .. } => {
            // Only spawn concurrent tasks after CDC is registered (bug trigger condition)
            state.cdc_registered
                && state.blocks_table_created
                && !state.blocks.contains_key(id)
                && (state.blocks.contains_key(parent_id) || parent_id.starts_with("holon-doc://"))
        }
        IvmBugTransition::SpawnConcurrentBatch { .. } => {
            state.cdc_registered && state.blocks_table_created
        }
        IvmBugTransition::YieldToTasks => state.spawned_task_count > 0,
    }
}

/// Generate transitions based on current state
fn generate_transitions(state: &IvmBugReferenceState) -> BoxedStrategy<IvmBugTransition> {
    // Phase 1: Schema setup (must happen first in order)
    if !state.blocks_table_created {
        return Just(IvmBugTransition::CreateBlocksTable).boxed();
    }
    if !state.navigation_tables_created {
        return Just(IvmBugTransition::CreateNavigationTables).boxed();
    }

    // Phase 2: Matview creation
    let mut strategies: Vec<(u32, BoxedStrategy<IvmBugTransition>)> = vec![];

    if !state.current_focus_matview {
        strategies.push((
            40,
            Just(IvmBugTransition::CreateCurrentFocusMatview).boxed(),
        ));
    }
    if !state.blocks_with_paths_matview {
        strategies.push((
            40,
            Just(IvmBugTransition::CreateBlocksWithPathsMatview).boxed(),
        ));
    }

    // Phase 3: CDC registration (after matviews)
    if state.current_focus_matview && state.blocks_with_paths_matview && !state.cdc_registered {
        strategies.push((50, Just(IvmBugTransition::RegisterCdcCallback).boxed()));
    }

    // Phase 4: Data operations
    if state.blocks_table_created {
        // Root block inserts
        strategies.push((
            15,
            ("[a-z]{3,8}", "[a-z ]{5,20}")
                .prop_map(|(id, content)| IvmBugTransition::InsertRootBlock {
                    id: format!("root_{}", id),
                    content,
                })
                .boxed(),
        ));

        // Child block inserts (if we have parent blocks)
        if !state.blocks.is_empty() {
            let parent_ids: Vec<String> = state.blocks.keys().cloned().collect();
            strategies.push((
                20,
                (
                    "[a-z]{3,8}",
                    prop::sample::select(parent_ids),
                    "[a-z ]{5,20}",
                )
                    .prop_map(
                        |(id, parent_id, content)| IvmBugTransition::InsertChildBlock {
                            id: format!("child_{}", id),
                            parent_id,
                            content,
                        },
                    )
                    .boxed(),
            ));
        }
    }

    // Navigation operations
    if state.navigation_tables_created {
        let block_ids: Vec<String> = state.blocks.keys().cloned().collect();
        if !block_ids.is_empty() {
            strategies.push((
                10,
                (
                    prop::sample::select(vec!["main".to_string(), "sidebar".to_string()]),
                    prop::sample::select(block_ids),
                )
                    .prop_map(|(region, block_id)| IvmBugTransition::InsertNavHistory {
                        region,
                        block_id,
                    })
                    .boxed(),
            ));
        }

        let regions_with_history: Vec<String> = state
            .nav_history
            .iter()
            .map(|(_, r, _)| r.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if !regions_with_history.is_empty() {
            strategies.push((
                8,
                prop::sample::select(regions_with_history)
                    .prop_map(|region| IvmBugTransition::UpdateNavCursor { region })
                    .boxed(),
            ));
        }
    }

    // Phase 5: Concurrent operations (BUG TRIGGER ZONE)
    if state.cdc_registered {
        // Spawn concurrent inserts - HIGH WEIGHT (this is where bug triggers)
        let parent_ids: Vec<String> = if state.blocks.is_empty() {
            vec!["holon-doc://test".to_string()]
        } else {
            state.blocks.keys().cloned().collect()
        };

        strategies.push((
            40,
            (
                "[a-z]{3,8}",
                prop::sample::select(parent_ids),
                "[a-z ]{5,20}",
            )
                .prop_map(
                    |(id, parent_id, content)| IvmBugTransition::SpawnConcurrentInsert {
                        id: format!("concurrent_{}", id),
                        parent_id,
                        content,
                    },
                )
                .boxed(),
        ));

        // Spawn batch of concurrent operations
        strategies.push((
            30,
            (2..5usize)
                .prop_map(|count| IvmBugTransition::SpawnConcurrentBatch { count })
                .boxed(),
        ));
    }

    // Yield to spawned tasks
    if state.spawned_task_count > 0 {
        strategies.push((25, Just(IvmBugTransition::YieldToTasks).boxed()));
    }

    if strategies.is_empty() {
        // Fallback: insert a root block
        return Just(IvmBugTransition::InsertRootBlock {
            id: "fallback".to_string(),
            content: "fallback content".to_string(),
        })
        .boxed();
    }

    prop::strategy::Union::new_weighted(strategies).boxed()
}

impl ReferenceStateMachine for IvmBugReferenceState {
    type State = Self;
    type Transition = IvmBugTransition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(IvmBugReferenceState::default()).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        generate_transitions(state)
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        check_preconditions(state, transition)
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        apply_to_reference(&mut state, transition);
        state
    }
}

impl StateMachineTest for IvmBugTest {
    type SystemUnderTest = Self;
    type Reference = IvmBugReferenceState;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        IvmBugTest::new(&ref_state.handle, ref_state.bug_detection.clone())
    }

    fn apply(
        mut state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        let result = tokio::task::block_in_place(|| {
            ref_state.handle.block_on(async {
                apply_to_turso(&mut state, &transition, &ref_state.handle).await
            })
        });

        // Check if bug was triggered
        if let Err(e) = result {
            if e.contains("Invalid join commit state")
                || e.contains("PageStack")
                || e.contains("assertion failed")
            {
                *state.bug_detection.panic_message.lock().unwrap() = Some(e);
            }
        }

        state
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        // Check if the bug was triggered
        if let Some(msg) = state.bug_detection.panic_message.lock().unwrap().as_ref() {
            eprintln!("\n=== TURSO IVM BUG TRIGGERED ===");
            eprintln!("Panic message: {}", msg);
            eprintln!("State at failure:");
            eprintln!(
                "  - blocks_with_paths matview: {}",
                ref_state.blocks_with_paths_matview
            );
            eprintln!(
                "  - current_focus matview: {}",
                ref_state.current_focus_matview
            );
            eprintln!("  - CDC registered: {}", ref_state.cdc_registered);
            eprintln!("  - Blocks count: {}", ref_state.blocks.len());
            eprintln!("  - Spawned tasks: {}", ref_state.spawned_task_count);
            eprintln!("================================\n");

            panic!(
                "Turso IVM bug reproduced! Message: {}. \
                 Save this seed to report to Turso team.",
                msg
            );
        }

        if state.bug_detection.hang_detected.load(Ordering::SeqCst) {
            eprintln!("\n=== TURSO IVM HANG DETECTED ===");
            eprintln!("Operation timed out - likely deadlock in JoinOperator");
            eprintln!("================================\n");

            panic!("Turso IVM bug reproduced: operation hang detected");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: proptest shrinking doesn't work for this bug because:
    // 1. The Turso bug causes an uncatchable panic (process abort)
    // 2. Fork mode doesn't work with tokio async runtime
    //
    // Use the deterministic test below for reliable reproduction.
    // The state machine test is kept for exploratory testing with different sequences.
    proptest_state_machine::prop_state_machine! {
        #![proptest_config(ProptestConfig {
            cases: 50,
            failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("regressions"))),
            timeout: 30000,
            max_shrink_iters: 0, // Disable shrinking - crashes can't be caught
            verbose: 1,
            .. ProptestConfig::default()
        })]

        #[test]
        #[ignore] // Run manually: cargo test -p holon turso_ivm_bug_proptest -- --ignored
        fn test_turso_ivm_concurrent_bug(sequential 1..30 => IvmBugTest);
    }

    /// MINIMAL REPRODUCER for Turso IVM btree assertion bug
    ///
    /// This test reliably triggers:
    ///   panicked at turso/core/storage/btree.rs:6290:9:
    ///   assertion failed: self.current_page >= 0
    ///
    /// Minimal reproduction sequence:
    /// 1. Create `blocks` table
    /// 2. Create `navigation_history` and `navigation_cursor` tables
    /// 3. Create `current_focus` materialized view (JOIN)
    /// 4. Create `blocks_with_paths` materialized view (recursive CTE + INNER JOIN)
    /// 5. Insert root block with parent_id = 'holon-doc://...'
    /// 6. Insert navigation history entry
    /// 7. Insert navigation cursor entry
    /// 8. Insert child block -> CRASH during IVM update of blocks_with_paths
    ///
    /// Run with: cargo test -p holon --lib turso::turso_ivm_bug_proptest::tests::test_deterministic_bug_sequence -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_deterministic_bug_sequence() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("deterministic_test.db");
        let backend = TursoBackend::new(&db_path).await.unwrap();
        let conn = backend.get_connection().unwrap();

        // Step 1: Create tables
        conn.execute(BLOCKS_TABLE_SQL, ()).await.unwrap();
        for stmt in NAVIGATION_TABLES_SQL.split(';') {
            let trimmed = stmt.trim();
            if !trimmed.is_empty() {
                conn.execute(trimmed, ()).await.unwrap();
            }
        }

        // Step 2: Create matviews with JOINs
        conn.execute(CURRENT_FOCUS_SQL, ()).await.unwrap();
        conn.execute(BLOCKS_WITH_PATHS_SQL, ()).await.unwrap();

        // Step 3: Enable CDC
        let cdc_conn = backend.get_raw_connection().unwrap();
        cdc_conn
            .execute("PRAGMA unstable_capture_data_changes_conn('full')", ())
            .await
            .unwrap();

        // Step 4: Insert some initial data
        cdc_conn
            .execute(
                "INSERT INTO blocks (id, parent_id, content) VALUES ('root1', 'holon-doc://test', 'root block')",
                (),
            )
            .await
            .unwrap();

        cdc_conn
            .execute(
                "INSERT INTO navigation_history (region, block_id) VALUES ('main', 'root1')",
                (),
            )
            .await
            .unwrap();

        cdc_conn
            .execute(
                "INSERT INTO navigation_cursor (region, history_id) VALUES ('main', 1)",
                (),
            )
            .await
            .unwrap();

        // Step 5: Run inserts using separate connections (simulating CacheEventSubscriber)
        // Note: This uses sequential inserts on separate connections to exercise
        // the IVM update path that triggers the bug in real usage.
        for i in 0..5 {
            let conn = backend.get_raw_connection().unwrap();
            conn.execute("BEGIN IMMEDIATE TRANSACTION", ())
                .await
                .unwrap();
            let sql = format!(
                "INSERT INTO blocks (id, parent_id, content) VALUES ('concurrent_{}', 'root1', 'concurrent block')",
                i
            );
            conn.execute(&sql, ()).await.unwrap();
            // This COMMIT triggers IVM - potential bug location
            conn.execute("COMMIT", ()).await.unwrap();
        }

        eprintln!("Deterministic sequence completed without triggering bug");
    }
}
