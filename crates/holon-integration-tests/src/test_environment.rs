//! Test environment for integration tests
//!
//! Provides a high-level wrapper around BackendEngine for testing.
//! Uses FrontendSession from holon-frontend to ensure identical initialization
//! path with production frontends (Flutter, TUI, etc.).
//!
//! ## Pre-Startup Testing
//!
//! TestEnvironment supports two phases:
//! 1. **Pre-startup** (`session: None`): Can write org files to temp_dir before the app starts
//! 2. **Running** (`session: Some`): Full application functionality
//!
//! This enables testing scenarios where files exist before the application starts,
//! reproducing the Flutter startup bug where DDL operations race with sync of existing files.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use tempfile::TempDir;
use tokio::sync::RwLock;

use crate::{
    apply_cdc_event_to_vec, block_belongs_to_document, serialize_blocks_to_org,
    wait_for_file_condition,
};

use holon::api::backend_engine::QueryContext;
use holon::api::{BackendEngine, RowChangeStream};
use holon::sync::LoroDocumentStore;
use holon::sync::document_entity::DOCUMENT_URI_SCHEME;
use holon::sync::event_bus::PublishErrorTracker;
use holon::testing::e2e_test_helpers::E2ETestContext;
use holon_api::block::Block;
use holon_api::{Value, WidgetSpec};
use holon_frontend::{DiResolver, FrontendConfig, FrontendSession};

/// Types of corruption for stale .loro files (for testing recovery)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoroCorruptionType {
    /// Empty file (0 bytes)
    Empty,
    /// File with partial/truncated Loro header
    Truncated,
    /// File with invalid magic bytes
    InvalidHeader,
}

/// Extra services resolved from DI for test assertions
pub struct TestExtras {
    pub doc_store: Arc<RwLock<LoroDocumentStore>>,
}

/// Test environment with optional running application.
///
/// Supports two phases:
/// - Pre-startup (session: None): Can write org files, loro files to temp_dir
/// - Running (session: Some): Full application functionality
pub struct TestEnvironment {
    /// Temp directory for Org files
    pub temp_dir: TempDir,

    /// Runtime for async operations
    pub runtime: Arc<tokio::runtime::Runtime>,

    /// The running application (None before start_app())
    session: Option<FrontendSession<TestExtras>>,

    /// The E2ETestContext for operations (wraps BackendEngine) - only valid after start_app()
    ctx: Option<E2ETestContext>,

    /// Created documents (doc_uri -> file path)
    pub documents: HashMap<String, PathBuf>,

    /// Active CDC watches (query_id -> stream)
    pub active_watches: HashMap<String, RowChangeStream>,

    /// UI model built from CDC events (query_id -> rows)
    pub ui_model: HashMap<String, Vec<HashMap<String, Value>>>,

    /// Current view filter
    pub current_view: String,

    /// Region CDC streams from AppFrame (region_id -> stream)
    pub region_streams: HashMap<String, RowChangeStream>,

    /// Region data built from CDC events (region_id -> rows)
    pub region_data: HashMap<String, Vec<HashMap<String, Value>>>,

    /// Whether to enable Todoist fake mode (adds concurrent DDL during startup)
    enable_todoist: bool,
}

impl std::fmt::Debug for TestEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestEnvironment")
            .field("documents", &self.documents)
            .field("temp_dir", &self.temp_dir.path())
            .field("is_running", &self.session.is_some())
            .finish_non_exhaustive()
    }
}

/// Builder for TestEnvironment that allows pre-populating org files before engine initialization.
///
/// This is critical for reproducing the Flutter startup bug where:
/// 1. Org files already exist when the app starts
/// 2. OrgModeSyncProvider scans and emits ALL existing files/blocks
/// 3. preload_startup_views runs DDL concurrently with event publishing
/// 4. Events are dropped due to "Database schema changed" errors
///
/// # Example
/// ```rust,ignore
/// let env = TestEnvironmentBuilder::new()
///     .with_org_file("test.org", "* Headline 1\n:PROPERTIES:\n:ID: block-1\n:END:\n")
///     .with_org_file("test2.org", "* Headline 2\n:PROPERTIES:\n:ID: block-2\n:END:\n")
///     .wait_for_file_watcher(false)  // Don't wait - capture the race
///     .build(runtime)
///     .await?;
///
/// // Check for startup errors
/// assert!(!env.has_startup_errors(), "Startup should not have errors");
/// ```
pub struct TestEnvironmentBuilder {
    /// Pre-populated org files (filename -> content)
    org_files: Vec<(String, String)>,
    /// Whether to wait for file watcher to be ready before returning
    wait_for_file_watcher: bool,
    /// Additional delay after file watcher ready (ms)
    settle_delay_ms: u64,
    /// Enable Todoist with fake client (for testing DDL race conditions)
    enable_todoist_fake: bool,
}

impl TestEnvironmentBuilder {
    /// Create a new TestEnvironmentBuilder
    pub fn new() -> Self {
        Self {
            org_files: Vec::new(),
            wait_for_file_watcher: true,
            settle_delay_ms: 100,
            enable_todoist_fake: false,
        }
    }

    /// Add an org file to be created BEFORE engine initialization
    ///
    /// The file will exist when OrgModeSyncProvider starts scanning,
    /// which triggers the sync/DDL race condition.
    pub fn with_org_file(
        mut self,
        filename: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        self.org_files.push((filename.into(), content.into()));
        self
    }

    /// Set whether to wait for file watcher to be ready before returning
    ///
    /// Set to `false` to capture the race condition where events are published
    /// while preload_views is still running DDL.
    pub fn wait_for_file_watcher(mut self, wait: bool) -> Self {
        self.wait_for_file_watcher = wait;
        self
    }

    /// Set the delay after file watcher is ready (in milliseconds)
    ///
    /// Only applies if `wait_for_file_watcher` is true.
    pub fn settle_delay_ms(mut self, ms: u64) -> Self {
        self.settle_delay_ms = ms;
        self
    }

    /// Enable Todoist with a fake in-memory client.
    ///
    /// This enables the same DI path as production (DDL for `todoist_tasks` and
    /// `todoist_projects` tables, same caches, streams, and event adapters),
    /// but uses a fake client instead of making real API calls.
    ///
    /// This is critical for testing the DDL race condition where Todoist tables
    /// are created concurrently with OrgMode sync events.
    pub fn with_todoist_fake(mut self) -> Self {
        self.enable_todoist_fake = true;
        self
    }

    /// Build the TestEnvironment, creating any pre-populated org files first
    ///
    /// Uses FrontendSession to ensure identical initialization path with production frontends.
    /// This simulates the Flutter scenario where files exist before the app starts.
    pub async fn build(self, runtime: Arc<tokio::runtime::Runtime>) -> Result<TestEnvironment> {
        let temp_dir =
            TempDir::new().map_err(|e| anyhow::anyhow!("Failed to create temp dir: {}", e))?;

        // Write pre-populated org files BEFORE engine initialization
        // This is the key to reproducing the Flutter bug
        let mut documents = HashMap::new();
        for (filename, content) in &self.org_files {
            let file_path = temp_dir.path().join(filename);
            tokio::fs::write(&file_path, content)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to write pre-populated org file: {}", e))?;

            let doc_uri = format!("{}{}", DOCUMENT_URI_SCHEME, filename);
            documents.insert(doc_uri, file_path);
        }

        eprintln!(
            "[TestEnvironmentBuilder] Pre-populated {} org files before engine init",
            self.org_files.len()
        );

        // Build FrontendConfig with wait/settle settings
        let mut config = FrontendConfig::new()
            .with_db_path(temp_dir.path().join("test.db"))
            .with_orgmode(temp_dir.path().to_path_buf())
            .with_loro()
            .with_settle_delay(self.settle_delay_ms);

        if !self.wait_for_file_watcher {
            config = config.without_wait();
        }

        if self.enable_todoist_fake {
            config = config.with_todoist_fake();
        }

        // NOW initialize via FrontendSession - this triggers the sync/DDL race
        let session = FrontendSession::new_with_extras(config, |provider| {
            let store = DiResolver::get_required::<LoroDocumentStore>(provider);
            TestExtras {
                doc_store: Arc::new(RwLock::new((*store).clone())),
            }
        })
        .await?;

        let ctx = E2ETestContext::from_engine(session.engine().clone());

        let startup_errors = session.error_tracker().errors();
        eprintln!(
            "[TestEnvironmentBuilder] Engine initialized. Startup errors: {}",
            startup_errors
        );

        Ok(TestEnvironment {
            temp_dir,
            runtime,
            session: Some(session),
            ctx: Some(ctx),
            documents,
            active_watches: HashMap::new(),
            ui_model: HashMap::new(),
            current_view: "all".to_string(),
            region_streams: HashMap::new(),
            region_data: HashMap::new(),
            enable_todoist: self.enable_todoist_fake,
        })
    }
}

impl Default for TestEnvironmentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestEnvironment {
    /// Create a new test environment (app not started yet).
    ///
    /// Use this for pre-startup testing scenarios. Call `start_app()` to start the application.
    pub fn new(runtime: Arc<tokio::runtime::Runtime>) -> Result<Self> {
        let temp_dir =
            TempDir::new().map_err(|e| anyhow::anyhow!("Failed to create temp dir: {}", e))?;

        Ok(Self {
            temp_dir,
            runtime,
            session: None,
            ctx: None,
            documents: HashMap::new(),
            active_watches: HashMap::new(),
            ui_model: HashMap::new(),
            current_view: "all".to_string(),
            region_streams: HashMap::new(),
            region_data: HashMap::new(),
            enable_todoist: false,
        })
    }

    /// Create and immediately start (existing behavior for backward compatibility).
    ///
    /// Equivalent to `new()` followed by `start_app(true)`.
    pub async fn new_running(runtime: Arc<tokio::runtime::Runtime>) -> Result<Self> {
        let mut env = Self::new(runtime)?;
        env.start_app(true).await?;
        Ok(env)
    }

    /// Write an org file to the temp directory.
    ///
    /// Can be called both before and after `start_app()`.
    /// When called before startup, the file will be synced when the app starts.
    pub async fn write_org_file(&mut self, filename: &str, content: &str) -> Result<PathBuf> {
        let file_path = self.temp_dir.path().join(filename);

        // Create parent directories if needed
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create parent directories: {}", e))?;
        }

        tokio::fs::write(&file_path, content)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write org file: {}", e))?;

        let doc_uri = format!("{}{}", DOCUMENT_URI_SCHEME, filename);
        self.documents.insert(doc_uri, file_path.clone());

        // Small delay to ensure file watcher detects the change (only if app is running)
        if self.session.is_some() {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        Ok(file_path)
    }

    /// Write a stale/corrupted .loro file to the temp directory.
    ///
    /// This simulates scenarios where a .loro file exists from a previous run
    /// but is corrupted or empty. The system should detect this and recover.
    ///
    /// Can only be called BEFORE `start_app()`.
    pub async fn write_stale_loro_file(
        &mut self,
        filename: &str,
        corruption_type: LoroCorruptionType,
    ) -> Result<PathBuf> {
        assert!(
            self.session.is_none(),
            "Cannot create stale loro file after app started"
        );

        // Replace .org extension with .loro if present
        let loro_filename = if filename.ends_with(".org") {
            filename.replace(".org", ".loro")
        } else {
            format!("{}.loro", filename)
        };

        let loro_path = self.temp_dir.path().join(&loro_filename);

        let content = match corruption_type {
            LoroCorruptionType::Empty => Vec::new(),
            LoroCorruptionType::Truncated => vec![0x4C, 0x6F, 0x72, 0x6F], // "Loro" prefix but truncated
            LoroCorruptionType::InvalidHeader => vec![0xFF, 0xFE, 0x00, 0x01], // Invalid magic bytes
        };

        tokio::fs::write(&loro_path, &content)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write stale loro file: {}", e))?;

        Ok(loro_path)
    }

    /// Enable Todoist fake mode for the next start_app() call.
    ///
    /// When enabled, start_app() will include Todoist with a fake client,
    /// which adds concurrent DDL (CREATE TABLE todoist_tasks, todoist_projects)
    /// during startup. This increases the race window and matches production DI path.
    pub fn set_enable_todoist(&mut self, enable: bool) {
        self.enable_todoist = enable;
    }

    /// Start the application.
    ///
    /// This triggers sync of any pre-existing files and may race with DDL.
    ///
    /// # Arguments
    /// * `wait_for_ready` - If true, wait for file watcher to be ready before returning
    pub async fn start_app(&mut self, wait_for_ready: bool) -> Result<()> {
        assert!(self.session.is_none(), "App already started");

        let mut config = FrontendConfig::new()
            .with_db_path(self.temp_dir.path().join("test.db"))
            .with_orgmode(self.temp_dir.path().to_path_buf())
            .with_loro();

        if !wait_for_ready {
            config = config.without_wait();
        }

        if self.enable_todoist {
            config = config.with_todoist_fake();
        }

        let session = FrontendSession::new_with_extras(config, |provider| {
            let store = DiResolver::get_required::<LoroDocumentStore>(provider);
            TestExtras {
                doc_store: Arc::new(RwLock::new((*store).clone())),
            }
        })
        .await?;

        let ctx = E2ETestContext::from_engine(session.engine().clone());

        self.session = Some(session);
        self.ctx = Some(ctx);

        Ok(())
    }

    /// Check if app is running
    pub fn is_running(&self) -> bool {
        self.session.is_some()
    }

    /// Get the running session (panics if not started)
    pub fn session(&self) -> &FrontendSession<TestExtras> {
        self.session
            .as_ref()
            .expect("App not started - call start_app() first")
    }

    /// Get the E2ETestContext (panics if not started)
    ///
    /// Use this for direct access to the test context operations.
    pub fn test_ctx(&self) -> &E2ETestContext {
        self.ctx
            .as_ref()
            .expect("App not started - call start_app() first")
    }

    /// Check for startup errors (delegates to FrontendSession)
    pub fn has_startup_errors(&self) -> bool {
        self.session().has_startup_errors()
    }

    /// Get the number of publish errors that occurred
    pub fn startup_error_count(&self) -> usize {
        self.session().startup_error_count()
    }

    /// Get the publish error tracker for monitoring startup errors
    pub fn publish_error_tracker(&self) -> &PublishErrorTracker {
        self.session().error_tracker()
    }

    /// Get the underlying engine (requires running app)
    pub fn engine(&self) -> &Arc<BackendEngine> {
        self.session().engine()
    }

    /// Get the doc store (requires running app)
    pub fn doc_store(&self) -> &Arc<RwLock<LoroDocumentStore>> {
        &self.session().extras().doc_store
    }

    /// Create an org file in the temp directory (requires running app)
    pub async fn create_document(&mut self, file_name: &str) -> Result<String> {
        let file_path = self.temp_dir.path().join(file_name);
        tokio::fs::write(&file_path, "")
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create org file: {}", e))?;

        let doc_uri = format!("{}{}", DOCUMENT_URI_SCHEME, file_name);

        {
            let mut store = self.doc_store().write().await;
            store
                .get_or_load(&file_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to load org file: {}", e))?;
        }

        self.documents.insert(doc_uri.clone(), file_path);

        Ok(doc_uri)
    }

    /// Execute an operation on the backend
    pub async fn execute_operation(
        &self,
        entity: &str,
        op: &str,
        params: HashMap<String, Value>,
    ) -> Result<()> {
        self.test_ctx().execute_op(entity, op, params).await
    }

    /// Query the backend
    pub async fn query(&self, prql: &str) -> Result<Vec<HashMap<String, Value>>> {
        let spec = self
            .test_ctx()
            .query(prql.to_string(), HashMap::new())
            .await?;
        Ok(spec.data)
    }

    /// Get path to an org file
    pub fn org_file_path(&self, file_name: &str) -> PathBuf {
        self.temp_dir.path().join(file_name)
    }

    /// Get the temp directory path
    pub fn temp_path(&self) -> &std::path::Path {
        self.temp_dir.path()
    }

    /// Get path to a document by doc_uri
    pub fn get_document_path(&self, doc_uri: &str) -> Option<&PathBuf> {
        self.documents.get(doc_uri)
    }

    /// Reload an org file from disk (removes from store and re-loads)
    pub async fn reload_org_file(&self, file_path: &PathBuf) -> Result<()> {
        let mut store = self.doc_store().write().await;
        store.remove(file_path).await;
        store
            .get_or_load(file_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to reload org file: {}", e))?;
        Ok(())
    }

    /// Call initial_widget and return both the widget spec and CDC stream.
    ///
    /// Use this when you need to track changes over time via the CDC stream.
    /// Uses FrontendSession directly to ensure identical code path with Flutter.
    pub async fn initial_widget_with_stream(
        &self,
    ) -> Result<(holon_api::WidgetSpec, RowChangeStream)> {
        self.session().initial_widget().await
    }

    /// Call initial_widget to get the root layout widget (discards stream).
    ///
    /// Use `initial_widget_with_stream` if you need to track changes.
    pub async fn initial_widget(&self) -> Result<holon_api::WidgetSpec> {
        let (widget_spec, _stream) = self.initial_widget_with_stream().await?;
        Ok(widget_spec)
    }

    /// Call initial_widget and recursively render all nested PRQL blocks.
    ///
    /// This simulates what the Flutter UI does:
    /// 1. Call initial_widget to get the root layout
    /// 2. For each row that is a PRQL source block, execute its query with parent context
    /// 3. Collect all rendered data into a single WidgetSpec
    ///
    /// Returns a combined WidgetSpec with data from all rendered panels.
    pub async fn initial_widget_fully_rendered(&self) -> Result<WidgetSpec> {
        let (root_spec, _stream) = self.initial_widget_with_stream().await?;

        // Collect all data: start with root layout data
        let mut all_data = root_spec.data.clone();

        // Process each row - if it's a PRQL source block, render it
        for row in &root_spec.data {
            let content_type = row.get("content_type").and_then(|v| v.as_string());
            let source_language = row.get("source_language").and_then(|v| v.as_string());

            if content_type == Some("source") && source_language == Some("prql") {
                // This is a PRQL source block - render it with parent context
                let block_id = row.get("id").and_then(|v| v.as_string());
                let parent_id = row.get("parent_id").and_then(|v| v.as_string());
                let prql_content = row.get("content").and_then(|v| v.as_string());

                if let (Some(_block_id), Some(parent_id), Some(prql)) =
                    (block_id, parent_id, prql_content)
                {
                    // Execute with PARENT's context (this is what the UI should do)
                    match self.query_with_context(prql, parent_id).await {
                        Ok(nested_spec) => {
                            // Add nested data to our collection
                            all_data.extend(nested_spec.data);
                        }
                        Err(e) => {
                            // Log error but continue - some queries may legitimately return no data
                            eprintln!(
                                "[test] Failed to render nested block under {}: {}",
                                parent_id, e
                            );
                        }
                    }
                }
            }
        }

        Ok(WidgetSpec {
            render_spec: root_spec.render_spec,
            data: all_data,
            actions: root_spec.actions,
        })
    }

    /// Execute a PRQL query with context (simulating nested render_block).
    ///
    /// This simulates what the Flutter UI does when it encounters `render_block this`:
    /// - Takes a PRQL source from a block
    /// - Executes it with the parent block's ID as context for `from children`
    /// Uses FrontendSession directly to ensure identical code path with Flutter.
    ///
    /// # Arguments
    /// * `prql` - The PRQL query to execute
    /// * `context_block_id` - The block ID to use for `from children` resolution
    pub async fn query_with_context(
        &self,
        prql: &str,
        context_block_id: &str,
    ) -> Result<WidgetSpec> {
        let session = self.session();
        let block_path = session.lookup_block_path(context_block_id).await?;
        let context =
            QueryContext::for_block_with_path(context_block_id.to_string(), None, block_path);
        let (widget_spec, _stream) = session
            .query_and_watch(prql.to_string(), HashMap::new(), Some(context))
            .await?;
        Ok(widget_spec)
    }

    /// Simulate what the Flutter UI does when rendering a PRQL source block.
    ///
    /// When the UI encounters `render_block this` for a PRQL source block,
    /// it should execute the query with the source block's PARENT as context.
    /// This is because `from children` in that query should get children of
    /// the heading (parent), not children of the source block itself.
    /// Uses FrontendSession directly to ensure identical code path with Flutter.
    ///
    /// # Arguments
    /// * `source_block_id` - The ID of the PRQL source block (e.g., "right_sidebar::src::0")
    ///
    /// # Returns
    /// The WidgetSpec from executing the source block's PRQL with parent context
    pub async fn render_prql_block(&self, source_block_id: &str) -> Result<WidgetSpec> {
        let session = self.session();

        // First, get the source block to find its content and parent
        let blocks = session
            .execute_query(
                "SELECT parent_id, content FROM blocks WHERE id = $id".to_string(),
                {
                    let mut params = HashMap::new();
                    params.insert("id".to_string(), Value::String(source_block_id.to_string()));
                    params
                },
                None,
            )
            .await?;

        let block = blocks
            .first()
            .ok_or_else(|| anyhow::anyhow!("Source block '{}' not found", source_block_id))?;

        let parent_id = block
            .get("parent_id")
            .and_then(|v| v.as_string())
            .ok_or_else(|| anyhow::anyhow!("Source block has no parent_id"))?;

        let prql_content = block
            .get("content")
            .and_then(|v| v.as_string())
            .ok_or_else(|| anyhow::anyhow!("Source block has no content"))?;

        // Execute the query with the PARENT's context (not the source block's own ID)
        self.query_with_context(prql_content, parent_id).await
    }

    /// Create a document and wait for the external_processing window to close.
    ///
    /// This is useful for PBT tests that need to ensure the file watcher has
    /// fully processed the new document before proceeding.
    pub async fn create_document_with_sync_wait(&mut self, file_name: &str) -> Result<String> {
        let doc_uri = self.create_document(file_name).await?;
        // Wait for external_processing window (3100ms) to ensure sync completes
        tokio::time::sleep(tokio::time::Duration::from_millis(3100)).await;
        Ok(doc_uri)
    }

    /// Drain CDC events from all active watches and update ui_model.
    pub async fn drain_cdc_events(&mut self) {
        use tokio::time::{Duration, timeout};

        for (query_id, stream) in &mut self.active_watches {
            while let Ok(Some(batch)) = timeout(Duration::from_millis(20), stream.next()).await {
                if let Some(ui_data) = self.ui_model.get_mut(query_id) {
                    for change in &batch.inner.items {
                        apply_cdc_event_to_vec(ui_data, change);
                    }
                }
            }
        }
    }

    /// Drain CDC events from all region streams and update region_data.
    pub async fn drain_region_cdc_events(&mut self) {
        use tokio::time::{Duration, timeout};

        for (region_id, stream) in &mut self.region_streams {
            while let Ok(Some(batch)) = timeout(Duration::from_millis(100), stream.next()).await {
                if let Some(region_data) = self.region_data.get_mut(region_id) {
                    for change in &batch.inner.items {
                        apply_cdc_event_to_vec(region_data, change);
                    }
                }
            }
        }
    }

    /// Parse all Org files in the temp directory and return blocks.
    ///
    /// Uses the production `Block` struct for accurate testing.
    pub async fn parse_org_file_blocks(&self) -> Result<Vec<Block>> {
        use holon_orgmode::parser::parse_org_file;

        let mut all_blocks = Vec::new();
        let root = self.temp_dir.path();

        for (_doc_uri, file_path) in &self.documents {
            let content = tokio::fs::read_to_string(file_path).await?;
            let result = parse_org_file(file_path, &content, "root", 0, root)?;
            all_blocks.extend(result.blocks);
        }

        Ok(all_blocks)
    }

    // =========================================================================
    // Navigation Operations
    // =========================================================================

    /// Navigate to focus on a specific block in a region.
    pub async fn navigate_focus(&mut self, region: &str, block_id: &str) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("region".to_string(), Value::String(region.to_string()));
        params.insert("block_id".to_string(), Value::String(block_id.to_string()));
        self.test_ctx()
            .execute_op("navigation", "focus", params)
            .await?;
        self.drain_region_cdc_events().await;
        Ok(())
    }

    /// Navigate back in history for a region.
    pub async fn navigate_back(&mut self, region: &str) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("region".to_string(), Value::String(region.to_string()));
        self.test_ctx()
            .execute_op("navigation", "go_back", params)
            .await?;
        self.drain_region_cdc_events().await;
        Ok(())
    }

    /// Navigate forward in history for a region.
    pub async fn navigate_forward(&mut self, region: &str) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("region".to_string(), Value::String(region.to_string()));
        self.test_ctx()
            .execute_op("navigation", "go_forward", params)
            .await?;
        self.drain_region_cdc_events().await;
        Ok(())
    }

    /// Navigate to home (root view) for a region.
    pub async fn navigate_home(&mut self, region: &str) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("region".to_string(), Value::String(region.to_string()));
        self.test_ctx()
            .execute_op("navigation", "go_home", params)
            .await?;
        self.drain_region_cdc_events().await;
        Ok(())
    }

    // =========================================================================
    // Watch Operations
    // =========================================================================

    /// Set up a CDC watch for a query.
    pub async fn setup_watch(&mut self, query_id: &str, prql: &str) -> Result<()> {
        let (widget_spec, stream) = self
            .test_ctx()
            .query_and_watch(prql.to_string(), HashMap::new())
            .await?;
        self.ui_model.insert(query_id.to_string(), widget_spec.data);
        self.active_watches.insert(query_id.to_string(), stream);
        Ok(())
    }

    /// Remove a watch.
    pub fn remove_watch(&mut self, query_id: &str) {
        self.active_watches.remove(query_id);
        self.ui_model.remove(query_id);
    }

    // =========================================================================
    // View Operations
    // =========================================================================

    /// Switch the active view filter.
    pub fn switch_view(&mut self, view_name: &str) {
        self.current_view = view_name.to_string();
    }

    // =========================================================================
    // Block CRUD Operations
    // =========================================================================

    /// Create a text block.
    pub async fn create_block(&self, id: &str, parent_id: &str, content: &str) -> Result<()> {
        use holon_api::block::CONTENT_TYPE_TEXT;

        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(id.to_string()));
        params.insert(
            "parent_id".to_string(),
            Value::String(parent_id.to_string()),
        );
        params.insert("content".to_string(), Value::String(content.to_string()));
        params.insert(
            "content_type".to_string(),
            Value::String(CONTENT_TYPE_TEXT.to_string()),
        );

        self.test_ctx().execute_op("blocks", "create", params).await
    }

    /// Create a source block with a specified language.
    pub async fn create_source_block(
        &self,
        id: &str,
        parent_id: &str,
        language: &str,
        content: &str,
    ) -> Result<()> {
        use holon_api::block::CONTENT_TYPE_SOURCE;

        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(id.to_string()));
        params.insert(
            "parent_id".to_string(),
            Value::String(parent_id.to_string()),
        );
        params.insert("content".to_string(), Value::String(content.to_string()));
        params.insert(
            "content_type".to_string(),
            Value::String(CONTENT_TYPE_SOURCE.to_string()),
        );
        params.insert(
            "source_language".to_string(),
            Value::String(language.to_string()),
        );

        self.test_ctx().execute_op("blocks", "create", params).await
    }

    /// Update a block's content.
    pub async fn update_block_content(&self, id: &str, new_content: &str) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(id.to_string()));
        params.insert("field".to_string(), Value::String("content".to_string()));
        params.insert("value".to_string(), Value::String(new_content.to_string()));

        self.test_ctx()
            .execute_op("blocks", "set_field", params)
            .await
    }

    /// Delete a block.
    pub async fn delete_block(&self, id: &str) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(id.to_string()));

        self.test_ctx().execute_op("blocks", "delete", params).await
    }

    // =========================================================================
    // Polling / Waiting Helpers
    // =========================================================================

    /// Wait until a specific block exists in the database.
    pub async fn wait_for_block(&self, block_id: &str, timeout: std::time::Duration) -> bool {
        use crate::wait_until;

        let prql = format!(
            "from blocks | filter id == \"{}\" | select {{id}} | render (list item_template:(text this.id))",
            block_id
        );
        let poll_interval = std::time::Duration::from_millis(50);

        wait_until(
            || async {
                self.test_ctx()
                    .query(prql.clone(), HashMap::new())
                    .await
                    .map(|widget_spec| !widget_spec.data.is_empty())
                    .unwrap_or(false)
            },
            timeout,
            poll_interval,
        )
        .await
    }

    /// Wait until expected block count is reached in the database.
    /// Returns the actual rows if condition met, or last result if timed out.
    pub async fn wait_for_block_count(
        &self,
        expected_count: usize,
        timeout: std::time::Duration,
    ) -> Vec<HashMap<String, Value>> {
        let poll_interval = std::time::Duration::from_millis(50);
        let start = std::time::Instant::now();
        let mut last_result = Vec::new();

        let sql = "SELECT id FROM blocks".to_string();

        while start.elapsed() < timeout {
            match self
                .engine()
                .execute_query(sql.clone(), HashMap::new(), None)
                .await
            {
                Ok(rows) => {
                    last_result = rows.clone();
                    if rows.len() == expected_count {
                        return rows;
                    }
                }
                Err(e) => {
                    eprintln!("[wait_for_block_count] Query error: {:?}", e);
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
        last_result
    }

    /// Simulate app restart by touching all org files to trigger re-parsing.
    /// This tests that re-parsing doesn't create orphan blocks.
    pub async fn simulate_restart(&self, expected_block_count: usize) -> Result<()> {
        use std::time::Duration;

        for (doc_uri, file_path) in &self.documents {
            eprintln!(
                "[simulate_restart] Re-triggering parse for: {} -> {}",
                doc_uri,
                file_path.display()
            );
            if let Ok(content) = tokio::fs::read_to_string(&file_path).await {
                // Add a space and remove it to ensure content is "different"
                let modified = format!("{} ", content);
                let _ = tokio::fs::write(&file_path, &modified).await;
                tokio::time::sleep(Duration::from_millis(50)).await;
                // Restore original content
                let _ = tokio::fs::write(&file_path, &content).await;
            }
        }

        // Wait for block count to stabilize
        let timeout = Duration::from_millis(5000);
        let start = std::time::Instant::now();
        let _ = self
            .wait_for_block_count(expected_block_count, timeout)
            .await;
        eprintln!(
            "[simulate_restart] Block count stabilized in {:?}",
            start.elapsed()
        );

        // Wait for external_processing window to expire
        tokio::time::sleep(Duration::from_millis(3100)).await;
        eprintln!("[simulate_restart] External processing window expired");

        Ok(())
    }

    // =========================================================================
    // External Mutation Helpers (for PBT and other tests)
    // =========================================================================

    /// Apply an external mutation by writing directly to org files.
    ///
    /// This simulates an external process (like Emacs) modifying the org file.
    /// The file watcher will detect the change and sync it to Loro.
    ///
    /// # Arguments
    /// * `expected_blocks` - All blocks that should exist after the mutation
    pub async fn apply_external_mutation(&self, expected_blocks: &[Block]) -> Result<()> {
        for (doc_uri, file_path) in &self.documents {
            let doc_blocks: Vec<&Block> = expected_blocks
                .iter()
                .filter(|b| block_belongs_to_document(b, expected_blocks, doc_uri))
                .collect();

            let org_content = serialize_blocks_to_org(&doc_blocks, doc_uri);
            eprintln!(
                "[apply_external_mutation] Writing to {:?}, content length={}",
                file_path,
                org_content.len()
            );
            tokio::fs::write(file_path, &org_content).await?;
            eprintln!(
                "[apply_external_mutation] File written, org_content:\n{}",
                org_content
            );
        }

        eprintln!("[apply_external_mutation] File written, polling will wait for sync");
        Ok(())
    }

    /// Wait for org files to sync to the expected block count.
    ///
    /// This waits for each document's org file to contain the expected number of blocks
    /// based on the reference blocks provided.
    ///
    /// # Arguments
    /// * `expected_blocks` - Reference blocks to count expected blocks per document
    /// * `timeout` - Maximum time to wait
    ///
    /// # Returns
    /// `true` if all files synced within timeout, `false` otherwise
    pub async fn wait_for_org_file_sync(
        &self,
        expected_blocks: &[Block],
        timeout: std::time::Duration,
    ) -> bool {
        let start = std::time::Instant::now();

        for (doc_uri, file_path) in &self.documents {
            let expected_in_doc: usize = expected_blocks
                .iter()
                .filter(|b| block_belongs_to_document(b, expected_blocks, doc_uri))
                .count();

            // Render expected blocks to Org format and compute content hash.
            // This ensures we wait until the OrgFileWriter has written the correct
            // structure (not just correct block count/content).
            let doc_blocks: Vec<Block> = expected_blocks
                .iter()
                .filter(|b| block_belongs_to_document(b, expected_blocks, doc_uri))
                .cloned()
                .collect();
            let expected_org = holon_orgmode::org_renderer::OrgRenderer::render_blocks(
                &doc_blocks,
                file_path,
                doc_uri,
            );
            let expected_hash = {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                expected_org.trim().hash(&mut hasher);
                hasher.finish()
            };

            let remaining = timeout.saturating_sub(start.elapsed());
            let condition_met = wait_for_file_condition(
                file_path,
                |content| {
                    let text_count = content.matches(":ID:").count();
                    let src_count = content.to_lowercase().matches("#+begin_src").count();
                    let actual_count = text_count + src_count;
                    if actual_count != expected_in_doc {
                        return false;
                    }
                    // Check content hash matches expected rendered output
                    let actual_hash = {
                        use std::hash::{Hash, Hasher};
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        content.trim().hash(&mut hasher);
                        hasher.finish()
                    };
                    actual_hash == expected_hash
                },
                remaining,
            )
            .await;

            if condition_met {
                eprintln!(
                    "[wait_for_org_file_sync] Org file {:?} synced ({} blocks) in {:?}",
                    file_path,
                    expected_in_doc,
                    start.elapsed()
                );
            } else {
                eprintln!(
                    "[wait_for_org_file_sync] WARNING: Org file {:?} not synced after {:?}",
                    file_path,
                    start.elapsed()
                );
                return false;
            }
        }
        true
    }

    /// Wait for write tracker window to expire (for UI mutations).
    ///
    /// After a UI mutation writes to an org file, the WriteTracker has a 2000ms
    /// window during which subsequent external mutations would be ignored.
    /// Call this after UI mutations to ensure external mutations can proceed.
    pub async fn wait_for_write_window_expiry(&self) {
        tokio::time::sleep(tokio::time::Duration::from_millis(2100)).await;
        eprintln!("[wait_for_write_window_expiry] Write window expired");
    }

    /// Wait for external processing window to expire.
    ///
    /// After an external mutation, the WriteTracker has a 3000ms window during
    /// which UI mutations won't trigger org file writes.
    /// Call this after external mutations to ensure UI mutations can proceed.
    pub async fn wait_for_external_processing_expiry(&self) {
        tokio::time::sleep(tokio::time::Duration::from_millis(3100)).await;
        eprintln!("[wait_for_external_processing_expiry] External processing window expired");
    }
}

// =============================================================================
// Backward Compatibility Aliases
// =============================================================================

/// Alias for backward compatibility
pub type TestContext = TestEnvironment;

/// Alias for backward compatibility
pub type TestContextBuilder = TestEnvironmentBuilder;
