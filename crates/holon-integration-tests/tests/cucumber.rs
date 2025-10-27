//! Cucumber BDD tests for Holon backend operations
//!
//! This module provides step definitions for testing:
//! 1. Writing org files to holon's org directory
//! 2. Starting a simulated frontend that interacts with BackendEngine
//! 3. Sending operations to the backend
//! 4. Tracking UI state via CDC streaming

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cucumber::{World, given, then, when};
use holon::api::RowChangeStream;
use holon_api::Value;
use holon_integration_tests::{
    TestContext, WidgetLocator, WidgetStateModel, drain_stream, extract_first_block_id,
    wait_for_text_in_widget,
};

#[derive(Debug, World)]
#[world(init = Self::new)]
pub struct HolonWorld {
    context: Option<TestContext>,
    runtime: Arc<tokio::runtime::Runtime>,
    last_error: Option<String>,
    /// Cached widget spec from initial_widget (legacy field)
    last_widget_spec: Option<holon_api::WidgetSpec>,
    /// Widget state models for root + each nested panel (for combined text extraction)
    widget_states: Vec<WidgetStateModel>,
    /// Widget state model for tracking UI state via CDC
    widget_state: Option<WidgetStateModel>,
    /// CDC stream for receiving change notifications
    #[allow(dead_code)]
    change_stream: Option<RowChangeStream>,
}

impl HolonWorld {
    fn new() -> Self {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to create runtime"),
        );
        Self {
            context: None,
            runtime,
            last_error: None,
            last_widget_spec: None,
            widget_states: Vec::new(),
            widget_state: None,
            change_stream: None,
        }
    }

    fn ctx(&self) -> &TestContext {
        self.context.as_ref().expect("Backend not initialized")
    }

    fn ctx_mut(&mut self) -> &mut TestContext {
        self.context.as_mut().expect("Backend not initialized")
    }
}

// =============================================================================
// Given Steps
// =============================================================================

#[given("the backend is initialized")]
async fn backend_initialized(world: &mut HolonWorld) {
    let runtime = world.runtime.clone();
    let ctx = runtime
        .block_on(async {
            let ctx = TestContext::new_running(runtime.clone()).await?;
            // Wait for file watcher and other background tasks to fully start
            // Increased to 3000ms to ensure file watcher is ready even under load
            tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
            Ok::<_, anyhow::Error>(ctx)
        })
        .expect("Failed to initialize backend");
    world.context = Some(ctx);
}

// =============================================================================
// Pre-Startup Steps (for testing startup race conditions)
// =============================================================================

/// Create an org file BEFORE the app starts (pre-startup phase)
#[given(regex = r#"an org file "([^"]+)" with content:"#)]
async fn given_org_file(world: &mut HolonWorld, filename: String, step: &cucumber::gherkin::Step) {
    let content = step.docstring().expect("Missing docstring content");
    let runtime = world.runtime.clone();

    // Ensure we have a test environment but NOT started yet
    if world.context.is_none() {
        let ctx = TestContext::new(runtime.clone()).expect("Failed to create test environment");
        world.context = Some(ctx);
    }

    runtime.block_on(async {
        world
            .ctx_mut()
            .write_org_file(&filename, content)
            .await
            .expect("Failed to write org file");
    });
}

/// Start the application (triggers sync of pre-existing files)
#[when("the application starts")]
async fn when_app_starts(world: &mut HolonWorld) {
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        world
            .ctx_mut()
            .start_app(true)
            .await
            .expect("Failed to start app");
    });
}

/// Start the application without waiting for file watcher (captures race condition)
#[when("the application starts without waiting")]
async fn when_app_starts_no_wait(world: &mut HolonWorld) {
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        world
            .ctx_mut()
            .start_app(false)
            .await
            .expect("Failed to start app");
    });
}

/// Assert no startup errors occurred (DDL/sync race detection)
#[then("there should be no startup errors")]
async fn then_no_startup_errors(world: &mut HolonWorld) {
    assert!(
        !world.ctx().has_startup_errors(),
        "Startup errors detected: {} errors occurred.\n\
         This indicates DDL/sync race condition during startup.",
        world.ctx().startup_error_count()
    );
}

// =============================================================================
// Post-Startup Steps (existing)
// =============================================================================

#[given(expr = "a document {string} exists")]
async fn document_exists(world: &mut HolonWorld, file_name: String) {
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        world
            .ctx_mut()
            .create_document(&file_name)
            .await
            .expect("Failed to create document");
    });
}

#[given(expr = "a block {string} with content {string} exists in {string}")]
async fn block_exists_in_doc(
    world: &mut HolonWorld,
    block_id: String,
    content: String,
    file_name: String,
) {
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        let doc_uri = format!("holon-doc://{}", file_name);
        world
            .ctx()
            .create_block(&block_id, &doc_uri, &content)
            .await
            .expect("Failed to create block");
        world
            .ctx()
            .wait_for_block(&block_id, Duration::from_secs(5))
            .await;
    });
}

// =============================================================================
// When Steps
// =============================================================================

#[when(expr = "I create a block with id {string} and content {string} in document {string}")]
async fn create_block(
    world: &mut HolonWorld,
    block_id: String,
    content: String,
    file_name: String,
) {
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        let doc_uri = format!("holon-doc://{}", file_name);
        if let Err(e) = world
            .ctx()
            .create_block(&block_id, &doc_uri, &content)
            .await
        {
            world.last_error = Some(e.to_string());
        }
    });
}

#[when(expr = "I update block {string} content to {string}")]
async fn update_block_content(world: &mut HolonWorld, block_id: String, new_content: String) {
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        if let Err(e) = world
            .ctx()
            .update_block_content(&block_id, &new_content)
            .await
        {
            world.last_error = Some(e.to_string());
        }
    });
}

#[when(expr = "I delete block {string}")]
async fn delete_block(world: &mut HolonWorld, block_id: String) {
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        if let Err(e) = world.ctx().delete_block(&block_id).await {
            world.last_error = Some(e.to_string());
        }
    });
}

#[when(expr = "I write the following content to {string}:")]
async fn write_org_content(
    world: &mut HolonWorld,
    file_name: String,
    step: &cucumber::gherkin::Step,
) {
    let content = step.docstring().expect("Missing docstring content");
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        world
            .ctx_mut()
            .write_org_file(&file_name, content)
            .await
            .expect("Failed to write org file");
    });
}

#[when(
    expr = "I create a source block with id {string}, language {string}, and content {string} under {string}"
)]
async fn create_source_block(
    world: &mut HolonWorld,
    block_id: String,
    language: String,
    content: String,
    parent_id: String,
) {
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        if let Err(e) = world
            .ctx()
            .create_source_block(&block_id, &parent_id, &language, &content)
            .await
        {
            world.last_error = Some(e.to_string());
        }
    });
}

// =============================================================================
// Then Steps
// =============================================================================

#[then(expr = "the block {string} should exist in the database")]
async fn block_should_exist(world: &mut HolonWorld, block_id: String) {
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        let exists = world
            .ctx()
            .wait_for_block(&block_id, Duration::from_secs(5))
            .await;
        assert!(exists, "Block '{}' should exist in database", block_id);
    });
}

#[then(expr = "within {int} seconds the block {string} should exist in the database")]
async fn block_should_exist_within(world: &mut HolonWorld, seconds: i32, block_id: String) {
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        let timeout = Duration::from_secs(seconds as u64);
        let exists = world.ctx().wait_for_block(&block_id, timeout).await;
        assert!(
            exists,
            "Block '{}' should exist within {} seconds",
            block_id, seconds
        );
    });
}

// =============================================================================
// Initial Widget Steps
// =============================================================================

#[when("I call initial_widget")]
async fn call_initial_widget(world: &mut HolonWorld) {
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        match world.ctx().initial_widget().await {
            Ok(widget_spec) => {
                world.last_widget_spec = Some(widget_spec);
                world.last_error = None;
            }
            Err(e) => {
                world.last_error = Some(e.to_string());
                world.last_widget_spec = None;
            }
        }
    });
}

#[then(expr = "the initial widget should return {int} data rows")]
async fn initial_widget_row_count(world: &mut HolonWorld, expected: i32) {
    if let Some(ref err) = world.last_error {
        panic!("initial_widget failed: {}", err);
    }
    let widget_spec = world
        .last_widget_spec
        .as_ref()
        .expect("No widget spec - call initial_widget first");
    let actual = widget_spec.data.len();
    assert_eq!(
        actual,
        expected as usize,
        "Expected {} data rows, got {}. Data: {:?}",
        expected,
        actual,
        widget_spec
            .data
            .iter()
            .map(|r| r.get("id"))
            .collect::<Vec<_>>()
    );
}

#[then(expr = "the initial widget data should contain id {string}")]
async fn initial_widget_contains_id(world: &mut HolonWorld, expected_id: String) {
    let widget_spec = world
        .last_widget_spec
        .as_ref()
        .expect("No widget spec - call initial_widget first");

    let ids: Vec<_> = widget_spec
        .data
        .iter()
        .filter_map(|r| r.get("id").and_then(|v| v.as_string()))
        .collect();

    assert!(
        ids.iter().any(|id| *id == expected_id),
        "Expected to find id '{}' in widget data. Found ids: {:?}",
        expected_id,
        ids
    );
}

#[then("the initial widget should have failed")]
async fn initial_widget_should_fail(world: &mut HolonWorld) {
    assert!(
        world.last_error.is_some(),
        "Expected initial_widget to fail, but it succeeded"
    );
}

#[then(expr = "the initial widget error should contain {string}")]
async fn initial_widget_error_contains(world: &mut HolonWorld, expected: String) {
    let err = world
        .last_error
        .as_ref()
        .expect("Expected an error but initial_widget succeeded");
    assert!(
        err.contains(&expected),
        "Expected error to contain '{}', got: {}",
        expected,
        err
    );
}

// =============================================================================
// New Workflow-Based Step Definitions
// =============================================================================

/// Write an org file with layout content (docstring)
///
/// For index.org layouts, also waits for the root layout block to sync to database.
#[given(expr = "the following {string} layout:")]
async fn given_layout_file(
    world: &mut HolonWorld,
    file_name: String,
    step: &cucumber::gherkin::Step,
) {
    let content = step.docstring().expect("Missing docstring content");
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        world
            .ctx_mut()
            .write_org_file(&file_name, content)
            .await
            .expect("Failed to write layout file");

        // Wait for the layout root block to sync to database
        // Extract the first block ID from the content (assumes :ID: property exists)
        if let Some(block_id) = extract_first_block_id(content) {
            let synced = world
                .ctx()
                .wait_for_block(&block_id, Duration::from_secs(10))
                .await;
            assert!(
                synced,
                "Layout block '{}' did not sync to database within timeout",
                block_id
            );
        }
    });
}

/// Write a journal file (creates parent directories if needed)
#[given(expr = "the following {string} journal:")]
async fn given_journal_file(
    world: &mut HolonWorld,
    file_name: String,
    step: &cucumber::gherkin::Step,
) {
    let content = step.docstring().expect("Missing docstring content");
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        let path = world.ctx().org_file_path(&file_name);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }

        world
            .ctx_mut()
            .write_org_file(&file_name, content)
            .await
            .expect("Failed to write journal file");

        // Wait for the first block to sync to database
        if let Some(block_id) = extract_first_block_id(content) {
            let synced = world
                .ctx()
                .wait_for_block(&block_id, Duration::from_secs(10))
                .await;
            assert!(
                synced,
                "Journal block '{}' did not sync to database within timeout",
                block_id
            );
        }
    });
}

/// Open the app (call initial_widget with CDC stream and render nested blocks)
///
/// This simulates the Flutter UI's behavior:
/// 1. Calls initial_widget to get the root layout with CDC stream
/// 2. For each PRQL source block in the data, executes its query with parent context
/// 3. Collects widget states from all panels for text extraction assertions
#[when("I open the app")]
async fn open_app(world: &mut HolonWorld) {
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        // Wait a bit for file sync to complete
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Get root widget with stream
        let result = world.ctx().initial_widget_with_stream().await;
        match result {
            Ok((root_spec, stream)) => {
                // Collect widget states from root and all nested panels
                let mut widget_states = Vec::new();
                widget_states.push(WidgetStateModel::from_widget_spec(&root_spec));

                // Recursively render nested PRQL blocks (like Flutter UI does)
                for row in &root_spec.data {
                    let content_type = row.get("content_type").and_then(|v| v.as_string());
                    let source_language = row.get("source_language").and_then(|v| v.as_string());

                    if content_type == Some("source") && source_language == Some("prql") {
                        let parent_id = row.get("parent_id").and_then(|v| v.as_string());
                        let prql_content = row.get("content").and_then(|v| v.as_string());

                        if let (Some(parent_id), Some(prql)) = (parent_id, prql_content) {
                            // Execute with PARENT's context (this is how the UI should work)
                            match world.ctx().query_with_context(prql, parent_id).await {
                                Ok(nested_spec) => {
                                    widget_states
                                        .push(WidgetStateModel::from_widget_spec(&nested_spec));
                                }
                                Err(e) => {
                                    eprintln!(
                                        "[test] Failed to render panel under {}: {}",
                                        parent_id, e
                                    );
                                }
                            }
                        }
                    }
                }

                // Store all widget states for combined text extraction
                world.widget_states = widget_states;
                world.last_widget_spec = Some(root_spec.clone());
                // Use root state for legacy widget_state field
                world.widget_state = Some(WidgetStateModel::from_widget_spec(&root_spec));
                world.change_stream = Some(stream);
                world.last_error = None;
            }
            Err(e) => {
                panic!("initial_widget failed: {}", e);
            }
        }
    });
}

/// Execute an operation with table parameters
#[when(expr = "I execute operation {string} with:")]
async fn execute_operation_with_table(
    world: &mut HolonWorld,
    operation: String,
    step: &cucumber::gherkin::Step,
) {
    let table = step.table().expect("Missing table");
    let mut params = HashMap::new();

    // Table format: | key | value |
    for row in table.rows.iter().skip(1) {
        // Skip header
        if row.len() >= 2 {
            params.insert(row[0].clone(), Value::String(row[1].clone()));
        }
    }

    // Parse "entity.operation" format
    let parts: Vec<&str> = operation.split('.').collect();
    assert!(
        parts.len() == 2,
        "Operation should be in 'entity.op' format, got: {}",
        operation
    );
    let (entity, op) = (parts[0], parts[1]);

    let runtime = world.runtime.clone();
    runtime.block_on(async {
        if let Err(e) = world.ctx().execute_operation(entity, op, params).await {
            world.last_error = Some(e.to_string());
        }
    });
}

/// Append content to an existing org file
#[when(expr = "I append to {string}:")]
async fn append_to_file(world: &mut HolonWorld, file_name: String, step: &cucumber::gherkin::Step) {
    let content = step.docstring().expect("Missing docstring content");
    let runtime = world.runtime.clone();
    runtime.block_on(async {
        let path = world.ctx().org_file_path(&file_name);
        let existing = tokio::fs::read_to_string(&path).await.unwrap_or_default();
        let new_content = format!("{}\n{}", existing, content);
        tokio::fs::write(&path, new_content)
            .await
            .expect("Failed to append to file");

        // Small delay to ensure file watcher detects the change
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    });
}

/// Assert the number of columns/views in the widget
#[then(expr = "I should see {int} columns")]
async fn should_see_columns(world: &mut HolonWorld, count: i32) {
    let state = world
        .widget_state
        .as_ref()
        .expect("No widget state - call 'I open the app' first");
    let actual = state.view_count();
    assert_eq!(
        actual,
        count as usize,
        "Expected {} columns, got {}. Views: {:?}",
        count,
        actual,
        state.view_names()
    );
}

/// Assert a widget shows exact content (multiline docstring)
/// Searches through all widget states (root + nested panels)
#[then(expr = "the {string} widget should show")]
async fn widget_should_show(
    world: &mut HolonWorld,
    locator_str: String,
    step: &cucumber::gherkin::Step,
) {
    let expected = step.docstring().expect("Missing docstring content");
    let locator = WidgetLocator::parse(&locator_str);

    let runtime = world.runtime.clone();
    runtime.block_on(async {
        assert!(
            !world.widget_states.is_empty(),
            "No widget states - call 'I open the app' first"
        );

        // Drain any pending CDC events first
        if let Some(ref mut stream) = world.change_stream {
            let changes = drain_stream(stream).await;
            for state in world.widget_states.iter_mut() {
                for change in &changes {
                    state.apply_change(change);
                }
            }
        }

        // Combine text from all widget states (root + nested panels)
        let actual: String = world
            .widget_states
            .iter()
            .map(|state| state.extract_text(&locator))
            .collect::<Vec<_>>()
            .join("\n");

        // Normalize whitespace for comparison
        let expected_normalized: Vec<&str> = expected.trim().lines().collect();
        let actual_normalized: Vec<&str> = actual.trim().lines().collect();

        assert_eq!(
            actual_normalized, expected_normalized,
            "Widget '{}' content mismatch.\nExpected:\n{}\n\nActual:\n{}",
            locator_str, expected, actual
        );
    });
}

/// Assert a widget contains text (substring match)
/// Searches through all widget states (root + nested panels)
#[then(expr = "the {string} widget should contain {string}")]
async fn widget_should_contain(world: &mut HolonWorld, locator_str: String, expected: String) {
    let locator = WidgetLocator::parse(&locator_str);

    let runtime = world.runtime.clone();
    runtime.block_on(async {
        // Drain any pending CDC events first
        if let Some(ref mut stream) = world.change_stream {
            let changes = drain_stream(stream).await;
            for state in world.widget_states.iter_mut() {
                for change in &changes {
                    state.apply_change(change);
                }
            }
        }

        assert!(
            !world.widget_states.is_empty(),
            "No widget states - call 'I open the app' first"
        );

        // Combine text from all widget states (root + nested panels)
        let actual: String = world
            .widget_states
            .iter()
            .map(|state| state.extract_text(&locator))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            actual.contains(&expected),
            "Widget '{}' should contain '{}' but got:\n{}",
            locator_str,
            expected,
            actual
        );
    });
}

/// Assert a widget contains text within a timeout (CDC streaming)
/// Searches through all widget states (root + nested panels)
#[then(expr = "within {int} seconds the {string} widget should contain {string}")]
async fn widget_should_contain_within(
    world: &mut HolonWorld,
    seconds: i32,
    locator_str: String,
    expected: String,
) {
    let locator = WidgetLocator::parse(&locator_str);
    let timeout = Duration::from_secs(seconds as u64);

    let runtime = world.runtime.clone();
    runtime.block_on(async {
        assert!(
            !world.widget_states.is_empty(),
            "No widget states - call 'I open the app' first"
        );

        // Drain any pending CDC events first
        if let Some(ref mut stream) = world.change_stream {
            let pending = drain_stream(stream).await;
            for state in world.widget_states.iter_mut() {
                for change in &pending {
                    state.apply_change(change);
                }
            }
        }

        // Check immediately across all widget states
        let contains_text = world
            .widget_states
            .iter()
            .any(|state| state.contains_text(&locator, &expected));

        if contains_text {
            return;
        }

        // Wait for changes via CDC stream (applies to root state)
        let stream = world
            .change_stream
            .as_mut()
            .expect("No change stream - call 'I open the app' first");

        let root_state = world
            .widget_states
            .first_mut()
            .expect("No root widget state");

        let found = wait_for_text_in_widget(stream, root_state, &locator, &expected, timeout).await;

        // Check all states again after waiting
        let contains_text = found
            || world
                .widget_states
                .iter()
                .any(|state| state.contains_text(&locator, &expected));

        let combined_text: String = world
            .widget_states
            .iter()
            .map(|state| state.extract_text(&locator))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            contains_text,
            "Widget '{}' should contain '{}' within {} seconds.\nCurrent content:\n{}",
            locator_str, expected, seconds, combined_text
        );
    });
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    futures::executor::block_on(
        HolonWorld::cucumber()
            .max_concurrent_scenarios(1)
            .run("tests/features"),
    );
}
