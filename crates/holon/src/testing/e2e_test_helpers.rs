//! End-to-end test helpers for BackendEngine
//!
//! This module provides high-level utilities for testing BackendEngine at the API level,
//! including PRQL query execution, CDC stream watching, and operation execution.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;

use crate::api::backend_engine::BackendEngine;
#[cfg(any(test, feature = "test-helpers"))]
use crate::di::test_helpers::{create_test_engine, create_test_engine_with_providers};
// Re-export TestProviderModule for use in tests
#[cfg(any(test, feature = "test-helpers"))]
pub use crate::di::test_helpers::TestProviderModule;
use crate::storage::turso::{ChangeData, RowChange, RowChangeStream};
use crate::storage::types::StorageEntity;
use holon_api::{BatchWithMetadata, Value};
use query_render::RenderSpec;

/// End-to-end test context for BackendEngine testing
///
/// Provides high-level utilities for:
/// - Executing PRQL queries
/// - Watching CDC streams
/// - Executing operations
/// - Asserting on stream changes
pub struct E2ETestContext {
    engine: Arc<BackendEngine>,
}

impl E2ETestContext {
    /// Create a new E2E test context with an in-memory database
    ///
    /// Uses the standard test engine setup with no custom providers.
    pub async fn new() -> Result<Self> {
        #[cfg(any(test, feature = "test-helpers"))]
        {
            let engine = create_test_engine()
                .await
                .context("Failed to create test engine")?;
            return Ok(Self { engine });
        }
        #[cfg(not(any(test, feature = "test-helpers")))]
        {
            Err(anyhow::anyhow!(
                "E2ETestContext::new() is only available in test builds. This is a test-only API."
            ))
        }
    }

    /// Create a new E2E test context with custom providers
    ///
    /// Uses `create_test_engine_with_providers` to set up the engine with
    /// custom operation providers for testing.
    ///
    /// # Example
    /// ```rust,no_run
    /// let ctx = E2ETestContext::with_providers(|module| {
    ///     module.with_operation_provider(my_provider)
    /// }).await?;
    /// ```
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn with_providers<F>(setup_fn: F) -> Result<Self>
    where
        F: FnOnce(TestProviderModule) -> TestProviderModule,
    {
        let engine = create_test_engine_with_providers(":memory:".into(), setup_fn)
            .await
            .context("Failed to create test engine with providers")?;
        Ok(Self { engine })
    }

    /// Stub for non-test builds - always returns an error
    #[cfg(not(any(test, feature = "test-helpers")))]
    pub async fn with_providers<F>(_setup_fn: F) -> Result<Self>
    where
        F: FnOnce(()) -> (),
    {
        Err(anyhow::anyhow!(
            "E2ETestContext::with_providers() is only available in test builds"
        ))
    }

    /// Send a PRQL query and get results
    ///
    /// Compiles the PRQL query, executes it, and returns both the render specification
    /// and the query results.
    ///
    /// # Arguments
    /// * `prql` - PRQL query string
    /// * `params` - Query parameters (can be empty HashMap)
    ///
    /// # Returns
    /// Tuple of (RenderSpec, Vec<HashMap<String, Value>>) containing the UI specification
    /// and the query results.
    pub async fn query(
        &self,
        prql: String,
        params: HashMap<String, Value>,
    ) -> Result<(RenderSpec, Vec<HashMap<String, Value>>)> {
        let (sql, render_spec) = self
            .engine
            .compile_query(prql)
            .context("Failed to compile PRQL query")?;

        let results = self
            .engine
            .execute_query(sql, params)
            .await
            .context("Failed to execute query")?;

        Ok((render_spec, results))
    }

    /// Query and watch for changes
    ///
    /// Compiles the PRQL query, executes it, and sets up CDC streaming for ongoing changes.
    /// Returns the render specification, initial data, and a stream of changes.
    ///
    /// # Arguments
    /// * `prql` - PRQL query string
    /// * `params` - Query parameters (can be empty HashMap)
    ///
    /// # Returns
    /// Tuple of (RenderSpec, Vec<HashMap<String, Value>>, RowChangeStream)
    pub async fn query_and_watch(
        &self,
        prql: String,
        params: HashMap<String, Value>,
    ) -> Result<(RenderSpec, Vec<HashMap<String, Value>>, RowChangeStream)> {
        self.engine
            .query_and_watch(prql, params)
            .await
            .context("Failed to query and watch")
    }

    /// Execute an operation
    ///
    /// Executes an operation on the specified entity.
    ///
    /// # Arguments
    /// * `entity_name` - Entity name (e.g., "blocks", "todoist-task")
    /// * `op_name` - Operation name (e.g., "set_field", "create", "delete")
    /// * `params` - Operation parameters as a StorageEntity (HashMap<String, Value>)
    pub async fn execute_op(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<()> {
        self.engine
            .execute_operation(entity_name, op_name, params)
            .await
            .context(format!(
                "Failed to execute operation '{}' on entity '{}'",
                op_name, entity_name
            ))
    }

    /// Collect stream events with timeout
    ///
    /// Collects events from a RowChangeStream up to a maximum number or until timeout.
    ///
    /// # Arguments
    /// * `stream` - The RowChangeStream to collect from
    /// * `timeout_duration` - Maximum time to wait for events
    /// * `max_events` - Maximum number of events to collect (None = collect all until timeout)
    ///
    /// # Returns
    /// Vector of BatchWithMetadata<RowChange> containing all collected events
    pub async fn collect_stream_events(
        &self,
        mut stream: RowChangeStream,
        timeout_duration: Duration,
        max_events: Option<usize>,
    ) -> Result<Vec<BatchWithMetadata<RowChange>>> {
        let mut events = Vec::new();
        let deadline = tokio::time::Instant::now() + timeout_duration;

        loop {
            // Check if we've reached max events
            if let Some(max) = max_events {
                if events.len() >= max {
                    break;
                }
            }

            // Check if timeout has passed
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            // Try to get next event with remaining timeout
            match timeout(remaining, stream.next()).await {
                Ok(Some(batch)) => {
                    events.push(batch);
                }
                Ok(None) => {
                    // Stream ended
                    break;
                }
                Err(_) => {
                    // Timeout
                    break;
                }
            }
        }

        Ok(events)
    }

    /// Get direct access to the underlying BackendEngine
    ///
    /// Useful for advanced testing scenarios that need direct engine access.
    pub fn engine(&self) -> &Arc<BackendEngine> {
        &self.engine
    }
}

/// Change type for assertion purposes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeType {
    Created,
    Updated,
    Deleted,
}

impl ChangeType {
    /// Check if a ChangeData matches this change type
    fn matches(&self, change: &ChangeData) -> bool {
        match (self, change) {
            (ChangeType::Created, ChangeData::Created { .. }) => true,
            (ChangeType::Updated, ChangeData::Updated { .. }) => true,
            (ChangeType::Deleted, ChangeData::Deleted { .. }) => true,
            _ => false,
        }
    }
}

/// Assert that a specific change type occurred in the collected events
///
/// # Arguments
/// * `batches` - Collected stream events (from `collect_stream_events`)
/// * `expected_type` - Expected change type
/// * `entity_id` - Optional entity ID to filter by (checks `data.get("id")` for Created/Updated, or `id` field for Deleted)
///
/// # Returns
/// Ok(()) if the change was found, Err with descriptive message otherwise
pub fn assert_change_type(
    batches: &[BatchWithMetadata<RowChange>],
    expected_type: ChangeType,
    entity_id: Option<&str>,
) -> Result<()> {
    for batch in batches {
        for row_change in &batch.inner.items {
            if expected_type.matches(&row_change.change) {
                // If entity_id is specified, check if it matches
                if let Some(expected_id) = entity_id {
                    let matches_id = match &row_change.change {
                        ChangeData::Created { data, .. } => data
                            .get("id")
                            .and_then(|v| v.as_string())
                            .map(|id| id == expected_id)
                            .unwrap_or(false),
                        ChangeData::Updated { data, .. } => data
                            .get("id")
                            .and_then(|v| v.as_string())
                            .map(|id| id == expected_id)
                            .unwrap_or(false),
                        ChangeData::Deleted { id, .. } => id.as_str() == expected_id,
                    };

                    if matches_id {
                        return Ok(());
                    }
                } else {
                    // No entity_id filter, any match is good
                    return Ok(());
                }
            }
        }
    }

    // Debug: collect all changes found for better error messages
    let mut found_changes = Vec::new();
    for batch in batches {
        for row_change in &batch.inner.items {
            let (change_type, entity_id_found) = match &row_change.change {
                ChangeData::Created { data, .. } => (
                    "Created",
                    data.get("id")
                        .and_then(|v| v.as_string_owned())
                        .unwrap_or_default(),
                ),
                ChangeData::Updated { data, .. } => (
                    "Updated",
                    data.get("id")
                        .and_then(|v| v.as_string_owned())
                        .unwrap_or_default(),
                ),
                ChangeData::Deleted { id, .. } => ("Deleted", id.clone()),
            };
            found_changes.push(format!("{}({})", change_type, entity_id_found));
        }
    }

    let entity_msg = entity_id
        .map(|id| format!(" with entity_id='{}'", id))
        .unwrap_or_default();
    Err(anyhow::anyhow!(
        "Expected {} change{} not found in {} batches. Found changes: {:?}",
        format!("{:?}", expected_type),
        entity_msg,
        batches.len(),
        found_changes
    ))
}

/// Wait for a specific change type with timeout
///
/// Collects events from the stream until the expected change is found or timeout occurs.
///
/// # Arguments
/// * `stream` - The RowChangeStream to watch
/// * `timeout_duration` - Maximum time to wait
/// * `expected_type` - Expected change type
/// * `entity_id` - Optional entity ID to filter by
///
/// # Returns
/// The matching RowChange if found, Err if timeout or not found
pub async fn wait_for_change(
    mut stream: RowChangeStream,
    timeout_duration: Duration,
    expected_type: ChangeType,
    entity_id: Option<&str>,
) -> Result<RowChange> {
    let deadline = tokio::time::Instant::now() + timeout_duration;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(anyhow::anyhow!(
                "Timeout waiting for {:?} change{}",
                expected_type,
                entity_id
                    .map(|id| format!(" with entity_id='{}'", id))
                    .unwrap_or_default()
            ));
        }

        match timeout(remaining, stream.next()).await {
            Ok(Some(batch)) => {
                for row_change in batch.inner.items {
                    if expected_type.matches(&row_change.change) {
                        // Check entity_id if specified
                        if let Some(expected_id) = entity_id {
                            let matches_id = match &row_change.change {
                                ChangeData::Created { data, .. } => data
                                    .get("id")
                                    .and_then(|v| v.as_string())
                                    .map(|id| id == expected_id)
                                    .unwrap_or(false),
                                ChangeData::Updated { data, .. } => data
                                    .get("id")
                                    .and_then(|v| v.as_string())
                                    .map(|id| id == expected_id)
                                    .unwrap_or(false),
                                ChangeData::Deleted { id, .. } => id.as_str() == expected_id,
                            };

                            if matches_id {
                                return Ok(row_change);
                            }
                        } else {
                            return Ok(row_change);
                        }
                    }
                }
            }
            Ok(None) => {
                return Err(anyhow::anyhow!(
                    "Stream ended before finding {:?} change",
                    expected_type
                ));
            }
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for {:?} change",
                    expected_type
                ));
            }
        }
    }
}

/// Assert that a sequence of changes occurred in order
///
/// Checks that the expected sequence of changes appears in the collected events,
/// in the specified order (but not necessarily consecutively).
///
/// # Arguments
/// * `batches` - Collected stream events
/// * `expected_sequence` - Vector of (ChangeType, Option<entity_id>) tuples representing expected changes
///
/// # Returns
/// Ok(()) if sequence found, Err with descriptive message otherwise
pub fn assert_change_sequence(
    batches: &[BatchWithMetadata<RowChange>],
    expected_sequence: &[(ChangeType, Option<&str>)],
) -> Result<()> {
    let mut found_changes: Vec<(ChangeType, String)> = Vec::new();

    // Collect all changes from batches
    for batch in batches {
        for row_change in &batch.inner.items {
            let change_type = match &row_change.change {
                ChangeData::Created { .. } => ChangeType::Created,
                ChangeData::Updated { .. } => ChangeType::Updated,
                ChangeData::Deleted { .. } => ChangeType::Deleted,
            };

            let entity_id: String = match &row_change.change {
                ChangeData::Created { data, .. } => data
                    .get("id")
                    .and_then(|v| v.as_string_owned())
                    .unwrap_or_default(),
                ChangeData::Updated { data, .. } => data
                    .get("id")
                    .and_then(|v| v.as_string_owned())
                    .unwrap_or_default(),
                ChangeData::Deleted { id, .. } => id.clone(),
            };

            found_changes.push((change_type, entity_id));
        }
    }

    // Check if expected sequence appears in found changes
    let mut expected_idx = 0;
    for (found_type, found_id) in &found_changes {
        if expected_idx >= expected_sequence.len() {
            break;
        }

        let (expected_type, expected_id_opt) = &expected_sequence[expected_idx];
        if *found_type == *expected_type {
            if let Some(expected_id) = expected_id_opt {
                if found_id == *expected_id {
                    expected_idx += 1;
                }
            } else {
                expected_idx += 1;
            }
        }
    }

    if expected_idx < expected_sequence.len() {
        return Err(anyhow::anyhow!(
            "Expected sequence not found. Found {} changes, but only matched {}/{} expected changes",
            found_changes.len(),
            expected_idx,
            expected_sequence.len()
        ));
    }

    Ok(())
}

/// Filter changes by entity ID
///
/// Returns all changes that match the specified entity ID.
///
/// # Arguments
/// * `batches` - Collected stream events
/// * `entity_id` - Entity ID to filter by
///
/// # Returns
/// Vector of RowChange matching the entity ID
pub fn filter_changes_by_entity(
    batches: &[BatchWithMetadata<RowChange>],
    entity_id: &str,
) -> Vec<RowChange> {
    let mut filtered = Vec::new();

    for batch in batches {
        for row_change in &batch.inner.items {
            let matches = match &row_change.change {
                ChangeData::Created { data, .. } => data
                    .get("id")
                    .and_then(|v| v.as_string())
                    .map(|id| id == entity_id)
                    .unwrap_or(false),
                ChangeData::Updated { data, .. } => data
                    .get("id")
                    .and_then(|v| v.as_string())
                    .map(|id| id == entity_id)
                    .unwrap_or(false),
                ChangeData::Deleted { id, .. } => id == entity_id,
            };

            if matches {
                filtered.push(row_change.clone());
            }
        }
    }

    filtered
}

/// Extract all entity IDs from changes
///
/// Returns a set of all unique entity IDs found in the collected events.
///
/// # Arguments
/// * `batches` - Collected stream events
///
/// # Returns
/// Vector of unique entity ID strings
pub fn extract_entity_ids(batches: &[BatchWithMetadata<RowChange>]) -> Vec<String> {
    use std::collections::HashSet;

    let mut ids = HashSet::new();

    for batch in batches {
        for row_change in &batch.inner.items {
            match &row_change.change {
                ChangeData::Created { data, .. } | ChangeData::Updated { data, .. } => {
                    if let Some(Value::String(id)) = data.get("id") {
                        ids.insert(id.clone());
                    }
                }
                ChangeData::Deleted { id, .. } => {
                    ids.insert(id.clone());
                }
            }
        }
    }

    ids.into_iter().collect()
}
