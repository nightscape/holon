//! Polling utilities for waiting on async conditions

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, Instant};

use holon::api::{RowChange, RowChangeStream};
use holon::testing::e2e_test_helpers::E2ETestContext;
use holon_api::Value;
use tokio_stream::StreamExt;

use crate::widget_state::{WidgetLocator, WidgetStateModel};

/// Wait until a condition is met or timeout expires.
/// Returns true if condition was met, false if timed out.
pub async fn wait_until<F, Fut>(predicate: F, timeout: Duration, poll_interval: Duration) -> bool
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if predicate().await {
            return true;
        }
        tokio::time::sleep(poll_interval).await;
    }
    false
}

/// Wait until expected block count is reached in a query result.
pub async fn wait_for_block_count(
    ctx: &E2ETestContext,
    prql: &str,
    expected_count: usize,
    timeout: Duration,
) -> Vec<HashMap<String, Value>> {
    let poll_interval = Duration::from_millis(10);
    let start = Instant::now();
    let mut last_result = Vec::new();

    while start.elapsed() < timeout {
        match ctx.query(prql.to_string(), HashMap::new()).await {
            Ok(spec) => {
                last_result = spec.data.clone();
                if spec.data.len() == expected_count {
                    return spec.data;
                }
            }
            Err(_) => {}
        }
        tokio::time::sleep(poll_interval).await;
    }
    last_result
}

/// Wait until a specific block exists in the database.
pub async fn wait_for_block(ctx: &E2ETestContext, block_id: &str, timeout: Duration) -> bool {
    // Include render() call since compile_query requires it for widget rendering
    let prql = format!(
        "from blocks | filter id == \"{}\" | select {{id}} | render (list item_template:(text this.id))",
        block_id
    );
    let poll_interval = Duration::from_millis(50);

    wait_until(
        || async {
            ctx.query(prql.clone(), HashMap::new())
                .await
                .map(|spec| !spec.data.is_empty())
                .unwrap_or(false)
        },
        timeout,
        poll_interval,
    )
    .await
}

/// Wait until file content matches a condition.
pub async fn wait_for_file_condition<F>(file_path: &Path, condition: F, timeout: Duration) -> bool
where
    F: Fn(&str) -> bool,
{
    let poll_interval = Duration::from_millis(10);
    let start = Instant::now();

    while start.elapsed() < timeout {
        if let Ok(content) = tokio::fs::read_to_string(file_path).await {
            if condition(&content) {
                return true;
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
    false
}

/// Drain all pending events from a CDC stream without blocking.
///
/// Returns all events that were available immediately. Use this to
/// process any pending changes before making assertions.
pub async fn drain_stream(stream: &mut RowChangeStream) -> Vec<RowChange> {
    let mut changes = Vec::new();
    let drain_timeout = Duration::from_millis(10);

    loop {
        match tokio::time::timeout(drain_timeout, stream.next()).await {
            Ok(Some(batch)) => {
                changes.extend(batch.inner.items);
            }
            _ => break,
        }
    }

    changes
}

/// Wait until a widget matching the locator contains the expected text.
///
/// Drains CDC events and applies them to the state model until the text
/// is found or timeout expires.
///
/// Returns true if text was found, false if timed out.
pub async fn wait_for_text_in_widget(
    stream: &mut RowChangeStream,
    state: &mut WidgetStateModel,
    locator: &WidgetLocator,
    expected_text: &str,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;

    // First check current state
    if state.contains_text(locator, expected_text) {
        return true;
    }

    // Then wait for changes
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(batch)) => {
                for change in batch.inner.items {
                    state.apply_change(&change);
                }
                if state.contains_text(locator, expected_text) {
                    return true;
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    false
}
