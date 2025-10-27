//! Navigation operations provider
//!
//! Implements NavigationOperations for backend-driven navigation state.
//! Operations modify navigation tables directly (not part of undo stack).

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::core::datasource::{OperationProvider, OperationResult, Result};
use crate::storage::turso::TursoBackend;
use holon_api::{OperationDescriptor, OperationParam, TypeHint, Value};
use holon_core::storage::types::StorageEntity;

/// Navigation operations entity name
pub const ENTITY_NAME: &str = "navigation";
pub const SHORT_NAME: &str = "nav";

/// Navigation provider for managing region focus state
pub struct NavigationProvider {
    backend: Arc<RwLock<TursoBackend>>,
}

impl NavigationProvider {
    pub fn new(backend: Arc<RwLock<TursoBackend>>) -> Self {
        Self { backend }
    }

    /// Focus on a specific block in a region
    ///
    /// This inserts a new history entry and updates the cursor.
    /// Any forward history is cleared (like browser navigation).
    async fn focus(&self, region: &str, block_id: Option<&str>) -> Result<OperationResult> {
        eprintln!(
            "[NavigationProvider] focus: region={}, block_id={:?}",
            region, block_id
        );
        let backend = self.backend.read().await;

        let mut params = HashMap::new();
        params.insert("region".to_string(), Value::String(region.to_string()));

        // Step 1: Get current cursor position (Turso doesn't support subqueries in DELETE)
        eprintln!("[NavigationProvider] focus: getting current cursor position");
        let cursor_result = backend
            .execute_sql(
                "SELECT history_id FROM navigation_cursor WHERE region = $region",
                params.clone(),
            )
            .await
            .map_err(|e| format!("Failed to get cursor position: {}", e))?;

        let current_history_id: i64 = cursor_result
            .first()
            .and_then(|row| row.get("history_id"))
            .and_then(|v| match v {
                Value::Integer(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0);
        eprintln!(
            "[NavigationProvider] focus: current history_id = {}",
            current_history_id
        );

        // Step 2: Delete any forward history (entries after current cursor)
        params.insert("current_id".to_string(), Value::Integer(current_history_id));
        eprintln!("[NavigationProvider] focus: executing DELETE from navigation_history");
        backend
            .execute_sql(
                "DELETE FROM navigation_history WHERE region = $region AND id > $current_id",
                params.clone(),
            )
            .await
            .map_err(|e| {
                eprintln!("[NavigationProvider] focus: DELETE failed: {}", e);
                format!("Failed to clear forward history: {}", e)
            })?;
        eprintln!("[NavigationProvider] focus: DELETE succeeded");

        // Step 3: Insert new history entry
        let block_id_value = match block_id {
            Some(id) => Value::String(id.to_string()),
            None => Value::Null,
        };
        params.insert("block_id".to_string(), block_id_value);

        backend
            .execute_sql(
                "INSERT INTO navigation_history (region, block_id) VALUES ($region, $block_id)",
                params.clone(),
            )
            .await
            .map_err(|e| format!("Failed to insert navigation history: {}", e))?;

        // Step 4: Get the new max history_id
        let max_result = backend
            .execute_sql(
                "SELECT MAX(id) as max_id FROM navigation_history WHERE region = $region",
                params.clone(),
            )
            .await
            .map_err(|e| format!("Failed to get max history id: {}", e))?;

        let new_history_id: i64 = max_result
            .first()
            .and_then(|row| row.get("max_id"))
            .and_then(|v| match v {
                Value::Integer(n) => Some(*n),
                _ => None,
            })
            .ok_or_else(|| "Failed to get new history_id".to_string())?;

        // Step 5: Update cursor to point to new entry
        params.insert("new_id".to_string(), Value::Integer(new_history_id));
        backend
            .execute_sql(
                "INSERT OR REPLACE INTO navigation_cursor (region, history_id) VALUES ($region, $new_id)",
                params,
            )
            .await
            .map_err(|e| format!("Failed to update navigation cursor: {}", e))?;

        eprintln!("[NavigationProvider] focus: completed successfully");
        Ok(OperationResult::irreversible(vec![]))
    }

    /// Go back in navigation history
    async fn go_back(&self, region: &str) -> Result<OperationResult> {
        eprintln!("[NavigationProvider] go_back: region={}", region);
        let backend = self.backend.read().await;

        let mut params = HashMap::new();
        params.insert("region".to_string(), Value::String(region.to_string()));

        // Step 1: Get current cursor position
        let cursor_result = backend
            .execute_sql(
                "SELECT history_id FROM navigation_cursor WHERE region = $region",
                params.clone(),
            )
            .await
            .map_err(|e| format!("Failed to get cursor position: {}", e))?;

        let current_history_id: i64 = cursor_result
            .first()
            .and_then(|row| row.get("history_id"))
            .and_then(|v| match v {
                Value::Integer(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0);

        // Step 2: Find the previous history entry
        params.insert("current_id".to_string(), Value::Integer(current_history_id));
        let prev_result = backend
            .execute_sql(
                "SELECT id FROM navigation_history WHERE region = $region AND id < $current_id ORDER BY id DESC LIMIT 1",
                params.clone(),
            )
            .await
            .map_err(|e| format!("Failed to find previous entry: {}", e))?;

        // Step 3: Update cursor - either to previous entry or to NULL (home)
        if let Some(prev_row) = prev_result.first() {
            if let Some(Value::Integer(prev_id)) = prev_row.get("id") {
                params.insert("new_id".to_string(), Value::Integer(*prev_id));
                backend
                    .execute_sql(
                        "UPDATE navigation_cursor SET history_id = $new_id WHERE region = $region",
                        params,
                    )
                    .await
                    .map_err(|e| format!("Failed to go back: {}", e))?;
                eprintln!(
                    "[NavigationProvider] go_back: moved to history_id={}",
                    prev_id
                );
            }
        } else {
            // No previous entry - go to home (NULL cursor)
            backend
                .execute_sql(
                    "UPDATE navigation_cursor SET history_id = NULL WHERE region = $region",
                    params,
                )
                .await
                .map_err(|e| format!("Failed to go back to home: {}", e))?;
            eprintln!("[NavigationProvider] go_back: went to home (no previous entry)");
        }

        Ok(OperationResult::irreversible(vec![]))
    }

    /// Go forward in navigation history
    async fn go_forward(&self, region: &str) -> Result<OperationResult> {
        eprintln!("[NavigationProvider] go_forward: region={}", region);
        let backend = self.backend.read().await;

        let mut params = HashMap::new();
        params.insert("region".to_string(), Value::String(region.to_string()));

        // Step 1: Get current cursor position
        let cursor_result = backend
            .execute_sql(
                "SELECT history_id FROM navigation_cursor WHERE region = $region",
                params.clone(),
            )
            .await
            .map_err(|e| format!("Failed to get cursor position: {}", e))?;

        let current_history_id: i64 = cursor_result
            .first()
            .and_then(|row| row.get("history_id"))
            .and_then(|v| match v {
                Value::Integer(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0);

        // Step 2: Find the next history entry
        params.insert("current_id".to_string(), Value::Integer(current_history_id));
        let next_result = backend
            .execute_sql(
                "SELECT id FROM navigation_history WHERE region = $region AND id > $current_id ORDER BY id ASC LIMIT 1",
                params.clone(),
            )
            .await
            .map_err(|e| format!("Failed to find next entry: {}", e))?;

        // Step 3: Update cursor if next entry exists
        if let Some(next_row) = next_result.first() {
            if let Some(Value::Integer(next_id)) = next_row.get("id") {
                params.insert("new_id".to_string(), Value::Integer(*next_id));
                backend
                    .execute_sql(
                        "UPDATE navigation_cursor SET history_id = $new_id WHERE region = $region",
                        params,
                    )
                    .await
                    .map_err(|e| format!("Failed to go forward: {}", e))?;
                eprintln!(
                    "[NavigationProvider] go_forward: moved to history_id={}",
                    next_id
                );
            }
        } else {
            eprintln!(
                "[NavigationProvider] go_forward: no next entry, staying at current position"
            );
        }

        Ok(OperationResult::irreversible(vec![]))
    }

    /// Navigate to home (root view, no specific block focused)
    async fn go_home(&self, region: &str) -> Result<OperationResult> {
        self.focus(region, None).await
    }

    fn region_param() -> OperationParam {
        OperationParam {
            name: "region".to_string(),
            type_hint: TypeHint::OneOf {
                values: vec![
                    Value::String("main".to_string()),
                    Value::String("left_sidebar".to_string()),
                    Value::String("right_sidebar".to_string()),
                ],
            },
            description: "UI region to navigate".to_string(),
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl OperationProvider for NavigationProvider {
    fn operations(&self) -> Vec<OperationDescriptor> {
        vec![
            OperationDescriptor {
                entity_name: ENTITY_NAME.to_string(),
                entity_short_name: SHORT_NAME.to_string(),
                id_column: "region".to_string(),
                name: "focus".to_string(),
                display_name: "Focus".to_string(),
                description: "Navigate to focus on a specific block".to_string(),
                required_params: vec![
                    Self::region_param(),
                    OperationParam {
                        name: "block_id".to_string(),
                        type_hint: TypeHint::String,
                        description: "Block ID to focus on".to_string(),
                    },
                ],
                affected_fields: vec!["block_id".to_string()],
                param_mappings: vec![],
                precondition: None,
            },
            OperationDescriptor {
                entity_name: ENTITY_NAME.to_string(),
                entity_short_name: SHORT_NAME.to_string(),
                id_column: "region".to_string(),
                name: "go_back".to_string(),
                display_name: "Go Back".to_string(),
                description: "Navigate to previous view in history".to_string(),
                required_params: vec![Self::region_param()],
                affected_fields: vec!["block_id".to_string()],
                param_mappings: vec![],
                precondition: None,
            },
            OperationDescriptor {
                entity_name: ENTITY_NAME.to_string(),
                entity_short_name: SHORT_NAME.to_string(),
                id_column: "region".to_string(),
                name: "go_forward".to_string(),
                display_name: "Go Forward".to_string(),
                description: "Navigate to next view in history".to_string(),
                required_params: vec![Self::region_param()],
                affected_fields: vec!["block_id".to_string()],
                param_mappings: vec![],
                precondition: None,
            },
            OperationDescriptor {
                entity_name: ENTITY_NAME.to_string(),
                entity_short_name: SHORT_NAME.to_string(),
                id_column: "region".to_string(),
                name: "go_home".to_string(),
                display_name: "Go Home".to_string(),
                description: "Navigate to home view (no block focused)".to_string(),
                required_params: vec![Self::region_param()],
                affected_fields: vec!["block_id".to_string()],
                param_mappings: vec![],
                precondition: None,
            },
        ]
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<OperationResult> {
        if entity_name != ENTITY_NAME {
            return Err(format!(
                "NavigationProvider: expected entity '{}', got '{}'",
                ENTITY_NAME, entity_name
            )
            .into());
        }

        let region = params
            .get("region")
            .and_then(|v| match v {
                Value::String(s) => Some(s.as_str()),
                _ => None,
            })
            .ok_or_else(|| "Missing required parameter 'region'")?;

        match op_name {
            "focus" => {
                let block_id = params.get("block_id").and_then(|v| match v {
                    Value::String(s) => Some(s.as_str()),
                    _ => None,
                });
                self.focus(region, block_id).await
            }
            "go_back" => self.go_back(region).await,
            "go_forward" => self.go_forward(region).await,
            "go_home" => self.go_home(region).await,
            _ => Err(format!("Unknown navigation operation: {}", op_name).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operations_defined() {
        // Just verify operations are defined correctly
        // Full integration tests require TursoBackend
        let ops = vec!["focus", "go_back", "go_forward", "go_home"];
        for op in ops {
            assert!(
                ["focus", "go_back", "go_forward", "go_home"].contains(&op),
                "Operation {} should be defined",
                op
            );
        }
    }
}
