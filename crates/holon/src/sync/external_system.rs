//! External system integration trait
//!
//! Provides a unified interface for interacting with external systems (real or fake)
//! using contract-based specifications for validation and simulation.
//!
//! System-specific implementations should be in separate crates (e.g., holon-todoist).

use anyhow::Result;
use async_trait::async_trait;
use holon_api::Value;
use std::collections::HashMap;

/// Unified interface for external systems (both real and fake)
///
/// Implementations can either:
/// - Generate fake responses using contracts (for optimistic updates)
/// - Call real HTTP APIs and validate responses against contracts
///
/// # Example Implementation
///
/// ```ignore
/// use holon::sync::ExternalSystem;
/// use async_trait::async_trait;
///
/// pub struct MySystemFake;
///
/// #[async_trait]
/// impl ExternalSystem for MySystemFake {
///     async fn apply_command(
///         &self,
///         command_type: &str,
///         inputs: &HashMap<String, Value>,
///         state_access: &dyn StateAccess<Entity>,
///     ) -> Result<HashMap<String, Value>> {
///         let contract = get_contract(command_type)?;
///         let mut rng = thread_rng();
///         contract.generate(inputs, state_access, &mut rng)
///     }
///
///     fn system_id(&self) -> &str {
///         "my-system-fake"
///     }
/// }
/// ```
#[async_trait]
pub trait ExternalSystem: Send + Sync {
    /// Apply a command and return the response
    ///
    /// # Arguments
    /// - `command_type`: Operation type (e.g., "indent", "create_task")
    /// - `inputs`: Command parameters
    /// - `state_access`: Access to current system state for contract evaluation
    ///
    /// # Returns
    /// Response fields as defined by the contract
    async fn apply_command(
        &self,
        command_type: &str,
        inputs: &HashMap<String, Value>,
    ) -> Result<HashMap<String, Value>>;

    /// Get the system identifier (e.g., "todoist", "notion", "todoist-fake")
    fn system_id(&self) -> &str;
}

/// Helper to convert serde_json::Value to holon Value enum
///
/// Useful for external system implementations that receive JSON from HTTP APIs.
pub fn json_to_value(v: &serde_json::Value) -> Result<Value> {
    match v {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Integer(i))
            } else {
                Ok(Value::Json(serde_json::to_string(v).unwrap_or_default()))
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(s.clone())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Ok(Value::Json(serde_json::to_string(v).unwrap_or_default()))
        }
    }
}
