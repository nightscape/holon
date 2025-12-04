//! Generic state machine for testing any OperationProvider implementation
//!
//! Tracks which entities exist and generates only valid operations based on
//! parameter dependencies encoded in operation metadata.

use crate::core::datasource::{OperationProvider, Result};
use crate::storage::types::StorageEntity;
use holon_api::Value;
use holon_api::{OperationDescriptor, TypeHint};
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Generic state machine for testing any OperationProvider implementation
///
/// Tracks which entities exist and generates only valid operations.
#[derive(Clone)]
pub struct GenericProviderState<P: OperationProvider> {
    /// Map of entity_name → set of entity IDs
    ///
    /// Example: { "project": {"proj-1", "proj-2"}, "task": {"task-1"} }
    entities: HashMap<String, HashSet<String>>,

    /// The provider being tested
    provider: Arc<P>,

    /// Operation execution history (for debugging)
    history: Vec<OperationExecution>,
}

#[derive(Debug, Clone)]
pub struct OperationExecution {
    pub entity_name: String,
    pub op_name: String,
    pub params: StorageEntity,
    pub result: Option<String>, // ID if create operation
}

impl<P: OperationProvider> GenericProviderState<P> {
    /// Create initial state with empty entity sets
    pub fn new(provider: P) -> Self {
        Self {
            entities: HashMap::new(),
            provider: Arc::new(provider),
            history: Vec::new(),
        }
    }

    /// Get all operations that can be executed in current state
    ///
    /// Filters operations to only those whose parameter dependencies
    /// can be satisfied by existing entities.
    pub fn executable_operations(&self) -> Vec<OperationDescriptor> {
        self.provider
            .operations()
            .into_iter()
            .filter(|op| self.can_satisfy_params(op))
            .collect()
    }

    /// Check if all required parameters can be satisfied
    fn can_satisfy_params(&self, op: &OperationDescriptor) -> bool {
        op.required_params.iter().all(|param| {
            match &param.type_hint {
                TypeHint::EntityId { entity_name } => {
                    // Need at least one entity of this type
                    self.entities
                        .get(entity_name)
                        .map(|ids| !ids.is_empty())
                        .unwrap_or(false)
                }
                // Primitives can always be generated
                TypeHint::Bool | TypeHint::String | TypeHint::Number => true,
            }
        })
    }

    /// Generate valid parameters for an operation
    ///
    /// - EntityId params: randomly select from existing entities
    /// - Primitive params: generate using proptest::any()
    pub fn generate_params(&self, op: &OperationDescriptor) -> BoxedStrategy<StorageEntity> {
        let param_strategies: Vec<_> = op
            .required_params
            .iter()
            .map(|param| {
                let name = param.name.clone();
                let strategy: BoxedStrategy<Value> = match &param.type_hint {
                    TypeHint::EntityId { entity_name } => {
                        // Get existing entity IDs
                        let ids: Vec<String> = self
                            .entities
                            .get(entity_name)
                            .map(|set| set.iter().cloned().collect())
                            .unwrap_or_default();

                        if ids.is_empty() {
                            // No entities available - this shouldn't happen if can_satisfy_params worked
                            // Return a strategy that will fail
                            return (name, Just(Value::String("".to_string())).boxed());
                        }

                        // Randomly select one
                        prop::sample::select(ids)
                            .prop_map(|id| Value::String(id))
                            .boxed()
                    }
                    TypeHint::Bool => any::<bool>().prop_map(Value::Boolean).boxed(),
                    TypeHint::String => any::<String>().prop_map(Value::String).boxed(),
                    TypeHint::Number => any::<i64>().prop_map(Value::Integer).boxed(),
                };

                (name, strategy)
            })
            .collect();

        // Combine all parameter strategies into StorageEntity
        combine_params(param_strategies).boxed()
    }

    /// Execute an operation and update state
    pub async fn execute_operation(
        &mut self,
        op: &OperationDescriptor,
        params: StorageEntity,
    ) -> Result<()> {
        // Execute via provider
        self.provider
            .execute_operation(&op.entity_name, &op.name, params.clone())
            .await?;

        // Update state based on operation type
        let op_name = op.name.as_str();
        if op_name == "create" || op_name.starts_with("create_") {
            // Try to extract ID from provider's get_last_created_id method
            let created_id = self.provider.get_last_created_id();

            if let Some(id) = created_id {
                // Add the created entity to our tracking
                self.entities
                    .entry(op.entity_name.clone())
                    .or_default()
                    .insert(id.clone());

                self.history.push(OperationExecution {
                    entity_name: op.entity_name.clone(),
                    op_name: op.name.clone(),
                    params,
                    result: Some(id),
                });
            } else {
                // Provider doesn't support ID extraction, just track the operation
                self.history.push(OperationExecution {
                    entity_name: op.entity_name.clone(),
                    op_name: op.name.clone(),
                    params,
                    result: None,
                });
            }
        } else if op_name == "delete" || op_name.starts_with("delete_") {
            // Remove ID from state
            if let Some(id_value) = params.get("id") {
                if let Some(id) = id_value.as_string() {
                    if let Some(entity_set) = self.entities.get_mut(&op.entity_name) {
                        entity_set.remove(id);
                    }
                }
            }

            self.history.push(OperationExecution {
                entity_name: op.entity_name.clone(),
                op_name: op.name.clone(),
                params,
                result: None,
            });
        } else {
            // Update operations don't change entity sets
            self.history.push(OperationExecution {
                entity_name: op.entity_name.clone(),
                op_name: op.name.clone(),
                params,
                result: None,
            });
        }

        Ok(())
    }

    /// Get operation execution history (for debugging)
    pub fn history(&self) -> &[OperationExecution] {
        &self.history
    }

    /// Manually add an entity ID to state (for testing)
    pub fn add_entity(&mut self, entity_name: String, id: String) {
        self.entities.entry(entity_name).or_default().insert(id);
    }

    /// Get all entity IDs for a given entity type
    pub fn get_entities(&self, entity_name: &str) -> Vec<String> {
        self.entities
            .get(entity_name)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get all entities (for testing/invariants)
    pub fn all_entities(&self) -> &HashMap<String, HashSet<String>> {
        &self.entities
    }
}

/// Combine parameter strategies into a StorageEntity strategy
fn combine_params(
    param_strategies: Vec<(String, BoxedStrategy<Value>)>,
) -> BoxedStrategy<StorageEntity> {
    if param_strategies.is_empty() {
        return Just(HashMap::new()).boxed();
    }

    // Build strategy by combining all parameter strategies
    let mut strategies = param_strategies.into_iter();
    let (first_name, first_strategy) = strategies.next().unwrap();

    let initial = first_strategy.prop_map({
        let first_name = first_name.clone();
        move |value| {
            let mut map = HashMap::new();
            map.insert(first_name.clone(), value);
            map
        }
    });

    strategies.fold(initial.boxed(), |acc, (name, value_strategy)| {
        (acc, value_strategy)
            .prop_map(move |(mut entity, value)| {
                entity.insert(name.clone(), value);
                entity
            })
            .boxed()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::datasource::OperationProvider;
    use async_trait::async_trait;

    // Mock provider for testing
    struct MockProvider {
        operations_list: Vec<OperationDescriptor>,
    }

    #[async_trait]
    impl OperationProvider for MockProvider {
        fn operations(&self) -> Vec<OperationDescriptor> {
            self.operations_list.clone()
        }

        async fn execute_operation(
            &self,
            _entity_name: &str,
            _op_name: &str,
            _params: StorageEntity,
        ) -> Result<Option<holon_api::Operation>> {
            // Mock implementation - return None (no inverse operation)
            Ok(None)
        }
    }

    #[test]
    fn test_empty_state_only_parameter_free_ops() {
        let provider = MockProvider {
            operations_list: vec![
                OperationDescriptor {
                    entity_name: "project".to_string(),
                    entity_short_name: "project".to_string(),
                    id_column: "id".to_string(),
                    name: "create_project".to_string(),
                    display_name: "Create Project".to_string(),
                    description: "Create a new project".to_string(),
                    required_params: vec![holon_api::OperationParam {
                        name: "name".to_string(),
                        type_hint: TypeHint::String,
                        description: "Project name".to_string(),
                    }],
                    affected_fields: vec![],
                    param_mappings: vec![],
                    precondition: None,
                },
                OperationDescriptor {
                    entity_name: "task".to_string(),
                    entity_short_name: "task".to_string(),
                    id_column: "id".to_string(),
                    name: "create_task".to_string(),
                    display_name: "Create Task".to_string(),
                    description: "Create a new task".to_string(),
                    required_params: vec![holon_api::OperationParam {
                        name: "project_id".to_string(),
                        type_hint: TypeHint::EntityId {
                            entity_name: "project".to_string(),
                        },
                        description: "Project ID".to_string(),
                    }],
                    affected_fields: vec![],
                    param_mappings: vec![],
                    precondition: None,
                },
            ],
        };

        let state = GenericProviderState::new(provider);
        let executable = state.executable_operations();

        // Only create_project should be executable (no dependencies)
        assert_eq!(executable.len(), 1);
        assert_eq!(executable[0].name, "create_project");
    }

    #[test]
    fn test_with_entities_both_ops_executable() {
        let provider = MockProvider {
            operations_list: vec![
                OperationDescriptor {
                    entity_name: "project".to_string(),
                    entity_short_name: "project".to_string(),
                    id_column: "id".to_string(),
                    name: "create_project".to_string(),
                    display_name: "Create Project".to_string(),
                    description: "Create a new project".to_string(),
                    required_params: vec![holon_api::OperationParam {
                        name: "name".to_string(),
                        type_hint: TypeHint::String,
                        description: "Project name".to_string(),
                    }],
                    affected_fields: vec![],
                    param_mappings: vec![],
                    precondition: None,
                },
                OperationDescriptor {
                    entity_name: "task".to_string(),
                    entity_short_name: "task".to_string(),
                    id_column: "id".to_string(),
                    name: "create_task".to_string(),
                    display_name: "Create Task".to_string(),
                    description: "Create a new task".to_string(),
                    required_params: vec![holon_api::OperationParam {
                        name: "project_id".to_string(),
                        type_hint: TypeHint::EntityId {
                            entity_name: "project".to_string(),
                        },
                        description: "Project ID".to_string(),
                    }],
                    affected_fields: vec![],
                    param_mappings: vec![],
                    precondition: None,
                },
            ],
        };

        let mut state = GenericProviderState::new(provider);
        state.add_entity("project".to_string(), "proj-1".to_string());

        let executable = state.executable_operations();

        // Both operations should be executable now
        assert_eq!(executable.len(), 2);
    }
}
