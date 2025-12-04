# Generic Property-Based Testing for Providers

**Date**: 2025-01-10
**Status**: ✅ Approved Design
**Updated**: 2025-01-10 (Incorporated research findings and corrections)
**Related**: codev/reviews/operations-integration-architecture.md

**Key Updates**:
- Corrected mry usage (mock struct implementations, not traits)
- Added Shadow ID pattern documentation references (CouchDB, Firebase, AgileData.org)
- Documented PRQL transparency for Shadow IDs
- Integrated Todoist `temp_id_mapping` batch API approach
- Replaced contracts/anodized discussion with built-in `#[require(...)]` implementation
- Documented automatic precondition handling in property-based testing

## Executive Summary

This document defines a generic testing infrastructure for external system providers (Todoist, Logseq, etc.) using property-based testing with `proptest-state-machine`.

**Key Innovation**: By encoding entity dependencies in operation metadata, we can automatically generate valid test sequences without hard-coding provider-specific knowledge.

**Solution**:
1. Extend `TypeHint` to encode entity ID references (`"entity_id:project"`)
2. Use hybrid macro annotation strategy (convention + escape hatches)
3. Build generic `GenericProviderState<P>` that tracks entity availability
4. Generate only executable operations based on current state
5. Automatically test both live and fake implementations

**Benefits**:
- ✅ **Zero manual work per provider** - same tests for all providers
- ✅ **Automatic edge case discovery** - proptest finds dependency bugs
- ✅ **Validates offline mode** - ensures fake implementations work
- ✅ **Type-safe** - compiler catches mismatches

---

## Problem Statement

### Testing Challenge

External system providers (Todoist, Logseq, etc.) require two implementations:
1. **Live implementation** - API calls to real service
2. **Fake implementation** - In-memory for offline/testing

**Current situation**:
- Each provider requires manual test writing
- Hard to ensure fake matches live behavior
- Edge cases (dependency ordering, state transitions) are easy to miss
- No systematic way to test all operation sequences

### Core Insight

Operations have **parameter dependencies** that form a directed graph:

```
create_project() → project_id
    ↓
create_task(project_id, ...) → task_id
    ↓
set_parent(task_id, parent_task_id)
```

**Key Observation**: At any point in time, only certain operations are **executable** based on which entities exist.

- Empty system → Only parameter-free creates work
- After creating project → Can create tasks
- After creating 2+ tasks → Can set parent relationships

### Goal

Build a **generic test infrastructure** that:
1. Automatically determines which operations can execute
2. Generates valid parameters from current state
3. Explores state space using `proptest-state-machine`
4. Works for ANY provider implementing `OperationProvider`

---

## Architecture Overview

### Two Testing Layers

This document covers **two complementary testing strategies**:

#### Layer 1: Provider Correctness Testing
Tests that a provider (Fake or Real) **implements operations correctly**:
- Generate valid operation sequences using state machine
- Verify operations produce correct results
- Ensure Fake behaves like Real (structural equivalence)

**Applies to**: Any `OperationProvider` implementation

#### Layer 2: QueryableCache Orchestration Testing
Tests that QueryableCache **orchestrates offline/online sync correctly**:
- Queue operations for offline execution
- Execute against Fake immediately (optimistic UI)
- Execute against Real when online (eventual consistency)
- Track which data came from Fake vs Real (`_operation_source`)
- Clean up Fake data once Real confirms
- Handle Real failures/delays/conflicts

**Applies to**: The `QueryableCache<Source, T>` wrapper

### QueryableCache Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        User / UI                              │
│  (expects immediate responses, offline-first)                 │
└───────────────────────────┬──────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────┐
│               QueryableCache<Source, T>                       │
│  ┌────────────────────────────────────────────────────┐     │
│  │ Operation Queue (persistent)                       │     │
│  │  - Stores operation intents for offline execution  │     │
│  │  - Replays when online                            │     │
│  └────────────────────────────────────────────────────┘     │
│                            ↓                                  │
│  ┌─────────────────────┐      ┌──────────────────────┐     │
│  │ Fake (in-memory)    │      │ Real (API/network)   │     │
│  │ • Immediate response│      │ • Eventual response  │     │
│  │ • Always available  │      │ • May be offline     │     │
│  │ • Optimistic UI     │      │ • Source of truth    │     │
│  └─────────────────────┘      └──────────────────────┘     │
│            ↓                              ↓                   │
│  ┌────────────────────────────────────────────────────┐     │
│  │ Local Database (Turso)                             │     │
│  │  • Caches Real data                                │     │
│  │  • Tracks Fake data via _operation_source column  │     │
│  │    - "fake:operation_17681" → from operation #17681│     │
│  │    - "real" → confirmed by Real system             │     │
│  └────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────┘
```

**Key Responsibilities**:
1. **Parallel execution**: Send operations to both Fake and Real
2. **Optimistic UI**: Return Fake result immediately
3. **Eventual consistency**: Replace with Real result when available
4. **Operation tracking**: Mark data with source (`_operation_source`)
5. **Conflict resolution**: Handle Real denying/conflicting with Fake
6. **Cleanup**: Remove Fake data once Real confirms

---

## Solution Architecture

### Layer 1: Provider Correctness Testing

#### Core Components

```
┌─────────────────────────────────────────────────────────┐
│  GenericProviderState<P: OperationProvider>             │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │ State Tracking                                 │    │
│  │  entities: HashMap<String, HashSet<String>>   │    │
│  │    "project" → {"proj-1", "proj-2"}           │    │
│  │    "task"    → {"task-1", "task-2", "task-3"} │    │
│  └────────────────────────────────────────────────┘    │
│                        ↓                                 │
│  ┌────────────────────────────────────────────────┐    │
│  │ Operation Filtering                            │    │
│  │  executable_operations() → Vec<OpDesc>        │    │
│  │    • Query provider.operations()              │    │
│  │    • Filter by can_satisfy_params()           │    │
│  │    • Only return executable ops               │    │
│  └────────────────────────────────────────────────┘    │
│                        ↓                                 │
│  ┌────────────────────────────────────────────────┐    │
│  │ Parameter Generation                           │    │
│  │  generate_params(op, state) → StorageEntity   │    │
│  │    • EntityId params → random from state      │    │
│  │    • Primitive params → proptest::any()       │    │
│  └────────────────────────────────────────────────┘    │
│                        ↓                                 │
│  ┌────────────────────────────────────────────────┐    │
│  │ Execution & State Update                       │    │
│  │  apply(transition) → Result<()>               │    │
│  │    • Execute operation via provider           │    │
│  │    • Update entities map if create            │    │
│  │    • Remove from map if delete                │    │
│  └────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

### Data Flow

```
1. Initial State: entities = {}
   ↓
2. executable_operations() → ["create_project"]
   ↓
3. Generate params: { "name": "My Project" }
   ↓
4. Execute: create_project(name) → "proj-1"
   ↓
5. Update state: entities["project"] = {"proj-1"}
   ↓
6. executable_operations() → ["create_project", "create_task"]
   ↓
7. Generate params: { "project_id": "proj-1", "title": "Task 1" }
   ↓
8. Execute: create_task(project_id, title) → "task-1"
   ↓
9. Update state: entities["task"] = {"task-1"}
   ↓
... and so on
```

---

## Type System Enhancements

### Extended TypeHint Enum

**Location**: `crates/query-render/src/types.rs`

```rust
/// Type hints for operation parameters
///
/// Encodes whether a parameter is a primitive value or an entity reference.
/// Entity references enable the test infrastructure to track dependencies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TypeHint {
    /// Boolean value
    Bool,

    /// String value
    String,

    /// Numeric value
    Number,

    /// Reference to an entity ID
    ///
    /// Example: `EntityId { entity_name: "project" }` means this parameter
    /// must be the ID of a "project" entity.
    EntityId {
        entity_name: String,
    },
}
```

**Serialization Format**:
```json
"bool"
"string"
"number"
{ "type": "EntityId", "entity_name": "project" }
```

**Alternative compact format** (if JSON size matters):
```
"bool"
"string"
"number"
"entity_id:project"
"entity_id:task"
```

### Enhanced OperationParam

```rust
/// Parameter metadata for an operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationParam {
    pub name: String,
    pub type_hint: TypeHint,  // ← Was String, now enum
    pub description: String,
}
```

---

## Macro Annotation Strategy

### Hybrid Approach: Convention + Escape Hatches

**Philosophy**: Make the common case ergonomic, provide explicit control for edge cases.

### Convention-Based Detection

**Rule**: Parameter names matching `{entity_name}_id` are automatically entity references.

```rust
#[operations_trait]
trait MutableTaskDataSource {
    // Automatically detected:
    // - project_id → EntityId { entity_name: "project" }
    // - title → String
    async fn create_task(
        &self,
        project_id: &str,
        title: String,
    ) -> Result<String>;

    // Automatically detected:
    // - task_id → EntityId { entity_name: "task" }
    // - parent_task_id → EntityId { entity_name: "task" }
    async fn set_parent(
        &self,
        task_id: &str,
        parent_task_id: &str,
    ) -> Result<()>;
}
```

**Parsing Logic**:
1. Extract parameter name: `"project_id"`
2. Check if ends with `"_id"`: YES
3. Extract prefix: `"project"`
4. Generate: `TypeHint::EntityId { entity_name: "project" }`

### Attribute-Based Override

For cases where convention doesn't work:

```rust
#[operations_trait]
trait MutableTaskDataSource {
    // Override entity name
    async fn assign_user(
        &self,
        task_id: &str,
        #[entity_ref("account")] user_id: &str,  // ← entity is "account", not "user"
    ) -> Result<()>;

    // Prevent false positive
    async fn validate_format(
        &self,
        #[not_entity] uuid: &str,  // ← ends with "id" but isn't an entity ref
    ) -> Result<bool>;
}
```

**Attribute Syntax**:
- `#[entity_ref("name")]` - Override entity name
- `#[not_entity]` - Prevent convention detection

### Macro Output Enhancement

The `#[operations_trait]` macro generates `OperationDescriptor` with enhanced type hints:

```rust
// Generated by macro
pub mod __operations_MutableTaskDataSource {
    pub fn all_operations() -> Vec<OperationDescriptor> {
        vec![
            OperationDescriptor {
                entity_name: "".to_string(),  // Filled by implementor
                table: "".to_string(),
                id_column: "id".to_string(),
                name: "create_task".to_string(),
                display_name: "Create task".to_string(),
                description: "Create a new task in a project".to_string(),
                required_params: vec![
                    OperationParam {
                        name: "project_id".to_string(),
                        type_hint: TypeHint::EntityId {
                            entity_name: "project".to_string(),
                        },
                        description: "ID of the project".to_string(),
                    },
                    OperationParam {
                        name: "title".to_string(),
                        type_hint: TypeHint::String,
                        description: "Task title".to_string(),
                    },
                ],
            },
            // ... other operations
        ]
    }
}
```

---

## Generic Test Infrastructure

### GenericProviderState

**Location**: `crates/holon/src/testing/generic_provider_state.rs` (new file)

```rust
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use proptest::prelude::*;
use crate::core::{OperationProvider, OperationDescriptor, TypeHint, StorageEntity};

/// Generic state machine for testing any OperationProvider implementation
///
/// Tracks which entities exist and generates only valid operations.
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
    pub result: Option<String>,  // ID if create operation
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
        self.provider.operations()
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
    pub fn generate_params(
        &self,
        op: &OperationDescriptor,
    ) -> BoxedStrategy<StorageEntity> {
        let param_strategies: Vec<_> = op.required_params.iter()
            .map(|param| {
                let name = param.name.clone();
                let strategy = match &param.type_hint {
                    TypeHint::EntityId { entity_name } => {
                        // Get existing entity IDs
                        let ids: Vec<String> = self.entities
                            .get(entity_name)
                            .map(|set| set.iter().cloned().collect())
                            .unwrap_or_default();

                        // Randomly select one
                        prop::sample::select(ids)
                            .prop_map(|id| StorageValue::String(id))
                            .boxed()
                    }
                    TypeHint::Bool => {
                        any::<bool>()
                            .prop_map(StorageValue::Bool)
                            .boxed()
                    }
                    TypeHint::String => {
                        any::<String>()
                            .prop_map(StorageValue::String)
                            .boxed()
                    }
                    TypeHint::Number => {
                        any::<i64>()
                            .prop_map(StorageValue::Number)
                            .boxed()
                    }
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
        let result = self.provider
            .execute_operation(&op.entity_name, &op.name, params.clone())
            .await?;

        // Update state based on operation type
        match op.name.as_str() {
            "create" | name if name.starts_with("create_") => {
                // Extract ID from result
                if let Some(id) = result.get("id").and_then(|v| v.as_string()) {
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
                }
            }
            "delete" | name if name.starts_with("delete_") => {
                // Remove ID from state
                if let Some(id) = params.get("id").and_then(|v| v.as_string()) {
                    if let Some(entity_set) = self.entities.get_mut(&op.entity_name) {
                        entity_set.remove(id);
                    }

                    self.history.push(OperationExecution {
                        entity_name: op.entity_name.clone(),
                        op_name: op.name.clone(),
                        params,
                        result: None,
                    });
                }
            }
            _ => {
                // Update operations don't change entity sets
                self.history.push(OperationExecution {
                    entity_name: op.entity_name.clone(),
                    op_name: op.name.clone(),
                    params,
                    result: None,
                });
            }
        }

        Ok(())
    }

    /// Get operation execution history (for debugging)
    pub fn history(&self) -> &[OperationExecution] {
        &self.history
    }
}
```

### proptest-state-machine Integration

```rust
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};

impl<P: OperationProvider + Clone + 'static> ReferenceStateMachine for GenericProviderState<P> {
    type State = Self;
    type Transition = OperationTransition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(GenericProviderState::new(P::default())).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        // Get operations that can execute
        let executable = state.executable_operations();

        if executable.is_empty() {
            // No operations available - shouldn't happen if at least one
            // parameter-free create exists, but handle gracefully
            return prop::strategy::Just(OperationTransition::NoOp).boxed();
        }

        // Randomly select an operation
        prop::sample::select(executable)
            .prop_flat_map(move |op| {
                // Generate valid params for this operation
                state.generate_params(&op)
                    .prop_map(move |params| {
                        OperationTransition::Execute {
                            op: op.clone(),
                            params,
                        }
                    })
            })
            .boxed()
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            OperationTransition::Execute { op, params } => {
                // Execute operation (blocking version for proptest)
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(state.execute_operation(op, params.clone()))
                    .expect("Operation execution failed");
                state
            }
            OperationTransition::NoOp => state,
        }
    }

    fn check_invariants(state: &Self::State) {
        // Verify state consistency
        for (entity_name, ids) in &state.entities {
            // All IDs should be non-empty
            assert!(ids.iter().all(|id| !id.is_empty()),
                "Empty ID found for entity: {}", entity_name);
        }
    }
}

#[derive(Debug, Clone)]
pub enum OperationTransition {
    Execute {
        op: OperationDescriptor,
        params: StorageEntity,
    },
    NoOp,
}
```

### Usage Example

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::proptest;

    proptest! {
        #[test]
        fn test_todoist_provider_state_machine(
            ops in GenericProviderState::<TodoistProvider>::sequential_operations(1..20)
        ) {
            // proptest-state-machine automatically generates valid operation sequences
            // and checks invariants after each step
        }
    }
}
```

---

## Layer 2: QueryableCache Orchestration Testing

### Overview

While Layer 1 tests that **providers implement operations correctly**, Layer 2 tests that **QueryableCache orchestrates offline/online sync correctly**.

**Key difference**: Layer 1 tests `OperationProvider` implementations (Fake vs Real). Layer 2 tests the `QueryableCache` wrapper that manages both.

### Generic Test Pattern (Following loro_backend_pbt.rs)

Following the proven pattern from `loro_backend_pbt.rs`, we use:
- **Reference implementation**: `QueryableCache<FakeProvider, T>` (always synchronous, no network)
- **System under test**: `QueryableCache<PartiallyMockedRealProvider, T>`
- **Generic over trait bounds**: Not hardcoded to specific providers

```rust
/// Reference state: QueryableCache with Fake only (synchronous, deterministic)
struct ReferenceState<T: HasSchema> {
    cache: QueryableCache<FakeProvider<T>, T>,
    runtime: Arc<tokio::runtime::Runtime>,
}

/// System under test: QueryableCache with mocked Real provider
struct CacheOrchestrationTest<T: HasSchema> {
    cache: QueryableCache<PartiallyMockedRealProvider<T>, T>,
    /// Track which operations were queued
    queued_operations: Vec<OperationIntent>,
    runtime: Arc<tokio::runtime::Runtime>,
}

/// Operation intent (stored in queue)
#[derive(Debug, Clone)]
struct OperationIntent {
    id: u64,
    entity_name: String,
    op_name: String,
    params: StorageEntity,
    timestamp: Instant,
}
```

### Using mry for Partial Mocking

Use `mry` to create **partial mocks** of concrete provider implementations - override specific methods for failure scenarios.

**Key insight**: Mock the concrete struct implementations (not traits) to simulate different failure modes without modifying actual code.

**How mry works**:
- **For structs**: Add `#[mry::mry]` attribute. Instantiate with `mry::new!(Struct { ... })`
- **For trait impls**: Add `#[mry::mry]` to both struct and impl block
- **Mock methods**: Use `mock_method_name()` to override behavior
- **Real impl**: Use `.calls_real_impl()` to delegate to actual implementation

```rust
use mry::*;

// Add mry to struct and impl
#[mry::mry]
struct TodoistProvider {
    api_key: String,
    client: reqwest::Client,
}

#[mry::mry]
impl OperationProvider for TodoistProvider {
    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<StorageEntity> {
        // Real implementation
    }
}

// Test code - mock specific scenarios
let mut real_provider = mry::new!(TodoistProvider {
    api_key: "test-key".to_string(),
    client: reqwest::Client::new(),
});

let fake_provider = FakeTodoistProvider::new();

// Scenario 1: Fake succeeds immediately, Real validates and denies
real_provider
    .mock_execute_operation("todoist-task", "create_task", mry::Any)
    .returns_once(|_, _, params| {
        let title = params.get("title").unwrap().as_string().unwrap();
        if title.len() > 100 {
            Err(ApiError::Validation("Title too long".into()))
        } else {
            // Could also use .calls_real_impl() to use actual implementation
            Ok(StorageEntity::from([
                ("id".to_string(), StorageValue::String("real-456".to_string()))
            ]))
        }
    });

// Scenario 2: Most operations use real implementation
real_provider
    .mock_execute_operation(mry::Any, mry::Any, mry::Any)
    .calls_real_impl();

let cache = QueryableCache::new(real_provider, fake_provider).await?;
```

**Benefits**:
- ✅ Tests real QueryableCache orchestration logic
- ✅ Selective mocking - only mock failure scenarios
- ✅ Can use `.calls_real_impl()` for most operations
- ✅ Provider-independent - works with any concrete provider struct

**mry Documentation**: https://github.com/ryo33/mry

Key `mry` features:
- `.calls_real_impl()` - Delegates to actual implementation
- `.returns_once()` - Overrides with custom behavior (one-time)
- `.returns()` - Overrides permanently
- `mry::Any` - Matches any parameter value

### Operation Queueing

QueryableCache must persist operation intents for offline execution.

**Design decision**: `execute_operation` takes `OperationIntent` directly (without ID). The database auto-generates the ID when queuing.

```rust
/// Operation intent (without ID - DB generates it)
#[derive(Debug, Clone)]
pub struct OperationIntent {
    pub entity_name: String,
    pub op_name: String,
    pub params: StorageEntity,
    pub timestamp: Instant,
}

impl<S, T> QueryableCache<S, T>
where
    S: CrudOperationProvider<T>,
    T: HasSchema + Send + Sync + 'static,
{
    /// Execute operation with offline support
    pub async fn execute_operation(
        &self,
        intent: OperationIntent,
    ) -> Result<StorageEntity> {
        // 1. Store intent in queue (DB returns auto-incremented ID)
        let op_id = self.queue_operation(intent.clone()).await?;

        // 2. Execute against Fake immediately (optimistic UI)
        let fake_result = self.execute_on_fake(
            &intent.entity_name,
            &intent.op_name,
            intent.params.clone()
        ).await?;

        // 3. Mark result as from Fake
        if let Some(id) = fake_result.get("id") {
            self.mark_operation_source(id, format!("fake:operation_{}", op_id)).await?;
        }

        // 4. Try to execute against Real (async, don't wait)
        let real_source = self.source.clone();
        let cache_clone = self.clone();
        let intent_clone = intent.clone();

        tokio::spawn(async move {
            match real_source.execute_operation(
                &intent_clone.entity_name,
                &intent_clone.op_name,
                intent_clone.params
            ).await {
                Ok(real_result) => {
                    // Real succeeded - replace Fake data
                    cache_clone.reconcile_real_result(op_id, fake_result, real_result).await;
                }
                Err(e) => {
                    cache_clone.handle_real_error(op_id, e).await;
                }
            }
        });

        // 5. Return Fake result immediately (snappy UI)
        Ok(fake_result)
    }

    /// Queue operation intent, returns DB-generated ID
    async fn queue_operation(&self, intent: OperationIntent) -> Result<u64> {
        let db = self.db.read().await;
        let db = db.as_ref().ok_or("Database not initialized")?;
        let conn = db.connect()?;

        let sql = "INSERT INTO operation_queue (entity_name, op_name, params, timestamp)
                   VALUES (?, ?, ?, ?) RETURNING id";

        let mut rows = conn.query(&sql, params![
            intent.entity_name,
            intent.op_name,
            serde_json::to_string(&intent.params)?,
            intent.timestamp.elapsed().as_millis() as i64,
        ]).await?;

        let row = rows.next().await?.ok_or("No ID returned")?;
        let id: i64 = row.get(0)?;
        Ok(id as u64)
    }
}
```

### Operation Source Tracking (`_operation_source`)

Add column to database schema to track data origin:

```rust
impl<S, T> QueryableCache<S, T> {
    async fn initialize_schema(&self) -> Result<()> {
        let schema = T::schema();

        // Add _operation_source column to track data origin
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (\n  {},\n  _operation_source TEXT NOT NULL DEFAULT 'real'\n)",
            schema.table_name,
            schema.fields.iter()
                .map(|f| format!("{} {}", f.name, f.sql_type))
                .collect::<Vec<_>>()
                .join(",\n  ")
        );

        // ... execute SQL
    }

    async fn mark_operation_source(&self, entity_id: &str, source: String) -> Result<()> {
        let schema = T::schema();
        let sql = format!(
            "UPDATE {} SET _operation_source = ? WHERE id = ?",
            schema.table_name
        );

        self.execute_sql(&sql, &[source, entity_id.to_string()]).await
    }

    async fn cleanup_fake_data(&self, op_id: u64) -> Result<()> {
        let schema = T::schema();
        let sql = format!(
            "DELETE FROM {} WHERE _operation_source = ?",
            schema.table_name
        );

        self.execute_sql(&sql, &[format!("fake:operation_{}", op_id)]).await
    }
}
```

### Error Handling & Reconciliation

Distinguish between **transient** (retry) and **permanent** (clean up) errors:

```rust
impl<S, T> QueryableCache<S, T> {
    async fn handle_real_error(&self, op_id: u64, error: ApiError) {
        match error {
            // Transient errors: Keep Fake data, retry later
            ApiError::Timeout(_) | ApiError::NetworkError(_) | ApiError::RateLimit(_) => {
                log::warn!("Transient error for op {}: {:?}. Will retry.", op_id, error);
                // Operation stays in queue with backoff
                self.schedule_retry(op_id).await;
            }

            // Permanent errors: Remove Fake data immediately (prevent divergence)
            ApiError::Validation(_) | ApiError::Unauthorized(_) | ApiError::NotFound(_) => {
                log::error!("Permanent error for op {}: {:?}. Cleaning up Fake data.", op_id, error);

                // Remove Fake data from database
                if let Err(e) = self.cleanup_fake_data(op_id).await {
                    log::error!("Failed to cleanup fake data for op {}: {}", op_id, e);
                }

                // Remove from queue (won't retry)
                if let Err(e) = self.dequeue_operation(op_id).await {
                    log::error!("Failed to dequeue op {}: {}", op_id, e);
                }

                // Notify UI that operation failed
                self.notify_operation_failed(op_id, error).await;
            }
        }
    }

    async fn reconcile_real_result(
        &self,
        op_id: u64,
        fake_result: StorageEntity,
        real_result: StorageEntity,
    ) -> Result<()> {
        // 1. Extract IDs
        let fake_id = fake_result.get("id").ok_or("Missing ID in fake result")?;
        let real_id = real_result.get("id").ok_or("Missing ID in real result")?;

        // 2. Update ID mapping (Fake ID → Real ID) - see Shadow ID section below
        self.update_id_mapping(fake_id, real_id).await?;

        // 3. Replace Fake data with Real data
        self.upsert_to_cache(&real_result, "real").await?;

        // 4. Clean up Fake data
        self.cleanup_fake_data(op_id).await?;

        // 5. Remove operation from queue (successfully synced)
        self.dequeue_operation(op_id).await?;

        // 6. Notify UI of ID change (optional)
        self.notify_id_change(fake_id, real_id).await;

        Ok(())
    }
}
```

### Test Scenarios

#### Scenario 1: Happy Path (Online)

```rust
#[tokio::test]
async fn test_online_execution_happy_path() {
    let fake = FakeTodoistProvider::new();
    let mut mock_real = MockTodoistProvider::new();

    // Real succeeds normally
    mock_real
        .expect_create_task()
        .returning(|project_id, title| {
            Ok("real-task-id".to_string())
        });

    let cache = QueryableCache::new(mock_real, fake).await.unwrap();

    // Execute operation
    let result = cache.create_task("proj-1", "Buy milk".to_string()).await.unwrap();

    // Immediately returns Fake result
    assert!(result.starts_with("fake-"));

    // Wait for Real to sync
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify Real data replaced Fake
    let task = cache.get_by_id("real-task-id").await.unwrap().unwrap();
    assert_eq!(task.title, "Buy milk");

    // Verify Fake data was cleaned up
    assert!(cache.get_by_id(&result).await.unwrap().is_none());
}
```

#### Scenario 2: Real Denies Operation (Permanent Error)

```rust
#[tokio::test]
async fn test_real_denies_operation() {
    let fake = FakeTodoistProvider::new();
    let mut mock_real = MockTodoistProvider::new();

    // Real rejects (validation error - permanent)
    mock_real
        .expect_create_task()
        .returning(|_, title| {
            if title.len() > 100 {
                Err(ApiError::Validation("Title too long".into()))
            } else {
                Ok("real-task-id".to_string())
            }
        });

    let cache = QueryableCache::new(mock_real, fake).await.unwrap();

    // Execute operation (title too long)
    let fake_id = cache.create_task("proj-1", "A".repeat(150)).await.unwrap();

    // Fake succeeds immediately
    assert!(fake_id.starts_with("fake-"));

    // Wait for Real to respond and clean up
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify Fake data was REMOVED (prevent divergence)
    assert!(cache.get_by_id(&fake_id).await.unwrap().is_none());

    // Verify operation removed from queue (won't retry permanent errors)
    assert!(!cache.has_queued_operations().await);

    // Verify UI was notified of failure
    assert_eq!(cache.last_error().await, Some(ApiError::Validation("Title too long".into())));
}
```

**Key difference from transient errors**: Fake data is **immediately removed** to prevent local/remote divergence. User sees the operation failed.

#### Scenario 3: Real is Offline (Timeout)

```rust
#[tokio::test]
async fn test_real_offline_timeout() {
    let fake = FakeTodoistProvider::new();
    let mut mock_real = MockTodoistProvider::new();

    // Real times out
    mock_real
        .expect_create_task()
        .returning(|_, _| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Err("Timeout".into())
        });

    let cache = QueryableCache::new(mock_real, fake).await.unwrap();

    // Execute operation
    let start = Instant::now();
    let result = cache.create_task("proj-1", "Buy milk".to_string()).await.unwrap();
    let elapsed = start.elapsed();

    // Returns immediately (< 100ms), doesn't wait for Real
    assert!(elapsed < Duration::from_millis(100));

    // Fake data is available
    let task = cache.get_by_id(&result).await.unwrap().unwrap();
    assert_eq!(task.title, "Buy milk");
    assert!(task._operation_source.starts_with("fake:"));
}
```

#### Scenario 4: Conflicting ID from Real (Shadow ID Solution)

**This is the default case** - most external systems generate their own IDs.

**Challenge**: Offline operations create dependency chains with fake IDs:
1. Create project offline → `fake-proj-1`
2. Create task offline with `project_id="fake-proj-1"` → `fake-task-1`
3. Real responds: project → `"real-abc"`, task → `"real-xyz"`
4. **Problem**: Task's `project_id` still points to non-existent `"fake-proj-1"`!

**Solution: Shadow ID Mapping**

The Shadow ID pattern is an architectural approach used in offline-first systems. It uses **stable internal UUIDs** everywhere in local DB - no cascade updates needed!

**References and Reading Material**:
1. **AgileData.org - Shadow Information in O/R Mapping**: https://agiledata.org/essays/mappingobjects.html
   - Discusses shadow information, temporary IDs, and persistence tracking
2. **CouchDB Document IDs**: https://docs.couchdb.org/en/stable/api/document/common.html#id
   - Client-generated UUIDs as stable `_id` values (sidesteps shadow ID mapping)
3. **Firebase Offline Persistence**: https://firebase.google.com/docs/database/web/offline-capabilities
   - App-level mapping of temporary IDs to push IDs in IndexedDB

**Pattern Components**:
- **Internal stable ID** (UUID, never changes) - used in all local DB FKs
- **External temporary ID** (generated by fake provider) - placeholder
- **External permanent ID** (assigned by real API) - final value
- **Mapping table** tracks: `internal_id → external_id`

**Why it works**: All foreign keys use `internal_id`, which never changes. Only the mapping table gets updated when Real responds.

```sql
CREATE TABLE id_mappings (
    internal_id TEXT PRIMARY KEY,  -- Stable UUID (never changes)
    external_id TEXT,               -- Real system ID (updated after sync)
    source TEXT NOT NULL,           -- 'todoist', 'logseq', etc.
    command_id TEXT NOT NULL,       -- Operation that created it
    state TEXT DEFAULT 'pending'    -- 'pending', 'synced', 'failed'
);

-- Entity tables use internal_id everywhere
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,            -- Internal UUID
    project_id TEXT REFERENCES projects(id),  -- Also internal UUID!
    title TEXT,
    _operation_source TEXT DEFAULT 'real'
);
```

**Implementation Flow**:

```rust
impl<S, T> QueryableCache<S, T> {
    pub async fn execute_operation(&self, intent: OperationIntent) -> Result<StorageEntity> {
        // 1. Generate stable internal UUID
        let internal_id = Uuid::new_v4().to_string();

        // 2. Execute against Fake (optimistic)
        let fake_result = self.fake.execute_operation(&intent).await?;
        let fake_external_id = fake_result.get("id").unwrap();

        // 3. Store ID mapping (pending state)
        self.db.execute(
            "INSERT INTO id_mappings (internal_id, external_id, source, state)
             VALUES (?, ?, 'todoist', 'pending')",
            params![internal_id, fake_external_id],
        ).await?;

        // 4. Store entity using INTERNAL ID
        self.db.execute(
            "INSERT INTO tasks (id, project_id, title, _operation_source)
             VALUES (?, ?, ?, ?)",
            params![
                internal_id,                          // ← Internal UUID
                intent.params.get("project_id"),      // ← Also internal UUID!
                intent.params.get("title"),
                format!("fake:operation_{}", op_id)
            ],
        ).await?;

        // 5. Spawn background sync
        tokio::spawn(async move {
            let real_result = self.real.execute_operation(&intent).await?;
            let real_external_id = real_result.get("id").unwrap();

            // 6. Update mapping (now synced)
            self.db.execute(
                "UPDATE id_mappings SET external_id = ?, state = 'synced'
                 WHERE internal_id = ?",
                params![real_external_id, internal_id],
            ).await?;

            // 7. NO CASCADE UPDATES NEEDED!
            // All FKs still point to same internal_id
        });

        // Return internal ID to UI
        Ok(internal_id)
    }

    /// Resolve internal ID to external ID (for API calls)
    pub async fn resolve_external_id(&self, internal_id: &str) -> Result<Option<String>> {
        let row: Option<(String,)> = self.db
            .query_row(
                "SELECT external_id FROM id_mappings WHERE internal_id = ?",
                params![internal_id],
            )
            .await?;

        Ok(row.map(|r| r.0))
    }
}
```

**PRQL Integration - Completely Transparent to Users**:

Shadow IDs are an **implementation detail** of QueryableCache, not exposed to users. PRQL queries always use internal UUIDs:

```prql
# User always works with internal UUIDs - completely transparent!
from tasks
filter project_id == "uuid-1"  # Internal UUID reference (never changes)
select [title, project_id]
```

**How it works**:
- **PRQL queries**: Operate directly on internal IDs (no mapping needed)
- **API calls**: QueryableCache automatically resolves internal → external IDs before calling real provider
- **User experience**: Users never see or interact with the mapping table

```rust
impl<S, T> QueryableCache<S, T> {
    /// Execute PRQL query - IDs are already internal, no resolution needed
    pub async fn query_prql(&self, query: &str) -> Result<Vec<T>> {
        self.db.execute_prql(query).await  // Direct execution!
    }

    /// Only when calling external API do we need resolution
    pub async fn execute_operation(&self, op: &str, params: StorageEntity) -> Result<()> {
        let external_params = self.resolve_ids_for_api(params).await?;  // ← Resolution here
        self.real_provider.execute_operation(op, external_params).await
    }
}
```

**Benefits**:
1. ✅ **No cascade updates** - FKs never change
2. ✅ **Simple** - One table tracks all mappings
3. ✅ **Fast** - No recursive FK rewriting
4. ✅ **Flexible** - Works with any external system
5. ✅ **Proven** - Used in production systems (CouchDB, Firebase - see references above)
6. ✅ **PRQL transparent** - Users work with internal IDs naturally

**Example: Create Project + Task Offline**:

```rust
// 1. Create project offline
let internal_proj_id = cache.create_project("My Project").await?;
// DB: projects(id="uuid-1", name="My Project")
// Mapping: uuid-1 → fake-proj-123 (pending)

// 2. Create task offline (references internal project ID)
let internal_task_id = cache.create_task(internal_proj_id, "Task 1").await?;
// DB: tasks(id="uuid-2", project_id="uuid-1", title="Task 1")
//                       ↑ Points to internal UUID
// Mapping: uuid-2 → fake-task-456 (pending)

// 3. Real system responds (background)
// Mapping: uuid-1 → real-proj-abc (synced)
// Mapping: uuid-2 → real-task-xyz (synced)

// 4. Task FK UNCHANGED!
// tasks.project_id still points to "uuid-1" - correct!
```

**Todoist Sync API Integration with temp_id_mapping**:

Todoist's batch sync API provides `temp_id_mapping` which perfectly integrates with our Shadow ID approach:

**How Todoist temp_id works**:
1. Each creation command includes a `temp_id` (any string, we use our internal UUID)
2. Submit batch of commands in one sync request
3. Response includes `temp_id_mapping: { "<temp_id>": "<real_todoist_id>" }`

**Implementation**:
```rust
impl TodoistProvider {
    async fn execute_operations_batch(&self, operations: Vec<OperationIntent>) -> Result<BatchResult> {
        let mut commands = Vec::new();

        for op in operations {
            let command = match op.op_name.as_str() {
                "create_project" => {
                    json!({
                        "type": "project_add",
                        "temp_id": op.internal_id,  // ← Use internal UUID as temp_id!
                        "uuid": Uuid::new_v4(),     // ← Idempotency key
                        "args": { "name": op.params.get("name").unwrap() }
                    })
                }
                "create_task" => {
                    // Resolve project_id from internal to external ID
                    let internal_project_id = op.params.get("project_id").unwrap();
                    let external_project_id = self.resolve_external_id(internal_project_id).await?;

                    json!({
                        "type": "item_add",
                        "temp_id": op.internal_id,  // ← Internal UUID as temp_id
                        "uuid": Uuid::new_v4(),
                        "args": {
                            "content": op.params.get("title").unwrap(),
                            "project_id": external_project_id,  // ← Resolved real Todoist project ID
                        }
                    })
                }
                _ => continue,
            };
            commands.push(command);
        }

        // Send batch request to Todoist Sync API
        let response: TodoistBatchResponse = self.client
            .post("https://api.todoist.com/sync/v9/sync")
            .json(&json!({ "commands": commands }))
            .send().await?
            .json().await?;

        // Process temp_id_mapping to update our Shadow ID mappings
        for (temp_id, real_id) in response.temp_id_mapping {
            self.update_id_mapping(&temp_id, &real_id).await?;
        }

        Ok(BatchResult { ... })
    }
}
```

**Example flow**:
```rust
// 1. Create project offline → internal UUID "uuid-1"
// 2. Create task offline → internal UUID "uuid-2", references project_id="uuid-1"
// 3. Batch sync to Todoist:
//    - Command 1: project_add with temp_id="uuid-1"
//    - Command 2: item_add with temp_id="uuid-2", project_id=RESOLVED_REAL_ID("uuid-1")
// 4. Response: temp_id_mapping = { "uuid-1": "2342342342", "uuid-2": "8978978978" }
// 5. Update mappings: uuid-1 → 2342342342, uuid-2 → 8978978978
// 6. Local DB unchanged: tasks.project_id still "uuid-1" ✅
```

**Key integration points**:
- ✅ **Use internal UUID as temp_id** - gives us stable references
- ✅ **Resolve foreign keys before submission** - convert internal IDs to external IDs for API params
- ✅ **Process temp_id_mapping in response** - update Shadow ID mapping table
- ✅ **Preserve internal UUID references** - all local DB FKs stay unchanged

**Test**:

```rust
#[tokio::test]
async fn test_shadow_id_cascade() {
    let cache = QueryableCache::new(mock_real, mock_fake).await.unwrap();

    // Create project offline
    let proj_uuid = cache.create_project("Work").await.unwrap();
    assert!(proj_uuid.starts_with("uuid-")); // Internal UUID

    // Create task referencing project (offline)
    let task_uuid = cache.create_task(proj_uuid.clone(), "Task 1").await.unwrap();

    // Verify FK uses internal UUID
    let task = cache.get_by_id(&task_uuid).await.unwrap().unwrap();
    assert_eq!(task.project_id, proj_uuid); // ← Internal UUID

    // Wait for sync
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify FK STILL correct (not updated)
    let task = cache.get_by_id(&task_uuid).await.unwrap().unwrap();
    assert_eq!(task.project_id, proj_uuid); // ← Still internal UUID!

    // Can resolve to external IDs when needed
    assert_eq!(
        cache.resolve_external_id(&proj_uuid).await.unwrap(),
        Some("real-proj-abc".to_string())
    );
}
```

**Alternative Approaches Considered**:

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| **ID Mapping Table** (chosen) | Simple, no FK updates | Extra join on external queries | ✅ Best |
| **FK Rewriting** | Clean DB afterward | Complex, recursive, error-prone | ❌ Too complex |
| **Logical IDs** | No mapping needed | Not all systems support | ❌ Not universal |
| **Operation Replay** | Bulletproof | Most complex | ❌ Overkill |

<!--
I'm not yet so convinced...
-->

### Property-Based Testing with proptest-state-machine

Combine with Layer 1 infrastructure for comprehensive testing:

```rust
#[derive(Debug, Clone)]
enum CacheTransition {
    // Layer 1: Provider operations
    ExecuteOperation { op: OperationDescriptor, params: StorageEntity },

    // Layer 2: Cache orchestration
    GoOffline,
    GoOnline,
    FlushQueue,
    SimulateRealDenial { op_name: String },
    SimulateRealTimeout { op_name: String },
}

impl ReferenceStateMachine for ReferenceState<TodoistTask> {
    type State = Self;
    type Transition = CacheTransition;

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        // Combine Layer 1 operation generation with Layer 2 orchestration
        let layer1_ops = state.cache.executable_operations()
            .into_iter()
            .map(|op| CacheTransition::ExecuteOperation { op, params })
            .collect();

        let layer2_ops = vec![
            CacheTransition::GoOffline,
            CacheTransition::GoOnline,
            CacheTransition::FlushQueue,
        ];

        prop::sample::select(layer1_ops.chain(layer2_ops)).boxed()
    }
}
```
<!--
Do we need to hard-code `TodoistTask` here?
In the best case, I would like to have something like the Scala Discipline library (https://typelevel.org/blog/2013/11/17/discipline.html)
that provides laws for all providers and I only need to write very little code to apply the laws to a concrete provider.
Please run `jj log -p -r qwxyzwum` (which removed a crate that used this approach) and extract the relevant code from there.
-->

### Verification Strategy

After each transition, verify:

```rust
impl StateMachineTest for CacheOrchestrationTest<TodoistTask> {
    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        // 1. Verify final state matches (ignore _operation_source)
        let ref_tasks = ref_state.cache.get_all().await.unwrap();
        let sut_tasks = state.cache.get_all().await.unwrap();

        assert_eq!(ref_tasks.len(), sut_tasks.len());

        // 2. Verify queue state
        if state.is_offline {
            assert!(!state.queued_operations.is_empty(), "Should have queued ops while offline");
        }

        // 3. Verify no orphaned Fake data after sync
        if state.is_online && state.queued_operations.is_empty() {
            let fake_count = state.cache.count_fake_data().await.unwrap();
            assert_eq!(fake_count, 0, "Should have no Fake data after sync");
        }

        // 4. Verify ID mappings are consistent
        for fake_id in state.id_mappings.keys() {
            let real_id = state.id_mappings.get(fake_id).unwrap();
            assert!(
                sut_tasks.iter().any(|t| t.id == *real_id),
                "Mapped Real ID should exist"
            );
        }
    }
}
```

---

## Edge Cases & Solutions

### 1. Circular Dependencies

**Problem**: What if `create_task` requires `project_id`, but `create_project` requires `task_id`?

**Solution**: At least one entity type must have a **parameter-free create** operation.

**Validation**:
```rust
/// Verify provider has at least one executable operation without entity dependencies
///
/// This ensures the provider can bootstrap from empty state. Accepts any operation
/// that doesn't require entity IDs - not just "create" operations.
///
/// Examples of valid bootstrap operations:
/// - `create_project(name: String)` - Standard create
/// - `setup_sample_data()` - Initialization helper
/// - `import_from_csv(path: String)` - Bulk import
pub fn validate_provider_bootstrap<P: OperationProvider>(provider: &P) -> Result<()> {
    let executable_from_empty = provider.operations().iter().any(|op| {
        op.required_params.iter().all(|p| {
            !matches!(p.type_hint, TypeHint::EntityId { .. })
        })
    });

    if !executable_from_empty {
        return Err(anyhow!(
            "Provider has no operations executable from empty state - circular dependency"
        ));
    }

    Ok(())
}
```

**Note**: We don't hardcode "create" - ANY operation without entity dependencies works. This supports:
- Non-empty initial states (system comes with default entities)
- Setup operations (`setup_sample_data()`)
- Import operations (`import_from_csv()`)

If no operations are executable, `proptest-state-machine` will error when trying to generate transitions.

### 2. Self-References

**Problem**: `set_parent(task_id, parent_task_id)` - both are task IDs. Need `task_id != parent_task_id`.

**Solution**: Use our built-in `#[require(...)]` attribute to automatically handle preconditions!

### Our Built-in `#[require(...)]` Implementation

**Location**: `crates/holon-macros/src/lib.rs`

We've implemented a custom precondition system that's integrated with `#[operations_trait]`. It automatically extracts preconditions and makes them available for property-based testing.

**Usage Example**:
```rust
#[operations_trait]
#[async_trait]
trait MutableTaskDataSource {
    /// Set parent task relationship
    #[require(task_id != parent_task_id)]  // ← Automatically prevents self-reference!
    #[require(task_id.len() > 0 && parent_task_id.len() > 0)]
    async fn set_parent(&self, task_id: &str, parent_task_id: &str) -> Result<()>;

    /// Set priority (must be 1-4)
    #[require(priority >= 1 && priority <= 4)]
    async fn set_priority(&self, id: &str, priority: i64) -> Result<()>;
}
```

**How It Works**:
1. `#[operations_trait]` macro extracts all `#[require(...)]` attributes
2. Generates type-safe precondition closures with parameter extraction
3. Stores in `OperationDescriptor.precondition` field
4. `GenericProviderState` automatically uses them to filter executable operations

**Automatic Integration with Property-Based Testing**:
```rust
impl<P: OperationProvider> GenericProviderState<P> {
    pub fn executable_operations(&self) -> Vec<OperationDescriptor> {
        self.provider.operations()
            .into_iter()
            .filter(|op| {
                // 1. Check structural dependencies (entity existence)
                if !self.can_satisfy_params(op) {
                    return false;
                }

                // 2. Check operation preconditions (from #[require(...)])
                if let Some(ref precondition) = op.precondition {
                    let params = self.generate_params_as_any(op);
                    match precondition(&params) {
                        Ok(true) => true,       // Precondition satisfied
                        Ok(false) => false,     // Precondition failed
                        Err(_) => false,        // Precondition evaluation error
                    }
                } else {
                    true  // No precondition, always executable
                }
            })
            .collect()
    }
}
```

**What happens in proptest**:
1. `transitions()` generates all possible operations
2. `executable_operations()` filters using `#[require(...)]` preconditions
3. Operations where `task_id == parent_task_id` are **never generated**
4. Only valid operations reach `apply()`
5. If operation fails in `apply()` → bug in precondition or implementation → proptest shrinks to minimal case

**Benefits**:
- ✅ **Zero boilerplate** - preconditions extracted automatically
- ✅ **Type-safe** - macro generates correct type conversions
- ✅ **Composable** - multiple `#[require(...)]` attributes combined with `&&`
- ✅ **Provider-independent** - works for any trait with `#[operations_trait]`
- ✅ **No external dependencies** - built into our macro system
- ✅ **proptest shrinking still works** - if precondition is wrong, proptest finds it

### 3. Multi-Entity Dependencies

**Problem**: "Move task to project" needs `task_id` AND `project_id`

**Solution**: Automatically handled by `can_satisfy_params()` - ALL dependencies must be satisfied.

```rust
// Only executable when BOTH projects and tasks exist
async fn move_task(
    &self,
    task_id: &str,
    new_project_id: &str,
) -> Result<()>;
```

### 4. Cross-Entity Constraints

**Problem**: "Create subtask" might require `parent_task_id` to be from same project

**Solution**: Out of scope for initial implementation. These are **semantic constraints**, not structural dependencies.

**Future Enhancement**: Add constraint validators:
```rust
#[operations_trait]
trait MutableTaskDataSource {
    #[constraint(same_project(task_id, parent_task_id))]
    async fn create_subtask(
        &self,
        task_id: &str,
        parent_task_id: &str,
    ) -> Result<String>;
}
```

### 5. Field Name Validation

**Problem**: How do we know valid field names for `set_field(id, field, value)`?

**Solution**: Out of scope. `set_field` is intentionally untyped.

**Workaround**: Use specific operations (`set_title`, `set_priority`) which don't need field metadata.

### 6. Entity Type Naming Conflicts

**Problem**: What if two providers use different names for same concept?

**Example**:
- TodoistProvider: `"todoist-task"`
- LogseqProvider: `"logseq-block"`

**Solution**: Entity names are **namespaced per provider**. No conflict.

**State tracking**:
```rust
entities: {
    "todoist-task": {"task-1", "task-2"},
    "logseq-block": {"block-1", "block-2"},
}
```

---

## Implementation Plan

### Phase 1: Type System Enhancement (Non-Breaking)

**Files to modify**:
- `crates/query-render/src/types.rs`

**Tasks**:
1. Convert `OperationParam.type_hint` from `String` to `TypeHint` enum
2. Add `TypeHint::EntityId { entity_name }` variant
3. Implement `Serialize`/`Deserialize` with backward compatibility
4. Add migration helper for old string format

**Risk**: Low - Can maintain backward compat with custom serde

**Validation**:
```rust
#[test]
fn test_type_hint_serde_backward_compat() {
    // Old format still deserializes
    let old_json = r#""string""#;
    let hint: TypeHint = serde_json::from_str(old_json).unwrap();
    assert_eq!(hint, TypeHint::String);

    // New format works
    let new_json = r#"{"type":"EntityId","entity_name":"project"}"#;
    let hint: TypeHint = serde_json::from_str(new_json).unwrap();
    assert_eq!(hint, TypeHint::EntityId { entity_name: "project".to_string() });
}
```

### Phase 2: Macro Enhancement (Non-Breaking)

**Files to modify**:
- `crates/holon-macros/src/lib.rs`

**Tasks**:
1. Parse parameter names for `_id` suffix
2. Extract entity name from prefix
3. Generate `TypeHint::EntityId` instead of `TypeHint::String` for entity refs
4. Add attribute parsing for `#[entity_ref("name")]` and `#[not_entity]`

**Parsing Logic**:
```rust
fn parse_param_type_hint(param: &syn::FnArg) -> TypeHint {
    // Extract param name and attributes
    let (param_name, attrs) = extract_param_info(param);

    // Check for explicit override
    if let Some(entity_name) = find_entity_ref_attr(&attrs) {
        return TypeHint::EntityId { entity_name };
    }

    if has_not_entity_attr(&attrs) {
        return TypeHint::String;  // or infer from type
    }

    // Convention: {entity_name}_id
    if let Some(entity_name) = param_name.strip_suffix("_id") {
        return TypeHint::EntityId {
            entity_name: entity_name.to_string(),
        };
    }

    // Infer from Rust type
    infer_from_rust_type(param)
}
```

**Risk**: Medium - Touches macro internals

**Validation**:
```rust
#[test]
fn test_macro_entity_detection() {
    #[operations_trait]
    trait TestOps {
        async fn create_task(project_id: &str, title: String) -> Result<String>;
    }

    let ops = __operations_TestOps::all_operations();
    let create_op = ops.iter().find(|o| o.name == "create_task").unwrap();

    assert_eq!(
        create_op.required_params[0].type_hint,
        TypeHint::EntityId { entity_name: "project".to_string() }
    );
    assert_eq!(
        create_op.required_params[1].type_hint,
        TypeHint::String
    );
}
```

### Phase 3: Generic Test Infrastructure (New Module)

**Files to create**:
- `crates/holon/src/testing/mod.rs`
- `crates/holon/src/testing/generic_provider_state.rs`

**Files to modify**:
- `crates/holon/src/lib.rs` - Add `pub mod testing` under `#[cfg(test)]`
- `crates/holon/Cargo.toml` - Add `proptest-state-machine` dev-dependency

**Tasks**:
1. Implement `GenericProviderState<P>` struct
2. Implement state tracking (`entities: HashMap<String, HashSet<String>>`)
3. Implement `executable_operations()` filtering
4. Implement `generate_params()` with proptest strategies
5. Implement `execute_operation()` with state updates
6. Integrate with `proptest-state-machine` crate

**Risk**: Medium-High - New complex component

### Phase 4: Todoist Integration (Validation)

**Files to modify**:
- `crates/holon-todoist/src/lib.rs`
- `crates/holon-todoist/tests/property_tests.rs` (new)

**Tasks**:
1. Ensure Todoist operations have correct `TypeHint` metadata
2. Create proptest using `GenericProviderState<TodoistProvider>`
3. Run against live API (with cleanup)
4. Run against fake implementation
5. Verify both pass same tests

**Test Template**:
```rust
#[cfg(test)]
mod property_tests {
    use super::*;
    use holon::testing::GenericProviderState;
    use proptest::proptest;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn test_todoist_fake_provider_consistency(
            ops in GenericProviderState::<TodoistFakeProvider>::sequential_operations(5..20)
        ) {
            // Automatically tests all valid operation sequences
        }

        #[test]
        #[ignore]  // Only run with --ignored flag
        fn test_todoist_live_provider_consistency(
            ops in GenericProviderState::<TodoistLiveProvider>::sequential_operations(5..10)
        ) {
            // Test against real API (with cleanup in Drop)
        }
    }
}
```

**Risk**: Low - Just applies existing infrastructure

### Phase 5: Documentation & Examples

**Files to create**:
- `crates/holon/docs/provider-testing-guide.md`

**Files to modify**:
- `README.md` - Add testing section
- `crates/holon-todoist/README.md` - Document testing approach

**Tasks**:
1. Write provider testing guide
2. Add examples for new provider authors
3. Document entity dependency conventions
4. Document attribute override syntax

**Risk**: Low - Documentation only

---

## Design Decisions

### ✅ Decision 1: Enum TypeHint (vs String)

**Rationale**: Type safety and extensibility
- Compile-time validation of entity references
- Easy to add new variants (e.g., `List`, `Optional`)
- Self-documenting code

**Alternatives Considered**:
- Keep as `String` with format `"entity_id:project"` - rejected (no type safety)
- Use generic type `EntityId<T>` - rejected (too heavyweight, requires type system changes)

### ✅ Decision 2: Hybrid Annotation Strategy

**Rationale**: Ergonomics for common case, flexibility for edge cases
- 90% of params follow `{entity}_id` convention
- Attributes provide escape hatch without magic
- Low cognitive overhead

**Alternatives Considered**:
- Pure convention - rejected (no way to handle exceptions)
- Pure attributes - rejected (too verbose)
- Generic types - rejected (too invasive to existing code)

### ✅ Decision 3: State Tracking in Test, Not Provider

**Rationale**: Separation of concerns
- Providers don't need test-specific code
- Same provider can be tested different ways
- Cleaner architecture

**Alternatives Considered**:
- Providers track their own state - rejected (mixes concerns)
- Separate test fixture - accepted (this approach)

### ✅ Decision 4: proptest-state-machine Integration

**Rationale**: Proven tool for state machine testing
- Mature library with good ergonomics
- Automatic shrinking of failing cases
- Built-in invariant checking

**Alternatives Considered**:
- Raw proptest - rejected (too much manual state management)
- Quickcheck - rejected (less Rust-native)
- Custom framework - rejected (reinventing wheel)

### ✅ Decision 5: No Semantic Constraint Validation (Phase 1)

**Rationale**: YAGNI - start simple
- Structural dependencies (entity existence) cover 95% of cases
- Semantic constraints (same project, valid field names) are rare
- Can be added later if needed

**Future Enhancement**: Add `#[constraint(...)]` attribute when needed

---

## Future Enhancements

### 1. Semantic Constraint Validation

**Problem**: "Create subtask" requires parent task to be in same project

**Solution**: Add constraint DSL
```rust
#[operations_trait]
trait MutableTaskDataSource {
    #[constraint(same_field(task_id, parent_task_id, "project_id"))]
    async fn create_subtask(&self, task_id: &str, parent_task_id: &str) -> Result<String>;
}
```

### 2. Optional Entity References

**Problem**: "Set parent" where `parent_id` can be `None` (unparent operation)

**Solution**: Add `optional_params` to `OperationDescriptor`
```rust
OperationDescriptor {
    // ...
    optional_params: vec![
        OperationParam {
            name: "parent_task_id".to_string(),
            type_hint: TypeHint::EntityId { entity_name: "task" },
            description: "New parent (omit to unparent)".to_string(),
        },
    ],
}
```

### 3. Field Metadata for set_field

**Problem**: Can't validate field names in `set_field(id, field, value)`

**Solution**: Add field registry to entity metadata
```rust
pub struct EntityMetadata {
    pub entity_name: String,
    pub fields: Vec<FieldDescriptor>,
}

pub struct FieldDescriptor {
    pub name: String,
    pub type_hint: TypeHint,
    pub description: String,
}
```

### 4. Multi-Provider Testing

**Problem**: Test interactions between providers (Todoist ↔ Logseq sync)

**Solution**: Composite `GenericProviderState`
```rust
pub struct MultiProviderState {
    providers: HashMap<String, Box<dyn OperationProvider>>,
    entities: HashMap<String, HashSet<String>>,
}
```

### 5. Invariant Specification

**Problem**: Want to check provider-specific invariants (e.g., "completed tasks have completion_date")

**Solution**: Add `check_invariants` hook
```rust
impl GenericProviderState<TodoistProvider> {
    fn check_invariants(&self) {
        for task_id in &self.entities["todoist-task"] {
            let task = self.provider.get(task_id).await.unwrap();
            if task.completed {
                assert!(task.completion_date.is_some());
            }
        }
    }
}
```

---

## Summary

**This design provides a powerful, reusable testing infrastructure that:**

1. ✅ **Eliminates manual test writing** - Works for any `OperationProvider`
2. ✅ **Finds edge cases automatically** - proptest explores state space
3. ✅ **Validates offline mode** - Tests fake implementations
4. ✅ **Type-safe** - Compiler catches entity reference errors
5. ✅ **Ergonomic** - Convention-based with escape hatches
6. ✅ **Extensible** - Can add semantic constraints, field metadata, etc.

**Key Innovation**: By encoding parameter dependencies in operation metadata, we transform a manual testing problem into an automatic graph exploration problem.

**Next Steps**: Implement Phase 1 (TypeHint enum) and Phase 2 (macro enhancement) to validate the approach with Todoist provider.
