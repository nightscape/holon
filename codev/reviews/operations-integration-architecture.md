# Operations Integration Architecture

**Date**: 2025-01-10
**Status**: ✅ Approved Design
**Related**: codev/plans/0001-reactive-prql-rendering.md (Phase 3.4)

## Executive Summary

This document defines how to integrate the new trait-based Operations system (Phase 3.4) with RenderEngine's generic query interface.

**Key Discovery**: Operations are **already wired into RenderSpec** at compile-time via lineage analysis. We don't need runtime discovery - we just need to enhance the existing wiring with rich metadata from OperationProvider.

**Solution**:
1. Simplify OperationWiring to reference OperationDescriptor (no duplication)
2. Use Composite Pattern - both caches and dispatcher implement OperationProvider
3. Generate dispatch via macro helper functions
4. Bridge lineage analysis to OperationProvider lookup during compilation

---

## Problem Statement

### Core Challenge

RenderEngine executes generic PRQL queries returning untyped `Vec<StorageEntity>` rows, but the new Operations system is built on typed `QueryableCache<T>` instances with trait-based operations.

**We need to:**
1. Discover which operations are available for items in query results
2. Execute those operations correctly by dispatching to the right cache
3. Support heterogeneous queries (JOINs across different entity types)

### Current Architectural State

**Two parallel, incompatible systems exist:**

**OLD System** (render_engine.rs:41):
- Flat `OperationRegistry` with string-based dispatch
- Operations directly mutate TursoBackend via SQL
- Tightly coupled to RenderEngine

**NEW System** (Phase 3.4, implemented but not integrated):
- Typed trait operations on `QueryableCache<T>`
- Operations delegate to CrudOperationProvider, updates arrive via stream
- Clean separation: Cache/DataSource/Provider layers
- Type-safe with trait composition

### Key Realization

**Operations ARE already wired into RenderSpec!**

From `query-render/src/types.rs:48-53`:
```rust
pub enum RenderExpr {
    FunctionCall {
        name: String,
        args: Vec<Arg>,
        operations: Vec<OperationWiring>,  // ← Already embedded here!
    },
    // ...
}
```

The lineage analysis system already:
- Tracks table provenance for each column
- Infers widget types from render expressions
- Embeds OperationWiring into FunctionCall nodes

**We just need to enhance this with rich metadata from OperationProvider.**

---

## Final Architecture Design

### Core Components

1. **OperationProvider trait** - Unified interface (Composite Pattern)
2. **OperationDispatcher** - Composite that aggregates providers
3. **QueryableCache<T>** - Leaf that handles specific entity types
4. **OperationWiring** - Simplified to reference descriptor
5. **Macro-generated dispatch** - Helper functions per trait

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Frontend (Flutter)                        │
│  - Receives RenderSpec with operations pre-wired             │
│  - Connects operations to UI widgets                         │
│  - Dispatches: (entity_name, op_name, params)               │
└───────────────────────┬─────────────────────────────────────┘
                        │ HTTP / RPC
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                    Backend (Rust)                            │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │ RenderEngine                                       │    │
│  │  compile_query(prql) → (sql, RenderSpec)          │    │
│  │  • Lineage analysis (table provenance)            │    │
│  │  • Query OperationDispatcher.find_operations()    │    │
│  │  • Wire operations into FunctionCall nodes        │    │
│  └────────────────────────────────────────────────────┘    │
│                        ↓                                     │
│  ┌────────────────────────────────────────────────────┐    │
│  │ OperationDispatcher (OperationProvider)           │    │
│  │  • Composite: aggregates all providers            │    │
│  │  • find_operations(entity, args) → Vec<OpDesc>    │    │
│  │  • execute_operation() → routes to correct cache  │    │
│  └────────────────────────────────────────────────────┘    │
│         ↓                              ↓                     │
│  ┌─────────────────┐          ┌─────────────────┐          │
│  │ QueryableCache  │          │ QueryableCache  │          │
│  │ <TodoistTask>   │          │ <LogseqBlock>   │          │
│  │                 │          │                 │          │
│  │ • Leaf node     │          │ • Leaf node     │          │
│  │ • Validates     │          │ • Validates     │          │
│  │   entity_name   │          │   entity_name   │          │
│  │ • Macro         │          │ • Macro         │          │
│  │   dispatch      │          │   dispatch      │          │
│  └─────────────────┘          └─────────────────┘          │
│         ↓                              ↓                     │
│  ┌─────────────────┐          ┌─────────────────┐          │
│  │ TodoistTask     │          │ LogseqBlock     │          │
│  │ DataSource      │          │ DataSource      │          │
│  │                 │          │                 │          │
│  │ • API calls     │          │ • API calls     │          │
│  │ • Returns ()    │          │ • Returns ()    │          │
│  │ • Updates via   │          │ • Updates via   │          │
│  │   stream        │          │   stream        │          │
│  └─────────────────┘          └─────────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Structures

### OperationWiring (Simplified)

```rust
/// Connects lineage analysis results to operation metadata
///
/// Embedded in FunctionCall nodes in RenderSpec and sent to Flutter frontend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationWiring {
    // Lineage-derived UI binding info
    pub widget_type: String,        // "checkbox", "text", "button"
    pub modified_param: String,     // "checked", "content", "onClick"

    // Complete operation metadata (no duplication!)
    pub descriptor: OperationDescriptor,
}
```

**Removed fields:**
- ❌ `table` - now in descriptor
- ❌ `id_column` - now in descriptor
- ❌ `field` - treat all operations uniformly, not special-casing field updates
- ❌ `entity_type` - now `entity_name` in descriptor
- ❌ `operation_name` - now `name` in descriptor
- ❌ `optional_params` - YAGNI, add later if needed

### OperationDescriptor (Extended)

```rust
/// Complete metadata for an operation
///
/// Generated by #[operations_trait] macro.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationDescriptor {
    // NEW: Entity and table identification
    pub entity_name: String,        // "todoist-task", "logseq-block"
    pub table: String,              // "todoist_tasks", "logseq_blocks"
    pub id_column: String,          // "id"

    // Operation metadata (already exists in current code)
    pub name: String,               // "set_completion", "indent_block", "create"
    pub display_name: String,       // "Mark as complete", "Indent"
    pub description: String,        // Human-readable description for UI
    pub required_params: Vec<OperationParam>,
}
```

### OperationParam

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationParam {
    pub name: String,               // "completed", "new_parent_id"
    pub type_hint: String,          // "bool", "string", "id"
    pub description: String,        // "Whether task is completed"
}
```

---

## Trait Design

### OperationProvider (Composite Pattern)

```rust
/// Unified interface for operation providers
///
/// Implemented by:
/// - Leaf nodes: QueryableCache<T> (handle specific entity types)
/// - Composite: OperationDispatcher (aggregates providers, routes operations)
#[async_trait]
pub trait OperationProvider: Send + Sync {
    /// Get all operations this provider supports
    fn operations(&self) -> Vec<OperationDescriptor>;

    /// Find operations that can be executed with given arguments
    ///
    /// Filters to operations where required_params ⊆ available_args.
    ///
    /// Example:
    /// ```
    /// // Lineage: checkbox modifies "completed" field
    /// // Available: ["id", "completed"]
    /// let ops = provider.find_operations("todoist-task", &["id", "completed"]);
    /// // Returns: ["set_field", "set_completion", "delete"]
    /// // Excludes: ["create"] (needs more fields), ["indent_block"] (needs parent_id)
    /// ```
    fn find_operations(
        &self,
        entity_name: &str,
        available_args: &[String]
    ) -> Vec<OperationDescriptor> {
        self.operations()
            .into_iter()
            .filter(|op| {
                op.entity_name == entity_name &&
                op.required_params.iter().all(|p| available_args.contains(&p.name))
            })
            .collect()
    }

    /// Execute an operation
    ///
    /// - Individual caches: validate entity_name, dispatch to trait methods
    /// - Composite dispatcher: route to correct registered provider
    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity
    ) -> Result<()>;
}
```

---

## Macro-Generated Dispatch

The `#[operations_trait]` macro generates helper modules with dispatch functions:

```rust
// Example: CrudOperationProvider trait generates this module
pub mod __operations_CrudOperationProvider {
    /// Get operation descriptors for all trait methods
    pub fn all_operations() -> Vec<OperationDescriptor> {
        vec![
            OperationDescriptor {
                entity_name: "".to_string(),  // Filled by implementor
                table: "".to_string(),
                id_column: "id".to_string(),
                name: "set_field".to_string(),
                display_name: "Set field value".to_string(),
                description: "Update a single field".to_string(),
                required_params: vec![
                    OperationParam {
                        name: "id".to_string(),
                        type_hint: "string".to_string(),
                        description: "Entity ID".to_string(),
                    },
                    OperationParam {
                        name: "field".to_string(),
                        type_hint: "string".to_string(),
                        description: "Field name".to_string(),
                    },
                    OperationParam {
                        name: "value".to_string(),
                        type_hint: "any".to_string(),
                        description: "New value".to_string(),
                    },
                ],
            },
            // ... create, delete, etc.
        ]
    }

    /// Dispatch operation to appropriate trait method
    pub async fn dispatch_operation<T>(
        target: &T,
        op_name: &str,
        params: &StorageEntity
    ) -> Result<()>
    where
        T: CrudOperationProvider<E>,
        E: Send + Sync + 'static,
    {
        match op_name {
            "set_field" => {
                let id = params.get("id")?.as_string()?;
                let field = params.get("field")?.as_string()?;
                let value = params.get("value")?.clone();
                target.set_field(id, field, value).await
            }
            "create" => {
                let fields = params.get("fields")?.as_hashmap()?;
                target.create(fields).await.map(|_| ())
            }
            "delete" => {
                let id = params.get("id")?.as_string()?;
                target.delete(id).await
            }
            _ => Err(format!("Unknown operation: {}", op_name).into())
        }
    }
}
```

**Why helper functions (not direct trait impl)?**
- ✅ More flexible and composable
- ✅ Enables trait composition (combine multiple dispatchers)
- ✅ Keeps trait definitions clean
- ✅ No manual match statements needed

---

## Implementation Flow

### 1. Query Compilation (RenderEngine)

```rust
impl RenderEngine {
    pub fn compile_query(&self, prql: &str) -> Result<(String, RenderSpec)> {
        // 1. Lineage analysis (existing code)
        let lineage = LineagePreprocessor::new().analyze_query(prql)?;

        // 2. Walk render expression tree
        let mut render_spec = /* ... parse PRQL ... */;

        for function_call in &mut render_spec.function_calls {
            // 3. Get table from lineage for this function call
            let table_name = lineage.get_source_table(&function_call)?;
            let entity_name = self.table_to_entity_map.get(table_name)?;

            // 4. Get available columns from lineage
            let available_args = lineage.get_available_columns(&function_call)?;

            // 5. Query dispatcher for compatible operations
            let compatible_ops = self.dispatcher.find_operations(
                entity_name,
                &available_args
            )?;

            // 6. Create OperationWiring for each operation
            let wirings: Vec<OperationWiring> = compatible_ops.into_iter()
                .map(|descriptor| OperationWiring {
                    widget_type: lineage.infer_widget_type(&function_call),
                    modified_param: lineage.infer_modified_param(&function_call),
                    descriptor,
                })
                .collect();

            // 7. Attach to function call
            function_call.operations = wirings;
        }

        Ok((sql, render_spec))
    }
}
```

### 2. Frontend Dispatch (Flutter)

```dart
// Receive RenderSpec with pre-wired operations
final renderSpec = await backend.compileQuery(prql);

// Wire operations to UI
for (final funcCall in renderSpec.functionCalls) {
  for (final wiring in funcCall.operations) {
    // Create widget based on widget_type
    final widget = createWidget(wiring.widgetType);

    // Wire operation to widget event
    widget.on(wiring.modifiedParam, () {
      // Collect params from UI
      final params = collectParams(wiring.descriptor.requiredParams);

      // Dispatch to backend
      await backend.executeOperation(
        wiring.descriptor.entityName,
        wiring.descriptor.name,
        params,
      );
    });
  }
}
```

### 3. Backend Dispatch (OperationDispatcher)

```rust
pub struct OperationDispatcher {
    providers: HashMap<String, Arc<dyn OperationProvider>>,
}

#[async_trait]
impl OperationProvider for OperationDispatcher {
    fn operations(&self) -> Vec<OperationDescriptor> {
        // Aggregate from all providers
        self.providers.values()
            .flat_map(|p| p.operations())
            .collect()
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<()> {
        // Route to correct provider
        let provider = self.providers
            .get(entity_name)
            .ok_or_else(|| anyhow!("No provider for: {}", entity_name))?;

        provider.execute_operation(entity_name, op_name, params).await
    }
}

impl OperationDispatcher {
    pub fn register(&mut self, entity_name: String, provider: Arc<dyn OperationProvider>) {
        self.providers.insert(entity_name, provider);
    }

    pub fn unregister(&mut self, entity_name: &str) {
        self.providers.remove(entity_name);
    }
}
```

### 4. Cache Dispatch (QueryableCache)

```rust
#[async_trait]
impl<T> OperationProvider for QueryableCache<T>
where
    T: OperationRegistry + Send + Sync + 'static,
{
    fn operations(&self) -> Vec<OperationDescriptor> {
        T::all_operations()
    }

    async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity
    ) -> Result<()> {
        // Validate entity type
        if entity_name != T::entity_name() {
            return Err(format!("Expected {}, got {}", T::entity_name(), entity_name).into());
        }

        // Try each trait dispatcher (composable!)
        __operations_CrudOperationProvider::dispatch_operation(self, op_name, &params).await
            .or_else(|_| __operations_MutableBlockDataSource::dispatch_operation(self, op_name, &params).await)
            .or_else(|_| __operations_MutableTaskDataSource::dispatch_operation(self, op_name, &params).await)
    }
}
```

---

## Implementation Plan

### Phase 1: Add OperationProvider Trait (Non-Breaking) ✅

**Files to modify:**
- `core/datasource.rs` - Add OperationProvider trait
- `query-render/src/types.rs` - Simplify OperationWiring, extend OperationDescriptor

**Tasks:**
1. Define OperationProvider trait with operations(), find_operations(), execute_operation()
2. Simplify OperationWiring to just reference descriptor
3. Extend OperationDescriptor with entity_name, table, id_column
4. Remove field, optional_params from structs

**Risk**: Low - Pure additions

### Phase 2: Enhance Macro (Non-Breaking) ✅

**Files to modify:**
- `crates/holon-macros/src/lib.rs` - Enhance #[operations_trait]

**Tasks:**
1. Generate dispatch_operation() helper function per trait
2. Generate complete OperationDescriptor metadata
3. Add entity_name() method to OperationRegistry
4. Test with TodoistTask

**Risk**: Low - Additive

### Phase 3: Create OperationDispatcher (Non-Breaking) ✅

**Files to create:**
- `api/operation_dispatcher.rs`

**Files to modify:**
- `api/mod.rs` - Export new module

**Tasks:**
1. Implement OperationDispatcher struct
2. Implement OperationProvider for OperationDispatcher (composite)
3. Add register/unregister methods
4. Add comprehensive tests

**Risk**: Low - New component

### Phase 4: Integrate with RenderEngine (Non-Breaking) ✅

**Files to modify:**
- `api/render_engine.rs`

**Tasks:**
1. Add dispatcher: Arc<RwLock<OperationDispatcher>> field
2. Add table_to_entity_map: HashMap<String, String>
3. Implement register_provider() / unregister_provider() APIs
4. Keep old OperationRegistry temporarily (backward compatibility)

**Risk**: Low - Parallel systems coexist

### Phase 5: Bridge Lineage → Operations (Non-Breaking) ✅

**Files to modify:**
- `api/render_engine.rs` - compile_query() method

**Tasks:**
1. During compilation, query dispatcher.find_operations()
2. Filter by available args from lineage
3. Create enhanced OperationWiring from descriptors
4. Embed into FunctionCall nodes
5. Old and new systems coexist

**Risk**: Medium - Touches query compilation path

### Phase 6: Deprecate Old System (Breaking) ⚠️

**Files to modify:**
- `operations/registry.rs` - Add deprecation warnings
- `api/render_engine.rs` - Remove old execute_operation() method

**Files to delete:**
- `operations/block_ops.rs`
- `operations/block_movements.rs`
- `operations/registry.rs`

**Tasks:**
1. Mark old Operation trait as deprecated
2. Remove execute_operation() from RenderEngine
3. Provide migration guide
4. Major version bump (2.0.0)

**Risk**: High - Breaking change

---

## Design Decisions

### ✅ Decision 1: Simplify OperationWiring

**Rationale**: Don't duplicate fields from OperationDescriptor
- Just reference the descriptor
- Remove `field` - treat all operations uniformly
- Remove `optional_params` - YAGNI

### ✅ Decision 2: Use Composite Pattern

**Rationale**: Both caches and dispatcher implement same trait
- Enables recursive composition
- Each provider validates entity_name
- Clear separation of concerns

### ✅ Decision 3: Macro Helper Functions (Option B)

**Rationale**: More flexible than direct trait impl
- Can compose multiple trait dispatchers
- Keeps traits clean
- Enables trait composition

### ✅ Decision 4: Container vs Row Operations

**Rationale**: Context-aware operation wiring
- **Row operations**: set_field, delete, set_completion (wire to items)
- **Container operations**: create (wire to table/list headers)
- Lineage analysis detects context

### ✅ Decision 5: Compile-Time Wiring

**Rationale**: Leverage existing lineage infrastructure
- NO runtime operation discovery
- Operations determined during compile_query()
- Frontend receives complete metadata in RenderSpec

### ✅ Decision 6: CrudOperationProvider is Valuable

**Clarification**: It's CrudOperationProvider (not DataSource) that's valuable
- `DataSource<T>`: Read-only queries (RenderEngine uses via TursoBackend)
- `CrudOperationProvider<T>`: Write operations (wrapped by OperationProvider)

---

## Open Questions & Future Work

### Q1: How to handle disabled datasources?

**Phase 1 approach**: Keep tables in Turso, just disable operations
- Operations disappear from RenderSpec
- Queries still work

**Phase 2 enhancement**: Add table management
- Remove tables when provider disabled
- More complex, defer for now

### Q2: Should we support operation middleware?

**Future enhancement**: Add hooks for logging, validation, auth

```rust
pub trait OperationMiddleware: Send + Sync {
    async fn before_execute(&self, op: &OperationDescriptor, params: &StorageEntity);
    async fn after_execute(&self, op: &OperationDescriptor, result: &Result<()>);
}
```

### Q3: How to handle operation batching?

**Future enhancement**: Add execute_batch() for multiple operations

```rust
impl OperationDispatcher {
    pub async fn execute_batch(
        &self,
        operations: Vec<(String, String, StorageEntity)>
    ) -> Vec<Result<()>> {
        // Group by entity_name, execute in parallel
    }
}
```

---

## Summary

**This design is superior to the original proposal because:**

1. ✅ **Simpler**: No field duplication in OperationWiring
2. ✅ **Compile-time wiring**: No runtime overhead
3. ✅ **Composable**: Composite pattern allows flexibility
4. ✅ **Macro-driven**: Less boilerplate, stays in sync
5. ✅ **Leverages existing infrastructure**: Works with lineage analysis

**Key insight**: Operations are already being wired into RenderSpec! We just need to:
- Enhance wiring with rich metadata from OperationProvider
- Bridge lineage analysis → operation lookup
- Use macro-generated dispatch for type-safe execution

**Next step**: Implement Phase 1 - Add OperationProvider trait and simplified structures.
