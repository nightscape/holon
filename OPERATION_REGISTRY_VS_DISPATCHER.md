# OperationRegistry vs OperationDispatcher Comparison

## Overview

Both `OperationRegistry` and `OperationDispatcher` manage operations, but they serve different purposes and use different architectures. `OperationRegistry` is the legacy system, while `OperationDispatcher` is the newer trait-based approach.

## OperationRegistry (Legacy)

**Location**: `crates/holon/src/operations/registry.rs`

### Purpose
- Simple HashMap-based registry for direct operation execution
- Stores concrete `Operation` trait implementations
- Used for legacy block operations (Indent, Outdent, MoveBlock, UpdateField, etc.)

### Architecture
```rust
pub struct OperationRegistry {
    operations: HashMap<String, Arc<dyn Operation>>,
}
```

### Key Methods
- `register(operation: Arc<dyn Operation>)` - Register by operation name
- `execute(name, row_data, ui_state, db)` - Execute with full context
- `has_operation(name)` - Check if registered
- `operation_names()` - List all registered names

### Execution Signature
```rust
async fn execute(
    &self,
    name: &str,
    row_data: &StorageEntity,
    ui_state: &UiState,
    db: &mut TursoBackend,
) -> Result<()>
```

### Current Usage
- ✅ **Still actively used** in `RenderEngine.execute_operation()` (line 430)
- ✅ Used for querying available operations (`available_operations()`, `has_operation()`)
- ✅ Registered with default block operations in `create_default_registry()`
- ✅ Marked as "Legacy registry (kept for backward compatibility)" in RenderEngine

### Characteristics
- **Direct execution**: Operations receive full context (row_data, ui_state, db)
- **Simple lookup**: Operations stored by name in HashMap
- **Tight coupling**: Operations must implement `Operation` trait with specific signature
- **No entity awareness**: Operations don't know about entity types

---

## OperationDispatcher (Newer)

**Location**: `crates/holon/src/api/operation_dispatcher.rs`

### Purpose
- Composite pattern implementation for operation routing
- Aggregates multiple `OperationProvider` instances
- Routes operations to correct provider based on `entity_name`
- Manages both regular operations and sync operations

### Architecture
```rust
pub struct OperationDispatcher {
    providers: HashMap<String, Arc<dyn OperationProvider>>,
    syncable_providers: HashMap<String, Arc<Mutex<dyn SyncableProvider>>>,
}
```

### Key Methods
- `register(entity_name, provider)` - Register provider for entity type
- `register_syncable_provider(provider_name, provider)` - Register sync provider
- `execute_operation(entity_name, op_name, params)` - Route to correct provider
- `find_operations(entity_name, available_args)` - Find compatible operations
- `operations()` - Aggregate all operations from all providers
- `sync_all_providers()` - Sync all registered providers

### Execution Signature
```rust
async fn execute_operation(
    &self,
    entity_name: &str,
    op_name: &str,
    params: StorageEntity,
) -> Result<()>
```

### Current Usage
- ✅ Used during query compilation (`enhance_operations_with_dispatcher()`)
- ✅ Used to find compatible operations based on entity and available args
- ✅ Used for entity-based operations (todoist-task, logseq-block, etc.)
- ✅ Manages sync operations for external providers (todoist.sync, etc.)
- ✅ Registered in dependency injection system

### Characteristics
- **Provider-based**: Routes to `OperationProvider` instances
- **Entity-aware**: Operations organized by entity type
- **Composite pattern**: Dispatcher implements `OperationProvider` itself
- **Parameter-based**: Operations receive `StorageEntity` params, not full context
- **Sync support**: Special handling for syncable providers

---

## Key Differences

| Aspect | OperationRegistry | OperationDispatcher |
|--------|------------------|-------------------|
| **Architecture** | Direct HashMap lookup | Composite pattern with providers |
| **Storage** | `HashMap<String, Arc<dyn Operation>>` | `HashMap<String, Arc<dyn OperationProvider>>` |
| **Execution Context** | Full context (row_data, ui_state, db) | Parameters only (StorageEntity) |
| **Entity Awareness** | None | Entity-based routing |
| **Operation Discovery** | Simple name lookup | `find_operations()` with filtering |
| **Sync Support** | None | Built-in sync provider management |
| **Use Case** | Legacy block operations | Entity-based operations from providers |
| **Registration** | By operation name | By entity name |

---

## Overlap Analysis

### Similar Responsibilities
1. ✅ **Operation Registration**: Both allow registering operations
2. ✅ **Operation Execution**: Both execute operations
3. ✅ **Operation Lookup**: Both provide ways to find operations

### Different Approaches
1. **OperationRegistry**: Direct operation storage and execution
2. **OperationDispatcher**: Provider-based routing and aggregation

### Why Both Exist

Looking at the code, both are currently used:

1. **OperationRegistry** is used for:
   - Legacy block operations (hardcoded defaults)
   - Direct execution via `RenderEngine.execute_operation()`
   - Simple operation queries (`has_operation()`, `available_operations()`)

2. **OperationDispatcher** is used for:
   - Entity-based operations from providers (QueryableCache<T>)
   - Query compilation (finding compatible operations)
   - Sync operations for external systems
   - Operations that come from datasources

---

## Migration Path

Based on the code comments and architecture:

1. **OperationRegistry** is marked as "Legacy registry (kept for backward compatibility)"
2. **OperationDispatcher** is the newer, preferred approach
3. Both coexist because:
   - Legacy operations still use the old `Operation` trait
   - New entity-based operations use `OperationProvider` trait
   - Migration would require refactoring all legacy operations

### Potential Consolidation

To fully migrate to `OperationDispatcher`:
1. Create adapter `OperationProvider` implementations for legacy `Operation` types
2. Register legacy operations as providers in the dispatcher
3. Update `RenderEngine.execute_operation()` to use dispatcher
4. Remove `OperationRegistry` struct (but keep `Operation` trait for backward compat)

---

## Recommendation

**Current State**: Both are needed and serve different purposes:
- `OperationRegistry`: Legacy block operations
- `OperationDispatcher`: Entity-based operations from providers

**Future**: Consider migrating legacy operations to `OperationDispatcher` for consistency, but this requires:
- Creating `OperationProvider` wrappers for legacy `Operation` implementations
- Updating execution paths
- Testing thoroughly

**For Now**: Keep both, but be aware that `OperationDispatcher` is the preferred path for new code.

