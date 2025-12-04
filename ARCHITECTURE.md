# Architecture: Holon

## Overview

Holon is a Personal Knowledge & Task Management system that treats external systems (Todoist, org-mode, etc.) as first-class data sources. Unlike traditional PKM tools that import/export data, Holon maintains live bidirectional sync with external systems while enabling unified queries across all sources.

## Core Principles

### External Systems as First-Class Citizens

Data from external systems is stored in a format as close to the source as possible:
- All operations available in the external system can be performed locally
- All data can be displayed without loss
- Round-trip fidelity when syncing back

### Reactive Data Flow

Operations flow without blocking the UI:

```
User Action → Operation Dispatch → External/Internal System
                                          ↓
UI ← CDC Stream ← QueryableCache ← Sync Provider
```

- Operations are fire-and-forget
- Effects are observed through sync
- Changes propagate as streams
- Internal and external modifications are treated identically

### Declarative Queries with PRQL

Users specify data needs using PRQL, including rendering hints:

```prql
from todoist_tasks
filter completed == false
select {id, content, priority}
render (list item_template:(row
  (checkbox checked:this.completed)
  (text content:this.content)))
```

## Crate Structure

```
crates/
├── holon/           # Main orchestration crate
├── holon-api/       # Shared types for all frontends (Flutter, Tauri, REST)
├── holon-core/      # Core trait definitions
├── holon-macros/    # Procedural macros for code generation
├── holon-todoist/   # Todoist API integration
├── holon-orgmode/   # Org-mode file integration
├── holon-filesystem/# File system directory integration
└── query-render/    # PRQL compilation and render spec extraction

frontends/
└── flutter/         # Flutter frontend with FFI bridge
```

### Crate Responsibilities

| Crate | Purpose |
|-------|---------|
| `holon-api` | Value types, Operation descriptors, Change/CDC types. No frontend-specific deps. |
| `holon-core` | Core traits: DataSource, CrudOperations, BlockOperations, Lens, Predicate |
| `holon-macros` | `#[operations_trait]`, `#[affects(...)]`, entity derives |
| `holon-todoist` | Todoist sync provider, operation provider, API client |
| `holon-orgmode` | Org file parsing, DataSource, sync via file watching |
| `query-render` | PRQL→SQL compilation, RenderSpec extraction, lineage analysis |

## Core Traits

### Data Access

```rust
pub trait DataSource<T>: MaybeSendSync {
    async fn get_all(&self) -> Result<Vec<T>>;
    async fn get_by_id(&self, id: &str) -> Result<Option<T>>;
    async fn get_children(&self, parent_id: &str) -> Result<Vec<T>>; // BlockEntity
}

pub trait CrudOperations<T>: MaybeSendSync {
    async fn set_field(&self, id: &str, field: &str, value: Value) -> Result<Option<Operation>>;
    async fn create(&self, fields: HashMap<String, Value>) -> Result<(String, Option<Operation>)>;
    async fn delete(&self, id: &str) -> Result<Option<Operation>>;
}
```

### Entity Behavior

```rust
pub trait BlockEntity: MaybeSendSync {
    fn id(&self) -> &str;
    fn parent_id(&self) -> Option<&str>;
    fn sort_key(&self) -> &str;     // Fractional index for ordering
    fn depth(&self) -> i64;
    fn content(&self) -> &str;
}

pub trait TaskEntity: MaybeSendSync {
    fn completed(&self) -> bool;
    fn priority(&self) -> Option<i64>;
    fn due_date(&self) -> Option<DateTime<Utc>>;
}
```

### Domain Operations

```rust
pub trait BlockOperations<T>: BlockDataSourceHelpers<T> {
    async fn indent(&self, id: &str, parent_id: &str) -> Result<Option<Operation>>;
    async fn outdent(&self, id: &str) -> Result<Option<Operation>>;
    async fn move_block(&self, id: &str, parent_id: &str, after_block_id: Option<&str>) -> Result<Option<Operation>>;
    async fn split_block(&self, id: &str, position: i64) -> Result<Option<Operation>>;
    async fn move_up(&self, id: &str) -> Result<Option<Operation>>;
    async fn move_down(&self, id: &str) -> Result<Option<Operation>>;
}

pub trait TaskOperations<T>: CrudOperations<T> {
    async fn set_completion(&self, id: &str, completed: bool) -> Result<Option<Operation>>;
    async fn set_priority(&self, id: &str, priority: i64) -> Result<Option<Operation>>;
    async fn set_due_date(&self, id: &str, due_date: Option<DateTime<Utc>>) -> Result<Option<Operation>>;
}
```

### Type-Safe Queries

```rust
pub trait Lens<T, U>: Clone + Send + Sync + 'static {
    fn get(&self, source: &T) -> Option<U>;
    fn set(&self, source: &mut T, value: U);
    fn field_name(&self) -> &'static str;
}

pub trait Predicate<T>: Send + Sync {
    fn test(&self, item: &T) -> bool;
    fn to_sql(&self) -> Option<SqlPredicate>;  // For query pushdown
    fn and<P: Predicate<T>>(self, other: P) -> And<T, Self, P>;
    fn or<P: Predicate<T>>(self, other: P) -> Or<T, Self, P>;
    fn not(self) -> Not<T, Self>;
}
```

### Operation Discovery

```rust
pub trait OperationProvider: Send + Sync {
    fn operations(&self) -> Vec<OperationDescriptor>;
    fn find_operations(&self, entity_name: &str, available_args: &[String]) -> Vec<OperationDescriptor>;
}

pub struct OperationDescriptor {
    pub entity_name: String,
    pub name: String,
    pub required_params: Vec<OperationParam>,
    pub affected_fields: Vec<String>,
    pub precondition: Option<PreconditionChecker>,
}
```

## Data Flow Architecture

### Storage Layer

```
┌─────────────────────────────────────────────────────────┐
│                     Application                          │
└─────────────────────────────────────────────────────────┘
                           │
           ┌───────────────┴───────────────┐
           ▼                               ▼
┌─────────────────────┐         ┌─────────────────────────┐
│  QueryableCache<T>  │         │   QueryableCache<T>     │
│  (Todoist tasks)    │         │   (Org-mode headlines)  │
└─────────────────────┘         └─────────────────────────┘
           │                               │
           ▼                               ▼
┌─────────────────────┐         ┌─────────────────────────┐
│   TursoBackend      │         │     TursoBackend        │
│   (SQLite cache)    │         │     (SQLite cache)      │
└─────────────────────┘         └─────────────────────────┘
           │                               │
           ▼                               ▼
┌─────────────────────┐         ┌─────────────────────────┐
│  TodoistSyncProvider│         │  OrgModeSyncProvider    │
│  (API sync)         │         │  (File watching)        │
└─────────────────────┘         └─────────────────────────┘
```

### QueryableCache

Wraps any `DataSource<T>` to provide:
- Local caching in Turso (SQLite)
- CDC streaming of changes
- Operation dispatch to external systems

```rust
pub struct QueryableCache<S, T>
where
    S: DataSource<T>,
    T: HasSchema + Send + Sync + 'static,
{
    source: Arc<S>,
    backend: Arc<RwLock<TursoBackend>>,
}

// Implements: DataSource<T>, CrudOperations<T>, OperationProvider
```

### Change Data Capture (CDC)

Changes propagate from storage to UI via CDC streams:

```
Database Write → Turso CDC → BatchWithMetadata<RowChange> → UI Stream
```

Features:
- DELETE+INSERT coalescing into UPDATE events (prevents UI flicker)
- Trace context propagation via `_change_origin` column
- Batched delivery for efficiency

```rust
pub struct RowChange {
    pub relation_name: String,
    pub change: ChangeData,  // Created | Updated | Deleted
}

pub trait ChangeNotifications<T>: Send + Sync {
    async fn watch_changes_since(
        &self,
        position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<Change<T>>>> + Send>>;
}
```

## Query & Render Pipeline

### PRQL Compilation

```
PRQL string
    ↓ prqlc::Parser
RQ AST (Relational Query)
    ↓ TransformPipeline
Transformed RQ
    ↓ SQL generation
SQL string + RenderSpec
```

### Transform Pipeline

Transformers applied in sequence:
1. **ChangeOriginTransformer** - Adds `_change_origin` for operation tracing
2. **EntityTypeInjector** - Adds `_entity_type` for discriminated unions
3. **ColumnPreservationTransformer** - Preserves source columns in derived tables
4. **JsonAggregationTransformer** - Aggregates related rows into JSON arrays

### RenderSpec Tree

Query compilation produces a `RenderSpec` describing the UI:

```
RenderSpec
├── RenderExpr::FunctionCall("list", ...)
│   └── RenderExpr::FunctionCall("row", ...)
│       ├── RenderExpr::FunctionCall("checkbox", checked: ColumnRef("completed"))
│       │   └── operations: [OperationWiring { set_completion... }]
│       └── RenderExpr::FunctionCall("text", content: ColumnRef("content"))
```

### Automatic Operation Wiring

1. **Lineage Analysis**: Traces which database columns flow into which UI widgets
2. **Operation Matching**: Compares widget parameters against operation `required_params`
3. **UI Annotation**: Attaches available operations to the rendered tree

A checkbox bound to `completed` automatically gets `set_completion` wired up because:
- Widget type is "checkbox"
- Its `checked` parameter traces to `completed` column
- An operation exists that modifies `completed` with available parameters

## Operation System

### Fire-and-Forget Pattern

```rust
// Operation execution doesn't wait for confirmation
dispatcher.execute_operation("todoist-task", "set_completion", params)?;
// Returns immediately with inverse operation for undo

// Confirmation comes via CDC stream
watch_changes().await  // UI updates when change arrives
```

### Composite Dispatcher

```rust
pub struct OperationDispatcher {
    providers: Vec<Arc<dyn OperationProvider>>,
}

// Routes by entity_name to appropriate provider:
// "todoist-task" → TodoistOperationProvider
// "org-headline" → OrgModeOperationProvider
```

### Operation Metadata via Macros

```rust
#[operations_trait]
pub trait TaskOperations<T>: CrudOperations<T> {
    #[affects("completed")]
    async fn set_completion(&self, id: &str, completed: bool) -> Result<Option<Operation>>;
}
```

Generates `OperationDescriptor` with:
- Required parameters and their types
- Affected fields for UI updates
- Preconditions for availability

## Procedural Macros (holon-macros)

The `holon-macros` crate provides procedural macros for code generation, eliminating boilerplate for entity definitions and operation dispatch.

### Entity Derive Macro

`#[derive(Entity)]` generates schema introspection, serialization, and SQL generation:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Entity)]
#[entity(name = "todoist_tasks", short_name = "task")]
pub struct TodoistTask {
    #[primary_key]
    #[indexed]
    pub id: String,

    pub content: String,

    #[indexed]
    pub priority: Option<i32>,

    #[indexed]
    pub due_date: Option<DateTime<Utc>>,

    #[reference(todoist_projects)]
    pub project_id: Option<String>,
}
```

**Generated Code:**

```rust
impl TodoistTask {
    // Schema metadata for table creation
    pub fn entity_schema() -> EntitySchema { ... }

    // Short name for parameter naming ("task" → "task_id")
    pub fn short_name() -> Option<&'static str> { Some("task") }
}

impl HasSchema for TodoistTask {
    // SQL schema generation (CREATE TABLE, indexes)
    fn schema() -> Schema { ... }

    // Convert struct to HashMap<String, Value>
    fn to_entity(&self) -> DynamicEntity { ... }

    // Reconstruct struct from HashMap<String, Value>
    fn from_entity(entity: DynamicEntity) -> Result<Self> { ... }
}
```

**Field Attributes:**

| Attribute | Effect |
|-----------|--------|
| `#[primary_key]` | Marks field as PRIMARY KEY |
| `#[indexed]` | Creates index on this column |
| `#[reference(entity)]` | Foreign key reference |
| `#[lens(skip)]` | Exclude from lens generation |

### Operations Trait Macro

`#[operations_trait]` transforms a trait definition into a complete operation system:

```rust
#[holon_macros::operations_trait]
#[async_trait]
pub trait BlockOperations<T>: BlockDataSourceHelpers<T>
where
    T: BlockEntity + MaybeSendSync + 'static,
{
    /// Move block under a new parent
    #[holon_macros::affects("parent_id", "depth", "sort_key")]
    async fn indent(&self, id: &str, parent_id: &str) -> Result<Option<Operation>>;

    /// Move block to different position
    #[holon_macros::affects("parent_id", "depth", "sort_key")]
    #[holon_macros::triggered_by(availability_of = "tree_position", providing = ["parent_id", "after_block_id"])]
    async fn move_block(
        &self,
        id: &str,
        parent_id: &str,
        after_block_id: Option<&str>,
    ) -> Result<Option<Operation>>;
}
```

**Generated Code (in module `__operations_block_operations`):**

```rust
// 1. Operation descriptor functions for each method
pub fn INDENT_OP(entity_name: &str, entity_short_name: &str, table: &str, id_column: &str)
    -> OperationDescriptor { ... }

pub fn MOVE_BLOCK_OP(entity_name: &str, entity_short_name: &str, table: &str, id_column: &str)
    -> OperationDescriptor { ... }

// 2. Operation constructor functions (for building inverse operations)
pub fn indent_op(entity_name: &str, id: &str, parent_id: &str) -> Operation { ... }
pub fn move_block_op(entity_name: &str, id: &str, parent_id: &str, after_block_id: Option<&str>)
    -> Operation { ... }

// 3. Aggregate function returning all operations
pub fn block_operations(entity_name: &str, entity_short_name: &str, table: &str, id_column: &str)
    -> Vec<OperationDescriptor> { ... }

// 4. Dispatch function for dynamic operation execution
pub async fn dispatch_operation<DS, E>(
    target: &DS,
    op_name: &str,
    params: &StorageEntity
) -> Result<Option<Operation>>
where
    DS: BlockOperations<E> + Send + Sync,
    E: BlockEntity + Send + Sync + 'static,
{ ... }
```

### Method Attributes

**`#[affects("field1", "field2")]`**

Declares which database fields an operation modifies. Used for:
- UI reactivity (only re-render affected widgets)
- Conflict detection
- Audit logging

```rust
#[holon_macros::affects("parent_id", "depth", "sort_key")]
async fn indent(&self, id: &str, parent_id: &str) -> Result<Option<Operation>>;
```

**`#[triggered_by(availability_of = "...", providing = [...])]`**

Declares operation availability based on contextual parameters:

```rust
// Operation available when "tree_position" param exists
// Provides parent_id and after_block_id from tree_position
#[holon_macros::triggered_by(
    availability_of = "tree_position",
    providing = ["parent_id", "after_block_id"]
)]
async fn move_block(&self, id: &str, parent_id: &str, after_block_id: Option<&str>)
    -> Result<Option<Operation>>;

// Simple case: operation triggered when "completed" param available
#[holon_macros::triggered_by(availability_of = "completed")]
async fn set_completion(&self, id: &str, completed: bool) -> Result<Option<Operation>>;
```

**`#[require(expr)]`**

Compile-time precondition that generates runtime validation:

```rust
#[require(priority >= 1)]
#[require(priority <= 5)]
async fn set_priority(&self, id: &str, priority: i64) -> Result<Option<Operation>>;
```

### Type Inference

The macro automatically infers parameter types for `OperationDescriptor`:

| Rust Type | Inferred TypeHint |
|-----------|-------------------|
| `&str`, `String` | `TypeHint::String` |
| `bool` | `TypeHint::Bool` |
| `i64`, `i32` | `TypeHint::Number` |
| `*_id` (naming convention) | `TypeHint::EntityId { entity_name }` |

Parameters ending in `_id` are automatically detected as entity references:
- `project_id` → `TypeHint::EntityId { entity_name: "project" }`
- `parent_id` → `TypeHint::EntityId { entity_name: "parent" }`

### Generated OperationDescriptor

```rust
OperationDescriptor {
    entity_name: "todoist-task",
    entity_short_name: "task",
    id_column: "id",
    name: "set_completion",
    display_name: "Set Completion",
    description: "Toggle or set task completion status",
    required_params: vec![
        OperationParam { name: "id", type_hint: TypeHint::EntityId { entity_name: "task" }, ... },
        OperationParam { name: "completed", type_hint: TypeHint::Bool, ... },
    ],
    affected_fields: vec!["completed"],
    param_mappings: vec![
        ParamMapping { from: "completed", provides: vec!["completed"], ... }
    ],
    precondition: None,
}
```

### Dispatch Function Generation

The generated `dispatch_operation` function extracts parameters from `StorageEntity` and calls the appropriate trait method:

```rust
// Generated code (simplified)
pub async fn dispatch_operation<DS, E>(
    target: &DS,
    op_name: &str,
    params: &StorageEntity
) -> Result<Option<Operation>> {
    match op_name {
        "indent" => {
            let id: String = params.get("id")?.as_string()?.to_string();
            let parent_id: String = params.get("parent_id")?.as_string()?.to_string();
            target.indent(&id, &parent_id).await
        }
        "move_block" => {
            let id: String = params.get("id")?.as_string()?.to_string();
            let parent_id: String = params.get("parent_id")?.as_string()?.to_string();
            let after_block_id: Option<String> = params.get("after_block_id")
                .and_then(|v| v.as_string().map(|s| s.to_string()));
            target.move_block(&id, &parent_id, after_block_id.as_deref()).await
        }
        _ => Err(UnknownOperationError::new("BlockOperations", op_name).into())
    }
}
```

### Usage in Operation Providers

```rust
impl OperationProvider for TodoistOperationProvider {
    fn operations(&self) -> Vec<OperationDescriptor> {
        let mut ops = vec![];
        // Aggregate from all applicable traits
        ops.extend(__operations_crud_operations::crud_operations(
            "todoist-task", "task", "todoist_tasks", "id"));
        ops.extend(__operations_task_operations::task_operations(
            "todoist-task", "task", "todoist_tasks", "id"));
        ops
    }

    async fn execute_operation(&self, op: &Operation) -> Result<Option<Operation>> {
        let params = op.to_storage_entity();

        // Try each trait's dispatch function
        match __operations_crud_operations::dispatch_operation(&self.datasource, &op.name, &params).await {
            Ok(result) => return Ok(result),
            Err(e) if UnknownOperationError::is_unknown(&*e) => {}
            Err(e) => return Err(e),
        }

        match __operations_task_operations::dispatch_operation(&self.datasource, &op.name, &params).await {
            Ok(result) => return Ok(result),
            Err(e) => return Err(e),
        }
    }
}
```

## External System Integration

### Integration Pattern

Each external system provides:

1. **SyncProvider** - Fetches data from external API
2. **DataSource** - Read access to cached data
3. **OperationProvider** - Routes operations to external API

```rust
// Todoist example
TodoistSyncProvider
  → Incremental sync with sync tokens
  → HTTP requests to Todoist REST API

TodoistTaskDataSource
  → Implements DataSource<TodoistTask>
  → Reads from QueryableCache

TodoistOperationProvider
  → Routes set_field() to Todoist API
  → Returns inverse operation for undo
```

### Adding a New External System

1. Define entity types implementing `HasSchema`
2. Implement `DataSource<T>` for read access
3. Implement domain traits (`TaskOperations`, etc.)
4. Create `SyncProvider` for data synchronization
5. Register in DI container

## Frontend Architecture

### Flutter FFI Bridge

The Rust backend exposes a minimal FFI surface:

```rust
fn init_render_engine() -> RenderEngine;
fn compile_query(prql: &str) -> CompiledQuery;
fn execute_operation(entity: &str, op: &str, params: StorageEntity);
fn watch_changes() -> Stream<Change<StorageEntity>>;
```

Using `flutter_rust_bridge` for type-safe code generation.

### Reactive Updates

Frontends subscribe to change streams:

```dart
watchChanges().listen((changes) {
  for (change in changes) {
    updateWidget(change.id, change.data);
  }
});
```

No explicit refresh calls—UI state derives from the change stream.

## Dependency Injection

Using `ferrous-di` for service composition:

```rust
pub async fn create_backend_engine<F>(
    db_path: PathBuf,
    setup_fn: F,
) -> Result<Arc<BackendEngine>>

// Registers:
// - TursoBackend
// - OperationDispatcher
// - TransformPipeline
// - Provider modules (Todoist, OrgMode, etc.)
```

## Value Types

```rust
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    DateTime(DateTime<Utc>),
    Json(serde_json::Value),
    Null,
}

pub type StorageEntity = HashMap<String, Value>;
```

## Schema System

```rust
pub struct Schema {
    pub table_name: String,
    pub fields: Vec<FieldSchema>,
}

pub struct FieldSchema {
    pub name: String,
    pub data_type: DataType,
    pub indexed: bool,
    pub primary_key: bool,
    pub nullable: bool,
}

pub trait HasSchema {
    fn schema() -> Schema;
    fn to_entity(&self) -> Result<StorageEntity>;
    fn from_entity(entity: StorageEntity) -> Result<Self>;
}
```

Auto-generates CREATE TABLE and CREATE INDEX SQL from schema definitions.

## Ordering with Fractional Indexing

Block ordering uses fractional indexing:
- Sort keys are base-26-like strings
- Supports arbitrary insertion without rewriting all keys
- Automatic rebalancing when keys get too long

## Platform Support

### WASM Compatibility

- `MaybeSendSync` trait alias relaxes Send+Sync on WASM
- `#[async_trait(?Send)]` for non-Send futures
- Conditional compilation for platform-specific features

### Supported Frontends

| Frontend | Status | Notes |
|----------|--------|-------|
| Flutter | Primary | Full FFI bridge via flutter_rust_bridge |
| TUI | Planned | Terminal interface |
| Tauri | Planned | Desktop native |

## Consistency Model

### Local Consistency
- Database transactions ensure atomic updates
- CDC delivers changes in commit order
- UI reflects committed state

### External Consistency
- Eventually consistent (5-30 second typical delay)
- Last-write-wins for concurrent edits
- Sync tokens prevent duplicate processing

## Future: Internal Content (Loro CRDT)

For user-owned content (notes, internal tasks), planned architecture:
- Loro CRDT for collaborative editing
- P2P sync via Iroh
- Works offline, syncs when connected

External systems remain server-authoritative via the existing cache pattern.

## Key Files

| Path | Description |
|------|-------------|
| `crates/holon-core/src/traits.rs` | Core trait definitions (DataSource, CrudOperations, BlockOperations) |
| `crates/holon-macros/src/lib.rs` | Procedural macros (#[derive(Entity)], #[operations_trait]) |
| `crates/holon-api/src/entity.rs` | Entity types (DynamicEntity, Schema, HasSchema) |
| `crates/holon/src/storage/turso.rs` | Turso backend + CDC |
| `crates/holon/src/core/transform/` | PRQL transform pipeline |
| `crates/holon-todoist/src/` | Todoist integration |
| `crates/query-render/src/` | PRQL compilation |
| `frontends/flutter/rust/src/` | FFI bridge |
