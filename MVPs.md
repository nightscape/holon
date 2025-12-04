# Rusty Knowledge: MVP Roadmap

This document outlines a series of Minimum Viable Products (MVPs) that progressively build toward the full vision outlined in `VISION.md`. Each MVP is designed to be independently valuable while building foundational capabilities for future enhancements.

## Architecture Context

The current architecture provides:
- **RenderEngine**: Manages internal blocks with PRQL queries and CDC streaming via Turso materialized views
- **ChangeNotifications**: Stream-based change notification system (`crates/holon/src/api/streaming.rs`)
- **QueryableCache**: Transparent caching layer wrapping DataSource implementations
- **Todoist Integration**: Stream-based sync provider (`crates/holon-todoist/`) with `TodoistSyncProvider` and `QueryableCache` support

The TUI frontend (`frontends/tui/`) currently:
- Uses `RenderEngine.query_and_watch()` for internal blocks
- Receives CDC updates via `RowChange` stream
- Renders blocks using PRQL render specifications
- Supports operations via `engine.execute_operation()`

---

## MVP 1: External System Tasks in TUI with Manual Sync

**Goal**: Display tasks from external systems (e.g., Todoist) in the TUI and enable manual sync via keypress, using a completely generic architecture that works for any external system.

### Architecture Principles

1. **No System-Specific Code**: After instantiation, everything uses generic interfaces (`SyncableProvider`, `OperationProvider`, `StorageEntity`)
2. **Operation-Based Sync**: Sync is triggered via the operation system, not hardcoded handlers
3. **Unified Data Model**: State works with `Vec<StorageEntity>` from `query_and_watch`, not system-specific types
4. **PRQL-Driven Rendering**: How tasks are rendered is determined by PRQL `render` clause, not hardcoded UI

### Requirements

1. **Display External System Tasks**
   - Show tasks from external systems (e.g., Todoist) in the TUI
   - Use `RenderEngine.query_and_watch()` with PRQL query for external system table
   - Display via PRQL render specification (no hardcoded rendering)
   - State uses `Vec<StorageEntity>` from query results

2. **Manual Sync Trigger**
   - Add keyboard shortcut (e.g., `Ctrl+S` or `r`) to trigger sync on all registered `SyncableProvider`s
   - Sync executed via operation system (not hardcoded)
   - Updates flow through ChangeNotifications → QueryableCache → RenderEngine database → CDC stream → TUI

3. **Generic Integration**
   - `SyncableProvider` implements `#[operations_trait]` so `sync()` becomes an operation
   - `RenderEngine` maintains collection of `SyncableProvider`s
   - External system data written to `RenderEngine`'s database via `QueryableCache`
   - No Todoist-specific code in State or ViewMode

### Architecture Decisions (Answered)

1. **SyncableProvider as Operation**: 
   - ✅ Per-provider sync using `provider.operation` convention (e.g., "todoist.sync", "jira.sync")
   - ✅ Operation name split at `.` to extract provider name
   - ✅ `#[operations_trait(provider_name = "...")]` macro attribute is required
   - ✅ Can sync specific providers or all of them

2. **External Data Integration**:
   - ✅ `RenderEngine` uses `QueryableCache` instead of `TursoBackend` directly
   - ✅ `QueryableCache` stores `TursoBackend` and delegates to it
   - ✅ `QueryableCache` implements `ChangeNotifications<StorageEntity>` via `TursoBackend`
   - ✅ Changes flow: External system → QueryableCache → TursoBackend CDC → RenderEngine CDC stream
   - ⚠️ **Major refactor**: RenderEngine functionality needs to be moved/rewritten to use QueryableCache

3. **SyncableProvider Collection**:
   - ✅ `OperationDispatcher` manages both `OperationProvider`s and `SyncableProvider`s
   - ✅ Unified registry pattern

4. **ChangeNotifications Integration**:
   - ✅ `QueryableCache` implements `ChangeNotifications<StorageEntity>` via `TursoBackend.row_changes()`
   - ✅ No auto-sync on `watch_changes_since(StreamPosition::Beginning)` - caller must sync first
   - ✅ This allows offline startup (no sync attempt if offline)
   - ⚠️ **Open question**: How to handle multiple QueryableCache instances sharing TursoBackend? (See proposals below)

### Technical Approach (Generic)

#### 1. Make SyncableProvider an Operation Trait

**File**: `crates/holon/src/core/datasource.rs`

```rust
/// Type-independent sync trait for providers
///
/// Providers that can sync from external systems implement this trait.
/// Sync is exposed as an operation via #[operations_trait] with provider-specific entity_name.
///
/// Example:
/// ```rust
/// #[operations_trait(provider_name = "todoist")]
/// #[async_trait]
/// pub trait SyncableProvider: Send + Sync {
///     async fn sync(&mut self) -> Result<()>;
/// }
/// ```
/// This generates an operation with entity_name="todoist-sync".
#[holon_macros::operations_trait(provider_name = "...")] // Macro accepts provider_name arg
#[async_trait]
pub trait SyncableProvider: Send + Sync {
    /// Sync data from the external system
    ///
    /// This operation:
    /// - Fetches updates from the external system
    /// - Emits changes via streams (if applicable)
    /// - Updates internal state (sync tokens, etc.)
    ///
    /// Operation name: "sync"
    /// Entity name: "{provider_name}-sync" (e.g., "todoist-sync", "jira-sync")
    /// Parameters: None (or provider-specific params in future?)
    async fn sync(&mut self) -> Result<()>;
}
```

**Note**: The `#[operations_trait]` macro needs to be extended to accept a required `provider_name` argument. Operation names follow `provider.operation` convention (e.g., "todoist.sync").

#### 2. OperationDispatcher Manages SyncableProviders

**File**: `crates/holon/src/api/operation_dispatcher.rs`

```rust
pub struct OperationDispatcher {
    /// Map from entity_name to operation provider
    providers: HashMap<String, Arc<dyn OperationProvider>>,
    
    /// Map from provider_name to syncable provider
    /// Key: provider_name (e.g., "todoist", "jira")
    /// Value: SyncableProvider wrapped in Mutex for mutable sync access
    syncable_providers: HashMap<String, Arc<Mutex<dyn SyncableProvider>>>,
}

impl OperationDispatcher {
    /// Register a syncable provider
    ///
    /// # Arguments
    /// * `provider_name` - Provider identifier (e.g., "todoist", "jira")
    /// * `provider` - The SyncableProvider instance to register
    pub fn register_syncable_provider(&mut self, provider_name: String, provider: Arc<Mutex<dyn SyncableProvider>>) {
        self.syncable_providers.insert(provider_name, provider);
    }
    
    /// Sync a specific provider by name
    ///
    /// Provider name can be extracted from operation name using `provider.operation` convention.
    /// Example: "todoist.sync" → provider_name = "todoist"
    pub async fn sync_provider(&self, provider_name: &str) -> Result<()> {
        let provider = self.syncable_providers
            .get(provider_name)
            .ok_or_else(|| format!("No syncable provider registered: {}", provider_name))?;
        
        let mut provider_guard = provider.lock().await;
        provider_guard.sync().await
    }
    
    /// Sync provider from operation name
    ///
    /// Extracts provider name from `provider.operation` format.
    /// Example: "todoist.sync" → syncs "todoist" provider
    pub async fn sync_provider_from_operation(&self, operation_name: &str) -> Result<()> {
        let provider_name = operation_name
            .split('.')
            .next()
            .ok_or_else(|| format!("Invalid operation name format: {}", operation_name))?;
        self.sync_provider(provider_name).await
    }
    
    /// Sync all registered providers
    pub async fn sync_all_providers(&self) -> Result<()> {
        for (name, provider) in self.syncable_providers.iter() {
            let mut provider_guard = provider.lock().await;
            if let Err(e) = provider_guard.sync().await {
                eprintln!("Failed to sync provider {}: {}", name, e);
                // Continue syncing other providers
            }
        }
        Ok(())
    }
    
    /// Get list of registered syncable provider names
    pub fn syncable_provider_names(&self) -> Vec<String> {
        self.syncable_providers.keys().cloned().collect()
    }
}
```

**File**: `crates/holon/src/api/render_engine.rs`

```rust
impl RenderEngine {
    /// Register a syncable provider (delegates to dispatcher)
    pub async fn register_syncable_provider(&self, provider_name: String, provider: Arc<Mutex<dyn SyncableProvider>>) {
        let mut dispatcher = self.dispatcher.write().await;
        dispatcher.register_syncable_provider(provider_name, provider);
    }
    
    /// Sync all registered providers (delegates to dispatcher)
    pub async fn sync_all_providers(&self) -> Result<()> {
        let dispatcher = self.dispatcher.read().await;
        dispatcher.sync_all_providers().await
    }
    
    /// Sync a specific provider (delegates to dispatcher)
    pub async fn sync_provider(&self, provider_name: &str) -> Result<()> {
        let dispatcher = self.dispatcher.read().await;
        dispatcher.sync_provider(provider_name).await
    }
}
```

#### 3. QueryableCache Uses TursoBackend and Implements ChangeNotifications

**File**: `crates/holon/src/core/queryable_cache.rs`

```rust
use crate::storage::turso::TursoBackend;
use crate::api::streaming::{ChangeNotifications, Change, StreamPosition, ApiError};
use crate::storage::types::StorageEntity;

pub struct QueryableCache<S, T>
where
    S: DataSource<T>,
    T: HasSchema + Send + Sync + 'static,
{
    source: Arc<S>,
    backend: Arc<RwLock<TursoBackend>>, // Changed from Database to TursoBackend
    _phantom: PhantomData<T>,
}

impl<S, T> QueryableCache<S, T>
where
    S: DataSource<T>,
    T: HasSchema + Send + Sync + 'static,
{
    /// Create QueryableCache with TursoBackend
    pub async fn new_with_backend(source: S, backend: Arc<RwLock<TursoBackend>>) -> Result<Self> {
        let cache = Self {
            source: Arc::new(source),
            backend,
            _phantom: PhantomData,
        };
        
        cache.initialize_schema().await?;
        Ok(cache)
    }
    
    // ... existing methods updated to use backend ...
}

// Implement ChangeNotifications<StorageEntity> via TursoBackend
#[async_trait]
impl<S, T> ChangeNotifications<StorageEntity> for QueryableCache<S, T>
where
    S: DataSource<T>,
    T: HasSchema + Send + Sync + 'static,
{
    async fn watch_changes_since(
        &self,
        position: StreamPosition,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<Change<StorageEntity>>, ApiError>> + Send>> {
        // IMPORTANT: No auto-sync here - caller must sync first
        // This allows offline startup without sync attempts
        
        let backend = self.backend.read().await;
        let schema = T::schema();
        let table_name = schema.table_name.clone();
        
        // Get CDC stream from TursoBackend
        let (_, row_change_stream) = backend.row_changes()
            .map_err(|e| ApiError::InternalError { message: e.to_string() })?;
        
        // Filter stream for this table and convert RowChange to Change<StorageEntity>
        // See proposals below for handling multiple QueryableCache instances
        use tokio_stream::StreamExt;
        let filtered_stream = row_change_stream
            .filter(move |row_change| {
                row_change.relation_name == table_name
            })
            .map(|row_change| {
                // Convert RowChange to Change<StorageEntity>
                match row_change.change {
                    ChangeData::Created { data, .. } => {
                        Ok(vec![Change::Created { 
                            data: StorageEntity::from(data),
                            origin: ChangeOrigin::Remote,
                        }])
                    }
                    ChangeData::Updated { id, data, .. } => {
                        Ok(vec![Change::Updated {
                            id,
                            data: StorageEntity::from(data),
                            origin: ChangeOrigin::Remote,
                        }])
                    }
                    ChangeData::Deleted { id, .. } => {
                        Ok(vec![Change::Deleted {
                            id,
                            origin: ChangeOrigin::Remote,
                        }])
                    }
                }
            });
        
        Box::pin(filtered_stream)
    }
    
    async fn get_current_version(&self) -> Result<Vec<u8>, ApiError> {
        // Return empty version vector for now
        // Could be enhanced to track sync tokens
        Ok(vec![])
    }
}
```

**File**: `frontends/tui/src/launcher.rs`

```rust
// Initialize RenderEngine
// NOTE: After refactoring, RenderEngine will use QueryableCache internally
// For now, this is a placeholder showing the intended architecture
let mut engine = RenderEngine::new(db_path.clone()).await?;

// Get backend from RenderEngine (for sharing with QueryableCache)
// After refactoring, RenderEngine will expose backend accessor
let backend = engine.get_backend().await; // New method needed

// Initialize Todoist (only instantiation is Todoist-specific)
let todoist_integration = if let Ok(api_key) = std::env::var("TODOIST_API_KEY") {
    use holon_todoist::{TodoistClient, TodoistSyncProvider, TodoistSyncProviderBuilder};
    use holon_todoist::datasource::TodoistTaskDataSource;
    use holon::core::queryable_cache::QueryableCache;
    
    // Create Todoist-specific components
    let client = TodoistClient::new(&api_key);
    let datasource = Arc::new(TodoistTaskDataSource::new(&api_key));
    
    // Create QueryableCache using RenderEngine's backend
    let cache = Arc::new(
        QueryableCache::new_with_backend(datasource, backend.clone())
            .await
            .map_err(|e| miette::miette!("Failed to create cache: {}", e))?
    );
    
    // Create sync provider
    let provider = Arc::new(
        TodoistSyncProviderBuilder::new(client)
            .with_tasks_cache(cache.clone())
            .build()
    );
    
    // Register operation provider
    engine.register_provider("todoist-task".to_string(), cache.clone() as Arc<dyn OperationProvider>).await?;
    
    // Register syncable provider
    engine.register_syncable_provider("todoist".to_string(), provider.clone() as Arc<Mutex<dyn SyncableProvider>>).await;
    
    // Map table to entity
    engine.map_table_to_entity("todoist_tasks".to_string(), "todoist-task".to_string()).await;
    
    // Initial sync
    {
        let mut provider_mut = provider.lock().await;
        provider_mut.sync().await?;
    }
    
    Some(()) // Just a marker that Todoist is enabled
} else {
    None
};
```

**Note**: `RenderEngine` needs major refactoring to use `QueryableCache` instead of `TursoBackend` directly. This is a significant architectural change that will require moving functionality.

### Open Question: Multiple QueryableCache Instances Sharing TursoBackend

**Problem**: When multiple `QueryableCache` instances share the same `TursoBackend`, how do we handle CDC events efficiently?

**Proposals**:

#### Option A: Each QueryableCache Filters by Table Name (Current Proposal)

**Approach**: Each `QueryableCache` subscribes to `TursoBackend.row_changes()` and filters events by its table name.

**Pros**:
- ✅ Simple implementation - just filter the stream
- ✅ Each cache is independent
- ✅ No changes needed to TursoBackend
- ✅ Works with existing CDC infrastructure

**Cons**:
- ❌ Inefficient: All caches receive all CDC events, even if they filter them out
- ❌ If 10 caches exist, TursoBackend emits 10 copies of each event
- ❌ Wastes CPU/memory on filtering

#### Option B: TursoBackend Supports Table-Specific Subscriptions

**Approach**: Extend `TursoBackend.row_changes()` to accept table name filter. Each cache subscribes only to its table.

**Pros**:
- ✅ Efficient: Only relevant events sent to each cache
- ✅ Scalable: Performance doesn't degrade with more caches
- ✅ Better resource usage

**Cons**:
- ❌ Requires changes to TursoBackend CDC infrastructure
- ❌ More complex subscription management
- ❌ Need to handle subscription lifecycle (add/remove tables)

**Implementation Sketch**:
```rust
// TursoBackend
pub fn row_changes_for_table(&self, table_name: &str) -> Result<(Connection, RowChangeStream)> {
    // Set up callback that filters by table_name before emitting
}

// QueryableCache
let (_, stream) = backend.row_changes_for_table(&table_name)?;
```

#### Option C: Single QueryableCache Manager

**Approach**: Create a `QueryableCacheManager` that manages multiple tables in one cache, handles all CDC internally.

**Pros**:
- ✅ Single CDC subscription
- ✅ Centralized change handling
- ✅ Can optimize internally

**Cons**:
- ❌ Breaks single-responsibility principle
- ❌ Harder to extend (need to modify manager for new systems)
- ❌ Less flexible than independent caches

#### Option D: RenderEngine Subscribes to TursoBackend Directly

**Approach**: `QueryableCache` doesn't implement `ChangeNotifications`. Instead, `RenderEngine` subscribes to `TursoBackend` CDC and routes changes to appropriate caches.

**Pros**:
- ✅ Centralized CDC handling in RenderEngine
- ✅ RenderEngine can optimize routing
- ✅ Single CDC subscription

**Cons**:
- ❌ QueryableCache loses reactive capabilities
- ❌ Tighter coupling between RenderEngine and QueryableCache
- ❌ Harder to use QueryableCache standalone

**Recommendation**: **Option B** (Table-specific subscriptions) seems best for scalability, but requires TursoBackend changes. **Option A** (filtering) is simplest for MVP and can be optimized later.

**Question**: Which approach do you prefer? Should we start with Option A for MVP and optimize to Option B later?

#### 4. Generic State (No Todoist-Specific Types)

**File**: `frontends/tui/src/state.rs`

```rust
pub struct State {
    // Existing fields - all generic
    pub engine: Arc<RwLock<RenderEngine>>,
    pub render_spec: RenderSpec,
    pub data: Vec<StorageEntity>, // Generic StorageEntity, not TodoistTask
    pub selected_index: usize,
    pub status_message: String,
    pub cdc_receiver: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<RowChange>>>,
    pub selected_block_id_cache: Option<String>,
    pub main_thread_sender_channel: Arc<Mutex<Option<tokio::sync::mpsc::Sender<...>>>>,
    pub has_pending_cdc_changes: Arc<Mutex<bool>>,
    
    // r3bl framework fields
    pub editor_buffers: HashMap<FlexBoxId, EditorBuffer>,
    pub dialog_buffers: HashMap<FlexBoxId, DialogBuffer>,
    pub editing_block_index: Option<usize>,
    pub editing_buffer: Option<EditorBuffer>,
    pub keybindings: Arc<KeyBindingConfig>,
    
    // NO Todoist-specific fields!
    // NO ViewMode enum!
    // Everything comes from PRQL query results
}
```

#### 5. PRQL Query for External System

**File**: `frontends/tui/src/launcher.rs`

```rust
// Comment out hardcoded blocks query, replace with Todoist query (temporary)
// Later: make queries user-configurable

let prql_query = if todoist_integration.is_some() {
    // Query Todoist tasks
    r#"
    from todoist_tasks
    select {
        id,
        content as title,
        completed,
        priority,
        due_date,
        project_id,
        parent_id
    }
    render (list hierarchical_sort:[parent_id, order] item_template:(row (checkbox checked:this.completed) (text content:this.title) (badge content:this.priority color:"cyan")))
    "#.to_string()
} else {
    // Original blocks query
    r#"
    from blocks
    select {
        id,
        parent_id,
        depth,
        sort_key,
        content,
        completed,
        block_type,
        collapsed
    }
    render (list hierarchical_sort:[parent_id, sort_key] item_template:(row (checkbox checked:this.completed) (editable_text content:this.content) (text content:" ") (badge content:block_type color:"cyan")))
    "#.to_string()
};

let params = HashMap::new();

// Query and set up CDC streaming (works for any table)
let (render_spec, initial_data, cdc_stream) = engine
    .query_and_watch(prql_query, params)
    .await
    .map_err(|e| miette::miette!("Failed to query: {}", e))?;
```

#### 6. Generic Sync Operation Handler

**File**: `frontends/tui/src/app_main.rs`

```rust
// In app_handle_input_event:
match input_event {
    // ... existing handlers ...
    
    // Generic sync trigger (works for any SyncableProvider)
    InputEvent::Keyboard(KeyPress::Plain { key: Key::Character('r') }) => {
        let engine = global_data.state.engine.clone();
        let sender_opt = global_data.state.main_thread_sender_channel.lock().unwrap().clone();
        
        tokio::spawn(async move {
            let engine_guard = engine.read().await;
            match engine_guard.sync_all_providers().await {
                Ok(_) => {
                    if let Some(sender) = sender_opt {
                        let _ = sender.send(
                            r3bl_tui::TerminalWindowMainThreadSignal::ApplyAppSignal(
                                AppSignal::OperationResult {
                                    operation_name: "sync".to_string(),
                                    success: true,
                                    error_message: None,
                                }
                            )
                        ).await;
                    }
                }
                Err(e) => {
                    if let Some(sender) = sender_opt {
                        let _ = sender.send(
                            r3bl_tui::TerminalWindowMainThreadSignal::ApplyAppSignal(
                                AppSignal::OperationResult {
                                    operation_name: "sync".to_string(),
                                    success: false,
                                    error_message: Some(e.to_string()),
                                }
                            )
                        ).await;
                    }
                }
            }
        });
        
        global_data.state.status_message = "Syncing...".to_string();
        return Ok(EventPropagation::ConsumedRender);
    }
}
```

**Note**: This is hardcoded for now (keyboard shortcut), but sync itself is generic.

### Success Criteria

- ✅ External system tasks appear in TUI when configured
- ✅ Pressing sync key triggers sync on all `SyncableProvider`s
- ✅ Tasks refresh in UI after sync completes
- ✅ Changes flow through ChangeNotifications → QueryableCache → RenderEngine database → CDC stream → TUI
- ✅ No Todoist-specific code in State or ViewMode
- ✅ Rendering determined by PRQL `render` clause

### Dependencies

- `SyncableProvider` implements `#[operations_trait]`
- `RenderEngine` manages `SyncableProvider` collection
- `QueryableCache` writes to `RenderEngine`'s database
- PRQL query for external system table

### Testing

- Use fake datasource for testing without API key
- Verify sync triggers updates in UI
- Test with real external system API key (manual testing)

---

## MVP 2: Unified Query Interface

**Goal**: Query both internal blocks and Todoist tasks using a single PRQL query interface.

### Requirements

1. **Cross-System Queries**
   - PRQL queries can reference both `blocks` and `todoist_tasks` tables
   - RenderEngine can compile queries spanning multiple data sources
   - Results merge seamlessly in UI

2. **Unified Rendering**
   - Single render spec handles mixed data types
   - Visual distinction (icons/colors) for different sources
   - Operations work on both block and task types

### Technical Approach

- Extend `RenderEngine` to support multiple data sources
- Create unified schema mapping for cross-system queries
- Enhance PRQL compiler to handle joins across sources

---

## MVP 3: Bi-Directional Sync

**Goal**: Changes made in TUI sync back to Todoist API.

### Requirements

1. **Update Operations**
   - Marking task complete updates Todoist
   - Editing task title updates Todoist
   - Changes queue when offline

2. **Sync Status Indicators**
   - Show sync status (synced ✓, pending ⏳, error ⚠️)
   - Display last sync time
   - Error messages for failed syncs

### Technical Approach

- Use `QueryableCache` CRUD operations (already delegates to DataSource)
- Add operation queue for offline changes
- Implement retry logic for failed syncs

---

## MVP 4: Multiple External Systems

**Goal**: Support JIRA and Linear integrations alongside Todoist.

### Requirements

1. **Pluggable Integrations**
   - Same architecture works for any external system
   - Configuration-driven integration setup
   - Unified type system (Task trait)

2. **Cross-System Operations**
   - Link blocks to tasks from any system
   - Unified search across all systems
   - Cross-system references in UI

### Technical Approach

- Implement trait system for unified types (`Task`, `Project`, etc.)
- Create integration registry
- Extend QueryableCache to support multiple sources

---

## MVP 5: Offline-First with Conflict Resolution

**Goal**: Work offline with automatic sync and conflict resolution when back online.

### Requirements

1. **Offline Queue**
   - Changes queue locally when offline
   - Automatic retry when connection restored
   - Clear indication of pending changes

2. **Conflict Resolution**
   - Detect conflicts (local vs remote changes)
   - UI for resolving conflicts
   - Automatic resolution strategies (last-write-wins, merge, etc.)

### Technical Approach

- Extend operation queue with conflict detection
- Add conflict resolution UI component
- Implement reconciliation engine

---

## MVP 6: Custom Visualizations

**Goal**: Support kanban boards, tables, and other custom views for tasks.

### Requirements

1. **View Types**
   - Kanban board (drag-and-drop columns)
   - Table view (spreadsheet-like)
   - Calendar view (time-based)

2. **View Configuration**
   - Declarative view definitions
   - User-configurable views
   - Save/load view configurations

### Technical Approach

- Extend PRQL render spec with view types
- Implement view-specific rendering components
- Add view configuration storage

---

## MVP 7: Automation Rules

**Goal**: Define rules for automatic actions (e.g., create Todoist task when block created).

### Requirements

1. **Rule Engine**
   - Trigger → Action rules
   - Conditions and filters
   - Rule execution engine

2. **Built-in Rules**
   - Create task when block tagged `#task`
   - Update JIRA when block marked done
   - Auto-tag based on content

### Technical Approach

- Create rule engine with trigger/action system
- Add rule configuration UI
- Implement rule execution runtime

---

## MVP 8: Sharing and Collaboration

**Goal**: Share parts of knowledge graph with others.

### Requirements

1. **Sharing Model**
   - Read-only sharing
   - Collaborative editing (P2P sync)
   - Permission management

2. **P2P Sync**
   - Direct peer connections
   - CRDT-based conflict resolution
   - Real-time collaboration

### Technical Approach

- Use existing Loro CRDT infrastructure
- Implement P2P connection layer
- Add sharing/permission UI

---

## Implementation Notes

### Architecture Principles

1. **Separation of Concerns**: Internal blocks (Loro CRDT) vs external systems (cached views)
2. **Stream-Based Updates**: All changes flow through ChangeNotifications
3. **QueryableCache Pattern**: Transparent caching layer for all external systems
4. **Progressive Enhancement**: Each MVP builds on previous capabilities

### Key Design Decisions

- **ChangeNotifications First**: All updates flow through streams, enabling reactive UI
- **QueryableCache Abstraction**: Unified interface for all data sources
- **PRQL for Queries**: Declarative query language for flexibility
- **Render Specs**: Declarative UI definitions separate from data

### Migration Path

Each MVP is designed to be:
- **Backward Compatible**: Existing functionality continues to work
- **Optional**: Features can be enabled/disabled via configuration
- **Incremental**: Can be implemented and tested independently

---

## Next Steps

1. **Start with MVP 1**: Implement Todoist tasks display with manual sync
2. **Validate Architecture**: Ensure ChangeNotifications flow works end-to-end
3. **Iterate**: Refine based on user feedback before moving to MVP 2
4. **Document**: Update architecture docs as patterns emerge
