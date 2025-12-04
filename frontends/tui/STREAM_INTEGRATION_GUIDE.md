# TUI-R3BL Adaptation Guide for Stream-Based External System Integration

## Overview

The new stream-based external system integration (Phase 3.4) provides a reactive architecture for integrating external systems like Todoist. This document outlines what needs to be adapted in the tui frontend to use this new implementation.

## Current Architecture

The tui frontend currently uses:
- **RenderEngine** for internal database operations (blocks)
- **CDC streaming** via `query_and_watch()` for reactive updates
- **Operations** executed via `engine.execute_operation()`

This is for **internal blocks** stored in the local Turso database.

## New Stream-Based Architecture

The Phase 3.4 implementation provides:
- **QueryableCache** - Transparent proxy wrapping datasources
- **TodoistProvider** - Polls external API and emits changes on streams
- **TodoistTaskDataSource** - Real HTTP implementation (stateless)
- **TodoistTaskFake** - Fake implementation for testing/offline mode
- **Stream-based sync** - Changes flow via broadcast channels

This is for **external systems** like Todoist that need to sync with remote APIs.

## When to Use Each Architecture

### Internal Blocks (Current - No Changes Needed)
- **Use**: `RenderEngine` + `query_and_watch()` + CDC streams
- **For**: Blocks stored in local Turso database
- **Status**: ✅ Already implemented and working

### External Systems (New - Needs Integration)
- **Use**: `QueryableCache` + `TodoistProvider` + stream-based sync
- **For**: Todoist tasks, projects, or other external APIs
- **Status**: ⚠️ Not yet integrated into tui

## Integration Requirements

### 1. Add Dependencies

**File**: `frontends/tui/Cargo.toml`

```toml
[dependencies]
# ... existing dependencies ...
holon-todoist = { path = "../../crates/holon-todoist" }
```

### 2. Update State Structure

**File**: `frontends/tui/src/state.rs`

**Current State** (for internal blocks):
```rust
pub struct State {
    pub engine: Arc<RwLock<RenderEngine>>,  // Internal blocks
    pub render_spec: RenderSpec,
    pub data: Vec<HashMap<String, Value>>,
    // ... CDC receiver for internal blocks ...
}
```

**Add External System State** (for Todoist tasks):
```rust
pub struct State {
    // Existing internal blocks state
    pub engine: Arc<RwLock<RenderEngine>>,
    pub render_spec: RenderSpec,
    pub data: Vec<HashMap<String, Value>>,

    // NEW: External system integration
    pub todoist_cache: Option<Arc<holon::core::StreamCache as QueryableCache<holon_todoist::TodoistTask>>>,
    pub todoist_provider: Option<Arc<holon_todoist::stream_provider::TodoistProvider>>,
    pub todoist_data: Vec<holon_todoist::TodoistTask>,  // Cached Todoist tasks
    pub todoist_selected_index: Option<usize>,  // Selected Todoist task index
}
```

### 3. Initialize Todoist Integration in Launcher

**File**: `frontends/tui/src/launcher.rs`

**Add Todoist initialization** (optional - only if API key provided):

```rust
use holon::core::StreamCache as QueryableCache;
use holon::storage::turso::TursoBackend;
use holon_todoist::stream_provider::TodoistProvider;
use holon_todoist::stream_datasource::TodoistTaskDataSource;
use holon_todoist::models::TodoistTask;

pub async fn run_app(db_path: PathBuf) -> CommonResult<()> {
    // ... existing RenderEngine setup ...

    // NEW: Initialize Todoist integration (if API key provided)
    let todoist_api_key = std::env::var("TODOIST_API_KEY").ok();
    let (todoist_cache, todoist_provider) = if let Some(api_key) = todoist_api_key {
        // Create datasource (real HTTP implementation)
        let datasource = Arc::new(TodoistTaskDataSource::new(&api_key));

        // Create cache database (separate from blocks database)
        let todoist_db = Arc::new(RwLock::new(
            Box::new(TursoBackend::new_in_memory().await?) as Box<dyn holon::storage::backend::StorageBackend>
        ));

        // Create cache wrapping datasource
        let cache = Arc::new(QueryableCache::new(
            datasource,
            todoist_db,
            "todoist_tasks".to_string(),
        ));

        // Create provider with builder pattern
        let client = holon_todoist::TodoistClient::new(&api_key);
        let provider = Arc::new(
            TodoistProvider::new(client)
                .with_tasks(cache.clone())
                .build()
        );

        // Initial sync to populate cache
        let mut provider_mut = Arc::try_unwrap(provider.clone()).unwrap_or_else(|arc| {
            // If Arc has multiple references, create a new provider
            let client = holon_todoist::TodoistClient::new(&api_key);
            TodoistProvider::new(client)
                .with_tasks(cache.clone())
                .build()
        });
        provider_mut.sync().await?;

        // Load initial tasks from cache
        let initial_tasks = cache.get_all().await?;

        (Some(cache), Some(provider))
    } else {
        (None, None)
    };

    // Update State initialization to include Todoist
    let initial_state = State::new(
        engine,
        render_spec,
        initial_data,
        cdc_receiver,
        todoist_cache,
        todoist_provider,
    );

    // ... rest of launcher setup ...
}
```

### 4. Update State Methods

**File**: `frontends/tui/src/state.rs`

**Add methods for Todoist operations**:

```rust
impl State {
    // ... existing methods ...

    /// Get all Todoist tasks from cache
    pub async fn get_todoist_tasks(&self) -> Result<Vec<TodoistTask>, String> {
        if let Some(ref cache) = self.todoist_cache {
            cache.get_all().await
                .map_err(|e| format!("Failed to get Todoist tasks: {}", e))
        } else {
            Ok(vec![])
        }
    }

    /// Update Todoist task field
    pub async fn update_todoist_task_field(
        &self,
        task_id: &str,
        field: &str,
        value: holon::storage::types::Value,
    ) -> Result<(), String> {
        if let Some(ref cache) = self.todoist_cache {
            cache.set_field(task_id, field, value).await
                .map_err(|e| format!("Failed to update Todoist task: {}", e))
        } else {
            Err("Todoist not configured".to_string())
        }
    }

    /// Create new Todoist task
    pub async fn create_todoist_task(
        &self,
        fields: std::collections::HashMap<String, holon::storage::types::Value>,
    ) -> Result<String, String> {
        if let Some(ref cache) = self.todoist_cache {
            cache.create(fields).await
                .map_err(|e| format!("Failed to create Todoist task: {}", e))
        } else {
            Err("Todoist not configured".to_string())
        }
    }

    /// Delete Todoist task
    pub async fn delete_todoist_task(&self, task_id: &str) -> Result<(), String> {
        if let Some(ref cache) = self.todoist_cache {
            cache.delete(task_id).await
                .map_err(|e| format!("Failed to delete Todoist task: {}", e))
        } else {
            Err("Todoist not configured".to_string())
        }
    }

    /// Trigger Todoist sync (manual refresh)
    pub async fn sync_todoist(&self) -> Result<(), String> {
        if let Some(ref provider) = self.todoist_provider {
            // Note: Provider needs &mut self, so we'd need to wrap differently
            // For now, this is a limitation - sync happens automatically via stream
            Err("Manual sync not yet implemented - changes arrive via stream".to_string())
        } else {
            Err("Todoist not configured".to_string())
        }
    }
}
```

### 5. Add Todoist Stream Subscription

**File**: `frontends/tui/src/launcher.rs`

**Subscribe to Todoist change stream** (similar to CDC watcher):

```rust
// After creating Todoist cache and provider
if let Some(ref cache) = todoist_cache {
    let mut rx = provider.subscribe_tasks();
    let todoist_data_ref = initial_state.todoist_data.clone(); // Need to make this Arc<Mutex<Vec<...>>>

    // Spawn background task to ingest Todoist changes
    tokio::spawn(async move {
        use tokio_stream::wrappers::BroadcastStream;
        use tokio_stream::StreamExt;

        let mut stream = BroadcastStream::from(rx);
        while let Some(Ok(changes)) = stream.next().await {
            // Changes are already ingested into cache by QueryableCache
            // We just need to trigger UI refresh
            // This could use the same CDC watcher pattern
        }
    });
}
```

### 6. Update UI to Display Todoist Tasks

**File**: `frontends/tui/src/app_main.rs` or `src/components/block_list.rs`

**Add Todoist task rendering** (if Todoist is enabled):

```rust
// In render method, check if Todoist is enabled
if let Some(ref todoist_data) = global_data.state.todoist_data {
    // Render Todoist tasks alongside or instead of blocks
    for (idx, task) in todoist_data.iter().enumerate() {
        // Render task with checkbox, content, priority, etc.
    }
}
```

### 7. Add Keyboard Shortcuts for Todoist Operations

**File**: `frontends/tui/src/app_main.rs`

**Add handlers for Todoist-specific operations**:

```rust
// In app_handle_input_event
match input_event {
    // ... existing handlers ...

    // NEW: Todoist-specific shortcuts (only if Todoist enabled)
    InputEvent::Keyboard(KeyPress::Plain { key: Key::Character('t') }) => {
        if global_data.state.todoist_cache.is_some() {
            // Toggle between blocks view and Todoist view
            // Or show Todoist tasks in a separate panel
        }
    }

    // Space on Todoist task: Toggle completion
    InputEvent::Keyboard(KeyPress::Plain { key: Key::Character(' ') }) => {
        if let Some(ref selected_task_id) = global_data.state.selected_todoist_task_id() {
            let cache = global_data.state.todoist_cache.clone().unwrap();
            let task_id = selected_task_id.clone();
            tokio::spawn(async move {
                // Get current task
                if let Ok(Some(task)) = cache.get_by_id(&task_id).await {
                    // Toggle completion
                    let new_value = holon::storage::types::Value::Boolean(!task.completed);
                    let _ = cache.set_field(&task_id, "completed", new_value).await;
                }
            });
        }
    }
}
```

## Key Differences from Current Architecture

### Internal Blocks (Current)
- **Data Source**: Local Turso database
- **Sync**: CDC streams from database changes
- **Operations**: Via RenderEngine.execute_operation()
- **Updates**: Immediate (local database)

### External Systems (New)
- **Data Source**: External API (Todoist)
- **Sync**: Provider polls API → emits on broadcast channel → cache ingests
- **Operations**: Via QueryableCache (delegates to DataSource)
- **Updates**: Asynchronous (arrive via stream after API call)

## Migration Strategy

### Option 1: Separate Views (Recommended for MVP)
- Keep internal blocks view as-is
- Add separate Todoist view (toggle with 't' key)
- Each view has its own state and rendering logic

### Option 2: Unified View (Future Enhancement)
- Merge blocks and Todoist tasks into single hierarchical view
- Use different icons/colors to distinguish sources
- More complex but provides unified experience

### Option 3: Hybrid Approach
- Show blocks by default
- Show Todoist tasks in sidebar or separate panel
- Allow linking blocks to Todoist tasks

## Testing Considerations

### Use Fake Datasource for Testing
```rust
// In tests or development mode
use holon_todoist::fake::TodoistTaskFake;

let fake = Arc::new(TodoistTaskFake::new().await?);
let cache = Arc::new(QueryableCache::new(
    fake.clone(),
    todoist_db,
    "todoist_tasks".to_string(),
));
```

### Integration Test Flow
1. Create fake datasource
2. Create cache wrapping fake
3. Create tasks via cache
4. Verify tasks appear in UI
5. Update tasks via cache
6. Verify UI updates via stream

## Implementation Checklist

### Phase 1: Basic Integration
- [ ] Add `holon-todoist` dependency
- [ ] Update State to include Todoist cache/provider
- [ ] Initialize Todoist in launcher (if API key provided)
- [ ] Add methods to State for Todoist operations
- [ ] Test with fake datasource

### Phase 2: UI Integration
- [ ] Add Todoist task rendering
- [ ] Add keyboard shortcuts for Todoist operations
- [ ] Add view toggle (blocks vs Todoist)
- [ ] Handle stream updates in UI

### Phase 3: Polish
- [ ] Add error handling for API failures
- [ ] Add loading indicators during sync
- [ ] Add status messages for Todoist operations
- [ ] Add configuration UI for API key

## Example: Complete Integration Pattern

```rust
// In launcher.rs
pub async fn run_app(db_path: PathBuf) -> CommonResult<()> {
    // ... existing RenderEngine setup ...

    // Initialize Todoist (optional)
    let todoist_integration = if let Ok(api_key) = std::env::var("TODOIST_API_KEY") {
        Some(setup_todoist(&api_key).await?)
    } else {
        None
    };

    // Create state with both internal and external systems
    let initial_state = State::new(
        engine,
        render_spec,
        initial_data,
        cdc_receiver,
        todoist_integration,
    );

    // ... rest of setup ...
}

async fn setup_todoist(api_key: &str) -> Result<TodoistIntegration, Box<dyn std::error::Error>> {
    // Create datasource
    let datasource = Arc::new(TodoistTaskDataSource::new(api_key));

    // Create cache database
    let db = Arc::new(RwLock::new(
        Box::new(TursoBackend::new_in_memory().await?) as Box<dyn StorageBackend>
    ));

    // Create cache
    let cache = Arc::new(QueryableCache::new(
        datasource,
        db,
        "todoist_tasks".to_string(),
    ));

    // Create provider
    let client = TodoistClient::new(api_key);
    let provider = Arc::new(
        TodoistProvider::new(client)
            .with_tasks(cache.clone())
            .build()
    );

    // Initial sync
    let mut provider_mut = Arc::try_unwrap(provider.clone())
        .map_err(|_| "Failed to unwrap provider")?;
    provider_mut.sync().await?;

    // Load initial tasks
    let initial_tasks = cache.get_all().await?;

    Ok(TodoistIntegration {
        cache,
        provider,
        tasks: initial_tasks,
    })
}

struct TodoistIntegration {
    cache: Arc<QueryableCache<TodoistTask>>,
    provider: Arc<TodoistProvider>,
    tasks: Vec<TodoistTask>,
}
```

## Notes

1. **Separation of Concerns**: Internal blocks and external systems use different architectures by design
2. **No Breaking Changes**: Existing block functionality remains unchanged
3. **Optional Integration**: Todoist integration is optional - app works without it
4. **Stream Updates**: Changes arrive asynchronously via broadcast channels
5. **Fire-and-Forget**: Operations return immediately, updates arrive via stream

## Future Enhancements

- Add support for multiple external systems (not just Todoist)
- Unified query interface that can query both internal and external data
- Cross-system linking (link blocks to Todoist tasks)
- Sync status indicators in UI
- Conflict resolution for concurrent edits

