# Tatuin vs holon: Comparison & Reusable Techniques

## Executive Summary

**Tatuin** is a terminal-based task aggregator (TUI) that brings together tasks from multiple external providers (Todoist, Obsidian, GitLab, GitHub, iCal, CalDAV). It's simpler, more focused, and has already solved several problems that holon will face.

**Key Insight**: While architecturally different (TUI aggregator vs. desktop PKM), Tatuin has **production-ready patterns** for external system integration that align perfectly with holon's Phase 2-3 goals.

---

## Architecture Alignment Matrix

| Feature | Tatuin | holon (Planned) | Reusability |
|---------|--------|--------------------------|-------------|
| **External System Integration** | ✅ 6+ providers | ✅ Planned (Todoist, Gmail, Jira, Linear) | **HIGH** |
| **Provider Trait System** | ✅ Trait-based plugins | ✅ Planned adapter pattern | **HIGH** |
| **Local Cache** | ✅ redb for native tasks | ✅ SQLite for external data | **MEDIUM** |
| **Task Model** | ✅ Task/Project traits | ✅ Schema with Entity trait | **HIGH** |
| **Incremental Updates** | ✅ ValuePatch pattern | ❓ Not specified | **HIGH** |
| **Filter System** | ✅ Multi-level filters | ✅ Planned (location, time, energy) | **MEDIUM** |
| **Conflict Resolution** | ✅ Last-write-wins | ✅ Last-write-wins for external | **MEDIUM** |
| **State Persistence** | ✅ TOML-based states | ✅ Loro for internal data | **LOW** |
| **CRDT Support** | ❌ None | ✅ Loro for internal content | **N/A** |
| **Rich Text Editing** | ❌ None | ✅ TipTap/ProseMirror | **N/A** |
| **UI Framework** | Ratatui (TUI) | Tauri (Desktop) | **LOW** |

---

## Top 5 Reusable Techniques from Tatuin

### 1. ⭐ **Provider Trait Architecture** (HIGHEST PRIORITY)

**What Tatuin Does:**
```rust
#[async_trait]
pub trait ProviderTrait: TaskProviderTrait + ProjectProviderTrait + Send + Sync {
    fn name(&self) -> String;
    fn type_name(&self) -> String;
    fn capabilities(&self) -> Capabilities;
    async fn reload(&mut self);
}

#[async_trait]
pub trait TaskProviderTrait {
    async fn list(
        &mut self,
        project: Option<Box<dyn ProjectTrait>>,
        filter: &Filter,
    ) -> Result<Vec<Box<dyn TaskTrait>>, StringError>;

    async fn create(&mut self, project_id: &str, patch: &TaskPatch)
        -> Result<(), StringError>;

    async fn update(&mut self, patches: &[TaskPatch]) -> Vec<PatchError>;

    async fn delete(&mut self, task: &dyn TaskTrait) -> Result<(), StringError>;
}
```

**Why It's Perfect for holon:**
- ✅ Already handles 6+ different external systems
- ✅ Supports varying capabilities (read-only, full CRUD, partial update)
- ✅ Async-first design (won't block UI)
- ✅ Send + Sync for thread safety
- ✅ Trait composition (ProviderTrait = TaskProviderTrait + ProjectProviderTrait)

**How to Adapt:**
```rust
// holon adaptation
#[async_trait]
pub trait ExternalProvider: Send + Sync {
    fn name(&self) -> String;
    fn capabilities(&self) -> ProviderCapabilities;

    // Generic fetch/sync
    async fn sync_from_remote(&mut self, storage: &dyn StorageBackend)
        -> Result<SyncStats>;
    async fn sync_to_remote(&mut self, storage: &dyn StorageBackend)
        -> Result<SyncStats>;

    // Query operations (read from cache)
    async fn list_tasks(&self, filter: &Filter)
        -> Result<Vec<Entity>>;
    async fn get_task(&self, id: &str)
        -> Result<Option<Entity>>;
}

pub struct ProviderCapabilities {
    pub can_create: bool,
    pub can_update: bool,
    pub can_delete: bool,
    pub sync_interval: Option<Duration>,
}
```

**Code to Reuse:**
- `tatuin-core/src/provider.rs` - Core trait definitions
- `tatuin-providers/src/todoist.rs` - Full CRUD example (320 lines)
- `tatuin-providers/src/obsidian.rs` - File-based provider (290 lines)
- `tatuin-providers/src/gitlab_todo.rs` - Limited update example (180 lines)

---

### 2. ⭐ **ValuePatch Pattern for Incremental Updates**

**What Tatuin Does:**
```rust
// Distinguishes "not set" vs "clear field" vs "set to value"
pub enum ValuePatch<T> {
    NotSet,      // Don't update this field
    Empty,       // Clear/set to null
    Value(T),    // Set to specific value
}

pub struct TaskPatch {
    pub task: Option<Box<dyn TaskTrait>>,  // Original task for reference
    pub name: ValuePatch<String>,
    pub description: ValuePatch<String>,
    pub due: ValuePatch<DuePatchItem>,
    pub priority: ValuePatch<Priority>,
    pub state: ValuePatch<State>,
}

// Usage example:
let patch = TaskPatch {
    task: Some(current_task),
    name: ValuePatch::NotSet,           // Don't change
    description: ValuePatch::Empty,      // Clear description
    due: ValuePatch::Value(new_date),    // Set new due date
    priority: ValuePatch::NotSet,        // Don't change
    state: ValuePatch::Value(State::Completed),
};

provider.update(&[patch]).await?;
```

**Why It's Valuable:**
- ✅ Avoids unnecessary API calls (only send changed fields)
- ✅ Handles nullable fields correctly
- ✅ Type-safe representation of "optional changes"
- ✅ Works across different external APIs with different update semantics

**holon Adaptation:**
```rust
// Could be used in StorageBackend trait
pub enum FieldUpdate<T> {
    Keep,        // Don't modify
    Clear,       // Set to null
    Set(T),      // Update value
}

pub struct EntityPatch {
    pub entity_type: String,
    pub entity_id: String,
    pub fields: HashMap<String, FieldUpdate<Value>>,
}

impl StorageBackend {
    async fn patch(&mut self, patch: EntityPatch) -> Result<()> {
        // Only update changed fields
    }
}
```

**Code to Reuse:**
- `tatuin-core/src/task_patch.rs` - Complete implementation (150 lines)

---

### 3. ⭐ **Todoist Provider Implementation**

**What It Provides:**
- Full CRUD operations against Todoist REST API v2
- Proper error handling and rate limiting
- Efficient filtering (completed vs uncompleted)
- Priority mapping (Todoist's 1-4 → generic 6-level priority)
- In-memory caching with smart invalidation

**Key Implementation Details:**
```rust
pub struct TodoistProvider {
    api_key: String,
    client: reqwest::Client,
    name: String,
    tasks_cache: Option<Vec<TodoistTask>>,
    projects_cache: Option<Vec<TodoistProject>>,
    last_reload: Option<SystemTime>,
}

impl TodoistProvider {
    // Smart caching: reload if filter changes or cache expired
    async fn list(&mut self, project: Option<Box<dyn ProjectTrait>>,
                  filter: &Filter) -> Result<Vec<Box<dyn TaskTrait>>> {
        if self.should_reload(filter) {
            self.reload().await?;
        }

        // Apply project filter
        let tasks = match project {
            Some(proj) => self.tasks_cache.iter()
                .filter(|t| t.project_id == proj.id())
                .collect(),
            None => self.tasks_cache.clone(),
        };

        // Apply state filter
        filter_tasks(tasks, filter)
    }

    // Efficient update: detect which fields changed
    async fn update(&mut self, patches: &[TaskPatch]) -> Vec<PatchError> {
        let mut errors = vec![];

        for patch in patches {
            // Route to appropriate endpoint based on what changed
            if let ValuePatch::Value(state) = &patch.state {
                self.update_task_state(&patch.task.id(), state).await?;
            }
            if let ValuePatch::Value(due) = &patch.due {
                self.update_task_due(&patch.task.id(), due).await?;
            }
            if patch.name != ValuePatch::NotSet ||
               patch.description != ValuePatch::NotSet {
                self.update_task_content(&patch.task.id(), patch).await?;
            }
        }

        errors
    }
}
```

**Why This Matters for holon:**
- ✅ **Todoist is in the roadmap** (architecture.md Phase 2)
- ✅ Can be adapted directly with minimal changes
- ✅ Already handles all edge cases (completed tasks, projects, priorities)
- ✅ Production-tested code (not a prototype)

**Adaptation Strategy:**
1. Keep the Todoist API client almost unchanged
2. Replace in-memory cache with `StorageBackend` trait
3. Add dirty tracking for bidirectional sync
4. Use holon's schema system for type safety

**Code Location:**
- `tatuin-providers/src/todoist.rs` (320 lines) - **READY TO REUSE**

---

### 4. **Multi-Level Filter System**

**What Tatuin Does:**
```rust
#[derive(Debug, Clone)]
pub struct Filter {
    pub states: Vec<FilterState>,
    pub due: Vec<Due>,
}

pub enum FilterState {
    Completed,
    Uncompleted,
    InProgress,
    Unknown,
}

pub enum Due {
    Overdue,
    Today,
    Future,
    NoDate,
}

// Filters applied at 3 levels:
// 1. Provider level (optimizes API calls)
provider.list(project, &filter).await?;

// 2. UI level (additional client-side filtering)
tasks.retain(|t| matches_filter(t, &ui_filter));

// 3. User toggles (interactive filter widget)
filter_widget.toggle_state(FilterState::Completed);
```

**holon Adaptation:**
```rust
// Extends to more dimensions (location, energy, people)
pub struct Filter {
    pub states: Vec<TaskState>,
    pub due: Vec<DueFilter>,
    pub locations: Vec<String>,      // NEW: "home", "office", "anywhere"
    pub energy_levels: Vec<u8>,      // NEW: 1-5
    pub people: Vec<String>,         // NEW: "@alice", "@bob"
    pub tags: Vec<String>,
}

// Can be passed to both internal (Loro) and external (SQLite) storage
trait StorageBackend {
    async fn query(&self, entity: &str, filter: Filter)
        -> Result<Vec<Entity>>;
}
```

**Code to Reuse:**
- `tatuin-core/src/filter.rs` (80 lines) - Base filter types
- `tatuin/src/ui/filter_widget.rs` (295 lines) - Interactive UI (adapt for Tauri)

---

### 5. **Configuration Management**

**What Tatuin Does:**
```rust
// settings.toml
[providers.MyTodoist]
type = "Todoist"
api_key = "secret123"
disabled = false

[providers.WorkObsidian]
type = "Obsidian"
path = "/Users/me/Vault"

[[states.work]]
Provider = "MyTodoist"
Project = "Work"
Filter = "Uncompleted,Today"

// Code:
pub struct Settings {
    pub providers: HashMap<String, HashMap<String, String>>,
    pub states: HashMap<String, State>,
    pub theme: Option<String>,
}

impl Settings {
    pub fn load() -> Self {
        let config_dir = folders::config_dir();
        config::Config::builder()
            .add_source(config::File::with_name(&format!(
                "{}/settings.toml", config_dir
            )))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap()
    }
}
```

**Why This Matters:**
- ✅ Multi-provider configuration already solved
- ✅ Credentials management (API keys, tokens)
- ✅ Per-provider settings (paths, URLs, filters)
- ✅ Human-readable TOML format

**holon Adaptation:**
```rust
// Extend to include sync intervals, conflict strategies
[providers.todoist]
type = "Todoist"
api_key = "..."
sync_interval = "5m"
conflict_resolution = "last-write-wins"

[providers.gmail]
type = "Gmail"
oauth_token = "..."
sync_interval = "15m"
filter = "label:tasks"
```

**Code to Reuse:**
- `tatuin/src/settings.rs` (150 lines) - Configuration loading
- `tatuin-core/src/folders.rs` (40 lines) - Cross-platform config paths

---

## Additional Reusable Components

### 6. **Async Job Queue**
**File:** `tatuin/src/async_jobs.rs` (80 lines)

```rust
pub struct AsyncJobStorage {
    jobs: Vec<String>,
    tx: broadcast::Sender<()>,  // Notifies UI on changes
}

// Usage:
async_jobs.add("Syncing Todoist tasks...").await;
// ... long operation ...
async_jobs.remove(job_id).await;
```

**holon Use Case:**
- Background sync jobs (don't block UI)
- Show sync status in Tauri status bar
- Track multiple simultaneous syncs

---

### 7. **Error Handling Pattern**
**File:** `tatuin-core/src/string_error.rs`

```rust
pub type StringError = Box<dyn std::error::Error + Send + Sync + Display>;

// Allows any error type to be converted
impl From<reqwest::Error> for StringError { ... }
impl From<std::io::Error> for StringError { ... }
```

**Benefit:** Simplified error propagation across provider boundaries

---

### 8. **Capabilities System**
```rust
pub struct Capabilities {
    pub create_task: bool,
}

impl ProviderTrait {
    fn capabilities(&self) -> Capabilities {
        Capabilities { create_task: true }
    }
}

// UI can disable "Create Task" button if !provider.capabilities().create_task
```

**holon Use Case:**
- Dynamic UI based on provider capabilities
- Show/hide features based on what's supported
- Graceful degradation for read-only providers

---

## Code Migration Strategy

### Phase 1: Extract Reusable Core (1-2 weeks)
1. **Copy trait definitions** from `tatuin-core`:
   - `provider.rs` → Adapt to `ExternalProvider` trait
   - `task_patch.rs` → Rename to `entity_patch.rs`
   - `filter.rs` → Extend with location/energy/people filters
   - `string_error.rs` → Use as-is

2. **Create adapter layer**:
   ```rust
   // Adapter converts Tatuin's ProviderTrait to holon's ExternalProvider
   struct TatuinProviderAdapter<P: ProviderTrait> {
       inner: P,
       storage: Arc<Mutex<dyn StorageBackend>>,
   }

   impl<P: ProviderTrait> ExternalProvider for TatuinProviderAdapter<P> {
       async fn sync_from_remote(&mut self, storage: &dyn StorageBackend)
           -> Result<SyncStats> {
           // 1. Call inner.reload()
           // 2. Call inner.list()
           // 3. Write to storage
       }
   }
   ```

### Phase 2: Migrate Todoist Provider (2-3 days)
1. Copy `tatuin-providers/src/todoist.rs`
2. Replace in-memory cache with SQLite storage
3. Add dirty tracking for bidirectional sync
4. Test against holon's StorageBackend trait
5. Add version tracking (etags) for conflict detection

### Phase 3: Add More Providers (1 week each)
- Gmail provider (similar to iCal - read-only initially)
- Jira provider (similar to GitLab TODO)
- Linear provider (similar to GitHub Issues)

---

## Key Differences to Address

| Tatuin | holon | Migration Strategy |
|--------|-----------------|-------------------|
| In-memory cache | SQLite/Loro storage | Replace cache with StorageBackend calls |
| Manual reload (Ctrl+R) | Periodic background sync | Add sync scheduler with intervals |
| No dirty tracking | Bidirectional sync | Add `mark_dirty()` calls on local changes |
| No version tracking | Conflict detection | Add `version` field to entities |
| Single-threaded Tokio | Multi-threaded (likely) | Ensure Send + Sync everywhere |
| TUI widgets | Tauri IPC | Replace UI with Tauri commands |
| redb for native tasks | SQLite for external cache | Adapt schema to SQLite tables |

---

## Implementation Timeline

### Immediate (Next 2 Weeks)
1. ✅ **Create `external-providers` crate** in holon
2. ✅ **Copy core traits** from tatuin-core
3. ✅ **Adapt Todoist provider** with StorageBackend integration
4. ✅ **Write integration tests** against mock StorageBackend

### Short-term (1-2 Months)
1. ✅ Implement sync scheduler (periodic background sync)
2. ✅ Add dirty tracking + conflict resolution
3. ✅ Build UI for provider configuration
4. ✅ Add 2-3 more providers (Gmail, Jira)

### Long-term (3-6 Months)
1. ✅ Block reference system (reference external tasks in notes)
2. ✅ Cross-provider queries (search across all systems)
3. ✅ Smart sync (only sync changed data)

---

## Recommended Next Steps

1. **Read Tatuin's Code:**
   - `tatuin-core/src/provider.rs` - Trait definitions (essential)
   - `tatuin-providers/src/todoist.rs` - Full CRUD example
   - `tatuin-core/src/task_patch.rs` - Incremental update pattern

2. **Create Proof of Concept:**
   - Copy `tatuin-providers/src/todoist.rs` into holon
   - Implement `TatuinProviderAdapter` to bridge trait systems
   - Test sync_from_remote() with SQLite backend
   - Verify conflict handling works

3. **Design Decisions to Make:**
   - Use Tatuin's trait system as-is, or adapt to architecture.md's design?
   - Keep provider-specific types (TodoistTask), or force everything into Entity?
   - Sync interval strategy: per-provider or global?

4. **Potential Issues:**
   - **Type erasure:** Tatuin uses `Box<dyn TaskTrait>`, which loses type info
     - **Solution:** Use `Entity` (HashMap) consistently, or add `as_any()` method
   - **Async complexity:** Nested async calls can be hard to debug
     - **Solution:** Add comprehensive tracing (Tatuin already has this)
   - **Error handling:** StringError is simple but loses type info
     - **Solution:** Consider structured error types for better error recovery

---

## Conclusion

**Tatuin is a goldmine for holon's external integration layer.** The code is:
- ✅ Production-ready (not a toy project)
- ✅ Well-architected (trait-based, modular)
- ✅ Directly applicable (Todoist provider can be reused almost unchanged)
- ✅ Battle-tested (handles 6+ different external APIs)

**Highest Priority Reuse:**
1. **Todoist provider** (320 lines) - Ready to adapt for Phase 2
2. **ProviderTrait system** - Perfect match for architecture.md's adapter pattern
3. **ValuePatch pattern** - Solves incremental update problem elegantly
4. **Filter system** - Good foundation for holon's extended filters

**Estimated Time Savings:** 2-4 weeks of development + testing by reusing Tatuin's provider infrastructure instead of building from scratch.
