# Architectural Principles

This document describes the foundational architectural decisions that guide the design of Holon. These principles are stable and should not require frequent updates as the implementation evolves.

For detailed technical documentation, see `docs/architecture.md` and `docs/architecture2.md`.

## Foundational Goal: Trust Enables Flow

Every architectural decision ultimately serves one purpose: enabling users to achieve **flow states** through **trust**.

- Trust that nothing is forgotten
- Trust that the right thing is being worked on
- Trust that relevant context is accessible

This shapes our priorities: reliability over features, transparency over magic, user control over automation.

---

## Core Principle: External Systems as First-Class Citizens

Unlike traditional PKM tools that treat external systems as import/export targets, Holon treats them as primary data sources with full operational capability.

**Implications:**

1. **Lossless Storage**: Data from external systems is stored in a format as close to the source as possible. Deviations must be bijective (reversible), such as column renaming. This ensures:
   - All operations available in the external system can be performed locally
   - All data from the external system can be displayed without loss
   - Round-trip fidelity when syncing back

2. **Operations, Not Just Data**: We expose every useful operation that the external system's API provides, not just CRUD. Users can mark tasks complete, change priorities, move items between projects—all without leaving the app.

3. **Unified View, Diverse Sources**: Items from different systems can appear in the same query result and view. A project page can show Todoist tasks alongside JIRA issues alongside internal notes, each with its native capabilities intact.

---

## The Three Modes

The UI architecture is organized around three modes that match how humans actually work:

### Capture Mode
**Purpose**: Quick input, get it out of my head

**Architectural Requirements**:
- Sub-100ms input latency for block creation
- Works offline with instant local commit
- Keyboard-driven quick add (global hotkey, command palette)
- Capture to current context (project/task) or to inbox
- Inbox that processes to zero

**Out of Scope**: Mobile-optimized capture, voice notes, email forwarding. These are better handled by integrated tools (Todoist, etc.). Holon's strength is integration, not competing on every feature.

### Orient Mode
**Purpose**: Big picture, daily/weekly reviews
**Architectural Requirements**:
- Watcher Dashboard with cross-system synthesis
- Efficient aggregation queries across all data sources
- CDC-driven real-time updates
- Risk/deadline/dependency analysis
- "Nothing forgotten" completeness guarantees

### Flow Mode
**Purpose**: Deep focus on present task

**Architectural Requirements**:
- Context Bundle assembly (related items across systems)
- Selective loading (only relevant context)
- Distraction hiding (non-relevant items filtered)
- Single-task view with all needed context
- Minimal UI chrome

---

## Context Bundles

When a user focuses on a project or task, the system assembles a **Context Bundle**:

```
Context Bundle for "Project X"
├── Native Holon blocks about X
├── Todoist tasks in project X
├── JIRA issues linked to X
├── Calendar events tagged X
├── Gmail threads about X
└── Related items (via embeddings)
```

**Architectural Principles**:
1. Context Bundles are computed, not stored (derived from links + queries)
2. Links are explicit (user-created) or inferred (AI-generated with confidence scores)
3. Bundle assembly must be fast (<200ms for typical project)
4. Bundles update reactively as underlying data changes

---

## Data Flow Architecture

### Reactive Sync Pattern

Operations flow one-way without blocking the UI for responses:

```
User Action → Operation Dispatch → External/Internal System
                                          ↓
UI ← CDC Stream ← QueryableCache ← Sync Provider
```

**Key aspects:**
- Operations are "fire and forget"—the UI doesn't await a response
- Effects are observed through sync with the external system
- Changes propagate through the QueryableCache as a stream
- Internal and external modifications are treated identically

### Change Data Capture (CDC)

Changes propagate from storage to UI via CDC streams:

```
Database Write → Turso CDC → BatchWithMetadata<RowChange> → UI Stream
```

This architecture enables:
- Real-time UI updates without polling
- Distributed tracing through the entire pipeline
- Consistent handling of local and remote changes

### Trace Context Propagation

Every operation carries trace context through the entire system:

```
Flutter UI (trace_id) → FFI Bridge → Operation → Database (_change_origin column)
                                                        ↓
                                     CDC Callback reads trace context
                                                        ↓
                                     Change event includes origin trace
```

This enables debugging, audit trails, and understanding causation across async boundaries.

---

## AI Services Architecture

AI is organized into three services that map to user needs:

```
┌─────────────────────────────────────────────────────────────────┐
│                    AI Services Layer (Rust)                     │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │   Watcher    │  │  Integrator  │  │    Guide     │          │
│  │              │  │              │  │              │          │
│  │ • Monitoring │  │ • Linking    │  │ • Patterns   │          │
│  │ • Alerts     │  │ • Context    │  │ • Insights   │          │
│  │ • Synthesis  │  │ • Search     │  │ • Growth     │          │
│  │ • Conflicts  │  │ • Bundles    │  │ • Shadow     │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Foundation Layer                                        │  │
│  │  • Local embeddings (sentence-transformers)              │  │
│  │  • Full-text search (Tantivy)                            │  │
│  │  • Pattern/conflict logs                                 │  │
│  │  • Trust Ladder state                                    │  │
│  │  • LLM access (local or cloud, user-controlled)          │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### AI Architectural Principles

1. **Async & Non-Blocking**: AI operations never block the UI thread
2. **Local-First**: Embeddings, search, and classification run on-device
3. **Progressive Trust**: AI earns autonomy through demonstrated accuracy
4. **Transparent Reasoning**: Every AI suggestion includes explanation
5. **Easy Override**: User can always undo or correct AI decisions
6. **Learning Loop**: Corrections feed back into training data

### Trust Ladder

AI autonomy is gated by demonstrated competence:

| Level | Behavior | Gate |
|-------|----------|------|
| Passive | Answers when asked | Default |
| Advisory | Suggests, user decides | >80% acceptance |
| Agentic | Acts with permission | Low correction rate |
| Autonomous | Acts within bounds | Extended track record + opt-in |

Trust is tracked **per-feature**, not globally.

---

## Privacy & Deployment Architecture

Three deployment models with different privacy/capability tradeoffs:

### Option 1: Fully Local (Maximum Privacy)

```
┌─────────────────────────────────────────┐
│              User Device                │
│  ┌─────────────────────────────────┐   │
│  │  Holon App                      │   │
│  │  • All data local               │   │
│  │  • GGUF models (llama.cpp)      │   │
│  │  • Zero cloud dependency        │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

### Option 2: Hybrid (Recommended)

```
┌─────────────────────────────────────────┐
│              User Device                │
│  ┌─────────────────────────────────┐   │
│  │  Holon App                      │   │
│  │  • All data local               │   │
│  │  • Embeddings local             │   │
│  │  • Classification local         │   │
│  └──────────────┬──────────────────┘   │
└─────────────────┼───────────────────────┘
                  │ Opt-in, minimal context
                  ▼
┌─────────────────────────────────────────┐
│           Cloud LLM (GPT-4/Claude)      │
│  • Complex reasoning only               │
│  • User controls what is sent           │
└─────────────────────────────────────────┘
```

### Option 3: Self-Hosted

```
┌─────────────────────────────────────────┐
│              User Device                │
│  ┌─────────────────────────────────┐   │
│  │  Holon App                      │   │
│  └──────────────┬──────────────────┘   │
└─────────────────┼───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│        User's LLM Server                │
│  (Ollama, vLLM, etc.)                   │
│  • Full control                         │
│  • Good model quality                   │
└─────────────────────────────────────────┘
```

### Privacy Architectural Principles

1. **Data Never Leaves Without Consent**: All data stays local by default
2. **Minimal Context**: When cloud AI is used, send minimum necessary context
3. **User Visibility**: Clear indication of what goes where
4. **Graceful Degradation**: App fully functional without cloud AI

---

## Query and Render Architecture

### Declarative Queries with PRQL

Users specify what data they want using PRQL, including how it should be rendered:

```prql
from todoist_tasks
filter completed == false
select {id, content, priority, completed}
render (list item_template:(row
  (checkbox checked:this.completed)
  (text content:this.content)))
```

The `render` clause is a declarative UI specification that gets compiled alongside the SQL.

### Automatic Operation Discovery

The system automatically determines which operations are available for each rendered item:

1. **Lineage Analysis**: Traces which database columns flow into which UI widgets
2. **Operation Matching**: Compares widget parameters against operation `required_params`
3. **UI Annotation**: Attaches available operations to the rendered tree

A checkbox bound to `completed` automatically gets `set_completion` wired up because:
- The widget type is "checkbox"
- Its `checked` parameter traces to the `completed` column
- An operation exists that modifies `completed` with the available parameters

For multi-widget operations (e.g., `move_block` requiring `parent_id` from a drop target), **Gesture-Scoped Parameter Providers** extend this system. Widgets declare what params they provide, gestures accumulate params into a context, and operations declare mappings from alternative sources (e.g., `selected_id` → `parent_id`). See `docs/GESTURE_PARAM_PROVIDERS.md` for details.

### RenderSpec Tree

Query compilation produces a `RenderSpec`—a data structure describing what to render:

```
RenderSpec
├── RenderExpr::FunctionCall("list", ...)
│   └── RenderExpr::FunctionCall("row", ...)
│       ├── RenderExpr::FunctionCall("checkbox", checked: ColumnRef("completed"))
│       │   └── operations: [OperationWiring { set_completion... }]
│       └── RenderExpr::FunctionCall("text", content: ColumnRef("content"))
```

The frontend interprets this tree to create native UI components while preserving operation bindings.

---

## Storage Architecture

### Unified Query Cache (Turso)

All data—regardless of source—flows into a **single Turso cache** for querying:

```
┌─────────────────────────────────────────────────────────────────┐
│                    UNIFIED TURSO CACHE                          │
│            (SQLite-compatible, single query surface)            │
│                                                                 │
│    PRQL/SQL queries run here against ALL data uniformly        │
│    Operations modify data here, then sync to sources           │
└───────────────────┬─────────────────────────┬───────────────────┘
                    │                         │
          syncs from/to              syncs from/to
                    │                         │
            ┌───────▼───────┐         ┌───────▼───────────┐
            │  LORO CRDT    │         │  THIRD-PARTY      │
            │               │         │  APIs             │
            │  Source of    │         │                   │
            │  truth for    │         │  Source of truth  │
            │  owned data   │         │  for external     │
            └───────────────┘         └───────────────────┘
```

**Key insight**: The UI never queries Loro or external APIs directly. Everything goes through the unified Turso cache. This enables:
- Single query language (PRQL/SQL) for all data
- Consistent CDC stream for all changes
- Uniform operation dispatch regardless of data source

### Source of Truth by Data Type

| Data Type | Source of Truth | Sync Direction |
|-----------|-----------------|----------------|
| Owned blocks, links, properties | Loro CRDT | Loro → Turso (and Turso → Loro for edits) |
| External system data (Todoist, JIRA, etc.) | External API | API → Turso (and Turso → API for operations) |
| User metadata on external items | Loro CRDT | Loro → Turso |
| AI embeddings | Generated on-device | Computed → Turso |
| Pattern/conflict logs | Local only | Local Turso (not synced) |

**Rationale**: CRDTs excel at collaborative editing of owned data. External systems are server-authoritative—we cache their data and queue operations, but don't pretend to own it.

### Plain-Text File Layer

Local files (Markdown or Org Mode) provide an additional interface to owned data:

```
┌─────────────────────────────────────────┐
│           External Editors              │
│     (Vim, Emacs, VS Code, etc.)         │
└────────────────┬────────────────────────┘
                 │ reads/writes
                 │
┌────────────────▼────────────────────────┐
│         Plain-Text Files                │
│    (Markdown/Org Mode on disk)          │
└────────────────┬────────────────────────┘
                 │ bidirectional sync
                 │
┌────────────────▼────────────────────────┐
│            Loro CRDT                    │
│      (Source of truth for owned)        │
└─────────────────────────────────────────┘
```

**Capabilities**:
- Files act as a bidirectional cache of CRDT content
- External edits to files are detected and merged into CRDTs
- Enables interop with other tools
- Provides human-readable backup and portability

**Open questions**: Exact reconciliation strategy between file edits and CRDT state is TBD. Goal: you can always edit your notes in any text editor, and Holon will incorporate those changes.

### Sync Token Management

Sync tokens are persisted atomically with data in a single transaction:

```
BEGIN TRANSACTION
  -- Apply all data changes
  INSERT/UPDATE/DELETE ...
  -- Save sync token
  INSERT INTO sync_states (provider_name, sync_token) VALUES (...)
COMMIT
```

This prevents inconsistency between cached data and sync position.

---

## Operation System Architecture

### Trait-Based Operations

Operations are defined via traits, not string-based dispatch:

```rust
trait MutableTaskDataSource<T> {
    async fn set_completion(&self, id: &str, completed: bool);
    async fn set_priority(&self, id: &str, priority: i64);
}
```

Procedural macros generate `OperationDescriptor` metadata from these traits.

### Operation Descriptors

Each operation is described with metadata for UI generation:

```rust
OperationDescriptor {
    entity_name: "todoist-task",
    name: "set_completion",
    required_params: ["id", "completed"],
    affected_fields: ["completed"],
    precondition: Some(PreconditionChecker { ... }),
}
```

The UI uses this to:
- Show only applicable operations (based on available params)
- Wire operation callbacks to widgets
- Validate before dispatch

### Composite Operation Dispatch

`OperationDispatcher` aggregates multiple `OperationProvider` implementations:

```
OperationDispatcher
├── QueryableCache<TodoistDataSource, TodoistTask>
├── QueryableCache<JiraDataSource, JiraIssue>
└── QueryableCache<InternalBlockSource, Block>
```

Operations are routed by `entity_name` to the appropriate provider.

---

## UI Architecture

### Frontend Agnosticism

The backend exposes a minimal FFI surface that any frontend can implement:

```rust
// Core FFI functions
fn init_render_engine() -> RenderEngine;
fn compile_query(prql: &str) -> CompiledQuery;
fn execute_operation(entity: &str, op: &str, params: StorageEntity);
fn watch_changes() -> Stream<Change<StorageEntity>>;
```

This enables:
- Flutter frontend (current primary)
- TUI frontend (secondary)
- Future: native Rust UI, web frontend

### Reactive Updates

Frontends subscribe to change streams and update reactively:

```dart
watchChanges().listen((changes) {
  for (change in changes) {
    updateWidget(change.id, change.data);
  }
});
```

No explicit refresh calls—UI state derives from the change stream.

---

## Trust & Flow Observability

The system exposes observable properties that support trust and flow:

### Sync Status (Trust)

Every external item has visible sync status:
- ✓ Synced (matches external system)
- ⏳ Pending (local changes queued)
- ⚠️ Conflict (requires resolution)
- ❌ Error (sync failed)

### Completeness Indicators (Trust)

Orient mode shows system completeness:
- All systems connected and synced
- No unprocessed inbox items
- All reviews completed
- No stuck/overdue items (or explicit count)

### Focus Metrics (Flow)

Flow mode tracks:
- Time in current focus session
- Context switches (should be zero)
- Interruption count

---

## Dependency Injection

The system uses `ferrous-di` for service resolution:

```rust
// Registration
container.register::<dyn OperationProvider>(QueryableCache::new(...));

// Resolution
let dispatcher = OperationDispatcher::from_container(&container);
```

This enables:
- Testability (mock providers)
- Modularity (add providers without changing core code)
- Configuration (different providers for different environments)

---

## Extension Points

### Adding a New External System

1. Implement `DataSource<T>` for read-only cache access
2. Implement `CrudOperationProvider<T>` for write operations
3. Implement domain-specific traits (e.g., `MutableTaskDataSource`)
4. Create a `SyncProvider` for incremental sync
5. Register in DI container via a module

### Adding a New Operation

1. Add method to appropriate trait (or create new trait)
2. Annotate with `#[affects("field1", "field2")]`
3. Implement in relevant providers
4. Macros auto-generate operation descriptor

### Adding a New UI Widget Type

1. Add function stub in lineage preprocessor (if using auto-wiring)
2. Implement widget in each frontend
3. Widget receives `RenderExpr` with operation bindings

### Adding a New AI Capability

1. Determine which service owns it (Watcher/Integrator/Guide)
2. Define required data inputs and outputs
3. Implement with local-first approach where possible
4. Add Trust Ladder gating if the feature takes autonomous actions
5. Include reasoning/explanation in output

---

## Consistency Guarantees

### Local Consistency

Within a single client:
- Database transactions ensure atomic updates
- CDC delivers changes in commit order
- UI reflects committed state

### External Consistency

With external systems:
- Eventually consistent (5-30 second typical delay)
- Last-write-wins for concurrent edits
- Sync tokens prevent duplicate processing
- AI-assisted conflict detection and resolution

### P2P Consistency (Future)

Between devices:
- Loro CRDTs ensure convergence
- No central server required
- Works offline, syncs when connected

---

## Related Documents

- `VISION_LONG_TERM.md` - Philosophical foundation and product vision
- `VISION.md` - Technical vision and roadmap
- `VISION_AI.md` - AI feature specifications
- `ARCHITECTURE.md` - Detailed technical architecture
- `REACTIVE_PRQL_RENDERING.md` - Query/render system details
