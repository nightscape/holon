# OperationLog: Unified Undo/Redo (Offline Sync Deferred)

## Goal

Replace the in-memory `UndoStack` with a persistent `OperationLog` entity that:
1. Survives app restarts (persistent undo history)
2. Integrates with existing CDC/query infrastructure (Flutter watches undo state like any data)
3. Uses existing entity macros for schema derivation

**Offline sync is deferred to a separate session.**

## Key Insight

Treat `OperationLog` as a first-class entity using `#[entity]` macro. Flutter queries for undo/redo state via PRQL and receives updates automatically through CDC.

---

## Phase 1: OperationLog Entity

### 1.1 Define OperationLog entity with macro

**File:** `crates/holon-api/src/operation_log.rs` (NEW)

```rust
use holon_macros::entity;
use super::Operation;

/// Status of an operation in the log
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationStatus {
    PendingSync,  // Waiting for sync (future use)
    Synced,       // Confirmed synced (future use)
    Undone,       // Operation was undone
    Cancelled,    // Undone before sync (future use)
}

/// Logged operation - wraps Operation with undo/redo/sync metadata
#[entity(name = "operations", short_name = "op")]
pub struct OperationLog {
    pub id: i64,

    /// The operation itself (embedded, not serialized to JSON)
    pub operation: Operation,

    /// The inverse operation for undo
    pub inverse: Option<Operation>,

    /// Current status
    pub status: OperationStatus,

    /// When the operation was executed
    pub created_at: i64,

    /// For sync (future): when eligible to sync
    pub sync_eligible_at: Option<i64>,
}
```

The `#[entity]` macro will derive:
- DDL for table creation
- Field mappings for queries
- Operation wiring metadata

### 1.2 OperationLog service

**File:** `crates/holon/src/core/operation_log.rs` (NEW)

```rust
pub struct OperationLogService {
    backend: Arc<RwLock<TursoBackend>>,
    max_log_size: usize,  // Default 100, trim oldest on insert
}

impl OperationLogService {
    pub async fn new(backend: Arc<RwLock<TursoBackend>>) -> Result<Self>;
    pub async fn initialize_table(&self) -> Result<()>;

    // Core operations
    pub async fn log_operation(&self, op: Operation, inverse: Option<Operation>) -> Result<i64>;
    pub async fn get_undo_candidate(&self) -> Result<Option<OperationLog>>;
    pub async fn get_redo_candidate(&self) -> Result<Option<OperationLog>>;
    pub async fn mark_undone(&self, id: i64) -> Result<()>;
    pub async fn mark_redone(&self, id: i64) -> Result<()>;

    // Trim old entries
    async fn trim_if_needed(&self) -> Result<()>;
}
```

### 1.3 Undo/Redo Logic

**Undo candidate:** Most recent operation where `status NOT IN ('undone', 'cancelled')`

**Redo candidate:** Most recent operation where `status = 'undone'` AND no newer non-undone operation exists

**Undo action:**
1. Get undo candidate
2. If `status = 'pending_sync'`: set `status = 'cancelled'` (never syncs)
3. If `status = 'synced'`: set `status = 'undone'` (inverse will sync)
4. Execute inverse operation
5. Log the inverse as a new operation (for redo)

**Redo action:**
1. Get redo candidate
2. Set `status` back to `pending_sync` or `synced`
3. Execute the original operation again

---

## Phase 2: Integrate with BackendEngine

### 2.1 Update BackendEngine

**File:** `crates/holon/src/api/backend_engine.rs`

- Replace `undo_stack: Arc<RwLock<UndoStack>>` with `operation_log: Arc<OperationLog>`
- Update `execute_operation()` to call `operation_log.log_operation()`
- Update `undo()` / `redo()` to use `operation_log`
- Keep `can_undo()` / `can_redo()` methods (query the log)

### 2.2 Update DI registration

**File:** `crates/holon/src/di/mod.rs`

- Register `OperationLogConfig` singleton
- Register `OperationLog` singleton factory
- Inject `OperationLog` into `BackendEngine`

---

## Phase 3: Expose Undo State via Query/CDC

### 3.1 Create undo_state virtual entity

Flutter can query undo/redo state like any data:

```prql
from undo_state
select {can_undo, can_redo, undo_display_name, redo_display_name}
```

This could be:
- A SQL view over `operation_log`
- A virtual table registered with the query engine
- A special PRQL function

**Simplest approach:** SQL view

```sql
CREATE VIEW undo_state AS
SELECT
    (SELECT COUNT(*) > 0 FROM operation_log
     WHERE status NOT IN ('undone', 'cancelled')) AS can_undo,
    (SELECT display_name FROM operation_log
     WHERE status NOT IN ('undone', 'cancelled')
     ORDER BY id DESC LIMIT 1) AS undo_display_name,
    (SELECT COUNT(*) > 0 FROM operation_log
     WHERE status = 'undone') AS can_redo,
    (SELECT display_name FROM operation_log
     WHERE status = 'undone'
     ORDER BY id DESC LIMIT 1) AS redo_display_name;
```

### 3.2 CDC for undo state

When `operation_log` changes, CDC fires for any query watching it.

Flutter's existing `query_and_watch` will automatically receive updates when:
- New operation logged
- Operation status changes (undo/redo)

---

## Phase 4: Undo/Redo as Operations

### 4.1 Register undo/redo operations

Create an `OperationLogProvider` that exposes:

```
entity_name: "*"  (wildcard - available globally)
operations:
  - name: "undo", display_name: "Undo", params: []
  - name: "redo", display_name: "Redo", params: []
```

Flutter can trigger undo via:
```dart
backendService.executeOperation(
  entityName: "*",
  opName: "undo",
  params: {},
);
```

### 4.2 Special handling for undo/redo operations

These operations:
- Do NOT log themselves to the operation_log (would cause infinite loop)
- Execute the inverse/original operation internally
- Update status in the log

---

## Phase 5: Debounced Sync (Offline Support)

### 5.1 SyncWorker background task

**File:** `crates/holon/src/core/sync_worker.rs` (NEW)

```rust
pub struct SyncWorker {
    log: Arc<OperationLog>,
    dispatcher: Arc<OperationDispatcher>,
    poll_interval: Duration,
}

impl SyncWorker {
    pub async fn run(&self) {
        loop {
            let pending = self.log.get_pending_sync().await;
            for op in pending.filter(|o| o.sync_eligible_at <= now()) {
                self.log.mark_syncing(&[op.id]).await;
                match self.dispatcher.sync_to_external(&op).await {
                    Ok(_) => self.log.mark_synced(&[op.id]).await,
                    Err(_) => self.log.mark_sync_failed(op.id).await,
                }
            }
            sleep(self.poll_interval).await;
        }
    }
}
```

### 5.2 Undo cancels pending sync

In `OperationLog::mark_undone()`:
- If `status = 'pending_sync'`: change to `'cancelled'`
- The operation never reaches the external system
- User's undo is instant, no network round-trip wasted

---

## Implementation Order

### Step 1: OperationLog basics
- [ ] Create `operation_log.rs` with struct and table creation
- [ ] Implement `log_operation`, `get_undo_candidate`, `get_redo_candidate`
- [ ] Implement `mark_undone`, `mark_redone`
- [ ] Add tests

### Step 2: BackendEngine integration
- [ ] Add `OperationLog` to `BackendEngine`
- [ ] Update `execute_operation` to log operations
- [ ] Update `undo()` / `redo()` to use log
- [ ] Update DI registration
- [ ] Remove old `UndoStack` usage

### Step 3: Query/CDC for undo state
- [ ] Create `undo_state` SQL view
- [ ] Verify CDC fires on `operation_log` changes
- [ ] Test Flutter receiving undo state updates

### Step 4: Undo/Redo as operations
- [ ] Create `OperationLogProvider`
- [ ] Register with `OperationDispatcher`
- [ ] Handle special case (don't log undo/redo operations)

### Step 5: Sync worker (offline support)
- [ ] Implement `SyncWorker`
- [ ] Add `sync_eligible_at` debounce logic
- [ ] Implement undo-cancels-sync
- [ ] Start worker in engine initialization

---

## Files to Modify

| File | Change |
|------|--------|
| `crates/holon/src/core/operation_log.rs` | NEW - OperationLog struct |
| `crates/holon/src/core/sync_worker.rs` | NEW - Background sync worker |
| `crates/holon/src/core/mod.rs` | Export new modules |
| `crates/holon/src/api/backend_engine.rs` | Replace UndoStack with OperationLog |
| `crates/holon/src/di/mod.rs` | Register OperationLog |
| `crates/holon-core/src/undo.rs` | DEPRECATED (keep for reference) |

Flutter changes are minimal - existing `query_and_watch` handles undo state reactively.

---

## Open Questions

1. **Redo semantics**: Should redo re-execute the operation (getting fresh inverse) or replay the logged inverse?
   - Recommendation: Re-execute for consistency with current behavior

2. **Log retention**: How many operations to keep?
   - Recommendation: Configurable, default 100, trim oldest on insert

3. **Sync retry policy**: Exponential backoff on failure?
   - Recommendation: Start simple (fixed interval), enhance later
