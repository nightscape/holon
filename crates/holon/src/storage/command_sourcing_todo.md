# TODO: Command Sourcing Architecture (Offline-First)

## Overview
Implement event sourcing pattern to enable offline-first operation with external system sync.

**Problem**: Current optimistic update strategy doesn't work offline:
- Turso updates pile up without external sync validation
- No replay mechanism when reconnecting
- No handling for long offline periods

**Solution**: Command sourcing with durable log + background sync worker

## Architecture Components

### 1. Command Log Schema
```sql
CREATE TABLE commands (
    id TEXT PRIMARY KEY,              -- Client-generated UUID (idempotency key)
    entity_id TEXT NOT NULL,          -- Block/document ID (for ordering)
    command_type TEXT NOT NULL,       -- 'indent', 'update_content', 'move_block', etc.
    payload JSON NOT NULL,            -- Command parameters
    status TEXT DEFAULT 'pending',    -- 'pending', 'syncing', 'synced', 'failed'
    target_system TEXT NOT NULL,      -- 'loro', 'todoist', 'local'
    created_at INTEGER NOT NULL,
    synced_at INTEGER,
    error_details TEXT,               -- API rejection reason for user feedback

    INDEX idx_pending (status, created_at),
    INDEX idx_entity (entity_id, created_at)
);
```

### 2. Command Types Enum
```rust
// crates/holon/src/storage/commands.rs (NEW FILE)
pub enum CommandType {
    // Content operations
    UpdateContent { block_id: String, content: String },
    CreateBlock { id: String, parent_id: String, content: String },
    DeleteBlock { id: String },

    // Structure operations
    IndentBlock { id: String },
    OutdentBlock { id: String },
    MoveBlock { id: String, target_parent: String, position: usize },

    // Bulk operations (for performance)
    BulkMove { ids: Vec<String>, target_parent: String },
    BulkDelete { ids: Vec<String> },
}

pub enum TargetSystem {
    Loro,      // CRDT, auto-merges
    Todoist,   // API, can reject
    Local,     // No external sync needed
}
```

### 3. Command Executor
```rust
// crates/holon/src/storage/command_executor.rs (NEW FILE)
pub struct CommandExecutor {
    db: Database,
    sync_queue: Arc<Mutex<VecDeque<Command>>>,
}

impl CommandExecutor {
    pub async fn execute(&mut self, cmd: Command) -> Result<()> {
        // 1. Generate client-side UUID (idempotency key)
        let cmd_id = Uuid::new_v4();

        // 2. Persist to command log FIRST (durable)
        self.persist_command(&cmd_id, &cmd)?;

        // 3. Apply optimistically to Turso (queryable cache)
        cmd.apply_to_turso(&mut self.db)?;

        // 4. CDC triggers UI update immediately (< 50ms)
        // (handled by existing view_changes stream)

        Ok(())
    }
}
```

### 4. Sync Worker
```rust
// crates/holon/src/storage/sync_worker.rs (NEW FILE)
pub struct SyncWorker {
    db: Database,
    loro_client: Arc<dyn LoroClient>,
    todoist_client: Arc<dyn TodoistClient>,
}

impl SyncWorker {
    pub async fn sync_loop(&mut self) {
        loop {
            if self.is_online().await {
                self.process_pending_commands().await;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn process_pending_commands(&mut self) -> Result<()> {
        // 1. Load pending, group by entity_id, order by created_at
        let commands = self.load_pending_commands()?;
        let by_entity = self.group_by_entity(commands);

        // 2. Process each entity's commands serially
        for (entity_id, cmds) in by_entity {
            // 3. Compact commands (optimization for long offline periods)
            let compacted = self.compact_commands(&cmds);

            // 4. Batch sync (up to 100 commands per HTTP request)
            for batch in compacted.chunks(100) {
                self.sync_batch(entity_id, batch).await?;
            }
        }

        Ok(())
    }

    async fn sync_batch(&mut self, entity_id: &str, cmds: &[Command]) -> Result<()> {
        // Send batch request with idempotency keys
        match self.sync_command_batch(cmds).await {
            Ok(results) => {
                for (cmd, result) in cmds.iter().zip(results) {
                    match result {
                        Ok(_) => self.mark_synced(&cmd.id)?,
                        Err(e) => {
                            self.mark_failed(&cmd.id, &e)?;
                            self.refetch_entity(entity_id).await?;
                            break;  // Stop processing this entity (Option A: Stop on Failure)
                        }
                    }
                }
            }
            Err(e) => {
                // Network error - will retry next loop
                log::warn!("Batch sync failed: {}", e);
            }
        }

        Ok(())
    }

    async fn refetch_entity(&mut self, entity_id: &str) -> Result<()> {
        // Fetch canonical state from external system
        let canonical = self.fetch_from_source(entity_id).await?;

        // Overwrite Turso cache with canonical state
        self.db.execute(
            "UPDATE blocks SET content = ?, updated_at = ? WHERE id = ?",
            [&canonical.content, &now(), entity_id]
        )?;

        // CDC triggers UI update (shows "rolled back" state)

        Ok(())
    }
}
```

### 5. Command Compaction (Optimization)
```rust
impl SyncWorker {
    fn compact_commands(&self, cmds: &[Command]) -> Vec<Command> {
        let mut last_content_update: Option<Command> = None;
        let mut non_compactable = Vec::new();

        for cmd in cmds {
            match cmd.command_type {
                CommandType::UpdateContent { .. } => {
                    // Keep only latest content update
                    last_content_update = Some(cmd.clone());
                }
                _ => {
                    // Structural changes must be preserved
                    non_compactable.push(cmd.clone());
                }
            }
        }

        if let Some(update) = last_content_update {
            non_compactable.push(update);
        }

        // Re-sort by created_at
        non_compactable.sort_by_key(|c| c.created_at);
        non_compactable
    }
}
```

## Open Question: Partial Batch Failure Handling

**Scenario**: Syncing 10 commands for `entity_id="block-123"`, command #5 fails.

### Option A: Stop on First Failure (RECOMMENDED)
- Mark failure, refetch entity, leave remaining commands pending
- **Pros**: Simple, predictable, entity in known state
- **Cons**: Partial sync (commands 1-4 applied, 6-8 pending)
- **When**: Default strategy, safest option

### Option B: Continue All, Mark Failures
- Execute all commands, collect failures, refetch at end
- **Pros**: Maximizes successful commands
- **Cons**: Commands 6-8 might depend on command 5
- **When**: Commands are known to be independent (rare)

### Option C: Abort, Refetch, Retry Remaining
- Refetch canonical state, adjust remaining commands, retry
- **Pros**: Ensures remaining commands apply to correct state
- **Cons**: Complex (need to adjust commands), expensive (refetch + recompute)
- **When**: Commands have complex dependencies

### Option D: Server-Side Transactional Batch
- Send entire batch as atomic transaction
- **Pros**: All-or-nothing semantics (cleanest)
- **Cons**: Requires server transaction support, one failure aborts all
- **When**: Server supports transactions, batch operations tightly coupled

**DECISION**: Implement Option A (stop on failure) initially. Add hooks for plugins to customize strategy per command type.

## Implementation Phases

### Phase 3.4: Command Sourcing Infrastructure
- [ ] Create `commands` table schema
- [ ] Implement `Command` struct and `CommandType` enum
- [ ] Implement `CommandExecutor` with optimistic Turso updates
- [ ] Add `persist_command()` and `apply_to_turso()` methods
- [ ] Unit tests for command persistence

### Phase 5.3: External System Sync
- [ ] Implement `SyncWorker` with `sync_loop()`
- [ ] Add network detection (`is_online()`)
- [ ] Implement `process_pending_commands()` with entity grouping
- [ ] Add command compaction for long offline periods
- [ ] Implement `refetch_entity()` for rollback on failure
- [ ] Add batch sync with idempotency keys
- [ ] Integration tests: offline edits → reconnect → sync

### Phase 5.4: Conflict Resolution
- [ ] Implement Loro CRDT client interface
- [ ] Implement Todoist API client interface
- [ ] Test CRDT auto-merge for content conflicts
- [ ] Test API rejection handling with rollback
- [ ] Add user notifications for sync failures

### Phase 6: Testing
- [ ] Property-based tests with failure injection at random positions
- [ ] Test command compaction (500 commands → 200 after compaction)
- [ ] Test long offline periods (1 week, 1000 edits)
- [ ] Test partial batch failures (all 4 strategies)
- [ ] Test idempotency (command replayed multiple times)

## Key Design Principles

1. **Idempotency**: Every command has client-generated UUID, sent as `Idempotency-Key` header
2. **Entity-Level Ordering**: Commands grouped by `entity_id`, processed serially per entity
3. **Optimistic UI**: Turso updates immediately, UI sees changes in < 50ms
4. **Background Sync**: Worker polls every 5 seconds, replays pending commands
5. **Rollback via Refetch**: Don't compute rollback, fetch canonical state from server
6. **Performance**: Command compaction + batch sync for long offline periods

## References

- Spec: `codev/specs/0001-reactive-prql-outliner-complete-spec.md`
- Plan: `codev/plans/0001-reactive-prql-rendering.md`
- Discussion: Architecture decisions (2025-01-04) with Gemini-2.5-Pro validation
