# Goals and Wishlist

This document describes what Rusty Knowledge aims to achieve. It focuses on the "what" and "why" from a user perspective, not the "how" (see `ARCHITECTURE_PRINCIPLES.md` for that).

For the implementation roadmap, see `VISION.md`.

## Ultimate Goal

A low-friction system that integrates all your sources and sinks of information and tasks, so when you check the system you have everything available to achieve your goals optimally.

No more:
- Switching between 5 apps to get context on a project
- Missing tasks because they're in a different system
- Manually copying information between tools
- Losing track of what's connected to what

## Core Goals

### 1. External Systems as First-Class Citizens

External systems (Todoist, JIRA, Linear, Gmail, Calendar) are not import/export targets—they are primary data sources with full operational capability.

**What this means:**
- See Todoist tasks, JIRA issues, and emails in the same view
- Perform operations directly (mark done, change priority, reply) without leaving the app
- Bi-directional sync: changes made here appear there, and vice versa
- Unified keyboard shortcuts for common operations across systems

**Status**: Todoist integration implemented with full CRUD operations. Architecture supports additional integrations.

### 2. Unified Query Across Systems

Query data from any system using a single language (PRQL), including joining data across systems.

**What this means:**
- `from todoist_tasks filter due_date < @today` works
- `from todoist_tasks | join jira_issues (...)` works
- Union queries show items from multiple systems in one list
- Same filtering, sorting, grouping syntax regardless of source

**Status**: PRQL compilation working. Single-source queries implemented. Cross-source joins planned.

### 3. Declarative Rendering with Automatic Operations

Specify how data should be displayed declaratively, and operations wire up automatically.

**What this means:**
- `render (checkbox checked:this.completed)` creates a checkbox
- The checkbox automatically triggers `set_completion` when clicked
- No manual wiring of callbacks—lineage analysis handles it
- Custom views without writing code

**Status**: Basic render expressions working. Lineage-based operation inference implemented.

### 4. Observe-Don't-Wait Architecture

Operations don't block the UI. Effects are observed through sync.

**What this means:**
- Click "complete" and the UI responds immediately (optimistically)
- The actual API call happens in the background
- If it fails, the UI corrects itself when sync completes
- Same flow for local and remote changes

**Status**: Implemented via CDC streams and reactive UI updates.

### 5. Offline-First with P2P Sync

Work without internet. Sync between devices without a central server.

**What this means:**
- Full functionality offline
- Changes queue and sync when connectivity returns
- Internal data syncs device-to-device via CRDTs
- External system operations queue until API is reachable

**Status**: Offline storage working. Loro CRDT integration partial. Iroh P2P planned.

### 6. Outliner/Hierarchical View

A block-based outliner is central to the experience, supporting workflows like PARA, GTD, and Zettelkasten.

**What this means:**
- Blocks can be nested arbitrarily deep
- Indent/outdent/move operations work naturally
- Any block can be "zoomed into" as the root
- Backlinks show where blocks are referenced
- Transclusion embeds blocks in multiple places

**Status**: Block hierarchy implemented. Basic outliner operations working. Backlinks planned.

### 7. Cross-System Linking

Link between items in different systems. Create project pages that aggregate related items.

**What this means:**
- Link a Todoist task to a Gmail message
- Link a JIRA issue to internal meeting notes
- Project pages show all related items from all systems
- Links can be internal (stored locally) when external systems shouldn't link

**Status**: Architecture supports linking via internal metadata. Implementation planned.

### 8. Multiple Frontend Support

Same backend, different UIs for different contexts.

**Current frontends:**
- Flutter (primary): Desktop and mobile
- TUI: Terminal-based for keyboard-heavy workflows

**Planned:**
- WASM/web frontend
- Pure Rust UI (when ecosystem matures)

**Status**: Flutter and TUI frontends exist. Backend exposes minimal FFI surface.

### 9. AI Integration

AI assistance for knowledge work, implemented as another client of the system.

**Envisioned capabilities:**
- AI writes PRQL queries to retrieve relevant data
- AI performs operations based on query results
- Automatic task suggestions
- Intelligent summarization and linking

**Status**: Not implemented. Architecture supports it (AI as operation dispatcher client).

### 10. Durable Storage Format

Data stored in formats that won't break with dependency changes.

**What this means:**
- Plain text where possible (Markdown, Org Mode)
- Neutral binary formats (Parquet) for structured data
- CRDTs export to human-readable formats
- No vendor lock-in to specific database format

**Status**: SQLite (Turso) storage. Plain text export planned.

### 11. Supported Workflows

The system should naturally support:

| Workflow | Requirements |
|----------|--------------|
| **PARA** (Projects, Areas, Resources, Archive) | Hierarchical organization, tagging, archival |
| **GTD** (Getting Things Done) | Contexts, next actions, someday/maybe, reviews |
| **Zettelkasten** | Backlinks, atomic notes, emergent structure |

**Status**: Basic hierarchy and tagging. Workflow-specific features planned.

## Planned External Systems

Priority order based on personal use:

1. **Todoist** - Task management (implemented)
2. **Gmail** - Email integration
3. **Google Calendar** - Event management
4. **JIRA** - Work issue tracking
5. **Linear** - Modern issue tracking
6. **GitHub** - Code and issues
7. **Notion** - Documents and databases (import)
8. **Obsidian** - Markdown vault (import/export)

## Non-Goals

Things explicitly out of scope:

- **Real-time collaboration**: Focus on personal/small team use. CRDTs enable multi-device, not Google Docs-style collaboration.
- **Sync server hosting**: P2P sync means no server to run. External systems handle their own sync.
- **Feature parity with each external system**: We expose useful operations, not every UI feature.
- **Beginner-friendly**: This is a power user tool. Complexity is acceptable for capability.

## Success Criteria

**Phase 1 (Current)**: Daily-drivable Todoist integration
- Can use as primary Todoist client
- Outliner view with full task operations
- Offline mode works reliably

**Phase 2**: Multi-system integration
- Two or more external systems integrated
- Queries span multiple systems
- Cross-system linking works

**Phase 3**: Full vision
- All priority external systems integrated
- P2P sync between devices
- AI assistance functional
- Plugin ecosystem (WASM)

## Related Documents

- `ARCHITECTURE_PRINCIPLES.md` - How the system is built
- `VISION.md` - Detailed roadmap and trade-offs
- `docs/architecture.md` - Technical architecture details
