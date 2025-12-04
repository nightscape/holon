# Holon: Technical Vision & Roadmap

## Overview

Holon is an offline-first, high-performance knowledge management system that treats third-party services (Todoist, JIRA, Linear, Gmail, calendars) as first-class citizens within a unified outliner interface. Think LogSeq meets Notion, but with deep integrations, local data ownership, and a focus on enabling **flow states** through **trust**.

> For the philosophical foundation and product vision, see [VISION_LONG_TERM.md](VISION_LONG_TERM.md).

## Core Purpose: Trust Enables Flow

The ultimate goal is not productivity metrics—it's helping users achieve **flow states**. Flow requires trust:

1. **Trust that nothing is forgotten** → The Watcher monitors everything
2. **Trust that you're working on the right thing** → Intelligent prioritization
3. **Trust that context is accessible** → Cross-system unified view

Every technical decision serves this purpose.

## The Three Modes

Holon's UX is organized around three modes that match how humans actually work:

| Mode | Purpose | Key Technical Requirements |
|------|---------|---------------------------|
| **Capture** | Quick input, get it out of my head | Fast block creation, keyboard-driven input, inbox for unprocessed items |
| **Orient** | Big picture, daily/weekly reviews | Watcher Dashboard, cross-system synthesis, nothing-forgotten guarantee |
| **Flow** | Deep focus on present task | Context Bundles, distraction hiding, single-task view, relevant context surfaced |

**Note on Capture**: Holon focuses on in-app capture (quick block creation while working). For mobile capture, voice notes, and email forwarding, use integrated tools like Todoist—Holon doesn't need to be best at everything, just best at integrating everything.

## Core Technical Vision

### 1. Third-Party Systems as First-Class Citizens

You can embed blocks from external systems anywhere in the graph. For a "Build Carport" project, see:
- All outliner blocks with your custom notes
- All Todoist tasks from the corresponding project
- All Google Drive files from the project folder
- All calendar entries with the project tag
- All JIRA issues linked to the project

Everything in one unified view. Perform actions directly in Holon that you'd normally do in the external system (mark done, update priority, change status).

For operations with identical semantics across systems (task status, priority), use unified shortcuts and menus regardless of the underlying system.

#### 1.0 Unified Item Types

Define item types with unified representations across third-party systems. A "task" type represents Todoist tasks, JIRA issues, Linear issues, etc. with:
- **Common properties**: title, description, status, priority, due date
- **System-specific extensions**: JIRA sprints/story points, Todoist sections, Linear cycles

Implementation uses Rust traits for common interfaces and extension structs for system-specific features. Macros generate serialization/deserialization boilerplate.

#### 1.1 Context Bundles

When working on a project or task, Holon automatically assembles a **Context Bundle**:
- All tasks related to the context (from any system)
- All calendar events for the context
- All communications about it
- All notes about it

Not as separate panels, but as a **unified view** where the source system is just metadata. This enables Flow mode.

#### 1.2 Project-Based Organization (P.A.R.A.)

Holon uses a **P.A.R.A.-inspired** approach to organize content by actionability:

- **Projects**: Active work with defined outcomes
- **Areas**: Ongoing responsibilities
- **Resources**: Reference material
- **Archives**: Inactive items

**Default Mapping Strategy**: Identically-named projects across systems are automatically linked. A Holon project "Website Redesign" automatically aggregates:
- Todoist project "Website Redesign"
- Gmail label "Website Redesign"
- Google Drive folder "Website Redesign"
- JIRA project (if name matches)

**Custom Overrides**: Embed a different query in any Holon project page to customize which external items appear. This enables both convention-over-configuration simplicity and full flexibility when needed.

#### 1.3 Embedding

Embed third-party items anywhere:
- Todoist task as a block inside another block
- JIRA issue embedded in a project block
- Calendar event in a meeting notes block

#### 1.4 Bi-Directional Sync

Changes in external systems reflect in Holon, and vice versa. Update a Todoist task in either place, see changes everywhere.

#### 1.5 Unified Search

Search across all systems from Holon's search bar. Find tasks in Todoist, issues in JIRA, emails in Gmail, all from one interface.

### 2. Custom Visualizations

Different item types get appropriate visualizations:
- **Tasks**: Round bullet showing status icon
- **Tables**: Spreadsheet-like views
- **Kanban boards**: Drag-and-drop task management
- **Inline content**: Images, rich text, embeds

**HTML Embedding**: Full HTML embed support is desirable but has cross-platform challenges in Flutter. Options under consideration:
- `flutter_inappwebview` for full HTML (heavier, platform-dependent)
- Markdown with custom renderers (lighter, more portable)
- Native Flutter widgets generated from structured data (best performance)

Current approach: Prioritize structured data rendering; HTML embedding as optional enhancement for desktop.

Customization via declarative descriptions (where reasonable) to avoid abstraction leakage.

### 3. Strong Automation

Define rules for automatic actions:
- Create Todoist tasks when certain blocks are created
- Update JIRA status when corresponding Holon block marked done
- Auto-tag blocks based on content analysis

**PRQL-Based Automation**: Automation rules follow the same pattern as UI rendering—specify a PRQL query on data, then declare what should happen with matched items:

```prql
from holon_blocks
filter has_tag("work") && type == "task" && !has_external_link("todoist")
action create_todoist_task(
  project: "Work Inbox",
  content: this.title
)
```

This unifies the mental model: queries select data, then either `render` (for UI) or `action` (for automation).

### 4. Offline-First with Strong Sync

Work on notes and tasks across devices without internet connection. Everything syncs automatically when back online.

**Architecture**:
- **CRDTs (Loro)** for owned data (blocks, links, properties)
- **Local cache + operation queue** for third-party systems
- **Conflict resolution** when offline edits clash with remote changes

**Plain-Text File Layer**: Local files (Markdown or Org Mode) provide an additional interface:
- Files act as a bidirectional cache of CRDT content
- External edits to files are detected and merged back into CRDTs
- Enables interop with other tools (Vim, Emacs, VS Code)
- Provides human-readable backup and portability

The exact reconciliation strategy between file edits and CRDT state is TBD, but the goal is: you can always edit your notes in any text editor, and Holon will incorporate those changes.


### 5. Sharing

Share parts of your knowledge graph with others:
- Read-only sharing for documentation
- Collaborative editing for team projects

### 6. AI Integration

Three AI services support the user experience:

| Service | Role | Supports |
|---------|------|----------|
| **The Watcher** | Monitors all systems, surfaces what you're missing | Orient mode, Trust |
| **The Integrator** | Connects related items, surfaces context | Flow mode, Context Bundles |
| **The Guide** | Tracks patterns, surfaces growth opportunities | Long-term development |

See [VISION_AI.md](VISION_AI.md) for detailed AI architecture.

### 7. Flexible Customization

- **UI themes**: Easy customization
- **Logic extensions**: Choose best abstraction (scripting languages, Rust plugins)
- **Strong typing**: Rust structs for item types to minimize runtime errors
- **Plugin sandboxing**: WASM for security

### 8. Cross-Platform

**Target platforms**: Windows, macOS, Linux, iOS, Android.

**Current frontends**:
- **Flutter**: Primary frontend for desktop and mobile
- **TUI**: Terminal interface for keyboard-driven workflows

**Future**: Tauri desktop wrapper is on hold; Flutter desktop is the current focus.

### 9. Performance & Scalability

Handle large knowledge graphs (100k+ blocks) with many third-party integrations without slowdowns.

**Strategies**:
- Virtual scrolling and lazy loading
- Full-text search indexing (Tantivy)
- Selective loading (only visible + recently accessed)
- Multi-layer caching (memory → Turso embedded → API)

### 10. Modularity

Each component does one thing well. Makes maintenance and extension easier.

---

## Critical Architectural Challenges

### Challenge 1: Offline-First + Third-Party Sync

**The Paradox**: CRDTs work for multi-master data (your outline), but third-party APIs are server-authoritative. You can't "merge" with JIRA; you must send requests that can be rejected.

**Solution: Hybrid Architecture**

```
┌─────────────────────────────────────────────────────────┐
│                 UNIFIED VIEW LAYER (UI)                 │
│            Merged view + sync indicators                │
└────────────────────────┬────────────────────────────────┘
                         │
                         │ PRQL/SQL queries
                         │
┌────────────────────────▼────────────────────────────────┐
│              UNIFIED TURSO CACHE                        │
│         (SQLite-compatible, single query surface)       │
│                                                         │
│    All data—owned and third-party—queryable here        │
│    All modifications via unified Operation system       │
└─────────────┬─────────────────────────┬─────────────────┘
              │                         │
              │ syncs from              │ syncs from
              │                         │
      ┌───────▼───────┐         ┌───────▼───────────┐
      │  LORO CRDT    │         │  THIRD-PARTY      │
      │               │         │  APIs             │
      │  Source of    │         │                   │
      │  truth for    │         │  Source of truth  │
      │  owned data   │         │  for external     │
      └───────────────┘         └───────────────────┘
```

**Key insight**: Both owned data (from Loro CRDT) and third-party data (from external APIs) flow into the **same Turso cache**. The UI queries this single unified cache. The difference is only in where the source of truth lives and how sync works.

**Key Principles**:
1. Never store third-party data in Loro (it's a cached view)
2. Queue offline changes as operations to replay
3. Reconciliation engine handles conflicts
4. Clear UI indicators (synced ✓, pending ⏳, conflict ⚠️)

### Challenge 2: API Rate Limits & Costs

With 1000s of third-party items, naive polling exceeds rate limits.

**Solution: Intelligent Sync**
- Webhook-first (JIRA, Linear, Google APIs)
- Smart caching (ETags, conditional requests)
- Batch operations (Gmail batch API, JIRA bulk)
- Selective sync (active items real-time, others background)
- Cost monitoring (track usage, alert on limits)

### Challenge 3: Type Unification vs. System Diversity

JIRA has sprints, Todoist has sections, Linear has cycles. How to unify without losing features?

**Solution: Trait-Based Protocol + Extensions**

```rust
pub trait Task {
    fn id(&self) -> TaskId;
    fn title(&self) -> &str;
    fn status(&self) -> TaskStatus;
    fn priority(&self) -> Option<Priority>;
    fn extensions(&self) -> &dyn Any;  // System-specific
}

struct JiraExtensions {
    story_points: Option<f32>,
    sprint: Option<SprintId>,
}

#[derive(UnifiedTask)]
struct JiraTask {
    #[common] id: TaskId,
    #[common] title: String,
    #[extension] jira: JiraExtensions,
}
```

Common operations work uniformly; system-specific features accessible when needed.

---

## Phased Implementation Roadmap

### Phase 1: Core Outliner ← START HERE

**Goal**: Usable as LogSeq alternative

**Deliverables**:
- Loro-based outliner with blocks, links, backlinks
- Local-only (no third-party integrations yet)
- Basic task visualization (checkbox bullets with status icons)
- Full-text search (Tantivy)
- Flutter desktop + mobile
- Cross-device sync via Loro CRDT

**Validates**:
- Loro performance at scale
- Core UX and outliner interactions
- CRDT sync reliability

### Phase 2: First Integration (Todoist)

**Goal**: Prove hybrid architecture

**Deliverables**:
- Todoist integration only (simplest API)
- Operation log + local cache
- Reconciliation engine
- Conflict resolution UI
- Offline queue with retry logic
- OAuth credential management (OS keychain)

**Validates**:
- Hybrid sync architecture works
- Conflict resolution UX is acceptable
- API rate limits are manageable

### Phase 3: Multiple Integrations

**Goal**: Validate type unification scales

**Deliverables**:
- Add JIRA + Linear integrations
- Implement trait system for tasks
- Common task interface in UI
- System-specific extension panels
- Macro-generated boilerplate

**Validates**:
- Type abstraction approach scales
- UI can handle multiple systems elegantly
- Users can access system-specific features

### Phase 4: AI Foundation

**Goal**: Infrastructure for AI features

**Deliverables**:
- Local embeddings (sentence-transformers)
- Semantic search
- Entity linking (manual → automatic)
- Pattern logging for future learning
- Conflict logging for training data

**Validates**:
- Local AI performance acceptable
- Embedding quality sufficient for linking

### Phase 5: AI Features

**Goal**: Three AI services operational

**Deliverables**:
- The Watcher (monitoring, alerts, synthesis)
- The Integrator (context surfacing, linking)
- The Guide (patterns, insights)
- AI Trust Ladder progression

**Validates**:
- AI provides daily value
- Users trust AI suggestions

### Phase 6: Flow Optimization

**Goal**: Users achieve flow states regularly

**Deliverables**:
- Focus mode with Context Bundles
- Orient Dashboard (Watcher synthesis)
- Review workflows (daily, weekly)
- Shadow Work: obstacle identification for stuck tasks

**Validates**:
- Flow metrics improve
- Trust metrics improve
- Users report reduced anxiety

### Phase 7: Team Features

**Goal**: Teams leverage individual excellence

**Deliverables**:
- Shared views
- Collaborative editing
- Team dashboards

**Validates**:
- Individual features scale to teams
- Collaboration doesn't break personal trust

---

## Trade-Offs to Accept

1. **Eventually Consistent**: Third-party sync is NOT real-time CRDT. Expect 5-30 second delays.
2. **API Limits**: Power users may hit rate limits. Document this; offer throttling controls.
3. **Platform Differences**: Some features won't work on mobile (complex plugins). That's OK.
4. **Complexity**: This is an advanced tool. Not targeting complete beginners.
5. **Flow over Features**: We prioritize flow-enabling features over feature count.

---

## Success Criteria

### Trust Metrics
- Users check other apps less frequently (measured via survey)
- Review completion rate increases
- Inbox zero frequency increases

### Flow Metrics
- Time spent in focus mode
- Frequency of context switches (should decrease)
- User-reported flow states (sampling)

### Phase-Specific
- **Phase 1**: Daily-drivable outliner, better performance than LogSeq, reliable sync
- **Phase 2**: Todoist integration feels native, offline mode works reliably
- **Phase 3**: Working across JIRA/Todoist/Linear feels unified
- **Full Vision**: All features implemented, active users, thriving plugin ecosystem

---

## Related Documents

- [VISION_LONG_TERM.md](VISION_LONG_TERM.md) - Philosophical foundation and product vision
- [VISION_AI.md](VISION_AI.md) - AI feature specifications and development path
- [ARCHITECTURE_PRINCIPLES.md](ARCHITECTURE_PRINCIPLES.md) - Foundational architectural decisions
- `docs/adr/0001-hybrid-sync-architecture.md` - Architectural decision record for sync strategy
