# Holon: AI Integration Vision

## Overview

This document outlines AI integration for Holon. Unlike traditional productivity tools where AI operates on siloed data, Holon's hybrid CRDT + third-party shadow architecture enables AI to reason across multiple systems simultaneously.

AI in Holon isn't just an optimizer—it's an **externalized part of your awareness** that sees what you can't see because you're focused elsewhere. The goal is enabling **trust** and **flow states**, not maximizing productivity metrics.

## Core Architectural Advantages for AI

1. **Unified Local Data**: All third-party data (Todoist, JIRA, Linear, Gmail, Calendar) aggregated locally
2. **Offline-First**: AI can operate on complete dataset without API latency or rate limits
3. **Strong Typing**: Rust trait system provides semantic understanding across different systems
4. **Operation Queue**: Rich context about user intent and workflow patterns
5. **CRDT + Shadow Layer**: Inevitable conflicts create opportunities for intelligent resolution

---

## The Three AI Roles

AI in Holon is organized around three complementary roles that map to how humans need support:

### The Watcher (Awareness)

**Purpose**: See what the user can't see because they're focused elsewhere.

**Answers**: "What am I not seeing?"

**Capabilities**:
- Continuously monitors all systems for changes
- Synthesizes daily/weekly summaries (Orient mode)
- Detects when reality diverges from intention
- Alerts on risks, deadlines, dependencies
- Creates the "nothing forgotten" feeling

**Key Features**:
- Cross-system monitoring and alerts
- Daily orientation synthesis
- Weekly review generation
- Risk and deadline tracking
- Dependency chain analysis

### The Integrator (Wholeness)

**Purpose**: Connect related items and surface relevant context.

**Answers**: "What else matters for this?"

**Capabilities**:
- Links related items across systems automatically
- Surfaces relevant context when working on a task (Flow mode)
- Powers unified search across all systems
- Creates Context Bundles for focus sessions
- Maintains the "unified field" view

**Key Features**:
- Automatic entity linking via embeddings
- Context Bundle assembly
- Semantic search across all systems
- Related item discovery
- Cross-system deduplication

### The Guide (Growth)

**Purpose**: Track patterns over time and surface growth opportunities.

**Answers**: "What am I avoiding?" / "Where am I stuck?"

**Capabilities**:
- Tracks behavioral patterns over time
- Notices where user is stuck or avoiding tasks
- Gently surfaces uncomfortable truths (Shadow Work)
- Provides insights about work habits
- Supports long-term development

**Key Features**:
- Pattern recognition across time
- Stuck task identification
- Shadow Work prompts (see below)
- Velocity and capacity analysis
- Growth tracking

---

## Mapping to Integral Theory

The three AI roles support Ken Wilber's Five Paths:

| Integral Path | What It Means | AI Role | Example |
|---------------|---------------|---------|---------|
| **Waking Up** | Present-moment awareness | Watcher | "You've been in reactive mode for 3 hours. Pause and review priorities?" |
| **Growing Up** | Expanding perspective | Guide | "You consistently underestimate tasks involving X. Adjust estimates?" |
| **Opening Up** | Multiple intelligences | Integrator | Show same project from different stakeholder viewpoints |
| **Cleaning Up** | Integrating shadow | Guide | "You've postponed this 7 times. What's blocking you?" |
| **Showing Up** | Embodying in action | Watcher | "You committed to X. Here's your progress." |

---

## Shadow Work: Facing What We Avoid

Not motivation-trainer platitudes. Practical help overcoming obstacles.

When The Guide detects a stuck task:

```
┌─────────────────────────────────────────────────────────────────┐
│  📋 Task: "Write performance review for Alex"                   │
│  ⚠️  Postponed 7 times over 3 weeks                             │
│                                                                 │
│  This task seems stuck. What's blocking you?                    │
│                                                                 │
│  [ ] It's too big → Let's break it down together                │
│  [ ] It's unclear → Let's clarify what "done" looks like        │
│  [ ] It's uncomfortable → Let's make it less daunting           │
│  [ ] Wrong time → Reschedule to a better slot                   │
│  [ ] Shouldn't be mine → Delegate or decline                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Implementation**:
1. Track postponement count and patterns
2. Detect stuck tasks (>3 postponements, aging without progress)
3. Offer structured options, not open-ended questions
4. Each option leads to concrete action
5. Learn which interventions work for this user

---

## The AI Trust Ladder

AI earns autonomy through demonstrated competence. Users progress through levels:

| Level | Name | Behavior | How Trust Is Earned |
|-------|------|----------|---------------------|
| 1 | **Passive** | Answers when asked | Starting point for all users |
| 2 | **Advisory** | Suggests, user decides | Suggestions accepted >80% |
| 3 | **Agentic** | Takes actions with permission | Low correction rate over time |
| 4 | **Autonomous** | Acts within defined bounds | Extended track record, user opt-in |

**Key Principles**:
- Never assume trust—earn it
- Show reasoning for every suggestion
- Easy undo for any AI action
- Learn from corrections
- User can demote AI at any time

**Per-Feature Trust**:
Trust levels are tracked per feature, not globally. The Watcher might be at Level 3 (proven reliable) while The Guide is still at Level 1 (user hasn't engaged much).

---

## Feature Tiers by Competitive Moat

### Tier 1: Core Architectural Differentiators ⭐⭐⭐

These features are **nearly impossible to replicate** without Holon's unified data layer.

#### 1.1 Cross-System Intelligence (Watcher + Integrator)

**Why Uniquely Powerful**:
- Existing tools are siloed (Linear AI only sees Linear, Notion AI only sees Notion)
- Holon has ALL user data locally—AI sees complete work context
- Can perform analysis that would require N integrations + complex aggregation elsewhere

**Concrete Use Cases**:

**Capacity Analysis (Watcher)**:
```
User: "Am I overcommitted this week?"

AI analyzes:
- JIRA sprint commitments (40 story points)
- Todoist personal tasks (15 items)
- Calendar availability (only 20 hours free)
- Email threads requiring responses (8 urgent)

Response: "Yes - you have 40 SP committed but only 20 hours available.
Consider moving PROJ-456 to next sprint (low priority, no dependencies)."
```

**Root Cause Analysis (Guide)**:
```
User: "Why is my JIRA velocity dropping?"

AI correlates:
- JIRA velocity: 30 SP → 20 SP over 3 sprints
- Todoist: 40% increase in personal tasks (tagged #house-move)
- Calendar: 8 new recurring meetings added
- Gmail: 200% increase in support emails

Response: "Velocity drop correlates with house move (Todoist) and
new customer onboarding (meetings + emails). This is temporary."
```

**Smart Linking (Integrator)**:
```
AI suggestion: "JIRA-789 'API authentication' relates to:
- Todoist task 'Update API docs'
- Calendar event 'Security review 3/15'
- Gmail thread 'Auth bug reports'
- Linear issue 'Mobile auth flow'
Want to link these?"
```

#### 1.2 Intelligent Conflict Reconciliation (Watcher)

**Why Uniquely Powerful**:
- Most tools use crude "last write wins" or force manual resolution
- Holon's hybrid architecture makes conflicts inevitable and frequent
- AI can use context from entire graph to make semantically correct decisions

**Smart Resolution**:
```
Conflict: Offline you marked JIRA-123 "Done"
         Online someone added comment: "Blocked by security review"

Traditional: "Which version do you want?" (forces user choice)

AI Resolution:
1. Analyzes comment content (understands "blocked")
2. Checks calendar (security review scheduled next week)
3. Suggests: "Keep 'In Progress', create follow-up task 'Address security feedback'"
4. Shows reasoning: "Task isn't actually done - blocker identified"
```

### Tier 2: High-Potential Features with Unique Edge ⭐⭐

#### 2.1 Context-Aware Task Decomposition (Integrator + Guide)

**Unique Advantage**: Holon can **automatically route subtasks to appropriate systems** based on content + learned patterns.

```
User: "Build new authentication system"

AI creates:
→ JIRA Epic: "Authentication System" with subtasks:
  - JIRA-890: Implement OAuth 2.0 flow
  - JIRA-891: Add JWT token validation

→ Todoist Project: "Auth Documentation" with tasks:
  - Write API authentication guide
  - Update developer onboarding

→ Calendar events:
  - "Security design review" (linked to JIRA-890)

All items automatically linked in Holon graph
```

#### 2.2 Smart Cross-System Notifications (Watcher)

**Unique Advantage**: Calendar app can't warn that JIRA ticket for next meeting is blocked.

```
"Meeting in 30 min: 'API Design Review'
⚠️  Linked ticket JIRA-456 is blocked by PROJ-123
📄 Blocker has PR ready for review (GitHub PR #789)"

"High priority: JIRA-890 due tomorrow
⚠️  No calendar time available today
💡 Suggestion: Reschedule 'Team sync' (low priority) or extend deadline"
```

#### 2.3 AI-Powered Local Search (Integrator)

**Unique Advantages**:
- **Speed**: Data is local, search is instantaneous, works offline
- **Scope**: Search across all systems simultaneously
- **Privacy**: Vector embeddings stored locally
- **Context**: Rank based on ALL user behavior

```
Query: "authentication bug"

Results (ranked by relevance across ALL systems):
1. JIRA-456: "OAuth authentication fails on mobile" (exact match)
2. Gmail thread: "User reports login issues" (semantic match)
3. Calendar: "Security review 3/15" (linked to JIRA-456)
4. Todoist: "Update auth documentation" (related task)
5. Holon block: Notes from "Auth postmortem meeting"
```

### Tier 3: Valuable but Less Differentiated ⭐

These features don't leverage Holon's architectural moat as strongly:

- **Predictive Task Scheduling**: Reclaim.ai, Motion already do this well
- **Natural Language Task Creation**: Many tools have this
- **Automated Time Tracking**: Not fundamentally different from competitors

Recommendation: De-prioritize until Tier 1/2 features are complete.

---

## Implementation Principles

### 1. Foundation First

Before building fancy features, establish infrastructure:

```rust
pub trait UnifiedItem {
    fn id(&self) -> ItemId;
    fn title(&self) -> &str;
    fn status(&self) -> ItemStatus;
    fn item_type(&self) -> ItemType;  // Task, Event, Email, Note
    fn links(&self) -> Vec<ItemId>;   // Cross-system relationships
    fn embeddings(&self) -> Option<&[f32]>;  // For semantic search
}
```

Required infrastructure:
- Unified Data Model (UDM) with embeddings
- Conflict logging (capture every conflict + resolution)
- Entity linking (manual first, then automatic)
- Pattern logging (for Guide to learn from)

### 2. Privacy-First AI

- **Prefer local models**: Embeddings, classification on-device
- **Explicit consent**: Cloud LLM features require opt-in
- **Data minimization**: Only send minimum context to cloud
- **Encryption**: End-to-end with user keys if cloud is used

### 3. Transparent & Controllable

- **Always show reasoning**: Why did AI suggest this?
- **Easy undo**: One click to revert AI decisions
- **Learn from corrections**: When user overrides AI, improve
- **Confidence scores**: Show how certain AI is

### 4. Progressive Enhancement

- **Start with rules**: Simple heuristics for common cases
- **Add ML incrementally**: Only when you have training data
- **Fallback gracefully**: If AI fails, degrade to simple behavior
- **Measure everything**: Track accuracy, satisfaction, performance

---

## Development Roadmap

### Phase 1: Foundation

**Goal**: Establish infrastructure for AI features

- [ ] Define and implement Unified Data Model (UDM)
- [ ] Build conflict logging system
- [ ] Implement local vector embeddings (sentence-transformers)
- [ ] Create entity linking UI (manual links)
- [ ] Set up local full-text search (Tantivy)
- [ ] Implement pattern logging for Guide

**Success Criteria**: Can query "show all items linked to X" across all systems

### Phase 2: The Integrator (Search & Discovery)

**Goal**: First user-facing AI feature

- [ ] Implement semantic search using local embeddings
- [ ] Add behavioral ranking (learn from clicked results)
- [ ] Build unified search UI
- [ ] Context Bundle assembly for Flow mode
- [ ] Automatic link inference

**Success Criteria**: Search across all systems in <100ms, >85% top-3 accuracy

### Phase 3: The Watcher (Monitoring & Reconciliation)

**Goal**: Prove AI can handle complex sync and monitoring

- [ ] Implement rule-based conflict resolver
- [ ] Build conflict resolution UI with reasoning display
- [ ] Train classifier on logged conflicts
- [ ] Add LLM-based resolution for low-confidence cases
- [ ] Daily/weekly synthesis for Orient mode
- [ ] Cross-system alerting

**Success Criteria**: >80% conflicts auto-resolved, <10% user corrections

### Phase 4: The Guide (Patterns & Growth)

**Goal**: Deliver unique insights and Shadow Work

- [ ] Build query templates for common analyses
- [ ] Implement Shadow Work prompts for stuck tasks
- [ ] Create insight generation pipeline
- [ ] Pattern detection across time
- [ ] Growth tracking and visualization

**Success Criteria**: Users report making workflow changes based on AI insights

### Phase 5: Trust Ladder Progression

**Goal**: AI earns autonomy

- [ ] Implement per-feature trust tracking
- [ ] Build UI for trust level visualization
- [ ] Enable Level 2 (Advisory) features
- [ ] Enable Level 3 (Agentic) with permission prompts
- [ ] Level 4 (Autonomous) for power users

**Success Criteria**: Power users have AI at Level 3+ for core features

---

## Technical Architecture

### AI Services Stack

```
┌─────────────────────────────────────────────────────────────────┐
│                    UI Layer (Flutter)                           │
│         Orient Dashboard, Flow Mode, Capture, Search            │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                   AI Services (Rust)                            │
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
│  │  Foundation: Embeddings (local) + LLM (hybrid/optional)  │  │
│  │  Trust Ladder: Per-feature autonomy tracking             │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                  Unified Data Layer                             │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Holon Store: CRDT + Shadow Cache + Links + Embeddings  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Pattern Logs: Conflicts, Behaviors, User Corrections   │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Model Selection

| Component | Model | Why |
|-----------|-------|-----|
| **Embeddings** | sentence-transformers/all-MiniLM-L6-v2 | Fast, runs locally, good quality |
| **Conflict Classification** | Lightweight classifier (trained on logs) | Low latency, works offline |
| **Insight Generation** | GPT-4 / Claude / Local LLM | Complex reasoning, can use cloud or self-hosted |
| **Task Decomposition** | GPT-4 / Claude | Needs strong reasoning |
| **Link Inference** | Hybrid: embeddings + rules | Fast, mostly deterministic |

### Privacy & Deployment Options

**Option 1: Fully Local (Maximum Privacy)**
- All AI on-device (GGUF models via llama.cpp)
- Zero cloud dependency
- Target: Privacy-conscious enterprise users

**Option 2: Hybrid (Recommended)**
- Local: Embeddings, search, classification
- Cloud: Complex reasoning (insights, decomposition)
- User controls what goes to cloud
- Target: Most users

**Option 3: Self-Hosted**
- User runs own LLM server (vLLM, Ollama)
- Holon connects to user's server
- Target: Technical users, teams

---

## Success Metrics

### Trust Metrics (Primary)
| Metric | Target |
|--------|--------|
| Users check other apps | Decreases over time |
| Review completion rate | >80% |
| "Nothing forgotten" feeling (survey) | >70% agree |

### Flow Metrics (Primary)
| Metric | Target |
|--------|--------|
| Time in Focus mode | Increases over time |
| Context switches per hour | Decreases |
| User-reported flow states | Increases |

### AI Feature Metrics
| Feature | Metric | Target |
|---------|--------|--------|
| Search | Top-3 accuracy | >90% |
| Conflict Resolution | Auto-resolve rate | >80% |
| Conflict Resolution | User correction rate | <10% |
| Cross-System Insights | Actionable insights/week | >5 |
| Shadow Work | Stuck tasks resolved | >50% engagement |
| Trust Ladder | Users at Level 2+ | >60% |

### Product Metrics
- **Daily Active Usage**: AI features used in >50% of sessions
- **Time Saved**: Users report >30 min/week saved
- **Competitive Moat**: Users cite AI as reason they can't switch

---

## Risks & Mitigations

### Risk 1: AI Accuracy Too Low
**Impact**: Users lose trust, stop using features
**Mitigation**:
- Start conservative (only high-confidence suggestions)
- Always show reasoning
- Learn from corrections
- Trust Ladder prevents over-automation

### Risk 2: Privacy Concerns
**Impact**: Users refuse to enable AI
**Mitigation**:
- Local-first by default
- Explicit opt-in for cloud
- Clear documentation of data flow
- Self-hosted option

### Risk 3: Performance Impact
**Impact**: AI slows down app
**Mitigation**:
- All AI operations async
- Background processing
- Local models optimized for speed
- Lazy loading

### Risk 4: Shadow Work Feels Intrusive
**Impact**: Users feel judged, disable Guide
**Mitigation**:
- Gentle, non-judgmental framing
- User controls frequency
- Focus on practical help, not motivation
- Easy to dismiss or disable

---

## Related Documents

- [VISION_LONG_TERM.md](VISION_LONG_TERM.md) - Philosophical foundation
- [VISION.md](VISION.md) - Technical vision and roadmap
- [ARCHITECTURE_PRINCIPLES.md](ARCHITECTURE_PRINCIPLES.md) - Foundational decisions
