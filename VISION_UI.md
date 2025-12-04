# Holon: UI/UX Vision

## Core Experience Goal

Holon should feel like a **trusted companion** that helps you achieve flow states. The UI serves trust and focus—never competing for attention, always ready when needed.

**The feeling we're after**: Opening Holon should feel like sitting down at a well-organized desk where everything is in its place. Not sterile or cold, but warm, alive, and ready. The app breathes with you—responsive without being hyperactive, present without being needy.

---

## Design Philosophy

### 1. Calm Technology

Holon follows the principles of **calm technology**:
- Information is available at a glance, not demanding attention
- The periphery is used wisely—important status visible, not intrusive
- Transitions between modes feel natural, not jarring
- The app recedes when you're in flow, surfaces when you need orientation

### 2. Progressive Disclosure & Concealment

Complexity reveals itself only when needed—and hides when not:

**Disclosure** (revealing on demand):
- Start with essentials, reveal depth on interaction
- Power user features don't clutter the beginner experience
- Context-appropriate UI—Flow mode is minimal, Orient mode is comprehensive

**Concealment** (automatic focus assistance):
- When you focus on a block (sustained typing/editing), surrounding content gradually fades
- Peripheral elements dim over ~2-3 seconds of focused activity
- Moving cursor/keyboard focus outside the block restores full visibility
- Creates natural "soft flow" without explicit mode switching
- User never has to think about it—the UI responds to their attention
### 3. Trust Through Transparency

Every UI element should build trust:
- Sync status always visible but not alarming
- AI suggestions show their reasoning
- Nothing happens without the user understanding why
- Undo is always available, always obvious

---

## The Three Modes: Visual Language

### Capture Mode

**Feel**: Quick, effortless, gets out of your way

**Visual Characteristics**:
- **Minimal chrome**: Just an input field and essential context
- **Fast transitions**: Appears instantly, dismisses instantly
- **Muted colors**: No distractions, focus on the text you're typing
- **Keyboard-first**: Visual feedback for keyboard shortcuts
- **Subtle depth**: Light shadow to float above current context

**Key Interactions**:
- Global hotkey summons capture overlay (think Spotlight/Alfred)
- Type immediately—no clicking required
- Auto-complete for tags, projects, links
- Quick dismiss with Escape or click-outside
- Visual confirmation when captured (brief, satisfying)

**Animation**:
- Fade-in: 100ms ease-out
- Fade-out: 80ms ease-in (faster than appear—feels snappy)
- Input field has subtle focus glow

```
┌─────────────────────────────────────────────────────┐
│  ╭─────────────────────────────────────────────╮    │
│  │ ▸ Meeting notes from call with Sarah...    │    │
│  │   #work @project:website                   │    │
│  ╰─────────────────────────────────────────────╯    │
│                                    ↵ Enter to save  │
└─────────────────────────────────────────────────────┘
```

### Orient Mode

**Feel**: Grounded, comprehensive, "I see everything"

**Visual Characteristics**:
- **Structured layout**: Clear sections for different concerns
- **Information density**: More content visible, well-organized
- **Status colors**: Meaningful use of color for sync status, priorities, deadlines
- **Subtle data visualization**: Progress indicators, capacity hints
- **Warm neutrals with accent colors**: Professional but not cold

**Key Sections**:
1. **Today's Focus**: What matters right now
2. **Inbox**: Unprocessed items awaiting triage
3. **Upcoming**: Deadlines, calendar, commitments
4. **The Watcher**: AI-synthesized insights and alerts

**Color Usage**:
- Background: Warm off-white or soft dark (theme-dependent)
- Section headers: Subtle, not competing for attention
- Status indicators:
  - ✓ Synced: Muted green (not neon, think sage)
  - ⏳ Pending: Warm amber
  - ⚠️ Attention needed: Soft coral (not aggressive red)
  - ❌ Error: Muted red with clear action path

**Animation**:
- Sections load with staggered fade-in (50ms delay between sections)
- Numbers/counts animate when they change (countUp effect)
- Smooth reordering when priorities shift
- Subtle pulse on items that need attention (very subtle—once, not repeating)

**Row Hover Effects**:
- Text color becomes slightly more pronounced on hover
- Action icons (pie menu trigger) fade from dim to visible
- Subtle background tint appears (2-3% opacity shift)
- Creates clear affordance: "this row is interactive"
- Transition: 100ms ease-out (fast enough to feel responsive)
```
┌─────────────────────────────────────────────────────────────────┐
│  ☀️ Wednesday, December 4                                        │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  TODAY'S FOCUS                                          │   │
│  │  ─────────────────────────────────────────────────────  │   │
│  │  ▸ Complete API authentication (JIRA-456)         ●●●○  │   │
│  │  ▸ Review PR from Sarah                           ●●○○  │   │
│  │  ▸ Prepare slides for Friday                      ●○○○  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌──────────────────────────┐  ┌────────────────────────────┐  │
│  │  INBOX (3)               │  │  WATCHER INSIGHTS          │  │
│  │  ──────────────────────  │  │  ────────────────────────  │  │
│  │  ○ Email from client...  │  │  💡 JIRA-789 is blocked    │  │
│  │  ○ Idea: refactor auth   │  │  📊 Velocity up 15%        │  │
│  │  ○ Book recommendation   │  │  ⏰ 2 items due tomorrow   │  │
│  └──────────────────────────┘  └────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Flow Mode

**Feel**: Deep focus, nothing else exists, the task is everything

**Visual Characteristics**:
- **Maximum content, minimum chrome**: Almost full-screen for the task
- **Monochromatic or very limited palette**: Reduce visual noise
- **Typography-focused**: Beautiful, readable text
- **Context available on demand**: Related items one gesture away, hidden by default
- **Progress visibility**: Subtle indicator of focus session duration

**Key Principles**:
- No notifications unless truly urgent (user-defined)
- No visible task count or inbox—that's Orient mode's job
- Timer/focus indicator is peripheral, not central
- Related context slides in from edge when requested, slides out when done

**Animation**:
- Transition into Flow: Gentle zoom + fade, other elements recede (progressive concealment)
- Context panel: Slides from right edge, 200ms ease-out
- Typing: No animation—instant response, no lag
- Exit Flow: Elements fade back, gentle return to Orient

**Text Recency Effect** (optional, user-enabled):
- Recently typed text is slightly more pronounced than older text
- Text gradually fades to secondary color over ~30 seconds of inactivity
- Keeps visual weight on current work vs accumulated content
- Disabled by default—some users may find it distracting
- When disabled, all text has uniform weight

```
┌─────────────────────────────────────────────────────────────────┐
│                                                          ◷ 47m │
│                                                                 │
│                                                                 │
│     API Authentication Implementation                           │
│     ═══════════════════════════════════                         │
│                                                                 │
│     The OAuth 2.0 flow needs to handle three scenarios:        │
│                                                                 │
│     1. Fresh login - redirect to provider                       │
│     2. Token refresh - background refresh before expiry         │
│     3. Token revocation - handle gracefully, re-auth            │
│                                                                 │
│     ▸ Current focus: implementing refresh flow                  │
│       └─ Need to check: how does Todoist handle this?          │
│                                                                 │
│                                                                 │
│                                                                 │
│                                                     [Context →] │
└─────────────────────────────────────────────────────────────────┘
```

---

## Visual Design System

### Color Palette

**Philosophy**: Warm, professional, alive. Not clinical, not childish.

#### Light Theme

| Role | Color | Usage |
|------|-------|-------|
| Background | Warm white (#FAFAF8) | Main canvas |
| Surface | Soft cream (#F5F4F0) | Cards, panels |
| Text Primary | Warm charcoal (#2D2D2A) | Body text |
| Text Secondary | Warm gray (#6B6B65) | Labels, hints |
| Accent Primary | Deep teal (#2A7D7D) | Interactive elements, links |
| Accent Secondary | Warm coral (#E07A5F) | Highlights, attention |
| Success | Sage green (#7D9D7D) | Completed, synced |
| Warning | Warm amber (#D4A373) | Pending, caution |
| Error | Muted rose (#C97064) | Errors, blockers |

#### Dark Theme

| Role | Color | Usage |
|------|-------|-------|
| Background | Warm dark (#1A1A18) | Main canvas |
| Surface | Soft charcoal (#252522) | Cards, panels |
| Text Primary | Warm off-white (#E8E6E1) | Body text |
| Text Secondary | Warm gray (#9D9D95) | Labels, hints |
| Accent Primary | Light teal (#5DBDBD) | Interactive elements |
| Accent Secondary | Soft coral (#E8A090) | Highlights |

### Typography

**Philosophy**: Readable, professional, with personality in the details.

| Element | Font | Weight | Size |
|---------|------|--------|------|
| Headings | Inter or equivalent | 600 | 18-24px |
| Body | Inter or equivalent | 400 | 15-16px |
| Monospace | JetBrains Mono or equivalent | 400 | 14px |
| UI Labels | Inter | 500 | 13px |

**Line height**: 1.5-1.6 for body text (generous, readable)

**Special touches**:
- Block references use slightly different weight or subtle background
- Code blocks have gentle background tint
- Links underline on hover, not by default

### Spacing & Layout

**Base unit**: 4px grid

| Element | Spacing |
|---------|---------|
| Component padding | 12-16px |
| Section gaps | 24px |
| Card margins | 16px |
| Inline element gaps | 8px |

**Responsive behavior**:
- Desktop: Multi-column layouts in Orient
- Tablet: Adaptive columns
- Mobile: Single column, larger touch targets

### Iconography

**Style**: Outline icons, 1.5px stroke, rounded caps

**Philosophy**:
- Icons support text, don't replace it for critical actions
- Consistent visual weight across icon set
- Subtle, not cartoonish
- Custom icons for Holon-specific concepts (Watcher, Flow, etc.)

---

## Making the App Feel Alive

### Micro-interactions

Small moments that add life without demanding attention:

| Interaction | Effect |
|-------------|--------|
| Checkbox complete | Satisfying checkmark draw animation (150ms) |
| Task reorder | Smooth spring physics, items settle naturally |
| Sync complete | Brief pulse on sync indicator |
| New item appears | Gentle fade + slide from origin |
| Delete item | Fade + slight shrink, space closes smoothly |
| Focus session start | Gentle zoom, periphery dims |
| Achievement unlocked | Subtle confetti burst (very brief, 800ms) |

### Breathing UI

Elements that subtly indicate the app is alive and working:

- **Sync indicator**: Gentle pulse when syncing (not spinning—too anxious)
- **AI thinking**: Soft shimmer or wave effect (not loading spinner)
- **Live updates**: Items slide into position, never "pop" in
- **Cursor/selection**: Gentle glow, not harsh outline

### Personality Without Annoyance

**Empty states**: Friendly, helpful illustrations with clear next action
- "Your inbox is empty" → Celebration, not just absence
- "No tasks today" → Gentle prompt to plan or enjoy the freedom

**Error states**: Honest, actionable, not alarming
- "Couldn't sync with Todoist" → Clear explanation + retry button
- Never: aggressive red, panic-inducing language

**AI interactions**: Conversational but not overly casual
- "I noticed you've postponed this 7 times. Would you like help?"
- Not: "Hey there! 👋 Looks like you're stuck!"

### Sound Design (Optional, Off by Default)

If implemented:
- **Capture save**: Soft "pop" (like a bubble)
- **Task complete**: Gentle chime
- **Focus start**: Soft transition tone
- **Focus end**: Gentle bell

All sounds: subtle, optional, easily disabled.

---

## UI Guidelines

### Do

- **Prioritize content over chrome**: The user's data is the star
- **Use animation purposefully**: Animations communicate state changes; some can exist purely for delight, but must never annoy or repeat endlessly
- **Maintain spatial consistency**: Elements should feel anchored to their locations
- **Provide immediate feedback**: Every action gets a response
- **Design for keyboard-first**: Mouse/touch as enhancement, not requirement
- **Show sync status clearly**: Users need to trust their data is safe
- **Use progressive disclosure**: Complexity on demand, simplicity by default
- **Celebrate completion**: Small wins deserve recognition

**Which-Key Navigation System**:
- Trigger key (e.g., Space in normal mode) opens command mode
- Mnemonic keys for actions: `f` for file, `b` for block, `t` for task
- After brief delay (~300ms), popup shows available keys and their actions
- Hierarchical: `Space → b → d` = "block → delete"
- Inspired by Spacemacs, VSpaceCode, Helix
- Discoverable for beginners, fast for experts
- Context-aware: available actions depend on current selection/mode

### Don't

- **Never interrupt Flow mode**: Notifications wait unless critical
- **No attention-seeking animations**: Nothing should loop or pulse continuously
- **Avoid harsh colors for status**: Soft indicators, not alarm signals
- **No gamification pressure**: Streaks optional, never guilting
- **Don't hide essential info**: Sync status, undo, navigation always available
- **No dark patterns**: Never trick users into actions
- **Avoid information overload**: Orient shows overview, details on demand

### Accessibility

- **Contrast ratios**: WCAG AA minimum, AAA preferred
- **Focus indicators**: Clear, visible keyboard focus states
- **Screen reader support**: Semantic HTML, ARIA labels
- **Motion sensitivity**: Respect `prefers-reduced-motion`
- **Color independence**: Never use color as sole indicator

---

## Mode Transitions

### Capture → Dismiss
- Duration: 80ms
- Effect: Fade out + slight scale down
- Feel: Quick, got it, moving on

### Orient → Flow
- Duration: 300ms
- Effect: Selected task zooms to fill, other elements fade and recede
- Feel: Deliberate, entering a different mental space

### Flow → Orient
- Duration: 250ms
- Effect: Task shrinks back, context fades in around it
- Feel: Gentle return, not jarring exit

### Between Orient Sections
- Duration: 150ms
- Effect: Cross-fade or slide depending on spatial relationship
- Feel: Smooth, oriented in space

---

## Inspiration & References

**Calm, focused interfaces**:
- iA Writer (typography, focus)
- Things 3 (polish, satisfaction)
- Linear (density, professionalism)
- Notion (flexibility, but more opinionated)

**Alive without anxiety**:
- Stripe Dashboard (subtle animations, clear status)
- Vercel (smooth transitions, dark mode)
- Apple Music (playfulness in details)

**Trust through transparency**:
- 1Password (security indicators)
- Figma (collaboration status)
- Git clients (sync status patterns)

---

## Summary

Holon's UI should feel like a **calm, competent assistant** that:

1. **Knows when to recede**: Flow mode is about your work, not the app
2. **Provides grounding when needed**: Orient mode shows the whole picture
3. **Captures without friction**: Quick thoughts go in effortlessly
4. **Earns trust through transparency**: Always clear what's happening
5. **Delights in small moments**: Micro-interactions that feel satisfying
6. **Never demands attention**: The user is in control, always

The app is alive—it breathes, responds, and feels present—but it's not performing for you. It's there when you need it, invisible when you don't.

---

## Related Documents

- [VISION_LONG_TERM.md](VISION_LONG_TERM.md) - Philosophical foundation
- [VISION.md](VISION.md) - Technical vision
- [ARCHITECTURE_PRINCIPLES.md](ARCHITECTURE_PRINCIPLES.md) - Architectural decisions
