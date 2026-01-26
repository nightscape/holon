# UI Implementation TODO

This document tracks the work needed to achieve the UI vision described in [VISION_UI.md](../../VISION_UI.md).

## Current State Summary

The Flutter frontend has:
- **RenderInterpreter**: Generic PRQL→Widget renderer (~2300 lines)
- **Riverpod 3.x state management**: Providers for settings, queries, UI state
- **Multi-theme support**: 16+ themes via YAML, including Holon Light/Dark
- **Pie menu system**: Field-based auto-attachment for context operations
- **CDC streaming**: Reactive updates from Rust backend
- **Basic render functions**: list, tree, outline, row, block, text, editable_text, checkbox, badge, bullet, pie_menu, icon, spacer, draggable, flexible
- **Dashboard render functions** (Phase 2): section, grid, stack, scroll, date_header, progress, count_badge, status_indicator, hover_row, focusable, staggered, animated, pulse
- **Focus state infrastructure**: focusedBlockIdProvider, focusDepthProvider, flowSessionStartProvider, captureOverlayProvider
- **Animation constants**: AnimDurations, AnimCurves, ConcealmentOpacity

Missing from vision:
- Capture overlay widget
- Orient dashboard document
- Progressive concealment behavior
- Which-Key navigation
- Micro-interactions (checkbox animation, reordering physics)

---

## Phase 0: Design System Foundation ✅ COMPLETE

### 0.1 Holon Theme Files (YAML only) ✅
- [x] Created `assets/themes/holon.yaml` with holonLight and holonDark themes
- [x] Set Holon Light as default theme
- [x] Updated fallback colors in `app_styles.dart` to match Holon palette

### 0.2 Typography Refinement
- [ ] Audit current typography against vision (Inter 15-16px body, 600 weight headings)
- [ ] Ensure line-height 1.5-1.6 for body text
- [ ] Add subtle block reference styling (different weight or background)
- [ ] Configure code blocks with gentle background tint

### 0.3 Animation Constants ✅
- [x] Created `lib/styles/animation_constants.dart` with timing/curve constants
- [x] AnimDurations: capture, focus, stagger, hover, item appear/disappear, pulse
- [x] AnimCurves: easeOut/easeIn variants, spring for reordering
- [x] ConcealmentOpacity: helper for progressive concealment

### 0.4 Focus State Infrastructure ✅
- [x] Added `focusedBlockIdProvider` - which block has deep focus (null = none)
- [x] Added `focusDepthProvider` - 0.0 (orient/overview) to 1.0 (deep flow)
- [x] Added `flowSessionStartProvider` - when focus session started (for timer)
- [x] Added `captureOverlayProvider` - transient overlay state

---

## Phase 1: Capture Overlay

Build the quick-capture overlay inspired by Spotlight/Alfred.

### 1.1 Capture Overlay Widget
- [ ] Create `lib/ui/capture/capture_overlay.dart`
- [ ] Design minimal chrome: input field + essential context
- [ ] Implement keyboard-first interaction:
  - Auto-focus on open
  - Enter to save
  - Escape to dismiss
  - Click-outside to dismiss
- [ ] Add subtle depth (light shadow) to float above context

### 1.2 Capture Animations
- [ ] Fade-in: 100ms ease-out
- [ ] Fade-out: 80ms ease-in
- [ ] Input field subtle focus glow
- [ ] Brief satisfaction animation on save (checkmark or pulse)

### 1.3 Capture Hotkey Options

Flutter can detect modifier keys alone and implement timing-based gestures:

**Option A: Double-tap modifier** (recommended)
- Detect double-tap of Ctrl/Cmd/Shift within ~400ms
- Implemented via key down/up timestamps + state machine
- Works only when Flutter window has focus

**Option B: Long-press modifier**
- Detect modifier held for 300-500ms without other keys
- Fire on timer, or on key-up if held long enough

**Option C: Modifier alone (on key-up)**
- If modifier goes down→up with no other key between = "modifier alone"
- Fast but may conflict with normal modifier usage

**Implementation notes:**
- Use `RawKeyboardListener` or `Focus` widget to capture key events
- Maintain timestamps for last key-down/key-up per modifier
- For global hotkeys (app not focused): requires platform plugin (`hotkey_manager` or similar)

- [ ] Implement double-tap Ctrl detection for capture overlay
- [ ] Add setting to choose trigger (double-tap Ctrl, Cmd+Space, etc.)
- [ ] Consider `hotkey_manager` package for system-wide hotkey

### 1.4 Backend Integration
- [ ] Connect capture to block creation operation
- [ ] Route captured items to inbox

### 1.5 Capture Auto-complete (DEFERRED)
Backend not ready for autocomplete queries. Defer until:
- Tag/project listing queries available
- Block link search available

---

## Phase 2: RenderInterpreter Extensions for Dashboards ✅ COMPLETE

### 2.1 Layout Primitives ✅
- [x] `section(title:, child:)` - Card container with header, optional collapse
- [x] `grid(columns:, gap:, children:)` - Multi-column CSS Grid-like layout
- [x] `stack(children:)` - Overlapping children (for overlays)
- [x] `scroll(child:)` - Scrollable container

### 2.2 Dashboard Widgets ✅
- [x] `date_header(format:)` - Formatted date display (e.g., "Wednesday, December 4")
- [x] `progress(value:, max:, style:)` - Progress indicator (●●●○ style or bar)
- [x] `count_badge(count:, animate:)` - Count badge with optional countUp animation
- [x] `status_indicator(status:)` - Sync/status with appropriate color

### 2.3 Interactive Enhancements ✅
- [x] `hover_row(child:)` - Row with hover effects:
  - Text color becomes more pronounced
  - Action icons fade in
  - Subtle background tint (2-3% opacity)
  - 100ms ease-out transition
- [x] `focusable(child:, on_focus:)` - Block that can become the focus target

### 2.4 Animation Support ✅
- [x] `staggered(delay:, children:)` - Staggered fade-in for children
- [x] `animated(property:, duration:, child:)` - Generic animation wrapper
- [x] `pulse(once:, child:)` - Single or continuous pulse effect

### 2.5 Context-Aware Rendering ✅
- [x] Added `focusDepth` to RenderContext
- [x] Propagated through enrichedContext in all function calls
- [x] Widgets can now adapt rendering based on focus depth

---

## Phase 3: Orient Dashboard

Define the Orient view as a PRQL document using the extended render functions.

### 3.1 Orient Document Structure
- [ ] Create default Orient document in database (user-editable)
- [ ] Document uses PRQL queries for each section's data
- [ ] Rendered via `render_interpreter.dart`

Example structure (now implementable with Phase 2 functions):
```prql
# Orient dashboard layout
render(
  staggered(delay: 50,
    date_header(),
    grid(columns: 2, gap: 24,
      section(title: "Today's Focus",
        list(
          from tasks
          filter due_date == @today
          sort priority desc
          render: hover_row(
            row(
              checkbox(checked: completed),
              flexible(text(content)),
              progress(value: effort_spent, max: effort_estimate)
            )
          )
        )
      ),
      section(title: "Inbox",
        list(
          from blocks
          filter inbox == true
          sort created_at desc
          limit 5
          render: hover_row(row(bullet(), text(content)))
        )
      ),
      section(title: "Watcher",
        list(
          from ai_insights
          sort priority desc
          limit 5
          render: row(status_indicator(status: type), text(message))
        )
      )
    )
  )
)
```

### 3.2 Orient-Specific Features
- [ ] Date header at top (auto-updating)
- [ ] Staggered section fade-in on load
- [ ] Count badges on section headers
- [ ] Quick navigation to any section

### 3.3 AI Integration (Watcher)
- [ ] AI recommendations stored in a table (e.g., `ai_insights`)
- [ ] Treated as regular data, queried via PRQL
- [ ] Background job populates insights periodically
- [ ] Insights are actionable (link to relevant block/task)

### 3.4 Status Indicators
- [ ] Sync status always visible in corner
  - ✓ Synced: Sage green (muted)
  - ⏳ Pending: Warm amber
  - ⚠️ Attention: Soft coral
  - ❌ Error: Muted red + clear action
- [ ] Gentle pulse when syncing (not spinning)

---

## Phase 4: Flow State & Transitions

Flow isn't a separate "mode"—it's the deep end of the focus spectrum.

### 4.1 Focus Depth Transitions
- [ ] Clicking/selecting a block increases `focusDepth` toward 1.0
- [ ] Sustained editing (2-3 seconds) deepens focus further
- [ ] Moving away/clicking elsewhere reduces `focusDepth` toward 0.0
- [ ] Transition duration: 300ms into deeper focus, 250ms out

### 4.2 Progressive Concealment
As `focusDepth` increases:
- [ ] Surrounding content gradually fades (opacity reduction)
- [ ] Peripheral elements (sidebar, header, other sections) dim
- [ ] At full depth: almost full-screen for focused content
- [ ] User never explicitly switches modes—it responds to attention

### 4.3 Flow Timer
- [ ] When `focusDepth` > 0.5 for sustained period, show timer
- [ ] Timer appears in corner (◷ 47m style)
- [ ] Unobtrusive, peripheral

### 4.4 Context Panel
- [ ] Related items available on demand (hotkey or gesture)
- [ ] Slides in from right edge (200ms ease-out)
- [ ] Content loaded via PRQL query (related blocks, backlinks)
- [ ] Slides out when dismissed

### 4.5 Flow Protections
When `focusDepth` > 0.8:
- [ ] Suppress non-urgent notifications
- [ ] Hide task counts and inbox badge
- [ ] Show sync status only on error
- [ ] Minimal UI chrome

---

## Phase 5: Micro-interactions & Polish

### 5.1 Checkbox Animation
- [ ] Satisfying checkmark draw animation (150ms)
- [ ] Consider subtle bounce or glow on complete

### 5.2 Task Reordering
- [ ] Smooth spring physics for drag-reorder
- [ ] Items settle naturally, not snap
- [ ] Space closes smoothly when item removed

### 5.3 Item Appearance/Disappearance
- [ ] New item: gentle fade + slide from origin
- [ ] Delete item: fade + slight shrink, space closes smoothly
- [ ] Never "pop" in or out—always animate

### 5.4 Sync Indicator
- [ ] Gentle pulse when syncing (not spinning)
- [ ] Clear "synced" state (static icon)

### 5.5 AI Thinking Indicator
- [ ] Soft shimmer or wave effect
- [ ] Not a loading spinner

### 5.6 Empty States
- [ ] Friendly, helpful text with clear next action
- [ ] "Your inbox is empty" → celebration
- [ ] "No tasks today" → prompt to plan or enjoy

### 5.7 Error States
- [ ] Honest, actionable, not alarming
- [ ] Clear explanation + retry button
- [ ] Always provide escape path

---

## Phase 6: Which-Key Navigation

### 6.1 Which-Key Infrastructure
- [ ] Create `lib/ui/which_key/which_key_overlay.dart`
- [ ] Create `lib/ui/which_key/which_key_binding.dart` for key definitions
- [ ] Build hierarchical key tree structure

### 6.2 Trigger Mechanism
- [ ] Define trigger key (Space when not editing, or configurable)
- [ ] Show overlay after 300ms delay
- [ ] Immediate action on key press (no delay if user knows key)

### 6.3 Which-Key Display
- [ ] Show available keys and their actions in popup
- [ ] Mnemonic associations: `f`=file, `b`=block, `t`=task, `n`=navigation
- [ ] Support multi-key sequences: `Space → b → d` = "block → delete"

### 6.4 Context Awareness
- [ ] Available actions depend on current selection and focusDepth
- [ ] Different keys when editing vs navigating
- [ ] Different keys when block selected vs nothing selected

---

## Phase 7: Sound Design (Optional)

All sounds off by default.

- [ ] Capture save: soft "pop"
- [ ] Task complete: gentle chime
- [ ] Focus start: soft transition tone
- [ ] Focus end: gentle bell
- [ ] Master enable/disable in settings

---

## Phase 8: Accessibility

- [ ] Audit all color combinations for WCAG AA minimum
- [ ] Clear, visible keyboard focus states
- [ ] Semantic widget tree with Semantics widgets
- [ ] Respect `MediaQuery.disableAnimations`
- [ ] Provide reduced-motion alternatives

---

## Phase 9: Platform Polish

- [ ] Remember window position and size
  - https://pub.dev/packages/window_manager
- [ ] Menu bar integration (macOS)
- [ ] Profile animation performance (60fps target)
- [ ] Optimize widget rebuilds
- [ ] Open holon-links:// URLs from browser
  - https://pub.dev/packages/app_links
  - https://github.com/leanflutter/protocol_handler

---

## Phase 10: Beauty & Delight
- [ ] Make window semi-transparent
  - https://pub.dev/packages/flutter_acrylic
  - https://pub.dev/packages/window_manager
    - https://leanflutter.dev/documentation/window_manager/quick-start
- [ ] Glass-like look
  - https://pub.dev/packages/soft_edge_blur
  - https://pub.dev/packages/blurrycontainer
- [ ] Make titlebar adapt color of app
  - https://pub.dev/packages/macos_window_utils
  - https://pub.dev/packages/mobile_window_features
- [ ] Add toasts for incoming notifications
  - https://pub.dev/packages/motion_toast
  - https://github.com/leanflutter/local_notifier

  

## Implementation Notes

### Key Architectural Decisions

1. **No hard modes**: Focus is a continuous spectrum (0.0→1.0), not discrete states
2. **Document-driven layouts**: Orient/Flow layouts defined as PRQL documents, rendered by `render_interpreter.dart`
3. **YAML-only themes**: No hardcoded colors in Dart (fallbacks match Holon theme)
4. **AI as data**: Watcher insights stored in regular tables, queried like other data

### Files Modified (Phase 0 & 2)
- `lib/render/render_interpreter.dart` - Added 13 dashboard render functions
- `lib/render/render_context.dart` - Added focusDepth field
- `lib/providers/ui_state_providers.dart` - Added focus state providers
- `lib/providers/settings_provider.dart` - Changed default to holonLight
- `lib/styles/app_styles.dart` - Updated fallback colors to Holon palette
- `lib/styles/theme_loader.dart` - Added holon.yaml to theme list

### New Files Created (Phase 0 & 2)
- `assets/themes/holon.yaml` - Holon Light and Dark themes
- `lib/styles/animation_constants.dart` - AnimDurations, AnimCurves, ConcealmentOpacity

### Files to Create (Future Phases)
- `lib/ui/capture/capture_overlay.dart`
- `lib/ui/which_key/which_key_overlay.dart`

### Packages to Consider
- `flutter_animate` - declarative animations
- `hotkey_manager` - system-wide hotkeys
- `audioplayers` - sound effects (if Phase 7 implemented)

---

## Success Criteria

1. **Calm Technology**: Information available at a glance, not demanding attention
2. **Continuous Focus**: Seamless transition from overview to deep work
3. **Progressive Concealment**: Focus naturally creates visual quiet
4. **Trust Through Transparency**: Sync status clear, undo always available
5. **Keyboard-First**: Which-Key makes all actions discoverable and fast
6. **Document-Driven**: Layouts customizable via PRQL, not hardcoded
7. **Warm, Professional Look**: Holon theme feels alive, not clinical
8. **Accessible**: WCAG compliant, motion-sensitive users accommodated

The app should feel like a **calm, competent assistant** that knows when to recede and when to help.
