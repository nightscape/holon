# R3BL TUI Refactoring Guide

## ✅ Completed Refactorings (Nov 2025)

### Phase 1: Separation of Concerns & Async Operations

**Status**: ✅ **COMPLETED**

We've successfully refactored the RenderInterpreter to separate interpretation from rendering, with operations attached and proper async execution:

#### 1. UIElement Intermediate Representation
- **File**: `src/ui_element.rs`
- **What**: Created enum with `Text`, `Checkbox`, `Badge`, `Icon`, `Row` variants
- **Operations**: `OperationWiring` attached to interactive elements (from backend)
- **Benefit**: Clean separation between interpretation and rendering

#### 2. RenderInterpreter Refactoring
- **File**: `src/render_interpreter.rs`
- **Methods**:
  - `build_element_tree()` - RenderSpec → Vec<UIElement> (interpretation)
  - `render_element_tree()` - UIElement → RenderOpIRVec (rendering)
- **Benefit**: Each phase can be tested/modified independently

#### 3. BlockListComponent Enhancement
- **File**: `src/components/block_list.rs`
- **What**: Stores `element_tree`, rebuilds on render, extracts operations
- **Async Flow**: Space key → extract operation → send signal → spawn async task
- **Benefit**: Non-blocking operation execution

#### 4. Signal-Based Async Execution
- **Files**: `src/app_main.rs`, `src/state.rs`
- **AppSignal::ExecuteOperation**: Carries table, field, value
- **app_handle_signal**: Spawns `tokio::spawn()` for DB operations
- **Benefit**: UI stays responsive, no blocking

#### Architecture Flow
```
User Input (Space key)
  ↓
BlockListComponent::handle_event (sync)
  ↓
Extract operation from element_tree[selected_index]
  ↓
Send AppSignal::ExecuteOperation via channel
  ↓
app_handle_signal receives signal
  ↓
tokio::spawn(async { execute_operation() })  ← Non-blocking!
  ↓
UI continues rendering
  ↓
CDC picks up DB change → UI auto-updates
```

#### Results
- ✅ No blocking async operations
- ✅ Operations discovered & attached from backend
- ✅ Clean separation: RenderSpec → UIElement → RenderOpIRVec
- ✅ Component-based architecture in place
- ✅ Proper R3BL signal patterns

---

## Executive Summary (Original Analysis)

Our current implementation treats `r3bl-tui` as a low-level rendering API when it's actually a **component-based application framework** (similar to React). This fundamental architectural mismatch results in:

- **~40% utilization** of r3bl's capabilities while writing **3x more code**
- **Critical performance issues** from blocking async operations ← **FIXED ✅**
- **Poor scalability** - adding features requires exponential complexity growth
- **High maintenance burden** from fragile manual layout and scattered logic

**Impact:** We're fighting against the framework instead of leveraging it.

**ROI:** Refactoring will reduce code complexity while unlocking powerful built-in features.

---

## Critical Issues (Ordered by Priority)

### ✅ 1. Blocking Async Operations - **FIXED**

**Original Problem** (`state.rs:157-221`):
```rust
// ANTI-PATTERN: Creates new runtime per operation!
let result = std::thread::spawn(move || {
    let rt = tokio::runtime::Runtime::new().unwrap();  // Expensive!
    rt.block_on(async move {
        let result = engine.execute_operation(&op_name_owned, params).await;
        result
    })
})
.join()  // BLOCKS UI THREAD!
```

**Why This is Critical:**
- Creates new Tokio runtime (~expensive allocation) for every keypress
- `.join()` blocks event loop, freezing entire application
- Completely defeats purpose of async Rust
- Makes UI feel sluggish and unresponsive

**r3bl Pattern:**
```rust
// edi/state.rs:243-268 - Non-blocking with tokio::spawn
tokio::spawn(async move {
    let result = std::fs::write(&*file_path, &content);
    // No blocking, UI stays responsive
});
```

**✅ Implemented Solution:**

The fix uses R3BL's signal system to handle async operations without blocking:

1. **Component sends signal** (`components/block_list.rs`):
   ```rust
   // Space key handler
   send_signal!(
       global_data.main_thread_channel_sender,
       TerminalWindowMainThreadSignal::ApplyAppSignal(
           AppSignal::ExecuteOperation { table, field, value, ... }
       )
   );
   ```

2. **Signal handler spawns async task** (`app_main.rs`):
   ```rust
   fn app_handle_signal(&mut self, action: &AppSignal, ...) {
       match action {
           AppSignal::ExecuteOperation { ... } => {
               let engine = global_data.state.engine.clone();
               tokio::spawn(async move {
                   engine.read().await.execute_operation(...).await
               });
           }
       }
   }
   ```

**Benefits:**
- ✅ No blocking - UI stays responsive
- ✅ No runtime creation overhead
- ✅ Proper async/await usage
- ✅ CDC automatically updates UI when operation completes

**Files modified:** `src/state.rs`, `src/app_main.rs`, `src/components/block_list.rs`

---

### ✅ 2. Component Architecture - **PARTIALLY FIXED**

**Original Problem** (`app_main.rs:174-219`):
```rust
// ANTI-PATTERN: Monolithic rendering
fn app_render(...) -> CommonResult<RenderPipeline> {
    let mut pipeline = render_pipeline!();

    // Manual rendering - no abstraction!
    pipeline.push(ZOrder::Normal, {
        let mut render_ops = RenderOpIRVec::new();
        RenderInterpreter::render(&mut render_ops, ...);
        render_ops
    });
}
```

**Why This is Critical:**
- All UI logic in single function - doesn't scale
- No reusable abstractions
- Can't use built-in components (EditorComponent, DialogComponent)
- Adding features requires modifying monolithic code
- Can't test UI components in isolation

**r3bl Pattern** (`edi/app_main.rs:98-106, 527-580`):
```rust
// Component-based architecture
fn app_init(
    &mut self,
    component_registry_map: &mut ComponentRegistryMap<State, AppSignal>,
    has_focus: &mut HasFocus,
) {
    // Register reusable components
    let editor_component = EditorComponent::new_boxed(
        FlexBoxId::from(Id::ComponentEditor),
        config,
        on_buffer_change,
    );
    ComponentRegistry::put(component_registry_map, id, editor_component);

    // Set initial focus
    has_focus.set_id(id);
}

fn app_render(...) -> CommonResult<RenderPipeline> {
    // Declarative layout with components
    box_start!(
        in: surface,
        id: FlexBoxId::from(Id::ComponentEditor),
        dir: LayoutDirection::Vertical,
        requested_size_percent: req_size_pc!(width: 100, height: 100)
    );
    render_component_in_current_box!(
        in: surface,
        component_id: FlexBoxId::from(Id::ComponentEditor),
        from: component_registry_map
    );
    box_end!(in: surface);
}
```

**✅ Implemented Solution:**

We now have a proper component-based architecture:

#### Phase 1: ✅ Custom Component Created
**File**: `src/components/block_list.rs`

```rust
pub struct BlockListComponent {
    id: FlexBoxId,
    element_tree: Vec<UIElement>,  // ← Intermediate representation
}

impl Component<State, AppSignal> for BlockListComponent {
    fn handle_event(...) -> CommonResult<EventPropagation> {
        // Handles arrow keys, space (operation execution), indent/outdent, etc
        // Sends signals for async operations
    }

    fn render(...) -> CommonResult<RenderPipeline> {
        // Rebuild element tree from RenderSpec
        self.rebuild_element_tree(global_data);

        // Render using element tree
        RenderInterpreter::render_element_tree(&self.element_tree, ...);
    }
}
```

#### Phase 2: ✅ Component Registration
**File**: `src/app_main.rs`

Components are now properly registered in `app_init` and used declaratively with Surface + FlexBox layout.

#### Phase 3: ✅ Component Usage
**File**: `src/app_main.rs`

The app uses proper R3BL patterns:
- Surface-based layout with stylesheets
- FlexBox for responsive sizing
- Components registered and rendered via registry

**Achievements:**
- ✅ Reusable BlockListComponent
- ✅ Event routing via HasFocus
- ✅ Component encapsulates rendering logic
- ✅ Operations executed via signals

**Remaining Work:**
- ⏳ Could add more components (QueryEditor, StatusBar)
- ⏳ Could leverage EditorComponent for PRQL editing
- ⏳ Could add DialogComponent for confirmations

---

### 🟠 3. State Not Integrated with Framework (HIGH - Integration)

**Current Problem** (`state.rs:11-20`):
```rust
// Standalone state - doesn't work with r3bl components
pub struct State {
    pub engine: Arc<RwLock<RenderEngine>>,
    pub render_spec: RenderSpec,
    pub data: Vec<HashMap<String, Value>>,
    // No r3bl traits implemented!
}
```

**Why This Matters:**
- Can't use EditorComponent (needs HasEditorBuffers)
- Can't use DialogComponent (needs HasDialogBuffers)
- Reinventing what framework provides

**r3bl Pattern** (`edi/state.rs:14-17, 271-291`):
```rust
#[derive(Clone, PartialEq)]
pub struct State {
    pub editor_buffers: HashMap<FlexBoxId, EditorBuffer>,
    pub dialog_buffers: HashMap<FlexBoxId, DialogBuffer>,
}

impl HasEditorBuffers for State {
    fn get_mut_editor_buffer(&mut self, id: FlexBoxId) -> Option<&mut EditorBuffer> {
        self.editor_buffers.get_mut(&id)
    }

    fn insert_editor_buffer(&mut self, id: FlexBoxId, buffer: EditorBuffer) {
        self.editor_buffers.insert(id, buffer);
    }

    fn contains_editor_buffer(&self, id: FlexBoxId) -> bool {
        self.editor_buffers.contains_key(&id)
    }
}

impl HasDialogBuffers for State {
    fn get_mut_dialog_buffer(&mut self, id: FlexBoxId) -> Option<&mut DialogBuffer> {
        self.dialog_buffers.get_mut(&id)
    }
}
```

**Solution:**
```rust
// state.rs - Enhanced
use r3bl_tui::{EditorBuffer, DialogBuffer, HasEditorBuffers, HasDialogBuffers, FlexBoxId};

pub struct State {
    // Existing fields
    pub engine: Arc<RwLock<RenderEngine>>,
    pub render_spec: RenderSpec,
    pub data: Vec<HashMap<String, Value>>,
    pub selected_index: usize,

    // Add r3bl integration
    pub editor_buffers: HashMap<FlexBoxId, EditorBuffer>,  // For PRQL editor!
    pub dialog_buffers: HashMap<FlexBoxId, DialogBuffer>,  // For confirmations!
}

impl HasEditorBuffers for State {
    fn get_mut_editor_buffer(&mut self, id: FlexBoxId) -> Option<&mut EditorBuffer> {
        self.editor_buffers.get_mut(&id)
    }

    fn insert_editor_buffer(&mut self, id: FlexBoxId, buffer: EditorBuffer) {
        self.editor_buffers.insert(id, buffer);
    }

    fn contains_editor_buffer(&self, id: FlexBoxId) -> bool {
        self.editor_buffers.contains_key(&id)
    }
}

impl HasDialogBuffers for State {
    fn get_mut_dialog_buffer(&mut self, id: FlexBoxId) -> Option<&mut DialogBuffer> {
        self.dialog_buffers.get_mut(&id)
    }
}
```

**Benefits:**
- ✅ Unlock EditorComponent for PRQL query editing
- ✅ Unlock DialogComponent for confirmations/prompts
- ✅ Use built-in focus management
- ✅ Standard state patterns

**Files to modify:** `state.rs`

**References:**
- edi/state.rs:14-17 (State structure)
- edi/state.rs:271-301 (trait implementations)

---

### 🟠 4. Manual Layout Instead of FlexBox (HIGH - Maintainability)

**Current Problem** (`render_interpreter.rs:16-21, 138-141`):
```rust
// Manual cursor tracking
struct RenderContext {
    pub current_row: usize,
    pub current_col: usize,
    // ...
}

// Hardcoded positioning
*render_ops += RenderOpCommon::MoveCursorPositionAbs(Pos::from((
    col(context.current_col),
    row(context.current_row),
)));
```

**Why This is Fragile:**
- Every element position manually calculated
- Breaks with window resize
- Adding columns requires recalculating everything
- No automatic wrapping, scrolling, or clipping
- Can't nest layouts

**r3bl Pattern** (`edi/app_main.rs:484-500`):
```rust
// Declarative layout with automatic positioning
surface.surface_start(SurfaceProps {
    pos: row(0) + col(0),
    size: window_size.col_width + (window_size.row_height - height(1)),
})?;

box_start!(
    in: surface,
    id: FlexBoxId::from(Id::ComponentEditor),
    dir: LayoutDirection::Vertical,
    requested_size_percent: req_size_pc!(width: 100, height: 100),
    styles: [Id::StyleEditorDefault]
);
render_component_in_current_box!(...);
box_end!(in: surface);

surface.surface_end()?;
```

**Solution:**
```rust
// Use Surface + FlexBox in app_render
fn app_render(...) -> CommonResult<RenderPipeline> {
    throws_with_return!({
        let window_size = global_data.window_size;

        let mut surface = surface!(stylesheet: create_stylesheet()?);

        surface.surface_start(SurfaceProps {
            pos: row(0) + col(0),
            size: window_size,
        })?;

        // Split layout: 90% list, 10% status bar
        box_start!(
            in: surface,
            id: FlexBoxId::from(Id::BlockListBox),
            dir: LayoutDirection::Vertical,
            requested_size_percent: req_size_pc!(width: 100, height: 90)
        );
        render_component_in_current_box!(...);
        box_end!(in: surface);

        box_start!(
            in: surface,
            id: FlexBoxId::from(Id::StatusBarBox),
            dir: LayoutDirection::Horizontal,
            requested_size_percent: req_size_pc!(width: 100, height: 10)
        );
        render_status_bar_component!(...);
        box_end!(in: surface);

        surface.surface_end()?;
        surface.render_pipeline
    });
}
```

**Benefits:**
- ✅ Automatic positioning and sizing
- ✅ Responsive layouts (window resize handling)
- ✅ Nested layouts for complex UIs
- ✅ Scrolling, clipping handled by framework

**Files to modify:** `app_main.rs`, reduce usage in `render_interpreter.rs`

**References:**
- edi/app_main.rs:252-281 (Surface setup)
- edi/app_main.rs:484-516 (FlexBox layout)

---

### 🟡 5. No Stylesheet System (MEDIUM - Maintainability)

**Current Problem** (`render_interpreter.rs:419-437`, `app_main.rs:223-233`):
```rust
// Scattered inline styles
let fg = fg_color.unwrap_or_else(|| tui_color!(hex "#CCCCCC"));
let bg = bg_color.unwrap_or_else(|| tui_color!(hex "#000000"));

let styled_texts = tui_styled_texts! {
    tui_styled_text! {
        @style: new_style!(color_fg: {fg} color_bg: {bg}),
        @text: text
    },
};
```

**Why This Matters:**
- Colors and styles duplicated throughout code
- Hard to maintain consistent theme
- Can't easily switch themes or support user preferences
- No style reuse

**r3bl Pattern** (`edi/app_main.rs:583-624`):
```rust
// Centralized stylesheet
mod stylesheet {
    pub fn create_stylesheet() -> CommonResult<TuiStylesheet> {
        throws_with_return!({
            tui_stylesheet! {
                new_style!(
                    id: {Id::StyleEditorDefault}
                    padding: {1}
                    attrib: [bold]
                    color_fg: TuiColor::Blue
                ),
                new_style!(
                    id: {Id::StyleDialogTitle}
                    lolcat  // Built-in gradient!
                ),
                new_style!(
                    id: {Id::StyleDialogBorder}
                    dim
                    color_fg: {tui_color!(green)}
                ),
            }
        })
    }
}
```

**Solution:**
```rust
// New file: src/stylesheet.rs
use r3bl_tui::{TuiStylesheet, tui_stylesheet, new_style, tui_color};

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StyleId {
    BlockSelected = 1,
    BlockNormal = 2,
    BlockCompleted = 3,
    CheckboxChecked = 4,
    CheckboxUnchecked = 5,
    BadgeDefault = 6,
    StatusBarBg = 7,
    StatusBarFg = 8,
}

impl From<StyleId> for u8 {
    fn from(id: StyleId) -> u8 { id as u8 }
}

pub fn create_stylesheet() -> CommonResult<TuiStylesheet> {
    throws_with_return!({
        tui_stylesheet! {
            new_style!(
                id: {StyleId::BlockSelected}
                color_bg: {tui_color!(hex "#333333")}
            ),
            new_style!(
                id: {StyleId::BlockNormal}
                color_fg: {tui_color!(hex "#CCCCCC")}
            ),
            new_style!(
                id: {StyleId::CheckboxChecked}
                color_fg: {tui_color!(hex "#00FF00")}
            ),
            // ... more styles
        }
    })
}

// Usage in components
let style = get_tui_style!(@from_result: stylesheet, StyleId::BlockSelected);
```

**Benefits:**
- ✅ Centralized theme management
- ✅ Easy to add dark/light themes
- ✅ Reusable style IDs
- ✅ Built-in gradients (lolcat)

**Files to create:** `src/stylesheet.rs`
**Files to modify:** `render_interpreter.rs`, `app_main.rs`

**References:**
- edi/app_main.rs:583-624 (stylesheet pattern)

---

### 🟡 6. Minimal Signal Usage (MEDIUM - Architecture)

**Current Problem** (`state.rs:433-438`, `app_main.rs:162-172`):
```rust
// Underutilized signal system
pub enum AppSignal {
    #[default]
    Noop,
    RefreshData,  // Never actually used!
}

fn app_handle_signal(...) -> CommonResult<EventPropagation> {
    throws_with_return!({
        EventPropagation::ConsumedRender  // Does nothing!
    });
}
```

**Why Signals Matter:**
- Enable async operations to update UI
- Background tasks can notify main thread
- Modal dialogs communicate via signals
- CDC changes can trigger UI refresh

**r3bl Pattern** (`edi/app_main.rs:23-30, 169-241`):
```rust
#[derive(Default, Clone, Debug)]
pub enum AppSignal {
    AskForFilenameToSaveFile,
    SaveFile,
    #[default]
    Noop,
}

fn app_handle_signal(
    &mut self,
    action: &AppSignal,
    global_data: &mut GlobalData<State, AppSignal>,
    component_registry_map: &mut ComponentRegistryMap<State, AppSignal>,
    has_focus: &mut HasFocus,
) -> CommonResult<EventPropagation> {
    match action {
        AppSignal::SaveFile => {
            // Complex save logic with buffer access
            let editor_buffer = state.editor_buffers.get_mut(...);
            // ...
        }
        AppSignal::AskForFilenameToSaveFile => {
            // Show modal dialog
            ComponentRegistry::reset_component(...);
            modal_dialog::show(...)?;
            return Ok(EventPropagation::ConsumedRender);
        }
        // ...
    }
}
```

**Solution:**
```rust
// Enhanced signal system
#[derive(Clone, Debug)]
pub enum AppSignal {
    #[default]
    Noop,

    // Operation results from async tasks
    OperationComplete(Result<(), String>),
    OperationFailed(String),

    // CDC updates
    DataUpdated(Vec<HashMap<String, Value>>),

    // User interactions
    ConfirmDeleteBlock(String),  // Block ID
    ShowQueryEditor,
    HideQueryEditor,
}

fn app_handle_signal(
    &mut self,
    action: &AppSignal,
    global_data: &mut GlobalData<State, AppSignal>,
    // ...
) -> CommonResult<EventPropagation> {
    match action {
        AppSignal::OperationComplete(Ok(())) => {
            global_data.state.status_message = "Operation succeeded".to_string();
            Ok(EventPropagation::ConsumedRender)
        }
        AppSignal::OperationFailed(err) => {
            global_data.state.status_message = format!("Error: {}", err);
            Ok(EventPropagation::ConsumedRender)
        }
        AppSignal::DataUpdated(new_data) => {
            global_data.state.update_data(new_data);
            Ok(EventPropagation::ConsumedRender)
        }
        // ...
    }
}
```

**Benefits:**
- ✅ Clean async → UI communication
- ✅ Background task notifications
- ✅ Modal dialog interactions

**Files to modify:** `state.rs`, `app_main.rs`

**References:**
- edi/app_main.rs:23-30 (AppSignal enum)
- edi/app_main.rs:169-241 (signal handling)

---

## Quick Wins (Low Effort, High Value)

### 1. Fix CDC Polling in Wrong Place
**Problem:** `app_render` has side effects (state.rs:182)
```rust
fn app_render(...) {
    // WRONG: Render should be pure!
    global_data.state.poll_cdc_changes();
}
```

**Solution:** Move to proper location
```rust
fn app_handle_input_event(...) {
    // Poll CDC before handling input
    if global_data.state.poll_cdc_changes() > 0 {
        return Ok(EventPropagation::ConsumedRender);
    }
    // ... handle input
}
```

**References:** edi/app_main.rs:108-165 (pure event handling)

### 2. Remove Debug Logging to Files
**Problem:** Scattered file logging (state.rs:152, 210, 285)
```rust
let _ = std::fs::write("/tmp/cdc-debug.log", ...);  // Debug leftover!
```

**Solution:** Use proper logging
```rust
tracing::debug!("CDC change received: {:?}", change);
```

**References:** edi/app_main.rs uses tracing throughout

### 3. Remove panic! in Default
**Problem:** Misleading Default impl (state.rs:34)
```rust
impl Default for State {
    fn default() -> Self {
        panic!("State::default() should never be called...");
    }
}
```

**Solution:** Remove Default or make it valid
```rust
// Just don't impl Default if it's not needed
```

---

## Migration Strategy

### Phase 1: Foundation (Week 1)
**Goal:** Fix critical performance and enable component architecture

1. **Fix async blocking** (Priority 1)
   - Make event handlers async
   - Remove Runtime::new() workaround
   - Use tokio::spawn for operations

2. **Implement state traits** (Priority 1)
   - Add editor_buffers, dialog_buffers to State
   - Implement HasEditorBuffers
   - Implement HasDialogBuffers

3. **Create stylesheet** (Quick win)
   - Centralize styles in stylesheet.rs
   - Convert inline styles to style IDs

**Validation:** Operations complete without blocking, state traits compile

### Phase 2: Component Migration (Week 2)
**Goal:** Convert to component-based architecture

4. **Create BlockListComponent**
   - Move rendering logic from RenderInterpreter
   - Implement Component trait
   - Handle input events in component

5. **Update app_init**
   - Register components with ComponentRegistry
   - Set up HasFocus management

6. **Convert app_render to use Surface**
   - Replace direct RenderOpIRVec with Surface
   - Use box_start!/box_end! macros
   - Render components declaratively

**Validation:** UI works identically but with component architecture

### Phase 3: Feature Enablement (Week 3+)
**Goal:** Leverage new architecture for features

7. **Add QueryEditorComponent**
   - Integrate r3bl EditorComponent
   - PRQL syntax highlighting
   - Query execution on save

8. **Add modal dialogs**
   - Confirmation dialogs for destructive actions
   - Input dialogs for new blocks

9. **Enhanced signal system**
   - Rich AppSignal enum
   - Proper async operation feedback

**Validation:** New features work with minimal code

---

## Testing Strategy

### Before Refactoring
1. Document current keyboard shortcuts and behavior
2. Create manual test checklist:
   - [ ] Navigate with arrow keys
   - [ ] Toggle completion with Space
   - [ ] Indent/outdent with [ and ]
   - [ ] Move blocks with Ctrl+arrows
   - [ ] CDC updates reflected in UI

### During Refactoring
1. Test after each phase
2. Ensure no regressions in existing features
3. Add integration tests for components

### After Refactoring
1. Verify all manual tests pass
2. Measure performance improvements
3. Verify no UI freezes during operations

---

## Benefits Summary

| Area | Before | After |
|------|--------|-------|
| **Code Volume** | ~1000 lines | ~600 lines (40% reduction) |
| **Components** | 0 (monolithic) | 3+ reusable |
| **Layout** | Manual positioning | Declarative FlexBox |
| **Async** | Blocking workaround | Non-blocking native |
| **Styling** | Scattered inline | Centralized stylesheet |
| **Features** | Hard to add | Compose components |
| **Testability** | Integration only | Unit + integration |
| **Performance** | UI freezes | Responsive |

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Breaking existing features | Medium | High | Phase approach, manual testing |
| Async migration issues | Low | Medium | r3bl supports async natively |
| Component architecture complexity | Low | Low | Follow edi example closely |
| CDC integration breaks | Low | High | Test CDC thoroughly each phase |

---

## References

### Key r3bl Example Files
- `edi/app_main.rs` - Complete component architecture example
- `edi/state.rs` - State trait implementations
- `edi/launcher.rs` - Async runtime setup
- `ch/choose_prompt.rs` - Simple async pattern

### r3bl Documentation
- Component architecture: r3bl-tui docs
- Layout system: FlexBox documentation
- Event handling: App trait documentation

---

## Appendix: Code Comparison

### Architecture Pattern Comparison

**Current (Imperative):**
```rust
fn app_render() {
    let mut ops = RenderOpIRVec::new();
    ops += MoveCursorPositionAbs(row(2) + col(2));
    ops += PaintText("Item 1");
    ops += MoveCursorPositionAbs(row(3) + col(2));
    ops += PaintText("Item 2");
    pipeline.push(ZOrder::Normal, ops);
}
```

**r3bl Way (Declarative):**
```rust
fn app_init(&mut self, registry, focus) {
    let list = BlockListComponent::new_boxed(...);
    ComponentRegistry::put(registry, id, list);
}

fn app_render() {
    box_start!(in: surface, width: 100%, height: 100%);
    render_component_in_current_box!(component_id: BlockList);
    box_end!(in: surface);
}
```

### Event Handling Comparison

**Current (Manual):**
```rust
fn app_handle_input_event(input_event, global_data, ...) {
    match input_event {
        KeyPress::Plain { key: Key::SpecialKey(SpecialKey::Up) } => {
            global_data.state.select_previous();
        }
        // ... 100 lines of event handling
    }
}
```

**r3bl Way (Routed):**
```rust
fn app_handle_input_event(input_event, global_data, registry, focus) {
    // Framework routes to focused component!
    ComponentRegistry::route_event_to_focused_component(
        global_data,
        input_event,
        registry,
        focus,
    )
}

// In BlockListComponent
fn handle_event(&mut self, input_event, global_data) {
    match input_event {
        KeyPress { key: Key::Up } => {
            global_data.state.select_previous();
        }
    }
}
```

---

## Conclusion

The current implementation is fighting against r3bl's design. By adopting the component-based architecture, we:

1. **Reduce complexity** - Let the framework handle layout, focus, and rendering
2. **Improve performance** - Fix blocking async operations
3. **Enable features** - Unlock built-in components (editor, dialogs)
4. **Increase maintainability** - Modular, testable components

The refactoring follows a clear path: **Fix async → Add traits → Create components → Migrate rendering**. Each phase is independently testable and delivers immediate value.

**Next Steps:** Start with Phase 1 (async fixes + state traits) as they're critical and enable everything else.
