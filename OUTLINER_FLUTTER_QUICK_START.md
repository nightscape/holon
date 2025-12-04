# Outliner-Flutter Quick Start Guide

## What You Have

- **Mature, production-ready library** at `/Users/martin/Workspaces/pkm/outliner-flutter`
- **Version**: 0.1.0 (stable API, suitable for use)
- **License**: MIT
- **Main widget**: `OutlinerListView` - renders hierarchical outline UI
- **State management**: Riverpod + Freezed (immutable, type-safe)
- **Persistence**: Via `OutlinerRepository` interface (bring your own backend)

## 5-Minute Integration Path

### 1. Add to pubspec.yaml
```yaml
dependencies:
  outliner_view: ^0.1.0
  flutter_riverpod: ^2.6.1
  hooks_riverpod: ^2.6.1
  flutter_hooks: ^0.20.5
```

### 2. Wrap app with ProviderScope
```dart
void main() {
  runApp(
    ProviderScope(
      child: MaterialApp(home: MyOutlinerScreen()),
    ),
  );
}
```

### 3. Use OutlinerListView
```dart
class MyOutlinerScreen extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text('My Outline')),
      body: OutlinerListView(),
    );
  }
}
```

That's it for basic usage. The library provides default in-memory storage and Material-ish UI.

## Custom Repository (Required for Rust Backend)

To connect to your Rust backend:

```dart
// 1. Implement the interface
class RustyOutlinerRepository implements OutlinerRepository {
  @override
  Future<List<Block>> getRootBlocks() async {
    // Fetch from Rust backend
    final response = await http.get('/api/blocks');
    return parseBlocks(response);
  }

  @override
  Future<void> addRootBlock(Block block) async {
    // Send to Rust backend
    await http.post('/api/blocks', body: block.toJson());
  }

  // ... implement other methods
}

// 2. Override provider
final outlinerRepositoryProvider = Provider<OutlinerRepository>((ref) {
  return RustyOutlinerRepository();
});
```

## Block Model

```dart
Block(
  id: 'uuid',              // Auto-generated
  content: 'Text',         // The block content
  children: [...],         // Nested blocks
  isCollapsed: false,       // Collapse state
  createdAt: DateTime,
  updatedAt: DateTime,
)
```

Create easily: `Block.create(content: 'Hello')`

## Repository Interface (What You Must Implement)

```dart
abstract class OutlinerRepository {
  Future<List<Block>> getRootBlocks();
  Future<Block?> findBlockById(String blockId);
  Future<String?> findParentId(String blockId);
  Future<int> findBlockIndex(String blockId);
  Future<int> getTotalBlocks();
  
  Future<void> addRootBlock(Block block);
  Future<void> insertRootBlock(int index, Block block);
  Future<void> removeRootBlock(Block block);
  Future<void> updateBlock(String blockId, String content);
  Future<void> toggleBlockCollapse(String blockId);
  Future<void> addChildBlock(String parentId, Block child);
  Future<void> removeBlock(String blockId);
  Future<void> moveBlock(String blockId, String? newParentId, int newIndex);
  Future<void> indentBlock(String blockId);
  Future<void> outdentBlock(String blockId);
  Future<void> splitBlock(String blockId, int cursorPosition);
}
```

That's 15 methods. Each is straightforward - manage blocks in whatever way your backend needs.

## Key Builder Callbacks (Customize UI)

```dart
OutlinerListView(
  // Display mode - how blocks appear
  blockBuilder: (context, block) => Text(block.content),

  // Edit mode - custom editor
  editingBlockBuilder: (context, block, controller, focusNode, onSubmitted) {
    return TextField(controller: controller, focusNode: focusNode, ...);
  },

  // Collapse indicator (bullet point)
  bulletBuilder: (context, block, hasChildren, isCollapsed, onToggle) {
    return hasChildren
      ? Icon(isCollapsed ? Icons.chevron_right : Icons.arrow_drop_down)
      : Icon(Icons.circle, size: 6);
  },

  // Loading/Error/Empty states
  loadingBuilder: (context) => CircularProgressIndicator(),
  errorBuilder: (context, msg, retry) => ErrorWidget(...),
  emptyBuilder: (context, onAdd) => EmptyWidget(...),
)
```

## State Management (Perform Operations)

```dart
Consumer(
  builder: (context, ref, child) {
    final notifier = ref.read(outlinerProvider.notifier);
    
    return Column(
      children: [
        ElevatedButton(
          onPressed: () => notifier.addRootBlock(Block.create(content: '')),
          child: Text('Add Block'),
        ),
        ElevatedButton(
          onPressed: () => notifier.indentFocusedBlock(),
          child: Text('Indent'),
        ),
        Expanded(child: OutlinerListView()),
      ],
    );
  },
)
```

## Configuration

```dart
OutlinerListView(
  config: OutlinerConfig(
    keyboardShortcutsEnabled: true,  // Tab/Shift+Tab on desktop
    blockStyle: BlockStyle(
      textStyle: TextStyle(fontSize: 16),
      indentWidth: 24.0,
      bulletColor: Colors.blue,
    ),
    padding: EdgeInsets.all(16),
  ),
)
```

## What The Library Does

- Renders hierarchical block list with proper indentation
- Handles inline editing (click to edit, Enter/Escape to save/cancel)
- Drag-and-drop reordering (into siblings or as children)
- Expand/collapse sections with children
- Focus tracking for keyboard navigation
- Keyboard shortcuts (Tab, Shift+Tab, Enter)
- Theme/platform agnostic - you control all UI

## What You Must Provide

1. **Repository implementation** - connect to your Rust backend
2. **Custom builders** (optional) - if you want rich text, markdown, etc.
3. **Theme colors** - via BlockStyle config
4. **Keyboard handling** - disable on mobile if needed

## Performance Notes

- All blocks loaded at startup (suitable for KB to MB datasets)
- After each operation, library reloads all blocks
- For huge datasets, implement pagination in your repository
- Riverpod efficiently rebuilds only affected widgets

## Key Files in outliner-flutter

| File | Purpose |
|------|---------|
| `lib/models/block.dart` | Block data model (Freezed) |
| `lib/repositories/outliner_repository.dart` | Interface you implement |
| `lib/providers/outliner_provider.dart` | State management + operations |
| `lib/widgets/outliner_list_view.dart` | Main widget |
| `lib/config/block_style.dart` | Styling config |
| `example/lib/screens/demo_screen.dart` | Reference implementation |

## Example Location

Full working example at `/Users/martin/Workspaces/pkm/outliner-flutter/example/`

Run it:
```bash
cd /Users/martin/Workspaces/pkm/outliner-flutter/example
flutter run
```

## Gotchas

1. **Both hooks_riverpod and flutter_hooks required** - hooks_riverpod doesn't re-export hooks in v2.6.1
2. **Every operation reloads all blocks** - this is intentional, simple design. Optimize in custom repo if needed.
3. **No circular parent-child** - library checks and prevents, but your repo should too
4. **Keyboard shortcuts only on desktop/web** - disable on mobile and use custom UI buttons
5. **Immutable models** - all state changes via `copyWith()` and new instances (Freezed handles this)

## Comparison: outliner-flutter vs Roll Your Own

| Feature | outliner-flutter | Build It |
|---------|------------------|----------|
| Drag-and-drop | ✓ (3-zone system) | Hours of work |
| Expand/collapse | ✓ | 1 hour |
| Inline editing | ✓ (focus mgmt included) | 2+ hours |
| State management | ✓ (Riverpod setup) | Architecture time |
| Keyboard shortcuts | ✓ (Tab, Shift+Tab, Enter) | 1+ hour |
| UI customization | ✓ (builder callbacks) | Depends on scope |
| Testing | ✓ (property-based) | 2+ hours per feature |

**Bottom line**: Using outliner-flutter saves you 20-40 hours of development and testing.

## Next Steps

1. Read full research document: `OUTLINER_FLUTTER_RESEARCH.md`
2. Create your `RustyOutlinerRepository` implementation
3. Test with `InMemoryOutlinerRepository` first
4. Swap in your custom repo
5. Integrate into Rusty Knowledge app UI

## Documentation

- **Research**: `/Users/martin/Workspaces/pkm/holon/OUTLINER_FLUTTER_RESEARCH.md` (comprehensive)
- **README**: `/Users/martin/Workspaces/pkm/outliner-flutter/README.md` (official)
- **Example**: `/Users/martin/Workspaces/pkm/outliner-flutter/example/lib/screens/demo_screen.dart` (reference)
- **API**: In source files (well documented with doc comments)

