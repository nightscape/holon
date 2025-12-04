# Outliner-Flutter Repository Interface Reference

## Complete Interface You Must Implement

The `OutlinerRepository` abstract class defines 15 methods you must implement to connect outliner-flutter to your Rust backend.

### Reading Operations

#### getRootBlocks()
**Returns**: `Future<List<Block>>`
**Purpose**: Load all root-level blocks (top level of hierarchy)

```dart
@override
Future<List<Block>> getRootBlocks() async {
  // Load from backend, return flat list of root blocks
  // Child relationships are captured in Block.children
  final response = await api.get('/blocks?parent=null');
  return response.map((json) => Block.fromJson(json)).toList();
}
```

#### findBlockById(String blockId)
**Returns**: `Future<Block?>`
**Purpose**: Find any block by its ID (including nested blocks)

```dart
@override
Future<Block?> findBlockById(String blockId) async {
  // Return the block with given ID, or null if not found
  // Must traverse entire tree to find it
  final response = await api.get('/blocks/$blockId');
  if (response.statusCode == 200) {
    return Block.fromJson(response.body);
  }
  return null;
}
```

#### findParentId(String blockId)
**Returns**: `Future<String?>`
**Purpose**: Get the parent block ID of a block (or null if root)

```dart
@override
Future<String?> findParentId(String blockId) async {
  // Query backend: "what block is the parent of blockId?"
  final response = await api.get('/blocks/$blockId/parent');
  if (response.statusCode == 200) {
    return response.body['parent_id'];
  }
  return null;
}
```

#### findBlockIndex(String blockId)
**Returns**: `Future<int>`
**Purpose**: Get the position index of a block among its siblings

```dart
@override
Future<int> findBlockIndex(String blockId) async {
  // Query: "what position is this block in its parent's children?"
  // Return -1 if not found
  final response = await api.get('/blocks/$blockId/index');
  if (response.statusCode == 200) {
    return response.body['index'] as int;
  }
  return -1;
}
```

#### getTotalBlocks()
**Returns**: `Future<int>`
**Purpose**: Count total blocks (including all nested)

```dart
@override
Future<int> getTotalBlocks() async {
  // Can be optimized with a `/stats` endpoint if available
  final response = await api.get('/blocks/count');
  return response.body['total'] as int;
}
```

---

### Write Operations (Root Level)

#### addRootBlock(Block block)
**Parameters**: `Block block`
**Returns**: `Future<void>`
**Purpose**: Add a new block at root level

```dart
@override
Future<void> addRootBlock(Block block) async {
  // Insert block as root (parent_id = null)
  await api.post('/blocks', body: block.toJson());
}
```

#### insertRootBlock(int index, Block block)
**Parameters**: `int index`, `Block block`
**Returns**: `Future<void>`
**Purpose**: Insert block at specific position among root blocks

```dart
@override
Future<void> insertRootBlock(int index, Block block) async {
  // Insert at specific position, shifting others down
  await api.post('/blocks', body: {
    ...block.toJson(),
    'index': index,
    'parent_id': null,
  });
}
```

#### removeRootBlock(Block block)
**Parameters**: `Block block`
**Returns**: `Future<void>`
**Purpose**: Remove a block from root level

```dart
@override
Future<void> removeRootBlock(Block block) async {
  // Delete the block (cascades to children)
  await api.delete('/blocks/${block.id}');
}
```

---

### Write Operations (Any Block)

#### updateBlock(String blockId, String content)
**Parameters**: `String blockId`, `String content`
**Returns**: `Future<void>`
**Purpose**: Update the text content of a block

```dart
@override
Future<void> updateBlock(String blockId, String content) async {
  // Update block content and updatedAt timestamp
  await api.patch('/blocks/$blockId', body: {
    'content': content,
    'updated_at': DateTime.now().toIso8601String(),
  });
}
```

#### toggleBlockCollapse(String blockId)
**Parameters**: `String blockId`
**Returns**: `Future<void>`
**Purpose**: Toggle the collapsed state of a block

```dart
@override
Future<void> toggleBlockCollapse(String blockId) async {
  // Toggle is_collapsed field
  final block = await findBlockById(blockId);
  if (block != null) {
    await api.patch('/blocks/$blockId', body: {
      'is_collapsed': !block.isCollapsed,
    });
  }
}
```

#### removeBlock(String blockId)
**Parameters**: `String blockId`
**Returns**: `Future<void>`
**Purpose**: Remove any block (including all children)

```dart
@override
Future<void> removeBlock(String blockId) async {
  // Delete block and all descendants
  await api.delete('/blocks/$blockId');
}
```

---

### Write Operations (Hierarchy)

#### addChildBlock(String parentId, Block child)
**Parameters**: `String parentId`, `Block child`
**Returns**: `Future<void>`
**Purpose**: Add a block as child of another block

```dart
@override
Future<void> addChildBlock(String parentId, Block child) async {
  // Insert as child at end of parent's children list
  await api.post('/blocks', body: {
    ...child.toJson(),
    'parent_id': parentId,
  });
}
```

#### moveBlock(String blockId, String? newParentId, int newIndex)
**Parameters**: `String blockId`, `String? newParentId`, `int newIndex`
**Returns**: `Future<void>`
**Purpose**: Move block to new parent and/or position

```dart
@override
Future<void> moveBlock(
  String blockId,
  String? newParentId,
  int newIndex,
) async {
  // Move block to new parent and position
  // newParentId = null means move to root level
  // Important: Prevent moving block into its own descendants

  if (newParentId != null && await _isDescendantOf(newParentId, blockId)) {
    return; // Prevent circular relationships
  }

  await api.patch('/blocks/$blockId', body: {
    'parent_id': newParentId,
    'index': newIndex,
  });
}

Future<bool> _isDescendantOf(String ancestorId, String descendantId) async {
  // Check if descendantId is in the tree under ancestorId
  final block = await findBlockById(ancestorId);
  if (block == null) return false;
  return _checkDescendant(block, descendantId);
}

bool _checkDescendant(Block block, String descendantId) {
  if (block.id == descendantId) return true;
  for (var child in block.children) {
    if (_checkDescendant(child, descendantId)) return true;
  }
  return false;
}
```

#### indentBlock(String blockId)
**Parameters**: `String blockId`
**Returns**: `Future<void>`
**Purpose**: Increase nesting (make previous sibling the parent)

```dart
@override
Future<void> indentBlock(String blockId) async {
  // Get current parent and index
  final parentId = await findParentId(blockId);
  final currentIndex = await findBlockIndex(blockId);

  if (currentIndex <= 0) return; // Cannot indent first child

  List<Block> siblings;
  if (parentId == null) {
    siblings = await getRootBlocks();
  } else {
    final parent = await findBlockById(parentId);
    siblings = parent?.children ?? [];
  }

  if (currentIndex <= 0) return;

  // Move to end of previous sibling's children
  final previousSibling = siblings[currentIndex - 1];
  await moveBlock(blockId, previousSibling.id, previousSibling.children.length);
}
```

#### outdentBlock(String blockId)
**Parameters**: `String blockId`
**Returns**: `Future<void>`
**Purpose**: Decrease nesting (move to parent's parent)

```dart
@override
Future<void> outdentBlock(String blockId) async {
  final parentId = await findParentId(blockId);
  if (parentId == null) return; // Already at root

  final grandparentId = await findParentId(parentId);
  final parentIndex = await findBlockIndex(parentId);

  if (grandparentId == null) {
    // Parent is root, move to root after parent
    await moveBlock(blockId, null, parentIndex + 1);
  } else {
    // Move to grandparent's children after parent
    await moveBlock(blockId, grandparentId, parentIndex + 1);
  }
}
```

---

### Complex Operations

#### splitBlock(String blockId, int cursorPosition)
**Parameters**: `String blockId`, `int cursorPosition`
**Returns**: `Future<void>`
**Purpose**: Split a block at cursor position (text before/after)

```dart
@override
Future<void> splitBlock(String blockId, int cursorPosition) async {
  // 1. Get the block
  final block = await findBlockById(blockId);
  if (block == null) return;

  // 2. Split the content
  final content = block.content;
  final safePosition = cursorPosition.clamp(0, content.length);
  final beforeCursor = content.substring(0, safePosition);
  final afterCursor = content.substring(safePosition);

  // 3. Update original block
  await updateBlock(blockId, beforeCursor);

  // 4. Create new sibling block after this one
  final newBlock = Block.create(content: afterCursor);

  // 5. Insert new block as sibling (after current)
  final parentId = await findParentId(blockId);
  final currentIndex = await findBlockIndex(blockId);

  if (parentId == null) {
    // Current block is root
    final roots = await getRootBlocks();
    await insertRootBlock(currentIndex + 1, newBlock);
  } else {
    // Current block has parent, insert as sibling
    final parent = await findBlockById(parentId);
    final siblingIndex = parent?.children.indexWhere((c) => c.id == blockId) ?? -1;
    if (siblingIndex != -1) {
      await api.post('/blocks', body: {
        ...newBlock.toJson(),
        'parent_id': parentId,
        'index': siblingIndex + 1,
      });
    }
  }
}
```

---

## Data Model: Block

Every operation works with `Block` objects:

```dart
@freezed
class Block with _$Block {
  const factory Block({
    required String id,                  // UUID v4 identifier
    required String content,             // Text content
    @Default([]) List<Block> children,   // Child blocks (nested)
    @Default(false) bool isCollapsed,    // UI state
    required DateTime createdAt,
    required DateTime updatedAt,
  }) = _Block;

  factory Block.create({
    String? id,                    // Generate UUID if null
    required String content,
    List<Block>? children,
    bool? isCollapsed,
  })

  factory Block.fromJson(Map<String, dynamic> json) => _$BlockFromJson(json);

  Map<String, dynamic> toJson() => _$BlockToJson(this);

  bool get hasChildren => children.isNotEmpty;
  int get totalBlocks => 1 + children.fold(0, (sum, child) => sum + child.totalBlocks);
  Block? findBlockById(String blockId) => /* recursive search */;
}
```

**JSON Serialization Example**:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "content": "Hello world",
  "children": [],
  "is_collapsed": false,
  "created_at": "2025-10-25T20:00:00Z",
  "updated_at": "2025-10-25T20:01:00Z"
}
```

---

## Implementation Strategy

### Option 1: REST API Backend
If your Rust backend has REST endpoints:

```dart
class RustyOutlinerRepository implements OutlinerRepository {
  final String baseUrl;
  final http.Client client;

  RustyOutlinerRepository({
    required this.baseUrl,
    http.Client? client,
  }) : client = client ?? http.Client();

  @override
  Future<List<Block>> getRootBlocks() async {
    final response = await client.get(
      Uri.parse('$baseUrl/api/blocks?parent=null'),
    );
    if (response.statusCode == 200) {
      final json = jsonDecode(response.body) as List;
      return json.map((j) => Block.fromJson(j as Map<String, dynamic>)).toList();
    }
    throw Exception('Failed to load blocks');
  }

  // ... implement other methods similarly
}
```

### Option 2: gRPC Backend
If using gRPC:

```dart
class RustyOutlinerRepository implements OutlinerRepository {
  late final BlockService.Client _client;

  RustyOutlinerRepository({required String host, required int port}) {
    final channel = GrpcWebClientChannel.xhr(
      Uri.parse('http://$host:$port'),
    );
    _client = BlockService.Client(channel);
  }

  @override
  Future<List<Block>> getRootBlocks() async {
    final request = GetRootBlocksRequest();
    final response = await _client.getRootBlocks(request);
    return response.blocks
        .map((pb) => _pbToBlock(pb))
        .toList();
  }

  // ... implement other methods
}
```

### Option 3: Native Platform Channel
If calling native Rust directly:

```dart
const platform = MethodChannel('space.holon/outliner');

class RustyOutlinerRepository implements OutlinerRepository {
  @override
  Future<List<Block>> getRootBlocks() async {
    final List<dynamic> result =
        await platform.invokeMethod('getRootBlocks');
    return result
        .map((json) => Block.fromJson(Map<String, dynamic>.from(json)))
        .toList();
  }

  // ... implement other methods
}
```

---

## Testing Your Repository

```dart
void main() {
  group('RustyOutlinerRepository', () {
    late RustyOutlinerRepository repo;

    setUp(() {
      repo = RustyOutlinerRepository(baseUrl: 'http://localhost:3000');
    });

    test('getRootBlocks returns list of blocks', () async {
      final blocks = await repo.getRootBlocks();
      expect(blocks, isA<List<Block>>());
    });

    test('addRootBlock persists block', () async {
      final block = Block.create(content: 'Test');
      await repo.addRootBlock(block);

      final blocks = await repo.getRootBlocks();
      expect(blocks.any((b) => b.id == block.id), true);
    });

    test('updateBlock changes content', () async {
      final block = Block.create(content: 'Original');
      await repo.addRootBlock(block);

      await repo.updateBlock(block.id, 'Updated');

      final updated = await repo.findBlockById(block.id);
      expect(updated?.content, 'Updated');
    });

    test('moveBlock prevents circular relationships', () async {
      final parent = Block.create(content: 'Parent');
      final child = Block.create(content: 'Child');

      await repo.addRootBlock(parent);
      await repo.addChildBlock(parent.id, child);

      // Should not allow moving parent into child
      await repo.moveBlock(parent.id, child.id, 0);

      final parentParent = await repo.findParentId(parent.id);
      expect(parentParent, isNull);
    });
  });
}
```

---

## Summary

| Method | You Must Handle |
|--------|-----------------|
| getRootBlocks() | Load root blocks from backend |
| findBlockById() | Find any block by ID |
| findParentId() | Get parent ID or null |
| findBlockIndex() | Get position among siblings |
| getTotalBlocks() | Count all blocks |
| addRootBlock() | Insert at root |
| insertRootBlock() | Insert at specific position |
| removeRootBlock() | Delete from root |
| updateBlock() | Change content |
| toggleBlockCollapse() | Toggle UI state |
| removeBlock() | Delete any block |
| addChildBlock() | Add as child |
| moveBlock() | Reposition in hierarchy |
| indentBlock() | Increase nesting |
| outdentBlock() | Decrease nesting |
| splitBlock() | Split at cursor |

**Total**: 15 methods, all straightforward CRUD-like operations on a tree structure.

The most complex are `moveBlock`, `indentBlock`, `outdentBlock`, and `splitBlock` - but the reference implementations above show the logic patterns needed.
