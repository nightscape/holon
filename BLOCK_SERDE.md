# Block Serialization: Unified Data Model

This document describes the unified data model for blocks that can be serialized to/from Org Mode, Markdown, and Loro. The goal is to support PRQL query blocks (and other source blocks) while maintaining a single canonical model.

## Design Principles

1. **Single canonical model** - `Block` is the source of truth; Org/Markdown/Loro are serialization formats
2. **Loro is lossless** - round-trip through Loro preserves all data
3. **Org Mode is near-lossless** - Tier 1+2 features fully supported with write-back
4. **Markdown is import-friendly** - good for reading, but Tier 2+ features don't survive export
5. **No special-casing** - PRQL is just `language: "prql"`, visualization config in `header_args`

## Data Model

### Block (Extended)

```rust
// crates/holon-api/src/block.rs

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Block {
    pub id: String,
    pub parent_id: String,

    /// Typed content (replaces plain String)
    pub content: BlockContent,

    /// Key-value properties (Tier 2: works fully in Org + Loro)
    #[serde(default)]
    pub properties: HashMap<String, Value>,

    pub children: Vec<String>,
    pub metadata: BlockMetadata,
}
```

### BlockContent

```rust
/// Content of a block - discriminated union for different content types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum BlockContent {
    /// Plain text content (paragraphs, prose)
    Text { raw: String },

    /// Source code block
    Source(SourceBlock),

    // Future extensions:
    // List { items: Vec<ListItem>, ordered: bool },
    // Table { headers: Vec<String>, rows: Vec<Vec<Value>> },
    // Quote { content: String, attribution: Option<String> },
}

impl Default for BlockContent {
    fn default() -> Self {
        BlockContent::Text { raw: String::new() }
    }
}
```

### SourceBlock

```rust
/// A source code block (language-agnostic)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceBlock {
    /// Language identifier (e.g., "prql", "sql", "python", "rust")
    pub language: String,

    /// The source code itself
    pub source: String,

    /// Optional block name for references (#+NAME: in Org)
    pub name: Option<String>,

    /// Header arguments / parameters
    /// Org: `:var x=1 :results table :connection main`
    /// Loro: native HashMap
    #[serde(default)]
    pub header_args: HashMap<String, Value>,

    /// Cached execution results
    pub results: Option<BlockResult>,
}
```

### BlockResult

```rust
/// Results from executing a source block
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockResult {
    pub output: ResultOutput,
    pub executed_at: i64,  // Unix timestamp ms
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ResultOutput {
    /// Plain text output
    Text { content: String },

    /// Tabular data
    Table {
        headers: Vec<String>,
        rows: Vec<Vec<Value>>
    },

    /// Execution error
    Error { message: String },
}
```

### Helper Methods

```rust
// BlockContent helpers
BlockContent::text("Hello world")
BlockContent::source("prql", "from tasks")
content.as_text()     // -> Option<&str>
content.as_source()   // -> Option<&SourceBlock>
content.to_plain_text() // -> &str (works for both variants)

// SourceBlock builder
SourceBlock::new("prql", "from tasks")
    .with_name("my-query")
    .with_header_arg("connection", Value::String("main".into()))
    .with_results(BlockResult::table(headers, rows))

// BlockResult constructors
BlockResult::text("output")
BlockResult::table(vec!["col1", "col2"], rows)
BlockResult::error("Something went wrong")

// Block constructors
Block::new_text("id", "parent", "Hello")
Block::new_source("id", "parent", "prql", "from tasks")

// NewBlock builders
NewBlock::text("parent_id", "Hello world")
NewBlock::source("parent_id", "prql", "from tasks")
    .after("sibling_id")
    .with_id("custom://id")
```

## Feature Tiers

| Feature | Tier | Org Mode | Markdown | Loro |
|---------|------|----------|----------|------|
| Hierarchical blocks | 1 | Headlines | Headers | Tree |
| Text content | 1 | Section body | Paragraphs | Native |
| Source blocks (lang + code) | 1 | `#+BEGIN_SRC` | ` ```lang ` | Native |
| Block IDs | 1 | `:ID:` property | Generated | Native |
| Named blocks | 2 | `#+NAME:` | - | Native |
| Properties | 2 | `:PROPERTIES:` | Frontmatter only | Native |
| Header arguments | 2 | `:var :results` | - | Native |
| Result caching | 2 | `#+RESULTS:` | - | Native |
| Tags | 2 | `:tag1:tag2:` | - | Native |
| Inline block refs | 2 | `<<name>>` | - | References |
| CRDT history | 3 | - | - | Native |
| Real-time sync | 3 | - | - | Native |

**Tier 1**: Full support in all formats - the minimum viable feature set
**Tier 2**: Full in Org + Loro, lossy/ignored in Markdown
**Tier 3**: Loro-only collaborative features

## Serialization Mappings

### Org Mode

**Parsing:**
```
#+NAME: query-tasks
#+BEGIN_SRC prql :connection main :results table
from tasks
filter completed == false
sort priority desc
#+END_SRC

#+RESULTS: query-tasks
| id | title      | priority |
|----+------------+----------|
| 1  | Fix bug    | 3        |
| 2  | Add tests  | 2        |
```

Maps to:
```rust
SourceBlock {
    language: "prql",
    source: "from tasks\nfilter completed == false\nsort priority desc",
    name: Some("query-tasks"),
    header_args: {
        "connection": Value::String("main"),
        "results": Value::String("table"),
    },
    results: Some(BlockResult {
        output: ResultOutput::Table {
            headers: vec!["id", "title", "priority"],
            rows: vec![
                vec![Value::Integer(1), Value::String("Fix bug"), Value::Integer(3)],
                vec![Value::Integer(2), Value::String("Add tests"), Value::Integer(2)],
            ],
        },
        executed_at: 1733350000000,
    }),
}
```

**Writing:**
- `SourceBlock` → `#+BEGIN_SRC {language} {header_args}\n{source}\n#+END_SRC`
- `name` → `#+NAME: {name}` before the block
- `results` → `#+RESULTS: {name}\n{formatted_output}` after the block
- `properties` → `:PROPERTIES:` drawer on parent headline

### Markdown

**Parsing:**
~~~markdown
```prql
from tasks
filter completed == false
sort priority desc
```
~~~

Maps to:
```rust
SourceBlock {
    language: "prql",
    source: "from tasks\nfilter completed == false\nsort priority desc",
    name: None,           // Lost
    header_args: {},      // Lost
    results: None,        // Lost
}
```

**Writing:**
- `SourceBlock` → ` ```{language}\n{source}\n``` `
- `name`, `header_args`, `results` are **not written** (lossy)
- `properties` → YAML frontmatter for document-level only

### Loro

Native storage - all fields preserved exactly:
```rust
// LoroMap structure for a text block
{
    "content_type": "text",
    "content_raw": "Hello world",
    "parent_id": "local://parent",
    "created_at": 1733350000000,
    "updated_at": 1733350000000,
    "deleted_at": null,
    "properties": "{...}",  // JSON-serialized HashMap
}

// LoroMap structure for a source block
{
    "content_type": "source",
    "source_language": "prql",
    "source_code": "from tasks\nfilter completed == false",
    "source_name": "query-tasks",
    "source_header_args": "{\"connection\":\"main\",...}",  // JSON-serialized
    "source_results": "{\"output\":{...},\"executed_at\":...}",  // JSON-serialized
    "parent_id": "local://parent",
    "created_at": 1733350000000,
    "updated_at": 1733350000000,
    "deleted_at": null,
    "properties": "{}",
}
```

**Backward Compatibility**: Blocks without `content_type` are read as `Text` with the old `content` field.

## Usage Examples

### PRQL Query Block for Dashboard

```rust
// Using builder pattern
let source_block = SourceBlock::new("prql", r#"
from tasks
filter due_date == @today && !completed
sort priority desc
take 10
"#)
    .with_name("today-tasks")
    .with_header_arg("visualization", Value::String("list".into()))
    .with_header_arg("render", Value::String(
        "hover_row(row(checkbox(checked: completed), flexible(text(content))))".into()
    ));

Block {
    id: "local://550e8400-e29b-41d4-a716-446655440000".into(),
    parent_id: "local://page-orient".into(),
    content: BlockContent::Source(source_block),
    properties: HashMap::new(),
    children: vec![],
    metadata: BlockMetadata::default(),
}

// Or using the Block constructor
let block = Block::new_source(
    "local://550e8400-e29b-41d4-a716-446655440000",
    "local://page-orient",
    "prql",
    "from tasks | filter due_date == @today"
);
```

### Multiple Query Blocks on One Page

```rust
// Parent page block
let page = Block::new_text("local://page-orient", ROOT_PARENT_ID, "Orient Dashboard");
// Set children after creating child blocks

// Each section is a source block with its own PRQL query
let today_section = Block::new_source(
    "local://section-today",
    "local://page-orient",
    "prql",
    "from tasks | filter due_date == @today | sort priority desc"
);

let inbox_section = Block::new_source(
    "local://section-inbox",
    "local://page-orient",
    "prql",
    "from blocks | filter inbox == true | sort created_at desc"
);

let calendar_section = Block::new_source(
    "local://section-calendar",
    "local://page-orient",
    "prql",
    "from events | filter date >= @today | take 7"
);
```

### Org Mode Round-Trip

Input `.org` file:
```org
* Orient Dashboard
:PROPERTIES:
:ID: page-orient
:END:

** Today's Tasks
:PROPERTIES:
:ID: section-today
:END:

#+NAME: today-tasks
#+BEGIN_SRC prql :visualization list
from tasks
filter due_date == @today
#+END_SRC
```

Parsed → Modified → Written back preserves all structure.

## Migration Strategy

### Existing `Block.content: String`

The current model stores content as a plain `String`. Migration:

```rust
// Old
Block { content: "Some text content".to_string(), ... }

// New (equivalent)
Block {
    content: BlockContent::Text { raw: "Some text content".to_string() },
    ...
}
```

For database migration:
1. Add `content_type` column (default: `"text"`)
2. Rename `content` → `content_raw`
3. Add nullable `source_*` columns for source block fields
4. Application layer handles the union

---

## Implementation Status

**Summary**: Phases 1-4 are complete. The unified `BlockContent` model is fully implemented in `holon-api`, both backends (Loro and Memory), the Org Mode parser extracts source blocks with full metadata, and the Org Mode writer can serialize source blocks back to `#+BEGIN_SRC ... #+END_SRC` format with names, header arguments, and results. Remaining work includes Markdown support, Flutter integration, and query execution pipeline.

### Phase 1: Core Model Extension ✅ COMPLETE
Location: `crates/holon-api/src/block.rs`

- [x] Add `BlockContent` enum with `Text` and `Source` variants
- [x] Add `SourceBlock` struct with language, source, name, header_args, results
- [x] Add `BlockResult` and `ResultOutput` types (Text, Table, Error)
- [x] Add `properties: HashMap<String, Value>` to `Block`
- [x] Update `Block` to use `content: BlockContent`
- [x] Add helper methods: `BlockContent::text()`, `BlockContent::source()`, etc.
- [x] Add builder pattern for `SourceBlock` and `NewBlock`
- [x] Export all new types from `holon_api`
- [x] Add `Display` impl for `BlockContent`

### Phase 2: Backend Updates ✅ COMPLETE
Location: `crates/holon/src/api/`

**Loro Backend** (`loro_backend.rs`):
- [x] Add `write_content_to_map()` and `read_content_from_map()` helpers
- [x] Store `content_type`, `content_raw`, `source_*` fields
- [x] Backward compatibility: old `content` string → `BlockContent::Text`
- [x] Update all CRUD operations: `create_block`, `update_block`, `get_block`, etc.
- [x] Update `initialize_schema()` for new format
- [x] Update change notification payloads

**Memory Backend** (`memory_backend.rs`):
- [x] Update `MemoryBlock` to store `content: BlockContent` and `properties`
- [x] Update all CRUD operations
- [x] Update change notification payloads

**Repository Trait** (`repository.rs`):
- [x] Update `create_block` signature: `content: String` → `content: BlockContent`
- [x] Update `update_block` signature: `content: String` → `content: BlockContent`
- [x] Update `update_block_by_ref` convenience method

**Types** (`types.rs`):
- [x] Update `NewBlock` to use `content: BlockContent`
- [x] Add `NewBlock::text()` and `NewBlock::source()` constructors

**PBT Infrastructure** (`pbt_infrastructure.rs`):
- [x] Update `apply_transition` to wrap String content in `BlockContent::text()`
- [x] Update block comparisons to use `content_text()` method

### Phase 3: Org Mode Source Block Parsing ✅ COMPLETE
Location: `crates/holon-orgmode/src/`

- [x] Add `OrgSourceBlock` struct to models with language, source, name, header_args, byte positions
- [x] Add `ParsedSectionContent` struct for structured section content
- [x] Parse `#+BEGIN_SRC ... #+END_SRC` within sections using orgize's `SourceBlock` AST
- [x] Extract `#+NAME:` before source blocks via `find_block_name()` function
- [x] Parse header arguments (`:var :results :connection` etc.) via `parse_header_args()`
- [x] Handle mixed content (text + source blocks in one section) - text preserved separately
- [x] Add `source_blocks: Option<String>` field to `OrgHeadline` (JSON-serialized Vec<OrgSourceBlock>)
- [x] Add helper methods: `get_source_blocks()`, `set_source_blocks()`, `prql_blocks()`, `has_source_blocks()`
- [x] Add `to_api_source_block()` conversion method
- [x] Add comprehensive tests for source block parsing
- [ ] Parse `#+RESULTS:` after source blocks (future work)

### Phase 4: Org Mode Source Block Writing ✅ COMPLETE
Location: `crates/holon-orgmode/src/writer.rs`

- [x] Write `SourceBlock` → `#+BEGIN_SRC ... #+END_SRC`
- [x] Write `name` → `#+NAME:` line
- [x] Write `header_args` → inline arguments
- [x] Write `results` → `#+RESULTS:` block (text, table, error formats)
- [x] Test round-trip: parse → modify → write → parse

**Functions implemented:**
- `format_org_source_block(block: &OrgSourceBlock) -> String` - formats OrgSourceBlock to Org Mode syntax
- `format_api_source_block(block: &SourceBlock) -> String` - formats holon_api::SourceBlock with results
- `format_header_args(args: &HashMap<String, String>) -> String` - formats header args as `:key value`
- `format_header_args_from_values(args: &HashMap<String, Value>) -> String` - handles Value types
- `format_block_result(result: &BlockResult, name: Option<&str>) -> String` - formats #+RESULTS: block
- `insert_source_block()` / `insert_api_source_block()` - insert at position
- `update_source_block()` / `update_api_source_block()` - replace by byte range
- `delete_source_block()` - delete by byte range (also removes #+NAME: prefix)
- `value_to_header_arg_string(value: &Value) -> String` - converts Value to Org Mode string

### Phase 5: Markdown Parser (New Crate)
Location: `crates/holon-markdown/` (new)

- [ ] Create new crate with `pulldown-cmark` dependency
- [ ] Parse headers → hierarchical blocks
- [ ] Parse fenced code blocks → `SourceBlock`
- [ ] Parse paragraphs → `Text` content
- [ ] Handle frontmatter (optional, for document properties)
- [ ] Implement `MarkdownSerde` trait

### Phase 6: Markdown Writer
Location: `crates/holon-markdown/src/writer.rs`

- [ ] Write `Text` → paragraphs
- [ ] Write `SourceBlock` → fenced code blocks
- [ ] Write hierarchy → headers (with configurable max depth)
- [ ] Document lossy behavior (Tier 2 features not written)

### Phase 7: Flutter Integration
Location: `frontends/flutter/`

- [ ] Update Dart `Block` model for `BlockContent`
- [ ] Update `RenderInterpreter` to handle `Source` blocks
- [ ] Add PRQL source block editor widget
- [ ] Add syntax highlighting for PRQL (if feasible)
- [ ] Add result display for executed queries

### Phase 8: Query Execution Pipeline
Location: `crates/holon/src/core/`

- [ ] Add `execute_source_block()` function
- [ ] Route PRQL blocks through existing transform pipeline
- [ ] Cache results in `BlockResult`
- [ ] Add CDC streaming for result updates
- [ ] Handle execution errors gracefully

---

## Open Questions

1. **Mixed content sections**: ✅ RESOLVED - Org Mode sections can contain mixed text and source blocks. The `OrgHeadline` stores:
   - `content: Option<String>` - plain text portions (concatenated)
   - `source_blocks: Option<String>` - JSON-serialized `Vec<OrgSourceBlock>` with byte positions

   For the unified `Block` model, each source block could become a child `Block` with `BlockContent::Source`, while text becomes `BlockContent::Text`. This maps well to Loro's tree structure.

2. **Result invalidation**: When should cached results be cleared? On source change? On dependency change? Manual only?
   - Suggestion: Clear on source change, keep `executed_at` timestamp for staleness detection

3. **Block references**: How do `<<name>>` references in Org Mode map to the unified model?
   - `OrgSourceBlock.name` captures `#+NAME:` directives
   - Resolution at render time via name lookup in `header_args` or separate index

4. **Visualization config**: ✅ RESOLVED - Stays in `header_args` for flexibility. PRQL blocks use:
   - `header_args["visualization"]` = "list", "table", etc.
   - `header_args["render"]` = render DSL string
   - `header_args["connection"]` = database connection name

5. **Markdown extensions**: Should we support any Markdown extensions for Tier 2 features?
   - Suggestion: Start with basic fenced code blocks only (Tier 1)
   - Consider YAML frontmatter for document-level properties
   - HTML comments or custom syntax are fragile; accept lossy export to Markdown
