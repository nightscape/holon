# Cucumber BDD Tests for Rusty Knowledge

This directory contains behavior-driven development (BDD) tests using Cucumber for testing the Rusty Knowledge backend business logic and Tauri commands.

## Overview

These tests focus on **backend logic testing** rather than UI automation. They test your Tauri commands and business logic directly by calling Rust functions, providing fast, reliable tests that work on all platforms including macOS.

## Structure

```
tests/
├── cucumber.rs              # Main test runner and AppWorld definition
├── steps/                   # Step definitions
│   ├── mod.rs
│   └── task_steps.rs       # Task management step definitions
└── features/                # Gherkin feature files
    └── task_management.feature
```

## Why Backend-Focused Testing?

**The Challenge:** Tauri on macOS uses WKWebView, which doesn't support WebDriver automation tools (Selenium, WebDriverIO, Playwright with CDP). This means traditional UI automation doesn't work on macOS.

**The Solution:** Test the Tauri commands and business logic directly! Since Tauri commands are just Rust functions with the `#[tauri::command]` attribute, we can:
- Call them directly in tests without the UI
- Test all business logic comprehensively
- Get fast, reliable test execution
- Work on any platform (macOS, Linux, Windows)

## Running Tests

### Run all Cucumber tests:
```bash
cargo test --test cucumber
```

### Run with verbose output:
```bash
cargo test --test cucumber -- --verbose
```

### Run in watch mode (requires cargo-watch):
```bash
cargo watch -x 'test --test cucumber'
```

## What Gets Tested

The current test suite covers:
- ✅ Task retrieval (`get_tasks` command)
- ✅ Task creation (`add_task` command)
- ✅ Child task creation
- ✅ Task completion toggling (`toggle_task` command)
- ✅ Task updates (`update_task` command)
- ✅ Task deletion (`delete_task` command)
- ✅ Task moving/reorganization (`move_task` command)

## Writing New Tests

### 1. Create a Feature File

Add a new `.feature` file in `tests/features/`:

```gherkin
Feature: Document Management
  As a user
  I want to create and manage documents
  So that I can organize my knowledge

  Scenario: Create a new document
    Given I have an empty document store
    When I create a document with title "My First Note"
    Then the document should be created
    And the document should have title "My First Note"

  Scenario: Update document content
    Given I have a document with title "Test Doc"
    When I update the document content to "Hello World"
    Then the document content should be "Hello World"
```

### 2. Implement Step Definitions

Create or update files in `tests/steps/`:

```rust
use cucumber::{given, when, then};
use crate::AppWorld;

#[given("I have an empty document store")]
async fn empty_document_store(world: &mut AppWorld) {
    world.document_store = DocumentStore::new();
}

#[when(regex = r#"^I create a document with title "(.+)"$"#)]
async fn create_document(world: &mut AppWorld, title: String) {
    // Call your business logic function directly!
    let doc = world.document_store.create_document(title);
    world.last_document = Some(doc);
}

#[then("the document should be created")]
async fn document_created(world: &mut AppWorld) {
    assert!(world.last_document.is_some(), "Document was not created");
}
```

### 3. Add to AppWorld if Needed

Update `tests/cucumber.rs` to add new state:

```rust
#[derive(Debug, WorldInit)]
pub struct AppWorld {
    pub task_store: TaskStore,
    pub document_store: DocumentStore,  // Add new stores as needed
    pub last_task: Option<Task>,
    pub last_document: Option<Document>,
}
```

## Testing Tauri Commands

Your Tauri commands are defined in `src-tauri/src/lib.rs`. Here's how they map to tests:

### Command Definition
```rust
#[tauri::command]
fn add_task(title: String, parent_id: Option<String>, state: State<AppState>) -> Task {
    state.task_store.lock().unwrap().add_task(title, parent_id)
}
```

### Test Implementation
```rust
// In your test steps, call the underlying function directly:
#[when(regex = r#"^I add a task with title "(.+)"$"#)]
async fn add_task(world: &mut AppWorld, title: String) {
    let task = world.task_store.add_task(title, None);
    world.last_task = Some(task);
}
```

You're testing the **exact same logic** that the Tauri command uses, just without the IPC layer!

## Example: Testing Different Scenarios

### Happy Path
```gherkin
Scenario: Successfully create a task
  Given the task store has default tasks
  When I add a task with title "New Feature"
  Then I should see 3 root tasks
```

### Edge Cases
```gherkin
Scenario: Add task with empty title
  Given the task store has default tasks
  When I add a task with title ""
  Then I should see an error about invalid title
```

### Complex Workflows
```gherkin
Scenario: Reorganize task hierarchy
  Given the task store has default tasks
  And I add a task with title "Backend"
  When I move task "1-1" under task "Backend"
  Then task "Backend" should have 1 child
  And task "1" should have 1 child
```

## Best Practices

1. **Test Business Logic, Not Implementation Details**
   - Focus on behavior from a user's perspective
   - Avoid testing internal implementation

2. **Keep Scenarios Independent**
   - Each scenario should start with a clean state
   - Don't depend on previous scenario outcomes

3. **Use Descriptive Names**
   - Scenario names should explain the business value
   - Step text should be readable and clear

4. **Test Edge Cases**
   - Empty inputs
   - Boundary conditions
   - Error scenarios

5. **Reuse Step Definitions**
   - Write generic steps that work across scenarios
   - Use regex patterns to parameterize steps

## Integration with CI/CD

Add to your CI pipeline:

```yaml
# .github/workflows/test.yml
- name: Run Cucumber Tests
  run: cargo test --test cucumber
```

## Complementary Testing Strategies

While these tests cover backend logic thoroughly, consider:

1. **Frontend Component Tests** (Vitest/Jest)
   - Test React components in isolation
   - Mock Tauri `invoke()` calls
   - Fast feedback on UI logic

2. **Manual E2E Testing**
   - Use Cucumber scenarios as acceptance criteria
   - Test critical user journeys manually
   - Verify full integration on macOS

3. **Property-Based Testing** (PropTest)
   - Already set up in your project
   - Great for testing invariants
   - Complements scenario-based tests

## Troubleshooting

### Tests fail to compile
- Ensure all dependencies are up to date: `cargo update`
- Check that feature files have matching step definitions

### Scenario passes but shouldn't
- Add more specific assertions
- Check that you're testing the right conditions

### Can't find my step definition
- Ensure the step is exported in `tests/steps/mod.rs`
- Check regex patterns match exactly
- Look for typos in feature file vs step definition

## Future Additions

As you build more features, add tests for:
- Document storage operations
- Loro CRDT synchronization
- Iroh peer-to-peer sync
- Reference resolution
- Search functionality
- Import/export operations

Each Tauri command you write can have corresponding Cucumber scenarios!
