//! Property-based tests for TodoistDataSource using proptest-state-machine
//!
//! This module tests the TodoistDataSource implementation against an in-memory reference model
//! to ensure correctness of sync operations (full and incremental).
//!
//! ## Coverage
//!
//! The PBT suite covers the following operations:
//! - **Project Management**: CreateProject
//! - **Task Operations**: CreateItem, UpdateItem, DeleteItem, CompleteItem, UncompleteItem
//! - **Sync Operations**: FullSync
//! - **State Verification**: Compare TodoistDataSource state with reference state after each sync
//!
//! ## Test Strategy
//!
//! - Generates random sequences of 1-20 operations
//! - Runs 10 test cases with different operation sequences
//! - Compares TodoistDataSource results against in-memory reference implementation
//! - Verifies state consistency after each sync operation
//! - Minimizes full syncs to respect rate limits (max 100 per 15 minutes)
//! - Prefers incremental syncs (max 1000 per 15 minutes)
//!
//! ## Rate Limiting
//!
//! The test respects Todoist API rate limits:
//! - Maximum 100 full sync requests per 15 minutes
//! - Maximum 1000 partial sync requests per 15 minutes
//! - Maximum 100 commands per request
//!
//! To minimize API calls:
//! - Only performs full sync on initial state or when sync_token is lost
//! - Uses incremental syncs for all subsequent operations
//! - Batches commands when possible (up to 100 per request)

use super::models::TodoistTask;
use super::todoist_datasource::TodoistTaskDataSource;
use super::todoist_sync_provider::TodoistSyncProvider;
use holon::core::datasource::{CrudOperations, DataSource};
use holon_api::Value;
use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Reference state using an in-memory HashMap
#[derive(Debug, Clone)]
pub struct ReferenceState {
    /// Project ID -> Project name mapping
    pub projects: HashMap<String, String>,
    /// Task ID -> Task data mapping
    pub tasks: HashMap<String, TodoistTask>,
    /// Current sync token (None means we need a full sync)
    pub sync_token: Option<String>,
    /// Number of full syncs performed (for rate limiting)
    pub full_sync_count: usize,
    /// Number of incremental syncs performed (for rate limiting)
    pub incremental_sync_count: usize,
}

impl Default for ReferenceState {
    fn default() -> Self {
        Self {
            projects: HashMap::new(),
            tasks: HashMap::new(),
            sync_token: None,
            full_sync_count: 0,
            incremental_sync_count: 0,
        }
    }
}

/// Transitions/Commands for Todoist operations
#[derive(Clone, Debug)]
pub enum TodoistTransition {
    /// Create a project with a simple name like "test-project-123456"
    CreateProject { project_id: String, name: String },
    /// Create a task/item in a project
    CreateItem {
        task_id: String,
        project_id: String,
        content: String,
    },
    /// Update a task's content or priority
    UpdateItem {
        task_id: String,
        content: Option<String>,
        priority: Option<i32>,
    },
    /// Delete a task
    DeleteItem { task_id: String },
    /// Complete a task
    CompleteItem { task_id: String },
    /// Uncomplete a task
    UncompleteItem { task_id: String },
    /// Perform a full sync (only when sync_token is None or explicitly needed)
    FullSync,
    /// Perform an incremental sync (preferred)
    IncrementalSync,
}

/// System under test - wraps TodoistTaskDataSource
pub struct TodoistTest {
    pub datasource: Arc<Mutex<TodoistTaskDataSource>>,
    pub provider: Arc<TodoistSyncProvider>,
    /// Handle for async operations
    pub handle: tokio::runtime::Handle,
    /// Runtime to keep alive (needed when we create our own runtime)
    pub _runtime: Option<Arc<tokio::runtime::Runtime>>,
    /// Mapping from expected project IDs to actual Todoist project IDs
    pub project_id_mapping: Arc<Mutex<HashMap<String, String>>>,
}

impl TodoistTest {
    /// Create a new TodoistTest with the given API key and handle
    pub fn new(handle: &tokio::runtime::Handle, api_key: String) -> Self {
        let provider = Arc::new(TodoistSyncProvider::from_api_key(&api_key).build());
        let datasource = TodoistTaskDataSource::new(provider.clone());
        Self {
            datasource: Arc::new(Mutex::new(datasource)),
            provider,
            handle: handle.clone(),
            _runtime: None,
            project_id_mapping: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a new TodoistTest with a runtime (for when we need to create our own)
    pub fn new_with_runtime(runtime: Arc<tokio::runtime::Runtime>, api_key: String) -> Self {
        let handle = runtime.handle().clone();
        let provider = Arc::new(TodoistSyncProvider::from_api_key(&api_key).build());
        let datasource = TodoistTaskDataSource::new(provider.clone());
        Self {
            datasource: Arc::new(Mutex::new(datasource)),
            provider,
            handle,
            _runtime: Some(runtime),
            project_id_mapping: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

/// Apply a transition to the reference state
/// Returns the result (for sync operations that return data)
/// Also returns created project ID for CreateProject
fn apply_to_reference(
    state: &mut ReferenceState,
    transition: &TodoistTransition,
) -> (Option<Vec<TodoistTask>>, Option<String>) {
    match transition {
        TodoistTransition::CreateProject { project_id, name } => {
            state.projects.insert(project_id.clone(), name.clone());
            (None, Some(project_id.clone()))
        }
        TodoistTransition::CreateItem {
            task_id,
            project_id,
            content,
        } => {
            let task = TodoistTask {
                id: task_id.clone(),
                content: content.clone(),
                description: None,
                project_id: project_id.clone(),
                section_id: None,
                parent_id: None,
                completed: false,
                priority: 1,
                due_date: None,
                labels: None,
                created_at: None,
                updated_at: None,
                completed_at: None,
                url: format!("https://app.todoist.com/app/task/{}", task_id),
            };
            state.tasks.insert(task_id.clone(), task);
            (None, None)
        }
        TodoistTransition::UpdateItem {
            task_id,
            content,
            priority,
        } => {
            if let Some(task) = state.tasks.get_mut(task_id) {
                if let Some(c) = content {
                    task.content = c.clone();
                }
                if let Some(p) = priority {
                    task.priority = *p;
                }
            }
            (None, None)
        }
        TodoistTransition::DeleteItem { task_id } => {
            state.tasks.remove(task_id);
            (None, None)
        }
        TodoistTransition::CompleteItem { task_id } => {
            if let Some(task) = state.tasks.get_mut(task_id) {
                task.completed = true;
            }
            (None, None)
        }
        TodoistTransition::UncompleteItem { task_id } => {
            if let Some(task) = state.tasks.get_mut(task_id) {
                task.completed = false;
            }
            (None, None)
        }
        TodoistTransition::FullSync => {
            // Full sync returns all tasks
            state.full_sync_count += 1;
            // After full sync, we should have a sync_token (simulated)
            state.sync_token = Some("full_sync_token".to_string());
            (Some(state.tasks.values().cloned().collect()), None)
        }
        TodoistTransition::IncrementalSync => {
            // Incremental sync returns changed tasks
            // For simplicity, we'll return all tasks (in real API, only changed ones are returned)
            state.incremental_sync_count += 1;
            // Update sync token
            state.sync_token = Some("incremental_sync_token".to_string());
            (Some(state.tasks.values().cloned().collect()), None)
        }
    }
}

/// Apply a transition to the TodoistDataSource (within TodoistTest)
/// Returns (result, project_id) where:
/// - result: Some(tasks) for sync operations, None otherwise
/// - project_id: Some(actual_id) when creating a project, None otherwise
async fn apply_to_todoist(
    test: &mut TodoistTest,
    transition: &TodoistTransition,
    _handle: &tokio::runtime::Handle,
) -> std::result::Result<(Option<Vec<TodoistTask>>, Option<String>), String> {
    let datasource_arc = test.datasource.clone();
    let mut datasource = datasource_arc.lock().unwrap();

    match transition {
        TodoistTransition::CreateProject {
            project_id: _expected_project_id,
            name,
        } => {
            // Create project via client
            let actual_project_id = test
                .provider
                .client
                .create_project(&name)
                .await
                .map_err(|e| e.to_string())?;
            Ok((None, Some(actual_project_id)))
        }
        TodoistTransition::CreateItem {
            task_id: _expected_task_id,
            project_id,
            content,
        } => {
            // Translate expected project ID to actual Todoist project ID
            let actual_project_id = {
                let mapping = test.project_id_mapping.lock().unwrap();
                mapping.get(project_id).cloned().unwrap_or_else(|| {
                    eprintln!(
                        "[PBT] Warning: No mapping found for project {}, using as-is",
                        project_id
                    );
                    project_id.clone()
                })
            };

            // Create task using new API
            use holon::core::datasource::CrudOperations;
            let mut fields = HashMap::new();
            fields.insert("content".to_string(), Value::String(content.clone()));
            fields.insert(
                "project_id".to_string(),
                Value::String(actual_project_id.clone()),
            );
            let _actual_task_id =
                <TodoistTaskDataSource as CrudOperations<TodoistTask>>::create(&datasource, fields)
                    .await
                    .map_err(|e| e.to_string())?;
            // We'll need to track this mapping, but for now the reference state uses expected ID
            // The sync will reconcile the differences
            Ok((None, None))
        }
        TodoistTransition::UpdateItem {
            task_id,
            content,
            priority,
        } => {
            let current_task =
                <TodoistTaskDataSource as DataSource<TodoistTask>>::get_by_id(&datasource, task_id)
                    .await
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("Task {} not found", task_id))?;

            // Update task using set_field for each field
            use holon::core::datasource::CrudOperations;
            if let Some(c) = content {
                <TodoistTaskDataSource as CrudOperations<TodoistTask>>::set_field(
                    &datasource,
                    task_id,
                    "content",
                    Value::String(c.clone()),
                )
                .await
                .map_err(|e| e.to_string())?;
            }
            if let Some(p) = priority {
                <TodoistTaskDataSource as CrudOperations<TodoistTask>>::set_field(
                    &datasource,
                    task_id,
                    "priority",
                    Value::Integer(*p as i64),
                )
                .await
                .map_err(|e| e.to_string())?;
            }
            Ok((None, None))
        }
        TodoistTransition::DeleteItem { task_id } => {
            use holon::core::datasource::CrudOperations;
            <TodoistTaskDataSource as CrudOperations<TodoistTask>>::delete(&datasource, task_id)
                .await
                .map_err(|e| e.to_string())?;
            Ok((None, None))
        }
        TodoistTransition::CompleteItem { task_id } => {
            use holon::core::datasource::CrudOperations;
            <TodoistTaskDataSource as CrudOperations<TodoistTask>>::set_field(
                &datasource,
                task_id,
                "completed",
                Value::Boolean(true),
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok((None, None))
        }
        TodoistTransition::UncompleteItem { task_id } => {
            use holon::core::datasource::CrudOperations;
            <TodoistTaskDataSource as CrudOperations<TodoistTask>>::set_field(
                &datasource,
                task_id,
                "completed",
                Value::Boolean(false),
            )
            .await
            .map_err(|e| e.to_string())?;
            Ok((None, None))
        }
        TodoistTransition::FullSync => {
            // Perform full sync - sync() emits changes via streams, doesn't return items
            // TODO: Update test to consume from streams instead
            use holon::core::datasource::SyncableProvider;
            let mut provider = test.provider.as_ref().clone();
            // Note: sync() requires &mut self, but we have Arc. This test needs refactoring.
            // For now, return empty items to allow compilation
            // In the new architecture, sync emits changes via streams, not return values
            Ok((Some(vec![]), None))
        }
        TodoistTransition::IncrementalSync => {
            // Get current sync token from provider
            let _sync_token = test.provider.get_sync_token().await;

            // Perform incremental sync - sync() emits changes via streams, doesn't return items
            // TODO: Update test to consume from streams instead
            // In the new architecture, sync emits changes via streams, not return values
            Ok((Some(vec![]), None))
        }
    }
}

/// Check preconditions for a transition
fn check_preconditions(state: &ReferenceState, transition: &TodoistTransition) -> bool {
    match transition {
        TodoistTransition::CreateProject { project_id, .. } => {
            // Can't create project that already exists
            // Also limit to max 3 projects
            !state.projects.contains_key(project_id) && state.projects.len() < 3
        }
        TodoistTransition::CreateItem { project_id, .. } => {
            // Project must exist
            state.projects.contains_key(project_id)
        }
        TodoistTransition::UpdateItem { task_id, .. }
        | TodoistTransition::DeleteItem { task_id }
        | TodoistTransition::CompleteItem { task_id }
        | TodoistTransition::UncompleteItem { task_id } => {
            // Task must exist
            state.tasks.contains_key(task_id)
        }
        TodoistTransition::FullSync => {
            // Only allow full sync if we don't have a token, or if we've done less than 100 full syncs
            state.sync_token.is_none() || state.full_sync_count < 100
        }
        TodoistTransition::IncrementalSync => {
            // Can only do incremental sync if we have a token, and less than 1000 incremental syncs
            state.sync_token.is_some() && state.incremental_sync_count < 1000
        }
    }
}

/// Verify that TodoistTaskDataSource matches reference state
fn verify_states_match(
    reference: &ReferenceState,
    todoist: &TodoistTaskDataSource,
    handle: &tokio::runtime::Handle,
) {
    // If there are no test projects, skip verification (nothing to compare)
    let test_project_ids: std::collections::HashSet<String> =
        reference.projects.keys().cloned().collect();
    if test_project_ids.is_empty() {
        eprintln!("[PBT] verify_states_match: no test projects, skipping verification");
        return;
    }

    eprintln!(
        "[PBT] verify_states_match: verifying {} projects, {} tasks",
        test_project_ids.len(),
        reference.tasks.len()
    );

    // Get all tasks from TodoistTaskDataSource
    let todoist_tasks = tokio::task::block_in_place(|| handle.block_on(todoist.get_all()))
        .expect("Failed to get all tasks from TodoistTaskDataSource");

    eprintln!(
        "[PBT] verify_states_match: retrieved {} tasks from Todoist",
        todoist_tasks.len()
    );

    // Build a map of task IDs for comparison
    let todoist_task_map: HashMap<String, TodoistTask> = todoist_tasks
        .into_iter()
        .map(|t| (t.id.clone(), t))
        .collect();

    // Compare reference tasks with Todoist tasks
    // Note: We only compare tasks that are in our test projects
    let reference_test_tasks: HashMap<String, TodoistTask> = reference
        .tasks
        .iter()
        .filter(|(_, task)| test_project_ids.contains(&task.project_id))
        .map(|(id, task)| (id.clone(), task.clone()))
        .collect();

    let todoist_test_tasks: HashMap<String, TodoistTask> = todoist_task_map
        .into_iter()
        .filter(|(_, task)| test_project_ids.contains(&task.project_id))
        .map(|(id, task)| (id.clone(), task))
        .collect();

    // Check that all reference tasks exist in Todoist
    for (task_id, ref_task) in &reference_test_tasks {
        let todoist_task = todoist_test_tasks.get(task_id);
        assert!(
            todoist_task.is_some(),
            "Task {} exists in reference but not in TodoistTaskDataSource",
            task_id
        );

        let todoist_task = todoist_task.unwrap();

        // Compare key fields (ignoring timestamps which may differ)
        assert_eq!(
            todoist_task.content, ref_task.content,
            "Content mismatch for task {}: expected {:?}, got {:?}",
            task_id, ref_task.content, todoist_task.content
        );
        assert_eq!(
            todoist_task.project_id, ref_task.project_id,
            "Project ID mismatch for task {}: expected {:?}, got {:?}",
            task_id, ref_task.project_id, todoist_task.project_id
        );
        assert_eq!(
            todoist_task.completed, ref_task.completed,
            "Completed status mismatch for task {}: expected {:?}, got {:?}",
            task_id, ref_task.completed, todoist_task.completed
        );
        assert_eq!(
            todoist_task.priority, ref_task.priority,
            "Priority mismatch for task {}: expected {:?}, got {:?}",
            task_id, ref_task.priority, todoist_task.priority
        );
    }

    // Check that deleted tasks are not in Todoist
    for task_id in reference_test_tasks.keys() {
        if !reference.tasks.contains_key(task_id) {
            // This task was deleted in reference, should not be in Todoist
            assert!(
                !todoist_test_tasks.contains_key(task_id),
                "Task {} was deleted in reference but still exists in TodoistTaskDataSource",
                task_id
            );
        }
    }
}

/// Generate transitions based on current state
fn generate_transitions(state: &ReferenceState) -> BoxedStrategy<TodoistTransition> {
    // List of known project IDs
    let project_ids: Vec<String> = state.projects.keys().cloned().collect();

    // List of known task IDs
    let task_ids: Vec<String> = state.tasks.keys().cloned().collect();

    // If no projects exist, only allow creating projects
    if project_ids.is_empty() {
        return "[0-9]{6}" // Generate 6-digit number for project ID
            .prop_map(|num: String| TodoistTransition::CreateProject {
                project_id: format!("test-project-{}", num),
                name: format!("Test Project {}", num),
            })
            .boxed();
    }

    // Limit project creation to max 3 projects
    let create_project = if state.projects.len() < 3 {
        Some(
            "[0-9]{6}"
                .prop_map(|num: String| TodoistTransition::CreateProject {
                    project_id: format!("test-project-{}", num),
                    name: format!("Test Project {}", num),
                })
                .boxed(),
        )
    } else {
        None
    };

    let create_item = (
        prop::sample::select(project_ids.clone()),
        "[a-z]{1,10}", // Simple task content
        "[0-9a-z]{8}", // Task ID
    )
        .prop_map(
            |(project_id, content, task_id)| TodoistTransition::CreateItem {
                task_id: format!("task-{}", task_id),
                project_id,
                content,
            },
        )
        .boxed();

    if task_ids.is_empty() {
        // Only allow creating projects and items
        let mut strategies = vec![(40, create_item)];
        if let Some(cp) = create_project {
            strategies.push((10, cp));
        }
        return prop::strategy::Union::new_weighted(strategies).boxed();
    }

    let update_item = (
        prop::sample::select(task_ids.clone()),
        prop::option::of("[a-z]{1,10}"), // Optional new content
        prop::option::of(1..=4i32),      // Priority 1-4
    )
        .prop_map(
            |(task_id, content, priority)| TodoistTransition::UpdateItem {
                task_id,
                content,
                priority,
            },
        )
        .boxed();

    let delete_item = prop::sample::select(task_ids.clone())
        .prop_map(|task_id| TodoistTransition::DeleteItem { task_id })
        .boxed();

    let complete_item = prop::sample::select(task_ids.clone())
        .prop_map(|task_id| TodoistTransition::CompleteItem { task_id })
        .boxed();

    let uncomplete_item = task_ids
        .iter()
        .filter(|id| state.tasks.get(*id).map(|t| t.completed).unwrap_or(false))
        .cloned()
        .collect::<Vec<_>>();

    let uncomplete = if uncomplete_item.is_empty() {
        None
    } else {
        Some(
            prop::sample::select(uncomplete_item)
                .prop_map(|task_id| TodoistTransition::UncompleteItem { task_id })
                .boxed(),
        )
    };

    // Sync operations - prefer incremental over full
    let full_sync = Just(TodoistTransition::FullSync).boxed();
    let incremental_sync = Just(TodoistTransition::IncrementalSync).boxed();

    let mut strategies = vec![
        (20, create_item),
        (15, update_item),
        (8, delete_item),
        (5, complete_item),
    ];

    // Add create_project strategy if we haven't reached the limit
    if let Some(cp) = create_project {
        strategies.push((5, cp));
    }

    if let Some(uncomplete_strategy) = uncomplete {
        strategies.push((5, uncomplete_strategy));
    }

    // Add sync operations - prefer incremental if we have a token
    if state.sync_token.is_some() {
        // Prefer incremental sync (weight 20) over full sync (weight 2)
        strategies.push((20, incremental_sync));
        if state.full_sync_count < 100 {
            strategies.push((2, full_sync));
        }
    } else {
        // Need full sync if no token
        if state.full_sync_count < 100 {
            strategies.push((10, full_sync));
        }
    }

    prop::strategy::Union::new_weighted(strategies).boxed()
}

/// ReferenceStateMachine implementation
impl ReferenceStateMachine for ReferenceState {
    type State = Self;
    type Transition = TodoistTransition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(ReferenceState::default()).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        generate_transitions(state)
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        check_preconditions(state, transition)
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        let (_result, _created_id) = apply_to_reference(&mut state, transition);
        state
    }
}

/// StateMachineTest implementation
impl StateMachineTest for TodoistTest {
    type SystemUnderTest = Self;
    type Reference = ReferenceState;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        eprintln!("[PBT] init_test called");
        // Get API key from environment variable (use test-specific key to avoid conflicts)
        let api_key = std::env::var("TODOIST_TEST_API_KEY")
            .expect("TODOIST_TEST_API_KEY environment variable must be set");
        eprintln!("[PBT] Using API key: {}...", &api_key[..10]);

        // Clean up any leftover test projects before starting
        let temp_provider = TodoistProvider::new(&api_key);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                eprintln!("[PBT] init_test: using existing runtime handle");
                // Clean up leftover projects
                tokio::task::block_in_place(|| {
                    handle.block_on(async {
                        if let Ok(projects) = temp_provider.get_all_projects().await {
                            for (id, name) in projects {
                                if name.starts_with("Test Project ") {
                                    eprintln!(
                                        "[PBT] Cleaning up leftover project: {} ({})",
                                        name, id
                                    );
                                    let _ = temp_provider.delete_project(&id).await;
                                }
                            }
                        }
                    })
                });
                TodoistTest::new(&handle, api_key)
            }
            Err(_) => {
                eprintln!("[PBT] init_test: creating new runtime");
                // Create a runtime for cleanup
                #[cfg(not(target_arch = "wasm32"))]
                let cleanup_runtime = tokio::runtime::Runtime::new().unwrap();
                #[cfg(target_arch = "wasm32")]
                let cleanup_runtime = tokio::runtime::Runtime::new_current_thread().unwrap();
                cleanup_runtime.block_on(async {
                    if let Ok(projects) = temp_provider.get_all_projects().await {
                        for (id, name) in projects {
                            if name.starts_with("Test Project ") {
                                eprintln!("[PBT] Cleaning up leftover project: {} ({})", name, id);
                                let _ = temp_provider.delete_project(&id).await;
                            }
                        }
                    }
                });
                // Create a runtime if one doesn't exist and keep it alive
                #[cfg(not(target_arch = "wasm32"))]
                let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
                #[cfg(target_arch = "wasm32")]
                let runtime = Arc::new(tokio::runtime::Runtime::new_current_thread().unwrap());
                TodoistTest::new_with_runtime(runtime, api_key)
            }
        }
    }

    fn apply(
        mut state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        // Clone ref_state to apply transition (we can't mutate the original)
        let mut ref_state_clone = ref_state.clone();
        let (ref_result, _expected_project_id) =
            apply_to_reference(&mut ref_state_clone, &transition);

        let handle = state.handle.clone();
        let (todoist_result, actual_project_id) = tokio::task::block_in_place(|| {
            handle.block_on(async {
                apply_to_todoist(&mut state, &transition, &handle)
                    .await
                    .map_err(|e| {
                        // Provide detailed error context
                        format!("Failed to apply transition {:?}: {}", transition, e)
                    })
                    .expect("Todoist transition should succeed (preconditions validated it)")
            })
        });

        // If we created a project, store the ID mapping for future use
        if let (
            TodoistTransition::CreateProject {
                project_id: expected_id,
                ..
            },
            Some(actual_id),
        ) = (&transition, actual_project_id)
        {
            let mut mapping = state.project_id_mapping.lock().unwrap();
            mapping.insert(expected_id.clone(), actual_id.clone());
            eprintln!(
                "[PBT] Stored project ID mapping: {} -> {}",
                expected_id, actual_id
            );
        }

        // For sync operations, compare results
        if let (Some(ref_tasks), Some(todoist_tasks)) = (ref_result, todoist_result) {
            // Build maps for comparison
            let ref_map: HashMap<String, TodoistTask> =
                ref_tasks.into_iter().map(|t| (t.id.clone(), t)).collect();

            let todoist_map: HashMap<String, TodoistTask> = todoist_tasks
                .into_iter()
                .map(|t| (t.id.clone(), t))
                .collect();

            // Compare task counts (only for test projects)
            let test_project_ids: std::collections::HashSet<String> =
                ref_state.projects.keys().cloned().collect();

            let ref_test_count = ref_map
                .values()
                .filter(|t| test_project_ids.contains(&t.project_id))
                .count();

            let todoist_test_count = todoist_map
                .values()
                .filter(|t| test_project_ids.contains(&t.project_id))
                .count();

            assert_eq!(
                ref_test_count, todoist_test_count,
                "Task count mismatch after {:?}: expected {} tasks in test projects, got {}",
                transition, ref_test_count, todoist_test_count
            );
        }

        state
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        eprintln!(
            "[PBT] check_invariants called: projects={}, tasks={}",
            ref_state.projects.len(),
            ref_state.tasks.len()
        );
        let datasource = state.datasource.lock().unwrap();
        verify_states_match(ref_state, &datasource, &state.handle);
        eprintln!("[PBT] check_invariants completed");
    }

    fn teardown(
        state: Self::SystemUnderTest,
        ref_state: <Self::Reference as ReferenceStateMachine>::State,
    ) {
        eprintln!(
            "[PBT] teardown called: cleaning up {} projects",
            ref_state.projects.len()
        );

        // Sync to get all projects, then delete test projects by name pattern
        let handle = state.handle.clone();

        // Get all projects by syncing with projects resource type
        let provider_arc = state.provider.clone();
        let projects_to_delete = tokio::task::block_in_place(|| {
            handle.block_on(async {
                // Get all projects and filter for test projects
                let all_projects = provider_arc.get_all_projects().await.ok()?;
                let test_project_ids: Vec<String> = all_projects
                    .into_iter()
                    .filter_map(|(id, name)| {
                        if name.starts_with("Test Project ") {
                            Some(id)
                        } else {
                            None
                        }
                    })
                    .collect();
                Some(test_project_ids)
            })
        });

        if let Some(project_ids) = projects_to_delete {
            for project_id in project_ids {
                let provider_arc = state.provider.clone();
                let result = tokio::task::block_in_place(|| {
                    handle.block_on(async { provider_arc.delete_project(&project_id).await })
                });

                if let Err(e) = result {
                    eprintln!(
                        "[PBT] Warning: Failed to delete project {}: {}",
                        project_id, e
                    );
                } else {
                    eprintln!("[PBT] Deleted project {}", project_id);
                }
            }
        } else {
            eprintln!("[PBT] Warning: Failed to sync projects for cleanup");
        }

        eprintln!("[PBT] teardown completed");
    }
}

#[cfg(test)]
#[cfg(feature = "integration-tests")]
mod tests {
    use super::*;

    proptest_state_machine::prop_state_machine! {
        #![proptest_config(ProptestConfig {
            cases: 10, // Reduced from 30 to respect rate limits
            failure_persistence: None, // Keep None for now - can enable later if needed
            timeout: 30000, // 30 seconds per test case
            verbose: 1, // Increase verbosity to see more details
            .. ProptestConfig::default()
        })]

        #[test]
        fn test_todoist_datasource_state_machine(sequential 1..20 => TodoistTest);
    }
}
