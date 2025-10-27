use crate::AppWorld;
use cucumber::{given, then, when};

#[given("the task store has default tasks")]
async fn task_store_has_default_tasks(_world: &mut AppWorld) {}

#[when("I get all tasks")]
async fn get_all_tasks(world: &mut AppWorld) {
    world.all_tasks = world.task_store.get_all_tasks();
}

#[when(regex = r#"^I add a task with title "([^"]+)" as a child of task "([^"]+)"$"#)]
async fn add_child_task(world: &mut AppWorld, title: String, parent_id: String) {
    let task = world.task_store.add_task(title, Some(parent_id), None);
    world.last_task = Some(task);
}

#[when(regex = r#"^I add a task with title "([^"]+)"$"#)]
async fn add_task(world: &mut AppWorld, title: String) {
    let task = world.task_store.add_task(title, None, None);
    world.last_task = Some(task);
}

#[when(regex = r#"^I toggle task "(.+)"$"#)]
async fn toggle_task(world: &mut AppWorld, task_id: String) {
    world.task_store.toggle_task(&task_id);
}

#[when(regex = r#"^I toggle task "(.+)" again$"#)]
async fn toggle_task_again(world: &mut AppWorld, task_id: String) {
    world.task_store.toggle_task(&task_id);
}

#[when(regex = r#"^I update task "(.+)" with title "(.+)"$"#)]
async fn update_task(world: &mut AppWorld, task_id: String, title: String) {
    world.task_store.update_task(&task_id, title);
}

#[when(regex = r#"^I delete task "(.+)"$"#)]
async fn delete_task(world: &mut AppWorld, task_id: String) {
    world.task_store.delete_task(&task_id);
}

#[when(regex = r#"^I move task "(.+)" to root level at index (\d+)$"#)]
async fn move_task_to_root(world: &mut AppWorld, task_id: String, index: usize) {
    world.task_store.move_task(&task_id, None, index);
}

#[when(regex = r#"^I move task "(.+)" under task "(.+)" at index (\d+)$"#)]
async fn move_task_under_parent(
    world: &mut AppWorld,
    task_id: String,
    parent_id: String,
    index: usize,
) {
    world.task_store.move_task(&task_id, Some(parent_id), index);
}

#[then(regex = r"^I should see (\d+) root tasks?$")]
async fn should_see_root_tasks(world: &mut AppWorld, count: usize) {
    world.all_tasks = world.task_store.get_all_tasks();
    assert_eq!(
        world.all_tasks.len(),
        count,
        "Expected {} root tasks, but found {}",
        count,
        world.all_tasks.len()
    );
}

#[then(regex = r#"^the first task should have the title "(.+)"$"#)]
async fn first_task_has_title(world: &mut AppWorld, expected_title: String) {
    world.all_tasks = world.task_store.get_all_tasks();
    assert!(!world.all_tasks.is_empty(), "Expected at least one task");
    assert_eq!(
        world.all_tasks[0].title, expected_title,
        "Expected first task to have title '{}', but got '{}'",
        expected_title, world.all_tasks[0].title
    );
}

#[then(regex = r#"^the last task should have the title "(.+)"$"#)]
async fn last_task_has_title(world: &mut AppWorld, expected_title: String) {
    world.all_tasks = world.task_store.get_all_tasks();
    assert!(!world.all_tasks.is_empty(), "Expected at least one task");
    let last_task = world.all_tasks.last().unwrap();
    assert_eq!(
        last_task.title, expected_title,
        "Expected last task to have title '{}', but got '{}'",
        expected_title, last_task.title
    );
}

#[then(regex = r#"^task "(.+)" should have (\d+) children?$"#)]
async fn task_should_have_children(world: &mut AppWorld, task_id: String, count: usize) {
    world.all_tasks = world.task_store.get_all_tasks();
    let task = find_task(&world.all_tasks, &task_id);
    assert!(task.is_some(), "Task '{}' not found", task_id);
    let task = task.unwrap();
    assert_eq!(
        task.children.len(),
        count,
        "Expected task '{}' to have {} children, but found {}",
        task_id,
        count,
        task.children.len()
    );
}

#[then(regex = r#"^the child task should have the title "(.+)"$"#)]
async fn child_task_has_title(world: &mut AppWorld, expected_title: String) {
    if let Some(ref task) = world.last_task {
        assert_eq!(
            task.title, expected_title,
            "Expected child task to have title '{}', but got '{}'",
            expected_title, task.title
        );
    } else {
        panic!("No last task found");
    }
}

#[then(regex = r#"^task "(.+)" should be completed$"#)]
async fn task_should_be_completed(world: &mut AppWorld, task_id: String) {
    world.all_tasks = world.task_store.get_all_tasks();
    let task = find_task(&world.all_tasks, &task_id);
    assert!(task.is_some(), "Task '{}' not found", task_id);
    let task = task.unwrap();
    assert!(
        task.completed,
        "Expected task '{}' to be completed, but it was not",
        task_id
    );
}

#[then(regex = r#"^task "(.+)" should not be completed$"#)]
async fn task_should_not_be_completed(world: &mut AppWorld, task_id: String) {
    world.all_tasks = world.task_store.get_all_tasks();
    let task = find_task(&world.all_tasks, &task_id);
    assert!(task.is_some(), "Task '{}' not found", task_id);
    let task = task.unwrap();
    assert!(
        !task.completed,
        "Expected task '{}' to not be completed, but it was",
        task_id
    );
}

#[then(regex = r#"^task "(.+)" should have the title "(.+)"$"#)]
async fn task_should_have_title(world: &mut AppWorld, task_id: String, expected_title: String) {
    world.all_tasks = world.task_store.get_all_tasks();
    let task = find_task(&world.all_tasks, &task_id);
    assert!(task.is_some(), "Task '{}' not found", task_id);
    let task = task.unwrap();
    assert_eq!(
        task.title, expected_title,
        "Expected task '{}' to have title '{}', but got '{}'",
        task_id, expected_title, task.title
    );
}

#[then(regex = r#"^I should not see task "(.+)"$"#)]
async fn should_not_see_task(world: &mut AppWorld, task_id: String) {
    world.all_tasks = world.task_store.get_all_tasks();
    let task = find_task(&world.all_tasks, &task_id);
    assert!(
        task.is_none(),
        "Expected not to find task '{}', but it exists",
        task_id
    );
}

#[then(regex = r#"^task "(.+)" should be at root level$"#)]
async fn task_should_be_at_root(world: &mut AppWorld, task_id: String) {
    world.all_tasks = world.task_store.get_all_tasks();
    let found = world.all_tasks.iter().any(|t| t.id == task_id);
    assert!(
        found,
        "Expected task '{}' to be at root level, but it was not found",
        task_id
    );
}

fn find_task<'a>(tasks: &'a [holon::tasks::Task], task_id: &str) -> Option<&'a holon::tasks::Task> {
    for task in tasks {
        if task.id == task_id {
            return Some(task);
        }
        if let Some(found) = find_task(&task.children, task_id) {
            return Some(found);
        }
    }
    None
}
