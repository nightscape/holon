use rusty_knowledge::tasks::{Task, TaskStore};
use std::sync::Mutex;
use tauri::State;

struct AppState {
    task_store: Mutex<TaskStore>,
}

#[tauri::command]
fn get_tasks(state: State<AppState>) -> Vec<Task> {
    state.task_store.lock().unwrap().get_all_tasks()
}

#[tauri::command]
fn add_task(
    title: String,
    parent_id: Option<String>,
    index: Option<usize>,
    state: State<AppState>,
) -> Task {
    state
        .task_store
        .lock()
        .unwrap()
        .add_task(title, parent_id, index)
}

#[tauri::command]
fn toggle_task(task_id: String, state: State<AppState>) {
    state.task_store.lock().unwrap().toggle_task(&task_id);
}

#[tauri::command]
fn delete_task(task_id: String, state: State<AppState>) {
    state.task_store.lock().unwrap().delete_task(&task_id);
}

#[tauri::command]
fn update_task(task_id: String, title: String, state: State<AppState>) {
    state
        .task_store
        .lock()
        .unwrap()
        .update_task(&task_id, title);
}

#[tauri::command]
fn move_task(task_id: String, new_parent_id: Option<String>, index: usize, state: State<AppState>) {
    state
        .task_store
        .lock()
        .unwrap()
        .move_task(&task_id, new_parent_id, index);
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(AppState {
            task_store: Mutex::new(TaskStore::new()),
        })
        .plugin(tauri_plugin_shell::init())
        .invoke_handler(tauri::generate_handler![
            get_tasks,
            add_task,
            toggle_task,
            delete_task,
            update_task,
            move_task
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
