use cucumber::World;
use holon::storage::turso::TursoBackend;
use holon::tasks::{Task, TaskStore};

mod steps;

#[derive(Debug, World, Default)]
pub struct AppWorld {
    pub task_store: TaskStore,
    pub last_task: Option<Task>,
    pub all_tasks: Vec<Task>,
    pub sqlite_backend: Option<TursoBackend>,
    pub last_task_id: Option<String>,
    pub child_task_id: Option<String>,
}

#[tokio::main]
async fn main() {
    AppWorld::cucumber().run_and_exit("tests/features").await;
}
