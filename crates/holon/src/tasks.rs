use holon_macros::Entity;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Entity)]
#[entity(name = "tasks")]
pub struct Task {
    #[primary_key]
    #[indexed]
    pub id: String,
    pub title: String,
    #[indexed]
    pub completed: bool,
    #[reference(entity = "tasks")]
    #[indexed]
    pub parent_id: Option<String>,
    #[serde(skip)]
    pub children: Vec<Task>,
}

impl Task {
    pub fn new(title: String, parent_id: Option<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            title,
            completed: false,
            parent_id,
            children: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskStore {
    tasks: Vec<Task>,
}

impl TaskStore {
    pub fn new() -> Self {
        Self {
            tasks: Self::default_tasks(),
        }
    }

    fn default_tasks() -> Vec<Task> {
        vec![
            Task {
                id: "1".to_string(),
                title: "Build Rusty Knowledge MVP".to_string(),
                completed: false,
                parent_id: None,
                children: vec![
                    Task {
                        id: "1-1".to_string(),
                        title: "Set up Tauri".to_string(),
                        completed: true,
                        parent_id: Some("1".to_string()),
                        children: vec![],
                    },
                    Task {
                        id: "1-2".to_string(),
                        title: "Create task UI".to_string(),
                        completed: false,
                        parent_id: Some("1".to_string()),
                        children: vec![],
                    },
                ],
            },
            Task {
                id: "2".to_string(),
                title: "Add Loro integration".to_string(),
                completed: false,
                parent_id: None,
                children: vec![],
            },
        ]
    }

    pub fn get_all_tasks(&self) -> Vec<Task> {
        self.tasks.clone()
    }

    pub fn add_task(
        &mut self,
        title: String,
        parent_id: Option<String>,
        index: Option<usize>,
    ) -> Task {
        let task = Task::new(title, parent_id.clone());

        if let Some(parent_id) = parent_id {
            self.add_child_task(&parent_id, task.clone(), index);
        } else if let Some(idx) = index {
            self.tasks.insert(idx.min(self.tasks.len()), task.clone());
        } else {
            self.tasks.push(task.clone());
        }

        task
    }

    fn add_child_task(&mut self, parent_id: &str, task: Task, index: Option<usize>) {
        for parent_task in &mut self.tasks {
            if parent_task.id == parent_id {
                if let Some(idx) = index {
                    parent_task
                        .children
                        .insert(idx.min(parent_task.children.len()), task);
                } else {
                    parent_task.children.push(task);
                }
                return;
            }
            Self::add_child_task_recursive(
                &mut parent_task.children,
                parent_id,
                task.clone(),
                index,
            );
        }
    }

    fn add_child_task_recursive(
        tasks: &mut Vec<Task>,
        parent_id: &str,
        task: Task,
        index: Option<usize>,
    ) {
        for parent_task in tasks {
            if parent_task.id == parent_id {
                if let Some(idx) = index {
                    parent_task
                        .children
                        .insert(idx.min(parent_task.children.len()), task);
                } else {
                    parent_task.children.push(task);
                }
                return;
            }
            Self::add_child_task_recursive(
                &mut parent_task.children,
                parent_id,
                task.clone(),
                index,
            );
        }
    }

    pub fn toggle_task(&mut self, task_id: &str) {
        for task in &mut self.tasks {
            if task.id == task_id {
                task.completed = !task.completed;
                return;
            }
            Self::toggle_task_recursive(&mut task.children, task_id);
        }
    }

    fn toggle_task_recursive(tasks: &mut Vec<Task>, task_id: &str) {
        for task in tasks {
            if task.id == task_id {
                task.completed = !task.completed;
                return;
            }
            Self::toggle_task_recursive(&mut task.children, task_id);
        }
    }

    pub fn delete_task(&mut self, task_id: &str) {
        self.tasks.retain(|task| task.id != task_id);

        for task in &mut self.tasks {
            Self::delete_task_recursive(&mut task.children, task_id);
        }
    }

    fn delete_task_recursive(tasks: &mut Vec<Task>, task_id: &str) {
        tasks.retain(|task| task.id != task_id);

        for task in tasks {
            Self::delete_task_recursive(&mut task.children, task_id);
        }
    }

    pub fn update_task(&mut self, task_id: &str, title: String) {
        for task in &mut self.tasks {
            if task.id == task_id {
                task.title = title;
                return;
            }
            Self::update_task_recursive(&mut task.children, task_id, title.clone());
        }
    }

    fn update_task_recursive(tasks: &mut Vec<Task>, task_id: &str, title: String) {
        for task in tasks {
            if task.id == task_id {
                task.title = title;
                return;
            }
            Self::update_task_recursive(&mut task.children, task_id, title.clone());
        }
    }

    pub fn move_task(&mut self, task_id: &str, new_parent_id: Option<String>, index: usize) {
        if let Some(mut task) = self.find_and_remove_task(task_id) {
            task.parent_id = new_parent_id.clone();
            if let Some(parent_id) = new_parent_id {
                self.add_child_task(&parent_id, task, Some(index));
            } else {
                self.tasks.insert(index.min(self.tasks.len()), task);
            }
        }
    }

    fn find_and_remove_task(&mut self, task_id: &str) -> Option<Task> {
        for i in 0..self.tasks.len() {
            if self.tasks[i].id == task_id {
                return Some(self.tasks.remove(i));
            }
        }

        for task in &mut self.tasks {
            if let Some(found) = Self::find_and_remove_task_recursive(&mut task.children, task_id) {
                return Some(found);
            }
        }

        None
    }

    fn find_and_remove_task_recursive(tasks: &mut Vec<Task>, task_id: &str) -> Option<Task> {
        for i in 0..tasks.len() {
            if tasks[i].id == task_id {
                return Some(tasks.remove(i));
            }
        }

        for task in tasks {
            if let Some(found) = Self::find_and_remove_task_recursive(&mut task.children, task_id) {
                return Some(found);
            }
        }

        None
    }
}

impl Default for TaskStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::HasSchema;

    #[test]
    fn test_task_has_schema() {
        let schema = Task::schema();
        assert_eq!(schema.table_name, "tasks");

        let id_field = schema.fields.iter().find(|f| f.name == "id").unwrap();
        assert!(id_field.primary_key);
        assert!(id_field.indexed);

        let completed_field = schema
            .fields
            .iter()
            .find(|f| f.name == "completed")
            .unwrap();
        assert!(completed_field.indexed);

        let parent_id_field = schema
            .fields
            .iter()
            .find(|f| f.name == "parent_id")
            .unwrap();
        assert!(parent_id_field.indexed);
        assert!(parent_id_field.nullable);
    }

    #[test]
    fn test_task_to_entity() {
        let task = Task::new("Convert Me".to_string(), Some("parent-123".to_string()));
        let entity = task.to_entity();

        assert_eq!(entity.type_name, "tasks");
        assert_eq!(
            entity.get("title").unwrap().as_string().unwrap(),
            "Convert Me"
        );
        assert!(!entity.get("completed").unwrap().as_bool().unwrap());
        assert_eq!(
            entity.get("parent_id").unwrap().as_string().unwrap(),
            "parent-123"
        );
    }

    #[test]
    fn test_task_from_entity() {
        use holon_api::DynamicEntity;

        let mut entity = DynamicEntity::new("tasks");
        entity.set("id", "test-id-123".to_string());
        entity.set("title", "From Entity".to_string());
        entity.set("completed", false);
        entity.set("parent_id", Some("parent-456".to_string()));

        let task = Task::from_entity(entity).unwrap();
        assert_eq!(task.id, "test-id-123");
        assert_eq!(task.title, "From Entity");
        assert!(!task.completed);
        assert_eq!(task.parent_id, Some("parent-456".to_string()));
        assert!(task.children.is_empty());
    }
}
