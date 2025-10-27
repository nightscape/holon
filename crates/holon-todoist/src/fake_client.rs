//! Fake Todoist API client for testing.
//!
//! This module provides an in-memory implementation of `TodoistApiClient`
//! that stores data locally instead of making real HTTP requests.
//!
//! The fake client allows tests to use the same DI path as production,
//! including running the DDL for `todoist_tasks` and `todoist_projects` tables,
//! while avoiding actual API calls.

use crate::api_client::{ApiResult, TodoistApiClient};
use crate::models::{
    CreateTaskRequest, SyncResponse, TodoistDue, TodoistTask, TodoistTaskApiResponse,
    UpdateTaskRequest,
};
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use tracing::info;

/// Fake Todoist client for testing.
///
/// Stores tasks and projects in-memory, enabling the same DI wiring
/// as production while avoiding real HTTP calls. This ensures:
///
/// - Same DDL runs (CREATE TABLE todoist_tasks, todoist_projects)
/// - Same `QueryableCache` registration
/// - Same stream subscriptions and event adapters
/// - Only the HTTP layer is swapped
pub struct TodoistFakeClient {
    tasks: RwLock<HashMap<String, TodoistTask>>,
    projects: RwLock<HashMap<String, FakeProject>>,
    sync_token_counter: AtomicU64,
}

#[derive(Clone, Debug)]
struct FakeProject {
    id: String,
    name: String,
    parent_id: Option<String>,
    is_deleted: bool,
}

impl TodoistFakeClient {
    pub fn new() -> Self {
        info!("[TodoistFakeClient] Creating new fake client");
        Self {
            tasks: RwLock::new(HashMap::new()),
            projects: RwLock::new(HashMap::new()),
            sync_token_counter: AtomicU64::new(1),
        }
    }

    fn generate_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn next_sync_token(&self) -> String {
        let counter = self.sync_token_counter.fetch_add(1, Ordering::SeqCst);
        format!("fake_sync_token_{}", counter)
    }
}

impl Default for TodoistFakeClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TodoistApiClient for TodoistFakeClient {
    async fn sync_items(&self, _sync_token: Option<&str>) -> ApiResult<SyncResponse> {
        info!("[TodoistFakeClient] sync_items called");
        let tasks = self.tasks.read().await;

        let items: Vec<TodoistTaskApiResponse> = tasks
            .values()
            .map(|task| TodoistTaskApiResponse {
                id: task.id.clone(),
                content: task.content.clone(),
                description: task.description.clone(),
                project_id: task.project_id.clone(),
                section_id: task.section_id.clone(),
                parent_id: task.parent_id.clone(),
                checked: Some(task.completed),
                priority: Some(task.priority),
                due: task.due_date.as_ref().map(|d| TodoistDue {
                    date: d.clone(),
                    timezone: None,
                    string: d.clone(),
                    is_recurring: false,
                }),
                labels: task.labels.as_ref().map(|l| vec![l.clone()]),
                added_at: task.created_at.clone(),
                updated_at: task.updated_at.clone(),
                completed_at: task.completed_at.clone(),
                is_deleted: task.is_deleted,
            })
            .collect();

        info!(
            "[TodoistFakeClient] sync_items returning {} items",
            items.len()
        );

        Ok(SyncResponse {
            items,
            sync_token: Some(self.next_sync_token()),
            full_sync: Some(true),
            full_sync_date_utc: None,
            sync_status: None,
        })
    }

    async fn sync_projects(&self, _sync_token: Option<&str>) -> ApiResult<serde_json::Value> {
        info!("[TodoistFakeClient] sync_projects called");
        let projects = self.projects.read().await;

        let projects_array: Vec<serde_json::Value> = projects
            .values()
            .map(|p| {
                serde_json::json!({
                    "id": p.id,
                    "name": p.name,
                    "parent_id": p.parent_id,
                    "is_deleted": p.is_deleted,
                    "is_archived": false,
                    "is_favorite": false,
                    "color": "charcoal",
                    "view_style": "list",
                    "sort_order": 0,
                })
            })
            .collect();

        info!(
            "[TodoistFakeClient] sync_projects returning {} projects",
            projects_array.len()
        );

        Ok(serde_json::json!({
            "projects": projects_array,
            "sync_token": self.next_sync_token(),
            "full_sync": true,
        }))
    }

    async fn create_task(
        &self,
        request: &CreateTaskRequest<'_>,
    ) -> ApiResult<TodoistTaskApiResponse> {
        info!(
            "[TodoistFakeClient] create_task: content={}",
            request.content
        );
        let id = Self::generate_id();
        let now = Utc::now().to_rfc3339();

        let task = TodoistTask {
            id: id.clone(),
            content: request.content.to_string(),
            description: request.description.map(|s| s.to_string()),
            project_id: request.project_id.unwrap_or("inbox").to_string(),
            section_id: None,
            parent_id: request.parent_id.map(|s| s.to_string()),
            completed: false,
            priority: request.priority.unwrap_or(1),
            due_date: request.due_string.map(|s| s.to_string()),
            labels: None,
            created_at: Some(now.clone()),
            updated_at: Some(now.clone()),
            completed_at: None,
            url: format!("https://todoist.com/showTask?id={}", id),
            is_deleted: Some(false),
        };

        let response = TodoistTaskApiResponse {
            id: task.id.clone(),
            content: task.content.clone(),
            description: task.description.clone(),
            project_id: task.project_id.clone(),
            section_id: task.section_id.clone(),
            parent_id: task.parent_id.clone(),
            checked: Some(task.completed),
            priority: Some(task.priority),
            due: task.due_date.as_ref().map(|d| TodoistDue {
                date: d.clone(),
                timezone: None,
                string: d.clone(),
                is_recurring: false,
            }),
            labels: task.labels.as_ref().map(|l| vec![l.clone()]),
            added_at: task.created_at.clone(),
            updated_at: task.updated_at.clone(),
            completed_at: task.completed_at.clone(),
            is_deleted: task.is_deleted,
        };

        self.tasks.write().await.insert(id, task);
        Ok(response)
    }

    async fn update_task(&self, task_id: &str, request: &UpdateTaskRequest<'_>) -> ApiResult<()> {
        info!("[TodoistFakeClient] update_task: id={}", task_id);
        let mut tasks = self.tasks.write().await;

        if let Some(task) = tasks.get_mut(task_id) {
            if let Some(content) = request.content {
                task.content = content.to_string();
            }
            if let Some(description) = request.description {
                task.description = Some(description.to_string());
            }
            if let Some(due_string) = request.due_string {
                task.due_date = Some(due_string.to_string());
            }
            if let Some(priority) = request.priority {
                task.priority = priority;
            }
            if request.clear_parent {
                task.parent_id = None;
            } else if let Some(parent_id) = request.parent_id {
                task.parent_id = Some(parent_id.to_string());
            }
            task.updated_at = Some(Utc::now().to_rfc3339());
            Ok(())
        } else {
            Err(format!("Task {} not found", task_id).into())
        }
    }

    async fn close_task(&self, task_id: &str) -> ApiResult<()> {
        info!("[TodoistFakeClient] close_task: id={}", task_id);
        let mut tasks = self.tasks.write().await;

        if let Some(task) = tasks.get_mut(task_id) {
            task.completed = true;
            task.completed_at = Some(Utc::now().to_rfc3339());
            task.updated_at = Some(Utc::now().to_rfc3339());
            Ok(())
        } else {
            Err(format!("Task {} not found", task_id).into())
        }
    }

    async fn reopen_task(&self, task_id: &str) -> ApiResult<()> {
        info!("[TodoistFakeClient] reopen_task: id={}", task_id);
        let mut tasks = self.tasks.write().await;

        if let Some(task) = tasks.get_mut(task_id) {
            task.completed = false;
            task.completed_at = None;
            task.updated_at = Some(Utc::now().to_rfc3339());
            Ok(())
        } else {
            Err(format!("Task {} not found", task_id).into())
        }
    }

    async fn delete_task(&self, task_id: &str) -> ApiResult<()> {
        info!("[TodoistFakeClient] delete_task: id={}", task_id);
        let mut tasks = self.tasks.write().await;

        if let Some(task) = tasks.get_mut(task_id) {
            task.is_deleted = Some(true);
            task.updated_at = Some(Utc::now().to_rfc3339());
            Ok(())
        } else {
            Err(format!("Task {} not found", task_id).into())
        }
    }

    async fn move_task(
        &self,
        task_id: &str,
        parent_id: Option<&str>,
        project_id: Option<&str>,
        _section_id: Option<&str>,
    ) -> ApiResult<()> {
        info!(
            "[TodoistFakeClient] move_task: id={}, parent={:?}, project={:?}",
            task_id, parent_id, project_id
        );
        let mut tasks = self.tasks.write().await;

        if let Some(task) = tasks.get_mut(task_id) {
            if let Some(pid) = parent_id {
                task.parent_id = Some(pid.to_string());
            } else {
                task.parent_id = None;
                if let Some(project) = project_id {
                    task.project_id = project.to_string();
                }
            }
            task.updated_at = Some(Utc::now().to_rfc3339());
            Ok(())
        } else {
            Err(format!("Task {} not found", task_id).into())
        }
    }

    async fn create_project(&self, name: &str) -> ApiResult<String> {
        info!("[TodoistFakeClient] create_project: name={}", name);
        let id = Self::generate_id();

        let project = FakeProject {
            id: id.clone(),
            name: name.to_string(),
            parent_id: None,
            is_deleted: false,
        };

        self.projects.write().await.insert(id.clone(), project);
        Ok(id)
    }

    async fn move_project(&self, project_id: &str, parent_id: Option<&str>) -> ApiResult<()> {
        info!(
            "[TodoistFakeClient] move_project: id={}, parent={:?}",
            project_id, parent_id
        );
        let mut projects = self.projects.write().await;

        if let Some(project) = projects.get_mut(project_id) {
            project.parent_id = parent_id.map(|s| s.to_string());
            Ok(())
        } else {
            Err(format!("Project {} not found", project_id).into())
        }
    }

    async fn delete_project(&self, project_id: &str) -> ApiResult<()> {
        info!("[TodoistFakeClient] delete_project: id={}", project_id);
        let mut projects = self.projects.write().await;

        if let Some(project) = projects.get_mut(project_id) {
            project.is_deleted = true;
            Ok(())
        } else {
            Err(format!("Project {} not found", project_id).into())
        }
    }

    async fn archive_project(&self, project_id: &str) -> ApiResult<()> {
        info!("[TodoistFakeClient] archive_project: id={}", project_id);
        // In the fake client, we don't track archive status separately
        // Just return success
        if self.projects.read().await.contains_key(project_id) {
            Ok(())
        } else {
            Err(format!("Project {} not found", project_id).into())
        }
    }

    async fn unarchive_project(&self, project_id: &str) -> ApiResult<()> {
        info!("[TodoistFakeClient] unarchive_project: id={}", project_id);
        if self.projects.read().await.contains_key(project_id) {
            Ok(())
        } else {
            Err(format!("Project {} not found", project_id).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fake_client_create_task() {
        let client = TodoistFakeClient::new();

        let request = CreateTaskRequest {
            content: "Test task",
            description: Some("Test description"),
            project_id: None,
            due_string: None,
            priority: Some(2),
            parent_id: None,
        };

        let response = client.create_task(&request).await.unwrap();
        assert_eq!(response.content, "Test task");
        assert_eq!(response.description, Some("Test description".to_string()));

        let sync = client.sync_items(None).await.unwrap();
        assert_eq!(sync.items.len(), 1);
        assert_eq!(sync.items[0].content, "Test task");
    }

    #[tokio::test]
    async fn test_fake_client_complete_task() {
        let client = TodoistFakeClient::new();

        let request = CreateTaskRequest {
            content: "Task to complete",
            description: None,
            project_id: None,
            due_string: None,
            priority: None,
            parent_id: None,
        };

        let task = client.create_task(&request).await.unwrap();
        client.close_task(&task.id).await.unwrap();

        let sync = client.sync_items(None).await.unwrap();
        assert_eq!(sync.items[0].checked, Some(true));
    }
}
