use holon_macros::Entity;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Entity)]
#[entity(name = "todoist_tasks", short_name = "task")]
pub struct TodoistTask {
    #[primary_key]
    #[indexed]
    pub id: String,

    pub content: String,

    pub description: Option<String>,

    #[indexed]
    pub project_id: String,

    pub section_id: Option<String>,

    pub parent_id: Option<String>,

    #[indexed]
    pub completed: bool,

    pub priority: i32,

    pub due_date: Option<String>,

    pub labels: Option<String>,

    pub created_at: Option<String>,

    pub updated_at: Option<String>,

    pub completed_at: Option<String>,

    pub url: String,

    /// Indicates if this item has been deleted
    #[serde(default)]
    pub is_deleted: Option<bool>,
}

impl TodoistTask {
    pub fn new(id: String, content: String, project_id: String) -> Self {
        Self {
            id: id.clone(),
            content,
            description: None,
            project_id,
            section_id: None,
            parent_id: None,
            completed: false,
            priority: 1,
            due_date: None,
            labels: None,
            created_at: None,
            updated_at: None,
            completed_at: None,
            url: format!("https://app.todoist.com/app/task/{}", id),
            is_deleted: Some(false),
        }
    }
}

// Implement BlockEntity trait for TodoistTask
// Note: sort_key and depth are computed dynamically since they're not stored in TodoistTask
impl holon::core::datasource::BlockEntity for TodoistTask {
    fn id(&self) -> &str {
        &self.id
    }

    fn parent_id(&self) -> Option<&str> {
        self.parent_id.as_deref()
    }

    fn sort_key(&self) -> &str {
        // TODO: Use order field or compute from parent_id + created_at
        // For now, return a placeholder - this should be computed from order or created_at
        "a0"
    }

    fn depth(&self) -> i64 {
        // TODO: Compute depth by traversing parent_id chain
        // For now, return 0 for root items, 1 for children
        if self.parent_id.is_some() {
            1
        } else {
            0
        }
    }

    fn content(&self) -> &str {
        &self.content
    }
}

// Implement TaskEntity trait for TodoistTask
impl holon::core::datasource::TaskEntity for TodoistTask {
    fn completed(&self) -> bool {
        self.completed
    }

    fn priority(&self) -> Option<i64> {
        Some(self.priority as i64)
    }

    fn due_date(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.due_date.as_ref().and_then(|d| {
            chrono::DateTime::parse_from_rfc3339(d)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        })
    }
}

// Implement OperationRegistry to expose all operations for TodoistTask
// Since TodoistTask implements both BlockEntity and TaskEntity,
// it gets operations from all three traits: CrudOperations, BlockOperations, TaskOperations
impl holon::core::datasource::OperationRegistry for TodoistTask {
    fn all_operations() -> Vec<holon::core::datasource::OperationDescriptor> {
        let entity_name = Self::entity_name();
        let short_name = Self::short_name().expect("TodoistTask must have short_name");
        let table = entity_name; // Use entity_name as table name - no mapping needed!
        let id_column = "id";

        // Aggregate operations from all applicable traits
        #[cfg(not(target_arch = "wasm32"))]
        {
            use holon::core::datasource::{
                __operations_crud_operation_provider, __operations_mutable_block_data_source,
                __operations_mutable_task_data_source,
            };
            __operations_crud_operation_provider::crud_operations(
                entity_name,
                short_name,
                table,
                id_column,
            )
            .into_iter()
            .chain(
                __operations_mutable_block_data_source::block_operations(
                    entity_name,
                    short_name,
                    table,
                    id_column,
                )
                .into_iter(),
            )
            .chain(
                __operations_mutable_task_data_source::task_operations(
                    entity_name,
                    short_name,
                    table,
                    id_column,
                )
                .into_iter(),
            )
            .collect()
        }
        #[cfg(target_arch = "wasm32")]
        {
            // Operations macros not available on WASM
            Vec::new()
        }
    }

    fn entity_name() -> &'static str {
        "todoist_tasks"
    }

    fn short_name() -> Option<&'static str> {
        TodoistTask::short_name()
    }
}

#[derive(Debug, Deserialize)]
pub struct TodoistTaskApiResponse {
    pub id: String,
    pub content: String,
    pub description: Option<String>,
    pub project_id: String,
    pub section_id: Option<String>,
    pub parent_id: Option<String>,
    pub checked: Option<bool>,
    pub priority: Option<i32>,
    pub due: Option<TodoistDue>,
    pub labels: Option<Vec<String>>,
    pub added_at: Option<String>,
    pub updated_at: Option<String>,
    pub completed_at: Option<String>,
    /// Indicates if this item has been deleted (only present during incremental sync)
    #[serde(default)]
    pub is_deleted: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct TodoistDue {
    pub date: String,
    pub timezone: Option<String>,
    pub string: String,
    pub is_recurring: bool,
}

#[derive(Debug, Serialize)]
pub struct CreateTaskRequest<'a> {
    pub content: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_string: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<&'a str>,
}

#[derive(Debug, Default)]
pub struct UpdateTaskRequest<'a> {
    pub content: Option<&'a str>,
    pub description: Option<&'a str>,
    pub due_string: Option<&'a str>,
    pub priority: Option<i32>,
    pub parent_id: Option<&'a str>,
    pub clear_parent: bool,
}

#[derive(Debug, Deserialize)]
pub struct PagedResponse<T> {
    #[serde(alias = "items")]
    #[serde(alias = "results")]
    pub data: Vec<T>,
    pub next_cursor: Option<String>,
}

/// Sync API response structure
#[derive(Debug, Deserialize)]
pub struct SyncResponse {
    /// Items (tasks) returned from sync
    pub items: Vec<TodoistTaskApiResponse>,
    /// Sync token for next incremental sync (may be in sync_status or separate field)
    #[serde(default)]
    pub sync_token: Option<String>,
    /// Full sync flag
    #[serde(default)]
    pub full_sync: Option<bool>,
    /// Full sync date (only present during initial sync)
    #[serde(rename = "full_sync_date_utc")]
    pub full_sync_date_utc: Option<String>,
    /// Sync status (may contain sync_token)
    #[serde(default)]
    pub sync_status: Option<serde_json::Value>,
}

/// Command for Sync API write operations
#[derive(Debug, Serialize)]
pub struct SyncCommand {
    /// Command type (e.g., "item_add", "item_update", "item_delete", "item_close", "item_uncomplete")
    #[serde(rename = "type")]
    pub command_type: String,
    /// Unique UUID for this command
    pub uuid: String,
    /// Temporary ID for newly created resources (required for create commands)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temp_id: Option<String>,
    /// Command arguments
    pub args: serde_json::Value,
}

/// Command response from Sync API
#[derive(Debug, Deserialize)]
pub struct CommandResponse {
    /// UUID of the command
    pub uuid: String,
    /// Status of the command ("ok" or error)
    pub status: String,
    /// Error message if status is not "ok"
    #[serde(default)]
    pub error: Option<String>,
    /// Temporary ID mapping (for newly created items)
    #[serde(default)]
    pub temp_id_mapping: Option<serde_json::Value>,
}

impl From<TodoistTaskApiResponse> for TodoistTask {
    fn from(api: TodoistTaskApiResponse) -> Self {
        TodoistTask {
            id: api.id.clone(),
            content: api.content,
            description: api.description,
            project_id: api.project_id,
            section_id: api.section_id,
            parent_id: api.parent_id,
            completed: api.checked.unwrap_or(false),
            priority: api.priority.unwrap_or(1),
            due_date: api.due.map(|d| d.date),
            labels: api.labels.map(|labels| labels.join(",")),
            created_at: api.added_at,
            updated_at: api.updated_at,
            completed_at: api.completed_at,
            url: format!("https://app.todoist.com/app/task/{}", api.id),
            is_deleted: api.is_deleted,
        }
    }
}

/// Todoist Project model
#[derive(Debug, Clone, Serialize, Deserialize, Entity)]
#[entity(name = "todoist_projects", short_name = "project")]
pub struct TodoistProject {
    #[primary_key]
    #[indexed]
    pub id: String,

    pub name: String,

    pub color: Option<String>,

    #[indexed]
    pub parent_id: Option<String>,

    /// Sort order (renamed from `order` to avoid SQL reserved keyword)
    #[serde(rename = "order")]
    pub sort_order: Option<i32>,

    pub is_archived: Option<bool>,

    pub is_favorite: Option<bool>,

    pub view_style: Option<String>,

    pub shared: Option<bool>,

    pub sync_id: Option<String>,

    pub created_at: Option<String>,

    pub updated_at: Option<String>,

    /// Whether this is the Inbox project
    pub inbox_project: Option<bool>,
}

/// Todoist Project API response structure
#[derive(Debug, Deserialize)]
pub struct TodoistProjectApiResponse {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub color: Option<String>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub order: Option<i32>,
    #[serde(default)]
    pub is_archived: Option<bool>,
    #[serde(default)]
    pub is_favorite: Option<bool>,
    #[serde(default)]
    pub view_style: Option<String>,
    #[serde(default)]
    pub shared: Option<bool>,
    #[serde(default)]
    pub sync_id: Option<String>,
    #[serde(default)]
    pub added_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    /// Indicates if this item has been deleted (only present during incremental sync)
    #[serde(default)]
    pub is_deleted: Option<bool>,
    /// Whether this is the Inbox project (only present and true for the Inbox)
    #[serde(default)]
    pub inbox_project: Option<bool>,
}

impl From<TodoistProjectApiResponse> for TodoistProject {
    fn from(api: TodoistProjectApiResponse) -> Self {
        TodoistProject {
            id: api.id.clone(),
            name: api.name,
            color: api.color,
            parent_id: api.parent_id,
            sort_order: api.order,
            is_archived: api.is_archived,
            is_favorite: api.is_favorite,
            view_style: api.view_style,
            shared: api.shared,
            sync_id: api.sync_id,
            created_at: api.added_at,
            updated_at: api.updated_at,
            inbox_project: api.inbox_project,
        }
    }
}
