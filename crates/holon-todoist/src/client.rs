use super::models::{
    CommandResponse, CreateTaskRequest, SyncCommand, SyncResponse, TodoistTaskApiResponse,
    UpdateTaskRequest,
};
use crate::api_client::{ApiResult, TodoistApiClient};
use async_trait::async_trait;
use reqwest::header::HeaderMap;
use serde_json::json;
use tracing::{debug, error, info};
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const BASE_URL: &str = "https://app.todoist.com/api/v1";

pub struct TodoistClient {
    default_headers: HeaderMap,
    client: reqwest::Client,
    max_retries: u32,
}

impl TodoistClient {
    pub fn new(api_key: &str) -> Self {
        Self::with_config(api_key, 30, 3)
    }

    pub fn with_config(api_key: &str, timeout_secs: u64, max_retries: u32) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            format!("Bearer {}", api_key)
                .parse()
                .expect("Invalid API key format"),
        );

        let mut builder = reqwest::Client::builder();
        #[cfg(not(target_arch = "wasm32"))]
        {
            builder = builder.timeout(std::time::Duration::from_secs(timeout_secs));
        }
        let client = builder.build().expect("Failed to create HTTP client");

        Self {
            default_headers: headers,
            client,
            max_retries,
        }
    }

    fn is_retryable_error(e: &reqwest::Error) -> bool {
        e.is_timeout() || e.is_connect()
    }

    fn is_retryable_status(status: reqwest::StatusCode) -> bool {
        status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            || status == reqwest::StatusCode::GATEWAY_TIMEOUT
    }

    async fn send_with_retry(
        &self,
        url: &str,
        body: &serde_json::Value,
        headers: HeaderMap,
        operation: &str,
    ) -> Result<String> {
        let mut last_error = String::new();
        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                let backoff_ms = 500 * 2u64.pow(attempt - 1); // 500ms, 1s, 2s
                info!(
                    "[TodoistClient] Retry {}/{} for {} after {}ms",
                    attempt, self.max_retries, operation, backoff_ms
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }

            let result = self
                .client
                .post(url)
                .headers(headers.clone())
                .json(body)
                .send()
                .await;

            match result {
                Ok(response) => {
                    let status = response.status();
                    if Self::is_retryable_status(status) && attempt < self.max_retries {
                        // Parse Retry-After header for 429 responses
                        let retry_after_ms = response
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|s| s.parse::<u64>().ok())
                            .map(|secs| secs * 1000);
                        if let Some(ms) = retry_after_ms {
                            info!(
                                "[TodoistClient] Rate limited (429). Server says retry after {}s for {}",
                                ms / 1000,
                                operation
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                        }
                        last_error = format!("HTTP {} from {}", status.as_u16(), url);
                        info!("[TodoistClient] Retryable status {}: {}", status, operation);
                        continue;
                    }
                    return Self::handle_response(response, url).await;
                }
                Err(e) => {
                    if Self::is_retryable_error(&e) && attempt < self.max_retries {
                        last_error = Self::format_reqwest_error(e, url, operation);
                        info!(
                            "[TodoistClient] Retryable error for {}: {}",
                            operation, last_error
                        );
                        continue;
                    }
                    let error_msg = Self::format_reqwest_error(e, url, operation);
                    error!("[TodoistClient] {} failed: {}", operation, error_msg);
                    return Err(error_msg.into());
                }
            }
        }
        Err(format!(
            "{} failed after {} retries: {}",
            operation, self.max_retries, last_error
        )
        .into())
    }

    /// Helper to create better error messages from reqwest errors
    fn format_reqwest_error(e: reqwest::Error, url: &str, operation: &str) -> String {
        // Check error type first and provide specific guidance
        if e.is_timeout() {
            format!(
                "Failed to {} for {}: timeout - request took too long (check network or increase timeout)",
                operation, url
            )
        } else if {
            #[cfg(not(target_arch = "wasm32"))]
            {
                e.is_connect()
            }
            #[cfg(target_arch = "wasm32")]
            {
                false // is_connect not available on WASM
            }
        } {
            format!(
                "Failed to {} for {}: connection error - check network connectivity, DNS resolution, and firewall settings. Error: {}",
                operation, url, e
            )
        } else if e.is_request() {
            format!(
                "Failed to {} for {}: request error - invalid URL format or malformed request parameters. Error: {}",
                operation, url, e
            )
        } else if e.is_decode() {
            format!(
                "Failed to {} for {}: decode error - unexpected response format from server. Error: {}",
                operation, url, e
            )
        } else {
            // For other errors, try to get more details
            let error_str = format!("{:?}", e); // Use Debug for more details
            let display_str = e.to_string();

            // Check for common error patterns
            if display_str.contains("error sending request") {
                format!(
                    "Failed to {} for {}: network/connection issue - check internet connection, API availability, and proxy settings. Debug details: {}",
                    operation, url, error_str
                )
            } else if display_str.contains("certificate")
                || display_str.contains("TLS")
                || error_str.contains("certificate")
                || error_str.contains("TLS")
            {
                format!(
                    "Failed to {} for {}: TLS/certificate error - check SSL certificate configuration. Error: {}",
                    operation, url, e
                )
            } else if display_str.contains("redirect") || error_str.contains("redirect") {
                format!(
                    "Failed to {} for {}: redirect error - too many redirects or invalid redirect. Error: {}",
                    operation, url, e
                )
            } else {
                format!(
                    "Failed to {} for {}: {}. Debug details: {}",
                    operation, url, display_str, error_str
                )
            }
        }
    }

    /// Helper to handle HTTP responses with better error messages
    async fn handle_response(response: reqwest::Response, url: &str) -> Result<String> {
        let status = response.status();
        let response_text = response
            .text()
            .await
            .map_err(|e| format!("Failed to read response body from {}: {}", url, e))?;

        if !status.is_success() {
            return Err(format!(
                "HTTP {} error from {}: {}",
                status.as_u16(),
                url,
                if response_text.len() > 500 {
                    format!("{}... (truncated)", &response_text[..500])
                } else {
                    response_text
                }
            )
            .into());
        }

        Ok(response_text)
    }

    /// Parse command response from various formats the Sync API can return
    /// Handles:
    /// 1. Direct array: [CommandResponse, ...]
    /// 2. In sync_status as array: {"sync_status": [CommandResponse, ...]}
    /// 3. In sync_status as map: {"sync_status": {"uuid": "ok" or CommandResponse, ...}}
    fn parse_command_response(response_text: &str) -> Result<Vec<CommandResponse>> {
        // Try parsing directly as Vec<CommandResponse>
        match serde_json::from_str::<Vec<CommandResponse>>(response_text) {
            Ok(resp) => Ok(resp),
            Err(_) => {
                // Try parsing as SyncResponse with sync_status
                let sync_resp: serde_json::Value = serde_json::from_str(response_text)
                    .map_err(|e| format!("Failed to parse response: {}", e))?;

                // Get temp_id_mapping from top level (if present)
                let top_level_temp_id_mapping = sync_resp.get("temp_id_mapping").cloned();

                if let Some(sync_status) = sync_resp.get("sync_status") {
                    // Try parsing as array first
                    match serde_json::from_value::<Vec<CommandResponse>>(sync_status.clone()) {
                        Ok(mut resp) => {
                            // If we have top-level temp_id_mapping and responses don't have it, add it
                            if let Some(ref temp_mapping) = top_level_temp_id_mapping {
                                for cmd_resp in &mut resp {
                                    if cmd_resp.temp_id_mapping.is_none() {
                                        cmd_resp.temp_id_mapping = Some(temp_mapping.clone());
                                    }
                                }
                            }
                            Ok(resp)
                        }
                        Err(_) => {
                            // Try parsing as map (UUID -> status object or string)
                            // When it's a map, the key is the UUID and the value might be:
                            // - Just the string "ok" (success case)
                            // - A status/error object
                            // - A full CommandResponse object
                            if let Some(map) = sync_status.as_object() {
                                let mut responses = Vec::new();
                                for (uuid, value) in map {
                                    // Check if value is just a string (e.g., "ok")
                                    if let Some(status_str) = value.as_str() {
                                        // Use top-level temp_id_mapping if available
                                        let temp_id_mapping = top_level_temp_id_mapping.clone();
                                        responses.push(CommandResponse {
                                            uuid: uuid.clone(),
                                            status: status_str.to_string(),
                                            error: None,
                                            temp_id_mapping,
                                        });
                                    } else if let Some(obj) = value.as_object() {
                                        // Try to parse as full CommandResponse first
                                        match serde_json::from_value::<CommandResponse>(
                                            value.clone(),
                                        ) {
                                            Ok(mut resp) => {
                                                // Ensure UUID matches the key
                                                resp.uuid = uuid.clone();
                                                // Use top-level temp_id_mapping if response doesn't have it
                                                if resp.temp_id_mapping.is_none() {
                                                    resp.temp_id_mapping =
                                                        top_level_temp_id_mapping.clone();
                                                }
                                                responses.push(resp);
                                            }
                                            Err(_) => {
                                                // If that fails, try parsing as just status/error object
                                                // and construct CommandResponse manually
                                                let status = obj
                                                    .get("status")
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or("error")
                                                    .to_string();
                                                let error = obj
                                                    .get("error")
                                                    .and_then(|v| v.as_str())
                                                    .map(|s| s.to_string());
                                                let temp_id_mapping = obj
                                                    .get("temp_id_mapping")
                                                    .cloned()
                                                    .or_else(|| top_level_temp_id_mapping.clone());
                                                responses.push(CommandResponse {
                                                    uuid: uuid.clone(),
                                                    status,
                                                    error,
                                                    temp_id_mapping,
                                                });
                                            }
                                        }
                                    } else {
                                        return Err(format!(
                                            "Unexpected sync_status value type for UUID {}: {}",
                                            uuid, value
                                        )
                                        .into());
                                    }
                                }
                                Ok(responses)
                            } else {
                                Err(format!(
                                    "sync_status is not an array or object: {}",
                                    response_text
                                )
                                .into())
                            }
                        }
                    }
                } else {
                    Err(format!("Unexpected response format: {}", response_text).into())
                }
            }
        }
    }

    /// Execute a sync command and return the command response
    async fn execute_command(&self, command: SyncCommand) -> Result<CommandResponse> {
        let url = format!("{}/sync", BASE_URL);
        let command_uuid = command.uuid.clone();

        let body = serde_json::json!({
            "sync_token": "*",
            "commands": [command],
        });

        debug!(
            "[TodoistClient] Executing command: type={}, uuid={}",
            command.command_type, command_uuid
        );

        // Inject trace context into HTTP headers for distributed tracing
        let mut headers = self.default_headers.clone();
        #[cfg(not(target_arch = "wasm32"))]
        {
            use opentelemetry::Context;
            use opentelemetry::global;

            // Create a carrier for injecting trace context
            struct HeaderInjector {
                headers: reqwest::header::HeaderMap,
            }
            impl opentelemetry::propagation::Injector for HeaderInjector {
                fn set(&mut self, key: &str, value: String) {
                    if let Ok(header_name) = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                    {
                        if let Ok(header_value) = reqwest::header::HeaderValue::from_str(&value) {
                            self.headers.insert(header_name, header_value);
                        }
                    }
                }
            }

            // Inject current trace context into headers
            let mut injector = HeaderInjector { headers };
            global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&Context::current(), &mut injector);
            });
            headers = injector.headers;
        }

        let response_text = self
            .send_with_retry(&url, &body, headers, "execute_command")
            .await?;

        debug!(
            "[TodoistClient] Command response received: uuid={}, response_length={}",
            command_uuid,
            response_text.len()
        );

        let cmd_responses = Self::parse_command_response(&response_text).map_err(|e| {
            error!(
                "[TodoistClient] Failed to parse command response: {} - Response: {}",
                e,
                &response_text.chars().take(200).collect::<String>()
            );
            e
        })?;

        let cmd_result = cmd_responses
            .into_iter()
            .find(|r| r.uuid == command_uuid)
            .ok_or_else(|| {
                let error = format!("Command response not found for uuid {}", command_uuid);
                error!("[TodoistClient] {}", error);
                error
            })?;

        if cmd_result.status != "ok" {
            let error_msg = cmd_result
                .error
                .unwrap_or_else(|| "Unknown error".to_string());
            let full_error = format!(
                "Command failed: {} (full response: {})",
                error_msg, response_text
            );
            // Keep error logging for actual failures
            error!("[TodoistClient] Command failed: {}", full_error);
            return Err(full_error.into());
        }

        debug!("[TodoistClient] Command succeeded: uuid={}", command_uuid);
        Ok(cmd_result)
    }

    /// Extract the created resource ID from temp_id_mapping
    fn extract_temp_id(cmd_result: &CommandResponse, temp_id: &str) -> String {
        if let Some(temp_mapping) = &cmd_result.temp_id_mapping {
            if let Some(id) = temp_mapping.get(temp_id) {
                id.as_str().unwrap_or(temp_id).to_string()
            } else {
                temp_id.to_string()
            }
        } else {
            temp_id.to_string()
        }
    }

    /// Sync items using the Sync API
    ///
    /// - `sync_token`: Token from previous sync, or None for full sync (use "*" for full sync)
    /// - Returns: SyncResponse with items and new sync_token
    pub async fn sync_items(&self, sync_token: Option<&str>) -> Result<SyncResponse> {
        let url = format!("{}/sync", BASE_URL);
        let sync_token = sync_token.unwrap_or("*");

        // Sync API expects JSON body (not form-urlencoded)
        let body = serde_json::json!({
            "resource_types": ["items"],
            "sync_token": sync_token,
        });

        info!(
            "[TodoistClient] Syncing items: sync_token={} (actual value: '{}')",
            if sync_token == "*" {
                "full_sync"
            } else {
                "incremental"
            },
            sync_token
        );

        // Inject trace context into HTTP headers for distributed tracing
        let mut headers = self.default_headers.clone();
        #[cfg(not(target_arch = "wasm32"))]
        {
            use opentelemetry::Context;
            use opentelemetry::global;

            // Create a carrier for injecting trace context
            struct HeaderInjector {
                headers: reqwest::header::HeaderMap,
            }
            impl opentelemetry::propagation::Injector for HeaderInjector {
                fn set(&mut self, key: &str, value: String) {
                    if let Ok(header_name) = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                    {
                        if let Ok(header_value) = reqwest::header::HeaderValue::from_str(&value) {
                            self.headers.insert(header_name, header_value);
                        }
                    }
                }
            }

            // Inject current trace context into headers
            let mut injector = HeaderInjector { headers };
            global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&Context::current(), &mut injector);
            });
            headers = injector.headers;
        }

        let response_text = self
            .send_with_retry(&url, &body, headers, "sync_items")
            .await?;

        info!(
            "[TodoistClient] Sync response received: length={}",
            response_text.len()
        );

        // Check for error response first
        if let Ok(error_resp) = serde_json::from_str::<serde_json::Value>(&response_text) {
            if error_resp.get("error").is_some() {
                let error = format!(
                    "Todoist API error: {} - {}",
                    error_resp
                        .get("error_tag")
                        .and_then(|v| v.as_str())
                        .unwrap_or("UNKNOWN"),
                    error_resp
                        .get("error")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown error")
                );
                error!("[TodoistClient] {}", error);
                return Err(error.into());
            }
        }

        let response: SyncResponse = serde_json::from_str(&response_text).map_err(|e| {
            let error = format!(
                "Failed to parse SyncResponse: {} - Response (first 500): {}",
                e,
                &response_text.chars().take(500).collect::<String>()
            );
            error!("[TodoistClient] {}", error);
            error
        })?;

        info!(
            "[TodoistClient] Sync completed: items_count={}, sync_token_present={}",
            response.items.len(),
            response.sync_token.is_some()
        );

        Ok(response)
    }

    /// Get all tasks (backward compatibility - performs full sync)
    pub async fn get_all_tasks(&self) -> Result<Vec<TodoistTaskApiResponse>> {
        let response = self.sync_items(None).await?;
        Ok(response.items)
    }

    pub async fn get_task(&self, task_id: &str) -> Result<TodoistTaskApiResponse> {
        // Use sync API to get all items and filter by ID
        let response = self.sync_items(None).await?;
        response
            .items
            .into_iter()
            .find(|item| item.id == task_id)
            .ok_or_else(|| format!("Task {} not found", task_id).into())
    }

    pub async fn get_completed_tasks(
        &self,
        project_id: Option<&str>,
    ) -> Result<Vec<TodoistTaskApiResponse>> {
        // Sync API returns all items including completed ones
        // Filter for completed items (checked = true)
        let response = self.sync_items(None).await?;
        let mut completed = response
            .items
            .into_iter()
            .filter(|item| item.checked.unwrap_or(false))
            .collect::<Vec<_>>();

        // Filter by project_id if specified
        if let Some(project_id) = project_id {
            completed.retain(|item| item.project_id == project_id);
        }

        Ok(completed)
    }

    pub async fn create_task(
        &self,
        request: &CreateTaskRequest<'_>,
    ) -> Result<TodoistTaskApiResponse> {
        let temp_id = Uuid::new_v4().to_string();

        let mut args = json!({
            "content": request.content,
        });

        if let Some(desc) = request.description {
            args["note"] = json!(desc);
        }
        if let Some(project_id) = request.project_id {
            args["project_id"] = json!(project_id);
        }
        if let Some(due_string) = request.due_string {
            args["due_string"] = json!(due_string);
        }
        if let Some(priority) = request.priority {
            args["priority"] = json!(priority);
        }
        if let Some(parent_id) = request.parent_id {
            args["parent_id"] = json!(parent_id);
        }

        let command = SyncCommand {
            command_type: "item_add".to_string(),
            uuid: Uuid::new_v4().to_string(),
            temp_id: Some(temp_id.clone()),
            args,
        };

        let cmd_result = self.execute_command(command).await?;

        // Extract the created item ID from temp_id_mapping
        let item_id = Self::extract_temp_id(&cmd_result, &temp_id);

        // Fetch the created task via sync
        let sync_response = self.sync_items(None).await?;
        sync_response
            .items
            .into_iter()
            .find(|item| item.id == item_id)
            .ok_or_else(|| format!("Created task {} not found in sync", item_id).into())
    }

    pub async fn update_task(&self, task_id: &str, request: &UpdateTaskRequest<'_>) -> Result<()> {
        let mut args = json!({
            "id": task_id,
        });

        if let Some(content) = request.content {
            args["content"] = json!(content);
        }
        if let Some(description) = request.description {
            args["note"] = json!(description);
        }
        if let Some(due_string) = request.due_string {
            args["due_string"] = json!(due_string);
        }
        if let Some(priority) = request.priority {
            args["priority"] = json!(priority);
        }
        if request.clear_parent {
            args["parent_id"] = serde_json::Value::Null;
        } else if let Some(parent_id) = request.parent_id {
            args["parent_id"] = json!(parent_id);
        }

        let command = SyncCommand {
            command_type: "item_update".to_string(),
            uuid: Uuid::new_v4().to_string(),
            temp_id: None,
            args,
        };

        self.execute_command(command).await?;
        Ok(())
    }

    pub async fn move_task(
        &self,
        task_id: &str,
        parent_id: Option<&str>,
        project_id: Option<&str>,
        section_id: Option<&str>,
    ) -> Result<()> {
        let mut args = json!({
            "id": task_id,
        });

        match parent_id {
            Some(pid) => {
                args["parent_id"] = json!(pid);
            }
            None => {
                if let Some(section_id) = section_id {
                    args["section_id"] = json!(section_id);
                } else if let Some(project_id) = project_id {
                    args["project_id"] = json!(project_id);
                } else {
                    return Err(
                        "move_task requires section_id or project_id when parent_id is None".into(),
                    );
                }
            }
        }

        let command = SyncCommand {
            command_type: "item_move".to_string(),
            uuid: Uuid::new_v4().to_string(),
            temp_id: None,
            args,
        };

        self.execute_command(command).await?;
        Ok(())
    }

    pub async fn close_task(&self, task_id: &str) -> Result<()> {
        let command = SyncCommand {
            command_type: "item_close".to_string(),
            uuid: Uuid::new_v4().to_string(),
            temp_id: None,
            args: json!({
                "id": task_id,
            }),
        };

        self.execute_command(command).await?;
        Ok(())
    }

    pub async fn reopen_task(&self, task_id: &str) -> Result<()> {
        let command = SyncCommand {
            command_type: "item_uncomplete".to_string(),
            uuid: Uuid::new_v4().to_string(),
            temp_id: None,
            args: json!({
                "id": task_id,
            }),
        };

        self.execute_command(command).await?;
        Ok(())
    }

    /// Create a project using the Sync API
    pub async fn create_project(&self, name: &str) -> Result<String> {
        let temp_id = Uuid::new_v4().to_string();

        let command = SyncCommand {
            command_type: "project_add".to_string(),
            uuid: Uuid::new_v4().to_string(),
            temp_id: Some(temp_id.clone()),
            args: json!({
                "name": name,
            }),
        };

        let cmd_result = self.execute_command(command).await?;

        // Extract the created project ID from temp_id_mapping
        let project_id = Self::extract_temp_id(&cmd_result, &temp_id);

        Ok(project_id)
    }

    /// Move a project under another project (or to root)
    pub async fn move_project(&self, project_id: &str, parent_id: Option<&str>) -> Result<()> {
        let mut args = json!({
            "id": project_id,
        });

        match parent_id {
            Some(pid) => {
                args["parent_id"] = json!(pid);
            }
            None => {
                args["parent_id"] = json!(null);
            }
        }

        let command = SyncCommand {
            command_type: "project_move".to_string(),
            uuid: Uuid::new_v4().to_string(),
            temp_id: None,
            args,
        };

        self.execute_command(command).await?;
        Ok(())
    }

    pub async fn delete_task(&self, task_id: &str) -> Result<()> {
        let command = SyncCommand {
            command_type: "item_delete".to_string(),
            uuid: Uuid::new_v4().to_string(),
            temp_id: None,
            args: json!({
                "id": task_id,
            }),
        };

        self.execute_command(command).await?;
        Ok(())
    }

    /// Delete a project using the Sync API
    pub async fn delete_project(&self, project_id: &str) -> Result<()> {
        let command = SyncCommand {
            command_type: "project_delete".to_string(),
            uuid: Uuid::new_v4().to_string(),
            temp_id: None,
            args: json!({
                "id": project_id,
            }),
        };

        self.execute_command(command).await?;
        Ok(())
    }

    /// Archive a project and its descendants using the Sync API
    pub async fn archive_project(&self, project_id: &str) -> Result<()> {
        let command = SyncCommand {
            command_type: "project_archive".to_string(),
            uuid: Uuid::new_v4().to_string(),
            temp_id: None,
            args: json!({
                "id": project_id,
            }),
        };

        self.execute_command(command).await?;
        Ok(())
    }

    /// Unarchive a project using the Sync API
    pub async fn unarchive_project(&self, project_id: &str) -> Result<()> {
        let command = SyncCommand {
            command_type: "project_unarchive".to_string(),
            uuid: Uuid::new_v4().to_string(),
            temp_id: None,
            args: json!({
                "id": project_id,
            }),
        };

        self.execute_command(command).await?;
        Ok(())
    }

    /// Sync projects using the Sync API
    pub async fn sync_projects(&self, sync_token: Option<&str>) -> Result<serde_json::Value> {
        let url = format!("{}/sync", BASE_URL);
        let sync_token = sync_token.unwrap_or("*");

        let body = serde_json::json!({
            "resource_types": ["projects"],
            "sync_token": sync_token,
        });

        let response_text = self
            .send_with_retry(&url, &body, self.default_headers.clone(), "sync_projects")
            .await?;
        let sync_resp: serde_json::Value = serde_json::from_str(&response_text)?;
        Ok(sync_resp)
    }
}

/// Implementation of TodoistApiClient trait for TodoistClient.
///
/// This allows the real HTTP client to be used interchangeably with
/// the fake client through the trait abstraction.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TodoistApiClient for TodoistClient {
    async fn sync_items(&self, sync_token: Option<&str>) -> ApiResult<SyncResponse> {
        self.sync_items(sync_token).await
    }

    async fn sync_projects(&self, sync_token: Option<&str>) -> ApiResult<serde_json::Value> {
        self.sync_projects(sync_token).await
    }

    async fn create_task(
        &self,
        request: &CreateTaskRequest<'_>,
    ) -> ApiResult<TodoistTaskApiResponse> {
        self.create_task(request).await
    }

    async fn update_task(&self, task_id: &str, request: &UpdateTaskRequest<'_>) -> ApiResult<()> {
        self.update_task(task_id, request).await
    }

    async fn close_task(&self, task_id: &str) -> ApiResult<()> {
        self.close_task(task_id).await
    }

    async fn reopen_task(&self, task_id: &str) -> ApiResult<()> {
        self.reopen_task(task_id).await
    }

    async fn delete_task(&self, task_id: &str) -> ApiResult<()> {
        self.delete_task(task_id).await
    }

    async fn move_task(
        &self,
        task_id: &str,
        parent_id: Option<&str>,
        project_id: Option<&str>,
        section_id: Option<&str>,
    ) -> ApiResult<()> {
        self.move_task(task_id, parent_id, project_id, section_id)
            .await
    }

    async fn create_project(&self, name: &str) -> ApiResult<String> {
        self.create_project(name).await
    }

    async fn move_project(&self, project_id: &str, parent_id: Option<&str>) -> ApiResult<()> {
        self.move_project(project_id, parent_id).await
    }

    async fn delete_project(&self, project_id: &str) -> ApiResult<()> {
        self.delete_project(project_id).await
    }

    async fn archive_project(&self, project_id: &str) -> ApiResult<()> {
        self.archive_project(project_id).await
    }

    async fn unarchive_project(&self, project_id: &str) -> ApiResult<()> {
        self.unarchive_project(project_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = TodoistClient::new("test_api_key_12345");
        assert_eq!(
            client.default_headers.get("Authorization").unwrap(),
            "Bearer test_api_key_12345"
        );
    }
}
