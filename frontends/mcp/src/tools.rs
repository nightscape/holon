use crate::server::HolonMcpServer;
use crate::types::*;
use holon::api::backend_engine::{BackendEngine, QueryContext};
use holon::storage::types::StorageEntity;
use holon_api::{Change, Value};
use rmcp::{handler::server::wrapper::Parameters, model::*, tool, tool_router};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use uuid::Uuid;

/// Build a QueryContext from explicit context_id/context_parent_id values.
/// Also looks up the block path so `from descendants` works correctly.
async fn build_context(
    engine: &BackendEngine,
    context_id: Option<&str>,
    context_parent_id: Option<&str>,
) -> Option<QueryContext> {
    match context_id {
        Some(id) => {
            let path = engine
                .lookup_block_path(id)
                .await
                .unwrap_or_else(|_| format!("/{}", id));
            Some(QueryContext::for_block_with_path(
                id.to_string(),
                context_parent_id.map(String::from),
                path,
            ))
        }
        None => None,
    }
}

/// Extract context_id/context_parent_id from a generic params map and build QueryContext.
async fn extract_context_from_params(
    engine: &BackendEngine,
    params: &HashMap<String, serde_json::Value>,
) -> Option<QueryContext> {
    let context_id = params.get("context_id").and_then(|v| v.as_str());
    let context_parent_id = params.get("context_parent_id").and_then(|v| v.as_str());
    build_context(engine, context_id, context_parent_id).await
}

// Helper function to convert serde_json::Value to holon_api::Value
fn json_to_holon_value(v: serde_json::Value) -> Value {
    Value::from_json_value(v)
}

// Helper function to convert holon_api::Value to serde_json::Value
fn holon_to_json_value(v: &Value) -> serde_json::Value {
    match v {
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Integer(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Value::Number(
            serde_json::Number::from_f64(*f).unwrap_or_else(|| serde_json::Number::from(0)),
        ),
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::DateTime(s) => serde_json::Value::String(s.clone()),
        Value::Json(s) => serde_json::from_str(s).unwrap_or(serde_json::Value::String(s.clone())),
        Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(holon_to_json_value).collect())
        }
        Value::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (k, v) in obj {
                map.insert(k.clone(), holon_to_json_value(v));
            }
            serde_json::Value::Object(map)
        }
        Value::Null => serde_json::Value::Null,
    }
}

// Helper function to convert HashMap<String, serde_json::Value> to StorageEntity
fn json_map_to_storage_entity(map: HashMap<String, serde_json::Value>) -> StorageEntity {
    map.into_iter()
        .map(|(k, v)| (k, json_to_holon_value(v)))
        .collect()
}

// Helper function to expose tool_router
pub(crate) fn get_tool_router() -> rmcp::handler::server::router::tool::ToolRouter<HolonMcpServer> {
    HolonMcpServer::tool_router()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_to_holon_string() {
        let v = json_to_holon_value(serde_json::json!("hello"));
        assert_eq!(v, Value::String("hello".into()));
    }

    #[test]
    fn json_to_holon_integer() {
        let v = json_to_holon_value(serde_json::json!(42));
        assert_eq!(v, Value::Integer(42));
    }

    #[test]
    fn json_to_holon_float() {
        let v = json_to_holon_value(serde_json::json!(3.14));
        assert_eq!(v, Value::Float(3.14));
    }

    #[test]
    fn json_to_holon_bool() {
        let v = json_to_holon_value(serde_json::json!(true));
        assert_eq!(v, Value::Boolean(true));
    }

    #[test]
    fn json_to_holon_null() {
        let v = json_to_holon_value(serde_json::json!(null));
        assert_eq!(v, Value::Null);
    }

    #[test]
    fn json_to_holon_array() {
        let v = json_to_holon_value(serde_json::json!([1, "two"]));
        match v {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], Value::Integer(1));
                assert_eq!(arr[1], Value::String("two".into()));
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn json_to_holon_object() {
        let v = json_to_holon_value(serde_json::json!({"key": "value"}));
        match v {
            Value::Object(map) => {
                assert_eq!(map.get("key").unwrap(), &Value::String("value".into()));
            }
            _ => panic!("expected Object"),
        }
    }

    #[test]
    fn holon_to_json_string() {
        let v = holon_to_json_value(&Value::String("hello".into()));
        assert_eq!(v, serde_json::json!("hello"));
    }

    #[test]
    fn holon_to_json_integer() {
        let v = holon_to_json_value(&Value::Integer(42));
        assert_eq!(v, serde_json::json!(42));
    }

    #[test]
    fn holon_to_json_float() {
        let v = holon_to_json_value(&Value::Float(3.14));
        assert_eq!(v, serde_json::json!(3.14));
    }

    #[test]
    fn holon_to_json_bool() {
        let v = holon_to_json_value(&Value::Boolean(false));
        assert_eq!(v, serde_json::json!(false));
    }

    #[test]
    fn holon_to_json_null() {
        let v = holon_to_json_value(&Value::Null);
        assert_eq!(v, serde_json::Value::Null);
    }

    #[test]
    fn holon_to_json_datetime() {
        let v = holon_to_json_value(&Value::DateTime("2024-01-01T00:00:00Z".into()));
        assert_eq!(v, serde_json::json!("2024-01-01T00:00:00Z"));
    }

    #[test]
    fn holon_to_json_valid_json_string_is_parsed() {
        let v = holon_to_json_value(&Value::Json(r#"{"nested": true}"#.into()));
        assert_eq!(v, serde_json::json!({"nested": true}));
    }

    #[test]
    fn holon_to_json_invalid_json_falls_back_to_string() {
        let v = holon_to_json_value(&Value::Json("not json".into()));
        assert_eq!(v, serde_json::json!("not json"));
    }

    #[test]
    fn holon_to_json_array() {
        let v = holon_to_json_value(&Value::Array(vec![
            Value::Integer(1),
            Value::String("two".into()),
        ]));
        assert_eq!(v, serde_json::json!([1, "two"]));
    }

    #[test]
    fn holon_to_json_object() {
        let mut map = HashMap::new();
        map.insert("k".into(), Value::Boolean(true));
        let v = holon_to_json_value(&Value::Object(map));
        assert_eq!(v, serde_json::json!({"k": true}));
    }

    #[test]
    fn roundtrip_json_to_holon_to_json() {
        let original = serde_json::json!({
            "name": "test",
            "count": 42,
            "active": true,
            "tags": ["a", "b"],
            "meta": null
        });
        let holon = json_to_holon_value(original.clone());
        let back = holon_to_json_value(&holon);
        assert_eq!(original, back);
    }

    #[test]
    fn json_map_to_storage_entity_converts_all_fields() {
        let mut map = HashMap::new();
        map.insert("id".into(), serde_json::json!("block-1"));
        map.insert("priority".into(), serde_json::json!(3));
        let entity = json_map_to_storage_entity(map);
        assert_eq!(entity.get("id").unwrap(), &Value::String("block-1".into()));
        assert_eq!(entity.get("priority").unwrap(), &Value::Integer(3));
    }
}

#[tool_router]
impl HolonMcpServer {
    #[tool(description = "Create a table with specified schema")]
    async fn create_table(
        &self,
        Parameters(params): Parameters<CreateTableParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let mut sql_parts = vec![
            "CREATE TABLE IF NOT EXISTS".to_string(),
            params.table_name.clone(),
            "(".to_string(),
        ];
        let mut column_defs = Vec::new();

        for col in &params.columns {
            let mut col_def = format!("{} {}", col.name, col.sql_type);
            if col.primary_key {
                col_def.push_str(" PRIMARY KEY");
            }
            if let Some(ref default) = col.default {
                col_def.push_str(&format!(" DEFAULT {}", default));
            }
            column_defs.push(col_def);
        }

        sql_parts.push(column_defs.join(", "));
        sql_parts.push(")".to_string());
        let sql = sql_parts.join(" ");

        self.engine
            .execute_query(sql.clone(), HashMap::new(), None)
            .await
            .map_err(|e| {
                rmcp::ErrorData::internal_error(
                    format!("Failed to create table '{}': {}", params.table_name, e),
                    Some(serde_json::json!({"sql": sql})),
                )
            })?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Table '{}' created successfully",
            params.table_name
        ))]))
    }

    #[tool(description = "Insert rows into a table")]
    async fn insert_data(
        &self,
        Parameters(params): Parameters<InsertDataParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if params.rows.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "0 rows inserted".to_string(),
            )]));
        }

        // Get column names from first row
        let columns: Vec<String> = params.rows[0].keys().cloned().collect();
        let placeholders: Vec<String> = (0..columns.len()).map(|i| format!("${}", i + 1)).collect();

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            params.table_name,
            columns.join(", "),
            placeholders.join(", ")
        );

        let mut row_count = 0;
        for row in &params.rows {
            let mut values = HashMap::new();
            for (i, col) in columns.iter().enumerate() {
                if let Some(val) = row.get(col) {
                    values.insert(format!("{}", i + 1), json_to_holon_value(val.clone()));
                }
            }

            // Replace placeholders with actual values in SQL (simple approach)
            // For production, should use parameterized queries properly
            let mut row_sql = sql.clone();
            for (i, col) in columns.iter().enumerate() {
                if let Some(val) = row.get(col) {
                    let sql_val = match val {
                        serde_json::Value::String(s) => format!("'{}'", s.replace("'", "''")),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
                        serde_json::Value::Null => "NULL".to_string(),
                        _ => format!("'{}'", val.to_string().replace("'", "''")),
                    };
                    row_sql = row_sql.replace(&format!("${}", i + 1), &sql_val);
                }
            }

            self.engine
                .execute_query(row_sql.clone(), HashMap::new(), None)
                .await
                .map_err(|e| {
                    rmcp::ErrorData::internal_error(
                        format!("Failed to insert into '{}': {}", params.table_name, e),
                        Some(serde_json::json!({"sql": row_sql, "row_index": row_count})),
                    )
                })?;
            row_count += 1;
        }

        Ok(CallToolResult::success(vec![Content::text(format!(
            "{} rows inserted",
            row_count
        ))]))
    }

    #[tool(description = "Drop a table")]
    async fn drop_table(
        &self,
        Parameters(params): Parameters<DropTableParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let sql = format!("DROP TABLE IF EXISTS {}", params.table_name);

        self.engine
            .execute_query(sql.clone(), HashMap::new(), None)
            .await
            .map_err(|e| {
                rmcp::ErrorData::internal_error(
                    format!("Failed to drop table '{}': {}", params.table_name, e),
                    Some(serde_json::json!({"sql": sql})),
                )
            })?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Table '{}' dropped successfully",
            params.table_name
        ))]))
    }

    #[tool(
        description = "Execute a PRQL query and return results. IMPORTANT: Queries must end with a render() call that specifies UI rendering, e.g., 'render (list sortkey:id item_template:(row (text this.name)))'"
    )]
    async fn execute_prql(
        &self,
        Parameters(params): Parameters<ExecutePrqlParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let context = build_context(
            &self.engine,
            params.context_id.as_deref(),
            params.context_parent_id.as_deref(),
        )
        .await;

        // Convert params from serde_json::Value to holon_api::Value
        let mut holon_params = HashMap::new();
        for (k, v) in &params.params {
            holon_params.insert(k.clone(), json_to_holon_value(v.clone()));
        }

        // Compile PRQL to SQL
        let (sql, _render_spec) = self
            .engine
            .compile_query(params.prql.clone(), None)
            .map_err(|e| {
                rmcp::ErrorData::internal_error(
                    format!("PRQL compilation failed: {}", e),
                    Some(serde_json::json!({"prql": params.prql})),
                )
            })?;

        // Execute query
        let rows = self
            .engine
            .execute_query(sql.clone(), holon_params, context)
            .await
            .map_err(|e| {
                rmcp::ErrorData::internal_error(
                    format!("PRQL execution failed: {}", e),
                    Some(serde_json::json!({"prql": params.prql, "sql": sql})),
                )
            })?;

        // Convert rows to JSON
        let json_rows: Vec<HashMap<String, serde_json::Value>> = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|(k, v)| (k.clone(), holon_to_json_value(v)))
                    .collect()
            })
            .collect();

        let result = QueryResult {
            rows: json_rows.clone(),
            row_count: json_rows.len(),
        };

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string(&result).map_err(|e| {
                rmcp::ErrorData::internal_error(
                    "serialization_failed",
                    Some(serde_json::json!({"error": e.to_string()})),
                )
            })?,
        )]))
    }

    #[tool(
        description = "Execute a SQL query and return results. No render() required - use this for raw SQL operations (SELECT, INSERT, UPDATE, DELETE)"
    )]
    async fn execute_sql(
        &self,
        Parameters(params): Parameters<ExecuteSqlParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        // Convert params from serde_json::Value to holon_api::Value
        let mut holon_params = HashMap::new();
        for (k, v) in &params.params {
            holon_params.insert(k.clone(), json_to_holon_value(v.clone()));
        }

        // Execute query
        let rows = self
            .engine
            .execute_query(params.sql.clone(), holon_params, None)
            .await
            .map_err(|e| {
                rmcp::ErrorData::internal_error(
                    format!("SQL execution failed: {}", e),
                    Some(serde_json::json!({"sql": params.sql})),
                )
            })?;

        // Convert rows to JSON
        let json_rows: Vec<HashMap<String, serde_json::Value>> = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|(k, v)| (k.clone(), holon_to_json_value(v)))
                    .collect()
            })
            .collect();

        let result = QueryResult {
            rows: json_rows.clone(),
            row_count: json_rows.len(),
        };

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string(&result).map_err(|e| {
                rmcp::ErrorData::internal_error(
                    "serialization_failed",
                    Some(serde_json::json!({"error": e.to_string()})),
                )
            })?,
        )]))
    }

    #[tool(
        description = "Start watching a PRQL query for CDC changes. IMPORTANT: Queries must end with a render() call that specifies UI rendering"
    )]
    async fn watch_query(
        &self,
        Parameters(params): Parameters<WatchQueryParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let context = extract_context_from_params(&self.engine, &params.params).await;

        // Convert params from serde_json::Value to holon_api::Value
        let mut holon_params = HashMap::new();
        for (k, v) in &params.params {
            holon_params.insert(k.clone(), json_to_holon_value(v.clone()));
        }

        // Query and watch
        let (widget_spec, stream) = self
            .engine
            .query_and_watch(params.prql.clone(), holon_params, context)
            .await
            .map_err(|e| {
                rmcp::ErrorData::internal_error(
                    format!("Watch query failed: {}", e),
                    Some(serde_json::json!({"prql": params.prql})),
                )
            })?;

        // Convert initial data to JSON
        let json_initial_data: Vec<HashMap<String, serde_json::Value>> = widget_spec
            .data
            .iter()
            .map(|row: &HashMap<String, holon_api::Value>| {
                row.iter()
                    .map(|(k, v): (&String, &holon_api::Value)| (k.clone(), holon_to_json_value(v)))
                    .collect()
            })
            .collect();

        // Generate watch ID
        let watch_id = Uuid::new_v4().to_string();

        // Create pending changes buffer
        let pending_changes = Arc::new(Mutex::new(Vec::<RowChangeJson>::new()));
        let pending_changes_clone = pending_changes.clone();

        // Spawn background task to collect changes
        let task_handle = tokio::spawn(async move {
            let mut stream = stream;
            while let Some(batch) = stream.next().await {
                let mut changes = pending_changes_clone.lock().await;
                for row_change in batch.inner.items {
                    let change: &holon_api::Change<HashMap<String, holon_api::Value>> =
                        &row_change.change;
                    let change_json = RowChangeJson {
                        change_type: match change {
                            Change::Created { .. } => "Created".to_string(),
                            Change::Updated { .. } => "Updated".to_string(),
                            Change::Deleted { .. } => "Deleted".to_string(),
                            Change::FieldsChanged { .. } => "Updated".to_string(),
                        },
                        entity_id: match change {
                            Change::Created { data, .. } => data
                                .get("id")
                                .and_then(|v: &holon_api::Value| v.as_string_owned()),
                            Change::Updated { id, .. } => Some(id.clone()),
                            Change::Deleted { id, .. } => Some(id.clone()),
                            Change::FieldsChanged { entity_id, .. } => Some(entity_id.clone()),
                        },
                        data: match change {
                            Change::Created { data, .. } => Some(
                                data.iter()
                                    .map(|(k, v): (&String, &holon_api::Value)| {
                                        (k.clone(), holon_to_json_value(v))
                                    })
                                    .collect(),
                            ),
                            Change::Updated { data, .. } => Some(
                                data.iter()
                                    .map(|(k, v): (&String, &holon_api::Value)| {
                                        (k.clone(), holon_to_json_value(v))
                                    })
                                    .collect(),
                            ),
                            Change::Deleted { .. } => None,
                            Change::FieldsChanged { fields, .. } => {
                                // Convert fields vec to a map
                                let mut map = HashMap::new();
                                for (field_name, _old_val, new_val) in fields {
                                    map.insert(field_name.clone(), holon_to_json_value(&new_val));
                                }
                                Some(map)
                            }
                        },
                    };
                    changes.push(change_json);
                }
            }
        });

        // Store watch state
        let mut watches = self.watches.lock().await;
        watches.insert(
            watch_id.clone(),
            crate::server::WatchState {
                pending_changes,
                _task_handle: task_handle,
            },
        );

        let handle = WatchHandle {
            watch_id: watch_id.clone(),
            initial_data: json_initial_data,
        };

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string(&handle).map_err(|e| {
                rmcp::ErrorData::internal_error(
                    "serialization_failed",
                    Some(serde_json::json!({"error": e.to_string()})),
                )
            })?,
        )]))
    }

    #[tool(description = "Poll for accumulated CDC changes")]
    async fn poll_changes(
        &self,
        Parameters(params): Parameters<PollChangesParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let mut watches = self.watches.lock().await;

        let watch_state = watches.get_mut(&params.watch_id).ok_or_else(|| {
            rmcp::ErrorData::invalid_params(
                "watch_not_found",
                Some(serde_json::json!({"watch_id": params.watch_id})),
            )
        })?;

        let mut changes = watch_state.pending_changes.lock().await;
        let result = changes.drain(..).collect::<Vec<_>>();

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string(&result).map_err(|e| {
                rmcp::ErrorData::internal_error(
                    "serialization_failed",
                    Some(serde_json::json!({"error": e.to_string()})),
                )
            })?,
        )]))
    }

    #[tool(description = "Stop watching a query")]
    async fn stop_watch(
        &self,
        Parameters(params): Parameters<StopWatchParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let mut watches = self.watches.lock().await;

        watches.remove(&params.watch_id).ok_or_else(|| {
            rmcp::ErrorData::invalid_params(
                "watch_not_found",
                Some(serde_json::json!({"watch_id": params.watch_id})),
            )
        })?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Watch '{}' stopped successfully",
            params.watch_id
        ))]))
    }

    #[tool(
        description = "Execute an operation on an entity. Use list_operations first to discover available operations and their required parameters"
    )]
    async fn execute_operation(
        &self,
        Parameters(params): Parameters<ExecuteOperationParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        // Convert params to StorageEntity
        let storage_entity = json_map_to_storage_entity(params.params);

        // Execute operation
        self.engine.execute_operation(&params.entity_name, &params.operation, storage_entity)
            .await
            .map_err(|e| rmcp::ErrorData::internal_error(
                format!("Operation '{}' on '{}' failed: {}", params.operation, params.entity_name, e),
                Some(serde_json::json!({"entity": params.entity_name, "operation": params.operation}))
            ))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Operation '{}' on entity '{}' executed successfully",
            params.operation, params.entity_name
        ))]))
    }

    #[tool(
        description = "List available operations for an entity. Returns operation names, required parameters, and descriptions. Common entities: blocks, directories, documents"
    )]
    async fn list_operations(
        &self,
        Parameters(params): Parameters<ListOperationsParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let ops = self.engine.available_operations(&params.entity_name).await;

        // Convert OperationDescriptor to JSON
        let json_ops: Vec<serde_json::Value> = ops
            .iter()
            .map(|op| {
                serde_json::json!({
                    "entity_name": op.entity_name,
                    "entity_short_name": op.entity_short_name,
                    "id_column": op.id_column,
                    "name": op.name,
                    "display_name": op.display_name,
                    "description": op.description,
                    "required_params": op.required_params.iter().map(|p| serde_json::json!({
                        "name": p.name,
                        "type_hint": format!("{:?}", p.type_hint),
                        "description": p.description,
                    })).collect::<Vec<_>>(),
                    "affected_fields": op.affected_fields,
                })
            })
            .collect();

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string(&json_ops).map_err(|e| {
                rmcp::ErrorData::internal_error(
                    "serialization_failed",
                    Some(serde_json::json!({"error": e.to_string()})),
                )
            })?,
        )]))
    }

    #[tool(description = "Undo the last operation")]
    async fn undo(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        let result = self.engine.undo().await;

        match result {
            Ok(success) => {
                let undo_result = UndoRedoResult {
                    success,
                    message: if success {
                        "Operation undone successfully".to_string()
                    } else {
                        "Nothing to undo".to_string()
                    },
                };
                Ok(CallToolResult::success(vec![Content::text(
                    serde_json::to_string(&undo_result).map_err(|e| {
                        rmcp::ErrorData::internal_error(
                            "serialization_failed",
                            Some(serde_json::json!({"error": e.to_string()})),
                        )
                    })?,
                )]))
            }
            Err(e) => Err(rmcp::ErrorData::internal_error(
                format!("Undo failed: {}", e),
                None,
            )),
        }
    }

    #[tool(description = "Redo the last undone operation")]
    async fn redo(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        let result = self.engine.redo().await;

        match result {
            Ok(success) => {
                let redo_result = UndoRedoResult {
                    success,
                    message: if success {
                        "Operation redone successfully".to_string()
                    } else {
                        "Nothing to redo".to_string()
                    },
                };
                Ok(CallToolResult::success(vec![Content::text(
                    serde_json::to_string(&redo_result).map_err(|e| {
                        rmcp::ErrorData::internal_error(
                            "serialization_failed",
                            Some(serde_json::json!({"error": e.to_string()})),
                        )
                    })?,
                )]))
            }
            Err(e) => Err(rmcp::ErrorData::internal_error(
                format!("Redo failed: {}", e),
                None,
            )),
        }
    }

    #[tool(description = "Check if undo is available")]
    async fn can_undo(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        let available = self.engine.can_undo().await;
        let result = CanUndoRedoResult { available };

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string(&result).map_err(|e| {
                rmcp::ErrorData::internal_error(
                    "serialization_failed",
                    Some(serde_json::json!({"error": e.to_string()})),
                )
            })?,
        )]))
    }

    #[tool(description = "Check if redo is available")]
    async fn can_redo(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        let available = self.engine.can_redo().await;
        let result = CanUndoRedoResult { available };

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string(&result).map_err(|e| {
                rmcp::ErrorData::internal_error(
                    "serialization_failed",
                    Some(serde_json::json!({"error": e.to_string()})),
                )
            })?,
        )]))
    }
}
