use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct CreateTableParams {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ColumnDef {
    pub name: String,
    pub sql_type: String, // TEXT, INTEGER, BOOLEAN, etc.
    #[serde(default)]
    pub primary_key: bool,
    pub default: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InsertDataParams {
    pub table_name: String,
    pub rows: Vec<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ExecutePrqlParams {
    pub prql: String,
    #[serde(default)]
    pub params: HashMap<String, serde_json::Value>,
    /// Block ID for `from children` context resolution. When set, `from children` returns
    /// children of this block. Without this, `from children` returns empty results.
    pub context_id: Option<String>,
    /// Parent block ID for `from siblings` context resolution.
    pub context_parent_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ExecuteSqlParams {
    pub sql: String,
    #[serde(default)]
    pub params: HashMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ExecuteOperationParams {
    pub entity_name: String,
    pub operation: String,
    pub params: HashMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct WatchQueryParams {
    pub prql: String,
    #[serde(default)]
    pub params: HashMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct WatchHandle {
    pub watch_id: String,
    pub initial_data: Vec<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct QueryResult {
    pub rows: Vec<HashMap<String, serde_json::Value>>,
    pub row_count: usize,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct RowChangeJson {
    pub change_type: String, // "Created", "Updated", "Deleted"
    pub entity_id: Option<String>,
    pub data: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct DropTableParams {
    pub table_name: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ListOperationsParams {
    pub entity_name: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct StopWatchParams {
    pub watch_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct PollChangesParams {
    pub watch_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct UndoRedoResult {
    pub success: bool,
    pub message: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct CanUndoRedoResult {
    pub available: bool,
}
