use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncStats {
    pub inserted: usize,
    pub updated: usize,
    pub pushed: usize,
    pub conflicts: Vec<ConflictInfo>,
    pub errors: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictInfo {
    pub entity_id: String,
    pub field: String,
    pub local_value: String,
    pub remote_value: String,
}
