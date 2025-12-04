use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ViewConfig {
    pub show_completed: bool,
    pub group_by: Option<String>,
    pub sort_by: Vec<(String, Order)>,
    pub max_depth: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Order {
    Asc,
    Desc,
}
