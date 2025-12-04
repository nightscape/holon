use crate::references::view_config::{Order, ViewConfig};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BlockReference {
    Internal {
        block_id: String,
    },
    External {
        system: String,
        entity_type: String,
        entity_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        view: Option<ViewConfig>,
    },
}

impl BlockReference {
    pub fn internal(block_id: impl Into<String>) -> Self {
        Self::Internal {
            block_id: block_id.into(),
        }
    }

    pub fn external(
        system: impl Into<String>,
        entity_type: impl Into<String>,
        entity_id: impl Into<String>,
    ) -> Self {
        Self::External {
            system: system.into(),
            entity_type: entity_type.into(),
            entity_id: entity_id.into(),
            view: None,
        }
    }

    pub fn with_view(mut self, view: ViewConfig) -> Self {
        if let Self::External {
            view: view_slot, ..
        } = &mut self
        {
            *view_slot = Some(view);
        }
        self
    }

    pub fn todoist_section(id: impl Into<String>) -> Self {
        Self::External {
            system: "todoist".into(),
            entity_type: "section".into(),
            entity_id: id.into(),
            view: Some(ViewConfig {
                show_completed: false,
                sort_by: vec![("priority".into(), Order::Desc)],
                max_depth: Some(2),
                ..Default::default()
            }),
        }
    }

    pub fn todoist_project(id: impl Into<String>) -> Self {
        Self::External {
            system: "todoist".into(),
            entity_type: "project".into(),
            entity_id: id.into(),
            view: Some(ViewConfig {
                show_completed: false,
                sort_by: vec![("order".into(), Order::Asc)],
                max_depth: None,
                ..Default::default()
            }),
        }
    }
}
