//! Widget specification types for backend-driven UI
//!
//! WidgetSpec is the unified return type for all rendered widgets.
//! The backend executes queries and returns both the render spec and data.
//! The frontend just renders using RenderInterpreter.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::render_types::RenderSpec;
use crate::Value;

/// Specification for rendering a widget
///
/// WidgetSpec is the unified type returned by query_and_watch and initial_widget.
/// It contains everything the frontend needs to render:
/// - render_spec: How to render (e.g., columns, list, tree)
/// - data: The query result data (rows)
/// - actions: Available actions (may be empty for non-root widgets)
///
/// The change stream is returned separately (not in this struct) since
/// streams are not serializable for FFI.
///
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetSpec {
    /// Render spec defining how to render (parsed from PRQL render directive)
    pub render_spec: RenderSpec,

    /// Query result data (rows to render)
    pub data: Vec<HashMap<String, Value>>,

    /// Available actions (global actions for root, contextual for others)
    pub actions: Vec<ActionSpec>,
}

impl WidgetSpec {
    /// Create a new WidgetSpec
    pub fn new(render_spec: RenderSpec, data: Vec<HashMap<String, Value>>) -> Self {
        Self {
            render_spec,
            data,
            actions: Vec::new(),
        }
    }

    /// Add an action
    pub fn with_action(mut self, action: ActionSpec) -> Self {
        self.actions.push(action);
        self
    }

    /// Set actions
    pub fn with_actions(mut self, actions: Vec<ActionSpec>) -> Self {
        self.actions = actions;
        self
    }
}

/// Specification for a global action
///
/// Actions are operations that can be triggered from anywhere in the app,
/// like sync, settings, or undo/redo.
///
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionSpec {
    /// Unique identifier
    pub id: String,

    /// Human-readable display name
    pub display_name: String,

    /// Optional icon name (e.g., "sync", "settings")
    pub icon: Option<String>,

    /// The operation to execute when this action is triggered
    pub operation: ActionOperation,
}

/// Operation to execute for an action
///
/// flutter_rust_bridge:non_opaque
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionOperation {
    /// Entity name (e.g., "*" for wildcard operations)
    pub entity_name: String,

    /// Operation name (e.g., "sync_from_remote")
    pub op_name: String,

    /// Default parameters for this operation
    pub params: HashMap<String, Value>,
}

impl ActionSpec {
    /// Create a new action specification
    pub fn new(
        id: impl Into<String>,
        display_name: impl Into<String>,
        entity_name: impl Into<String>,
        op_name: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            display_name: display_name.into(),
            icon: None,
            operation: ActionOperation {
                entity_name: entity_name.into(),
                op_name: op_name.into(),
                params: HashMap::new(),
            },
        }
    }

    /// Set the icon
    pub fn with_icon(mut self, icon: impl Into<String>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    /// Add a parameter
    pub fn with_param(mut self, key: impl Into<String>, value: Value) -> Self {
        self.operation.params.insert(key.into(), value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_types::{RenderExpr, RenderSpec};

    #[test]
    fn test_widget_spec_builder() {
        let render_spec = RenderSpec {
            root: RenderExpr::FunctionCall {
                name: "columns".to_string(),
                args: vec![],
                operations: vec![],
            },
            default_view: "default".to_string(),
            views: HashMap::new(),
            nested_queries: vec![],
            operations: HashMap::new(),
            row_templates: vec![],
        };

        let spec = WidgetSpec::new(render_spec, vec![]).with_action(
            ActionSpec::new("sync", "Sync", "*", "sync_from_remote").with_icon("sync"),
        );

        assert_eq!(spec.actions.len(), 1);
        assert!(spec.data.is_empty());
    }
}
