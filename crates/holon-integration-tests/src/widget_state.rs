//! Widget state tracking for Cucumber integration tests
//!
//! Provides a model to track UI state by applying CDC events to initial WidgetSpec data.
//! Enables text-based assertions for realistic end-to-end testing.

use std::collections::HashMap;

use holon::api::{ChangeData, RowChange};
use holon_api::render_types::{FilterExpr, RenderExpr, RenderSpec};
use holon_api::{Value, WidgetSpec};
use indexmap::IndexMap;
use std::collections::HashSet;

/// Apply a CDC event to a Vec-based row collection.
///
/// This is the shared implementation used by both PBT tests (directly) and
/// Cucumber tests (through WidgetStateModel). It handles all ChangeData variants:
/// - Created: appends the new row
/// - Updated: replaces all fields in the matching row
/// - FieldsChanged: updates specific fields in the matching row
/// - Deleted: removes the row with matching entity ID
///
/// Rows are matched by their "id" field.
pub fn apply_cdc_event_to_vec(rows: &mut Vec<HashMap<String, Value>>, event: &RowChange) {
    match &event.change {
        ChangeData::Created { data, .. } => {
            rows.push(data.clone());
        }
        ChangeData::Updated { data, .. } => {
            if let Some(entity_id) = data.get("id").and_then(|v| v.as_string()) {
                if let Some(row) = rows.iter_mut().find(|r| {
                    r.get("id")
                        .and_then(|v| v.as_string())
                        .map(|s| s == entity_id)
                        .unwrap_or(false)
                }) {
                    for (k, v) in data {
                        row.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        ChangeData::FieldsChanged {
            entity_id, fields, ..
        } => {
            if let Some(row) = rows.iter_mut().find(|r| {
                r.get("id")
                    .and_then(|v| v.as_string())
                    .map(|s| s == *entity_id)
                    .unwrap_or(false)
            }) {
                for (field_name, _old_value, new_value) in fields {
                    row.insert(field_name.clone(), new_value.clone());
                }
            }
        }
        ChangeData::Deleted { id: rowid, .. } => {
            rows.retain(|r| {
                r.get("id")
                    .and_then(|v| v.as_string())
                    .map(|s| s != *rowid)
                    .unwrap_or(true)
            });
        }
    }
}

/// Widget locator for targeting specific widgets in assertions.
///
/// Designed for extensibility - "column 1" is just one locator type.
/// Future expansion could include path-based selectors like "main-view > list > item 3".
#[derive(Debug, Clone)]
pub enum WidgetLocator {
    /// Match by column index (1-based): "column 1", "column 2"
    Column(usize),
    /// Match by view ID: "sidebar", "main-view"
    ViewId(String),
    /// Match all widgets (for global assertions)
    All,
}

impl WidgetLocator {
    /// Parse a widget locator from a Gherkin step string.
    ///
    /// Supports:
    /// - "column 1", "column 2" → Column(n)
    /// - "sidebar", "main-view" → ViewId(s)
    /// - "all" → All
    pub fn parse(s: &str) -> Self {
        if s == "all" {
            return WidgetLocator::All;
        }
        if let Some(n_str) = s.strip_prefix("column ") {
            if let Ok(n) = n_str.parse::<usize>() {
                return WidgetLocator::Column(n);
            }
        }
        WidgetLocator::ViewId(s.to_string())
    }
}

/// Tracks the current state of a widget by applying CDC events to initial data.
///
/// Maintains an ordered collection of rows (preserving query result order) and
/// provides text extraction for assertions.
pub struct WidgetStateModel {
    /// Current data rows keyed by entity ID, preserving insertion order
    rows: IndexMap<String, HashMap<String, Value>>,
    /// The render specification (for view identification)
    render_spec: RenderSpec,
}

impl WidgetStateModel {
    /// Create a new WidgetStateModel from an initial WidgetSpec.
    pub fn from_widget_spec(spec: &WidgetSpec) -> Self {
        let mut rows = IndexMap::new();
        for row in &spec.data {
            if let Some(id) = row.get("id").and_then(|v| v.as_string()) {
                rows.insert(id.to_string(), row.clone());
            }
        }
        Self {
            rows,
            render_spec: spec.render_spec.clone(),
        }
    }

    /// Apply a CDC change event to update the state.
    pub fn apply_change(&mut self, change: &RowChange) {
        match &change.change {
            ChangeData::Created { data, .. } => {
                if let Some(id) = data.get("id").and_then(|v| v.as_string()) {
                    self.rows.insert(id.to_string(), data.clone());
                }
            }
            ChangeData::Updated { data, .. } => {
                if let Some(id) = data.get("id").and_then(|v| v.as_string()) {
                    self.rows.insert(id.to_string(), data.clone());
                }
            }
            ChangeData::Deleted { id, .. } => {
                self.rows.shift_remove(id);
            }
            ChangeData::FieldsChanged {
                entity_id, fields, ..
            } => {
                if let Some(row) = self.rows.get_mut(entity_id) {
                    for (field, _old, new) in fields {
                        row.insert(field.clone(), new.clone());
                    }
                }
            }
        }
    }

    /// Extract text content for widgets matching the given locator.
    pub fn extract_text(&self, locator: &WidgetLocator) -> String {
        match locator {
            WidgetLocator::All => self.rows_to_text(self.rows.values().collect()),
            WidgetLocator::Column(n) => self.extract_column_text(*n),
            WidgetLocator::ViewId(id) => self.extract_view_text(id),
        }
    }

    /// Check if widgets matching the locator contain the expected text.
    pub fn contains_text(&self, locator: &WidgetLocator, expected: &str) -> bool {
        self.extract_text(locator).contains(expected)
    }

    /// Get the number of views (columns) in the render spec.
    pub fn view_count(&self) -> usize {
        if self.render_spec.views.is_empty() {
            1 // Single default view
        } else {
            self.render_spec.views.len()
        }
    }

    /// Get all view names.
    pub fn view_names(&self) -> Vec<String> {
        self.render_spec.views.keys().cloned().collect()
    }

    /// Get all current row IDs (for debugging).
    pub fn row_ids(&self) -> Vec<String> {
        self.rows.keys().cloned().collect()
    }

    /// Get the current row count.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn extract_column_text(&self, column: usize) -> String {
        if self.render_spec.views.is_empty() {
            // Single view - return all rows
            return self.rows_to_text(self.rows.values().collect());
        }

        // Get view names in iteration order
        let view_names: Vec<&String> = self.render_spec.views.keys().collect();
        if column == 0 || column > view_names.len() {
            // Out of bounds - return all rows as fallback
            return self.rows_to_text(self.rows.values().collect());
        }

        let view_name = view_names[column - 1];
        self.extract_view_text(view_name)
    }

    fn extract_view_text(&self, view_id: &str) -> String {
        let view = match self.render_spec.views.get(view_id) {
            Some(v) => v,
            None => return self.rows_to_text(self.rows.values().collect()),
        };

        let filtered_rows: Vec<&HashMap<String, Value>> = self
            .rows
            .values()
            .filter(|row| self.matches_filter(row, &view.filter))
            .collect();

        self.rows_to_text(filtered_rows)
    }

    fn matches_filter(&self, row: &HashMap<String, Value>, filter: &Option<FilterExpr>) -> bool {
        match filter {
            None => true,
            Some(FilterExpr::All) => true,
            Some(FilterExpr::Eq { column, value }) => {
                row.get(column).map(|v| v == value).unwrap_or(false)
            }
            Some(FilterExpr::Ne { column, value }) => {
                row.get(column).map(|v| v != value).unwrap_or(true)
            }
            Some(FilterExpr::And(filters)) => filters
                .iter()
                .all(|f| self.matches_filter(row, &Some(f.clone()))),
            Some(FilterExpr::Or(filters)) => filters
                .iter()
                .any(|f| self.matches_filter(row, &Some(f.clone()))),
        }
    }

    /// Extract column references from a render expression (recursively)
    fn extract_column_refs(expr: &RenderExpr, columns: &mut HashSet<String>) {
        match expr {
            RenderExpr::ColumnRef { name } => {
                columns.insert(name.clone());
            }
            RenderExpr::FunctionCall { args, .. } => {
                for arg in args {
                    Self::extract_column_refs(&arg.value, columns);
                }
            }
            RenderExpr::BinaryOp { left, right, .. } => {
                Self::extract_column_refs(left, columns);
                Self::extract_column_refs(right, columns);
            }
            RenderExpr::Array { items } => {
                for item in items {
                    Self::extract_column_refs(item, columns);
                }
            }
            RenderExpr::Object { fields } => {
                for value in fields.values() {
                    Self::extract_column_refs(value, columns);
                }
            }
            RenderExpr::Literal { .. } => {
                // Literals don't reference columns
            }
        }
    }

    fn rows_to_text(&self, rows: Vec<&HashMap<String, Value>>) -> String {
        // Extract column references from render template
        let mut referenced_columns = HashSet::new();
        if let Some(root) = self.render_spec.root() {
            Self::extract_column_refs(root, &mut referenced_columns);
        }

        // If no columns referenced (e.g., literals only), show all fields
        if referenced_columns.is_empty() {
            return rows
                .iter()
                .flat_map(|row| {
                    row.values()
                        .filter_map(|v| v.as_string())
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
                .join("\n");
        }

        // Extract only referenced columns
        rows.iter()
            .flat_map(|row| {
                referenced_columns
                    .iter()
                    .filter_map(|col_name| {
                        row.get(col_name)
                            .and_then(|v| v.as_string())
                            .map(|s| s.to_string())
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl std::fmt::Debug for WidgetStateModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WidgetStateModel")
            .field("row_count", &self.rows.len())
            .field("row_ids", &self.row_ids())
            .field("views", &self.view_names())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use holon_api::render_types::RenderExpr;

    fn make_row(id: &str, content: &str) -> HashMap<String, Value> {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::String(id.to_string()));
        row.insert("content".to_string(), Value::String(content.to_string()));
        row
    }

    fn make_widget_spec(rows: Vec<HashMap<String, Value>>) -> WidgetSpec {
        WidgetSpec {
            render_spec: RenderSpec {
                views: HashMap::new(),
                default_view: "default".to_string(),
                root: RenderExpr::Literal { value: Value::Null },
                nested_queries: vec![],
                operations: HashMap::new(),
                row_templates: vec![],
            },
            data: rows,
            actions: vec![],
        }
    }

    #[test]
    fn test_locator_parse() {
        assert!(matches!(
            WidgetLocator::parse("column 1"),
            WidgetLocator::Column(1)
        ));
        assert!(matches!(
            WidgetLocator::parse("column 2"),
            WidgetLocator::Column(2)
        ));
        assert!(matches!(WidgetLocator::parse("all"), WidgetLocator::All));
        assert!(
            matches!(WidgetLocator::parse("sidebar"), WidgetLocator::ViewId(s) if s == "sidebar")
        );
    }

    #[test]
    fn test_from_widget_spec() {
        let rows = vec![make_row("block-1", "Hello"), make_row("block-2", "World")];
        let spec = make_widget_spec(rows);
        let state = WidgetStateModel::from_widget_spec(&spec);

        assert_eq!(state.row_count(), 2);
        assert!(state.contains_text(&WidgetLocator::All, "Hello"));
        assert!(state.contains_text(&WidgetLocator::All, "World"));
    }

    #[test]
    fn test_extract_text() {
        let rows = vec![make_row("block-1", "Hello"), make_row("block-2", "World")];
        let spec = make_widget_spec(rows);
        let state = WidgetStateModel::from_widget_spec(&spec);

        let text = state.extract_text(&WidgetLocator::All);
        assert!(text.contains("Hello"));
        assert!(text.contains("World"));
    }
}
