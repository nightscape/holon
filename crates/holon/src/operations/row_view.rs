use anyhow::{Context, Result};

use crate::storage::types::StorageEntity;
use holon_api::Value;

/// Typed accessor for row data with validation
///
/// Provides type-safe access to common block fields while maintaining
/// the flexibility of HashMap-based operations.
pub struct RowView<'a> {
    data: &'a StorageEntity,
}

impl<'a> RowView<'a> {
    pub fn new(data: &'a StorageEntity) -> Self {
        Self { data }
    }

    /// Get block ID (required field)
    pub fn id(&self) -> Result<&str> {
        self.data
            .get("id")
            .context("Missing required field: id")?
            .as_string()
            .context("Field 'id' is not a string")
    }

    /// Get parent block ID (optional)
    pub fn parent_id(&self) -> Result<Option<&str>> {
        match self.data.get("parent_id") {
            None => Ok(None),
            Some(Value::Null) => Ok(None),
            Some(v) => v
                .as_string()
                .map(Some)
                .context("Field 'parent_id' is not a string"),
        }
    }

    /// Get block depth in tree hierarchy
    pub fn depth(&self) -> Result<i64> {
        self.data
            .get("depth")
            .context("Missing required field: depth")?
            .as_i64()
            .context("Field 'depth' is not an integer")
    }

    /// Get sort key for ordering
    pub fn sort_key(&self) -> Result<&str> {
        self.data
            .get("sort_key")
            .context("Missing required field: sort_key")?
            .as_string()
            .context("Field 'sort_key' is not a string")
    }

    /// Get block content
    pub fn content(&self) -> Result<&str> {
        self.data
            .get("content")
            .context("Missing required field: content")?
            .as_string()
            .context("Field 'content' is not a string")
    }

    /// Get collapsed state
    pub fn is_collapsed(&self) -> Result<bool> {
        self.data
            .get("collapsed")
            .and_then(|v| v.as_bool())
            .context("Field 'collapsed' is missing or not a boolean")
    }

    /// Get completed state (for tasks)
    pub fn is_completed(&self) -> Result<bool> {
        match self.data.get("completed") {
            None => Ok(false), // Default to not completed
            Some(v) => v.as_bool().context("Field 'completed' is not a boolean"),
        }
    }

    /// Get block type (e.g., "note", "task", "heading")
    pub fn block_type(&self) -> Result<Option<&str>> {
        match self.data.get("block_type") {
            None => Ok(None),
            Some(Value::Null) => Ok(None),
            Some(v) => v
                .as_string()
                .map(Some)
                .context("Field 'block_type' is not a string"),
        }
    }

    /// Access raw field by name for custom operations
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.data.get(field)
    }

    /// Get reference to underlying entity
    pub fn entity(&self) -> &StorageEntity {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_entity() -> StorageEntity {
        let mut entity = StorageEntity::new();
        entity.insert("id".to_string(), Value::String("block-1".to_string()));
        entity.insert(
            "parent_id".to_string(),
            Value::String("block-0".to_string()),
        );
        entity.insert("depth".to_string(), Value::Integer(2));
        entity.insert("sort_key".to_string(), Value::String("a0".to_string()));
        entity.insert(
            "content".to_string(),
            Value::String("Test content".to_string()),
        );
        entity.insert("collapsed".to_string(), Value::Boolean(false));
        entity.insert("completed".to_string(), Value::Boolean(true));
        entity.insert("block_type".to_string(), Value::String("task".to_string()));
        entity
    }

    #[test]
    fn test_row_view_required_fields() {
        let entity = create_test_entity();
        let view = RowView::new(&entity);

        assert_eq!(view.id().unwrap(), "block-1");
        assert_eq!(view.depth().unwrap(), 2);
        assert_eq!(view.sort_key().unwrap(), "a0");
        assert_eq!(view.content().unwrap(), "Test content");
    }

    #[test]
    fn test_row_view_optional_fields() {
        let entity = create_test_entity();
        let view = RowView::new(&entity);

        assert_eq!(view.parent_id().unwrap(), Some("block-0"));
        assert_eq!(view.block_type().unwrap(), Some("task"));
    }

    #[test]
    fn test_row_view_boolean_fields() {
        let entity = create_test_entity();
        let view = RowView::new(&entity);

        assert!(!view.is_collapsed().unwrap());
        assert!(view.is_completed().unwrap());
    }

    #[test]
    fn test_row_view_missing_id() {
        let entity = StorageEntity::new();
        let view = RowView::new(&entity);

        assert!(view.id().is_err());
    }

    #[test]
    fn test_row_view_null_parent_id() {
        let mut entity = StorageEntity::new();
        entity.insert("id".to_string(), Value::String("root".to_string()));
        entity.insert("parent_id".to_string(), Value::Null);

        let view = RowView::new(&entity);
        assert_eq!(view.parent_id().unwrap(), None);
    }

    #[test]
    fn test_row_view_missing_completed() {
        let mut entity = StorageEntity::new();
        entity.insert("id".to_string(), Value::String("block-1".to_string()));

        let view = RowView::new(&entity);
        // Should default to false
        assert!(!view.is_completed().unwrap());
    }
}
