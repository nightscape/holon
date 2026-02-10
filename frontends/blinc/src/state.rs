use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use holon_api::widget_spec::WidgetSpec;
use holon_api::Value;

/// Shared application state accessible from both the render thread and CDC listener.
///
/// Wraps the current WidgetSpec and row data in an RwLock so that multiple
/// render reads can proceed concurrently, only blocking when CDC updates write.
pub struct AppState {
    inner: Arc<RwLock<AppStateInner>>,
}

struct AppStateInner {
    widget_spec: WidgetSpec,
    /// True when CDC has mutated data since last render
    dirty: bool,
}

#[allow(dead_code)]
impl AppState {
    pub fn new(widget_spec: WidgetSpec) -> Self {
        Self {
            inner: Arc::new(RwLock::new(AppStateInner {
                widget_spec,
                dirty: false,
            })),
        }
    }

    pub fn clone_handle(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn widget_spec(&self) -> WidgetSpec {
        self.inner.read().unwrap().widget_spec.clone()
    }

    pub fn data(&self) -> Vec<HashMap<String, Value>> {
        self.inner.read().unwrap().widget_spec.data.clone()
    }

    pub fn set_data(&self, data: Vec<HashMap<String, Value>>) {
        let mut inner = self.inner.write().unwrap();
        inner.widget_spec.data = data;
        inner.dirty = true;
    }

    /// Update a single row by matching on the "id" field.
    pub fn update_row(&self, id: &str, new_data: HashMap<String, Value>) {
        let mut inner = self.inner.write().unwrap();
        if let Some(row) = inner
            .widget_spec
            .data
            .iter_mut()
            .find(|r| r.get("id").and_then(|v| v.as_string()) == Some(id))
        {
            *row = new_data;
        }
        inner.dirty = true;
    }

    /// Insert a new row.
    pub fn insert_row(&self, data: HashMap<String, Value>) {
        let mut inner = self.inner.write().unwrap();
        inner.widget_spec.data.push(data);
        inner.dirty = true;
    }

    /// Remove a row by id.
    pub fn remove_row(&self, id: &str) {
        let mut inner = self.inner.write().unwrap();
        inner
            .widget_spec
            .data
            .retain(|r| r.get("id").and_then(|v| v.as_string()) != Some(id));
        inner.dirty = true;
    }

    /// Patch specific fields on a row.
    pub fn patch_row(&self, id: &str, fields: &[(String, Value)]) {
        let mut inner = self.inner.write().unwrap();
        if let Some(row) = inner
            .widget_spec
            .data
            .iter_mut()
            .find(|r| r.get("id").and_then(|v| v.as_string()) == Some(id))
        {
            for (field, value) in fields {
                row.insert(field.clone(), value.clone());
            }
        }
        inner.dirty = true;
    }

    /// Mark state as dirty to trigger a re-render.
    pub fn mark_dirty(&self) {
        self.inner.write().unwrap().dirty = true;
    }

    /// Check and clear the dirty flag.
    pub fn take_dirty(&self) -> bool {
        let mut inner = self.inner.write().unwrap();
        let was_dirty = inner.dirty;
        inner.dirty = false;
        was_dirty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn make_widget_spec(data: Vec<HashMap<String, Value>>) -> WidgetSpec {
        use holon_api::{RenderExpr, RenderSpec};
        let render_spec = RenderSpec {
            views: HashMap::new(),
            default_view: "default".into(),
            root: RenderExpr::Literal { value: Value::Null },
            nested_queries: vec![],
            operations: HashMap::new(),
            row_templates: vec![],
        };
        WidgetSpec::new(render_spec, data)
    }

    fn row(id: &str, content: &str) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert("id".into(), Value::String(id.into()));
        m.insert("content".into(), Value::String(content.into()));
        m
    }

    #[test]
    fn new_state_is_not_dirty() {
        let state = AppState::new(make_widget_spec(vec![]));
        assert!(!state.take_dirty());
    }

    #[test]
    fn insert_row_sets_dirty_and_appends() {
        let state = AppState::new(make_widget_spec(vec![]));
        state.insert_row(row("1", "hello"));
        assert!(state.take_dirty());
        assert_eq!(state.data().len(), 1);
        assert_eq!(
            state.data()[0].get("content").unwrap().as_string().unwrap(),
            "hello"
        );
    }

    #[test]
    fn update_row_replaces_matching_row() {
        let state = AppState::new(make_widget_spec(vec![row("1", "old")]));
        state.update_row("1", row("1", "new"));
        assert!(state.take_dirty());
        assert_eq!(
            state.data()[0].get("content").unwrap().as_string().unwrap(),
            "new"
        );
    }

    #[test]
    fn update_row_nonexistent_id_still_sets_dirty() {
        let state = AppState::new(make_widget_spec(vec![row("1", "hello")]));
        state.update_row("999", row("999", "ghost"));
        assert!(state.take_dirty());
        // Original row unchanged, ghost row not inserted
        assert_eq!(state.data().len(), 1);
        assert_eq!(state.data()[0].get("id").unwrap().as_string().unwrap(), "1");
    }

    #[test]
    fn remove_row_deletes_by_id() {
        let state = AppState::new(make_widget_spec(vec![row("1", "a"), row("2", "b")]));
        state.remove_row("1");
        assert!(state.take_dirty());
        assert_eq!(state.data().len(), 1);
        assert_eq!(state.data()[0].get("id").unwrap().as_string().unwrap(), "2");
    }

    #[test]
    fn remove_row_nonexistent_is_noop_but_dirty() {
        let state = AppState::new(make_widget_spec(vec![row("1", "a")]));
        state.remove_row("999");
        assert!(state.take_dirty());
        assert_eq!(state.data().len(), 1);
    }

    #[test]
    fn patch_row_updates_specific_fields() {
        let state = AppState::new(make_widget_spec(vec![row("1", "old")]));
        state.patch_row("1", &[("content".into(), Value::String("patched".into()))]);
        assert!(state.take_dirty());
        assert_eq!(
            state.data()[0].get("content").unwrap().as_string().unwrap(),
            "patched"
        );
        // id unchanged
        assert_eq!(state.data()[0].get("id").unwrap().as_string().unwrap(), "1");
    }

    #[test]
    fn patch_row_adds_new_fields() {
        let state = AppState::new(make_widget_spec(vec![row("1", "hello")]));
        state.patch_row("1", &[("priority".into(), Value::Integer(3))]);
        assert!(state.take_dirty());
        let data = state.data();
        assert_eq!(*data[0].get("priority").unwrap(), Value::Integer(3));
    }

    #[test]
    fn take_dirty_clears_flag() {
        let state = AppState::new(make_widget_spec(vec![]));
        state.mark_dirty();
        assert!(state.take_dirty());
        assert!(!state.take_dirty()); // cleared
    }

    #[test]
    fn clone_handle_shares_state() {
        let state = AppState::new(make_widget_spec(vec![]));
        let handle = state.clone_handle();
        state.insert_row(row("1", "shared"));
        assert_eq!(handle.data().len(), 1);
        assert!(handle.take_dirty());
    }

    #[test]
    fn set_data_replaces_all_rows() {
        let state = AppState::new(make_widget_spec(vec![row("1", "a"), row("2", "b")]));
        state.set_data(vec![row("3", "c")]);
        assert!(state.take_dirty());
        assert_eq!(state.data().len(), 1);
        assert_eq!(state.data()[0].get("id").unwrap().as_string().unwrap(), "3");
    }

    #[test]
    fn concurrent_reads_dont_block() {
        let state = AppState::new(make_widget_spec(vec![row("1", "a")]));
        let h1 = state.clone_handle();
        let h2 = state.clone_handle();
        // Both reads should succeed without deadlock
        let d1 = h1.data();
        let d2 = h2.data();
        assert_eq!(d1.len(), 1);
        assert_eq!(d2.len(), 1);
    }
}
