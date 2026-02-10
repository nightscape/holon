use std::collections::HashMap;
use std::sync::Arc;

use blinc_core::State;
use holon_api::render_types::OperationWiring;
use holon_api::Value;
use holon_frontend::FrontendSession;

/// Context passed through the render tree during interpretation.
///
/// `data_rows` serves dual purpose: container widgets (columns, list, tree) iterate
/// all rows to render one child per row; leaf widgets (ColumnRef) resolve columns
/// from the first row. When a container binds a specific row, it sets
/// `data_rows = vec![that_row]`.
pub struct RenderContext {
    pub data_rows: Vec<HashMap<String, Value>>,
    /// Operations available on the current render node
    pub operations: Vec<OperationWiring>,
    /// Session handle for dispatching operations
    pub session: Arc<FrontendSession>,
    /// Tokio runtime handle for executing async queries from the render thread
    pub runtime_handle: tokio::runtime::Handle,
    /// Nesting depth (for indentation in block builders)
    pub depth: usize,
    /// Query nesting depth — tracks recursive PRQL query execution to prevent stack overflow.
    pub query_depth: usize,
    /// When true, the columns builder renders as sidebar + main content instead of a plain row.
    pub is_screen_layout: bool,
    /// Reactive sidebar open/closed state. Uses blinc's State<bool> so
    /// update_rebuild() triggers a proper UI rebuild.
    pub sidebar_open: Option<State<bool>>,
}

impl RenderContext {
    pub fn new(session: Arc<FrontendSession>, runtime_handle: tokio::runtime::Handle) -> Self {
        Self {
            data_rows: Vec::new(),
            operations: Vec::new(),
            session,
            runtime_handle,
            depth: 0,
            query_depth: 0,
            is_screen_layout: false,
            sidebar_open: None,
        }
    }

    /// The current row for ColumnRef resolution (first row, or empty).
    pub fn row(&self) -> &HashMap<String, Value> {
        static EMPTY: std::sync::LazyLock<HashMap<String, Value>> =
            std::sync::LazyLock::new(HashMap::new);
        self.data_rows.first().unwrap_or(&EMPTY)
    }

    fn child(&self) -> Self {
        Self {
            data_rows: self.data_rows.clone(),
            operations: self.operations.clone(),
            session: Arc::clone(&self.session),
            runtime_handle: self.runtime_handle.clone(),
            depth: self.depth,
            query_depth: self.query_depth,
            is_screen_layout: false,
            sidebar_open: self.sidebar_open.clone(),
        }
    }

    /// Create a child context bound to a single row.
    pub fn with_row(&self, row: HashMap<String, Value>) -> Self {
        Self {
            data_rows: vec![row],
            ..self.child()
        }
    }

    /// Create a child context with the given data rows.
    pub fn with_data_rows(&self, data_rows: Vec<HashMap<String, Value>>) -> Self {
        Self {
            data_rows,
            is_screen_layout: self.is_screen_layout,
            ..self.child()
        }
    }

    #[allow(dead_code)]
    /// Create a child context with incremented depth.
    pub fn indented(&self) -> Self {
        Self {
            depth: self.depth + 1,
            ..self.child()
        }
    }

    pub fn deeper_query(&self) -> Self {
        Self {
            query_depth: self.query_depth + 1,
            ..self.child()
        }
    }
}
