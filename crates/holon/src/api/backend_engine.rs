use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Result of extracting available columns from widget arguments
enum AvailableColumns {
    /// All columns from the query are available (e.g., when `this.*` is used)
    All,
    /// Only specific columns are available
    Selected(Vec<String>),
}

use tokio::sync::broadcast;

use crate::api::operation_dispatcher::OperationDispatcher;
use crate::core::datasource::OperationProvider;
use crate::core::transform::TransformPipeline;
use crate::storage::sql_utils::value_to_sql_literal;
use crate::storage::turso::priority;
use crate::storage::turso::{RowChange, RowChangeStream, TursoBackend};
use crate::storage::types::StorageEntity;
use crate::storage::{DbHandle, Resource, extract_table_refs};
use holon_api::{ActionSpec, BatchWithMetadata, Operation, OperationDescriptor, Value, WidgetSpec};
use holon_core::{UndoAction, UndoStack};
use holon_prql_render::RenderSpec;

/// Context for query compilation - determines what virtual tables resolve to
#[derive(Debug, Clone)]
pub struct QueryContext {
    /// Current block ID for `from children` resolution. None = root level (parent_id IS NULL)
    pub current_block_id: Option<String>,
    /// Parent of current block for `from siblings` resolution
    pub context_parent_id: Option<String>,
    /// Path prefix for descendants queries (e.g., "/block-123/%")
    /// Computed from blocks_with_paths matview when context is created with path lookup
    pub context_path_prefix: Option<String>,
}

impl QueryContext {
    /// Create a root-level context (for queries at the top level)
    pub fn root() -> Self {
        Self {
            current_block_id: None,
            context_parent_id: None,
            context_path_prefix: None,
        }
    }

    /// Create a context for a specific block
    pub fn for_block(block_id: String, parent_id: Option<String>) -> Self {
        Self {
            current_block_id: Some(block_id.clone()),
            context_parent_id: parent_id,
            context_path_prefix: None,
        }
    }

    /// Create a context for a specific block with path prefix for descendants queries
    pub fn for_block_with_path(block_id: String, parent_id: Option<String>, path: String) -> Self {
        Self {
            current_block_id: Some(block_id),
            context_parent_id: parent_id,
            // Path prefix for starts_with: "/block-123/" (starts_with adds the % wildcard)
            context_path_prefix: Some(format!("{}/", path)),
        }
    }
}

/// PRQL stdlib defining virtual tables for hierarchical queries
///
/// Note: When $context_id is NULL, PRQL generates `parent_id = NULL` which is always false in SQL.
/// The `children` virtual table should be used with QueryContext::for_block() which sets a non-NULL context_id.
///
/// The `roots` virtual table returns top-level blocks (blocks whose parent is a document, not another block).
/// In the Holon data model, document-level blocks have parent_id starting with "holon-doc://".
///
/// The `descendants` and `grandchildren` virtual tables use `blocks_with_paths` materialized
/// view with path prefix matching. This enables efficient tree traversal using precomputed
/// hierarchical paths.
///
/// Note: We cannot use PRQL's `loop` in the stdlib because `let descendants = (... loop ...)`
/// creates nested CTEs (outer CTE for `let`, inner recursive CTE for `loop`), which Turso
/// doesn't support. Direct use of `loop` without `let` wrapping would work.
const PRQL_STDLIB: &str = r#"
let children = (from blocks | filter parent_id == $context_id)
let roots = (from blocks | filter (parent_id | text.starts_with "holon-doc://"))
let siblings = (from blocks | filter parent_id == $context_parent_id)
let descendants = (
  from blocks_with_paths
  filter (path | text.starts_with $context_path_prefix)
  select {id, parent_id, content, content_type, source_language, source_name, properties, created_at, updated_at, path}
)
let grandchildren = descendants
let block_children = (
  from b = blocks
  filter b.parent_id == $context_parent_id
)
"#;

/// Inject stdlib before user PRQL query
fn inject_stdlib(user_prql: &str) -> String {
    format!("{}\n{}", PRQL_STDLIB, user_prql)
}

/// Strip ORDER BY clause from SQL (Turso materialized views don't support it)
fn strip_order_by(sql: &str) -> String {
    // Simple regex-like approach: find ORDER BY and remove everything until end or next clause
    let upper = sql.to_uppercase();
    if let Some(order_idx) = upper.find("ORDER BY") {
        // Find the end of ORDER BY clause (LIMIT, OFFSET, or end of string)
        let rest = &upper[order_idx..];
        let end_idx = rest
            .find("LIMIT")
            .or_else(|| rest.find("OFFSET"))
            .unwrap_or(rest.len());

        let mut result = sql[..order_idx].to_string();
        result.push_str(&sql[order_idx + end_idx..]);
        result.trim().to_string()
    } else {
        sql.to_string()
    }
}

/// Main render engine managing database, query compilation, and operations
pub struct BackendEngine {
    /// Handle for all database operations (query, execute, DDL, dependency tracking)
    db_handle: DbHandle,
    /// CDC broadcast sender for subscribing to change events
    cdc_broadcast: broadcast::Sender<BatchWithMetadata<RowChange>>,
    /// Operation dispatcher for routing operations
    dispatcher: Arc<OperationDispatcher>,
    /// Pipeline for AST transformations
    transform_pipeline: Arc<TransformPipeline>,
    /// Maps table names to entity names
    table_to_entity_map: Arc<RwLock<HashMap<String, String>>>,
    /// Undo/redo history
    undo_stack: Arc<RwLock<UndoStack>>,
    /// DDL serialization mutex - ensures only one CREATE MATERIALIZED VIEW runs at a time.
    ddl_mutex: Arc<tokio::sync::Mutex<()>>,
    /// Keeps the TursoBackend alive for as long as BackendEngine exists.
    ///
    /// The database actor is spawned by TursoBackend::new() and runs independently.
    /// The actor only exits when ALL senders to its channel are dropped. While DbHandle
    /// holds one sender clone, we also keep the TursoBackend (which holds another sender)
    /// alive to ensure the actor survives even if the DI container is dropped.
    ///
    /// This prevents the "Actor channel closed" bug where the actor dies between
    /// init_render_engine() completing (DI container dropped) and initial_widget() being called.
    _backend_keepalive: Arc<RwLock<TursoBackend>>,
}

impl BackendEngine {
    /// Create BackendEngine from dependencies (for dependency injection)
    ///
    /// This constructor takes a DbHandle (for all database operations including
    /// dependency-aware DDL), a CDC broadcast sender (for change notifications),
    /// and a reference to the TursoBackend to keep it alive.
    ///
    /// # Actor Lifetime Guarantee
    ///
    /// The `backend` parameter ensures the database actor stays alive for as long
    /// as this BackendEngine exists. The actor is spawned by `TursoBackend::new()`
    /// and only exits when ALL senders to its channel are dropped. By holding both
    /// the `db_handle` (one sender clone) AND the `backend` (another sender), we
    /// ensure the actor survives regardless of DI container lifetime.
    pub fn new(
        db_handle: DbHandle,
        cdc_broadcast: broadcast::Sender<BatchWithMetadata<RowChange>>,
        dispatcher: Arc<OperationDispatcher>,
        transform_pipeline: Arc<TransformPipeline>,
        backend: Arc<RwLock<TursoBackend>>,
    ) -> Self {
        Self {
            db_handle,
            cdc_broadcast,
            dispatcher,
            transform_pipeline,
            table_to_entity_map: Arc::new(RwLock::new(HashMap::new())),
            undo_stack: Arc::new(RwLock::new(UndoStack::default())),
            ddl_mutex: Arc::new(tokio::sync::Mutex::new(())),
            _backend_keepalive: backend,
        }
    }

    /// Get the database handle for direct database operations
    pub fn db_handle(&self) -> &DbHandle {
        &self.db_handle
    }

    /// Get the CDC broadcast sender for subscribing to change events
    pub fn cdc_broadcast(&self) -> &broadcast::Sender<BatchWithMetadata<RowChange>> {
        &self.cdc_broadcast
    }

    /// Pre-create materialized views for the given PRQL queries.
    ///
    /// This should be called during initialization, BEFORE any data loading or
    /// file watching starts. By pre-creating views:
    /// - Views start empty and are populated by IVM as data arrives
    /// - Later `query_and_watch` calls find existing views (no DDL needed)
    /// - No contention between view creation and IVM processing
    ///
    /// This eliminates the "database is locked" bug that occurs when
    /// `query_and_watch` tries to CREATE MATERIALIZED VIEW while IVM is busy.
    pub async fn preload_views(&self, prql_queries: &[&str]) -> Result<()> {
        tracing::info!(
            "[BackendEngine] preload_views: pre-creating {} views",
            prql_queries.len()
        );

        for prql in prql_queries {
            // Compile to get the SQL
            let (sql, _render_spec) = self.compile_query(prql.to_string(), None)?;

            // Inline parameters (with NULL context - views start empty)
            let params = HashMap::new();
            let sql_with_params = Self::inline_parameters(&sql, &params);

            // Generate view name (same hash logic as watch_query)
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            sql_with_params.hash(&mut hasher);
            let view_name = format!("watch_view_{:x}", hasher.finish());

            tracing::debug!(
                "[preload_views] Pre-creating view {} for query: {}",
                view_name,
                prql
            );

            // Check if view already exists
            let check_view_sql = format!(
                "SELECT name FROM sqlite_master WHERE type='view' AND name='{}'",
                view_name
            );
            let view_exists = match self.db_handle.query(&check_view_sql, HashMap::new()).await {
                Ok(results) => !results.is_empty(),
                Err(_) => false,
            };

            if view_exists {
                tracing::debug!(
                    "[preload_views] View {} already exists, skipping",
                    view_name
                );
                continue;
            }

            // Create the view (no contention since no data yet)
            // Use IF NOT EXISTS for idempotency - safe because view name is a hash of the query
            let sql_for_view = strip_order_by(&sql_with_params);
            let create_view_sql = format!(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS {} AS {}",
                view_name, sql_for_view
            );

            // Retry with exponential backoff for lock contention
            let mut last_error = None;
            for attempt in 0..3 {
                match self.db_handle.execute_ddl(&create_view_sql).await {
                    Ok(_) => {
                        tracing::info!("[preload_views] Created view {}", view_name);
                        last_error = None;
                        break;
                    }
                    Err(e) => {
                        let err_str = format!("{:?}", e);
                        let is_retryable = err_str.contains("database is locked")
                            || err_str.contains("Database schema changed");
                        if is_retryable && attempt < 2 {
                            tracing::debug!(
                                "[preload_views] Retry {} for view {}: {}",
                                attempt + 1,
                                view_name,
                                err_str
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(
                                50 * (1 << attempt),
                            ))
                            .await;
                            last_error = Some(e);
                        } else {
                            last_error = Some(e);
                            break;
                        }
                    }
                }
            }
            if let Some(e) = last_error {
                tracing::warn!("[preload_views] Failed to create view {}: {}", view_name, e);
            }
        }

        tracing::info!("[BackendEngine] preload_views: completed");
        Ok(())
    }

    /// Compile a PRQL query with render() into SQL and UI specification
    ///
    /// Automatically infers operation wirings from PRQL lineage analysis.
    /// Widgets that reference direct table columns will have operations populated.
    ///
    /// This method:
    /// 1. Injects PRQL stdlib (virtual tables like `children`, `roots`, `siblings`) if context is provided
    /// 2. Parses the PRQL query to PL AST and extracts render() / row templates
    /// 3. Applies PL-phase AST transformations (e.g., JsonAggregationTransformer for UNIONs)
    /// 4. Converts PL to RQ AST
    /// 5. Applies RQ-phase AST transformations (e.g., ChangeOriginTransformer)
    /// 6. Generates SQL from the transformed RQ
    /// 7. Queries OperationDispatcher for compatible operations
    /// 8. Replaces placeholder operations with real OperationDescriptors
    /// 9. For UNION queries with row_templates, wires operations per-template using entity_name
    ///
    /// # Arguments
    /// * `prql` - The PRQL query to compile
    /// * `context` - Optional query context for virtual table resolution (e.g., `from children`)
    pub fn compile_query(
        &self,
        prql: String,
        _context: Option<QueryContext>,
    ) -> Result<(String, RenderSpec)> {
        // Always inject stdlib to enable virtual tables like `from children` and `from roots`.
        // When no context is provided, $context_id will be bound to NULL by execute_query/watch_query,
        // which means `from children` returns no results (correct for root-level queries).
        // Use `from roots` to query blocks with parent_id IS NULL.
        let prql_with_stdlib = inject_stdlib(&prql);

        // Step 1: Parse query to PL AST (before RQ conversion)
        // This extracts render() calls and row templates, but doesn't convert to RQ yet
        let holon_prql_render::ParsedQueryPl {
            query_module,
            render_data,
        } = holon_prql_render::parse_query_to_pl(&prql_with_stdlib)?;

        // Step 2: Apply PL-phase transformations (e.g., JsonAggregationTransformer for UNION queries)
        let transformed_pl = self.transform_pipeline.apply_pl_transforms(query_module)?;

        // Step 3: Complete PL→RQ conversion with the transformed PL module
        let parsed = render_data.complete_rq_conversion(transformed_pl)?;
        let mut render_spec = parsed.render_spec;
        let all_selected_columns = parsed.available_columns;

        // Step 4: Apply RQ-phase transformations (e.g., ChangeOriginTransformer)
        let transformed_rq = self.transform_pipeline.transform_rq(parsed.rq)?;

        // Step 5: Generate SQL from the transformed RQ
        let sql = holon_prql_render::ParsedQueryRender::to_sql_from_rq(&transformed_rq)?;

        // Step 6: Extract table name from original query (needed for entity lookup)
        // Use original prql, not prql_with_stdlib, to extract the actual table name
        let table_name = self.extract_table_name_from_prql(&prql)?;

        // Step 7: Walk the tree and enhance operations with real descriptors from dispatcher
        // Pass all selected columns as context for operation filtering
        // This now includes ALL columns from the query result (e.g., parent_id), not just widget-referenced columns
        self.enhance_operations_with_dispatcher(
            render_spec.get_root_mut(),
            &table_name,
            &all_selected_columns,
        )?;

        // Step 8: For UNION queries with row_templates, wire operations per-template
        // Each template knows its source entity_name, so we wire operations using that
        let all_ops = self.dispatcher.operations();
        for template in &mut render_spec.row_templates {
            debug!(
                "Wiring operations for row_template[{}] with entity_name='{}'",
                template.index, template.entity_name
            );

            // Extract entity_short_name from the first operation that matches this entity
            if let Some(op) = all_ops
                .iter()
                .find(|op| op.entity_name == template.entity_name)
            {
                template.entity_short_name = op.entity_short_name.clone();
                debug!(
                    "Set entity_short_name='{}' for template[{}]",
                    template.entity_short_name, template.index
                );
            } else {
                debug!(
                    "No operation found for entity '{}', entity_short_name remains empty",
                    template.entity_name
                );
            }

            self.enhance_operations_with_dispatcher(
                &mut template.expr,
                &template.entity_name,
                &all_selected_columns,
            )?;
        }

        Ok((sql, render_spec))
    }

    /// Extract table name from PRQL query string
    fn extract_table_name_from_prql(&self, prql: &str) -> Result<String> {
        // Simple extraction - look for "from <table_name>" pattern
        // Split by whitespace and look for "from" followed by a word
        let words: Vec<&str> = prql.split_whitespace().collect();
        for (i, word) in words.iter().enumerate() {
            if word.eq_ignore_ascii_case("from") && i + 1 < words.len() {
                return Ok(words[i + 1].to_string());
            }
        }
        anyhow::bail!("Could not extract table name from PRQL query")
    }

    /// Enhance operations in the render tree with real descriptors from OperationDispatcher
    ///
    /// Walks the tree and for each FunctionCall with operations:
    /// 1. Extracts available columns from the function call context
    /// 2. Merges with all selected columns from the query (for operations that need columns not in widget)
    /// 3. Finds entity_name by querying dispatcher for operations matching the table_name
    /// 4. Queries dispatcher.find_operations() with entity_name and available columns
    /// 5. Replaces placeholder operations with real ones
    fn enhance_operations_with_dispatcher(
        &self,
        expr: &mut holon_prql_render::RenderExpr,
        table_name: &str,
        all_selected_columns: &[String],
    ) -> Result<()> {
        match expr {
            holon_prql_render::RenderExpr::FunctionCall {
                name,
                args,
                operations,
            } => {
                // Extract available columns from this function call's arguments
                // Each widget only gets operations for columns it directly references
                let available_args = match self.extract_available_columns_from_args(args) {
                    AvailableColumns::All => all_selected_columns.to_vec(),
                    AvailableColumns::Selected(cols) => cols,
                };

                // Find entity_name by looking for operations that match this table_name
                // Since OperationDescriptor has both table and entity_name, we can find
                // the entity_name by querying the dispatcher
                let all_ops = self.dispatcher.operations();
                debug!(
                    "Enhancing operations for widget '{}' on table '{}'",
                    name, table_name
                );
                debug!(
                    "Total operations available from dispatcher: {}",
                    all_ops.len()
                );

                let entity_name = table_name;
                debug!(
                    "Available columns for widget '{}': {:?}",
                    name, available_args
                );
                // Query dispatcher for all compatible operations
                let compatible_ops = self
                    .dispatcher
                    .find_operations(&entity_name, &available_args);
                debug!(
                    "Found {} compatible operations for entity '{}': {:?}",
                    compatible_ops.len(),
                    entity_name,
                    compatible_ops.iter().map(|op| &op.name).collect::<Vec<_>>()
                );

                // DEBUG: Log all operations for this entity to see why set_field might be missing
                let all_entity_ops: Vec<_> = all_ops
                    .iter()
                    .filter(|op| op.entity_name == entity_name)
                    .collect();
                info!(
                    "[BackendEngine] All operations for entity '{}' (table '{}'): {} total",
                    entity_name,
                    table_name,
                    all_entity_ops.len()
                );
                for op in &all_entity_ops {
                    let required_params: Vec<_> =
                        op.required_params.iter().map(|p| &p.name).collect();
                    let has_all_params =
                        required_params.iter().all(|p| available_args.contains(*p));
                    info!(
                        "[BackendEngine]   - {}: required_params={:?}, available_args={:?}, matches={}",
                        op.name, required_params, available_args, has_all_params
                    );
                }

                // Replace placeholder operations with real ones
                // Keep existing operations that aren't placeholders, add new compatible ones
                let mut new_operations = Vec::new();

                // Add all compatible operations from dispatcher
                for op_desc in compatible_ops {
                    // Check if we already have this operation (by name)
                    if !operations
                        .iter()
                        .any(|existing| existing.descriptor.name == op_desc.name)
                    {
                        new_operations.push(holon_prql_render::OperationWiring {
                            widget_type: name.clone(),
                            modified_param: String::new(), // Will be filled by lineage if needed
                            descriptor: op_desc,
                        });
                    }
                }

                // Also keep existing operations (they might be from lineage analysis)
                operations.extend(new_operations);

                // Recurse into nested expressions
                for arg in args.iter_mut() {
                    self.enhance_operations_with_dispatcher(
                        &mut arg.value,
                        table_name,
                        all_selected_columns,
                    )?;
                }
            }
            holon_prql_render::RenderExpr::Array { items } => {
                for item in items.iter_mut() {
                    self.enhance_operations_with_dispatcher(
                        item,
                        table_name,
                        all_selected_columns,
                    )?;
                }
            }
            holon_prql_render::RenderExpr::BinaryOp { left, right, .. } => {
                self.enhance_operations_with_dispatcher(left, table_name, all_selected_columns)?;
                self.enhance_operations_with_dispatcher(right, table_name, all_selected_columns)?;
            }
            holon_prql_render::RenderExpr::Object { fields } => {
                for value in fields.values_mut() {
                    self.enhance_operations_with_dispatcher(
                        value,
                        table_name,
                        all_selected_columns,
                    )?;
                }
            }
            _ => {} // ColumnRef, Literal - no recursion needed
        }
        Ok(())
    }

    /// Extract available column names from function call arguments
    ///
    /// This extracts column names that are available in the context, which can be used
    /// to filter operations (operations that require columns not available won't be shown).
    /// Returns `AvailableColumns::All` if `this.*` is encountered, indicating all query columns.
    fn extract_available_columns_from_args(
        &self,
        args: &[holon_prql_render::Arg],
    ) -> AvailableColumns {
        let mut columns = Vec::new();
        for arg in args {
            match &arg.value {
                holon_prql_render::RenderExpr::ColumnRef { name } => {
                    // `this.*` means "all columns"
                    if name == "this.*" {
                        return AvailableColumns::All;
                    }
                    columns.push(name.clone());
                }
                _ => {
                    // Recurse into nested expressions
                    if let AvailableColumns::All =
                        self.collect_columns_from_expr(&arg.value, &mut columns)
                    {
                        return AvailableColumns::All;
                    }
                }
            }
        }
        // Always include "id" as it's typically available
        if !columns.contains(&"id".to_string()) {
            columns.push("id".to_string());
        }
        AvailableColumns::Selected(columns)
    }

    /// Recursively collect column names from an expression
    /// Returns `AvailableColumns::All` if `this.*` is encountered anywhere.
    fn collect_columns_from_expr(
        &self,
        expr: &holon_prql_render::RenderExpr,
        columns: &mut Vec<String>,
    ) -> AvailableColumns {
        match expr {
            holon_prql_render::RenderExpr::ColumnRef { name } => {
                if name == "this.*" {
                    return AvailableColumns::All;
                }
                if !columns.contains(name) {
                    columns.push(name.clone());
                }
            }
            holon_prql_render::RenderExpr::FunctionCall { args, .. } => {
                for arg in args {
                    if let AvailableColumns::All =
                        self.collect_columns_from_expr(&arg.value, columns)
                    {
                        return AvailableColumns::All;
                    }
                }
            }
            holon_prql_render::RenderExpr::Array { items } => {
                for item in items {
                    if let AvailableColumns::All = self.collect_columns_from_expr(item, columns) {
                        return AvailableColumns::All;
                    }
                }
            }
            holon_prql_render::RenderExpr::BinaryOp { left, right, .. } => {
                if let AvailableColumns::All = self.collect_columns_from_expr(left, columns) {
                    return AvailableColumns::All;
                }
                if let AvailableColumns::All = self.collect_columns_from_expr(right, columns) {
                    return AvailableColumns::All;
                }
            }
            holon_prql_render::RenderExpr::Object { fields } => {
                for value in fields.values() {
                    if let AvailableColumns::All = self.collect_columns_from_expr(value, columns) {
                        return AvailableColumns::All;
                    }
                }
            }
            _ => {} // Literal - no columns
        }
        AvailableColumns::Selected(columns.clone())
    }

    /// Inline parameter values directly into SQL (for materialized view definitions)
    ///
    /// Unlike bind_parameters which uses `?` placeholders, this function substitutes
    /// actual values into the SQL string. This is necessary for CREATE MATERIALIZED VIEW
    /// statements where the view definition must contain literal values, not parameters.
    ///
    /// Values are properly escaped/quoted:
    /// - Strings: 'escaped''quotes'
    /// - Numbers: literal
    /// - Null: NULL
    /// - Bool: 1/0
    fn inline_parameters(sql: &str, params: &HashMap<String, Value>) -> String {
        let mut result = String::with_capacity(sql.len());
        let mut chars = sql.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '$' {
                if let Some(&next_ch) = chars.peek() {
                    if next_ch.is_alphanumeric() || next_ch == '_' {
                        let mut param_name = String::new();
                        while let Some(&next_ch) = chars.peek() {
                            if next_ch.is_alphanumeric() || next_ch == '_' {
                                param_name.push(chars.next().unwrap());
                            } else {
                                break;
                            }
                        }

                        if let Some(value) = params.get(&param_name) {
                            result.push_str(&value_to_sql_literal(value));
                        } else {
                            result.push('$');
                            result.push_str(&param_name);
                        }
                    } else {
                        result.push('$');
                    }
                } else {
                    result.push('$');
                }
            } else {
                result.push(ch);
            }
        }

        result
    }

    /// Compute a deterministic view name for a given SQL query and parameters.
    ///
    /// This is used to create materialized views with consistent names, allowing
    /// us to create the view first and then query it for initial data.
    fn compute_view_name(sql: &str, params: &HashMap<String, Value>) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let sql_with_params = Self::inline_parameters(sql, params);
        let mut hasher = DefaultHasher::new();
        sql_with_params.hash(&mut hasher);
        format!("watch_view_{:x}", hasher.finish())
    }

    /// Bind context parameters to the parameter map
    ///
    /// Adds `$context_id`, `$context_parent_id`, and `$context_path_prefix` parameters
    /// based on QueryContext. None values are bound as Value::Null.
    fn bind_context_params(&self, params: &mut HashMap<String, Value>, context: &QueryContext) {
        match &context.current_block_id {
            Some(id) => {
                params.insert("context_id".to_string(), Value::String(id.clone()));
            }
            None => {
                params.insert("context_id".to_string(), Value::Null);
            }
        }
        match &context.context_parent_id {
            Some(id) => {
                params.insert("context_parent_id".to_string(), Value::String(id.clone()));
            }
            None => {
                params.insert("context_parent_id".to_string(), Value::Null);
            }
        }
        match &context.context_path_prefix {
            Some(prefix) => {
                params.insert(
                    "context_path_prefix".to_string(),
                    Value::String(prefix.clone()),
                );
            }
            None => {
                // No path prefix means descendants queries won't match anything
                // This is intentional - use for_block_with_path() to enable descendants
                params.insert(
                    "context_path_prefix".to_string(),
                    Value::String("__NO_PATH__/".to_string()),
                );
            }
        }
    }

    /// Execute a SQL query and return the result set
    ///
    /// Supports parameter binding by replacing `$param_name` placeholders with actual values.
    /// Parameters are bound safely using SQL parameter binding to prevent SQL injection.
    ///
    /// # Arguments
    /// * `sql` - The SQL query to execute
    /// * `params` - Parameters to bind to the query
    /// * `context` - Optional query context for virtual table parameter binding
    pub async fn execute_query(
        &self,
        sql: String,
        mut params: HashMap<String, Value>,
        context: Option<QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Always bind context params (using NULL if no context provided).
        // This enables stdlib virtual tables like `from children` to compile even without context.
        let ctx = context.unwrap_or_else(QueryContext::root);
        self.bind_context_params(&mut params, &ctx);

        // Retry with fresh connections to handle "Database schema changed" errors
        // that occur when DDL operations race with queries during startup.
        // Fresh connections don't have stale prepared statement caches.
        // db_handle used directly
        let mut last_error = None;
        for attempt in 0..5 {
            // On first attempt, use normal connection. On retries, use fresh connection
            // to avoid stale prepared statement caches.
            let result = if attempt == 0 {
                self.db_handle.query(&sql, params.clone()).await
            } else {
                self.db_handle.query(&sql, params.clone()).await
            };
            match result {
                Ok(result) => return Ok(result),
                Err(e) => {
                    let err_str = format!("{:?}", e);
                    let is_schema_error = err_str.contains("Database schema changed");
                    if is_schema_error && attempt < 4 {
                        tracing::debug!(
                            "[execute_query] Retry {} due to schema change: {}",
                            attempt + 1,
                            err_str
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(50 * (1 << attempt)))
                            .await;
                        last_error = Some(e);
                    } else {
                        return Err(anyhow::anyhow!("SQL execution failed: {}", e));
                    }
                }
            }
        }
        Err(anyhow::anyhow!(
            "SQL execution failed after retries: {:?}",
            last_error
        ))
    }

    /// Watch a query for changes via CDC streaming
    ///
    /// Returns a stream of RowChange events from the underlying database.
    /// The CDC connection is stored in the BackendEngine to keep it alive.
    ///
    /// Note: The SQL should include `_change_origin` column for CDC trace propagation.
    /// When using `compile_query` or `query_and_watch`, this is handled automatically
    /// by the TransformPipeline.
    ///
    /// # Arguments
    /// * `sql` - The SQL query to watch
    /// * `params` - Parameters to bind to the query
    /// * `context` - Optional query context for virtual table parameter binding
    pub async fn watch_query(
        &self,
        sql: String,
        mut params: HashMap<String, Value>,
        context: Option<QueryContext>,
    ) -> Result<RowChangeStream> {
        // Always bind context params (using NULL if no context provided).
        // This enables stdlib virtual tables like `from children` to compile even without context.
        let ctx = context.unwrap_or_else(QueryContext::root);
        self.bind_context_params(&mut params, &ctx);

        // Inline parameters into SQL - materialized views can't use parameter placeholders
        let sql_with_params = Self::inline_parameters(&sql, &params);

        // Generate a unique view name including both SQL and params
        // Different param values create different materialized views
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        sql_with_params.hash(&mut hasher);
        let view_name = format!("watch_view_{:x}", hasher.finish());

        tracing::debug!(
            "[watch_query] Starting materialized view setup for view: {}",
            view_name
        );
        tracing::debug!("[watch_query] SQL query: {}", sql);

        // Check if view already exists - if so, skip DDL entirely
        // Since view_name is a hash of the query, an existing view with this name
        // is guaranteed to be the correct view for this query
        // Use execute_sql which routes through db_handle if available
        // db_handle used directly
        let check_view_sql = format!(
            "SELECT name FROM sqlite_master WHERE type='view' AND name='{}'",
            view_name
        );
        let view_exists = match self.db_handle.query(&check_view_sql, HashMap::new()).await {
            Ok(results) => !results.is_empty(),
            Err(_) => false,
        };

        if view_exists {
            tracing::debug!(
                "[watch_query] View {} already exists, reusing (skipping DDL)",
                view_name
            );
        } else {
            tracing::debug!(
                "[watch_query] View {} does not exist, creating...",
                view_name
            );

            // Acquire DDL mutex to serialize CREATE MATERIALIZED VIEW operations.
            // This prevents multiple concurrent DDL operations from blocking each other
            // for the full busy_timeout when IVM is active during startup.
            let _ddl_guard = self.ddl_mutex.lock().await;
            tracing::debug!("[watch_query] Acquired DDL mutex for view: {}", view_name);

            // Re-check if view exists - another thread may have created it while we waited
            let view_now_exists = match self.db_handle.query(&check_view_sql, HashMap::new()).await
            {
                Ok(results) => !results.is_empty(),
                Err(_) => false,
            };

            if view_now_exists {
                tracing::debug!(
                    "[watch_query] View {} was created while waiting for DDL mutex, reusing",
                    view_name
                );
            } else {
                // Check for orphaned DBSP state tables from failed previous attempts
                // The DBSP state table name pattern is: __turso_internal_dbsp_state_v{version}_{view_name}
                let dbsp_table_pattern = format!("__turso_internal_dbsp_state_v%_{}", view_name);
                let check_dbsp_sql = format!(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{}'",
                    dbsp_table_pattern
                );

                // Query for orphaned tables and clean them up
                if let Ok(orphaned_tables) =
                    self.db_handle.query(&check_dbsp_sql, HashMap::new()).await
                {
                    for table_row in orphaned_tables {
                        if let Some(Value::String(table_name)) = table_row.get("name") {
                            tracing::debug!(
                                "[watch_query] Cleaning up orphaned DBSP state table: {}",
                                table_name
                            );
                            // Use execute_ddl for cleanup DDL
                            let _ = self
                                .db_handle
                                .execute_ddl(&format!("DROP TABLE IF EXISTS {}", table_name))
                                .await;
                        }
                    }
                }

                // Strip ORDER BY clause - Turso materialized views don't support it
                let sql_for_view = strip_order_by(&sql_with_params);

                // Create the materialized view
                // Note: For best performance, use preload_views() during init to create views
                // before data loading starts. This avoids DDL/IVM contention.
                // Use IF NOT EXISTS for idempotency - safe because view name is a hash of the query.
                // This prevents "database is locked" errors during concurrent DDL and IVM operations.
                let create_view_sql = format!(
                    "CREATE MATERIALIZED VIEW IF NOT EXISTS {} AS {}",
                    view_name, sql_for_view
                );
                tracing::debug!(
                    "[watch_query] Creating materialized view: {}",
                    create_view_sql
                );

                // Use scheduler to execute DDL with dependency tracking.
                // The scheduler ensures this view is created only after its source tables exist.
                let provides = vec![Resource::schema(view_name.clone())];
                let requires = extract_table_refs(&sql_for_view);

                tracing::debug!(
                    "[watch_query] Submitting DDL via scheduler - provides: {:?}, requires: {:?}",
                    provides,
                    requires
                );

                self.db_handle
                    .execute_ddl_with_deps(
                        &create_view_sql,
                        provides,
                        requires,
                        priority::DDL_MATVIEW,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create materialized view: {}", e))?;

                tracing::debug!(
                    "[watch_query] Successfully created materialized view: {}",
                    view_name
                );
            } // Close if view_now_exists {} else { ... }
        }

        // Set up change stream for the view by subscribing to cdc_broadcast
        tracing::debug!("[watch_query] Setting up CDC stream...");

        // Subscribe to the broadcast channel and convert to stream
        let mut broadcast_rx = self.cdc_broadcast.subscribe();
        tracing::debug!(
            "[watch_query] Subscribed to CDC broadcast, receiver_count={}",
            self.cdc_broadcast.receiver_count()
        );

        // Convert broadcast receiver to mpsc-based stream for compatibility
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        tokio::spawn(async move {
            loop {
                match broadcast_rx.recv().await {
                    Ok(batch) => {
                        if tx.send(batch).await.is_err() {
                            tracing::debug!("[watch_query] CDC subscriber stream closed");
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            "[watch_query] CDC subscriber lagged by {} messages, continuing",
                            n
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::debug!("[watch_query] CDC broadcast channel closed");
                        break;
                    }
                }
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        tracing::debug!("[watch_query] CDC stream ready");

        // Filter the stream to only include events for this specific view
        // The CDC callback fires for ALL materialized views, so we need to filter
        // by the view_name we just created to avoid mixing events from different queries
        use tokio_stream::StreamExt;
        let view_name_for_filter = view_name.clone();
        let filtered_stream = stream.filter(move |batch| {
            let matches = batch.metadata.relation_name == view_name_for_filter;
            if !matches {
                tracing::debug!(
                    "[watch_query] Filtering out CDC event for view '{}' (expected '{}')",
                    batch.metadata.relation_name,
                    view_name_for_filter
                );
            }
            matches
        });

        // Convert filtered stream back to RowChangeStream type
        // Using Box::pin to create a pinned stream that can be converted
        let boxed_stream: std::pin::Pin<Box<dyn tokio_stream::Stream<Item = _> + Send>> =
            Box::pin(filtered_stream);

        // Create a channel to adapt the filtered stream back to ReceiverStream
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        tokio::spawn(async move {
            tokio::pin!(boxed_stream);
            while let Some(item) = boxed_stream.next().await {
                if tx.send(item).await.is_err() {
                    break; // Receiver dropped
                }
            }
        });

        Ok(tokio_stream::wrappers::ReceiverStream::new(rx))
    }

    /// Convenience method that compiles a PRQL query, executes it, and sets up CDC streaming
    ///
    /// This combines `compile_query`, `execute_query`, and `watch_query` into a single call.
    /// Returns a WidgetSpec (render spec + data) and a stream of ongoing changes.
    ///
    /// # Arguments
    /// * `prql` - The PRQL query to compile and execute
    /// * `params` - Parameters to bind to the query
    /// * `context` - Optional query context for virtual table parameter binding
    ///
    /// # Returns
    /// A tuple containing:
    /// - `WidgetSpec`: Contains render_spec and data (actions empty for regular queries)
    /// - `RowChangeStream`: Stream of ongoing changes to the query results
    pub async fn query_and_watch(
        &self,
        prql: String,
        params: HashMap<String, Value>,
        context: Option<QueryContext>,
    ) -> Result<(WidgetSpec, RowChangeStream)> {
        // Log with timestamp to detect rapid re-executions
        tracing::warn!(
            "[query_and_watch] CALLED - this should only happen once per query change. PRQL hash: {:x}",
            {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                prql.hash(&mut hasher);
                hasher.finish()
            }
        );

        let (sql, render_spec) = self.compile_query(prql, context.clone())?;
        tracing::debug!("[BackendEngine] Generated SQL:\n{}", sql);

        // Create materialized view FIRST, then query it for initial data.
        // This is critical for recursive CTEs: Turso only supports WITH RECURSIVE
        // in matview definitions, not in regular SELECT queries.
        let change_stream = self
            .watch_query(sql.clone(), params.clone(), context.clone())
            .await?;

        // Compute the view name to query it for initial data
        let mut params_with_context = params.clone();
        let ctx = context.unwrap_or_else(QueryContext::root);
        self.bind_context_params(&mut params_with_context, &ctx);
        let view_name = Self::compute_view_name(&sql, &params_with_context);

        // Query the materialized view for initial data
        // Use fresh connection to ensure we see the newly created view.
        // Pooled connections may have stale schema caches.
        let view_query = format!("SELECT * FROM {}", view_name);
        tracing::debug!(
            "[query_and_watch] Querying matview for initial data: {}",
            view_query
        );
        // db_handle used directly

        // Retry with fresh connections to handle schema synchronization delays
        // Also retry on "Database schema changed" which can happen when concurrent DDL operations
        // invalidate the query's view of the schema.
        let mut data = None;
        for attempt in 0..10 {
            match self.db_handle.query(&view_query, HashMap::new()).await {
                Ok(results) => {
                    data = Some(results);
                    break;
                }
                Err(e) => {
                    let err_str = format!("{:?}", e);
                    let is_retryable = err_str.contains("no such table")
                        || err_str.contains("Database schema changed");
                    if is_retryable && attempt < 9 {
                        tracing::debug!(
                            "[query_and_watch] Retryable error (attempt {}): {}",
                            attempt + 1,
                            err_str
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(
                            50 * (1 << attempt.min(4)),
                        ))
                        .await;
                        continue;
                    }
                    return Err(anyhow::anyhow!(
                        "Failed to query matview for initial data: {}",
                        e
                    ));
                }
            }
        }
        let data = data.ok_or_else(|| anyhow::anyhow!("Failed to query matview after retries"))?;

        let widget_spec = WidgetSpec::new(render_spec, data);
        Ok((widget_spec, change_stream))
    }

    /// Execute a block operation
    ///
    /// This method provides a clean interface for executing operations without exposing
    /// the internal TursoBackend. It handles locking and passes the current UI state.
    ///
    /// # Arguments
    /// * `op_name` - Name of the operation to execute (e.g., "indent", "outdent", "move_block")
    /// * `params` - Parameters for the operation (typically includes block ID and operation-specific fields)
    ///
    /// # Returns
    /// Result indicating success or failure. On success, UI should re-query to get updated data.
    ///
    /// # Example
    /// ```no_run
    /// use std::collections::HashMap;
    /// use holon::api::backend_engine::BackendEngine;
    /// use holon::holon_prql_render::types::Value;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let engine = BackendEngine::new_in_memory().await?;
    ///
    /// let mut params = HashMap::new();
    /// params.insert("id".to_string(), Value::String("block-1".to_string()));
    ///
    /// engine.execute_operation("indent", params).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_operation(
        &self,
        entity_name: &str,
        op_name: &str,
        params: StorageEntity,
    ) -> Result<()> {
        use tracing::Instrument;
        use tracing::info;

        // Create tracing span that will be bridged to OpenTelemetry
        // Use .instrument() to maintain context across async boundaries
        let span = tracing::span!(
            tracing::Level::INFO,
            "backend.execute_operation",
            "operation.entity" = entity_name,
            "operation.name" = op_name
        );

        async {
            info!(
                "[BackendEngine] execute_operation: entity={}, op={}, params={:?}",
                entity_name, op_name, params
            );

            // Build original operation for undo stack
            let original_op = Operation::new(
                entity_name,
                op_name,
                "", // display_name will be set from OperationDescriptor if needed
                params.clone(),
            );

            // Execute via dispatcher using entity_name
            // Span context will be propagated via tracing-opentelemetry bridge
            let operation_result = self.dispatcher
                .execute_operation(entity_name, op_name, params)
                .await;

            match &operation_result {
                Ok(result) => {
                    match &result.undo {
                        UndoAction::Undo(_) => {
                            info!(
                                "[BackendEngine] execute_operation succeeded: entity={}, op={} (inverse operation available)",
                                entity_name, op_name
                            );
                        }
                        UndoAction::Irreversible => {
                            info!(
                                "[BackendEngine] execute_operation succeeded: entity={}, op={} (no inverse operation)",
                                entity_name, op_name
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "[BackendEngine] Operation '{}' on entity '{}' failed: {}",
                        op_name, entity_name, e
                    );
                }
            }

            // If operation succeeded and has an inverse, push to undo stack
            if let Ok(result) = &operation_result {
                if let UndoAction::Undo(inverse_op) = &result.undo {
                    let mut undo_stack = self.undo_stack.write().await;
                    undo_stack.push(original_op, inverse_op.clone());
                }
            }

            operation_result.map(|_| ()).map_err(|e| {
                anyhow::anyhow!(
                    "Operation '{}' on entity '{}' failed: {}",
                    op_name,
                    entity_name,
                    e
                )
            })
        }
        .instrument(span)
        .await
    }

    /// Undo the last operation
    ///
    /// Executes the inverse operation from the undo stack and pushes it to the redo stack.
    /// Returns true if an operation was undone, false if the undo stack is empty.
    pub async fn undo(&self) -> Result<bool> {
        // Pop the inverse operation from undo stack (automatically moves to redo stack)
        let inverse_op = {
            let mut undo_stack = self.undo_stack.write().await;
            undo_stack
                .pop_for_undo()
                .ok_or_else(|| anyhow::anyhow!("Nothing to undo"))?
        };

        // Execute the inverse operation
        let operation_result = self
            .dispatcher
            .execute_operation(
                &inverse_op.entity_name,
                &inverse_op.op_name,
                inverse_op.params.clone(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute undo operation: {}", e))?;

        // Update the redo stack with the new inverse operation
        // The UndoStack already moved (inverse, original) to redo stack,
        // but we need to update it with the new inverse we got from execution
        if let UndoAction::Undo(new_inverse_op) = operation_result.undo {
            let mut undo_stack = self.undo_stack.write().await;
            undo_stack.update_redo_top(new_inverse_op);
        }

        Ok(true)
    }

    /// Redo the last undone operation
    ///
    /// Executes the inverse of the last undone operation and pushes it back to the undo stack.
    /// Returns true if an operation was redone, false if the redo stack is empty.
    pub async fn redo(&self) -> Result<bool> {
        // Pop the operation to redo from redo stack (automatically moves back to undo stack)
        let operation_to_redo = {
            let mut undo_stack = self.undo_stack.write().await;
            undo_stack
                .pop_for_redo()
                .ok_or_else(|| anyhow::anyhow!("Nothing to redo"))?
        };

        // Execute the operation to redo
        let operation_result = self
            .dispatcher
            .execute_operation(
                &operation_to_redo.entity_name,
                &operation_to_redo.op_name,
                operation_to_redo.params.clone(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute redo operation: {}", e))?;

        // Update the undo stack with the new inverse operation
        // The UndoStack already moved (inverse, operation_to_redo) back to undo stack,
        // but we need to update it with the new inverse we got from execution
        if let UndoAction::Undo(new_inverse_op) = operation_result.undo {
            let mut undo_stack = self.undo_stack.write().await;
            undo_stack.update_undo_top(new_inverse_op);
        }

        Ok(true)
    }

    /// Check if undo is available
    pub async fn can_undo(&self) -> bool {
        self.undo_stack.read().await.can_undo()
    }

    /// Check if redo is available
    pub async fn can_redo(&self) -> bool {
        self.undo_stack.read().await.can_redo()
    }

    /// Register a custom OperationProvider
    ///
    /// This allows registering additional operation providers for entity types.
    /// Operations are automatically discovered via the OperationProvider trait.
    ///
    /// # Example
    /// ```no_run
    /// use std::sync::Arc;
    /// use holon::api::backend_engine::BackendEngine;
    /// use holon::core::datasource::OperationProvider;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let engine = BackendEngine::new_in_memory().await?;
    ///
    /// // Register custom provider
    /// // engine.register_provider("my-entity", my_provider).await?;
    /// # Ok(())
    /// # }
    /// ```

    pub async fn available_operations(&self, entity_name: &str) -> Vec<OperationDescriptor> {
        self.dispatcher
            .operations()
            .into_iter()
            .filter(|op| op.entity_name == entity_name)
            .collect()
    }

    pub async fn has_operation(&self, entity_name: &str, op_name: &str) -> bool {
        self.dispatcher
            .operations()
            .into_iter()
            .any(|op| op.entity_name == entity_name && op.name == op_name)
    }

    /// Get the initial widget for the application root
    ///
    /// This is the main entry point for the backend-driven UI architecture.
    /// Returns a WidgetSpec containing:
    /// - render_spec: How to render the layout (e.g., columns with regions)
    /// - data: The layout data (region blocks with widths, content, etc.)
    /// - actions: Global actions available throughout the app
    ///
    /// The frontend renders this using RenderInterpreter, just like any other widget.
    /// Each region's content is rendered via `render_block this` which triggers
    /// nested query_and_watch calls.
    ///
    /// # Example
    /// ```no_run
    /// use holon::api::backend_engine::BackendEngine;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let engine = BackendEngine::new_in_memory().await?;
    /// let (widget_spec, change_stream) = engine.initial_widget().await?;
    ///
    /// // Frontend renders widget_spec using RenderInterpreter
    /// // and subscribes to change_stream for updates
    /// # Ok(())
    /// # }
    /// ```
    pub async fn initial_widget(&self) -> Result<(WidgetSpec, RowChangeStream)> {
        // Note: Block hierarchy schema is initialized during DI setup (in di/mod.rs)
        // to prevent race conditions between DDL and concurrent query_and_watch calls.

        // Load root layout block (contains block ID and PRQL source)
        let root_block = self.load_root_layout_block().await?;

        // Extract block ID (context_id for "from children" queries)
        let layout_block_id = root_block
            .get("id")
            .and_then(|v| v.as_string())
            .ok_or_else(|| anyhow::anyhow!("Root layout block has no id"))?
            .to_string();

        // Extract parent_id (context_parent_id for "from block_children" queries)
        let layout_parent_id = root_block
            .get("parent_id")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string());

        // Extract PRQL source from root block
        let prql_source = root_block
            .get("prql_source")
            .and_then(|v| v.as_string())
            .ok_or_else(|| anyhow::anyhow!("Root layout block has no PRQL source block child"))?
            .to_string();

        // Look up the block's path from blocks_with_paths for descendants queries
        let block_path = self.lookup_block_path(&layout_block_id).await?;

        // Execute the layout query with context bound to the layout block (including path)
        let context =
            QueryContext::for_block_with_path(layout_block_id, layout_parent_id, block_path);
        let (mut widget_spec, change_stream) = self
            .query_and_watch(prql_source, HashMap::new(), Some(context))
            .await?;

        // Add global actions to the root widget
        widget_spec.actions = self.build_global_actions();

        Ok((widget_spec, change_stream))
    }

    /// Look up a block's path from the blocks_with_paths materialized view
    ///
    /// Returns the hierarchical path for a block (e.g., "/parent/block_id").
    /// This path is used for descendants queries via path prefix matching.
    pub async fn lookup_block_path(&self, block_id: &str) -> Result<String> {
        let sql = "SELECT path FROM blocks_with_paths WHERE id = $block_id LIMIT 1";
        let mut params = HashMap::new();
        params.insert("block_id".to_string(), Value::String(block_id.to_string()));

        let rows = self.execute_query(sql.to_string(), params, None).await?;

        if let Some(row) = rows.first() {
            if let Some(Value::String(path)) = row.get("path") {
                return Ok(path.clone());
            }
        }

        // Block not in blocks_with_paths yet - use block_id as fallback path
        // This can happen if the matview hasn't refreshed yet
        Ok(format!("/{}", block_id))
    }

    /// Load the root layout block from the database
    ///
    /// Finds the first root block in the index.org document that has a PRQL child.
    /// The root block should have a PRQL source block child that defines the layout query.
    ///
    /// The document hierarchy is: index.org (root container) → child documents (e.g., "index").
    /// Blocks reference documents via "holon-doc://{document_id}" URIs as parent_id.
    /// We search blocks under both the root container and its child documents.
    async fn load_root_layout_block(&self) -> Result<HashMap<String, Value>> {
        use holon_api::ROOT_DOC_ID;

        // Build set of all document URIs to search: the root container and its children.
        // Root: holon-doc://index.org
        // Children: holon-doc://{child_doc_id} for each doc with parent_id = "index.org"
        let root_doc_id = ROOT_DOC_ID.to_string();
        let child_docs_sql = "SELECT id FROM documents WHERE parent_id = $root_doc_id";
        let mut params = HashMap::new();
        params.insert(
            "root_doc_id".to_string(),
            Value::String(root_doc_id.clone()),
        );
        let child_doc_rows = self
            .execute_query(child_docs_sql.to_string(), params, None)
            .await?;

        let mut doc_uris: Vec<Value> = vec![Value::String(format!("holon-doc://{}", ROOT_DOC_ID))];
        for row in &child_doc_rows {
            if let Some(Value::String(id)) = row.get("id") {
                doc_uris.push(Value::String(format!("holon-doc://{}", id)));
            }
        }

        // Find the first block with a PRQL source child under any of these documents.
        // We search direct children of the document (depth 1) and their children (depth 2).
        let placeholders: Vec<String> = doc_uris
            .iter()
            .enumerate()
            .map(|(i, _)| format!("$doc_uri_{i}"))
            .collect();
        let in_clause = placeholders.join(", ");
        let sql = format!(
            r#"
            SELECT
                b.id,
                b.parent_id,
                b.content,
                b.properties,
                src.content as prql_source
            FROM blocks b
            INNER JOIN blocks src ON src.parent_id = b.id
                AND src.content_type = 'source'
                AND src.source_language = 'prql'
            WHERE b.parent_id IN ({in_clause})
               OR b.parent_id IN (
                   SELECT id FROM blocks WHERE parent_id IN ({in_clause})
               )
            ORDER BY b.id
            LIMIT 1
            "#
        );

        let mut params = HashMap::new();
        for (i, uri) in doc_uris.into_iter().enumerate() {
            params.insert(format!("doc_uri_{i}"), uri);
        }

        let rows = self.execute_query(sql, params, None).await?;

        if rows.is_empty() {
            anyhow::bail!(
                "No root layout block found. Expected a root block with PRQL child in index.org document (under {} container).",
                ROOT_DOC_ID
            );
        }

        Ok(rows[0].clone())
    }

    /// Build global actions available throughout the app
    fn build_global_actions(&self) -> Vec<ActionSpec> {
        vec![
            ActionSpec::new("sync", "Sync", "*", "sync_from_remote").with_icon("sync"),
            ActionSpec::new("undo", "Undo", "*", "undo").with_icon("undo"),
            ActionSpec::new("redo", "Redo", "*", "redo").with_icon("redo"),
        ]
    }

    /// Map a table name to an entity name
    ///
    /// This mapping is used during query compilation to determine which
    /// entity type operations are available for a given table.
    ///
    /// # Arguments
    /// * `table_name` - Database table name (e.g., "todoist_tasks", "logseq_blocks")
    /// * `entity_name` - Entity identifier (e.g., "todoist-task", "logseq-block")
    pub async fn map_table_to_entity(&self, table_name: String, entity_name: String) {
        let mut map = self.table_to_entity_map.write().await;
        map.insert(table_name, entity_name);
    }

    /// Get the entity name for a table
    ///
    /// # Arguments
    /// * `table_name` - Database table name
    ///
    /// # Returns
    /// `Some(entity_name)` if mapped, `None` otherwise
    pub async fn get_entity_for_table(&self, table_name: &str) -> Option<String> {
        let map = self.table_to_entity_map.read().await;
        map.get(table_name).cloned()
    }

    /// Get a clone of the operation dispatcher Arc
    ///
    /// This allows querying available operations without mutating the dispatcher.
    pub fn get_dispatcher(&self) -> Arc<OperationDispatcher> {
        self.dispatcher.clone()
    }

    /// Initialize database schema and sample data if the database doesn't exist
    ///
    /// This creates the blocks table and inserts sample data for new databases.
    /// Should be called after creating the BackendEngine for a new database.
    pub async fn initialize_database_if_needed(&self, db_path: &PathBuf) -> Result<()> {
        let db_exists = db_path.exists();

        if !db_exists {
            // Create blocks table schema
            let create_table_sql = r#"
                CREATE TABLE IF NOT EXISTS blocks (
                    id TEXT PRIMARY KEY,
                    parent_id TEXT,
                    depth INTEGER NOT NULL DEFAULT 0,
                    sort_key TEXT NOT NULL,
                    content TEXT NOT NULL,
                    collapsed INTEGER NOT NULL DEFAULT 0,
                    completed INTEGER NOT NULL DEFAULT 0,
                    block_type TEXT NOT NULL DEFAULT 'text',
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
            "#;

            self.execute_query(create_table_sql.to_string(), HashMap::new(), None)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create blocks table: {}", e))?;

            // Generate proper fractional index keys for sample data
            use crate::storage::gen_key_between;

            let root_1_key = gen_key_between(None, None)
                .map_err(|e| anyhow::anyhow!("Failed to generate root-1 key: {}", e))?;
            let root_2_key = gen_key_between(Some(&root_1_key), None)
                .map_err(|e| anyhow::anyhow!("Failed to generate root-2 key: {}", e))?;

            let child_1_key = gen_key_between(None, None)
                .map_err(|e| anyhow::anyhow!("Failed to generate child-1 key: {}", e))?;
            let child_2_key = gen_key_between(Some(&child_1_key), None)
                .map_err(|e| anyhow::anyhow!("Failed to generate child-2 key: {}", e))?;

            let grandchild_1_key = gen_key_between(None, None)
                .map_err(|e| anyhow::anyhow!("Failed to generate grandchild-1 key: {}", e))?;

            // Insert sample data for testing with fractional indexing sort_keys
            let sample_data_sql = format!(
                r#"
                INSERT OR IGNORE INTO blocks (id, parent_id, depth, sort_key, content, block_type, completed)
                VALUES
                    ('root-1', NULL, 0, '{}', 'Welcome to Block Outliner', 'heading', 0),
                    ('child-1', 'root-1', 1, '{}', 'This is a child block', 'text', 0),
                    ('child-2', 'root-1', 1, '{}', 'Another child block', 'text', 1),
                    ('grandchild-1', 'child-1', 2, '{}', 'A nested grandchild', 'text', 0),
                    ('root-2', NULL, 0, '{}', 'Second top-level block', 'heading', 0)
            "#,
                root_1_key, child_1_key, child_2_key, grandchild_1_key, root_2_key
            );

            self.execute_query(sample_data_sql.to_string(), HashMap::new(), None)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to insert sample data: {}", e))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::datasource::{OperationProvider, OperationResult, Result as DatasourceResult};
    use crate::di::test_helpers::{create_test_engine, create_test_engine_with_providers};
    use crate::storage::DbHandle;
    use async_trait::async_trait;
    use holon_api::OperationDescriptor;
    use std::sync::Arc;

    // Simple SQL-based provider for testing
    struct SqlOperationProvider {
        db_handle: DbHandle,
        table_name: String,
        entity_name: String,
        entity_short_name: String,
    }

    impl SqlOperationProvider {
        fn new(
            db_handle: DbHandle,
            table_name: String,
            entity_name: String,
            entity_short_name: String,
        ) -> Self {
            Self {
                db_handle,
                table_name,
                entity_name,
                entity_short_name,
            }
        }
    }

    /// Create basic CRUD operations for testing
    fn test_crud_operations(
        entity_name: &str,
        entity_short_name: &str,
        _table_name: &str,
        _id_column: &str,
    ) -> Vec<OperationDescriptor> {
        vec![
            OperationDescriptor {
                entity_name: entity_name.to_string(),
                entity_short_name: entity_short_name.to_string(),
                id_column: "id".to_string(),
                name: "create".to_string(),
                display_name: "Create".to_string(),
                description: format!("Create a new {}", entity_short_name),
                required_params: vec![],
                affected_fields: vec![],
                param_mappings: vec![],
                precondition: None,
            },
            OperationDescriptor {
                entity_name: entity_name.to_string(),
                entity_short_name: entity_short_name.to_string(),
                id_column: "id".to_string(),
                name: "update".to_string(),
                display_name: "Update".to_string(),
                description: format!("Update {}", entity_short_name),
                required_params: vec![],
                affected_fields: vec![],
                param_mappings: vec![],
                precondition: None,
            },
            OperationDescriptor {
                entity_name: entity_name.to_string(),
                entity_short_name: entity_short_name.to_string(),
                id_column: "id".to_string(),
                name: "delete".to_string(),
                display_name: "Delete".to_string(),
                description: format!("Delete {}", entity_short_name),
                required_params: vec![],
                affected_fields: vec![],
                param_mappings: vec![],
                precondition: None,
            },
        ]
    }

    #[async_trait]
    impl OperationProvider for SqlOperationProvider {
        fn operations(&self) -> Vec<OperationDescriptor> {
            test_crud_operations(
                &self.entity_name,
                &self.entity_short_name,
                &self.table_name,
                "id",
            )
        }

        async fn execute_operation(
            &self,
            entity_name: &str,
            op_name: &str,
            params: StorageEntity,
        ) -> DatasourceResult<OperationResult> {
            if entity_name != self.entity_name {
                return Err(format!(
                    "Expected entity_name '{}', got '{}'",
                    self.entity_name, entity_name
                )
                .into());
            }

            match op_name {
                "set_field" => {
                    let id = params
                        .get("id")
                        .and_then(|v| v.as_string())
                        .ok_or_else(|| "Missing 'id' parameter".to_string())?;
                    let field = params
                        .get("field")
                        .and_then(|v| v.as_string())
                        .ok_or_else(|| "Missing 'field' parameter".to_string())?;
                    let value = params
                        .get("value")
                        .ok_or_else(|| "Missing 'value' parameter".to_string())?;

                    // Convert value to SQL
                    let sql_value = match value {
                        Value::String(s) => format!("'{}'", s.replace("'", "''")),
                        Value::Integer(i) => i.to_string(),
                        Value::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
                        Value::Null => "NULL".to_string(),
                        Value::DateTime(s) => format!("'{}'", s.replace("'", "''")),
                        Value::Json(s) => format!("'{}'", s.replace("'", "''")),
                        Value::Float(f) => f.to_string(),
                        Value::Array(_) | Value::Object(_) => {
                            todo!("Complex types not supported in test")
                        }
                    };

                    let sql = format!(
                        "UPDATE {} SET {} = {} WHERE id = '{}'",
                        self.table_name,
                        field,
                        sql_value,
                        id.replace("'", "''")
                    );

                    self.db_handle
                        .execute(&sql, vec![])
                        .await
                        .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                    Ok(OperationResult::irreversible(Vec::new()))
                }
                "create" => {
                    let mut columns = Vec::new();
                    let mut values = Vec::new();
                    for (key, value) in params.iter() {
                        columns.push(key.clone());
                        let sql_value = match value {
                            Value::String(s) => format!("'{}'", s.replace("'", "''")),
                            Value::Integer(i) => i.to_string(),
                            Value::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
                            Value::Null => "NULL".to_string(),
                            Value::DateTime(s) => format!("'{}'", s.replace("'", "''")),
                            Value::Json(s) => format!("'{}'", s.replace("'", "''")),
                            Value::Float(f) => f.to_string(),
                            Value::Array(_) | Value::Object(_) => {
                                todo!("Complex types not supported in test")
                            }
                        };
                        values.push(sql_value);
                    }

                    let sql = format!(
                        "INSERT INTO {} ({}) VALUES ({})",
                        self.table_name,
                        columns.join(", "),
                        values.join(", ")
                    );

                    self.db_handle
                        .execute(&sql, vec![])
                        .await
                        .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                    Ok(OperationResult::irreversible(Vec::new()))
                }
                "delete" => {
                    let id = params
                        .get("id")
                        .and_then(|v| v.as_string())
                        .ok_or_else(|| "Missing 'id' parameter".to_string())?;

                    let sql = format!(
                        "DELETE FROM {} WHERE id = '{}'",
                        self.table_name,
                        id.replace("'", "''")
                    );

                    self.db_handle
                        .execute(&sql, vec![])
                        .await
                        .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                    Ok(OperationResult::irreversible(Vec::new()))
                }
                _ => Err(format!("Unknown operation: {}", op_name).into()),
            }
        }
    }

    #[test]
    fn test_inline_parameters() {
        let mut params = HashMap::new();
        params.insert(
            "context_id".to_string(),
            Value::String("block-123".to_string()),
        );
        params.insert("context_parent_id".to_string(), Value::Null);
        params.insert("num".to_string(), Value::Integer(42));
        params.insert("flag".to_string(), Value::Boolean(true));

        // Test string parameter
        let sql = "SELECT * FROM blocks WHERE id = $context_id";
        let result = BackendEngine::inline_parameters(sql, &params);
        assert_eq!(result, "SELECT * FROM blocks WHERE id = 'block-123'");

        // Test NULL parameter
        let sql = "SELECT * FROM blocks WHERE parent_id = $context_parent_id";
        let result = BackendEngine::inline_parameters(sql, &params);
        assert_eq!(result, "SELECT * FROM blocks WHERE parent_id = NULL");

        // Test integer parameter
        let sql = "SELECT * FROM blocks WHERE count = $num";
        let result = BackendEngine::inline_parameters(sql, &params);
        assert_eq!(result, "SELECT * FROM blocks WHERE count = 42");

        // Test boolean parameter
        let sql = "SELECT * FROM blocks WHERE active = $flag";
        let result = BackendEngine::inline_parameters(sql, &params);
        assert_eq!(result, "SELECT * FROM blocks WHERE active = 1");

        // Test multiple parameters
        let sql = "SELECT * FROM blocks WHERE id = $context_id AND parent_id = $context_parent_id";
        let result = BackendEngine::inline_parameters(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM blocks WHERE id = 'block-123' AND parent_id = NULL"
        );

        // Test unknown parameter is preserved
        let sql = "SELECT * FROM blocks WHERE id = $unknown_param";
        let result = BackendEngine::inline_parameters(sql, &params);
        assert_eq!(result, "SELECT * FROM blocks WHERE id = $unknown_param");

        // Test SQL injection prevention (quotes are escaped)
        let mut params_with_quote = HashMap::new();
        params_with_quote.insert("name".to_string(), Value::String("O'Brien".to_string()));
        let sql = "SELECT * FROM users WHERE name = $name";
        let result = BackendEngine::inline_parameters(sql, &params_with_quote);
        assert_eq!(result, "SELECT * FROM users WHERE name = 'O''Brien'");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_render_engine_creation() {
        let engine = create_test_engine().await;
        assert!(engine.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compile_simple_query() {
        let engine = create_test_engine().await.unwrap();

        let prql = r#"
            from blocks
            render (text "Hello")
        "#;

        let result = engine.compile_query(prql.to_string(), None);
        assert!(result.is_ok());

        let (_sql, spec) = result.unwrap();
        match spec.root() {
            Some(holon_prql_render::RenderExpr::FunctionCall { name, .. }) => {
                assert_eq!(name, "text");
            }
            _ => panic!("Expected function call"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_query_with_parameters() {
        let engine = create_test_engine().await.unwrap();

        // Create a test table and insert data using db_handle
        let _ = engine
            .db_handle()
            .execute_ddl("DROP TABLE IF EXISTS test_blocks")
            .await;
        engine
            .db_handle()
            .execute_ddl(
                "CREATE TABLE test_blocks (id TEXT PRIMARY KEY, title TEXT, depth INTEGER)",
            )
            .await
            .unwrap();

        engine
            .db_handle()
            .execute(
                "INSERT INTO test_blocks (id, title, depth) VALUES ('block-1', 'Test Block', 0)",
                vec![],
            )
            .await
            .unwrap();

        engine
            .db_handle()
            .execute(
                "INSERT INTO test_blocks (id, title, depth) VALUES ('block-2', 'Nested Block', 1)",
                vec![],
            )
            .await
            .unwrap();

        // Test query with parameter binding
        let mut params = HashMap::new();
        params.insert("min_depth".to_string(), Value::Integer(0));

        let sql = "SELECT id, title, depth FROM test_blocks WHERE depth >= $min_depth ORDER BY id";
        let results = engine
            .execute_query(sql.to_string(), params, None)
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("id").unwrap().as_string(), Some("block-1"));
        assert_eq!(results[1].get("id").unwrap().as_string(), Some("block-2"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_parameter_binding() {
        let engine = create_test_engine().await.unwrap();

        // Create table and insert data using db_handle
        let _ = engine
            .db_handle()
            .execute_ddl("DROP TABLE IF EXISTS users")
            .await;
        engine
            .db_handle()
            .execute_ddl("CREATE TABLE users (id TEXT, name TEXT, age INTEGER)")
            .await
            .unwrap();

        engine
            .db_handle()
            .execute(
                "INSERT INTO users VALUES ('u1', 'Alice', 30), ('u2', 'Bob', 25), ('u3', 'Charlie', 35)",
                vec![],
            )
            .await
            .unwrap();

        // Test multiple parameters
        let mut params = HashMap::new();
        params.insert("min_age".to_string(), Value::Integer(25));
        params.insert("max_age".to_string(), Value::Integer(35));

        let sql =
            "SELECT name, age FROM users WHERE age >= $min_age AND age <= $max_age ORDER BY age";
        let results = engine
            .execute_query(sql.to_string(), params, None)
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("name").unwrap().as_string(), Some("Bob"));
        assert_eq!(results[2].get("name").unwrap().as_string(), Some("Charlie"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_operation() {
        // Use the provider factory pattern so the provider gets the correct db_handle
        let engine = create_test_engine_with_providers(":memory:".into(), |module| {
            module.with_operation_provider_factory(|backend| {
                // Get db_handle from backend using try_read to avoid blocking
                let db_handle =
                    tokio::task::block_in_place(|| backend.blocking_read().handle().clone());
                Arc::new(SqlOperationProvider::new(
                    db_handle,
                    "blocks".to_string(),
                    "blocks".to_string(),
                    "block".to_string(),
                ))
            })
        })
        .await
        .unwrap();

        // Create test table using db_handle
        let _ = engine
            .db_handle()
            .execute_ddl("DROP TABLE IF EXISTS blocks")
            .await;
        engine
            .db_handle()
            .execute_ddl(
                "CREATE TABLE blocks (id TEXT PRIMARY KEY, content TEXT, completed BOOLEAN)",
            )
            .await
            .unwrap();

        engine
            .db_handle()
            .execute(
                "INSERT INTO blocks (id, content, completed) VALUES ('block-1', 'Test task', 0)",
                vec![],
            )
            .await
            .unwrap();

        // Execute operation to update completed field
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String("block-1".to_string()));
        params.insert("field".to_string(), Value::String("completed".to_string()));
        params.insert("value".to_string(), Value::Boolean(true));

        let result = engine
            .execute_operation("blocks", "set_field", params)
            .await;
        assert!(result.is_ok(), "Operation should succeed: {:?}", result);

        // Verify the update
        let sql = "SELECT id, completed FROM blocks WHERE id = 'block-1'";
        let results = engine
            .execute_query(sql.to_string(), HashMap::new(), None)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("id").unwrap().as_string(), Some("block-1"));

        // SQLite stores booleans as integers (0/1), so check for Integer value
        match results[0].get("completed").unwrap() {
            Value::Integer(i) => assert_eq!(*i, 1, "Expected completed=1 (true)"),
            Value::Boolean(b) => assert!(b, "Expected completed=true"),
            other => panic!("Unexpected value type for completed: {:?}", other),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_operation_failure() {
        let engine = create_test_engine().await.unwrap();

        // Try to execute non-existent operation
        let params = HashMap::new();
        let result = engine
            .execute_operation("blocks", "nonexistent", params)
            .await;

        assert!(result.is_err(), "Should fail for non-existent operation");
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("nonexistent"),
            "Error should mention operation name"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_custom_operation() {
        // Use the provider factory pattern so the provider gets the correct db_handle
        let engine = create_test_engine_with_providers(":memory:".into(), |module| {
            module.with_operation_provider_factory(|backend| {
                // Get db_handle from backend using block_in_place to avoid blocking issues
                let db_handle =
                    tokio::task::block_in_place(|| backend.blocking_read().handle().clone());
                Arc::new(SqlOperationProvider::new(
                    db_handle,
                    "blocks".to_string(),
                    "blocks".to_string(),
                    "block".to_string(),
                ))
            })
        })
        .await
        .unwrap();

        // Verify operations are available
        let ops = engine.available_operations("blocks").await;
        assert!(!ops.is_empty(), "Should have operations available");
        // Verify we get OperationDescriptor objects with proper properties
        assert!(ops.iter().all(|op| op.entity_name == "blocks"));
        assert!(ops.iter().any(|op| !op.name.is_empty()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_operations_inference() {
        let engine = create_test_engine().await.unwrap();

        // PRQL query with widgets that reference direct table columns
        let prql = r#"
from blocks
select {id, content, completed}
render (list item_template:(row (checkbox checked:this.completed) (text content:this.content)))
        "#;

        let result = engine.compile_query(prql.to_string(), None);
        assert!(
            result.is_ok(),
            "Should compile query with render: {:?}",
            result.err()
        );

        let (_sql, spec) = result.unwrap();

        // Debug: print the tree structure
        eprintln!("Root expr: {:#?}", spec.root());

        // Helper to find all widgets with operations in the tree
        fn find_all_operations(
            expr: &holon_prql_render::RenderExpr,
        ) -> Vec<&holon_prql_render::OperationWiring> {
            let mut ops = Vec::new();
            match expr {
                holon_prql_render::RenderExpr::FunctionCall {
                    operations, args, ..
                } => {
                    ops.extend(operations.iter());
                    for arg in args {
                        ops.extend(find_all_operations(&arg.value));
                    }
                }
                holon_prql_render::RenderExpr::Array { items } => {
                    for item in items {
                        ops.extend(find_all_operations(item));
                    }
                }
                holon_prql_render::RenderExpr::BinaryOp { left, right, .. } => {
                    ops.extend(find_all_operations(left));
                    ops.extend(find_all_operations(right));
                }
                _ => {}
            }
            ops
        }

        let all_ops = find_all_operations(spec.root().expect("RenderSpec must have root"));
        assert!(
            !all_ops.is_empty(),
            "Should have auto-inferred operations in tree"
        );

        // Find checkbox operation
        let checkbox_op = all_ops.iter().find(|op| op.widget_type == "checkbox");
        assert!(checkbox_op.is_some(), "Should find checkbox operation");

        let checkbox = checkbox_op.unwrap();
        assert_eq!(checkbox.modified_param, "checked");
        assert_eq!(checkbox.descriptor.id_column, "id");

        // Find text operation
        let text_op = all_ops.iter().find(|op| op.widget_type == "text");
        assert!(text_op.is_some(), "Should find text operation");

        let text = text_op.unwrap();
        assert_eq!(text.modified_param, "content");
        assert_eq!(text.descriptor.id_column, "id");
    }
}
