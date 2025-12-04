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

use crate::api::operation_dispatcher::OperationDispatcher;
use crate::core::datasource::OperationProvider;
use crate::core::transform::TransformPipeline;
use crate::storage::turso::{RowChangeStream, TursoBackend};
use crate::storage::types::StorageEntity;
use holon_api::{Operation, OperationDescriptor, Value};
use holon_core::{UndoAction, UndoStack};
use query_render::RenderSpec;

/// Main render engine managing database, query compilation, and operations
pub struct BackendEngine {
    backend: Arc<RwLock<TursoBackend>>,
    dispatcher: Arc<OperationDispatcher>, // Operation dispatcher for routing operations
    transform_pipeline: Arc<TransformPipeline>, // Pipeline for AST transformations
    table_to_entity_map: Arc<RwLock<HashMap<String, String>>>, // Maps table names to entity names
    undo_stack: Arc<RwLock<UndoStack>>,   // Undo/redo history
    // CDC connection kept alive for streaming
    // CRITICAL: This must stay alive for CDC callbacks to work
    // The callback closure captures the channel sender, which closes the stream if dropped
    // Uses interior mutability so watch_query can take &self
    _cdc_conn: Arc<tokio::sync::Mutex<Option<Arc<tokio::sync::Mutex<turso::Connection>>>>>,
}

impl BackendEngine {
    /// Create BackendEngine from dependencies (for dependency injection)
    ///
    /// This constructor allows creating BackendEngine with pre-constructed dependencies,
    /// useful for dependency injection frameworks.
    pub fn from_dependencies(
        backend: Arc<RwLock<TursoBackend>>,
        dispatcher: Arc<OperationDispatcher>,
        transform_pipeline: Arc<TransformPipeline>,
    ) -> Result<Self> {
        // Operations are now provided via OperationProvider implementations
        // No legacy operations need to be registered

        Ok(Self {
            backend,
            dispatcher,
            transform_pipeline,
            table_to_entity_map: Arc::new(RwLock::new(HashMap::new())),
            undo_stack: Arc::new(RwLock::new(UndoStack::default())),
            _cdc_conn: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    /// Compile a PRQL query with render() into SQL and UI specification
    ///
    /// Automatically infers operation wirings from PRQL lineage analysis.
    /// Widgets that reference direct table columns will have operations populated.
    ///
    /// This method:
    /// 1. Parses the PRQL query to RQ AST and extracts table name
    /// 2. Applies AST transformations (e.g., adding `_change_origin` column)
    /// 3. Generates SQL from the transformed RQ
    /// 4. Extracts available columns from the query
    /// 5. Queries OperationDispatcher for compatible operations
    /// 6. Replaces placeholder operations with real OperationDescriptors
    /// 7. For UNION queries with row_templates, wires operations per-template using entity_name
    pub fn compile_query(&self, prql: String) -> Result<(String, RenderSpec)> {
        // Step 1: Parse query to RQ AST with placeholder operations
        // This gives us the RQ AST before SQL generation
        let parsed = query_render::parse_query_render_to_rq(&prql)?;
        let mut render_spec = parsed.render_spec;
        let all_selected_columns = parsed.available_columns;

        // Step 2: Apply RQ transformations (e.g., ChangeOriginTransformer)
        let transformed_rq = self.transform_pipeline.transform_rq(parsed.rq)?;

        // Step 3: Generate SQL from the transformed RQ
        let sql = query_render::ParsedQueryRender::to_sql_from_rq(&transformed_rq)?;

        // Step 4: Extract table name from query (needed for entity lookup)
        let table_name = self.extract_table_name_from_prql(&prql)?;

        // Step 5: Walk the tree and enhance operations with real descriptors from dispatcher
        // Pass all selected columns as context for operation filtering
        // This now includes ALL columns from the query result (e.g., parent_id), not just widget-referenced columns
        self.enhance_operations_with_dispatcher(
            &mut render_spec.root,
            &table_name,
            &all_selected_columns,
        )?;

        // Step 6: For UNION queries with row_templates, wire operations per-template
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
        expr: &mut query_render::RenderExpr,
        table_name: &str,
        all_selected_columns: &[String],
    ) -> Result<()> {
        match expr {
            query_render::RenderExpr::FunctionCall {
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
                        new_operations.push(query_render::OperationWiring {
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
            query_render::RenderExpr::Array { items } => {
                for item in items.iter_mut() {
                    self.enhance_operations_with_dispatcher(
                        item,
                        table_name,
                        all_selected_columns,
                    )?;
                }
            }
            query_render::RenderExpr::BinaryOp { left, right, .. } => {
                self.enhance_operations_with_dispatcher(left, table_name, all_selected_columns)?;
                self.enhance_operations_with_dispatcher(right, table_name, all_selected_columns)?;
            }
            query_render::RenderExpr::Object { fields } => {
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
    fn extract_available_columns_from_args(&self, args: &[query_render::Arg]) -> AvailableColumns {
        let mut columns = Vec::new();
        for arg in args {
            match &arg.value {
                query_render::RenderExpr::ColumnRef { name } => {
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
        expr: &query_render::RenderExpr,
        columns: &mut Vec<String>,
    ) -> AvailableColumns {
        match expr {
            query_render::RenderExpr::ColumnRef { name } => {
                if name == "this.*" {
                    return AvailableColumns::All;
                }
                if !columns.contains(name) {
                    columns.push(name.clone());
                }
            }
            query_render::RenderExpr::FunctionCall { args, .. } => {
                for arg in args {
                    if let AvailableColumns::All =
                        self.collect_columns_from_expr(&arg.value, columns)
                    {
                        return AvailableColumns::All;
                    }
                }
            }
            query_render::RenderExpr::Array { items } => {
                for item in items {
                    if let AvailableColumns::All = self.collect_columns_from_expr(item, columns) {
                        return AvailableColumns::All;
                    }
                }
            }
            query_render::RenderExpr::BinaryOp { left, right, .. } => {
                if let AvailableColumns::All = self.collect_columns_from_expr(left, columns) {
                    return AvailableColumns::All;
                }
                if let AvailableColumns::All = self.collect_columns_from_expr(right, columns) {
                    return AvailableColumns::All;
                }
            }
            query_render::RenderExpr::Object { fields } => {
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

    /// Execute a SQL query and return the result set
    ///
    /// Supports parameter binding by replacing `$param_name` placeholders with actual values.
    /// Parameters are bound safely using SQL parameter binding to prevent SQL injection.
    pub async fn execute_query(
        &self,
        sql: String,
        params: HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let backend = self.backend.read().await;
        backend
            .execute_sql(&sql, params)
            .await
            .map_err(|e| anyhow::anyhow!("SQL execution failed: {}", e))
    }

    /// Watch a query for changes via CDC streaming
    ///
    /// Returns a stream of RowChange events from the underlying database.
    /// The CDC connection is stored in the BackendEngine to keep it alive.
    ///
    /// Note: The SQL should include `_change_origin` column for CDC trace propagation.
    /// When using `compile_query` or `query_and_watch`, this is handled automatically
    /// by the TransformPipeline.
    pub async fn watch_query(
        &self,
        sql: String,
        _params: HashMap<String, Value>,
    ) -> Result<RowChangeStream> {
        // Generate a unique view name for this query
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        let view_name = format!("watch_view_{:x}", hasher.finish());

        tracing::debug!(
            "[watch_query] Starting materialized view creation for view: {}",
            view_name
        );
        tracing::debug!("[watch_query] SQL query: {}", sql);

        // Create materialized view for the query
        let backend = self.backend.read().await;
        let conn = backend
            .get_connection()
            .map_err(|e| anyhow::anyhow!("Failed to get connection: {}", e))?;

        // Drop the view if it exists
        // Turso doesn't support IF NOT EXISTS for materialized views
        // Dropping the view will automatically cascade to its internal state table
        tracing::debug!(
            "[watch_query] Attempting to drop view if exists: {}",
            view_name
        );
        let drop_result = conn
            .execute(&format!("DROP VIEW IF EXISTS {}", view_name), ())
            .await;
        match &drop_result {
            Ok(_) => tracing::debug!("[watch_query] DROP VIEW IF EXISTS succeeded"),
            Err(e) => tracing::debug!("[watch_query] DROP VIEW IF EXISTS failed: {}", e),
        }

        // Check if view exists and drop without IF EXISTS if needed
        let check_view_sql = format!(
            "SELECT name FROM sqlite_master WHERE type='view' AND name='{}'",
            view_name
        );
        tracing::debug!("[watch_query] Checking if view exists: {}", check_view_sql);
        if let Ok(mut stmt) = conn.prepare(&check_view_sql).await {
            match stmt.query_row(()).await {
                Ok(_) => {
                    tracing::debug!("[watch_query] View still exists, dropping without IF EXISTS");
                    let drop_result = conn.execute(&format!("DROP VIEW {}", view_name), ()).await;
                    match &drop_result {
                        Ok(_) => tracing::debug!("[watch_query] DROP VIEW succeeded"),
                        Err(e) => tracing::warn!("[watch_query] DROP VIEW failed: {}", e),
                    }
                }
                Err(_) => tracing::debug!("[watch_query] View does not exist in schema"),
            }
        } else {
            tracing::warn!("[watch_query] Failed to prepare check_view_sql");
        }

        // Explicitly drop the DBSP state table if it exists
        // This handles the case where a previous materialized view creation failed
        // partway through, leaving the DBSP state table but not the view itself
        // The DBSP state table name pattern is: __turso_internal_dbsp_state_v{version}_{view_name}
        // Current version is 1, but we check for any version to be safe
        let dbsp_table_pattern = format!("__turso_internal_dbsp_state_v%_{}", view_name);
        let check_dbsp_sql = format!(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{}'",
            dbsp_table_pattern
        );
        // Debug: List all tables/views in sqlite_master to see what exists
        let debug_list_sql = "SELECT type, name FROM sqlite_master WHERE name LIKE '%watch_view%' OR name LIKE '%__turso_internal_dbsp_state%' ORDER BY type, name";
        tracing::debug!("[watch_query] Listing relevant tables/views in sqlite_master");
        if let Ok(mut stmt) = conn.prepare(debug_list_sql).await {
            if let Ok(mut rows) = stmt.query(()).await {
                let mut found_items = Vec::new();
                while let Ok(Some(row)) = rows.next().await {
                    if let (Ok(type_val), Ok(name_val)) =
                        (row.get::<String>(0), row.get::<String>(1))
                    {
                        found_items.push(format!("{}: {}", type_val, name_val));
                    }
                }
                if found_items.is_empty() {
                    tracing::debug!(
                        "[watch_query] No relevant tables/views found in sqlite_master"
                    );
                } else {
                    tracing::debug!("[watch_query] Found in sqlite_master: {:?}", found_items);
                }
            }
        }

        tracing::debug!(
            "[watch_query] Checking for existing DBSP state tables: {}",
            check_dbsp_sql
        );
        if let Ok(mut stmt) = conn.prepare(&check_dbsp_sql).await {
            let rows = stmt.query(()).await.ok();
            if let Some(mut rows) = rows {
                let mut found_tables = Vec::new();
                while let Ok(Some(row)) = rows.next().await {
                    if let Ok(table_name) = row.get::<String>(0) {
                        found_tables.push(table_name.clone());
                        tracing::debug!(
                            "[watch_query] Found existing DBSP state table: {}",
                            table_name
                        );
                        // Drop the DBSP state table
                        let drop_result = conn
                            .execute(&format!("DROP TABLE IF EXISTS {}", table_name), ())
                            .await;
                        match &drop_result {
                            Ok(_) => tracing::debug!(
                                "[watch_query] Successfully dropped DBSP table: {}",
                                table_name
                            ),
                            Err(e) => tracing::warn!(
                                "[watch_query] Failed to drop DBSP table {}: {}",
                                table_name,
                                e
                            ),
                        }
                    }
                }
                if found_tables.is_empty() {
                    tracing::debug!("[watch_query] No existing DBSP state tables found");
                }
            } else {
                tracing::debug!("[watch_query] Failed to query for DBSP tables");
            }
        } else {
            tracing::warn!("[watch_query] Failed to prepare check_dbsp_sql");
        }

        // Create the materialized view
        let create_view_sql = format!("CREATE MATERIALIZED VIEW {} AS {}", view_name, sql);
        tracing::debug!(
            "[watch_query] Creating materialized view: {}",
            create_view_sql
        );
        let create_result = conn.execute(&create_view_sql, ()).await;
        match &create_result {
            Ok(_) => tracing::debug!(
                "[watch_query] Successfully created materialized view: {}",
                view_name
            ),
            Err(e) => {
                tracing::error!("[watch_query] Failed to create materialized view: {}", e);
                tracing::error!("[watch_query] View name: {}, SQL: {}", view_name, sql);
            }
        }
        create_result.map_err(|e| anyhow::anyhow!("Failed to create materialized view: {}", e))?;

        drop(backend); // Release read lock before acquiring for row_changes

        // Set up change stream for the view
        tracing::debug!("[watch_query] Setting up CDC stream...");
        let backend = self.backend.read().await;
        tracing::debug!("[watch_query] Got backend read lock for CDC setup");

        let (cdc_conn, stream) = backend
            .row_changes()
            .map_err(|e| anyhow::anyhow!("Failed to set up CDC stream: {}", e))?;

        // Check CDC connection state
        let cdc_autocommit = cdc_conn.is_autocommit().unwrap_or(true);
        tracing::debug!(
            "[watch_query] CDC connection created. Autocommit: {}",
            cdc_autocommit
        );

        // Store the connection to keep it alive for CDC callbacks
        // CRITICAL: The connection MUST stay alive for the callback closure to stay alive
        // The callback closure captures the channel sender (tx), which closes the stream if dropped
        let mut cdc_conn_guard = self._cdc_conn.lock().await;
        *cdc_conn_guard = Some(Arc::new(tokio::sync::Mutex::new(cdc_conn)));
        tracing::debug!("[watch_query] CDC connection stored, stream ready");

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
    /// Returns the render specification, current table data, and a stream of ongoing changes.
    ///
    /// # Returns
    /// A tuple containing:
    /// - `RenderSpec`: UI rendering specification from the PRQL query
    /// - `Vec<Entity>`: Current query results
    /// - `RowChangeStream`: Stream of ongoing changes to the query results
    pub async fn query_and_watch(
        &self,
        prql: String,
        params: HashMap<String, Value>,
    ) -> Result<(RenderSpec, Vec<HashMap<String, Value>>, RowChangeStream)> {
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

        let (sql, render_spec) = self.compile_query(prql)?;
        let current_data = self.execute_query(sql.clone(), params.clone()).await?;
        let change_stream = self.watch_query(sql, params).await?;

        Ok((render_spec, current_data, change_stream))
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
    /// use holon::query_render::types::Value;
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
        use tracing::info;
        use tracing::Instrument;

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
            let inverse_result = self.dispatcher
                .execute_operation(entity_name, op_name, params)
                .await;

            match &inverse_result {
                Ok(UndoAction::Undo(_)) => {
                    info!(
                        "[BackendEngine] execute_operation succeeded: entity={}, op={} (inverse operation available)",
                        entity_name, op_name
                    );
                }
                Ok(UndoAction::Irreversible) => {
                    info!(
                        "[BackendEngine] execute_operation succeeded: entity={}, op={} (no inverse operation)",
                        entity_name, op_name
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "[BackendEngine] Operation '{}' on entity '{}' failed: {}",
                        op_name, entity_name, e
                    );
                }
            }

            // If operation succeeded and has an inverse, push to undo stack
            if let Ok(UndoAction::Undo(inverse_op)) = &inverse_result {
                let mut undo_stack = self.undo_stack.write().await;
                undo_stack.push(original_op, inverse_op.clone());
            }

            inverse_result.map(|_| ()).map_err(|e| {
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
        let new_inverse = self
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
        if let UndoAction::Undo(new_inverse_op) = new_inverse {
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
        let new_inverse = self
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
        if let UndoAction::Undo(new_inverse_op) = new_inverse {
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

    /// Execute a closure with read access to the backend
    ///
    /// This is a helper for testing and advanced use cases where direct
    /// backend access is needed.
    pub async fn with_backend_read<F, Fut, R>(&self, f: F) -> R
    where
        F: FnOnce(&TursoBackend) -> Fut,
        Fut: std::future::Future<Output = R>,
    {
        let backend = self.backend.read().await;
        f(&*backend).await
    }

    /// Execute a closure with write access to the backend
    ///
    /// This is a helper for testing and advanced use cases where direct
    /// backend access is needed.
    pub async fn with_backend_write<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut TursoBackend) -> R,
    {
        let mut backend = self.backend.write().await;
        f(&mut *backend)
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

    /// Get a clone of the backend Arc
    ///
    /// This allows sharing the backend with QueryableCache instances.
    pub fn get_backend(&self) -> Arc<RwLock<TursoBackend>> {
        self.backend.clone()
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

            self.execute_query(create_table_sql.to_string(), HashMap::new())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create blocks table: {}", e))?;

            // Generate proper fractional index keys for sample data
            use crate::storage::fractional_index::gen_key_between;

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

            self.execute_query(sample_data_sql.to_string(), HashMap::new())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to insert sample data: {}", e))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::datasource::{OperationProvider, Result as DatasourceResult, UndoAction};
    use crate::di::test_helpers::{create_test_engine, create_test_engine_with_providers};
    use async_trait::async_trait;
    use holon_api::OperationDescriptor;
    use std::sync::Arc;

    // Simple SQL-based provider for testing
    struct SqlOperationProvider {
        backend: Arc<RwLock<TursoBackend>>,
        table_name: String,
        entity_name: String,
        entity_short_name: String,
    }

    impl SqlOperationProvider {
        fn new(
            backend: Arc<RwLock<TursoBackend>>,
            table_name: String,
            entity_name: String,
        ) -> Self {
            // Derive short_name from entity_name for tests (e.g., "test_tasks" -> "task")
            let short_name = entity_name
                .strip_prefix("test_")
                .unwrap_or(&entity_name)
                .trim_end_matches('s')
                .to_string();
            Self {
                backend,
                table_name,
                entity_name,
                entity_short_name: short_name,
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
        ) -> DatasourceResult<UndoAction> {
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

                    let backend = self.backend.write().await;
                    let conn = backend
                        .get_connection()
                        .map_err(|e| format!("Failed to get connection: {}", e))?;

                    // Convert value to SQL
                    let sql_value = match value {
                        Value::String(s) => format!("'{}'", s.replace("'", "''")),
                        Value::Integer(i) => i.to_string(),
                        Value::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
                        Value::Null => "NULL".to_string(),
                        Value::DateTime(s) => format!("'{}'", s.replace("'", "''")),
                        Value::Json(s) => format!("'{}'", s.replace("'", "''")),
                        Value::Reference(r) => format!("'{}'", r.replace("'", "''")),
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
                    conn.execute(&sql, ())
                        .await
                        .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                    Ok(UndoAction::Irreversible)
                }
                "create" => {
                    let backend = self.backend.write().await;
                    let conn = backend
                        .get_connection()
                        .map_err(|e| format!("Failed to get connection: {}", e))?;

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
                            Value::Reference(r) => format!("'{}'", r.replace("'", "''")),
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
                    conn.execute(&sql, ())
                        .await
                        .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                    Ok(UndoAction::Irreversible)
                }
                "delete" => {
                    let id = params
                        .get("id")
                        .and_then(|v| v.as_string())
                        .ok_or_else(|| "Missing 'id' parameter".to_string())?;

                    let backend = self.backend.write().await;
                    let conn = backend
                        .get_connection()
                        .map_err(|e| format!("Failed to get connection: {}", e))?;

                    let sql = format!(
                        "DELETE FROM {} WHERE id = '{}'",
                        self.table_name,
                        id.replace("'", "''")
                    );
                    conn.execute(&sql, ())
                        .await
                        .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                    Ok(UndoAction::Irreversible)
                }
                _ => Err(format!("Unknown operation: {}", op_name).into()),
            }
        }
    }

    #[tokio::test]
    async fn test_render_engine_creation() {
        let engine = create_test_engine().await;
        assert!(engine.is_ok());
    }

    #[tokio::test]
    async fn test_compile_simple_query() {
        let engine = create_test_engine().await.unwrap();

        let prql = r#"
            from blocks
            render (text "Hello")
        "#;

        let result = engine.compile_query(prql.to_string());
        assert!(result.is_ok());

        let (_sql, spec) = result.unwrap();
        match spec.root {
            query_render::RenderExpr::FunctionCall { name, .. } => {
                assert_eq!(name, "text");
            }
            _ => panic!("Expected function call"),
        }
    }

    #[tokio::test]
    async fn test_execute_query_with_parameters() {
        let engine = create_test_engine().await.unwrap();
        let backend = engine.backend.write().await;

        // Create a test table
        let conn = backend.get_connection().unwrap();
        conn.execute(
            "CREATE TABLE test_blocks (id TEXT PRIMARY KEY, title TEXT, depth INTEGER)",
            (),
        )
        .await
        .unwrap();

        // Insert test data
        conn.execute(
            "INSERT INTO test_blocks (id, title, depth) VALUES ('block-1', 'Test Block', 0)",
            (),
        )
        .await
        .unwrap();

        conn.execute(
            "INSERT INTO test_blocks (id, title, depth) VALUES ('block-2', 'Nested Block', 1)",
            (),
        )
        .await
        .unwrap();

        drop(conn);
        drop(backend);

        // Test query with parameter binding
        let mut params = HashMap::new();
        params.insert("min_depth".to_string(), Value::Integer(0));

        let sql = "SELECT id, title, depth FROM test_blocks WHERE depth >= $min_depth ORDER BY id";
        let results = engine.execute_query(sql.to_string(), params).await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("id").unwrap().as_string(), Some("block-1"));
        assert_eq!(results[1].get("id").unwrap().as_string(), Some("block-2"));
    }

    #[tokio::test]
    async fn test_parameter_binding() {
        let engine = create_test_engine().await.unwrap();
        let backend = engine.backend.write().await;

        let conn = backend.get_connection().unwrap();
        conn.execute("CREATE TABLE users (id TEXT, name TEXT, age INTEGER)", ())
            .await
            .unwrap();

        conn.execute(
            "INSERT INTO users VALUES ('u1', 'Alice', 30), ('u2', 'Bob', 25), ('u3', 'Charlie', 35)",
            ()
        ).await.unwrap();

        drop(conn);
        drop(backend);

        // Test multiple parameters
        let mut params = HashMap::new();
        params.insert("min_age".to_string(), Value::Integer(25));
        params.insert("max_age".to_string(), Value::Integer(35));

        let sql =
            "SELECT name, age FROM users WHERE age >= $min_age AND age <= $max_age ORDER BY age";
        let results = engine.execute_query(sql.to_string(), params).await.unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("name").unwrap().as_string(), Some("Bob"));
        assert_eq!(results[2].get("name").unwrap().as_string(), Some("Charlie"));
    }

    #[tokio::test]
    async fn test_execute_operation() {
        // Create a temporary engine to get the backend for the provider
        let temp_engine = create_test_engine().await.unwrap();
        let provider = Arc::new(SqlOperationProvider::new(
            temp_engine.backend.clone(),
            "blocks".to_string(),
            "blocks".to_string(),
        ));

        // Create engine with SqlOperationProvider registered via TestProviderModule
        let engine = create_test_engine_with_providers(":memory:".into(), |module| {
            module.with_operation_provider(provider)
        })
        .await
        .unwrap();

        // Create test table (using the engine's backend)
        {
            let backend = engine.backend.write().await;
            let conn = backend.get_connection().unwrap();
            conn.execute(
                "CREATE TABLE blocks (id TEXT PRIMARY KEY, content TEXT, completed BOOLEAN)",
                (),
            )
            .await
            .unwrap();

            conn.execute(
                "INSERT INTO blocks (id, content, completed) VALUES ('block-1', 'Test task', 0)",
                (),
            )
            .await
            .unwrap();
        }

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
            .execute_query(sql.to_string(), HashMap::new())
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

    #[tokio::test]
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

    #[tokio::test]
    async fn test_register_custom_operation() {
        // Create engine with SqlOperationProvider registered via TestProviderModule
        let temp_engine = create_test_engine().await.unwrap();
        let provider = Arc::new(SqlOperationProvider::new(
            temp_engine.backend.clone(),
            "blocks".to_string(),
            "blocks".to_string(),
        ));

        let engine = create_test_engine_with_providers(":memory:".into(), |module| {
            module.with_operation_provider(provider)
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

    #[tokio::test]
    async fn test_operations_inference() {
        let engine = create_test_engine().await.unwrap();

        // PRQL query with widgets that reference direct table columns
        let prql = r#"
from blocks
select {id, content, completed}
render (list item_template:(row (checkbox checked:this.completed) (text content:this.content)))
        "#;

        let result = engine.compile_query(prql.to_string());
        assert!(
            result.is_ok(),
            "Should compile query with render: {:?}",
            result.err()
        );

        let (_sql, spec) = result.unwrap();

        // Debug: print the tree structure
        eprintln!("Root expr: {:#?}", spec.root);

        // Helper to find all widgets with operations in the tree
        fn find_all_operations(
            expr: &query_render::RenderExpr,
        ) -> Vec<&query_render::OperationWiring> {
            let mut ops = Vec::new();
            match expr {
                query_render::RenderExpr::FunctionCall {
                    operations, args, ..
                } => {
                    ops.extend(operations.iter());
                    for arg in args {
                        ops.extend(find_all_operations(&arg.value));
                    }
                }
                query_render::RenderExpr::Array { items } => {
                    for item in items {
                        ops.extend(find_all_operations(item));
                    }
                }
                query_render::RenderExpr::BinaryOp { left, right, .. } => {
                    ops.extend(find_all_operations(left));
                    ops.extend(find_all_operations(right));
                }
                _ => {}
            }
            ops
        }

        let all_ops = find_all_operations(&spec.root);
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
