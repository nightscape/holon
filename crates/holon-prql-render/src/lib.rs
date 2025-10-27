pub mod compiler;
pub mod lineage;
pub mod parser;
pub mod types;

pub use compiler::compile_render_spec;
pub use lineage::{LineagePreprocessor, WidgetOperationMapping};
pub use parser::{ExtractedRender, QueryRenderSplit};
// Re-export prqlc types needed for RQ transformation
pub use prqlc::ir::rq::RelationalQuery;
// Re-export Number from types module (which re-exports from holon-api)
pub use types::Number;
// Re-export render types from types module (which re-exports from holon-api)
pub use types::{
    Arg, BinaryOperator, FilterExpr, OperationDescriptor, OperationParam, OperationWiring,
    PreconditionChecker, RenderExpr, RenderSpec, RowTemplate, TypeHint, ViewSpec,
};

use anyhow::Result;

/// Main entry point: Parse PRQL with render(), split into SQL query + UI instructions
pub fn parse_query_render(prql_source: &str) -> Result<(String, RenderSpec)> {
    let split = parser::split_prql_at_render(prql_source)?;

    let rq = prqlc::pl_to_rq(split.query_module)?;
    let sql = prqlc::rq_to_sql(rq, &prqlc::Options::default())?;

    let render_spec = compiler::compile_render_spec_from_extracted(&split.render_ast)?;

    Ok((sql, render_spec))
}

/// Intermediate result from parsing PRQL with render(), before SQL generation.
///
/// This allows callers to apply transformations to the RQ before generating SQL.
pub struct ParsedQueryRender {
    /// The RelationalQuery AST (ready for transformation and SQL generation)
    pub rq: prqlc::ir::rq::RelationalQuery,
    /// The render specification for UI
    pub render_spec: RenderSpec,
    /// Columns available in the query result (for operation filtering)
    pub available_columns: Vec<String>,
}

/// Intermediate result from parsing PRQL, BEFORE PL→RQ conversion.
///
/// This allows callers to apply PL-phase AST transformations before completing
/// the conversion to RQ. Use `complete_rq_conversion` to finish the process.
pub struct ParsedQueryPl {
    /// The PL module ready for transformation (render() calls already extracted)
    pub query_module: prqlc::pr::ModuleDef,
    /// Render-related data needed to complete the conversion
    pub render_data: ParsedRenderData,
}

/// Render-related data extracted during PL parsing.
///
/// This is separated from the PL module to allow ownership transfer of the module
/// to the TransformPipeline while keeping the render data for later use.
pub struct ParsedRenderData {
    /// The extracted render expression (AST form, not yet compiled)
    pub render_ast: parser::ExtractedRender,
    /// Extracted row templates from derive { ui = (render ...) } patterns
    pub extracted_templates: Vec<parser::ExtractedRowTemplate>,
    /// Table name from the main query (for operation wiring)
    pub table_name: String,
}

impl ParsedQueryRender {
    /// Generate SQL from the RQ AST.
    ///
    /// Call this after applying any transformations to `rq`.
    pub fn to_sql(&self) -> Result<String> {
        let sql = prqlc::rq_to_sql(self.rq.clone(), &prqlc::Options::default())?;
        Ok(sql)
    }

    /// Generate SQL from a transformed RQ AST.
    ///
    /// Use this when you have a separate transformed RQ (e.g., from TransformPipeline).
    pub fn to_sql_from_rq(rq: &prqlc::ir::rq::RelationalQuery) -> Result<String> {
        let sql = prqlc::rq_to_sql(rq.clone(), &prqlc::Options::default())?;
        Ok(sql)
    }
}

impl ParsedRenderData {
    /// Complete the PL→RQ conversion and build the final ParsedQueryRender.
    ///
    /// Call this after applying any PL-phase transformations to `query_module`.
    ///
    /// # Example
    /// ```ignore
    /// let pl_parsed = parse_query_to_pl(prql)?;
    /// let transformed_module = pipeline.apply_pl_transforms(pl_parsed.query_module)?;
    /// let parsed = pl_parsed.render_data.complete_rq_conversion(transformed_module)?;
    /// ```
    pub fn complete_rq_conversion(
        self,
        transformed_module: prqlc::pr::ModuleDef,
    ) -> Result<ParsedQueryRender> {
        // Convert PL to RQ
        let rq = prqlc::pl_to_rq(transformed_module)?;

        // Extract available columns from RQ (for operation filtering)
        let available_columns = extract_columns_from_rq(&rq);

        let mut render_spec = compiler::compile_render_spec_from_extracted(&self.render_ast)?;

        // Compile extracted row templates and populate row_templates in RenderSpec
        for template in self.extracted_templates {
            let template_json = parser::prql_ast_to_json(&template.render_expr)?;
            let template_expr = compiler::compile_render_expr_from_json(&template_json)?;

            render_spec.row_templates.push(RowTemplate {
                index: template.index,
                entity_name: template.entity_name,
                entity_short_name: String::new(), // Will be filled by BackendEngine from operations
                expr: template_expr,
            });
        }

        // Annotate tree with auto-operations
        // For single-table queries, use the table name from the query
        // For UNION queries with row_templates, operations are wired per-template (done later in backend)
        if render_spec.row_templates.is_empty() {
            annotate_tree_with_operations(render_spec.get_root_mut(), &self.table_name);
        }

        Ok(ParsedQueryRender {
            rq,
            render_spec,
            available_columns,
        })
    }
}

/// Parse PRQL up to the PL stage, BEFORE PL→RQ conversion.
///
/// This allows callers to apply PL-phase AST transformations (e.g., JsonAggregationTransformer)
/// before completing the conversion to RQ.
///
/// # Example
/// ```ignore
/// let pl_parsed = parse_query_to_pl(prql)?;
/// let transformed_module = pipeline.apply_pl_transforms(pl_parsed.query_module)?;
/// let parsed = pl_parsed.render_data.complete_rq_conversion(transformed_module)?;
/// ```
pub fn parse_query_to_pl(prql_source: &str) -> Result<ParsedQueryPl> {
    // Step 1: Split query and render (removes final render() call from pipeline)
    let split = parser::split_prql_at_render(prql_source)?;
    let mut query_module = split.query_module;

    // Step 2: Extract row templates from derive { ui = (render ...) } patterns
    // This modifies query_module in place, replacing render() calls with integer literals
    let extracted_templates = parser::extract_row_templates_from_module(&mut query_module)?;

    // Step 3: Extract table name from the main query (for single-table queries)
    let table_name = extract_table_name(&query_module)?;

    Ok(ParsedQueryPl {
        query_module,
        render_data: ParsedRenderData {
            render_ast: split.render_ast,
            extracted_templates,
            table_name,
        },
    })
}

/// Parse PRQL with automatic operation inference from lineage
///
/// This version extracts the table name from the query and annotates the render tree with
/// auto-operations based on direct column references.
///
/// Returns (SQL, RenderSpec, available_columns) where available_columns are all columns
/// from the query result that can be used for operation filtering.
pub fn parse_query_render_with_operations(
    prql_source: &str,
) -> Result<(String, RenderSpec, Vec<String>)> {
    let parsed = parse_query_render_to_rq(prql_source)?;
    let sql = parsed.to_sql()?;
    Ok((sql, parsed.render_spec, parsed.available_columns))
}

/// Parse PRQL to RQ AST and RenderSpec without generating SQL.
///
/// This allows callers to apply transformations (e.g., adding `_change_origin` column)
/// to the RQ AST before generating SQL.
///
/// # Example
/// ```ignore
/// let parsed = parse_query_render_to_rq(prql)?;
/// let transformed_rq = pipeline.transform_rq(parsed.rq)?;
/// let sql = ParsedQueryRender::to_sql_from_rq(&transformed_rq)?;
/// ```
pub fn parse_query_render_to_rq(prql_source: &str) -> Result<ParsedQueryRender> {
    // Step 1: Split query and render (removes final render() call from pipeline)
    let split = parser::split_prql_at_render(prql_source)?;
    let mut query_module = split.query_module;

    // Step 2: Extract row templates from derive { ui = (render ...) } patterns
    // This modifies query_module in place, replacing render() calls with integer literals
    let extracted_templates = parser::extract_row_templates_from_module(&mut query_module)?;

    // Step 3: Extract table name from the main query (for single-table queries)
    let table_name = extract_table_name(&query_module)?;

    // Step 4: Convert PL to RQ
    let rq = prqlc::pl_to_rq(query_module)?;

    // Step 4.5: Extract available columns from RQ (for operation filtering)
    let available_columns = extract_columns_from_rq(&rq);

    let mut render_spec = compiler::compile_render_spec_from_extracted(&split.render_ast)?;

    // Step 5: Compile extracted row templates and populate row_templates in RenderSpec
    for template in extracted_templates {
        let template_json = parser::prql_ast_to_json(&template.render_expr)?;
        let template_expr = compiler::compile_render_expr_from_json(&template_json)?;

        render_spec.row_templates.push(RowTemplate {
            index: template.index,
            entity_name: template.entity_name,
            entity_short_name: String::new(), // Will be filled by BackendEngine from operations
            expr: template_expr,
        });
    }

    // Step 6: Annotate tree with auto-operations
    // For single-table queries, use the table name from the query
    // For UNION queries with row_templates, operations are wired per-template (done later in backend)
    if render_spec.row_templates.is_empty() {
        annotate_tree_with_operations(render_spec.get_root_mut(), &table_name);
    }

    Ok(ParsedQueryRender {
        rq,
        render_spec,
        available_columns,
    })
}

/// Extract the table name from the main query pipeline
fn extract_table_name(module: &prqlc::pr::ModuleDef) -> Result<String> {
    use prqlc::pr::*;

    // Find the main query
    for stmt in &module.stmts {
        if let StmtKind::VarDef(var_def) = &stmt.kind {
            if matches!(var_def.kind, VarDefKind::Main) {
                if let Some(value) = &var_def.value {
                    // Look for 'from' in the pipeline
                    return find_from_in_expr(value);
                }
            }
        }
    }
    anyhow::bail!("No main query found in module")
}

/// Find the 'from' transform in an expression and extract the table name
fn find_from_in_expr(expr: &prqlc::pr::Expr) -> Result<String> {
    use prqlc::pr::*;

    match &expr.kind {
        ExprKind::Pipeline(pipeline) => {
            // Check each expression in the pipeline
            for e in &pipeline.exprs {
                if let ExprKind::FuncCall(func_call) = &e.kind {
                    // Check if this is a 'from' call
                    if let ExprKind::Ident(ident) = &func_call.name.kind {
                        if ident.name == "from" {
                            // The first argument should be the table name (an identifier)
                            if let Some(arg) = func_call.args.first() {
                                if let ExprKind::Ident(table_ident) = &arg.kind {
                                    return Ok(table_ident.name.clone());
                                }
                            }
                        }
                    }
                }
            }
            anyhow::bail!("No 'from' transform found in pipeline")
        }
        _ => anyhow::bail!("Expected pipeline expression"),
    }
}

/// Extract all column names from the RelationalQuery result
///
/// Returns a list of column names that are available in the query result.
/// For UNION queries, this returns the merged column set.
fn extract_columns_from_rq(rq: &prqlc::ir::rq::RelationalQuery) -> Vec<String> {
    use prqlc::ir::rq::RelationColumn;

    rq.relation
        .columns
        .iter()
        .filter_map(|col| match col {
            RelationColumn::Single(Some(name)) => Some(name.clone()),
            _ => None,
        })
        .collect()
}

/// Walk the RenderExpr tree and attach auto-operations based on table name
///
/// For each FunctionCall with ColumnRef parameters that reference "this.",
/// attach an auto-operation for updating that column.
fn annotate_tree_with_operations(expr: &mut RenderExpr, table_name: &str) {
    match expr {
        RenderExpr::FunctionCall {
            name,
            args,
            operations,
        } => {
            // Check each argument for direct column references
            for arg in args.iter() {
                if let (Some(param_name), RenderExpr::ColumnRef { name: col_name }) =
                    (&arg.name, &arg.value)
                {
                    // Strip "this." prefix if present
                    let field_name = col_name.strip_prefix("this.").unwrap_or(col_name);

                    // If the column reference uses "this." prefix, it's a direct column reference
                    // NOTE: This is legacy auto-operation code. In the new architecture, operations
                    // are discovered via OperationProvider during compile_query() in BackendEngine.
                    // This creates a placeholder descriptor - real operations should come from OperationProvider.
                    if col_name.starts_with("this.") {
                        operations.push(OperationWiring {
                            widget_type: name.clone(),
                            modified_param: param_name.clone(),
                            descriptor: OperationDescriptor {
                                entity_name: String::new(), // Will be filled by OperationProvider
                                entity_short_name: "placeholder".to_string(), // Will be replaced by OperationProvider
                                id_column: "id".to_string(),
                                name: "set_field".to_string(),
                                display_name: format!("Set {}", field_name),
                                description: format!("Update {} field", field_name),
                                required_params: vec![
                                    OperationParam {
                                        name: "id".to_string(),
                                        type_hint: TypeHint::String,
                                        description: "Entity ID".to_string(),
                                    },
                                    OperationParam {
                                        name: "field".to_string(),
                                        type_hint: TypeHint::String,
                                        description: format!("Field name: {}", field_name),
                                    },
                                    OperationParam {
                                        name: "value".to_string(),
                                        type_hint: TypeHint::String, // "any" not supported, use String
                                        description: format!("New value for {}", field_name),
                                    },
                                ],
                                affected_fields: vec![field_name.to_string()], // set_field affects the specified field
                                param_mappings: vec![], // set_field doesn't use param mappings
                                precondition: None,
                            },
                        });
                    }
                }
            }

            // Recurse into nested expressions
            for arg in args.iter_mut() {
                annotate_tree_with_operations(&mut arg.value, table_name);
            }
        }
        RenderExpr::Array { items } => {
            for item in items.iter_mut() {
                annotate_tree_with_operations(item, table_name);
            }
        }
        RenderExpr::BinaryOp { left, right, .. } => {
            annotate_tree_with_operations(left, table_name);
            annotate_tree_with_operations(right, table_name);
        }
        RenderExpr::Object { fields } => {
            for value in fields.values_mut() {
                annotate_tree_with_operations(value, table_name);
            }
        }
        _ => {} // ColumnRef, Literal - no recursion needed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_function_call() {
        let prql = r#"
from blocks
render (text "Hello World")
        "#;

        let result = parse_query_render(prql);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

        let (_sql, spec) = result.unwrap();
        match spec.root() {
            Some(RenderExpr::FunctionCall { name, args, .. }) => {
                assert_eq!(name, "text");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_with_column_reference() {
        let prql = r#"
from blocks
render (text content)
        "#;

        let result = parse_query_render(prql);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

        let (_sql, spec) = result.unwrap();
        match spec.root() {
            Some(RenderExpr::FunctionCall { name, args, .. }) => {
                assert_eq!(name, "text");
                assert_eq!(args.len(), 1);
                match &args[0].value {
                    RenderExpr::ColumnRef { name } => {
                        assert_eq!(name, "content");
                    }
                    _ => panic!("Expected column ref"),
                }
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_nested_function_calls() {
        let prql = r#"
from blocks
render (row (text "A") (text "B"))
        "#;

        let result = parse_query_render(prql);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

        let (_sql, spec) = result.unwrap();
        match spec.root() {
            Some(RenderExpr::FunctionCall { name, args, .. }) => {
                assert_eq!(name, "row");
                assert_eq!(args.len(), 2);
                for arg in args {
                    match &arg.value {
                        RenderExpr::FunctionCall { name, .. } => {
                            assert_eq!(name, "text");
                        }
                        _ => panic!("Expected nested function call"),
                    }
                }
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_named_arguments() {
        let prql = r#"
from blocks
render (block indent:depth content:(text title))
        "#;

        let result = parse_query_render(prql);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

        let (_sql, spec) = result.unwrap();
        match spec.root() {
            Some(RenderExpr::FunctionCall { name, args, .. }) => {
                assert_eq!(name, "block");
                assert!(args.iter().any(|a| a.name == Some("indent".to_string())));
                assert!(args.iter().any(|a| a.name == Some("content".to_string())));
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn test_helper_function_expansion() {
        let prql = r#"
let make_row = func -> (row (text "A") (text "B"))
from blocks
render (make_row)
        "#;

        let result = parse_query_render(prql);
        assert!(
            result.is_ok(),
            "Failed to expand helper function: {:?}",
            result.err()
        );

        let (_sql, spec) = result.unwrap();
        match spec.root() {
            Some(RenderExpr::FunctionCall { name, args, .. }) => {
                assert_eq!(name, "row");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected row function call from expansion"),
        }
    }

    #[test]
    fn test_helper_function_with_params() {
        let prql = r#"
let make_text = func content -> (text content)
from blocks
render (make_text "Hello")
        "#;

        let result = parse_query_render(prql);
        assert!(
            result.is_ok(),
            "Failed to expand parameterized function: {:?}",
            result.err()
        );

        let (_sql, spec) = result.unwrap();
        match spec.root() {
            Some(RenderExpr::FunctionCall { name, args, .. }) => {
                assert_eq!(name, "text");
                match &args[0].value {
                    RenderExpr::Literal { value } => {
                        assert_eq!(value.as_string(), Some("Hello"));
                    }
                    _ => panic!("Expected literal value after expansion"),
                }
            }
            _ => panic!("Expected text function call"),
        }
    }

    #[test]
    fn test_sql_generation() {
        let prql = r#"
from blocks
filter depth > 0
select [id, title, depth]
render (text title)
        "#;

        let result = parse_query_render(prql);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

        let (sql, _spec) = result.unwrap();
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM"));
        assert!(sql.contains("WHERE"));
        assert!(sql.to_lowercase().contains("depth"));
    }
}

#[cfg(test)]
mod rq_inspection_tests {
    #[test]
    fn test_extract_columns_simple_query() {
        let prql = r#"
from blocks
select {id, content, parent_id, completed}
"#;

        let pl = prqlc::prql_to_pl(prql).unwrap();
        let rq = prqlc::pl_to_rq(pl).unwrap();

        let columns = super::extract_columns_from_rq(&rq);

        assert_eq!(columns, vec!["id", "content", "parent_id", "completed"]);
    }

    #[test]
    fn test_extract_columns_union_query() {
        let prql = r#"
from blocks
select {id, content, completed}
append (
  from todoist_tasks
  select {id, content, completed}
)
"#;

        let pl = prqlc::prql_to_pl(prql).unwrap();
        let rq = prqlc::pl_to_rq(pl).unwrap();

        let columns = super::extract_columns_from_rq(&rq);

        // UNION query should have the merged column set
        assert_eq!(columns, vec!["id", "content", "completed"]);
    }

    #[test]
    fn test_parse_query_render_to_rq_with_row_templates() {
        let prql = r#"
from todoist_tasks
derive { ui = (render (row (checkbox checked:this.completed) (text this.content))) }
append (
  from todoist_projects
  derive { ui = (render (row (text this.name))) }
)
render (tree parent_id:parent_id sortkey:sort_key item_template:this.ui)
"#;

        let parsed = super::parse_query_render_to_rq(prql).unwrap();

        // Should have 2 row templates
        assert_eq!(
            parsed.render_spec.row_templates.len(),
            2,
            "Should have 2 row templates"
        );

        // First template for todoist_tasks (index 0)
        let template0 = &parsed.render_spec.row_templates[0];
        assert_eq!(template0.index, 0);
        assert_eq!(template0.entity_name, "todoist_tasks");
        match &template0.expr {
            super::RenderExpr::FunctionCall { name, .. } => assert_eq!(name, "row"),
            _ => panic!("Expected row function call for template 0"),
        }

        // Second template for todoist_projects (index 1)
        let template1 = &parsed.render_spec.row_templates[1];
        assert_eq!(template1.index, 1);
        assert_eq!(template1.entity_name, "todoist_projects");
        match &template1.expr {
            super::RenderExpr::FunctionCall { name, .. } => assert_eq!(name, "row"),
            _ => panic!("Expected row function call for template 1"),
        }

        // SQL should contain integer ui values
        let sql = parsed.to_sql().unwrap();
        assert!(
            sql.contains("0 AS ui") || sql.contains("0 as ui"),
            "SQL should contain '0 AS ui'"
        );
        assert!(
            sql.contains("1 AS ui") || sql.contains("1 as ui"),
            "SQL should contain '1 AS ui'"
        );

        // The root render spec should be a tree()
        match parsed.render_spec.root() {
            Some(super::RenderExpr::FunctionCall { name, .. }) => assert_eq!(name, "tree"),
            _ => panic!("Expected tree function call as root"),
        }
    }
}

#[cfg(test)]
mod pl_structure_tests {
    use prqlc::prql_to_pl;

    #[test]
    fn inspect_this_star_structure() {
        // Query with explicit this.* to see how it's represented
        let query_with_star = r#"
from products
select { this.* }
select { id, name }
        "#;

        println!("\n=== Query with this.* ===");
        match prql_to_pl(query_with_star) {
            Ok(module) => println!("{:#?}", module),
            Err(e) => println!("Parse error: {:?}", e),
        }

        // Also try derive with this.*
        let query_derive_star = r#"
from products
derive { all_cols = this.* }
select { id, name }
        "#;

        println!("\n=== Query with derive this.* ===");
        match prql_to_pl(query_derive_star) {
            Ok(module) => println!("{:#?}", module),
            Err(e) => println!("Parse error: {:?}", e),
        }
    }
}

#[cfg(test)]
mod pl_fold_integration_tests {
    use prqlc::ir::pl::{Expr, ExprKind, FuncCall, PlFold, TransformCall};
    use prqlc::prql_to_pl;
    use prqlc::semantic::ast_expand::expand_module_def;
    use prqlc::Result as PrqlResult;

    /// A folder that logs expressions to understand the PL structure
    struct DebugFolder;

    impl PlFold for DebugFolder {
        fn fold_transform_call(&mut self, tc: TransformCall) -> PrqlResult<TransformCall> {
            println!("TransformCall kind: {:?}", tc.kind);
            prqlc::ir::pl::fold_transform_call(self, tc)
        }

        fn fold_func_call(&mut self, fc: FuncCall) -> PrqlResult<FuncCall> {
            if let ExprKind::Ident(ref ident) = fc.name.kind {
                println!("FuncCall: {}", ident);
            }
            prqlc::ir::pl::fold_func_call(self, fc)
        }

        fn fold_expr(&mut self, expr: Expr) -> PrqlResult<Expr> {
            // Override to inspect before and after
            let folded_kind = prqlc::ir::pl::fold_expr_kind(self, expr.kind)?;
            Ok(Expr {
                kind: folded_kind,
                ..expr
            })
        }
    }

    #[test]
    fn test_pl_fold_with_ast_expand() {
        let query = r#"
from products
derive { entity_name = "products" }
select { id, name, entity_name }
        "#;

        // Step 1: Parse to PR (parser representation)
        let pr_module = prql_to_pl(query).expect("Should parse");
        println!("PR module parsed successfully");

        // Step 2: Expand to PL (intermediate representation)
        let pl_module = expand_module_def(pr_module).expect("Should expand");
        println!("\n=== PL module structure ===");
        println!("{:#?}", pl_module);

        // Step 3: Apply fold
        let mut folder = DebugFolder;
        let folded = folder.fold_module_def(pl_module);

        println!("\nFold successful: {:?}", folded.is_ok());
    }

    #[test]
    fn test_pl_fold_with_union() {
        let query = r#"
from products
derive { entity_name = "products" }
select { id, name, entity_name }
append (
    from services
    derive { entity_name = "services" }
    select { id, name = title, entity_name }
)
        "#;

        let pr_module = prql_to_pl(query).expect("Should parse");
        let pl_module = expand_module_def(pr_module).expect("Should expand");

        println!("\n=== UNION query PL structure ===");
        println!("{:#?}", pl_module);

        let mut folder = DebugFolder;
        let folded = folder.fold_module_def(pl_module);

        println!("\nFold successful: {:?}", folded.is_ok());
    }
}

#[cfg(test)]
mod column_preservation_pr_tests {
    use prqlc::pr::{Expr, ExprKind, FuncCall, Ident, ModuleDef, Pipeline, StmtKind};

    /// Check if an expression is an identifier with a specific name
    fn is_ident_named(expr: &Expr, name: &str) -> bool {
        if let ExprKind::Ident(ident) = &expr.kind {
            return ident.path.is_empty() && ident.name == name;
        }
        false
    }

    /// Check if an expression is a `from` function call
    fn is_from_call(expr: &Expr) -> bool {
        if let ExprKind::FuncCall(func_call) = &expr.kind {
            return is_ident_named(&func_call.name, "from");
        }
        false
    }

    /// Create a derive expression that references this.*
    fn create_this_star_derive(counter: usize) -> Expr {
        let this_star = Expr {
            kind: ExprKind::Ident(Ident::from_path(vec!["this".to_string(), "*".to_string()])),
            span: None,
            alias: Some(format!("__preserve_{}", counter)),
            doc_comment: None,
        };

        let tuple = Expr {
            kind: ExprKind::Tuple(vec![this_star]),
            span: None,
            alias: None,
            doc_comment: None,
        };

        Expr {
            kind: ExprKind::FuncCall(FuncCall {
                name: Box::new(Expr {
                    kind: ExprKind::Ident(Ident::from_name("derive")),
                    span: None,
                    alias: None,
                    doc_comment: None,
                }),
                args: vec![tuple],
                named_args: std::collections::HashMap::new(),
            }),
            span: None,
            alias: None,
            doc_comment: None,
        }
    }

    /// Inject derive into a pipeline after each from
    fn inject_into_pipeline(pipeline: &mut Pipeline, counter: &mut usize) {
        let mut insert_positions = Vec::new();
        for (i, e) in pipeline.exprs.iter().enumerate() {
            if is_from_call(e) {
                insert_positions.push(i + 1);
            }
        }
        for pos in insert_positions.into_iter().rev() {
            let derive_expr = create_this_star_derive(*counter);
            *counter += 1;
            pipeline.exprs.insert(pos, derive_expr);
        }
    }

    fn is_append_call(expr: &Expr) -> bool {
        if let ExprKind::FuncCall(func_call) = &expr.kind {
            return is_ident_named(&func_call.name, "append");
        }
        false
    }

    fn has_append_in_expr(expr: &Expr) -> bool {
        match &expr.kind {
            ExprKind::Pipeline(pipeline) => pipeline
                .exprs
                .iter()
                .any(|e| is_append_call(e) || has_append_in_expr(e)),
            ExprKind::FuncCall(func_call) => {
                if is_ident_named(&func_call.name, "append") {
                    return true;
                }
                func_call.args.iter().any(has_append_in_expr)
            }
            ExprKind::Tuple(items) | ExprKind::Array(items) => items.iter().any(has_append_in_expr),
            _ => false,
        }
    }

    fn inject_column_preservation(expr: &mut Expr, counter: &mut usize) {
        match &mut expr.kind {
            ExprKind::Pipeline(pipeline) => {
                inject_into_pipeline(pipeline, counter);
                for e in &mut pipeline.exprs {
                    inject_column_preservation(e, counter);
                }
            }
            ExprKind::FuncCall(func_call) => {
                for arg in &mut func_call.args {
                    inject_column_preservation(arg, counter);
                }
            }
            ExprKind::Tuple(items) | ExprKind::Array(items) => {
                for item in items {
                    inject_column_preservation(item, counter);
                }
            }
            _ => {}
        }
    }

    fn transform_module(mut module: ModuleDef) -> ModuleDef {
        for stmt in &mut module.stmts {
            if let StmtKind::VarDef(var_def) = &mut stmt.kind {
                if let Some(value) = &mut var_def.value {
                    if has_append_in_expr(value) {
                        let mut counter = 0;
                        inject_column_preservation(value, &mut counter);
                    }
                }
            }
        }
        module
    }

    #[test]
    fn test_this_star_injection_compiles() {
        let query = r#"
from products
derive { entity_name = "products" }
select { id, name, entity_name }
append (
    from services
    derive { entity_name = "services" }
    select { id, name = title, entity_name }
)
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");
        let transformed = transform_module(module);

        println!("\n=== Transformed PR module ===");
        println!("{:#?}", transformed);

        // Convert to RQ and SQL
        let rq = prqlc::pl_to_rq(transformed).expect("Should convert to RQ");

        println!("\n=== RQ columns ===");
        println!("{:?}", rq.relation.columns);

        let sql = prqlc::rq_to_sql(rq, &Default::default()).expect("Should generate SQL");

        println!("\n=== Generated SQL ===");
        println!("{}", sql);

        assert!(sql.contains("UNION"), "Should contain UNION");
    }

    #[test]
    fn test_this_star_preserves_all_columns() {
        // Test with products table that has more columns than selected
        let query = r#"
from products
derive { entity_name = "products" }
select { id, name, entity_name }
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");

        // Transform - even without append, let's inject this.* manually to see if it works
        let mut transformed = module;
        for stmt in &mut transformed.stmts {
            if let StmtKind::VarDef(var_def) = &mut stmt.kind {
                if let Some(value) = &mut var_def.value {
                    let mut counter = 0;
                    inject_column_preservation(value, &mut counter);
                }
            }
        }

        // Convert to RQ
        let rq = prqlc::pl_to_rq(transformed).expect("Should convert to RQ");

        println!("\n=== RQ columns after this.* injection ===");
        for col in &rq.relation.columns {
            println!("  {:?}", col);
        }

        // The columns should now include __preserve which contains this.*
        // This means all underlying columns should be available
    }
}

#[cfg(test)]
mod this_star_expansion_tests {
    use prqlc::pr::{Expr, ExprKind, FuncCall, Ident, ModuleDef, Pipeline, StmtKind};

    #[test]
    fn test_what_this_star_becomes() {
        // Minimal query with just this.*
        let query = r#"
from products
derive { all = this.* }
select { id, name, all }
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");

        println!("\n=== PR module with this.* in derive ===");
        // Don't print full structure, just check if it parses

        // Convert to RQ
        let rq = prqlc::pl_to_rq(module).expect("Should convert to RQ");

        println!("\n=== RQ columns ===");
        for col in &rq.relation.columns {
            println!("  {:?}", col);
        }

        println!("\n=== RQ relation kind ===");
        println!("{:#?}", rq.relation.kind);

        let sql = prqlc::rq_to_sql(rq, &Default::default()).expect("Should generate SQL");
        println!("\n=== Generated SQL ===");
        println!("{}", sql);
    }

    #[test]
    fn test_select_this_star_directly() {
        // What if we just select this.*?
        let query = r#"
from products
select { this.* }
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");

        // Convert to RQ
        let rq = prqlc::pl_to_rq(module).expect("Should convert to RQ");

        println!("\n=== RQ columns for select this.* ===");
        for col in &rq.relation.columns {
            println!("  {:?}", col);
        }

        // Note: this.* in select should expand to all columns
        // But we don't know what columns exist - PRQL doesn't know either!
    }
}

#[cfg(test)]
mod wildcard_combination_tests {
    #[test]
    fn test_this_star_with_other_columns() {
        // Can we combine this.* with other columns?
        let query = r#"
from products
select { this.*, extra = "hello" }
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");

        match prqlc::pl_to_rq(module) {
            Ok(rq) => {
                println!("\n=== RQ columns for select {{ this.*, extra }} ===");
                for col in &rq.relation.columns {
                    println!("  {:?}", col);
                }

                let sql = prqlc::rq_to_sql(rq, &Default::default()).expect("SQL");
                println!("\n=== SQL ===\n{}", sql);
            }
            Err(e) => {
                println!("RQ conversion failed: {:?}", e);
            }
        }
    }

    #[test]
    fn test_select_star_then_derive() {
        // What about: select this.* | derive { extra = ... }
        let query = r#"
from products
select { this.* }
derive { entity_name = "products" }
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");

        match prqlc::pl_to_rq(module) {
            Ok(rq) => {
                println!("\n=== RQ columns: select this.* | derive ===");
                for col in &rq.relation.columns {
                    println!("  {:?}", col);
                }

                let sql = prqlc::rq_to_sql(rq, &Default::default()).expect("SQL");
                println!("\n=== SQL ===\n{}", sql);
            }
            Err(e) => {
                println!("RQ conversion failed: {:?}", e);
            }
        }
    }

    #[test]
    fn test_derive_then_select_star() {
        // What about: derive { extra } | select { this.* }
        let query = r#"
from products
derive { entity_name = "products" }
select { this.* }
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");

        match prqlc::pl_to_rq(module) {
            Ok(rq) => {
                println!("\n=== RQ columns: derive | select this.* ===");
                for col in &rq.relation.columns {
                    println!("  {:?}", col);
                }

                let sql = prqlc::rq_to_sql(rq, &Default::default()).expect("SQL");
                println!("\n=== SQL ===\n{}", sql);
            }
            Err(e) => {
                println!("RQ conversion failed: {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod union_wildcard_tests {
    #[test]
    fn test_union_with_select_star() {
        let query = r#"
from products
derive { entity_name = "products" }
select { this.* }
append (
    from services
    derive { entity_name = "services" }
    select { this.* }
)
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");

        match prqlc::pl_to_rq(module) {
            Ok(rq) => {
                println!("\n=== RQ columns for UNION with select this.* ===");
                for col in &rq.relation.columns {
                    println!("  {:?}", col);
                }

                let sql = prqlc::rq_to_sql(rq, &Default::default()).expect("SQL");
                println!("\n=== SQL ===\n{}", sql);
            }
            Err(e) => {
                println!("RQ conversion failed: {:?}", e);
            }
        }
    }

    #[test]
    fn test_union_with_explicit_columns_then_star() {
        // Original query style but with this.* at the end
        let query = r#"
from products
derive { entity_name = "products" }
select { id, name, entity_name, this.* }
append (
    from services
    derive { entity_name = "services", name = title }
    select { id, name, entity_name, this.* }
)
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");

        match prqlc::pl_to_rq(module) {
            Ok(rq) => {
                println!("\n=== RQ columns for UNION with explicit + this.* ===");
                for col in &rq.relation.columns {
                    println!("  {:?}", col);
                }

                let sql = prqlc::rq_to_sql(rq, &Default::default()).expect("SQL");
                println!("\n=== SQL ===\n{}", sql);
            }
            Err(e) => {
                println!("RQ conversion failed: {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod select_to_wildcard_transformer_tests {
    use prqlc::pr::{Expr, ExprKind, FuncCall, Ident, ModuleDef, Pipeline, StmtKind};

    fn is_ident_named(expr: &Expr, name: &str) -> bool {
        if let ExprKind::Ident(ident) = &expr.kind {
            return ident.path.is_empty() && ident.name == name;
        }
        false
    }

    fn is_select_call(expr: &Expr) -> bool {
        if let ExprKind::FuncCall(func_call) = &expr.kind {
            return is_ident_named(&func_call.name, "select");
        }
        false
    }

    fn is_append_call(expr: &Expr) -> bool {
        if let ExprKind::FuncCall(func_call) = &expr.kind {
            return is_ident_named(&func_call.name, "append");
        }
        false
    }

    fn has_append_in_expr(expr: &Expr) -> bool {
        match &expr.kind {
            ExprKind::Pipeline(pipeline) => pipeline
                .exprs
                .iter()
                .any(|e| is_append_call(e) || has_append_in_expr(e)),
            ExprKind::FuncCall(func_call) => {
                if is_ident_named(&func_call.name, "append") {
                    return true;
                }
                func_call.args.iter().any(has_append_in_expr)
            }
            ExprKind::Tuple(items) | ExprKind::Array(items) => items.iter().any(has_append_in_expr),
            _ => false,
        }
    }

    fn convert_select_to_wildcard(expr: &mut Expr) {
        if let ExprKind::FuncCall(ref mut func_call) = expr.kind {
            if is_ident_named(&func_call.name, "select") {
                let this_star = Expr {
                    kind: ExprKind::Ident(Ident::from_path(vec![
                        "this".to_string(),
                        "*".to_string(),
                    ])),
                    span: None,
                    alias: None,
                    doc_comment: None,
                };

                let tuple = Expr {
                    kind: ExprKind::Tuple(vec![this_star]),
                    span: None,
                    alias: None,
                    doc_comment: None,
                };

                func_call.args = vec![tuple];
            }
        }
    }

    fn convert_selects_to_wildcard(expr: &mut Expr) {
        match &mut expr.kind {
            ExprKind::Pipeline(pipeline) => {
                for e in &mut pipeline.exprs {
                    if is_select_call(e) {
                        convert_select_to_wildcard(e);
                    }
                    convert_selects_to_wildcard(e);
                }
            }
            ExprKind::FuncCall(func_call) => {
                for arg in &mut func_call.args {
                    convert_selects_to_wildcard(arg);
                }
            }
            ExprKind::Tuple(items) | ExprKind::Array(items) => {
                for item in items {
                    convert_selects_to_wildcard(item);
                }
            }
            _ => {}
        }
    }

    fn transform_module(mut module: ModuleDef) -> ModuleDef {
        for stmt in &mut module.stmts {
            if let StmtKind::VarDef(var_def) = &mut stmt.kind {
                if let Some(value) = &mut var_def.value {
                    if has_append_in_expr(value) {
                        convert_selects_to_wildcard(value);
                    }
                }
            }
        }
        module
    }

    #[test]
    fn test_select_to_wildcard_transformation() {
        let query = r#"
from products
derive { entity_name = "products" }
select { id, name, entity_name }
append (
    from services
    derive { entity_name = "services" }
    select { id, name = title, entity_name }
)
        "#;

        let module = prqlc::prql_to_pl(query).expect("Should parse");
        let transformed = transform_module(module);

        let rq = prqlc::pl_to_rq(transformed).expect("Should convert to RQ");

        println!("\n=== RQ columns after transformation ===");
        for col in &rq.relation.columns {
            println!("  {:?}", col);
        }

        let sql = prqlc::rq_to_sql(rq, &Default::default()).expect("Should generate SQL");

        println!("\n=== Generated SQL ===\n{}", sql);

        // Should have SELECT * from both branches
        assert!(sql.contains("*"), "Should contain wildcard");
        assert!(sql.contains("UNION"), "Should contain UNION");

        // The Wildcard in RQ should indicate all columns are preserved
    }
}
