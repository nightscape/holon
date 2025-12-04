use anyhow::{bail, Result};
use holon_api::Value;
use prqlc::pr::*;

#[derive(Debug, Clone)]
/// flutter_rust_bridge:ignore
pub struct QueryRenderSplit {
    pub query_module: ModuleDef,
    pub render_ast: Expr,
}

/// Extracted row template with its source table context.
/// Used for heterogeneous UNION queries where each table has a different UI template.
#[derive(Debug, Clone)]
pub struct ExtractedRowTemplate {
    /// The index assigned to this template (used in SQL as `ui` column value)
    pub index: usize,
    /// The source table name (e.g., "todoist_tasks", "todoist_projects")
    pub entity_name: String,
    /// The extracted render expression (without the outer `render()` wrapper)
    pub render_expr: Expr,
}

/// Parse PRQL source using PRQL's native parser and extract render() call
/// flutter_rust_bridge:ignore
pub fn split_prql_at_render(source: &str) -> Result<QueryRenderSplit> {
    // Parse using PRQL's parser
    let mut module = prqlc::prql_to_pl(source)?;

    // Find and extract the render() call from the last statement
    let mut render_ast = extract_render_from_module(&mut module)?;

    // Expand user-defined functions in render() expression
    expand_functions_in_expr(&mut render_ast, &module)?;

    Ok(QueryRenderSplit {
        query_module: module,
        render_ast,
    })
}

/// Expand user-defined function calls in an expression
fn expand_functions_in_expr(expr: &mut Expr, module: &ModuleDef) -> Result<()> {
    match &mut expr.kind {
        ExprKind::FuncCall(func_call) => {
            // Recursively expand in function arguments
            for arg in &mut func_call.args {
                expand_functions_in_expr(arg, module)?;
            }
            for (_name, arg) in &mut func_call.named_args {
                expand_functions_in_expr(arg, module)?;
            }

            // Check if the function name is a user-defined function
            if let ExprKind::Ident(ident) = &func_call.name.kind {
                if ident.path.is_empty() {
                    if let Some(func_def) = find_function_in_module(&ident.name, module) {
                        // Expand the function call inline
                        *expr =
                            expand_function_call(func_def, &func_call.args, &func_call.named_args)?;
                        // Recursively expand in the expanded result
                        expand_functions_in_expr(expr, module)?;
                    }
                }
            }
        }
        ExprKind::Ident(_) => {
            // Check if this identifier is a zero-argument function call
            if let ExprKind::Ident(ident) = &expr.kind {
                if ident.path.is_empty() {
                    if let Some(func_def) = find_function_in_module(&ident.name, module) {
                        // Expand zero-arg function
                        *expr =
                            expand_function_call(func_def, &[], &std::collections::HashMap::new())?;
                        expand_functions_in_expr(expr, module)?;
                    }
                }
            }
        }
        ExprKind::Pipeline(pipeline) => {
            for e in &mut pipeline.exprs {
                expand_functions_in_expr(e, module)?;
            }
        }
        ExprKind::Array(items) | ExprKind::Tuple(items) => {
            for item in items {
                expand_functions_in_expr(item, module)?;
            }
        }
        ExprKind::Binary(binary) => {
            expand_functions_in_expr(&mut binary.left, module)?;
            expand_functions_in_expr(&mut binary.right, module)?;
        }
        _ => {}
    }
    Ok(())
}

/// Find a function definition in the module by name
fn find_function_in_module<'a>(name: &str, module: &'a ModuleDef) -> Option<&'a Func> {
    for stmt in &module.stmts {
        if let StmtKind::VarDef(var_def) = &stmt.kind {
            if var_def.name == name {
                if let Some(value) = &var_def.value {
                    if let ExprKind::Func(func) = &value.kind {
                        return Some(func);
                    }
                }
            }
        }
    }
    None
}

/// Expand a function call by substituting parameters
fn expand_function_call(
    func: &Func,
    args: &[Expr],
    named_args: &std::collections::HashMap<String, Expr>,
) -> Result<Expr> {
    // Build parameter substitution map
    let mut substitutions = std::collections::HashMap::new();

    // Map positional arguments to parameters
    for (i, arg) in args.iter().enumerate() {
        if i < func.params.len() {
            substitutions.insert(func.params[i].name.clone(), arg.clone());
        }
    }

    // Map named arguments to parameters
    for (name, arg) in named_args {
        substitutions.insert(name.clone(), arg.clone());
    }

    // Clone the function body and substitute parameters
    let mut expanded_body = (*func.body).clone();
    substitute_params(&mut expanded_body, &substitutions)?;

    Ok(expanded_body)
}

/// Substitute parameter references with actual arguments
fn substitute_params(
    expr: &mut Expr,
    substitutions: &std::collections::HashMap<String, Expr>,
) -> Result<()> {
    match &mut expr.kind {
        ExprKind::Ident(ident) => {
            if ident.path.is_empty() {
                if let Some(replacement) = substitutions.get(&ident.name) {
                    *expr = replacement.clone();
                }
            }
        }
        ExprKind::FuncCall(func_call) => {
            substitute_params(&mut func_call.name, substitutions)?;
            for arg in &mut func_call.args {
                substitute_params(arg, substitutions)?;
            }
            for (_name, arg) in &mut func_call.named_args {
                substitute_params(arg, substitutions)?;
            }
        }
        ExprKind::Pipeline(pipeline) => {
            for e in &mut pipeline.exprs {
                substitute_params(e, substitutions)?;
            }
        }
        ExprKind::Array(items) | ExprKind::Tuple(items) => {
            for item in items {
                substitute_params(item, substitutions)?;
            }
        }
        ExprKind::Binary(binary) => {
            substitute_params(&mut binary.left, substitutions)?;
            substitute_params(&mut binary.right, substitutions)?;
        }
        _ => {}
    }
    Ok(())
}

/// Extract render() call from the PRQL module
/// Modifies the module in-place to remove the render() call
fn extract_render_from_module(module: &mut ModuleDef) -> Result<Expr> {
    // PRQL queries are typically in a VarDef with kind Main
    for stmt in &mut module.stmts {
        match &mut stmt.kind {
            StmtKind::VarDef(var_def) => {
                // Check if this is the main query
                if matches!(var_def.kind, VarDefKind::Main) {
                    if let Some(value) = &mut var_def.value {
                        if let Some(render) = extract_render_from_expr(value) {
                            return Ok(render);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    bail!("No render() call found in PRQL source")
}

/// Extract render() from an expression if it's a pipeline ending with render()
/// Returns the render expression and modifies the input to remove it
fn extract_render_from_expr(expr: &mut Expr) -> Option<Expr> {
    match &mut expr.kind {
        ExprKind::Pipeline(pipeline) => {
            // Check if last element is render()
            if let Some(last) = pipeline.exprs.last() {
                if is_render_call(last) {
                    // Remove and return the render call
                    return pipeline.exprs.pop();
                }
            }
        }
        _ => {}
    }
    None
}

/// Check if an expression is a render() function call
fn is_render_call(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::FuncCall(func_call) => {
            // Check if the function name is "render"
            if let ExprKind::Ident(ident) = &func_call.name.kind {
                return ident.name == "render";
            }
            false
        }
        _ => false,
    }
}

/// Extract row templates from `derive { ui = (render ...) }` patterns in a pipeline.
///
/// This function walks the PL AST and:
/// 1. Finds `derive` function calls where an argument has alias "ui" and contains `render()`
/// 2. Tracks the current source table from preceding `from` calls
/// 3. Extracts the render expression and assigns an index
/// 4. Replaces the render expression with an integer literal
///
/// Returns a list of extracted templates with their source table context.
/// flutter_rust_bridge:ignore
pub fn extract_row_templates_from_module(
    module: &mut ModuleDef,
) -> Result<Vec<ExtractedRowTemplate>> {
    // First pass: collect render expressions without expanding functions
    // We need to avoid borrowing module mutably and immutably at the same time
    let mut templates = Vec::new();

    // Find the main query
    for stmt in &mut module.stmts {
        if let StmtKind::VarDef(var_def) = &mut stmt.kind {
            if matches!(var_def.kind, VarDefKind::Main) {
                if let Some(value) = &mut var_def.value {
                    extract_row_templates_from_expr(value, &mut templates, None)?;
                }
            }
        }
    }

    // Second pass: expand functions in extracted render expressions
    // Now we can borrow module immutably
    for template in &mut templates {
        expand_functions_in_expr(&mut template.render_expr, module)?;
    }

    Ok(templates)
}

/// Extract row templates from an expression (recursive).
/// `current_table` tracks the most recent `from <table>` in the current pipeline.
fn extract_row_templates_from_expr(
    expr: &mut Expr,
    templates: &mut Vec<ExtractedRowTemplate>,
    current_table: Option<&str>,
) -> Result<Option<String>> {
    match &mut expr.kind {
        ExprKind::Pipeline(pipeline) => {
            let mut table_name: Option<String> = current_table.map(String::from);

            for pipe_expr in &mut pipeline.exprs {
                if let ExprKind::FuncCall(func_call) = &mut pipe_expr.kind {
                    if let ExprKind::Ident(ident) = &func_call.name.kind {
                        let fn_name = &ident.name;

                        // Track `from <table>` calls to know the current source table
                        if fn_name == "from" {
                            if let Some(arg) = func_call.args.first() {
                                if let ExprKind::Ident(table_ident) = &arg.kind {
                                    table_name = Some(table_ident.name.clone());
                                }
                            }
                        }

                        // Look for `derive { ui = (render ...) }`
                        if fn_name == "derive" {
                            for arg in &mut func_call.args {
                                // Derive takes a tuple/record argument
                                if let ExprKind::Tuple(tuple_items) = &mut arg.kind {
                                    for item in tuple_items {
                                        // Check if this item has alias "ui" and is a render() call
                                        if item.alias.as_deref() == Some("ui") {
                                            if is_render_call(item) {
                                                // Extract the render expression
                                                let entity_name = table_name.clone()
                                                    .ok_or_else(|| anyhow::anyhow!(
                                                        "derive {{ ui = (render ...) }} found but no source table detected. \
                                                        Use `from <table>` before derive."
                                                    ))?;

                                                let index = templates.len();

                                                // Clone the render expression before we replace it
                                                // Function expansion happens in second pass
                                                let render_expr = item.clone();

                                                templates.push(ExtractedRowTemplate {
                                                    index,
                                                    entity_name,
                                                    render_expr,
                                                });

                                                // Replace the render() call with an integer literal
                                                *item = Expr {
                                                    kind: ExprKind::Literal(Literal::Integer(
                                                        index as i64,
                                                    )),
                                                    span: item.span.clone(),
                                                    alias: Some("ui".to_string()),
                                                    doc_comment: None,
                                                };
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Recurse into `append(pipeline)` calls
                        if fn_name == "append" {
                            for arg in &mut func_call.args {
                                // Pass None as current_table since append starts a new pipeline
                                extract_row_templates_from_expr(arg, templates, None)?;
                            }
                        }
                    }
                }
            }

            Ok(table_name)
        }
        ExprKind::FuncCall(func_call) => {
            // Handle nested function calls
            for arg in &mut func_call.args {
                extract_row_templates_from_expr(arg, templates, current_table)?;
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Convert PRQL PR AST expression to JSON for easier processing
/// flutter_rust_bridge:ignore
pub fn prql_ast_to_json(expr: &Expr) -> Result<Value> {
    match &expr.kind {
        ExprKind::Literal(lit) => Ok(literal_to_json(lit)),
        ExprKind::Ident(ident) => {
            // Column reference - join path parts
            let full_name = if ident.path.is_empty() {
                ident.name.clone()
            } else {
                let mut parts = ident.path.clone();
                parts.push(ident.name.clone());
                parts.join(".")
            };
            Ok(Value::String(format!("$col:{}", full_name)))
        }
        ExprKind::FuncCall(func_call) => {
            let mut obj = std::collections::HashMap::new();

            // Get function name
            if let ExprKind::Ident(ident) = &func_call.name.kind {
                let func_name = if ident.path.is_empty() {
                    ident.name.clone()
                } else {
                    let mut parts = ident.path.clone();
                    parts.push(ident.name.clone());
                    parts.join(".")
                };
                obj.insert("__fn".to_string(), Value::String(func_name));
            } else {
                bail!("Expected identifier for function name");
            }

            // Process positional arguments
            for (idx, arg) in func_call.args.iter().enumerate() {
                obj.insert(format!("arg{}", idx), prql_ast_to_json(arg)?);
            }

            // Process named arguments
            for (name, value) in &func_call.named_args {
                obj.insert(name.clone(), prql_ast_to_json(value)?);
            }

            Ok(Value::Object(obj))
        }
        ExprKind::Tuple(items) | ExprKind::Array(items) => {
            let values: Result<Vec<_>> = items.iter().map(prql_ast_to_json).collect();
            Ok(Value::Array(values?))
        }
        ExprKind::Binary(binary) => {
            let mut obj = std::collections::HashMap::new();
            obj.insert(
                "__op".to_string(),
                Value::String(format!("{:?}", binary.op)),
            );
            obj.insert("left".to_string(), prql_ast_to_json(&binary.left)?);
            obj.insert("right".to_string(), prql_ast_to_json(&binary.right)?);
            Ok(Value::Object(obj))
        }
        _ => bail!("Unsupported expression type for render: {:?}", expr.kind),
    }
}

fn literal_to_json(lit: &Literal) -> Value {
    match lit {
        Literal::Null => Value::Null,
        Literal::Boolean(b) => Value::Boolean(*b),
        Literal::Integer(n) => Value::Integer(*n),
        Literal::Float(f) => Value::Float(*f),
        Literal::String(s) | Literal::RawString(s) => Value::String(s.clone()),
        Literal::Date(d) => Value::String(d.to_string()),
        Literal::Time(t) => Value::String(t.to_string()),
        Literal::Timestamp(ts) => Value::String(ts.to_string()),
        Literal::ValueAndUnit(v) => {
            let mut obj = std::collections::HashMap::new();
            obj.insert("value".to_string(), Value::Integer(v.n));
            obj.insert("unit".to_string(), Value::String(format!("{:?}", v.unit)));
            Value::Object(obj)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple() {
        let source = r#"
from tasks
filter priority > 2
render (list item_template:(block indent:10))
        "#;

        let split = split_prql_at_render(source).unwrap();

        // Check that render was extracted
        assert!(is_render_call(&split.render_ast));

        // Convert to JSON for inspection
        let json = prql_ast_to_json(&split.render_ast).unwrap();
        match json {
            Value::Object(obj) => {
                assert_eq!(obj.get("__fn"), Some(&Value::String("render".to_string())));
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_render_with_nested_calls() {
        let source = r#"
from tasks
render (row (checkbox checked:status) (text content))
        "#;

        let split = split_prql_at_render(source).unwrap();
        let json = prql_ast_to_json(&split.render_ast).unwrap();

        match &json {
            Value::Object(obj) => {
                assert_eq!(obj.get("__fn"), Some(&Value::String("render".to_string())));
                // First arg should be row(...)
                if let Some(Value::Object(arg0)) = obj.get("arg0") {
                    assert_eq!(arg0.get("__fn"), Some(&Value::String("row".to_string())));
                } else {
                    panic!("Expected arg0 to be Object");
                }
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_function_expansion() {
        let source = r#"
let make_row = func -> (row (text "A") (text "B"))
from tasks
render (make_row)
        "#;

        let split = split_prql_at_render(source).unwrap();
        let json = prql_ast_to_json(&split.render_ast).unwrap();

        // Function should be expanded
        match &json {
            Value::Object(obj) => {
                assert_eq!(obj.get("__fn"), Some(&Value::String("render".to_string())));
                if let Some(Value::Object(arg0)) = obj.get("arg0") {
                    assert_eq!(arg0.get("__fn"), Some(&Value::String("row".to_string())));
                    // Should have 2 children (text "A" and text "B")
                    if let Some(Value::Object(arg00)) = arg0.get("arg0") {
                        assert_eq!(arg00.get("__fn"), Some(&Value::String("text".to_string())));
                        assert_eq!(arg00.get("arg0"), Some(&Value::String("A".to_string())));
                    } else {
                        panic!("Expected arg0.arg0 to be Object");
                    }
                    if let Some(Value::Object(arg01)) = arg0.get("arg1") {
                        assert_eq!(arg01.get("__fn"), Some(&Value::String("text".to_string())));
                        assert_eq!(arg01.get("arg0"), Some(&Value::String("B".to_string())));
                    } else {
                        panic!("Expected arg0.arg1 to be Object");
                    }
                } else {
                    panic!("Expected arg0 to be Object");
                }
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_function_expansion_with_params() {
        let source = r#"
let make_text = func content -> (text content)
from tasks
render (make_text "Hello")
        "#;

        let split = split_prql_at_render(source).unwrap();
        let json = prql_ast_to_json(&split.render_ast).unwrap();

        // Function should be expanded with parameter substitution
        match &json {
            Value::Object(obj) => {
                assert_eq!(obj.get("__fn"), Some(&Value::String("render".to_string())));
                if let Some(Value::Object(arg0)) = obj.get("arg0") {
                    assert_eq!(arg0.get("__fn"), Some(&Value::String("text".to_string())));
                    assert_eq!(arg0.get("arg0"), Some(&Value::String("Hello".to_string())));
                } else {
                    panic!("Expected arg0 to be Object");
                }
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_extract_row_templates_union_query() {
        let source = r#"
from todoist_tasks
derive { ui = (render (row (checkbox checked:this.completed) (text this.content))) }
append (
  from todoist_projects
  derive { ui = (render (row (text this.name))) }
)
render (tree parent_id:parent_id sortkey:sort_key item_template:this.ui)
        "#;

        // Use split_prql_at_render to also remove the final render() call
        let split = split_prql_at_render(source).unwrap();
        let mut module = split.query_module;

        // Extract row templates (this modifies the module in place)
        let templates = extract_row_templates_from_module(&mut module).unwrap();

        assert_eq!(templates.len(), 2, "Should extract 2 row templates");

        // First template should be for todoist_tasks with index 0
        assert_eq!(templates[0].index, 0);
        assert_eq!(templates[0].entity_name, "todoist_tasks");
        assert!(
            is_render_call(&templates[0].render_expr),
            "First template should be a render call"
        );

        // Second template should be for todoist_projects with index 1
        assert_eq!(templates[1].index, 1);
        assert_eq!(templates[1].entity_name, "todoist_projects");
        assert!(
            is_render_call(&templates[1].render_expr),
            "Second template should be a render call"
        );

        // Now convert to RQ and SQL to verify the replacement worked
        let rq = prqlc::pl_to_rq(module).unwrap();
        let sql = prqlc::rq_to_sql(rq, &prqlc::Options::default()).unwrap();

        eprintln!("Generated SQL:\n{}", sql);

        // The SQL should contain "0 AS ui" and "1 AS ui" instead of render expressions
        assert!(
            sql.contains("0 AS ui") || sql.contains("0 as ui"),
            "SQL should contain '0 AS ui'"
        );
        assert!(
            sql.contains("1 AS ui") || sql.contains("1 as ui"),
            "SQL should contain '1 AS ui'"
        );
    }

    #[test]
    fn test_extract_row_templates_single_table_no_extraction() {
        let source = r#"
from todoist_tasks
render (tree parent_id:parent_id sortkey:sort_key item_template:(row (text this.content)))
        "#;

        let mut module = prqlc::prql_to_pl(source).unwrap();

        // No derive { ui = (render ...) }, so no templates should be extracted
        let templates = extract_row_templates_from_module(&mut module).unwrap();

        assert_eq!(
            templates.len(),
            0,
            "Should not extract templates when no derive {{{{ ui = (render ...) }}}} is used"
        );
    }

    #[test]
    fn test_bullet_function_with_this_arg() {
        let source = r#"
from todoist_projects
derive { ui = (render (row (bullet this) (text this.content))) }
render (tree parent_id:parent_id sortkey:sort_key item_template:this.ui)
        "#;

        let split = split_prql_at_render(source).unwrap();
        let mut module = split.query_module;
        let templates = extract_row_templates_from_module(&mut module).unwrap();

        assert_eq!(templates.len(), 1);

        // Convert to JSON and verify structure
        let json = prql_ast_to_json(&templates[0].render_expr).unwrap();
        eprintln!(
            "Template JSON:\n{}",
            serde_json::to_string_pretty(&json).unwrap()
        );

        // The render expression should contain a row() with bullet(this) as first arg
        let obj = json.as_object().unwrap();
        assert_eq!(
            obj.get("__fn").unwrap().as_string_owned(),
            Some("render".to_string())
        );

        let arg0 = obj.get("arg0").unwrap().as_object().unwrap();
        assert_eq!(
            arg0.get("__fn").unwrap().as_string_owned(),
            Some("row".to_string())
        );

        // First argument of row() should be bullet(this) function call
        let bullet_arg = arg0.get("arg0").unwrap().as_object().unwrap();
        assert_eq!(
            bullet_arg.get("__fn").unwrap().as_string_owned(),
            Some("bullet".to_string()),
            "bullet should be parsed as a function call"
        );

        // bullet's argument should be $col:this (column reference to "this")
        let bullet_this_arg = bullet_arg.get("arg0").unwrap().as_string_owned();
        assert_eq!(
            bullet_this_arg,
            Some("$col:this".to_string()),
            "bullet's argument should be column reference 'this'"
        );
    }

    #[test]
    fn test_this_star_syntax() {
        // Test if PRQL supports this.* syntax for "all columns"
        let source = r#"
from todoist_projects
derive { ui = (render (row (bullet this.*) (text this.content))) }
render (tree parent_id:parent_id sortkey:sort_key item_template:this.ui)
        "#;

        let result = split_prql_at_render(source);
        eprintln!("this.* parse result: {:?}", result.is_ok());
        if let Ok(split) = result {
            let mut module = split.query_module;
            let templates = extract_row_templates_from_module(&mut module).unwrap();
            let json = prql_ast_to_json(&templates[0].render_expr).unwrap();
            eprintln!(
                "this.* Template JSON:\n{}",
                serde_json::to_string_pretty(&json).unwrap()
            );
        } else {
            eprintln!("this.* parse error: {:?}", result.err());
        }
    }
}
