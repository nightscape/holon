//! PRQL Lineage Analysis for Automatic Operation Inference
//!
//! This module uses PRQL's s-string feature to preserve lineage through render functions,
//! allowing automatic inference of which operations can be performed on widgets.
//!
//! # Approach
//!
//! 1. **S-String Stub Injection**: Inject UI function stubs that use s-strings to preserve lineage
//! 2. **Direct Lineage Analysis**: Run lineage on the original query structure (no transformation!)
//! 3. **Tree Annotation**: Walk the RenderExpr tree and attach operations where lineage shows direct column refs
//!
//! # Example
//!
//! ```prql
//! # Injected stubs (preserve lineage via s-strings):
//! let ui_checkbox = checked:null -> s"checkbox({checked})"
//! let ui_text = content:null -> s"text({content})"
//!
//! # Original query (no transformation needed):
//! from blocks
//! select {id, content, completed}
//! render (list item_template:(row (checkbox checked:this.completed) (text content:this.content)))
//! ```

use anyhow::{Context, Result};
use prqlc::internal::pl_to_lineage;
use prqlc::ir::pl::Lineage;
use prqlc::prql_to_pl;
use regex::Regex;
use std::collections::{HashMap, HashSet};

/// Discovered function in render() expression
#[derive(Debug, Clone)]
pub struct FunctionDiscovery {
    pub name: String,
    pub params: Vec<String>,
}

/// Derived column generated from flattening
#[derive(Debug, Clone)]
pub struct DerivedColumn {
    pub alias: String,
    pub expr: String,
}

/// Column source information from lineage
#[derive(Debug, Clone)]
pub struct ColumnSource {
    pub table: String,
    pub column: String,
    pub is_direct: bool, // false if computed/derived
}

/// Widget to operation mapping
#[derive(Debug, Clone)]
pub struct WidgetOperationMapping {
    pub widget_alias: String,                  // "checkbox_el_0"
    pub widget_type: String,                   // "checkbox"
    pub modified_param: String,                // "checked"
    pub operation: holon_api::OperationWiring, // Use the canonical OperationWiring from holon-api
}

/// Lineage preprocessor for automatic operation inference
/// Using empty struct body for FRB compatibility (unit structs not supported)
/// flutter_rust_bridge:non_opaque
pub struct LineagePreprocessor {}

impl LineagePreprocessor {
    pub fn new() -> Self {
        Self {}
    }

    /// Analyze query with s-string stub injection to preserve lineage
    ///
    /// Returns: Lineage information for later tree annotation
    ///
    /// This implements the simplified pipeline:
    /// 1. Discover function calls in render()
    /// 2. Generate s-string stubs that preserve lineage
    /// 3. Inject stubs and run lineage analysis directly
    /// 4. Return lineage for tree walker to use
    /// flutter_rust_bridge:ignore
    pub fn analyze_query(&self, original_query: &str) -> Result<Lineage> {
        // Step 1: Extract the query before render() and the render expression
        let (before_render, render_expr) = self.extract_render_parts(original_query)?;

        // Step 2: Discover function calls using regex pre-scan
        let functions = self.discover_functions(&render_expr)?;

        // Step 3: Generate s-string stubs that preserve lineage
        let stubs = self.generate_sstring_stubs(&functions);

        // Step 4: Build augmented query (inject stubs + select the render expression)
        // Note: We don't use the "render" keyword in lineage analysis - just select the expression
        // If there's no render expression, use the query as-is (it already has a select)
        // If there's a render expression, use derive to add it (since query already has select)
        // Replace function names in render_expr with ui_ prefixed versions to match stubs
        // Extract prql directive if present and put it first
        // The prql directive must be the first non-whitespace line
        let trimmed = before_render.trim_start();
        let (prql_directive, query_body) = if trimmed.starts_with("prql") {
            if let Some(pos) = trimmed.find('\n') {
                let directive = trimmed[..pos].trim().to_string();
                let body = trimmed[pos + 1..].trim_start().to_string();
                (Some(directive), body)
            } else {
                (Some(trimmed.trim().to_string()), String::new())
            }
        } else {
            (None, before_render.clone())
        };

        let augmented = if render_expr.is_empty() {
            if stubs.is_empty() {
                before_render
            } else {
                // Put prql directive first (if present), then stubs, then query body
                if let Some(prql) = prql_directive {
                    format!("{}\n\n{}\n\n{}", prql, stubs, query_body)
                } else {
                    format!("{}\n\n{}", stubs, query_body)
                }
            }
        } else {
            // Replace function names with ui_ prefixed versions
            let mut render_expr_prefixed = render_expr.clone();
            for func in functions {
                let pattern = format!("({} ", func.name);
                let replacement = format!("(ui_{} ", func.name);
                render_expr_prefixed = render_expr_prefixed.replace(&pattern, &replacement);
            }
            // Put prql directive first (if present), then stubs, then query body, then derive
            // Note: prql directive must come before let statements
            if let Some(prql) = prql_directive {
                format!(
                    "{}\n\n{}\n\n{}\nderive {{_render_expr = {}}}",
                    prql, stubs, query_body, render_expr_prefixed
                )
            } else {
                format!(
                    "{}\n\n{}\nderive {{_render_expr = {}}}",
                    stubs, query_body, render_expr_prefixed
                )
            }
        };

        // Step 5: Parse and analyze lineage
        #[cfg(test)]
        eprintln!("About to analyze lineage for query:\n{}\n", &augmented);

        let module_def = prql_to_pl(&augmented)
            .with_context(|| format!("Failed to parse augmented query:\n{}", &augmented))?;

        let frame_collector = pl_to_lineage(module_def)
            .map_err(|e| anyhow::anyhow!("Lineage analysis failed: {:?}", e))?;

        // Extract the final lineage from the last frame
        let lineage = frame_collector
            .frames
            .last()
            .ok_or_else(|| anyhow::anyhow!("No lineage frames found"))?
            .1
            .clone();

        #[cfg(test)]
        eprintln!("=== LINEAGE ===\n{:#?}\n", &lineage);

        Ok(lineage)
    }

    /// Extract the query before render() and the render expression itself
    fn extract_render_parts(&self, query: &str) -> Result<(String, String)> {
        let render_re =
            Regex::new(r"(?s)(.*)render\s+(.*)$").context("Failed to compile render regex")?;

        if let Some(caps) = render_re.captures(query) {
            let before = caps.get(1).unwrap().as_str().trim_end().to_string();
            let expr = caps.get(2).unwrap().as_str().trim().to_string();
            Ok((before, expr))
        } else {
            // No render() call - return query as-is and empty expression
            Ok((query.to_string(), String::new()))
        }
    }

    /// Discover function calls using recursive parsing
    ///
    /// This scans for function call patterns like `(funcname param:value)`
    /// and extracts unique function names and their parameters, handling nesting
    fn discover_functions(&self, render_expr: &str) -> Result<Vec<FunctionDiscovery>> {
        let mut functions: HashMap<String, HashSet<String>> = HashMap::new();
        self.discover_functions_recursive(render_expr, &mut functions);

        // Convert to sorted FunctionDiscovery structs
        let mut result: Vec<FunctionDiscovery> = functions
            .into_iter()
            .map(|(name, params)| {
                let mut sorted_params: Vec<String> = params.into_iter().collect();
                sorted_params.sort();
                FunctionDiscovery {
                    name,
                    params: sorted_params,
                }
            })
            .collect();

        result.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(result)
    }

    /// Recursively discover functions in expression
    fn discover_functions_recursive(
        &self,
        expr: &str,
        functions: &mut HashMap<String, HashSet<String>>,
    ) {
        let chars: Vec<char> = expr.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            if chars[i] == '(' {
                // Find the function name (starts right after '(')
                i += 1;
                let start = i;

                // Collect function name (lowercase letters, digits, underscores)
                while i < chars.len()
                    && (chars[i].is_ascii_lowercase()
                        || chars[i].is_ascii_digit()
                        || chars[i] == '_')
                {
                    i += 1;
                }

                if i > start && i < chars.len() && chars[i].is_whitespace() {
                    let func_name = chars[start..i].iter().collect::<String>();

                    // Skip whitespace
                    while i < chars.len() && chars[i].is_whitespace() {
                        i += 1;
                    }

                    // Find matching closing paren for this function call
                    let args_start = i;
                    let mut paren_depth = 1;
                    while i < chars.len() && paren_depth > 0 {
                        if chars[i] == '(' {
                            paren_depth += 1;
                        } else if chars[i] == ')' {
                            paren_depth -= 1;
                        }
                        if paren_depth > 0 {
                            i += 1;
                        }
                    }

                    let args = chars[args_start..i].iter().collect::<String>();

                    // Extract parameter names from THIS function's direct parameters
                    // (not from nested function calls)
                    // We look for patterns like "param:" that are NOT inside nested parens
                    let param_chars: Vec<char> = args.chars().collect();
                    let mut j = 0;
                    let mut depth = 0;
                    let mut current_params = HashSet::new();

                    while j < param_chars.len() {
                        if param_chars[j] == '(' {
                            depth += 1;
                        } else if param_chars[j] == ')' {
                            depth -= 1;
                        } else if depth == 0 && param_chars[j].is_ascii_lowercase() {
                            // Potential parameter name at depth 0
                            let param_start = j;
                            while j < param_chars.len()
                                && (param_chars[j].is_ascii_lowercase()
                                    || param_chars[j].is_ascii_digit()
                                    || param_chars[j] == '_')
                            {
                                j += 1;
                            }
                            if j < param_chars.len() && param_chars[j] == ':' {
                                let param_name =
                                    param_chars[param_start..j].iter().collect::<String>();
                                current_params.insert(param_name);
                            }
                            continue;
                        }
                        j += 1;
                    }

                    functions
                        .entry(func_name)
                        .or_insert_with(HashSet::new)
                        .extend(current_params);

                    // Recursively process nested calls in args
                    self.discover_functions_recursive(&args, functions);
                }
            } else {
                i += 1;
            }
        }
    }

    /// Generate s-string stubs that preserve lineage
    ///
    /// S-strings allow lineage to flow through function calls by treating
    /// parameters as SQL expressions that get substituted.
    ///
    /// Functions are prefixed with `ui_` to avoid conflicts with PRQL standard library.
    fn generate_sstring_stubs(&self, functions: &[FunctionDiscovery]) -> String {
        functions
            .iter()
            .map(|func| {
                let ui_name = format!("ui_{}", func.name);
                if func.params.is_empty() {
                    // Functions with no parameters - simple pass-through
                    format!(
                        "let {} = _items:null -> s\"{}({{_items}})\"",
                        ui_name, func.name
                    )
                } else {
                    // Build parameter list and s-string template
                    let param_list = func
                        .params
                        .iter()
                        .map(|p| format!("{}:null", p))
                        .collect::<Vec<_>>()
                        .join(" ");

                    let template_params = func
                        .params
                        .iter()
                        .map(|p| format!("{{{}}}", p))
                        .collect::<Vec<_>>()
                        .join(", ");

                    format!(
                        "let {} = {} -> s\"{}({})\"",
                        ui_name, param_list, func.name, template_params
                    )
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Extract everything before render() from query
    #[allow(dead_code)] // Used in tests
    fn extract_before_render(&self, query: &str) -> String {
        if let Some(pos) = query.find("render") {
            query[..pos].trim_end().to_string()
        } else {
            query.to_string()
        }
    }
}

impl Default for LineagePreprocessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// H3: Validate lineage traces columns through CASE expressions
    ///
    /// When using CASE to compute parent_id, lineage should still track
    /// the source columns (parent_id, project_id) from the original tables.
    #[test]
    fn test_case_expression_preserves_lineage() {
        let preprocessor = LineagePreprocessor::new();

        let case_query = r#"
prql target:sql.sqlite

from tasks
select {
  id,
  effective_parent = case [
    parent_id != null => parent_id,
    true => project_id
  ],
  content
}
"#;

        let lineage = preprocessor
            .analyze_query(case_query)
            .expect("Should successfully analyze CASE query");

        // Verify the query parses and produces lineage
        assert!(
            !lineage.columns.is_empty(),
            "CASE query should produce columns in lineage"
        );

        // Check that we have columns in the output
        // LineageColumn is an enum - extract names from Single variant
        use prqlc::ir::pl::LineageColumn;
        let column_names: Vec<String> = lineage
            .columns
            .iter()
            .filter_map(|col| {
                if let LineageColumn::Single { name, .. } = col {
                    name.as_ref().map(|ident| ident.to_string())
                } else {
                    None
                }
            })
            .collect();

        assert!(
            column_names
                .iter()
                .any(|n| n == "effective_parent" || n.contains("case")),
            "Should have effective_parent or case-derived column. Found: {:?}",
            column_names
        );
    }

    /// H4: Validate that literal string columns (like node_type) survive through the pipeline
    #[test]
    fn test_literal_column_survives_pipeline() {
        let preprocessor = LineagePreprocessor::new();

        let literal_query = r#"
prql target:sql.sqlite

from tasks
select {
  id,
  content,
  node_type = "todoist_tasks"
}
"#;

        let lineage = preprocessor
            .analyze_query(literal_query)
            .expect("Should successfully analyze literal column query");

        // Verify node_type appears in output columns
        use prqlc::ir::pl::LineageColumn;
        let column_names: Vec<String> = lineage
            .columns
            .iter()
            .filter_map(|col| {
                if let LineageColumn::Single { name, .. } = col {
                    name.as_ref().map(|ident| ident.to_string())
                } else {
                    None
                }
            })
            .collect();

        assert!(
            column_names.contains(&"node_type".to_string()),
            "node_type literal column should survive pipeline. Found: {:?}",
            column_names
        );
    }

    /// H2 + H4 combined: Validate unified hierarchy pattern with UNION and node_type
    #[test]
    fn test_unified_hierarchy_pattern() {
        let preprocessor = LineagePreprocessor::new();

        // This is the actual pattern we'll use for Todoist hierarchy
        let hierarchy_query = r#"
prql target:sql.sqlite

from projects
select {
  id,
  parent_id,
  content = name,
  node_type = "todoist_projects"
}
append (
  from tasks
  select {
    id,
    parent_id = case [
      parent_id != null => parent_id,
      true => project_id
    ],
    content,
    node_type = "todoist_tasks"
  }
)
select {
  id,
  parent_id,
  content,
  node_type
}
"#;

        let lineage = preprocessor
            .analyze_query(hierarchy_query)
            .expect("Should successfully analyze unified hierarchy query");

        // H2: Both source tables tracked
        assert_eq!(
            lineage.inputs.len(),
            2,
            "Unified hierarchy should track both input tables"
        );

        let table_names: Vec<String> = lineage
            .inputs
            .iter()
            .map(|input| format!("{:?}", input.table))
            .collect();
        let table_names_str = table_names.join(" ");

        assert!(
            table_names_str.contains("projects"),
            "Should include 'projects' table. Found: {}",
            table_names_str
        );
        assert!(
            table_names_str.contains("tasks"),
            "Should include 'tasks' table. Found: {}",
            table_names_str
        );

        // H4: node_type column survives
        use prqlc::ir::pl::LineageColumn;
        let column_names: Vec<String> = lineage
            .columns
            .iter()
            .filter_map(|col| {
                if let LineageColumn::Single { name, .. } = col {
                    name.as_ref().map(|ident| ident.to_string())
                } else {
                    None
                }
            })
            .collect();

        assert!(
            column_names.contains(&"node_type".to_string()),
            "node_type should survive through UNION. Found: {:?}",
            column_names
        );
    }

    #[test]
    fn test_union_lineage_tracks_both_tables() {
        let preprocessor = LineagePreprocessor::new();

        // Test UNION query with two actual tables
        let union_query = r#"
prql target:sql.sqlite

from blocks
select {id, content, completed}
append (
  from tasks
  select {id, content, completed}
)
select {
  id,
  content,
  completed
}
"#;

        let lineage = preprocessor
            .analyze_query(union_query)
            .expect("Should successfully analyze UNION query");

        // Verify that both tables are tracked in inputs
        assert_eq!(
            lineage.inputs.len(),
            2,
            "UNION query should track both input tables"
        );

        // Verify both tables are present by checking table names
        let table_names: Vec<String> = lineage
            .inputs
            .iter()
            .map(|input| {
                // input.table is an Ident, convert to string representation
                format!("{:?}", input.table)
            })
            .collect();

        let table_names_str = table_names.join(" ");
        assert!(
            table_names_str.contains("blocks"),
            "Should include 'blocks' table. Found: {}",
            table_names_str
        );
        assert!(
            table_names_str.contains("tasks"),
            "Should include 'tasks' table. Found: {}",
            table_names_str
        );
    }

    /// Test that PRQL lineage sees through CTEs (let statements)
    ///
    /// When using `let todoist_hierarchy = (...)` followed by `from todoist_hierarchy`,
    /// lineage should still track the underlying source tables, not just the CTE name.
    #[test]
    fn test_cte_lineage_sees_through_to_source_tables() {
        let preprocessor = LineagePreprocessor::new();

        let cte_query = r#"
prql target:sql.sqlite

let todoist_hierarchy = (
    from todoist_projects
    select {id, parent_id, content = name, node_type = "todoist_projects"}
    append (
        from todoist_tasks
        select {id, parent_id, content, node_type = "todoist_tasks"}
    )
)

from todoist_hierarchy
select {id, parent_id, content, node_type}
"#;

        let lineage = preprocessor
            .analyze_query(cte_query)
            .expect("Should analyze CTE query");

        // PRQL should see through the CTE to the underlying tables
        let table_names: Vec<String> = lineage
            .inputs
            .iter()
            .map(|input| input.table.name.clone())
            .collect();

        println!("CTE lineage inputs: {:?}", table_names);

        assert!(
            lineage.inputs.len() >= 2,
            "CTE lineage should see both source tables. Found {} inputs: {:?}",
            lineage.inputs.len(),
            table_names
        );

        assert!(
            table_names.iter().any(|n| n.contains("todoist_projects")),
            "Should include 'todoist_projects'. Found: {:?}",
            table_names
        );
        assert!(
            table_names.iter().any(|n| n.contains("todoist_tasks")),
            "Should include 'todoist_tasks'. Found: {:?}",
            table_names
        );
    }
}
