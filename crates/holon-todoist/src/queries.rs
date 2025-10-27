//! PRQL query templates for Todoist hierarchy
//!
//! This module provides PRQL CTE definitions that unify tasks and projects
//! into a single hierarchy suitable for outliner-style rendering.
//!
//! The hierarchy uses `node_type` column containing the full entity name
//! (e.g., "todoist_tasks", "todoist_projects") for operation routing.

/// PRQL CTE that defines the unified Todoist hierarchy
///
/// This CTE combines `todoist_projects` and `todoist_tasks` into a single
/// hierarchical structure where:
/// - Projects can be parents of other projects (sub-projects)
/// - Projects can be parents of tasks (top-level tasks)
/// - Tasks can be parents of tasks (subtasks)
///
/// The `parent_id` column is computed to create this unified hierarchy:
/// - For projects: `parent_id` points to parent project (or NULL for root)
/// - For tasks with `parent_id`: points to parent task
/// - For tasks without `parent_id`: points to `project_id` (the containing project)
///
/// The `node_type` column contains the full entity name for operation routing.
///
/// # Example Usage
///
/// ```prql
/// # Include the CTE definition
/// let todoist_hierarchy = ...  # (use TODOIST_HIERARCHY_CTE constant)
///
/// # Query the unified hierarchy
/// from todoist_hierarchy
/// filter parent_id == null  # Get root nodes
/// select {id, content, node_type}
/// render (list item_template:(text content:this.content))
/// ```
pub const TODOIST_HIERARCHY_CTE: &str = r#"let todoist_hierarchy = (
    from todoist_projects
    filter (is_archived == null || is_archived == false)
    select {
        id,
        parent_id,
        content = name,
        node_type = "todoist_projects",
        sort_order = order ?? 0,
        color,
        completed = null,
        priority = null,
        due_date = null,
        project_id = null,
        is_favorite
    }
    append (
        from todoist_tasks
        filter (is_deleted == null || is_deleted == false)
        select {
            id,
            parent_id = case [
                parent_id != null => parent_id,
                true => project_id
            ],
            content,
            node_type = "todoist_tasks",
            sort_order = 0,
            color = null,
            completed,
            priority,
            due_date,
            project_id,
            is_favorite = null
        }
    )
)"#;

/// Returns a complete PRQL query that uses the hierarchy CTE
///
/// # Arguments
/// * `body` - The PRQL query body that references `todoist_hierarchy`
///
/// # Example
/// ```
/// use holon_todoist::queries::with_hierarchy;
///
/// let query = with_hierarchy(r#"
/// from todoist_hierarchy
/// filter parent_id == null
/// select {id, content, node_type, completed}
/// "#);
/// ```
pub fn with_hierarchy(body: &str) -> String {
    format!("{}\n\n{}", TODOIST_HIERARCHY_CTE, body.trim())
}

/// Entity name for Todoist tasks (matches the value in node_type column)
pub const ENTITY_TODOIST_TASKS: &str = "todoist_tasks";

/// Entity name for Todoist projects (matches the value in node_type column)
pub const ENTITY_TODOIST_PROJECTS: &str = "todoist_projects";

#[cfg(test)]
mod tests {
    use super::*;

    fn compile_prql(query: &str) -> Result<String, String> {
        holon_prql_render::parse_query_render(query)
            .map(|(sql, _)| sql)
            .map_err(|e| e.to_string())
    }

    #[test]
    fn test_hierarchy_cte_is_valid_prql() {
        let query = with_hierarchy(
            r#"
from todoist_hierarchy
select {id, content, node_type}
render (text content:this.content)
"#,
        );

        let result = compile_prql(&query);
        assert!(
            result.is_ok(),
            "Failed to compile hierarchy query: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_hierarchy_produces_union_sql() {
        let query = with_hierarchy(
            r#"
from todoist_hierarchy
select {id, content, node_type}
render (text content:this.content)
"#,
        );

        let sql = compile_prql(&query).unwrap();

        // Should contain UNION ALL (may be split across lines in formatted SQL)
        let sql_no_whitespace = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            sql_no_whitespace.contains("UNION ALL"),
            "Expected UNION ALL in generated SQL: {}",
            sql
        );

        // Should reference both tables
        assert!(
            sql.contains("todoist_projects"),
            "Expected todoist_projects in SQL: {}",
            sql
        );
        assert!(
            sql.contains("todoist_tasks"),
            "Expected todoist_tasks in SQL: {}",
            sql
        );
    }

    #[test]
    fn test_node_type_contains_entity_names() {
        let query = with_hierarchy(
            r#"
from todoist_hierarchy
select {id, node_type}
render (text content:this.node_type)
"#,
        );

        let sql = compile_prql(&query).unwrap();

        // Should contain the literal entity names
        assert!(
            sql.contains("'todoist_projects'") || sql.contains("\"todoist_projects\""),
            "Expected 'todoist_projects' literal in SQL: {}",
            sql
        );
        assert!(
            sql.contains("'todoist_tasks'") || sql.contains("\"todoist_tasks\""),
            "Expected 'todoist_tasks' literal in SQL: {}",
            sql
        );
    }
}
