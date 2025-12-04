use query_render::parser::*;

fn main() -> anyhow::Result<()> {
    let prql_source = r#"
from tasks
render (row (checkbox checked:status) (text content))
    "#;

    let split = split_prql_at_render(prql_source)?;

    println!("Query Module:\n{:#?}\n", split.query_module);

    let render_json = prql_ast_to_json(&split.render_ast)?;
    println!(
        "Render Call JSON:\n{}\n",
        serde_json::to_string_pretty(&render_json)?
    );

    Ok(())
}
