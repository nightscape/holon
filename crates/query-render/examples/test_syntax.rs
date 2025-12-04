fn main() {
    let examples = vec![
        (
            "single_line",
            r#"from tasks
render (row (text "hello"))"#,
        ),
        (
            "multiline_paren",
            r#"from tasks
render (
  row (text "hello")
)"#,
        ),
        (
            "tuple_syntax",
            r#"from tasks
select { ui: render (row (text "hello")) }"#,
        ),
    ];

    for (name, src) in examples {
        println!("\n=== {} ===", name);
        println!("Source:\n{}\n", src);
        match prqlc::prql_to_pl(src) {
            Ok(module) => println!("✓ Parses successfully\n{:#?}", module),
            Err(e) => println!("✗ Parse error:\n{}", e),
        }
    }
}
