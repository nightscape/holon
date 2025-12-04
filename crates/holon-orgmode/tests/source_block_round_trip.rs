//! Round-trip tests for source block parsing and writing.

use holon_api::{BlockResult, SourceBlock, Value};
use holon_filesystem::directory::ROOT_ID;
use holon_orgmode::models::OrgSourceBlock;
use holon_orgmode::{
    delete_source_block, format_api_source_block, format_block_result, format_org_source_block,
    insert_source_block, parse_org_file, update_source_block,
};
use std::path::PathBuf;

#[test]
fn test_source_block_parse_and_format_roundtrip() {
    let original = r#"* Headline with code
:PROPERTIES:
:ID: test-headline
:END:

Some intro text.

#+NAME: my-query
#+BEGIN_SRC prql :connection main :results table
from tasks
filter completed == false
sort priority desc
#+END_SRC
"#;

    let path = PathBuf::from("/test/file.org");
    let result = parse_org_file(&path, original, ROOT_ID, 0).unwrap();

    assert_eq!(result.headlines.len(), 1);
    let headline = &result.headlines[0];
    let source_blocks = headline.get_source_blocks();
    assert_eq!(source_blocks.len(), 1);

    let block = &source_blocks[0];
    assert_eq!(block.language, Some("prql".to_string()));
    assert!(block.source.contains("from tasks"));

    let formatted = format_org_source_block(block);
    assert!(formatted.contains("#+BEGIN_SRC prql"));
    assert!(formatted.contains("from tasks"));
    assert!(formatted.contains("filter completed == false"));
    assert!(formatted.contains("#+END_SRC"));
}

#[test]
fn test_source_block_with_header_args_roundtrip() {
    let original = r#"* Test
#+BEGIN_SRC python :results output :session main
print("hello")
#+END_SRC
"#;

    let path = PathBuf::from("/test/file.org");
    let result = parse_org_file(&path, original, ROOT_ID, 0).unwrap();

    let headline = &result.headlines[0];
    let source_blocks = headline.get_source_blocks();
    assert_eq!(source_blocks.len(), 1);

    let block = &source_blocks[0];
    assert_eq!(block.language, Some("python".to_string()));
    assert!(block.header_args.contains_key("results"));
    assert!(block.header_args.contains_key("session"));

    let formatted = format_org_source_block(block);
    assert!(formatted.contains("#+BEGIN_SRC python"));
    assert!(formatted.contains(":results output"));
    assert!(formatted.contains(":session main"));
}

#[test]
fn test_update_source_block_preserves_context() {
    let content = "* Headline\nBefore text\n#+BEGIN_SRC python\nold code\n#+END_SRC\nAfter text\n";
    let block_start = content.find("#+BEGIN_SRC").unwrap();
    let block_end = content.find("#+END_SRC").unwrap() + "#+END_SRC".len();

    let new_block = OrgSourceBlock::new(Some("rust".to_string()), "fn main() {}".to_string(), 0, 0);

    let result = update_source_block(content, block_start, block_end, &new_block).unwrap();

    assert!(result.contains("Before text"));
    assert!(result.contains("After text"));
    assert!(result.contains("#+BEGIN_SRC rust"));
    assert!(result.contains("fn main() {}"));
    assert!(!result.contains("old code"));
    assert!(!result.contains("python"));
}

#[test]
fn test_insert_and_delete_roundtrip() {
    let content = "* Headline\nSome text\n";

    let block = OrgSourceBlock::new(
        Some("sql".to_string()),
        "SELECT * FROM users".to_string(),
        0,
        0,
    );

    let with_block = insert_source_block(content, content.len(), &block).unwrap();
    assert!(with_block.contains("#+BEGIN_SRC sql"));
    assert!(with_block.contains("SELECT * FROM users"));

    let block_start = with_block.find("#+BEGIN_SRC").unwrap();
    let block_end = with_block.find("#+END_SRC").unwrap() + "#+END_SRC".len();

    let without_block = delete_source_block(&with_block, block_start, block_end).unwrap();
    assert!(!without_block.contains("#+BEGIN_SRC"));
    assert!(without_block.contains("Some text"));
}

#[test]
fn test_format_api_source_block_with_results() {
    let block = SourceBlock::new("prql", "from tasks | take 5")
        .with_name("task-query")
        .with_header_arg("connection", Value::String("main".to_string()))
        .with_results(BlockResult::table(
            vec!["id".to_string(), "title".to_string()],
            vec![
                vec![Value::Integer(1), Value::String("Task 1".to_string())],
                vec![Value::Integer(2), Value::String("Task 2".to_string())],
            ],
        ));

    let formatted = format_api_source_block(&block);

    assert!(formatted.contains("#+NAME: task-query"));
    assert!(formatted.contains("#+BEGIN_SRC prql"));
    assert!(formatted.contains(":connection main"));
    assert!(formatted.contains("from tasks | take 5"));
    assert!(formatted.contains("#+RESULTS: task-query"));
    assert!(formatted.contains("| id | title |"));
    assert!(formatted.contains("| 1 | Task 1 |"));
}

#[test]
fn test_format_block_result_text() {
    let result = BlockResult::text("Line 1\nLine 2\nLine 3");
    let output = format_block_result(&result, Some("my-block"));

    assert!(output.starts_with("#+RESULTS: my-block"));
    assert!(output.contains(": Line 1"));
    assert!(output.contains(": Line 2"));
    assert!(output.contains(": Line 3"));
}

#[test]
fn test_format_block_result_error() {
    let result = BlockResult::error("Query failed: connection refused");
    let output = format_block_result(&result, None);

    assert!(output.starts_with("#+RESULTS:"));
    assert!(output.contains("#+begin_error"));
    assert!(output.contains("Query failed: connection refused"));
    assert!(output.contains("#+end_error"));
}

#[test]
fn test_multiple_source_blocks_in_section() {
    let content = r#"* Dashboard
#+NAME: tasks
#+BEGIN_SRC prql
from tasks
#+END_SRC

#+NAME: projects
#+BEGIN_SRC prql
from projects
#+END_SRC
"#;

    let path = PathBuf::from("/test/file.org");
    let result = parse_org_file(&path, content, ROOT_ID, 0).unwrap();

    let headline = &result.headlines[0];
    let source_blocks = headline.get_source_blocks();

    assert_eq!(source_blocks.len(), 2);
    assert!(source_blocks[0].source.contains("from tasks"));
    assert!(source_blocks[1].source.contains("from projects"));
}
