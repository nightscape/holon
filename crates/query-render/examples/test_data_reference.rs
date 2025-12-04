fn main() {
    // Test: Does PRQL allow referencing a column that doesn't exist?
    let prql = r#"
from projects
derive { content = name, entity_name = "projects" }
select { id, parent_id, entity_name, data }
    "#;

    println!("=== Testing undefined 'data' column reference ===\n");
    println!("PRQL:\n{}\n", prql);

    match prqlc::prql_to_pl(prql) {
        Ok(pl) => {
            println!("✓ PL parsing succeeded");
            match prqlc::pl_to_rq(pl) {
                Ok(rq) => {
                    println!("✓ RQ conversion succeeded");
                    println!("\nRQ columns: {:?}", rq.relation.columns);
                    match prqlc::rq_to_sql(rq, &prqlc::Options::default()) {
                        Ok(sql) => println!("\nSQL:\n{}", sql),
                        Err(e) => println!("✗ SQL generation failed: {}", e),
                    }
                }
                Err(e) => println!("✗ RQ conversion failed: {}", e),
            }
        }
        Err(e) => println!("✗ PL parsing failed: {}", e),
    }
}
