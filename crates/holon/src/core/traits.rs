use async_trait::async_trait;
use std::fmt::Debug;
use std::sync::Arc;

use holon_api::Value;

// Re-export schema types from holon_api to avoid duplication
pub use holon_api::{DynamicEntity, FieldSchema, HasSchema, Schema};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub trait Lens<T, U>: Clone + Send + Sync + 'static {
    fn get(&self, source: &T) -> Option<U>;
    fn set(&self, source: &mut T, value: U);
    fn sql_column(&self) -> &'static str {
        self.field_name()
    }
    fn field_name(&self) -> &'static str;
}

pub trait Predicate<T>: Send + Sync {
    fn test(&self, item: &T) -> bool;
    fn to_sql(&self) -> Option<SqlPredicate>;

    fn and<P>(self, other: P) -> And<T, Self, P>
    where
        Self: Sized,
        P: Predicate<T>,
    {
        And {
            left: self,
            right: other,
            _phantom: std::marker::PhantomData,
        }
    }

    fn or<P>(self, other: P) -> Or<T, Self, P>
    where
        Self: Sized,
        P: Predicate<T>,
    {
        Or {
            left: self,
            right: other,
            _phantom: std::marker::PhantomData,
        }
    }

    fn not(self) -> Not<T, Self>
    where
        Self: Sized,
    {
        Not {
            inner: self,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Predicate<T> for Arc<dyn Predicate<T>>
where
    T: Send + Sync,
{
    fn test(&self, item: &T) -> bool {
        (**self).test(item)
    }

    fn to_sql(&self) -> Option<SqlPredicate> {
        (**self).to_sql()
    }
}

#[derive(Debug, Clone)]
pub struct SqlPredicate {
    pub sql: String,
    pub params: Vec<Value>,
}

impl SqlPredicate {
    pub fn new(sql: String, params: Vec<Value>) -> Self {
        Self { sql, params }
    }

    pub fn to_params(&self) -> Vec<turso::Value> {
        self.params
            .iter()
            .map(|p| match p {
                Value::String(s) => turso::Value::Text(s.clone()),
                Value::Integer(i) => turso::Value::Integer(*i),
                Value::Float(f) => turso::Value::Real(*f),
                Value::Boolean(b) => turso::Value::Integer(if *b { 1 } else { 0 }),
                Value::Null => turso::Value::Null,
                _ => turso::Value::Null,
            })
            .collect()
    }
}

pub struct And<T, L, R> {
    left: L,
    right: R,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, L, R> Predicate<T> for And<T, L, R>
where
    T: Send + Sync,
    L: Predicate<T>,
    R: Predicate<T>,
{
    fn test(&self, item: &T) -> bool {
        self.left.test(item) && self.right.test(item)
    }

    fn to_sql(&self) -> Option<SqlPredicate> {
        match (self.left.to_sql(), self.right.to_sql()) {
            (Some(left), Some(right)) => {
                let mut params = left.params;
                params.extend(right.params);
                Some(SqlPredicate::new(
                    format!("({}) AND ({})", left.sql, right.sql),
                    params,
                ))
            }
            _ => None,
        }
    }
}

pub struct Or<T, L, R> {
    left: L,
    right: R,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, L, R> Predicate<T> for Or<T, L, R>
where
    T: Send + Sync,
    L: Predicate<T>,
    R: Predicate<T>,
{
    fn test(&self, item: &T) -> bool {
        self.left.test(item) || self.right.test(item)
    }

    fn to_sql(&self) -> Option<SqlPredicate> {
        match (self.left.to_sql(), self.right.to_sql()) {
            (Some(left), Some(right)) => {
                let mut params = left.params;
                params.extend(right.params);
                Some(SqlPredicate::new(
                    format!("({}) OR ({})", left.sql, right.sql),
                    params,
                ))
            }
            _ => None,
        }
    }
}

pub struct Not<T, P> {
    inner: P,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, P> Predicate<T> for Not<T, P>
where
    T: Send + Sync,
    P: Predicate<T>,
{
    fn test(&self, item: &T) -> bool {
        !self.inner.test(item)
    }

    fn to_sql(&self) -> Option<SqlPredicate> {
        self.inner
            .to_sql()
            .map(|pred| SqlPredicate::new(format!("NOT ({})", pred.sql), pred.params))
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait Queryable<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    async fn query<P>(&self, predicate: P) -> Result<Vec<T>>
    where
        P: Predicate<T> + Send + 'static;
}

/// Result of an incremental sync operation
#[derive(Debug, Clone)]
pub struct SyncResult<T, Token> {
    /// All items from sync (for full sync) or changed items (for incremental)
    pub items: Vec<T>,
    /// Items that were updated (empty for full sync, populated for incremental)
    pub updated: Vec<T>,
    /// IDs of deleted items (empty for full sync, populated for incremental)
    pub deleted: Vec<String>,
    /// Token for next incremental sync (None if no more updates available)
    pub next_token: Option<Token>,
}

// HasSchema, Schema, and FieldSchema are re-exported from holon_api above

#[cfg(test)]
mod tests {
    use super::*;

    struct TestItem {
        value: i64,
    }

    struct TestPredicate;

    impl Predicate<TestItem> for TestPredicate {
        fn test(&self, item: &TestItem) -> bool {
            item.value > 10
        }

        fn to_sql(&self) -> Option<SqlPredicate> {
            Some(SqlPredicate::new(
                "value > ?".to_string(),
                vec![Value::Integer(10)],
            ))
        }
    }

    #[test]
    fn test_predicate_and() {
        let item = TestItem { value: 15 };

        let pred = TestPredicate.and(TestPredicate);
        assert!(pred.test(&item));
    }

    #[test]
    fn test_predicate_or() {
        let item = TestItem { value: 5 };

        let pred = TestPredicate.or(TestPredicate);
        assert!(!pred.test(&item));
    }

    #[test]
    fn test_predicate_not() {
        let item = TestItem { value: 5 };

        let pred = TestPredicate.not();
        assert!(pred.test(&item));
    }

    #[test]
    fn test_sql_generation() {
        let pred = TestPredicate.and(TestPredicate);
        let sql = pred.to_sql().unwrap();
        assert_eq!(sql.sql, "(value > ?) AND (value > ?)");
        assert_eq!(sql.params.len(), 2);
    }

    #[test]
    fn test_schema_to_sql() {
        let schema = Schema::new(
            "tasks",
            vec![
                FieldSchema::new("id", "TEXT").primary_key(),
                FieldSchema::new("title", "TEXT"),
                FieldSchema::new("priority", "INTEGER").indexed().nullable(),
            ],
        );

        let sql = schema.to_create_table_sql();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS tasks"));
        assert!(sql.contains("id TEXT PRIMARY KEY"));
        assert!(sql.contains("title TEXT NOT NULL"));
        assert!(sql.contains("priority INTEGER"));

        let indexes = schema.to_index_sql();
        assert_eq!(indexes.len(), 1);
        assert!(indexes[0].contains("CREATE INDEX IF NOT EXISTS idx_tasks_priority"));
    }
}
