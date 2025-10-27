use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use super::traits::{Predicate, Queryable, Result};

/// Type alias for deduplication key function
type DedupKeyFn<T> = Box<dyn Fn(&T) -> String + Send + Sync>;

pub struct UnifiedQuery<T>
where
    T: Send + Sync + 'static,
{
    sources: Vec<Box<dyn QueryableErased<T>>>,
    dedup_key: Option<DedupKeyFn<T>>,
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
trait QueryableErased<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    async fn query_erased(&self, predicate: Arc<dyn Predicate<T>>) -> Result<Vec<T>>;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<T, Q> QueryableErased<T> for Q
where
    T: Send + Sync + 'static,
    Q: Queryable<T> + Send + Sync,
{
    async fn query_erased(&self, predicate: Arc<dyn Predicate<T>>) -> Result<Vec<T>> {
        self.query(predicate).await
    }
}

impl<T> UnifiedQuery<T>
where
    T: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            dedup_key: None,
        }
    }

    pub fn add_source<Q>(mut self, source: Q) -> Self
    where
        Q: Queryable<T> + Send + Sync + 'static,
    {
        self.sources.push(Box::new(source));
        self
    }

    pub fn with_dedup<F>(mut self, key_fn: F) -> Self
    where
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        self.dedup_key = Some(Box::new(key_fn));
        self
    }

    pub async fn query<P>(&self, predicate: P) -> Result<Vec<T>>
    where
        P: Predicate<T> + Send + 'static,
    {
        let pred_arc: Arc<dyn Predicate<T>> = Arc::new(predicate);
        let mut all_results = Vec::new();

        for source in &self.sources {
            match source.query_erased(Arc::clone(&pred_arc)).await {
                Ok(mut results) => all_results.append(&mut results),
                Err(e) => {
                    eprintln!("Warning: Source query failed: {}", e);
                }
            }
        }

        if let Some(ref dedup_fn) = self.dedup_key {
            Ok(self.deduplicate(all_results, dedup_fn))
        } else {
            Ok(all_results)
        }
    }

    fn deduplicate(&self, items: Vec<T>, key_fn: &dyn Fn(&T) -> String) -> Vec<T> {
        let mut seen = HashMap::new();
        let mut result = Vec::new();

        for item in items {
            let key = key_fn(&item);
            if let std::collections::hash_map::Entry::Vacant(e) = seen.entry(key) {
                e.insert(());
                result.push(item);
            }
        }

        result
    }
}

impl<T> Default for UnifiedQuery<T>
where
    T: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<T> Queryable<T> for UnifiedQuery<T>
where
    T: Send + Sync + 'static,
{
    async fn query<P>(&self, predicate: P) -> Result<Vec<T>>
    where
        P: Predicate<T> + Send + 'static,
    {
        let pred_arc: Arc<dyn Predicate<T>> = Arc::new(predicate);
        let mut all_results = Vec::new();

        for source in &self.sources {
            match source.query_erased(Arc::clone(&pred_arc)).await {
                Ok(mut results) => all_results.append(&mut results),
                Err(e) => {
                    eprintln!("Warning: Source query failed: {}", e);
                }
            }
        }

        if let Some(ref dedup_fn) = self.dedup_key {
            Ok(self.deduplicate(all_results, dedup_fn))
        } else {
            Ok(all_results)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::Predicate;

    #[derive(Clone, Debug, PartialEq)]
    struct TestTask {
        id: String,
        title: String,
        completed: bool,
    }

    struct MockSource {
        data: Vec<TestTask>,
    }

    #[async_trait]
    impl Queryable<TestTask> for MockSource {
        async fn query<P>(&self, predicate: P) -> Result<Vec<TestTask>>
        where
            P: Predicate<TestTask> + Send + 'static,
        {
            Ok(self
                .data
                .iter()
                .filter(|item| predicate.test(item))
                .cloned()
                .collect())
        }
    }

    #[derive(Clone)]
    struct CompletedPredicate;

    impl Predicate<TestTask> for CompletedPredicate {
        fn test(&self, item: &TestTask) -> bool {
            item.completed
        }

        fn to_sql(&self) -> Option<crate::core::traits::SqlPredicate> {
            None
        }
    }

    #[tokio::test]
    async fn test_unified_query_single_source() {
        let source = MockSource {
            data: vec![
                TestTask {
                    id: "1".to_string(),
                    title: "Task 1".to_string(),
                    completed: true,
                },
                TestTask {
                    id: "2".to_string(),
                    title: "Task 2".to_string(),
                    completed: false,
                },
            ],
        };

        let unified = UnifiedQuery::new().add_source(source);

        let results = unified.query(CompletedPredicate).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "1");
    }

    #[tokio::test]
    async fn test_unified_query_multiple_sources() {
        let source1 = MockSource {
            data: vec![TestTask {
                id: "1".to_string(),
                title: "Task 1".to_string(),
                completed: true,
            }],
        };

        let source2 = MockSource {
            data: vec![TestTask {
                id: "2".to_string(),
                title: "Task 2".to_string(),
                completed: true,
            }],
        };

        let unified = UnifiedQuery::new().add_source(source1).add_source(source2);

        let results = unified.query(CompletedPredicate).await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_unified_query_with_dedup() {
        let source1 = MockSource {
            data: vec![
                TestTask {
                    id: "1".to_string(),
                    title: "Task 1".to_string(),
                    completed: true,
                },
                TestTask {
                    id: "2".to_string(),
                    title: "Task 2".to_string(),
                    completed: true,
                },
            ],
        };

        let source2 = MockSource {
            data: vec![TestTask {
                id: "1".to_string(),
                title: "Task 1 (duplicate)".to_string(),
                completed: true,
            }],
        };

        let unified = UnifiedQuery::new()
            .add_source(source1)
            .add_source(source2)
            .with_dedup(|task| task.id.clone());

        let results = unified.query(CompletedPredicate).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "1");
        assert_eq!(results[1].id, "2");
    }

    #[tokio::test]
    async fn test_unified_query_as_queryable() {
        let source = MockSource {
            data: vec![TestTask {
                id: "1".to_string(),
                title: "Task 1".to_string(),
                completed: true,
            }],
        };

        let unified = UnifiedQuery::new().add_source(source);

        let results = unified.query(CompletedPredicate).await.unwrap();
        assert_eq!(results.len(), 1);
    }
}
