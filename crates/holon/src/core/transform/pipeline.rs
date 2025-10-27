//! Transform Pipeline for orchestrating AST modifications

use std::sync::Arc;

use anyhow::Result;
use prqlc::ir::rq::RelationalQuery;
use prqlc::pr::ModuleDef;
use tracing::{debug, instrument};

use super::traits::AstTransformer;

/// Pipeline that orchestrates AST transformations in phase order.
///
/// The pipeline collects transformers from DI and applies them in the correct
/// order during query compilation:
/// 1. All Pl transformers (sorted by priority)
/// 2. PL → RQ conversion
/// 3. All Rq transformers (sorted by priority)
pub struct TransformPipeline {
    transformers: Vec<Arc<dyn AstTransformer>>,
}

impl TransformPipeline {
    /// Create a new pipeline with the given transformers.
    ///
    /// Transformers will be sorted by phase (Pl before Rq, then by priority).
    pub fn new(mut transformers: Vec<Arc<dyn AstTransformer>>) -> Self {
        transformers.sort_by_key(|t| t.phase());
        Self { transformers }
    }

    /// Create an empty pipeline with no transformers.
    pub fn empty() -> Self {
        Self {
            transformers: Vec::new(),
        }
    }

    /// Add a transformer to the pipeline.
    ///
    /// The pipeline will be re-sorted to maintain phase ordering.
    pub fn with_transformer(mut self, transformer: Arc<dyn AstTransformer>) -> Self {
        self.transformers.push(transformer);
        self.transformers.sort_by_key(|t| t.phase());
        self
    }

    /// Compile a PRQL query string with all transformations applied.
    ///
    /// This method:
    /// 1. Parses PRQL to PL AST
    /// 2. Applies all Pl transformers (sorted by priority)
    /// 3. Converts PL to RQ AST
    /// 4. Applies all Rq transformers (sorted by priority)
    /// 5. Generates SQL from the final RQ
    ///
    /// Returns the generated SQL and the final RQ AST.
    #[instrument(skip(self, prql_source), fields(source_len = prql_source.len()))]
    pub fn compile(&self, prql_source: &str) -> Result<(String, RelationalQuery)> {
        // Step 1: Parse PRQL to PL AST
        let pl = prqlc::prql_to_pl(prql_source)?;

        // Step 2: Apply PL transformations
        let pl = self.apply_pl_transforms(pl)?;

        // Step 3: Convert to RQ AST
        let rq = prqlc::pl_to_rq(pl)?;

        // Step 4: Apply RQ transformations
        let rq = self.apply_rq_transforms(rq)?;

        // Step 5: Generate SQL
        let sql = prqlc::rq_to_sql(rq.clone(), &prqlc::Options::default())?;

        Ok((sql, rq))
    }

    /// Compile a pre-parsed PL module with transformations.
    ///
    /// Use this when you've already parsed the PRQL and extracted render() etc.
    #[instrument(skip(self, pl))]
    pub fn compile_from_pl(&self, pl: ModuleDef) -> Result<(String, RelationalQuery)> {
        // Apply PL transformations
        let pl = self.apply_pl_transforms(pl)?;

        // Convert to RQ
        let rq = prqlc::pl_to_rq(pl)?;

        // Apply RQ transformations
        let rq = self.apply_rq_transforms(rq)?;

        // Generate SQL
        let sql = prqlc::rq_to_sql(rq.clone(), &prqlc::Options::default())?;

        Ok((sql, rq))
    }

    /// Apply only RQ transformations to a pre-converted RQ.
    ///
    /// Use this when the PL→RQ conversion has already happened elsewhere.
    #[instrument(skip(self, rq))]
    pub fn transform_rq(&self, rq: RelationalQuery) -> Result<RelationalQuery> {
        self.apply_rq_transforms(rq)
    }

    /// Apply Pl-phase transformers to a PL AST.
    ///
    /// Use this when you have a PL module that needs transformation before RQ conversion.
    /// This is called automatically by `compile()` and `compile_from_pl()`, but can also
    /// be called directly when using a split parsing workflow (e.g., with holon_prql_render).
    #[instrument(skip(self, pl))]
    pub fn apply_pl_transforms(&self, mut pl: ModuleDef) -> Result<ModuleDef> {
        for transformer in self.transformers.iter().filter(|t| t.phase().is_pl()) {
            debug!(
                transformer = transformer.name(),
                priority = transformer.phase().priority(),
                "Applying PL transformer"
            );
            pl = transformer.transform_pl(pl)?;
        }
        Ok(pl)
    }

    /// Apply Rq-phase transformers to an RQ AST.
    fn apply_rq_transforms(&self, mut rq: RelationalQuery) -> Result<RelationalQuery> {
        for transformer in self.transformers.iter().filter(|t| t.phase().is_rq()) {
            debug!(
                transformer = transformer.name(),
                priority = transformer.phase().priority(),
                "Applying RQ transformer"
            );
            rq = transformer.transform_rq(rq)?;
        }
        Ok(rq)
    }

    /// Get the number of registered transformers.
    pub fn transformer_count(&self) -> usize {
        self.transformers.len()
    }

    /// Check if the pipeline has any Pl-phase transformers.
    pub fn has_pl_transformers(&self) -> bool {
        self.transformers.iter().any(|t| t.phase().is_pl())
    }

    /// Check if the pipeline has any Rq-phase transformers.
    pub fn has_rq_transformers(&self) -> bool {
        self.transformers.iter().any(|t| t.phase().is_rq())
    }
}

impl Default for TransformPipeline {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::super::traits::TransformPhase;
    use super::*;

    struct NoOpTransformer {
        phase: TransformPhase,
        name: &'static str,
    }

    impl AstTransformer for NoOpTransformer {
        fn phase(&self) -> TransformPhase {
            self.phase
        }

        fn name(&self) -> &'static str {
            self.name
        }
    }

    #[test]
    fn test_transformer_ordering() {
        let transformers: Vec<Arc<dyn AstTransformer>> = vec![
            Arc::new(NoOpTransformer {
                phase: TransformPhase::Rq(100),
                name: "rq_late",
            }),
            Arc::new(NoOpTransformer {
                phase: TransformPhase::Pl(0),
                name: "pl_default",
            }),
            Arc::new(NoOpTransformer {
                phase: TransformPhase::Rq(0),
                name: "rq_default",
            }),
            Arc::new(NoOpTransformer {
                phase: TransformPhase::Rq(-10),
                name: "rq_early",
            }),
        ];

        let pipeline = TransformPipeline::new(transformers);

        let names: Vec<_> = pipeline.transformers.iter().map(|t| t.name()).collect();

        // PL comes first, then RQ sorted by priority
        assert_eq!(
            names,
            vec!["pl_default", "rq_early", "rq_default", "rq_late"]
        );
    }

    #[test]
    fn test_compile_simple_query() {
        let pipeline = TransformPipeline::empty();
        let result = pipeline.compile("from tasks | select {id, content}");

        assert!(result.is_ok());
        let (sql, _rq) = result.unwrap();
        assert!(sql.to_lowercase().contains("select"));
        assert!(sql.to_lowercase().contains("from"));
    }
}
