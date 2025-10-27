//! Core traits for AST transformation

use anyhow::Result;
use prqlc::ir::rq::RelationalQuery;
use prqlc::pr::ModuleDef;

/// Phase ordering for semantic sequencing of AST transformations.
///
/// Transformers are executed in phase order:
/// 1. All `Pl` transformers (sorted by priority)
/// 2. PL → RQ conversion
/// 3. All `Rq` transformers (sorted by priority)
///
/// Within each phase, lower priority values run first.
///
/// # Example
///
/// ```rust,ignore
/// // Runs early in PL phase
/// TransformPhase::Pl(-10)
///
/// // Runs at default priority in RQ phase
/// TransformPhase::Rq(0)
///
/// // Runs late in RQ phase (e.g., metadata columns)
/// TransformPhase::Rq(100)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransformPhase {
    /// Transformation on PL (Pipeline Language) AST, before RQ conversion.
    /// The i32 is the priority within the PL phase (lower runs first).
    Pl(i32),

    /// Transformation on RQ (Relational Query) AST, after PL→RQ conversion.
    /// The i32 is the priority within the RQ phase (lower runs first).
    Rq(i32),
}

impl TransformPhase {
    /// Check if this phase operates on PL (Pipeline Language) AST
    pub fn is_pl(&self) -> bool {
        matches!(self, TransformPhase::Pl(_))
    }

    /// Check if this phase operates on RQ (Relational Query) AST
    pub fn is_rq(&self) -> bool {
        matches!(self, TransformPhase::Rq(_))
    }

    /// Get the priority value within the phase
    pub fn priority(&self) -> i32 {
        match self {
            TransformPhase::Pl(p) | TransformPhase::Rq(p) => *p,
        }
    }
}

impl Ord for TransformPhase {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            // PL always comes before RQ
            (TransformPhase::Pl(_), TransformPhase::Rq(_)) => std::cmp::Ordering::Less,
            (TransformPhase::Rq(_), TransformPhase::Pl(_)) => std::cmp::Ordering::Greater,
            // Within same phase, compare by priority
            (TransformPhase::Pl(a), TransformPhase::Pl(b)) => a.cmp(b),
            (TransformPhase::Rq(a), TransformPhase::Rq(b)) => a.cmp(b),
        }
    }
}

impl PartialOrd for TransformPhase {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A transformer that modifies PRQL AST at a specific compilation phase.
///
/// Implement this trait to create custom AST modifications. The transformer
/// will be called at the appropriate phase during query compilation.
///
/// # Example
///
/// ```rust,ignore
/// struct MyTransformer;
///
/// impl AstTransformer for MyTransformer {
///     fn phase(&self) -> TransformPhase {
///         // Run late in RQ phase (after most other transformers)
///         TransformPhase::Rq(100)
///     }
///
///     fn transform_rq(&self, rq: RelationalQuery) -> Result<RelationalQuery> {
///         // Modify the RQ AST here
///         Ok(rq)
///     }
/// }
/// ```
pub trait AstTransformer: Send + Sync {
    /// The phase at which this transformer runs.
    /// The phase includes both the AST level (PL vs RQ) and priority.
    fn phase(&self) -> TransformPhase;

    /// Human-readable name for debugging and logging.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Transform the PL (Pipeline Language) AST.
    ///
    /// Override this for transformations at Pl phase.
    /// Default implementation returns the input unchanged.
    fn transform_pl(&self, pl: ModuleDef) -> Result<ModuleDef> {
        Ok(pl)
    }

    /// Transform the RQ (Relational Query) AST.
    ///
    /// Override this for transformations at Rq phase.
    /// Default implementation returns the input unchanged.
    fn transform_rq(&self, rq: RelationalQuery) -> Result<RelationalQuery> {
        Ok(rq)
    }
}
