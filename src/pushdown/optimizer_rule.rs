//! Custom DataFusion optimizer rule for source-aware pushdown.
//! pushdown/optimizer_rule.rs
//! Implements plan-level filter optimization that considers multiple filters simultaneously
//! and makes combined cost-based decisions on which filters to push to the source.

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use crate::pushdown::stats::SourceStatistics;
use crate::pushdown::cost_model::{CostParams, IndexInfo};

/// Utility for making cost-based filter decisions at the plan level.
/// This is a helper for Phase 2.5+ full optimizer rule implementation.
#[derive(Debug)]
pub struct SourceAwarePushdown;

impl SourceAwarePushdown {
    /// Collect all filter predicates from a conjunction tree (AND-based filters).
    /// Returns a vector of (expr, fidelity, predicate) tuples.
    pub fn collect_filters(expr: &Expr) -> Vec<Expr> {
        let mut filters = Vec::new();
        Self::collect_filters_recursive(expr, &mut filters);
        filters
    }

    fn collect_filters_recursive(expr: &Expr, filters: &mut Vec<Expr>) {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                // Recurse into both sides of AND
                Self::collect_filters_recursive(&binary.left, filters);
                Self::collect_filters_recursive(&binary.right, filters);
            }
            _ => {
                // Collect this filter
                filters.push(expr.clone());
            }
        }
    }

    /// Make cost-based decisions for a set of filters given statistics.
    /// Returns (push_filters, keep_filters) where each is a Vec of expressions.
    pub fn decide_filters(
        filters: &[Expr],
        _stats: &SourceStatistics,
        _params: &CostParams,
        _indexes: &[IndexInfo],
    ) -> (Vec<Expr>, Vec<Expr>) {
        // Placeholder for phase 2.5+
        // For now, keep all filters
        (Vec::new(), filters.to_vec())
    }

    /// Reconstruct a filter expression from a list of conjunctions.
    pub fn reconstruct_filter(filters: &[Expr]) -> Option<Expr> {
        if filters.is_empty() {
            return None;
        }

        if filters.len() == 1 {
            return Some(filters[0].clone());
        }

        // Build: filter[0] AND filter[1] AND ... AND filter[n]
        let mut result = filters[0].clone();
        for filter in &filters[1..] {
            result = Expr::BinaryExpr(BinaryExpr {
                left: Box::new(result),
                op: Operator::And,
                right: Box::new(filter.clone()),
            });
        }

        Some(result)
    }
}

/// Reference implementation for phase 2.5+ full DataFusion OptimizerRule.
/// This is pseudocode showing how to integrate with DataFusion's optimizer.
///
/// ```ignore
/// use datafusion::optimizer::OptimizerRule;
/// use datafusion::error::Result as DataFusionResult;
/// use datafusion::logical_expr::LogicalPlan;
/// use datafusion::optimizer::optimizer::OptimizerConfig;
///
/// #[derive(Debug)]
/// pub struct SourceAwarePushdownRule;
///
/// impl OptimizerRule for SourceAwarePushdownRule {
///     fn name(&self) -> &str {
///         "source_aware_pushdown"
///     }
///
///     fn try_optimize(
///         &self,
///         plan: &LogicalPlan,
///         _config: &dyn OptimizerConfig,
///     ) -> DataFusionResult<Option<LogicalPlan>> {
///         // 1. Walk the plan looking for Filter → TableScan pairs
///         // 2. If TableScan is a PostgresTableProvider:
///         //    a. Extract all filters from the Filter node
///         //    b. Fetch table statistics
///         //    c. Call SourceAwarePushdown::decide_filters()
///         //    d. Rewrite TableScan with pushed filters
///         //    e. Keep remaining filters above the scan
///         // 3. Return modified plan
///         Ok(None)
///     }
/// }
/// ```

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[test]
    fn test_optimizer_creation() {
        let _rule = SourceAwarePushdown;
        assert!(true);
    }

    #[test]
    fn test_collect_filters_single() {
        let expr = col("id").eq(lit(42i64));
        let filters = SourceAwarePushdown::collect_filters(&expr);
        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn test_collect_filters_conjunction() {
        let expr = col("id")
            .eq(lit(42i64))
            .and(col("status").eq(lit("PAID")));
        let filters = SourceAwarePushdown::collect_filters(&expr);
        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn test_reconstruct_filter_single() {
        let expr = col("id").eq(lit(42i64));
        let filters = vec![expr];
        let reconstructed = SourceAwarePushdown::reconstruct_filter(&filters);
        assert!(reconstructed.is_some());
    }
}
