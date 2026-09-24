//! Source-aware pushdown as a DataFusion optimizer rule.
//! pushdown/optimizer_rule.rs
//! Splits conjunctive filters sitting above one of our table scans into the pushable part
//! (absorbed into the `TableScan`'s own filter list, honoring the provider's policy, denylist,
//! hints, statistics, and EXPLAIN cache) and the keep part (left in a `Filter` for Arrow).
//!
//! This is the plan-level twin of the provider path: [`PostgresTableProvider`] answers the
//! same question through `supports_filters_pushdown` during physical planning, and both funnel
//! through [`PostgresTableProvider::decide_cost`], so the rule can never push something the
//! provider would refuse. DataFusion's built-in filter pushdown would eventually do the same
//! split; this rule does it earlier, with source cost awareness, and its decisions are what
//! `rel plan --explain` reports.
//!
//! [`PostgresTableProvider`]: crate::connector::postgres::PostgresTableProvider

use std::any::Any;
use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, TableSource};
use datafusion::optimizer::OptimizerConfig;
use datafusion::optimizer::OptimizerRule;

use crate::connector::postgres::PostgresTableProvider;
use crate::pushdown::{self, Decision, Fidelity};

/// Optimizer rule that pushes source-pushable filter conjuncts into Postgres scans.
#[derive(Debug)]
pub struct SourceAwarePushdownRule;

impl SourceAwarePushdownRule {
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

    /// Make cost-based decisions for a set of filters against one provider.
    /// Returns (push_filters, keep_filters) where each is a Vec of expressions.
    pub fn decide_filters(
        provider: &PostgresTableProvider,
        filters: &[Expr],
    ) -> (Vec<Expr>, Vec<Expr>) {
        let mut push = Vec::new();
        let mut keep = Vec::new();
        for filter in filters {
            match provider.decide_cost(filter) {
                Decision::Push { .. } => push.push(filter.clone()),
                Decision::Keep => keep.push(filter.clone()),
            }
        }
        (push, keep)
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

    /// The [`PostgresTableProvider`] behind a scan's table source, if it is one of ours.
    /// Registered providers are wrapped in a [`DefaultTableSource`], hence the two-step
    /// downcast.
    fn postgres_provider(source: &Arc<dyn TableSource>) -> Option<&PostgresTableProvider> {
        let as_any = source.as_ref() as &dyn Any;
        let default = as_any.downcast_ref::<DefaultTableSource>()?;
        default
            .table_provider
            .downcast_ref::<PostgresTableProvider>()
    }
}

impl OptimizerRule for SourceAwarePushdownRule {
    fn name(&self) -> &str {
        "source_aware_pushdown"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };
        let LogicalPlan::TableScan(mut scan) = (*filter.input).clone() else {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        };
        let Some(provider) = Self::postgres_provider(&scan.source) else {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        };

        let conjuncts = Self::collect_filters(&filter.predicate);
        let (push, mut keep) = Self::decide_filters(provider, &conjuncts);

        // `Inexact` predicates are pushed for narrowing but must ALSO stay above the scan:
        // only Arrow re-checking makes their semantics exact. `Exact` predicates are fully
        // absorbed. This mirrors what DataFusion's own planner does with the
        // `Exact`/`Inexact` answers from `supports_filters_pushdown`.
        for expr in &push {
            if matches!(
                pushdown::translate(expr).map(|(fidelity, _)| fidelity),
                Some(Fidelity::Inexact)
            ) && !keep.contains(expr)
            {
                keep.push(expr.clone());
            }
        }

        // Only absorb conjuncts the scan doesn't already carry: the built-in pushdown rule
        // may have run first, and pushing twice would duplicate the predicate.
        let fresh_push: Vec<Expr> = push
            .into_iter()
            .filter(|expr| !scan.filters.contains(expr))
            .collect();
        if fresh_push.is_empty() {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }
        scan.filters.extend(fresh_push);

        match Self::reconstruct_filter(&keep) {
            Some(predicate) => {
                let input = Arc::new(LogicalPlan::TableScan(scan));
                Ok(Transformed::yes(LogicalPlan::Filter(
                    datafusion::logical_expr::Filter::try_new(predicate, input)?,
                )))
            }
            None => Ok(Transformed::yes(LogicalPlan::TableScan(scan))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::postgres::inline_sql::PredicateInlineSql;
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::optimizer::OptimizerContext;
    use datafusion::prelude::*;

    use crate::connector::postgres::distributed::connection::PostgresConnectionDescriptor;
    use crate::pushdown::PushdownPolicy;
    use crate::types::table_metadata::TableMetadata;

    fn test_provider(policy: PushdownPolicy) -> PostgresTableProvider {
        use crate::connector::postgres::table_provider::PostgresTableProviderModel;

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
            arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, true),
        ]));
        // Decoded-model providers carry no statistics, so `cost_based` degrades to the
        // optimistic legacy behavior here; the rule test pins the *splitting* mechanics,
        // while the stats-driven snapshots live in `pushdown::tests`.
        let model = PostgresTableProviderModel {
            descriptor: PostgresConnectionDescriptor {
                host: "localhost".to_string(),
                port: 5432,
                user: "postgres".to_string(),
                password_env: "UNUSED_TEST_ENV".to_string(),
                database: "db".to_string(),
                pool_max: 8,
                expected_workers: 1,
                statement_timeout_ms: 1000,
                application_name: "test".to_string(),
                schema: "public".to_string(),
            },
            table_metadata: TableMetadata {
                schema_name: "public".to_string(),
                table_name: "orders".to_string(),
                columns: vec![],
            },
            policy,
            deny: vec![],
            push: vec![],
            batch_size: 8192,
            use_copy: false,
            max_batch_bytes: 16 * 1024 * 1024,
            parallel_workers: 1,
            partition_column: None,
            strategy: crate::connector::postgres::parallel::ParallelStrategy::None,
            enum_columns: vec!["status".to_string()],
        };
        PostgresTableProvider::from_model(schema, model)
    }

    fn scan_plan(provider: PostgresTableProvider, predicate: Expr) -> LogicalPlan {
        use datafusion::logical_expr::Filter;

        // Built by hand, not via the builder: `LogicalPlanBuilder::filter` already applies
        // pushdown itself, which would leave no Filter node for the rule to rewrite.
        let source = provider_as_source(Arc::new(provider));
        let scan = LogicalPlanBuilder::scan("orders", source, None).expect("scan");
        let input = Arc::new(scan.build().expect("plan"));
        LogicalPlan::Filter(Filter::try_new(predicate, input).expect("filter"))
    }

    #[test]
    fn test_optimizer_creation() {
        let rule = SourceAwarePushdownRule;
        assert_eq!(rule.name(), "source_aware_pushdown");
    }

    #[test]
    fn test_collect_filters_single() {
        let expr = col("id").eq(lit(42i64));
        let filters = SourceAwarePushdownRule::collect_filters(&expr);
        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn test_collect_filters_conjunction() {
        let expr = col("id").eq(lit(42i64)).and(col("status").eq(lit("PAID")));
        let filters = SourceAwarePushdownRule::collect_filters(&expr);
        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn test_reconstruct_filter_single_returns_same_expr() {
        let expr = col("id").eq(lit(42i64));
        let reconstructed =
            SourceAwarePushdownRule::reconstruct_filter(std::slice::from_ref(&expr));
        assert_eq!(
            reconstructed,
            Some(expr),
            "a single filter must round-trip unchanged"
        );
    }

    #[test]
    fn test_reconstruct_filter_empty_is_none() {
        // No kept conjuncts: the caller (rewrite) uses this to decide whether to emit a
        // Filter node at all above the rewritten TableScan.
        assert_eq!(SourceAwarePushdownRule::reconstruct_filter(&[]), None);
    }

    #[test]
    fn test_reconstruct_filter_multiple_ands_left_to_right_in_order() {
        let a = col("id").eq(lit(1i64));
        let b = col("status").eq(lit("PAID"));
        let c = col("amount").gt(lit(0i64));
        let reconstructed =
            SourceAwarePushdownRule::reconstruct_filter(&[a.clone(), b.clone(), c.clone()])
                .expect("three filters must reconstruct to Some");
        // (a AND b) AND c -- left-associative, in input order.
        assert_eq!(reconstructed, a.and(b).and(c));
    }

    #[test]
    fn test_rewrite_pushes_translatable_conjunct() {
        let rule = SourceAwarePushdownRule;
        let config = OptimizerContext::new();

        // `id = 42` translates (Exact); `1 = 1`-style tautologies and anything outside the
        // allowlist stays above the scan.
        let predicate = col("id").eq(lit(42i64)).and(col("status").eq(lit("PAID")));
        let plan = scan_plan(test_provider(PushdownPolicy::Always), predicate);

        let rewritten = rule.rewrite(plan, &config).expect("rewrite").data;
        match rewritten {
            LogicalPlan::Filter(filter) => {
                // status is Inexact: pushed into the scan AND kept above it for re-checking.
                assert_eq!(
                    filter.predicate,
                    col("status").eq(lit("PAID")),
                    "only the kept conjunct stays in Filter"
                );
                match filter.input.as_ref() {
                    LogicalPlan::TableScan(scan) => {
                        assert_eq!(scan.filters.len(), 2, "both conjuncts absorbed");
                    }
                    other => panic!("expected TableScan under Filter, got {other:?}"),
                }
            }
            other => panic!("expected Filter above scan, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_never_pushes_nothing() {
        let rule = SourceAwarePushdownRule;
        let config = OptimizerContext::new();

        let plan = scan_plan(
            test_provider(PushdownPolicy::Never),
            col("id").eq(lit(42i64)),
        );

        let transformed = rule.rewrite(plan, &config).expect("rewrite");
        assert!(
            !transformed.transformed,
            "policy=never must leave the plan untouched"
        );
    }

    #[test]
    fn test_enum_decisions_through_provider() {
        // The fixture declares `status` an enum column: text comparisons push as label
        // comparisons (Inexact, re-checked above the scan), integer comparisons keep.
        let provider = test_provider(PushdownPolicy::Always);

        match provider.decide_cost(&col("status").eq(lit("PAID"))) {
            crate::pushdown::Decision::Push {
                fidelity,
                predicate,
            } => {
                assert_eq!(fidelity, crate::pushdown::Fidelity::Inexact);
                assert_eq!(predicate.render_inline_pg(), "(\"status\"::text = 'PAID')");
            }
            crate::pushdown::Decision::Keep => {
                panic!("enum-vs-text should push as a label comparison")
            }
        }

        assert!(matches!(
            provider.decide_cost(&col("status").eq(lit(42i64))),
            crate::pushdown::Decision::Keep
        ));

        let reason = provider.explain_decision(&col("status").eq(lit(42i64)));
        assert!(
            reason.contains("no pushable form"),
            "keep reason should name the enum hazard, got: {reason}"
        );
    }
}
