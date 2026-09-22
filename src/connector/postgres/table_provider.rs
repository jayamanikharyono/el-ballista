use std::sync::Arc;

use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::{
    datasource::TableProvider,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};
use serde::{Deserialize, Serialize};

use crate::connector::errors::ExtractorError;
use crate::connector::postgres::execution_plan::PostgresExecutionPlan;
use crate::connector::postgres::parallel::ParallelStrategy;
use crate::distributed::connection::PostgresConnectionDescriptor;
use crate::distributed::pool_registry::{SourcePool, registry};
use crate::pushdown::cost_model::CostParams;
use crate::pushdown::explain::ExplainEstimator;
use crate::pushdown::stats::{IndexInfo, SourceStatistics, StatisticsCollector, TableStatsSource};
use crate::pushdown::{self, CostInputs, Decision, PushdownPolicy};
use crate::types::TableMetadata;

use super::{row_adapter::PostgresRowAdapter, schema_reader::PostgresSchemaReader};

/// The serializable form of a `PostgresTableProvider` — what the codec embeds in the logical
/// plan the `rel distribute` client sends to the scheduler (see `distributed::table_codec`).
/// The scheduler and every executor rebuild the provider from this model; only the descriptor is
/// carried, never a live pool or a password.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresTableProviderModel {
    pub descriptor: PostgresConnectionDescriptor,
    pub table_metadata: TableMetadata,
    pub policy: PushdownPolicy,
    pub deny: Vec<String>,
    #[serde(default)]
    pub push: Vec<String>,
    pub batch_size: usize,
    pub parallel_workers: usize,
    pub partition_column: Option<String>,
    #[serde(default)]
    pub strategy: ParallelStrategy,
    /// True-enum columns (from `pg_enum`) for enum normalization. Defaults empty for old
    /// payloads, which then keep enum comparisons in Arrow instead of pushing them.
    #[serde(default)]
    pub enum_columns: Vec<String>,
}

#[derive(Debug)]
pub struct PostgresTableProvider {
    pool: SourcePool,
    /// Process-independent description of the source. `Some` on every provider built from
    /// config or decoded from a plan; carries the connection budget this process may use.
    descriptor: PostgresConnectionDescriptor,
    table_name: String,
    schema: Arc<Schema>,
    table_metadata: TableMetadata,
    policy: PushdownPolicy,
    deny: Vec<String>,
    push: Vec<String>,
    params: CostParams,
    /// Statistics and index metadata cached at construction, so the sync planning path
    /// (`supports_filters_pushdown`) can make cost-based decisions without touching the
    /// source. Providers rebuilt from serialized plans carry empty statistics and decide
    /// optimistically (see `cost_enabled`).
    stats: SourceStatistics,
    indexes: Vec<IndexInfo>,
    /// Column name → `information_schema` data type, for the `strict` primitive-type gate.
    /// Rebuilt from the model metadata on decode, so it travels with the plan.
    column_types: std::collections::HashMap<String, String>,
    /// Columns whose type is a true Postgres enum (from `pg_enum`), for enum normalization.
    /// Only these get the `::text` label comparison; `citext` and other UDTs keep native
    /// semantics. Travels in the model so scheduler-side scans normalize identically.
    enum_columns: std::collections::HashSet<String>,
    estimator: Option<ExplainEstimator>,
    /// Whether cost-based decisions use cached statistics. `false` for providers rebuilt
    /// from a serialized plan on a scheduler, which must not open source connections:
    /// they push everything translatable, preserving Phase 4 behavior.
    cost_enabled: bool,
    batch_size: usize,
    parallel_workers: usize,
    partition_column: Option<String>,
    strategy: ParallelStrategy,
}

impl PostgresTableProvider {
    /// Discovers the table schema (through the process-shared, budgeted pool), fetches cost
    /// statistics and index metadata (best-effort: missing stats only make `cost_based`
    /// conservative, never wrong), and builds a provider ready for local or distributed use.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        descriptor: PostgresConnectionDescriptor,
        table_name: &str,
        policy: PushdownPolicy,
        deny: Vec<String>,
        push: Vec<String>,
        params: CostParams,
        statistics_ttl_secs: u64,
        batch_size: usize,
    ) -> Result<Self, ExtractorError> {
        let pool = registry().pool(&descriptor)?;

        let postgres_schema_reader = PostgresSchemaReader::new(&pool);

        let table_metadata = postgres_schema_reader
            .get_table_metadata(table_name)
            .await?;

        let table_schema = PostgresRowAdapter::build_arrow_schema(&table_metadata)?;

        let collector = StatisticsCollector::new(Arc::new(pool.clone()), statistics_ttl_secs);
        let stats = collector
            .get_statistics(&table_metadata.schema_name, &table_metadata.table_name)
            .await
            .unwrap_or_else(|e| {
                log::warn!(
                    "cost statistics unavailable for {table_name} ({e}); \
                     cost_based decisions will keep non-indexed filters"
                );
                SourceStatistics::empty(table_name)
            });
        let indexes = pool
            .table_indexes(&table_metadata.schema_name, &table_metadata.table_name)
            .await
            .unwrap_or_else(|e| {
                log::warn!("index metadata unavailable for {table_name} ({e})");
                Vec::new()
            });

        let estimator = ExplainEstimator::new(Arc::new(pool.clone()), statistics_ttl_secs);

        let column_types = table_metadata
            .columns
            .iter()
            .map(|c| (c.column_name.clone(), c.data_type.clone()))
            .collect::<std::collections::HashMap<_, _>>();

        let enum_columns = pool
            .table_enum_columns(&table_metadata.schema_name, &table_metadata.table_name)
            .await
            .unwrap_or_else(|e| {
                log::warn!("enum metadata unavailable for {table_name} ({e})");
                std::collections::HashSet::new()
            });

        Ok(Self {
            pool: SourcePool::connected(pool),
            descriptor,
            table_name: table_name.to_string(),
            table_metadata,
            schema: table_schema,
            policy,
            deny,
            push,
            params,
            stats,
            indexes,
            column_types,
            enum_columns,
            estimator: Some(estimator),
            cost_enabled: true,
            batch_size,
            parallel_workers: 1,
            partition_column: None,
            strategy: ParallelStrategy::None,
        })
    }

    /// Reconstructs a provider from a serialized model — the decoder path on the scheduler
    /// (which plans, and computes partition bounds, but must not open connections) and on each
    /// executor (which resolves the shared pool on first scan).
    pub fn from_model(schema: Arc<Schema>, model: PostgresTableProviderModel) -> Self {
        let table_name = format!(
            "{}.{}",
            model.table_metadata.schema_name, model.table_metadata.table_name
        );
        let empty_stats = SourceStatistics::empty(&table_name);
        let column_types = model
            .table_metadata
            .columns
            .iter()
            .map(|c| (c.column_name.clone(), c.data_type.clone()))
            .collect::<std::collections::HashMap<_, _>>();

        Self {
            pool: SourcePool::deferred(model.descriptor.clone()),
            descriptor: model.descriptor,
            table_name,
            schema,
            table_metadata: model.table_metadata,
            policy: model.policy,
            deny: model.deny,
            push: model.push,
            params: CostParams::default(),
            stats: empty_stats,
            indexes: Vec::new(),
            column_types,
            enum_columns: model.enum_columns.into_iter().collect(),
            estimator: None,
            cost_enabled: false,
            batch_size: model.batch_size,
            parallel_workers: model.parallel_workers,
            partition_column: model.partition_column,
            strategy: model.strategy,
        }
    }

    pub fn to_model(&self) -> PostgresTableProviderModel {
        PostgresTableProviderModel {
            descriptor: self.descriptor.clone(),
            table_metadata: self.table_metadata.clone(),
            policy: self.policy,
            deny: self.deny.clone(),
            push: self.push.clone(),
            batch_size: self.batch_size,
            parallel_workers: self.parallel_workers,
            partition_column: self.partition_column.clone(),
            strategy: self.strategy,
            enum_columns: self.enum_columns.iter().cloned().collect(),
        }
    }

    #[allow(dead_code)]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Phase 4: paper over `bytesize`... this sets how many Ballista scan tasks (one per key
    /// range) this provider's `scan()` produces, and therefore how many executor processes share
    /// the source connection budget.
    pub fn with_parallel_workers(
        mut self,
        workers: usize,
        partition_column: Option<String>,
    ) -> Self {
        self.parallel_workers = workers.max(1);
        self.partition_column = partition_column;
        self
    }

    /// Parallel partition strategy for `scan()`: `Keyset` splits on `partition_column`
    /// (set via [`Self::with_parallel_workers`]), `Ctid` splits by physical pages and needs
    /// no column, `None` scans unsplit. Exported snapshots for cross-connection consistency
    /// are deferred — see `parallel.rs` — so prefer keyset for hot tables.
    pub fn with_parallel_strategy(mut self, strategy: ParallelStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    fn cost_inputs_with_explain(&self, predicate_sql: &str) -> Option<CostInputs<'_>> {
        if !self.cost_enabled {
            return None;
        }
        let explain = self.estimator.as_ref().and_then(|estimator| {
            estimator.cached_estimate(
                &self.table_metadata.table_name,
                &self.table_metadata.schema_name,
                predicate_sql,
            )
        });
        Some(CostInputs {
            stats: &self.stats,
            params: &self.params,
            indexes: &self.indexes,
            column_types: &self.column_types,
            explain,
        })
    }

    /// The single decision point for this provider. `supports_filters_pushdown`, `scan`
    /// (via the pushed set), the optimizer rule, and `rel plan --explain` all funnel through
    /// here, so they can never disagree.
    pub fn decide_cost(&self, expr: &Expr) -> Decision {
        // The EXPLAIN cache is read-only here: warming happens explicitly (`warm_explain`,
        // called by `rel plan` and opportunistically by `scan` for future queries), so the
        // sync planning path and the async execution path always decide over identical inputs.
        let Some((fidelity, predicate)) = pushdown::translate(expr) else {
            return Decision::Keep;
        };
        // Enum normalization first: without it an enum-vs-text predicate would report Exact
        // (or push unresolvable SQL) and fail at execution with 42883.
        let Some((fidelity, predicate)) =
            pushdown::normalize_enum_comparison(fidelity, predicate, &self.enum_columns)
        else {
            return Decision::Keep;
        };
        let predicate_sql = predicate.render_inline();
        match self.cost_inputs_with_explain(&predicate_sql) {
            Some(inputs) => pushdown::decide_translated(
                fidelity,
                predicate,
                self.policy,
                &self.deny,
                &self.push,
                Some(&inputs),
            ),
            None => pushdown::decide_translated(
                fidelity,
                predicate,
                self.policy,
                &self.deny,
                &self.push,
                None,
            ),
        }
    }

    /// Human-readable reason for a decision, for `rel plan --explain`. Matches once on the
    /// same verdict [`Self::decide_cost`] returns, so the explanation can never describe a
    /// different decision than the one taken.
    pub fn explain_decision(&self, expr: &Expr) -> String {
        match self.decide_cost(expr) {
            Decision::Push {
                fidelity,
                predicate,
            } => {
                format!(
                    "PUSH ({fidelity:?}; {}; {})",
                    self.push_reason(fidelity, &predicate),
                    predicate.render_inline()
                )
            }
            Decision::Keep => format!("KEEP ({})", self.keep_reason(expr)),
        }
    }

    fn push_reason(&self, fidelity: pushdown::Fidelity, predicate: &pushdown::Predicate) -> String {
        match self.policy {
            PushdownPolicy::Never => "unreachable: never keeps everything".to_string(),
            PushdownPolicy::Always => "policy=always".to_string(),
            PushdownPolicy::Strict => {
                match self.cost_inputs_with_explain(&predicate.render_inline()) {
                    Some(inputs) => pushdown::describe_strict(predicate, Some(&inputs)),
                    None => pushdown::describe_strict(predicate, None),
                }
            }
            PushdownPolicy::Hinted
                if pushdown::references_push_column_public(predicate, &self.push) =>
            {
                "hinted column".to_string()
            }
            PushdownPolicy::Hinted | PushdownPolicy::CostBased => {
                match self.cost_inputs_with_explain(&predicate.render_inline()) {
                    Some(inputs) => {
                        let estimated_cost = inputs
                            .explain
                            .as_ref()
                            .map(|est| est.total_cost.max(0.0) as u64);
                        match pushdown::cost_model::decide_push(
                            predicate,
                            fidelity,
                            inputs.stats,
                            inputs.params,
                            estimated_cost,
                            inputs.indexes,
                        ) {
                            pushdown::cost_model::CostDecision::Push { reason, .. } => reason,
                            pushdown::cost_model::CostDecision::Keep { reason, .. } => {
                                format!("cost model kept it after all ({reason})")
                            }
                        }
                    }
                    None => "no statistics reachable".to_string(),
                }
            }
        }
    }

    fn keep_reason(&self, expr: &Expr) -> String {
        if self.policy == PushdownPolicy::Never {
            return "policy=never".to_string();
        }
        let Some((fidelity, predicate)) = pushdown::translate(expr) else {
            return "unsupported expression; stays in Arrow".to_string();
        };
        if pushdown::references_denied_column_public(&predicate, &self.deny) {
            return "denylisted column".to_string();
        }
        // Mirror decide_cost: an enum compared with a non-text literal has no pushable
        // form, which reads differently from an ordinary cost-model keep.
        let Some((_, predicate)) =
            pushdown::normalize_enum_comparison(fidelity, predicate, &self.enum_columns)
        else {
            return "enum column compared with non-text literal: no pushable form".to_string();
        };
        if self.policy == PushdownPolicy::Strict {
            return match self.cost_inputs_with_explain(&predicate.render_inline()) {
                Some(inputs) => pushdown::describe_strict(&predicate, Some(&inputs)),
                None => pushdown::describe_strict(&predicate, None),
            };
        }
        match self.cost_inputs_with_explain(&predicate.render_inline()) {
            Some(inputs) => {
                let estimated_cost = inputs
                    .explain
                    .as_ref()
                    .map(|est| est.total_cost.max(0.0) as u64);
                match pushdown::cost_model::decide_push(
                    &predicate,
                    fidelity,
                    inputs.stats,
                    inputs.params,
                    estimated_cost,
                    inputs.indexes,
                ) {
                    pushdown::cost_model::CostDecision::Keep { reason, .. } => reason,
                    pushdown::cost_model::CostDecision::Push { reason, .. } => {
                        format!("kept despite model approval ({reason})")
                    }
                }
            }
            None => "no statistics reachable".to_string(),
        }
    }

    /// Warm the EXPLAIN estimate cache for a set of filter expressions (best-effort; failures
    /// only log). Call before planning when estimates should influence decisions
    /// (`rel plan` does this). `scan` also calls it — for *future* queries only; current
    /// decisions always use the pre-warm snapshot, keeping them identical to what the sync
    /// planning path promised.
    pub async fn warm_explain(&self, filters: &[Expr]) {
        let Some(estimator) = self.estimator.as_ref() else {
            return;
        };
        for expr in filters {
            let Some((fidelity, predicate)) = pushdown::translate(expr) else {
                continue;
            };
            // Warm the normalized shape — the cache is keyed by SQL text, and lookups use
            // the normalized form, so warming the raw form would never hit.
            let Some((_, predicate)) =
                pushdown::normalize_enum_comparison(fidelity, predicate, &self.enum_columns)
            else {
                continue;
            };
            let inline = predicate.render_inline();
            if let Err(e) = estimator
                .estimate_cost(
                    &self.table_metadata.table_name,
                    &self.table_metadata.schema_name,
                    &inline,
                )
                .await
            {
                log::debug!("EXPLAIN warm-up failed for {inline}: {e}");
            }
        }
    }
}

#[async_trait]
impl TableProvider for PostgresTableProvider {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// docs/pushdown.md §1/§2 — per-filter `Exact`/`Inexact`/`Unsupported` decisions, using
    /// [`Self::decide_cost`] so this can never disagree with what `scan` below actually
    /// pushes into the query, nor with the optimizer rule or `rel plan --explain`.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| match self.decide_cost(f) {
                Decision::Push {
                    fidelity: pushdown::Fidelity::Exact,
                    ..
                } => TableProviderFilterPushDown::Exact,
                Decision::Push {
                    fidelity: pushdown::Fidelity::Inexact,
                    ..
                } => TableProviderFilterPushDown::Inexact,
                Decision::Keep => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let projected_metadata = match projection {
            Some(indices) => self.table_metadata.select_indices(indices),
            None => self.table_metadata.clone(),
        };

        let projected_schema = PostgresRowAdapter::build_arrow_schema(&projected_metadata)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // DataFusion only ever passes filters here that `supports_filters_pushdown` already
        // marked Exact or Inexact. Every received filter goes back through `decide_cost` and
        // pushes the predicate *it* returns — translated and normalized (enum casts) exactly
        // as planning saw it. Re-translating here instead would silently drop the
        // normalization and push unresolvable SQL (e.g. `order_status = text` → 42883).
        // (For an `Exact` filter DataFusion drops its own check, so any divergence here
        // would risk wrong results, not just slow ones.)
        let mut pushed = Vec::new();
        let mut any_inexact = false;

        for f in filters {
            match self.decide_cost(f) {
                Decision::Push {
                    fidelity,
                    predicate,
                } => {
                    if fidelity == pushdown::Fidelity::Inexact {
                        any_inexact = true;
                    }
                    pushed.push(predicate);
                }
                Decision::Keep => {
                    return Err(datafusion::error::DataFusionError::Internal(
                        "scan received a filter the planner never marked pushable".to_string(),
                    ));
                }
            }
        }

        // docs/pushdown.md §5 — never push LIMIT alongside an Inexact filter: a source-side
        // LIMIT could leave fewer than `limit` valid rows once DataFusion re-checks the filter.
        // Under `strict`, LIMIT is never pushed at all (filters only).
        let pushed_limit = if self.policy == PushdownPolicy::Strict || any_inexact {
            None
        } else {
            limit
        };

        // Partition bounds, computed here so they ship inside the serialized plan and every
        // executor scans only its own range. Keyset needs a partition column; ctid splits by
        // physical pages and needs none; anything else scans unsplit.
        let pool = self
            .pool
            .get()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let partitions = if self.parallel_workers > 1 {
            match self.strategy {
                ParallelStrategy::Keyset => match &self.partition_column {
                    Some(partition_column) => {
                        let bounds =
                            crate::connector::postgres::parallel::compute_keyset_partitions(
                                &pool,
                                &self.table_metadata.schema_name,
                                &self.table_metadata.table_name,
                                partition_column,
                                self.parallel_workers,
                            )
                            .await
                            .map_err(|e| {
                                datafusion::error::DataFusionError::External(Box::new(e))
                            })?;

                        // A single, bound-less partition (empty table, or min >= max) means
                        // "scan everything" — collapse it to one unsplit task so rows are
                        // never doubled.
                        if bounds.len() == 1 && bounds[0].predicate.is_none() {
                            Vec::new()
                        } else {
                            bounds
                        }
                    }
                    None => Vec::new(),
                },
                ParallelStrategy::Ctid => {
                    crate::connector::postgres::parallel::compute_ctid_partitions(
                        &pool,
                        &self.table_metadata.schema_name,
                        &self.table_metadata.table_name,
                        self.parallel_workers,
                    )
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                }
                ParallelStrategy::None => Vec::new(),
            }
        } else {
            Vec::new()
        };

        // Opportunistic EXPLAIN warming for *future* queries only: decisions above already used
        // the pre-warm cache snapshot, so warming here can never make this scan contradict what
        // the planner promised. Best-effort — failures only log.
        self.warm_explain(filters).await;

        let plan = PostgresExecutionPlan::try_new(
            Some(self.descriptor.clone()),
            projected_metadata,
            projected_schema,
            pushed,
            pushed_limit,
            self.batch_size,
            partitions,
            crate::connector::query_tag::fresh_run_id(),
        )?;

        Ok(Arc::new(plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::postgres::parallel::ParallelStrategy;
    use datafusion::prelude::{SessionContext, col, lit};

    /// A decode-side provider (no pool): `scan()` resolves the pool lazily, so with a
    /// dummy password env var this exercises the full filter→SQL path with zero network.
    /// Each test passes its OWN var name: lib tests run in parallel and env vars are
    /// process-global, so sharing one var between set/remove pairs races.
    fn test_provider(password_env: &str) -> PostgresTableProvider {
        use crate::types::TableMetadata;

        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "status",
            arrow::datatypes::DataType::Utf8,
            true,
        )]));
        let model = PostgresTableProviderModel {
            descriptor: PostgresConnectionDescriptor {
                host: "test-invalid-host".to_string(),
                port: 1,
                user: "test".to_string(),
                password_env: password_env.to_string(),
                database: "testdb".to_string(),
                pool_max: 1,
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
            policy: PushdownPolicy::CostBased,
            deny: vec![],
            push: vec![],
            batch_size: 8192,
            parallel_workers: 1,
            partition_column: None,
            strategy: ParallelStrategy::None,
            enum_columns: vec!["status".to_string()],
        };
        PostgresTableProvider::from_model(schema, model)
    }

    #[tokio::test]
    async fn test_scan_pushes_normalized_enum_predicate() {
        // Regression test for the 42883 that escaped `decide_cost`: `scan()` used to
        // re-translate raw Exprs and push `"status" = $1`, while planning had approved the
        // normalized `"status"::text = $1`. Now scan reuses the single decision point.
        unsafe {
            std::env::set_var("REL_TEST_DUMMY_PW", "dummy");
        }
        let provider = test_provider("REL_TEST_DUMMY_PW");
        let ctx = SessionContext::new();
        let state = ctx.state();

        let plan = provider
            .scan(&state, None, &[col("status").eq(lit("PAID"))], None)
            .await
            .unwrap();
        let plan = plan
            .downcast_ref::<PostgresExecutionPlan>()
            .expect("a PostgresExecutionPlan");
        let sql_str = plan.build_query(0).sql();
        let sql = sql_str.as_str();

        assert!(
            sql.contains(r#""status"::text = $1"#),
            "scan must push the normalized label comparison, got: {sql}"
        );

        unsafe {
            std::env::remove_var("REL_TEST_DUMMY_PW");
        }
    }

    #[tokio::test]
    async fn test_scan_empty_projection_for_count_star() {
        // `SELECT COUNT(*)` prunes the scan projection to zero columns: the plan
        // must carry the (empty) projected schema so the aggregate counts rows,
        // and the generated SQL must stay valid (`SELECT 1`, never `SELECT FROM`).
        unsafe {
            std::env::set_var("REL_TEST_DUMMY_PW2", "dummy");
        }
        let provider = test_provider("REL_TEST_DUMMY_PW2");
        let ctx = SessionContext::new();
        let state = ctx.state();

        let plan = provider
            .scan(&state, Some(&vec![]), &[], None)
            .await
            .unwrap();
        assert_eq!(
            plan.schema().fields().len(),
            0,
            "empty projection must reach the scan as a 0-field schema"
        );
        let plan = plan
            .downcast_ref::<PostgresExecutionPlan>()
            .expect("a PostgresExecutionPlan");
        let sql = plan.build_query(0).sql();
        assert!(
            sql.as_str().contains("SELECT 1 FROM"),
            "empty projection must select a constant, got: {}",
            sql.as_str()
        );

        unsafe {
            std::env::remove_var("REL_TEST_DUMMY_PW2");
        }
    }
}
