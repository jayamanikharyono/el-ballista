use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::Schema;
use chrono::{DateTime, Utc};
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
use crate::distributed::pool_registry::{registry, SourcePool};
use crate::pushdown::cost_model::CostParams;
use crate::pushdown::explain::ExplainEstimator;
use crate::pushdown::stats::{IndexInfo, SourceStatistics, StatisticsCollector, TableStatsSource};
use crate::pushdown::{self, CostInputs, Decision, PushdownPolicy};
use crate::types::TableMetadata;

use super::{
    row_adapter::PostgresRowAdapter,
    schema_reader::PostgresSchemaReader,
};

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
    pub watermark_column: Option<String>,
    pub window: Option<(DateTime<Utc>, DateTime<Utc>)>,
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
    watermark_column: Option<String>,
    window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    batch_size: usize,
    parallel_workers: usize,
    partition_column: Option<String>,
    strategy: ParallelStrategy,
}

impl PostgresTableProvider {
    /// Discovers the table schema (through the process-shared, budgeted pool), fetches cost
    /// statistics and index metadata (best-effort: missing stats only make `cost_based`
    /// conservative, never wrong), and builds a provider ready for local or distributed use.
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

        let table_schema =
            PostgresRowAdapter::build_arrow_schema(&table_metadata)?;

        let collector =
            StatisticsCollector::new(Arc::new(pool.clone()), statistics_ttl_secs);
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
            watermark_column: None,
            window: None,
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
            watermark_column: model.watermark_column,
            window: model.window,
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
            watermark_column: self.watermark_column.clone(),
            window: self.window,
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
    pub fn with_parallel_workers(mut self, workers: usize, partition_column: Option<String>) -> Self {
        self.parallel_workers = workers.max(1);
        self.partition_column = partition_column;
        self
    }

    #[allow(dead_code)]
    pub fn with_watermark(
        mut self,
        column: impl Into<String>,
        lo: DateTime<Utc>,
        hi: DateTime<Utc>,
    ) -> Self {
        self.watermark_column = Some(column.into());
        self.window = Some((lo, hi));
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

    fn cost_inputs_with_explain(
        &self,
        predicate_sql: &str,
    ) -> Option<CostInputs<'_>> {
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
        let Some((fidelity, predicate)) = pushdown::normalize_enum_comparison(
            fidelity,
            predicate,
            &self.enum_columns,
        ) else {
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
            Decision::Push { fidelity, predicate } => {
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
            PushdownPolicy::Strict => match self.cost_inputs_with_explain(&predicate.render_inline()) {
                Some(inputs) => pushdown::describe_strict(predicate, Some(&inputs)),
                None => pushdown::describe_strict(predicate, None),
            },
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
            let Some((_, predicate)) = pushdown::translate(expr) else {
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
                Decision::Push { fidelity: pushdown::Fidelity::Exact, .. } => {
                    TableProviderFilterPushDown::Exact
                }
                Decision::Push { fidelity: pushdown::Fidelity::Inexact, .. } => {
                    TableProviderFilterPushDown::Inexact
                }
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
        // marked Exact or Inexact, so every received filter is pushed unconditionally — the
        // decision lives in exactly one place (`decide_cost`), and `scan` can never contradict
        // what planning promised. (For an `Exact` filter DataFusion drops its own check, so a
        // second independent judgment here would risk wrong results, not just slow ones.)
        let mut pushed = Vec::new();
        let mut any_inexact = false;

        for f in filters {
            let Some((fidelity, predicate)) = pushdown::translate(f) else {
                return Err(datafusion::error::DataFusionError::Internal(
                    "scan received a filter the planner never marked pushable".to_string(),
                ));
            };
            if fidelity == pushdown::Fidelity::Inexact {
                any_inexact = true;
            }
            pushed.push(predicate);
        }

        // docs/pushdown.md §5 — never push LIMIT alongside an Inexact filter: a source-side
        // LIMIT could leave fewer than `limit` valid rows once DataFusion re-checks the filter.
        // Under `strict`, LIMIT is never pushed at all (filters only).
        let pushed_limit = if self.policy == PushdownPolicy::Strict {
            None
        } else if any_inexact {
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
                        let bounds = crate::connector::postgres::parallel::compute_keyset_partitions(
                            &pool,
                            &self.table_metadata.schema_name,
                            &self.table_metadata.table_name,
                            partition_column,
                            self.parallel_workers,
                        )
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

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
            self.watermark_column.clone(),
            self.window,
            self.batch_size,
            partitions,
        )?;

        Ok(Arc::new(plan))
    }
}