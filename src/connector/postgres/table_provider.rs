use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::error::DataFusionError;
use datafusion::{
    datasource::TableProvider,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::connector::errors::ExtractorError;
use crate::connector::postgres::dialect::column_kinds;
use crate::connector::postgres::distributed::connection::PostgresConnectionDescriptor;
use crate::connector::postgres::distributed::pool_registry::{SourcePool, registry};
use crate::connector::postgres::execution_plan::PostgresExecutionPlan;
use crate::connector::postgres::explain::ExplainEstimator;
use crate::connector::postgres::inline_sql::PredicateInlineSql;
use crate::connector::postgres::parallel::{ParallelStrategy, ScanPartition};
use crate::pushdown::cost_model::CostParams;
use crate::pushdown::stats::{IndexInfo, SourceStatistics, TableStatsSource};
use crate::pushdown::{
    self, ColumnKinds, CostInputs, Decision, Fidelity, Predicate, PushdownPolicy,
};
use crate::types::TableMetadata;

use super::{row_adapter::PostgresRowAdapter, schema_reader::PostgresSchemaReader};

/// Default byte cap per flushed Arrow batch (16 MiB).
fn default_provider_batch_bytes() -> usize {
    16 * 1024 * 1024
}

/// The serializable form of a `PostgresTableProvider` — what the codec embeds in the logical
/// plan the `el-ballista distribute` client sends to the scheduler (see `distributed::table_codec`).
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
    /// Byte cap per flushed Arrow batch. Defaults for old serialized plans.
    #[serde(default = "default_provider_batch_bytes")]
    pub max_batch_bytes: usize,
    /// Prefer binary COPY over cursor SELECT when the scan shape allows it.
    #[serde(default)]
    pub use_copy: bool,
    /// `statement_timeout` override for COPY scans (see `ExecutionConfig`).
    #[serde(default)]
    pub copy_statement_timeout_ms: Option<u64>,
    pub parallel_workers: usize,
    pub partition_column: Option<String>,
    #[serde(default)]
    pub strategy: ParallelStrategy,
    /// True-enum columns (from `pg_enum`), so a decoded provider classifies columns exactly
    /// like the one that planned. Defaults empty for old payloads (enum columns then classify
    /// as text-cast: only `=`/`<>` translate).
    #[serde(default)]
    pub enum_columns: Vec<String>,
    /// Whether the source's `server_encoding` is UTF8 (text ordering pushdown relies on it).
    /// Defaults to `false` — the conservative reading — for payloads that don't carry it.
    #[serde(default)]
    pub server_utf8: bool,
    /// Pre-computed scan partitions (see `PostgresTableProvider::with_fixed_partitions`).
    #[serde(default)]
    pub fixed_partitions: Option<Vec<ScanPartition>>,
    /// Run id every scan query of this run is tagged with (see `with_run_id`). `None` (and
    /// old payloads): each `scan()` generates its own.
    #[serde(default)]
    pub run_id: Option<String>,
}

/// Cost-model inputs cached by a provider. Swapped as a whole on refresh, so one decision
/// always reads a consistent snapshot.
#[derive(Debug)]
struct CostState {
    stats: SourceStatistics,
    indexes: Vec<IndexInfo>,
    fetched_at: Instant,
}

/// Lazy statistics refresh for providers built from config (never for decoded providers,
/// which must not open catalog connections on a scheduler).
#[derive(Debug)]
struct StatsRefresh {
    ttl: Duration,
    in_flight: AtomicBool,
}

/// Clears `in_flight` even if the refreshing future is dropped mid-way (cancel safety).
struct InFlightGuard<'a>(&'a AtomicBool);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[derive(Debug)]
pub struct PostgresTableProvider {
    pool: SourcePool,
    /// Process-independent description of the source; carries the connection budget this
    /// process may use.
    descriptor: PostgresConnectionDescriptor,
    table_name: String,
    schema: Arc<Schema>,
    table_metadata: TableMetadata,
    policy: PushdownPolicy,
    deny: Vec<String>,
    push: Vec<String>,
    params: CostParams,
    /// Statistics and index metadata, so the sync planning path (`supports_filters_pushdown`)
    /// can decide without touching the source. Empty on providers rebuilt from serialized
    /// plans: there the cost model keeps every filter the planning client did not already
    /// push (strict and cost_based never push blindly).
    cost_state: RwLock<Arc<CostState>>,
    /// `Some` when `cost_state` may be refreshed after its TTL (see [`Self::scan`]).
    refresh: Option<StatsRefresh>,
    /// Engine-neutral comparison kinds per column (from the catalog types and `pg_enum`).
    /// Rebuilt from the model on decode, so every process translates identically.
    column_kinds: ColumnKinds,
    /// Columns whose type is a true Postgres enum; kept to serialize into the model.
    enum_columns: HashSet<String>,
    /// `server_encoding = UTF8` (see [`column_kinds`]); kept to serialize into the model.
    server_utf8: bool,
    estimator: Option<ExplainEstimator>,
    batch_size: usize,
    max_batch_bytes: usize,
    use_copy: bool,
    copy_statement_timeout_ms: Option<u64>,
    parallel_workers: usize,
    partition_column: Option<String>,
    strategy: ParallelStrategy,
    /// When set, `scan()` uses exactly these partitions (one output partition each) instead
    /// of computing bounds from the live table — how a resumed job re-scans the splits its
    /// checkpoint stored.
    fixed_partitions: Option<Vec<ScanPartition>>,
    /// When set, every `scan()` tags its queries with this run id (the run report's), so a
    /// run's queries and its report share one id. `None`: a fresh id per `scan()`.
    run_id: Option<String>,
}

/// Best-effort statistics + index fetch. Missing statistics only make `cost_based`
/// conservative (keep), never wrong. `previous` is kept for any part that fails to refresh.
async fn fetch_cost_state(
    pool: &PgPool,
    table_metadata: &TableMetadata,
    previous: Option<&CostState>,
) -> CostState {
    let (schema, table) = (&table_metadata.schema_name, &table_metadata.table_name);
    let stats = match pool.table_statistics(schema, table).await {
        Ok(stats) => stats,
        Err(e) => {
            log::warn!(
                "cost statistics unavailable for {schema}.{table} ({e}); \
                 cost_based decisions will keep non-indexed filters"
            );
            previous
                .map(|p| p.stats.clone())
                .unwrap_or_else(|| SourceStatistics::empty(table))
        }
    };
    let indexes = match pool.table_indexes(schema, table).await {
        Ok(indexes) => indexes,
        Err(e) => {
            log::warn!("index metadata unavailable for {schema}.{table} ({e})");
            previous.map(|p| p.indexes.clone()).unwrap_or_default()
        }
    };
    CostState {
        stats,
        indexes,
        fetched_at: Instant::now(),
    }
}

/// `SHOW server_encoding = UTF8`? A failed lookup is treated as "not UTF8" (conservative:
/// text ordering comparisons then stay in Arrow).
async fn server_encoding_is_utf8(pool: &PgPool) -> bool {
    match sqlx::query_scalar::<_, String>("SHOW server_encoding")
        .fetch_one(pool)
        .await
    {
        Ok(enc) => enc.eq_ignore_ascii_case("UTF8"),
        Err(e) => {
            log::warn!("cannot read server_encoding ({e}); text ordering pushdown disabled");
            false
        }
    }
}

impl PostgresTableProvider {
    /// Discovers the table schema (through the process-shared, budgeted pool), fetches cost
    /// statistics and index metadata (best-effort: missing stats only make `cost_based`
    /// conservative, never wrong), and builds a provider ready for local or distributed use.
    /// Statistics are refreshed lazily once older than `statistics_ttl_secs`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::sync::Arc;
    /// use datafusion::prelude::SessionContext;
    /// use el_ballista::config::JobConfig;
    /// use el_ballista::connector::postgres::PostgresTableProvider;
    /// use el_ballista::connector::postgres::distributed::PostgresConnectionDescriptor;
    /// use el_ballista::pushdown::cost_model::CostParams;
    ///
    /// let cfg = JobConfig::from_file("job.json")?;
    /// let provider = PostgresTableProvider::new(
    ///     PostgresConnectionDescriptor::from_config(&cfg.source, 1),
    ///     &cfg.resolved_table(),
    ///     cfg.pushdown.policy, cfg.pushdown.deny.clone(), cfg.pushdown.push.clone(),
    ///     CostParams::default(), cfg.pushdown.statistics_ttl_secs, cfg.execution.batch_size,
    /// ).await?;
    /// let ctx = SessionContext::new();
    /// ctx.register_table("orders", Arc::new(provider))?;
    /// ctx.sql("SELECT id FROM orders WHERE status = 'PAID'").await?.show().await?;
    /// # Ok(()) }
    /// ```
    #[allow(clippy::too_many_arguments)] // mirrors the config sections one-to-one; builder later
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

        let table_metadata = PostgresSchemaReader::new(&pool)
            .get_table_metadata(table_name)
            .await?;

        let table_schema = PostgresRowAdapter::build_arrow_schema(&table_metadata)?;

        let cost_state = fetch_cost_state(&pool, &table_metadata, None).await;

        let estimator = ExplainEstimator::new(Arc::new(pool.clone()), statistics_ttl_secs);

        let enum_columns = pool
            .table_enum_columns(&table_metadata.schema_name, &table_metadata.table_name)
            .await
            .unwrap_or_else(|e| {
                // Without enum metadata, enum columns classify as text-cast: only `=`/`<>`
                // translate, through the label text, which is still exact.
                log::warn!("enum metadata unavailable for {table_name} ({e})");
                HashSet::new()
            });
        let server_utf8 = server_encoding_is_utf8(&pool).await;
        let column_kinds = column_kinds(&table_metadata, &enum_columns, server_utf8);

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
            cost_state: RwLock::new(Arc::new(cost_state)),
            refresh: Some(StatsRefresh {
                ttl: Duration::from_secs(statistics_ttl_secs),
                in_flight: AtomicBool::new(false),
            }),
            column_kinds,
            enum_columns,
            server_utf8,
            estimator: Some(estimator),
            batch_size,
            max_batch_bytes: default_provider_batch_bytes(),
            use_copy: false,
            copy_statement_timeout_ms: None,
            parallel_workers: 1,
            partition_column: None,
            strategy: ParallelStrategy::None,
            fixed_partitions: None,
            run_id: None,
        })
    }

    /// Reconstructs a provider from a serialized model — the decoder path on the scheduler
    /// (which plans, and computes partition bounds, but must not open catalog connections) and
    /// on each executor (which resolves the shared pool on first scan). Carries no statistics:
    /// filters the planning client already pushed arrive in `scan` and are pushed as-is; any
    /// filter offered anew is kept unless the policy is `always` or a `push` hint.
    pub(crate) fn from_model(schema: Arc<Schema>, model: PostgresTableProviderModel) -> Self {
        let table_name = format!(
            "{}.{}",
            model.table_metadata.schema_name, model.table_metadata.table_name
        );
        let enum_columns: HashSet<String> = model.enum_columns.into_iter().collect();
        let column_kinds = column_kinds(&model.table_metadata, &enum_columns, model.server_utf8);
        let cost_state = CostState {
            stats: SourceStatistics::empty(&table_name),
            indexes: Vec::new(),
            fetched_at: Instant::now(),
        };

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
            cost_state: RwLock::new(Arc::new(cost_state)),
            refresh: None,
            column_kinds,
            enum_columns,
            server_utf8: model.server_utf8,
            estimator: None,
            batch_size: model.batch_size,
            max_batch_bytes: model.max_batch_bytes.max(1),
            use_copy: model.use_copy,
            copy_statement_timeout_ms: model.copy_statement_timeout_ms,
            parallel_workers: model.parallel_workers,
            partition_column: model.partition_column,
            strategy: model.strategy,
            fixed_partitions: model.fixed_partitions,
            run_id: model.run_id,
        }
    }

    pub(crate) fn to_model(&self) -> PostgresTableProviderModel {
        let mut enum_columns: Vec<String> = self.enum_columns.iter().cloned().collect();
        enum_columns.sort();
        PostgresTableProviderModel {
            descriptor: self.descriptor.clone(),
            table_metadata: self.table_metadata.clone(),
            policy: self.policy,
            deny: self.deny.clone(),
            push: self.push.clone(),
            batch_size: self.batch_size,
            max_batch_bytes: self.max_batch_bytes,
            use_copy: self.use_copy,
            copy_statement_timeout_ms: self.copy_statement_timeout_ms,
            parallel_workers: self.parallel_workers,
            partition_column: self.partition_column.clone(),
            strategy: self.strategy,
            enum_columns,
            server_utf8: self.server_utf8,
            fixed_partitions: self.fixed_partitions.clone(),
            run_id: self.run_id.clone(),
        }
    }

    /// The table this provider scans (unqualified).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use el_ballista::connector::postgres::PostgresTableProvider;
    /// # fn demo(provider: PostgresTableProvider) {
    /// println!("scanning {}", provider.table_name());
    /// # }
    /// ```
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Phase 4: this sets how many Ballista scan tasks (one per key range) this provider's
    /// `scan()` produces, and therefore how many executor processes share the source
    /// connection budget.
    #[must_use]
    pub(crate) fn with_parallel_workers(
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
    #[must_use]
    pub(crate) fn with_parallel_strategy(mut self, strategy: ParallelStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Override the per-batch byte cap (from `execution.max_batch_bytes`). Builder-style
    /// so `new()`'s signature stays stable.
    #[must_use]
    pub(crate) fn with_max_batch_bytes(mut self, max_batch_bytes: usize) -> Self {
        self.max_batch_bytes = max_batch_bytes.max(1);
        self
    }

    /// Prefer binary COPY over cursor SELECT when the scan shape allows it
    /// (from `execution.use_copy`). Builder-style, same stability rationale.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use el_ballista::connector::postgres::PostgresTableProvider;
    /// # fn demo(provider: PostgresTableProvider) {
    /// // Prefer binary COPY; scans whose shape needs a cursor still use one.
    /// let provider = provider.with_use_copy(true);
    /// # let _ = provider;
    /// # }
    /// ```
    #[must_use]
    pub fn with_use_copy(mut self, use_copy: bool) -> Self {
        self.use_copy = use_copy;
        self
    }

    /// `statement_timeout` (ms) for COPY scans instead of the session's (from
    /// `execution.copy_statement_timeout_ms`; `None` keeps the session timeout).
    #[must_use]
    pub(crate) fn with_copy_statement_timeout_ms(mut self, timeout_ms: Option<u64>) -> Self {
        self.copy_statement_timeout_ms = timeout_ms;
        self
    }

    /// Scan exactly these partitions, in order (output partition `i` scans `partitions[i]`),
    /// instead of computing bounds in `scan()`. Overrides the parallel strategy. A single
    /// partition without a predicate is one whole-table scan. Used by the split pipeline so
    /// a resumed job scans the bounds its checkpoint stored.
    #[must_use]
    pub(crate) fn with_fixed_partitions(mut self, partitions: Vec<ScanPartition>) -> Self {
        self.fixed_partitions = Some(partitions);
        self
    }

    /// Tag every query this provider's scans issue with `run_id` instead of a fresh id per
    /// scan — how a run's queries carry the same id as its run report.
    #[must_use]
    pub(crate) fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// The catalog metadata of the whole table (every column), as discovered.
    pub(crate) fn table_metadata(&self) -> &TableMetadata {
        &self.table_metadata
    }

    /// The source pool this provider scans through (the process-shared, budgeted pool).
    pub(crate) fn source_pool(&self) -> Result<PgPool, ExtractorError> {
        self.pool.get()
    }

    /// The connection descriptor (carries the per-process connection budget).
    pub(crate) fn descriptor(&self) -> &PostgresConnectionDescriptor {
        &self.descriptor
    }

    fn cost_snapshot(&self) -> Arc<CostState> {
        // The lock only guards an `Arc` swap, so a poisoned lock still holds a whole snapshot.
        match self.cost_state.read() {
            Ok(guard) => Arc::clone(&guard),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        }
    }

    /// Translate one DataFusion filter into the pushed predicate, using this table's column
    /// kinds. `None` = no source form that is exact or a superset of Arrow's answer.
    fn translate_filter(&self, expr: &Expr) -> Option<(Fidelity, Predicate)> {
        pushdown::translate_with(expr, &self.column_kinds)
    }

    /// Policy decision plus its human-readable reason, over the current cost snapshot and any
    /// cached EXPLAIN estimate. The EXPLAIN cache is read-only here: warming happens explicitly
    /// (`warm_explain`), and `scan` never re-decides, so a warm-up can only affect later plans.
    fn decide_explained(&self, expr: &Expr) -> (Decision, String) {
        self.decide_explained_among(expr, &[])
    }

    /// [`Self::decide_explained`] for one filter of a set: `siblings` are the other filters'
    /// translations (all ANDed with this one), so a range filter is estimated together with
    /// the ranges on the same column — see [`CostInputs::siblings`].
    fn decide_explained_among(&self, expr: &Expr, siblings: &[Predicate]) -> (Decision, String) {
        let Some((fidelity, predicate)) = self.translate_filter(expr) else {
            return (
                Decision::Keep,
                "no exact or superset source form (expression, type, or operator not \
                 supported); stays in Arrow"
                    .to_string(),
            );
        };
        let snapshot = self.cost_snapshot();
        let explain = self.estimator.as_ref().and_then(|estimator| {
            estimator.cached_estimate(
                &self.table_metadata.table_name,
                &self.table_metadata.schema_name,
                &predicate.render_inline(),
            )
        });
        let inputs = CostInputs {
            stats: &snapshot.stats,
            params: &self.params,
            indexes: &snapshot.indexes,
            explain,
            column_kinds: &self.column_kinds,
            siblings,
        };
        pushdown::decide_explained(
            fidelity,
            predicate,
            self.policy,
            &self.deny,
            &self.push,
            &inputs,
        )
    }

    /// The policy decision for one filter judged **alone** (translation fidelity × policy × cost
    /// model). `supports_filters_pushdown` and `el-ballista plan` decide the whole filter set instead
    /// (see [`Self::explain_decisions`]): a range filter is estimated together with the other
    /// ranges on its column there, so a lone half of a window can be kept here yet pushed as
    /// part of the set. (`scan` does not re-decide — see there.)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use el_ballista::connector::postgres::PostgresTableProvider;
    /// # fn demo(provider: PostgresTableProvider) {
    /// use datafusion::prelude::{col, lit};
    /// use el_ballista::pushdown::Decision;
    ///
    /// match provider.decide_cost(&col("status").eq(lit("PAID"))) {
    ///     Decision::Push { fidelity, .. } => println!("pushed ({fidelity:?})"),
    ///     _ => println!("kept in Arrow"),
    /// }
    /// # }
    /// ```
    pub fn decide_cost(&self, expr: &Expr) -> Decision {
        self.decide_explained(expr).0
    }

    /// Human-readable decision for one filter judged alone, computed in the same call as
    /// [`Self::decide_cost`]'s verdict. For filters that run together, use
    /// [`Self::explain_decisions`], which matches what `supports_filters_pushdown` decides.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use el_ballista::connector::postgres::PostgresTableProvider;
    /// # fn demo(provider: PostgresTableProvider) {
    /// use datafusion::prelude::{col, lit};
    ///
    /// // e.g. "PUSH (Exact; ...; status = 'PAID')" or "KEEP (...)"
    /// println!("{}", provider.explain_decision(&col("status").eq(lit("PAID"))));
    /// # }
    /// ```
    pub fn explain_decision(&self, expr: &Expr) -> String {
        describe_decision(self.decide_explained(expr))
    }

    /// Decisions for a whole filter set, as DataFusion hands it to
    /// [`TableProvider::supports_filters_pushdown`]: each filter is decided with the others
    /// as siblings (see [`CostInputs::siblings`]), so the two sides of a range window share one
    /// estimate. Same order as `filters`.
    fn decide_all(&self, filters: &[&Expr]) -> Vec<(Decision, String)> {
        let translated: Vec<Option<Predicate>> = filters
            .iter()
            .map(|f| self.translate_filter(f).map(|(_, p)| p))
            .collect();
        (0..filters.len())
            .map(|i| {
                let siblings: Vec<Predicate> = translated
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .filter_map(|(_, p)| p.clone())
                    .collect();
                self.decide_explained_among(filters[i], &siblings)
            })
            .collect()
    }

    /// Human-readable decisions for a filter set, in order — what
    /// [`TableProvider::supports_filters_pushdown`] decides for the same set, with the reason.
    /// Prefer this over calling [`Self::explain_decision`] per filter when the filters run
    /// together: a range filter's estimate depends on the sibling ranges on its column.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use el_ballista::connector::postgres::PostgresTableProvider;
    /// # fn demo(provider: PostgresTableProvider) {
    /// use datafusion::prelude::{col, lit};
    ///
    /// let lo = col("id").gt_eq(lit(100i64));
    /// let hi = col("id").lt(lit(200i64));
    /// for reason in provider.explain_decisions(&[&lo, &hi]) {
    ///     println!("{reason}");
    /// }
    /// # }
    /// ```
    pub fn explain_decisions(&self, filters: &[&Expr]) -> Vec<String> {
        self.decide_all(filters)
            .into_iter()
            .map(describe_decision)
            .collect()
    }

    /// Warm the EXPLAIN estimate cache for a set of filter expressions (best-effort; failures
    /// only log). Call before planning when estimates should influence decisions
    /// (`el-ballista plan` does this). `scan` also calls it — for *future* queries only.
    pub(crate) async fn warm_explain(&self, filters: &[Expr]) {
        let Some(estimator) = self.estimator.as_ref() else {
            return;
        };
        for expr in filters {
            let Some((_, predicate)) = self.translate_filter(expr) else {
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

    /// Refresh statistics and index metadata once they are older than the TTL (best-effort;
    /// failures keep the previous snapshot). Only affects decisions for *later* plans:
    /// `scan` never re-decides filters that planning already accepted.
    async fn refresh_stats_if_stale(&self) {
        let Some(refresh) = &self.refresh else {
            return;
        };
        let previous = self.cost_snapshot();
        if previous.fetched_at.elapsed() < refresh.ttl {
            return;
        }
        if refresh.in_flight.swap(true, Ordering::AcqRel) {
            return; // another scan is already refreshing
        }
        let _guard = InFlightGuard(&refresh.in_flight);
        let pool = match self.pool.get() {
            Ok(pool) => pool,
            Err(e) => {
                log::warn!("statistics refresh skipped for {}: {e}", self.table_name);
                return;
            }
        };
        let fresh = fetch_cost_state(&pool, &self.table_metadata, Some(&previous)).await;
        let fresh = Arc::new(fresh);
        match self.cost_state.write() {
            Ok(mut guard) => *guard = fresh,
            Err(poisoned) => *poisoned.into_inner() = fresh,
        }
        log::debug!("refreshed cost statistics for {}", self.table_name);
    }
}

/// `PUSH (<fidelity>; <reason>; <sql>)` or `KEEP (<reason>)` — the `el-ballista plan` wording.
fn describe_decision((decision, reason): (Decision, String)) -> String {
    match decision {
        Decision::Push {
            fidelity,
            predicate,
        } => format!(
            "PUSH ({fidelity:?}; {reason}; {})",
            predicate.render_inline()
        ),
        Decision::Keep => format!("KEEP ({reason})"),
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

    /// docs/pushdown.md §1/§2 — per-filter `Exact`/`Inexact`/`Unsupported` decisions for the
    /// whole set (translation fidelity × policy × cost model, each filter with the others as
    /// siblings — see [`Self::explain_decisions`]).
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(self
            .decide_all(filters)
            .into_iter()
            .map(|(decision, _)| {
                let (outcome, pushdown) = match decision {
                    Decision::Push {
                        fidelity: Fidelity::Exact,
                        ..
                    } => ("exact", TableProviderFilterPushDown::Exact),
                    Decision::Push {
                        fidelity: Fidelity::Inexact,
                        ..
                    } => ("inexact", TableProviderFilterPushDown::Inexact),
                    Decision::Keep => ("kept", TableProviderFilterPushDown::Unsupported),
                };
                crate::telemetry::record_pushdown(outcome);
                pushdown
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
            Some(indices) => self
                .table_metadata
                .select_indices(indices)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?,
            None => self.table_metadata.clone(),
        };

        let projected_schema = PostgresRowAdapter::build_arrow_schema(&projected_metadata)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // DataFusion only passes filters here that `supports_filters_pushdown` (in this
        // process, or in the planning client for a decoded plan) already accepted as Exact or
        // Inexact. The policy decision is therefore *not* re-run: re-deciding could flip
        // between planning and scan (EXPLAIN cache warmed, stats refreshed, or a scheduler-side
        // provider without statistics), and for an `Exact` filter DataFusion has already
        // dropped its own check. Each filter is translated — deterministic, from the column
        // kinds that travel with the plan — and pushed. A filter that cannot be translated is
        // an error: dropping it would silently return extra rows.
        let mut pushed = Vec::with_capacity(filters.len());
        let mut any_inexact = false;
        for f in filters {
            let Some((fidelity, predicate)) = self.translate_filter(f) else {
                return Err(DataFusionError::Plan(format!(
                    "Postgres scan of {} received filter `{f}` that has no source \
                     translation; refusing to drop it",
                    self.table_name
                )));
            };
            any_inexact |= fidelity == Fidelity::Inexact;
            pushed.push(predicate);
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
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let partitions = if let Some(fixed) = &self.fixed_partitions {
            if fixed.len() == 1 && fixed[0].predicate.is_none() {
                Vec::new()
            } else {
                fixed.clone()
            }
        } else if self.parallel_workers > 1 {
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
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

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
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                }
                ParallelStrategy::None => Vec::new(),
            }
        } else {
            Vec::new()
        };

        // Opportunistic EXPLAIN warming and TTL statistics refresh for *future* queries only:
        // this scan's filters were decided above/at planning. Best-effort — failures only log.
        self.warm_explain(filters).await;
        self.refresh_stats_if_stale().await;

        let plan = PostgresExecutionPlan::try_new(
            Some(self.descriptor.clone()),
            projected_metadata,
            projected_schema,
            pushed,
            pushed_limit,
            self.batch_size,
            partitions,
            self.run_id
                .clone()
                .unwrap_or_else(crate::connector::query_tag::fresh_run_id),
        )
        .map(|plan| {
            plan.with_max_batch_bytes(self.max_batch_bytes)
                .with_use_copy(self.use_copy)
                .with_copy_statement_timeout_ms(self.copy_statement_timeout_ms)
        })?;

        Ok(Arc::new(plan))
    }
}

/// Register the job's table in a plain DataFusion `SessionContext` — **pure DataFusion, no
/// Ballista**: one process, one Tokio runtime (one worker thread per visible CPU), and the
/// whole `pool_max` connection budget for this process. The table is registered under
/// `config.table`, split into `parallel_scan.partitions` keyset partitions on
/// `parallel_scan.partition_column`; DataFusion runs the partitions concurrently and the
/// process-wide scan limiter keeps at most `pool_max` of them querying the source at once
/// (the rest wait without holding a connection). Pushdown, projection, batch size and the
/// COPY/cursor choice all follow the job config.
///
/// # Examples
///
/// ```no_run
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use datafusion::prelude::SessionContext;
/// use el_ballista::config::JobConfig;
/// use el_ballista::connector::postgres::register_table;
///
/// let config = JobConfig::from_file("job.json")?;
/// let ctx = SessionContext::new();
/// register_table(&ctx, &config).await?;
/// let df = ctx.sql(&format!("SELECT count(*) FROM {}", config.table)).await?;
/// df.show().await?;
/// # Ok(()) }
/// ```
pub async fn register_table(
    ctx: &datafusion::prelude::SessionContext,
    config: &crate::config::JobConfig,
) -> Result<(), crate::errors::AppError> {
    let descriptor = PostgresConnectionDescriptor::from_config(&config.source, 1);
    register_job_table(
        ctx,
        config,
        descriptor,
        1,
        Some(config.parallel_scan.partition_column.clone()),
        None,
    )
    .await
}

/// Shared by [`register_table`] (this process uses the whole budget) and the distributed
/// context (`descriptor` budgets `pool_max / workers` per executor process).
/// `default_partitions` is the partition count when `parallel_scan.partitions <= 1`.
pub(crate) async fn register_job_table(
    ctx: &datafusion::prelude::SessionContext,
    config: &crate::config::JobConfig,
    descriptor: PostgresConnectionDescriptor,
    default_partitions: usize,
    partition_column: Option<String>,
    run_id: Option<&str>,
) -> Result<(), crate::errors::AppError> {
    let configured = config.parallel_scan.partitions;
    let partitions = if configured > 1 {
        configured
    } else {
        default_partitions.max(1)
    };
    log::info!(
        "registering {}.{}: {} source connection(s) for this process ({} process(es) share \
         pool_max = {}), {} scan partition(s)",
        config.source.schema,
        config.table,
        descriptor.budgeted_max_connections(),
        descriptor.expected_workers,
        config.source.pool_max,
        partitions,
    );
    let provider = PostgresTableProvider::new(
        descriptor,
        &config.resolved_table(),
        config.pushdown.policy,
        config.pushdown.deny.clone(),
        config.pushdown.push.clone(),
        CostParams {
            max_source_cost: config.pushdown.max_source_cost,
            keep_threshold: config.pushdown.keep_threshold,
        },
        config.pushdown.statistics_ttl_secs,
        config.execution.batch_size,
    )
    .await?
    .with_parallel_workers(partitions, partition_column)
    .with_max_batch_bytes(config.execution.max_batch_bytes)
    .with_use_copy(config.execution.use_copy)
    .with_copy_statement_timeout_ms(config.execution.copy_statement_timeout_ms)
    .with_parallel_strategy(config.parallel_scan.strategy);
    let provider = match run_id {
        Some(run_id) => provider.with_run_id(run_id),
        None => provider,
    };
    ctx.register_table(&config.table, Arc::new(provider))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    /// Tests never mutate the process environment (other test threads read it). The
    /// providers here are lazy and never connect, so any always-present variable works as
    /// the password source.
    const ALWAYS_SET_ENV: &str = "PATH";
    use super::*;
    use crate::connector::postgres::parallel::ParallelStrategy;
    use crate::types::ColumnMetadata;
    use datafusion::prelude::{SessionContext, col, lit};

    fn column(name: &str, data_type: &str) -> ColumnMetadata {
        ColumnMetadata {
            column_name: name.to_string(),
            data_type: data_type.to_string(),
            is_nullable: true,
            numeric_precision: None,
            numeric_scale: None,
            udt_name: None,
            collation_name: None,
        }
    }

    /// A decode-side provider (no pool): `scan()` resolves the pool lazily, so with a
    /// dummy password env var this exercises the full filter→SQL path with zero network.
    /// Each test passes its OWN var name: lib tests run in parallel and env vars are
    /// process-global, so sharing one var between set/remove pairs races.
    fn test_provider(password_env: &str, policy: PushdownPolicy) -> PostgresTableProvider {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Float64, true),
        ]));
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
                columns: vec![
                    column("id", "bigint"),
                    column("status", "USER-DEFINED"),
                    column("x", "double precision"),
                ],
            },
            policy,
            deny: vec![],
            push: vec![],
            batch_size: 8192,
            use_copy: false,
            copy_statement_timeout_ms: None,
            max_batch_bytes: 16 * 1024 * 1024,
            parallel_workers: 1,
            partition_column: None,
            strategy: ParallelStrategy::None,
            enum_columns: vec!["status".to_string()],
            server_utf8: true,
            fixed_partitions: None,
            run_id: None,
        };
        PostgresTableProvider::from_model(schema, model)
    }

    async fn scan_sql(
        provider: &PostgresTableProvider,
        filters: &[Expr],
    ) -> Result<String, String> {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider
            .scan(&state, None, filters, None)
            .await
            .map_err(|e| e.to_string())?;
        let plan = plan
            .downcast_ref::<PostgresExecutionPlan>()
            .ok_or("not a PostgresExecutionPlan")?;
        Ok(plan.build_query(0).sql().as_str().to_string())
    }

    #[tokio::test]
    async fn test_scan_pushes_enum_label_predicate() {
        // `scan()` pushes the translated label comparison (never `order_status = text`,
        // which Postgres rejects with 42883).
        let provider = test_provider(ALWAYS_SET_ENV, PushdownPolicy::CostBased);
        let sql = scan_sql(&provider, &[col("status").eq(lit("PAID"))])
            .await
            .unwrap();
        assert!(
            sql.contains(r#"((CAST("status" AS text) COLLATE "C") = $1)"#),
            "scan must push the label comparison, got: {sql}"
        );
    }

    #[tokio::test]
    async fn test_scheduler_side_strict_scan_pushes_planned_filters() {
        // A provider decoded from a plan (no statistics) under `strict` used to re-run the
        // policy in `scan`, get Keep, and fail every query. It must trust the filters the
        // planning client accepted and push them.
        let provider = test_provider(ALWAYS_SET_ENV, PushdownPolicy::Strict);
        // The scheduler-side provider itself would not accept new filters (no stats)...
        assert!(matches!(
            provider.decide_cost(&col("id").eq(lit(5i64))),
            Decision::Keep
        ));
        // ...but a filter that arrives in scan was accepted at planning, and is pushed.
        let sql = scan_sql(&provider, &[col("id").eq(lit(5i64))])
            .await
            .unwrap();
        assert!(sql.contains(r#"("id" = $1)"#), "got: {sql}");
    }

    #[tokio::test]
    async fn test_scan_rejects_untranslatable_filter() {
        // A filter DataFusion hands to scan must never be silently dropped.
        let provider = test_provider(ALWAYS_SET_ENV, PushdownPolicy::Always);
        let err = scan_sql(&provider, &[col("x").lt(lit(0.0f64))])
            .await
            .unwrap_err();
        assert!(err.contains("refusing to drop"), "{err}");
    }

    #[test]
    fn test_decoded_provider_decisions() {
        // Always: translation decides. Cost-based without stats: keep (never push blindly).
        let always = test_provider("EL_BALLISTA_TEST_UNUSED", PushdownPolicy::Always);
        let verdicts = always
            .supports_filters_pushdown(&[
                &col("id").eq(lit(1i64)),
                &col("x").eq(lit(1.0f64)),
                &col("x").lt(lit(1.0f64)),
                &col("status").eq(lit(42i64)),
            ])
            .unwrap();
        assert_eq!(
            verdicts,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
        let cost = test_provider("EL_BALLISTA_TEST_UNUSED", PushdownPolicy::CostBased);
        assert!(matches!(
            cost.decide_cost(&col("status").eq(lit("PAID"))),
            Decision::Keep
        ));
        let reason = always.explain_decision(&col("status").eq(lit(42i64)));
        assert!(reason.starts_with("KEEP"), "{reason}");
        let reason = always.explain_decision(&col("id").eq(lit(1i64)));
        assert_eq!(reason, r#"PUSH (Exact; policy=always; ("id" = 1))"#);
    }

    #[test]
    fn test_model_round_trip_keeps_column_kinds() {
        let provider = test_provider("EL_BALLISTA_TEST_UNUSED", PushdownPolicy::Always);
        let decoded = PostgresTableProvider::from_model(provider.schema(), provider.to_model());
        assert_eq!(decoded.column_kinds, provider.column_kinds);
    }

    #[tokio::test]
    async fn test_scan_empty_projection_for_count_star() {
        // `SELECT COUNT(*)` prunes the scan projection to zero columns: the plan
        // must carry the (empty) projected schema so the aggregate counts rows,
        // and the generated SQL must stay valid (`SELECT 1`, never `SELECT FROM`).
        let provider = test_provider(ALWAYS_SET_ENV, PushdownPolicy::CostBased);
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
    }
}
