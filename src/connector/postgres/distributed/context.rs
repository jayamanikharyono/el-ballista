//! Distributed execution context.
//! distributed/context.rs
//! Docs/roadmap.md Phase 4: runs an extraction job through a Ballista scheduler+workers.
//! Two deployment modes:
//!
//! - `standalone` — scheduler and an in-proc executor live in this process (what `ballista`
//!   calls "standalone"). One process, but the whole scheduler→executor plan-shipping path
//!   (codecs, partition distribution, budgeted pools) is exercised, which is what the exit
//!   criterion in docs/roadmap.md Phase 4 needs to be validated before adding machines.
//! - `remote` — connect to an already-running `rel scheduler`; workers are separate processes
//!   (`rel worker`). Source load stays bounded only if the deployment matches the config:
//!   each executing process opens at most `pool_max / workers` connections (see
//!   `SourcePoolRegistry`), so exactly `workers` executor processes must be registered and
//!   each should run with `--concurrent-tasks` <= that share (extra tasks wait for a pooled
//!   connection). `remote` checks this through the scheduler's REST API
//!   (`GET /api/executors`, see [`super::executors`]): more registered executors than
//!   `workers` is an error, fewer (or more task slots than connections) a warning; if the
//!   REST API is unreachable or disabled the deployment is not verified and a warning says so.
//!
//! Connection budget in `standalone`: the in-process executor gets exactly
//! `pool_max / workers` task slots (`ballista.standalone.parallelism`), one per budgeted
//! connection, so concurrent scan tasks never outnumber the connections they share.

use std::sync::Arc;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::execution::SessionState;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::config::JobConfig;
use crate::connector::postgres::PostgresTableProvider;
use crate::errors::AppError;

use super::connection::PostgresConnectionDescriptor;
use super::plan_codec::PostgresPhysicalCodec;
use super::table_codec::PostgresLogicalCodec;

/// A `SessionContext` whose planner runs queries on a Ballista cluster, together with the
/// worker/concurrency settings shared with that cluster's processes.
pub struct DistributedContext {
    /// Client-side context; plans submitted through it are executed on the cluster.
    pub session: SessionContext,
    /// Number of worker processes (or, in standalone mode, expected workers) the source
    /// connection budget is divided across.
    pub workers: usize,
    /// When set, the provider's `scan()` splits the table into this many keyset partitions, one
    /// per scheduled Ballista scan task (docs/roadmap.md Phase 4 distribution).
    pub partition_column: Option<String>,
}

impl DistributedContext {
    /// Base config for driver/executor sessions: carries the job's `batch_size` (so
    /// DataFusion-side batching matches the source `FETCH`/flush size),
    /// `target_partitions` (so local parallelism matches the worker budget) and, for the
    /// in-process executor, one task slot per budgeted source connection.
    fn session_config_for(config: &JobConfig, workers: usize) -> SessionConfig {
        let budget = PostgresConnectionDescriptor::from_config(&config.source, workers.max(1))
            .budgeted_max_connections();
        SessionConfig::new()
            .with_batch_size(config.execution.batch_size.max(1))
            .with_target_partitions(workers.max(1))
            .with_ballista_standalone_parallelism(usize::try_from(budget).unwrap_or(1).max(1))
    }

    fn session_state(config: &SessionConfig) -> SessionState {
        let config = config
            .clone()
            .with_ballista_logical_extension_codec(Arc::new(PostgresLogicalCodec::new()))
            .with_ballista_physical_extension_codec(Arc::new(PostgresPhysicalCodec::new()));
        SessionContext::new_with_config(config).state()
    }

    async fn from_session(
        session: SessionContext,
        workers: usize,
        partition_column: Option<String>,
    ) -> Self {
        Self {
            session,
            workers: workers.max(1),
            partition_column,
        }
    }

    /// Scheduler + in-proc executor in this process. `workers` has two effects: it divides the
    /// source connection budget (`pool_max / workers`, shared through the process-wide
    /// registry) and it becomes the keyset partition count.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use rust_ballista_extraction_layer::config::JobConfig;
    /// use rust_ballista_extraction_layer::connector::postgres::distributed::DistributedContext;
    ///
    /// let config = JobConfig::from_file("job.json")?;
    /// let ctx = DistributedContext::standalone(&config, config.distributed.workers).await?;
    /// ctx.register_source(&config).await?;
    /// let df = ctx.session.sql(&format!("SELECT count(*) FROM {}", config.table)).await?;
    /// df.show().await?;
    /// # Ok(()) }
    /// ```
    pub async fn standalone(config: &JobConfig, workers: usize) -> Result<Self, AppError> {
        log::info!("starting standalone Ballista scheduler + executor in this process");
        let workers = if workers > 0 {
            workers
        } else {
            config.distributed.workers.max(1)
        };
        let session = SessionContext::standalone_with_state(Self::session_state(
            &Self::session_config_for(config, workers),
        ))
        .await?;

        Ok(Self::from_session(
            session,
            workers,
            Some(config.parallel_scan.partition_column.clone()),
        )
        .await)
    }

    /// Connect to an already-running `rel scheduler` (workers are separate `rel worker`
    /// processes). The connection budget is still `pool_max / workers`, applied independently in
    /// each process, so a three-machine deployment adds machines without adding source load.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use rust_ballista_extraction_layer::config::JobConfig;
    /// use rust_ballista_extraction_layer::connector::postgres::distributed::DistributedContext;
    ///
    /// let config = JobConfig::from_file("job.json")?;
    /// let dist = &config.distributed;
    /// let ctx = DistributedContext::remote(&config, &dist.scheduler_url, dist.workers).await?;
    /// ctx.register_source(&config).await?;
    /// let df = ctx.session.sql(&format!("SELECT count(*) FROM {}", config.table)).await?;
    /// df.show().await?;
    /// # Ok(()) }
    /// ```
    pub async fn remote(
        config: &JobConfig,
        scheduler_url: &str,
        workers: usize,
    ) -> Result<Self, AppError> {
        log::info!("connecting to Ballista scheduler at {scheduler_url}");
        let workers = if workers > 0 {
            workers
        } else {
            config.distributed.workers.max(1)
        };
        let budget = PostgresConnectionDescriptor::from_config(&config.source, workers)
            .budgeted_max_connections();
        // S1: check the registered executors against the budget via the scheduler's REST
        // API; more executors than budgeted is refused, anything unverifiable only warns.
        super::executors::verify_remote_executors(scheduler_url, workers, budget).await?;
        let session = SessionContext::remote_with_state(
            scheduler_url,
            Self::session_state(&Self::session_config_for(config, workers)),
        )
        .await?;

        Ok(Self::from_session(
            session,
            workers,
            Some(config.parallel_scan.partition_column.clone()),
        )
        .await)
    }

    /// Opens (once, process-wide) the budgeted source pool, discovers the table schema, and
    /// registers a `PostgresTableProvider` that splits its scan into keyset partitions when
    /// a partition column is configured. Partition *count* comes from
    /// `parallel_scan.partitions` (so scans can oversubscribe workers, e.g. 128 partitions
    /// over 4 workers, for placement spread); `workers` only divides the connection budget
    /// and is the fallback count when `partitions <= 1`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use rust_ballista_extraction_layer::config::JobConfig;
    /// use rust_ballista_extraction_layer::connector::postgres::distributed::DistributedContext;
    ///
    /// let config = JobConfig::from_file("job.json")?;
    /// let ctx = DistributedContext::standalone(&config, 4).await?;
    /// // Registered under `config.table`; queries on it run as Ballista scan tasks.
    /// ctx.register_source(&config).await?;
    /// let df = ctx.session.sql(&format!("SELECT count(*) FROM {}", config.table)).await?;
    /// df.show().await?;
    /// # Ok(()) }
    /// ```
    pub async fn register_source(&self, config: &JobConfig) -> Result<(), AppError> {
        let descriptor = PostgresConnectionDescriptor::from_config(&config.source, self.workers);
        let policy = config.pushdown.policy;

        let configured = config.parallel_scan.partitions;
        // workers doubles as the default count, so existing configs (partitions <= 1)
        // behave exactly as before.
        let partitions = if configured > 1 {
            configured
        } else {
            self.workers
        };
        log::info!(
            "registering {}.{} with {}-worker budget (pool_max/workers = {}) and {} scan partitions",
            config.source.schema,
            config.table,
            self.workers,
            descriptor.budgeted_max_connections(),
            partitions,
        );

        let provider = PostgresTableProvider::new(
            descriptor,
            &config.resolved_table(),
            policy,
            config.pushdown.deny.clone(),
            config.pushdown.push.clone(),
            crate::pushdown::cost_model::CostParams {
                max_source_cost: config.pushdown.max_source_cost,
                keep_threshold: config.pushdown.keep_threshold,
            },
            config.pushdown.statistics_ttl_secs,
            config.execution.batch_size,
        )
        .await?;

        let provider = provider
            .with_parallel_workers(partitions, self.partition_column.clone())
            .with_max_batch_bytes(config.execution.max_batch_bytes)
            .with_use_copy(config.execution.use_copy)
            .with_copy_statement_timeout_ms(config.execution.copy_statement_timeout_ms)
            .with_parallel_strategy(config.parallel_scan.strategy);

        self.session
            .register_table(&config.table, Arc::new(provider))?;

        Ok(())
    }
}
