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
//!   (`rel worker`). Source load stays bounded because every worker reads the same config and
//!   therefore budgets its pool as `pool_main / workers` (see `SourcePoolRegistry`).

use std::sync::Arc;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::execution::SessionState;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::config::JobConfig;
use crate::connector::postgres::PostgresTableProvider;
use crate::errors::AppError;
use crate::pushdown::PushdownPolicy;

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
    pub async fn standalone(config: &JobConfig, workers: usize) -> Result<Self, AppError> {
        log::info!("starting standalone Ballista scheduler + executor in this process");
        let session =
            SessionContext::standalone_with_state(Self::session_state(&SessionConfig::new()))
                .await?;

        let workers = if workers > 0 {
            workers
        } else {
            config.distributed.workers.max(1)
        };

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
    pub async fn remote(
        config: &JobConfig,
        scheduler_url: &str,
        workers: usize,
    ) -> Result<Self, AppError> {
        log::info!("connecting to Ballista scheduler at {scheduler_url}");
        let session = SessionContext::remote_with_state(
            scheduler_url,
            Self::session_state(&SessionConfig::new()),
        )
        .await?;

        let workers = if workers > 0 {
            workers
        } else {
            config.distributed.workers.max(1)
        };

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
    pub async fn register_source(&self, config: &JobConfig) -> Result<(), AppError> {
        let descriptor = PostgresConnectionDescriptor::from_config(&config.source, self.workers);
        let policy = PushdownPolicy::parse(&config.pushdown.policy);

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
            .with_parallel_strategy(
                crate::connector::postgres::parallel::ParallelStrategy::parse(
                    &config.parallel_scan.strategy,
                ),
            );

        self.session
            .register_table(&config.table, Arc::new(provider))?;

        Ok(())
    }
}
