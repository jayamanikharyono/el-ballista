//! Distributed execution context.
//! distributed/context.rs
//! Docs/roadmap.md Phase 4: runs an extraction job through a Ballista scheduler + workers.
//!
//! Distributed always means a running cluster: [`DistributedContext::remote`] connects to a
//! `rel scheduler`, and the workers are separate `rel worker` processes. (Single-process
//! extraction is plain DataFusion — [`crate::connector::postgres::register_table`] or the
//! connector's `.standalone()` — with no Ballista involved.)
//!
//! Source load stays bounded only if the deployment matches the config: each executing
//! process opens at most `pool_max / workers` connections (see `SourcePoolRegistry`), so
//! exactly `workers` executor processes must be registered and each should run with
//! `--concurrent-tasks` <= that share (extra tasks wait for a pooled connection). `remote`
//! checks this through the scheduler's REST API (`GET /api/executors`, see
//! [`super::executors`]): more registered executors than `workers` is an error, fewer (or
//! more task slots than connections) a warning; if the REST API is unreachable or disabled
//! the deployment is not verified and a warning says so.

use std::sync::Arc;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::execution::SessionState;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::config::JobConfig;
use crate::errors::AppError;

use super::connection::PostgresConnectionDescriptor;
use super::plan_codec::PostgresPhysicalCodec;
use super::table_codec::PostgresLogicalCodec;

/// A `SessionContext` whose planner runs queries on a Ballista cluster, together with the
/// worker/concurrency settings shared with that cluster's processes.
pub struct DistributedContext {
    /// Client-side context; plans submitted through it are executed on the cluster.
    pub session: SessionContext,
    /// Number of worker processes the source connection budget is divided across.
    pub workers: usize,
    /// When set, the provider's `scan()` splits the table into this many keyset partitions, one
    /// per scheduled Ballista scan task (docs/roadmap.md Phase 4 distribution).
    pub partition_column: Option<String>,
    /// The scheduler this session submits to.
    pub scheduler_url: String,
    /// Unique `ballista.job.name` of every job this session submits (`rel-<job_id>-<id>`), so
    /// the job watchdog can find and cancel this extraction's jobs on the scheduler.
    pub job_name: String,
}

impl DistributedContext {
    /// Client session config: carries the job's `batch_size` (so DataFusion-side batching
    /// matches the source `FETCH`/flush size) and `target_partitions` = `workers` (executors
    /// size their own task slots).
    fn session_config_for(config: &JobConfig, workers: usize, job_name: &str) -> SessionConfig {
        SessionConfig::new()
            .with_batch_size(config.execution.batch_size.max(1))
            .with_target_partitions(workers.max(1))
            .with_ballista_job_name(job_name)
    }

    /// A job name unique to this session: `rel-<job_id>-<8 hex chars>`.
    fn unique_job_name(config: &JobConfig) -> String {
        let id = uuid::Uuid::new_v4().simple().to_string();
        format!("rel-{}-{}", config.job_id, &id[..8])
    }

    fn session_state(config: &SessionConfig) -> SessionState {
        let config = config
            .clone()
            .with_ballista_logical_extension_codec(Arc::new(PostgresLogicalCodec::new()))
            .with_ballista_physical_extension_codec(Arc::new(PostgresPhysicalCodec::new()));
        SessionContext::new_with_config(config).state()
    }

    /// Connect to an already-running `rel scheduler` (workers are separate `rel worker`
    /// processes). The connection budget is `pool_max / workers`, applied independently in
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
        // Check the registered executors against the budget via the scheduler's REST
        // API; more executors than budgeted is refused, anything unverifiable only warns.
        super::executors::verify_remote_executors(scheduler_url, workers, budget).await?;
        let job_name = Self::unique_job_name(config);
        let session = SessionContext::remote_with_state(
            scheduler_url,
            Self::session_state(&Self::session_config_for(config, workers, &job_name)),
        )
        .await?;

        Ok(Self {
            session,
            workers: workers.max(1),
            partition_column: Some(config.parallel_scan.partition_column.clone()),
            scheduler_url: scheduler_url.to_string(),
            job_name,
        })
    }

    /// Registers the job's table in the cluster session: a `PostgresTableProvider` whose
    /// descriptor budgets `pool_max / workers` connections per executing process, split into
    /// keyset partitions when a partition column is configured. Partition *count* comes from
    /// `parallel_scan.partitions` (scans can oversubscribe workers, e.g. 128 partitions over 4
    /// workers, for placement spread); `workers` is the fallback count when `partitions <= 1`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use rust_ballista_extraction_layer::config::JobConfig;
    /// use rust_ballista_extraction_layer::connector::postgres::distributed::DistributedContext;
    ///
    /// let config = JobConfig::from_file("job.json")?;
    /// let ctx = DistributedContext::remote(&config, "http://localhost:50050", 4).await?;
    /// // Registered under `config.table`; queries on it run as Ballista scan tasks.
    /// ctx.register_source(&config).await?;
    /// let df = ctx.session.sql(&format!("SELECT count(*) FROM {}", config.table)).await?;
    /// df.show().await?;
    /// # Ok(()) }
    /// ```
    pub async fn register_source(&self, config: &JobConfig) -> Result<(), AppError> {
        self.register_source_tagged(config, None).await
    }

    /// [`Self::register_source`] with every scan query tagged with `run_id` (the run
    /// report's id) instead of a fresh id per scan.
    pub(crate) async fn register_source_tagged(
        &self,
        config: &JobConfig,
        run_id: Option<&str>,
    ) -> Result<(), AppError> {
        let descriptor = PostgresConnectionDescriptor::from_config(&config.source, self.workers);
        crate::connector::postgres::table_provider::register_job_table(
            &self.session,
            config,
            descriptor,
            self.workers,
            self.partition_column.clone(),
            run_id,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CheckpointConfig, DistributedConfig, ExecutionConfig, ParallelScanConfig, PushdownConfig,
        SourceConfig,
    };
    use crate::types::JobId;

    #[test]
    fn remote_session_targets_one_partition_per_worker() {
        let config = JobConfig {
            job_id: JobId::new("ctx").unwrap(),
            table: "orders".to_string(),
            columns: None,
            filters: Vec::new(),
            source: SourceConfig {
                host: "localhost".to_string(),
                port: 5432,
                user: "postgres".to_string(),
                password_env: "UNUSED".to_string(),
                database: "db".to_string(),
                pool_max: 8,
                statement_timeout_ms: 1000,
                application_name: "ctx".to_string(),
                schema: "public".to_string(),
            },
            checkpoint: CheckpointConfig::default(),
            pushdown: PushdownConfig::default(),
            parallel_scan: ParallelScanConfig::default(),
            execution: ExecutionConfig::default(),
            distributed: DistributedConfig::default(),
        };
        let c = DistributedContext::session_config_for(&config, 4, "rel-ctx-0000");
        assert_eq!(c.target_partitions(), 4);
        assert_eq!(
            c.options()
                .entries()
                .iter()
                .find(|e| e.key == "ballista.job.name")
                .and_then(|e| e.value.clone())
                .as_deref(),
            Some("rel-ctx-0000")
        );
        let name = DistributedContext::unique_job_name(&config);
        assert!(name.starts_with("rel-ctx-") && name.len() == "rel-ctx-".len() + 8);
        assert_eq!(c.batch_size(), config.execution.batch_size);
        // Each of 4 executor processes gets pool_max / 4 = 2 source connections.
        assert_eq!(
            PostgresConnectionDescriptor::from_config(&config.source, 4).budgeted_max_connections(),
            2
        );
    }
}
