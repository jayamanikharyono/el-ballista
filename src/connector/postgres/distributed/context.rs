//! Distributed execution context.
//! distributed/context.rs
//! Docs/roadmap.md Phase 4: runs an extraction job through a Ballista scheduler + workers.
//!
//! Distributed always means a running cluster: [`DistributedContext::remote`] connects to a
//! `el-ballista scheduler`, and the workers are separate `el-ballista worker` processes. (Single-process
//! extraction is plain DataFusion — [`crate::connector::postgres::register_table`] or the
//! connector's `.standalone()` — with no Ballista involved.)
//!
//! Source load stays bounded only if the deployment matches the config: each executing
//! process opens at most `pool_max / workers` connections (see `SourcePoolRegistry`), so
//! exactly `workers` executor processes must be registered and each should run with
//! `--concurrent-tasks` <= that share (extra tasks wait for a pooled connection). `remote`
//! checks this through the scheduler's REST API (`GET /api/executors`, see
//! [`super::executors`]): more registered executors than `workers` is an error, fewer (or
//! more task slots than connections) a warning.
//!
//! Every query runs under the job watchdog ([`super::watchdog`]), which also needs that REST
//! API to see a worker die. When it cannot be asked (unreachable, disabled, or a non-`http://`
//! URL), `remote` refuses unless `distributed.job_timeout_secs` bounds every attempt: without
//! either, a lost worker would leave the job hanging.
//!
//! The scheduler never touches the source: `register_source` computes the scan partitions
//! here, on the client, and the plan carries them.

use std::sync::Arc;
use tracing::info;

use arrow::record_batch::RecordBatch;
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::execution::{SendableRecordBatchStream, SessionState};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;

use crate::config::JobConfig;
use crate::errors::AppError;

use super::connection::PostgresConnectionDescriptor;
use super::plan_codec::PostgresPhysicalCodec;
use super::table_codec::PostgresLogicalCodec;
use super::watchdog::{WatchSettings, watched_stream};

/// A `SessionContext` whose planner runs queries on a Ballista cluster, together with the
/// worker/concurrency settings shared with that cluster's processes. Queries go through
/// [`Self::stream_sql`] / [`Self::collect_sql`], which run them under the job watchdog.
pub struct DistributedContext {
    /// Client-side context; plans submitted through it are executed on the cluster. Not
    /// public: a query run on it directly would bypass the watchdog.
    pub(crate) session: SessionContext,
    /// Number of worker processes the source connection budget is divided across.
    pub(crate) workers: usize,
    /// When set, the table is split into keyset partitions on this column, one per
    /// scheduled Ballista scan task (docs/roadmap.md Phase 4 distribution).
    pub(crate) partition_column: Option<String>,
    /// How the watchdog guards every query of this session: the scheduler it submits to and
    /// the session's unique `ballista.job.name` (`el-ballista-<job_id>-<id>`), by which the
    /// watchdog finds and cancels this extraction's jobs on the scheduler.
    pub(crate) watch: WatchSettings,
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

    /// A job name unique to this session: `el-ballista-<job_id>-<8 hex chars>`.
    fn unique_job_name(config: &JobConfig) -> String {
        let id = uuid::Uuid::new_v4().simple().to_string();
        format!("el-ballista-{}-{}", config.job_id, &id[..8])
    }

    fn session_state(config: &SessionConfig) -> SessionState {
        let config = config
            .clone()
            .with_ballista_logical_extension_codec(Arc::new(PostgresLogicalCodec::new()))
            .with_ballista_physical_extension_codec(Arc::new(PostgresPhysicalCodec::new()));
        SessionContext::new_with_config(config).state()
    }

    /// Connect to an already-running `el-ballista scheduler` (workers are separate `el-ballista worker`
    /// processes). The connection budget is `pool_max / workers`, applied independently in
    /// each process, so a three-machine deployment adds machines without adding source load.
    /// More `workers` than `source.pool_max` is an [`AppError::Config`]: every worker needs at
    /// least one connection, so the total would exceed `pool_max`. So is a scheduler whose
    /// REST API does not answer while `distributed.job_timeout_secs` is unset: nothing could
    /// then notice a lost worker, and the job would hang.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use el_ballista::config::JobConfig;
    /// use el_ballista::connector::postgres::distributed::DistributedContext;
    ///
    /// let config = JobConfig::from_file("job.json")?;
    /// let dist = &config.distributed;
    /// let ctx = DistributedContext::remote(&config, &dist.scheduler_url, dist.workers).await?;
    /// ctx.register_source(&config).await?;
    /// let sql = format!("SELECT count(*) FROM {}", config.table);
    /// let batches = ctx.collect_sql(&sql).await?;
    /// # let _ = batches; Ok(()) }
    /// ```
    pub async fn remote(
        config: &JobConfig,
        scheduler_url: &str,
        workers: usize,
    ) -> Result<Self, AppError> {
        info!(scheduler_url = %scheduler_url, "connecting to the Ballista scheduler");
        let workers = if workers > 0 {
            workers
        } else {
            config.distributed.workers.max(1)
        };
        check_worker_budget(workers, config.source.pool_max)?;
        let budget = PostgresConnectionDescriptor::from_config(&config.source, workers)
            .budgeted_max_connections();
        // Check the registered executors against the budget via the scheduler's REST
        // API; more executors than budgeted is refused, anything unverifiable only warns.
        let rest_answers =
            super::executors::verify_remote_executors(scheduler_url, workers, budget).await?;
        check_hang_detection(
            rest_answers,
            config.distributed.job_timeout_secs,
            scheduler_url,
        )?;
        let job_name = Self::unique_job_name(config);
        let session = SessionContext::remote_with_state(
            scheduler_url,
            Self::session_state(&Self::session_config_for(config, workers, &job_name)),
        )
        .await?;

        Ok(Self {
            session,
            workers,
            partition_column: Some(config.parallel_scan.partition_column.clone()),
            watch: WatchSettings::new(&config.distributed, scheduler_url, &job_name),
        })
    }

    /// Registers the job's table in the cluster session: a `PostgresTableProvider` whose
    /// descriptor budgets `pool_max / workers` connections per executing process, split into
    /// keyset partitions when a partition column is configured. Partition *count* comes from
    /// `parallel_scan.partitions` (scans can oversubscribe workers, e.g. 128 partitions over 4
    /// workers, for placement spread); `workers` is the fallback count when `partitions <= 1`.
    /// The partition bounds are computed here, once, from the live table: every query of this
    /// session scans those ranges (the first and last are open-ended, so rows added later are
    /// still covered), and the scheduler plans without touching the source.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use el_ballista::config::JobConfig;
    /// use el_ballista::connector::postgres::distributed::DistributedContext;
    ///
    /// let config = JobConfig::from_file("job.json")?;
    /// let ctx = DistributedContext::remote(&config, "http://localhost:50050", 4).await?;
    /// // Registered under `config.table`; queries on it run as Ballista scan tasks.
    /// ctx.register_source(&config).await?;
    /// let mut batches = ctx.stream_sql(&format!("SELECT * FROM {}", config.table)).await?;
    /// # let _ = &mut batches; Ok(()) }
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
        let provider = crate::connector::postgres::table_provider::job_provider(
            config,
            descriptor,
            &config.resolved_table(),
            self.workers,
            self.partition_column.clone(),
            run_id,
        )
        .await?
        .with_partitions_planned()
        .await?;
        self.session
            .register_table(&config.table, Arc::new(provider))?;
        Ok(())
    }

    /// Run `sql` on the cluster and stream its batches under the job watchdog: a hung job (a
    /// worker that died, or `distributed.job_timeout_secs` passed) is cancelled and re-run
    /// while it has delivered nothing, up to `distributed.max_retries` times, then the stream
    /// fails with `DistributedJobAborted`; a hang after the first batch is an error, never a
    /// re-run (that would duplicate rows). Planning happens here; the job is submitted when
    /// the stream is first polled.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use futures::TryStreamExt;
    /// use el_ballista::config::JobConfig;
    /// use el_ballista::connector::postgres::distributed::DistributedContext;
    ///
    /// let config = JobConfig::from_file("job.json")?;
    /// let ctx = DistributedContext::remote(&config, "http://localhost:50050", 4).await?;
    /// ctx.register_source(&config).await?;
    /// let mut stream = ctx.stream_sql(&format!("SELECT * FROM {}", config.table)).await?;
    /// while let Some(batch) = stream.try_next().await? {
    ///     println!("{} rows", batch.num_rows());
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn stream_sql(&self, sql: &str) -> Result<SendableRecordBatchStream, AppError> {
        let df = self.session.sql(sql).await?;
        let schema = df.schema().inner().clone();
        Ok(watched_stream(self.watch.clone(), schema, move || {
            let df = df.clone();
            async move { df.execute_stream().await }
        }))
    }

    /// The optimized logical plan of `sql`, as the session would submit it to the cluster
    /// (`LogicalPlan::display_indent`): which filters each Postgres scan pushes to the source
    /// (`full_filters` exact, `partial_filters` re-checked) and which it keeps. Plans only;
    /// nothing runs.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use el_ballista::config::JobConfig;
    /// use el_ballista::connector::postgres::distributed::DistributedContext;
    ///
    /// let config = JobConfig::from_file("job.json")?;
    /// let ctx = DistributedContext::remote(&config, "http://localhost:50050", 4).await?;
    /// ctx.register_source(&config).await?;
    /// let sql = format!("SELECT * FROM {} WHERE id > 7", config.table);
    /// println!("{}", ctx.explain_sql(&sql).await?);
    /// # Ok(()) }
    /// ```
    pub async fn explain_sql(&self, sql: &str) -> Result<String, AppError> {
        let plan = self.session.sql(sql).await?.into_optimized_plan()?;
        Ok(plan.display_indent().to_string())
    }

    /// [`Self::stream_sql`], collected: every batch of the result is held in memory.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use el_ballista::config::JobConfig;
    /// use el_ballista::connector::postgres::distributed::DistributedContext;
    ///
    /// let config = JobConfig::from_file("job.json")?;
    /// let ctx = DistributedContext::remote(&config, "http://localhost:50050", 4).await?;
    /// ctx.register_source(&config).await?;
    /// let batches = ctx.collect_sql(&format!("SELECT count(*) FROM {}", config.table)).await?;
    /// # let _ = batches; Ok(()) }
    /// ```
    pub async fn collect_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, AppError> {
        Ok(self.stream_sql(sql).await?.try_collect().await?)
    }
}

/// The watchdog sees a worker die only through the scheduler REST API; without it, only
/// `distributed.job_timeout_secs` can end a hung attempt. With neither, a lost worker would
/// leave the job running forever (Ballista 54 never re-offers its tasks): refused.
fn check_hang_detection(
    rest_answers: bool,
    job_timeout_secs: Option<u64>,
    scheduler_url: &str,
) -> Result<(), AppError> {
    if rest_answers || job_timeout_secs.is_some() {
        return Ok(());
    }
    Err(AppError::Config(format!(
        "the scheduler REST API at {scheduler_url} does not answer (only plain http:// \
         schedulers are probed), so a lost worker could not be detected and the job would \
         hang: run `el-ballista scheduler` (REST on) at an http:// URL, or set \
         distributed.job_timeout_secs to bound every attempt"
    )))
}

/// Every worker process opens at least one source connection, so more workers than
/// `pool_max` would put more than `pool_max` connections on the source: refused, instead of
/// silently exceeding the budget. Checked for every distributed entry point (config,
/// `.workers(n)`, `--workers`), not in `JobConfig::validate`, because standalone runs ignore
/// `distributed.workers`.
fn check_worker_budget(workers: usize, pool_max: u32) -> Result<(), AppError> {
    let within = u32::try_from(workers).is_ok_and(|w| w <= pool_max);
    if within {
        Ok(())
    } else {
        Err(AppError::Config(format!(
            "{workers} distributed workers exceed source.pool_max = {pool_max}: each worker \
             opens at least one source connection, so use at most {pool_max} workers or raise \
             pool_max"
        )))
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

    fn test_config() -> JobConfig {
        JobConfig {
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
        }
    }

    #[test]
    fn remote_session_targets_one_partition_per_worker() {
        let config = test_config();
        let c = DistributedContext::session_config_for(&config, 4, "el-ballista-ctx-0000");
        assert_eq!(c.target_partitions(), 4);
        assert_eq!(
            c.options()
                .entries()
                .iter()
                .find(|e| e.key == "ballista.job.name")
                .and_then(|e| e.value.clone())
                .as_deref(),
            Some("el-ballista-ctx-0000")
        );
        let name = DistributedContext::unique_job_name(&config);
        assert!(name.starts_with("el-ballista-ctx-") && name.len() == "el-ballista-ctx-".len() + 8);
        assert_eq!(c.batch_size(), config.execution.batch_size);
        // Each of 4 executor processes gets pool_max / 4 = 2 source connections.
        assert_eq!(
            PostgresConnectionDescriptor::from_config(&config.source, 4).budgeted_max_connections(),
            2
        );
    }

    #[test]
    fn a_run_nothing_could_unhang_is_refused() {
        // The REST API answers: the watchdog sees workers die.
        check_hang_detection(true, None, "http://s:50050").unwrap();
        // No REST API, but every attempt is bounded by the job timeout.
        check_hang_detection(false, Some(600), "https://s:50050").unwrap();
        // Neither: a lost worker would hang the job forever.
        let err = check_hang_detection(false, None, "https://s:50050")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("https://s:50050") && err.contains("distributed.job_timeout_secs"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn remote_refuses_an_unprobeable_scheduler_without_a_job_timeout() {
        // `https://` is never probed, so no network is touched before the refusal.
        let mut config = test_config();
        config.distributed.job_timeout_secs = None;
        let err = DistributedContext::remote(&config, "https://127.0.0.1:1", 1)
            .await
            .err()
            .expect("must refuse")
            .to_string();
        assert!(err.contains("distributed.job_timeout_secs"), "{err}");
    }

    #[test]
    fn more_workers_than_pool_max_is_refused() {
        // Up to one connection per worker fits the budget.
        check_worker_budget(1, 8).unwrap();
        check_worker_budget(8, 8).unwrap();
        // One more worker would open a ninth connection.
        let err = check_worker_budget(9, 8).unwrap_err().to_string();
        assert!(
            err.contains("9 distributed workers") && err.contains("source.pool_max = 8"),
            "{err}"
        );
        assert!(check_worker_budget(usize::MAX, u32::MAX).is_err());
    }
}
