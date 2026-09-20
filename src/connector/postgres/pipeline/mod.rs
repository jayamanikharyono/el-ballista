//! Job pipeline: turn a parsed [`JobConfig`] into a runnable extraction and execute it.
//!
//! A [`Pipeline`] is the single orchestration path shared by the CLI (`rel run` /
//! `rel distribute`) and by library/example callers. Build one from a config
//! (`Pipeline::from_config` or `Pipeline::from_config_file`) and then either:
//!
//! * [`Pipeline::extract`] / [`Pipeline::extract_distributed`] — run the extraction the config
//!   describes and hand back the Arrow [`RecordBatch`]es, with no checkpoint side effects. This
//!   is the programmatic entry point (what the examples use).
//! * [`Pipeline::run`] / [`Pipeline::run_distributed`] — run the operational job: incremental
//!   mode drives the checkpoint protocol (lease -> window -> commit), full mode is a stateless
//!   scan. Returns a [`RunOutcome`] with row counts and the advanced watermark. This is what the
//!   CLI wraps.
//!
//! The config drives the shape in both cases: `mode` (incremental | full), `parallel_scan`
//! (none | keyset) for full extraction, and `execution.batch_size` for the cursor FETCH size.

use std::path::Path;

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Duration, Utc};
use datafusion::common::ScalarValue;
use datafusion::prelude::{DataFrame, col, lit};
use uuid::Uuid;

use crate::checkpoint::json_store::JsonCheckpointStore;
use crate::checkpoint::{CheckpointStore, JobKey, RunStats};
use crate::config::{ExtractionMode, JobConfig};
use crate::connector::postgres::PostgresExtractor;
use crate::connector::postgres::parallel::{ParallelStrategy, compute_keyset_partitions};
use crate::distributed::pool_registry::registry;
use crate::distributed::{DistributedContext, PostgresConnectionDescriptor};
use crate::errors::AppError;
use crate::incremental::{
    build_window, clamp_to_observed, max_timestamp_column, min_watermark, safe_high_watermark,
};

/// Result of an operational run ([`Pipeline::run`] / [`Pipeline::run_distributed`]).
#[derive(Debug, Clone)]
pub struct RunOutcome {
    /// Rows extracted this run.
    pub rows_extracted: u64,
    /// The committed high watermark (incremental mode only; `None` in full mode).
    pub committed_watermark: Option<DateTime<Utc>>,
    /// The resolved `(lo, hi]` window (incremental mode only; `None` in full mode).
    pub window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    /// Number of Ballista workers used (distributed runs only; `None` for single-node).
    pub workers: Option<usize>,
}

/// A runnable extraction job built from a [`JobConfig`].
pub struct Pipeline {
    config: JobConfig,
}

impl Pipeline {
    /// Build a pipeline from an already-parsed config.
    pub fn from_config(config: JobConfig) -> Self {
        Self { config }
    }

    /// Parse a config file and build a pipeline from it.
    pub fn from_config_file(path: impl AsRef<Path>) -> Result<Self, AppError> {
        Ok(Self::from_config(JobConfig::from_file(path)?))
    }

    /// The config backing this pipeline.
    pub fn config(&self) -> &JobConfig {
        &self.config
    }

    // ----- single-node -----

    /// Extract the data the config describes and return the Arrow batches. No checkpoint is
    /// read or written. Full mode honors `parallel_scan`; incremental mode extracts the current
    /// window (earliest row -> safe high watermark).
    pub async fn extract(&self) -> Result<Vec<RecordBatch>, AppError> {
        let extractor = self.connect().await?;
        match self.config.mode {
            ExtractionMode::Full => Ok(self.extract_full(&extractor).await?.0),
            ExtractionMode::Incremental => {
                let (lo, hi) = self.resolve_window(&extractor, None).await?;
                Ok(vec![self.extract_incremental_batch(&extractor, lo, hi).await?])
            }
        }
    }

    /// Run the operational job. Incremental mode drives the checkpoint protocol and advances the
    /// watermark; full mode is a stateless scan. Extraction output is the Arrow batch — the
    /// project ships no sink, so `run` returns stats and leaves materialization to the caller.
    pub async fn run(&self) -> Result<RunOutcome, AppError> {
        let extractor = self.connect().await?;
        match self.config.mode {
            ExtractionMode::Full => {
                let (_, rows_extracted) = self.extract_full(&extractor).await?;
                Ok(RunOutcome {
                    rows_extracted,
                    committed_watermark: None,
                    window: None,
                    workers: None,
                })
            }
            ExtractionMode::Incremental => self.run_incremental(&extractor).await,
        }
    }

    async fn connect(&self) -> Result<PostgresExtractor, AppError> {
        let password = self.config.resolve_password()?;
        let extractor = PostgresExtractor::connect(
            &self.config.source.host,
            self.config.source.port,
            &self.config.source.user,
            &password,
            &self.config.source.database,
            self.config.source.pool_max,
            self.config.source.statement_timeout_ms,
            &self.config.source.application_name,
        )
        .await?;
        Ok(extractor)
    }

    fn columns(&self) -> Option<Vec<&str>> {
        self.config
            .columns
            .as_ref()
            .map(|c| c.iter().map(String::as_str).collect())
    }

    /// Full / parallel extraction (honors `parallel_scan`). Returns the batches and the row count.
    async fn extract_full(
        &self,
        extractor: &PostgresExtractor,
    ) -> Result<(Vec<RecordBatch>, u64), AppError> {
        let table = self.config.resolved_table();
        let batch_size = self.config.execution.batch_size;
        let partition_column = &self.config.parallel_scan.partition_column;

        let (schema_name, table_only) = match self.config.table.split_once('.') {
            Some((s, t)) => (s.to_string(), t.to_string()),
            None => (self.config.source.schema.clone(), self.config.table.clone()),
        };

        let strategy = ParallelStrategy::parse(&self.config.parallel_scan.strategy);
        let scan_partitions = match strategy {
            ParallelStrategy::Keyset => compute_keyset_partitions(
                extractor.pool(),
                &schema_name,
                &table_only,
                partition_column,
                self.config.parallel_scan.partitions,
            )
            .await
            .map_err(AppError::Extractor)?,
            ParallelStrategy::Ctid => {
                // The single-node extractor scans keyset (integer) ranges, not ctid predicates,
                // so ctid partitioning is only available on the distributed path. Fall back to one
                // scan rather than re-reading the whole table once per partition.
                log::warn!(
                    "job '{}': parallel_scan.strategy='ctid' is not supported by single-node extraction; \
                     running a single full scan (use `rel distribute` for ctid partitioning)",
                    self.config.job_id
                );
                Vec::new()
            }
            ParallelStrategy::None => Vec::new(),
        };

        log::info!(
            "job '{}': full extraction of {} (strategy {:?}, partitions {}, batch_size {})",
            self.config.job_id,
            table,
            strategy,
            scan_partitions.len().max(1),
            batch_size
        );

        let mut batches = Vec::new();
        let mut rows_extracted: u64 = 0;

        if scan_partitions.is_empty() {
            let batch = extractor
                .extract_full_table_with_batch_size(&table, self.columns(), batch_size)
                .await?;
            rows_extracted += batch.num_rows() as u64;
            batches.push(batch);
        } else {
            for part in &scan_partitions {
                let batch = match (part.lo, part.hi) {
                    (Some(lo), Some(hi)) => {
                        extractor
                            .extract_keyset_partition_with_batch_size(
                                &table,
                                self.columns(),
                                partition_column,
                                lo,
                                hi,
                                batch_size,
                            )
                            .await?
                    }
                    _ => {
                        extractor
                            .extract_full_table_with_batch_size(&table, self.columns(), batch_size)
                            .await?
                    }
                };
                log::info!(
                    "job '{}': partition {} extracted {} row(s)",
                    self.config.job_id,
                    part.partition_id,
                    batch.num_rows()
                );
                rows_extracted += batch.num_rows() as u64;
                batches.push(batch);
            }
        }

        Ok((batches, rows_extracted))
    }

    /// Resolve the incremental `(lo, hi]` window. `lo` is the committed watermark (or `None` on a
    /// fresh run, then seeded from the earliest row — docs/incremental-extraction.md §4).
    async fn resolve_window(
        &self,
        extractor: &PostgresExtractor,
        lo: Option<DateTime<Utc>>,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>), AppError> {
        let safety_lag = Duration::seconds(self.config.incremental.safety_lag_secs);
        let max_window = Duration::seconds(self.config.incremental.max_window_secs);
        let hi_candidate = safe_high_watermark(extractor.pool(), safety_lag).await?;

        let lo = match lo {
            Some(v) => Some(v),
            None => min_watermark(
                extractor.pool(),
                &self.config.resolved_table(),
                &self.config.incremental.column,
            )
            .await?
            .map(|min| min - Duration::microseconds(1)),
        };

        let window = build_window(lo, hi_candidate, max_window);
        Ok((window.lo, window.hi))
    }

    async fn extract_incremental_batch(
        &self,
        extractor: &PostgresExtractor,
        lo: DateTime<Utc>,
        hi: DateTime<Utc>,
    ) -> Result<RecordBatch, AppError> {
        let batch = extractor
            .extract_incremental_window_with_batch_size(
                &self.config.resolved_table(),
                self.columns(),
                &self.config.incremental.column,
                lo,
                hi,
                self.config.execution.batch_size,
            )
            .await?;
        Ok(batch)
    }

    async fn run_incremental(&self, extractor: &PostgresExtractor) -> Result<RunOutcome, AppError> {
        let store = JsonCheckpointStore::new(&self.config.checkpoint.dir)?;
        let key = JobKey::new(self.config.job_id.clone());
        let run_id = Uuid::new_v4();
        // Generous relative to expected run time; not yet configurable per job.
        let lease = Duration::minutes(30);

        let checkpoint = store
            .acquire(&key, run_id, lease, &self.config.incremental.column)
            .await?;

        match self
            .extract_incremental_committed(extractor, checkpoint.watermark_value)
            .await
        {
            Ok((committed_hi, rows_extracted, lo, hi)) => {
                store
                    .commit(
                        &key,
                        run_id,
                        committed_hi,
                        RunStats {
                            rows_extracted,
                            window_lo: Some(lo),
                            window_hi: Some(committed_hi),
                        },
                    )
                    .await?;
                Ok(RunOutcome {
                    rows_extracted,
                    committed_watermark: Some(committed_hi),
                    window: Some((lo, hi)),
                    workers: None,
                })
            }
            Err(e) => {
                let _ = store.abandon(&key, run_id, &e.to_string()).await;
                Err(e)
            }
        }
    }

    /// Resolve the window from `lo`, extract it, and clamp the committable watermark to what was
    /// actually observed (docs/incremental-extraction.md §3.1). Returns
    /// `(committed_hi, rows, window_lo, window_hi)`.
    async fn extract_incremental_committed(
        &self,
        extractor: &PostgresExtractor,
        lo: Option<DateTime<Utc>>,
    ) -> Result<(DateTime<Utc>, u64, DateTime<Utc>, DateTime<Utc>), AppError> {
        let (lo, hi) = self.resolve_window(extractor, lo).await?;
        log::info!("job '{}': window ({}, {}]", self.config.job_id, lo, hi);

        let batch = self.extract_incremental_batch(extractor, lo, hi).await?;
        let rows_extracted = batch.num_rows() as u64;

        let max_observed = max_timestamp_column(&batch, &self.config.incremental.column);
        let committed_hi = clamp_to_observed(hi, max_observed);

        Ok((committed_hi, rows_extracted, lo, hi))
    }

    // ----- distributed (Ballista) -----

    /// Extract via a Ballista cluster and return the batches. No checkpoint side effects.
    pub async fn extract_distributed(
        &self,
        workers: Option<usize>,
        scheduler_url: Option<&str>,
    ) -> Result<Vec<RecordBatch>, AppError> {
        let ctx = self.make_distributed_ctx(workers, scheduler_url).await?;
        let df = match self.config.mode {
            ExtractionMode::Full => ctx.session.table(&self.config.table).await?,
            ExtractionMode::Incremental => {
                let (lo, hi) = self.distributed_window(ctx.workers, None).await?;
                self.windowed_table(&ctx, lo, hi).await?
            }
        };
        df.collect().await.map_err(AppError::DataFusion)
    }

    /// Run the operational distributed job (checkpoint protocol in incremental mode).
    pub async fn run_distributed(
        &self,
        workers: Option<usize>,
        scheduler_url: Option<&str>,
    ) -> Result<RunOutcome, AppError> {
        let ctx = self.make_distributed_ctx(workers, scheduler_url).await?;
        let ctx_workers = ctx.workers;

        match self.config.mode {
            ExtractionMode::Full => {
                let df = ctx.session.table(&self.config.table).await?;
                let batches = df.collect().await.map_err(AppError::DataFusion)?;
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                Ok(RunOutcome {
                    rows_extracted: rows as u64,
                    committed_watermark: None,
                    window: None,
                    workers: Some(ctx_workers),
                })
            }
            ExtractionMode::Incremental => {
                let store = JsonCheckpointStore::new(&self.config.checkpoint.dir)?;
                let key = JobKey::new(self.config.job_id.clone());
                let run_id = Uuid::new_v4();
                let lease = Duration::minutes(30);
                let checkpoint = store
                    .acquire(&key, run_id, lease, &self.config.incremental.column)
                    .await?;

                let (lo, hi) = self
                    .distributed_window(ctx_workers, checkpoint.watermark_value)
                    .await?;
                log::info!("job '{}': window ({}, {}]", self.config.job_id, lo, hi);

                let df = self.windowed_table(&ctx, lo, hi).await?;
                let batches = match df.collect().await {
                    Ok(batches) => batches,
                    Err(e) => {
                        // Release the lease so the next run isn't wedged behind it.
                        let _ = store.abandon(&key, run_id, &e.to_string()).await;
                        return Err(AppError::DataFusion(e));
                    }
                };
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

                let column = &self.config.incremental.column;
                let max_observed = batches
                    .iter()
                    .filter_map(|b| max_timestamp_column(b, column))
                    .max();
                let committed_hi = clamp_to_observed(hi, max_observed);

                store
                    .commit(
                        &key,
                        run_id,
                        committed_hi,
                        RunStats {
                            rows_extracted: rows as u64,
                            window_lo: Some(lo),
                            window_hi: Some(committed_hi),
                        },
                    )
                    .await?;

                Ok(RunOutcome {
                    rows_extracted: rows as u64,
                    committed_watermark: Some(committed_hi),
                    window: Some((lo, hi)),
                    workers: Some(ctx_workers),
                })
            }
        }
    }

    async fn make_distributed_ctx(
        &self,
        workers: Option<usize>,
        scheduler_url: Option<&str>,
    ) -> Result<DistributedContext, AppError> {
        let workers = workers.unwrap_or(self.config.distributed.workers);
        let ctx = match scheduler_url {
            Some(url) => DistributedContext::remote(&self.config, url, workers).await?,
            None => DistributedContext::standalone(&self.config, workers).await?,
        };
        ctx.register_source(&self.config).await?;
        Ok(ctx)
    }

    async fn distributed_window(
        &self,
        workers_for_pool: usize,
        lo: Option<DateTime<Utc>>,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>), AppError> {
        let descriptor =
            PostgresConnectionDescriptor::from_config(&self.config.source, workers_for_pool);
        let pool = registry().pool(&descriptor).map_err(AppError::Extractor)?;
        let safety_lag = Duration::seconds(self.config.incremental.safety_lag_secs);
        let max_window = Duration::seconds(self.config.incremental.max_window_secs);
        let hi_candidate = safe_high_watermark(&pool, safety_lag).await?;
        let window = build_window(lo, hi_candidate, max_window);
        Ok((window.lo, window.hi))
    }

    /// The registered source table with the `(lo, hi]` watermark window applied as a pushed
    /// filter (the same predicate `rel plan` renders, ANDed with the provider's keyset bounds).
    async fn windowed_table(
        &self,
        ctx: &DistributedContext,
        lo: DateTime<Utc>,
        hi: DateTime<Utc>,
    ) -> Result<DataFrame, AppError> {
        let column = &self.config.incremental.column;
        let window_lo = ScalarValue::TimestampMicrosecond(Some(lo.timestamp_micros()), None);
        let window_hi = ScalarValue::TimestampMicrosecond(Some(hi.timestamp_micros()), None);
        let window_expr = col(column)
            .gt(lit(window_lo))
            .and(col(column).lt_eq(lit(window_hi)));
        let df = ctx
            .session
            .table(&self.config.table)
            .await?
            .filter(window_expr)?;
        Ok(df)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_config_file_selects_full_mode() {
        let pipeline =
            Pipeline::from_config_file("examples/configs/full_extract.example.json").unwrap();
        assert_eq!(pipeline.config().mode, ExtractionMode::Full);
        assert_eq!(pipeline.config().job_id, "orders_full");
    }

    #[test]
    fn from_config_defaults_to_incremental() {
        let config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        let pipeline = Pipeline::from_config(config);
        assert_eq!(pipeline.config().mode, ExtractionMode::Incremental);
        assert_eq!(pipeline.config().job_id, "orders_incremental");
    }
}
