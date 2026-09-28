//! Fluent extraction entry point for the Postgres connector.
//!
//! `PostgresConnector::from_config(cfg)?.extract()` returns a builder; choose the execution
//! target and finish with a terminal:
//!
//! | terminal | returns | checkpoints | memory |
//! |---|---|---|---|
//! | `run_with(consumer)` | [`RunOutcome`] | **yes** — a split is Completed only after `consumer` returned `Ok` for it | bounded |
//! | `stream()` | `SendableRecordBatchStream` | no | bounded |
//! | `collect()` | `Vec<RecordBatch>` | no | whole result |
//! | `run()` | [`RunOutcome`] (row counts) | no — **diagnostic only** | bounded |
//!
//! ```no_run
//! # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
//! use futures::TryStreamExt;
//! use rust_ballista_extraction_layer::config::JobConfig;
//! use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;
//!
//! let connector = PostgresConnector::from_config(JobConfig::from_file("job.json")?)?;
//!
//! // Operational job: hand each split to your writer; retries skip acknowledged splits.
//! let outcome = connector
//!     .extract()
//!     .standalone()
//!     .run_with(|split, mut stream| async move {
//!         while let Some(batch) = stream.try_next().await? {
//!             // write `batch` for `split.split_id` somewhere durable
//!             let _ = (&split, batch);
//!         }
//!         Ok(())
//!     })
//!     .await?;
//! println!("{} rows, {} splits skipped", outcome.rows_delivered, outcome.splits_skipped);
//!
//! // Distributed over a running Ballista cluster (`rel scheduler` + `rel worker`s); the
//! // endpoint defaults to the configured / standard scheduler URL.
//! let rows = connector.extract().distributed().workers(4).collect().await?;
//! # let _ = rows; Ok(()) }
//! ```

use std::future::Future;

use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;

use super::pipeline::{DistributedTarget, Pipeline, RunOutcome, SplitInfo};
use crate::config::JobConfig;
use crate::errors::{AppError, ConsumerError};

/// Default Ballista scheduler endpoint used by `.distributed()` when the config sets none.
pub const DEFAULT_SCHEDULER_URL: &str = "http://localhost:50050";

/// A Postgres source connector built from a job config — the single entry point for extraction.
#[derive(Debug)]
pub struct PostgresConnector {
    pipeline: Pipeline,
}

impl PostgresConnector {
    /// Build a connector from an already-parsed config. Validates the config
    /// (`JobConfig::validate`); a degenerate value is an error here, never an empty
    /// extraction later.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rust_ballista_extraction_layer::config::JobConfig;
    /// use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;
    ///
    /// let connector = PostgresConnector::from_config(JobConfig::from_file("job.json")?)?;
    /// # let _ = connector;
    /// # Ok::<(), rust_ballista_extraction_layer::errors::AppError>(())
    /// ```
    pub fn from_config(config: JobConfig) -> Result<Self, AppError> {
        Ok(Self {
            pipeline: Pipeline::from_config(config)?,
        })
    }

    /// Parse and validate a config file and build a connector from it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;
    /// let connector = PostgresConnector::from_config_file("job.json")?;
    /// # let _ = connector;
    /// # Ok::<(), rust_ballista_extraction_layer::errors::AppError>(())
    /// ```
    pub fn from_config_file(path: impl AsRef<std::path::Path>) -> Result<Self, AppError> {
        Ok(Self {
            pipeline: Pipeline::from_config_file(path)?,
        })
    }

    /// The config backing this connector.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// println!("job {} reads {}", connector.config().job_id, connector.config().table);
    /// # Ok(()) }
    /// ```
    pub fn config(&self) -> &JobConfig {
        self.pipeline.config()
    }

    /// The underlying pipeline — programmatic access to the job's parsed filters
    /// ([`Pipeline::filter_exprs`]), the per-filter pushdown preview
    /// ([`Pipeline::explain_filters`]) and a limited row preview ([`Pipeline::preview`]).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// for decision in connector.pipeline().explain_filters().await? {
    ///     println!("{} -> pushed={}", decision.filter, decision.pushed_to_source);
    /// }
    /// # Ok(()) }
    /// ```
    pub fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// Begin an extraction; pick the target with [`ExtractBuilder::standalone`] or
    /// [`ExtractBuilder::distributed`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// let batches = connector.extract().standalone().collect().await?;
    /// # let _ = batches; Ok(()) }
    /// ```
    pub fn extract(&self) -> ExtractBuilder<'_> {
        ExtractBuilder {
            pipeline: &self.pipeline,
        }
    }
}

/// Chooses the execution target for an extraction.
#[must_use = "an extraction does nothing until a terminal (collect/stream/run/run_with) is awaited"]
pub struct ExtractBuilder<'a> {
    pipeline: &'a Pipeline,
}

impl<'a> ExtractBuilder<'a> {
    /// Single-node extraction in this process.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// let stream = connector.extract().standalone().stream().await?;
    /// # let _ = stream; Ok(()) }
    /// ```
    pub fn standalone(self) -> StandaloneExtraction<'a> {
        StandaloneExtraction {
            pipeline: self.pipeline,
        }
    }

    /// Distributed extraction over Ballista. The scheduler endpoint defaults to the config's
    /// `distributed.scheduler_url`, or [`DEFAULT_SCHEDULER_URL`] when that is empty; override with
    /// [`DistributedExtraction::scheduler`]. A running cluster (`rel scheduler` + `rel worker`
    /// processes) is required; single-process extraction is [`Self::standalone`] (plain
    /// DataFusion, no Ballista).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// let batches = connector
    ///     .extract()
    ///     .distributed()
    ///     .scheduler("http://scheduler:50050")
    ///     .workers(3)
    ///     .collect()
    ///     .await?;
    /// # let _ = batches; Ok(()) }
    /// ```
    pub fn distributed(self) -> DistributedExtraction<'a> {
        let configured = self.pipeline.config().distributed.scheduler_url.trim();
        let scheduler = if configured.is_empty() {
            DEFAULT_SCHEDULER_URL.to_string()
        } else {
            configured.to_string()
        };
        DistributedExtraction {
            pipeline: self.pipeline,
            scheduler,
            workers: None,
        }
    }
}

/// A single-node extraction — plain DataFusion, no Ballista — ready for a terminal. Splits are
/// the keyset partitions of `parallel_scan` (or one whole-table split), scanned at most
/// `execution.concurrent_partitions` at a time (default: the whole `pool_max`, never more).
#[must_use = "an extraction does nothing until a terminal (collect/stream/run/run_with) is awaited"]
pub struct StandaloneExtraction<'a> {
    pipeline: &'a Pipeline,
}

impl StandaloneExtraction<'_> {
    /// Execute and return every Arrow batch, in split order. No checkpoint is read or
    /// written. **Materializes the whole result** — prefer [`Self::stream`] or
    /// [`Self::run_with`] for large tables.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// let batches = connector.extract().standalone().collect().await?;
    /// let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    /// # let _ = rows; Ok(()) }
    /// ```
    pub async fn collect(self) -> Result<Vec<RecordBatch>, AppError> {
        self.pipeline.collect_batches(None).await
    }

    /// Execute as one bounded-memory stream over every split (batches of concurrently
    /// scanned splits interleave). No checkpoint is read or written. Dropping the stream
    /// stops the source scans.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// use futures::TryStreamExt;
    /// let mut stream = connector.extract().standalone().stream().await?;
    /// while let Some(batch) = stream.try_next().await? {
    ///     println!("{} rows", batch.num_rows());
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn stream(self) -> Result<SendableRecordBatchStream, AppError> {
        self.pipeline.stream_batches(None).await
    }

    /// **Diagnostic only.** Scans every split, counts rows and discards the batches. Does
    /// NOT read or write checkpoints and delivers no data — use it to measure or smoke-test
    /// a job. For the operational job use [`Self::run_with`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// let counted = connector.extract().standalone().run().await?;
    /// println!("{} rows in {} splits", counted.rows_extracted, counted.splits_total);
    /// # Ok(()) }
    /// ```
    pub async fn run(self) -> Result<RunOutcome, AppError> {
        self.pipeline.count_rows(None).await
    }

    /// The operational job with split checkpointing. Each pending split's stream is passed
    /// to `consumer` (up to `concurrency` splits at a time, so `consumer` must be `Fn`); a
    /// split is recorded **Completed only after `consumer` returns `Ok` having read its
    /// stream to the end**. A consumer error (or an unread / error-swallowed stream) records
    /// the split Failed with that error; other splits continue, and the run then fails with
    /// [`AppError::SplitsFailed`] listing the failed split ids. Every failure of a run that
    /// started comes wrapped in [`AppError::RunFailed`], which carries the run's id and report
    /// (match on [`AppError::underlying`]); the returned [`RunOutcome`] carries them on
    /// success. A lock refusal is not wrapped: that run never started. A retry with the same config
    /// skips completed splits and re-scans the others with their stored bounds; a changed
    /// plan (filters, table, projection, partitioning) is a
    /// [`CheckpointError::PlanMismatch`](crate::checkpoint::CheckpointError::PlanMismatch).
    /// Holds the job's exclusive lock for the whole run.
    ///
    /// Delivery is at-least-once per split: a split whose consumer failed (or whose process
    /// died) mid-stream is re-delivered in full on retry, so consumers should write each
    /// split idempotently (e.g. replace per `split_id`).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// use futures::TryStreamExt;
    /// let outcome = connector
    ///     .extract()
    ///     .standalone()
    ///     .run_with(|split, mut stream| async move {
    ///         let mut rows = 0;
    ///         while let Some(batch) = stream.try_next().await? {
    ///             rows += batch.num_rows();
    ///         }
    ///         println!("{}: {rows} rows", split.split_id);
    ///         Ok(())
    ///     })
    ///     .await?;
    /// # let _ = outcome; Ok(()) }
    /// ```
    pub async fn run_with<F, Fut>(self, consumer: F) -> Result<RunOutcome, AppError>
    where
        F: Fn(SplitInfo, SendableRecordBatchStream) -> Fut,
        Fut: Future<Output = Result<(), ConsumerError>>,
    {
        self.pipeline.run_splits_with(None, consumer).await
    }
}

/// A distributed (Ballista) extraction, ready for a terminal. The whole cluster query is one
/// split for checkpointing purposes.
#[must_use = "an extraction does nothing until a terminal (collect/stream/run/run_with) is awaited"]
pub struct DistributedExtraction<'a> {
    pipeline: &'a Pipeline,
    /// The scheduler endpoint to connect to.
    scheduler: String,
    workers: Option<usize>,
}

impl DistributedExtraction<'_> {
    /// Connect to this Ballista scheduler endpoint instead of the default. The registered
    /// executors are checked against the source budget (see
    /// [`crate::connector::postgres::distributed::executors`]).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// let outcome = connector
    ///     .extract()
    ///     .distributed()
    ///     .scheduler("http://scheduler:50050")
    ///     .workers(4)
    ///     .run()
    ///     .await?;
    /// # let _ = outcome; Ok(()) }
    /// ```
    pub fn scheduler(mut self, url: impl Into<String>) -> Self {
        self.scheduler = url.into();
        self
    }

    /// Override the worker count (defaults to the config's `distributed.workers`). The
    /// source budget `pool_max` is split across this many executing processes.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// let outcome = connector.extract().distributed().workers(3).run().await?;
    /// # let _ = outcome; Ok(()) }
    /// ```
    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = Some(workers);
        self
    }

    fn target(&self) -> DistributedTarget {
        DistributedTarget {
            workers: self.workers,
            scheduler: self.scheduler.clone(),
        }
    }

    /// Execute and return the Arrow batches. No checkpoint side effects. Materializes the
    /// whole result.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// let batches = connector.extract().distributed().collect().await?;
    /// # let _ = batches; Ok(()) }
    /// ```
    pub async fn collect(self) -> Result<Vec<RecordBatch>, AppError> {
        self.pipeline.collect_batches(Some(&self.target())).await
    }

    /// Execute as one bounded-memory stream. No checkpoint side effects.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// let stream = connector.extract().distributed().stream().await?;
    /// # let _ = stream; Ok(()) }
    /// ```
    pub async fn stream(self) -> Result<SendableRecordBatchStream, AppError> {
        self.pipeline.stream_batches(Some(&self.target())).await
    }

    /// **Diagnostic only**: count rows through the cluster; no checkpoint, no data delivered.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// let counted = connector.extract().distributed().run().await?;
    /// # let _ = counted; Ok(()) }
    /// ```
    pub async fn run(self) -> Result<RunOutcome, AppError> {
        self.pipeline.count_rows(Some(&self.target())).await
    }

    /// The operational job with checkpointing, as
    /// [`StandaloneExtraction::run_with`]; the whole distributed scan is one split
    /// (`split-0`), so a failure re-runs the whole scan on retry.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), rust_ballista_extraction_layer::errors::AppError> {
    /// # let connector = rust_ballista_extraction_layer::connector::postgres::PostgresConnector::from_config_file("job.json")?;
    /// use futures::TryStreamExt;
    /// let outcome = connector
    ///     .extract()
    ///     .distributed()
    ///     .run_with(|_split, mut stream| async move {
    ///         // Consume batch by batch (bounded memory); hand each to your sink here.
    ///         while let Some(batch) = stream.try_next().await? {
    ///             let _ = batch.num_rows();
    ///         }
    ///         Ok(())
    ///     })
    ///     .await?;
    /// # let _ = outcome; Ok(()) }
    /// ```
    pub async fn run_with<F, Fut>(self, consumer: F) -> Result<RunOutcome, AppError>
    where
        F: Fn(SplitInfo, SendableRecordBatchStream) -> Fut,
        Fut: Future<Output = Result<(), ConsumerError>>,
    {
        let target = self.target();
        self.pipeline.run_splits_with(Some(&target), consumer).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distributed_defaults_to_standard_scheduler_when_config_empty() {
        // extract.example.json leaves distributed.scheduler_url empty, so `.distributed()`
        // falls back to the standard endpoint.
        let connector =
            PostgresConnector::from_config_file("examples/configs/extract.example.json").unwrap();
        let dist = connector.extract().distributed();
        assert_eq!(dist.scheduler, DEFAULT_SCHEDULER_URL);
    }

    #[test]
    fn scheduler_overrides_the_default() {
        let connector =
            PostgresConnector::from_config_file("examples/configs/extract.example.json").unwrap();
        let dist = connector
            .extract()
            .distributed()
            .scheduler("http://sched:50050");
        assert_eq!(dist.scheduler, "http://sched:50050");
    }

    #[test]
    fn from_config_validates() {
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.execution.batch_size = 0;
        let err = PostgresConnector::from_config(config).unwrap_err();
        assert!(err.to_string().contains("batch_size"), "{err}");
    }
}
