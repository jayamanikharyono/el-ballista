//! Execution of a job: one prepared scan per run, one `scan_split` dispatch, and the
//! terminals built on it (`collect`, `stream`, diagnostic `run`, checkpointed `run_with`).
//!
//! # Checkpoint contract (`run_with`)
//!
//! ```text
//! lock job → plan (fingerprint; stored bounds on resume) → begin
//!   → for each pending split, ≤ concurrency at a time:
//!        mark_running → scan split → consumer(split, stream)
//!        → consumer Ok AND stream read to the end → mark_completed
//!        → otherwise                               → mark_failed (others continue)
//!   → release lock → Ok(outcome) | Err(SplitsFailed [ids])
//! ```
//!
//! `collect`, `stream` and `run` never read or write checkpoints.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext, ident};
use datafusion::sql::TableReference;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use super::Pipeline;
use super::splits::{
    ExecutionMode, SplitInfo, SplitProgress, SplitStream, compute_splits, pending_splits,
    plan_identity, scan_partitions, single_split,
};
use crate::checkpoint::json_store::JsonCheckpointStore;
use crate::checkpoint::lock::JobLock;
use crate::checkpoint::progress::{PartitionStatus, ProgressFlusher, ProgressReporter};
use crate::checkpoint::{CheckpointStore, PlanIdentity, PlannedSplit, SplitPlan};
use crate::connector::errors::ExtractorError;
use crate::connector::postgres::PostgresTableProvider;
use crate::connector::postgres::distributed::DistributedContext;
use crate::connector::postgres::row_adapter::PostgresRowAdapter;
use crate::errors::{AppError, ConsumerError, SplitFailure, error_chain};

/// Result of a run (`run()` diagnostic or checkpointed `run_with()`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RunOutcome {
    /// Rows of every completed split: delivered in this run plus the recorded counts of
    /// splits an earlier attempt completed (skipped now).
    pub rows_extracted: u64,
    /// Rows streamed to the consumer in this run.
    pub rows_delivered: u64,
    /// Completed splits, including skipped ones.
    pub splits_completed: usize,
    /// Splits completed by an earlier attempt and therefore not scanned again.
    pub splits_skipped: usize,
    /// Splits in the job's plan.
    pub splits_total: usize,
    /// Ballista workers (distributed runs only).
    pub workers: Option<usize>,
}

/// Where a split's rows come from — the one dispatch every terminal goes through.
#[derive(Clone)]
enum ScanSource {
    /// One physical plan whose output partition `i` scans split `i`.
    Standalone {
        plan: Arc<dyn ExecutionPlan>,
        task_ctx: Arc<TaskContext>,
    },
    /// The whole Ballista query is split 0.
    Distributed { df: Box<DataFrame> },
}

impl ScanSource {
    /// Start scanning split `index`.
    async fn scan_split(&self, index: usize) -> Result<SendableRecordBatchStream, AppError> {
        match self {
            ScanSource::Standalone { plan, task_ctx } => {
                Ok(plan.execute(index, Arc::clone(task_ctx))?)
            }
            ScanSource::Distributed { df } => {
                if index != 0 {
                    return Err(ExtractorError::Internal(format!(
                        "distributed runs have one split, got split index {index}"
                    ))
                    .into());
                }
                Ok(df.as_ref().clone().execute_stream().await?)
            }
        }
    }
}

/// A run ready to execute: its splits, how to scan them, and its identity.
struct PreparedRun {
    source: ScanSource,
    splits: Vec<PlannedSplit>,
    schema: SchemaRef,
    identity: PlanIdentity,
    concurrency: usize,
    workers: Option<usize>,
}

/// First half of standalone preparation (everything before the split layout is known).
struct StandalonePrep {
    provider: PostgresTableProvider,
    filters: Vec<datafusion::logical_expr::Expr>,
    identity: PlanIdentity,
}

/// First half of distributed preparation.
struct DistributedPrep {
    df: DataFrame,
    identity: PlanIdentity,
    workers: usize,
}

enum Prep {
    Standalone(Box<StandalonePrep>),
    Distributed(Box<DistributedPrep>),
}

impl Prep {
    fn identity(&self) -> &PlanIdentity {
        match self {
            Prep::Standalone(p) => &p.identity,
            Prep::Distributed(p) => &p.identity,
        }
    }
}

/// Where a distributed run executes.
#[derive(Debug, Clone)]
pub(crate) struct DistributedTarget {
    pub(crate) workers: Option<usize>,
    /// `Some(url)`: a running scheduler; `None`: in-process cluster.
    pub(crate) scheduler: Option<String>,
}

const TABLE_NAME: &str = "extract_source";

impl Pipeline {
    // ----- preparation -----

    async fn prepare_standalone(&self) -> Result<StandalonePrep, AppError> {
        let provider = self.provider().await?;
        let filters = self.filter_exprs_with_schema(&provider.schema())?;
        // Typed projection error (unknown column names) before any scan.
        let columns = self.columns();
        let projected = provider
            .table_metadata()
            .select_columns(columns.as_deref())
            .map_err(ExtractorError::from)?;
        let output_schema = PostgresRowAdapter::build_arrow_schema(&projected)?;
        let identity = plan_identity(
            &self.config,
            &output_schema,
            &filters,
            ExecutionMode::Standalone,
        );
        Ok(StandalonePrep {
            provider,
            filters,
            identity,
        })
    }

    async fn finish_standalone(
        &self,
        prep: StandalonePrep,
        splits: Vec<PlannedSplit>,
    ) -> Result<PreparedRun, AppError> {
        let partitions = scan_partitions(&self.config, &splits)?;
        let budget = prep.provider.descriptor().budgeted_max_connections();
        let provider = prep.provider.with_fixed_partitions(partitions);
        let n = splits.len().max(1);

        // One output partition per split: no repartitioning may reshuffle rows between
        // partitions, or split `i` would no longer be exactly partition `i`'s rows.
        let session_config = SessionConfig::new()
            .with_target_partitions(n)
            .with_round_robin_repartition(false)
            .with_batch_size(self.config.execution.batch_size);
        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(TableReference::bare(TABLE_NAME), Arc::new(provider))?;
        let mut df = ctx.table(TableReference::bare(TABLE_NAME)).await?;
        for expr in prep.filters {
            df = df.filter(expr)?;
        }
        if let Some(columns) = &self.config.columns {
            df = df.select(
                columns
                    .iter()
                    .map(|c| ident(c.as_str()))
                    .collect::<Vec<_>>(),
            )?;
        }
        let task_ctx = Arc::new(df.task_ctx());
        let plan = df.create_physical_plan().await?;
        let produced = plan.output_partitioning().partition_count();
        if produced != n {
            return Err(ExtractorError::Internal(format!(
                "split plan produced {produced} partition(s) for {n} split(s)"
            ))
            .into());
        }
        let schema = plan.schema();
        let concurrency = self
            .config
            .execution
            .concurrent_partitions
            .min(usize::try_from(budget).unwrap_or(1))
            .max(1);
        Ok(PreparedRun {
            source: ScanSource::Standalone { plan, task_ctx },
            splits,
            schema,
            identity: prep.identity,
            concurrency,
            workers: None,
        })
    }

    async fn prepare_distributed(
        &self,
        target: &DistributedTarget,
    ) -> Result<DistributedPrep, AppError> {
        let workers = target.workers.unwrap_or(self.config.distributed.workers);
        let ctx = match target.scheduler.as_deref() {
            Some(url) => DistributedContext::remote(&self.config, url, workers).await?,
            None => DistributedContext::standalone(&self.config, workers).await?,
        };
        ctx.register_source(&self.config).await?;
        let mut df = ctx.session.table(&self.config.table).await?;
        let table_schema = df.schema().inner().clone();
        let filters = self.filter_exprs_with_schema(&table_schema)?;
        for expr in filters.iter().cloned() {
            df = df.filter(expr)?;
        }
        if let Some(columns) = &self.config.columns {
            df = df.select(
                columns
                    .iter()
                    .map(|c| ident(c.as_str()))
                    .collect::<Vec<_>>(),
            )?;
        }
        let output_schema = df.schema().inner().clone();
        let identity = plan_identity(
            &self.config,
            &output_schema,
            &filters,
            ExecutionMode::Distributed,
        );
        Ok(DistributedPrep {
            df,
            identity,
            workers: ctx.workers,
        })
    }

    fn finish_distributed(prep: DistributedPrep) -> PreparedRun {
        let schema = prep.df.schema().inner().clone();
        PreparedRun {
            source: ScanSource::Distributed {
                df: Box::new(prep.df),
            },
            splits: single_split(),
            schema,
            identity: prep.identity,
            concurrency: 1,
            workers: Some(prep.workers),
        }
    }

    async fn prepare(&self, target: Option<&DistributedTarget>) -> Result<Prep, AppError> {
        Ok(match target {
            None => Prep::Standalone(Box::new(self.prepare_standalone().await?)),
            Some(t) => Prep::Distributed(Box::new(self.prepare_distributed(t).await?)),
        })
    }

    /// Fresh split layout (live table) for a prepared run.
    async fn fresh_splits(&self, prep: &Prep) -> Result<Vec<PlannedSplit>, AppError> {
        match prep {
            Prep::Standalone(p) => compute_splits(&self.config, &p.provider).await,
            Prep::Distributed(_) => Ok(single_split()),
        }
    }

    async fn finish(&self, prep: Prep, splits: Vec<PlannedSplit>) -> Result<PreparedRun, AppError> {
        match prep {
            Prep::Standalone(p) => self.finish_standalone(*p, splits).await,
            Prep::Distributed(p) => Ok(Self::finish_distributed(*p)),
        }
    }

    async fn prepare_fresh(
        &self,
        target: Option<&DistributedTarget>,
    ) -> Result<PreparedRun, AppError> {
        let prep = self.prepare(target).await?;
        let splits = self.fresh_splits(&prep).await?;
        self.finish(prep, splits).await
    }

    // ----- terminals without checkpoints -----

    /// All batches, in split order. Materializes the whole result.
    pub(crate) async fn collect_batches(
        &self,
        target: Option<&DistributedTarget>,
    ) -> Result<Vec<RecordBatch>, AppError> {
        let run = self.prepare_fresh(target).await?;
        let source = &run.source;
        let per_split: Vec<Vec<RecordBatch>> = futures::stream::iter(0..run.splits.len())
            .map(|i| async move {
                let stream = source.scan_split(i).await?;
                stream.try_collect::<Vec<_>>().await.map_err(AppError::from)
            })
            .buffered(run.concurrency)
            .try_collect()
            .await?;
        Ok(per_split.into_iter().flatten().collect())
    }

    /// One bounded-memory stream over every split (at most `concurrency` scanning at once;
    /// batches from different splits interleave).
    pub(crate) async fn stream_batches(
        &self,
        target: Option<&DistributedTarget>,
    ) -> Result<SendableRecordBatchStream, AppError> {
        let run = self.prepare_fresh(target).await?;
        let PreparedRun {
            source,
            splits,
            schema,
            concurrency,
            ..
        } = run;
        if let ScanSource::Distributed { .. } = source {
            return source.scan_split(0).await;
        }
        let source = Arc::new(source);
        let merged = futures::stream::iter(0..splits.len())
            .then(move |i| {
                let source = Arc::clone(&source);
                async move { source.scan_split(i).await }
            })
            .map(
                |scan| -> BoxStream<'static, datafusion::error::Result<RecordBatch>> {
                    match scan {
                        Ok(stream) => stream.boxed(),
                        Err(e) => futures::stream::once(async move {
                            Err(DataFusionError::External(Box::new(e)))
                        })
                        .boxed(),
                    }
                },
            )
            .flatten_unordered(concurrency);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, merged)))
    }

    /// Diagnostic count: scans every split, counts rows, discards batches. No checkpoint.
    pub(crate) async fn count_rows(
        &self,
        target: Option<&DistributedTarget>,
    ) -> Result<RunOutcome, AppError> {
        let run = self.prepare_fresh(target).await?;
        let all: Vec<usize> = (0..run.splits.len()).collect();
        let counter = |_: SplitInfo, mut stream: SendableRecordBatchStream| async move {
            while let Some(batch) = stream.next().await {
                batch?;
            }
            Ok::<(), ConsumerError>(())
        };
        let done = self.execute_splits(&run, all, &counter, None, None).await;
        self.outcome(&run, done, 0, 0)
    }

    // ----- checkpointed terminal -----

    /// `run_with`: lock, plan (resume stored bounds or plan fresh), begin, run pending splits
    /// through `consumer`, commit per split, release the lock.
    pub(crate) async fn run_splits_with<F, Fut>(
        &self,
        target: Option<&DistributedTarget>,
        consumer: F,
    ) -> Result<RunOutcome, AppError>
    where
        F: Fn(SplitInfo, SendableRecordBatchStream) -> Fut,
        Fut: Future<Output = Result<(), ConsumerError>>,
    {
        let store = JsonCheckpointStore::new(&self.config.checkpoint.dir)?;
        let ttl = Duration::from_secs(self.config.checkpoint.lock_ttl_secs);
        let lock = store.lock(&self.config.job_id, ttl).await?;
        let result = self.run_locked(target, &store, &lock, &consumer).await;
        let released = lock.release().await;
        match (result, released) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Ok(_), Err(e)) => Err(e.into()),
            (Err(e), Err(release_err)) => {
                log::warn!(
                    "job '{}': releasing the run lock failed: {release_err}",
                    self.config.job_id
                );
                Err(e)
            }
            (Err(e), Ok(())) => Err(e),
        }
    }

    async fn run_locked<F, Fut>(
        &self,
        target: Option<&DistributedTarget>,
        store: &JsonCheckpointStore,
        lock: &JobLock,
        consumer: &F,
    ) -> Result<RunOutcome, AppError>
    where
        F: Fn(SplitInfo, SendableRecordBatchStream) -> Fut,
        Fut: Future<Output = Result<(), ConsumerError>>,
    {
        let job_id = &self.config.job_id;
        let prep = self.prepare(target).await?;
        let splits = match store.read(job_id).await? {
            Some(existing) => {
                existing.check_plan(prep.identity())?;
                log::info!(
                    "job '{job_id}': resuming checkpoint {} with its stored split bounds",
                    prep.identity().fingerprint()
                );
                existing.stored_splits()
            }
            None => self.fresh_splits(&prep).await?,
        };
        let run = self.finish(prep, splits).await?;
        let checkpoint = store
            .begin(
                job_id,
                &SplitPlan {
                    identity: run.identity.clone(),
                    splits: run.splits.clone(),
                },
            )
            .await?;
        let (pending, skipped, skipped_rows) = pending_splits(&checkpoint);
        log::info!(
            "job '{job_id}': {} split(s), {} pending, {skipped} already completed (concurrency {})",
            run.splits.len(),
            pending.len(),
            run.concurrency
        );

        let flusher = ProgressFlusher::start(
            self.config.checkpoint.dir.clone(),
            job_id,
            run.splits.len(),
            Duration::from_secs(self.config.checkpoint.flush_interval_secs.max(1)),
            self.config.checkpoint.flush_rows.max(1),
        );
        let reporter = flusher.reporter();
        let done = self
            .execute_splits(
                &run,
                pending,
                consumer,
                Some((store, lock)),
                Some(&reporter),
            )
            .await;
        flusher.shutdown().await;
        self.outcome(&run, done, skipped, skipped_rows)
    }

    /// Run `targets` through `consumer`, at most `run.concurrency` at a time. A failed split
    /// never stops the others. With `commit`, split states are recorded: Completed only
    /// after the consumer returned `Ok` having read the whole stream.
    async fn execute_splits<F, Fut>(
        &self,
        run: &PreparedRun,
        targets: Vec<usize>,
        consumer: &F,
        commit: Option<(&JsonCheckpointStore, &JobLock)>,
        reporter: Option<&ProgressReporter>,
    ) -> Vec<(usize, Result<u64, AppError>)>
    where
        F: Fn(SplitInfo, SendableRecordBatchStream) -> Fut,
        Fut: Future<Output = Result<(), ConsumerError>>,
    {
        futures::stream::iter(targets)
            .map(|index| async move {
                let result = self
                    .execute_split(run, index, consumer, commit, reporter)
                    .await;
                (index, result)
            })
            .buffer_unordered(run.concurrency.max(1))
            .collect()
            .await
    }

    async fn execute_split<F, Fut>(
        &self,
        run: &PreparedRun,
        index: usize,
        consumer: &F,
        commit: Option<(&JsonCheckpointStore, &JobLock)>,
        reporter: Option<&ProgressReporter>,
    ) -> Result<u64, AppError>
    where
        F: Fn(SplitInfo, SendableRecordBatchStream) -> Fut,
        Fut: Future<Output = Result<(), ConsumerError>>,
    {
        let job_id = &self.config.job_id;
        let split = &run.splits[index];
        let started = Instant::now();
        if let Some((store, _)) = commit {
            store.mark_running(job_id, &split.split_id).await?;
        }

        let attempt = async {
            let stream = run.source.scan_split(index).await?;
            let (stream, progress) = SplitStream::new(
                stream,
                job_id.clone(),
                split.split_id.clone(),
                reporter.cloned(),
            );
            let info = SplitInfo {
                job_id: job_id.clone(),
                split_id: split.split_id.clone(),
                index,
                total: run.splits.len(),
                bounds: split.bounds.clone(),
            };
            let consumed = consumer(info, Box::pin(stream)).await;
            settle(&split.split_id, consumed, &progress)
        };
        let result = attempt.await;

        match (&result, commit) {
            (Ok(rows), Some((store, lock))) => {
                lock.ensure_held()?;
                store.mark_completed(job_id, &split.split_id, *rows).await?;
            }
            (Err(e), Some((store, _))) => {
                let message = error_chain(e).join(": ");
                if let Err(mark_err) = store.mark_failed(job_id, &split.split_id, &message).await {
                    log::error!(
                        "job '{job_id}': cannot record {} as failed: {mark_err}",
                        split.split_id
                    );
                }
            }
            (_, None) => {}
        }
        if let Some(reporter) = reporter {
            reporter.try_report(match &result {
                Ok(rows) => PartitionStatus::completed(index, split.split_id.clone(), *rows),
                Err(e) => PartitionStatus::failed(index, split.split_id.clone(), 0, e.to_string()),
            });
        }
        match &result {
            Ok(rows) => log::info!(
                "job '{job_id}' split={} done rows={rows} elapsed_ms={}",
                split.split_id,
                started.elapsed().as_millis()
            ),
            Err(e) => log::warn!("job '{job_id}' split={} failed: {e}", split.split_id),
        }
        result
    }

    fn outcome(
        &self,
        run: &PreparedRun,
        done: Vec<(usize, Result<u64, AppError>)>,
        skipped: usize,
        skipped_rows: u64,
    ) -> Result<RunOutcome, AppError> {
        let mut delivered = 0u64;
        let mut completed = skipped;
        let mut failures = Vec::new();
        for (index, result) in done {
            match result {
                Ok(rows) => {
                    delivered += rows;
                    completed += 1;
                }
                Err(error) => failures.push(SplitFailure {
                    split_id: run.splits[index].split_id.clone(),
                    error: Box::new(error),
                }),
            }
        }
        let job = self.config.job_id.to_string();
        crate::telemetry::record_splits(&job, "completed", completed as u64);
        crate::telemetry::record_splits(&job, "skipped", skipped as u64);
        crate::telemetry::record_splits(&job, "failed", failures.len() as u64);
        if !failures.is_empty() {
            failures.sort_by(|a, b| a.split_id.cmp(&b.split_id));
            return Err(AppError::SplitsFailed {
                job_id: self.config.job_id.to_string(),
                total: run.splits.len(),
                failures,
            });
        }
        Ok(RunOutcome {
            rows_extracted: delivered + skipped_rows,
            rows_delivered: delivered,
            splits_completed: completed,
            splits_skipped: skipped,
            splits_total: run.splits.len(),
            workers: run.workers,
        })
    }
}

/// Decide what a consumer's return means for its split.
fn settle(
    split_id: &str,
    consumed: Result<(), ConsumerError>,
    progress: &SplitProgress,
) -> Result<u64, AppError> {
    match consumed {
        // The stream failed and the consumer propagated it: an extraction error.
        Err(e) if progress.errored() => match e.downcast::<DataFusionError>() {
            Ok(df) => Err(AppError::DataFusion(*df)),
            Err(e) => Err(AppError::Consumer {
                split_id: split_id.to_string(),
                source: e,
            }),
        },
        Err(e) => Err(AppError::Consumer {
            split_id: split_id.to_string(),
            source: e,
        }),
        Ok(()) if progress.errored() => Err(AppError::SplitIncomplete {
            split_id: split_id.to_string(),
            reason: "the stream yielded an error the consumer did not propagate",
        }),
        Ok(()) if !progress.finished() => Err(AppError::SplitIncomplete {
            split_id: split_id.to_string(),
            reason: "the stream was not read to the end",
        }),
        Ok(()) => {
            log::debug!(
                "split={split_id} delivered rows={} batches={} bytes={}",
                progress.rows(),
                progress.batches(),
                progress.bytes()
            );
            Ok(progress.rows())
        }
    }
}
