//! Split planning and per-split bookkeeping.
//!
//! A job is executed as a list of splits: one per keyset partition (single-node
//! `parallel_scan.strategy = keyset` with `partitions > 1`), otherwise a single whole-table
//! split (`split-0`); a distributed run is one split covering the whole Ballista scan.
//!
//! - [`plan_identity`] describes the plan (for the checkpoint fingerprint).
//! - [`compute_splits`] plans fresh splits from the live table (keyset MIN/MAX);
//!   [`scan_partitions`] re-creates the exact scans of stored splits (a resumed run never
//!   recomputes bounds).
//! - [`pending_splits`] is the one "skip what already completed" rule.
//! - [`SplitStream`] wraps each split's stream: per-batch logging and progress, row counts,
//!   and whether the consumer really read it to the end.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::execution::RecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::Stream;

use crate::checkpoint::progress::{PartitionStatus, ProgressReporter};
use crate::checkpoint::{JobCheckpoint, PlanIdentity, PlannedSplit, SplitBounds, SplitState};
use crate::config::JobConfig;
use crate::connector::errors::ExtractorError;
use crate::connector::postgres::PostgresTableProvider;
use crate::connector::postgres::parallel::{
    ParallelStrategy, ScanPartition, compute_keyset_partitions, keyset_partition,
};
use crate::errors::AppError;
use crate::types::JobId;

/// What one split covers — handed to a `run_with` consumer together with the split's stream.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SplitInfo {
    /// The job this split belongs to.
    pub job_id: JobId,
    /// Stable split id (`split-<index>`), as recorded in the checkpoint.
    pub split_id: String,
    /// 0-based position of the split in the job's plan.
    pub index: usize,
    /// Number of splits in the job's plan (completed ones included).
    pub total: usize,
    /// Key range of a keyset split (`None` = the whole table / whole distributed scan).
    pub bounds: Option<SplitBounds>,
}

/// Which way a run executes; part of the plan identity (the split layout differs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionMode {
    Standalone,
    Distributed,
}

impl ExecutionMode {
    fn name(self) -> &'static str {
        match self {
            ExecutionMode::Standalone => "standalone",
            ExecutionMode::Distributed => "distributed",
        }
    }
}

fn strategy_name(strategy: ParallelStrategy) -> &'static str {
    match strategy {
        ParallelStrategy::None => "none",
        ParallelStrategy::Keyset => "keyset",
        ParallelStrategy::Ctid => "ctid",
    }
}

/// The plan identity a checkpoint is bound to: table, output schema, projection, the
/// resolved (schema-coerced) filter predicates, parallel strategy, partitions, partition
/// column and execution mode. Batch size, COPY vs cursor and pushdown policy are excluded:
/// they change how rows are fetched, not which rows a split holds.
pub(crate) fn plan_identity(
    config: &JobConfig,
    output_schema: &Schema,
    filters: &[Expr],
    mode: ExecutionMode,
) -> PlanIdentity {
    let schema = output_schema
        .fields()
        .iter()
        .map(|f| {
            format!(
                "{}: {}{}",
                f.name(),
                f.data_type(),
                if f.is_nullable() { " null" } else { "" }
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let projection = config
        .columns
        .as_ref()
        .map_or_else(|| "*".to_string(), |c| c.join(", "));
    let filters = if filters.is_empty() {
        "(none)".to_string()
    } else {
        filters
            .iter()
            .map(|e| format!("({e})"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    PlanIdentity::new()
        .with("table", config.resolved_table())
        .with("schema", schema)
        .with("projection", projection)
        .with("filters", filters)
        .with("strategy", strategy_name(config.parallel_scan.strategy))
        .with("partitions", config.parallel_scan.partitions.to_string())
        .with(
            "partition_column",
            config.parallel_scan.partition_column.clone(),
        )
        .with("execution", mode.name())
}

/// `split-<index>`.
pub(crate) fn split_id(index: usize) -> String {
    format!("split-{index}")
}

/// The single whole-table split.
pub(crate) fn single_split() -> Vec<PlannedSplit> {
    vec![PlannedSplit {
        split_id: split_id(0),
        bounds: None,
    }]
}

/// Plan fresh splits for a single-node run from the live table: keyset bounds from one
/// `MIN`/`MAX` read, or a single split.
pub(crate) async fn compute_splits(
    config: &JobConfig,
    provider: &PostgresTableProvider,
) -> Result<Vec<PlannedSplit>, AppError> {
    let scan = &config.parallel_scan;
    match scan.strategy {
        ParallelStrategy::Keyset if scan.partitions > 1 => {
            let pool = provider.source_pool()?;
            let meta = provider.table_metadata();
            let partitions = compute_keyset_partitions(
                &pool,
                &meta.schema_name,
                &meta.table_name,
                &scan.partition_column,
                scan.partitions,
            )
            .await?;
            Ok(partitions
                .into_iter()
                .map(|p| PlannedSplit {
                    split_id: split_id(p.partition_id),
                    bounds: match (p.lo, p.hi) {
                        (None, None) => None,
                        (lo, hi) => Some(SplitBounds {
                            lo,
                            hi,
                            predicate: p.predicate,
                        }),
                    },
                })
                .collect())
        }
        ParallelStrategy::Ctid => {
            // Single-node splits are keyset ranges (re-creatable from stored integer bounds);
            // ctid ranges shift under VACUUM/updates, so ctid partitioning is only offered on
            // the distributed path. One whole-table split, never a scan per partition.
            log::warn!(
                "job '{}': parallel_scan.strategy='ctid' is not supported by single-node \
                 extraction; running one whole-table split (use `.distributed()` for ctid)",
                config.job_id
            );
            Ok(single_split())
        }
        _ => Ok(single_split()),
    }
}

/// Re-create the exact scan partition of every split, in order. Keyset predicates are
/// re-rendered from the stored integer bounds (never read back as SQL); a stored
/// `predicate` copy that disagrees with the re-rendered one is refused as corrupt.
pub(crate) fn scan_partitions(
    config: &JobConfig,
    splits: &[PlannedSplit],
) -> Result<Vec<ScanPartition>, AppError> {
    let column = &config.parallel_scan.partition_column;
    splits
        .iter()
        .enumerate()
        .map(|(index, split)| {
            if split.split_id != split_id(index) {
                return Err(ExtractorError::InvalidConfig(format!(
                    "split {index} is named '{}', expected '{}'",
                    split.split_id,
                    split_id(index)
                ))
                .into());
            }
            let partition = match &split.bounds {
                None if splits.len() == 1 => keyset_partition(column, 0, None, None)?,
                None => {
                    return Err(ExtractorError::InvalidConfig(format!(
                        "split '{}' has no bounds but the plan has {} splits",
                        split.split_id,
                        splits.len()
                    ))
                    .into());
                }
                Some(bounds) => {
                    let partition = keyset_partition(column, index, bounds.lo, bounds.hi)?;
                    if let Some(stored) = &bounds.predicate
                        && partition.predicate.as_ref() != Some(stored)
                    {
                        return Err(ExtractorError::InvalidConfig(format!(
                            "split '{}': stored predicate {stored:?} does not match its bounds",
                            split.split_id
                        ))
                        .into());
                    }
                    partition
                }
            };
            Ok(partition)
        })
        .collect()
}

/// The one "skip completed splits" rule: indices of splits that still need to run, plus
/// the count and recorded rows of those completed by an earlier attempt.
pub(crate) fn pending_splits(checkpoint: &JobCheckpoint) -> (Vec<usize>, usize, u64) {
    let mut pending = Vec::new();
    let (mut skipped, mut skipped_rows) = (0usize, 0u64);
    for (index, split) in checkpoint.splits.iter().enumerate() {
        if split.state == SplitState::Completed {
            log::info!(
                "job '{}': {} already completed ({} rows), skipping",
                checkpoint.job_id,
                split.split_id,
                split.rows_extracted
            );
            skipped += 1;
            skipped_rows += split.rows_extracted;
        } else {
            pending.push(index);
        }
    }
    (pending, skipped, skipped_rows)
}

/// What a [`SplitStream`] observed.
#[derive(Debug, Default)]
pub(crate) struct SplitProgress {
    rows: AtomicU64,
    batches: AtomicU64,
    bytes: AtomicU64,
    finished: AtomicBool,
    errored: AtomicBool,
}

impl SplitProgress {
    pub(crate) fn rows(&self) -> u64 {
        self.rows.load(Ordering::Acquire)
    }
    pub(crate) fn batches(&self) -> u64 {
        self.batches.load(Ordering::Acquire)
    }
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Acquire)
    }
    /// The stream reached its end (`None`) without the consumer dropping it early.
    pub(crate) fn finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
    /// The stream yielded an error.
    pub(crate) fn errored(&self) -> bool {
        self.errored.load(Ordering::Acquire)
    }
}

/// A split's batch stream as handed to consumers: passes batches through unchanged, logs one
/// structured line per batch (`rows=`, `batch_bytes=`, `split=`; never per row), feeds the
/// advisory progress reporter, and records whether it was read to the end.
pub(crate) struct SplitStream {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    job_id: JobId,
    /// 0-based position of the split in the plan (for progress reports).
    index: usize,
    split_id: String,
    progress: Arc<SplitProgress>,
    reporter: Option<ProgressReporter>,
}

impl SplitStream {
    pub(crate) fn new(
        inner: SendableRecordBatchStream,
        job_id: JobId,
        index: usize,
        split_id: String,
        reporter: Option<ProgressReporter>,
    ) -> (Self, Arc<SplitProgress>) {
        let progress = Arc::new(SplitProgress::default());
        let schema = inner.schema();
        (
            Self {
                inner,
                schema,
                job_id,
                index,
                split_id,
                progress: progress.clone(),
                reporter,
            },
            progress,
        )
    }
}

impl Stream for SplitStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poll = this.inner.as_mut().poll_next(cx);
        match &poll {
            Poll::Ready(Some(Ok(batch))) => {
                let rows = batch.num_rows() as u64;
                let bytes = batch.get_array_memory_size() as u64;
                let p = &this.progress;
                let total_rows = p.rows.fetch_add(rows, Ordering::AcqRel) + rows;
                let n = p.batches.fetch_add(1, Ordering::AcqRel) + 1;
                p.bytes.fetch_add(bytes, Ordering::AcqRel);
                log::debug!(
                    "extract batch job={} split={} batch={n} rows={rows} batch_bytes={bytes}",
                    this.job_id,
                    this.split_id
                );
                crate::telemetry::record_batch(this.job_id.as_str(), rows, bytes);
                if let Some(reporter) = &this.reporter {
                    // The split's running total, not a delta: the progress writer replaces the
                    // split's state with it, so the final `completed` report can't double it.
                    reporter.try_report(PartitionStatus::running(
                        this.index,
                        this.split_id.clone(),
                        total_rows,
                    ));
                }
            }
            Poll::Ready(Some(Err(e))) => {
                this.progress.errored.store(true, Ordering::Release);
                log::warn!(
                    "extract stream error job={} split={}: {e}",
                    this.job_id,
                    this.split_id
                );
            }
            Poll::Ready(None) => this.progress.finished.store(true, Ordering::Release),
            Poll::Pending => {}
        }
        poll
    }
}

impl RecordBatchStream for SplitStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::SplitStatus;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::{col, lit};
    use futures::StreamExt;

    fn config() -> JobConfig {
        JobConfig::from_file("examples/configs/extract.example.json").unwrap()
    }

    fn schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int64, false)])
    }

    #[test]
    fn identity_changes_with_filters_and_partitioning_but_not_with_fetch_tuning() {
        let c = config();
        let base = plan_identity(
            &c,
            &schema(),
            &[col("id").gt(lit(8))],
            ExecutionMode::Standalone,
        );
        let other_filter = plan_identity(
            &c,
            &schema(),
            &[col("id").gt(lit(2))],
            ExecutionMode::Standalone,
        );
        assert_ne!(base.fingerprint(), other_filter.fingerprint());
        assert_eq!(base.diff(other_filter.components()), vec!["filters"]);

        let mut more = c.clone();
        more.parallel_scan.partitions = 8;
        let more = plan_identity(
            &more,
            &schema(),
            &[col("id").gt(lit(8))],
            ExecutionMode::Standalone,
        );
        assert_eq!(base.diff(more.components()), vec!["partitions"]);

        let dist = plan_identity(
            &c,
            &schema(),
            &[col("id").gt(lit(8))],
            ExecutionMode::Distributed,
        );
        assert_eq!(base.diff(dist.components()), vec!["execution"]);

        let mut tuned = c.clone();
        tuned.execution.batch_size = 7;
        tuned.execution.use_copy = true;
        let tuned = plan_identity(
            &tuned,
            &schema(),
            &[col("id").gt(lit(8))],
            ExecutionMode::Standalone,
        );
        assert_eq!(base.fingerprint(), tuned.fingerprint());
    }

    #[test]
    fn scan_partitions_rebuild_stored_bounds_and_refuse_tampering() {
        let mut c = config();
        c.parallel_scan.partition_column = "id".into();
        let splits = vec![
            PlannedSplit {
                split_id: "split-0".into(),
                bounds: Some(SplitBounds {
                    lo: Some(1),
                    hi: Some(5),
                    predicate: Some(r#""id" < 5 OR "id" IS NULL"#.into()),
                }),
            },
            PlannedSplit {
                split_id: "split-1".into(),
                bounds: Some(SplitBounds {
                    lo: Some(5),
                    hi: None,
                    predicate: None,
                }),
            },
        ];
        let parts = scan_partitions(&c, &splits).unwrap();
        assert_eq!(parts[1].predicate.as_deref(), Some(r#""id" >= 5"#));

        let mut tampered = splits.clone();
        tampered[1].bounds.as_mut().unwrap().predicate = Some("true; DROP TABLE x".into());
        assert!(scan_partitions(&c, &tampered).is_err());

        let mut renamed = splits;
        renamed[1].split_id = "split-7".into();
        assert!(scan_partitions(&c, &renamed).is_err());

        let whole = scan_partitions(&c, &single_split()).unwrap();
        assert_eq!(whole.len(), 1);
        assert!(whole[0].predicate.is_none());
    }

    #[test]
    fn pending_splits_skips_only_completed() {
        let now = chrono::Utc::now();
        let status = |id: &str, state| SplitStatus {
            split_id: id.into(),
            state,
            rows_extracted: 3,
            updated_at: now,
            error: None,
            bounds: None,
        };
        let cp = JobCheckpoint {
            job_id: JobId::new("j").unwrap(),
            fingerprint: None,
            plan: Default::default(),
            splits: vec![
                status("split-0", SplitState::Completed),
                status("split-1", SplitState::Failed),
                status("split-2", SplitState::Pending),
            ],
            updated_at: now,
        };
        assert_eq!(pending_splits(&cp), (vec![1, 2], 1, 3));
    }

    #[tokio::test]
    async fn split_stream_tracks_rows_and_exhaustion() {
        let schema = Arc::new(schema());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let inner = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch.clone()), Ok(batch)]),
        );
        let (mut s, progress) = SplitStream::new(
            Box::pin(inner),
            JobId::new("j").unwrap(),
            0,
            "split-0".into(),
            None,
        );
        assert!(s.next().await.is_some());
        assert!(!progress.finished());
        assert!(s.next().await.is_some());
        assert!(s.next().await.is_none());
        assert!(progress.finished());
        assert_eq!(progress.rows(), 6);
        assert_eq!(progress.batches(), 2);
        assert!(progress.bytes() > 0);
        assert!(!progress.errored());
    }
}
