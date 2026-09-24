//! Job pipeline: turn a parsed [`JobConfig`] into a runnable extraction and execute it.
//!
//! A [`Pipeline`] is the single orchestration path shared by the CLI (`rel run` /
//! `rel distribute`) and by library/example callers. Build one from a config
//! (`Pipeline::from_config` or `Pipeline::from_config_file`) and then either:
//!
//! * [`Pipeline::extract`] / [`Pipeline::extract_distributed`] — run the extraction the config
//!   describes and hand back the Arrow [`RecordBatch`]es, with no checkpoint side effects. This
//!   is the programmatic entry point (what the examples use).
//! * [`Pipeline::run`] / [`Pipeline::run_distributed`] — run the operational job with
//!   split-execution checkpointing (completed splits are skipped on retry) and return a
//!   [`RunOutcome`]. This is what the CLI wraps.
//!
//! The config drives the shape: `columns` for projection, `filters` (empty means a full
//! extraction — every row), `parallel_scan` (none | keyset) for splitting, and
//! `execution.batch_size` for the cursor FETCH size. Filtered extraction uses
//! caller-provided predicates pushed to the source through the normal DataFusion
//! pushdown path; the extraction layer never manages watermarks or backfills —
//! the orchestrator decides WHAT range to extract, this layer decides HOW.

use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::prelude::{SessionContext, col, lit};
use futures::StreamExt;

use crate::checkpoint::json_store::JsonCheckpointStore;
use crate::checkpoint::progress::{PartitionStatus, ProgressFlusher};
use crate::checkpoint::{CheckpointStore, JobKey};
use crate::config::{FilterEntry, FilterInput, FilterOp, FilterSpec, JobConfig};
use crate::connector::postgres::PostgresExtractor;
use crate::connector::postgres::parallel::{
    ParallelStrategy, ScanPartition, compute_keyset_partitions,
};
use crate::connector::postgres::table_provider::PostgresTableProvider;
use crate::distributed::DistributedContext;
use crate::distributed::connection::PostgresConnectionDescriptor;
use crate::distributed::pool_registry::registry;
use crate::errors::AppError;
use crate::pushdown::PushdownPolicy;
use crate::pushdown::cost_model::CostParams;
use crate::pushdown::optimizer_rule::SourceAwarePushdownRule;

/// Result of an operational run ([`Pipeline::run`] / [`Pipeline::run_distributed`]).
#[derive(Debug, Clone)]
pub struct RunOutcome {
    /// Rows extracted this run (including rows from splits completed by an earlier attempt).
    pub rows_extracted: u64,
    /// Splits completed (including ones completed by an earlier attempt and skipped now).
    pub splits_completed: usize,
    /// Total splits in this job's plan.
    pub splits_total: usize,
    /// Number of Ballista workers used (distributed runs only; `None` for single-node).
    pub workers: Option<usize>,
}

/// What the extraction layer will do with one caller-provided filter: the parsed
/// predicate plus the decision whether it executes in the source database.
/// Returned by [`Pipeline::explain_filters`] — the programmatic form of what
/// `rel plan` prints.
#[derive(Debug, Clone)]
pub struct FilterDecision {
    /// Canonical display of the filter (`status = 'PAID'`), regardless of whether
    /// the job spec used the shorthand or structured form.
    pub filter: String,
    /// The parsed DataFusion predicate (what actually executes).
    pub expr: Expr,
    /// DataFusion's pushdown verdict for this filter.
    pub pushdown: TableProviderFilterPushDown,
    /// True when the filter executes in the source database (`Exact` or `Inexact`).
    /// `Inexact` still re-checks in Arrow; `Unsupported` stays in Arrow entirely.
    pub pushed_to_source: bool,
    /// Human-readable reason (same text `rel plan` prints).
    pub reason: String,
}

/// One finished keyset partition: id, its Arrow batches, and its row count.
type PartitionBatches = (usize, Vec<RecordBatch>, u64);

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
    /// read or written. Unfiltered configs use the cursor extractor (honoring
    /// `parallel_scan`); filtered configs run through a local DataFusion provider so
    /// caller-provided predicates push to the source.
    pub async fn extract(&self) -> Result<Vec<RecordBatch>, AppError> {
        if self.config.filters.is_empty() {
            let extractor = self.connect().await?;
            Ok(self.extract_full(&extractor).await?.0)
        } else {
            self.extract_filtered_local().await
        }
    }

    /// Run the operational job with split-execution checkpointing. Completed splits are
    /// skipped on retry; a failed split is recorded and the run returns the error so the
    /// orchestrator can retry. Extraction output is the Arrow batch — the project ships
    /// no sink, so `run` returns stats and leaves materialization to the caller.
    pub async fn run(&self) -> Result<RunOutcome, AppError> {
        if self.config.filters.is_empty() {
            self.run_full_with_splits().await
        } else {
            self.run_filtered_with_splits().await
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

    /// The job's filters as [`FilterSpec`]s, flattened across OR-groups —
    /// shorthand strings parsed, structured entries passed through.
    /// Pure: no I/O, safe to call for validation alone. For the grouped form
    /// (one entry per AND-conjunct) see [`Pipeline::filter_groups`].
    pub fn filter_specs(&self) -> Result<Vec<FilterSpec>, AppError> {
        Ok(self.filter_groups()?.into_iter().flatten().collect())
    }

    /// The job's filters grouped by AND-conjunct: one inner vec per outer
    /// `filters` entry. A `Single` yields one spec; an `OrGroup` yields its
    /// branches in order. Pure: no I/O, safe to call for validation alone.
    pub fn filter_groups(&self) -> Result<Vec<Vec<FilterSpec>>, AppError> {
        self.config
            .filters
            .iter()
            .map(|entry| match entry {
                FilterEntry::Single(input) => resolve_input(input).map(|spec| vec![spec]),
                FilterEntry::OrGroup(inputs) => {
                    if inputs.is_empty() {
                        return Err(AppError::Config(
                            "filters contains an empty OR-group (inner array must hold >= 1 predicate)".to_string(),
                        ));
                    }
                    inputs.iter().map(resolve_input).collect()
                }
            })
            .collect()
    }

    /// The job's filters as parsed DataFusion predicates — the single choke point
    /// every extraction path funnels through, so config entries, the CLI, and the
    /// provider can never disagree about what a filter means. Pure: no I/O, safe
    /// to call for validation alone. One `Expr` per AND-conjunct: an OR-group
    /// lowers to a single `a OR b` expression. Literals follow the JSON value
    /// types; string values stay text (for timestamp/date coercion use
    /// [`Pipeline::filter_exprs_with_schema`]).
    pub fn filter_exprs(&self) -> Result<Vec<Expr>, AppError> {
        self.filter_groups()?
            .iter()
            .map(|group| {
                group
                    .iter()
                    .map(FilterSpec::to_expr)
                    .collect::<Result<Vec<_>, _>>()
                    .and_then(or_group)
            })
            .collect()
    }

    /// Like [`Pipeline::filter_exprs`], but string values are coerced against the
    /// extraction schema: an RFC3339 (or `%Y-%m-%d`) string on a timestamp column
    /// becomes a real timestamp literal that pushes to the source — so
    /// orchestrator-supplied time ranges push instead of degrading to Arrow-side
    /// filtering after a full scan. Date strings become date literals that
    /// evaluate correctly in Arrow (date-literal pushdown is deferred).
    pub fn filter_exprs_with_schema(&self, schema: &Schema) -> Result<Vec<Expr>, AppError> {
        self.filter_groups()?
            .iter()
            .map(|group| {
                group
                    .iter()
                    .map(|spec| {
                        let dtype = schema
                            .index_of(spec.column.trim())
                            .ok()
                            .map(|i| schema.field(i).data_type());
                        spec.to_expr_with_type(dtype)
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .and_then(or_group)
            })
            .collect()
    }

    /// Preview what the extraction layer will do with each caller-provided filter:
    /// the parsed predicate plus whether it pushes to the source database under the
    /// job's pushdown policy. One entry per AND-conjunct — an OR-group previews
    /// as a single `(a OR b)` decision, because that is the unit `scan()` decides
    /// on. Same decision point `scan()` uses, so the preview can never disagree
    /// with execution. Use it to decide — before extracting — which
    /// filters need a source index and which will run in Arrow.
    pub async fn explain_filters(&self) -> Result<Vec<FilterDecision>, AppError> {
        let provider = self.filtered_provider().await?;
        let groups = self.filter_groups()?;
        let exprs = self.filter_exprs_with_schema(&provider.schema())?;
        let refs: Vec<&Expr> = exprs.iter().collect();
        // Warm EXPLAIN estimates first so cost-based decisions use them, exactly as
        // a warmed production provider would.
        provider.warm_explain(&exprs).await;
        let verdicts = provider.supports_filters_pushdown(&refs)?;

        Ok(groups
            .iter()
            .zip(exprs)
            .zip(verdicts)
            .map(|((group, expr), pushdown)| {
                let pushed_to_source = pushdown != TableProviderFilterPushDown::Unsupported;
                let reason = provider.explain_decision(&expr);
                FilterDecision {
                    filter: describe_group(group),
                    expr,
                    pushdown,
                    pushed_to_source,
                    reason,
                }
            })
            .collect())
    }

    /// Compute the keyset scan partitions for this job (empty means one unsplit scan).
    async fn compute_scan_partitions(
        &self,
        extractor: &PostgresExtractor,
    ) -> Result<Vec<ScanPartition>, AppError> {
        let (schema_name, table_only) = match self.config.table.split_once('.') {
            Some((s, t)) => (s.to_string(), t.to_string()),
            None => (self.config.source.schema.clone(), self.config.table.clone()),
        };

        let strategy = ParallelStrategy::parse(&self.config.parallel_scan.strategy);
        match strategy {
            ParallelStrategy::Keyset => compute_keyset_partitions(
                extractor.pool(),
                &schema_name,
                &table_only,
                &self.config.parallel_scan.partition_column,
                self.config.parallel_scan.partitions,
            )
            .await
            .map_err(AppError::Extractor),
            ParallelStrategy::Ctid => {
                // The single-node extractor scans keyset (integer) ranges, not ctid predicates,
                // so ctid partitioning is only available on the distributed path. Fall back to one
                // scan rather than re-reading the whole table once per partition.
                log::warn!(
                    "job '{}': parallel_scan.strategy='ctid' is not supported by single-node extraction; \
                     running a single full scan (use `rel distribute` for ctid partitioning)",
                    self.config.job_id
                );
                Ok(Vec::new())
            }
            ParallelStrategy::None => Ok(Vec::new()),
        }
    }

    /// Full extraction honoring `parallel_scan`. Returns the batches and the row count.
    ///
    /// Partitions scan **concurrently** (bounded by `execution.concurrent_partitions`;
    /// the source pool still caps connections). Each partition streams cursor `FETCH`
    /// windows — or one `COPY … TO STDOUT (FORMAT BINARY)` stream when
    /// `execution.use_copy` — through a byte-capped builder, so peak memory is
    /// `O(concurrency × batch)` instead of `O(table)`. Results are re-sorted by
    /// partition id for deterministic batch order.
    async fn extract_full(
        &self,
        extractor: &PostgresExtractor,
    ) -> Result<(Vec<RecordBatch>, u64), AppError> {
        let table = self.config.resolved_table();
        let batch_size = self.config.execution.batch_size;
        let max_batch_bytes = self.config.execution.max_batch_bytes;
        let use_copy = self.config.execution.use_copy;
        let partition_column = &self.config.parallel_scan.partition_column;
        let strategy = ParallelStrategy::parse(&self.config.parallel_scan.strategy);
        let scan_partitions = self.compute_scan_partitions(extractor).await?;

        log::info!(
            "job '{}': full extraction of {} (strategy {:?}, partitions {}, batch_size {}, max_batch_bytes {}, {})",
            self.config.job_id,
            table,
            strategy,
            scan_partitions.len().max(1),
            batch_size,
            max_batch_bytes,
            if use_copy { "COPY" } else { "cursor" }
        );

        if scan_partitions.is_empty() {
            let batches = if use_copy {
                extractor
                    .extract_full_table_via_copy_with_limits(
                        &table,
                        self.columns(),
                        batch_size,
                        max_batch_bytes,
                    )
                    .await?
            } else {
                extractor
                    .extract_full_table_via_cursor_with_limits(
                        &table,
                        self.columns(),
                        batch_size,
                        max_batch_bytes,
                    )
                    .await?
            };
            let rows_extracted: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            return Ok((batches, rows_extracted));
        }

        // Owned per-partition inputs so concurrent tasks share nothing but the pool.
        let columns_owned: Option<Vec<String>> = self.config.columns.clone();
        let concurrency = self
            .config
            .execution
            .concurrent_partitions
            .max(1)
            .min(scan_partitions.len());

        let results: Vec<Result<PartitionBatches, AppError>> =
            futures::stream::iter(scan_partitions.iter().map(|part| {
                let extractor = extractor.clone();
                let table = table.clone();
                let columns_owned = columns_owned.clone();
                let partition_column = partition_column.clone();
                let part = part.clone();
                async move {
                    let columns: Option<Vec<&str>> = columns_owned
                        .as_ref()
                        .map(|c| c.iter().map(String::as_str).collect());
                    let batches = match (part.lo, part.hi) {
                        (Some(lo), Some(hi)) => {
                            if use_copy {
                                extractor
                                    .extract_keyset_partition_via_copy_with_limits(
                                        &table,
                                        columns,
                                        &partition_column,
                                        lo,
                                        hi,
                                        batch_size,
                                        max_batch_bytes,
                                    )
                                    .await?
                            } else {
                                extractor
                                    .extract_keyset_partition_via_cursor_with_limits(
                                        &table,
                                        columns,
                                        &partition_column,
                                        lo,
                                        hi,
                                        batch_size,
                                        max_batch_bytes,
                                    )
                                    .await?
                            }
                        }
                        _ => {
                            if use_copy {
                                extractor
                                    .extract_full_table_via_copy_with_limits(
                                        &table,
                                        columns,
                                        batch_size,
                                        max_batch_bytes,
                                    )
                                    .await?
                            } else {
                                extractor
                                    .extract_full_table_via_cursor_with_limits(
                                        &table,
                                        columns,
                                        batch_size,
                                        max_batch_bytes,
                                    )
                                    .await?
                            }
                        }
                    };
                    let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                    log::info!("partition {} extracted {} row(s)", part.partition_id, rows);
                    Ok((part.partition_id, batches, rows))
                }
            }))
            .buffer_unordered(concurrency)
            .collect()
            .await;

        // Deterministic order regardless of completion order.
        let mut results = results;
        results.sort_by_key(|r| r.as_ref().map(|(id, _, _)| *id).unwrap_or(usize::MAX));
        let mut batches = Vec::new();
        let mut rows_extracted: u64 = 0;
        for result in results {
            let (_, mut part_batches, rows) = result?;
            rows_extracted += rows;
            batches.append(&mut part_batches);
        }

        Ok((batches, rows_extracted))
    }

    /// Split ids for the unfiltered plan: one per keyset partition, or a single split.
    fn split_ids_for_partitions(partitions: &[ScanPartition]) -> Vec<String> {
        if partitions.is_empty() {
            vec!["split-0".to_string()]
        } else {
            partitions
                .iter()
                .map(|p| format!("split-{}", p.partition_id))
                .collect()
        }
    }

    /// Operational full extraction with per-split checkpointing. Each keyset partition
    /// is one split; completed splits are skipped on retry.
    ///
    /// Bounded memory: each split streams cursor batches through `for_each_batch`,
    /// counting rows and dropping batches immediately — the split's row count (not its
    /// data) is what reaches `mark_completed`. Split state transitions are unchanged:
    /// completed splits are never re-scanned, failures record the split without
    /// touching completed ones.
    async fn run_full_with_splits(&self) -> Result<RunOutcome, AppError> {
        let extractor = self.connect().await?;
        let scan_partitions = self.compute_scan_partitions(&extractor).await?;
        let split_ids = Self::split_ids_for_partitions(&scan_partitions);

        let store = JsonCheckpointStore::new(&self.config.checkpoint.dir)?;
        let key = JobKey::new(self.config.job_id.clone());
        let checkpoint = store.begin(&key, &split_ids).await?;

        // Driver-owned progress: per-split completion reports flow through a
        // non-blocking channel to a background writer; the checkpoint file stays the
        // commit point. Workers never touch either.
        let flusher = ProgressFlusher::start(
            self.config.checkpoint.dir.clone(),
            self.config.job_id.clone(),
            split_ids.len().max(1),
            std::time::Duration::from_secs(self.config.checkpoint.flush_interval_secs.max(1)),
            self.config.checkpoint.flush_rows.max(1),
        );
        let reporter = flusher.reporter();

        let table = self.config.resolved_table();
        let batch_size = self.config.execution.batch_size;
        let max_batch_bytes = self.config.execution.max_batch_bytes;
        let use_copy = self.config.execution.use_copy;
        let partition_column = self.config.parallel_scan.partition_column.clone();

        let mut rows_extracted: u64 = 0;
        let mut splits_completed: usize = 0;

        if scan_partitions.is_empty() {
            let split_id = &split_ids[0];
            let already_done = checkpoint.splits.iter().any(|s| {
                &s.split_id == split_id && s.state == crate::checkpoint::SplitState::Completed
            });
            if already_done {
                let prev = checkpoint
                    .splits
                    .iter()
                    .find(|s| &s.split_id == split_id)
                    .expect("checked");
                rows_extracted += prev.rows_extracted;
                splits_completed += 1;
                reporter.try_report(PartitionStatus::completed(
                    0,
                    split_id.clone(),
                    prev.rows_extracted,
                ));
                log::info!(
                    "job '{}': {split_id} already completed, skipping",
                    self.config.job_id
                );
            } else {
                store.mark_running(&key, split_id).await?;
                let columns_owned = self.config.columns.clone();
                let columns: Option<Vec<&str>> = columns_owned
                    .as_ref()
                    .map(|c| c.iter().map(String::as_str).collect());
                let mut rows: u64 = 0;
                let scan = if use_copy {
                    extractor
                        .extract_full_table_via_copy_for_each_batch(
                            &table,
                            columns,
                            batch_size,
                            max_batch_bytes,
                            &mut |batch| {
                                rows += batch.num_rows() as u64;
                                Ok(())
                            },
                        )
                        .await
                } else {
                    extractor
                        .extract_full_table_for_each_batch(
                            &table,
                            columns,
                            batch_size,
                            max_batch_bytes,
                            &mut |batch| {
                                rows += batch.num_rows() as u64;
                                Ok(())
                            },
                        )
                        .await
                };
                match scan {
                    Ok(_) => {
                        store.mark_completed(&key, split_id, rows).await?;
                        reporter.try_report(PartitionStatus::completed(0, split_id.clone(), rows));
                        rows_extracted += rows;
                        splits_completed += 1;
                    }
                    Err(e) => {
                        reporter.try_report(PartitionStatus::failed(
                            0,
                            split_id.clone(),
                            rows,
                            e.to_string(),
                        ));
                        flusher.shutdown().await;
                        let _ = store.mark_failed(&key, split_id, &e.to_string()).await;
                        return Err(AppError::Extractor(e));
                    }
                }
            }
        } else {
            for part in &scan_partitions {
                let split_id = format!("split-{}", part.partition_id);
                let already_done = checkpoint.splits.iter().any(|s| {
                    s.split_id == split_id && s.state == crate::checkpoint::SplitState::Completed
                });
                if already_done {
                    let prev = checkpoint
                        .splits
                        .iter()
                        .find(|s| s.split_id == split_id)
                        .expect("checked");
                    rows_extracted += prev.rows_extracted;
                    splits_completed += 1;
                    reporter.try_report(PartitionStatus::completed(
                        part.partition_id,
                        split_id.clone(),
                        prev.rows_extracted,
                    ));
                    log::info!(
                        "job '{}': {split_id} already completed, skipping",
                        self.config.job_id
                    );
                    continue;
                }
                store.mark_running(&key, &split_id).await?;
                let columns_owned = self.config.columns.clone();
                let columns: Option<Vec<&str>> = columns_owned
                    .as_ref()
                    .map(|c| c.iter().map(String::as_str).collect());
                let mut rows: u64 = 0;
                let scan = match (part.lo, part.hi) {
                    (Some(lo), Some(hi)) => {
                        if use_copy {
                            extractor
                                .extract_keyset_partition_via_copy_for_each_batch(
                                    &table,
                                    columns,
                                    &partition_column,
                                    lo,
                                    hi,
                                    batch_size,
                                    max_batch_bytes,
                                    &mut |batch| {
                                        rows += batch.num_rows() as u64;
                                        Ok(())
                                    },
                                )
                                .await
                        } else {
                            extractor
                                .extract_keyset_partition_for_each_batch(
                                    &table,
                                    columns,
                                    &partition_column,
                                    lo,
                                    hi,
                                    batch_size,
                                    max_batch_bytes,
                                    &mut |batch| {
                                        rows += batch.num_rows() as u64;
                                        Ok(())
                                    },
                                )
                                .await
                        }
                    }
                    _ => {
                        if use_copy {
                            extractor
                                .extract_full_table_via_copy_for_each_batch(
                                    &table,
                                    columns,
                                    batch_size,
                                    max_batch_bytes,
                                    &mut |batch| {
                                        rows += batch.num_rows() as u64;
                                        Ok(())
                                    },
                                )
                                .await
                        } else {
                            extractor
                                .extract_full_table_for_each_batch(
                                    &table,
                                    columns,
                                    batch_size,
                                    max_batch_bytes,
                                    &mut |batch| {
                                        rows += batch.num_rows() as u64;
                                        Ok(())
                                    },
                                )
                                .await
                        }
                    }
                };
                match scan {
                    Ok(_) => {
                        store.mark_completed(&key, &split_id, rows).await?;
                        reporter.try_report(PartitionStatus::completed(
                            part.partition_id,
                            split_id.clone(),
                            rows,
                        ));
                        rows_extracted += rows;
                        splits_completed += 1;
                    }
                    Err(e) => {
                        reporter.try_report(PartitionStatus::failed(
                            part.partition_id,
                            split_id.clone(),
                            rows,
                            e.to_string(),
                        ));
                        flusher.shutdown().await;
                        let _ = store.mark_failed(&key, &split_id, &e.to_string()).await;
                        return Err(AppError::Extractor(e));
                    }
                }
            }
        }

        flusher.shutdown().await;
        Ok(RunOutcome {
            rows_extracted,
            splits_completed,
            splits_total: split_ids.len(),
            workers: None,
        })
    }

    /// Local filtered extraction through a DataFusion provider so caller-provided
    /// predicates push to the source. No checkpoint side effects.
    async fn extract_filtered_local(&self) -> Result<Vec<RecordBatch>, AppError> {
        let df = self.filtered_dataframe_local().await?;
        df.collect().await.map_err(AppError::DataFusion)
    }

    /// Operational filtered extraction: one logical split tracked in the checkpoint store.
    ///
    /// Bounded memory: the DataFrame streams (`execute_stream`) with per-batch counting —
    /// the split's row count (not its data) is what reaches `mark_completed`.
    async fn run_filtered_with_splits(&self) -> Result<RunOutcome, AppError> {
        let store = JsonCheckpointStore::new(&self.config.checkpoint.dir)?;
        let key = JobKey::new(self.config.job_id.clone());
        let split_ids = vec!["split-0".to_string()];
        let checkpoint = store.begin(&key, &split_ids).await?;

        if checkpoint
            .splits
            .iter()
            .any(|s| s.split_id == "split-0" && s.state == crate::checkpoint::SplitState::Completed)
        {
            let prev = checkpoint
                .splits
                .iter()
                .find(|s| s.split_id == "split-0")
                .expect("checked");
            log::info!(
                "job '{}': split-0 already completed, skipping",
                self.config.job_id
            );
            return Ok(RunOutcome {
                rows_extracted: prev.rows_extracted,
                splits_completed: 1,
                splits_total: 1,
                workers: None,
            });
        }

        store.mark_running(&key, "split-0").await?;
        let df = match self.filtered_dataframe_local().await {
            Ok(df) => df,
            Err(e) => {
                let _ = store.mark_failed(&key, "split-0", &e.to_string()).await;
                return Err(e);
            }
        };
        let mut rows: u64 = 0;
        let mut stream = match df.execute_stream().await {
            Ok(stream) => stream,
            Err(e) => {
                let _ = store.mark_failed(&key, "split-0", &e.to_string()).await;
                return Err(AppError::DataFusion(e));
            }
        };
        use futures::StreamExt as _;
        while let Some(batch) = stream.next().await {
            match batch {
                Ok(batch) => rows += batch.num_rows() as u64,
                Err(e) => {
                    let _ = store.mark_failed(&key, "split-0", &e.to_string()).await;
                    return Err(AppError::DataFusion(e));
                }
            }
        }
        store.mark_completed(&key, "split-0", rows).await?;
        Ok(RunOutcome {
            rows_extracted: rows,
            splits_completed: 1,
            splits_total: 1,
            workers: None,
        })
    }

    /// Build the cost-aware provider for this job's table (schema discovery +
    /// statistics through the shared budgeted pool). Shared by local filtered
    /// extraction and [`Pipeline::explain_filters`] so preview and execution use
    /// the identical provider.
    async fn filtered_provider(&self) -> Result<PostgresTableProvider, AppError> {
        let policy = PushdownPolicy::parse(&self.config.pushdown.policy);
        let descriptor = PostgresConnectionDescriptor::from_config(&self.config.source, 1);
        let provider = PostgresTableProvider::new(
            descriptor,
            &self.config.resolved_table(),
            policy,
            self.config.pushdown.deny.clone(),
            self.config.pushdown.push.clone(),
            CostParams {
                max_source_cost: self.config.pushdown.max_source_cost,
                keep_threshold: self.config.pushdown.keep_threshold,
            },
            self.config.pushdown.statistics_ttl_secs,
            self.config.execution.batch_size,
        )
        .await
        .map_err(AppError::Extractor)?;

        let strategy = ParallelStrategy::parse(&self.config.parallel_scan.strategy);
        Ok(provider
            .with_parallel_strategy(strategy)
            .with_max_batch_bytes(self.config.execution.max_batch_bytes)
            .with_use_copy(self.config.execution.use_copy))
    }

    /// Build the local filtered DataFrame: register a cost-aware provider and apply the
    /// config's `filters` as DataFusion predicates.
    async fn filtered_dataframe_local(&self) -> Result<datafusion::prelude::DataFrame, AppError> {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rule(Arc::new(SourceAwarePushdownRule))
            .build();
        let ctx = SessionContext::new_with_state(state);

        let provider = self.filtered_provider().await?;
        let exprs = self.filter_exprs_with_schema(&provider.schema())?;
        ctx.register_table(&self.config.table, Arc::new(provider))?;
        let mut df = ctx.table(&self.config.table).await?;
        for expr in exprs {
            df = df.filter(expr)?;
        }
        // Config-level column projection, if any.
        if let Some(columns) = &self.config.columns {
            let exprs: Vec<Expr> = columns.iter().map(|c| col(c.as_str())).collect();
            df = df.select(exprs)?;
        }
        Ok(df)
    }

    // ----- distributed (Ballista) -----

    /// Extract via a Ballista cluster and return the batches. No checkpoint side effects.
    /// Config `filters` are applied as DataFusion predicates (pushed to the source).
    pub async fn extract_distributed(
        &self,
        workers: Option<usize>,
        scheduler_url: Option<&str>,
    ) -> Result<Vec<RecordBatch>, AppError> {
        let ctx = self.make_distributed_ctx(workers, scheduler_url).await?;
        let mut df = ctx.session.table(&self.config.table).await?;
        let schema = df.schema().inner().clone();
        for expr in self.filter_exprs_with_schema(&schema)? {
            df = df.filter(expr)?;
        }
        if let Some(columns) = &self.config.columns {
            let exprs: Vec<Expr> = columns.iter().map(|c| col(c.as_str())).collect();
            df = df.select(exprs)?;
        }
        df.collect().await.map_err(AppError::DataFusion)
    }

    /// Run the operational distributed job with split checkpointing. The distributed scan
    /// is one logical split executed by the cluster (`workers` recorded in the outcome);
    /// per-task execution lives in Ballista.
    ///
    /// Bounded memory: the cluster result streams (`execute_stream`) with per-batch
    /// counting and driver-side progress reporting — batches are dropped immediately.
    pub async fn run_distributed(
        &self,
        workers: Option<usize>,
        scheduler_url: Option<&str>,
    ) -> Result<RunOutcome, AppError> {
        let store = JsonCheckpointStore::new(&self.config.checkpoint.dir)?;
        let key = JobKey::new(self.config.job_id.clone());
        let split_ids = vec!["split-0".to_string()];
        let checkpoint = store.begin(&key, &split_ids).await?;
        if checkpoint
            .splits
            .iter()
            .any(|s| s.split_id == "split-0" && s.state == crate::checkpoint::SplitState::Completed)
        {
            let prev = checkpoint
                .splits
                .iter()
                .find(|s| s.split_id == "split-0")
                .expect("checked");
            let ctx_workers = workers.unwrap_or(self.config.distributed.workers);
            log::info!(
                "job '{}': split-0 already completed, skipping",
                self.config.job_id
            );
            return Ok(RunOutcome {
                rows_extracted: prev.rows_extracted,
                splits_completed: 1,
                splits_total: 1,
                workers: Some(ctx_workers),
            });
        }

        store.mark_running(&key, "split-0").await?;
        let ctx = self.make_distributed_ctx(workers, scheduler_url).await?;
        let ctx_workers = ctx.workers;

        // Driver-owned progress: executor tasks stream batches back; the driver counts
        // rows and its background task persists advisory progress. Workers never touch
        // the checkpoint dir and never wait for a progress write.
        let flusher = ProgressFlusher::start(
            self.config.checkpoint.dir.clone(),
            self.config.job_id.clone(),
            1,
            std::time::Duration::from_secs(self.config.checkpoint.flush_interval_secs.max(1)),
            self.config.checkpoint.flush_rows.max(1),
        );
        let reporter = flusher.reporter();
        let report_every = self.config.checkpoint.flush_rows.max(1);

        let mut df = match ctx.session.table(&self.config.table).await {
            Ok(df) => df,
            Err(e) => {
                flusher.shutdown().await;
                let _ = store.mark_failed(&key, "split-0", &e.to_string()).await;
                return Err(AppError::DataFusion(e));
            }
        };
        let schema = df.schema().inner().clone();
        let exprs = match self.filter_exprs_with_schema(&schema) {
            Ok(exprs) => exprs,
            Err(e) => {
                flusher.shutdown().await;
                let _ = store.mark_failed(&key, "split-0", &e.to_string()).await;
                return Err(e);
            }
        };
        for expr in exprs {
            match df.filter(expr) {
                Ok(next) => df = next,
                Err(e) => {
                    flusher.shutdown().await;
                    let _ = store.mark_failed(&key, "split-0", &e.to_string()).await;
                    return Err(AppError::DataFusion(e));
                }
            }
        }
        if let Some(columns) = &self.config.columns {
            let exprs: Vec<Expr> = columns.iter().map(|c| col(c.as_str())).collect();
            match df.select(exprs) {
                Ok(next) => df = next,
                Err(e) => {
                    flusher.shutdown().await;
                    let _ = store.mark_failed(&key, "split-0", &e.to_string()).await;
                    return Err(AppError::DataFusion(e));
                }
            }
        }
        let mut stream = match df.execute_stream().await {
            Ok(stream) => stream,
            Err(e) => {
                flusher.shutdown().await;
                // Release the split so the next run retries it instead of wedging.
                let _ = store.mark_failed(&key, "split-0", &e.to_string()).await;
                return Err(AppError::DataFusion(e));
            }
        };
        let mut rows: u64 = 0;
        let mut since_report: u64 = 0;
        use futures::StreamExt as _;
        while let Some(batch) = stream.next().await {
            match batch {
                Ok(batch) => {
                    let n = batch.num_rows() as u64;
                    rows += n;
                    since_report += n;
                    if since_report >= report_every {
                        reporter.try_add_rows(since_report);
                        since_report = 0;
                    }
                }
                Err(e) => {
                    flusher.shutdown().await;
                    // Release the split so the next run retries it instead of wedging.
                    let _ = store.mark_failed(&key, "split-0", &e.to_string()).await;
                    return Err(AppError::DataFusion(e));
                }
            }
        }
        if since_report > 0 {
            reporter.try_add_rows(since_report);
        }
        store.mark_completed(&key, "split-0", rows).await?;
        reporter.try_report(PartitionStatus::completed(0, "split-0", rows));
        flusher.shutdown().await;

        Ok(RunOutcome {
            rows_extracted: rows,
            splits_completed: 1,
            splits_total: 1,
            workers: Some(ctx_workers),
        })
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

    /// The shared budgeted pool for this job (used by tests/diagnostics).
    #[allow(dead_code)]
    pub(crate) fn budgeted_pool(&self, workers_for_pool: usize) -> Result<sqlx::PgPool, AppError> {
        let descriptor =
            PostgresConnectionDescriptor::from_config(&self.config.source, workers_for_pool);
        registry().pool(&descriptor).map_err(AppError::Extractor)
    }
}

/// Resolve one [`FilterInput`] to its [`FilterSpec`] — shorthand strings parsed,
/// structured entries passed through. Shared by the single and OR-group paths so
/// both JSON forms lower identically.
fn resolve_input(input: &FilterInput) -> Result<FilterSpec, AppError> {
    match input {
        FilterInput::Shorthand(raw) => parse_filter_shorthand(raw),
        FilterInput::Structured(spec) => Ok(spec.clone()),
    }
}

/// Fold one AND-conjunct's branch predicates into a single `Expr`: a singleton
/// stays as-is, an OR-group becomes `a OR b OR ...` left-associatively.
fn or_group(exprs: Vec<Expr>) -> Result<Expr, AppError> {
    let mut iter = exprs.into_iter();
    let Some(first) = iter.next() else {
        return Err(AppError::Config(
            "filters contains an empty OR-group (inner array must hold >= 1 predicate)".to_string(),
        ));
    };
    Ok(iter.fold(first, |acc, e| acc.or(e)))
}

/// Canonical display for one AND-conjunct: a singleton renders as its spec,
/// an OR-group as `(a OR b)`.
fn describe_group(group: &[FilterSpec]) -> String {
    match group {
        [] => "(empty OR-group)".to_string(),
        [single] => single.describe(),
        _ => format!(
            "({})",
            group
                .iter()
                .map(FilterSpec::describe)
                .collect::<Vec<_>>()
                .join(" OR ")
        ),
    }
}

/// Parse a shorthand filter `column<op>value` where `<op>` is one of
/// `= != > >= < <=` into a [`FilterSpec`]. Longer operators are checked before
/// their single-character prefixes (`!=`/`>=`/`<=` before `=`/`>`/`<`) so e.g.
/// `amount>=100` doesn't get mis-split on the `=`. Shared by the pipeline
/// (config `filters`) and the CLI (`--filter` flags); both funnel through here
/// so they can never disagree.
pub fn parse_filter_shorthand(raw: &str) -> Result<FilterSpec, AppError> {
    const OPS: [(&str, usize, FilterOp); 6] = [
        ("!=", 2, FilterOp::NotEq),
        (">=", 2, FilterOp::GtEq),
        ("<=", 2, FilterOp::LtEq),
        ("=", 1, FilterOp::Eq),
        (">", 1, FilterOp::Gt),
        ("<", 1, FilterOp::Lt),
    ];

    for (op_str, op_len, op) in OPS {
        if let Some(idx) = raw.find(op_str) {
            let column = raw[..idx].trim();
            if column.is_empty() {
                break;
            }
            let value_str = raw[idx + op_len..].trim();
            return Ok(FilterSpec {
                column: column.to_string(),
                op,
                value: shorthand_value(value_str),
            });
        }
    }

    Err(AppError::Config(format!(
        "cannot parse filter '{raw}' — expected 'column<op>value' with op one of = != > >= < <="
    )))
}

/// Parse a shorthand filter all the way to a DataFusion predicate (no schema
/// coercion — string values stay text). Prefer [`Pipeline::filter_exprs`] and
/// [`Pipeline::filter_exprs_with_schema`] on the pipeline itself.
pub fn parse_filter_expr(raw: &str) -> Result<Expr, AppError> {
    parse_filter_shorthand(raw)?.to_expr()
}

/// Infer a JSON value from a shorthand value string, mirroring JSON typing:
/// integers, floats, and booleans become their native types, everything else
/// stays text (quotes are stripped first, so `'PAID'` and `PAID` agree).
fn shorthand_value(raw: &str) -> serde_json::Value {
    let unquoted = raw.trim_matches('\'').trim_matches('"');

    if let Ok(v) = unquoted.parse::<i64>() {
        v.into()
    } else if let Ok(v) = unquoted.parse::<f64>() {
        serde_json::Number::from_f64(v).map_or_else(
            || serde_json::Value::String(unquoted.to_string()),
            serde_json::Value::Number,
        )
    } else if unquoted.eq_ignore_ascii_case("true") {
        true.into()
    } else if unquoted.eq_ignore_ascii_case("false") {
        false.into()
    } else {
        serde_json::Value::String(unquoted.to_string())
    }
}

impl FilterSpec {
    /// Canonical display (`status = 'PAID'`) for logs, `rel plan`, and previews.
    pub fn describe(&self) -> String {
        match self.op {
            FilterOp::IsNull => format!("{} is null", self.column.trim()),
            FilterOp::IsNotNull => format!("{} is not null", self.column.trim()),
            _ => format!(
                "{} {} {}",
                self.column.trim(),
                self.op.as_str(),
                render_filter_json_value(&self.value)
            ),
        }
    }

    /// Lower to a DataFusion predicate from the JSON value types alone: numbers
    /// become int/float literals, booleans boolean literals, strings text
    /// literals, null an `IS NULL` / `IS NOT NULL` check.
    pub fn to_expr(&self) -> Result<Expr, AppError> {
        self.to_expr_with_type(None)
    }

    /// Like [`FilterSpec::to_expr`], but a string value is coerced when the
    /// column's Arrow type says more: an RFC3339 (or `%Y-%m-%d`) string on a
    /// timestamp column becomes a real timestamp literal that pushes to the
    /// source; a `%Y-%m-%d` string on a date column becomes a date literal that
    /// evaluates correctly in Arrow (date-literal pushdown is deferred — only
    /// timestamp literals translate today). Anything else falls back to
    /// [`FilterSpec::to_expr`].
    pub fn to_expr_with_type(&self, dtype: Option<&DataType>) -> Result<Expr, AppError> {
        let column = self.column.trim();
        if column.is_empty() {
            return Err(AppError::Config(
                "filter column must not be empty".to_string(),
            ));
        }

        match self.op {
            FilterOp::IsNull => return Ok(col(column).is_null()),
            FilterOp::IsNotNull => return Ok(col(column).is_not_null()),
            _ => {}
        }

        let value = match &self.value {
            serde_json::Value::Null => match self.op {
                FilterOp::Eq => return Ok(col(column).is_null()),
                FilterOp::NotEq => return Ok(col(column).is_not_null()),
                _ => {
                    return Err(AppError::Config(format!(
                        "filter '{column}' uses null with '{}': only '=' / '!=' apply to null (or use is_null)",
                        self.op.as_str()
                    )));
                }
            },
            serde_json::Value::Bool(b) => lit(*b),
            serde_json::Value::Number(n) => {
                if let Some(v) = n.as_i64() {
                    lit(v)
                } else if let Some(v) = n.as_f64() {
                    lit(v)
                } else {
                    return Err(AppError::Config(format!(
                        "filter '{column}' has an out-of-range numeric value: {n}"
                    )));
                }
            }
            serde_json::Value::String(s) => {
                if let Some(expr) = coerce_string_to_type(s, dtype) {
                    expr
                } else {
                    lit(s.as_str())
                }
            }
            _ => {
                return Err(AppError::Config(format!(
                    "filter '{column}' value must be a number, boolean, string, or null"
                )));
            }
        };

        Ok(match self.op {
            FilterOp::Eq => col(column).eq(value),
            FilterOp::NotEq => col(column).not_eq(value),
            FilterOp::Gt => col(column).gt(value),
            FilterOp::GtEq => col(column).gt_eq(value),
            FilterOp::Lt => col(column).lt(value),
            FilterOp::LtEq => col(column).lt_eq(value),
            FilterOp::IsNull | FilterOp::IsNotNull => unreachable!("handled above"),
        })
    }
}

fn render_filter_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{s}'"),
        _ => "(complex value)".to_string(),
    }
}

/// Try to read a string as a timestamp literal for the given column type.
/// Accepts RFC3339 (`2026-01-01T00:00:00Z`) and, for convenience, plain
/// `%Y-%m-%d` dates (midnight UTC). Returns `None` when the string is not a
/// timestamp or the column type is not temporal — the caller keeps the text
/// literal instead.
fn coerce_string_to_type(s: &str, dtype: Option<&DataType>) -> Option<Expr> {
    let dtype = dtype?;
    let s = s.trim();
    match dtype {
        DataType::Timestamp(unit, tz) => {
            let micros = parse_timestamp_micros(s)?;
            let value = match unit {
                TimeUnit::Second => {
                    ScalarValue::TimestampSecond(Some(micros / 1_000_000), tz.clone())
                }
                TimeUnit::Millisecond => {
                    ScalarValue::TimestampMillisecond(Some(micros / 1_000), tz.clone())
                }
                TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(micros), tz.clone())
                }
                TimeUnit::Nanosecond => {
                    ScalarValue::TimestampNanosecond(Some(micros * 1_000), tz.clone())
                }
            };
            Some(lit(value))
        }
        DataType::Date32 => {
            let days = parse_date_days(s)?;
            Some(lit(ScalarValue::Date32(Some(days))))
        }
        DataType::Date64 => {
            let days = parse_date_days(s)?;
            Some(lit(ScalarValue::Date64(Some(days as i64 * 86_400_000))))
        }
        _ => None,
    }
}

fn parse_timestamp_micros(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_micros());
    }
    // Plain dates read as midnight UTC — the common daily-range shorthand.
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(d.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros());
    }
    None
}

fn parse_date_days(s: &str) -> Option<i32> {
    let d = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()?;
    let days = d
        .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?)
        .num_days();
    i32::try_from(days).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_config_file_loads_extract_job() {
        let pipeline =
            Pipeline::from_config_file("examples/configs/full_extract.example.json").unwrap();
        assert_eq!(pipeline.config().job_id, "orders_full");
    }

    #[test]
    fn example_config_filters_parse_to_pushable_predicates() {
        // extract.example.json ships four ANDed structured filters (the pure-AND
        // case). OR-group lowering is pinned separately by
        // `or_group_lowers_to_single_or_expr`, which builds its config
        // synthetically instead of coupling to this fixture.
        let config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        let pipeline = Pipeline::from_config(config);
        assert_eq!(pipeline.config().job_id, "orders_extract");
        // Flat specs: 4 predicates across 4 AND-conjuncts.
        let specs = pipeline.filter_specs().unwrap();
        assert_eq!(specs.len(), 4);
        assert_eq!(specs[0].describe(), "status = 'PAID'");
        assert_eq!(specs[1].describe(), "amount > 100");
        assert_eq!(specs[2].describe(), "updated_at >= '2026-01-01T00:00:00Z'");
        assert_eq!(specs[3].describe(), "user_id >= 500");
        // Grouped: 4 singleton conjuncts, each lowering to its own expression.
        let groups = pipeline.filter_groups().unwrap();
        assert_eq!(groups.len(), 4);
        assert!(groups.iter().all(|g| g.len() == 1));
        let exprs = pipeline.filter_exprs().unwrap();
        assert_eq!(exprs.len(), 4);
        assert_eq!(exprs[0], col("status").eq(lit("PAID")));
        assert_eq!(exprs[1], col("amount").gt(lit(100i64)));
    }

    #[test]
    fn or_group_lowers_to_single_or_expr() {
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.filters = vec![FilterEntry::OrGroup(vec![
            FilterInput::Shorthand("status=PAID".to_string()),
            FilterInput::Shorthand("amount>100".to_string()),
        ])];
        let pipeline = Pipeline::from_config(config);
        let exprs = pipeline.filter_exprs().unwrap();
        assert_eq!(exprs.len(), 1);
        assert_eq!(
            exprs[0],
            col("status")
                .eq(lit("PAID"))
                .or(col("amount").gt(lit(100i64)))
        );
        let decisions = pipeline.filter_groups().unwrap();
        assert_eq!(
            describe_group(&decisions[0]),
            "(status = 'PAID' OR amount > 100)"
        );
    }

    #[test]
    fn empty_or_group_is_config_error_not_empty_scan() {
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.filters = vec![FilterEntry::OrGroup(vec![])];
        let pipeline = Pipeline::from_config(config);
        assert!(pipeline.filter_exprs().is_err());
        assert!(pipeline.filter_groups().is_err());
    }

    #[test]
    fn parse_filter_expr_shapes() {
        let expr = parse_filter_expr("status=PAID").unwrap();
        assert_eq!(expr, col("status").eq(lit("PAID")));
        let expr = parse_filter_expr("amount>=100").unwrap();
        assert_eq!(expr, col("amount").gt_eq(lit(100i64)));
        assert!(parse_filter_expr("not-a-filter").is_err());
    }

    #[test]
    fn filter_exprs_parses_every_config_filter() {
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.filters = vec![
            FilterEntry::Single(FilterInput::Shorthand("status=PAID".to_string())),
            FilterEntry::Single(FilterInput::Shorthand("amount>100".to_string())),
        ];
        let pipeline = Pipeline::from_config(config);
        let exprs = pipeline.filter_exprs().unwrap();
        assert_eq!(exprs.len(), 2);
        assert_eq!(exprs[0], col("status").eq(lit("PAID")));
        assert_eq!(exprs[1], col("amount").gt(lit(100i64)));
    }

    #[test]
    fn filter_exprs_surfaces_the_bad_filter() {
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.filters = vec![
            FilterEntry::Single(FilterInput::Shorthand("status=PAID".to_string())),
            FilterEntry::Single(FilterInput::Shorthand("not-a-filter".to_string())),
        ];
        let pipeline = Pipeline::from_config(config);
        let err = pipeline.filter_exprs().unwrap_err();
        assert!(err.to_string().contains("not-a-filter"));
    }

    #[test]
    fn filter_exprs_empty_means_full_extraction() {
        let config = JobConfig::from_file("examples/configs/full_extract.example.json").unwrap();
        let pipeline = Pipeline::from_config(config);
        assert!(pipeline.filter_exprs().unwrap().is_empty());
    }

    fn mk_spec(column: &str, op: FilterOp, value: serde_json::Value) -> FilterSpec {
        FilterSpec {
            column: column.to_string(),
            op,
            value,
        }
    }

    #[test]
    fn filter_spec_lowering_follows_json_types() {
        assert_eq!(
            mk_spec("a", FilterOp::Eq, serde_json::json!(100))
                .to_expr()
                .unwrap(),
            col("a").eq(lit(100i64))
        );
        assert_eq!(
            mk_spec("a", FilterOp::Gt, serde_json::json!(1.5))
                .to_expr()
                .unwrap(),
            col("a").gt(lit(1.5f64))
        );
        assert_eq!(
            mk_spec("a", FilterOp::Eq, serde_json::json!(true))
                .to_expr()
                .unwrap(),
            col("a").eq(lit(true))
        );
        assert_eq!(
            mk_spec("a", FilterOp::Eq, serde_json::json!("PAID"))
                .to_expr()
                .unwrap(),
            col("a").eq(lit("PAID"))
        );
        assert_eq!(
            mk_spec("a", FilterOp::Eq, serde_json::json!(null))
                .to_expr()
                .unwrap(),
            col("a").is_null()
        );
        assert_eq!(
            mk_spec("a", FilterOp::NotEq, serde_json::json!(null))
                .to_expr()
                .unwrap(),
            col("a").is_not_null()
        );
        assert_eq!(
            mk_spec("a", FilterOp::IsNull, serde_json::json!(null))
                .to_expr()
                .unwrap(),
            col("a").is_null()
        );
    }

    #[test]
    fn filter_spec_rejects_misused_null_and_nested_values() {
        assert!(
            mk_spec("a", FilterOp::Gt, serde_json::json!(null))
                .to_expr()
                .is_err()
        );
        assert!(
            mk_spec("a", FilterOp::Eq, serde_json::json!({"n": 1}))
                .to_expr()
                .is_err()
        );
        assert!(
            mk_spec("", FilterOp::Eq, serde_json::json!(1))
                .to_expr()
                .is_err()
        );
    }

    #[test]
    fn shorthand_and_structured_lower_identically() {
        // The two JSON forms must agree predicate-for-predicate.
        for raw in ["status=PAID", "amount>=100", "flag=true", "ratio<1.5"] {
            let from_shorthand = parse_filter_expr(raw).unwrap();
            let spec = parse_filter_shorthand(raw).unwrap();
            assert_eq!(from_shorthand, spec.to_expr().unwrap(), "{raw}");
            assert_eq!(spec.describe(), spec.describe());
        }
    }

    #[test]
    fn timestamp_string_coerces_with_schema_but_not_without() {
        use arrow::datatypes::{DataType, TimeUnit};

        let spec = parse_filter_shorthand("updated_at>=2026-01-01T00:00:00Z").unwrap();
        // Without schema knowledge the bound stays text (documented limitation).
        assert_eq!(
            spec.to_expr().unwrap(),
            col("updated_at").gt_eq(lit("2026-01-01T00:00:00Z"))
        );

        // With a timestamp column type it becomes a real timestamp literal that pushes.
        let dtype = DataType::Timestamp(TimeUnit::Microsecond, None);
        let expected_micros = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .timestamp_micros();
        assert_eq!(
            spec.to_expr_with_type(Some(&dtype)).unwrap(),
            col("updated_at").gt_eq(lit(ScalarValue::TimestampMicrosecond(
                Some(expected_micros),
                None
            )))
        );

        // Plain dates read as midnight UTC.
        let day = parse_filter_shorthand("updated_at>=2026-01-02").unwrap();
        let expected_day = chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(
            day.to_expr_with_type(Some(&dtype)).unwrap(),
            col("updated_at").gt_eq(lit(ScalarValue::TimestampMicrosecond(
                Some(expected_day),
                None
            )))
        );

        // A text column keeps the string — coercion never fires blindly.
        assert_eq!(
            spec.to_expr_with_type(Some(&DataType::Utf8)).unwrap(),
            col("updated_at").gt_eq(lit("2026-01-01T00:00:00Z"))
        );

        // Date columns coerce date strings.
        let holiday = mk_spec("day", FilterOp::Eq, serde_json::json!("2024-02-29"));
        let expected_days = chrono::NaiveDate::from_ymd_opt(2024, 2, 29)
            .unwrap()
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        assert_eq!(
            holiday.to_expr_with_type(Some(&DataType::Date32)).unwrap(),
            col("day").eq(lit(ScalarValue::Date32(Some(expected_days))))
        );
    }

    #[test]
    fn filter_specs_mixes_both_json_forms() {
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.filters = vec![
            FilterEntry::Single(FilterInput::Shorthand("status=PAID".to_string())),
            FilterEntry::Single(FilterInput::Structured(mk_spec(
                "amount",
                FilterOp::Gt,
                serde_json::json!(100),
            ))),
        ];
        let pipeline = Pipeline::from_config(config);
        let specs = pipeline.filter_specs().unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].column, "status");
        assert_eq!(specs[0].describe(), "status = 'PAID'");
        assert_eq!(specs[1].describe(), "amount > 100");
        let exprs = pipeline.filter_exprs().unwrap();
        assert_eq!(exprs[1], col("amount").gt(lit(100i64)));
    }
}
