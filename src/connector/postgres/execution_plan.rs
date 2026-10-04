use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::execution_plan::{Boundedness, EmissionType},
    physical_plan::stream::RecordBatchStreamAdapter,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
    },
};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, QueryBuilder};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore, mpsc};
use tracing::{Instrument, debug, info_span, warn};

use crate::connector::errors::ExtractorError;
use crate::connector::postgres::copy::copy_scan;
use crate::connector::postgres::distributed::connection::PostgresConnectionDescriptor;
use crate::connector::postgres::distributed::pool_registry::registry;
use crate::connector::postgres::extractor::{cursor_scan, declare_prefix, new_cursor_name};
use crate::connector::postgres::{
    parallel::ScanPartition, query_builder::PostgresQueryBuilder, row_adapter,
};
use crate::connector::query_tag::QuerySession;
use crate::pushdown::Predicate;
use crate::types::table_metadata::TableMetadata;

/// Default byte cap per Arrow batch flushed by executor tasks (16 MiB).
pub(crate) const DEFAULT_EXEC_BATCH_BYTES: usize = 16 * 1024 * 1024;

fn default_exec_batch_bytes() -> usize {
    DEFAULT_EXEC_BATCH_BYTES
}

/// The serializable form of a `PostgresExecutionPlan` — what gets embedded in the Ballista
/// physical plan sent from the scheduler to each executor (see `distributed::plan_codec`).
/// Passwords never travel here: only the descriptor carries the password's environment
/// variable name, resolved at pool-creation time in the executing process.
///
/// CHECKPOINT OWNERSHIP: executor tasks (this plan's `execute`) never touch any
/// `CheckpointStore`. They stream `RecordBatch`es back to the driver; the driver alone
/// records per-split state (after its consumer acknowledged each split) and advisory
/// progress (`checkpoint::progress`). This keeps exactly one writer per job and workers
/// never block on file I/O.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresExecutionPlanModel {
    pub descriptor: PostgresConnectionDescriptor,
    pub table_metadata: TableMetadata,
    pub pushed_filters: Vec<Predicate>,
    pub pushed_limit: Option<usize>,
    pub batch_size: usize,
    /// Byte cap per flushed batch. `#[serde(default)]` keeps old serialized plans readable.
    #[serde(default = "default_exec_batch_bytes")]
    pub max_batch_bytes: usize,
    /// Prefer `COPY … TO STDOUT (FORMAT BINARY)` over cursor `SELECT` when the scan
    /// shape allows it (no pushed filters). `#[serde(default)]` keeps old plans readable.
    #[serde(default)]
    pub use_copy: bool,
    /// `statement_timeout` override for COPY scans; `#[serde(default)]` for old plans.
    #[serde(default)]
    pub copy_statement_timeout_ms: Option<u64>,
    pub partitions: Vec<ScanPartition>,
    /// Debug identity for the SQL comment tag (see `connector::query_tag`) — carried across
    /// the wire so every executor tags its partition's query with the *same* run_id the
    /// scheduler generated, rather than each process inventing its own.
    pub run_id: String,
}

/// A streaming scan of one Postgres table. Each DataFusion/Ballista partition runs as a
/// cursor (`DECLARE … CURSOR` + `FETCH FORWARD <batch_size>` windows, each its own
/// statement, so `statement_timeout` applies per window) or, with `use_copy` and no pushed
/// filters, as one binary `COPY` statement (subject to `statement_timeout` as a whole).
/// Dropping the output stream stops the source: the cursor is closed and its transaction
/// rolled back after at most one in-flight window; an unfinished COPY's connection is
/// closed and its backend cancelled (`pg_cancel_backend`). Each partition reads its own
/// snapshot; partitions are not mutually consistent (see `extractor` module docs).
///
/// Whenever more than one partition is
/// embedded (keyset bounds computed by the scheduler in `PostgresTableProvider::scan`), each
/// Ballista task scans exactly one `ScanPartition`, so the table is read, not copied, as it
/// scales across executor processes — and the source sees the same number of connections
/// whether it runs on one machine or three.
#[derive(Debug)]
pub struct PostgresExecutionPlan {
    /// `None` only when created through the (legacy, in-process-only) test constructor.
    /// Serialized plans always carry the source descriptor so the executing process can open
    /// its share of the connection budget.
    descriptor: Option<PostgresConnectionDescriptor>,
    table_metadata: TableMetadata,
    schema: Arc<Schema>,
    properties: Arc<PlanProperties>,
    pushed_filters: Vec<Predicate>,
    pushed_limit: Option<usize>,
    batch_size: usize,
    max_batch_bytes: usize,
    use_copy: bool,
    copy_statement_timeout_ms: Option<u64>,
    partitions: Vec<ScanPartition>,
    query_session: QuerySession,
}

/// How a `LIMIT` renders: bound (`$n`, for `SELECT`) or inlined (for `COPY`,
/// which accepts no bind parameters — the value is a `usize` this crate computed,
/// never user text).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LimitRender {
    Bind,
    Inline,
}

/// One partition's binary COPY: its inner `SELECT` (described first, so the server's column
/// types are checked against the decoders) and the `COPY (…) TO STDOUT` that streams it.
pub(crate) struct CopyStatements {
    pub(crate) select: String,
    pub(crate) copy: String,
}

/// What one partition's task runs.
enum PartitionScan {
    Copy(CopyStatements),
    Cursor {
        cursor_name: String,
        declare: QueryBuilder<Postgres>,
    },
}

impl PostgresExecutionPlan {
    // Justification (AGENTS §1): the parameters are the independent scan knobs (source,
    // projection, bounds, batch/byte caps, callback); a params struct would only rename them.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        descriptor: Option<PostgresConnectionDescriptor>,
        table_metadata: TableMetadata,
        schema: Arc<Schema>,
        pushed_filters: Vec<Predicate>,
        pushed_limit: Option<usize>,
        batch_size: usize,
        partitions: Vec<ScanPartition>,
        run_id: String,
    ) -> DataFusionResult<Self> {
        // Pipeline label for the debug SQL comment tag (`connector::query_tag`): reuses
        // `application_name`, already threaded through config to identify the Postgres
        // connection itself, rather than plumbing a second identifier through for the same
        // purpose. `run_id` is caller-supplied (not generated here) so it can be shared
        // across every executor task working the same logical scan — see the model's
        // `run_id` field doc.
        let pipeline = descriptor
            .as_ref()
            .map(|d| d.application_name.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let query_session = QuerySession::from_parts(pipeline, run_id);
        // Ballista schedules one task per partition. A single (or empty) partition list means
        // one task; several means several — the keyset-scan parallelism of docs/roadmap.md
        // Phase 3 carried over unchanged into the distributed phase.
        let partition_count = if partitions.len() > 1 {
            partitions.len()
        } else {
            1
        };

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            descriptor,
            table_metadata,
            schema,
            properties,
            pushed_filters,
            pushed_limit,
            batch_size,
            max_batch_bytes: DEFAULT_EXEC_BATCH_BYTES,
            use_copy: false,
            copy_statement_timeout_ms: None,
            partitions,
            query_session,
        })
    }

    /// Override the byte cap after construction (e.g. from `execution.max_batch_bytes`).
    /// Builder-style so `try_new`'s signature — and every existing call site — stays stable.
    #[must_use]
    pub(crate) fn with_max_batch_bytes(mut self, max_batch_bytes: usize) -> Self {
        self.max_batch_bytes = max_batch_bytes.max(1);
        self
    }

    /// Prefer binary `COPY` over cursor `SELECT` when the scan shape allows it.
    /// Builder-style, same stability rationale as [`Self::with_max_batch_bytes`].
    #[must_use]
    pub(crate) fn with_use_copy(mut self, use_copy: bool) -> Self {
        self.use_copy = use_copy;
        self
    }

    /// `statement_timeout` (ms) for this plan's COPY scans instead of the session's; `None`
    /// keeps the session timeout, `Some(0)` disables it. Cursor scans are unaffected.
    #[must_use]
    pub(crate) fn with_copy_statement_timeout_ms(mut self, timeout_ms: Option<u64>) -> Self {
        self.copy_statement_timeout_ms = timeout_ms;
        self
    }

    pub(crate) fn to_model(&self) -> DataFusionResult<PostgresExecutionPlanModel> {
        let descriptor = self.descriptor.clone().ok_or_else(|| {
            DataFusionError::NotImplemented(
                "a PostgresExecutionPlan without a source descriptor cannot be serialized"
                    .to_string(),
            )
        })?;

        Ok(PostgresExecutionPlanModel {
            descriptor,
            table_metadata: self.table_metadata.clone(),
            pushed_filters: self.pushed_filters.clone(),
            pushed_limit: self.pushed_limit,
            batch_size: self.batch_size,
            max_batch_bytes: self.max_batch_bytes,
            use_copy: self.use_copy,
            copy_statement_timeout_ms: self.copy_statement_timeout_ms,
            partitions: self.partitions.clone(),
            run_id: self.query_session.run_id().to_string(),
        })
    }

    pub(crate) fn from_model(model: PostgresExecutionPlanModel) -> DataFusionResult<Self> {
        let schema = row_adapter::PostgresRowAdapter::build_arrow_schema(&model.table_metadata)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Self::try_new(
            Some(model.descriptor),
            model.table_metadata,
            schema,
            model.pushed_filters,
            model.pushed_limit,
            model.batch_size,
            model.partitions,
            model.run_id,
        )
        .map(|plan| {
            plan.with_max_batch_bytes(model.max_batch_bytes)
                .with_use_copy(model.use_copy)
                .with_copy_statement_timeout_ms(model.copy_statement_timeout_ms)
        })
    }

    /// Assembles the `WHERE`/`LIMIT` for one partition. Pushed predicates render through the
    /// backend-neutral [`Predicate::render_to`] into a Postgres sink, which binds each literal
    /// at render position (sqlx `push_bind` appends `$n` inline, so text and numbering align
    /// by construction); a MySQL connector would render the same way into its own sink. The
    /// partition bounds (composed in parallel.rs from source min/max queries) are inlined.
    /// Caller-provided filter ranges arrive as ordinary pushed predicates.
    ///
    /// The tagged `SELECT` itself is what [`Self::build_cursor_declare`] wraps in a
    /// `DECLARE`; this test-only form lets unit tests inspect it directly.
    ///
    /// [`Predicate::render_to`]: crate::pushdown::Predicate::render_to
    #[cfg(test)]
    pub(crate) fn build_query(&self, partition_idx: usize) -> QueryBuilder<Postgres> {
        let mut qb = QueryBuilder::<Postgres>::new(self.tag_for(partition_idx).render());
        self.push_select(&mut qb, partition_idx, LimitRender::Bind);
        qb
    }

    /// `{tag}DECLARE <cursor_name> CURSOR WITHOUT HOLD FOR SELECT …` for one partition,
    /// with pushed-filter literals and the limit bound as `$n` (DECLARE accepts binds).
    pub(crate) fn build_cursor_declare(
        &self,
        partition_idx: usize,
        tag: &str,
        cursor_name: &str,
    ) -> QueryBuilder<Postgres> {
        let mut qb = declare_prefix(tag, cursor_name);
        self.push_select(&mut qb, partition_idx, LimitRender::Bind);
        qb
    }

    /// Whether the scan shape allows binary COPY: no pushed filters (COPY takes no bind
    /// parameters) and a decoder for every projected column.
    fn copy_shape(&self) -> bool {
        self.pushed_filters.is_empty()
            && crate::connector::postgres::copy::supports_binary_copy(&self.table_metadata.columns)
    }

    /// Whether `execute` scans with binary COPY (requested, and the shape allows it) rather
    /// than a cursor. Shown in the plan display (`scan=copy|cursor`).
    fn scans_with_copy(&self) -> bool {
        self.use_copy && self.copy_shape()
    }

    /// `{tag}COPY (SELECT …) TO STDOUT (FORMAT BINARY)` for one partition, or `None` when
    /// COPY cannot run it: pushed filters render with bind parameters, which `COPY`
    /// forbids, so any pushed filter falls back to `SELECT` (loudly logged by the
    /// caller — benchmark comparisons must know which path executed). The projection
    /// must also be binary-decodable ([`supports_binary_copy`](crate::connector::postgres::copy::supports_binary_copy)).
    pub(crate) fn build_copy_sql(&self, partition_idx: usize, tag: &str) -> Option<CopyStatements> {
        if !self.copy_shape() {
            return None;
        }
        let mut inner = QueryBuilder::<Postgres>::new("");
        self.push_select(&mut inner, partition_idx, LimitRender::Inline);
        let select = inner.sql().as_str().to_string();
        let copy = format!("{tag}COPY ({select}) TO STDOUT (FORMAT BINARY)");
        Some(CopyStatements { select, copy })
    }

    /// The debug SQL-comment tag for one partition (see `connector::query_tag`).
    fn tag_for(&self, partition_idx: usize) -> crate::connector::query_tag::QueryTag {
        // Debug tag: identifies this exact statement on the Postgres instance itself
        // (pg_stat_activity, pg_stat_statements, logs) without cross-referencing anything
        // in this process. `strategy` names the scan shape; `partition` (1-based) is
        // included only when this table is actually split across more than one scan.
        let strategy = if self.pushed_filters.is_empty() {
            "full"
        } else {
            "full+pushdown"
        };
        let mut tag = self.query_session.tag(strategy);
        if self.partitions.len() > 1 {
            tag = tag.with_partition(partition_idx + 1, self.partitions.len());
        }
        tag
    }

    /// The `SELECT … WHERE … LIMIT …` body shared by [`Self::build_query`] (`Bind`)
    /// and [`Self::build_copy_sql`] (`Inline`).
    fn push_select(
        &self,
        qb: &mut QueryBuilder<Postgres>,
        partition_idx: usize,
        limit_render: LimitRender,
    ) {
        use crate::connector::postgres::dialect::PostgresDialect;
        use crate::connector::postgres::param_sink::PgParamSink;
        use crate::pushdown::{SqlParam, SqlSink};

        let dialect = PostgresDialect;

        PostgresQueryBuilder::build_full_table(&mut *qb, &self.table_metadata);

        let mut sink = PgParamSink::new(&mut *qb);
        {
            let sink = &mut sink;
            let mut conditions = 0u8;

            for predicate in &self.pushed_filters {
                if conditions == 0 {
                    sink.push_sql(" WHERE ");
                    conditions = 1;
                } else {
                    sink.push_sql(" AND ");
                }
                predicate.render_to(&dialect, sink);
            }

            if let Some(partition) = self.partitions.get(partition_idx)
                && let Some(bounds) = &partition.predicate
            {
                if conditions == 0 {
                    sink.push_sql(" WHERE ");
                } else {
                    sink.push_sql(" AND ");
                }
                // Rendered by `parallel` from integer key or page bounds and a quoted column
                // name — on a decoded plan too, which re-renders it from the typed bounds
                // (`ScanPartition` never deserializes SQL text) — so it is safe to inline.
                // Parenthesized: the first keyset partition is `(… ) OR "col" IS NULL`, which
                // must not bind to the pushed filters' `AND`.
                sink.push_sql("(");
                sink.push_sql(bounds.as_str());
                sink.push_sql(")");
            }

            if let Some(limit) = self.pushed_limit {
                match limit_render {
                    LimitRender::Bind => {
                        sink.push_sql(" LIMIT ");
                        sink.push_param(SqlParam::Int(limit as i64));
                    }
                    LimitRender::Inline => {
                        sink.push_sql(format!(" LIMIT {limit}").as_str());
                    }
                }
            }
        }
        // `PgParamSink` holds `&mut QueryBuilder`; dropping it only ends the borrow.
        // No explicit `drop` needed — let the borrow end naturally.
    }
}

impl DisplayAs for PostgresExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "PostgresExecutionPlan: table={} pushed_filters={} limit={:?} partitions={} scan={}",
                    self.table_metadata.table_name,
                    self.pushed_filters.len(),
                    self.pushed_limit,
                    self.partitions.len(),
                    if self.scans_with_copy() {
                        "copy"
                    } else {
                        "cursor"
                    },
                )
            }
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for PostgresExecutionPlan {
    fn name(&self) -> &str {
        "PostgresExecutionPlan"
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Resolve the process-shared, budgeted pool on first use. Every task in this process —
        // and thanks to the registry, every task in the whole process tree — shares the one
        // pool, so N executors still open only `budgeted_max_connections` connections.
        let (pool, scan_slots) = match &self.descriptor {
            Some(descriptor) => (
                registry()
                    .pool(descriptor)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
                registry()
                    .scan_slots(descriptor)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ),
            None => {
                return Err(DataFusionError::NotImplemented(
                    "a pool-less PostgresExecutionPlan cannot execute; \
                     scan through PostgresTableProvider instead"
                        .to_string(),
                ));
            }
        };

        // The scan runs in its own task feeding a small bounded channel (backpressure),
        // so dropping the output stream is observed by the task at its next send and it
        // can clean up the source (CLOSE + ROLLBACK, or close + cancel for COPY).
        let runtime = tokio::runtime::Handle::try_current().map_err(|e| {
            DataFusionError::Execution(format!("PostgresExecutionPlan needs a tokio runtime: {e}"))
        })?;

        let table_metadata = self.table_metadata.clone();
        let schema = self.schema.clone();
        let batch_size = self.batch_size;
        let max_batch_bytes = self.max_batch_bytes;
        let copy_statement_timeout_ms = self.copy_statement_timeout_ms;
        // One tag per partition scan: DECLARE/FETCH/CLOSE (or the COPY) share it, and the
        // COPY cancel matches the backend's running statement by it. Its `query_id` is the
        // scan span's, so log lines and `pg_stat_activity` line up.
        let query_tag = self.tag_for(partition);
        let span = info_span!("scan", partition, query_id = %query_tag.query_id());
        let _entered = span.enter();
        let tag = query_tag.render();
        // Binary COPY when enabled and the scan shape allows it (no pushed filters,
        // decodable projection); otherwise the cursor. The fallback is loud (warn)
        // because benchmark comparisons must know which path executed.
        let copy_sql = if self.use_copy {
            self.build_copy_sql(partition, &tag)
        } else {
            None
        };
        // Once per plan (the choice is the same for every partition).
        if self.use_copy && copy_sql.is_none() && partition == 0 {
            warn!(
                "use_copy requested, but the scan shape needs a cursor (pushed filters); \
                 scanning with the cursor"
            );
        } else if let Some(statements) = &copy_sql {
            debug!(sql = %statements.copy, "scan statement");
        }
        let scan = match copy_sql {
            Some(statements) => PartitionScan::Copy(statements),
            None => {
                let cursor_name = new_cursor_name();
                let declare = self.build_cursor_declare(partition, &tag, &cursor_name);
                debug!(sql = %declare.sql().as_str(), "scan statement");
                PartitionScan::Cursor {
                    cursor_name,
                    declare,
                }
            }
        };

        // NOTE: no checkpoint access here by design — see the model docs. This task
        // streams batches; the driver persists progress.
        let (tx, mut rx) = mpsc::channel::<DataFusionResult<RecordBatch>>(2);
        let task = runtime.spawn(
            async move {
                // The shared source budget: hold one of the process's `budgeted_max_connections`
                // scan slots until this partition's scan is finished.
                let _slot = match scan_slot(scan_slots, &tx).await {
                    None => return,
                    Some(Ok(slot)) => slot,
                    Some(Err(_)) => {
                        let _ = tx
                            .send(Err(DataFusionError::Execution(
                                "source scan limiter closed".to_string(),
                            )))
                            .await;
                        return;
                    }
                };
                let batch_tx = tx.clone();
                let send = move |batch: RecordBatch| {
                    let batch_tx = batch_tx.clone();
                    async move {
                        batch_tx.send(Ok(batch)).await.map_err(|_| {
                            ExtractorError::Internal("scan consumer dropped the stream".into())
                        })
                    }
                };
                let result = match scan {
                    PartitionScan::Copy(statements) => {
                        copy_scan(
                            &pool,
                            &statements,
                            &tag,
                            &table_metadata,
                            batch_size,
                            max_batch_bytes,
                            copy_statement_timeout_ms,
                            send,
                        )
                        .await
                    }
                    PartitionScan::Cursor {
                        cursor_name,
                        declare,
                    } => {
                        cursor_scan(
                            &pool,
                            &tag,
                            &cursor_name,
                            declare,
                            &table_metadata,
                            batch_size,
                            max_batch_bytes,
                            send,
                        )
                        .await
                    }
                };
                if let Err(e) = result {
                    if tx.is_closed() {
                        debug!(error = %e, "scan stopped: the consumer dropped the stream");
                    } else {
                        let _ = tx.send(Err(DataFusionError::External(Box::new(e)))).await;
                    }
                }
            }
            .instrument(span.clone()),
        );

        let stream = async_stream::stream! {
            while let Some(item) = rx.recv().await {
                yield item;
            }
            // Channel closed: the task finished (its error, if any, was already sent).
            // Surface a panic instead of ending the stream as if it were complete. If the
            // consumer drops this stream early, the handle is dropped with it and the
            // task finishes its source cleanup on its own (detached by design).
            if let Err(e) = task.await {
                yield Err(DataFusionError::External(Box::new(e)));
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Wait (no timeout) for one of the process's scan slots. `None` once the consumer has
/// dropped the stream: dropping it drops the scan task's `JoinHandle`, which detaches the task
/// rather than aborting it, so a partition still queued here must notice on its own and never
/// open its query. `Some(Err)` when the limiter is closed.
async fn scan_slot<T>(
    slots: Arc<Semaphore>,
    tx: &mpsc::Sender<T>,
) -> Option<Result<OwnedSemaphorePermit, AcquireError>> {
    let acquired = tokio::select! {
        slot = slots.acquire_owned() => slot,
        () = tx.closed() => return None,
    };
    // Both can be ready at once, and `select!` picks either: check before the scan starts.
    if tx.is_closed() {
        return None;
    }
    Some(acquired)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::postgres::parallel::keyset_partition;
    use crate::pushdown::{CmpOp, Literal, Predicate};
    use datafusion::physical_plan::displayable;

    #[tokio::test]
    async fn a_queued_scan_stops_waiting_when_the_consumer_leaves() {
        // Every slot is taken: the scan waits, then its consumer drops the stream.
        let slots = Arc::new(Semaphore::new(0));
        let (tx, rx) = mpsc::channel::<()>(1);
        let queued = tokio::spawn({
            let slots = Arc::clone(&slots);
            async move { scan_slot(slots, &tx).await.is_none() }
        });
        drop(rx);
        let gave_up = tokio::time::timeout(std::time::Duration::from_secs(5), queued)
            .await
            .expect("still waiting for a slot after the consumer left")
            .unwrap();
        assert!(gave_up);

        // A free slot and a consumer: the scan starts.
        let slots = Arc::new(Semaphore::new(1));
        let (tx, rx) = mpsc::channel::<()>(1);
        assert!(matches!(
            scan_slot(Arc::clone(&slots), &tx).await,
            Some(Ok(_))
        ));
        // A free slot but no consumer: it does not.
        drop(rx);
        assert!(scan_slot(Arc::clone(&slots), &tx).await.is_none());
        // A closed limiter with a consumer: the error case.
        let (tx, _rx) = mpsc::channel::<()>(1);
        slots.close();
        assert!(matches!(scan_slot(slots, &tx).await, Some(Err(_))));
    }

    #[test]
    fn test_build_query_binds_in_placeholder_order() {
        let schema = Arc::new(Schema::empty());
        let table_metadata = TableMetadata {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![],
        };

        let plan = PostgresExecutionPlan::try_new(
            None,
            table_metadata,
            schema,
            vec![Predicate::Cmp {
                left: Box::new(Predicate::Column("status".to_string())),
                op: crate::pushdown::CmpOp::Eq,
                right: Box::new(Predicate::Literal(Literal::Text("PAID".to_string()))),
            }],
            Some(100),
            8192,
            vec![],
            "test-run".to_string(),
        )
        .unwrap();

        let sql = plan.build_query(0).sql();
        let sql = sql.as_str();
        // Pushed filter first ($1), then the limit ($2):
        // binds happen in exactly placeholder order, so `$n` always means `params[n - 1]`.
        assert!(
            sql.contains(r#"("status" = $1)"#),
            "filter placeholder first, got: {sql}"
        );
        assert!(
            sql.contains("LIMIT $2"),
            "limit placeholder last, got: {sql}"
        );
    }

    #[test]
    fn test_build_query_is_tagged_with_debug_comment() {
        // The debug SQL comment tag (connector::query_tag) must actually be there, with the
        // right strategy/pipeline defaults, so it's visible in pg_stat_activity as intended
        // -- this isn't optional decoration, it was the whole point of adding it.
        let schema = Arc::new(Schema::empty());
        let table_metadata = TableMetadata {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![],
        };
        let plan = PostgresExecutionPlan::try_new(
            None,
            table_metadata,
            schema,
            vec![],
            None,
            8192,
            vec![],
            "r_fixedtest".to_string(),
        )
        .unwrap();

        let sql = plan.build_query(0).sql();
        let sql = sql.as_str();
        assert!(sql.starts_with("/* el-ballista query_id=q_"), "got: {sql}");
        assert!(
            sql.contains("pipeline=unknown"),
            "no descriptor -> pipeline defaults to unknown, got: {sql}"
        );
        assert!(
            sql.contains("run_id=r_fixedtest"),
            "must carry the caller-supplied run_id, got: {sql}"
        );
        assert!(
            sql.contains("strategy=full"),
            "no pushed filters means a full scan, got: {sql}"
        );
        assert!(
            !sql.contains("partition="),
            "single/unsplit scan must omit partition, got: {sql}"
        );
    }

    #[test]
    fn test_build_query_tag_includes_partition_when_split() {
        let schema = Arc::new(Schema::empty());
        let table_metadata = TableMetadata {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![],
        };
        let partitions = vec![
            keyset_partition("id", 0, Some(0), Some(1)).unwrap(),
            keyset_partition("id", 1, Some(1), None).unwrap(),
        ];
        let plan = PostgresExecutionPlan::try_new(
            None,
            table_metadata,
            schema,
            vec![],
            None,
            8192,
            partitions,
            "r_fixedtest".to_string(),
        )
        .unwrap();

        // 1-based partition index in the tag, out of the total partition count.
        assert!(plan.build_query(0).sql().as_str().contains("partition=1/2"));
        assert!(plan.build_query(1).sql().as_str().contains("partition=2/2"));
    }

    #[test]
    fn test_empty_projection_selects_constant() {
        // `COUNT(*)` prunes the scan to zero columns: the query must stay valid
        // SQL (`SELECT 1`, not `SELECT  FROM`), with pushed filters/limits still
        // applied. Row values are never read downstream — only row counts.
        use crate::types::TableMetadata;
        let schema = Arc::new(Schema::empty());
        let table_metadata = TableMetadata {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![],
        };
        let plan = PostgresExecutionPlan::try_new(
            None,
            table_metadata,
            schema,
            vec![],
            None,
            8192,
            vec![],
            "test-run".to_string(),
        )
        .unwrap();
        let sql = plan.build_query(0).sql();
        let sql = sql.as_str();
        assert!(
            sql.contains(r#"SELECT 1 FROM "public"."orders""#),
            "empty projection must select a constant, got: {sql}"
        );
    }

    #[test]
    fn test_display_as_does_not_panic() {
        let schema = Arc::new(Schema::empty());
        let table_metadata = TableMetadata {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![],
        };

        let plan = PostgresExecutionPlan::try_new(
            None,
            table_metadata,
            schema,
            vec![],
            Some(10),
            8192,
            vec![],
            "test-run".to_string(),
        )
        .unwrap();

        let display_execution_plan = displayable(&plan);
        let s1 = format!("{}", display_execution_plan.indent(true));
        assert!(s1.contains("orders"));

        let s2 = format!("{}", display_execution_plan.one_line());
        assert!(s2.contains("orders"));
    }

    #[test]
    fn test_partition_count() {
        let schema = Arc::new(Schema::empty());
        let table_metadata = TableMetadata {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![],
        };

        let single = PostgresExecutionPlan::try_new(
            None,
            table_metadata.clone(),
            schema.clone(),
            vec![],
            None,
            8192,
            vec![],
            "test-run".to_string(),
        )
        .unwrap();
        match &single.properties().partitioning {
            Partitioning::UnknownPartitioning(n) => assert_eq!(*n, 1),
            other => panic!("expected UnknownPartitioning(1), got {other:?}"),
        }

        let partitions = (0..4i64)
            .map(|i| keyset_partition("id", i as usize, Some(i), Some(i + 1)).unwrap())
            .collect();
        let split = PostgresExecutionPlan::try_new(
            None,
            table_metadata,
            schema,
            vec![],
            None,
            8192,
            partitions,
            "test-run".to_string(),
        )
        .unwrap();
        match &split.properties().partitioning {
            Partitioning::UnknownPartitioning(n) => assert_eq!(*n, 4),
            other => panic!("expected UnknownPartitioning(4), got {other:?}"),
        }
    }

    #[test]
    fn the_plan_display_names_the_scan_path() {
        let shown = |plan: &PostgresExecutionPlan| displayable(plan).one_line().to_string();
        let filter = Predicate::Cmp {
            left: Box::new(Predicate::Column("k".to_string())),
            op: CmpOp::Gt,
            right: Box::new(Predicate::Literal(Literal::Int(1))),
        };
        // COPY requested and the shape allows it.
        assert!(shown(&keyset_plan(vec![]).with_use_copy(true)).contains("scan=copy"));
        // Not requested, or a pushed filter (COPY takes no bind parameters): the cursor.
        assert!(shown(&keyset_plan(vec![])).contains("scan=cursor"));
        let filtered = keyset_plan(vec![filter]).with_use_copy(true);
        assert!(shown(&filtered).contains("scan=cursor"));
    }

    fn keyset_plan(pushed: Vec<Predicate>) -> PostgresExecutionPlan {
        let table_metadata = TableMetadata {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![],
        };
        let partitions = crate::connector::postgres::parallel::tests_support::keyset("k", 1, 10, 2);
        PostgresExecutionPlan::try_new(
            None,
            table_metadata,
            Arc::new(Schema::empty()),
            pushed,
            None,
            8192,
            partitions,
            "r_test".to_string(),
        )
        .unwrap()
    }

    #[test]
    fn test_first_partition_keeps_null_keys_and_is_parenthesized() {
        // `... AND (range) OR "k" IS NULL` without parentheses would bind the OR
        // around the pushed filter and return NULL-key rows that fail the filter.
        let plan = keyset_plan(vec![Predicate::Cmp {
            left: Box::new(Predicate::Column("status".to_string())),
            op: CmpOp::Eq,
            right: Box::new(Predicate::Literal(Literal::Text("PAID".to_string()))),
        }]);
        let first = plan.build_query(0).sql();
        assert!(
            first
                .as_str()
                .ends_with(r#"WHERE ("status" = $1) AND ("k" < 6 OR "k" IS NULL)"#),
            "got: {}",
            first.as_str()
        );
        let last = plan.build_query(1).sql();
        assert!(
            last.as_str().ends_with(r#"AND ("k" >= 6)"#),
            "last partition must be open-ended, got: {}",
            last.as_str()
        );
        assert!(!last.as_str().contains("IS NULL"));
    }

    #[test]
    fn test_cursor_declare_and_copy_sql_shapes() {
        let plan = keyset_plan(vec![]);
        let tag = "/* t */ ";
        let declare = plan.build_cursor_declare(0, tag, "extract_cur_x");
        assert!(
            declare
                .sql()
                .as_str()
                .starts_with("/* t */ DECLARE extract_cur_x CURSOR WITHOUT HOLD FOR SELECT 1 FROM"),
            "got: {}",
            declare.sql().as_str()
        );
        let statements = plan.build_copy_sql(1, tag).unwrap();
        let copy = statements.copy;
        assert!(copy.starts_with("/* t */ COPY (SELECT 1 FROM"), "{copy}");
        // The described SELECT is exactly the COPY's inner query.
        assert!(copy.contains(&format!("COPY ({}) TO STDOUT", statements.select)));
        assert!(
            copy.ends_with(r#"WHERE ("k" >= 6)) TO STDOUT (FORMAT BINARY)"#),
            "{copy}"
        );
        // Pushed filters need binds, which COPY cannot take.
        let filtered = keyset_plan(vec![Predicate::Column("flag".to_string())]);
        assert!(filtered.build_copy_sql(0, tag).is_none());
    }
}
