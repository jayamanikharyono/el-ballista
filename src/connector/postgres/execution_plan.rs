use std::sync::Arc;

use arrow::datatypes::Schema;
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

use crate::connector::errors::ExtractorError;
use crate::connector::postgres::{
    parallel::ScanPartition, query_builder::PostgresQueryBuilder, row_adapter,
};
use crate::connector::query_tag::QuerySession;
use crate::distributed::connection::PostgresConnectionDescriptor;
use crate::distributed::pool_registry::registry;
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
/// aggregates per-partition status (rows / watermark) and persists checkpoints through
/// its background, non-blocking writer (`checkpoint::progress`). This keeps exactly one
/// writer per job and workers never block on file I/O.
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
    pub partitions: Vec<ScanPartition>,
    /// Debug identity for the SQL comment tag (see `connector::query_tag`) — carried across
    /// the wire so every executor tags its partition's query with the *same* run_id the
    /// scheduler generated, rather than each process inventing its own.
    pub run_id: String,
}

/// A streaming `SELECT` against one Postgres table. Whenever more than one partition is
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

impl PostgresExecutionPlan {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
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
            partitions,
            query_session,
        })
    }

    /// Override the byte cap after construction (e.g. from `execution.max_batch_bytes`).
    /// Builder-style so `try_new`'s signature — and every existing call site — stays stable.
    pub fn with_max_batch_bytes(mut self, max_batch_bytes: usize) -> Self {
        self.max_batch_bytes = max_batch_bytes.max(1);
        self
    }

    /// Prefer binary `COPY` over cursor `SELECT` when the scan shape allows it.
    /// Builder-style, same stability rationale as [`Self::with_max_batch_bytes`].
    pub fn with_use_copy(mut self, use_copy: bool) -> Self {
        self.use_copy = use_copy;
        self
    }

    pub fn to_model(&self) -> DataFusionResult<PostgresExecutionPlanModel> {
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
            partitions: self.partitions.clone(),
            run_id: self.query_session.run_id().to_string(),
        })
    }

    pub fn from_model(model: PostgresExecutionPlanModel) -> DataFusionResult<Self> {
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
        })
    }

    /// Assembles the `WHERE`/`LIMIT` for one partition. Pushed predicates render through the
    /// backend-neutral [`Predicate::render_to`] into a Postgres sink, which binds each literal
    /// at render position (sqlx `push_bind` appends `$n` inline, so text and numbering align
    /// by construction); a MySQL connector would render the same way into its own sink. The
    /// partition bounds (composed in parallel.rs from source min/max queries) are inlined.
    /// Caller-provided filter ranges arrive as ordinary pushed predicates — the extraction
    /// layer never manages watermarks itself.
    ///
    /// [`Predicate::render_to`]: crate::pushdown::Predicate::render_to
    pub(crate) fn build_query(&self, partition_idx: usize) -> QueryBuilder<Postgres> {
        let mut qb = QueryBuilder::<Postgres>::new(self.tag_for(partition_idx).render());
        self.push_select(&mut qb, partition_idx, LimitRender::Bind);
        log::debug!(
            "PostgresExecutionPlan partition {partition_idx}: {:?}",
            qb.sql()
        );
        qb
    }

    /// `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` for one partition, or `None` when
    /// COPY cannot run it: pushed filters render with bind parameters, which `COPY`
    /// forbids, so any pushed filter falls back to `SELECT` (loudly logged by the
    /// caller — benchmark comparisons must know which path executed). The projection
    /// must also be binary-decodable ([`supports_binary_copy`](crate::connector::postgres::copy::supports_binary_copy)).
    pub(crate) fn build_copy_sql(&self, partition_idx: usize) -> Option<String> {
        use crate::connector::postgres::copy::supports_binary_copy;

        if !self.pushed_filters.is_empty() {
            return None;
        }
        if !supports_binary_copy(&self.table_metadata.columns) {
            return None;
        }
        let mut inner = QueryBuilder::<Postgres>::new("");
        self.push_select(&mut inner, partition_idx, LimitRender::Inline);
        let inner_sql = inner.sql();
        Some(format!(
            "{}COPY ({}) TO STDOUT (FORMAT BINARY)",
            self.tag_for(partition_idx).render(),
            inner_sql.as_str()
        ))
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
        use crate::pushdown::dialect::PostgresDialect;
        use crate::pushdown::{PgParamSink, SqlParam, SqlSink};

        let dialect = PostgresDialect;

        qb.push("SELECT ");

        PostgresQueryBuilder::push_columns(&mut *qb, &self.table_metadata);

        qb.push(" FROM ");

        PostgresQueryBuilder::push_identifier(&mut *qb, &self.table_metadata.schema_name);
        qb.push(".");
        PostgresQueryBuilder::push_identifier(&mut *qb, &self.table_metadata.table_name);

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
                // Partition bounds are integer (or ctid) literals composed in parallel.rs from
                // MIN/MAX queries — trusted input, safe to inline, unlike any user-facing text.
                sink.push_sql(bounds.as_str());
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
                    "PostgresExecutionPlan: table={} pushed_filters={} limit={:?} partitions={}",
                    self.table_metadata.table_name,
                    self.pushed_filters.len(),
                    self.pushed_limit,
                    self.partitions.len(),
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
        let pool = match &self.descriptor {
            Some(descriptor) => registry()
                .pool(descriptor)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
            None => {
                return Err(DataFusionError::NotImplemented(
                    "a pool-less PostgresExecutionPlan cannot execute; \
                     scan through PostgresTableProvider instead"
                        .to_string(),
                ));
            }
        };

        let table_metadata = self.table_metadata.clone();
        let schema = self.schema.clone();
        let batch_size = self.batch_size;
        let max_batch_bytes = self.max_batch_bytes;
        // Binary COPY when enabled and the scan shape allows it (no pushed filters,
        // decodable projection); otherwise the cursor SELECT. The fallback is loud
        // (warn) because benchmark comparisons must know which path executed.
        let copy_sql = if self.use_copy {
            self.build_copy_sql(partition)
        } else {
            None
        };
        if self.use_copy && copy_sql.is_none() {
            log::warn!(
                "PostgresExecutionPlan partition {partition}: use_copy requested but scan shape needs SELECT (pushed filters or unmapped type); falling back"
            );
        } else if let Some(sql) = &copy_sql {
            log::info!("PostgresExecutionPlan partition {partition}: scanning via {sql}");
        }
        let mut query = self.build_query(partition);

        // NOTE: no checkpoint access here by design — see the model docs. This task
        // streams batches; the driver persists progress.
        let stream = async_stream::stream! {
            use futures::TryStreamExt;
            use sqlx::postgres::PgPoolCopyExt as _;

            if let Some(copy_sql) = copy_sql {
                let mut byte_stream = match pool.copy_out_raw(copy_sql.as_str()).await {
                    Ok(s) => s,
                    Err(e) => {
                        yield Err(DataFusionError::External(Box::new(ExtractorError::Sqlx(e))));
                        return;
                    }
                };
                // Drive the COPY byte stream through an owned decoder, yielding batches.
                let mut copy_decoder = match crate::connector::postgres::copy::CopyBatchDecoder::new(&table_metadata, batch_size, max_batch_bytes) {
                    Ok(d) => d,
                    Err(e) => {
                        yield Err(DataFusionError::External(Box::new(e)));
                        return;
                    }
                };
                loop {
                    let chunk = match byte_stream.try_next().await {
                        Ok(Some(chunk)) => chunk,
                        Ok(None) => break,
                        Err(e) => {
                            yield Err(DataFusionError::External(Box::new(ExtractorError::Sqlx(e))));
                            return;
                        }
                    };
                    let batches = match copy_decoder.push_bytes(&chunk) {
                        Ok(b) => b,
                        Err(e) => {
                            yield Err(DataFusionError::External(Box::new(e)));
                            return;
                        }
                    };
                    for batch in batches {
                        yield Ok(batch);
                    }
                }
                match copy_decoder.finish() {
                    Ok(Some(batch)) => yield Ok(batch),
                    Ok(None) => {}
                    Err(e) => yield Err(DataFusionError::External(Box::new(e))),
                }
                return;
            }

            let mut rows = query.build().fetch(&pool);

            let mut batch_builder = match row_adapter::RowBatchBuilder::with_capacity(&table_metadata, batch_size) {
                Ok(builder) => builder,
                Err(e) => {
                    yield Err(DataFusionError::External(Box::new(e)));
                    return;
                }
            };

            loop {
                let row = match rows.try_next().await {
                    Ok(Some(row)) => row,
                    Ok(None) => break,
                    Err(e) => {
                        yield Err(DataFusionError::External(Box::new(ExtractorError::Sqlx(e))));
                        return;
                    }
                };

                if let Err(e) = batch_builder.append_row(&row) {
                    yield Err(DataFusionError::External(Box::new(e)));
                    return;
                }

                // Flush on rows OR bytes; `finish()` reuses builders (capacity retained).
                if batch_builder.should_flush(batch_size, max_batch_bytes) {
                    match batch_builder.finish() {
                        Ok(batch) => yield Ok(batch),
                        Err(e) => {
                            yield Err(DataFusionError::External(Box::new(e)));
                            return;
                        }
                    }
                }
            }

            if !batch_builder.is_empty() {
                match batch_builder.finish() {
                    Ok(batch) => yield Ok(batch),
                    Err(e) => yield Err(DataFusionError::External(Box::new(e))),
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pushdown::{Literal, Predicate};
    use datafusion::physical_plan::displayable;

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
                op: "=".to_string(),
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
        assert!(sql.starts_with("/* rust-extract query_id=q_"), "got: {sql}");
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
            ScanPartition {
                partition_id: 0,
                lo: Some(0),
                hi: Some(1),
                predicate: Some("id >= 0 AND id < 1".to_string()),
            },
            ScanPartition {
                partition_id: 1,
                lo: Some(1),
                hi: Some(2),
                predicate: Some("id >= 1 AND id < 2".to_string()),
            },
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

        let partitions = (0..4)
            .map(|i| ScanPartition {
                partition_id: i,
                lo: Some(i as i64),
                hi: Some(i as i64 + 1),
                predicate: Some(format!("id >= {} AND id < {}", i, i + 1)),
            })
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
}
