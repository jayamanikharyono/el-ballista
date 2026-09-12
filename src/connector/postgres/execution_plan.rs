use std::sync::Arc;

use arrow::datatypes::Schema;
use chrono::{DateTime, Utc};
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
    parallel::ScanPartition,
    query_builder::PostgresQueryBuilder,
    row_adapter,
};
use crate::distributed::connection::PostgresConnectionDescriptor;
use crate::distributed::pool_registry::registry;
use crate::pushdown::Predicate;
use crate::types::table_metadata::TableMetadata;

/// The serializable form of a `PostgresExecutionPlan` — what gets embedded in the Ballista
/// physical plan sent from the scheduler to each executor (see `distributed::plan_codec`).
/// Passwords never travel here: only the descriptor carries the password's environment
/// variable name, resolved at pool-creation time in the executing process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresExecutionPlanModel {
    pub descriptor: PostgresConnectionDescriptor,
    pub table_metadata: TableMetadata,
    pub pushed_filters: Vec<Predicate>,
    pub pushed_limit: Option<usize>,
    pub watermark_column: Option<String>,
    pub window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    pub batch_size: usize,
    pub partitions: Vec<ScanPartition>,
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
    watermark_column: Option<String>,
    window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    batch_size: usize,
    partitions: Vec<ScanPartition>,
}

impl PostgresExecutionPlan {
    pub fn try_new(
        descriptor: Option<PostgresConnectionDescriptor>,
        table_metadata: TableMetadata,
        schema: Arc<Schema>,
        pushed_filters: Vec<Predicate>,
        pushed_limit: Option<usize>,
        watermark_column: Option<String>,
        window: Option<(DateTime<Utc>, DateTime<Utc>)>,
        batch_size: usize,
        partitions: Vec<ScanPartition>,
    ) -> DataFusionResult<Self> {
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
            watermark_column,
            window,
            batch_size,
            partitions,
        })
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
            watermark_column: self.watermark_column.clone(),
            window: self.window,
            batch_size: self.batch_size,
            partitions: self.partitions.clone(),
        })
    }

    pub fn from_model(model: PostgresExecutionPlanModel) -> DataFusionResult<Self> {
        let schema =
            row_adapter::PostgresRowAdapter::build_arrow_schema(&model.table_metadata)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Self::try_new(
            Some(model.descriptor),
            model.table_metadata,
            schema,
            model.pushed_filters,
            model.pushed_limit,
            model.watermark_column,
            model.window,
            model.batch_size,
            model.partitions,
        )
    }

    /// Assembles the `WHERE`/`LIMIT` for one partition. Pushed predicates render through the
    /// backend-neutral [`Predicate::render_to`] into a Postgres sink, which binds each literal
    /// at render position (sqlx `push_bind` appends `$n` inline, so text and numbering align
    /// by construction); a MySQL connector would render the same way into its own sink. The
    /// watermark window is the half-open `(lo, hi]` from docs/incremental-extraction.md §2,
    /// and the partition bounds (composed in parallel.rs from source min/max queries) are
    /// inlined.
    ///
    /// [`Predicate::render_to`]: crate::pushdown::Predicate::render_to
    fn build_query(&self, partition_idx: usize) -> QueryBuilder<Postgres> {
        use crate::pushdown::dialect::{PostgresDialect, SqlDialect};
        use crate::pushdown::{PgParamSink, SqlParam, SqlSink};

        let dialect = PostgresDialect;
        let mut qb = QueryBuilder::<Postgres>::new("SELECT ");

        PostgresQueryBuilder::push_columns(&mut qb, &self.table_metadata);

        qb.push(" FROM ");

        PostgresQueryBuilder::push_identifier(&mut qb, &self.table_metadata.schema_name);
        qb.push(".");
        PostgresQueryBuilder::push_identifier(&mut qb, &self.table_metadata.table_name);

        let mut sink = PgParamSink::new(&mut qb);
        {
            let sink = &mut sink;
            let mut conditions = 0u8;

            if let (Some(column), Some((lo, hi))) = (&self.watermark_column, self.window) {
                sink.push_sql(" WHERE ");
                conditions = 1;

                sink.push_sql(&dialect.quote_ident(column));
                sink.push_sql(" > ");
                sink.push_param(SqlParam::Timestamp(lo));
                sink.push_sql(" AND ");
                sink.push_sql(&dialect.quote_ident(column));
                sink.push_sql(" <= ");
                sink.push_param(SqlParam::Timestamp(hi));
            }

            for predicate in &self.pushed_filters {
                if conditions == 0 {
                    sink.push_sql(" WHERE ");
                    conditions = 1;
                } else {
                    sink.push_sql(" AND ");
                }
                predicate.render_to(&dialect, sink);
            }

            if let Some(partition) = self.partitions.get(partition_idx) {
                if let Some(bounds) = &partition.predicate {
                    if conditions == 0 {
                        sink.push_sql(" WHERE ");
                    } else {
                        sink.push_sql(" AND ");
                    }
                    // Partition bounds are integer (or ctid) literals composed in parallel.rs from
                    // MIN/MAX queries — trusted input, safe to inline, unlike any user-facing text.
                    sink.push_sql(bounds.as_str());
                }
            }

            if let Some(limit) = self.pushed_limit {
                sink.push_sql(" LIMIT ");
                sink.push_param(SqlParam::Int(limit as i64));
            }
        }
        drop(sink);

        log::debug!(
            "PostgresExecutionPlan partition {partition_idx}: {:?}",
            qb.sql()
        );

        qb
    }
}

impl DisplayAs for PostgresExecutionPlan {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "PostgresExecutionPlan: table={} pushed_filters={} limit={:?} watermark={:?} partitions={}",
                    self.table_metadata.table_name,
                    self.pushed_filters.len(),
                    self.pushed_limit,
                    self.watermark_column,
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
                ))
            }
        };

        let table_metadata = self.table_metadata.clone();
        let schema = self.schema.clone();
        let batch_size = self.batch_size;
        let mut query = self.build_query(partition);

        let stream = async_stream::stream! {
            use futures::TryStreamExt;

            let mut rows = query.build().fetch(&pool);

            let mut batch_builder = match row_adapter::RowBatchBuilder::new(&table_metadata) {
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

                if batch_builder.row_count() >= batch_size {
                    match batch_builder.finish() {
                        Ok(batch) => yield Ok(batch),
                        Err(e) => {
                            yield Err(DataFusionError::External(Box::new(e)));
                            return;
                        }
                    }

                    batch_builder = match row_adapter::RowBatchBuilder::new(&table_metadata) {
                        Ok(builder) => builder,
                        Err(e) => {
                            yield Err(DataFusionError::External(Box::new(e)));
                            return;
                        }
                    };
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
        use chrono::TimeZone;

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
            Some("updated_at".to_string()),
            Some((
                Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap(),
            )),
            8192,
            vec![],
        )
        .unwrap();

        let sql = plan.build_query(0).sql();
        let sql = sql.as_str();
        // Window bounds first ($1, $2), then the pushed filter ($3), then the limit ($4):
        // binds happen in exactly placeholder order, so `$n` always means `params[n - 1]`.
        assert!(
            sql.contains(r#""updated_at" > $1 AND "updated_at" <= $2"#),
            "window placeholders first, got: {sql}"
        );
        assert!(
            sql.contains(r#"("status" = $3)"#),
            "filter placeholder third, got: {sql}"
        );
        assert!(sql.contains("LIMIT $4"), "limit placeholder last, got: {sql}");
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
            Some("updated_at".to_string()),
            None,
            8192,
            vec![],
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
            None,
            None,
            8192,
            vec![],
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
            None,
            None,
            8192,
            partitions,
        )
        .unwrap();
        match &split.properties().partitioning {
            Partitioning::UnknownPartitioning(n) => assert_eq!(*n, 4),
            other => panic!("expected UnknownPartitioning(4), got {other:?}"),
        }
    }
}