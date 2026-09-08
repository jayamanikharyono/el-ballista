use std::sync::Arc;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use chrono::{DateTime, Utc};
use datafusion::{execution::TaskContext, physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    PlanProperties,
    SendableRecordBatchStream,
}, error::Result as DataFusionResult, physical_plan};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use sqlx::PgPool;
use sqlx::postgres::PgRow;
use crate::extractor::errors::ExtractorError;
use crate::extractor::postgres::query_builder::PostgresQueryBuilder;
use crate::extractor::postgres::row_adapter;
use crate::types::table_metadata::TableMetadata;

use super::row_adapter::PostgresRowAdapter;

#[derive(Debug)]
pub struct PostgresExecutionPlan {
    pool: PgPool,
    table_metadata: TableMetadata,
    schema: Arc<Schema>,
    properties: Arc<PlanProperties>
}

impl PostgresExecutionPlan {
    pub fn try_new(
        pool: PgPool,
        table_metadata: TableMetadata,
        schema: Arc<Schema>
    ) -> DataFusionResult<Self> {

        let properties = Arc::new(
            PlanProperties::new(
                EquivalenceProperties::new(schema.clone(),),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded
        ));

        Ok(Self {
            pool,
            table_metadata,
            schema,
            properties
        })
    }

    async fn execute_query(
        pool: PgPool,
        table_metadata: TableMetadata,
        lo: DateTime<Utc>,
        hi: DateTime<Utc>,
    ) -> Result<Vec<PgRow>, ExtractorError> {
        let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new("");

        PostgresQueryBuilder::build_incremental(
            &mut query,
            &table_metadata,
            "updated_at",
            lo,
            hi,
        );

        let rows = query
            .build()
            .fetch_all(&pool)
            .await?;

        Ok(rows)
    }
}

impl DisplayAs for PostgresExecutionPlan {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "PostgresExecutionPlan: table={}",
                    self.table_metadata.table_name
                )
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "PostgresExecutionPlan: table={}",
                    self.table_metadata.table_name
                )
            },
            DisplayFormatType::TreeRender => todo!()
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

    fn properties(
        &self,
    ) -> &Arc<physical_plan::PlanProperties> {
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let pool = self.pool.clone();
        let table_metadata = self.table_metadata.clone();
        let schema = self.schema.clone();
        // NOTE: this TableProvider path is not yet wired to the checkpoint store or the safe
        // high watermark from `crate::incremental` — it still uses a fixed lookback window. The
        // checkpoint-driven path is `crate::cli::run_job` / `PostgresExtractor::extract_incremental_window`.
        // See docs/phase-one-implementation-plan.md §3 for unifying these into one code path.
        let lo = Utc::now() - chrono::Duration::days(30);
        let hi = Utc::now() - chrono::Duration::seconds(1);

        let stream = futures::stream::once(async move {
            let rows = Self::execute_query(
                pool,
                table_metadata.clone(),
                lo,
                hi,
            )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let arrow_schema = row_adapter::PostgresRowAdapter::build_arrow_schema(&table_metadata);

            let batch =
                PostgresRowAdapter::rows_to_record_batch(
                    &rows,
                    &table_metadata,
                    arrow_schema
                )
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok::<RecordBatch, DataFusionError>(batch)
        });

        Ok(Box::pin(
            RecordBatchStreamAdapter::new(schema, stream),
        ))
    }
}