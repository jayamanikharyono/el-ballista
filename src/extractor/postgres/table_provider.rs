use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::Schema;
use datafusion::{
    datasource::TableProvider,
    logical_expr::{Expr, TableType},
    physical_plan::ExecutionPlan,
};
use datafusion::catalog::Session;
use sqlx::PgPool;
use crate::extractor::errors::ExtractorError;
use crate::extractor::postgres::execution_plan::PostgresExecutionPlan;
use crate::types::TableMetadata;
use super::{
    row_adapter::PostgresRowAdapter,
    schema_reader::PostgresSchemaReader,
};

#[derive(Debug)]
pub struct PostgresTableProvider {
    pool: PgPool,
    table_name: String,
    schema: Arc<Schema>,
    table_metadata: TableMetadata,
}

impl PostgresTableProvider {
    pub async fn new(
        pool: PgPool,
        table_name: &str,
    ) -> Result<Self, ExtractorError> {
        let postgres_schema_reader =
            PostgresSchemaReader::new(&pool);

        let table_metadata = postgres_schema_reader
            .get_table_metadata(table_name)
            .await?;

        let table_schema =
            PostgresRowAdapter::build_arrow_schema(
                &table_metadata,
            );

        Ok(Self {
            pool,
            table_name: table_name.to_string(),
            table_metadata: table_metadata,
            schema: table_schema,
        })
    }
}

#[async_trait]
impl TableProvider for PostgresTableProvider {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // NOTE: projection/filters/limit are not yet honored here — see the note in
        // execution_plan.rs's `execute()` and docs/phase-one-implementation-plan.md §3.
        let plan = PostgresExecutionPlan::try_new(
            self.pool.clone(),
            self.table_metadata.clone(),
            self.schema.clone(),
        )?;

        Ok(Arc::new(plan))
    }
}