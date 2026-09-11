use std::sync::Arc;
//use futures::stream::StreamExt;
use arrow::datatypes::Schema;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
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

use crate::connector::errors::ExtractorError;
use crate::connector::postgres::row_adapter;
use crate::pushdown::Predicate;
use crate::types::table_metadata::TableMetadata;

#[derive(Debug)]
pub struct PostgresExecutionPlan {
    pool: PgPool,
    table_metadata: TableMetadata,
    schema: Arc<Schema>,
    properties: Arc<PlanProperties>,
    pushed_filters: Vec<Predicate>,
    pushed_limit: Option<usize>,
    watermark_column: Option<String>,
    window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    batch_size: usize,  // Phase 3: configurable batch size (default 8192)
}

impl PostgresExecutionPlan {
    pub fn try_new(
        pool: PgPool,
        table_metadata: TableMetadata,
        schema: Arc<Schema>,
        pushed_filters: Vec<Predicate>,
        pushed_limit: Option<usize>,
        watermark_column: Option<String>,
        window: Option<(DateTime<Utc>, DateTime<Utc>)>,
        batch_size: usize,
    ) -> DataFusionResult<Self> {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            pool,
            table_metadata,
            schema,
            properties,
            pushed_filters,
            pushed_limit,
            watermark_column,
            window,
            batch_size,
        })
    }

    /// Builds a streaming query using query_raw().
    /// Returns a SQL query string and binding parameters for streaming execution.
    fn build_query_string(
        table_metadata: &TableMetadata,
        watermark_column: Option<&str>,
        window: Option<(DateTime<Utc>, DateTime<Utc>)>,
        pushed_filters: &[Predicate],
        pushed_limit: Option<usize>,
    ) -> String {
        let mut query = String::from("SELECT ");

        // Add column list
        let cols: Vec<String> = table_metadata
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c.column_name))
            .collect();
        query.push_str(&cols.join(", "));

        query.push_str(" FROM ");
        query.push('"');
        query.push_str(&table_metadata.schema_name);
        query.push_str("\".");
        query.push('"');
        query.push_str(&table_metadata.table_name);
        query.push('"');

        let mut conditions = Vec::new();

        // Add watermark window condition
        if let (Some(col), Some((lo, hi))) = (watermark_column, window) {
            conditions.push(format!(
                "\"{}\" > '{}' AND \"{}\" <= '{}'",
                col, lo.to_rfc3339(), col, hi.to_rfc3339()
            ));
        }

        // Add pushed filters (for now, just use debug representation as placeholder)
        // Phase 3+ should integrate with QueryBuilder for proper rendering
        for _predicate in pushed_filters {
            // TODO: render predicate properly using QueryBuilder
        }

        if !conditions.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&conditions.join(" AND "));
        }

        if let Some(n) = pushed_limit {
            query.push_str(&format!(" LIMIT {}", n));
        }

        query
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
                    "PostgresExecutionPlan: table={} pushed_filters={} limit={:?} watermark={:?}",
                    self.table_metadata.table_name,
                    self.pushed_filters.len(),
                    self.pushed_limit,
                    self.watermark_column
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
        let pushed_filters = self.pushed_filters.clone();
        let pushed_limit = self.pushed_limit;
        let watermark_column = self.watermark_column.clone();
        let window = self.window;
        let batch_size = self.batch_size;

        // Build the query string
        let query_str = Self::build_query_string(
            &table_metadata,
            watermark_column.as_deref(),
            window,
            &pushed_filters,
            pushed_limit,
        );

        // Phase 3: Create streaming batch iterator using async-stream
        let stream = {
            use futures::stream::StreamExt;
            
            let query_str_clone = query_str.clone();
            let pool_clone = pool.clone();
            let table_meta_clone = table_metadata.clone();
            
            // Use async_stream macro to create a stream that yields RecordBatches
            async_stream::stream! {
                log::debug!("Streaming query with batch_size={}: {}", batch_size, query_str_clone);
                
                // Fetch rows incrementally using sqlx streaming API
                let mut rows = sqlx::query(sqlx::AssertSqlSafe(query_str_clone.as_str()))
                    .fetch(&pool_clone);
                
                // Accumulate rows into batches
                let mut batch_builder = match row_adapter::RowBatchBuilder::new(&table_meta_clone) {
                    Ok(builder) => builder,
                    Err(e) => {
                        yield Err(DataFusionError::External(Box::new(e)));
                        return;
                    }
                };
                
                // Loop: accumulate rows until batch_size, then yield
                while let Some(row_result) = rows.next().await {
                    let row = match row_result {
                        Ok(r) => r,
                        Err(e) => {
                            yield Err(DataFusionError::External(Box::new(ExtractorError::Sqlx(e))));
                            return;
                        }
                    };
                    
                    if let Err(e) = batch_builder.append_row(&row) {
                        yield Err(DataFusionError::External(Box::new(e)));
                        return;
                    }
                    
                    // When batch_size reached, yield the batch and reset builder
                    if batch_builder.row_count() >= batch_size {
                        match batch_builder.finish() {
                            Ok(batch) => {
                                yield Ok(batch);
                            }
                            Err(e) => {
                                yield Err(DataFusionError::External(Box::new(e)));
                                return;
                            }
                        }
                        
                        // Create new builder for next batch
                        batch_builder = match row_adapter::RowBatchBuilder::new(&table_meta_clone) {
                            Ok(builder) => builder,
                            Err(e) => {
                                yield Err(DataFusionError::External(Box::new(e)));
                                return;
                            }
                        };
                    }
                }
                
                // Yield final partial batch if non-empty
                if !batch_builder.is_empty() {
                    match batch_builder.finish() {
                        Ok(batch) => {
                            yield Ok(batch);
                        }
                        Err(e) => {
                            yield Err(DataFusionError::External(Box::new(e)));
                        }
                    }
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    async fn test_display_as_does_not_panic() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:password@localhost/app")
            .unwrap();
        let schema = Arc::new(Schema::empty());
        let table_metadata = TableMetadata {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![],
        };

        let plan = PostgresExecutionPlan::try_new(
            pool,
            table_metadata,
            schema,
            vec![],
            Some(10),
            Some("updated_at".to_string()),
            None,
            8192,  // batch_size
        )
        .unwrap();

        let display_execution_plan = displayable(&plan);
        let s1 = format!("{}", display_execution_plan.indent(true));
        assert!(s1.contains("orders"));

        let s2 = format!("{}", display_execution_plan.one_line());
        assert!(s2.contains("orders"));
    }
}
