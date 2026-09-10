use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::Schema;
use datafusion::{
    datasource::TableProvider,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};
use datafusion::catalog::Session;
use sqlx::PgPool;
use crate::extractor::errors::ExtractorError;
use crate::extractor::postgres::execution_plan::PostgresExecutionPlan;
use crate::pushdown::{self, PushdownPolicy};
use crate::types::TableMetadata;
use super::{
    row_adapter::PostgresRowAdapter,
    schema_reader::PostgresSchemaReader,
};

use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct PostgresTableProvider {
    pool: PgPool,
    table_name: String,
    schema: Arc<Schema>,
    table_metadata: TableMetadata,
    policy: PushdownPolicy,
    deny: Vec<String>,
    watermark_column: Option<String>,
    window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    batch_size: usize,  // Phase 3: configurable batch size
}

impl PostgresTableProvider {
    pub async fn new(
        pool: PgPool,
        table_name: &str,
        policy: PushdownPolicy,
        deny: Vec<String>,
        batch_size: usize,
    ) -> Result<Self, ExtractorError> {
        let postgres_schema_reader =
            PostgresSchemaReader::new(&pool);

        let table_metadata = postgres_schema_reader
            .get_table_metadata(table_name)
            .await?;

        let table_schema =
            PostgresRowAdapter::build_arrow_schema(&table_metadata)?;

        Ok(Self {
            pool,
            table_name: table_name.to_string(),
            table_metadata,
            schema: table_schema,
            policy,
            deny,
            watermark_column: None,
            window: None,
            batch_size,
        })
    }

    #[allow(dead_code)]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    #[allow(dead_code)]
    pub fn with_watermark(
        mut self,
        column: impl Into<String>,
        lo: DateTime<Utc>,
        hi: DateTime<Utc>,
    ) -> Self {
        self.watermark_column = Some(column.into());
        self.window = Some((lo, hi));
        self
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

    /// docs/pushdown.md §1/§2 — per-filter `Exact`/`Inexact`/`Unsupported` decisions, using
    /// `crate::pushdown::decide` so this can never disagree with what `scan` below actually
    /// pushes into the query.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| match pushdown::decide(f, self.policy, &self.deny) {
                pushdown::Decision::Push { fidelity: pushdown::Fidelity::Exact, .. } => {
                    TableProviderFilterPushDown::Exact
                }
                pushdown::Decision::Push { fidelity: pushdown::Fidelity::Inexact, .. } => {
                    TableProviderFilterPushDown::Inexact
                }
                pushdown::Decision::Keep => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // NOTE: this path still isn't wired to the checkpoint store or the safe high watermark
        // from `crate::incremental` — `execution_plan.rs`'s `execute()` still uses a fixed
        // lookback window as the base predicate, ANDed with whatever filters below get pushed.
        // See docs/phase-one-implementation-plan.md §3 for unifying this with the
        // checkpoint-driven CLI path.
        let projected_metadata = match projection {
            Some(indices) => self.table_metadata.select_indices(indices),
            None => self.table_metadata.clone(),
        };

        let projected_schema = PostgresRowAdapter::build_arrow_schema(&projected_metadata)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // DataFusion only ever passes filters here that `supports_filters_pushdown` already
        // marked Exact or Inexact, so recomputing the decision with the same policy/denylist is
        // deterministic, not a second independent judgment call.
        let mut pushed = Vec::new();
        let mut any_inexact = false;

        for f in filters {
            if let pushdown::Decision::Push { fidelity, predicate } =
                pushdown::decide(f, self.policy, &self.deny)
            {
                if fidelity == pushdown::Fidelity::Inexact {
                    any_inexact = true;
                }
                pushed.push(predicate);
            }
        }

        // docs/pushdown.md §5 — never push LIMIT alongside an Inexact filter: a source-side
        // LIMIT could leave fewer than `limit` valid rows once DataFusion re-checks the filter.
        let pushed_limit = if any_inexact { None } else { limit };

        let plan = PostgresExecutionPlan::try_new(
            self.pool.clone(),
            projected_metadata,
            projected_schema,
            pushed,
            pushed_limit,
            self.watermark_column.clone(),
            self.window,
            self.batch_size,
        )?;

        Ok(Arc::new(plan))
    }
}
