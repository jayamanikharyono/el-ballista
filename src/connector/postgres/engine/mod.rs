//! DataFrame builder API and query execution context.
//! engine/mod.rs
//! The front end over DataFusion: `ExtractContext` owns the session and the job config;
//! `source()` registers a cost-aware [`PostgresTableProvider`] and returns a
//! [`SourceDataFrame`] wrapping DataFusion's own `DataFrame`, so every builder method below is
//! a thin delegate — filtering, projection, and limits are planned, pushdown-optimized, and
//! executed by DataFusion itself.
//!
//! Filtering is caller-provided (full extraction or explicit predicates). The extraction
//! layer never manages watermarks: the orchestrator decides WHAT range to extract,
//! this layer decides HOW to extract it efficiently.
//!
//! [`PostgresTableProvider`]: crate::connector::postgres::PostgresTableProvider

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{DataFrame, SessionContext};

use crate::config::JobConfig;
use crate::connector::postgres::PostgresTableProvider;
use crate::connector::postgres::distributed::connection::PostgresConnectionDescriptor;
use crate::errors::AppError;
use crate::pushdown::cost_model::CostParams;

/// The connector reference [`ExtractContext::source`] accepts: the job config names exactly
/// one Postgres source.
pub const POSTGRES_CONNECTOR_REF: &str = "postgres";

/// The primary entry point for DataFrame-style extraction. Wraps DataFusion's
/// `SessionContext` and registers the job's source table on demand. Opens no connection
/// itself: providers resolve the process-shared, budgeted pool (`pool_max` for this single
/// process) when a table is registered.
pub struct ExtractContext {
    session_ctx: SessionContext,
    config: JobConfig,
}

impl ExtractContext {
    /// Create a context from a (validated) job configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), el_ballista::errors::AppError> {
    /// use el_ballista::config::JobConfig;
    /// use el_ballista::connector::postgres::engine::ExtractContext;
    ///
    /// let ctx = ExtractContext::from_config(JobConfig::from_file("job.json")?).await?;
    /// let batches = ctx.source("postgres", "public.orders").await?.collect().await?;
    /// # let _ = batches; Ok(()) }
    /// ```
    pub async fn from_config(config: JobConfig) -> Result<Self, AppError> {
        config.validate()?;
        let state = SessionStateBuilder::new().with_default_features().build();
        let session_ctx = SessionContext::new_with_state(state);
        Ok(Self {
            session_ctx,
            config,
        })
    }

    /// Register `table_name` from the source `connector_ref` (must be
    /// [`POSTGRES_CONNECTOR_REF`]: an unknown reference is an error, never a silent fallback)
    /// with a cost-aware `PostgresTableProvider` and return a [`SourceDataFrame`] over it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), el_ballista::errors::AppError> {
    /// use el_ballista::config::JobConfig;
    /// use el_ballista::connector::postgres::engine::ExtractContext;
    /// let ctx = ExtractContext::from_config(JobConfig::from_file("job.json")?).await?;
    /// let orders = ctx.source("postgres", "public.orders").await?;
    /// # let _ = orders; Ok(()) }
    /// ```
    pub async fn source(
        &self,
        connector_ref: &str,
        table_name: &str,
    ) -> Result<SourceDataFrame, AppError> {
        if connector_ref != POSTGRES_CONNECTOR_REF {
            return Err(AppError::Config(format!(
                "unknown connector '{connector_ref}' (available: [\"{POSTGRES_CONNECTOR_REF}\"])"
            )));
        }
        let descriptor = PostgresConnectionDescriptor::from_config(&self.config.source, 1);

        let provider = PostgresTableProvider::new(
            descriptor,
            table_name,
            self.config.pushdown.policy,
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

        let provider = provider
            .with_parallel_strategy(self.config.parallel_scan.strategy)
            .with_max_batch_bytes(self.config.execution.max_batch_bytes)
            .with_use_copy(self.config.execution.use_copy)
            .with_copy_statement_timeout_ms(self.config.execution.copy_statement_timeout_ms);

        self.session_ctx
            .register_table(table_name, Arc::new(provider))
            .map_err(AppError::DataFusion)?;
        let df = self
            .session_ctx
            .table(table_name)
            .await
            .map_err(AppError::DataFusion)?;

        Ok(SourceDataFrame { df })
    }

    /// Execute a SQL query against registered sources.
    /// Sources are pre-registered in the catalog, so FROM clauses reference them by name.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), el_ballista::errors::AppError> {
    /// use el_ballista::config::JobConfig;
    /// use el_ballista::connector::postgres::engine::ExtractContext;
    /// let ctx = ExtractContext::from_config(JobConfig::from_file("job.json")?).await?;
    /// let _ = ctx.source("postgres", "public.orders").await?;
    /// let df = ctx.sql("SELECT status, count(*) FROM \"public.orders\" GROUP BY status").await?;
    /// # let _ = df; Ok(()) }
    /// ```
    pub async fn sql(&self, sql: &str) -> Result<DataFrame, AppError> {
        self.session_ctx
            .sql(sql)
            .await
            .map_err(AppError::DataFusion)
    }
}

/// A builder for extraction queries on a single source table.
/// Thin wrapper over DataFusion's `DataFrame`: every method delegates, so planning,
/// source-aware pushdown, and execution are DataFusion's own.
#[must_use = "a SourceDataFrame does nothing until collected"]
pub struct SourceDataFrame {
    df: DataFrame,
}

impl SourceDataFrame {
    /// Add a filter predicate to the query.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo(orders: el_ballista::connector::postgres::engine::SourceDataFrame)
    /// # -> Result<(), el_ballista::errors::AppError> {
    /// use datafusion::prelude::{col, lit};
    /// let paid = orders.filter(col("status").eq(lit("PAID")))?;
    /// # let _ = paid; Ok(()) }
    /// ```
    pub fn filter(self, expr: Expr) -> Result<Self, AppError> {
        Ok(Self {
            df: self.df.filter(expr).map_err(AppError::DataFusion)?,
        })
    }

    /// Select specific columns.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo(orders: el_ballista::connector::postgres::engine::SourceDataFrame)
    /// # -> Result<(), el_ballista::errors::AppError> {
    /// use datafusion::prelude::{col, lit};
    /// let slim = orders.select(vec![col("order_id"), col("amount")])?;
    /// # let _ = slim; Ok(()) }
    /// ```
    pub fn select(self, columns: Vec<Expr>) -> Result<Self, AppError> {
        Ok(Self {
            df: self.df.select(columns).map_err(AppError::DataFusion)?,
        })
    }

    /// Add or rename a column with an expression.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo(orders: el_ballista::connector::postgres::engine::SourceDataFrame)
    /// # -> Result<(), el_ballista::errors::AppError> {
    /// use datafusion::prelude::{col, lit};
    /// let doubled = orders.with_column("amount2", col("amount") * lit(2))?;
    /// # let _ = doubled; Ok(()) }
    /// ```
    pub fn with_column(self, name: &str, expr: Expr) -> Result<Self, AppError> {
        Ok(Self {
            df: self
                .df
                .with_column(name, expr)
                .map_err(AppError::DataFusion)?,
        })
    }

    /// Limit results to N rows.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo(orders: el_ballista::connector::postgres::engine::SourceDataFrame)
    /// # -> Result<(), el_ballista::errors::AppError> {
    /// use datafusion::prelude::{col, lit};
    /// let first_ten = orders.limit(0, Some(10))?;
    /// # let _ = first_ten; Ok(()) }
    /// ```
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<Self, AppError> {
        Ok(Self {
            df: self.df.limit(skip, fetch).map_err(AppError::DataFusion)?,
        })
    }

    /// Execute the query and collect Arrow RecordBatches — the handoff point to DataFusion
    /// writers, Ballista, or the orchestrator. (Sinks are out of scope for this project.)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo(orders: el_ballista::connector::postgres::engine::SourceDataFrame)
    /// # -> Result<(), el_ballista::errors::AppError> {
    /// use datafusion::prelude::{col, lit};
    /// let batches = orders.filter(col("status").eq(lit("PAID")))?.collect().await?;
    /// # let _ = batches; Ok(()) }
    /// ```
    pub async fn collect(self) -> Result<Vec<RecordBatch>, AppError> {
        self.df.collect().await.map_err(AppError::DataFusion)
    }

    /// Execute the query as a stream of batches: the bounded-memory alternative to
    /// [`Self::collect`] (which holds the whole result).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo(orders: el_ballista::connector::postgres::engine::SourceDataFrame)
    /// # -> Result<(), el_ballista::errors::AppError> {
    /// use datafusion::prelude::{col, lit};
    /// use futures::TryStreamExt;
    /// let mut stream = orders.execute_stream().await?;
    /// while let Some(batch) = stream.try_next().await? {
    ///     let _ = batch.num_rows();
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn execute_stream(
        self,
    ) -> Result<datafusion::execution::SendableRecordBatchStream, AppError> {
        self.df.execute_stream().await.map_err(AppError::DataFusion)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unknown_connector_ref_is_an_error_not_a_fallback() {
        let config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        let ctx = ExtractContext::from_config(config).await.unwrap();
        let err = ctx
            .source("postgress", "public.payment")
            .await
            .err()
            .unwrap();
        assert!(
            err.to_string().contains("unknown connector 'postgress'"),
            "{err}"
        );
    }
}
