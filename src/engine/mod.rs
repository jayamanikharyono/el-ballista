//! DataFrame builder API and query execution context.
//! engine/mod.rs
//! The Phase 2 front end over DataFusion: `ExtractContext` owns the session (with the
//! [`SourceAwarePushdownRule`] registered), the source pool, the job config, and the
//! checkpoint store; `source()` registers a cost-aware [`PostgresTableProvider`] and returns a
//! [`SourceDataFrame`] wrapping DataFusion's own `DataFrame`, so every builder method below is
//! a thin delegate — filtering, projection, and limits are planned, pushdown-optimized, and
//! executed by DataFusion itself.
//!
//! [`SourceAwarePushdownRule`]: crate::pushdown::optimizer_rule::SourceAwarePushdownRule
//! [`PostgresTableProvider`]: crate::connector::postgres::PostgresTableProvider

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{DataFrame, SessionContext, col, lit};
use sqlx::PgPool;

use crate::checkpoint::{CheckpointStore, JobKey};
use crate::config::JobConfig;
use crate::connector::postgres::parallel::ParallelStrategy;
use crate::connector::postgres::{PostgresExtractor, PostgresTableProvider};
use crate::distributed::connection::PostgresConnectionDescriptor;
use crate::errors::AppError;
use crate::incremental::{build_window, safe_high_watermark};
use crate::pushdown::cost_model::CostParams;
use crate::pushdown::optimizer_rule::SourceAwarePushdownRule;
use crate::pushdown::PushdownPolicy;

/// The primary entry point for extraction pipelines. Wraps DataFusion's SessionContext,
/// manages source connectors and checkpoints, and provides a fluent builder API.
pub struct ExtractContext {
    session_ctx: SessionContext,
    sources: std::collections::HashMap<String, Arc<PgPool>>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    config: JobConfig,
}

impl ExtractContext {
    /// Create a new ExtractContext from a job configuration.
    /// Initializes the DataFusion session (with source-aware pushdown registered), creates
    /// the database connection pool, and sets up the checkpoint store.
    pub async fn from_config(config: JobConfig) -> Result<Self, AppError> {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rule(Arc::new(SourceAwarePushdownRule))
            .build();
        let session_ctx = SessionContext::new_with_state(state);

        // Create PostgreSQL connection pool from source config.
        let pool = PostgresExtractor::connect(
            &config.source.host,
            config.source.port,
            &config.source.user,
            &config.resolve_password()?,
            &config.source.database,
            config.source.pool_max,
            config.source.statement_timeout_ms,
            &config.source.application_name,
        )
        .await
        .map_err(|e| AppError::Config(format!("cannot connect to source: {e}")))?
        .pool()
        .clone();

        let mut sources = std::collections::HashMap::new();
        sources.insert("postgres".to_string(), Arc::new(pool));

        // Initialize checkpoint store.
        let checkpoint_store = Arc::new(
            crate::checkpoint::json_store::JsonCheckpointStore::new(&config.checkpoint.dir)?,
        );

        Ok(Self {
            session_ctx,
            sources,
            checkpoint_store,
            config,
        })
    }

    fn pool_for(&self, connector_ref: &str) -> Result<Arc<PgPool>, AppError> {
        if let Some(pool) = self.sources.get(connector_ref) {
            return Ok(pool.clone());
        }
        // Single-source convenience: the config names one source, so an unknown ref that is
        // the only pool almost certainly means it.
        if self.sources.len() == 1 {
            let pool = self.sources.values().next().expect("checked len").clone();
            log::debug!(
                "unknown connector '{connector_ref}'; using the only configured source pool"
            );
            return Ok(pool);
        }
        Err(AppError::Config(format!(
            "unknown connector '{connector_ref}' (available: {:?})",
            self.sources.keys().collect::<Vec<_>>(),
        )))
    }

    /// Get a DataSource reference for a given table.
    /// Registers a cost-aware `PostgresTableProvider` (schema discovery + statistics through
    /// the shared pool) and returns a [`SourceDataFrame`] over it.
    pub async fn source(
        &self,
        connector_ref: &str,
        table_name: &str,
    ) -> Result<SourceDataFrame<'_>, AppError> {
        let _pool = self.pool_for(connector_ref)?;
        let policy = PushdownPolicy::parse(&self.config.pushdown.policy);
        let descriptor =
            PostgresConnectionDescriptor::from_config(&self.config.source, 1);

        let provider = PostgresTableProvider::new(
            descriptor,
            table_name,
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

        let strategy =
            ParallelStrategy::parse(&self.config.parallel_scan.strategy);
        let provider = provider.with_parallel_strategy(strategy);

        self.session_ctx
            .register_table(table_name, Arc::new(provider))
            .map_err(AppError::DataFusion)?;
        let df = self
            .session_ctx
            .table(table_name)
            .await
            .map_err(AppError::DataFusion)?;

        Ok(SourceDataFrame { ctx: self, df })
    }

    /// Execute a SQL query against registered sources.
    /// Sources are pre-registered in the catalog, so FROM clauses reference them by name.
    pub async fn sql(&self, sql: &str) -> Result<DataFrame, AppError> {
        self.session_ctx
            .sql(sql)
            .await
            .map_err(AppError::DataFusion)
    }
}

/// A builder for extraction queries on a single source table.
/// Thin wrapper over DataFusion's `DataFrame`: every method delegates, so planning,
/// source-aware pushdown, and execution are DataFusion's own. Borrows the context for
/// checkpoint and pool access during [`SourceDataFrame::incremental`].
pub struct SourceDataFrame<'a> {
    ctx: &'a ExtractContext,
    df: DataFrame,
}

impl<'a> SourceDataFrame<'a> {
    /// Configure incremental extraction with a watermark column.
    /// Reads the committed watermark from the checkpoint store, resolves the safe high
    /// watermark against the source, caps the window at `max_window_secs`, and injects the
    /// resulting `(lo, hi]` predicate — the same window `rel run` would extract.
    pub async fn incremental(self, watermark: Watermark) -> Result<Self, AppError> {
        let ctx = self.ctx;
        let key = JobKey::new(ctx.config.job_id.clone());
        let checkpoint = ctx.checkpoint_store.read(&key).await?;
        let lo = checkpoint.and_then(|c| c.watermark_value);

        let pool = ctx.pool_for("postgres")?;
        let safety_lag =
            chrono::Duration::seconds(ctx.config.incremental.safety_lag_secs);
        let max_window =
            chrono::Duration::seconds(ctx.config.incremental.max_window_secs);
        let hi_candidate = safe_high_watermark(&pool, safety_lag).await?;
        let window = build_window(lo, hi_candidate, max_window);

        log::info!(
            "incremental window on {}: ({}, {}]",
            watermark.column_name,
            window.lo,
            window.hi
        );

        // Postgres timestamptz columns arrive as TimestampMicrosecond; fold the window into
        // matching literals so the predicate can push to the source.
        let lo_lit = lit(ScalarValue::TimestampMicrosecond(
            Some(window.lo.timestamp_micros()),
            None,
        ));
        let hi_lit = lit(ScalarValue::TimestampMicrosecond(
            Some(window.hi.timestamp_micros()),
            None,
        ));
        let df = self
            .df
            .filter(col(watermark.column_name.as_str()).gt(lo_lit))
            .map_err(AppError::DataFusion)?
            .filter(col(watermark.column_name.as_str()).lt_eq(hi_lit))
            .map_err(AppError::DataFusion)?;
        Ok(Self { ctx: self.ctx, df })
    }

    /// Add a filter predicate to the query.
    pub fn filter(self, expr: Expr) -> Result<Self, AppError> {
        Ok(Self { ctx: self.ctx, df: self.df.filter(expr).map_err(AppError::DataFusion)? })
    }

    /// Select specific columns.
    pub fn select(self, columns: Vec<Expr>) -> Result<Self, AppError> {
        Ok(Self { ctx: self.ctx, df: self.df.select(columns).map_err(AppError::DataFusion)? })
    }

    /// Add or rename a column with an expression.
    pub fn with_column(self, name: &str, expr: Expr) -> Result<Self, AppError> {
        Ok(Self { ctx: self.ctx, df: self.df.with_column(name, expr).map_err(AppError::DataFusion)? })
    }

    /// Limit results to N rows.
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<Self, AppError> {
        Ok(Self { ctx: self.ctx, df: self.df.limit(skip, fetch).map_err(AppError::DataFusion)? })
    }

    /// Execute the query and collect Arrow RecordBatches — the handoff point to DataFusion
    /// writers, Ballista, or the orchestrator. (Sinks are out of scope for this project.)
    pub async fn collect(self) -> Result<Vec<RecordBatch>, AppError> {
        self.df.collect().await.map_err(AppError::DataFusion)
    }
}

/// Configuration for incremental extraction's watermark column.
pub struct Watermark {
    column_name: String,
}

impl Watermark {
    /// Declare a timestamp watermark column.
    pub fn timestamp(column_name: &str) -> Self {
        Self {
            column_name: column_name.to_string(),
        }
    }

    /// Declare an integer (sequence) watermark column.
    #[allow(dead_code)]
    pub fn sequence(column_name: &str) -> Self {
        Self {
            column_name: column_name.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_timestamp() {
        let wm = Watermark::timestamp("updated_at");
        assert_eq!(wm.column_name, "updated_at");
    }

    #[test]
    fn test_watermark_sequence() {
        let wm = Watermark::sequence("id");
        assert_eq!(wm.column_name, "id");
    }
}
