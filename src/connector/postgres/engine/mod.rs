//! DataFrame builder API and query execution context.
//! engine/mod.rs
//! The front end over DataFusion: `ExtractContext` owns the session (with the
//! [`SourceAwarePushdownRule`] registered), the source pool, and the job config;
//! `source()` registers a cost-aware [`PostgresTableProvider`] and returns a
//! [`SourceDataFrame`] wrapping DataFusion's own `DataFrame`, so every builder method below is
//! a thin delegate — filtering, projection, and limits are planned, pushdown-optimized, and
//! executed by DataFusion itself.
//!
//! Filtering is caller-provided (full extraction or explicit predicates). The extraction
//! layer never manages watermarks: the orchestrator decides WHAT range to extract,
//! this layer decides HOW to extract it efficiently.
//!
//! [`SourceAwarePushdownRule`]: crate::pushdown::optimizer_rule::SourceAwarePushdownRule
//! [`PostgresTableProvider`]: crate::connector::postgres::PostgresTableProvider

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{DataFrame, SessionContext};
use sqlx::PgPool;

use crate::config::JobConfig;
use crate::connector::postgres::parallel::ParallelStrategy;
use crate::connector::postgres::{PostgresExtractor, PostgresTableProvider};
use crate::distributed::connection::PostgresConnectionDescriptor;
use crate::errors::AppError;
use crate::pushdown::PushdownPolicy;
use crate::pushdown::cost_model::CostParams;
use crate::pushdown::optimizer_rule::SourceAwarePushdownRule;

/// The primary entry point for extraction pipelines. Wraps DataFusion's SessionContext,
/// manages source connectors, and provides a fluent builder API.
pub struct ExtractContext {
    session_ctx: SessionContext,
    sources: std::collections::HashMap<String, Arc<PgPool>>,
    config: JobConfig,
}

impl ExtractContext {
    /// Create a new ExtractContext from a job configuration.
    /// Initializes the DataFusion session (with source-aware pushdown registered) and
    /// creates the database connection pool.
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

        Ok(Self {
            session_ctx,
            sources,
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
    ) -> Result<SourceDataFrame, AppError> {
        let _pool = self.pool_for(connector_ref)?;
        let policy = PushdownPolicy::parse(&self.config.pushdown.policy);
        let descriptor = PostgresConnectionDescriptor::from_config(&self.config.source, 1);

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

        let strategy = ParallelStrategy::parse(&self.config.parallel_scan.strategy);
        let provider = provider.with_parallel_strategy(strategy);

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
pub struct SourceDataFrame {
    df: DataFrame,
}

impl SourceDataFrame {
    /// Add a filter predicate to the query.
    pub fn filter(self, expr: Expr) -> Result<Self, AppError> {
        Ok(Self {
            df: self.df.filter(expr).map_err(AppError::DataFusion)?,
        })
    }

    /// Select specific columns.
    pub fn select(self, columns: Vec<Expr>) -> Result<Self, AppError> {
        Ok(Self {
            df: self.df.select(columns).map_err(AppError::DataFusion)?,
        })
    }

    /// Add or rename a column with an expression.
    pub fn with_column(self, name: &str, expr: Expr) -> Result<Self, AppError> {
        Ok(Self {
            df: self
                .df
                .with_column(name, expr)
                .map_err(AppError::DataFusion)?,
        })
    }

    /// Limit results to N rows.
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<Self, AppError> {
        Ok(Self {
            df: self.df.limit(skip, fetch).map_err(AppError::DataFusion)?,
        })
    }

    /// Execute the query and collect Arrow RecordBatches — the handoff point to DataFusion
    /// writers, Ballista, or the orchestrator. (Sinks are out of scope for this project.)
    pub async fn collect(self) -> Result<Vec<RecordBatch>, AppError> {
        self.df.collect().await.map_err(AppError::DataFusion)
    }
}
