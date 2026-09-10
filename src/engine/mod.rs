//! DataFrame builder API and query execution context.
//! engine/mod.rs
//! The ExtractContext wraps DataFusion's SessionContext, source connectors, checkpoints,
//! and sinks into a fluent API for building and executing extraction pipelines.
//! Implements the "intended API" from README.md and makes the project usable beyond CLI.

use std::sync::Arc;
use datafusion::prelude::SessionContext;
use sqlx::PgPool;

use crate::config::JobConfig;
use crate::checkpoint::CheckpointStore;
use crate::errors::AppError;
use crate::extractor::postgres::PostgresExtractor;

/// The primary entry point for extraction pipelines. Wraps DataFusion's SessionContext,
/// manages source connectors and checkpoints, and provides a fluent builder API.
pub struct ExtractContext {
    session_ctx: SessionContext,
    sources: std::collections::HashMap<String, Arc<PgPool>>,
    checkpoint_store: Arc<dyn CheckpointStore>,
}

impl ExtractContext {
    /// Create a new ExtractContext from a job configuration.
    /// Initializes the DataFusion session, creates database connections, and sets up
    /// the checkpoint store.
    pub async fn from_config(config: JobConfig) -> Result<Self, AppError> {
        let session_ctx = SessionContext::new();

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
            crate::checkpoint::json_store::JsonCheckpointStore::new(&config.checkpoint.dir)?
        );

        Ok(Self {
            session_ctx,
            sources,
            checkpoint_store,
        })
    }

    /// Get a DataSource reference for a given table.
    /// Returns a SourceDataFrame builder that can be configured with incremental windows,
    /// filters, projections, and written to a sink.
    pub fn source(&self, _connector_ref: &str, _table_name: &str) -> SourceDataFrame {
        SourceDataFrame {
            session_ctx: self.session_ctx.clone(),
        }
    }

    /// Execute a SQL query against registered sources.
    /// Sources are pre-registered in the catalog, so FROM clauses reference them by name.
    pub async fn sql(&self, _sql: &str) -> Result<(), AppError> {
        // TODO: Execute SQL query using session_ctx.sql()
        // This is a placeholder for Phase 2.5+ implementation.
        Err(AppError::Config(
            "SQL execution not yet implemented".to_string(),
        ))
    }

    /// Commit all checkpoints acquired during this session.
    /// Called after sinks are durable to advance watermarks.
    pub async fn commit_checkpoints(&self) -> Result<(), AppError> {
        // TODO: Iterate through acquired checkpoints and commit them.
        Ok(())
    }
}

/// A builder for extraction queries on a single source table.
/// Supports incremental windowing, filtering, projection, and writing to sinks.
pub struct SourceDataFrame {
    session_ctx: SessionContext,
}

impl SourceDataFrame {
    /// Configure incremental extraction with a watermark column.
    /// Resolves the `(lo, hi]` window from the checkpoint store and injects it into
    /// the query plan.
    pub fn incremental(self, _watermark: Watermark) -> Self {
        // TODO: Resolve checkpoint and inject watermark predicate
        self
    }

    /// Add a filter predicate to the query.
    pub fn filter(self, _expr: &str) -> Self {
        // TODO: Parse and add filter to logical plan
        self
    }

    /// Select specific columns.
    pub fn select(self, _columns: Vec<&str>) -> Self {
        // TODO: Add projection to logical plan
        self
    }

    /// Add or rename a column with an expression.
    pub fn with_column(self, _name: &str, _expr: &str) -> Self {
        // TODO: Add derived column to logical plan
        self
    }

    /// Limit results to N rows.
    pub fn limit(self, _n: usize) -> Self {
        // TODO: Add limit to logical plan
        self
    }

    /// Execute the query and write results to a Parquet sink.
    pub async fn write_parquet(self, _uri: &str) -> Result<(), AppError> {
        // TODO: Execute logical plan, collect batches, write to sink
        Err(AppError::Config(
            "Parquet sink not yet implemented".to_string(),
        ))
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
