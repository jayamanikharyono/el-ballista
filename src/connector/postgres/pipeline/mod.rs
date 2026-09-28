//! Job pipeline: turn a parsed [`JobConfig`] into a runnable extraction.
//!
//! A [`Pipeline`] is the single orchestration path behind
//! [`PostgresConnector`](crate::connector::postgres::PostgresConnector) (and therefore the
//! CLI). Callers normally use the connector's builder; the pipeline itself exposes the
//! filter helpers (`Pipeline::filter_exprs_with_schema`, [`Pipeline::explain_filters`]) and
//! [`Pipeline::preview`].
//!
//! The config drives the shape: `columns` for projection, `filters` (empty means a full
//! extraction — every row), `parallel_scan` (none | keyset) for splitting, and
//! `execution.batch_size` for the source fetch size. Filtered extraction uses
//! caller-provided predicates pushed to the source through the normal DataFusion pushdown
//! path: the orchestrator decides WHAT range to extract, this layer decides HOW.
//!
//! Every extraction is a list of splits executed through one DataFusion physical plan whose
//! output partition `i` is split `i` (keyset range, or the whole table). Terminals:
//!
//! - `collect` — every batch, in split order (materializes; small results only);
//! - `stream` — one bounded-memory stream over all splits;
//! - `run` — a **diagnostic** row count: scans and discards, no checkpoint;
//! - `run_with(consumer)` — the operational job: each pending split's stream is handed to
//!   the consumer, and the split is recorded Completed only after the consumer returns
//!   `Ok` having read it to the end.
//!
//! Module layout: `filters` (parsing/lowering/preview), `splits` (split planning, plan
//! identity for the checkpoint fingerprint, per-split stream), `run` (the one `scan_split`
//! dispatch, the terminals and the checkpoint contract).

mod filters;
mod run;
mod splits;

use std::path::Path;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{DataFrame, SessionContext, ident};
use datafusion::sql::TableReference;

use crate::config::JobConfig;
use crate::connector::postgres::PostgresTableProvider;
use crate::connector::postgres::distributed::connection::PostgresConnectionDescriptor;
use crate::errors::AppError;
use crate::pushdown::cost_model::CostParams;

pub use filters::{FilterDecision, parse_filter_expr, parse_filter_shorthand};
pub(crate) use run::DistributedTarget;
pub use run::RunOutcome;
pub use splits::SplitInfo;

/// A runnable extraction job built from a validated [`JobConfig`].
#[derive(Debug)]
pub struct Pipeline {
    config: JobConfig,
}

impl Pipeline {
    /// Build a pipeline from an already-parsed config. Runs `JobConfig::validate`, so a
    /// degenerate value (`batch_size = 0`, zero partitions, …) is an error here rather than a
    /// silently empty extraction later.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use el_ballista::config::JobConfig;
    /// use el_ballista::connector::postgres::pipeline::Pipeline;
    ///
    /// let config = JobConfig::from_file("job.json")?;
    /// let pipeline = Pipeline::from_config(config)?;
    /// println!("{:?}", pipeline.filter_exprs()?);
    /// # Ok::<(), el_ballista::errors::AppError>(())
    /// ```
    pub fn from_config(config: JobConfig) -> Result<Self, AppError> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Parse (and validate) a config file and build a pipeline from it.
    pub(crate) fn from_config_file(path: impl AsRef<Path>) -> Result<Self, AppError> {
        Self::from_config(JobConfig::from_file(path)?)
    }

    /// The config backing this pipeline.
    pub(crate) fn config(&self) -> &JobConfig {
        &self.config
    }

    fn columns(&self) -> Option<Vec<&str>> {
        self.config
            .columns
            .as_ref()
            .map(|c| c.iter().map(String::as_str).collect())
    }

    /// The cost-aware provider for this job's table (schema discovery + statistics through
    /// the shared budgeted pool). Every single-node path and the preview use it, so preview
    /// and execution decide pushdown identically.
    pub(crate) async fn provider(&self) -> Result<PostgresTableProvider, AppError> {
        let descriptor = PostgresConnectionDescriptor::from_config(&self.config.source, 1);
        let pushdown = &self.config.pushdown;
        let provider = PostgresTableProvider::new(
            descriptor,
            &self.config.resolved_table(),
            pushdown.policy,
            pushdown.deny.clone(),
            pushdown.push.clone(),
            CostParams {
                max_source_cost: pushdown.max_source_cost,
                keep_threshold: pushdown.keep_threshold,
            },
            pushdown.statistics_ttl_secs,
            self.config.execution.batch_size,
        )
        .await?;
        Ok(provider
            .with_parallel_strategy(self.config.parallel_scan.strategy)
            .with_max_batch_bytes(self.config.execution.max_batch_bytes)
            .with_use_copy(self.config.execution.use_copy)
            .with_copy_statement_timeout_ms(self.config.execution.copy_statement_timeout_ms))
    }

    /// A DataFrame over the job's table with the job's filters (the same schema-coerced
    /// predicates every run uses), its column projection and `LIMIT limit` — what `el-ballista plan`
    /// shows. Unsplit and checkpoint-free; the limit pushes to the source when the pushdown
    /// rules allow it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), el_ballista::errors::AppError> {
    /// use el_ballista::connector::postgres::PostgresConnector;
    ///
    /// let connector = PostgresConnector::from_config_file("job.json")?;
    /// connector.pipeline().preview(20).await?.show().await?;
    /// # Ok(()) }
    /// ```
    pub async fn preview(&self, limit: usize) -> Result<DataFrame, AppError> {
        let provider = self.provider().await?;
        let filters = self
            .filter_exprs_with_schema(&datafusion::datasource::TableProvider::schema(&provider))?;
        let state = SessionStateBuilder::new().with_default_features().build();
        let ctx = SessionContext::new_with_state(state);
        let name = TableReference::bare(self.config.table.as_str());
        ctx.register_table(name.clone(), Arc::new(provider))?;
        let mut df = ctx.table(name).await?;
        for expr in filters {
            df = df.filter(expr)?;
        }
        if let Some(columns) = &self.config.columns {
            df = df.select(
                columns
                    .iter()
                    .map(|c| ident(c.as_str()))
                    .collect::<Vec<_>>(),
            )?;
        }
        Ok(df.limit(0, Some(limit))?)
    }
}
