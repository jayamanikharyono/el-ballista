//! Fluent extraction entry point for the Postgres connector.
//!
//! `PostgresConnector::from_config(cfg).extract()` returns a builder; choose the execution
//! target and finish with a terminal:
//!
//! ```ignore
//! // single node
//! let batches = connector.extract().standalone().collect().await?;
//! let outcome = connector.extract().standalone().run().await?;
//! // distributed over Ballista (defaults to the standard scheduler URL)
//! let outcome = connector.extract().distributed().run().await?;
//! let outcome = connector.extract().distributed().scheduler("http://host:50050").workers(4).run().await?;
//! let outcome = connector.extract().distributed().in_process().run().await?;
//! ```
//!
//! `collect()` returns the Arrow batches with no checkpoint side effects; `run()` performs the
//! operational job (checkpoint protocol in incremental mode) and returns a [`RunOutcome`].

use arrow::record_batch::RecordBatch;

use super::pipeline::{Pipeline, RunOutcome};
use crate::config::JobConfig;
use crate::errors::AppError;

/// Default Ballista scheduler endpoint used by `.distributed()` when the config sets none.
pub const DEFAULT_SCHEDULER_URL: &str = "http://localhost:50050";

/// A Postgres source connector built from a job config — the single entry point for extraction.
pub struct PostgresConnector {
    pipeline: Pipeline,
}

impl PostgresConnector {
    /// Build a connector from an already-parsed config.
    pub fn from_config(config: JobConfig) -> Self {
        Self {
            pipeline: Pipeline::from_config(config),
        }
    }

    /// Parse a config file and build a connector from it.
    pub fn from_config_file(path: impl AsRef<std::path::Path>) -> Result<Self, AppError> {
        Ok(Self {
            pipeline: Pipeline::from_config_file(path)?,
        })
    }

    /// The config backing this connector.
    pub fn config(&self) -> &JobConfig {
        self.pipeline.config()
    }

    /// Begin an extraction; pick the target with [`ExtractBuilder::standalone`] or
    /// [`ExtractBuilder::distributed`].
    pub fn extract(&self) -> ExtractBuilder<'_> {
        ExtractBuilder {
            pipeline: &self.pipeline,
        }
    }
}

/// Chooses the execution target for an extraction.
pub struct ExtractBuilder<'a> {
    pipeline: &'a Pipeline,
}

impl<'a> ExtractBuilder<'a> {
    /// Single-node extraction in this process.
    pub fn standalone(self) -> StandaloneExtraction<'a> {
        StandaloneExtraction {
            pipeline: self.pipeline,
        }
    }

    /// Distributed extraction over Ballista. The scheduler endpoint defaults to the config's
    /// `distributed.scheduler_url`, or [`DEFAULT_SCHEDULER_URL`] when that is empty; override with
    /// [`DistributedExtraction::scheduler`] or run a local cluster with
    /// [`DistributedExtraction::in_process`].
    pub fn distributed(self) -> DistributedExtraction<'a> {
        let configured = self.pipeline.config().distributed.scheduler_url.trim();
        let scheduler = if configured.is_empty() {
            DEFAULT_SCHEDULER_URL.to_string()
        } else {
            configured.to_string()
        };
        DistributedExtraction {
            pipeline: self.pipeline,
            scheduler: Some(scheduler),
            workers: None,
        }
    }
}

/// A single-node extraction, ready to `collect()` (data) or `run()` (operational job).
pub struct StandaloneExtraction<'a> {
    pipeline: &'a Pipeline,
}

impl StandaloneExtraction<'_> {
    /// Execute and return the Arrow batches. No checkpoint side effects.
    pub async fn collect(self) -> Result<Vec<RecordBatch>, AppError> {
        self.pipeline.extract().await
    }

    /// Execute the operational job (checkpoint protocol in incremental mode). Returns stats.
    pub async fn run(self) -> Result<RunOutcome, AppError> {
        self.pipeline.run().await
    }
}

/// A distributed (Ballista) extraction, ready to `collect()` or `run()`.
pub struct DistributedExtraction<'a> {
    pipeline: &'a Pipeline,
    /// `Some(url)` connects to that scheduler; `None` spins up an in-process cluster.
    scheduler: Option<String>,
    workers: Option<usize>,
}

impl DistributedExtraction<'_> {
    /// Connect to this Ballista scheduler endpoint instead of the default.
    pub fn scheduler(mut self, url: impl Into<String>) -> Self {
        self.scheduler = Some(url.into());
        self
    }

    /// Run an in-process Ballista cluster instead of connecting to an external scheduler.
    pub fn in_process(mut self) -> Self {
        self.scheduler = None;
        self
    }

    /// Override the worker count (defaults to the config's `distributed.workers`).
    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = Some(workers);
        self
    }

    /// Execute and return the Arrow batches. No checkpoint side effects.
    pub async fn collect(self) -> Result<Vec<RecordBatch>, AppError> {
        self.pipeline
            .extract_distributed(self.workers, self.scheduler.as_deref())
            .await
    }

    /// Execute the operational job (checkpoint protocol in incremental mode). Returns stats.
    pub async fn run(self) -> Result<RunOutcome, AppError> {
        self.pipeline
            .run_distributed(self.workers, self.scheduler.as_deref())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distributed_defaults_to_standard_scheduler_when_config_empty() {
        // extract.example.json leaves distributed.scheduler_url empty, so `.distributed()`
        // falls back to the standard endpoint.
        let connector =
            PostgresConnector::from_config_file("examples/configs/extract.example.json").unwrap();
        let dist = connector.extract().distributed();
        assert_eq!(dist.scheduler.as_deref(), Some(DEFAULT_SCHEDULER_URL));
    }

    #[test]
    fn scheduler_and_in_process_override_the_default() {
        let connector =
            PostgresConnector::from_config_file("examples/configs/extract.example.json").unwrap();
        let dist = connector.extract().distributed().scheduler("http://sched:50050");
        assert_eq!(dist.scheduler.as_deref(), Some("http://sched:50050"));
        let local = connector.extract().distributed().in_process();
        assert!(local.scheduler.is_none());
    }
}
