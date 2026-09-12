//! Job and connection configuration.
//! config/mod.rs
//! Loads a job spec from JSON (see `extract.example.json`). Deliberately narrower than the
//! `extract.toml` shape in docs/architecture.md §6 — only the fields Phase 1 actually branches
//! on. Credentials are never inlined: the password is read from an environment variable named
//! in the file, per the doc's `dsn_env` convention.

use std::env;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::errors::AppError;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    /// Name of the environment variable holding the password.
    pub password_env: String,
    pub database: String,
    #[serde(default = "default_pool_max")]
    pub pool_max: u32,
    #[serde(default = "default_statement_timeout_ms")]
    pub statement_timeout_ms: u64,
    #[serde(default = "default_application_name")]
    pub application_name: String,
    #[serde(default = "default_schema")]
    pub schema: String,
}

fn default_pool_max() -> u32 {
    8
}

fn default_statement_timeout_ms() -> u64 {
    300_000
}

fn default_application_name() -> String {
    "rust-extract-layer".to_string()
}

fn default_schema() -> String {
    "public".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct IncrementalConfig {
    /// The watermark column, e.g. `updated_at`. Only `timestamp` mode is implemented —
    /// `append_id` / `snapshot` / `log` from docs/incremental-extraction.md §1 are follow-ups.
    pub column: String,
    #[serde(default = "default_safety_lag_secs")]
    pub safety_lag_secs: i64,
    #[serde(default = "default_max_window_secs")]
    pub max_window_secs: i64,
}

fn default_safety_lag_secs() -> i64 {
    300
}

fn default_max_window_secs() -> i64 {
    6 * 3600
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinkConfig {
    /// Local filesystem directory. Object-store URIs (gs://...) are a follow-up — see
    /// docs/phase-one-implementation-plan.md §7.
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CheckpointConfig {
    /// Directory holding one JSON checkpoint file per job. File-based by design for now —
    /// see docs/phase-one-implementation-plan.md §4 for why a Postgres-backed store is deferred.
    #[serde(default = "default_checkpoint_dir")]
    pub dir: String,
}

fn default_checkpoint_dir() -> String {
    ".checkpoints".to_string()
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            dir: default_checkpoint_dir(),
        }
    }
}

/// docs/pushdown.md §4.3. Cost-based pushdown with per-source budgets and selectivity thresholds.
#[derive(Debug, Clone, Deserialize)]
pub struct PushdownConfig {
    #[serde(default = "default_pushdown_policy")]
    pub policy: String,
    #[serde(default)]
    pub deny: Vec<String>,
    /// `hinted` policy: predicates touching these columns are forced to the source
    /// (whenever they translate — hints never override correctness). Deny wins over push.
    #[serde(default)]
    pub push: Vec<String>,
    #[serde(default = "default_max_source_cost")]
    pub max_source_cost: u64,
    #[serde(default = "default_keep_threshold")]
    pub keep_threshold: f64,
    #[serde(default = "default_statistics_ttl_secs")]
    pub statistics_ttl_secs: u64,
}

fn default_pushdown_policy() -> String {
    "cost_based".to_string()
}

fn default_max_source_cost() -> u64 {
    50_000
}

fn default_keep_threshold() -> f64 {
    0.30
}

fn default_statistics_ttl_secs() -> u64 {
    900
}

impl Default for PushdownConfig {
    fn default() -> Self {
        Self {
            policy: default_pushdown_policy(),
            deny: Vec::new(),
            push: Vec::new(),
            max_source_cost: default_max_source_cost(),
            keep_threshold: default_keep_threshold(),
            statistics_ttl_secs: default_statistics_ttl_secs(),
        }
    }
}

/// Phase 3: Execution configuration for streaming batches and memory management.
#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    8192
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
        }
    }
}

/// Phase 4 (docs/roadmap.md): distributed execution over Ballista.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DistributedConfig {
    /// Remote scheduler endpoint (`http://host:port`), or empty for a standalone deployment
    /// where the scheduler and `parallel_workers` executors run in this process.
    #[serde(default = "default_scheduler_url")]
    pub scheduler_url: String,
    /// Number of executor processes (standalone mode) or expected workers (remote mode — used
    /// only to budget the per-process source connection pools; see docs/roadmap.md Phase 4).
    #[serde(default = "default_workers")]
    pub workers: usize,
}

fn default_scheduler_url() -> String {
    String::new()
}

fn default_workers() -> usize {
    1
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            scheduler_url: default_scheduler_url(),
            workers: default_workers(),
        }
    }
}

/// Parallel scan configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ParallelScanConfig {
    #[serde(default = "default_parallel_strategy")]
    pub strategy: String,
    #[serde(default = "default_parallel_partitions")]
    pub partitions: usize,
    #[serde(default = "default_partition_column")]
    pub partition_column: String,
}

fn default_parallel_strategy() -> String {
    "none".to_string()
}

fn default_parallel_partitions() -> usize {
    1
}

fn default_partition_column() -> String {
    "id".to_string()
}

impl Default for ParallelScanConfig {
    fn default() -> Self {
        Self {
            strategy: default_parallel_strategy(),
            partitions: default_parallel_partitions(),
            partition_column: default_partition_column(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobConfig {
    pub job_id: String,
    pub table: String,
    /// `None` means "all columns" (still resolved against the catalog schema).
    pub columns: Option<Vec<String>>,
    pub source: SourceConfig,
    pub incremental: IncrementalConfig,
    pub sink: SinkConfig,
    #[serde(default)]
    pub checkpoint: CheckpointConfig,
    #[serde(default)]
    pub pushdown: PushdownConfig,
    #[serde(default)]
    pub parallel_scan: ParallelScanConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub distributed: DistributedConfig,
}

impl JobConfig {
    pub fn resolved_table(&self) -> String {
        if self.table.contains('.') {
            self.table.clone()
        } else {
            format!("{}.{}", self.source.schema, self.table)
        }
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, AppError> {
        let path = path.as_ref();

        let text = fs::read_to_string(path)
            .map_err(|e| AppError::Config(format!("cannot read {}: {e}", path.display())))?;

        let config: JobConfig = serde_json::from_str(&text)
            .map_err(|e| AppError::Config(format!("invalid config {}: {e}", path.display())))?;

        Ok(config)
    }

    pub fn resolve_password(&self) -> Result<String, AppError> {
        env::var(&self.source.password_env).map_err(|_| {
            AppError::Config(format!(
                "environment variable '{}' is not set (source.password_env)",
                self.source.password_env
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolved_table() {
        let mut config = JobConfig {
            job_id: "test".to_string(),
            table: "orders".to_string(),
            columns: None,
            source: SourceConfig {
                host: "localhost".to_string(),
                port: 5432,
                user: "postgres".to_string(),
                password_env: "TEST_PG_PASS".to_string(),
                database: "db".to_string(),
                pool_max: 4,
                statement_timeout_ms: 1000,
                application_name: "test".to_string(),
                schema: "public".to_string(),
            },
            incremental: IncrementalConfig {
                column: "updated_at".to_string(),
                safety_lag_secs: 60,
                max_window_secs: 3600,
            },
            sink: SinkConfig {
                path: "./tmp".to_string(),
            },
            checkpoint: CheckpointConfig::default(),
            pushdown: PushdownConfig::default(),
            parallel_scan: ParallelScanConfig::default(),
            execution: ExecutionConfig::default(),
            distributed: DistributedConfig::default(),
        };

        assert_eq!(config.resolved_table(), "public.orders");

        config.table = "custom.orders".to_string();
        assert_eq!(config.resolved_table(), "custom.orders");
    }

    #[test]
    fn test_resolve_password() {
        let config = JobConfig {
            job_id: "test".to_string(),
            table: "orders".to_string(),
            columns: None,
            source: SourceConfig {
                host: "localhost".to_string(),
                port: 5432,
                user: "postgres".to_string(),
                password_env: "REL_TEST_PASSWORD_ENV".to_string(),
                database: "db".to_string(),
                pool_max: 4,
                statement_timeout_ms: 1000,
                application_name: "test".to_string(),
                schema: "public".to_string(),
            },
            incremental: IncrementalConfig {
                column: "updated_at".to_string(),
                safety_lag_secs: 60,
                max_window_secs: 3600,
            },
            sink: SinkConfig {
                path: "./tmp".to_string(),
            },
            checkpoint: CheckpointConfig::default(),
            pushdown: PushdownConfig::default(),
            parallel_scan: ParallelScanConfig::default(),
            execution: ExecutionConfig::default(),
            distributed: DistributedConfig::default(),
        };

        unsafe {
            env::remove_var("REL_TEST_PASSWORD_ENV");
        }
        assert!(config.resolve_password().is_err());

        unsafe {
            env::set_var("REL_TEST_PASSWORD_ENV", "secret123");
        }
        assert_eq!(config.resolve_password().unwrap(), "secret123");
        unsafe {
            env::remove_var("REL_TEST_PASSWORD_ENV");
        }
    }
}
