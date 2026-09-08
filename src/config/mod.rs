//! Job and connection configuration.
//! config/mod.rs
//! Loads a job spec from JSON (see `extract.example.json`). Deliberately narrower than the
//! `extract.toml` shape in docs/architecture.md §6 — only the fields Phase 1 actually branches
//! on. Credentials are never inlined: the password is read from an environment variable named
//! in the file, per the doc's `dsn_env` convention.

use std::env;
use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::errors::AppError;

#[derive(Debug, Clone, Deserialize)]
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
}

impl JobConfig {
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
