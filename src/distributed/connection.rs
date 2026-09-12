//! Process-independent description of a Postgres source.
//! distributed/connection.rs
//! Scheduler and executor processes can't share a `PgPool` — pools live inside one process and
//! are built from records in the source database. So serialized plans carry this descriptor
//! instead, and each process resolves its own (budgeted, shared) pool from it. Only the name of
//! the password's environment variable is carried: never the password itself.

use std::env;

use serde::{Deserialize, Serialize};

use crate::config::SourceConfig;
use crate::connector::errors::ExtractorError;
use crate::connector::SourceDescriptor;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresConnectionDescriptor {
    pub host: String,
    pub port: u16,
    pub user: String,
    /// Name of the environment variable holding the password. Resolved in the *executing*
    /// process, never serialized as plaintext.
    pub password_env: String,
    pub database: String,
    /// Cluster-wide connection budget, per docs/roadmap.md Phase 4.
    pub pool_max: u32,
    /// Number of processes expected to share the budget (`workers` from `DistributedConfig`).
    pub expected_workers: usize,
    pub statement_timeout_ms: u64,
    pub application_name: String,
    pub schema: String,
}

impl PostgresConnectionDescriptor {
    pub fn from_config(source: &SourceConfig, expected_workers: usize) -> Self {
        Self {
            host: source.host.clone(),
            port: source.port,
            user: source.user.clone(),
            password_env: source.password_env.clone(),
            database: source.database.clone(),
            pool_max: source.pool_max,
            expected_workers,
            statement_timeout_ms: source.statement_timeout_ms,
            application_name: source.application_name.clone(),
            schema: source.schema.clone(),
        }
    }

    /// docs/roadmap.md Phase 4 — connection-pool coordination: `pool_max` is the cluster-wide
    /// budget for the source, so each process requests only its share. At least one connection,
    /// because a worker without any pool at all can't scan anything.
    pub fn budgeted_max_connections(&self) -> u32 {
        (self.pool_max / self.expected_workers.max(1) as u32).max(1)
    }

    /// Identifies one shared pool per source within a process. The budget is part of the key so
    /// two jobs that disagree about `expected_workers` don't silently share a pool sized wrong
    /// for either.
    pub fn pool_key(&self) -> (String, u16, String, String, u32) {
        (
            self.host.clone(),
            self.port,
            self.user.clone(),
            self.database.clone(),
            self.budgeted_max_connections(),
        )
    }

    pub fn resolved_password(&self) -> Result<String, ExtractorError> {
        env::var(&self.password_env).map_err(|_| {
            ExtractorError::Internal(format!(
                "environment variable '{}' (source.password_env) is not set",
                self.password_env
            ))
        })
    }
}

impl SourceDescriptor for PostgresConnectionDescriptor {
    fn registry_key(&self) -> String {
        let (host, port, user, database, budget) = self.pool_key();
        format!("pg:{host}:{port}:{user}:{database}:{budget}")
    }
}