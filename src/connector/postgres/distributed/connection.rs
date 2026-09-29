//! Process-independent description of a Postgres source.
//! distributed/connection.rs
//! Scheduler and executor processes can't share a `PgPool` — pools live inside one process and
//! are built from records in the source database. So serialized plans carry this descriptor
//! instead, and each process resolves its own (budgeted, shared) pool from it. Only the name of
//! the password's environment variable is carried: never the password itself.

use std::env;

use serde::{Deserialize, Serialize};

use crate::config::SourceConfig;
use crate::connector::SourceDescriptor;
use crate::connector::errors::ExtractorError;

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
    /// Describe `source` for `expected_workers` processes that share its `pool_max` budget.
    /// Only the password's environment-variable name is copied, never the password.
    ///
    /// # Examples
    ///
    /// ```
    /// use el_ballista::config::SourceConfig;
    /// use el_ballista::connector::postgres::distributed::PostgresConnectionDescriptor;
    ///
    /// let source: SourceConfig = serde_json::from_str(
    ///     r#"{"host": "db", "port": 5432, "user": "etl", "password_env": "PGPASSWORD", "database": "shop"}"#,
    /// )?;
    /// let descriptor = PostgresConnectionDescriptor::from_config(&source, 4);
    /// assert_eq!((descriptor.expected_workers, descriptor.password_env.as_str()), (4, "PGPASSWORD"));
    /// # Ok::<(), serde_json::Error>(())
    /// ```
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
    pub(crate) fn budgeted_max_connections(&self) -> u32 {
        (self.pool_max / self.expected_workers.max(1) as u32).max(1)
    }

    /// How long a scan may wait for a pooled connection. Scans of one run share the pool
    /// (the per-process budget), so a scan waiting behind another scan's connection is
    /// normal and must not fail: the wait is at least the statement timeout (a stuck holder
    /// is bounded by it), never the 30 s default cliff. `statement_timeout_ms = 0` (no
    /// statement timeout) waits up to one hour.
    pub(crate) fn acquire_timeout(&self) -> std::time::Duration {
        const FLOOR: std::time::Duration = std::time::Duration::from_secs(30);
        match self.statement_timeout_ms {
            0 => std::time::Duration::from_secs(3600),
            ms => std::time::Duration::from_millis(ms).max(FLOOR),
        }
    }

    /// Identifies one shared pool per source within a process. Everything that changes what
    /// a pooled session is (target, credentials source, session settings, budget) is part of
    /// the key, so a second job never silently inherits another job's `statement_timeout`,
    /// `application_name` or password, and two jobs that disagree about `expected_workers`
    /// don't share a pool sized wrong for either.
    pub(crate) fn pool_key(&self) -> PoolKey {
        PoolKey {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            database: self.database.clone(),
            password_env: self.password_env.clone(),
            statement_timeout_ms: self.statement_timeout_ms,
            application_name: self.application_name.clone(),
            budget: self.budgeted_max_connections(),
        }
    }

    pub(crate) fn resolved_password(&self) -> Result<String, ExtractorError> {
        env::var(&self.password_env).map_err(|_| {
            ExtractorError::Internal(format!(
                "environment variable '{}' (source.password_env) is not set",
                self.password_env
            ))
        })
    }
}

/// The registry identity of one budgeted pool (see
/// `PostgresConnectionDescriptor::pool_key`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    pub password_env: String,
    pub statement_timeout_ms: u64,
    pub application_name: String,
    pub budget: u32,
}

impl SourceDescriptor for PostgresConnectionDescriptor {
    fn registry_key(&self) -> String {
        let k = self.pool_key();
        // Debug formatting quotes every string component, so no component can smuggle a
        // separator into another's position.
        format!(
            "pg:{:?}:{}:{:?}:{:?}:{:?}:{}:{:?}:{}",
            k.host,
            k.port,
            k.user,
            k.database,
            k.password_env,
            k.statement_timeout_ms,
            k.application_name,
            k.budget
        )
    }
}
