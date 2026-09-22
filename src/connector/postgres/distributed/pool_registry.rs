//! Process-wide sharing of budgeted source pools.
//! distributed/pool_registry.rs
//! The client (scheduler or the `rel distribute` process) and every Ballista executor in the
//! same process must not each open their own `pool_max` connections to the source — with N
//! executors that would be N × `pool_max`, exactly the failure mode docs/roadmap.md Phase 4
//! names. A single process-wide registry keys one pool per source; executors that materialize a
//! serialized scan plan resolve the descriptor lazily through the same registry, so an
//! N-worker process still opens one `budgeted_max_connections`-sized pool per source.
//!
//! The registry is deliberately `PgPool`-concrete (see the SPI contract in
//! `crate::connector`): a MySQL backend owns an analogous registry over its own pool type,
//! keyed by its own [`crate::connector::SourceDescriptor`] implementation.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use super::connection::PostgresConnectionDescriptor;
use crate::connector::SourceDescriptor;
use crate::connector::errors::ExtractorError;

pub fn registry() -> &'static SourcePoolRegistry {
    static REGISTRY: OnceLock<SourcePoolRegistry> = OnceLock::new();
    REGISTRY.get_or_init(SourcePoolRegistry::new)
}

/// Identifies the current tokio runtime for pool sharing. sqlx sockets only
/// make progress on the reactor that opened them, so pools must never cross a
/// runtime boundary. Outside any runtime (a scheduler that only plans) there
/// is nothing to isolate from — those pools stay lazy and never connect.
fn runtime_key() -> String {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => format!("{:?}", handle.id()),
        Err(_) => "no-runtime".to_string(),
    }
}

#[derive(Debug, Default)]
pub struct SourcePoolRegistry {
    pools: Mutex<HashMap<String, PgPool>>,
}

impl SourcePoolRegistry {
    fn new() -> Self {
        Self::default()
    }

    /// Returns the process-shared pool for a source, creating it on first use (lazily, so a
    /// scheduler that only plans never opens a single connection) with the descriptor's
    /// *budgeted* max_connections and the session-hygiene settings from
    /// docs/connectors/postgres.md §6.
    pub fn pool(
        &self,
        descriptor: &PostgresConnectionDescriptor,
    ) -> Result<PgPool, ExtractorError> {
        // sqlx connections are bound to the I/O driver (reactor) of the tokio
        // runtime that opened them. A process-global pool outlives any one
        // runtime, so a pool created under a dead runtime (e.g. a previous
        // #[tokio::test]'s per-test runtime) hands out sockets that can never
        // make progress — acquires stall until timeout. Keying pools per
        // runtime keeps the documented invariant (one pool per source wherever
        // there is a single runtime: every CLI invocation, the scheduler, and
        // all executors sharing an executor process) while isolating runtimes
        // that merely share a process.
        let key = format!("{}|rt={}", descriptor.registry_key(), runtime_key());
        let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());

        if let Some(pool) = pools.get(&key) {
            return Ok(pool.clone());
        }

        let password = descriptor.resolved_password()?;

        let connect_options = PgConnectOptions::new()
            .host(&descriptor.host)
            .port(descriptor.port)
            .username(&descriptor.user)
            .password(&password)
            .database(&descriptor.database)
            .application_name(&descriptor.application_name);

        let statement_timeout = format!("{}ms", descriptor.statement_timeout_ms);

        let pool = PgPoolOptions::new()
            .max_connections(descriptor.budgeted_max_connections())
            .acquire_timeout(std::time::Duration::from_secs(30))
            .after_connect(move |conn, _meta| {
                let statement_timeout = statement_timeout.clone();
                Box::pin(async move {
                    sqlx::query("SET TIME ZONE 'UTC'")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "SET statement_timeout = '{statement_timeout}'"
                    )))
                    .execute(&mut *conn)
                    .await?;
                    sqlx::query("SET idle_in_transaction_session_timeout = '60s'")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("SET lock_timeout = '5s'")
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect_lazy_with(connect_options);

        pools.insert(key, pool.clone());
        Ok(pool)
    }
}

/// A source pool that may or may not have been opened yet. The client process holds a connected
/// `PgPool` (created during registration); executors that decode a serialized plan hold a
/// `Deferred` descriptor and resolve the same process-shared pool on first use.
#[derive(Debug)]
pub enum SourcePool {
    Connected(PgPool),
    Deferred {
        descriptor: PostgresConnectionDescriptor,
        pool: OnceLock<Result<PgPool, String>>,
    },
}

impl SourcePool {
    pub fn connected(pool: PgPool) -> Self {
        Self::Connected(pool)
    }

    pub fn deferred(descriptor: PostgresConnectionDescriptor) -> Self {
        Self::Deferred {
            descriptor,
            pool: OnceLock::new(),
        }
    }

    pub fn get(&self) -> Result<PgPool, ExtractorError> {
        match self {
            Self::Connected(pool) => Ok(pool.clone()),
            Self::Deferred { descriptor, pool } => {
                let result =
                    pool.get_or_init(|| registry().pool(descriptor).map_err(|e| e.to_string()));
                match result {
                    Ok(pool) => Ok(pool.clone()),
                    Err(e) => Err(ExtractorError::Internal(format!(
                        "cannot open source pool: {e}"
                    ))),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(expected_workers: usize) -> PostgresConnectionDescriptor {
        PostgresConnectionDescriptor {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password_env: "UNUSED_TEST_ENV".to_string(),
            database: "db".to_string(),
            pool_max: 8,
            expected_workers,
            statement_timeout_ms: 1000,
            application_name: "test".to_string(),
            schema: "public".to_string(),
        }
    }

    #[test]
    fn test_budget_division() {
        // 8 connections across 4 workers -> 2 each; never less than 1.
        assert_eq!(descriptor(4).budgeted_max_connections(), 2);
        assert_eq!(descriptor(1).budgeted_max_connections(), 8);
        // 8 connections across 16 workers -> floor is 1, not 0.
        assert_eq!(descriptor(16).budgeted_max_connections(), 1);
    }

    #[test]
    fn test_runtime_key_isolates_runtimes() {
        // Outside any runtime there is nothing to isolate from.
        assert_eq!(runtime_key(), "no-runtime");
        let key_in = |rt: &tokio::runtime::Runtime| rt.block_on(async { runtime_key() });
        let a = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let b = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // Distinct runtimes (e.g. per-test runtimes) must not share pools:
        // sqlx sockets from a dead runtime never make progress again.
        assert_ne!(key_in(&a), key_in(&b));
        // Same runtime is stable: one pool per source still holds everywhere
        // it matters (CLI, scheduler, co-located executors).
        assert_eq!(key_in(&a), key_in(&a));
    }

    #[test]
    fn test_pool_key_includes_budget() {
        let a = descriptor(2);
        let b = descriptor(4);
        assert_ne!(a.pool_key(), b.pool_key());
        assert_eq!(a.pool_key(), a.pool_key());
    }

    #[test]
    fn test_deferred_get_fails_cleanly_without_password_env() {
        unsafe {
            std::env::remove_var("UNUSED_TEST_ENV");
        }
        let pool = SourcePool::deferred(descriptor(1));
        assert!(pool.get().is_err());
        // A second get must not panic and must keep failing (lazy init already cached the error).
        assert!(pool.get().is_err());
    }
}
