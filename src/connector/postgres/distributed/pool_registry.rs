//! Process-wide sharing of budgeted source pools.
//! distributed/pool_registry.rs
//! Every scan in a process — the client's planning queries, and each scan task of an
//! `el-ballista worker` — must share one budgeted pool per source, or a process running N
//! scans would open N × its share, exactly the failure mode docs/roadmap.md Phase 4 names. A
//! single process-wide registry keys one pool per source; a worker that materializes a
//! serialized scan plan resolves the descriptor lazily through the same registry, so it opens
//! one `budgeted_max_connections`-sized pool per source however many tasks it runs.
//!
//! Lifecycle: pools are created lazily on first use and shared for the life of the process.
//! Idle connections are closed after [`IDLE_TIMEOUT`], so a pool nobody uses any more holds
//! no source connections; [`SourcePoolRegistry::close_all`] closes every pool gracefully
//! (the CLI calls it before exiting; long-lived library hosts can call it at shutdown), and
//! closed pools are pruned from the registry.
//!
//! The registry is deliberately `PgPool`-concrete (see the SPI contract in
//! `crate::connector`): a MySQL backend owns an analogous registry over its own pool type,
//! keyed by its own [`crate::connector::SourceDescriptor`] implementation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use sqlx::ConnectOptions;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::sync::Semaphore;

use super::connection::PostgresConnectionDescriptor;
use crate::connector::SourceDescriptor;
use crate::connector::errors::ExtractorError;

/// Idle pooled connections are closed after this long.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// The process-wide pool registry (created on first use).
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
    pools: Mutex<HashMap<String, Entry>>,
}

/// One registered source: its budgeted pool plus the scan limiter sized to the same budget.
#[derive(Debug, Clone)]
struct Entry {
    pool: PgPool,
    scans: Arc<Semaphore>,
}

impl SourcePoolRegistry {
    fn new() -> Self {
        Self::default()
    }

    /// Returns the process-shared pool for a source, creating it on first use (lazily, so a
    /// scheduler that only plans never opens a single connection) with the descriptor's
    /// *budgeted* max_connections and the session-hygiene settings from
    /// docs/connectors/postgres.md §6.
    pub(crate) fn pool(
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
        self.entry(descriptor).map(|e| e.pool)
    }

    /// The process-wide **scan limiter** for a source: a semaphore with one permit per
    /// budgeted connection (the same budget as [`Self::pool`]). A partition scan holds a
    /// permit for its whole duration, so at most `budgeted_max_connections` scans of this
    /// source run at once in this process and the rest *wait* (cancellably, without a
    /// timeout) instead of failing on the pool's acquire timeout. Metadata queries (schema,
    /// statistics, EXPLAIN) do not take permits; they are short and use the pool directly.
    pub(crate) fn scan_slots(
        &self,
        descriptor: &PostgresConnectionDescriptor,
    ) -> Result<Arc<Semaphore>, ExtractorError> {
        self.entry(descriptor).map(|e| e.scans)
    }

    fn entry(&self, descriptor: &PostgresConnectionDescriptor) -> Result<Entry, ExtractorError> {
        let size = descriptor.budgeted_max_connections();
        let key = format!("{}|rt={}", descriptor.registry_key(), runtime_key());
        let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        pools.retain(|_, entry| !entry.pool.is_closed());

        if let Some(entry) = pools.get(&key) {
            return Ok(entry.clone());
        }

        let password = descriptor.resolved_password()?;

        let connect_options = PgConnectOptions::new()
            .host(&descriptor.host)
            .port(descriptor.port)
            .username(&descriptor.user)
            .password(&password)
            .database(&descriptor.database)
            .application_name(&descriptor.application_name)
            // sqlx logs every statement it runs (each cursor FETCH included): per-FETCH detail
            // is `trace`; the scan's own SQL is logged at `debug` by the execution plan. A slow
            // statement is not degraded operation, so it is not a warning either.
            .log_statements(log::LevelFilter::Trace)
            .log_slow_statements(log::LevelFilter::Debug, Duration::from_secs(1));

        let statement_timeout = format!("{}ms", descriptor.statement_timeout_ms);

        let pool = PgPoolOptions::new()
            .max_connections(size.max(1))
            .acquire_timeout(descriptor.acquire_timeout())
            .idle_timeout(IDLE_TIMEOUT)
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

        let budget = usize::try_from(size).unwrap_or(1);
        let entry = Entry {
            pool,
            scans: Arc::new(Semaphore::new(budget.max(1))),
        };
        pools.insert(key, entry.clone());
        Ok(entry)
    }

    /// Open pools (closed ones are pruned first).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
        pools.retain(|_, entry| !entry.pool.is_closed());
        pools.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Close every registered pool (waiting for checked-out connections to return) and
    /// clear the registry. Later `pool()` calls open fresh pools.
    pub async fn close_all(&self) {
        let pools: Vec<PgPool> = {
            let mut pools = self.pools.lock().unwrap_or_else(|e| e.into_inner());
            pools.drain().map(|(_, entry)| entry.pool).collect()
        };
        for pool in pools {
            pool.close().await;
        }
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
        pool: OnceLock<Result<PgPool, Arc<ExtractorError>>>,
    },
}

impl SourcePool {
    pub(crate) fn connected(pool: PgPool) -> Self {
        Self::Connected(pool)
    }

    pub(crate) fn deferred(descriptor: PostgresConnectionDescriptor) -> Self {
        Self::Deferred {
            descriptor,
            pool: OnceLock::new(),
        }
    }

    pub(crate) fn get(&self) -> Result<PgPool, ExtractorError> {
        match self {
            Self::Connected(pool) => Ok(pool.clone()),
            Self::Deferred { descriptor, pool } => {
                let result = pool.get_or_init(|| registry().pool(descriptor).map_err(Arc::new));
                match result {
                    Ok(pool) => Ok(pool.clone()),
                    Err(e) => Err(ExtractorError::Shared {
                        context: "cannot open source pool".to_string(),
                        source: Arc::clone(e),
                    }),
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
    fn test_pool_key_includes_session_settings_and_credentials_source() {
        // A second job must not inherit the first job's session settings.
        let base = descriptor(1);
        let mut timeout = base.clone();
        timeout.statement_timeout_ms = 5;
        let mut app = base.clone();
        app.application_name = "other".into();
        let mut pw = base.clone();
        pw.password_env = "OTHER_ENV".into();
        for other in [timeout, app, pw] {
            assert_ne!(base.pool_key(), other.pool_key());
            assert_ne!(base.registry_key(), other.registry_key());
        }
    }

    #[tokio::test]
    async fn test_scan_slots_match_the_budget_and_are_shared() {
        // One limiter per source, sized to the per-process connection budget.
        let mut d = descriptor(4); // pool_max 8 / 4 workers = 2
        d.password_env = "PATH".into(); // lazy pool; any set variable works
        let a = registry().scan_slots(&d).unwrap();
        let b = registry().scan_slots(&d).unwrap();
        assert!(Arc::ptr_eq(&a, &b), "same source, same limiter");
        assert_eq!(a.available_permits(), 2);
        let _held = a.clone().acquire_many_owned(2).await.unwrap();
        assert!(
            b.clone().try_acquire_owned().is_err(),
            "a third scan must wait"
        );
    }

    #[test]
    fn test_acquire_timeout_never_undercuts_statement_timeout() {
        // A scan waiting for a connection held by a sibling scan must not hit a 30 s
        // cliff while the holder is still within its statement timeout.
        let mut d = descriptor(1);
        d.statement_timeout_ms = 300_000;
        assert_eq!(d.acquire_timeout(), Duration::from_secs(300));
        d.statement_timeout_ms = 1_000;
        assert_eq!(d.acquire_timeout(), Duration::from_secs(30));
        d.statement_timeout_ms = 0;
        assert_eq!(d.acquire_timeout(), Duration::from_secs(3600));
    }

    #[tokio::test]
    async fn test_close_all_empties_the_registry() {
        let reg = SourcePoolRegistry::new();
        // Lazy pool, never connects: an always-present variable is a valid password source,
        // so the test does not mutate the environment.
        let mut d = descriptor(1);
        d.password_env = "PATH".into();
        let pool = reg.pool(&d).unwrap(); // lazy: no connection is opened
        assert_eq!(reg.len(), 1);
        reg.close_all().await;
        assert!(pool.is_closed());
        assert!(reg.is_empty());
    }

    #[test]
    fn test_deferred_get_fails_cleanly_without_password_env() {
        // `UNUSED_TEST_ENV` is never set anywhere (no env mutation needed).
        let pool = SourcePool::deferred(descriptor(1));
        assert!(pool.get().is_err());
        // A second get must not panic and must keep failing (lazy init already cached the error).
        assert!(pool.get().is_err());
    }
}
