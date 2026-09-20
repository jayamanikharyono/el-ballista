#![allow(dead_code, clippy::all)]
//! Postgres test harness: a real database is always obtained, tests never skip.
//!
//! The live Postgres comes from the compose stack (`tests/docker/compose.yaml`) — the single source of truth
//! for a test database, the same one CI uses. `TestDb::connect()` reads `DATABASE_URL` and, when
//! unset, defaults to the compose endpoint `postgres://postgres:postgres@127.0.0.1:5432/test`. It
//! never self-provisions and never skips: a failure to connect is a hard `panic!`, not a silent
//! `None` — masking a broken harness as "skipped" is exactly what produced false-green CI in the
//! past.
//!
//! ```bash
//! docker compose -f tests/docker/compose.yaml up -d --wait
//! cargo test --test pg_paths -- --test-threads=1
//! docker compose -f tests/docker/compose.yaml down -v
//! ```
//!
//! Design: schema isolation (not database isolation) inside the one server — each `TestDb` gets
//! `test_<pid>_<n>`, builds the hostile fixture inside it, and drops the schema on `Drop`. Tests
//! can run in parallel.

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;
use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static COUNTER: AtomicU64 = AtomicU64::new(0);
static PASSWORD_ONCE: Once = Once::new();

/// Default Postgres endpoint when `DATABASE_URL` is unset: the compose stack's `postgres` service
/// (see `tests/docker/compose.yaml`), matching the CI environment.
const DEFAULT_URL: &str = "postgres://postgres:postgres@127.0.0.1:5432/test";

/// Name of the env var the test `JobConfig`s point at. Set once from the parsed `DATABASE_URL`
/// (or the compose default) password so configs never carry a password directly.
pub const TEST_PASSWORD_ENV: &str = "RELEX_TEST_PG_PASSWORD";

pub struct TestDb {
    pub pool: PgPool,
    pub schema: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl TestDb {
    /// Connect and provision a per-test schema. Never skips: either returns a live `TestDb`, or
    /// panics with a message explaining exactly what failed (server unreachable — is the compose
    /// stack up? — connection refused, or fixture setup failure).
    pub async fn connect() -> Self {
        let url = std::env::var("DATABASE_URL")
            .ok()
            .filter(|u| !u.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_URL.to_string());
        Self::connect_with(&url).await
    }

    /// Same, with an explicit URL (lets callers point at a specific database, e.g. one
    /// already exercised by a benchmark run).
    pub async fn connect_with(url: &str) -> Self {
        let (host, port, user, password, database) = parse_url(url).unwrap_or_else(|| {
            panic!(
                "TestDb::connect_with: cannot parse {url:?} as postgres://user:pass@host:port/db"
            )
        });

        let options = PgConnectOptions::from_str(url)
            .unwrap_or_else(|e| panic!("TestDb::connect_with: invalid URL {url:?}: {e}"));

        let pool = PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(Duration::from_secs(30))
            .connect_with(options)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "TestDb::connect_with: cannot connect to {host}:{port}/{database} ({e}). \
                     Is the compose stack up? Run `docker compose -f tests/docker/compose.yaml up -d --wait`, \
                     or set DATABASE_URL."
                )
            });

        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let schema = format!("test_{}_{}", std::process::id(), n);
        // Machine-generated identifier, safe by construction; assert it anyway so a
        // future refactor can't turn this into an injection sink.
        assert!(
            schema
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "unsafe schema name"
        );
        let sql = format!("CREATE SCHEMA {}", schema);
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("TestDb::connect_with: cannot create schema {schema}: {e}"));

        PASSWORD_ONCE.call_once(|| {
            // Required on Rust 2024: env mutation is unsafe (process-wide).
            unsafe { std::env::set_var(TEST_PASSWORD_ENV, &password) };
        });

        let db = Self {
            pool,
            schema,
            host,
            port,
            user,
            password,
            database,
        };
        db.setup()
            .await
            .unwrap_or_else(|e| panic!("TestDb::connect_with: hostile fixture setup failed: {e}"));
        db
    }

    /// Schema-qualified hostile table name.
    pub fn table(&self) -> String {
        format!("{}.hostile", self.schema)
    }

    /// Build the hostile fixture: every decode edge in one small deterministic table.
    /// `updated_at` is spread over 2024-01-01..08 so window queries can slice it.
    async fn setup(&self) -> Result<(), sqlx::Error> {
        let s = &self.schema;
        let sql = format!("CREATE TYPE {s}.mood AS ENUM ('sad', 'ok', 'ecstatic')");
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&self.pool)
            .await?;
        let sql = format!(
            "CREATE TABLE {s}.hostile (
                id bigserial primary key,
                name text,
                nick varchar(20),
                code bpchar(4),
                amount numeric(12,2),
                precise numeric(30,15),
                count integer,
                big bigint,
                ratio double precision,
                f real,
                flag boolean,
                tags text[],
                meta jsonb,
                uid uuid,
                day date,
                ts timestamptz,
                naive timestamp without time zone,
                feeling {s}.mood,
                updated_at timestamptz not null
            )"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&self.pool)
            .await?;
        // One row per edge; row 8 is the mostly-NULL row. Deterministic ids 1..8.
        let sql = format!(
            "INSERT INTO {s}.hostile
             (name, nick, code, amount, precise, count, big, ratio, f, flag, tags,
              meta, uid, day, ts, naive, feeling, updated_at) VALUES
             ('Zürich', 'MÜNCHEN', 'ab', 123.45, 3.141592653589793, 2147483647,
              9223372036854775807, 'NaN', 'Infinity', true, '{{\"a\",NULL,\"\"}}',
              '{{\"a\":1,\"b\":[true,null]}}', '123e4567-e89b-12d3-a456-426614174000',
              '2024-02-29', '2024-03-01 12:00:00+02', '2024-03-01 12:00:00',
              'ecstatic', '2024-01-01'),
             ('', '', '', -7.50, 0, -2147483648, -9223372036854775808,
              '-Infinity', 1.5, false, '{{}}', '{{}}', '123e4567-e89b-12d3-a456-426614174001',
              NULL, NULL, NULL, 'sad', '2024-01-02'),
             ('plain', 'plain', 'wxyz', 0.00, -0.5, 0, 0, 0.0, 0.0, NULL,
              '{{x,y,z}}', '[]', '123e4567-e89b-12d3-a456-426614174002',
              '1970-01-01', '1970-01-01 00:00:00+00', '1970-01-01 00:00:00',
              'ok', '2024-01-03'),
             ('MiXeD', 'MiXeD', 'q', 99999999.99, 100.25, 42, 42, 2.5, -3.25, true,
              NULL, NULL, '123e4567-e89b-12d3-a456-426614174003',
              '1999-12-31', '1999-12-31 23:59:59-05', '1999-12-31 23:59:59',
              'ok', '2024-01-04'),
             ('nulls', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
              NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2024-01-05'),
             ('six', 'six', 'six6', 1.00, 1.5, 6, 6, 6.0, 6.0, true,
              '{{s}}', '{{\"n\":6}}', '123e4567-e89b-12d3-a456-426614174005',
              '2024-06-15', '2024-06-15 06:30:00+00', '2024-06-15 06:30:00',
              'sad', '2024-01-06'),
             ('seven', 'seven', 'svn7', 42.42, -2.75, 7, 7, 7.0, 7.0, false,
              '{{a,b}}', '{{\"n\":7}}', '123e4567-e89b-12d3-a456-426614174006',
              '2024-07-07', '2024-07-07 07:07:07+00', '2024-07-07 07:07:07',
              'ecstatic', '2024-01-07'),
             ('eight', 'eight', 'eght', NULL, NULL, 8, 8, 8.0, 8.0, NULL,
              NULL, NULL,               NULL, NULL, NULL, NULL, NULL, '2024-01-08')"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&self.pool)
            .await?;
        let sql = format!("ANALYZE {s}.hostile");
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        // Best-effort: a failed drop must never fail a test. Needs its own runtime
        // because Drop has no async context.
        let pool = self.pool.clone();
        let schema = self.schema.clone();
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            if let Ok(rt) = rt {
                rt.block_on(async {
                    let sql = format!("DROP SCHEMA {schema} CASCADE");
                    let _ = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                        .execute(&pool)
                        .await;
                });
            }
        })
        .join();
    }
}

/// Minimal `postgres://user:pass@host:port/db` parse (no new deps for one split).
fn parse_url(url: &str) -> Option<(String, u16, String, String, String)> {
    let rest = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))?;
    let (auth, rest) = rest.split_once('@')?;
    let (user, password) = auth.split_once(':')?;
    let (hostport, database) = rest.split_once('/')?;
    let (host, port) = match hostport.split_once(':') {
        Some((h, p)) => (h, p.parse().ok()?),
        None => (hostport, 5432),
    };
    Some((
        host.to_string(),
        port,
        user.to_string(),
        password.to_string(),
        database.to_string(),
    ))
}

/// Fetch a Decimal128 column as unscaled i128s (None for NULL) for exact assertions.
pub fn decimal_col(batch: &arrow::record_batch::RecordBatch, name: &str) -> Vec<Option<i128>> {
    use arrow::array::Array;
    let idx = batch.schema().index_of(name).expect("column exists");
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .expect("decimal column");
    (0..arr.len())
        .map(|i| arr.is_valid(i).then(|| arr.value(i)))
        .collect()
}

/// Fetch an Int64 column as values for set comparisons.
pub fn int64_col(batch: &arrow::record_batch::RecordBatch, name: &str) -> Vec<i64> {
    use arrow::array::Array;
    let idx = batch.schema().index_of(name).expect("column exists");
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("int64 column");
    (0..arr.len())
        .filter_map(|i| arr.is_valid(i).then(|| arr.value(i)))
        .collect()
}
