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
//! `test_<pid>_<n>`, loads the hostile fixture (`tests/data/hostile.sql`) inside it, and drops
//! the schema on `Drop` (over a fresh connection on a private runtime, bounded by a 10 s
//! timeout, failures logged) or via the explicit `TestDb::cleanup().await`. Tests can run in
//! parallel.

use el_ballista::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, JobConfig, ParallelScanConfig,
    PushdownConfig, PushdownPolicy, SourceConfig,
};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPoolOptions};
use sqlx::{Connection, PgPool};
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
    /// Stored so teardown can open a fresh connection independent of the test runtime.
    connect_options: PgConnectOptions,
    /// Set by [`TestDb::cleanup`]; makes `Drop` a no-op.
    cleaned: bool,
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
            .connect_with(options.clone())
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
            connect_options: options,
            cleaned: false,
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

    /// Schema-qualified `hostile_infinity` table (±infinity timestamps/dates, which must fail
    /// extraction with a typed error; kept out of `hostile` on purpose).
    pub fn infinity_table(&self) -> String {
        format!("{}.hostile_infinity", self.schema)
    }

    /// Build the hostile fixture from `tests/data/hostile.sql` (the single source of truth),
    /// substituting this test's schema for every `__SCHEMA__` token. 13 rows, ids 1..13:
    /// rows 1..8 have `updated_at` 2024-01-01..08 (one per day), rows 9..13 tie on
    /// 2024-01-09 00:00:00+00. See the file header for the edges it covers.
    async fn setup(&self) -> Result<(), sqlx::Error> {
        let sql = HOSTILE_SQL.replace(SCHEMA_TOKEN, &self.schema);
        // The schema name is harness-generated and asserted identifier-safe in
        // `connect_with`; the rest of the script is a static, audited file.
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

/// The hostile fixture script (see its header). Loaded verbatim, schema token substituted.
const HOSTILE_SQL: &str = include_str!("../data/hostile.sql");
/// Placeholder in [`HOSTILE_SQL`] replaced by the per-test schema name.
const SCHEMA_TOKEN: &str = "__SCHEMA__";

/// Rows in `hostile` (`tests/data/hostile.sql`): ids 1..=13.
pub const HOSTILE_ROWS: usize = 13;
/// `updated_at` shared by the 5-row tie (ids 9..=13) in `hostile`.
pub const HOSTILE_TIE_TS: &str = "2024-01-09 00:00:00+00";
/// Ids of the 5-row `updated_at` tie.
pub const HOSTILE_TIE_IDS: [i64; 5] = [9, 10, 11, 12, 13];

impl TestDb {
    /// Drop the per-test schema on the test's own runtime and close the pool. Prefer this over
    /// relying on `Drop` when a test wants the teardown awaited (and its failure surfaced).
    /// `Drop` becomes a no-op afterwards.
    pub async fn cleanup(mut self) {
        self.cleaned = true;
        let sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema);
        let res = tokio::time::timeout(
            TEARDOWN_TIMEOUT,
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).execute(&self.pool),
        )
        .await;
        match res {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => eprintln!("TestDb::cleanup: DROP SCHEMA {} failed: {e}", self.schema),
            Err(_) => eprintln!(
                "TestDb::cleanup: DROP SCHEMA {} timed out after {TEARDOWN_TIMEOUT:?}",
                self.schema
            ),
        }
        self.pool.close().await;
    }
}

/// Upper bound on teardown: a stuck `DROP SCHEMA` must not hang the suite.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// `DROP SCHEMA … CASCADE` over a FRESH connection on a private current-thread runtime.
///
/// Why not the test's pool: `Drop` runs on the test runtime's thread, which is blocked in
/// `join()` below, so that runtime can never drive the pool's connection I/O — reusing it is
/// what used to deadlock (or wait out the 30 s acquire timeout). A fresh `PgConnection` is
/// owned entirely by the private runtime, so teardown cannot depend on the blocked one.
fn drop_schema_blocking(options: PgConnectOptions, schema: String) {
    let joined = std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("TestDb::drop: cannot build teardown runtime for {schema}: {e}");
                return;
            }
        };
        rt.block_on(async {
            let work = async {
                let mut conn = PgConnection::connect_with(&options).await?;
                let sql = format!("DROP SCHEMA IF EXISTS {schema} CASCADE");
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .execute(&mut conn)
                    .await?;
                conn.close().await
            };
            match tokio::time::timeout(TEARDOWN_TIMEOUT, work).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => eprintln!("TestDb::drop: DROP SCHEMA {schema} failed: {e}"),
                Err(_) => eprintln!(
                    "TestDb::drop: DROP SCHEMA {schema} timed out after {TEARDOWN_TIMEOUT:?}"
                ),
            }
        });
    })
    .join();
    if joined.is_err() {
        eprintln!("TestDb::drop: teardown thread panicked");
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        // A failed drop must never fail (or hang) a test, but it is logged, not swallowed.
        if self.cleaned {
            return;
        }
        drop_schema_blocking(self.connect_options.clone(), self.schema.clone());
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

/// `batch` reordered by its ``col`` column ascending. Extraction carries no `ORDER BY`, so row
/// order is unspecified; tests that assert on row positions must sort first.
pub fn sorted_by(
    batch: &arrow::record_batch::RecordBatch,
    col: &str,
) -> arrow::record_batch::RecordBatch {
    let idx = batch.schema().index_of(col).expect("sort column exists");
    let indices =
        arrow::compute::sort_to_indices(batch.column(idx), None, None).expect("sort_to_indices");
    arrow::compute::take_record_batch(batch, &indices).expect("take_record_batch")
}

impl TestDb {
    /// A job extracting `table` (this database's private schema) through the public connector:
    /// every row, `columns` (`None` = all), the cursor path, the default batch size, unsplit,
    /// pushdown `never` (filters, if a test adds any, evaluated in Arrow). Tests adjust
    /// `execution` / `parallel_scan` / `filters` / `pushdown` for the path under test.
    pub fn extraction_job(&self, table: &str, columns: Option<&[&str]>) -> JobConfig {
        JobConfig {
            job_id: format!("x-{}-{table}", self.schema).parse().unwrap(),
            table: table.to_string(),
            columns: columns.map(|c| c.iter().map(|s| s.to_string()).collect()),
            filters: Vec::new(),
            source: SourceConfig {
                host: self.host.clone(),
                port: self.port,
                user: self.user.clone(),
                password_env: TEST_PASSWORD_ENV.to_string(),
                database: self.database.clone(),
                pool_max: 4,
                statement_timeout_ms: 300_000,
                application_name: "relex-test".to_string(),
                schema: self.schema.clone(),
            },
            checkpoint: CheckpointConfig {
                dir: std::env::temp_dir()
                    .join(format!("relex_x_{}", self.schema))
                    .to_string_lossy()
                    .to_string(),
                ..CheckpointConfig::default()
            },
            pushdown: PushdownConfig {
                policy: PushdownPolicy::Never,
                ..PushdownConfig::default()
            },
            parallel_scan: ParallelScanConfig::default(),
            execution: ExecutionConfig {
                use_copy: false,
                ..ExecutionConfig::default()
            },
            distributed: DistributedConfig::default(),
        }
    }
}

/// Every batch of `config`'s standalone extraction, in delivery order, batch boundaries kept
/// (the public `stream()` terminal, read to the end).
pub async fn extract_batches(
    config: &JobConfig,
) -> Result<Vec<arrow::record_batch::RecordBatch>, el_ballista::errors::AppError> {
    use futures::TryStreamExt;
    let connector =
        el_ballista::connector::postgres::PostgresConnector::from_config(config.clone())?;
    let stream = connector.extract().standalone().stream().await?;
    Ok(stream.try_collect().await?)
}

/// [`extract_batches`] concatenated into one batch that carries the extraction's schema, also
/// for zero rows.
pub async fn extract_one(
    config: &JobConfig,
) -> Result<arrow::record_batch::RecordBatch, el_ballista::errors::AppError> {
    use futures::TryStreamExt;
    let connector =
        el_ballista::connector::postgres::PostgresConnector::from_config(config.clone())?;
    let stream = connector.extract().standalone().stream().await?;
    let schema = stream.schema();
    let batches: Vec<_> = stream.try_collect().await?;
    Ok(arrow::compute::concat_batches(&schema, &batches)
        .map_err(datafusion::error::DataFusionError::from)?)
}

/// Assert which path `config`'s scan takes — `"copy"` or `"cursor"` — as the physical plan of
/// the same provider shows it (`scan=…`). Non-vacuity for tests that compare the two paths:
/// a COPY request whose shape does not allow COPY silently scans with the cursor.
pub async fn assert_scan_path(config: &JobConfig, expected: &str) {
    use datafusion::physical_plan::displayable;
    let provider = el_ballista::connector::postgres::PostgresTableProvider::from_config(config)
        .await
        .expect("provider");
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_table("scan_path_probe", std::sync::Arc::new(provider))
        .expect("register");
    let columns = config.columns.as_ref().map_or_else(
        || "*".to_string(),
        |c| {
            c.iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", ")
        },
    );
    let plan = ctx
        .sql(&format!("SELECT {columns} FROM scan_path_probe"))
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        text.contains(&format!("scan={expected}")),
        "expected the {expected} path:\n{text}"
    );
}
