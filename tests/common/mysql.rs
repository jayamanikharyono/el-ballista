#![allow(dead_code, clippy::all)]
//! MySQL test harness — mirrors `tests/common/postgres.rs` for Postgres: a real database is always
//! obtained, tests never skip.
//!
//! The live MySQL comes from the compose stack (`tests/docker/compose.yaml`) — the single source of truth for
//! a test database, the same one CI uses. `MySqlTestDb::connect()` reads `MYSQL_URL` (or
//! `DATABASE_URL_MYSQL`) and, when unset, defaults to the compose endpoint
//! `mysql://root:password@127.0.0.1:3306/test`. It never self-provisions and never skips: if the
//! server is unreachable it panics with a message telling you to start the stack (masking a broken
//! harness as "skipped" is exactly what produced false-green CI in the past).
//!
//! ```bash
//! docker compose -f tests/docker/compose.yaml up -d --wait
//! cargo test --test mysql -- --test-threads=1
//! docker compose -f tests/docker/compose.yaml down -v
//! ```
//!
//! Design: database isolation inside the one server — each `MySqlTestDb` gets `test_<pid>_<n>`,
//! builds the hostile fixture inside it, and drops it on `Drop`. Tests can run in parallel.

use sqlx::MySqlPool;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Default MySQL endpoint when `MYSQL_URL` / `DATABASE_URL_MYSQL` is unset: the compose stack's
/// `mysql` service (see `tests/docker/compose.yaml`), matching the CI environment.
const DEFAULT_URL: &str = "mysql://root:password@127.0.0.1:3306/test";

pub struct MySqlTestDb {
    pub pool: MySqlPool,
    /// Per-test database name (e.g. `test_12345_0`), also used as schema prefix.
    pub database: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
}

impl MySqlTestDb {
    /// Connect, provisioning a per-test database inside the compose MySQL. Never skips: either
    /// returns a live `MySqlTestDb`, or panics with a message explaining exactly what failed
    /// (server unreachable — is the compose stack up? — connection refused, or fixture failure).
    pub async fn connect() -> Self {
        let url = std::env::var("MYSQL_URL")
            .ok()
            .or_else(|| std::env::var("DATABASE_URL_MYSQL").ok())
            .filter(|u| !u.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_URL.to_string());
        Self::connect_with(&url).await
    }

    /// Same, with an explicit admin URL (the server's `test` database).
    async fn connect_with(base_url: &str) -> Self {
        let (host, port, user, password, _db) = parse_url(base_url).unwrap_or_else(|| {
            panic!("MySqlTestDb: cannot parse {base_url:?} as mysql://user:pass@host:port/db")
        });

        let admin_options = MySqlConnectOptions::from_str(base_url)
            .unwrap_or_else(|e| panic!("MySqlTestDb: invalid URL {base_url:?}: {e}"));
        let admin_pool = MySqlPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(30))
            .connect_with(admin_options)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "MySqlTestDb: cannot connect to {host}:{port} ({e}). Is the compose stack up? \
                     Run `docker compose -f tests/docker/compose.yaml up -d --wait`, or set MYSQL_URL."
                )
            });

        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let db_name = format!("test_{}_{}", std::process::id(), n);
        // Machine-generated identifier, safe by construction; assert it anyway so a future
        // refactor can't turn this into an injection sink.
        assert!(
            db_name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "unsafe database name"
        );
        sqlx::query(sqlx::AssertSqlSafe(
            format!("CREATE DATABASE `{db_name}`").as_str(),
        ))
        .execute(&admin_pool)
        .await
        .unwrap_or_else(|e| panic!("MySqlTestDb: cannot create database {db_name}: {e}"));

        // Reconnect to the per-test database for fixture setup. Password may be empty, so build
        // the URL accordingly.
        let test_url = if password.is_empty() {
            format!("mysql://{user}@{host}:{port}/{db_name}")
        } else {
            format!("mysql://{user}:{password}@{host}:{port}/{db_name}")
        };
        let test_options = MySqlConnectOptions::from_str(&test_url)
            .unwrap_or_else(|e| panic!("MySqlTestDb: invalid per-test URL {test_url:?}: {e}"));
        let test_pool = MySqlPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(30))
            .connect_with(test_options)
            .await
            .unwrap_or_else(|e| panic!("MySqlTestDb: cannot connect to per-test db {db_name}: {e}"));

        let db = Self {
            pool: test_pool,
            database: db_name,
            host,
            port,
            user,
            password,
        };
        db.setup()
            .await
            .unwrap_or_else(|e| panic!("MySqlTestDb: hostile fixture setup failed: {e}"));
        db
    }

    pub fn table(&self) -> String {
        format!("{}.hostile", self.database)
    }

    /// Hostile fixture: every MySQL decode edge in one small deterministic table.
    async fn setup(&self) -> Result<(), sqlx::Error> {
        // MySQL hostile table — covers tinyint/smallint/int/bigint/float/double/decimal,
        // bit/bool, date/datetime/timestamp, char/varchar/text/enum/set/json, binary/blob,
        // nulls, empty strings, 0/empty, and edge timestamps.
        sqlx::query(sqlx::AssertSqlSafe(
            r#"
            CREATE TABLE hostile (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                nick VARCHAR(20),
                code CHAR(4),
                amount DECIMAL(12,2),
                precise DECIMAL(30,15),
                tiny TINYINT,
                small SMALLINT,
                `count` INT,
                big BIGINT,
                ratio DOUBLE,
                f FLOAT,
                flag BIT(1),
                is_bool BOOLEAN,
                day DATE,
                dt DATETIME(6),
                ts TIMESTAMP(6),
                feeling ENUM('sad','ok','ecstatic'),
                colors SET('red','green','blue'),
                meta JSON,
                uid CHAR(36),
                payload BLOB,
                updated_at TIMESTAMP(6) NOT NULL
            )
            "#,
        ))
        .execute(&self.pool)
        .await?;

        // Deterministic 8 rows — mirrors Postgres hostile but with MySQL types.
        // Row 5 is the mostly-NULL row.
        sqlx::query(sqlx::AssertSqlSafe(
            r#"
            INSERT INTO hostile
            (name, nick, code, amount, precise, tiny, small, `count`, big, ratio, f, flag, is_bool, day, dt, ts, feeling, colors, meta, uid, payload, updated_at) VALUES
            ('Zürich', 'MÜNCHEN', 'ab', 123.45, 3.141592653589793, 127, 32767, 2147483647, 9223372036854775807, 1.5, 1.5, b'1', true, '2024-02-29', '2024-03-01 12:00:00.123456', '2024-03-01 12:00:00.123456', 'ecstatic', 'red,green', '{"a":1,"b":[true,null]}', '123e4567-e89b-12d3-a456-426614174000', X'000102FF', '2024-01-01 00:00:00'),
            ('', '', '', -7.50, 0, -128, -32768, -2147483648, -9223372036854775808, -1.5, -1.5, b'0', false, NULL, NULL, NULL, 'sad', '', '[]', '123e4567-e89b-12d3-a456-426614174001', NULL, '2024-01-02 00:00:00'),
            ('plain', 'plain', 'wxyz', 0.00, -0.5, 0, 0, 0, 0, 0.0, 0.0, NULL, NULL, '1970-01-01', '1970-01-01 00:00:00.000000', '1970-01-01 00:00:01.000000', 'ok', 'blue', '[]', '123e4567-e89b-12d3-a456-426614174002', X'', '2024-01-03 00:00:00'),
            ('MiXeD', 'MiXeD', 'q', 99999999.99, 100.25, 42, 42, 42, 42, 2.5, -3.25, b'1', true, '1999-12-31', '1999-12-31 23:59:59.999999', '1999-12-31 23:59:59.999999', 'ok', 'red', NULL, '123e4567-e89b-12d3-a456-426614174003', X'FF', '2024-01-04 00:00:00'),
            ('nulls', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2024-01-05 00:00:00'),
            ('six', 'six', 'six6', 1.00, 1.5, 6, 6, 6, 6, 6.0, 6.0, b'1', true, '2024-06-15', '2024-06-15 06:30:00.000000', '2024-06-15 06:30:00.000000', 'sad', 'green', '{"n":6}', '123e4567-e89b-12d3-a456-426614174005', X'AA', '2024-01-06 00:00:00'),
            ('seven', 'seven', 'svn7', 42.42, -2.75, 7, 7, 7, 7, 7.0, 7.0, b'0', false, '2024-07-07', '2024-07-07 07:07:07.000000', '2024-07-07 07:07:07.000000', 'ecstatic', 'red,blue', '{"n":7}', '123e4567-e89b-12d3-a456-426614174006', X'BB', '2024-01-07 00:00:00'),
            ('eight', 'eight', 'eght', NULL, NULL, 8, 8, 8, 8, 8.0, 8.0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2024-01-08 00:00:00')
            "#,
        ))
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

impl Drop for MySqlTestDb {
    fn drop(&mut self) {
        let pool = self.pool.clone();
        let db_name = self.database.clone();
        // Best-effort cleanup — needs its own runtime because Drop has no async context.
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            if let Ok(rt) = rt {
                rt.block_on(async {
                    // MySQL allows dropping the currently-selected database; best-effort either
                    // way (the test database is ephemeral, torn down with the compose stack).
                    let _ = sqlx::query(sqlx::AssertSqlSafe(
                        format!("DROP DATABASE IF EXISTS `{db_name}`").as_str(),
                    ))
                    .execute(&pool)
                    .await;
                });
            }
        })
        .join();
    }
}

fn parse_url(url: &str) -> Option<(String, u16, String, String, String)> {
    let rest = url
        .strip_prefix("mysql://")
        .or_else(|| url.strip_prefix("mysql+pymysql://"))?;
    let (auth, rest) = rest.split_once('@')?;
    let (user, password) = auth.split_once(':').unwrap_or((auth, ""));
    let (hostport, database) = rest.split_once('/')?;
    let database = database.split('?').next().unwrap_or(database);
    let (host, port) = match hostport.split_once(':') {
        Some((h, p)) => (h, p.parse().ok()?),
        None => (hostport, 3306),
    };
    Some((
        host.to_string(),
        port,
        user.to_string(),
        password.to_string(),
        database.to_string(),
    ))
}
