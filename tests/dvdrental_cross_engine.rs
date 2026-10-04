//! Cross-engine correctness on the shared dvdrental dataset.
//!
//! Single source of truth: the `.dat` files under `tests/data/dvdrental`. The compose stack
//! (`tests/docker/compose.yaml`) seeds the SAME rows into both engines — Postgres via `pg_restore`, MySQL via
//! `tests/data/dvdrental_mysql.sql` (`LOAD DATA` over the identical files). These tests prove the
//! data really is identical, checked three ways:
//!   1. direct SQL `COUNT(*)` on each engine vs the known dvdrental row counts (reference oracle),
//!   2. the same counts via the Postgres and MySQL connectors (extraction matches the reference),
//!   3. the actual values (category names) read through each connector are byte-identical.
//!
//! The Postgres side is extracted through the public connector (`PostgresConnector` →
//! `standalone().stream()`, via `tests/common`'s `extract_one`), cursor path, one unsplit scan.
//!
//! Requires the compose stack up (`docker compose -f tests/docker/compose.yaml up -d --wait`). Never skips: a
//! failure to connect is a hard failure, with a message pointing at the stack.

mod common;

use arrow::array::{Array, BinaryArray, StringArray};
use el_ballista::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, JobConfig, ParallelScanConfig,
    PushdownConfig, SourceConfig,
};
use el_ballista::connector::mysql::MysqlExtractor;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;
use sqlx::{MySqlPool, PgPool, Row};

const PG_URL_DEFAULT: &str = "postgres://postgres:postgres@127.0.0.1:5432/test";
const MY_URL_DEFAULT: &str = "mysql://root:password@127.0.0.1:3306/test";

/// The 15 dvdrental tables with their canonical row counts — an oracle independent of both the
/// database and the Arrow decode path.
const TABLES: &[(&str, i64)] = &[
    ("actor", 200),
    ("address", 603),
    ("category", 16),
    ("city", 600),
    ("country", 109),
    ("customer", 599),
    ("film", 1000),
    ("film_actor", 5462),
    ("film_category", 1000),
    ("inventory", 4581),
    ("language", 6),
    ("payment", 14596),
    ("rental", 16044),
    ("staff", 2),
    ("store", 2),
];

/// Clean tables (no PG arrays/tsvector/bytea quirks) used for connector-extraction parity.
const CONNECTOR_TABLES: &[(&str, i64)] = &[
    ("actor", 200),
    ("category", 16),
    ("country", 109),
    ("language", 6),
    ("staff", 2),
    ("store", 2),
];

fn pg_url() -> String {
    std::env::var("DATABASE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
        .unwrap_or_else(|| PG_URL_DEFAULT.to_string())
}

fn my_url() -> String {
    std::env::var("MYSQL_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL_MYSQL").ok())
        .filter(|u| !u.trim().is_empty())
        .unwrap_or_else(|| MY_URL_DEFAULT.to_string())
}

/// (host, port, user, password, database) from a `scheme://user:pass@host:port/db` URL.
fn parse(url: &str, default_port: u16) -> (String, u16, String, String, String) {
    let rest = url
        .split_once("://")
        .map(|(_, r)| r)
        .unwrap_or_else(|| panic!("bad url {url:?}"));
    let (auth, rest) = rest
        .split_once('@')
        .unwrap_or_else(|| panic!("bad url {url:?}"));
    let (user, password) = auth.split_once(':').unwrap_or((auth, ""));
    let (hostport, database) = rest
        .split_once('/')
        .unwrap_or_else(|| panic!("bad url {url:?}"));
    let database = database.split('?').next().unwrap_or(database);
    let (host, port) = match hostport.split_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(default_port)),
        None => (hostport.to_string(), default_port),
    };
    (
        host,
        port,
        user.to_string(),
        password.to_string(),
        database.to_string(),
    )
}

async fn pg_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .connect(&pg_url())
        .await
        .unwrap_or_else(|e| {
            panic!(
                "cannot connect to Postgres ({e}). Is the compose stack up? \
                 `docker compose -f tests/docker/compose.yaml up -d --wait`, or set DATABASE_URL."
            )
        })
}

async fn my_pool() -> MySqlPool {
    MySqlPoolOptions::new()
        .max_connections(4)
        .connect(&my_url())
        .await
        .unwrap_or_else(|e| {
            panic!(
                "cannot connect to MySQL ({e}). Is the compose stack up? \
                 `docker compose -f tests/docker/compose.yaml up -d --wait`, or set MYSQL_URL."
            )
        })
}

/// Name of the env var every Postgres `JobConfig` in this file reads its password from.
const CROSS_PASSWORD_ENV: &str = "CROSS_ENGINE_PG_PASSWORD";
static CROSS_PASSWORD_ONCE: std::sync::Once = std::sync::Once::new();

/// Set [`CROSS_PASSWORD_ENV`] exactly once per process and never remove it (the connector reads
/// the password from `source.password_env`, never from the config). Every test in this file
/// calls this as its FIRST statement, so all of them block in `call_once` until the single
/// write is done — no env read in this binary can overlap it.
fn cross_password_env() -> &'static str {
    CROSS_PASSWORD_ONCE.call_once(|| {
        let (_, _, _, pw, _) = parse(&pg_url(), 5432);
        // SAFETY: runs once, inside `call_once`, and the value is never changed or removed
        // afterwards; every reader of this variable goes through this function first.
        unsafe { std::env::set_var(CROSS_PASSWORD_ENV, pw) };
    });
    CROSS_PASSWORD_ENV
}

/// `public.<table>` extracted through the Postgres connector (`columns`: `None` = all; cursor
/// path, default batch size and byte cap, one unsplit scan, no filters), as one batch.
async fn pg_extract(table: &str, columns: Option<&[&str]>) -> arrow::record_batch::RecordBatch {
    let env_var = cross_password_env();
    let (h, p, u, _pw, db) = parse(&pg_url(), 5432);
    let config = JobConfig {
        job_id: format!("cross-engine-{table}").parse().unwrap(),
        table: table.to_string(),
        columns: columns.map(|c| c.iter().map(|s| s.to_string()).collect()),
        filters: Vec::new(),
        source: SourceConfig {
            host: h,
            port: p,
            user: u,
            password_env: env_var.to_string(),
            database: db,
            pool_max: 4,
            statement_timeout_ms: 30_000,
            application_name: "el-ballista-cross-engine".to_string(),
            schema: "public".to_string(),
        },
        checkpoint: CheckpointConfig::default(),
        pushdown: PushdownConfig::default(),
        parallel_scan: ParallelScanConfig::default(),
        execution: ExecutionConfig {
            use_copy: false,
            ..ExecutionConfig::default()
        },
        distributed: DistributedConfig::default(),
    };
    common::extract_one(&config)
        .await
        .unwrap_or_else(|e| panic!("PG extract {table}: {e}"))
}

async fn my_extractor() -> (MysqlExtractor, String) {
    let (h, p, u, pw, db) = parse(&my_url(), 3306);
    let ex = MysqlExtractor::connect(&h, p, &u, &pw, &db, 4)
        .await
        .unwrap_or_else(|e| panic!("MysqlExtractor::connect: {e}"));
    (ex, db)
}

/// Non-null values of a `Utf8` column as a sorted Vec (both connectors surface text as `Utf8`).
fn sorted_string_col(batch: &arrow::record_batch::RecordBatch, name: &str) -> Vec<String> {
    let idx = batch.schema().index_of(name).expect("column exists");
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 column");
    let mut v: Vec<String> = (0..arr.len())
        .filter(|&i| arr.is_valid(i))
        .map(|i| arr.value(i).to_string())
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn direct_row_counts_match_reference_on_both_engines() {
    cross_password_env();
    let pg = pg_pool().await;
    let my = my_pool().await;
    for (t, expected) in TABLES {
        // Dynamic SQL audit: `t` is a compile-time constant from `TABLES` (no user input), so the
        // interpolated identifier is safe; wrapped in `AssertSqlSafe` per the repo convention.
        let pg_sql = format!("SELECT COUNT(*) FROM public.{t}");
        let pg_count: i64 = sqlx::query(sqlx::AssertSqlSafe(pg_sql))
            .fetch_one(&pg)
            .await
            .unwrap_or_else(|e| panic!("PG count {t}: {e}"))
            .try_get(0)
            .unwrap_or_else(|e| panic!("PG count decode {t}: {e}"));
        let my_sql = format!("SELECT COUNT(*) FROM `{t}`");
        let my_count: i64 = sqlx::query(sqlx::AssertSqlSafe(my_sql))
            .fetch_one(&my)
            .await
            .unwrap_or_else(|e| panic!("MySQL count {t}: {e}"))
            .try_get(0)
            .unwrap_or_else(|e| panic!("MySQL count decode {t}: {e}"));
        assert_eq!(pg_count, *expected, "Postgres row count for {t}");
        assert_eq!(my_count, *expected, "MySQL row count for {t}");
    }
}

#[tokio::test]
async fn connector_extraction_row_counts_match_reference() {
    cross_password_env();
    let (myx, db) = my_extractor().await;
    for (t, expected) in CONNECTOR_TABLES {
        let pg_batch = pg_extract(t, None).await;
        let my_batch = myx
            .extract_full_table(&format!("{db}.{t}"), None)
            .await
            .unwrap_or_else(|e| panic!("MySQL extract {t}: {e}"));
        assert_eq!(
            pg_batch.num_rows() as i64,
            *expected,
            "PG connector rows for {t}"
        );
        assert_eq!(
            my_batch.num_rows() as i64,
            *expected,
            "MySQL connector rows for {t}"
        );
    }
}

#[tokio::test]
async fn connectors_read_identical_category_names() {
    cross_password_env();
    let (myx, db) = my_extractor().await;

    let pg_batch = pg_extract("category", Some(&["name"])).await;
    let my_batch = myx
        .extract_full_table(&format!("{db}.category"), Some(vec!["name"]))
        .await
        .expect("MySQL extract category");

    let pg_names = sorted_string_col(&pg_batch, "name");
    let my_names = sorted_string_col(&my_batch, "name");

    // Cross-engine: the two connectors read the same values from the same source dataset.
    assert_eq!(
        pg_names, my_names,
        "category names must be identical across engines"
    );

    // Absolute oracle: the known dvdrental category set.
    let expected = vec![
        "Action",
        "Animation",
        "Children",
        "Classics",
        "Comedy",
        "Documentary",
        "Drama",
        "Family",
        "Foreign",
        "Games",
        "Horror",
        "Music",
        "New",
        "Sci-Fi",
        "Sports",
        "Travel",
    ];
    assert_eq!(
        pg_names, expected,
        "category names match the known dvdrental set"
    );
}

#[tokio::test]
async fn connectors_read_identical_staff_picture() {
    // staff.picture is the only bytea/blob column in the dataset. Postgres COPY stores it as
    // `\\x<hex>` text in the .dat; pg_restore decodes to bytes while MySQL LOAD DATA kept the
    // 18 ASCII chars — the seed's UNHEX post-step (see gen_mysql_seed.py) restores byte parity.
    // Both connectors surface it as Arrow Binary; differential oracle (PG vs MySQL) plus the
    // absolute bytes from tests/data/dvdrental/3079.dat (`\x89504e470d0a5a0a`, staff 1; NULL staff 2).
    cross_password_env();
    let (myx, db) = my_extractor().await;

    let pg_batch = pg_extract("staff", Some(&["picture"])).await;
    let my_batch = myx
        .extract_full_table(&format!("{db}.staff"), Some(vec!["picture"]))
        .await
        .expect("MySQL extract staff.picture");

    fn picture_bytes(batch: &arrow::record_batch::RecordBatch) -> Vec<Option<Vec<u8>>> {
        let idx = batch.schema().index_of("picture").expect("picture column");
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Binary picture column");
        let mut v: Vec<Option<Vec<u8>>> = (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_vec())
                }
            })
            .collect();
        v.sort();
        v
    }

    let pg_pic = picture_bytes(&pg_batch);
    let my_pic = picture_bytes(&my_batch);
    assert_eq!(
        pg_pic, my_pic,
        "staff.picture bytes must be identical across engines"
    );

    let expected = vec![
        None,
        Some(vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x5A, 0x0A]),
    ];
    assert_eq!(
        pg_pic, expected,
        "staff.picture matches the .dat payload (8 bytes + NULL)"
    );
}
