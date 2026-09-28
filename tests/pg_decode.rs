//! Decode and partition-coverage correctness against a live Postgres: ±infinity and extreme
//! timestamps, numeric precision, json/uuid text, keyset coverage, strict inputs, and scan
//! cancellation.
//!
//! Oracles (AGENTS.md §7):
//! - **reference**: the same value rendered/computed by Postgres itself
//!   (`::text`, `extract(epoch …)`, `count(*)`);
//! - **differential**: cursor path vs binary COPY path;
//! - **trivial**: hand-written expected values / typed error kinds.
//!
//! Self-contained (like `regressions.rs`): each test creates its own schema and drops it with
//! an explicit async cleanup instead of relying on `Drop`.
//! Run: `cargo test --test pg_decode -- --test-threads=1` (needs `tests/docker/compose.yaml`).

use arrow::array::TimestampMicrosecondArray;
use arrow::array::{Array, Date32Array, Decimal128Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use rust_ballista_extraction_layer::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, JobConfig, ParallelScanConfig,
    PushdownConfig, SourceConfig,
};
use rust_ballista_extraction_layer::connector::errors::ExtractorError;
use rust_ballista_extraction_layer::connector::postgres::extractor::PostgresExtractor;
use rust_ballista_extraction_layer::connector::postgres::parallel::compute_keyset_partitions;
use rust_ballista_extraction_layer::connector::postgres::register_table;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicUsize, Ordering};

type R = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const PW_ENV: &str = "PG_DECODE_TEST_PASSWORD";
static SEQ: AtomicUsize = AtomicUsize::new(0);

struct Pg {
    pool: sqlx::PgPool,
    schema: String,
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

fn pg_url() -> String {
    std::env::var("DATABASE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
        .unwrap_or_else(|| "postgres://postgres:postgres@127.0.0.1:5432/test".to_string())
}

fn parse_pg(url: &str) -> (String, u16, String, String, String) {
    let rest = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .expect("postgres url");
    let (auth, rest) = rest.split_once('@').expect("user:pass@");
    let (user, pw) = auth.split_once(':').expect("user:pass");
    let (hp, db) = rest.split_once('/').expect("/db");
    let (h, p) = hp.split_once(':').unwrap_or((hp, "5432"));
    (
        h.into(),
        p.parse().expect("port"),
        user.into(),
        pw.into(),
        db.into(),
    )
}

impl Pg {
    async fn new(setup: &[&str]) -> Pg {
        let url = pg_url();
        let (host, port, user, password, database) = parse_pg(&url);
        // Set once per process, before any connector reads it (no repeated env
        // mutation while other threads may be reading the environment).
        static PASSWORD_ONCE: std::sync::Once = std::sync::Once::new();
        PASSWORD_ONCE.call_once(|| {
            // SAFETY: runs once, at the first fixture creation, before this process starts
            // any connector work that reads the variable.
            unsafe { std::env::set_var(PW_ENV, &password) };
        });
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("Postgres not reachable — start tests/docker/compose.yaml");
        let schema = format!(
            "pgdecode_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        );
        let mut stmts = vec![
            format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
            format!("CREATE SCHEMA {schema}"),
        ];
        stmts.extend(setup.iter().map(|s| s.replace("$S", &schema)));
        for s in stmts {
            sqlx::query(sqlx::AssertSqlSafe(s.as_str()))
                .execute(&pool)
                .await
                .unwrap_or_else(|e| panic!("setup failed: {s}: {e}"));
        }
        Pg {
            pool,
            schema,
            host,
            port,
            user,
            password,
            database,
        }
    }

    async fn cleanup(self) {
        let s = format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema);
        let _ = sqlx::query(sqlx::AssertSqlSafe(s.as_str()))
            .execute(&self.pool)
            .await;
        self.pool.close().await;
    }

    fn table(&self, t: &str) -> String {
        format!("{}.{t}", self.schema)
    }

    async fn extractor(&self) -> PostgresExtractor {
        PostgresExtractor::connect(
            &self.host,
            self.port,
            &self.user,
            &self.password,
            &self.database,
            3,
            60_000,
            "pg-decode-test",
        )
        .await
        .expect("extractor connect")
    }

    fn job(&self, table: &str, application_name: &str) -> JobConfig {
        JobConfig {
            job_id: format!("pgdecode-{}-{table}", self.schema).parse().unwrap(),
            table: table.to_string(),
            columns: None,
            filters: Vec::new(),
            source: SourceConfig {
                host: self.host.clone(),
                port: self.port,
                user: self.user.clone(),
                password_env: PW_ENV.to_string(),
                database: self.database.clone(),
                pool_max: 4,
                statement_timeout_ms: 60_000,
                application_name: application_name.to_string(),
                schema: self.schema.clone(),
            },
            checkpoint: CheckpointConfig::default(),
            pushdown: PushdownConfig::default(),
            parallel_scan: ParallelScanConfig::default(),
            execution: ExecutionConfig::default(),
            distributed: DistributedConfig {
                scheduler_url: String::new(),
                workers: 1,
                ..DistributedConfig::default()
            },
        }
    }
}

/// Cursor path: every batch of a full scan.
async fn cursor_batches(
    ex: &PostgresExtractor,
    table: &str,
) -> Result<Vec<RecordBatch>, ExtractorError> {
    let mut out = Vec::new();
    ex.extract_full_table_for_each_batch(table, None, 2, 1 << 24, &mut |b| {
        out.push(b);
        Ok(())
    })
    .await?;
    Ok(out)
}

/// Binary COPY path: every batch of a full scan.
async fn copy_batches(
    ex: &PostgresExtractor,
    table: &str,
) -> Result<Vec<RecordBatch>, ExtractorError> {
    let mut out = Vec::new();
    ex.extract_full_table_via_copy_for_each_batch(table, None, 2, 1 << 24, &mut |b| {
        out.push(b);
        Ok(())
    })
    .await?;
    Ok(out)
}

fn concat(batches: &[RecordBatch]) -> RecordBatch {
    arrow::compute::concat_batches(&batches[0].schema(), batches).expect("concat")
}

/// Values of column `name` ordered by the `id` column.
fn by_id<T>(batch: &RecordBatch, name: &str, get: impl Fn(&dyn Array, usize) -> T) -> Vec<T> {
    let ids = batch
        .column(batch.schema().index_of("id").expect("id"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id is bigint")
        .clone();
    let col = batch.column(batch.schema().index_of(name).expect("column"));
    let mut pairs: Vec<(i64, T)> = (0..batch.num_rows())
        .map(|i| (ids.value(i), get(col.as_ref(), i)))
        .collect();
    pairs.sort_by_key(|(id, _)| *id);
    pairs.into_iter().map(|(_, v)| v).collect()
}

fn assert_unsupported_value(r: Result<Vec<RecordBatch>, ExtractorError>, column: &str, what: &str) {
    match r {
        Err(ExtractorError::UnsupportedValue { column: c, .. }) => {
            assert_eq!(c, column, "{what}: error must name the column")
        }
        Err(e) => panic!("{what}: expected UnsupportedValue, got {e}"),
        Ok(b) => panic!(
            "{what}: expected a typed error, got Ok with {} rows",
            b.iter().map(|b| b.num_rows()).sum::<usize>()
        ),
    }
}

// ---------------------------------------------------------------------------------------
// ±infinity and extreme timestamps/dates
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn infinity_is_a_typed_error_on_both_paths() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.tz (id bigint, ts timestamptz)",
        "CREATE TABLE $S.naive (id bigint, ts timestamp)",
        "CREATE TABLE $S.d (id bigint, day date)",
    ])
    .await;
    let ex = pg.extractor().await;
    for (table, column, values) in [
        ("tz", "ts", ["'infinity'", "'-infinity'"]),
        ("naive", "ts", ["'infinity'", "'-infinity'"]),
        ("d", "day", ["'infinity'", "'-infinity'"]),
    ] {
        for v in values {
            for s in [
                format!("TRUNCATE {}", pg.table(table)),
                format!("INSERT INTO {} VALUES (1, NULL), (2, {v})", pg.table(table)),
            ] {
                sqlx::query(sqlx::AssertSqlSafe(s))
                    .execute(&pg.pool)
                    .await?;
            }
            let what = format!("{table} {v}");
            assert_unsupported_value(
                cursor_batches(&ex, &pg.table(table)).await,
                column,
                &format!("cursor {what}"),
            );
            assert_unsupported_value(
                copy_batches(&ex, &pg.table(table)).await,
                column,
                &format!("copy {what}"),
            );
        }
    }
    pg.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn extreme_finite_timestamps_match_postgres_on_both_paths() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint, tz timestamptz, naive timestamp, day date)",
        "INSERT INTO $S.t VALUES
           (1, '1970-01-01 00:00:00+00', '1970-01-01 00:00:00', '1970-01-01'),
           (2, '2038-01-19 03:14:08+00', '2038-01-19 03:14:08', '2038-01-19'),
           (3, '9999-12-31 23:59:59.999999+00', '9999-12-31 23:59:59.999999', '9999-12-31'),
           (4, '0001-01-01 00:00:00+00', '0001-01-01 00:00:00', '0001-01-01'),
           (5, '1999-12-31 23:59:59.999999+00', '1999-12-31 23:59:59.999999', '1999-12-31'),
           (6, NULL, NULL, NULL)",
    ])
    .await;
    let ex = pg.extractor().await;
    // Reference oracle: Postgres computes the Unix-epoch values itself.
    let reference: Vec<(Option<i64>, Option<i64>, Option<i32>)> =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT (extract(epoch FROM tz) * 1000000)::bigint,
                    (extract(epoch FROM naive) * 1000000)::bigint,
                    (day - DATE '1970-01-01')::int
             FROM {} ORDER BY id",
            pg.table("t")
        )))
        .fetch_all(&pg.pool)
        .await?;
    for (path, batches) in [
        ("cursor", cursor_batches(&ex, &pg.table("t")).await?),
        ("copy", copy_batches(&ex, &pg.table("t")).await?),
    ] {
        let b = concat(&batches);
        let ts = |a: &dyn Array, i: usize| {
            let a = a
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp");
            a.is_valid(i).then(|| a.value(i))
        };
        let tz = by_id(&b, "tz", ts);
        let naive = by_id(&b, "naive", ts);
        let day = by_id(&b, "day", |a, i| {
            let a = a.as_any().downcast_ref::<Date32Array>().expect("date");
            a.is_valid(i).then(|| a.value(i))
        });
        let got: Vec<_> = (0..tz.len()).map(|i| (tz[i], naive[i], day[i])).collect();
        assert_eq!(got, reference, "{path} path");
    }
    pg.cleanup().await;
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Numeric never silently truncated
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn unconstrained_numeric_is_exact_or_a_typed_error_on_both_paths() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.ok (id bigint, n numeric)",
        "INSERT INTO $S.ok VALUES (1, 1.5), (2, -0.0000000001), (3, 123456789012345678.1234567891),
                                  (4, 0), (5, NULL), (6, 1.50000000000000)",
        "CREATE TABLE $S.overscale (id bigint, n numeric)",
        "INSERT INTO $S.overscale VALUES (1, 1.5), (2, 1.123456789012)",
        "CREATE TABLE $S.nan (id bigint, n numeric)",
        "INSERT INTO $S.nan VALUES (1, 'NaN')",
        "CREATE TABLE $S.big (id bigint, n numeric)",
        "INSERT INTO $S.big VALUES (1, 10000000000000000000000000000)",
        "CREATE TABLE $S.constrained (id bigint, n numeric(12,2))",
        "INSERT INTO $S.constrained VALUES (1, 123.45), (2, -7.5), (3, 9999999999.99)",
    ])
    .await;
    let ex = pg.extractor().await;
    let dec = |a: &dyn Array, i: usize| {
        let a = a
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal");
        a.is_valid(i).then(|| a.value(i))
    };
    for (path, batches) in [
        ("cursor", cursor_batches(&ex, &pg.table("ok")).await?),
        ("copy", copy_batches(&ex, &pg.table("ok")).await?),
    ] {
        let b = concat(&batches);
        assert_eq!(
            b.schema().field(1).data_type(),
            &arrow::datatypes::DataType::Decimal128(38, 10)
        );
        assert_eq!(
            by_id(&b, "n", dec),
            vec![
                Some(15_000_000_000),
                Some(-1),
                Some(1_234_567_890_123_456_781_234_567_891),
                Some(0),
                None,
                Some(15_000_000_000),
            ],
            "{path} path"
        );
    }
    for table in ["overscale", "nan", "big"] {
        assert_unsupported_value(
            cursor_batches(&ex, &pg.table(table)).await,
            "n",
            &format!("cursor {table}"),
        );
        assert_unsupported_value(
            copy_batches(&ex, &pg.table(table)).await,
            "n",
            &format!("copy {table}"),
        );
    }
    for (path, batches) in [
        (
            "cursor",
            cursor_batches(&ex, &pg.table("constrained")).await?,
        ),
        ("copy", copy_batches(&ex, &pg.table("constrained")).await?),
    ] {
        let b = concat(&batches);
        assert_eq!(
            by_id(&b, "n", dec),
            vec![Some(12345), Some(-750), Some(999_999_999_999)],
            "{path} path"
        );
    }
    pg.cleanup().await;
    Ok(())
}

// ---------------------------------------------------------------------------------------
// json/jsonb/uuid are Postgres' own text, identical on both paths
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn json_jsonb_uuid_match_postgres_text_on_both_paths() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint, j json, jb jsonb, u uuid)",
        r#"INSERT INTO $S.t VALUES
           (1, '{"b": 1,   "a": 12345678901234567.89}', '{"b": 1, "a": 12345678901234567.89, "big": 1e400}',
            '123e4567-e89b-12d3-a456-426614174000'),
           (2, '[1, 2.50, "x"]', '[]', NULL),
           (3, NULL, NULL, '00000000-0000-0000-0000-000000000000')"#,
    ])
    .await;
    let ex = pg.extractor().await;
    let reference: Vec<(Option<String>, Option<String>, Option<String>)> =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT j::text, jb::text, u::text FROM {} ORDER BY id",
            pg.table("t")
        )))
        .fetch_all(&pg.pool)
        .await?;
    let text = |a: &dyn Array, i: usize| {
        let a = a.as_any().downcast_ref::<StringArray>().expect("utf8");
        a.is_valid(i).then(|| a.value(i).to_string())
    };
    for (path, batches) in [
        ("cursor", cursor_batches(&ex, &pg.table("t")).await?),
        ("copy", copy_batches(&ex, &pg.table("t")).await?),
    ] {
        let b = concat(&batches);
        let (j, jb, u) = (
            by_id(&b, "j", text),
            by_id(&b, "jb", text),
            by_id(&b, "u", text),
        );
        let got: Vec<_> = (0..j.len())
            .map(|i| (j[i].clone(), jb[i].clone(), u[i].clone()))
            .collect();
        assert_eq!(got, reference, "{path} path");
    }
    pg.cleanup().await;
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Computed keyset partitions cover every row exactly once
// ---------------------------------------------------------------------------------------

async fn partitioned_ids(
    pg: &Pg,
    ex: &PostgresExtractor,
    column: &str,
    n: usize,
    use_copy: bool,
) -> Result<Vec<i64>, ExtractorError> {
    let parts = compute_keyset_partitions(ex.pool(), &pg.schema, "t", column, n).await?;
    let mut ids = Vec::new();
    for part in &parts {
        ex.extract_partition_for_each_batch(
            &pg.table("t"),
            Some(vec!["id"]),
            Some(part),
            use_copy,
            3,
            1 << 24,
            &mut |b| {
                let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                ids.extend(a.values().iter().copied());
                Ok(())
            },
        )
        .await?;
    }
    ids.sort_unstable();
    Ok(ids)
}

#[tokio::test]
async fn keyset_partitions_cover_nulls_and_extreme_keys_once() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint PRIMARY KEY, k bigint, small int)",
        // NULL keys, i64::MIN/MAX keys, and an int4 column (P3: raw MIN/MAX then cast).
        "INSERT INTO $S.t SELECT g, CASE WHEN g % 4 = 0 THEN NULL ELSE g END, g FROM generate_series(1, 40) g",
        "INSERT INTO $S.t VALUES (41, -9223372036854775808, NULL), (42, 9223372036854775807, -5)",
    ])
    .await;
    let ex = pg.extractor().await;
    let expected: Vec<i64> = (1..=42).collect();
    for column in ["k", "small", "id"] {
        for n in [2, 3, 7] {
            for use_copy in [false, true] {
                let got = partitioned_ids(&pg, &ex, column, n, use_copy).await?;
                assert_eq!(
                    got, expected,
                    "column {column}, {n} partitions, copy={use_copy}: rows lost or duplicated"
                );
            }
        }
    }
    pg.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn datafusion_keyset_scan_keeps_null_keys_under_pushed_filters() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint PRIMARY KEY, k bigint, flag boolean)",
        "INSERT INTO $S.t SELECT g, CASE WHEN g <= 3 THEN NULL ELSE g END, g % 2 = 0 FROM generate_series(1, 10) g",
    ])
    .await;
    let mut cfg = pg.job("t", "pg-decode-df");
    cfg.parallel_scan = ParallelScanConfig {
        strategy: "keyset".parse().unwrap(),
        partitions: 3,
        partition_column: "k".to_string(),
    };
    cfg.pushdown.policy = "always".parse().unwrap();
    let ctx = SessionContext::new();
    register_table(&ctx, &cfg).await?;
    let ids = |batches: Vec<RecordBatch>| {
        let mut v: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                a.values().to_vec()
            })
            .collect();
        v.sort_unstable();
        v
    };
    let all = ids(ctx.sql("SELECT id FROM t").await?.collect().await?);
    // A pushed filter ANDed with `(range) OR k IS NULL` must stay correctly parenthesized.
    let even = ids(ctx
        .sql("SELECT id FROM t WHERE flag = true")
        .await?
        .collect()
        .await?);
    pg.cleanup().await;
    assert_eq!(all, (1..=10).collect::<Vec<_>>(), "NULL-key rows dropped");
    assert_eq!(even, vec![2, 4, 6, 8, 10]);
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Strict inputs
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn strict_inputs_are_typed_errors() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint, name text)",
        "INSERT INTO $S.t VALUES (1, 'x')",
    ])
    .await;
    let ex = pg.extractor().await;
    let table = pg.table("t");

    // Unknown projection column, named.
    let err = ex
        .extract_full_table(&table, Some(vec!["id", "nmae"]))
        .await
        .expect_err("typo must error");
    assert!(
        matches!(err, ExtractorError::Projection(_)) && err.to_string().contains("nmae"),
        "{err}"
    );

    // `batch_size` 0 on every extractor path.
    let zero = ex
        .extract_full_table_for_each_batch(&table, None, 0, 1024, &mut |_| Ok(()))
        .await;
    assert!(
        matches!(zero, Err(ExtractorError::InvalidConfig(_))),
        "{zero:?}"
    );
    let zero = ex
        .extract_full_table_via_copy_for_each_batch(&table, None, 0, 1024, &mut |_| Ok(()))
        .await;
    assert!(
        matches!(zero, Err(ExtractorError::InvalidConfig(_))),
        "{zero:?}"
    );
    let zero = ex
        .extract_keyset_partition_for_each_batch(&table, None, "id", 0, 9, 0, 1024, &mut |_| Ok(()))
        .await;
    assert!(
        matches!(zero, Err(ExtractorError::InvalidConfig(_))),
        "{zero:?}"
    );

    // Missing table, extractor path and provider path.
    let missing = ex.extract_full_table(&pg.table("nope"), None).await;
    assert!(
        matches!(missing, Err(ExtractorError::TableNotFound(_))),
        "{missing:?}"
    );
    let cfg = pg.job("nope", "pg-decode-missing");
    let ctx = SessionContext::new();
    let registered = register_table(&ctx, &cfg).await;
    pg.cleanup().await;
    let err = registered.expect_err("provider must not register a missing table");
    assert!(err.to_string().contains("not found"), "{err}");
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Dropping a DataFusion scan stream stops the source query
// ---------------------------------------------------------------------------------------

/// Backends of `application_name` that are still running something or holding a
/// transaction open.
async fn busy_backends(pool: &sqlx::PgPool, application_name: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity
         WHERE application_name = $1 AND pid <> pg_backend_pid()
           AND state IN ('active', 'idle in transaction', 'idle in transaction (aborted)')",
    )
    .bind(application_name)
    .fetch_one(pool)
    .await
}

#[tokio::test]
async fn dropping_a_scan_stream_stops_the_source() -> R {
    use futures::StreamExt;

    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint, pad text)",
        "INSERT INTO $S.t SELECT g, repeat('x', 200) FROM generate_series(1, 1500000) g",
    ])
    .await;
    for use_copy in [false, true] {
        let app = format!("pg-decode-r5-{use_copy}");
        let mut cfg = pg.job("t", &app);
        cfg.execution.batch_size = 1000;
        cfg.execution.use_copy = use_copy;
        let ctx = SessionContext::new();
        register_table(&ctx, &cfg).await?;
        let mut stream = ctx
            .sql("SELECT id, pad FROM t")
            .await?
            .execute_stream()
            .await?;
        let first = stream.next().await.expect("one batch")?;
        assert!(first.num_rows() > 0);
        drop(stream);

        let mut busy = -1;
        for _ in 0..100 {
            busy = busy_backends(&pg.pool, &app).await?;
            if busy == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(
            busy, 0,
            "copy={use_copy}: the source kept scanning after the consumer dropped the stream"
        );
    }
    pg.cleanup().await;
    Ok(())
}
