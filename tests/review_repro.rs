//! Reproductions for `claude/comprehensive-review-2026-09-24.md`.
//!
//! Every test asserts the **correct** behaviour. All findings are fixed, so every test passes:
//! a passing test means the finding is FIXED, a failing one means its fix REGRESSED.
//! Test names carry the finding id from the review (b1_, b2_, c3_, ...).
//!
//! Oracles (AGENTS.md §7): differential = same SQL with pushdown policy `always` vs `never`;
//! reference = direct SQL against the source; trivial = hand-written expected value.
//!
//! Self-contained on purpose: it does NOT use `tests/common` because `TestDb::drop` could
//! deadlock when these were written (review finding B6, since fixed). Each test creates its
//! own schema / database and removes it with an explicit async cleanup.
//!
//! Needs the compose stack: `docker compose -f tests/docker/compose.yaml up -d --wait`
//! Run: `cargo test --test review_repro -- --test-threads=1`
//! (or `scripts/verify-review.sh`, which does all of it and prints a verdict table).

use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use rust_ballista_extraction_layer::checkpoint::json_store::JsonCheckpointStore;
use rust_ballista_extraction_layer::checkpoint::{CheckpointError, CheckpointStore};
use rust_ballista_extraction_layer::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, FilterEntry, FilterInput, JobConfig,
    ParallelScanConfig, PushdownConfig, SourceConfig,
};
use rust_ballista_extraction_layer::connector::mysql::MysqlExtractor;
use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;
use rust_ballista_extraction_layer::connector::postgres::distributed::DistributedContext;
use rust_ballista_extraction_layer::connector::postgres::extractor::PostgresExtractor;
use rust_ballista_extraction_layer::errors::AppError;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

type R = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const PW_ENV: &str = "REVIEW_REPRO_PG_PASSWORD";
static SEQ: AtomicUsize = AtomicUsize::new(0);

// ---------------------------------------------------------------------------------------
// Postgres fixture
// ---------------------------------------------------------------------------------------

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
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5432/test".to_string())
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
        p.parse().unwrap(),
        user.into(),
        pw.into(),
        db.into(),
    )
}

impl Pg {
    async fn new(setup: &[&str]) -> Pg {
        let url = pg_url();
        let (host, port, user, password, database) = parse_pg(&url);
        // Set once per process, before any connector reads it (T-6: no repeated env
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
            "review_{}_{}",
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

    async fn extractor(&self) -> PostgresExtractor {
        PostgresExtractor::connect(
            &self.host,
            self.port,
            &self.user,
            &self.password,
            &self.database,
            2,
            60_000,
            "review-repro",
        )
        .await
        .expect("extractor connect")
    }

    fn job(&self, table: &str) -> JobConfig {
        JobConfig {
            job_id: format!("review-{}-{}", self.schema, table).parse().unwrap(),
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
                application_name: "review-repro".to_string(),
                schema: self.schema.clone(),
            },
            checkpoint: CheckpointConfig {
                dir: std::env::temp_dir()
                    .join(format!("review_repro_{}", self.schema))
                    .to_string_lossy()
                    .to_string(),
                ..CheckpointConfig::default()
            },
            pushdown: PushdownConfig::default(),
            parallel_scan: ParallelScanConfig::default(),
            execution: ExecutionConfig::default(),
            distributed: DistributedConfig {
                scheduler_url: String::new(),
                workers: 1,
            },
        }
    }

    /// `SELECT id FROM <table> WHERE <filter>` through DataFusion with the given pushdown policy.
    async fn ids_where(&self, table: &str, policy: &str, filter: &str) -> Result<Vec<i64>, String> {
        let mut cfg = self.job(table);
        cfg.pushdown.policy = policy.parse().unwrap();
        let ctx = DistributedContext::standalone(&cfg, 1)
            .await
            .map_err(|e| e.to_string())?;
        ctx.register_source(&cfg).await.map_err(|e| e.to_string())?;
        let df = ctx
            .session
            .sql(&format!("SELECT id FROM {table} WHERE {filter}"))
            .await
            .map_err(|e| e.to_string())?;
        let batches = df.collect().await.map_err(|e| e.to_string())?;
        Ok(sorted_ids(&batches))
    }

    /// Differential oracle: `always` must return exactly what `never` (all filtering in Arrow) returns.
    async fn differential(&self, table: &str, filter: &str) -> R {
        let kept = self.ids_where(table, "never", filter).await;
        let pushed = self.ids_where(table, "always", filter).await;
        println!(
            "  filter: {filter}\n  never (Arrow only): {kept:?}\n  always (pushed)   : {pushed:?}"
        );
        // Return (not panic) so the caller still runs cleanup before failing the test.
        if pushed != kept {
            return Err(format!(
                "pushdown changed the answer for `{filter}`: pushed={pushed:?} kept={kept:?}"
            )
            .into());
        }
        Ok(())
    }
}

fn sorted_ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids = Vec::new();
    for b in batches {
        let a = b.column(b.schema().index_of("id").unwrap()).clone();
        let a = arrow::compute::cast(&a, &DataType::Int64).unwrap();
        let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
        ids.extend((0..a.len()).filter(|&i| a.is_valid(i)).map(|i| a.value(i)));
    }
    ids.sort_unstable();
    ids
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ---------------------------------------------------------------------------------------
// B1 — Inexact pushdown that returns FEWER rows than DataFusion's own evaluation
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn b1_text_range_under_icu_collation_loses_rows() -> R {
    let pg = Pg::new(&[
        r#"CREATE TABLE $S.t (id bigint PRIMARY KEY, name text COLLATE "en-US-x-icu")"#,
        "INSERT INTO $S.t VALUES (1,'B'),(2,'a'),(3,'c')",
        "ANALYZE $S.t",
    ])
    .await;
    let r = pg.differential("t", "name < 'a'").await;
    pg.cleanup().await;
    r
}

#[tokio::test]
async fn b1_not_over_inexact_under_nondeterministic_collation_loses_rows() -> R {
    let pg = Pg::new(&[
        "CREATE COLLATION IF NOT EXISTS $S.ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
        "CREATE TABLE $S.t (id bigint PRIMARY KEY, name text COLLATE $S.ci)",
        "INSERT INTO $S.t VALUES (1,'FOO'),(2,'bar')",
        "ANALYZE $S.t",
    ])
    .await;
    let r = pg.differential("t", "NOT (name = 'foo')").await;
    pg.cleanup().await;
    r
}

#[tokio::test]
async fn b1_negative_zero_float_range_loses_rows() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint PRIMARY KEY, x double precision)",
        "INSERT INTO $S.t VALUES (1,'-0'),(2,1.0),(3,-1.0)",
        "ANALYZE $S.t",
    ])
    .await;
    let r = pg.differential("t", "x < 0.0").await;
    pg.cleanup().await;
    r
}

// ---------------------------------------------------------------------------------------
// B5 — `(NOT x) IS NULL` rendered without parentheses
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn b5_not_is_null_rendered_sql_has_wrong_precedence() -> R {
    use datafusion::prelude::col;
    use rust_ballista_extraction_layer::pushdown::translate;
    // `render_inline` (Postgres inline SQL) moved to the Postgres connector (review A1).
    use rust_ballista_extraction_layer::connector::postgres::inline_sql::PredicateInlineSql;
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint PRIMARY KEY, flag boolean)",
        "INSERT INTO $S.t VALUES (1,true),(2,false),(3,NULL)",
    ])
    .await;
    // What the connector would put in the WHERE clause for `(NOT flag) IS NULL`.
    let expr = datafusion::logical_expr::Expr::IsNull(Box::new(!col("flag")));
    let (fidelity, pred) = translate(&expr).expect("translatable");
    let where_sql = pred.render_inline();
    let sql = format!(
        "SELECT id FROM {}.t WHERE {where_sql} ORDER BY id",
        pg.schema
    );
    let got: Vec<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
        .fetch_all(&pg.pool)
        .await?;
    let got: Vec<i64> = got.into_iter().map(|r| r.0).collect();
    println!(
        "  fidelity={fidelity:?}  rendered WHERE: {where_sql}\n  source returns {got:?}, correct is [3]"
    );
    // Also through DataFusion end-to-end (may be masked if DataFusion simplifies the expr).
    let e2e = pg.differential("t", "(NOT flag) IS NULL").await;
    pg.cleanup().await;
    assert_eq!(
        got,
        vec![3],
        "rendered SQL `{where_sql}` returns the wrong rows"
    );
    e2e
}

// ---------------------------------------------------------------------------------------
// C4 / C5 — pushed comparisons that make Postgres raise 42883
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn c4_enum_or_filter_fails_when_pushed() -> R {
    let pg = Pg::new(&[
        "CREATE TYPE $S.mood AS ENUM ('sad','ok','happy')",
        "CREATE TABLE $S.t (id bigint PRIMARY KEY, m $S.mood)",
        "INSERT INTO $S.t VALUES (1,'sad'),(2,'ok'),(3,'happy')",
        "ANALYZE $S.t",
    ])
    .await;
    let r = pg.differential("t", "m = 'sad' OR m = 'happy'").await;
    pg.cleanup().await;
    r
}

#[tokio::test]
async fn c5_uuid_equality_fails_when_pushed() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint PRIMARY KEY, u uuid UNIQUE)",
        "INSERT INTO $S.t VALUES (1,'123e4567-e89b-12d3-a456-426614174000'),(2,'123e4567-e89b-12d3-a456-426614174001')",
        "ANALYZE $S.t",
    ])
    .await;
    let r = pg
        .differential("t", "u = '123e4567-e89b-12d3-a456-426614174001'")
        .await;
    pg.cleanup().await;
    r
}

// ---------------------------------------------------------------------------------------
// B2 — checkpoint not tied to the filter that produced it
// ---------------------------------------------------------------------------------------

/// The checkpointed terminal (`run_with`) with a consumer that counts what it receives.
/// (`run()` is a checkpoint-free diagnostic since B3, so it cannot reproduce B2.)
async fn delivered_rows(c: JobConfig) -> Result<u64, AppError> {
    let delivered = AtomicU64::new(0);
    let delivered = &delivered;
    PostgresConnector::from_config(c)?
        .extract()
        .standalone()
        .run_with(move |_split, mut stream| async move {
            while let Some(batch) = stream.try_next().await? {
                delivered.fetch_add(batch.num_rows() as u64, Ordering::SeqCst);
            }
            Ok(())
        })
        .await?;
    Ok(delivered.load(Ordering::SeqCst))
}

#[tokio::test]
async fn b2_rerun_with_different_filter_is_skipped() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint PRIMARY KEY)",
        "INSERT INTO $S.t SELECT g FROM generate_series(1,10) g",
    ])
    .await;
    let mk = |f: &str| {
        let mut c = pg.job("t");
        // same job, as an orchestrator would reuse it
        c.job_id = format!("review-b2-{}", pg.schema).parse().unwrap();
        c.filters = vec![FilterEntry::Single(FilterInput::Shorthand(f.to_string()))];
        c
    };
    let first = delivered_rows(mk("id>8")).await?;
    let second = delivered_rows(mk("id>2")).await;
    let sql = format!("SELECT count(*) FROM {}.t WHERE id > 2", pg.schema);
    let (expected,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
        .fetch_one(&pg.pool)
        .await?;
    // The only acceptable outcomes: the second run delivers exactly the new range, or it
    // refuses with a typed plan-mismatch error — never "already completed" with run 1's rows.
    let second = match second {
        Ok(rows) => rows,
        Err(AppError::Checkpoint(CheckpointError::PlanMismatch { .. })) => {
            println!("  run 2 refused: plan mismatch (correct); resetting and re-running");
            let c = mk("id>2");
            let store = JsonCheckpointStore::new(&c.checkpoint.dir)?;
            store.reset(&c.job_id).await?;
            delivered_rows(c).await?
        }
        Err(e) => return Err(e.into()),
    };
    println!("  run 1 (id>8): {first} rows; run 2 (id>2): {second} rows; source says {expected}");
    pg.cleanup().await;
    assert_eq!(
        second as i64, expected,
        "second run reused the first run's completed split"
    );
    Ok(())
}

// ---------------------------------------------------------------------------------------
// B4 — ±infinity timestamps
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn b4_infinity_timestamp_cursor_path_does_not_panic() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint, ts timestamptz)",
        "INSERT INTO $S.t VALUES (1,'infinity')",
    ])
    .await;
    let ex = pg.extractor().await;
    let table = format!("{}.t", pg.schema);
    let joined = tokio::spawn(async move {
        ex.extract_full_table(&table, None)
            .await
            .map(|b| b.num_rows())
            .map_err(|e| e.to_string())
    })
    .await;
    pg.cleanup().await;
    match joined {
        Err(e) if e.is_panic() => panic!("extraction PANICKED on 'infinity': {e}"),
        Err(e) => panic!("task failed: {e}"),
        Ok(r) => {
            println!("  result: {r:?} (Ok or typed Err are both acceptable)");
            Ok(())
        }
    }
}

#[tokio::test]
async fn b4_neg_infinity_timestamp_copy_path_is_not_garbage() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint, ts timestamptz)",
        "INSERT INTO $S.t VALUES (1,'-infinity')",
    ])
    .await;
    let ex = pg.extractor().await;
    let mut out = Vec::new();
    let r = ex
        .extract_full_table_via_copy_for_each_batch(
            &format!("{}.t", pg.schema),
            None,
            1024,
            1 << 24,
            &mut |b| {
                out.push(b);
                Ok(())
            },
        )
        .await;
    pg.cleanup().await;
    if let Err(e) = r {
        println!("  typed error (acceptable): {e}");
        return Ok(());
    }
    let b = &out[0];
    let a = b
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    println!("  is_null={} raw_micros={}", a.is_null(0), a.value(0));
    assert!(
        a.is_null(0),
        "'-infinity' decoded as a real timestamp: {} µs since epoch",
        a.value(0)
    );
    Ok(())
}

// ---------------------------------------------------------------------------------------
// C1 / C2 — keyset partitioning
// ---------------------------------------------------------------------------------------

async fn keyset_count(pg: &Pg, column: &str) -> Result<usize, String> {
    let mut c = pg.job("t");
    c.parallel_scan = ParallelScanConfig {
        strategy: "keyset".parse().unwrap(),
        partitions: 2,
        partition_column: column.to_string(),
    };
    let batches = PostgresConnector::from_config(c)
        .map_err(|e| e.to_string())?
        .extract()
        .standalone()
        .collect()
        .await
        .map_err(|e| e.to_string())?;
    Ok(rows(&batches))
}

#[tokio::test]
async fn c1_keyset_partitioning_keeps_null_keys() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint PRIMARY KEY, k bigint)",
        "INSERT INTO $S.t SELECT g, CASE WHEN g <= 3 THEN NULL ELSE g END FROM generate_series(1,10) g",
    ])
    .await;
    let got = keyset_count(&pg, "k").await;
    pg.cleanup().await;
    println!("  keyset on nullable k: {got:?} rows, table has 10 (3 with k NULL)");
    assert_eq!(got?, 10, "rows with NULL partition key were dropped");
    Ok(())
}

#[tokio::test]
async fn c2_keyset_partitioning_survives_i64_max_key() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint PRIMARY KEY)",
        "INSERT INTO $S.t VALUES (1),(2),(9223372036854775807)",
    ])
    .await;
    use futures::FutureExt;
    let joined = std::panic::AssertUnwindSafe(keyset_count(&pg, "id"))
        .catch_unwind()
        .await;
    pg.cleanup().await;
    let got = match joined {
        Err(p) => panic!(
            "keyset bound arithmetic PANICKED (overflow): {}",
            p.downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default()
        ),
        Ok(r) => r,
    };
    println!("  keyset with max key = i64::MAX: {got:?} rows, table has 3");
    assert_eq!(got?, 3);
    Ok(())
}

// ---------------------------------------------------------------------------------------
// C3 / C6 / C8 / R4 — decoding and config strictness
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn c3_unconstrained_numeric_is_not_truncated() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint, n numeric)",
        "INSERT INTO $S.t VALUES (1, 1.123456789012)",
    ])
    .await;
    let ex = pg.extractor().await;
    let r = ex
        .extract_full_table(&format!("{}.t", pg.schema), None)
        .await;
    pg.cleanup().await;
    match r {
        Err(e) => {
            println!("  typed error (acceptable): {e}");
            Ok(())
        }
        Ok(b) => {
            let s = arrow::util::display::array_value_to_string(b.column(1), 0)?;
            println!(
                "  source 1.123456789012 -> arrow {s} ({})",
                b.column(1).data_type()
            );
            assert_eq!(
                s.trim_end_matches('0'),
                "1.123456789012",
                "value silently truncated"
            );
            Ok(())
        }
    }
}

#[tokio::test]
async fn c6_jsonb_big_number_round_trips() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint, j jsonb)",
        r#"INSERT INTO $S.t VALUES (1, '{"amt": 12345678901234567.89}')"#,
    ])
    .await;
    let ex = pg.extractor().await;
    let b = ex
        .extract_full_table(&format!("{}.t", pg.schema), None)
        .await;
    pg.cleanup().await;
    let b = b?;
    let a = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    println!("  cursor path jsonb -> {}", a.value(0));
    assert!(
        a.value(0).contains("12345678901234567.89"),
        "number changed: {}",
        a.value(0)
    );
    Ok(())
}

#[tokio::test]
async fn c8_projection_typo_is_an_error() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint, name text)",
        "INSERT INTO $S.t VALUES (1,'x')",
    ])
    .await;
    let mut c = pg.job("t");
    c.columns = Some(vec!["id".to_string(), "nmae".to_string()]);
    let r = PostgresConnector::from_config(c)
        .expect("valid job config")
        .extract()
        .standalone()
        .collect()
        .await;
    pg.cleanup().await;
    match r {
        Err(e) => {
            println!("  error (correct): {e}");
            Ok(())
        }
        Ok(b) => panic!(
            "unknown column `nmae` silently ignored; got {} column(s): {:?}",
            b.first().map(|b| b.num_columns()).unwrap_or(0),
            b.first().map(|b| b
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>())
        ),
    }
}

#[tokio::test]
async fn r4_batch_size_zero_is_rejected_not_zero_rows() -> R {
    let pg = Pg::new(&[
        "CREATE TABLE $S.t (id bigint PRIMARY KEY)",
        "INSERT INTO $S.t SELECT g FROM generate_series(1,5) g",
    ])
    .await;
    let mut c = pg.job("t");
    c.execution.batch_size = 0;
    c.execution.use_copy = false;
    // Rejected either when the connector is built (config validation) or at run time —
    // never a "successful" run with zero rows.
    let r = match PostgresConnector::from_config(c) {
        Ok(connector) => connector.extract().standalone().run().await,
        Err(e) => Err(e),
    };
    pg.cleanup().await;
    match r {
        Err(e) => {
            println!("  error (correct): {e}");
            Ok(())
        }
        Ok(o) => panic!(
            "batch_size=0 accepted: run 'succeeded' with {} rows (table has 5)",
            o.rows_extracted
        ),
    }
}

// ---------------------------------------------------------------------------------------
// C9 — MySQL decode
// ---------------------------------------------------------------------------------------

struct My {
    admin: sqlx::MySqlPool,
    db: String,
    host: String,
    port: u16,
    user: String,
    password: String,
}

impl My {
    async fn new(setup: &[&str]) -> My {
        let url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@127.0.0.1:3306/test".to_string());
        let rest = url.strip_prefix("mysql://").unwrap();
        let (auth, rest) = rest.split_once('@').unwrap();
        let (user, password) = auth.split_once(':').unwrap();
        let hp = rest.split('/').next().unwrap();
        let (host, port) = hp.split_once(':').unwrap_or((hp, "3306"));
        let admin = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("MySQL not reachable — start tests/docker/compose.yaml");
        let db = format!(
            "review_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        );
        let mut stmts = vec![
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("CREATE DATABASE {db}"),
        ];
        stmts.extend(setup.iter().map(|s| s.replace("$S", &db)));
        for s in stmts {
            sqlx::query(sqlx::AssertSqlSafe(s.as_str()))
                .execute(&admin)
                .await
                .unwrap_or_else(|e| panic!("setup failed: {s}: {e}"));
        }
        My {
            admin,
            db,
            host: host.into(),
            port: port.parse().unwrap(),
            user: user.into(),
            password: password.into(),
        }
    }
    async fn extract(&self, table: &str) -> Result<RecordBatch, String> {
        let ex = MysqlExtractor::connect(
            &self.host,
            self.port,
            &self.user,
            &self.password,
            &self.db,
            2,
        )
        .await
        .map_err(|e| e.to_string())?;
        ex.extract_full_table(table, None)
            .await
            .map_err(|e| e.to_string())
    }
    async fn cleanup(self) {
        let s = format!("DROP DATABASE IF EXISTS {}", self.db);
        let _ = sqlx::query(sqlx::AssertSqlSafe(s.as_str()))
            .execute(&self.admin)
            .await;
        self.admin.close().await;
    }
}

#[tokio::test]
async fn c9_mysql_bigint_unsigned_max_is_not_negative() -> R {
    let my = My::new(&[
        "CREATE TABLE $S.t (id int, u bigint unsigned)",
        "INSERT INTO $S.t VALUES (1, 18446744073709551615)",
    ])
    .await;
    let r = my.extract("t").await;
    my.cleanup().await;
    let b = match r {
        Err(e) => {
            println!("  typed error (acceptable): {e}");
            return Ok(());
        }
        Ok(b) => b,
    };
    let s = arrow::util::display::array_value_to_string(b.column(1), 0)?;
    println!(
        "  18446744073709551615 -> {s} ({})",
        b.column(1).data_type()
    );
    assert_eq!(s, "18446744073709551615", "unsigned value wrapped");
    Ok(())
}

#[tokio::test]
async fn c9_mysql_int_unsigned_max_is_not_negative() -> R {
    let my = My::new(&[
        "CREATE TABLE $S.t (id int, u int unsigned)",
        "INSERT INTO $S.t VALUES (1, 4294967295)",
    ])
    .await;
    let r = my.extract("t").await;
    my.cleanup().await;
    let b = r?;
    let s = arrow::util::display::array_value_to_string(b.column(1), 0)?;
    println!("  4294967295 -> {s} ({})", b.column(1).data_type());
    assert_eq!(s, "4294967295", "unsigned value wrapped");
    Ok(())
}

#[tokio::test]
async fn c9_mysql_boolean_maps_to_arrow_boolean() -> R {
    let my = My::new(&[
        "CREATE TABLE $S.t (id int, b BOOLEAN)",
        "INSERT INTO $S.t VALUES (1, TRUE)",
    ])
    .await;
    let r = my.extract("t").await;
    my.cleanup().await;
    let b = r?;
    println!("  BOOLEAN column -> {}", b.column(1).data_type());
    assert_eq!(b.column(1).data_type(), &DataType::Boolean);
    Ok(())
}

#[tokio::test]
async fn c9_mysql_year_and_time_decode() -> R {
    let my = My::new(&[
        "CREATE TABLE $S.t (id int, y YEAR, tm TIME)",
        "INSERT INTO $S.t VALUES (1, 2024, '12:34:56')",
    ])
    .await;
    let r = my.extract("t").await;
    my.cleanup().await;
    println!(
        "  YEAR/TIME extract -> {:?}",
        r.as_ref().map(|b| b.num_rows())
    );
    r?;
    Ok(())
}
