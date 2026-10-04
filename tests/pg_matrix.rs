//! Cross-cutting test matrix: metamorphic worker count, multi-batch through the
//! provider, `collect()` vs `stream()`, and full empty-result schema equality.
//!
//! Every test names its oracle (AGENTS.md §7):
//! - **metamorphic**: the same query under a changed, semantically irrelevant setting
//!   (worker count, batch size, terminal) must return the same multiset of rows;
//! - **reference**: Postgres itself (`count(*)`, the ids by direct SQL);
//! - **trivial**: a hand-written expected schema.
//!
//! Rows are compared as whole rendered tuples (every column), sorted — never per-column —
//! so a row-permutation bug cannot pass.
//!
//! Run: `cargo test --test pg_matrix -- --test-threads=1` (requires the compose stack up;
//! `DATABASE_URL` overrides the default endpoint).

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use common::{TEST_PASSWORD_ENV, TestCluster, TestDb};
use datafusion::prelude::{SessionContext, col, lit};
use el_ballista::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, JobConfig, ParallelScanConfig,
    PushdownConfig, SourceConfig,
};
use el_ballista::connector::postgres::PostgresConnector;
use el_ballista::connector::postgres::PostgresTableProvider;
use el_ballista::pushdown::PushdownPolicy;
use futures::TryStreamExt;

type R = Result<(), Box<dyn std::error::Error>>;

/// Rows in the generated `wide` table (enough for several batches per partition).
const WIDE_ROWS: i64 = 2_000;

fn job(db: &TestDb, table: &str, partitions: usize, batch_size: usize) -> JobConfig {
    JobConfig {
        job_id: format!("matrix-{}-{table}", db.schema).parse().unwrap(),
        table: table.to_string(),
        columns: None,
        filters: Vec::new(),
        source: SourceConfig {
            host: db.host.clone(),
            port: db.port,
            user: db.user.clone(),
            password_env: TEST_PASSWORD_ENV.to_string(),
            database: db.database.clone(),
            pool_max: 8,
            statement_timeout_ms: 300_000,
            application_name: "relex-matrix".to_string(),
            schema: db.schema.clone(),
        },
        checkpoint: CheckpointConfig::default(),
        pushdown: PushdownConfig::default(),
        parallel_scan: if partitions > 1 {
            ParallelScanConfig {
                strategy: "keyset".parse().unwrap(),
                partitions,
                partition_column: "id".to_string(),
            }
        } else {
            ParallelScanConfig::default()
        },
        execution: ExecutionConfig {
            batch_size,
            ..ExecutionConfig::default()
        },
        distributed: DistributedConfig {
            scheduler_url: String::new(),
            workers: 1,
            ..DistributedConfig::default()
        },
    }
}

/// `wide`: a generated table big enough for multi-batch, multi-partition scans, with NULLs,
/// text, numeric, timestamps and bytea so the comparison is not id-only.
async fn create_wide(db: &TestDb) -> R {
    let s = &db.schema;
    for sql in [
        format!(
            "CREATE TABLE {s}.wide (id bigint PRIMARY KEY, label text, amount numeric(12,2), \
             at timestamptz, bin bytea)"
        ),
        format!(
            "INSERT INTO {s}.wide
             SELECT g,
                    CASE WHEN g % 7 = 0 THEN NULL ELSE 'row-' || g END,
                    CASE WHEN g % 5 = 0 THEN NULL ELSE (g * 1.25)::numeric(12,2) END,
                    TIMESTAMPTZ '2024-01-01 00:00:00+00' + (g % 97) * INTERVAL '1 hour',
                    CASE WHEN g % 3 = 0 THEN NULL ELSE decode(lpad(to_hex(g % 256), 2, '0'), 'hex') END
             FROM generate_series(1, {WIDE_ROWS}) g"
        ),
        format!("ANALYZE {s}.wide"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&db.pool)
            .await
            .map_err(|e| format!("setup failed: {sql}: {e}"))?;
    }
    Ok(())
}

/// Every row as its full rendered tuple, sorted (order-independent multiset comparison).
fn rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    let opts = FormatOptions::default().with_null("<NULL>");
    let mut out = Vec::new();
    for b in batches {
        let formatters: Vec<_> = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts).expect("formatter"))
            .collect();
        for r in 0..b.num_rows() {
            out.push(formatters.iter().map(|f| f.value(r).to_string()).collect());
        }
    }
    out.sort();
    out
}

/// Sorted ids of every batch.
fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut v: Vec<i64> = batches
        .iter()
        .flat_map(|b| common::int64_col(b, "id"))
        .collect();
    v.sort_unstable();
    v
}

/// Reference oracle: the ids straight from Postgres.
async fn source_ids(db: &TestDb, table: &str) -> Result<Vec<i64>, sqlx::Error> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT id FROM {}.{table} ORDER BY id",
        db.schema
    )))
    .fetch_all(&db.pool)
    .await
}

#[tokio::test]
async fn one_vs_three_workers_return_identical_rows() -> R {
    // Oracle: metamorphic (1 vs 3 Ballista worker processes over the same 3 keyset
    // partitions must return the same multiset of whole rows) + reference (the ids by direct
    // SQL, so both sides being equally wrong cannot pass).
    let db = TestDb::connect().await;
    create_wide(&db).await?;
    let one_worker = TestCluster::start(1, 2).await;
    let three_workers = TestCluster::start(3, 2).await;
    for table in ["hostile", "wide"] {
        let connector = PostgresConnector::from_config(job(&db, table, 3, 256))?;
        let one = connector
            .extract()
            .distributed()
            .scheduler(&one_worker.url)
            .workers(1)
            .collect()
            .await?;
        let three = connector
            .extract()
            .distributed()
            .scheduler(&three_workers.url)
            .workers(3)
            .collect()
            .await?;
        assert_eq!(
            ids(&one),
            source_ids(&db, table).await?,
            "{table}: 1 worker"
        );
        let (one, three) = (rows(&one), rows(&three));
        assert!(!one.is_empty(), "{table}: vacuous comparison");
        assert_eq!(one, three, "{table}: worker count changed the result");
    }
    db.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn provider_streams_many_batches_with_the_source_row_set() -> R {
    // Oracle: reference (direct SQL ids) + metamorphic (the whole-row multiset at
    // batch_size 7 equals the one at the default batch size). Non-vacuity: the small batch
    // size really produces more than one batch through the provider (cursor and COPY).
    let db = TestDb::connect().await;
    create_wide(&db).await?;
    for table in ["hostile", "wide"] {
        let expected_ids = source_ids(&db, table).await?;
        let mut baseline: Option<Vec<Vec<String>>> = None;
        for (batch_size, use_copy) in [(8192, false), (7, false), (7, true)] {
            let mut config = job(&db, table, 1, batch_size);
            config.pushdown.policy = PushdownPolicy::Never;
            config.execution.use_copy = use_copy;
            let provider = PostgresTableProvider::from_config(&config).await?;
            let schema = provider_schema(&provider);
            let ctx = SessionContext::new();
            ctx.register_table(table, Arc::new(provider))?;
            let stream = ctx.table(table).await?.execute_stream().await?;
            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            let what = format!("{table} batch_size={batch_size} copy={use_copy}");
            if batch_size < 8192 {
                assert!(
                    batches.len() > 1,
                    "{what}: expected >1 batch, got {}",
                    batches.len()
                );
                assert!(
                    batches.iter().all(|b| b.num_rows() <= batch_size),
                    "{what}: a batch exceeded batch_size"
                );
            }
            for b in &batches {
                assert_eq!(b.schema(), schema, "{what}: batch schema drifted");
            }
            assert_eq!(ids(&batches), expected_ids, "{what}: row set != direct SQL");
            let got = rows(&batches);
            match &baseline {
                None => baseline = Some(got),
                Some(base) => assert_eq!(&got, base, "{what}: rows differ from batch_size 8192"),
            }
        }
    }
    db.cleanup().await;
    Ok(())
}

fn provider_schema(provider: &PostgresTableProvider) -> arrow::datatypes::SchemaRef {
    use datafusion::catalog::TableProvider;
    provider.schema()
}

#[tokio::test]
async fn collect_and_stream_return_identical_rows() -> R {
    // Oracle: metamorphic (the materializing `collect()` terminal and the streaming
    // `stream()` terminal of the same job return the same whole-row multiset, standalone and
    // distributed) + reference (direct SQL ids).
    let db = TestDb::connect().await;
    create_wide(&db).await?;
    let connector = PostgresConnector::from_config(job(&db, "wide", 3, 100))?;
    let expected_ids = source_ids(&db, "wide").await?;

    let collected = connector.extract().standalone().collect().await?;
    let streamed: Vec<RecordBatch> = connector
        .extract()
        .standalone()
        .stream()
        .await?
        .try_collect()
        .await?;
    assert_eq!(ids(&collected), expected_ids, "standalone collect");
    assert!(streamed.len() > 1, "stream() should yield many batches");
    assert_eq!(
        rows(&collected),
        rows(&streamed),
        "standalone collect != stream"
    );

    let cluster = TestCluster::start(1, 2).await;
    let collected = connector
        .extract()
        .distributed()
        .scheduler(&cluster.url)
        .collect()
        .await?;
    let streamed: Vec<RecordBatch> = connector
        .extract()
        .distributed()
        .scheduler(&cluster.url)
        .stream()
        .await?
        .try_collect()
        .await?;
    assert_eq!(ids(&collected), expected_ids, "distributed collect");
    assert_eq!(
        rows(&collected),
        rows(&streamed),
        "distributed collect != stream"
    );
    db.cleanup().await;
    Ok(())
}

/// The hostile table's Arrow schema, written out by hand from `tests/data/hostile.sql`
/// and the documented type mapping (docs/connectors/postgres.md).
fn expected_hostile_schema() -> Schema {
    let utc = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("nick", DataType::Utf8, true),
        Field::new("code", DataType::Utf8, true),
        Field::new("amount", DataType::Decimal128(12, 2), true),
        Field::new("precise", DataType::Decimal128(30, 15), true),
        Field::new("count", DataType::Int32, true),
        Field::new("big", DataType::Int64, true),
        Field::new("ratio", DataType::Float64, true),
        Field::new("f", DataType::Float32, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("meta", DataType::Utf8, true),
        Field::new("uid", DataType::Utf8, true),
        Field::new("day", DataType::Date32, true),
        Field::new("ts", utc(), true),
        Field::new(
            "naive",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("feeling", DataType::Utf8, true),
        Field::new("updated_at", utc(), false),
        Field::new("bin", DataType::Binary, true),
    ])
}

#[tokio::test]
async fn empty_result_schema_equals_the_table_schema() -> R {
    // Oracle: trivial (the hand-written schema: every name, type, timezone, precision and
    // nullability) against the provider's schema — the connector's single schema function over
    // the catalog metadata — and against an empty result, which carries no batch to inspect.
    let db = TestDb::connect().await;
    let expected = Arc::new(expected_hostile_schema());

    for use_copy in [false, true] {
        let mut config = job(&db, "hostile", 1, 8192);
        config.pushdown.policy = PushdownPolicy::Always;
        config.execution.use_copy = use_copy;
        let provider = PostgresTableProvider::from_config(&config).await?;
        assert_eq!(
            provider_schema(&provider),
            expected,
            "the provider schema drifted from the fixture"
        );
        let ctx = SessionContext::new();
        ctx.register_table("hostile", Arc::new(provider))?;
        let df = ctx
            .table("hostile")
            .await?
            .filter(col("id").gt(lit(1_000_000i64)))?;
        let stream = df.execute_stream().await?;
        assert_eq!(
            stream.schema(),
            expected,
            "copy={use_copy}: empty stream schema"
        );
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        for b in &batches {
            assert_eq!(b.schema(), expected, "copy={use_copy}: empty batch schema");
        }
    }

    // The connector's streaming terminal with a filter that matches nothing.
    let mut config = job(&db, "hostile", 1, 8192);
    config.filters = vec![el_ballista::config::FilterEntry::Single(
        el_ballista::config::FilterInput::Shorthand("id>1000000".to_string()),
    )];
    let stream = PostgresConnector::from_config(config)?
        .extract()
        .standalone()
        .stream()
        .await?;
    assert_eq!(stream.schema(), expected, "connector stream() schema");
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    db.cleanup().await;
    Ok(())
}

/// `execution.copy_statement_timeout_ms` scopes a statement_timeout to each COPY scan and
/// never leaks into the pooled session. Oracles: the server's own timeout error (57014), then
/// a follow-up scan on the same connector and the direct-SQL row set.
#[tokio::test]
async fn copy_statement_timeout_is_scoped_to_the_copy() -> R {
    let db = TestDb::connect().await;
    let s = &db.schema;
    for sql in [
        format!("CREATE TABLE {s}.slow (id bigint PRIMARY KEY)"),
        // Big enough that a 1 ms COPY cannot finish.
        format!("INSERT INTO {s}.slow SELECT g FROM generate_series(1, 400000) g"),
        format!("ANALYZE {s}.slow"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&db.pool)
            .await?;
    }
    let mut tight = job(&db, "slow", 1, 8192);
    tight.execution.use_copy = true;
    tight.execution.copy_statement_timeout_ms = Some(1);
    let err = PostgresConnector::from_config(tight)?
        .extract()
        .standalone()
        .collect()
        .await
        .expect_err("a 1 ms COPY timeout must fail the scan, not return partial rows");
    let msg = el_ballista::errors::error_chain(&err).join(": ");
    assert!(msg.contains("statement timeout"), "{msg}");

    let mut relaxed = job(&db, "slow", 1, 8192);
    relaxed.execution.use_copy = true;
    let batches = PostgresConnector::from_config(relaxed)?
        .extract()
        .standalone()
        .collect()
        .await?;
    assert_eq!(ids(&batches), source_ids(&db, "slow").await?);
    db.cleanup().await;
    Ok(())
}
