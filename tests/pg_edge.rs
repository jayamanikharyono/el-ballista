//! Extraction edge cases over the hostile fixture.
//!
//! Duplicate timestamps, empty results, batch boundaries, projection, and
//! date/timestamp fidelity — the "key scenarios" half of the testing strategy.
//! Filtered extraction is caller-provided (full scan or explicit predicates);
//! the reference oracle is always direct SQL against the same database.
//! Run: `cargo test --test pg_edge` (requires the compose stack up;
//! `DATABASE_URL` overrides the default endpoint).

#[path = "common/mod.rs"]
mod common;

use chrono::{NaiveDate, TimeZone, Utc};
use common::{TEST_PASSWORD_ENV, TestDb};
use rust_ballista_extraction_layer::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, JobConfig, ParallelScanConfig,
    PushdownConfig, SourceConfig,
};
use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;
use rust_ballista_extraction_layer::connector::postgres::extractor::PostgresExtractor;

/// Always uses a real database (the compose stack, unless `DATABASE_URL` is set) — never
/// skips. Kept as a macro only so call sites (`let db = live!();`) didn't need to change.
macro_rules! live {
    () => {
        TestDb::connect().await
    };
}

async fn extractor(db: &TestDb) -> Result<PostgresExtractor, sqlx::Error> {
    PostgresExtractor::connect(
        &db.host,
        db.port,
        &db.user,
        &db.password,
        &db.database,
        4,
        300_000,
        "relex-test",
    )
    .await
}

fn filtered_job(db: &TestDb, filters: Vec<String>) -> JobConfig {
    use rust_ballista_extraction_layer::config::{FilterEntry, FilterInput};
    let filters = filters
        .into_iter()
        .map(|s| FilterEntry::Single(FilterInput::Shorthand(s)))
        .collect();
    JobConfig {
        job_id: format!("edge-{}", db.schema).parse().unwrap(),
        table: "hostile".to_string(),
        columns: None,
        filters,
        source: SourceConfig {
            host: db.host.clone(),
            port: db.port,
            user: db.user.clone(),
            password_env: TEST_PASSWORD_ENV.to_string(),
            database: db.database.clone(),
            pool_max: 4,
            statement_timeout_ms: 300_000,
            application_name: "relex-test".to_string(),
            schema: db.schema.clone(),
        },
        checkpoint: CheckpointConfig {
            dir: std::env::temp_dir()
                .join(format!("relex_edge_{}", db.schema))
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

#[tokio::test]
async fn duplicate_timestamps_all_extracted() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    // The fixture's 5-row tie (ids 9..13 share one `updated_at`): a filter on the tied
    // timestamp never dedups or truncates — all five rows come back.
    // Oracle: reference (direct SQL over the same predicate) + trivial (the fixture's ids).
    let config = filtered_job(&db, vec!["updated_at=2024-01-09T00:00:00Z".to_string()]);
    let batches = PostgresConnector::from_config(config)
        .expect("valid job config")
        .extract()
        .standalone()
        .collect()
        .await?;
    let mut ids = Vec::new();
    for b in &batches {
        ids.extend(common::int64_col(b, "id"));
    }
    ids.sort_unstable();
    assert_eq!(ids, common::HOSTILE_TIE_IDS.to_vec());

    // Reference oracle: the same ids straight from Postgres.
    let sql = format!(
        "SELECT id FROM {}.hostile WHERE updated_at = '{}' ORDER BY id",
        db.schema,
        common::HOSTILE_TIE_TS
    );
    let rows: Vec<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
        .fetch_all(&db.pool)
        .await?;
    assert_eq!(ids, rows.into_iter().map(|r| r.0).collect::<Vec<_>>());
    Ok(())
}

#[tokio::test]
async fn infinity_table_fails_extraction_on_both_paths() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    // `hostile_infinity` holds ±infinity timestamps/dates, which have no Arrow value: the
    // connector (provider + execution plan) must return an error naming the column on both
    // the cursor and the COPY path — never rows, never a panic.
    // Oracle: trivial (a typed error is the expected outcome by design).
    for column in ["ts", "naive", "day"] {
        for use_copy in [false, true] {
            let mut config = filtered_job(&db, vec![]);
            config.table = "hostile_infinity".to_string();
            config.columns = Some(vec!["id".to_string(), column.to_string()]);
            config.execution.use_copy = use_copy;
            let result = PostgresConnector::from_config(config)?
                .extract()
                .standalone()
                .collect()
                .await;
            match result {
                Ok(batches) => panic!(
                    "{column} copy={use_copy}: expected an error, got {} rows",
                    batches.iter().map(|b| b.num_rows()).sum::<usize>()
                ),
                Err(e) => {
                    let chain = error_chain(&e);
                    assert!(
                        chain.contains(&format!("column '{column}'")) && chain.contains("infinity"),
                        "{column} copy={use_copy}: error must name the column and the value: {chain}"
                    );
                }
            }
        }
    }
    Ok(())
}

/// The error and all its `source()`s, joined (the typed decode error sits under the
/// DataFusion / application wrappers).
fn error_chain(e: &(dyn std::error::Error + 'static)) -> String {
    let mut out = e.to_string();
    let mut cur = e.source();
    while let Some(s) = cur {
        out.push_str(" <- ");
        out.push_str(&s.to_string());
        cur = s.source();
    }
    out
}

#[tokio::test]
async fn bytea_round_trips_exactly() -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::{Array, BinaryArray};
    let db = live!();
    let ex = extractor(&db).await?;
    // bytea -> Binary: 0x00 / 0xFF bytes, empty ('' vs NULL) and NULL survive unchanged.
    // Oracle: reference (Postgres' own hex encoding of each value).
    let reference: Vec<(i64, Option<String>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, encode(bin, 'hex') FROM {} ORDER BY id",
        db.table()
    )))
    .fetch_all(&db.pool)
    .await?;
    let batch = common::sorted_by(
        &ex.extract_full_table(&db.table(), Some(vec!["id", "bin"]))
            .await?,
        "id",
    );
    let bin = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("bytea maps to Binary");
    let got: Vec<(i64, Option<String>)> = common::int64_col(&batch, "id")
        .into_iter()
        .enumerate()
        .map(|(i, id)| {
            let hex = bin.is_valid(i).then(|| {
                bin.value(i)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            });
            (id, hex)
        })
        .collect();
    assert_eq!(got, reference);
    assert!(
        got.iter().any(|(_, v)| v.as_deref() == Some("")),
        "empty bytea covered"
    );
    assert!(got.iter().any(|(_, v)| v.is_none()), "NULL bytea covered");
    Ok(())
}

#[tokio::test]
async fn empty_filter_returns_empty_stream_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{SessionContext, col, lit};
    use futures::StreamExt;
    use rust_ballista_extraction_layer::connector::postgres::distributed::connection::PostgresConnectionDescriptor;
    use rust_ballista_extraction_layer::connector::postgres::table_provider::PostgresTableProvider;
    use rust_ballista_extraction_layer::pushdown::cost_model::CostParams;
    use std::sync::Arc;

    let db = live!();
    // Oracle: a filter matching nothing yields zero rows, but the stream still
    // carries the table schema (`collect()` returns zero batches for empty
    // results, so the schema must be read from the stream, not a batch).
    let config = filtered_job(&db, vec![]);
    let state = SessionStateBuilder::new().with_default_features().build();
    let ctx = SessionContext::new_with_state(state);
    let descriptor = PostgresConnectionDescriptor::from_config(&config.source, 1);
    let provider = PostgresTableProvider::new(
        descriptor,
        &config.resolved_table(),
        config.pushdown.policy,
        config.pushdown.deny.clone(),
        config.pushdown.push.clone(),
        CostParams {
            max_source_cost: config.pushdown.max_source_cost,
            keep_threshold: config.pushdown.keep_threshold,
        },
        config.pushdown.statistics_ttl_secs,
        config.execution.batch_size,
    )
    .await
    .map_err(|e| format!("provider: {e}"))?;
    ctx.register_table("hostile", Arc::new(provider))
        .map_err(|e| format!("register: {e}"))?;
    let df = ctx
        .table("hostile")
        .await
        .map_err(|e| format!("table: {e}"))?
        .filter(col("id").gt(lit(1_000_000i64)))
        .map_err(|e| format!("filter: {e}"))?;

    let mut stream = df
        .execute_stream()
        .await
        .map_err(|e| format!("stream: {e}"))?;
    assert!(
        stream.schema().index_of("id").is_ok(),
        "empty result still carries the schema"
    );
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        rows += batch.map_err(|e| format!("batch: {e}"))?.num_rows();
    }
    assert_eq!(rows, 0);
    Ok(())
}

#[tokio::test]
async fn cursor_respects_batch_boundaries() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let ex = extractor(&db).await?;
    // 13 rows at batch 3 -> [3, 3, 3, 3, 1]: batching cuts where it should, tail included.
    // Keyset partition scan over the whole id space exercises the cursor/FETCH batching.
    let batches = ex
        .extract_keyset_partition_via_cursor(&db.table(), None, "id", 0, 1_000_000, 3)
        .await?;
    let sizes: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
    assert_eq!(sizes, vec![3, 3, 3, 3, 1]);
    assert_eq!(sizes.iter().sum::<usize>(), common::HOSTILE_ROWS);
    Ok(())
}

#[tokio::test]
async fn projection_returns_requested_columns() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let ex = extractor(&db).await?;
    // No ORDER BY in extraction: sort by id before asserting per-row positions.
    let batch = common::sorted_by(
        &ex.extract_full_table(&db.table(), Some(vec!["id", "amount"]))
            .await?,
        "id",
    );
    assert_eq!(batch.num_rows(), common::HOSTILE_ROWS);
    let schema = batch.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "amount"]);
    assert_eq!(
        common::decimal_col(&batch, "amount")[0],
        Some(12345),
        "projection must not disturb decoding"
    );
    Ok(())
}

#[tokio::test]
async fn dates_and_timestamps_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::Array;
    let db = live!();
    let ex = extractor(&db).await?;
    // No ORDER BY in extraction: sort by id before asserting per-row positions.
    let batch = common::sorted_by(&ex.extract_full_table(&db.table(), None).await?, "id");

    // date -> Date32 days since unix epoch (2024-02-29, leap day included).
    let idx = batch.schema().index_of("day").unwrap();
    let days = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::Date32Array>()
        .unwrap();
    let expected_days = NaiveDate::from_ymd_opt(2024, 2, 29)
        .unwrap()
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
    assert!(days.is_valid(0));
    assert_eq!(days.value(0), expected_days);
    assert!(!days.is_valid(1), "NULL date stays NULL");

    // timestamptz -> micros instant: '2024-03-01 12:00:00+02' == 10:00Z.
    let idx = batch.schema().index_of("ts").unwrap();
    let ts = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .unwrap();
    let expected_micros = Utc
        .with_ymd_and_hms(2024, 3, 1, 10, 0, 0)
        .unwrap()
        .timestamp_micros();
    assert_eq!(ts.value(0), expected_micros);
    assert!(!ts.is_valid(1));

    // naive timestamp pinned to UTC (matches SET TIME ZONE 'UTC' session hygiene).
    let idx = batch.schema().index_of("naive").unwrap();
    let naive = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .unwrap();
    let expected_naive = Utc
        .with_ymd_and_hms(2024, 3, 1, 12, 0, 0)
        .unwrap()
        .timestamp_micros();
    assert_eq!(naive.value(0), expected_naive);

    // Every row, including the extremes (epoch - 1 µs, 2038-01-19 03:14:07/08,
    // 9999-12-31 23:59:59.999999, 1970-01-01, the leap day): Unix µs / days exactly as
    // Postgres computes them. Oracle: reference (`extract(epoch …)` in Postgres).
    let reference: Vec<(Option<i64>, Option<i64>, Option<i32>)> =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT (extract(epoch FROM ts) * 1000000)::bigint,
                    (extract(epoch FROM naive) * 1000000)::bigint,
                    (day - DATE '1970-01-01')::int
             FROM {} ORDER BY id",
            db.table()
        )))
        .fetch_all(&db.pool)
        .await?;
    let got: Vec<(Option<i64>, Option<i64>, Option<i32>)> = (0..batch.num_rows())
        .map(|i| {
            (
                ts.is_valid(i).then(|| ts.value(i)),
                naive.is_valid(i).then(|| naive.value(i)),
                days.is_valid(i).then(|| days.value(i)),
            )
        })
        .collect();
    assert_eq!(got, reference);
    let max_micros = Utc
        .with_ymd_and_hms(9999, 12, 31, 23, 59, 59)
        .unwrap()
        .timestamp_micros()
        + 999_999;
    assert!(
        got.iter().any(|r| r.0 == Some(max_micros)),
        "9999-12-31 23:59:59.999999 covered"
    );
    Ok(())
}
