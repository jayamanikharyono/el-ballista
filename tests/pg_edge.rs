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
    PushdownConfig, SinkConfig, SourceConfig,
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
        job_id: format!("edge-{}", db.schema),
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
        sink: SinkConfig {
            path: "/tmp/relex_test_sink".to_string(),
        },
        checkpoint: CheckpointConfig {
            dir: std::env::temp_dir()
                .join(format!("relex_edge_{}", db.schema))
                .to_string_lossy()
                .to_string(),
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
    // Three rows sharing one updated_at: filtered extraction never dedups —
    // every row in the range comes back. Oracle: direct SQL over the same range.
    // Schema name is harness-generated (test_<pid>_<n>); audited static shape.
    let sql = format!(
        "INSERT INTO {}.hostile (name, updated_at) VALUES
         ('d1', '2024-05-01 00:00:00+00'),
         ('d2', '2024-05-01 00:00:00+00'),
         ('d3', '2024-05-01 00:00:00+00')",
        db.schema
    );
    sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
        .execute(&db.pool)
        .await
        .map_err(|e| format!("seed dups: {e}"))?;

    let config = filtered_job(&db, vec!["id>8".to_string()]);
    let batches = PostgresConnector::from_config(config)
        .extract()
        .standalone()
        .collect()
        .await?;
    let mut ids = Vec::new();
    for b in &batches {
        ids.extend(common::int64_col(b, "id"));
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![9, 10, 11]);

    // Reference oracle: the same ids straight from Postgres.
    let sql = format!(
        "SELECT id FROM {}.hostile WHERE id > 8 ORDER BY id",
        db.schema
    );
    let rows: Vec<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
        .fetch_all(&db.pool)
        .await?;
    assert_eq!(ids, rows.into_iter().map(|r| r.0).collect::<Vec<_>>());
    Ok(())
}

#[tokio::test]
async fn empty_filter_returns_empty_stream_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{SessionContext, col, lit};
    use futures::StreamExt;
    use rust_ballista_extraction_layer::connector::postgres::table_provider::PostgresTableProvider;
    use rust_ballista_extraction_layer::distributed::connection::PostgresConnectionDescriptor;
    use rust_ballista_extraction_layer::pushdown::PushdownPolicy;
    use rust_ballista_extraction_layer::pushdown::cost_model::CostParams;
    use rust_ballista_extraction_layer::pushdown::optimizer_rule::SourceAwarePushdownRule;
    use std::sync::Arc;

    let db = live!();
    // Oracle: a filter matching nothing yields zero rows, but the stream still
    // carries the table schema (`collect()` returns zero batches for empty
    // results, so the schema must be read from the stream, not a batch).
    let config = filtered_job(&db, vec![]);
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_optimizer_rule(Arc::new(SourceAwarePushdownRule))
        .build();
    let ctx = SessionContext::new_with_state(state);
    let descriptor = PostgresConnectionDescriptor::from_config(&config.source, 1);
    let provider = PostgresTableProvider::new(
        descriptor,
        &config.resolved_table(),
        PushdownPolicy::parse(&config.pushdown.policy),
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
    // 8 rows at batch 3 -> [3, 3, 2]: batching cuts where it should, tail included.
    // Keyset partition scan over the whole id space exercises the cursor/FETCH batching.
    let batches = ex
        .extract_keyset_partition_via_cursor(&db.table(), None, "id", 0, 1_000_000, 3)
        .await?;
    let sizes: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
    assert_eq!(sizes, vec![3, 3, 2]);
    Ok(())
}

#[tokio::test]
async fn projection_returns_requested_columns() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let ex = extractor(&db).await?;
    let batch = ex
        .extract_full_table(&db.table(), Some(vec!["id", "amount"]))
        .await?;
    assert_eq!(batch.num_rows(), 8);
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
    let batch = ex.extract_full_table(&db.table(), None).await?;

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
    Ok(())
}
