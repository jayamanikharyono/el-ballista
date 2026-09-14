//! Extraction edge cases over the hostile fixture.
//!
//! Duplicate timestamps, empty results, batch boundaries, projection, and
//! date/timestamp fidelity — the "key scenarios" half of the testing strategy.
//! Run: `cargo test --test pg_edge` (self-provisions Postgres; set `DATABASE_URL` to
//! point at a specific server instead).

#[path = "common/mod.rs"]
mod common;

use chrono::{NaiveDate, TimeZone, Utc};
use common::TestDb;
use rust_ballista_extraction_layer::connector::postgres::extractor::PostgresExtractor;

/// Always provisions a real database (embedded, unless `DATABASE_URL` is set) — never
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

#[tokio::test]
async fn duplicate_timestamps_all_extracted() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    // Three rows sharing one updated_at: checkpoint windows are (lo, hi], never dedup.
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

    let ex = extractor(&db).await?;
    let lo = Utc.with_ymd_and_hms(2024, 4, 1, 0, 0, 0).unwrap();
    let hi = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
    let batch = ex
        .extract_incremental_window(&db.table(), None, "updated_at", lo, hi)
        .await?;
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(common::int64_col(&batch, "id"), vec![9, 10, 11]);
    Ok(())
}

#[tokio::test]
async fn empty_window_returns_empty_batch() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let ex = extractor(&db).await?;
    let lo = Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap();
    let hi = Utc.with_ymd_and_hms(2030, 2, 1, 0, 0, 0).unwrap();
    let batch = ex
        .extract_incremental_window(&db.table(), None, "updated_at", lo, hi)
        .await?;
    assert_eq!(batch.num_rows(), 0);
    Ok(())
}

#[tokio::test]
async fn cursor_respects_batch_boundaries() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let ex = extractor(&db).await?;
    let lo = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let hi = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    // 8 rows at batch 3 -> [3, 3, 2]: batching cuts where it should, tail included.
    let batches = ex
        .extract_incremental_via_cursor(&db.table(), None, "updated_at", lo, hi, 3)
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
    let names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
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
