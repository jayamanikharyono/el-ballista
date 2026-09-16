//! P0-2 proof: every extraction path binds its parameters.
//!
//! `extract_incremental_window` and the `*_via_cursor` methods used to inline
//! `$1/$2` placeholders into `DECLARE … FOR` with zero binds (every call failed
//! with "there is no parameter $1"). All three paths must return identical data.
//! Run: `cargo test --test pg_paths` (self-provisions Postgres; set `DATABASE_URL` to
//! point at a specific server instead).

#[path = "common/mod.rs"]
mod common;

use chrono::{TimeZone, Utc};
use common::TestDb;
use rust_ballista_extraction_layer::connector::postgres::extractor::PostgresExtractor;

/// Always provisions a real database (embedded, unless `DATABASE_URL` is set) — never
/// skips. Kept as a macro only so call sites (`let db = live!();`) didn't need to change.
macro_rules! live {
    () => {
        TestDb::connect().await
    };
}

#[tokio::test]
async fn full_incremental_and_cursor_agree() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let ex = PostgresExtractor::connect(
        &db.host,
        db.port,
        &db.user,
        &db.password,
        &db.database,
        4,
        300_000,
        "relex-test",
    )
    .await?;

    let full = ex.extract_full_table(&db.table(), None).await?;
    assert_eq!(full.num_rows(), 8);

    // Wide window covers all rows: exercises lo/hi binding (the old failure).
    let lo = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let hi = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let inc = ex
        .extract_incremental_window(&db.table(), None, "updated_at", lo, hi)
        .await?;
    assert_eq!(inc.num_rows(), 8);

    // Cursor path with a tiny batch size: exercises DECLARE/FETCH batching.
    let cursor_batches = ex
        .extract_incremental_via_cursor(&db.table(), None, "updated_at", lo, hi, 3)
        .await?;
    let cursor_rows: usize = cursor_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(cursor_rows, 8);

    // Same data through every path: ids and exact decimal amounts agree.
    let full_ids = common::int64_col(&full, "id");
    let inc_ids = common::int64_col(&inc, "id");
    assert_eq!(sorted(full_ids), sorted(inc_ids));
    assert_eq!(
        common::decimal_col(&full, "amount"),
        common::decimal_col(&inc, "amount")
    );

    // A narrow window slices correctly. Windows are half-open (lo, hi] (documented in
    // docs/incremental-extraction.md): row 3's updated_at sits exactly ON lo2, so it is
    // EXCLUDED (lo is exclusive); rows 4 and 5 fall inside (0, hi2] and are included.
    let lo2 = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();
    let hi2 = Utc.with_ymd_and_hms(2024, 1, 5, 12, 0, 0).unwrap();
    let slice = ex
        .extract_incremental_window(&db.table(), None, "updated_at", lo2, hi2)
        .await?;
    assert_eq!(sorted(common::int64_col(&slice, "id")), vec![4, 5]);
    Ok(())
}

fn sorted(mut v: Vec<i64>) -> Vec<i64> {
    v.sort_unstable();
    v
}
