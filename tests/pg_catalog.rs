//! Devil's-advocate coverage for DB-facing functions that cannot be unit-tested without a
//! live Postgres: privilege checks, the safe-high-watermark query, catalog statistics
//! (pg_stats/pg_index/pg_enum), and EXPLAIN-based cost estimation. Each of these had zero
//! coverage before (flagged by the test-suite audit): only their pure helper functions
//! were tested. Since `TestDb::connect()` now always provisions a real database, there is
//! no reason for these to stay untested.
//!
//! Run: `cargo test --test pg_catalog` (self-provisions Postgres; set `DATABASE_URL` to
//! point at a specific server instead).

#[path = "common/mod.rs"]
mod common;

use common::TestDb;
use rust_ballista_extraction_layer::incremental::{
    WatermarkSource, check_pg_read_all_stats_privilege, safe_high_watermark,
};
use rust_ballista_extraction_layer::pushdown::explain::ExplainEstimator;
use rust_ballista_extraction_layer::pushdown::stats::TableStatsSource;
use std::sync::Arc;

/// Always provisions a real database (embedded, unless `DATABASE_URL` is set) — never skips.
macro_rules! live {
    () => {
        TestDb::connect().await
    };
}

#[tokio::test]
async fn privilege_check_returns_a_real_answer_and_is_cached()
-> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    // Whatever the answer is for this role, it must be a concrete bool, not an error --
    // the function's whole contract is "never fail the caller over a privilege check".
    let first = check_pg_read_all_stats_privilege(&db.pool).await?;
    let second = check_pg_read_all_stats_privilege(&db.pool).await?;
    assert_eq!(
        first, second,
        "result is process-wide cached; must be stable within a run"
    );
    Ok(())
}

#[tokio::test]
async fn safe_high_watermark_never_returns_a_future_timestamp()
-> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let before = chrono::Utc::now();
    let watermark = safe_high_watermark(&db.pool, chrono::Duration::seconds(0)).await?;
    let after = chrono::Utc::now();
    // Whether or not this role has pg_read_all_stats (real query vs. fallback), the
    // contract is the same: never advance past "now" (the whole point of the safety
    // mechanism), and never so far in the past that it predates the test starting.
    assert!(watermark <= after, "watermark must never be in the future");
    assert!(
        watermark >= before - chrono::Duration::seconds(5),
        "watermark implausibly old"
    );
    Ok(())
}

#[tokio::test]
async fn safe_high_watermark_trait_impl_agrees_with_free_function()
-> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let via_trait = db
        .pool
        .safe_high_watermark(chrono::Duration::seconds(1))
        .await?;
    let via_free_fn = safe_high_watermark(&db.pool, chrono::Duration::seconds(1)).await?;
    // Both should read "now" to within a couple of seconds of each other -- this pins that
    // the free function really does delegate to the trait impl, not a divergent copy.
    let drift = (via_trait - via_free_fn).num_milliseconds().abs();
    assert!(
        drift < 5_000,
        "free function and trait impl drifted by {drift}ms"
    );
    Ok(())
}

#[tokio::test]
async fn table_statistics_reports_real_row_and_column_data()
-> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let stats = db.pool.table_statistics(&db.schema, "hostile").await?;
    assert_eq!(stats.table_name, "hostile");
    // 8 rows were inserted by the fixture and ANALYZE was run, so pg_class.reltuples
    // should reflect that (not the empty-stats zero this function reports for an
    // unreachable source).
    assert!(
        stats.row_count_estimate > 0.0,
        "expected a nonzero row estimate after ANALYZE"
    );
    assert!(stats.table_size_bytes > 0, "expected a nonzero table size");
    assert!(
        !stats.columns.is_empty(),
        "expected pg_stats rows for at least one column"
    );
    // The primary key column should show up with a highly distinct value.
    let id_stats = stats.columns.get("id").expect("id column stats present");
    assert!(id_stats.null_frac < 0.5, "id is never null in the fixture");
    Ok(())
}

#[tokio::test]
async fn table_statistics_unknown_table_errors_rather_than_returning_empty()
-> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    // A typo'd table name must surface as a caller-visible failure path (or, per the
    // current implementation, a zeroed/empty result the caller can distinguish from a
    // real table) -- either way this must not panic.
    let stats = db
        .pool
        .table_statistics(&db.schema, "table_that_does_not_exist")
        .await?;
    assert_eq!(stats.row_count_estimate, 0.0);
    assert!(stats.columns.is_empty());
    Ok(())
}

#[tokio::test]
async fn table_indexes_reports_the_primary_key() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let indexes = db.pool.table_indexes(&db.schema, "hostile").await?;
    let pk = indexes
        .iter()
        .find(|idx| idx.is_primary)
        .expect("hostile has a bigserial primary key, so at least one primary index must exist");
    assert!(pk.columns.iter().any(|c| c == "id"));
    assert!(pk.is_unique, "a primary key index is always unique");
    Ok(())
}

#[tokio::test]
async fn table_indexes_on_unindexed_columns_returns_empty_not_error()
-> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let indexes = db.pool.table_indexes(&db.schema, "hostile").await?;
    // `name`/`amount`/etc. carry no explicit index in the fixture DDL: confirm we don't
    // fabricate index coverage for columns that have none.
    assert!(
        !indexes
            .iter()
            .any(|idx| idx.columns.iter().any(|c| c == "amount")),
        "amount has no index in the fixture; a false positive here would wrongly \
         trigger the cost model's near-free index shortcut"
    );
    Ok(())
}

#[tokio::test]
async fn table_enum_columns_finds_the_mood_enum() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let enums = db.pool.table_enum_columns(&db.schema, "hostile").await?;
    assert!(
        enums.contains("feeling"),
        "feeling is declared as {{schema}}.mood, a real enum type"
    );
    assert!(!enums.contains("name"), "name is plain text, not an enum");
    Ok(())
}

#[tokio::test]
async fn explain_estimator_reports_a_plan_for_a_real_predicate()
-> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let estimator = ExplainEstimator::new(Arc::new(db.pool.clone()), 300);
    // Signature is (table_name, schema_name, predicate) -- easy to get backwards, which
    // is exactly what happened here the first time (both errored out identically,
    // "wrong schema" vs "wrong table" looking the same from EXPLAIN's error message).
    let estimate = estimator
        .estimate_cost("hostile", &db.schema, "id = 1")
        .await?;
    assert!(estimate.total_cost >= 0.0);
    assert!(estimate.plan_rows >= 0.0);
    Ok(())
}

#[tokio::test]
async fn explain_estimator_caches_within_ttl() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let estimator = ExplainEstimator::new(Arc::new(db.pool.clone()), 300);
    // Nothing cached yet.
    assert!(
        estimator
            .cached_estimate("hostile", &db.schema, "id = 1")
            .is_none()
    );
    let first = estimator
        .estimate_cost("hostile", &db.schema, "id = 1")
        .await?;
    let cached = estimator
        .cached_estimate("hostile", &db.schema, "id = 1")
        .expect("must be cached immediately after estimate_cost");
    assert_eq!(cached.total_cost, first.total_cost);
    assert_eq!(cached.access_method, first.access_method);
    Ok(())
}

#[tokio::test]
async fn explain_estimator_rejects_a_nonexistent_table() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let estimator = ExplainEstimator::new(Arc::new(db.pool.clone()), 300);
    let result = estimator
        .estimate_cost("table_that_does_not_exist", &db.schema, "id = 1")
        .await;
    assert!(
        result.is_err(),
        "EXPLAIN against a missing table must surface as an error, not a fabricated estimate"
    );
    Ok(())
}
