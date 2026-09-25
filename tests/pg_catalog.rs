//! Devil's-advocate coverage for DB-facing functions that cannot be unit-tested without a
//! live Postgres: catalog statistics (pg_stats/pg_index/pg_enum) and EXPLAIN-based cost
//! estimation. Since `TestDb::connect()` now always provisions a real database, there is
//! no reason for these to stay untested.
//!
//! Run: `cargo test --test pg_catalog` (requires the compose stack up;
//! `DATABASE_URL` overrides the default endpoint).

#[path = "common/mod.rs"]
mod common;

use common::TestDb;
use rust_ballista_extraction_layer::connector::postgres::explain::ExplainEstimator;
use rust_ballista_extraction_layer::pushdown::stats::TableStatsSource;
use std::sync::Arc;

/// Always uses a real database (the compose stack, unless `DATABASE_URL` is set) — never skips.
macro_rules! live {
    () => {
        TestDb::connect().await
    };
}

#[tokio::test]
async fn table_statistics_reports_real_row_and_column_data()
-> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let stats = db.pool.table_statistics(&db.schema, "hostile").await?;
    assert_eq!(stats.table_name, "hostile");
    // The fixture inserts `HOSTILE_ROWS` rows and runs ANALYZE, so pg_class.reltuples
    // should reflect that (not the empty-stats zero this function reports for an
    // unreachable source). ANALYZE on a table this small samples every row, so reltuples
    // is exact.
    assert_eq!(
        stats.row_count_estimate,
        common::HOSTILE_ROWS as f64,
        "expected the fixture's {} rows after ANALYZE",
        common::HOSTILE_ROWS
    );
    // pg_total_relation_size: at least one 8 KiB heap page, and nowhere near a MiB for a
    // handful of rows.
    assert!(
        (8192..1024 * 1024).contains(&stats.table_size_bytes),
        "implausible size for a {}-row table: {}",
        common::HOSTILE_ROWS,
        stats.table_size_bytes
    );
    assert!(
        !stats.columns.is_empty(),
        "expected pg_stats rows for at least one column"
    );
    // The primary key column should show up with a highly distinct value.
    let id_stats = stats.columns.get("id").expect("id column stats present");
    assert_eq!(id_stats.null_frac, 0.0, "id is never null in the fixture");
    Ok(())
}

#[tokio::test]
async fn table_statistics_unknown_table_returns_zeroed_stats()
-> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    // Current behaviour, pinned: a typo'd table name is NOT an error — it returns a zeroed,
    // column-less result the caller can distinguish from a real (analyzed) table. If this is
    // changed to an error, rename the test and flip the assertions.
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
    // Known fixture: 13 analyzed rows, `id` is the primary key → the planner expects ~1 row
    // (never more than the table), at a small but nonzero cost.
    let total_cost = estimate.total_cost.expect("EXPLAIN reports Total Cost");
    let plan_rows = estimate.plan_rows.expect("EXPLAIN reports Plan Rows");
    assert!(
        total_cost > 0.0 && total_cost < 100.0,
        "implausible cost for a PK lookup on 13 rows: {total_cost}"
    );
    assert!(
        (1.0..=common::HOSTILE_ROWS as f64).contains(&plan_rows),
        "implausible row estimate for id = 1: {plan_rows}"
    );
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
