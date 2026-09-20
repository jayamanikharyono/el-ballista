//! P0-1 proof: numeric columns decode with exact magnitude end to end.
//!
//! The old code stored `with_scale(s).to_i128()` (truncation: 123.45 → 123);
//! the fix rescales to the unscaled integer (123.45 scale 2 → 12345).
//! Run: `cargo test --test pg_numeric` (requires the compose stack up;
//! `DATABASE_URL` overrides the default endpoint).

#[path = "common/mod.rs"]
mod common;

use common::TestDb;
use rust_ballista_extraction_layer::connector::postgres::extractor::PostgresExtractor;

/// Always uses a real database (the compose stack, unless `DATABASE_URL` is set) — never
/// skips. Kept as a macro only so call sites (`let db = live!();`) didn't need to change.
macro_rules! live {
    () => {
        TestDb::connect().await
    };
}

#[tokio::test]
async fn numeric_columns_decode_exact() -> Result<(), Box<dyn std::error::Error>> {
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

    let batch = ex.extract_full_table(&db.table(), None).await?;
    assert_eq!(batch.num_rows(), 8);

    // amount numeric(12,2): exact unscaled values, NULLs preserved.
    assert_eq!(
        common::decimal_col(&batch, "amount"),
        vec![
            Some(12345),
            Some(-750),
            Some(0),
            Some(9999999999),
            None,
            Some(100),
            Some(4242),
            None,
        ]
    );
    // precise numeric(30,15): 3.141592653589793 -> 3141592653589793.
    let precise = common::decimal_col(&batch, "precise");
    assert_eq!(precise[0], Some(3141592653589793));
    assert_eq!(precise[3], Some(100250000000000000));
    assert_eq!(precise[4], None);
    Ok(())
}
