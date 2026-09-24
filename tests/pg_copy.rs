//! COPY-vs-cursor agreement: `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` returns
//! the identical data as the cursor/`SELECT` paths (differential oracle — the gold
//! standard for this repo). Run: `cargo test --test pg_copy` (requires the compose
//! stack up; `DATABASE_URL` overrides the default endpoint).

#[path = "common/mod.rs"]
mod common;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use common::TestDb;
use rust_ballista_extraction_layer::connector::postgres::extractor::PostgresExtractor;

/// Always uses a real database (the compose stack, unless `DATABASE_URL` is set) — never
/// skips. Kept as a macro only so call sites (`let db = live!();`) didn't need to change.
macro_rules! live {
    () => {
        TestDb::connect().await
    };
}

async fn extractor(db: &TestDb) -> PostgresExtractor {
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
    .expect("connect extractor")
}

/// Collect COPY batches for a full scan (tiny batch size forces multi-batch framing).
async fn copy_full(ex: &PostgresExtractor, table: &str, batch_size: usize) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    ex.extract_full_table_via_copy_for_each_batch(
        table,
        None,
        batch_size,
        16 * 1024 * 1024,
        &mut |b| {
            batches.push(b);
            Ok(())
        },
    )
    .await
    .expect("COPY full scan");
    batches
}

fn sorted<T: Ord>(mut v: Vec<T>) -> Vec<T> {
    v.sort();
    v
}

fn string_col(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).expect("column exists");
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("string column");
        out.extend((0..arr.len()).map(|i| arr.is_valid(i).then(|| arr.value(i).to_string())));
    }
    out
}

fn f64_bits(batches: &[RecordBatch], name: &str) -> Vec<u64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).expect("column exists");
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("f64 column");
        out.extend(
            (0..arr.len())
                .filter(|&i| arr.is_valid(i))
                .map(|i| arr.value(i).to_bits()),
        );
    }
    out
}

fn ts_micros(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).expect("column exists");
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .expect("timestamp column");
        out.extend((0..arr.len()).map(|i| arr.is_valid(i).then(|| arr.value(i))));
    }
    out
}

fn bool_col(batches: &[RecordBatch], name: &str) -> Vec<Option<bool>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).expect("column exists");
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .expect("bool column");
        out.extend((0..arr.len()).map(|i| arr.is_valid(i).then(|| arr.value(i))));
    }
    out
}

fn decimals(batches: &[RecordBatch], name: &str) -> Vec<Option<i128>> {
    batches
        .iter()
        .flat_map(|b| common::decimal_col(b, name))
        .collect()
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        out.extend(common::int64_col(b, "id"));
    }
    out
}

#[tokio::test]
async fn copy_full_matches_cursor() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let ex = extractor(&db).await;

    // Cursor baseline: the legacy single-batch full extract (concat of cursor batches).
    let expected = ex.extract_full_table(&db.table(), None).await?;
    // COPY with a tiny batch size: exercises multi-batch framing + byte caps.
    let actual = copy_full(&ex, &db.table(), 3).await;

    let actual_rows: usize = actual.iter().map(|b| b.num_rows()).sum();
    assert_eq!(actual_rows, expected.num_rows());
    assert_eq!(expected.num_rows(), 8);
    for b in &actual {
        assert_eq!(b.schema(), expected.schema(), "schema must match exactly");
    }

    // Every hostile edge, COPY vs cursor: ids, exact decimals (incl. NULLs),
    // unicode/empty text, NaN/-Infinity float bits, timestamps, bools.
    assert_eq!(
        sorted(ids(&actual)),
        sorted(common::int64_col(&expected, "id"))
    );
    assert_eq!(
        sorted(decimals(&actual, "amount")),
        sorted(decimals(std::slice::from_ref(&expected), "amount"))
    );
    assert_eq!(
        sorted(decimals(&actual, "precise")),
        sorted(decimals(std::slice::from_ref(&expected), "precise"))
    );
    assert_eq!(
        sorted(string_col(&actual, "name")),
        sorted(string_col(std::slice::from_ref(&expected), "name"))
    );
    assert_eq!(
        sorted(string_col(&actual, "nick")),
        sorted(string_col(std::slice::from_ref(&expected), "nick"))
    );
    assert_eq!(
        sorted(f64_bits(&actual, "ratio")),
        sorted(f64_bits(std::slice::from_ref(&expected), "ratio"))
    );
    assert_eq!(
        sorted(ts_micros(&actual, "ts")),
        sorted(ts_micros(std::slice::from_ref(&expected), "ts"))
    );
    assert_eq!(
        sorted(bool_col(&actual, "flag")),
        sorted(bool_col(std::slice::from_ref(&expected), "flag"))
    );
    Ok(())
}

#[tokio::test]
async fn copy_keyset_matches_cursor() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let ex = extractor(&db).await;

    // Whole range plus a sub-range plus an empty range (header + trailer only).
    for (lo, hi) in [(0i64, 1_000_000i64), (2, 6), (999_999, 999_999)] {
        let expected = ex
            .extract_keyset_partition(&db.table(), None, "id", lo, hi)
            .await?;
        let mut actual_batches = Vec::new();
        ex.extract_keyset_partition_via_copy_for_each_batch(
            &db.table(),
            None,
            "id",
            lo,
            hi,
            2,
            16 * 1024 * 1024,
            &mut |b| {
                actual_batches.push(b);
                Ok(())
            },
        )
        .await?;
        let actual_rows: usize = actual_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(actual_rows, expected.num_rows(), "range [{lo}, {hi})");
        assert_eq!(
            sorted(ids(&actual_batches)),
            sorted(common::int64_col(&expected, "id")),
            "range [{lo}, {hi})"
        );
        assert_eq!(
            sorted(decimals(&actual_batches, "precise")),
            sorted(decimals(std::slice::from_ref(&expected), "precise")),
            "range [{lo}, {hi})"
        );
    }
    Ok(())
}

#[tokio::test]
async fn copy_unsupported_type_errors_loudly() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let ex = extractor(&db).await;

    // `point` has no binary decoder (and no cursor decoder either): COPY must fail
    // with UnsupportedType, never silently wrong data.
    let ddl = format!("CREATE TABLE {}.copy_geom (id bigint, p point)", db.schema);
    sqlx::query(sqlx::AssertSqlSafe(ddl.as_str()))
        .execute(&db.pool)
        .await?;
    let table = format!("{}.copy_geom", db.schema);
    let err = ex
        .extract_full_table_via_copy_for_each_batch(&table, None, 8, 1024, &mut |_| Ok(()))
        .await
        .expect_err("point column must reject COPY");
    assert!(
        err.to_string().contains("Unsupported"),
        "unexpected error: {err}"
    );
    Ok(())
}
