//! COPY-vs-cursor agreement: `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` returns
//! the identical data as the cursor/`SELECT` paths (differential oracle — the gold
//! standard for this repo). Run: `cargo test --test pg_copy` (requires the compose
//! stack up; `DATABASE_URL` overrides the default endpoint).

#[path = "common/mod.rs"]
mod common;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use common::TestDb;
use el_ballista::connector::postgres::extractor::PostgresExtractor;

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

/// Every row as its full, displayed tuple (all columns, including `meta` jsonb, `uid`,
/// `tags`, dates and timestamps), sorted by the rendered row. Comparing whole rows — not
/// per-column sorted vectors — catches a row-permutation bug (values from different
/// source rows glued together), which per-column sorting cannot see.
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

fn column_names(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

#[tokio::test]
async fn copy_full_matches_cursor() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let ex = extractor(&db).await;

    // Cursor baseline: the single-batch full extract (concat of cursor batches).
    let expected = ex.extract_full_table(&db.table(), None).await?;
    // COPY with a tiny batch size: exercises multi-batch framing + byte caps.
    let actual = copy_full(&ex, &db.table(), 3).await;

    let actual_rows: usize = actual.iter().map(|b| b.num_rows()).sum();
    assert_eq!(actual_rows, expected.num_rows());
    assert_eq!(expected.num_rows(), common::HOSTILE_ROWS);
    for b in &actual {
        assert_eq!(b.schema(), expected.schema(), "schema must match exactly");
    }
    // The comparison below must actually cover the json/jsonb/uuid/array columns.
    let names = column_names(&expected);
    for c in [
        "meta", "uid", "tags", "day", "ts", "naive", "feeling", "amount", "bin",
    ] {
        assert!(names.iter().any(|n| n == c), "fixture lost column {c}");
    }

    // Whole-row differential oracle, every hostile edge at once.
    let expected_rows = rows(std::slice::from_ref(&expected));
    assert_eq!(rows(&actual), expected_rows);

    // jsonb is Postgres' own `::text` rendering on both paths (not a serde round trip).
    let meta: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT meta::text FROM {} WHERE meta IS NOT NULL ORDER BY id",
        db.table()
    )))
    .fetch_all(&db.pool)
    .await?;
    let meta_idx = expected.schema().index_of("meta")?;
    let got: Vec<String> = {
        let ids = common::int64_col(&expected, "id");
        let arr = expected
            .column(meta_idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("meta is Utf8");
        let mut pairs: Vec<(i64, String)> = (0..arr.len())
            .filter(|&i| arr.is_valid(i))
            .map(|i| (ids[i], arr.value(i).to_string()))
            .collect();
        pairs.sort();
        pairs.into_iter().map(|(_, v)| v).collect()
    };
    assert_eq!(got, meta, "jsonb must equal Postgres' text rendering");
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
            rows(&actual_batches),
            rows(std::slice::from_ref(&expected)),
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
