//! COPY-vs-cursor agreement: `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` returns
//! the identical data as the cursor/`SELECT` paths (differential oracle — the gold
//! standard for this repo). Run: `cargo test --test pg_copy` (requires the compose
//! stack up; `DATABASE_URL` overrides the default endpoint).
//!
//! Every extraction goes through the public connector (`PostgresConnector … standalone()`);
//! the path is chosen by `execution.use_copy`. COPY only runs when no filter is pushed to
//! the source, so every COPY job here pushes nothing (no filters, or pushdown `never`).

#[path = "common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Mutex;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use common::TestDb;
use el_ballista::config::{
    FilterEntry, FilterInput, JobConfig, ParallelScanConfig, ParallelStrategy, PushdownPolicy,
};
use el_ballista::connector::errors::ExtractorError;
use el_ballista::connector::postgres::{PostgresConnector, SplitInfo};
use futures::TryStreamExt;

/// Always uses a real database (the compose stack, unless `DATABASE_URL` is set) — never
/// skips. Kept as a macro only so call sites (`let db = live!();`) didn't need to change.
macro_rules! live {
    () => {
        TestDb::connect().await
    };
}

/// The binary-COPY variant of `config`: `batch_size` rows / `max_batch_bytes` bytes per
/// Arrow batch (tiny caps force multi-batch framing).
fn via_copy(mut config: JobConfig, batch_size: usize, max_batch_bytes: usize) -> JobConfig {
    config.execution.use_copy = true;
    config.execution.batch_size = batch_size;
    config.execution.max_batch_bytes = max_batch_bytes;
    config
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

/// The explicit key range `[lo, hi)` on `id` of `hostile`, as job filters `id>=lo`, `id<hi`.
fn range_job(db: &TestDb, lo: i64, hi: i64) -> JobConfig {
    let mut config = db.extraction_job("hostile", None);
    config.filters = vec![
        FilterEntry::Single(FilterInput::Shorthand(format!("id>={lo}"))),
        FilterEntry::Single(FilterInput::Shorthand(format!("id<{hi}"))),
    ];
    config
}

/// How many of `config`'s filters execute in the source (the rest run in Arrow).
async fn pushed_filters(config: &JobConfig) -> Result<usize, Box<dyn Error>> {
    let decisions = PostgresConnector::from_config(config.clone())?
        .explain_filters()
        .await?;
    assert_eq!(
        decisions.len(),
        config.filters.len(),
        "every filter decided"
    );
    Ok(decisions.iter().filter(|d| d.pushed_to_source).count())
}

/// One split as `run_with` delivered it: its keyset bounds and every batch of its stream.
type DeliveredSplit = (Option<(Option<i64>, Option<i64>)>, Vec<RecordBatch>);

/// Run `config` as a checkpointed job (`run_with`), keeping each split's batches apart, in
/// split order. The checkpoint goes to a private, fresh directory per `label`, so a COPY
/// run and a cursor run of the same job never skip each other's completed splits.
async fn delivered_splits(
    mut config: JobConfig,
    label: &str,
) -> Result<Vec<DeliveredSplit>, Box<dyn Error>> {
    let dir = std::env::temp_dir().join(format!("relex_copy_{}_{label}", config.source.schema));
    let _ = std::fs::remove_dir_all(&dir);
    config.checkpoint.dir = dir.to_string_lossy().to_string();

    let delivered = Mutex::new(BTreeMap::new());
    let sink = &delivered;
    PostgresConnector::from_config(config)?
        .extract()
        .standalone()
        .run_with(move |split: SplitInfo, stream| async move {
            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            let bounds = split.bounds.as_ref().map(|b| (b.lo, b.hi));
            sink.lock()
                .expect("split map")
                .insert(split.index, (bounds, batches));
            Ok(())
        })
        .await?;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(delivered
        .into_inner()
        .expect("split map")
        .into_values()
        .collect())
}

/// The first `ExtractorError` in `err`'s source chain (the typed cause the public API
/// wraps in `AppError` / `DataFusionError::External`).
fn extractor_error<'a>(err: &'a (dyn Error + 'static)) -> Option<&'a ExtractorError> {
    let mut cur = Some(err);
    while let Some(e) = cur {
        if let Some(x) = e.downcast_ref::<ExtractorError>() {
            return Some(x);
        }
        cur = e.source();
    }
    None
}

#[tokio::test]
async fn copy_full_matches_cursor() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();

    // Cursor baseline: the single-batch full extract (concat of cursor batches).
    let cursor = db.extraction_job("hostile", None);
    let expected = common::extract_one(&cursor).await?;
    // COPY with a tiny batch size: exercises multi-batch framing + byte caps.
    let copy = via_copy(cursor, 3, 16 * 1024 * 1024);
    common::assert_scan_path(&copy, "copy").await;
    let actual = common::extract_batches(&copy).await?;

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

    // 1. Explicit ranges: whole range plus a sub-range plus an empty range. Reference: the
    //    cursor with the range pushed into the source query (pushdown `always`). COPY:
    //    pushdown `never`, so nothing is pushed and the binary COPY scan runs; the range is
    //    applied to the COPY rows in Arrow.
    for (lo, hi) in [(0i64, 1_000_000i64), (2, 6), (999_999, 999_999)] {
        let mut cursor = range_job(&db, lo, hi);
        cursor.pushdown.policy = PushdownPolicy::Always;
        let mut copy = via_copy(range_job(&db, lo, hi), 2, 16 * 1024 * 1024);
        copy.pushdown.policy = PushdownPolicy::Never;
        assert_eq!(pushed_filters(&cursor).await?, 2, "range [{lo}, {hi})");
        assert_eq!(pushed_filters(&copy).await?, 0, "range [{lo}, {hi})");
        common::assert_scan_path(&copy, "copy").await;

        let expected = common::extract_one(&cursor).await?;
        let actual_batches = common::extract_batches(&copy).await?;
        let actual_rows: usize = actual_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(actual_rows, expected.num_rows(), "range [{lo}, {hi})");
        assert_eq!(
            rows(&actual_batches),
            rows(std::slice::from_ref(&expected)),
            "range [{lo}, {hi})"
        );
    }

    // 2. Keyset ranges inside the COPY statement itself: keyset splits on the sparse
    //    `big` column (i64 MIN/MAX, 0..13, 42, NULL). Four splits cover the whole key
    //    range, hold sub-ranges, NULL keys (split 0), and at least one empty range — a COPY
    //    stream of header + trailer only. Each COPY split must equal the cursor split with
    //    the same bounds.
    let keyset = |use_copy: bool| {
        let mut config = db.extraction_job("hostile", None);
        config.parallel_scan = ParallelScanConfig {
            strategy: ParallelStrategy::Keyset,
            partitions: 4,
            partition_column: "big".to_string(),
        };
        if use_copy {
            via_copy(config, 2, 16 * 1024 * 1024)
        } else {
            config
        }
    };
    common::assert_scan_path(&keyset(true), "copy").await;
    let expected = delivered_splits(keyset(false), "cursor").await?;
    let actual = delivered_splits(keyset(true), "copy").await?;
    assert_eq!(actual.len(), expected.len(), "same split plan");
    assert!(expected.len() > 1, "the table must actually be split");
    let mut counts = Vec::new();
    for ((copy_bounds, copy_batches), (cursor_bounds, cursor_batches)) in
        actual.iter().zip(&expected)
    {
        assert_eq!(copy_bounds, cursor_bounds, "same split bounds");
        let n: usize = copy_batches.iter().map(|b| b.num_rows()).sum();
        let m: usize = cursor_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, m, "split {copy_bounds:?}");
        assert_eq!(
            rows(copy_batches),
            rows(cursor_batches),
            "split {copy_bounds:?}"
        );
        counts.push(n);
    }
    assert_eq!(
        counts.iter().sum::<usize>(),
        common::HOSTILE_ROWS,
        "{counts:?}"
    );
    assert!(counts.contains(&0), "an empty key range: {counts:?}");
    assert!(
        counts.iter().filter(|&&n| n > 0).count() > 1,
        "non-empty sub-ranges: {counts:?}"
    );
    Ok(())
}

#[tokio::test]
async fn copy_unsupported_type_errors_loudly() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();

    // `point` has no binary decoder (and no cursor decoder either): COPY must fail
    // with UnsupportedType, never silently wrong data.
    let ddl = format!("CREATE TABLE {}.copy_geom (id bigint, p point)", db.schema);
    sqlx::query(sqlx::AssertSqlSafe(ddl.as_str()))
        .execute(&db.pool)
        .await?;
    let config = via_copy(db.extraction_job("copy_geom", None), 8, 1024);
    let err = common::extract_batches(&config)
        .await
        .expect_err("point column must reject COPY");
    assert!(
        matches!(
            extractor_error(&err),
            Some(ExtractorError::UnsupportedType(_))
        ),
        "unexpected error: {:?}",
        el_ballista::errors::error_chain(&err)
    );
    assert!(
        err.to_string().contains("Unsupported"),
        "unexpected error: {err}"
    );
    Ok(())
}

/// A column whose type changes between planning and the scan (`ALTER … TYPE`) fails the COPY
/// with a typed error. Binary COPY rows carry no types, and `bigint` and `double precision`
/// are both 8 bytes: without the describe-and-lock check the scan would decode the double's
/// bits as integers, silently. Oracle: trivial (the typed error, and no rows).
#[tokio::test]
async fn copy_detects_a_type_change_since_planning() -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::prelude::SessionContext;
    use el_ballista::connector::postgres::PostgresTableProvider;

    let db = TestDb::connect().await;
    let s = &db.schema;
    for sql in [
        format!("CREATE TABLE {s}.drift (id bigint, v bigint)"),
        format!("INSERT INTO {s}.drift VALUES (1, 10), (2, 20)"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&db.pool)
            .await?;
    }
    let mut config = db.extraction_job("drift", None);
    config.execution.use_copy = true;
    common::assert_scan_path(&config, "copy").await;

    // Planned while `v` is bigint.
    let provider = PostgresTableProvider::from_config(&config).await?;
    let ctx = SessionContext::new();
    ctx.register_table("drift", std::sync::Arc::new(provider))?;
    let df = ctx.sql("SELECT id, v FROM drift").await?;

    let alter = format!("ALTER TABLE {s}.drift ALTER COLUMN v TYPE double precision");
    sqlx::query(sqlx::AssertSqlSafe(alter.as_str()))
        .execute(&db.pool)
        .await?;

    let err = df
        .collect()
        .await
        .expect_err("a changed column type must fail the COPY, not decode wrong values");
    assert!(
        matches!(
            extractor_error(&err),
            Some(ExtractorError::UnsupportedType(m)) if m.contains("'v'") && m.to_lowercase().contains("float8")
        ),
        "unexpected error: {err}"
    );
    Ok(())
}
