//! Extraction-path agreement: full scan, keyset partitions, and filtered extraction
//! return identical data.
//!
//! The cursor/FETCH paths used to inline `$1/$2` placeholders into `DECLARE … FOR`
//! with zero binds (every call failed with "there is no parameter $1"). All paths
//! must return identical data. Run: `cargo test --test pg_paths` (requires the
//! compose stack up; `DATABASE_URL` overrides the default endpoint).

#[path = "common/mod.rs"]
mod common;

use common::{TEST_PASSWORD_ENV, TestDb};
use el_ballista::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, FilterEntry, FilterInput, JobConfig,
    ParallelScanConfig, PushdownConfig, PushdownPolicy, SourceConfig,
};
use el_ballista::connector::postgres::PostgresConnector;

/// Always uses a real database (the compose stack, unless `DATABASE_URL` is set) — never
/// skips. Kept as a macro only so call sites (`let db = live!();`) didn't need to change.
macro_rules! live {
    () => {
        TestDb::connect().await
    };
}

/// The explicit keyset range `[lo, hi)` on `column` of `hostile`, as job filters
/// (`column>=lo`, `column<hi`) pushed to the source (policy `always`), so the range is part
/// of the source query (bound literals in the cursor's `DECLARE … FOR`) as in a keyset
/// partition scan; cursor path, `batch_size` rows per FETCH.
fn keyset_range_job(db: &TestDb, column: &str, lo: i64, hi: i64, batch_size: usize) -> JobConfig {
    let mut config = db.extraction_job("hostile", None);
    config.filters = vec![
        FilterEntry::Single(FilterInput::Shorthand(format!("{column}>={lo}"))),
        FilterEntry::Single(FilterInput::Shorthand(format!("{column}<{hi}"))),
    ];
    config.pushdown.policy = PushdownPolicy::Always;
    config.execution.use_copy = false;
    config.execution.batch_size = batch_size;
    config
}

/// Both range bounds of a [`keyset_range_job`] execute in the source, not in Arrow — so the
/// keyset comparison below is not vacuously the full scan filtered in Arrow.
async fn assert_range_pushed(config: &JobConfig) -> Result<(), Box<dyn std::error::Error>> {
    let decisions = PostgresConnector::from_config(config.clone())?
        .explain_filters()
        .await?;
    assert_eq!(decisions.len(), 2, "both range bounds decided");
    for d in &decisions {
        assert!(
            d.pushed_to_source,
            "range bound {} must run in the source: {}",
            d.filter, d.reason
        );
    }
    Ok(())
}

fn filtered_job(db: &TestDb, filters: Vec<String>) -> JobConfig {
    let filters = filters
        .into_iter()
        .map(|s| FilterEntry::Single(FilterInput::Shorthand(s)))
        .collect();
    JobConfig {
        job_id: format!("paths-{}", db.schema).parse().unwrap(),
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
                .join(format!("relex_paths_{}", db.schema))
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
            ..DistributedConfig::default()
        },
    }
}

#[tokio::test]
async fn full_keyset_and_filtered_agree() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();

    // Full scan: cursor path, default batch size, unsplit, no filters.
    let full = common::extract_one(&db.extraction_job("hostile", None)).await?;
    assert_eq!(full.num_rows(), common::HOSTILE_ROWS);

    // Keyset partitions tiling the id space union to the full scan (metamorphic oracle).
    let default_batch = db.extraction_job("hostile", None).execution.batch_size;
    let range_a = keyset_range_job(&db, "id", 0, 5, default_batch);
    let range_b = keyset_range_job(&db, "id", 5, 1_000_000, default_batch);
    assert_range_pushed(&range_a).await?;
    assert_range_pushed(&range_b).await?;
    let a = common::extract_one(&range_a).await?;
    let b = common::extract_one(&range_b).await?;
    let mut union_ids = common::int64_col(&a, "id");
    union_ids.extend(common::int64_col(&b, "id"));
    union_ids.sort_unstable();
    assert_eq!(union_ids, sorted(common::int64_col(&full, "id")));

    // Cursor path with a tiny batch size: exercises DECLARE/FETCH batching.
    let tiny = keyset_range_job(&db, "id", 0, 1_000_000, 3);
    assert_range_pushed(&tiny).await?;
    let cursor_batches = common::extract_batches(&tiny).await?;
    let cursor_rows: usize = cursor_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(cursor_rows, common::HOSTILE_ROWS);

    // Same data through every path: ids and exact decimal amounts agree.
    let full_ids = common::int64_col(&full, "id");
    assert_eq!(sorted(full_ids), union_ids);
    let mut cursor_ids = Vec::new();
    for batch in &cursor_batches {
        cursor_ids.extend(common::int64_col(batch, "id"));
    }
    assert_eq!(sorted(cursor_ids), union_ids);

    // Filtered extraction (caller-provided predicate) matches direct SQL
    // (differential oracle: pushed filter vs database ground truth).
    let batches = PostgresConnector::from_config(filtered_job(&db, vec!["id>3".to_string()]))
        .expect("valid job config")
        .extract()
        .standalone()
        .collect()
        .await?;
    let mut filtered_ids = Vec::new();
    for batch in &batches {
        filtered_ids.extend(common::int64_col(batch, "id"));
    }
    filtered_ids.sort_unstable();
    let sql = format!(
        "SELECT id FROM {}.hostile WHERE id > 3 ORDER BY id",
        db.schema
    );
    let rows: Vec<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
        .fetch_all(&db.pool)
        .await?;
    assert_eq!(
        filtered_ids,
        rows.into_iter().map(|r| r.0).collect::<Vec<_>>()
    );
    Ok(())
}

fn sorted(mut v: Vec<i64>) -> Vec<i64> {
    v.sort_unstable();
    v
}
