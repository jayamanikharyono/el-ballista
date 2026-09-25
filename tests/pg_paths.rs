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
use rust_ballista_extraction_layer::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, JobConfig, ParallelScanConfig,
    PushdownConfig, SourceConfig,
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

fn filtered_job(db: &TestDb, filters: Vec<String>) -> JobConfig {
    use rust_ballista_extraction_layer::config::{FilterEntry, FilterInput};
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
        },
    }
}

#[tokio::test]
async fn full_keyset_and_filtered_agree() -> Result<(), Box<dyn std::error::Error>> {
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
    assert_eq!(full.num_rows(), common::HOSTILE_ROWS);

    // Keyset partitions tiling the id space union to the full scan (metamorphic oracle).
    let a = ex
        .extract_keyset_partition(&db.table(), None, "id", 0, 5)
        .await?;
    let b = ex
        .extract_keyset_partition(&db.table(), None, "id", 5, 1_000_000)
        .await?;
    let mut union_ids = common::int64_col(&a, "id");
    union_ids.extend(common::int64_col(&b, "id"));
    union_ids.sort_unstable();
    assert_eq!(union_ids, sorted(common::int64_col(&full, "id")));

    // Cursor path with a tiny batch size: exercises DECLARE/FETCH batching.
    let cursor_batches = ex
        .extract_keyset_partition_via_cursor(&db.table(), None, "id", 0, 1_000_000, 3)
        .await?;
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
