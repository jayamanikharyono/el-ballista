//! Pushdown differential: `always` vs `never` return identical rows.
//!
//! The single highest-value integration test (testing-plan.md Phase C.2): any fidelity
//! lie — including the fixed OR-index shortcut — shows up as a row mismatch.
//! Run: `cargo test --test pg_pushdown` (self-provisions Postgres; set `DATABASE_URL` to
//! point at a specific server instead).

#[path = "common/mod.rs"]
mod common;

use common::{TestDb, TEST_PASSWORD_ENV};
use rust_ballista_extraction_layer::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, IncrementalConfig, JobConfig,
    ParallelScanConfig, PushdownConfig, SinkConfig, SourceConfig,
};
use rust_ballista_extraction_layer::distributed::DistributedContext;

/// Always provisions a real database (embedded, unless `DATABASE_URL` is set) — never
/// skips. Kept as a macro only so call sites (`let db = live!();`) didn't need to change.
macro_rules! live {
    () => {
        TestDb::connect().await
    };
}

fn job_for(db: &TestDb, policy: &str) -> JobConfig {
    JobConfig {
        job_id: "pushdown_diff".to_string(),
        table: "hostile".to_string(),
        columns: None,
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
        incremental: IncrementalConfig {
            column: "updated_at".to_string(),
            safety_lag_secs: 300,
            max_window_secs: 21600,
        },
        sink: SinkConfig {
            path: "/tmp/relex_test_sink".to_string(),
        },
        checkpoint: CheckpointConfig::default(),
        pushdown: PushdownConfig {
            policy: policy.to_string(),
            ..Default::default()
        },
        parallel_scan: ParallelScanConfig {
            strategy: "keyset".to_string(),
            partitions: 2,
            partition_column: "id".to_string(),
        },
        execution: ExecutionConfig::default(),
        distributed: DistributedConfig {
            scheduler_url: String::new(),
            workers: 2,
        },
    }
}

async fn ids_where(
    db: &TestDb,
    policy: &str,
    filter: &str,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let config = job_for(db, policy);
    let ctx = DistributedContext::standalone(&config, 2).await?;
    ctx.register_source(&config).await?;
    let df = ctx
        .session
        .sql(&format!("SELECT id FROM hostile WHERE {}", filter))
        .await?;
    let batches = df.collect().await?;
    let mut ids = Vec::new();
    for b in &batches {
        ids.extend(common::int64_col(b, "id"));
    }
    ids.sort_unstable();
    Ok(ids)
}

#[tokio::test]
async fn pushdown_matches_no_pushdown() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    // Indexed-PK equality: pushed under `always`, kept under `never`.
    let pushed = ids_where(&db, "always", "id = 3").await?;
    let kept = ids_where(&db, "never", "id = 3").await?;
    assert_eq!(pushed, vec![3]);
    assert_eq!(pushed, kept);

    // OR mixing an indexed and an unindexed column: must agree either way
    // (regression cover for the cost-model branch rule).
    let pushed = ids_where(&db, "always", "id = 1 OR nick = 'seven'").await?;
    let kept = ids_where(&db, "never", "id = 1 OR nick = 'seven'").await?;
    assert_eq!(pushed, vec![1, 7]);
    assert_eq!(pushed, kept);
    Ok(())
}
