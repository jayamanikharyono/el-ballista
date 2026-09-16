//! Distributed standalone end-to-end over the hostile fixture.
//!
//! Exercises codecs, keyset partitioning, and budgeted pools with zero external
//! processes (in-process scheduler + executor = same code path as remote, minus
//! the network). Run: `cargo test --test pg_distributed` (self-provisions Postgres;
//! set `DATABASE_URL` to point at a specific server instead).

#[path = "common/mod.rs"]
mod common;

use common::{TEST_PASSWORD_ENV, TestDb};
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

#[tokio::test]
async fn standalone_collects_hostile_table() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let config = JobConfig {
        job_id: "dist_e2e".to_string(),
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
        pushdown: PushdownConfig::default(),
        parallel_scan: ParallelScanConfig {
            strategy: "keyset".to_string(),
            partitions: 4,
            partition_column: "id".to_string(),
        },
        execution: ExecutionConfig::default(),
        distributed: DistributedConfig {
            scheduler_url: String::new(),
            workers: 2,
        },
    };
    let ctx = DistributedContext::standalone(&config, 2).await?;
    ctx.register_source(&config).await?;

    // Full collect: 8 rows across 4 keyset partitions.
    let batches = ctx
        .session
        .sql("SELECT id FROM hostile")
        .await?
        .collect()
        .await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 8);

    // Exact decimals survive the distributed path (P0-1 cover).
    let batches = ctx
        .session
        .sql("SELECT amount FROM hostile ORDER BY id")
        .await?
        .collect()
        .await?;
    let mut amounts = Vec::new();
    for b in &batches {
        amounts.extend(common::decimal_col(b, "amount"));
    }
    assert_eq!(
        amounts,
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
    Ok(())
}
