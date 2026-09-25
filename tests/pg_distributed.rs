//! Distributed standalone end-to-end over the hostile fixture.
//!
//! Exercises codecs, keyset partitioning, and budgeted pools with zero external
//! processes (in-process scheduler + executor = same code path as remote, minus
//! the network). Run: `cargo test --test pg_distributed` (requires the compose stack
//! up; `DATABASE_URL` overrides the default endpoint).

#[path = "common/mod.rs"]
mod common;

use common::{TEST_PASSWORD_ENV, TestDb};
use rust_ballista_extraction_layer::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, JobConfig, ParallelScanConfig,
    PushdownConfig, SourceConfig,
};
use rust_ballista_extraction_layer::connector::postgres::distributed::DistributedContext;

/// Always uses a real database (the compose stack, unless `DATABASE_URL` is set) — never
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
        job_id: "dist_e2e".to_string().parse().unwrap(),
        table: "hostile".to_string(),
        columns: None,
        filters: Vec::new(),
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
        checkpoint: CheckpointConfig::default(),
        pushdown: PushdownConfig::default(),
        parallel_scan: ParallelScanConfig {
            strategy: "keyset".parse().unwrap(),
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

    // Full collect: every fixture row across 4 keyset partitions.
    let batches = ctx
        .session
        .sql("SELECT id FROM hostile")
        .await?
        .collect()
        .await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, common::HOSTILE_ROWS);

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
            // ids 9..13: the updated_at tie rows.
            None,
            Some(-1),
            None,
            None,
            None,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn distributed_run_with_checkpoints_one_split_and_skips_on_retry()
-> Result<(), Box<dyn std::error::Error>> {
    use futures::TryStreamExt;
    use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;
    use std::sync::atomic::{AtomicU64, Ordering};

    let db = live!();
    let dir = std::env::temp_dir().join(format!("relex_dist_ckpt_{}", db.schema));
    let _ = std::fs::remove_dir_all(&dir);
    let config = JobConfig {
        job_id: format!("dist-ckpt-{}", db.schema).parse()?,
        table: "hostile".to_string(),
        columns: Some(vec!["id".to_string()]),
        filters: Vec::new(),
        source: SourceConfig {
            host: db.host.clone(),
            port: db.port,
            user: db.user.clone(),
            password_env: TEST_PASSWORD_ENV.to_string(),
            database: db.database.clone(),
            pool_max: 4,
            statement_timeout_ms: 300_000,
            application_name: "relex-dist-ckpt".to_string(),
            schema: db.schema.clone(),
        },
        checkpoint: CheckpointConfig {
            dir: dir.to_string_lossy().to_string(),
            ..CheckpointConfig::default()
        },
        pushdown: PushdownConfig::default(),
        parallel_scan: ParallelScanConfig {
            strategy: "keyset".parse()?,
            partitions: 4,
            partition_column: "id".to_string(),
        },
        execution: ExecutionConfig::default(),
        distributed: DistributedConfig::default(),
    };
    let rows = AtomicU64::new(0);
    let rows = &rows;
    let consumer =
        move |_split, mut stream: datafusion::physical_plan::SendableRecordBatchStream| async move {
            while let Some(batch) = stream.try_next().await? {
                rows.fetch_add(batch.num_rows() as u64, Ordering::SeqCst);
            }
            Ok(())
        };
    let connector = PostgresConnector::from_config(config)?;
    // Reference oracle: the hostile fixture's row count (tests/data/hostile.sql).
    let first = connector
        .extract()
        .distributed()
        .in_process()
        .workers(2)
        .run_with(consumer)
        .await?;
    assert_eq!((first.splits_total, first.splits_completed), (1, 1));
    let n = common::HOSTILE_ROWS as u64;
    assert_eq!(first.rows_delivered, n);
    assert_eq!(rows.load(Ordering::SeqCst), n);
    let second = connector
        .extract()
        .distributed()
        .in_process()
        .workers(2)
        .run_with(consumer)
        .await?;
    assert_eq!(second.splits_skipped, 1);
    assert_eq!(second.rows_delivered, 0);
    assert_eq!(second.rows_extracted, n);
    assert_eq!(rows.load(Ordering::SeqCst), n, "nothing re-delivered");
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
