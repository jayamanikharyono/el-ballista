//! Distributed end-to-end over the hostile fixture, on a real cluster: `el-ballista scheduler` +
//! `el-ballista worker` child processes (see `common::TestCluster`), so codecs, keyset partitioning
//! and per-process budgeted pools run exactly as deployed. Also the single-process
//! (pure DataFusion) connection budget. Run: `cargo test --test pg_distributed` (requires the
//! compose stack up; `DATABASE_URL` overrides the default endpoint).

#[path = "common/mod.rs"]
mod common;

use common::{TEST_PASSWORD_ENV, TestCluster, TestDb};
use el_ballista::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, JobConfig, ParallelScanConfig,
    PushdownConfig, SourceConfig,
};
use el_ballista::connector::postgres::distributed::DistributedContext;
use el_ballista::connector::postgres::register_table;

/// Always uses a real database (the compose stack, unless `DATABASE_URL` is set) — never
/// skips. Kept as a macro only so call sites (`let db = live!();`) didn't need to change.
macro_rules! live {
    () => {
        TestDb::connect().await
    };
}

#[tokio::test]
async fn cluster_collects_hostile_table() -> Result<(), Box<dyn std::error::Error>> {
    let db = live!();
    let cluster = TestCluster::start(2, 2).await;
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
            ..DistributedConfig::default()
        },
    };
    let ctx = DistributedContext::remote(&config, &cluster.url, 2).await?;
    ctx.register_source(&config).await?;

    // Full collect: every fixture row across 4 keyset partitions.
    let batches = ctx.collect_sql("SELECT id FROM hostile").await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, common::HOSTILE_ROWS);

    // Exact decimals survive the distributed path (P0-1 cover).
    let batches = ctx
        .collect_sql("SELECT amount FROM hostile ORDER BY id")
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
    use el_ballista::connector::postgres::PostgresConnector;
    use futures::TryStreamExt;
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
    let cluster = TestCluster::start(2, 2).await;
    let connector = PostgresConnector::from_config(config)?;
    // Reference oracle: the hostile fixture's row count (tests/data/hostile.sql).
    let first = connector
        .extract()
        .distributed()
        .scheduler(&cluster.url)
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
        .scheduler(&cluster.url)
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

/// Job config of the failover tests: the hostile fixture over 4 keyset partitions, with a
/// short dead-worker timeout so the watchdog reacts within seconds.
fn failover_config(db: &TestDb, job: &str, workers: usize, max_retries: u32) -> JobConfig {
    JobConfig {
        job_id: format!("{job}-{}", db.schema).parse().unwrap(),
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
            application_name: "relex-failover".to_string(),
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
            workers,
            max_retries,
            executor_timeout_secs: 3,
            job_timeout_secs: Some(60),
        },
    }
}

const FAST_FAILOVER: common::Failover = common::Failover {
    executor_timeout_secs: Some(3),
    heartbeat_secs: Some(1),
};

/// A worker killed mid-job like an OOM kill (SIGKILL, no deregistration) must not hang the
/// job. Ballista 54 alone never re-offers a lost worker's tasks, so without the watchdog this
/// job stays "Running" forever; with it, the hung attempt is cancelled and re-run on the worker
/// that is left. Bounded by an outer timeout, so a regression fails instead of hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_killed_mid_job_does_not_hang_it() -> Result<(), Box<dyn std::error::Error>> {
    use el_ballista::connector::postgres::PostgresConnector;
    use std::time::{Duration, Instant};

    const ROWS: i64 = 3_000_000;
    let db = live!();
    // Big enough that the scan is still running when the worker is killed.
    let sql = format!(
        "CREATE TABLE {}.big AS SELECT g::bigint AS id FROM generate_series(1, {ROWS}) g",
        db.schema
    );
    sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
        .execute(&db.pool)
        .await?;
    let mut config = failover_config(&db, "failover", 2, 2);
    config.table = "big".to_string();
    let job_prefix = format!("el-ballista-{}-", config.job_id);
    let mut cluster = TestCluster::start_with(2, 2, FAST_FAILOVER).await;
    let connector = PostgresConnector::from_config(config)?;
    let url = cluster.url.clone();
    let job = tokio::spawn(async move {
        connector
            .extract()
            .distributed()
            .scheduler(&url)
            .workers(2)
            .collect()
            .await
    });
    // Wait until the job is running on the cluster, then kill a worker under it.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cluster
        .jobs()
        .await
        .iter()
        .any(|(name, status)| name.starts_with(&job_prefix) && status == "Running")
    {
        assert!(Instant::now() < deadline, "the job never started running");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cluster.kill_worker(0);
    let batches = tokio::time::timeout(Duration::from_secs(180), job)
        .await
        .expect("the job hung with a dead worker")??;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, ROWS as usize, "every row exactly once");
    let attempts = cluster
        .jobs()
        .await
        .into_iter()
        .filter(|(name, _)| name.starts_with(&job_prefix))
        .count();
    assert!(
        attempts >= 2,
        "the hung attempt was re-run (saw {attempts} job(s))"
    );
    Ok(())
}

/// With no worker left, the job aborts with the typed error instead of hanging.
#[tokio::test]
async fn a_job_with_no_worker_left_aborts() -> Result<(), Box<dyn std::error::Error>> {
    use el_ballista::connector::postgres::PostgresConnector;

    let db = live!();
    let mut cluster = TestCluster::start_with(1, 2, FAST_FAILOVER).await;
    cluster.kill_worker(0);
    let connector = PostgresConnector::from_config(failover_config(&db, "abort", 1, 1))?;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        connector
            .extract()
            .distributed()
            .scheduler(&cluster.url)
            .workers(1)
            .collect(),
    )
    .await
    .expect("the job hung instead of aborting");
    let err = result.expect_err("no worker is left, so the job cannot succeed");
    assert!(
        err.to_string().contains("distributed job aborted"),
        "unexpected error: {err}"
    );
    Ok(())
}

/// Single-process (pure DataFusion, `register_table`): the process is the whole deployment,
/// so it gets the whole `pool_max` (`distributed.workers` does not divide it) and DataFusion
/// runs the partitions concurrently — yet never more open source connections than `pool_max`,
/// planning queries included. Oracle: `pg_stat_activity`, polled while the scan runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn datafusion_standalone_uses_the_whole_pool_but_never_more()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::{AtomicBool, Ordering};

    let db = live!();
    let s = &db.schema;
    for sql in [
        format!("CREATE TABLE {s}.wide (id bigint PRIMARY KEY, pad text)"),
        format!(
            "INSERT INTO {s}.wide SELECT g, repeat('x', 300) FROM generate_series(1, 400000) g"
        ),
        format!("ANALYZE {s}.wide"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&db.pool)
            .await?;
    }
    const POOL_MAX: u32 = 4;
    const WORKERS: usize = 4; // would be a 4 / 4 = 1 connection share if it divided the pool
    let app = "relex-standalone-budget";
    // Small batches over the cursor path: many FETCH round trips, so scans overlap.
    let execution = ExecutionConfig {
        batch_size: 500,
        use_copy: false,
        ..ExecutionConfig::default()
    };
    let config = JobConfig {
        job_id: "dist_budget".to_string().parse().unwrap(),
        table: "wide".to_string(),
        columns: None,
        filters: Vec::new(),
        source: SourceConfig {
            host: db.host.clone(),
            port: db.port,
            user: db.user.clone(),
            password_env: TEST_PASSWORD_ENV.to_string(),
            database: db.database.clone(),
            pool_max: POOL_MAX,
            statement_timeout_ms: 300_000,
            application_name: app.to_string(),
            schema: db.schema.clone(),
        },
        checkpoint: CheckpointConfig::default(),
        pushdown: PushdownConfig::default(),
        parallel_scan: ParallelScanConfig {
            strategy: "keyset".parse().unwrap(),
            partitions: 16,
            partition_column: "id".to_string(),
        },
        execution,
        distributed: DistributedConfig {
            scheduler_url: String::new(),
            workers: WORKERS,
            ..DistributedConfig::default()
        },
    };
    let ctx = datafusion::prelude::SessionContext::new();
    register_table(&ctx, &config).await?;

    let done = AtomicBool::new(false);
    let scan = async {
        let r = ctx.sql("SELECT id, pad FROM wide").await?.collect().await;
        done.store(true, Ordering::SeqCst);
        r
    };
    let watch = async {
        let (mut max_busy, mut max_open) = (0i64, 0i64);
        while !done.load(Ordering::SeqCst) {
            let (busy, open): (i64, i64) = sqlx::query_as(
                "SELECT count(*) FILTER (WHERE state IN ('active', 'idle in transaction')), \
                        count(*) \
                 FROM pg_stat_activity WHERE application_name = $1",
            )
            .bind(app)
            .fetch_one(&db.pool)
            .await
            .unwrap_or((0, 0));
            max_busy = max_busy.max(busy);
            max_open = max_open.max(open);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        (max_busy, max_open)
    };
    let (batches, (max_busy, max_open)) = tokio::join!(scan, watch);
    let rows: usize = batches?.iter().map(|b| b.num_rows()).sum();
    db.cleanup().await;

    assert_eq!(rows, 400_000);
    println!("max concurrent scans {max_busy}, max open connections {max_open}");
    assert!(
        max_open <= i64::from(POOL_MAX),
        "{max_open} source connections open, budget is pool_max = {POOL_MAX}"
    );
    // pool_max / workers would be 1 connection; the single process gets all 4.
    assert!(
        max_busy >= 3,
        "only {max_busy} concurrent scan(s): standalone is still capped at pool_max / workers"
    );
    Ok(())
}
