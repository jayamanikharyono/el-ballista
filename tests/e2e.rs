//! End-to-end suite: Postgres → extractor → Arrow → Ballista → validation.
//!
//! The E2E tests run entirely in-process for the compute side: the Ballista
//! scheduler + executor run standalone in this same process
//! (`DistributedContext::standalone`, the same in-proc path `tests/pg_distributed.rs`
//! exercises). Postgres always comes from the Docker compose stack
//! (`tests/docker/compose.yaml` — `DATABASE_URL` overrides the default endpoint); no
//! embedded server, no external scheduler/workers, no silent skips.
//!
//! Filtering is caller-provided (full scan or explicit predicates) with direct SQL
//! as the oracle; split checkpointing is covered through `run_with` retries: a retry that
//! skips (T-3, counted through `RunOutcome::splits_skipped` and consumer calls) and crash
//! recovery (M15: a consumer failure and a cancelled run mid-split, then a retry whose
//! delivered-row union equals direct SQL with no duplicate or gap).
//!
//! ```bash
//! docker compose -f tests/docker/compose.yaml up -d --wait
//! cargo test --test e2e
//! # or: scripts/e2e.sh  (brings the stack up and down automatically)
//! ```

#[path = "common/mod.rs"]
mod common;

use common::{TEST_PASSWORD_ENV, TestDb};
use futures::TryStreamExt;
use rust_ballista_extraction_layer::checkpoint::{
    CheckpointStore, SplitState, json_store::JsonCheckpointStore,
};
use rust_ballista_extraction_layer::connector::postgres::pipeline::SplitInfo;
use rust_ballista_extraction_layer::errors::{AppError, ConsumerError};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

static JOB_COUNTER: AtomicU64 = AtomicU64::new(0);
use rust_ballista_extraction_layer::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, FilterEntry, FilterInput, JobConfig,
    ParallelScanConfig, PushdownConfig, SourceConfig,
};
use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;
use rust_ballista_extraction_layer::connector::postgres::distributed::DistributedContext;

struct E2E {
    db: TestDb,
}

impl E2E {
    async fn setup() -> Self {
        Self {
            db: TestDb::connect().await,
        }
    }

    fn job(&self, partitions: usize) -> JobConfig {
        // Unique pool per test invocation to avoid cross-test pool sharing when
        // 4 tests run sequentially in the same binary (global registry is per-process).
        // Budget = pool_max / workers, so step by workers*2 to guarantee unique budget.
        let n = JOB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let pool_max = 32 + (n * 4) as u32;
        self.job_with_pool(partitions, pool_max)
    }

    fn job_with_pool(&self, partitions: usize, pool_max: u32) -> JobConfig {
        JobConfig {
            job_id: format!("e2e-{}", pool_max).parse().unwrap(),
            table: "hostile".to_string(),
            columns: None,
            filters: Vec::new(),
            source: SourceConfig {
                host: self.db.host.clone(),
                port: self.db.port,
                user: self.db.user.clone(),
                password_env: TEST_PASSWORD_ENV.to_string(),
                database: self.db.database.clone(),
                pool_max,
                statement_timeout_ms: 300_000,
                application_name: format!("relex-e2e-{}", pool_max),
                schema: self.db.schema.clone(),
            },
            checkpoint: CheckpointConfig::default(),
            pushdown: PushdownConfig::default(),
            parallel_scan: ParallelScanConfig {
                strategy: "keyset".parse().unwrap(),
                partitions,
                partition_column: "id".to_string(),
            },
            execution: ExecutionConfig::default(),
            distributed: DistributedConfig {
                scheduler_url: String::new(),
                workers: 2,
            },
        }
    }

    /// In-process scheduler + executor (docs/roadmap.md Phase 4's "standalone" mode) — the
    /// whole plan-shipping path (codecs, partition distribution, budgeted pools) runs for
    /// real, with no separate scheduler/worker processes to stand up.
    async fn standalone(
        &self,
        partitions: usize,
    ) -> Result<DistributedContext, Box<dyn std::error::Error>> {
        let config = self.job(partitions);
        let ctx = DistributedContext::standalone(&config, partitions).await?;
        ctx.register_source(&config).await?;
        Ok(ctx)
    }

    /// Direct-from-Postgres expected ids for a predicate (the oracle).
    async fn expected_ids(&self, pred: &str) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
        let sql = format!(
            "SELECT id FROM {}.hostile WHERE {} ORDER BY id",
            self.db.schema, pred
        );
        let rows: Vec<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(&self.db.pool)
            .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    async fn remote_ids(
        &self,
        ctx: &DistributedContext,
        select: &str,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
        let batches = ctx.session.sql(select).await?.collect().await?;
        let mut ids = Vec::new();
        for b in &batches {
            ids.extend(common::int64_col(b, "id"));
        }
        ids.sort_unstable();
        Ok(ids)
    }
}

/// Always provisions a real, fully in-process E2E environment — never skips.
macro_rules! live {
    () => {
        E2E::setup().await
    };
}

#[tokio::test]
async fn e2e_full_extraction() -> Result<(), Box<dyn std::error::Error>> {
    let e = live!();
    let ctx = e.standalone(4).await?;
    let got = e.remote_ids(&ctx, "SELECT id FROM hostile").await?;
    assert_eq!(got, e.expected_ids("true").await?);
    assert_eq!(got.len(), common::HOSTILE_ROWS);
    Ok(())
}

#[tokio::test]
async fn e2e_filtered_extraction() -> Result<(), Box<dyn std::error::Error>> {
    let e = live!();
    let ctx = e.standalone(2).await?;

    // Caller-provided range predicate: the extraction layer pushes it to the source,
    // and the result must match Postgres exactly (differential oracle).
    let got = e
        .remote_ids(&ctx, "SELECT id FROM hostile WHERE id > 4 AND id <= 8")
        .await?;
    assert_eq!(got, vec![5, 6, 7, 8]);
    assert_eq!(got, e.expected_ids("id > 4 AND id <= 8").await?);

    // Same range through the connector's filtered path (config filters → pushdown).
    let mut config = e.job(2);
    config.filters = vec![
        FilterEntry::Single(FilterInput::Shorthand("id>4".to_string())),
        FilterEntry::Single(FilterInput::Shorthand("id<=8".to_string())),
    ];
    let batches = PostgresConnector::from_config(config)?
        .extract()
        .standalone()
        .collect()
        .await?;
    let mut ids = Vec::new();
    for b in &batches {
        ids.extend(common::int64_col(b, "id"));
    }
    ids.sort_unstable();
    assert_eq!(ids, got);
    Ok(())
}

/// A private checkpoint dir for one test's job.
fn checkpoint_dir(config: &mut JobConfig, tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "relex_e2e_{tag}_{}_{}",
        std::process::id(),
        config.job_id
    ));
    let _ = std::fs::remove_dir_all(&dir);
    config.checkpoint.dir = dir.to_string_lossy().to_string();
    dir
}

/// Ids of every batch of one split stream.
async fn split_ids(
    mut stream: datafusion::physical_plan::SendableRecordBatchStream,
) -> Result<Vec<i64>, ConsumerError> {
    let mut ids = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        ids.extend(common::int64_col(&batch, "id"));
    }
    Ok(ids)
}

#[tokio::test]
async fn e2e_split_checkpoint_retry_skips_completed() -> Result<(), Box<dyn std::error::Error>> {
    // T-3: the retry must be distinguishable from a re-extract. Oracle: the split checkpoint
    // (all Completed), `splits_skipped`, the consumer call count (0 on retry), and direct SQL
    // for the first run's delivered ids.
    let e = live!();
    let mut config = e.job(2);
    let dir = checkpoint_dir(&mut config, "retry");

    let calls = AtomicUsize::new(0);
    let delivered = Mutex::new(Vec::new());
    let (calls, delivered) = (&calls, &delivered);
    let consumer = move |_split: SplitInfo, stream| async move {
        calls.fetch_add(1, Ordering::SeqCst);
        let ids = split_ids(stream).await?;
        delivered.lock().unwrap().extend(ids);
        Ok(())
    };

    // First run completes and records per-split progress.
    let first = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(consumer)
        .await?;
    assert_eq!(first.splits_completed, first.splits_total);
    assert_eq!(first.splits_skipped, 0);
    assert_eq!(first.splits_total, 2, "2 keyset partitions");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let mut got = delivered.lock().unwrap().clone();
    got.sort_unstable();
    assert_eq!(got, e.expected_ids("true").await?);
    assert_eq!(first.rows_delivered, got.len() as u64);

    let store = JsonCheckpointStore::new(&dir)?;
    let saved = store
        .read(&config.job_id)
        .await?
        .expect("checkpoint recorded");
    assert!(saved.all_completed());

    // Second run skips every completed split: nothing is scanned or delivered.
    let second = PostgresConnector::from_config(config)?
        .extract()
        .standalone()
        .run_with(consumer)
        .await?;
    assert_eq!(second.splits_completed, second.splits_total);
    assert_eq!(second.splits_skipped, second.splits_total);
    assert_eq!(second.rows_delivered, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 2, "no split re-delivered");
    assert_eq!(second.rows_extracted, first.rows_extracted);

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test]
async fn e2e_crash_recovery_consumer_failure_mid_split() -> Result<(), Box<dyn std::error::Error>> {
    // M15: run 1's downstream writer "crashes" on split-1 after its first batch (batch_size 1,
    // so the split is mid-stream). Rows count as delivered only when a split's consumer
    // returns Ok (the writer commits per split). The retry with the same config must complete
    // only split-1, and the union of committed rows must equal direct SQL: no gap, no dup.
    let e = live!();
    let mut config = e.job(2);
    config.execution.batch_size = 1;
    let dir = checkpoint_dir(&mut config, "crash_consumer");

    let committed = Mutex::new(Vec::new());
    let scanned_splits = Mutex::new(Vec::new());
    let (committed, scanned_splits) = (&committed, &scanned_splits);
    let first = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(move |split: SplitInfo, mut stream| async move {
            scanned_splits.lock().unwrap().push(split.split_id.clone());
            let mut ids = Vec::new();
            while let Some(batch) = stream.try_next().await? {
                ids.extend(common::int64_col(&batch, "id"));
                if split.index == 1 {
                    return Err("simulated writer crash mid-split".into());
                }
            }
            committed.lock().unwrap().extend(ids);
            Ok(())
        })
        .await;
    match first {
        Err(AppError::SplitsFailed { failures, .. }) => {
            let ids: Vec<_> = failures.iter().map(|f| f.split_id.as_str()).collect();
            assert_eq!(ids, vec!["split-1"]);
            assert!(failures[0].message().contains("simulated writer crash"));
        }
        other => panic!("expected SplitsFailed for split-1, got {other:?}"),
    }
    let store = JsonCheckpointStore::new(&dir)?;
    let saved = store.read(&config.job_id).await?.expect("checkpoint");
    assert_eq!(saved.splits[0].state, SplitState::Completed);
    assert_eq!(saved.splits[1].state, SplitState::Failed);

    // Retry: only split-1 is scanned and delivered.
    scanned_splits.lock().unwrap().clear();
    let second = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(move |split: SplitInfo, stream| async move {
            scanned_splits.lock().unwrap().push(split.split_id.clone());
            let ids = split_ids(stream).await?;
            committed.lock().unwrap().extend(ids);
            Ok(())
        })
        .await?;
    assert_eq!(*scanned_splits.lock().unwrap(), vec!["split-1".to_string()]);
    assert_eq!(second.splits_skipped, 1);
    assert_eq!(second.splits_completed, 2);

    let mut union = committed.lock().unwrap().clone();
    union.sort_unstable();
    let expected = e.expected_ids("true").await?;
    assert_eq!(
        union, expected,
        "committed rows must equal the source, no gap/dup"
    );
    assert!(store.read(&config.job_id).await?.unwrap().all_completed());
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test]
async fn e2e_crash_recovery_cancelled_run_mid_split() -> Result<(), Box<dyn std::error::Error>> {
    // M15, process-crash flavour: the run is cancelled (future dropped) while split-1 is
    // mid-stream. split-0 was acknowledged and stays Completed; split-1 stays Running with
    // nothing committed. The dropped run releases its lock, and the retry re-runs split-1
    // only; committed rows == direct SQL.
    let e = live!();
    let mut config = e.job(2);
    config.execution.batch_size = 1;
    config.execution.concurrent_partitions = 1; // split-0 finishes before split-1 starts
    let dir = checkpoint_dir(&mut config, "crash_cancel");

    let committed: std::sync::Arc<Mutex<Vec<i64>>> = Default::default();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
    let started_tx = std::sync::Arc::new(Mutex::new(Some(started_tx)));
    let run = {
        let config = config.clone();
        let committed = committed.clone();
        tokio::spawn(async move {
            let connector = PostgresConnector::from_config(config).expect("valid config");
            let result = connector
                .extract()
                .standalone()
                .run_with(move |split: SplitInfo, mut stream| {
                    let committed = committed.clone();
                    let started_tx = started_tx.clone();
                    async move {
                        let mut ids = Vec::new();
                        while let Some(batch) = stream.try_next().await? {
                            ids.extend(common::int64_col(&batch, "id"));
                            if split.index == 1 {
                                if let Some(tx) = started_tx.lock().unwrap().take() {
                                    let _ = tx.send(());
                                }
                                // Hang mid-split until the run is killed.
                                std::future::pending::<()>().await;
                            }
                        }
                        committed.lock().unwrap().extend(ids);
                        Ok::<(), ConsumerError>(())
                    }
                })
                .await;
            result.map(|_| ()).map_err(|e| e.to_string())
        })
    };
    started_rx.await?;
    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());

    let store = JsonCheckpointStore::new(&dir)?;
    let saved = store.read(&config.job_id).await?.expect("checkpoint");
    assert_eq!(saved.splits[0].state, SplitState::Completed);
    assert_eq!(saved.splits[1].state, SplitState::Running);

    let calls = AtomicUsize::new(0);
    let calls = &calls;
    let committed_ref = &committed;
    let second = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(move |split: SplitInfo, stream| async move {
            assert_eq!(split.split_id, "split-1");
            calls.fetch_add(1, Ordering::SeqCst);
            let ids = split_ids(stream).await?;
            committed_ref.lock().unwrap().extend(ids);
            Ok(())
        })
        .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.splits_skipped, 1);
    let mut union = committed.lock().unwrap().clone();
    union.sort_unstable();
    assert_eq!(union, e.expected_ids("true").await?);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test]
async fn e2e_selective_extraction() -> Result<(), Box<dyn std::error::Error>> {
    let e = live!();
    let ctx = e.standalone(2).await?;
    // Narrow projection + pushed filter, checked against the database itself.
    let got = e
        .remote_ids(&ctx, "SELECT id FROM hostile WHERE nick = 'seven'")
        .await?;
    assert_eq!(got, e.expected_ids("nick = 'seven'").await?);
    assert_eq!(got, vec![7]);
    Ok(())
}

#[tokio::test]
async fn e2e_distributed_extraction() -> Result<(), Box<dyn std::error::Error>> {
    let e = live!();
    // Same result through a 4-way partitioned in-process scan as from Postgres directly:
    // distribution must not lose, duplicate, or corrupt rows.
    let ctx = e.standalone(4).await?;
    let got = e.remote_ids(&ctx, "SELECT id, amount FROM hostile").await?;
    assert_eq!(got, e.expected_ids("true").await?);
    Ok(())
}
