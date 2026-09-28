//! Checkpoint contract of `run_with` against a live Postgres.
//!
//! - A checkpoint is bound to its plan: bounds stored at the first run are reused on
//!   retry even after the table grew (oracle: direct SQL over the final table; the union of
//!   delivered rows must equal it), and a changed filter is a typed plan mismatch.
//! - A failing split does not stop the others; the run returns an aggregate error
//!   naming only the failed split (oracle: the split checkpoint).
//! - A second concurrent run of the same job fails with `LockHeld` (oracle: the
//!   typed error) and the job lock is gone after the first run.
//! - A consumer that returns `Ok` without reading its stream to the end does not
//!   complete the split (oracle: the split checkpoint).
//!
//! Needs the compose stack (`tests/docker/compose.yaml`); `DATABASE_URL` overrides it.

#[path = "common/mod.rs"]
mod common;

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use common::{TEST_PASSWORD_ENV, TestDb};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::TryStreamExt;
use rust_ballista_extraction_layer::checkpoint::json_store::JsonCheckpointStore;
use rust_ballista_extraction_layer::checkpoint::{CheckpointError, CheckpointStore, SplitState};
use rust_ballista_extraction_layer::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, FilterEntry, FilterInput, JobConfig,
    ParallelScanConfig, ParallelStrategy, PushdownConfig, SourceConfig,
};
use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;
use rust_ballista_extraction_layer::connector::postgres::pipeline::SplitInfo;
use rust_ballista_extraction_layer::errors::{AppError, ConsumerError};

type R = Result<(), Box<dyn std::error::Error>>;

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A job over `<schema>.t` with keyset partitions on `id` and a private checkpoint dir.
fn job(db: &TestDb, partitions: usize) -> JobConfig {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let job_id = format!("ckpt-{}-{n}", db.schema);
    let dir = std::env::temp_dir().join(format!("relex_ckpt_{job_id}"));
    let _ = std::fs::remove_dir_all(&dir);
    JobConfig {
        job_id: job_id.parse().unwrap(),
        table: "t".to_string(),
        columns: None,
        filters: Vec::new(),
        source: SourceConfig {
            host: db.host.clone(),
            port: db.port,
            user: db.user.clone(),
            password_env: TEST_PASSWORD_ENV.to_string(),
            database: db.database.clone(),
            pool_max: 4,
            statement_timeout_ms: 60_000,
            application_name: "relex-ckpt".to_string(),
            schema: db.schema.clone(),
        },
        checkpoint: CheckpointConfig {
            dir: dir.to_string_lossy().to_string(),
            ..CheckpointConfig::default()
        },
        pushdown: PushdownConfig::default(),
        parallel_scan: ParallelScanConfig {
            strategy: ParallelStrategy::Keyset,
            partitions,
            partition_column: "id".to_string(),
        },
        execution: ExecutionConfig::default(),
        distributed: DistributedConfig::default(),
    }
}

async fn exec(db: &TestDb, sql: &str) {
    let sql = sql.replace("$S", &db.schema);
    sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
        .execute(&db.pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn source_ids(db: &TestDb) -> Vec<i64> {
    let sql = format!("SELECT id FROM {}.t ORDER BY id", db.schema);
    let rows: Vec<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
        .fetch_all(&db.pool)
        .await
        .unwrap();
    rows.into_iter().map(|r| r.0).collect()
}

async fn ids_of(mut stream: SendableRecordBatchStream) -> Result<Vec<i64>, ConsumerError> {
    let mut ids = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        ids.extend(common::int64_col(&batch, "id"));
    }
    Ok(ids)
}

async fn setup_table(db: &TestDb, n: i64) {
    exec(db, "CREATE TABLE $S.t (id bigint PRIMARY KEY)").await;
    exec(
        db,
        &format!("INSERT INTO $S.t SELECT g FROM generate_series(1,{n}) g"),
    )
    .await;
}

#[tokio::test]
async fn b2_retry_reuses_stored_bounds_after_the_table_grew() -> R {
    // Splits 0 and 1 complete over [1, 9], split 2 fails; rows
    // 10..30 arrive; the retry must scan split 2 with its STORED range [7, ∞) — recomputing
    // from MIN/MAX would re-plan split 2 as [21, ∞) and silently lose 7..20.
    let db = TestDb::connect().await;
    setup_table(&db, 9).await;
    let config = job(&db, 3);

    let committed = Mutex::new(Vec::new());
    let committed = &committed;
    let first = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(move |split: SplitInfo, stream| async move {
            let ids = ids_of(stream).await?;
            if split.index == 2 {
                return Err("simulated failure".into());
            }
            committed.lock().unwrap().extend(ids);
            Ok(())
        })
        .await;
    assert!(
        matches!(
            first.as_ref().map_err(AppError::underlying),
            Err(AppError::SplitsFailed { .. })
        ),
        "{first:?}"
    );

    let store = JsonCheckpointStore::new(&config.checkpoint.dir)?;
    let stored = store.read(&config.job_id).await?.expect("checkpoint");
    let bounds: Vec<_> = stored
        .splits
        .iter()
        .map(|s| s.bounds.as_ref().map(|b| (b.lo, b.hi)))
        .collect();
    assert_eq!(
        bounds,
        vec![
            Some((Some(1), Some(4))),
            Some((Some(4), Some(7))),
            Some((Some(7), None))
        ]
    );

    exec(
        &db,
        "INSERT INTO $S.t SELECT g FROM generate_series(10,30) g",
    )
    .await;

    let retried = Mutex::new(Vec::new());
    let retried = &retried;
    let second = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(move |split: SplitInfo, stream| async move {
            retried.lock().unwrap().push(split.clone());
            let ids = ids_of(stream).await?;
            committed.lock().unwrap().extend(ids);
            Ok(())
        })
        .await?;
    assert_eq!(second.splits_skipped, 2);
    let retried = retried.lock().unwrap().clone();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].split_id, "split-2");
    assert_eq!(retried[0].bounds.as_ref().unwrap().lo, Some(7));

    let mut union = committed.lock().unwrap().clone();
    union.sort_unstable();
    assert_eq!(union, source_ids(&db).await, "no gap, no duplicate");
    let _ = std::fs::remove_dir_all(&config.checkpoint.dir);
    db.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn b2_retry_covers_keys_below_the_stored_minimum() -> R {
    // The first partition has no lower bound: rows inserted with a key below the MIN seen at
    // planning time must still be read by the retried split 0 (oracle: direct SQL).
    let db = TestDb::connect().await;
    setup_table(&db, 9).await;
    let config = job(&db, 3);

    let committed = Mutex::new(Vec::new());
    let committed = &committed;
    let first = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(move |split: SplitInfo, stream| async move {
            let ids = ids_of(stream).await?;
            if split.index == 0 {
                return Err("simulated failure".into());
            }
            committed.lock().unwrap().extend(ids);
            Ok(())
        })
        .await;
    assert!(
        matches!(
            first.as_ref().map_err(AppError::underlying),
            Err(AppError::SplitsFailed { .. })
        ),
        "{first:?}"
    );

    exec(&db, "INSERT INTO $S.t VALUES (-5), (0)").await;

    let second = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(move |_split: SplitInfo, stream| async move {
            let ids = ids_of(stream).await?;
            committed.lock().unwrap().extend(ids);
            Ok(())
        })
        .await?;
    assert_eq!(second.splits_skipped, 2);

    let mut union = committed.lock().unwrap().clone();
    union.sort_unstable();
    assert_eq!(
        union,
        source_ids(&db).await,
        "keys below the stored MIN were lost"
    );
    let _ = std::fs::remove_dir_all(&config.checkpoint.dir);
    db.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn b2_changed_filter_is_a_plan_mismatch() -> R {
    // Scenario A: same job id, new filter -> typed error; nothing is skipped silently.
    let db = TestDb::connect().await;
    setup_table(&db, 10).await;
    let mut config = job(&db, 1);
    config.filters = vec![FilterEntry::Single(FilterInput::Shorthand("id>8".into()))];
    let count = |_: SplitInfo, stream| async move { ids_of(stream).await.map(|_| ()) };
    PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(count)
        .await?;

    config.filters = vec![FilterEntry::Single(FilterInput::Shorthand("id>2".into()))];
    let err = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(count)
        .await
        .unwrap_err();
    match err.underlying() {
        AppError::Checkpoint(CheckpointError::PlanMismatch { differs, .. }) => {
            assert_eq!(differs, "filters")
        }
        other => panic!("expected PlanMismatch, got {other}"),
    }
    let _ = std::fs::remove_dir_all(&config.checkpoint.dir);
    db.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn r1_failed_split_does_not_block_the_others() -> R {
    let db = TestDb::connect().await;
    setup_table(&db, 30).await;
    let config = job(&db, 3);
    let delivered = AtomicU64::new(0);
    let delivered = &delivered;
    let result = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(move |split: SplitInfo, stream| async move {
            if split.index == 0 {
                return Err("split-0 consumer down".into());
            }
            let ids = ids_of(stream).await?;
            delivered.fetch_add(ids.len() as u64, Ordering::SeqCst);
            Ok(())
        })
        .await;
    match result.as_ref().map_err(AppError::underlying) {
        Err(AppError::SplitsFailed {
            failures, total, ..
        }) => {
            assert_eq!(*total, 3);
            let ids: Vec<_> = failures.iter().map(|f| f.split_id.clone()).collect();
            assert_eq!(ids, vec!["split-0".to_string()]);
        }
        other => panic!("expected SplitsFailed, got {other:?}"),
    }
    let store = JsonCheckpointStore::new(&config.checkpoint.dir)?;
    let states: Vec<_> = store
        .read(&config.job_id)
        .await?
        .unwrap()
        .splits
        .iter()
        .map(|s| s.state)
        .collect();
    assert_eq!(
        states,
        vec![
            SplitState::Failed,
            SplitState::Completed,
            SplitState::Completed
        ]
    );
    // Splits 1 and 2 hold every row except split 0's range [1, 11).
    assert_eq!(delivered.load(Ordering::SeqCst), 20);
    let _ = std::fs::remove_dir_all(&config.checkpoint.dir);
    db.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn r2_concurrent_run_of_the_same_job_is_refused() -> R {
    let db = TestDb::connect().await;
    setup_table(&db, 5).await;
    let config = job(&db, 1);

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let entered_tx = Mutex::new(Some(entered_tx));
    let release_rx = tokio::sync::Mutex::new(Some(release_rx));
    let (entered_tx, release_rx) = (&entered_tx, &release_rx);

    let connector = PostgresConnector::from_config(config.clone())?;
    let first = connector
        .extract()
        .standalone()
        .run_with(move |_: SplitInfo, stream| async move {
            if let Some(tx) = entered_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            if let Some(rx) = release_rx.lock().await.take() {
                let _ = rx.await;
            }
            ids_of(stream).await.map(|_| ())
        });
    let second = async {
        entered_rx.await.expect("first run entered its consumer");
        let result = PostgresConnector::from_config(config.clone())
            .expect("valid config")
            .extract()
            .standalone()
            .run_with(|_: SplitInfo, stream| async move { ids_of(stream).await.map(|_| ()) })
            .await;
        let _ = release_tx.send(());
        result
    };
    let (first, second) = tokio::join!(first, second);
    first?;
    match second {
        Err(AppError::Checkpoint(CheckpointError::LockHeld { .. })) => {}
        other => panic!("expected LockHeld, got {other:?}"),
    }
    // The first run released its lock: a later run proceeds (and skips the done split).
    let third = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(|_: SplitInfo, stream| async move { ids_of(stream).await.map(|_| ()) })
        .await?;
    assert_eq!(third.splits_skipped, 1);
    let _ = std::fs::remove_dir_all(&config.checkpoint.dir);
    db.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn b3_undrained_stream_does_not_complete_the_split() -> R {
    let db = TestDb::connect().await;
    setup_table(&db, 50).await;
    let mut config = job(&db, 1);
    config.execution.batch_size = 5;
    let result = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(|_: SplitInfo, mut stream| async move {
            // Reads one batch, then claims success.
            let _ = stream.try_next().await?;
            Ok(())
        })
        .await;
    match result.as_ref().map_err(AppError::underlying) {
        Err(AppError::SplitsFailed { failures, .. }) => {
            assert!(
                matches!(*failures[0].error, AppError::SplitIncomplete { .. }),
                "{}",
                failures[0].message()
            );
        }
        other => panic!("expected SplitsFailed, got {other:?}"),
    }
    let store = JsonCheckpointStore::new(&config.checkpoint.dir)?;
    let saved = store.read(&config.job_id).await?.unwrap();
    assert_eq!(saved.splits[0].state, SplitState::Failed);

    // The diagnostic `run()` never touches the checkpoint.
    store.reset(&config.job_id).await?;
    let counted = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run()
        .await?;
    assert_eq!(counted.rows_extracted, 50);
    assert!(store.read(&config.job_id).await?.is_none());

    // `stream()` delivers every row with bounded concurrency and no checkpoint either.
    let mut ids = ids_of(
        PostgresConnector::from_config(job(&db, 3))?
            .extract()
            .standalone()
            .stream()
            .await?,
    )
    .await
    .map_err(|e| e.to_string())?;
    ids.sort_unstable();
    assert_eq!(ids, source_ids(&db).await);
    let _ = std::fs::remove_dir_all(&config.checkpoint.dir);
    db.cleanup().await;
    Ok(())
}

/// Run reports (`<checkpoint.dir>/runs/<job>/<run_id>.json`): one per `run_with` attempt,
/// kept after later runs. Oracle: the reports against what each run actually did (the
/// consumer's failure, the checkpoint's skipped splits, the delivered row counts).
#[tokio::test]
async fn run_reports_record_every_attempt() -> R {
    use rust_ballista_extraction_layer::run_report::{
        RunKind, RunMode, RunStatus, SplitOutcome, list_reports,
    };

    let db = TestDb::connect().await;
    setup_table(&db, 30).await;
    let mut config = job(&db, 3);
    config.filters = vec![FilterEntry::Single(FilterInput::Shorthand("id>=1".into()))];
    let dir = std::path::PathBuf::from(&config.checkpoint.dir);

    // Run 1: split-1's consumer fails; splits 0 and 2 complete.
    let result = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(|split: SplitInfo, stream| async move {
            if split.index == 1 {
                return Err("warehouse write failed".into());
            }
            ids_of(stream).await.map(|_| ())
        })
        .await;
    // The error carries the failed run's id, report file and report.
    let err = result.unwrap_err();
    assert!(matches!(err.underlying(), AppError::SplitsFailed { .. }));
    let failed_report = err.run_report().expect("the failed run's report").clone();
    assert!(err.run_report_path().is_some());

    let reports = list_reports(&dir, &config.job_id).await?;
    assert_eq!(reports.len(), 1);
    let first = &reports[0];
    assert_eq!(err.run_id(), Some(first.run_id.as_str()));
    assert_eq!(&failed_report, first, "returned report == file");
    assert_eq!(first.status, RunStatus::Failed);
    assert_eq!(first.kind, RunKind::Checkpointed);
    assert_eq!(first.mode, RunMode::Standalone);
    assert!(first.run_id.starts_with("r_"), "{}", first.run_id);
    assert!(first.plan.fingerprint.is_some());
    assert_eq!(first.plan.filters, vec!["id >= 1".to_string()]);
    assert_eq!(first.pushdown.len(), 1);
    assert!(first.pushdown[0].pushed, "{:?}", first.pushdown);
    let outcomes: Vec<_> = first.splits.iter().map(|s| s.outcome).collect();
    assert_eq!(
        outcomes,
        vec![
            SplitOutcome::Completed,
            SplitOutcome::Failed,
            SplitOutcome::Completed
        ]
    );
    assert!(
        first.splits[1]
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("warehouse write failed"),
        "{:?}",
        first.splits[1].error
    );
    assert!(first.splits[0].bounds.is_some());
    assert_eq!(first.totals.splits_failed, 1);
    assert_eq!(first.totals.splits_total, 3);
    assert!(first.error.is_some() && first.finished_at.is_some());
    let delivered_first = first.totals.rows_delivered;

    // Run 2: the retry scans only split-1; the others are reported as skipped.
    let outcome = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(|_: SplitInfo, stream| async move { ids_of(stream).await.map(|_| ()) })
        .await?;
    let path = outcome.report_path.clone().expect("a report path");
    assert!(
        path.ends_with(format!("{}.json", outcome.run_id)),
        "{path:?}"
    );

    let reports = list_reports(&dir, &config.job_id).await?;
    assert_eq!(reports.len(), 2, "the first run's report is kept");
    let second = &reports[1];
    assert_eq!(second.run_id, outcome.run_id);
    assert_eq!(&outcome.report, second, "returned report == file");
    assert_ne!(second.run_id, reports[0].run_id);
    assert_eq!(second.status, RunStatus::Succeeded);
    assert_eq!(second.plan.fingerprint, reports[0].plan.fingerprint);
    let outcomes: Vec<_> = second.splits.iter().map(|s| s.outcome).collect();
    assert_eq!(
        outcomes,
        vec![
            SplitOutcome::Skipped,
            SplitOutcome::Completed,
            SplitOutcome::Skipped
        ]
    );
    assert_eq!(second.totals.rows_delivered, outcome.rows_delivered);
    assert_eq!(second.totals.rows_extracted, 30);
    assert_eq!(second.totals.splits_skipped, 2);
    // Across both runs every row was delivered exactly once here (no mid-stream failure).
    assert_eq!(delivered_first + second.totals.rows_delivered, 30);
    assert!(second.error.is_none());

    // Run 3 with a changed filter fails before scanning: reported with the error, no splits.
    config.filters = vec![FilterEntry::Single(FilterInput::Shorthand("id>=2".into()))];
    let err = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(|_: SplitInfo, stream| async move { ids_of(stream).await.map(|_| ()) })
        .await
        .unwrap_err();
    assert!(matches!(
        err.underlying(),
        AppError::Checkpoint(CheckpointError::PlanMismatch { .. })
    ));
    assert!(err.to_string().starts_with("run r_"), "{err}");
    let reports = list_reports(&dir, &config.job_id).await?;
    assert_eq!(reports.len(), 3);
    let third = &reports[2];
    assert_eq!(third.status, RunStatus::Failed);
    assert!(third.splits.is_empty());
    assert!(third.plan.fingerprint.is_some());
    assert!(
        third
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("filters")
    );

    let _ = std::fs::remove_dir_all(&dir);
    db.cleanup().await;
    Ok(())
}

/// Diagnostic runs write a report only when `diagnostic_run_reports` is on, and
/// `run_reports: false` turns `run_with` reports off. Oracle: the files on disk.
#[tokio::test]
async fn run_report_switches() -> R {
    use rust_ballista_extraction_layer::run_report::{RunKind, RunStatus, list_reports, runs_dir};

    let db = TestDb::connect().await;
    setup_table(&db, 10).await;
    let mut config = job(&db, 1);
    let dir = std::path::PathBuf::from(&config.checkpoint.dir);

    // Default: a diagnostic run leaves no report behind.
    let outcome = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run()
        .await?;
    assert!(outcome.report_path.is_none());
    assert!(!runs_dir(&dir, &config.job_id).exists());

    // Opted in: the diagnostic run is recorded as such.
    config.checkpoint.diagnostic_run_reports = true;
    let outcome = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run()
        .await?;
    assert!(outcome.report_path.is_some());
    let reports = list_reports(&dir, &config.job_id).await?;
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].kind, RunKind::Diagnostic);
    assert_eq!(reports[0].status, RunStatus::Succeeded);
    assert_eq!(reports[0].totals.rows_delivered, 10);

    // Reports off: `run_with` writes no file, but still returns the report.
    config.checkpoint.run_reports = false;
    let outcome = PostgresConnector::from_config(config.clone())?
        .extract()
        .standalone()
        .run_with(|_: SplitInfo, stream| async move { ids_of(stream).await.map(|_| ()) })
        .await?;
    assert!(outcome.report_path.is_none());
    assert_eq!(outcome.report.run_id, outcome.run_id);
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(outcome.report.totals.rows_delivered, 10);
    assert_eq!(list_reports(&dir, &config.job_id).await?.len(), 1);

    let _ = std::fs::remove_dir_all(&dir);
    db.cleanup().await;
    Ok(())
}
