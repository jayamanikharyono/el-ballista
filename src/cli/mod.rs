//! Minimal CLI.
//! cli/mod.rs
//! Hand-rolled arg parsing rather than pulling in `clap` — this pass added several new
//! dependencies already (`serde`, `parquet`) and there was no way to compile-check any of it in
//! the environment this was written in, so new dependencies were kept to ones already resolved
//! in Cargo.lock transitively. `clap` was not one of them.
//!
//! Two subcommands, matching the shape of `rel run` / `rel checkpoint` from docs/roadmap.md.
//! `rel plan --explain` is meaningfully Phase 2 work — there's no cost model yet to explain.

use chrono::Duration;
use uuid::Uuid;

use crate::checkpoint::json_store::JsonCheckpointStore;
use crate::checkpoint::{CheckpointStore, JobKey, RunStats};
use crate::config::JobConfig;
use crate::errors::AppError;
use crate::extractor::postgres::PostgresExtractor;
use crate::incremental::{build_window, clamp_to_observed, max_timestamp_column, safe_high_watermark};
use crate::sink;

const USAGE: &str = "usage:\n  rel run --config <path>\n  rel checkpoint show --config <path>\n  rel checkpoint reset --config <path>\n  rel demo";

pub async fn dispatch() -> Result<(), AppError> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(String::as_str) {
        Some("run") => {
            let config_path = expect_flag(&args, "--config")?;
            run_job(&config_path).await
        }
        Some("demo") => crate::demo::run().await,
        Some("checkpoint") => match args.get(1).map(String::as_str) {
            Some("show") => {
                let config_path = expect_flag(&args, "--config")?;
                checkpoint_show(&config_path).await
            }
            Some("reset") => {
                let config_path = expect_flag(&args, "--config")?;
                checkpoint_reset(&config_path).await
            }
            other => Err(AppError::Config(format!(
                "unknown 'checkpoint' subcommand: {other:?}\n{USAGE}"
            ))),
        },
        other => Err(AppError::Config(format!("unknown command: {other:?}\n{USAGE}"))),
    }
}

fn expect_flag(args: &[String], flag: &str) -> Result<String, AppError> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
        .ok_or_else(|| AppError::Config(format!("missing required flag {flag}\n{USAGE}")))
}

/// Runs the commit protocol from docs/incremental-extraction.md §5: acquire the checkpoint
/// lease, extract the resolved window, write it to the sink, *then* advance the checkpoint —
/// in that order, so a crash between the sink write and the commit just means the next run
/// recomputes the same window and overwrites the same (deterministically named) objects.
async fn run_job(config_path: &str) -> Result<(), AppError> {
    let config = JobConfig::from_file(config_path)?;
    let password = config.resolve_password()?;

    let extractor = PostgresExtractor::connect(
        &config.source.host,
        config.source.port,
        &config.source.user,
        &password,
        &config.source.database,
        config.source.pool_max,
        config.source.statement_timeout_ms,
        &config.source.application_name,
    )
    .await?;

    let store = JsonCheckpointStore::new(&config.checkpoint.dir)?;
    let key = JobKey::new(config.job_id.clone());
    let run_id = Uuid::new_v4();
    // Generous relative to expected run time; not yet configurable per job.
    let lease = Duration::minutes(30);

    let checkpoint = store
        .acquire(&key, run_id, lease, &config.incremental.column)
        .await?;

    match run_once(&config, &extractor, checkpoint.watermark_value).await {
        Ok((hi, rows_extracted, window_lo)) => {
            store
                .commit(
                    &key,
                    run_id,
                    hi,
                    RunStats {
                        rows_extracted,
                        window_lo,
                        window_hi: Some(hi),
                    },
                )
                .await?;

            println!(
                "job '{}': extracted {} row(s), watermark now {}",
                config.job_id, rows_extracted, hi
            );

            Ok(())
        }
        Err(e) => {
            // Best-effort: if releasing the lease also fails, the original error is still what
            // gets returned/reported — an operator can `rel checkpoint reset` to clear a stuck
            // lease.
            let _ = store.abandon(&key, run_id, &e.to_string()).await;
            Err(e)
        }
    }
}

async fn run_once(
    config: &JobConfig,
    extractor: &PostgresExtractor,
    lo: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(chrono::DateTime<chrono::Utc>, u64, Option<chrono::DateTime<chrono::Utc>>), AppError> {
    let safety_lag = Duration::seconds(config.incremental.safety_lag_secs);
    let max_window = Duration::seconds(config.incremental.max_window_secs);

    let hi_candidate = safe_high_watermark(extractor.pool(), safety_lag).await?;
    let window = build_window(lo, hi_candidate, max_window);

    log::info!("job '{}': window ({}, {}]", config.job_id, window.lo, window.hi);

    let columns = config
        .columns
        .as_ref()
        .map(|c| c.iter().map(String::as_str).collect::<Vec<_>>());

    let batch = extractor
        .extract_incremental_window(
            &config.table,
            columns,
            &config.incremental.column,
            window.lo,
            window.hi,
        )
        .await?;

    let rows_extracted = batch.num_rows() as u64;

    // docs/incremental-extraction.md §3.1 Mitigation 3 — don't let the committed watermark race
    // ahead of what was actually observed this run.
    let max_observed = max_timestamp_column(&batch, &config.incremental.column);
    let committed_hi = clamp_to_observed(window.hi, max_observed);

    let batch_with_meta =
        sink::with_metadata_columns(&batch, &config.job_id, chrono::Utc::now(), committed_hi)?;

    sink::write_window(
        &config.sink.path,
        batch_with_meta.schema(),
        &[batch_with_meta],
        window.lo,
        committed_hi,
    )?;

    Ok((committed_hi, rows_extracted, Some(window.lo)))
}

async fn checkpoint_show(config_path: &str) -> Result<(), AppError> {
    let config = JobConfig::from_file(config_path)?;
    let store = JsonCheckpointStore::new(&config.checkpoint.dir)?;
    let key = JobKey::new(config.job_id.clone());

    match store.read(&key).await? {
        Some(checkpoint) => println!("{checkpoint:#?}"),
        None => println!("no checkpoint yet for job '{}'", config.job_id),
    }

    Ok(())
}

/// Clears a stuck `RUNNING` state / expired lease without touching the committed watermark —
/// the operator escape hatch for a run that crashed hard enough not to call `abandon` itself.
async fn checkpoint_reset(config_path: &str) -> Result<(), AppError> {
    let config = JobConfig::from_file(config_path)?;
    let store = JsonCheckpointStore::new(&config.checkpoint.dir)?;
    let key = JobKey::new(config.job_id.clone());
    let run_id = Uuid::new_v4();

    store
        .acquire(&key, run_id, Duration::seconds(0), &config.incremental.column)
        .await?;
    store.abandon(&key, run_id, "manual reset").await
}
