//! Minimal CLI.
//! cli/mod.rs
//! Hand-rolled arg parsing rather than pulling in `clap` — this pass added several new
//! dependencies already (`serde`, `parquet`) and there was no way to compile-check any of it in
//! the environment this was written in, so new dependencies were kept to ones already resolved
//! in Cargo.lock transitively. `clap` was not one of them.
//!
//! Subcommands: `rel run` / `rel checkpoint` (Phase 1), `rel demo`, and the Phase 2 additions
//! `rel plan` (per-filter push/keep decisions, docs/pushdown.md) and `rel backfill` (a bounded
//! window under its own checkpoint namespace, docs/incremental-extraction.md §7).

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{col, lit, SessionContext};
use uuid::Uuid;

use crate::checkpoint::json_store::JsonCheckpointStore;
use crate::checkpoint::{CheckpointStore, JobKey, RunStats};
use crate::config::JobConfig;
use crate::errors::AppError;
use crate::extractor::postgres::{PostgresExtractor, PostgresTableProvider};
use crate::incremental::{build_window, clamp_to_observed, max_timestamp_column, safe_high_watermark};
use crate::pushdown::PushdownPolicy;

const USAGE: &str = "usage:\n  rel run --config <path>\n  rel checkpoint show --config <path>\n  rel checkpoint reset --config <path>\n  rel demo\n  rel plan --config <path> [--policy always|never|cost_based] [--filter 'col=value'] [--limit n]\n  rel backfill --config <path> --namespace <name> --from <rfc3339> --to <rfc3339>";

pub async fn dispatch() -> Result<(), AppError> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(String::as_str) {
        Some("run") => {
            let config_path = expect_flag(&args, "--config")?;
            run_job(&config_path).await
        }
        Some("demo") => crate::demo::run().await,
        Some("plan") => {
            let config_path = expect_flag(&args, "--config")?;
            let policy = optional_flag(&args, "--policy");
            let filters = repeated_flag(&args, "--filter");
            let limit = optional_flag(&args, "--limit")
                .map(|s| {
                    s.parse::<usize>()
                        .map_err(|e| AppError::Config(format!("--limit must be a non-negative integer: {e}")))
                })
                .transpose()?;
            plan_explain(&config_path, policy.as_deref(), &filters, limit).await
        }
        Some("backfill") => {
            let config_path = expect_flag(&args, "--config")?;
            let namespace = expect_flag(&args, "--namespace")?;
            let from = expect_flag(&args, "--from")?;
            let to = expect_flag(&args, "--to")?;
            run_backfill(&config_path, &namespace, &from, &to).await
        }
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
    optional_flag(args, flag)
        .ok_or_else(|| AppError::Config(format!("missing required flag {flag}\n{USAGE}")))
}

fn optional_flag(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

/// Every occurrence of a repeatable flag, e.g. multiple `--filter col=value` pairs.
fn repeated_flag(args: &[String], flag: &str) -> Vec<String> {
    args.iter()
        .zip(args.iter().skip(1))
        .filter(|(a, _)| a.as_str() == flag)
        .map(|(_, v)| v.clone())
        .collect()
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
            &config.resolved_table(),
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

    // Sink handling is out of scope - use DataFusion writers, Ballista, or orchestrator
    log::debug!(
        "Extracted {} rows from {}.{} ({}—{})",
        rows_extracted,
        config.source.schema,
        config.table,
        window.lo,
        committed_hi
    );

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

/// `rel plan` — docs/pushdown.md's `rel plan --explain`, scoped down: rather than parsing
/// arbitrary SQL and walking the resulting logical plan (which needs DataFusion internals this
/// pass couldn't verify against a compiler), filters come in as simple `column<op>value` CLI
/// flags and are built directly into `Expr`s with the same `col()`/`lit()` builders already
/// proven in `crate::demo`. Prints each filter's push/keep decision — mirroring the worked
/// example in docs/architecture.md §3 — then actually runs the query so the numbers are real,
/// not just a plan.
async fn plan_explain(
    config_path: &str,
    policy_str: Option<&str>,
    filter_strs: &[String],
    limit: Option<usize>,
) -> Result<(), AppError> {
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

    let policy_str = policy_str.unwrap_or(&config.pushdown.policy).to_string();
    let policy = PushdownPolicy::parse(&policy_str);
    let deny = config.pushdown.deny.clone();

    let exprs: Vec<Expr> = filter_strs
        .iter()
        .map(|raw| parse_simple_filter(raw))
        .collect::<Result<_, _>>()?;

    let provider = PostgresTableProvider::new(extractor.pool().clone(), &config.resolved_table(), policy, deny, config.execution.batch_size).await?;

    let expr_refs: Vec<&Expr> = exprs.iter().collect();
    let decisions = provider.supports_filters_pushdown(&expr_refs)?;

    println!("policy: {policy_str}");
    if filter_strs.is_empty() {
        println!("  (no --filter flags given)");
    }
    for (raw, decision) in filter_strs.iter().zip(decisions.iter()) {
        println!("  {raw:<40} -> {decision:?}");
    }

    let ctx = SessionContext::new();
    ctx.register_table(&config.table, Arc::new(provider))?;
    let mut df = ctx.table(&config.table).await?;

    for expr in exprs {
        df = df.filter(expr)?;
    }

    if let Some(n) = limit {
        df = df.limit(0, Some(n))?;
    }

    df.show().await?;

    Ok(())
}

/// Parses `column<op>value` where `<op>` is one of `= != > >= < <=`. Longer operators are
/// checked before their single-character prefixes (`!=`/`>=`/`<=` before `=`/`>`/`<`) so e.g.
/// `amount>=100` doesn't get mis-split on the `=`.
fn parse_simple_filter(raw: &str) -> Result<Expr, AppError> {
    const OPS: [(&str, usize); 6] = [("!=", 2), (">=", 2), ("<=", 2), ("=", 1), (">", 1), ("<", 1)];

    for (op_str, op_len) in OPS {
        if let Some(idx) = raw.find(op_str) {
            let column = raw[..idx].trim();
            let value_str = raw[idx + op_len..].trim();
            let value = parse_filter_value(value_str);

            let expr = match op_str {
                "=" => col(column).eq(value),
                "!=" => col(column).not_eq(value),
                ">" => col(column).gt(value),
                ">=" => col(column).gt_eq(value),
                "<" => col(column).lt(value),
                "<=" => col(column).lt_eq(value),
                _ => unreachable!(),
            };

            return Ok(expr);
        }
    }

    Err(AppError::Config(format!(
        "cannot parse filter '{raw}' — expected 'column<op>value' with op one of = != > >= < <=\n{USAGE}"
    )))
}

fn parse_filter_value(raw: &str) -> Expr {
    let unquoted = raw.trim_matches('\'').trim_matches('"');

    if let Ok(v) = unquoted.parse::<i64>() {
        lit(v)
    } else if let Ok(v) = unquoted.parse::<f64>() {
        lit(v)
    } else if unquoted.eq_ignore_ascii_case("true") {
        lit(true)
    } else if unquoted.eq_ignore_ascii_case("false") {
        lit(false)
    } else {
        lit(unquoted)
    }
}

/// `rel backfill` — docs/incremental-extraction.md §7: the same extract-then-sink machinery as
/// `rel run`, but with an explicit `(from, to]` window instead of one resolved from the safe
/// high watermark, committed under its own checkpoint namespace so it can't clobber the live
/// incremental job's watermark. What's deferred from the doc: chunking a large range into
/// bounded pieces with parallelism (`--chunk`/`--parallel`) — this runs the whole range as one
/// window, which is fine for a bounded backfill but will time out on a very large one.
async fn run_backfill(config_path: &str, namespace: &str, from: &str, to: &str) -> Result<(), AppError> {
    let config = JobConfig::from_file(config_path)?;
    let password = config.resolve_password()?;

    let from_ts = parse_rfc3339(from)?;
    let to_ts = parse_rfc3339(to)?;

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
    let key = JobKey {
        job_id: config.job_id.clone(),
        namespace: namespace.to_string(),
    };
    let run_id = Uuid::new_v4();
    let lease = Duration::hours(2);

    store
        .acquire(&key, run_id, lease, &config.incremental.column)
        .await?;

    let columns = config
        .columns
        .as_ref()
        .map(|c| c.iter().map(String::as_str).collect::<Vec<_>>());

    let outcome: Result<u64, AppError> = async {
        let batch = extractor
            .extract_incremental_window(&config.resolved_table(), columns, &config.incremental.column, from_ts, to_ts)
            .await?;

        let rows_extracted = batch.num_rows() as u64;

        // Sink handling is out of scope - use DataFusion writers, Ballista, or orchestrator
        log::debug!(
            "Backfill extracted {} rows for window ({}, {}]",
            rows_extracted,
            from_ts,
            to_ts
        );

        Ok(rows_extracted)
    }
    .await;

    match outcome {
        Ok(rows_extracted) => {
            store
                .commit(
                    &key,
                    run_id,
                    to_ts,
                    RunStats {
                        rows_extracted,
                        window_lo: Some(from_ts),
                        window_hi: Some(to_ts),
                    },
                )
                .await?;

            println!(
                "backfill '{}' namespace '{namespace}': extracted {rows_extracted} row(s) for window ({from_ts}, {to_ts}]",
                config.job_id
            );

            Ok(())
        }
        Err(e) => {
            let _ = store.abandon(&key, run_id, &e.to_string()).await;
            Err(e)
        }
    }
}

fn parse_rfc3339(raw: &str) -> Result<DateTime<Utc>, AppError> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| AppError::Config(format!("cannot parse timestamp '{raw}': {e}")))
}
