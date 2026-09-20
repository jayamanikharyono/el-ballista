//! Minimal CLI.
//! cli/mod.rs
//! Hand-rolled arg parsing rather than pulling in `clap` — this pass added several new
//! dependencies already (`serde`, `parquet`) and there was no way to compile-check any of it in
//! the environment this was written in, so new dependencies were kept to ones already resolved
//! in Cargo.Lock transitively. `clap` was not one of them.
//!
//! Subcommands: `rel run` / `rel checkpoint` (Phase 1), `rel demo`, and the Phase 2 additions
//! `rel plan` (per-filter push/keep decisions, docs/pushdown.md) and `rel backfill` (a bounded
//! window under its own checkpoint namespace, docs/incremental-extraction.md §7).

use std::net::SocketAddr;
use std::sync::Arc;

use ballista_core::utils::{default_config_producer, default_session_builder};
use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{SchedulerConfig, TaskDistributionPolicy};
use ballista_scheduler::scheduler_process::start_server;
use chrono::{DateTime, Duration, Utc};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{SessionContext, col, lit};
use uuid::Uuid;

use rust_ballista_extraction_layer::checkpoint::json_store::JsonCheckpointStore;
use rust_ballista_extraction_layer::checkpoint::{CheckpointStore, JobKey, RunStats};
use rust_ballista_extraction_layer::config::JobConfig;
use rust_ballista_extraction_layer::connector::postgres::{
    PostgresConnector, PostgresExtractor, PostgresTableProvider,
};
use rust_ballista_extraction_layer::distributed::{
    PostgresConnectionDescriptor, PostgresLogicalCodec, PostgresPhysicalCodec,
};
use rust_ballista_extraction_layer::errors::AppError;
use rust_ballista_extraction_layer::pushdown::PushdownPolicy;

const USAGE: &str = "usage:\n  rel run --config <path>\n  rel checkpoint show --config <path>\n  rel checkpoint reset --config <path>\n  rel demo\n  rel plan --config <path> [--policy always|never|cost_based|strict|hinted] [--filter 'col=value'] [--limit n]\n  rel backfill --config <path> --namespace <name> --from <rfc3339> --to <rfc3339>\n  rel distribute --config <path> [--workers N] [--scheduler-url http://host:port]\n  rel scheduler [--scheduler-url http://host:port] [--bind-host <ip>]\n  rel worker --scheduler-url http://host:port [--bind-host <ip>] [--external-host <name>] [--concurrent-tasks N]\n\nglobal options (place after the subcommand):\n  --log-level <off|error|warn|info|debug|trace>   log level (default info; also RUST_LOG)\n  --log-file <path>                               also append logs to a file (also REL_LOG_FILE)\n  note: at debug level every generated SQL query is logged\n  note: `rel run` extraction mode (incremental default, or full) is set by the config\'s \"mode\" field";

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
                    s.parse::<usize>().map_err(|e| {
                        AppError::Config(format!("--limit must be a non-negative integer: {e}"))
                    })
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
        Some("distribute") => {
            let config_path = expect_flag(&args, "--config")?;
            let workers = optional_flag(&args, "--workers")
                .map(|s| {
                    s.parse::<usize>().map_err(|e| {
                        AppError::Config(format!("--workers must be a positive integer: {e}"))
                    })
                })
                .transpose()?;
            let scheduler_url = optional_flag(&args, "--scheduler-url");
            run_distributed(&config_path, workers, scheduler_url.as_deref()).await
        }
        Some("scheduler") => {
            let scheduler_url = optional_flag(&args, "--scheduler-url");
            let bind_host = optional_flag(&args, "--bind-host");
            run_scheduler(scheduler_url.as_deref(), bind_host.as_deref()).await
        }
        Some("worker") => {
            let scheduler_url = expect_flag(&args, "--scheduler-url")?;
            let bind_host = optional_flag(&args, "--bind-host");
            let external_host = optional_flag(&args, "--external-host");
            let concurrent_tasks = optional_flag(&args, "--concurrent-tasks")
                .map(|s| {
                    s.parse::<usize>().map_err(|e| {
                        AppError::Config(format!(
                            "--concurrent-tasks must be a positive integer: {e}"
                        ))
                    })
                })
                .transpose()?;
            run_worker(
                &scheduler_url,
                bind_host.as_deref(),
                external_host.as_deref(),
                concurrent_tasks,
            )
            .await
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
        other => Err(AppError::Config(format!(
            "unknown command: {other:?}\n{USAGE}"
        ))),
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

/// Runs the checkpoint protocol from docs/incremental-extraction.md §5: acquire the checkpoint
/// lease, extract the resolved window as Arrow, and advance the committed watermark. The project
/// ships no sink layer by design — the extraction output is the Arrow batch, and materialization is
/// the downstream consumer's responsibility (README / AGENTS.md). The checkpoint file is the
/// progress-of-record, so a crash before the commit just means the next run recomputes the same
/// window; a resumed backfill picks up from the committed watermark rather than re-walking the
/// range.
async fn run_job(config_path: &str) -> Result<(), AppError> {
    let connector = PostgresConnector::from_config_file(config_path)?;
    let outcome = connector.extract().standalone().run().await?;
    match outcome.window {
        Some(_) => println!(
            "job '{}': extracted {} row(s), watermark now {}",
            connector.config().job_id,
            outcome.rows_extracted,
            outcome
                .committed_watermark
                .expect("incremental run always sets a watermark")
        ),
        None => println!(
            "job '{}' (full): extracted {} row(s)",
            connector.config().job_id,
            outcome.rows_extracted
        ),
    }
    Ok(())
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
        .acquire(
            &key,
            run_id,
            Duration::seconds(0),
            &config.incremental.column,
        )
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

    let policy_str = policy_str.unwrap_or(&config.pushdown.policy).to_string();
    let policy = PushdownPolicy::parse(&policy_str);
    let deny = config.pushdown.deny.clone();

    let exprs: Vec<Expr> = filter_strs
        .iter()
        .map(|raw| parse_simple_filter(raw))
        .collect::<Result<_, _>>()?;

    let descriptor = PostgresConnectionDescriptor::from_config(&config.source, 1);
    let provider = PostgresTableProvider::new(
        descriptor,
        &config.resolved_table(),
        policy,
        deny,
        config.pushdown.push.clone(),
        rust_ballista_extraction_layer::pushdown::cost_model::CostParams {
            max_source_cost: config.pushdown.max_source_cost,
            keep_threshold: config.pushdown.keep_threshold,
        },
        config.pushdown.statistics_ttl_secs,
        config.execution.batch_size,
    )
    .await?;

    let expr_refs: Vec<&Expr> = exprs.iter().collect();
    // Warm EXPLAIN estimates first so cost-based decisions below use them, exactly as a
    // warmed production provider would.
    provider.warm_explain(&exprs).await;
    let decisions = provider.supports_filters_pushdown(&expr_refs)?;

    println!("policy: {policy_str}");
    if filter_strs.is_empty() {
        println!("  (no --filter flags given)");
    }
    for ((raw, expr), decision) in filter_strs.iter().zip(exprs.iter()).zip(decisions.iter()) {
        println!(
            "  {raw:<40} -> {decision:?} ({})",
            provider.explain_decision(expr)
        );
    }

    let ctx = {
        use datafusion::execution::session_state::SessionStateBuilder;
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rule(Arc::new(
                rust_ballista_extraction_layer::pushdown::optimizer_rule::SourceAwarePushdownRule,
            ))
            .build();
        SessionContext::new_with_state(state)
    };
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
    const OPS: [(&str, usize); 6] = [
        ("!=", 2),
        (">=", 2),
        ("<=", 2),
        ("=", 1),
        (">", 1),
        ("<", 1),
    ];

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
/// `rel run`, but with an explicit `[from, to]` window instead of one resolved from the safe
/// high watermark, committed under its own checkpoint namespace so it can't clobber the live
/// incremental job's watermark. A large range is walked in `max_window_secs`-bounded chunks,
/// committing each chunk separately, so a crash resumes from the last committed chunk rather
/// than restarting the whole range (parallel `--chunk`/`--parallel` fan-out stays deferred).
async fn run_backfill(
    config_path: &str,
    namespace: &str,
    from: &str,
    to: &str,
) -> Result<(), AppError> {
    let config = JobConfig::from_file(config_path)?;
    let password = config.resolve_password()?;

    let from_ts = parse_rfc3339(from)?;
    let to_ts = parse_rfc3339(to)?;
    if from_ts > to_ts {
        return Err(AppError::Config(format!(
            "backfill --from ({}) is after --to ({}): empty range",
            from, to
        )));
    }

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
    let max_window = Duration::seconds(config.incremental.max_window_secs);

    let columns = config
        .columns
        .as_ref()
        .map(|c| c.iter().map(String::as_str).collect::<Vec<_>>());

    // Walk [from, to] in max_window-bounded chunks, acquiring and committing each chunk
    // separately: the watermark advances progressively, so a crash resumes from the last
    // committed chunk instead of restarting the whole range.
    // Resume from the last committed chunk instead of restarting at `--from`. Without this, a
    // re-run (crash recovery, or an accidental repeat of the same command) re-extracts chunks that
    // were already persisted and then trips the checkpoint's monotonicity guard on the first commit
    // (next watermark < current), aborting the whole backfill. Reading the namespace's committed
    // watermark makes the walk pick up where it left off.
    let resume_from = store.read(&key).await?.and_then(|c| c.watermark_value);
    let mut chunk_lo = match resume_from {
        Some(w) if w > from_ts => {
            log::info!(
                "backfill '{}' namespace '{namespace}': resuming from committed watermark {} (--from was {})",
                config.job_id,
                w,
                from_ts
            );
            w
        }
        _ => from_ts,
    };
    let mut rows_extracted = 0u64;

    while chunk_lo < to_ts {
        let chunk_hi = (chunk_lo + max_window).min(to_ts);

        store
            .acquire(&key, run_id, lease, &config.incremental.column)
            .await?;

        let outcome: Result<u64, AppError> = async {
            let batch = extractor
                .extract_incremental_window(
                    &config.resolved_table(),
                    columns.clone(),
                    &config.incremental.column,
                    chunk_lo,
                    chunk_hi,
                )
                .await?;

            // Same Arrow-native contract as `run_once`: the batch is the output, no sink is
            // written. The chunk's watermark is committed below so a resumed run picks up from the
            // checkpoint file rather than re-walking the whole range.
            let rows = batch.num_rows() as u64;
            log::debug!(
                "backfill '{}' namespace '{namespace}': extracted {} row(s) for window ({}, {}]",
                config.job_id,
                rows,
                chunk_lo,
                chunk_hi
            );

            Ok(rows)
        }
        .await;

        match outcome {
            Ok(rows) => {
                rows_extracted += rows;
                store
                    .commit(
                        &key,
                        run_id,
                        chunk_hi,
                        RunStats {
                            rows_extracted,
                            window_lo: Some(chunk_lo),
                            window_hi: Some(chunk_hi),
                        },
                    )
                    .await?;
            }
            Err(e) => {
                let _ = store.abandon(&key, run_id, &e.to_string()).await;
                return Err(e);
            }
        }

        chunk_lo = chunk_hi;
    }

    println!(
        "backfill '{}' namespace '{namespace}': extracted {rows_extracted} row(s) for window ({from_ts}, {to_ts}]",
        config.job_id
    );

    Ok(())
}

fn parse_rfc3339(raw: &str) -> Result<DateTime<Utc>, AppError> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| AppError::Config(format!("cannot parse timestamp '{raw}': {e}")))
}

/// `rel distribute` — docs/roadmap.md Phase 4. Same job semantics as `rel run` (checkpoint
/// lease, safe-high-watermark window, `clamp_to_observed` commit), but the extraction itself is
/// executed by a Ballista cluster: in-proc (`standalone`) or against a `rel scheduler`/`rel
/// worker` deployment. The table is registered so each scan splits into `workers` keyset
/// partitions, and every process budgets its source pool to `pool_max / workers`.
async fn run_distributed(
    config_path: &str,
    workers: Option<usize>,
    scheduler_url: Option<&str>,
) -> Result<(), AppError> {
    let connector = PostgresConnector::from_config_file(config_path)?;
    // No --scheduler-url keeps `rel distribute`'s zero-config behavior: an in-process cluster.
    let mut extraction = connector.extract().distributed();
    extraction = match scheduler_url {
        Some(url) => extraction.scheduler(url),
        None => extraction.in_process(),
    };
    if let Some(n) = workers {
        extraction = extraction.workers(n);
    }
    let outcome = extraction.run().await?;
    let watermark = match outcome.committed_watermark {
        Some(w) => format!(", watermark now {w}"),
        None => String::new(),
    };
    println!(
        "job '{}' (distributed, workers={}): extracted {} row(s){}",
        connector.config().job_id,
        outcome.workers.unwrap_or(0),
        outcome.rows_extracted,
        watermark
    );
    Ok(())
}

/// `rel scheduler` — standalone long-running Ballista scheduler. Runs forever; on its own
/// process so it stays up across `rel worker` restarts. Workers (`rel worker`) connect to the
/// URL it advertises. `bind_host` is the local interface to listen on (default: 127.0.0.1 for
/// `localhost`, else the URL host); containers pass `0.0.0.0` so other containers reach it.
async fn run_scheduler(
    scheduler_url: Option<&str>,
    bind_host: Option<&str>,
) -> Result<(), AppError> {
    let (host, port) = parse_scheduler_url(scheduler_url.unwrap_or("localhost:50050"))?;
    let bind_host = bind_host.map(str::to_string).unwrap_or_else(|| {
        if host == "localhost" {
            "127.0.0.1".to_string()
        } else {
            host.clone()
        }
    });
    let addr: SocketAddr = format!("{bind_host}:{port}").parse().map_err(|e| {
        AppError::Config(format!("cannot bind scheduler at {bind_host}:{port}: {e}"))
    })?;

    let scheduler_name = format!("{host}:{port}");

    let scheduler_config = SchedulerConfig {
        bind_host,
        external_host: host,
        bind_port: port,
        override_logical_codec: Some(Arc::new(PostgresLogicalCodec::new())),
        override_physical_codec: Some(Arc::new(PostgresPhysicalCodec::new())),
        // Round-robin, not the default Bias: Bias eagerly fills the first executor with
        // free slots (all 4 scan tasks landing on one worker), RoundRobin deals one task
        // per executor. With partitions == workers this forces an even 1:1:1:1 split.
        task_distribution: TaskDistributionPolicy::RoundRobin,
        ..SchedulerConfig::default()
    };

    // The scheduler rebuilds providers and plans with the Postgres codecs (never opening a
    // connection to the source — partition bounds are computed only when a task is planned).
    log::info!("starting Ballista scheduler at {scheduler_name}");
    let cluster = BallistaCluster::new_memory(
        scheduler_name,
        Arc::new(default_session_builder),
        Arc::new(default_config_producer),
    );

    start_server(cluster, addr, Arc::new(scheduler_config))
        .await
        .map_err(|e| {
            AppError::DataFusion(DataFusionError::External(format!("scheduler: {e}").into()))
        })
}

/// `rel worker` — one long-running Ballista executor, connected to `--scheduler-url`. Run one
/// per machine (or per process). Each resolves the source descriptor in encoded tasks and opens
/// only its `pool_max / workers` share of connections (via `SourcePoolRegistry`), so a
/// three-worker deployment still shows the source the same connection count as a single machine.
/// `bind_host` (default 127.0.0.1) is the local listen interface; `external_host` (default none
/// = localhost behavior) is the name other components dial back on — containers must set both
/// (`0.0.0.0` + the container name), otherwise the scheduler cannot reach the executor.
/// `concurrent_tasks` (default: host CPU count) is the task-slot budget — the closest thing
/// Ballista has to "CPUs per executor" (there is no CPU pinning; tasks run on a shared Tokio
/// runtime over all visible cores).
async fn run_worker(
    scheduler_url: &str,
    bind_host: Option<&str>,
    external_host: Option<&str>,
    concurrent_tasks: Option<usize>,
) -> Result<(), AppError> {
    let (scheduler_host, scheduler_port) = parse_scheduler_url(scheduler_url)?;
    let concurrent_tasks = match concurrent_tasks {
        // 0 means "no opinion" — same as omitting the flag.
        Some(0) | None => std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        Some(n) => n,
    };

    let opt = Arc::new(ExecutorProcessConfig {
        bind_host: bind_host.unwrap_or("127.0.0.1").to_string(),
        port: 50051,
        grpc_port: 50052,
        scheduler_host,
        scheduler_port,
        concurrent_tasks,
        override_logical_codec: Some(Arc::new(PostgresLogicalCodec::new())),
        override_physical_codec: Some(Arc::new(PostgresPhysicalCodec::new())),
        external_host: external_host.map(str::to_string),
        ..ExecutorProcessConfig::default()
    });

    log::info!(
        "starting Ballista executor (concurrent_tasks={concurrent_tasks}) connected to {scheduler_url}"
    );
    start_executor_process(opt).await.map_err(|e| {
        AppError::DataFusion(DataFusionError::External(format!("executor: {e}").into()))
    })
}

/// `"http://localhost:50050"` or `"localhost:50050"` → `(host, port)`; port defaults to 50050.
fn parse_scheduler_url(raw: &str) -> Result<(String, u16), AppError> {
    let rest = raw
        .trim_start_matches("http://")
        .trim_start_matches("HTTP://");

    if let Some((host, port)) = rest.rsplit_once(':') {
        let port = port
            .parse::<u16>()
            .map_err(|e| AppError::Config(format!("invalid scheduler port in '{raw}': {e}")))?;
        Ok((host.to_string(), port))
    } else {
        Ok((rest.to_string(), 50050))
    }
}
