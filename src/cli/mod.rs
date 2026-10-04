//! Minimal CLI.
//! cli/mod.rs
//! Hand-rolled arg parsing rather than pulling in `clap` — this pass added several new
//! dependencies already (`serde`, `parquet`) and there was no way to compile-check any of it in
//! the environment this was written in, so new dependencies were kept to ones already resolved
//! in Cargo.Lock transitively. `clap` was not one of them.
//!
//! Subcommands: `el-ballista run` / `el-ballista distribute` (DIAGNOSTIC full or filtered extraction:
//! scans, counts rows, discards them — no data delivered, no checkpoint written; this
//! project is not a sink, the operational job is the library's `run_with(consumer)`),
//! `el-ballista checkpoint show|reset` (split status of `run_with` jobs), `el-ballista runs list|show` (the
//! per-run reports under `<checkpoint.dir>/runs/`), `el-ballista demo`, and
//! `el-ballista plan` (per-filter push/keep decisions plus a limited row preview, docs/pushdown.md).

use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

use ballista_core::utils::{default_config_producer, default_session_builder};
use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{SchedulerConfig, TaskDistributionPolicy};
use ballista_scheduler::scheduler_process::start_server;
use datafusion::error::DataFusionError;

use el_ballista::checkpoint::CheckpointStore;
use el_ballista::checkpoint::json_store::JsonCheckpointStore;
use el_ballista::config::{FilterEntry, FilterInput, JobConfig, PushdownPolicy, policy_name};
use el_ballista::connector::postgres::distributed::{PostgresLogicalCodec, PostgresPhysicalCodec};
use el_ballista::connector::postgres::{PostgresConnector, parse_filter_expr};
use el_ballista::errors::AppError;
use el_ballista::run_report::{list_reports, read_report};

/// Rows `el-ballista plan` previews when `--limit` is not given.
const DEFAULT_PLAN_LIMIT: usize = 20;

pub(crate) const USAGE: &str = "usage:\n  el-ballista run --config <path> [--filter 'col=value' ...]\n  el-ballista distribute --config <path> [--workers N] [--scheduler-url http://host:port (default: config, else http://localhost:50050)] [--filter 'col=value' ...]\n  el-ballista plan --config <path> [--policy always|never|cost_based|strict|hinted] [--filter 'col=value' ...] [--limit n]\n  el-ballista checkpoint show --config <path>\n  el-ballista checkpoint reset --config <path>\n  el-ballista runs list --config <path>\n  el-ballista runs show --config <path> [--run <run_id> (default: the latest run)]\n  el-ballista demo\n  el-ballista scheduler [--scheduler-url http://host:port] [--bind-host <ip>] [--executor-timeout-secs N (default 30)]\n  el-ballista worker --scheduler-url http://host:port [--bind-host <ip>] [--external-host <name>] [--concurrent-tasks N] [--port P] [--grpc-port P] [--heartbeat-secs N (default 5)]\n\n  `el-ballista run` / `el-ballista distribute` are DIAGNOSTIC: they scan the job (full table, or the\n  config `filters` plus --filter flags), count rows and discard them. No data is delivered\n  and no checkpoint is read or written. This project is not a sink: the operational,\n  checkpointed job is the library API `PostgresConnector::...run_with(consumer)`, whose\n  split state `el-ballista checkpoint show|reset` inspects and clears.\n  Every `run_with` run also writes a run report (`<checkpoint.dir>/runs/<job>/<run_id>.json`),\n  listed by `el-ballista runs list` and printed by `el-ballista runs show`.\n  `el-ballista plan` prints each filter's pushdown decision and previews --limit rows (default 20).\n\nglobal options (place after the subcommand):\n  --log-level <off|error|warn|info|debug|trace>   log level (default info; also RUST_LOG)\n  --log-file <path>                               also append logs to a file (also EL_BALLISTA_LOG_FILE)\n  note: at debug level every generated SQL query is logged";

pub(crate) async fn dispatch() -> Result<(), AppError> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(String::as_str) {
        Some("run") => {
            let config_path = expect_flag(&args, "--config")?;
            let filters = repeated_flag(&args, "--filter");
            run_job(&config_path, &filters).await
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
            let filters = repeated_flag(&args, "--filter");
            run_distributed(&config_path, workers, scheduler_url.as_deref(), &filters).await
        }
        Some("scheduler") => {
            let scheduler_url = optional_flag(&args, "--scheduler-url");
            let bind_host = optional_flag(&args, "--bind-host");
            let executor_timeout = secs_flag(
                &args,
                "--executor-timeout-secs",
                DEFAULT_EXECUTOR_TIMEOUT_SECS,
            )?;
            run_scheduler(
                scheduler_url.as_deref(),
                bind_host.as_deref(),
                executor_timeout,
            )
            .await
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
            let port = |flag: &str, default: u16| -> Result<u16, AppError> {
                optional_flag(&args, flag).map_or(Ok(default), |s| {
                    s.parse::<u16>()
                        .map_err(|e| AppError::Config(format!("{flag} must be a port number: {e}")))
                })
            };
            let ports = WorkerPorts {
                flight: port("--port", 50051)?,
                grpc: port("--grpc-port", 50052)?,
            };
            let heartbeat = secs_flag(&args, "--heartbeat-secs", DEFAULT_HEARTBEAT_SECS)?;
            run_worker(
                &scheduler_url,
                bind_host.as_deref(),
                external_host.as_deref(),
                concurrent_tasks,
                ports,
                heartbeat,
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
        Some("runs") => match args.get(1).map(String::as_str) {
            Some("list") => {
                let config_path = expect_flag(&args, "--config")?;
                runs_list(&config_path).await
            }
            Some("show") => {
                let config_path = expect_flag(&args, "--config")?;
                let run_id = optional_flag(&args, "--run");
                runs_show(&config_path, run_id.as_deref()).await
            }
            other => Err(AppError::Config(format!(
                "unknown 'runs' subcommand: {other:?}\n{USAGE}"
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

/// `el-ballista worker` heartbeat interval. Ballista's own default is 60 s, which lets a dead worker
/// go unnoticed for minutes; 5 s lets the scheduler and the client-side job watchdog
/// (`distributed.executor_timeout_secs`) spot one within seconds.
const DEFAULT_HEARTBEAT_SECS: u64 = 5;
/// `el-ballista scheduler`: an executor whose heartbeat is older than this is removed (Ballista's
/// default is 180 s). Must stay well above the worker heartbeat interval.
const DEFAULT_EXECUTOR_TIMEOUT_SECS: u64 = 30;
/// How often the scheduler checks for executors past their timeout (Ballista default 15 s).
const EXPIRE_DEAD_EXECUTOR_INTERVAL_SECS: u64 = 5;

/// A positive whole number of seconds from `flag`, else `default`.
fn secs_flag(args: &[String], flag: &str, default: u64) -> Result<u64, AppError> {
    match optional_flag(args, flag) {
        None => Ok(default),
        Some(s) => match s.parse::<u64>() {
            Ok(n) if n >= 1 => Ok(n),
            _ => Err(AppError::Config(format!(
                "{flag} must be a whole number of seconds >= 1 (got '{s}')"
            ))),
        },
    }
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

/// Load a job config and extend its `filters` with CLI `--filter` flags (if any).
fn load_config_with_filters(
    config_path: &str,
    cli_filters: &[String],
) -> Result<JobConfig, AppError> {
    let mut config = JobConfig::from_file(config_path)?;
    // Validate the CLI filters early so a typo fails before any extraction starts.
    for raw in cli_filters {
        parse_filter_expr(raw)?;
    }
    // CLI flags are always AND-conjuncts (each `--filter` is one more `Single`).
    config.filters.extend(
        cli_filters
            .iter()
            .map(|s| FilterEntry::Single(FilterInput::Shorthand(s.clone()))),
    );
    Ok(config)
}

/// `el-ballista run` — DIAGNOSTIC: scan the job's splits (full table, or its filters pushed to
/// the source), count rows, discard the batches. Reads and writes no checkpoint and delivers
/// no data; the operational job is the library's `run_with(consumer)`.
async fn run_job(config_path: &str, cli_filters: &[String]) -> Result<(), AppError> {
    let config = load_config_with_filters(config_path, cli_filters)?;
    let job_id = config.job_id.clone();
    let filtered = !config.filters.is_empty();
    info!(
        job_id = %job_id,
        "diagnostic run: rows are counted and discarded; no checkpoint is written"
    );
    let connector = PostgresConnector::from_config(config)?;
    let outcome = connector.extract().standalone().run().await?;
    println!(
        "job '{}' ({}, diagnostic): counted {} row(s) in {} split(s); nothing delivered, no checkpoint written",
        job_id,
        if filtered { "filtered" } else { "full" },
        outcome.rows_extracted,
        outcome.splits_total,
    );
    if let Some(path) = &outcome.report_path {
        println!("run report: {}", path.display());
    }
    Ok(())
}

fn report_io(what: &str, e: std::io::Error) -> AppError {
    AppError::Io {
        context: format!("cannot read run reports ({what})"),
        source: e,
    }
}

/// `el-ballista runs list` — one line per run report of the job, oldest first.
async fn runs_list(config_path: &str) -> Result<(), AppError> {
    let config = JobConfig::from_file(config_path)?;
    let dir = std::path::Path::new(&config.checkpoint.dir);
    let reports = list_reports(dir, &config.job_id)
        .await
        .map_err(|e| report_io("list", e))?;
    if reports.is_empty() {
        println!("no run reports yet for job '{}'", config.job_id);
        return Ok(());
    }
    println!(
        "{:<14} {:<10} {:<12} {:<11} {:<25} {:>10} {:>14} {:>10}",
        "run_id", "status", "kind", "mode", "started_at", "ms", "rows_delivered", "splits"
    );
    for r in &reports {
        println!(
            "{:<14} {:<10} {:<12} {:<11} {:<25} {:>10} {:>14} {:>10}",
            r.run_id,
            json_name(&r.status),
            json_name(&r.kind),
            json_name(&r.mode),
            r.started_at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            r.duration_ms.map_or("-".to_string(), |ms| ms.to_string()),
            r.totals.rows_delivered,
            format!(
                "{}/{}{}",
                r.totals.splits_completed,
                r.totals.splits_total,
                if r.totals.splits_failed > 0 {
                    format!(" ({} failed)", r.totals.splits_failed)
                } else {
                    String::new()
                }
            ),
        );
    }
    Ok(())
}

/// The serialized (snake_case) name of a report enum value.
fn json_name<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

/// `el-ballista runs show` — the full report of one run (default: the latest) as JSON.
async fn runs_show(config_path: &str, run_id: Option<&str>) -> Result<(), AppError> {
    let config = JobConfig::from_file(config_path)?;
    let dir = std::path::Path::new(&config.checkpoint.dir);
    let report = match run_id {
        Some(run_id) => read_report(dir, &config.job_id, run_id)
            .await
            .map_err(|e| report_io(run_id, e))?,
        None => match list_reports(dir, &config.job_id)
            .await
            .map_err(|e| report_io("list", e))?
            .pop()
        {
            Some(report) => report,
            None => {
                println!("no run reports yet for job '{}'", config.job_id);
                return Ok(());
            }
        },
    };
    let text = serde_json::to_string_pretty(&report).map_err(|e| AppError::Io {
        context: "cannot render the run report".to_string(),
        source: std::io::Error::other(e),
    })?;
    println!("{text}");
    Ok(())
}

async fn checkpoint_show(config_path: &str) -> Result<(), AppError> {
    let config = JobConfig::from_file(config_path)?;
    let store = JsonCheckpointStore::new(&config.checkpoint.dir)?;

    match store.read(&config.job_id).await? {
        Some(checkpoint) => println!("{checkpoint:#?}"),
        None => println!("no checkpoint yet for job '{}'", config.job_id),
    }

    Ok(())
}

/// Delete the split checkpoint so the next run starts fresh — the operator escape
/// hatch after a plan change (`PlanMismatch`) or for a wedged split state. Takes the job's
/// run lock first, so it refuses while a run is active.
async fn checkpoint_reset(config_path: &str) -> Result<(), AppError> {
    let config = JobConfig::from_file(config_path)?;
    let store = JsonCheckpointStore::new(&config.checkpoint.dir)?;
    let ttl = std::time::Duration::from_secs(config.checkpoint.lock_ttl_secs);
    let lock = store.lock(&config.job_id, ttl).await?;
    let reset = store.reset(&config.job_id).await;
    lock.release().await?;
    reset?;
    println!("checkpoint reset for job '{}'", config.job_id);
    Ok(())
}

/// `el-ballista plan` — docs/pushdown.md's `el-ballista plan --explain`, scoped down: filters come from the
/// config's `filters` plus `column<op>value` `--filter` flags. Uses exactly the path every
/// run uses (`PostgresConnector::explain_filters`: schema-coerced predicates, the same
/// provider), so the preview cannot disagree with execution. Prints each filter's push/keep
/// decision, then previews at most `--limit` rows (default 20 — never the whole table).
async fn plan_explain(
    config_path: &str,
    policy_str: Option<&str>,
    filter_strs: &[String],
    limit: Option<usize>,
) -> Result<(), AppError> {
    let mut config = load_config_with_filters(config_path, filter_strs)?;
    if let Some(raw) = policy_str {
        config.pushdown.policy =
            PushdownPolicy::parse(raw).map_err(|e| AppError::Config(e.to_string()))?;
    }
    let policy = policy_name(config.pushdown.policy);
    let connector = PostgresConnector::from_config(config)?;

    println!("policy: {policy}");
    let decisions = connector.explain_filters().await?;
    if decisions.is_empty() {
        println!("  (no filters: full extraction)");
    }
    for d in &decisions {
        println!("  {:<40} -> {:?} ({})", d.filter, d.pushdown, d.reason);
    }

    let limit = limit.unwrap_or(DEFAULT_PLAN_LIMIT);
    println!("preview (at most {limit} row(s); --limit to change):");
    connector.preview(limit).await?.show().await?;
    Ok(())
}

/// `el-ballista distribute` — docs/roadmap.md Phase 4. DIAGNOSTIC like `el-ballista run` (rows counted and
/// discarded, no checkpoint), but the extraction itself is executed by a running Ballista
/// cluster (`el-ballista scheduler` + `el-ballista worker`s) at `--scheduler-url`, else the config's
/// `distributed.scheduler_url`, else `http://localhost:50050`. Every executor process budgets
/// its source pool to `pool_max / workers`.
async fn run_distributed(
    config_path: &str,
    workers: Option<usize>,
    scheduler_url: Option<&str>,
    cli_filters: &[String],
) -> Result<(), AppError> {
    let config = load_config_with_filters(config_path, cli_filters)?;
    let job_id = config.job_id.clone();
    let connector = PostgresConnector::from_config(config)?;
    // No --scheduler-url: the config's scheduler, else the standard local endpoint.
    let mut extraction = connector.extract().distributed();
    if let Some(url) = scheduler_url {
        extraction = extraction.scheduler(url);
    }
    if let Some(n) = workers {
        extraction = extraction.workers(n);
    }
    let outcome = extraction.run().await?;
    println!(
        "job '{}' (distributed, workers={}, diagnostic): counted {} row(s); nothing delivered, no checkpoint written",
        job_id,
        outcome.workers.unwrap_or(0),
        outcome.rows_extracted,
    );
    if let Some(path) = &outcome.report_path {
        println!("run report: {}", path.display());
    }
    Ok(())
}

/// `el-ballista scheduler` — standalone long-running Ballista scheduler. Runs forever; on its own
/// process so it stays up across `el-ballista worker` restarts. Workers (`el-ballista worker`) connect to the
/// URL it advertises. `bind_host` is the local interface to listen on (default: 127.0.0.1 for
/// `localhost`, else the URL host); containers pass `0.0.0.0` so other containers reach it.
///
/// `executor_timeout_secs` (`--executor-timeout-secs`, default 30) is how long an executor may
/// go without a heartbeat before the scheduler removes it; the dead-executor check runs every
/// 5 s. Note Ballista 54 then puts the lost executor's tasks back in the queue but never hands
/// them out again, so the job itself stalls: the client-side watchdog
/// ([`crate::connector::postgres::distributed::watchdog`]) cancels and re-runs it.
async fn run_scheduler(
    scheduler_url: Option<&str>,
    bind_host: Option<&str>,
    executor_timeout_secs: u64,
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
        AppError::Config(format!("invalid scheduler address {bind_host}:{port}: {e}"))
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
        executor_timeout_seconds: executor_timeout_secs,
        expire_dead_executor_interval_seconds: EXPIRE_DEAD_EXECUTOR_INTERVAL_SECS
            .min(executor_timeout_secs),
        ..SchedulerConfig::default()
    };

    // The scheduler rebuilds providers and plans with the Postgres codecs. It never connects
    // to the source: the client computed the partition bounds and they come in the plan.
    info!(scheduler = %scheduler_name, "starting the Ballista scheduler");
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

/// `el-ballista worker` — one long-running Ballista executor, connected to `--scheduler-url`. Run one
/// per machine (or per process). Each resolves the source descriptor in encoded tasks and opens
/// only its `pool_max / workers` share of connections (via `SourcePoolRegistry`), so a
/// three-worker deployment still shows the source the same connection count as a single machine.
/// `bind_host` (default 127.0.0.1) is the local listen interface; `external_host` (default none
/// = localhost behavior) is the name other components dial back on — containers must set both
/// (`0.0.0.0` + the container name), otherwise the scheduler cannot reach the executor.
/// `concurrent_tasks` (default: host CPU count) is the task-slot budget — the closest thing
/// Ballista has to "CPUs per executor" (there is no CPU pinning; tasks run on a shared Tokio
/// runtime over all visible cores). `ports` defaults to 50051 (Arrow Flight, shuffle data) and
/// 50052 (gRPC); set them to run several workers on one host. `heartbeat_secs`
/// (`--heartbeat-secs`, default 5) is how often it reports to the scheduler; keep it well below
/// the scheduler's `--executor-timeout-secs` and the job's `distributed.executor_timeout_secs`.
async fn run_worker(
    scheduler_url: &str,
    bind_host: Option<&str>,
    external_host: Option<&str>,
    concurrent_tasks: Option<usize>,
    ports: WorkerPorts,
    heartbeat_secs: u64,
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
        port: ports.flight,
        grpc_port: ports.grpc,
        scheduler_host,
        scheduler_port,
        concurrent_tasks,
        override_logical_codec: Some(Arc::new(PostgresLogicalCodec::new())),
        override_physical_codec: Some(Arc::new(PostgresPhysicalCodec::new())),
        external_host: external_host.map(str::to_string),
        executor_heartbeat_interval_seconds: heartbeat_secs,
        ..ExecutorProcessConfig::default()
    });

    info!(
        concurrent_tasks,
        scheduler_url = %scheduler_url,
        "starting the Ballista executor"
    );
    start_executor_process(opt).await.map_err(|e| {
        AppError::DataFusion(DataFusionError::External(format!("executor: {e}").into()))
    })
}

/// Listen ports of one `el-ballista worker`.
#[derive(Debug, Clone, Copy)]
struct WorkerPorts {
    /// Arrow Flight service (shuffle / result data), `--port`.
    flight: u16,
    /// Executor gRPC service, `--grpc-port`.
    grpc: u16,
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
