//! Minimal CLI.
//! cli/mod.rs
//! Hand-rolled arg parsing rather than pulling in `clap` — this pass added several new
//! dependencies already (`serde`, `parquet`) and there was no way to compile-check any of it in
//! the environment this was written in, so new dependencies were kept to ones already resolved
//! in Cargo.Lock transitively. `clap` was not one of them.
//!
//! Subcommands: `rel run` / `rel distribute` (DIAGNOSTIC full or filtered extraction:
//! scans, counts rows, discards them — no data delivered, no checkpoint written; this
//! project is not a sink, the operational job is the library's `run_with(consumer)`),
//! `rel checkpoint show|reset` (split status of `run_with` jobs), `rel demo`, and
//! `rel plan` (per-filter push/keep decisions plus a limited row preview, docs/pushdown.md).

use std::net::SocketAddr;
use std::sync::Arc;

use ballista_core::utils::{default_config_producer, default_session_builder};
use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{SchedulerConfig, TaskDistributionPolicy};
use ballista_scheduler::scheduler_process::start_server;
use datafusion::error::DataFusionError;

use rust_ballista_extraction_layer::checkpoint::CheckpointStore;
use rust_ballista_extraction_layer::checkpoint::json_store::JsonCheckpointStore;
use rust_ballista_extraction_layer::config::{
    FilterEntry, FilterInput, JobConfig, PushdownPolicy, policy_name,
};
use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;
use rust_ballista_extraction_layer::connector::postgres::distributed::{
    PostgresLogicalCodec, PostgresPhysicalCodec,
};
use rust_ballista_extraction_layer::connector::postgres::pipeline::parse_filter_expr;
use rust_ballista_extraction_layer::errors::AppError;

/// Rows `rel plan` previews when `--limit` is not given.
const DEFAULT_PLAN_LIMIT: usize = 20;

pub(crate) const USAGE: &str = "usage:\n  rel run --config <path> [--filter 'col=value' ...]\n  rel distribute --config <path> [--workers N] [--scheduler-url http://host:port] [--filter 'col=value' ...]\n  rel plan --config <path> [--policy always|never|cost_based|strict|hinted] [--filter 'col=value' ...] [--limit n]\n  rel checkpoint show --config <path>\n  rel checkpoint reset --config <path>\n  rel demo\n  rel scheduler [--scheduler-url http://host:port] [--bind-host <ip>]\n  rel worker --scheduler-url http://host:port [--bind-host <ip>] [--external-host <name>] [--concurrent-tasks N]\n\n  `rel run` / `rel distribute` are DIAGNOSTIC: they scan the job (full table, or the\n  config `filters` plus --filter flags), count rows and discard them. No data is delivered\n  and no checkpoint is read or written. This project is not a sink: the operational,\n  checkpointed job is the library API `PostgresConnector::...run_with(consumer)`, whose\n  split state `rel checkpoint show|reset` inspects and clears.\n  `rel plan` prints each filter's pushdown decision and previews --limit rows (default 20).\n\nglobal options (place after the subcommand):\n  --log-level <off|error|warn|info|debug|trace>   log level (default info; also RUST_LOG)\n  --log-file <path>                               also append logs to a file (also REL_LOG_FILE)\n  note: at debug level every generated SQL query is logged";

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

/// `rel run` — DIAGNOSTIC: scan the job's splits (full table, or its filters pushed to
/// the source), count rows, discard the batches. Reads and writes no checkpoint and delivers
/// no data; the operational job is the library's `run_with(consumer)`.
async fn run_job(config_path: &str, cli_filters: &[String]) -> Result<(), AppError> {
    let config = load_config_with_filters(config_path, cli_filters)?;
    let job_id = config.job_id.clone();
    let filtered = !config.filters.is_empty();
    log::info!(
        "job '{job_id}': diagnostic run (rows are counted and discarded; no checkpoint is written)"
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

/// `rel plan` — docs/pushdown.md's `rel plan --explain`, scoped down: filters come from the
/// config's `filters` plus `column<op>value` `--filter` flags. Uses exactly the path every
/// run uses ([`Pipeline::explain_filters`], schema-coerced predicates, the same provider), so
/// the preview cannot disagree with execution. Prints each filter's push/keep decision, then
/// previews at most `--limit` rows (default 20 — never the whole table).
///
/// [`Pipeline::explain_filters`]: rust_ballista_extraction_layer::connector::postgres::pipeline::Pipeline::explain_filters
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
    let pipeline = connector.pipeline();

    println!("policy: {policy}");
    let decisions = pipeline.explain_filters().await?;
    if decisions.is_empty() {
        println!("  (no filters: full extraction)");
    }
    for d in &decisions {
        println!("  {:<40} -> {:?} ({})", d.filter, d.pushdown, d.reason);
    }

    let limit = limit.unwrap_or(DEFAULT_PLAN_LIMIT);
    println!("preview (at most {limit} row(s); --limit to change):");
    pipeline.preview(limit).await?.show().await?;
    Ok(())
}

/// `rel distribute` — docs/roadmap.md Phase 4. DIAGNOSTIC like `rel run` (rows counted and
/// discarded, no checkpoint), but the extraction itself is executed by a Ballista cluster: in-proc (`standalone`) or against a `rel scheduler`/`rel
/// worker` deployment. The table is registered so each scan splits into `workers` keyset
/// partitions, and every process budgets its source pool to `pool_max / workers`.
async fn run_distributed(
    config_path: &str,
    workers: Option<usize>,
    scheduler_url: Option<&str>,
    cli_filters: &[String],
) -> Result<(), AppError> {
    let config = load_config_with_filters(config_path, cli_filters)?;
    let job_id = config.job_id.clone();
    let connector = PostgresConnector::from_config(config)?;
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
    println!(
        "job '{}' (distributed, workers={}, diagnostic): counted {} row(s); nothing delivered, no checkpoint written",
        job_id,
        outcome.workers.unwrap_or(0),
        outcome.rows_extracted,
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
