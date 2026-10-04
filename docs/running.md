# Running jobs

Operational reference for El Ballista: job spec, execution modes, CLI, filters, library
terminals, logging. For the reasoning behind the design, start with the
[README](../README.md); for internals, see [architecture](architecture.md).

---

## Job spec (full example)

Declarative jobs use the same extraction model through JSON configuration. This one reads
the dvdrental `payment` table from the demo stack (`tests/docker/compose.yaml`, database
`test`, password from `PGPASSWORD`) and spells out every block with its common keys:

```json
{
  "job_id": "payment_extract",
  "table": "payment",
  "columns": ["payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date"],
  "filters": [{ "column": "customer_id", "op": ">=", "value": 300 }],
  "source": {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password_env": "PGPASSWORD",
    "database": "test",
    "pool_max": 8,
    "statement_timeout_ms": 300000,
    "application_name": "el-ballista",
    "schema": "public"
  },
  "checkpoint": {
    "dir": "./.checkpoints",
    "lock_ttl_secs": 1800,
    "run_reports": true,
    "diagnostic_run_reports": false
  },
  "pushdown": {
    "policy": "cost_based",
    "deny": [],
    "push": [],
    "max_source_cost": 50000,
    "keep_threshold": 0.30,
    "statistics_ttl_secs": 900
  },
  "parallel_scan": {
    "strategy": "none",
    "partitions": 1,
    "partition_column": "payment_id"
  },
  "execution": {
    "batch_size": 8192,
    "max_batch_bytes": 16777216,
    "use_copy": false
  },
  "distributed": {
    "scheduler_url": "",
    "workers": 2,
    "max_retries": 2,
    "executor_timeout_secs": 30,
    "job_timeout_secs": 3600
  }
}
```

Optional keys left out above: `execution.concurrent_partitions` (default `pool_max`),
`execution.copy_statement_timeout_ms`, `checkpoint.flush_interval_secs` (5) /
`checkpoint.flush_rows` (100000). `distributed.job_timeout_secs` is optional (no limit when
unset) unless the scheduler REST API cannot be asked. With `columns` omitted, every column is
read, and a column whose type has no Arrow mapping (`tsvector`, `interval`, ...) is an error
naming it; list `columns` without it to extract the rest.

---

## Execution modes: standalone vs distributed

The same job config runs in exactly one of two modes. They are separate on purpose: standalone never
touches Ballista, and distributed never falls back to standalone. The source database is the input
in both; the table lists what each mode needs besides it.

| | **Standalone** | **Distributed** |
|---|---|---|
| What runs | DataFusion only, inside your process | Your process plans; a Ballista cluster executes |
| API | `connector.extract().standalone()` or `register_table(&ctx, &config)` | `connector.extract().distributed()` (`.scheduler(url)`, `.workers(n)`) |
| CLI | `el-ballista run` | `el-ballista distribute` (+ `el-ballista scheduler`, `el-ballista worker`) |
| Components to run | nothing extra: the library runs inside your process | a running `el-ballista scheduler` + `distributed.workers` running `el-ballista worker` processes |
| Connections to Postgres | up to `pool_max` | `pool_max / workers` per worker (more workers than `pool_max` is refused), plus the client's planning connections |
| Parallelism | all visible CPUs; up to `execution.concurrent_partitions` (default `pool_max`) partitions scanning at once | each worker's task slots (`--concurrent-tasks`, default: its CPUs); at most `pool_max / workers` scans per worker query the source |
| Checkpointed `run_with` | one split per keyset partition | the whole scan is one split |

**Standalone** needs no extra components. It is the default choice: one machine, every core, the
whole connection budget, and no scheduling overhead.

```rust
use datafusion::prelude::SessionContext;
use el_ballista::connector::postgres::{PostgresConnector, register_table};

// Library: the connector...
let connector = PostgresConnector::from_config(config.clone())?;
let batches = connector.extract().standalone().collect().await?;
// ...or plain DataFusion over the job's table.
let ctx = SessionContext::new();
register_table(&ctx, &config).await?;
let df = ctx.sql("SELECT staff_id, count(*) FROM payment GROUP BY staff_id").await?;
```

**Distributed** needs a cluster that is already running. All the pieces are this crate's binary
(`el-ballista` below stands for `cargo run --release --bin el-ballista --`, or the built
binary):

| Component | Command | Notes |
|---|---|---|
| Scheduler | `el-ballista scheduler --scheduler-url http://host:50050 [--bind-host 0.0.0.0] [--executor-timeout-secs 30]` | one per cluster; push-based scheduling; serves the REST API used for the executor check and the job watchdog; drops a worker whose heartbeat is older than the timeout; never connects to the source (partition bounds come in the plan) |
| Worker | `el-ballista worker --scheduler-url http://host:50050 [--bind-host 0.0.0.0 --external-host <name>] [--concurrent-tasks N] [--port 50051 --grpc-port 50052] [--heartbeat-secs 5]` | exactly `distributed.workers` of them; stock Ballista executors cannot decode the Postgres scan plans; give each worker on one host its own ports |
| Source password | `export <source.password_env>=…` | in the client's and **every worker's** environment, since each opens its own connections; the scheduler needs none |
| Network | client → scheduler, scheduler ↔ workers, client and workers → source database | containers set `--bind-host 0.0.0.0`; workers also set `--external-host <name>`, and the scheduler advertises its `--scheduler-url` host |

```bash
el-ballista scheduler &
el-ballista worker --scheduler-url http://localhost:50050 &
el-ballista worker --scheduler-url http://localhost:50050 --port 50061 --grpc-port 50062 &
el-ballista distribute --config my-job.json --workers 2
```

If the scheduler is unreachable, `.distributed()` / `el-ballista distribute` fails with an error. More
registered executors than `workers` is an error too, because the source would see more than `pool_max`
connections; fewer is a warning. Choose distributed when one machine's CPUs are the bottleneck.
Workers together stay within `pool_max`; the client's planning connections come on top.

**A dead worker never hangs the job.** On its own, Ballista 54 notices a dead worker only through
missed heartbeats, then puts its tasks back in the queue without ever handing them out again, so
the job would stay "Running" forever. The client runs every distributed job under a watchdog that
polls the scheduler's REST API:

- The job counts as hung when a worker it started with stops heartbeating for
  `distributed.executor_timeout_secs` (default 30) or is dropped by the scheduler, or when the
  optional `distributed.job_timeout_secs` passes.
- A hung job is cancelled on the scheduler and submitted again once the scheduler has dropped the
  dead worker, so the re-run goes to the workers that are left. After `distributed.max_retries`
  re-runs (default 2) the extraction fails with `DistributedJobAborted`; with no worker left it
  fails at once.
- Only a job that has not delivered rows yet is re-run. Ballista delivers results after the job
  finishes, so that covers the whole execution; a hang while results are being fetched is an
  error, because re-running would deliver rows twice.
- Detection needs the scheduler REST API (on in `el-ballista scheduler`, at a plain `http://`
  URL). When it cannot be asked, only `distributed.job_timeout_secs` could end a hung job, so
  `.distributed()` refuses to run unless that is set. The worker heartbeat (`--heartbeat-secs`,
  default 5) must stay well below both timeouts.
- `DistributedContext` (the lower-level entry point) runs queries only through `stream_sql` /
  `collect_sql`, which use the same watchdog.

---

## CLI, filters and job spec rules

`el-ballista run` and `el-ballista distribute` are **diagnostic**:

- They scan the job, count the rows and discard them. They deliver no data and read or write
  no checkpoint: this project is not a sink. The operational, checkpointed job is the
  library's `run_with(consumer)` (below).
- The scan is the full table by default. Add `filters` to the config, or pass
  `--filter 'col=value'` flags, for a filtered extraction whose predicates push to the source
  when possible (see [`examples/configs/extract.example.json`](../examples/configs/extract.example.json)).
- At `--log-level debug` the generated query is logged with strategy `full` or
  `full+pushdown`, plus one `batch` line per Arrow batch (`batch=`, `rows=`, `bytes=`, inside
  the `split{split_id=…}` span).
- `execution.batch_size` sets the source fetch size.
- `parallel_scan` with `strategy: "keyset"` and `partitions > 1` splits the table into
  non-overlapping `partition_column` ranges. `el-ballista run` scans up to
  `execution.concurrent_partitions` of them at a time (capped at `source.pool_max`);
  `el-ballista distribute` spreads them across Ballista workers.
- `strategy: "ctid"` applies only to `el-ballista distribute` / `.distributed()` (standalone warns
  and scans one split). A non-HOT `UPDATE` (or `VACUUM FULL` / `CLUSTER`) during the scan can
  move a row across ranges, so it can be read twice or missed; prefer keyset for changing tables.

`el-ballista plan` prints each filter's pushdown decision, using the same schema-coerced filters as a
run, and previews at most `--limit` rows (default 20).

Filters come in two forms that lower to the same predicate — structured objects
(the recommended JSON form) or shorthand strings (handy for `--filter` flags):

```json
"filters": [
  { "column": "customer_id", "op": ">=", "value": 300 },
  { "column": "amount", "op": ">", "value": 5 },
  "payment_date>=2007-04-06T00:00:00Z"
]
```

Accepted operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `is_null`, `is_not_null`. Shorthand
strings support the first six; `is_null` / `is_not_null` exist only in the structured form
and need no value. An inner JSON array is an OR-group, ANDed with its siblings.

Values follow JSON types (number → int/float, boolean, string, null). Date and time strings
are coerced to the column's type, so they can push to the source as real literals:

- on a timestamp column, an RFC3339 or `YYYY-MM-DD` string becomes a timestamp literal;
- on a date column, a `YYYY-MM-DD` string becomes a date literal.

Programmatically, the connector exposes `filter_exprs()` (parsed predicates),
`explain_filters()` (per-filter pushdown preview — the same decision `scan()` uses) and
`preview(n)` (a DataFrame of at most `n` rows, in no particular order, unsplit). A
shorthand value in quotes
stays a string (`zip='007'` compares against the text `007`, not the integer 7).
Split progress of `run_with` jobs is inspectable via `el-ballista checkpoint show --config <path>`
and resettable via `el-ballista checkpoint reset --config <path>`. Run reports are listed with
`el-ballista runs list --config <path>` and printed with `el-ballista runs show --config <path> [--run <run_id>]`
(default: the latest run).

The job spec is strict: unknown fields anywhere (including a leftover `"sink"` or
`"watermark"` block) are a load error, `parallel_scan.strategy` must be one of
`none`/`keyset`/`ctid` and `pushdown.policy` one of
`always`/`never`/`cost_based`/`strict`/`hinted` (exact lowercase), `job_id` must be
non-empty (max 128 bytes, no control characters or surrounding whitespace), and every
construction path validates the values (`batch_size >= 1`, …).

---

## Library entry point and terminals

The connector is the single entry point for library callers and the CLI: `PostgresConnector::from_config(cfg)?` (validates the config; or `from_config_file(path)?`), then `.extract()`, `.standalone()` or `.distributed()`, and a terminal:

| Terminal | Returns | Checkpoints | Memory |
| --- | --- | --- | --- |
| `.run_with(consumer)` | `RunOutcome` | yes — each split is recorded Completed only after `consumer(split, stream)` returned `Ok` having read the stream to the end | bounded |
| `.stream()` | `SendableRecordBatchStream` | no | bounded |
| `.collect()` | `Vec<RecordBatch>` | no | whole result |
| `.run()` | `RunOutcome` (row counts) | no — diagnostic only | bounded |

```rust
let outcome = PostgresConnector::from_config(config)?
    .extract()
    .standalone()
    .run_with(|split, mut stream| async move {
        while let Some(batch) = stream.try_next().await? {
            // write `batch` for `split.split_id` somewhere durable (idempotently per split)
        }
        Ok(())
    })
    .await?;
```

What `run_with` guarantees:

- **One run per job.** It holds an exclusive per-job lock file with a heartbeat; a stale lock
  is taken over after `checkpoint.lock_ttl_secs` (default 1800).
- **Checkpoints belong to one plan.** The checkpoint is bound to a fingerprint of the plan
  (source host:port/database, table, schema, projection, resolved filters, strategy,
  partitions, partition column, execution mode). Re-running a job id with a different plan
  (e.g. a new filter) is a typed `PlanMismatch` error: use a new `job_id` or
  `el-ballista checkpoint reset`. Checkpoints written before the source was part of the
  fingerprint report `PlanMismatch` once after upgrading.
- **Stable splits.** Each split's key range is stored and reused on retry.
- **Failures are isolated.** Pending splits run concurrently; a failed split is recorded
  without stopping the others, and the run then fails with the list of failed split ids.
- **At-least-once per split.** A split that failed mid-stream is re-delivered in full.
- **A report per run.** Once the lock is held, the run writes
  `<checkpoint.dir>/runs/<job>/<run_id>.json`: a `running` stub at start, replaced atomically
  by the final record (`succeeded` / `failed`). It holds the plan fingerprint and filters, each
  filter's pushdown decision with its reason, per-split outcome / rows / batches / bytes /
  elapsed time / error (splits completed by an earlier run appear as `skipped`), the totals,
  and the run-level error. `RunOutcome` returns the `run_id`, the report itself
  (`outcome.report`, also when report files are off) and the report path. A failed run returns
  `AppError::RunFailed { job_id, run_id, report_path, report, source }`: `err.underlying()` is the
  error to match on (`SplitsFailed`, `PlanMismatch`, …), and `err.run_report()` the record of
  the failed run. A report that cannot be written is logged and never fails the run; nothing
  deletes old reports.
  `checkpoint.run_reports: false` turns them off; `checkpoint.diagnostic_run_reports: true`
  also records `run()` / `el-ballista run` / `el-ballista distribute`. A run refused by the job lock writes no
  report. Summing `rows_delivered` across reports can count a re-delivered split twice.

How the two modes run it:

- `.standalone()` is plain DataFusion in this process, using every visible core and the whole
  `pool_max`, scanning up to `execution.concurrent_partitions` splits at once (default
  `pool_max`). There is no in-process Ballista.
- `.distributed()` treats the whole scan as one split. It uses the standard scheduler URL
  (`http://localhost:50050`) unless the config or `.scheduler(url)` sets one, and needs a
  running cluster (`el-ballista scheduler` + `el-ballista worker`s).
- Each worker gets its connection share (`pool_max / workers`) from the plan. Through the
  scheduler REST API the client refuses more registered executors than `workers` and warns
  about fewer, or about an executor with more task slots than its share (extra scan tasks
  wait for a connection; `--concurrent-tasks <pool_max / workers>` avoids that).

See [`examples/pipeline_extraction.rs`](../examples/pipeline_extraction.rs).

---

## Logging

The crate emits [`tracing`](https://docs.rs/tracing) events with fields, inside `run` (`job_id`, `run_id`) › `split` (`split_id`) › `scan` (`partition`, `query_id`) spans, so every line carries its run, split and scan; `query_id` and `run_id` are the ids in each query's SQL comment, so log lines, `pg_stat_activity` and run reports line up. The library never installs a subscriber; the `el-ballista` binary and every example install one at startup (`logging::init_from_env_and_args`) that writes to **stderr** and, optionally, to a **file**, and also carries dependencies' `log` records. Configuration is read from the command line and the environment (never the job JSON):

| Setting | CLI flag | Env var | Default |
| ------- | -------- | ------- | ------- |
| Level   | `--log-level <off\|error\|warn\|info\|debug\|trace>` | `RUST_LOG` (a level, or directives such as `warn,el_ballista=debug`) | `info` |
| File    | `--log-file <path>` | `EL_BALLISTA_LOG_FILE` | none (stderr only) |

CLI flags take precedence over environment variables. When a file is given, its parent directories are created and logs are **appended**.

Levels: **`error`** a run or split failed (logged once, by the run driver); **`warn`** degraded but continuing (COPY → cursor fallback, missing statistics, stale-lock takeover, a distributed re-run); **`info`** a few lines per run (run started and finished, each split completed); **`debug`** every generated SQL statement (`scan statement sql=…`), partition planning and one line per Arrow batch; **`trace`** sqlx's own line for every statement, each cursor `FETCH` included. So pointing a log file at a path with debug level captures each query:

```bash
# Every generated query goes to logs/run.log (place flags after the subcommand)
cargo run --bin el-ballista -- run \
  --config examples/configs/extract.example.json \
  --log-level debug --log-file logs/run.log

# Equivalent via environment
RUST_LOG=debug EL_BALLISTA_LOG_FILE=logs/run.log \
  cargo run --bin el-ballista -- demo
```

A debug line looks like:

```
2026-09-16T08:12:04.531234Z DEBUG run{job_id=payment_full run_id=r_…}:split{split_id=split-0}:scan{partition=0 query_id=q_…}: el_ballista::connector::postgres::execution_plan: scan statement sql=/* el-ballista query_id=q_… pipeline=payment_full run_id=r_… strategy=full */ DECLARE … CURSOR WITHOUT HOLD FOR SELECT "payment_id", ... FROM "public"."payment"
```

With `execution.use_copy: true` the statement is `COPY (SELECT …) TO STDOUT (FORMAT BINARY)`.

> Note: `el-ballista` with no arguments prints usage; the demo pipeline runs only as `el-ballista demo` (it reads the database password from `PGPASSWORD`). Global flags go after the subcommand (a leading `--log-file` would be treated as a subcommand).
