# El Ballista

**Extraction Layer on Ballista**

A Rust-native, source-aware extraction layer built on top of Apache DataFusion and Ballista.

El Ballista extracts data from source databases into bounded Apache Arrow `RecordBatch` streams, with source-aware predicate pushdown, partitioned extraction, and checkpointing for distributed execution.

The layer handles source-aware pushdown, extraction planning, split checkpointing, and streaming data as Arrow `RecordBatch`es. Operations can run in the source database or in DataFusion based on connector capabilities, statistics, and policy. Full and filtered extraction are the supported patterns — incremental and backfill use cases are expressed as caller-provided filter predicates, with no watermark state in the extraction layer. Ballista is used when distributed execution is configured.

> **Status (September 2026):** Phases 1–4 implemented and tested against live PostgreSQL; a MySQL connector exists as a prototype (schema read + full-table extract only). See [`docs/roadmap.md`](docs/roadmap.md) for the roadmap and [`docs/testing-plan.md`](docs/testing-plan.md) for test coverage.

---


## The one-paragraph pitch


The project combines **source-aware query optimization, full/filtered extraction, and Arrow-native execution**.


A typical JDBC-style extractor retrieves rows from a database and performs filtering in the client. A federated query engine can instead push operations into the source database. The Rust Extract Layer treats this boundary as part of the extraction plan.


For each operation, the layer can determine whether it should run in the source database or in DataFusion. The decision uses the source's capabilities, table statistics, and an explicit pushdown policy. The resulting data is exposed as an Arrow `SendableRecordBatchStream` for downstream DataFusion or Ballista execution.


---


## Architecture


```text
              ┌─────────────────────────┐
              │     User / Pipeline     │
              │                         │
              │  DataFrame API          │
              │  Job JSON               │
              └────────────┬────────────┘
                           │
                           ▼
              ┌──────────────────────────┐
              │    Rust Extract Layer    │
              │                          │
               │  Extraction Planning     │
               │  Source-Aware Pushdown   │
               │  PostgreSQL Connector    │
               │  Full / Filtered Scans   │
               │  Split Checkpoint Store  │
              └────────────┬─────────────┘
                           │
                           ▼
                       PostgreSQL
                           │
                           ▼
             SendableRecordBatchStream
                           │
                           ▼
              ┌──────────────────────────┐
              │        DataFusion        │
              │                          │
              │  DataFrame / Execution   │
              │  Transformations         │
              │                          │
              │  Ballista when configured│
              └────────────┬─────────────┘
                           │
                           ▼
                 local Parquet (examples)
```


The extraction layer reads from PostgreSQL and exposes the result as a
`SendableRecordBatchStream`. Each value is decoded once from the Postgres binary wire format
into Arrow builders; from there the batches are Arrow's columnar format end to end, and
DataFusion consumes them as they are (no second conversion).


Ballista can provide distributed execution when configured. The project does not write data anywhere itself (no sink layer): the caller consumes the batches — examples that write results locally use DataFusion writers or `parquet::arrow::ArrowWriter`.


PostgreSQL is the implemented connector.


### What We Implement vs. What We Reuse


This project builds on existing query-engine infrastructure rather than implementing a query engine from scratch.


DataFusion provides query planning and execution, Arrow provides the columnar data model, and Ballista provides distributed scheduling and execution.


The extraction-specific parts are implemented here:


| Part                                      | Project           |
| ----------------------------------------- | ----------------- |
| Query planning and execution              | Apache DataFusion |
| Expression evaluation, joins, aggregation | Apache DataFusion |
| Columnar data and memory format           | Apache Arrow      |
| Distributed scheduling and shuffle        | Apache Ballista   |
| PostgreSQL connector                      | This project      |
| Source capability handling                | This project      |
| Source-aware pushdown                     | This project      |
| Full / filtered extraction                | This project      |
| Split-execution checkpointing             | This project      |
| Extraction job configuration and CLI      | This project      |


The project currently uses DataFusion/Ballista `54.1.0` and Arrow `58.4`, the Arrow version pinned by DataFusion 54.


See [`docs/architecture.md`](docs/architecture.md) for the module layout and execution flow.


---


## Three Ideas Behind the Project


### 1. Extraction and transformation are separate concerns


The extraction layer decides which operations should run in the source database and which should run in the compute engine.


For example:


```text
PostgreSQL
    │
    │  indexed predicates
    │  partition pruning
    │  simple projections
    ▼
SendableRecordBatchStream
    │
    │  Arrow
    ▼
DataFusion
    │
    │  CPU-heavy transformations
    │  vectorized operations
    │  complex expressions
    │  cross-source processing
    ▼
Result
```


The source database can handle operations such as indexed predicates, partition pruning, projections, and aggregations that significantly reduce the amount of data returned.


DataFusion can handle vectorized transformations, custom logic, and operations that are better suited to the compute engine.


The extraction plan determines where each operation is executed.


### 2. Pushdown is based on capabilities and cost


The extraction layer does not treat source support as the only condition for pushdown.


First, it checks whether the connector can express an operation while preserving its semantics. It then uses statistics and the configured policy to determine whether pushing the operation into the source is appropriate.


```text
                Operator
                   │
                   ▼
        ┌──────────────────────┐
        │  Capability check    │
        │                      │
        │ Can the connector    │
        │ express it with      │
        │ identical semantics? │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │  Cost / policy model │
        │                      │
        │ Selectivity          │
        │ Index availability   │
        │ Source cost          │
        │ Expression cost     │
        │ Bytes saved          │
        └──────────┬───────────┘
                   │
             ┌─────┴─────┐
             ▼           ▼
       Push to source  Keep in Arrow
```


DataFusion's `TableProvider::supports_filters_pushdown` provides the semantic part of this decision through `Exact`, `Inexact`, and `Unsupported`.


`Exact` means the source can apply the filter with equivalent semantics, allowing DataFusion to omit its own `FilterExec`.


`Inexact` means the source can pre-filter the data, but DataFusion must evaluate the filter again to preserve correctness.


For example, database collation can affect string comparison semantics. Under a case-insensitive collation `status = 'PAID'` also matches `'paid'`, while Arrow compares bytes. The connector therefore renders every pushed text comparison with an explicit binary collation — `("status" COLLATE "C") = $1` on Postgres — which compares exactly like Arrow, so it can be `Exact`. Comparisons that have no faithful source form (float ranges, `NOT` over an inexact child, numeric comparisons today) are not pushed at all.


See [`docs/pushdown.md`](docs/pushdown.md) for the cost model, policy engine, and semantic compatibility rules.


### 3. Full and filtered extraction


The extraction layer focuses on HOW data is extracted, not on pipeline-level
incremental or backfill semantics. It supports full extraction (the complete
source dataset) and filtered extraction (caller-provided predicates or ranges),
parallelized through query splitting and tracked with split-execution
checkpoints.


```text
Pipeline / Orchestrator
    │  decides WHAT range to extract
    ▼
Extraction Layer
    │  decides HOW to extract it efficiently
    ├── Full / Filtered
    ├── Partitioning
    ├── Checkpointing
    └── DataFusion / Ballista
```


Incremental and backfill use cases are expressed as filtered extraction: an
incremental job provides a time-range predicate, a backfill provides a
historical range. Watermark management, incremental state, backfill
orchestration, CDC, and pipeline scheduling/retry policies are out of scope
for this layer (see `docs/deferred/incremental-extraction.md` for the deferred
design).

#### Filtered pipeline walkthrough

`cargo run --example filtered_extraction` with `examples/configs/extract.example.json`:

```json
{
  "job_id": "orders_extract",
  "table": "orders",
  "columns": ["order_id", "user_id", "status", "amount", "currency", "item_count", "tags", "metadata", "shipped_on", "ext_ref", "created_at", "updated_at"],
  "filters": [
    { "column": "status", "op": "=", "value": "PAID" },
    { "column": "amount", "op": ">", "value": 100 },
    { "column": "updated_at", "op": ">=", "value": "2026-01-01T00:00:00Z" },
    { "column": "user_id", "op": ">=", "value": 500 }
  ]
}
```

```text
═══════════════════════════════════════════════════════════  Filtered Extraction Example
  Caller-provided predicates → source pushdown
═══════════════════════════════════════════════════════════

► Step 1: Job spec + caller-provided filters
  Table: public.orders
  Filters: [Single(Structured(FilterSpec { column: "status", op: Eq, value: String("PAID") })), Single(Structured(FilterSpec { column: "amount", op: Gt, value: Number(100) })), Single(Structured(FilterSpec { column: "updated_at", op: GtEq, value: String("2026-01-01T00:00:00Z") })), Single(Structured(FilterSpec { column: "user_id", op: GtEq, value: Number(500) }))]

► Step 2: Preview pushdown decisions
  filter status = 'PAID' -> pushed_to_source=true (PUSH (Exact; low selectivity (20.00%) and cost (848) within budget (50000); ((CAST("status" AS text) COLLATE "C") = 'PAID')))
  filter amount > 100 -> pushed_to_source=false (KEEP (no exact or superset source form (expression, type, or operator not supported); stays in Arrow))
  filter updated_at >= '2026-01-01T00:00:00Z' -> pushed_to_source=true (PUSH (Exact; index available: orders_updated_at_idx; ("updated_at" >= '2026-01-01T00:00:00+00:00'::timestamptz)))
  filter user_id >= 500 -> pushed_to_source=true (PUSH (Exact; EXPLAIN index path; ("user_id" >= 500)))

► Step 3: Extract with pushdown
  ✓ Filtered extraction complete: 28 row(s) in 1 batch(es)

► Step 4: Operational run (split checkpointing)
2026-09-24T09:41:14.830Z [INFO ] rust_ballista_extraction_layer::checkpoint::lock: job 'orders_extract': acquired run lock ./.checkpoints/orders_extract.lock (owner 9125fa32033d48939771daca898fb1dd)
2026-09-24T09:41:14.863Z [INFO ] rust_ballista_extraction_layer::connector::postgres::pipeline::run: job 'orders_extract': 1 split(s), 1 pending, 0 already completed (concurrency 4)
    split-0: 28 row(s) consumed
2026-09-24T09:41:14.870Z [INFO ] rust_ballista_extraction_layer::checkpoint::json_store: job 'orders_extract' split 'split-0' completed (rows=28)
2026-09-24T09:41:14.870Z [INFO ] rust_ballista_extraction_layer::connector::postgres::pipeline::run: job 'orders_extract' split=split-0 done rows=28 elapsed_ms=6
2026-09-24T09:41:14.871Z [INFO ] rust_ballista_extraction_layer::checkpoint::lock: job 'orders_extract': released run lock
  ✓ Run outcome: 28 row(s) delivered, splits 1/1 (0 skipped from an earlier run)

═══════════════════════════════════════════════════════════
  ✓ Filtered extraction example complete
═══════════════════════════════════════════════════════════
```

What this shows: the orchestrator supplies predicates (Step 1); the layer
decides per predicate whether the source can answer it exactly, approximately
(`Inexact`, re-checked in Arrow), or not at all (Step 2 — `status` pushes as an `Exact`
byte-wise comparison on cost grounds, the indexed range predicates push, and `amount > 100`
stays in Arrow because a `numeric` comparison has no exact source form yet); extraction
returns Arrow batches (Step 3); the operational run (`run_with`) hands each
split's stream to a consumer and records the split completed only after the
consumer returned `Ok`, so a retry skips it (Step 4).


---


## Intended API


The DataFrame API provides a Rust interface over the extraction and DataFusion execution layers:


```rust
let ctx = ExtractContext::from_config(config).await?;


let batches = ctx
    .source("postgres", "public.orders").await?
    .filter(col("status").eq(lit("PAID")))?
    .select(vec![col("order_id"), col("user_id"), col("amount")])?
    .with_column("amount_usd", col("amount") * lit(2.0))?
    .limit(0, Some(1_000_000))?
    .collect().await?;
```


`collect()` materializes the whole result as `Vec<RecordBatch>`; for large tables use the connector's bounded-memory `stream()` / `run_with(consumer)` terminals (below) or DataFusion's `execute_stream()`.


Declarative jobs use the same extraction model through JSON configuration:


```json
{
  "job_id": "orders_extract",
  "table": "orders",
  "columns": ["order_id", "user_id", "status", "amount", "currency", "item_count", "tags", "metadata", "shipped_on", "ext_ref", "created_at", "updated_at"],
  "filters": [{ "column": "status", "op": "=", "value": "PAID" }],
  "source": {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password_env": "ORDERS_PG_PASSWORD",
    "database": "app",
    "pool_max": 8,
    "statement_timeout_ms": 300000,
    "application_name": "rust-extract-layer",
    "schema": "public"
  },
  "checkpoint": {
    "dir": "./.checkpoints"
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
    "partition_column": "order_id"
  },
  "execution": {
    "batch_size": 8192
  },
  "distributed": {
    "scheduler_url": "",
    "workers": 2
  }
}
```


---


## Getting Started


See [`QUICKSTART.md`](QUICKSTART.md) for complete setup, configuration, distributed execution, benchmarks, and testing.


**TL;DR — single-node extraction** against the seeded `app` database (500 users, 20k
`orders`) that the examples' configs point at (`localhost:5432`, database `app`):


```bash
docker compose -f benchmark/PostgresDB/compose.yaml up -d
export ORDERS_PG_PASSWORD=postgres

# Diagnostic run: scans with the config's filters pushed down, counts rows, discards them.
cargo run --bin rust-ballista-extraction-layer -- run \
  --config examples/configs/extract.example.json
# -> job 'orders_extract' (filtered, diagnostic): counted 28 row(s) in 1 split(s); ...
```

Or against the test stack (Postgres + MySQL with the dvdrental dataset, database `test`):

```bash
docker compose -f tests/docker/compose.yaml up -d --wait
export ORDERS_PG_PASSWORD=postgres
cargo run --bin rust-ballista-extraction-layer -- run \
  --config examples/configs/full_extract.dvd_rental.json
# -> job 'dvd_rental_full' (full, diagnostic): counted 14596 row(s) in 1 split(s); ...
```

Both compose files publish Postgres on port 5432, so run one at a time.


### Runnable Examples


The `examples/` directory contains runnable pipelines against a real PostgreSQL database. They are also useful as executable documentation and smoke tests.


| Example                                    | What it runs                                                          |
| ------------------------------------------ | --------------------------------------------------------------------- |
| `full_extraction`                          | Whole-table extraction, schema discovery, type mapping, Arrow batches |
| `filtered_extraction`                      | Caller-provided predicates pushed to the source                       |
| `parallel_extraction`                      | Partition-aware extraction across multiple scans                      |
| `distributed_extraction`                   | Distributed extraction using Ballista                                 |
| `dataframe_extraction`                     | DataFrame API against a job specification                             |


For example:


```bash
# Single-node
cargo run --bin rust-ballista-extraction-layer -- run \
  --config examples/configs/extract.example.json


# In-process distributed execution
cargo run --bin rust-ballista-extraction-layer -- distribute \
  --config examples/configs/extract.example.json --workers 2


# Explain pushdown decisions
cargo run --bin rust-ballista-extraction-layer -- plan \
  --config examples/configs/extract.example.json \
  --policy cost_based \
  --filter 'status=PAID'


# Full extraction (initial load / periodic full refresh)
cargo run --bin rust-ballista-extraction-layer -- run \
  --config examples/configs/full_extract.example.json
```

`rel run` and `rel distribute` are **diagnostic**: they scan the job (the full table by default; add `filters` to the config or pass `--filter 'col=value'` flags for a filtered extraction whose predicates push to the source when possible — see [`examples/configs/extract.example.json`](examples/configs/extract.example.json)), count the rows and discard them. They deliver no data and read or write no checkpoint: this project is not a sink. The operational, checkpointed job is the library's `run_with(consumer)` (below). At `--log-level debug` the generated `SELECT ... FROM <table>` is logged with strategy `full` or `full+pushdown`, plus one line per Arrow batch (`split=`, `rows=`, `batch_bytes=`). `execution.batch_size` sets the source fetch size, and `parallel_scan` with `strategy: "keyset"` and `partitions > 1` splits the table into non-overlapping `partition_column` ranges; single-node splits are scanned up to `execution.concurrent_partitions` (capped at `source.pool_max`) at a time, and `rel distribute` spreads them across Ballista workers. `rel plan` prints each filter's pushdown decision — through the same schema-coerced filter path as a run — and previews at most `--limit` rows (default 20).

Filters come in two forms that lower to the same predicate — structured objects
(the recommended JSON form) or shorthand strings (handy for `--filter` flags):

```json
"filters": [
  { "column": "status", "op": "=", "value": "PAID" },
  { "column": "amount", "op": ">", "value": 100 },
  "updated_at>=2026-01-01T00:00:00Z"
]
```

Values follow JSON types (number → int/float, boolean, string, null; `is_null` /
`is_not_null` need no value). RFC3339 strings on timestamp columns coerce to real
timestamp literals that push to the source. Programmatically, `connector.pipeline()`
exposes `filter_exprs()` (parsed predicates) and `explain_filters()` (per-filter
pushdown preview — the same decision `scan()` uses). A shorthand value in quotes
stays a string (`zip='007'` compares against the text `007`, not the integer 7).
Split progress of `run_with` jobs is inspectable via `rel checkpoint show --config <path>`
and resettable via `rel checkpoint reset --config <path>`.

The job spec is strict: unknown fields anywhere (including a leftover `"sink"` or
`"watermark"` block) are a load error, `parallel_scan.strategy` must be one of
`none`/`keyset`/`ctid` and `pushdown.policy` one of
`always`/`never`/`cost_based`/`strict`/`hinted` (exact lowercase), `job_id` must be
non-empty (max 128 bytes, no control characters or surrounding whitespace), and every
construction path validates the values (`batch_size >= 1`, …).

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

`run_with` holds an exclusive per-job lock file (heartbeat; a stale lock is taken over after `checkpoint.lock_ttl_secs`, default 1800), binds the checkpoint to a fingerprint of the plan (table, schema, projection, resolved filters, strategy, partitions, partition column), stores each split's key range and reuses those ranges on retry, runs pending splits concurrently and records a failed split without stopping the others (the run then fails with the list of failed split ids). Re-running a job id with a different plan (e.g. a new filter) is a typed `PlanMismatch` error — use a new `job_id` or `rel checkpoint reset`. Delivery is at-least-once per split: a split that failed mid-stream is re-delivered in full. A distributed run is one split. `.distributed()` defaults to the standard scheduler URL (`http://localhost:50050`) unless the config or `.scheduler(url)` sets one, and `.in_process()` runs a local Ballista cluster (one executor task slot per budgeted source connection); against a remote scheduler, each worker process must run with the same `distributed.workers` and at most `pool_max / workers` concurrent tasks (not verified — logged as a warning). All Postgres code lives under `src/connector/postgres/` (see AGENTS.md — Postgres connector modularization). See [`examples/pipeline_extraction.rs`](examples/pipeline_extraction.rs).


---


## Logging

The crate logs through the standard [`log`](https://docs.rs/log) facade. The `rust-ballista-extraction-layer` binary and every example install a `fern` backend at startup (`logging::init_from_env_and_args`) that writes to **stderr** and, optionally, to a **file**. Configuration is read from the command line and the environment (never the job JSON):

| Setting | CLI flag | Env var | Default |
| ------- | -------- | ------- | ------- |
| Level   | `--log-level <off\|error\|warn\|info\|debug\|trace>` | `RUST_LOG` | `info` |
| File    | `--log-file <path>` | `REL_LOG_FILE` | none (stderr only) |

CLI flags take precedence over environment variables. When a file is given, its parent directories are created and logs are **appended**.

At **`debug`** level, every generated SQL query is logged as it is produced — the query builders, the cursor extractor paths, and the DataFusion execution plan all emit the final SQL text through `log::debug!`. So pointing a log file at a path with debug level captures each query:

```bash
# Every generated query goes to queries.log (place flags after the subcommand)
cargo run --bin rust-ballista-extraction-layer -- run \
  --config examples/configs/extract.example.json \
  --log-level debug --log-file logs/run.log

# Equivalent via environment
RUST_LOG=debug REL_LOG_FILE=logs/run.log \
  cargo run --bin rust-ballista-extraction-layer -- demo
```

A debug line looks like:

```
  2026-09-16T08:12:04.531Z [DEBUG] rust_ballista_extraction_layer::connector::postgres::extractor: generated query [full]: SELECT "order_id", ... FROM "public"."orders"
```

> Note: `rel` with no arguments prints usage; the demo pipeline runs only as `rel demo` (it reads the database password from `PGPASSWORD`). Global flags go after the subcommand (a leading `--log-file` would be treated as a subcommand).

---


## Current Scope


**Phases 1–4 implemented** — PostgreSQL connector (binary-format cursor `FETCH` and binary `COPY` scans, one decoder per column, typed errors for unrepresentable values such as `±infinity` or NUMERIC overflow), full/filtered extraction with split-execution checkpointing (`run_with`: plan fingerprint, stored split bounds, per-job lock), cost-based pushdown (`always`/`never`/`cost_based`/`strict`/`hinted`) with keyset/`ctid` partitioning, bounded-memory streaming on the scan paths (`stream()`, `run_with`, `execute_stream()`; `batch_size` 8192 and a `max_batch_bytes` cap), and distributed execution (Ballista scheduler/workers, serializable plans, budgeted pools). Materializing helpers (`collect()`, `PostgresExtractor::extract_full_table`) remain for small results. Connector SPI (`SourceDescriptor`, `TableStatsSource`, `SqlDialect`/`Predicate::render_to`) is shared; PostgreSQL is the implemented connector and MySQL a prototype.

See [`docs/architecture.md`](docs/architecture.md), [`docs/pushdown.md`](docs/pushdown.md), and [`docs/connectors/postgres.md`](docs/connectors/postgres.md) for details. The watermark/backfill design is deferred under `docs/deferred/`.


**Testing**

215 library unit tests (no database) cover pushdown translation/policy/cost, strict config parsing, type mapping and decoding, partition math, split checkpoints and the job lock, filter lowering, and codecs; 48 doc tests cover the `# Examples` (47 run or compile-check, 1 is an ignored sketch). 88 integration tests in 15 files (`tests/*.rs`) run against the Docker compose stack (`tests/docker/compose.yaml`; `scripts/e2e.sh` handles up/down) with a deterministic hostile fixture (`tests/data/hostile.sql`: NULLs, `''` vs NULL, MIN/MAX ints, bytea `0x00`/`0xFF`, 1970/2038/9999 timestamps, a 5-row timestamp tie, enum, arrays) and the dvdrental dataset on both engines; they verify decode fidelity, cursor vs COPY, pushdown (`always` vs `never` differential), checkpoint retry and crash recovery, 1 vs 3 workers, and MySQL. Counts are from `cargo test … -- --list` on 2026-09-24; all pass with `cargo test --tests -- --test-threads=1`. See [`docs/testing-plan.md`](docs/testing-plan.md) for the per-file matrix and oracles.


**Deferred**


* Python/PyO3 wrapper
* Log-based CDC
* Additional connectors beyond PostgreSQL
* Metrics and tracing
* Output writers (sinks) — writing the batches is the caller's job


---


## Benchmark Summary


Full initial load, Rust vs PySpark on the same Postgres, both containerized.
Spec (equal-spec rule — a number is only quotable with all of this attached):
10M rows, best of 3, tool-default batching, containers pinned to cores 0-3,
bench-pg 2g, 157/157 scan fan-out both sides. **Caveat:** these numbers were recorded while
`benchmark/run.sh` started bench-pg under a `--cpus=4` quota instead of its documented 4-5 pin
(since restored), so the source database may have shared cores with the engines; re-run before
quoting them.


| Component | Version |
|---|---|
| Rust / DataFusion / Ballista | 1.98.1 / 54.1.0 / 54.1.0 |
| PySpark | 3.5.4 |
| PostgreSQL | 17.11 |


Scenarios: `full` (`SELECT *`, all 12 columns — pure extraction throughput) and
`selective` (`SELECT order_id,amount,status … WHERE status = 'REFUNDED'`,
~3% of rows via index — filter + projection pushdown end to end).


Headline results at the default 4g engine budget (engine totals; `postgres`
sub-rows are source-database cost on the same ticks, never summed into engine
totals — the way to tell a pushdown win from a fast scan):


| engine                   | scenario  | component         | elapsed_ms | avg_cpu_pct | peak_cpu_pct | peak_rss_mib |
|--------------------------|-----------|-------------------|------------|-------------|--------------|--------------|
| rust-ballista-remote     | full      |                   | 19461      | 269.1       | 400.0        | 1646         |
|                          |           | `postgres`        |            | 52.7        | 124.0        | 2048         |
| rust-ballista-standalone | full      |                   | 27296      | 185.1       | 221.3        | 1477         |
|                          |           | `postgres`        |            | 40.1        | 71.5         | 2048         |
| pyspark-3.5.4            | full      |                   | 21704      | 246.8       | 391.1        | 3154         |
|                          |           | `postgres`        |            | 62.5        | 138.0        | 2048         |
| rust-ballista-remote     | selective |                   | 1573       | 62.0        | 80.5         | 2987         |
|                          |           | `postgres`        |            | 137.9       | 195.1        | 2048         |
| rust-ballista-standalone | selective |                   | 10249      | 12.3        | 20.6         | 22           |
|                          |           | `postgres`        |            | 21.6        | 72.6         | 2048         |
| pyspark-3.5.4            | selective |                   | 5990       | 216.2       | 330.7        | 641          |
|                          |           | `postgres`        |            | 33.2        | 125.2        | 2048         |


**Takeaways** (averaged across 2g/4g/8g engine budgets):

- **Full extraction:** distributed Rust ~19.5 s, PySpark ~21.5 s, standalone Rust ~27.3 s.
- **Selective extraction:** distributed Rust ~1.5 s, PySpark ~5.5 s, standalone Rust ~10.2 s.
- **Memory (full):** distributed Rust ~1.5–1.65 GiB vs PySpark 2.0–4.3 GiB (heap-dependent).
- **Budgets 2g → 8g barely moved times** — the workloads are not memory-bound.

> These are measurements of the current implementation on this workload/hardware — not a general Rust-vs-PySpark claim. Never compare `output_bytes` across engines (different Parquet writers); the gate compares row counts and row *multisets*.

See [`benchmark/README.md`](benchmark/README.md) for the 2g/8g tables, per-container breakdowns, correctness checks, and the full equal-spec methodology.


---


## AI-Assisted Development


AI tools were used throughout the project for implementation, code exploration, debugging, and iteration.


The overall architecture, requirements, design decisions, technical trade-offs, and implementation direction were under my technical direction. AI-generated output was reviewed and tested rather than treated as automatically correct.


---


## Documentation


| Document                                                           | What it covers                                                                   |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------- |
| [`docs/roadmap.md`](docs/roadmap.md)                               | Phased delivery plan                                                             |
| [`QUICKSTART.md`](QUICKSTART.md)                                       | Setup, configuration, distributed execution, benchmarks, and testing             |
| [`docs/architecture.md`](docs/architecture.md)                     | Module layout, plan lifecycle, Arrow data model, execution and memory management |
| [`docs/connectors/README.md`](docs/connectors/README.md)           | Connector SPI, capabilities, scan planning, partitioning, and type mapping       |
| [`docs/connector-abstraction.md`](docs/connector-abstraction.md)   | Multi-connector plan: what is shared, what stays per backend, migration steps    |
| [`docs/connectors/postgres.md`](docs/connectors/postgres.md)       | PostgreSQL-specific implementation details                                       |
| [`docs/connectors/mysql.md`](docs/connectors/mysql.md)             | MySQL prototype status and design reference                                      |
| [`docs/pushdown.md`](docs/pushdown.md)                             | Pushdown rules, semantic compatibility, cost model, and policy engine            |
| [`docs/deferred/incremental-extraction.md`](docs/deferred/incremental-extraction.md) | Deferred watermark/backfill design (out of scope)              |
| [`docs/testing-plan.md`](docs/testing-plan.md)                     | Test files, what each proves and its oracle, how to run, measured counts         |
| [`docs/python-bindings.md`](docs/python-bindings.md)               | Future PyO3 wrapper design                                                       |


---


## What This Project Does Not Try to Be


* A Spark replacement or general-purpose cluster platform.
* A database or storage engine.
* A log-based CDC platform.
* A benchmark claiming a fixed performance multiplier over PySpark.
* A writer (sink) for downstream storage systems — it hands out Arrow batches and stops.


The focus is the extraction layer between operational databases and analytical execution.


---


## License


Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE) for details.


The project builds on [Apache DataFusion](https://github.com/apache/datafusion) and [Apache Arrow](https://github.com/apache/arrow-rs), both Apache 2.0 licensed.