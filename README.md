# El Ballista

**Extraction Layer on Ballista**

A Rust-native, source-aware extraction layer built on top of Apache DataFusion and Ballista.

El Ballista extracts data from source databases into bounded Apache Arrow `RecordBatch` streams, with source-aware predicate pushdown, partitioned extraction, and checkpointing for distributed execution.

The layer handles source-aware pushdown, extraction planning, split checkpointing, and streaming data as Arrow `RecordBatch`es. Operations can run in the source database or in DataFusion based on connector capabilities, statistics, and policy. Full and filtered extraction are the supported patterns — incremental and backfill use cases are expressed as caller-provided filter predicates, with no watermark state in the extraction layer. Ballista is used when distributed execution is configured.

> **Status (September 2026):** Phases 1–4 implemented and tested against live PostgreSQL. See [`docs/roadmap.md`](docs/roadmap.md) for the roadmap and [`docs/testing-plan.md`](docs/testing-plan.md) for test coverage.
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
`SendableRecordBatchStream`. Data stays in Arrow's columnar format throughout — no copying or conversion as it flows into DataFusion.


Ballista can provide distributed execution when configured. The project does not implement its own sink layer; examples that need to materialize results locally use DataFusion writers or `parquet::arrow::ArrowWriter`.


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


For example, database collation can affect string comparison semantics. A case-insensitive source comparison such as `status = 'PAID'` may also match `'paid'`, while Arrow's comparison does not necessarily behave the same way. Such a predicate must therefore remain `Inexact`.


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
  filter status = 'PAID' -> pushed_to_source=true (PUSH (Inexact; index available: orders_status_idx; ("status"::text = 'PAID')))
  filter amount > 100 -> pushed_to_source=false (KEEP (selectivity too high: 33.00% >= 30.00%))
  filter updated_at >= '2026-01-01T00:00:00Z' -> pushed_to_source=true (PUSH (Exact; index available: orders_updated_at_idx; ("updated_at" >= '2026-01-01T00:00:00+00:00'::timestamptz)))
  filter user_id >= 500 -> pushed_to_source=true (PUSH (Exact; index available: orders_user_id_idx; ("user_id" >= 500)))

► Step 3: Extract with pushdown
  ✓ Filtered extraction complete: 28 row(s) in 1 batch(es)

► Step 4: Operational run (split checkpointing)
2026-09-22T00:21:25.295Z [INFO ] rust_ballista_extraction_layer::checkpoint::json_store: job 'orders_extract' split 'split-0' completed (rows_extracted=28)
  ✓ Run outcome: 28 row(s), splits 1/1

═══════════════════════════════════════════════════════════
  ✓ Filtered extraction example complete
═══════════════════════════════════════════════════════════
```

What this shows: the orchestrator supplies predicates (Step 1); the layer
decides per predicate whether the source can answer it exactly, approximately
(`Inexact`, re-checked in Arrow), or not at all (Step 2 — note `amount > 100`
stays in Arrow on cost grounds while the indexed predicates push); extraction
returns Arrow batches (Step 3); the operational run tracks the split so a
retry skips it (Step 4).


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


The result is a stream of Arrow `RecordBatch`es that can be consumed by DataFusion or another downstream component.


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


**TL;DR — single-node extraction:**


```bash
docker run -d --name pg \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  postgres:17


export ORDERS_PG_PASSWORD=postgres


cargo run --bin rust-ballista-extraction-layer -- run \
  --config examples/configs/extract.example.json
```


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

`rel run` extracts the full table by default; add `filters` to the config (or pass `--filter 'col=value'` flags) for a filtered extraction whose predicates push to the source when possible (see [`examples/configs/extract.example.json`](examples/configs/extract.example.json)). At `--log-level debug` the generated `SELECT ... FROM <table>` is logged with strategy `full` or `full+pushdown`. `execution.batch_size` sets the server-side cursor FETCH size, and `parallel_scan` with `strategy: "keyset"` and `partitions > 1` splits the table into non-overlapping `partition_column` ranges and extracts each — sequentially on single-node `rel run`, and across workers on `rel distribute`. `run()` records per-split progress so a retry skips completed splits.

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
pushdown preview — the same decision `scan()` uses). Split progress is inspectable
via `rel checkpoint show --config <path>` and resettable via `rel checkpoint reset`.

Both the CLI and library callers go through one path: a `Pipeline` built from a `JobConfig`. `Pipeline::from_config_file(path)` (or `from_config(cfg)`) parses the config into a runnable job; `extract()` returns the Arrow `RecordBatch`es with no checkpoint side effects, while `run()` performs the operational job (split-execution checkpointing) and returns a `RunOutcome`. The connector is the single entry point: `PostgresConnector::from_config(cfg).extract()` then `.standalone()` or `.distributed()`, finishing with `.collect()` (Arrow batches) or `.run()` (operational job). `.distributed()` defaults to the standard scheduler URL (`http://localhost:50050`) unless the config or `.scheduler(url)` sets one, and `.in_process()` runs a local Ballista cluster. `rel run` and `rel distribute` are thin wrappers over this builder. All Postgres code lives under `src/connector/postgres/` (see AGENTS.md — Postgres connector modularization). See [`examples/pipeline_extraction.rs`](examples/pipeline_extraction.rs).


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

# Equivalent via environment (also works for the no-arg demo pipeline)
RUST_LOG=debug REL_LOG_FILE=logs/run.log \
  cargo run --bin rust-ballista-extraction-layer
```

A debug line looks like:

```
  2026-09-16T08:12:04.531Z [DEBUG] rust_ballista_extraction_layer::connector::postgres::extractor: generated query [full]: SELECT "order_id", ... FROM "public"."orders"
```

> Note: with the no-argument **demo** pipeline, pass level and file via the `RUST_LOG` / `REL_LOG_FILE` environment variables (a leading `--log-file` would be treated as a subcommand).

---


## Current Scope


**Phases 1–4 implemented** — PostgreSQL connector (cursor streaming, type mapping), full/filtered extraction with split-execution checkpointing, cost-based pushdown (`always`/`never`/`cost_based`/`strict`/`hinted`) with keyset/`ctid` partitioning, bounded-memory streaming (`RowBatchBuilder`, `batch_size` 8192), and distributed execution (Ballista scheduler/workers, serializable plans, budgeted pools). Connector SPI (`SourceDescriptor`, `TableStatsSource`, `SqlDialect`/`Predicate::render_to`) is ready for additional backends; PostgreSQL is the implemented connector.

See [`docs/architecture.md`](docs/architecture.md), [`docs/pushdown.md`](docs/pushdown.md), and [`docs/connectors/postgres.md`](docs/connectors/postgres.md) for details. The watermark/backfill design is deferred under `docs/deferred/`.


**Testing**

129 library unit tests cover pushdown, config/filter deserialization, type mapping, partitioning, split checkpoints, filter lowering and timestamp coercion, and codecs. Integration and e2e tests run against the Docker compose stack (`tests/docker/compose.yaml`; `scripts/e2e.sh` handles up/down automatically) with a deterministic hostile fixture (NULLs, distinct types, enum, arrays, edge timestamps) and verify extraction, filtered extraction, pushdown, split retry, and distributed execution. See [`docs/testing-plan.md`](docs/testing-plan.md) for the full matrix and how to run (`cargo test --test pg_*`, `cargo test --test e2e`).


**Deferred**


* Python/PyO3 wrapper
* Log-based CDC
* Additional connectors beyond PostgreSQL
* Metrics and tracing
* Sink implementations


---


## Benchmark Summary


Full initial load, Rust vs PySpark on the same Postgres, both containerized.
Spec (equal-spec rule — a number is only quotable with all of this attached):
10M rows, best of 3, tool-default batching, containers pinned to cores 0-3,
bench-pg fixed on cores 4-5 + 2g, 157/157 scan fan-out both sides.


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

> These are measurements of the current implementation on this workload/hardware — not a general Rust-vs-PySpark claim. Never compare `output_bytes` across engines (different Parquet writers); the gate compares row *sets*.

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
| [`docs/connectors/postgres.md`](docs/connectors/postgres.md)       | PostgreSQL-specific implementation details                                       |
| [`docs/connectors/mysql.md`](docs/connectors/mysql.md)             | MySQL prototype status and design reference                                      |
| [`docs/pushdown.md`](docs/pushdown.md)                             | Pushdown rules, semantic compatibility, cost model, and policy engine            |
| [`docs/deferred/incremental-extraction.md`](docs/deferred/incremental-extraction.md) | Deferred watermark/backfill design (out of scope)              |
| [`docs/testing-plan.md`](docs/testing-plan.md)                     | Unit and integration testing                                                     |
| [`docs/python-bindings.md`](docs/python-bindings.md)               | Future PyO3 wrapper design                                                       |


---


## What This Project Does Not Try to Be


* A Spark replacement or general-purpose cluster platform.
* A database or storage engine.
* A log-based CDC platform.
* A benchmark claiming a fixed performance multiplier over PySpark.
* A sink implementation for every downstream storage system.


The focus is the extraction layer between operational databases and analytical execution.


---


## License


Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE) for details.


The project builds on [Apache DataFusion](https://github.com/apache/datafusion) and [Apache Arrow](https://github.com/apache/arrow-rs), both Apache 2.0 licensed.