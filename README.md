# El Ballista

**A source-aware extraction layer on Apache DataFusion and Ballista, written in Rust.**

[![CI](https://github.com/jayamanikharyono/rust-ballista-extraction-layer/actions/workflows/ci.yml/badge.svg)](https://github.com/jayamanikharyono/rust-ballista-extraction-layer/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

El Ballista reads tables from PostgreSQL into bounded streams of Apache Arrow `RecordBatch`es.
For every filter it decides whether Postgres or DataFusion should evaluate it, splits large
tables into checkpointed key ranges, and runs either in one process (plain DataFusion) or on
a Ballista cluster.

> **Status (September 2026):** experimental. PostgreSQL connector implemented and tested
> against live Postgres 17; MySQL is a prototype (schema read + full-table extract only).
> See [`docs/roadmap.md`](docs/roadmap.md).

---

## Why this exists

El Ballista is an experiment on the extraction step: what does an extraction layer look like
when it is built as part of a query engine rather than in front of one? It knows what the
source can do, emits Arrow, and decides filter by filter whether Postgres or DataFusion
should do the work.

The questions it explores:

- **Where should each operation run?** Choosing between source and engine is a planning
  decision. Correctness comes first: would Postgres return exactly what Arrow would? Cost
  comes second: does pushing the filter actually save anything? Every filter's decision
  comes with its reason.
- **Can extraction be Arrow from the first byte?** Postgres's binary format is decoded
  straight into Arrow builders, so DataFusion consumes the batches without a second
  conversion, and memory stays bounded regardless of table size.
- **Can a long extraction be retried safely?** The table is split into key ranges. Each
  split is checkpointed only after the consumer confirms it, so a failed run resumes where
  it stopped.
- **When does distributing actually help?** The same job config runs standalone or on a
  Ballista cluster, and the source's connection budget stays fixed either way.

Out of scope on purpose: writing the data (sinks), watermark / incremental state, and CDC.
The orchestrator owns those.

**Why Rust?** Partly JVM fatigue: sizing heaps and GC, waiting on JVM start-up, and tuning
JDBC fetch sizes, all for a job that mostly moves bytes from a socket to a file. Partly to
see how far a Rust stack on DataFusion and Ballista gets when there is no garbage collector
and memory is bounded by design. The [benchmark](#benchmark) is the answer so far.

**Status:** experimental and learning-driven. No production users, no roadmap promises.
Related work: [ConnectorX](https://github.com/sfu-db/connector-x) (fast partitioned
DB → Arrow loads), [datafusion-table-providers](https://github.com/spiceai/datafusion-table-providers)
(DataFusion pushing queries into Postgres and other sources), and dlt / Sling / ingestr for
end-to-end ingestion.

---

## At a glance

50M-row initial load from Postgres into one Parquet file, every engine containerized on the
same 4 cores ([full method](#benchmark)):

| | Rust standalone | Rust distributed | PySpark 3.5.4 |
|---|---|---|---|
| Full load (12 columns) | **33.7 s**, 190 MiB | 66.9 s, 288 MiB | 150.4 s, 3.7 GiB |
| Selective (~3% of rows, 3 columns) | **8.3 s**, 31 MiB | 9.1 s, 70 MiB | 19.5 s, 541 MiB |

Time is end to end; memory is peak container working set at a 4g budget. Distributed runs a
scheduler and 3 workers on the same 4 cores, so on one host it pays shuffle and network
overhead by design.

**Is it for you?** It is a **Rust library**, not a finished ingestion tool:

- Source: PostgreSQL (MySQL prototype).
- Output: Arrow batches handed to your code. No built-in sink; two examples show writing Parquet.
- Incremental loads: you pass the range as a filter; it pushes to Postgres when an index
  serves it or the column statistics show the window is selective. The layer keeps no
  watermark state.
- No Python bindings yet ([design](docs/python-bindings.md)).

---

## Architecture

```text
   Job JSON / DataFrame API
              │
              ▼
┌───────────────────────────────┐
│          El Ballista          │
│  extraction planning          │
│  source-aware pushdown        │──── SQL (pushed filters, projection, key ranges) ───► PostgreSQL
│  split checkpoints            │◄─── binary COPY / cursor FETCH ─────────────────────┘
└──────────────┬────────────────┘
               │  Arrow RecordBatch stream (decoded once, no second conversion)
               ▼
   DataFusion  (in-process)   or   Ballista scheduler + workers
               │
               ▼
   your consumer (the examples write Parquet)
```

It builds on existing engines instead of writing one:

| Part | Provided by |
|---|---|
| Query planning, execution, expressions, joins, aggregation | Apache DataFusion |
| Columnar data and memory format | Apache Arrow |
| Distributed scheduling and shuffle | Apache Ballista |
| PostgreSQL connector (binary decoding, COPY and cursor scans) | **This project** |
| Source capability rules and cost-based pushdown | **This project** |
| Keyset / `ctid` partitioning and split checkpointing | **This project** |
| Job configuration, CLI, scheduler/worker binaries with Postgres plan codecs | **This project** |

Versions: DataFusion / Ballista `54.1.0`, Arrow `58.4`. See
[`docs/architecture.md`](docs/architecture.md) for the module layout and execution flow.

---

## How it works

### Pushdown: push, keep, or never

Every filter goes through two checks. **Can** the source evaluate it with the same result
as Arrow? If not, it is never pushed. **Should** it? That is decided by the deny list, the
policy mode (`always` / `never` / `cost_based` / `strict` / `hinted`), then how selective the
filter is and a cost budget. Pushed filters are reported to DataFusion as `Exact` (DataFusion
skips its own filter) or `Inexact` (the source pre-filters, DataFusion re-checks).

[`examples/configs/pushdown_showcase.json`](examples/configs/pushdown_showcase.json) runs
against `payment` in the dvdrental demo database (14,596 rows):

```json
"filters": [
  { "column": "customer_id",  "op": ">=", "value": 300 },
  { "column": "payment_date", "op": ">=", "value": "2007-04-06T00:00:00Z" },
  { "column": "payment_date", "op": "<",  "value": "2007-04-07T00:00:00Z" },
  { "column": "staff_id",     "op": "!=", "value": 1 },
  { "column": "amount",       "op": ">",  "value": 5 },
  { "column": "rental_id",    "op": ">=", "value": 1000 }
],
"pushdown": { "policy": "cost_based", "deny": ["rental_id"] }
```

```bash
cargo run --bin rust-ballista-extraction-layer -- plan \
  --config examples/configs/pushdown_showcase.json
```

| Filter | Decision | Why |
|---|---|---|
| `customer_id >= 300` | **Push** | Integer range, same result in both; Postgres plans it on `idx_fk_customer_id` |
| `payment_date >= …` and `< …` | **Push** (both) | No index on `payment_date`, but judged together the two sides are a one-day window: 2.91% of rows by the column's histogram |
| `staff_id != 1` | **Keep** (cost) | Could be pushed exactly, but is estimated to keep 50% of rows (over the 30% threshold). Pushing saves almost nothing, so Postgres just streams |
| `amount > 5` | **Never** (correctness rule) | `numeric` has no source form yet that is proven to compare exactly like Arrow's `Decimal128` |
| `rental_id >= 1000` | **Keep** (policy) | `rental_id` is on the job's deny list; the operator's rule wins over the cost model, even with an index |

Other rules that never push: float comparisons other than `=` (Postgres treats `-0 = 0`),
arithmetic (overflow behaves differently), `NOT` over an approximate child, `LIKE`, `IN`,
and any comparison where the column itself is wrapped in a cast (casts and aliases in the
projection are always DataFusion's job). Text is always compared under `COLLATE "C"`, so a
case-insensitive database collation cannot change the result.

**Incremental windows.** Timestamp and date comparisons push as plain column comparisons, so
an index on the watermark column serves them. Without an index, a range is estimated from the
column's `pg_stats` histogram and most-common values, and the two sides of a window are judged
together, as in the `payment_date` rows above:

```text
payment_date >= '2007-04-06T00:00:00Z' -> PUSH (Exact; low selectivity (2.91% from histogram, window of 2 range filters) ...)
payment_date < '2007-04-07T00:00:00Z'  -> PUSH (Exact; low selectivity (2.91% from histogram, window of 2 range filters) ...)
```

Judged alone, `payment_date >= '2007-04-06'` covers about half the table and would stay in
DataFusion.

**Why `status = 'REFUNDED'` stays in DataFusion in the benchmark:** `status` is an enum on the
benchmark's `orders` table, so to compare exactly like Arrow the connector renders it as
`CAST(status AS text) COLLATE "C"`. A cast expression cannot use the plain `orders_status_idx`.
With no index to use, the cost model estimates the filter keeps 1 in 5 rows (1 / number of
distinct values = 20%, under the 30% threshold) and prices it as table size × 20%. On the
20k-row seed table that is ~848, inside the 50,000 budget, so it would push. On the 50M-row
benchmark table the same full scan is far over budget, so it stays in DataFusion.

**Known limits / next:** exact `numeric(p ≤ 38)` comparisons; equality estimates from
most-common values (today `1 / n_distinct`: 20% for both PAID, really ~55%, and REFUNDED,
really ~3%); recognising an expression index on `(status::text COLLATE "C")` so enum filters
can use an index.

Full rules, cost model and policies: [`docs/pushdown.md`](docs/pushdown.md).

### Full and filtered extraction, with split checkpoints

The orchestrator decides **what** range to extract; the layer decides **how**. A full load
has no filters; an incremental or backfill run is a filtered extraction over a time range.
With `parallel_scan.strategy: "keyset"` the table is split into non-overlapping key ranges
(never `LIMIT/OFFSET`), scanned concurrently within `source.pool_max` connections.

The operational entry point, `run_with(consumer)`, hands each split's stream to your code and
records the split as completed only after your consumer returns `Ok`. The checkpoint is bound
to a fingerprint of the plan (changing a filter is a `PlanMismatch` error, not a silent mix),
split ranges are stored and reused on retry, and a per-job lock prevents two runs of the same
job. Delivery is at-least-once per split.

Each run also leaves a **run report**, `<checkpoint.dir>/runs/<job>/<run_id>.json`. The
checkpoint says what is left to do and is rewritten as splits finish; the report records what
one run did and is kept after later runs: the plan fingerprint, every filter's pushdown
decision with its reason, each split's outcome, rows, bytes and time (or error), and the
totals. The `run_id` is the same one every source query of the run carries in its SQL comment,
so a report lines up with Postgres logs. `rel runs list` and `rel runs show` read them.

### Standalone or distributed

| | Standalone | Distributed |
|---|---|---|
| What runs | DataFusion inside your process | Your process plans; `rel scheduler` + `rel worker`s execute |
| Postgres connections | up to `pool_max` | `pool_max / workers` per worker (total stays `pool_max`) |
| When to use | Default. One machine, every core, no scheduling overhead | When one machine's CPUs are the bottleneck |

Stock Ballista executors cannot decode the Postgres scan plans, so the crate ships its own
scheduler and worker binaries. A watchdog cancels and re-submits a job whose worker died,
because Ballista 54 on its own would leave it "Running" forever. Deployment, flags and
watchdog details: [`docs/running.md`](docs/running.md#execution-modes-standalone-vs-distributed).

---

## Getting started

Needs Rust stable (edition 2024) and Docker. There are two datasets, kept apart on purpose:

- **Demo and tests:** the dvdrental sample database, from `tests/docker/compose.yaml`. The
  examples, the `rel` commands below, the integration tests and CI all use it.
- **Benchmark:** a synthetic `orders` table, loaded and grown to 50M rows only inside the
  benchmark's own container by [`benchmark/run.sh`](benchmark/README.md).

```bash
# Demo database: Postgres 17 on localhost:5432, database `test`, dvdrental (also MySQL 8 on 3306)
docker compose -f tests/docker/compose.yaml up -d --wait
export PGPASSWORD=postgres

# See where each filter will run
cargo run --bin rust-ballista-extraction-layer -- plan \
  --config examples/configs/pushdown_showcase.json

# Diagnostic run: scans with pushdown, counts rows, discards them
cargo run --bin rust-ballista-extraction-layer -- run \
  --config examples/configs/extract.example.json
# -> job 'payment_extract' (filtered, diagnostic): counted 422 row(s) in 1 split(s); ...

# Real run: write the table to Parquet (one file per split) with a checkpoint and a run report
cargo run --release --example parquet_export -- examples/configs/full_extract.example.json output
# -> job 'payment_full': 14596 row(s) written to output/payment_full (1 of 1 split(s) this run, ...)
# -> run r_06e579f7: report ./.checkpoints/runs/payment_full/r_06e579f7.json

# The run reports of that job
cargo run --bin rust-ballista-extraction-layer -- runs list \
  --config examples/configs/full_extract.example.json
```

`rel run` and `rel distribute` are diagnostic: they deliver no data and write no checkpoint.
Real jobs use the library's `run_with` (below), as `parquet_export` does. Re-running the same
job skips the splits already completed; for a fresh export use a new `job_id` or
`rel checkpoint reset --config …`. (`rel` = the `rust-ballista-extraction-layer` binary.)

### Examples

| Example | What it shows |
|---|---|
| `full_extraction` | Whole-table extraction, schema discovery, type mapping into Arrow |
| `filtered_extraction` | Caller-provided filters and the per-filter pushdown preview |
| `pipeline_extraction` | The connector API and all four terminals (start here to learn the API) |
| `parquet_export` | A real `run_with` job: one Parquet file per split, checkpointed, with a run report |
| `dataframe_extraction` | DataFusion DataFrame and SQL over the source table |
| `parallel_extraction` | Keyset partitioning across concurrent scans |
| `distributed_extraction` | The same job on a Ballista cluster, streamed to Parquet |

Setup details, the job spec in 60 seconds and common errors: [`QUICKSTART.md`](QUICKSTART.md).

---

## API

One entry point for library callers and the CLI:

```rust
let outcome = PostgresConnector::from_config(config)?
    .extract()
    .standalone()                       // or .distributed().workers(3)
    .run_with(|split, mut stream| async move {
        while let Some(batch) = stream.try_next().await? {
            // write `batch` for `split.split_id` somewhere durable, idempotently per split
        }
        Ok(())
    })
    .await?;
```

| Terminal | Returns | Checkpoints | Memory |
|---|---|---|---|
| `.run_with(consumer)` | `RunOutcome` | yes, per split, after the consumer returns `Ok` | bounded |
| `.stream()` | `SendableRecordBatchStream` | no | bounded |
| `.collect()` | `Vec<RecordBatch>` | no | whole result |
| `.run()` | row counts | no (diagnostic) | bounded |

`RunOutcome` carries the run's `run_id`, its report (`outcome.report`, also when report files
are off) and the report file path. A run that fails returns `AppError::RunFailed` with the same
three (`err.run_id()`, `err.run_report()`, `err.run_report_path()`); `err.underlying()` is the
error to match on, such as `SplitsFailed` or `PlanMismatch`.

Or query the source through DataFusion directly:

```rust
let ctx = ExtractContext::from_config(config).await?;
let batches = ctx
    .source("postgres", "public.orders").await?
    .filter(col("status").eq(lit("PAID")))?
    .select(vec![col("order_id"), col("user_id"), col("amount")])?
    .collect().await?;
```

Full job spec, CLI flags, filter syntax and logging: [`docs/running.md`](docs/running.md).

---

## Benchmark

Initial loads from the same Postgres, both engines containerized. Equal-spec rule: a number
is only quotable with all of this attached.

- **Data:** 50M rows; 4 keyset / JDBC partitions on both sides (= engine cores); 8192 rows
  per batch / JDBC fetch on both sides.
- **Work:** one Parquet file per engine and scenario, timed end to end (program entry point
  to the last byte written; Spark's session start-up included).
- **Pinning:** engine containers on cores 0-3; Postgres on cores 4-7 with 2g.
- **Runs:** sequential, best of 3.
- **Rust modes:** standalone = plain DataFusion; distributed = scheduler + client on core 0
  and 3 workers on one core each. `pool_max` 12 in both.
- **Versions:** Rust 1.98.1, DataFusion / Ballista 54.1.0, PySpark 3.5.4, PostgreSQL 17.11.

Scenarios: `full` = all 12 columns, every row. `selective` = `status = 'REFUNDED'` (~3% of
rows) plus a 3-column projection; PySpark pushes the filter through JDBC, Rust keeps it in
DataFusion on cost grounds ([why](#pushdown-push-keep-or-never)).

Results at a 4g engine budget:

| engine | scenario | elapsed_ms | cpu_seconds | peak_mem_mib |
|---|---|---|---|---|
| rust-datafusion-standalone | full | 33702 | 52.17 | 190 |
| rust-ballista-remote | full | 66904 | 105.17 | 288 |
| pyspark-3.5.4 | full | 150437 | 212.2 | 3768 |
| rust-datafusion-standalone | selective | 8326 | 16.61 | 31 |
| rust-ballista-remote | selective | 9088 | 17.13 | 70 |
| pyspark-3.5.4 | selective | 19501 | 13.04 | 541 |

`peak_mem_mib` is the container working set, summed across containers for distributed.

- **Standalone Rust is fastest in both scenarios at every budget (2g / 4g / 8g):** 4.0–4.5×
  PySpark on the full load, 2.3–2.4× on selective.
- **Least CPU on the full load:** about half of distributed and a quarter of PySpark. On
  selective, PySpark uses the least CPU (it pushes the filter) yet takes 2.3× as long.
- **Small, flat memory:** Rust times move less than ~5% from 2g to 8g; PySpark takes what
  the budget offers.
- **Both engines fetch 8192 rows at a time.** Spark's default (no JDBC fetch size) lets the
  Postgres driver buffer a whole partition in the heap, so the fetch size is set explicitly
  to match Rust's batch size.
- **Distributed is slower on one host by design:** every row crosses a shuffle and Arrow
  Flight, and one client writes all 50M rows. It is meant to pay off when it adds machines;
  that is not measured yet.

> Measurements of this implementation on this workload and hardware, not a general
> Rust-vs-PySpark claim. See [`benchmark/README.md`](benchmark/README.md) for the 2g / 4g / 8g
> tables, memory columns, correctness checks and the full method.

---

## Testing

- **252 unit tests** (no database): pushdown translation, policy and cost, strict config
  parsing, type mapping and decoding, partition math, checkpoints and the job lock, codecs.
- **134 doc tests** on the public API examples (133 run, 1 ignored sketch).
- **98 integration tests in 16 files** against a Docker Compose stack (Postgres 17 + MySQL 8):
  a deliberately hostile fixture (NULL vs `''`, integer extremes, `0x00`/`0xFF` bytes,
  1970/2038/9999 timestamps, enums, arrays) plus the dvdrental dataset on both engines. They
  check decode fidelity, COPY vs cursor, `always` vs `never` pushdown differentials, a seeded
  property-based pushdown test, checkpoint retry and crash recovery, and 1 vs 3 workers.
- **CI** runs `fmt`, `clippy -D warnings`, and every test above on each pull request.

Counts from `cargo test … -- --list` on 2026-09-27. Per-file matrix and oracles:
[`docs/testing-plan.md`](docs/testing-plan.md).

---

## Design decisions

Things that changed along the way, and why:

- **Removed the Parquet sink.** Phase 1 shipped one. Writing data belongs to the caller or
  orchestrator; keeping it out made the layer's contract simply "Arrow batches out".
- **Removed watermark / incremental state.** Incremental and backfill runs became filtered
  extractions over a caller-supplied range. The earlier design is kept in
  [`docs/deferred/incremental-extraction.md`](docs/deferred/incremental-extraction.md).
- **No custom optimizer rule.** Push/keep decisions run inside DataFusion's own
  `supports_filters_pushdown`; an earlier `SourceAwarePushdownRule` was deleted.
- **Correctness before cost.** A filter is never pushed unless the source returns exactly
  what Arrow would, or a superset that DataFusion re-checks.
- **Standalone never touches Ballista.** Single-process runs are plain DataFusion (the
  fastest mode in the benchmark); distributed mode never silently falls back.
- **Real databases in tests.** Integration tests run against the same Docker Compose stack
  locally and in CI.

---

## What this project is not

- A Spark replacement or general-purpose cluster platform.
- A database, storage engine, or sink.
- A log-based CDC platform.
- A claim of a fixed performance multiplier over PySpark.

---

## AI-assisted development

AI tools were used throughout for implementation, code exploration, debugging and iteration.
The architecture, requirements, design decisions, trade-offs and implementation direction
were under my technical direction. AI-generated output was reviewed and tested rather than
treated as correct by default. [`AGENTS.md`](AGENTS.md) holds the correctness rules the AI
agents had to follow in this repo (Rust, Arrow/DataFusion and database semantics checked on
every change).

---

## Documentation

| Document | What it covers |
|---|---|
| [`QUICKSTART.md`](QUICKSTART.md) | Setup, examples in order, job spec basics, common errors |
| [`docs/running.md`](docs/running.md) | Full job spec, execution modes, distributed deployment and watchdog, CLI, filter syntax, logging |
| [`docs/architecture.md`](docs/architecture.md) | Module layout, job lifecycle, Arrow data model, execution and memory |
| [`docs/pushdown.md`](docs/pushdown.md) | Pushdown rules, semantic compatibility, cost model, policies |
| [`docs/connectors/README.md`](docs/connectors/README.md) | Connector SPI, capabilities, scan planning, type mapping |
| [`docs/connectors/postgres.md`](docs/connectors/postgres.md) | PostgreSQL implementation details |
| [`docs/connectors/mysql.md`](docs/connectors/mysql.md) | MySQL prototype status |
| [`docs/connector-abstraction.md`](docs/connector-abstraction.md) | Multi-connector plan |
| [`docs/testing-plan.md`](docs/testing-plan.md) | Test files, what each proves, how to run |
| [`docs/roadmap.md`](docs/roadmap.md) | Phases, their status and evidence, and what changed along the way |
| [`docs/history/`](docs/history/README.md) | The original phase plans, archived as written |
| [`docs/deferred/incremental-extraction.md`](docs/deferred/incremental-extraction.md) | Deferred watermark / backfill design |
| [`docs/python-bindings.md`](docs/python-bindings.md) | Future PyO3 wrapper design |
| [`benchmark/README.md`](benchmark/README.md) | Benchmark data, method and full results |

---

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
