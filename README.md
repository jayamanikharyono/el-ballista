# Rust Extract Layer

A Rust-native, Arrow-based **incremental extraction engine** designed to intelligently push operations into source databases when beneficial, execute analytical transformations locally through DataFusion, and distribute scans across a Ballista cluster. The system produces columnar results (Arrow `RecordBatch` streams) for consumption by DataFusion writers, Ballista, or an orchestrator.

> **Status (September 2026):** Phases 1–4 implemented and exercised against a live Postgres: single-node extraction with checkpointing, cost-based pushdown (`always`/`never`/`cost_based`/`strict`/`hinted`), true streaming with bounded memory, and distributed execution with budgeted source pools. 83 lib + 81 bin unit tests green; integration tests planned in [`docs/testing-plan.md`](docs/testing-plan.md) (Phase A done). See the [roadmap](docs/roadmap.md) for phased delivery.

---

## Experimental — Not Production Ready

This project is experimental and under active development. It is intended for learning, experimentation, and exploring the design of a distributed database extraction layer for Apache DataFusion.
Do not use this project in production blindly. The implementation has not yet been sufficiently validated for production workloads, and important concerns such as failure recovery, source-database consistency, concurrency, backpressure, performance, and operational behavior may still require further testing and hardening.
If you are evaluating this project for production use, review and validate the implementation thoroughly against your specific workload and source database before relying on it.

---

## The one-paragraph pitch

The interesting part of this project is *not* "Rust is faster than PySpark". The interesting part
is **source-aware query optimization + incremental extraction + Arrow-native execution**. A normal
JDBC-style extractor asks a database for rows and then filters them in the client. A normal
federated query engine pushes everything it can into the source. Both are wrong some of the time.
The Rust Extract Layer treats the *boundary between the database and the compute engine as a
planning decision*, made per-operator, using source capabilities, table statistics, and an explicit
cost policy.

---

## Architecture

```
              ┌─────────────────────────┐
              │     User / Pipeline     │
              │                         │
              │  Rust API  │  SQL API   │
              │  DataFrame │  job JSON  │
              └────────────┬────────────┘
                           │
                           ▼
              ┌──────────────────────────┐
              │    Rust Extract Layer    │
              │                          │
              │  Logical Plan            │
              │  Source-Aware Optimizer  │
              │  Connector (Postgres)    │
              │  Incremental Extractor   │
              │  Checkpoint Store        │
              └────────────┬─────────────┘
                           │
                           ▼
                       PostgreSQL
                           │
                           ▼
                  Arrow RecordBatch stream
                           │
                           ▼
              ┌──────────────────────────┐
              │   DataFusion execution   │
              │  (Ballista distributed   │
              │   when configured)       │
              └────────────┬─────────────┘
                           │
                           ▼
                 local Parquet (examples)
```

(MySQL appears in early sketches as the second connector; only Postgres exists. There is no
GCS/BigQuery sink — sinks are out of scope; examples write local Parquet via DataFusion
writers or `parquet::arrow::ArrowWriter`.)

### What we build vs. what we borrow

Building a Spark clone means owning a scheduler, shuffle, fault tolerance, resource management,
catalog, optimizer, planner, connectors, metrics, and a UI. We are not doing that. DataFusion is
explicitly designed as an *embeddable* query engine, so we borrow the engine and build the parts
that are actually specific to extraction.

| Concern | Owner |
| --- | --- |
| Logical/physical planning, expression eval, joins, aggregation | Apache DataFusion |
| Columnar memory model, compute kernels, IPC | Apache Arrow |
| Distributed scheduling and shuffle | Apache Ballista |
| Parquet encoding | `parquet` crate |
| **Source connectors and capability declaration** | **This project (Postgres only)** |
| **Source-aware pushdown policy and cost model** | **This project** |
| **Incremental extraction, watermarks, checkpointing** | **This project** |
| **Job specification, CLI** | **This project** |
| Sinks, partition layout, warehouse load orchestration | Delegated (DataFusion writers, Ballista, orchestrator) |

The dependency floor is DataFusion/Ballista `54.1.0` and Arrow `58.4` (the Arrow version DataFusion 54 pins).
See [`docs/architecture.md`](docs/architecture.md) for the module layout and version policy.

---

## The three ideas that matter

### 1. Extract ≠ Transform

A pipeline like this is the failure mode we are designing against:

```
Postgres  ──►  10 TB over the wire  ──►  Rust  ──►  filter  ──►  select  ──►  100 MB
```

The database is very good at index lookups, partition pruning, and projections. The Arrow engine is
very good at vectorized CPU-heavy work. The planner's job is to place each operator on the side
that will do it well:

```
DB                                   Rust / Arrow
──                                   ────────────
indexed predicates                   CPU-heavy transformations
partition pruning                    UDFs and custom logic
simple projections                   complex JSON manipulation
joins with good indexes/stats        vectorized analytics
aggregations that reduce data a lot  cross-source joins
transactional consistency            anything the DB does badly
                                     anything that steals prod CPU
```

### 2. Pushdown is a cost decision, not a capability check

The naive rule is "if the source can execute it, push it down." That rule is wrong whenever the
source *can* run the operator but will run it badly — an unindexed predicate that forces a
sequential scan on a production primary, a regex the DB evaluates row-at-a-time, a JSON extraction
that is 40× cheaper in Arrow, or a filter that removes 2% of rows and therefore saves almost no
network bytes while burning CPU that a live application needs.

So the decision is:

```
                Operator
                   │
                   ▼
        ┌──────────────────────┐
        │  Capability check    │   can the connector express it *with identical semantics*?
        └──────────┬───────────┘
                   ▼
        ┌──────────────────────┐
        │  Cost / policy model │   selectivity, index availability, source CPU budget,
        └──────────┬───────────┘   expression cost, bytes saved
          ┌────────┴────────┐
          ▼                 ▼
     push to source    keep in Arrow
```

DataFusion gives us exactly the right hook for the "identical semantics" half of this:
`TableProvider::supports_filters_pushdown` returns `Exact`, `Inexact`, or `Unsupported` per filter.
`Exact` means the engine drops its own `FilterExec`; `Inexact` means the source pre-filters but
DataFusion re-checks. That distinction is a correctness feature, and we use it aggressively —
for example, MySQL's default case-insensitive collation means `status = 'PAID'` matches `'paid'`
in the database but not in Arrow, so that predicate is pushed as **`Inexact`**, never `Exact`.

Details, including the cost model and the full semantic-divergence catalogue, are in
[`docs/pushdown.md`](docs/pushdown.md).

### 3. Incremental extraction is the actual use case

The real workload is not `SELECT * FROM orders`. It is:

```sql
SELECT order_id, user_id, amount, updated_at
FROM   orders
WHERE  updated_at >  :last_checkpoint
  AND  updated_at <= :new_checkpoint
```

run every N minutes, landing Parquet in GCS, loaded into BigQuery on a slower cadence:

```
00:00 ─┐
01:00 ─┤
02:00 ─┼──►  GCS Parquet  ──►  periodic BigQuery load / MERGE
03:00 ─┤
04:00 ─┘
```

This is dramatically cheaper than streaming every mutation into BigQuery, and it is where the
project earns its keep operationally. It is also full of correctness traps — commit-time vs.
`updated_at` skew, boundary ties at coarse timestamp precision, invisible hard deletes — which
are enumerated with mitigations in [`docs/incremental-extraction.md`](docs/incremental-extraction.md).

---

## Intended API

Two front ends over one logical plan. The DataFrame API is deliberately close to DataFusion's own,
which is in turn close to Spark/Pandas:

```rust
let ctx = ExtractContext::from_config(config).await?;

let batches = ctx
    .source("postgres", "public.orders").await?
    .incremental(Watermark::timestamp("updated_at")).await?   // resolves from the checkpoint store
    .filter(col("status").eq(lit("PAID")))?
    .select(vec![col("order_id"), col("user_id"), col("amount")])?
    .with_column("amount_usd", col("amount") * lit(2.0))?
    .limit(0, Some(1_000_000))?
    .collect().await?;   // Arrow RecordBatches — hand to a DataFusion writer or orchestrator
```

Declarative jobs, for orchestrator-driven runs, describe the same plan in JSON
(see `examples/configs/extract.example.json`):

```json
{
  "job_id": "orders_incremental",
  "table": "orders",
  "source": {
    "host": "localhost", "port": 5432,
    "user": "postgres", "password_env": "ORDERS_PG_PASSWORD",
    "database": "app", "pool_max": 8,
    "statement_timeout_ms": 300000,
    "application_name": "rust-extract-layer", "schema": "public"
  },
  "incremental": { "column": "updated_at", "safety_lag_secs": 300, "max_window_secs": 21600 },
  "checkpoint": { "dir": "./.checkpoints" },
  "pushdown": {
    "policy": "cost_based",
    "deny": [], "push": [],
    "max_source_cost": 50000, "keep_threshold": 0.30, "statistics_ttl_secs": 900
  },
  "parallel_scan": { "strategy": "none", "partitions": 1, "partition_column": "order_id" },
  "execution": { "batch_size": 8192 },
  "distributed": { "scheduler_url": "", "workers": 2 }
}
```

```bash
cargo run -- run --config examples/configs/extract.example.json
cargo run -- distribute --config examples/configs/extract.example.json --workers 2
```

---

## Examples

The `examples/` directory contains runnable pipelines against a real PostgreSQL. They are
executable documentation and smoke tests — not production entry points, and not a replacement
for the test suite ([`docs/testing-plan.md`](docs/testing-plan.md)).

| Example | What it runs |
| --- | --- |
| `full_extraction` | Whole-table load: schema discovery, type mapping, Arrow batches |
| `incremental_extraction` | Watermark windows (`updated_at > lo AND <= hi`), safety lag, checkpoints |
| `hourly_incremental` / `daily_incremental` | Scheduled-pipeline shape with checkpoint persistence and resume |
| `parallel_extraction` | Partition-aware extraction across multiple scans |
| `distributed_extraction` | Incremental job on the Ballista cluster (`<config> [workers] [scheduler-url]`) |
| `dataframe_extraction` | Phase 2 DataFrame front end (`ExtractContext`) against a job spec |
| `end_to_end` | Distributed **full load** → SQL transform → local Parquet file |

All of them need a live Postgres (connection details in `examples/configs/extract.example.json`,
password via the `password_env` variable) and, for the distributed ones, a running scheduler
plus `rel worker`s — or omit the scheduler URL for in-process standalone mode:

```bash
# Incremental job through the CLI (single-node DataFusion)
cargo run -- run --config examples/configs/extract.example.json

# Same job, distributed, in-process scheduler+executor
cargo run -- distribute --config examples/configs/extract.example.json --workers 2

# Same job, remote cluster
cargo run -- scheduler --scheduler-url localhost:50050   # terminal 1
cargo run -- worker --scheduler-url localhost:50050      # terminal 2 (+ more)
cargo run -- distribute --config examples/configs/extract.example.json --workers 2 \
    --scheduler-url http://localhost:50050

# Explain pushdown decisions for a filter
cargo run -- plan --config examples/configs/extract.example.json \
    --policy cost_based --filter 'status=PAID'
```

---

## Scope: Completed and Current

**Phase 1 (Completed):** PostgreSQL connector with schema resolution, streaming scans (cursor-based; binary-`COPY` path exists but is unused), type mapping (incl. `text[]`, enums-as-text, tz-aware timestamps), timestamp watermark mode with safe-high-watermark, checkpoint store with leases (local JSON, atomic rename), projection/filter/limit pushdown with fidelity rules, Arrow output, `run`/`plan`/`checkpoint` CLI, logging.

**Phase 2 (Completed):** DataFrame API over DataFusion's `DataFrame`. Cost-based pushdown with real statistics (`pg_stats`/`pg_class`), index metadata, EXPLAIN estimates, and policy engine (`always`/`never`/`cost_based`/`strict`/`hinted`). Real `SourceAwarePushdown` optimizer rule. Enum-vs-text normalization. Keyset + ctid partition strategies (exported snapshots deferred). Backfill with per-chunk commits under a separate namespace.

**Phase 3 (Completed):** True streaming execution with bounded memory (`RowBatchBuilder`, O(`batch_size`)), configurable `batch_size` (default 8192).

**Phase 4 (Completed):** Distributed execution via `rel scheduler`/`rel worker` (Ballista 54.1.0, codecs compiled in — stock binaries can't decode our plans). Serializable plans with magic-prefixed JSON payloads, per-process budgeted pools (`pool_max / workers`, passwords never serialized).

**Connector SPI (ready for Phase 5):** `SourceDescriptor`, `WatermarkSource`, `TableStatsSource` traits; backend-neutral SQL rendering (`Predicate::render_to` + `SqlSink` + `SqlParam` IR, `SqlDialect` conventions). Pools and `QueryBuilder` binding stay backend-concrete by design — see `src/connector/mod.rs`.

**Phase 5 (Planned):** MySQL connector to prove the SPI. Needs the `mysql` sqlx feature plus a live MySQL.

**Testing:** 83 lib + 81 bin unit tests green (see [`docs/testing-plan.md`](docs/testing-plan.md), Phase A done; integration suites planned).

**Explicitly deferred:** Python wrapper (placeholder design in [`docs/python-bindings.md`](docs/python-bindings.md)), log-based CDC, connectors beyond Postgres/MySQL, metrics/tracing, sink implementations.

---

### AI-Assisted Development

This project was developed with the assistance of AI tools for implementation, code exploration, debugging, and iteration.
The **overall system was architected and implemented under my technical direction**, including the requirements, architecture, design decisions, technical trade-offs, and implementation approach.
AI assistance does not imply that the resulting design or implementation has been automatically validated for correctness or production readiness. The code remains subject to my own review, testing, and engineering judgment.

---

## Documentation

| Document | What it covers |
| --- | --- |
| [`docs/roadmap.md`](docs/roadmap.md) | Phased delivery plan with exit criteria for Phases 1–5 |
| [`docs/phase-two-implementation-plan.md`](docs/phase-two-implementation-plan.md) | Phase 2 record: cost model, statistics, optimizer rule, front end (incl. second-pass amendment) |
| [`docs/phase-three-implementation-plan.md`](docs/phase-three-implementation-plan.md) | Phase 3 record: true streaming, RowBatchBuilder, configurable batch_size |
| [`docs/phase-four-implementation-plan.md`](docs/phase-four-implementation-plan.md) | Phase 4 record: codecs, pool registry, scheduler/worker commands |
| [`docs/testing-plan.md`](docs/testing-plan.md) | Unit + integration test strategy (Phase A done) |
| [`docs/architecture.md`](docs/architecture.md) | Module layout, plan lifecycle, Arrow data model, execution and memory management, config, observability |
| [`docs/connectors/README.md`](docs/connectors/README.md) | The connector SPI: capability declaration, scan planning, partitioning, type mapping rules |
| [`docs/connectors/postgres.md`](docs/connectors/postgres.md) | PostgreSQL specifics: session hygiene, watermark query, type mapping, statistics |
| [`docs/connectors/mysql.md`](docs/connectors/mysql.md) | MySQL **design** (no implementation): streaming protocol, collation hazards, zero-dates, GTID anchoring |
| [`docs/pushdown.md`](docs/pushdown.md) | Expression translation, `Exact`/`Inexact` rules, cost model, policy engine |
| [`docs/incremental-extraction.md`](docs/incremental-extraction.md) | Watermark modes (timestamp implemented), checkpoint protocol, correctness hazards, backfills |
| [`docs/python-bindings.md`](docs/python-bindings.md) | Deferred — placeholder design for the future PyO3 wrapper |

---

## Non-goals

- **Not a Spark replacement.** No cluster manager, no general-purpose RDD-style API, no notebook UI.
- **Not a database.** No storage layer, no transactions, no serving path.
- **Not a CDC platform** (at least initially). Log-based capture is a documented future path for
  Postgres and MySQL, not a Phase 1 deliverable; see the connector docs.
- **Not a benchmark-driven claim of "N× faster than Spark".** Removing the JVM, GC pauses, and the
  Python serialization boundary is a real structural advantage, and Arrow's columnar layout enables
  SIMD and cache-efficient execution — but the actual win is workload-dependent and we will publish
  measurements rather than multipliers.

---

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE) for details.

The project builds on [Apache DataFusion](https://github.com/apache/datafusion) and [Apache Arrow](https://github.com/apache/arrow-rs), both Apache 2.0 licensed.
