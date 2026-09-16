# Rust Ballista Extraction Layer

A Rust-native, source-aware **extraction layer that builds on DataFusion and Ballista and outputs native Arrow**.

The layer handles source-aware pushdown, extraction planning, checkpointing, and streaming data as Arrow `RecordBatch`es. Operations can run in the source database or in DataFusion based on connector capabilities, statistics, and policy. Incremental extraction is one of the supported patterns. Ballista is used when distributed execution is configured.

> **Status (September 2026):** Phases 1–4 implemented and tested against live PostgreSQL. See [`docs/roadmap.md`](docs/roadmap.md) for the roadmap and [`docs/testing-plan.md`](docs/testing-plan.md) for test coverage.
---


## The one-paragraph pitch


The project combines **source-aware query optimization, incremental extraction, and Arrow-native execution**.


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
              │  Incremental Extraction  │
              │  Checkpoint Store        │
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
| Incremental extraction and watermarks     | This project      |
| Checkpointing                             | This project      |
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


### 3. Incremental extraction


The primary extraction pattern is incremental rather than repeatedly scanning an entire table.


For example:


```sql
SELECT order_id, user_id, amount, updated_at
FROM orders
WHERE updated_at > :last_checkpoint
  AND updated_at <= :new_checkpoint
```


A pipeline can run this query repeatedly using a checkpoint:


```text
00:00 ─┐
01:00 ─┤
02:00 ─┼──► extracted data
03:00 ─┤
04:00 ─┘
```


The checkpoint store tracks the extraction boundary between runs. The implementation includes safety lag and bounded watermark windows to handle timestamp precision and late-arriving updates.


Incremental extraction also has correctness considerations such as commit-time versus `updated_at` ordering, boundary ties, and hard deletes.


See [`docs/incremental-extraction.md`](docs/incremental-extraction.md) for the extraction protocol and these edge cases.


---


## Intended API


The DataFrame API provides a Rust interface over the extraction and DataFusion execution layers:


```rust
let ctx = ExtractContext::from_config(config).await?;


let batches = ctx
    .source("postgres", "public.orders").await?
    .incremental(Watermark::timestamp("updated_at")).await?
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
  "job_id": "orders_incremental",
  "table": "orders",
  "columns": ["order_id", "user_id", "status", "amount", "currency", "item_count", "tags", "metadata", "shipped_on", "ext_ref", "created_at", "updated_at"],
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
  "incremental": {
    "column": "updated_at",
    "safety_lag_secs": 300,
    "max_window_secs": 21600
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
| `incremental_extraction`                   | Watermark windows, safety lag, and checkpoints                        |
| `hourly_incremental` / `daily_incremental` | Scheduled extraction with checkpoint persistence and resume           |
| `parallel_extraction`                      | Partition-aware extraction across multiple scans                      |
| `distributed_extraction`                   | Incremental extraction using Ballista                                 |
| `dataframe_extraction`                     | DataFrame API against a job specification                             |
| `end_to_end`                               | Distributed full load → SQL transformation → local Parquet            |


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
```


---


## Current Scope


**Phases 1–4 implemented** — PostgreSQL connector (cursor streaming, type mapping), timestamp watermarks and checkpointing, cost-based pushdown (`always`/`never`/`cost_based`/`strict`/`hinted`) with keyset/`ctid` partitioning, bounded-memory streaming (`RowBatchBuilder`, `batch_size` 8192), and distributed execution (Ballista scheduler/workers, serializable plans, budgeted pools). Connector SPI (`SourceDescriptor`, `WatermarkSource`, `TableStatsSource`, `SqlDialect`/`Predicate::render_to`) is ready for additional backends; PostgreSQL is the implemented connector.

See [`docs/architecture.md`](docs/architecture.md), [`docs/pushdown.md`](docs/pushdown.md), [`docs/incremental-extraction.md`](docs/incremental-extraction.md), and [`docs/connectors/postgres.md`](docs/connectors/postgres.md) for details.


**Testing**

109 library unit tests cover pushdown, type mapping, partitioning, watermarks and codecs. Integration and e2e tests use a self-provisioned PostgreSQL (via `postgresql_embedded`, no Docker required) with a deterministic hostile fixture (NULLs, distinct types, enum, arrays, edge timestamps) and verify extraction, pushdown, and distributed execution. See [`docs/testing-plan.md`](docs/testing-plan.md) for the full matrix and how to run (`cargo test --test pg_*`, `cargo test --test e2e`).


**Deferred**


* Python/PyO3 wrapper
* Log-based CDC
* Additional connectors beyond PostgreSQL
* Metrics and tracing
* Sink implementations


---


## Benchmark Summary


The current benchmark compares the experimental implementation with PySpark 3.5.4 using PostgreSQL 17.11.


| Workload  | Engine                   |     Elapsed |     Max RSS |
| --------- | ------------------------ | ----------: | ----------: |
| Full      | PySpark                  | 17.3–19.7 s | 1.9–3.8 GiB |
| Full      | Rust Ballista Standalone | 18.8–21.3 s | 127–303 MiB |
| Full      | Rust Ballista Remote     | 15.5–19.3 s | 294–584 MiB |
| Selective | PySpark                  | 4.16–4.44 s | 593–767 MiB |
| Selective | Rust Ballista Standalone | 6.16–6.28 s |   16–17 MiB |
| Selective | Rust Ballista Remote     | 0.60–0.70 s | 219–491 MiB |


**Key Findings:**

- **Full extraction:** Rust Ballista Remote achieved the best latency at **15.5 s**; Rust Standalone was broadly comparable to PySpark (18.8–21.3 s vs 17.3–19.7 s).
- **Selective extraction:** Rust Ballista Remote was **6–7× faster than PySpark** and **9–10× faster than Standalone** at **0.60–0.70 s** vs 4.16–4.44 s / 6.16–6.28 s.
- **Resource efficiency:** Rust Standalone used **127–303 MiB** for full (6–30× less than PySpark's 1.9–3.8 GiB) and **16–17 MiB** for selective.
- **Distributed execution:** Remote outperformed both PySpark and Standalone even for the small selective result set; workers were evenly utilized across 4 workers.
- **Memory & batch size:** 4 GB vs 8 GB did not change relative ordering; 64k batching was competitive or better for full, while selective was dominated by execution mode.

> These are measurements of the current implementation on this workload/hardware — not a general Rust-vs-PySpark claim.

See [`benchmark/README.md`](benchmark/README.md) for the complete per-run tables and methodology.


---


## AI-Assisted Development


AI tools were used throughout the project for implementation, code exploration, debugging, and iteration.


The overall architecture, requirements, design decisions, technical trade-offs, and implementation direction were under my technical direction. AI-generated output was reviewed and tested rather than treated as automatically correct.


---


## Documentation


| Document                                                           | What it covers                                                                   |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------- |
| [`docs/roadmap.md`](docs/roadmap.md)                               | Phased delivery plan                                                             |
| [`docs/quickstart.md`](docs/quickstart.md)                         | Setup, configuration, distributed execution, benchmarks, and testing             |
| [`docs/architecture.md`](docs/architecture.md)                     | Module layout, plan lifecycle, Arrow data model, execution and memory management |
| [`docs/connectors/README.md`](docs/connectors/README.md)           | Connector SPI, capabilities, scan planning, partitioning, and type mapping       |
| [`docs/connectors/postgres.md`](docs/connectors/postgres.md)       | PostgreSQL-specific implementation details                                       |
| [`docs/connectors/mysql.md`](docs/connectors/mysql.md)             | MySQL connector design (planned)                                                   |
| [`docs/pushdown.md`](docs/pushdown.md)                             | Pushdown rules, semantic compatibility, cost model, and policy engine            |
| [`docs/incremental-extraction.md`](docs/incremental-extraction.md) | Watermarks, checkpoints, backfills, and correctness considerations               |
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