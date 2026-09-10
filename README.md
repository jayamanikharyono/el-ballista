# Rust Extract Layer

A Rust-native, Arrow-based **incremental extraction and ETL engine** designed to intelligently push operations into source databases when beneficial, execute analytical transformations locally through DataFusion, and eventually support distributed execution through Ballista. The system is designed to produce columnar results for object storage and analytical warehouses.

> **Status:** Phase 2 complete, Phase 3 in progress. Phase 1 (single-node PostgreSQL extraction with checkpointing) is production-ready for validation workloads. Phase 2 (cost-based pushdown with statistics integration) is implemented and compilable. Phase 3 (true streaming with bounded memory) just completed. See the [roadmap](docs/roadmap.md) for phased delivery and detailed implementation plans.

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
             │  DataFrame │  Job YAML  │
             └────────────┬────────────┘
                          │
                          ▼
             ┌──────────────────────────┐
             │    Rust Extract Layer    │
             │                          │
             │  Logical Plan            │
             │  Source-Aware Optimizer  │
             │  Connector Manager       │
             │  Incremental Extractor   │
             │  Checkpoint Store        │
             └────────────┬─────────────┘
                          │
          ┌───────────────┼────────────────┐
          ▼               ▼                ▼
      PostgreSQL        MySQL         (future: Scylla,
      / CloudSQL        / CloudSQL     Mongo, object store)
          │               │                │
          └───────────────┼────────────────┘
                          │
                          ▼
                 Arrow RecordBatch stream
                          │
                          ▼
             ┌──────────────────────────┐
             │   DataFusion execution   │
             │  (Ballista when needed)  │
             └────────────┬─────────────┘
                          │
                   ┌──────┴──────┐
                   ▼             ▼
              GCS Parquet    BigQuery
```

### What we build vs. what we borrow

Building a Spark clone means owning a scheduler, shuffle, fault tolerance, resource management,
catalog, optimizer, planner, connectors, metrics, and a UI. We are not doing that. DataFusion is
explicitly designed as an *embeddable* query engine, so we borrow the engine and build the parts
that are actually specific to extraction.

| Concern | Owner |
| --- | --- |
| Logical/physical planning, expression eval, joins, aggregation | Apache DataFusion |
| Columnar memory model, compute kernels, IPC | Apache Arrow |
| Distributed scheduling and shuffle (Phase 4, optional) | Apache Ballista |
| Parquet encoding | `parquet` crate |
| **Source connectors and capability declaration** | **This project** |
| **Source-aware pushdown policy and cost model** | **This project** |
| **Incremental extraction, watermarks, checkpointing** | **This project** |
| **Sinks, partition layout, warehouse load orchestration** | **This project** |
| **Job specification, CLI, orchestrator integration** | **This project** |

The dependency floor is DataFusion `55.x` and Arrow `56.x` (the Arrow version DataFusion 55 pins).
See [`docs/architecture.md`](docs/architecture.md) for the workspace layout and version policy.

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
let ctx = ExtractContext::from_config("extract.toml").await?;

let df = ctx
    .source("orders_pg", "public.orders")
    .incremental(Watermark::timestamp("updated_at"))   // resolves from the checkpoint store
    .filter(col("status").eq(lit("PAID")))
    .select(vec![col("order_id"), col("user_id"), col("amount"), col("updated_at")])
    .with_column("amount_usd", col("amount") * lit(1.0 / 15_800.0));

df.write_parquet(
        "gs://warehouse/raw/orders/",
        ParquetSinkOptions::default().partition_by(["_extracted_date"]),
    )
    .await?;

ctx.commit_checkpoints().await?;    // only after the sink is durable
```

Declarative jobs, for orchestrator-driven runs, describe the same plan:

```yaml
job: orders_incremental
source:
  connector: postgres
  ref: orders_pg
  table: public.orders
incremental:
  mode: timestamp
  column: updated_at
  safety_lag: 5m          # guards against late-committing transactions
  primary_key: [order_id] # tiebreaker for equal watermarks
pushdown:
  policy: cost_based
  max_source_cost: 50000  # planner-estimated units; above this, execute in Arrow
sink:
  type: parquet
  uri: gs://warehouse/raw/orders/
  partition_by: [_extracted_date]
  target_file_size: 256MiB
```

---

## Examples and Capability Tests

The `examples/` directory serves two purposes: **demonstrating what the extraction layer can do and providing executable smoke tests for its capabilities**.

Each example focuses on a specific feature of the system. Running an example exercises the corresponding implementation against realistic inputs, making the examples useful both as documentation and as an early validation layer while the project is under development.

These are **not production entry points or a replacement for a comprehensive test suite**. They are intentionally small, observable demonstrations that help verify that the major pieces of the system are wired together correctly.

### Capability Matrix

| Example | Capability demonstrated | Validation |
| --- | --- | --- |
| `basic_extraction.rs` | PostgreSQL → Arrow extraction | Connection, schema, type mapping, `RecordBatch` |
| `incremental_extraction.rs` | Watermark-based extraction | Extraction boundaries and checkpoint semantics |
| `arrow_stream.rs` | `RecordBatch` → `SendableRecordBatchStream` | DataFusion execution boundary |
| `pushdown.rs` | Source-aware predicate pushdown | SQL translation and semantic fidelity |
| `statistics.rs` | Source statistics collection | Statistics retrieval and planning inputs |
| `parallel_scan.rs` | Partition-aware extraction | Multiple source partitions and result merging |
| `datafusion_transform.rs` | Local analytical execution | Filter, projection, expressions, aggregation |
| `end_to_end.rs` | Combined pipeline | End-to-end integration smoke test |

### Running Examples

Each example runs standalone and requires no external setup beyond what is noted in its documentation comments:

```bash
# Basic extraction: creates Arrow RecordBatches from simulated data
cargo run --example basic_extraction

# Incremental extraction: watermark-based windows and checkpoints
cargo run --example incremental_extraction

# Arrow streaming: RecordBatchStream integration
cargo run --example arrow_stream

# Pushdown decisions: capability declarations and cost model
cargo run --example pushdown

# Statistics collection: table and column statistics for planning
cargo run --example statistics

# Parallel scan: partition-aware extraction across multiple scans
cargo run --example parallel_scan

# DataFusion transforms: filter, project, aggregate on Arrow data
cargo run --example datafusion_transform

# End-to-end pipeline: extraction → checkpoint → transform → results
cargo run --example end_to_end
```

### Example Descriptions

#### 1. Basic Extraction (`basic_extraction.rs`)

Tests the fundamental extraction path: PostgreSQL → Source Connector → Arrow RecordBatch.

Validates:
- Arrow schema creation and field definition
- `RecordBatch` construction from typed column arrays
- Column selection and type mapping
- Schema introspection and consistency

#### 2. Incremental Extraction (`incremental_extraction.rs`)

Exercises the watermark-based extraction path with time-bounded windows.

Validates:
- Watermark window definition (`updated_at > :lo AND updated_at <= :hi`)
- Checkpoint storage and recovery
- Safety-lag semantics to prevent uncommitted reads
- Repeated incremental extraction patterns

#### 3. Arrow RecordBatch Streaming (`arrow_stream.rs`)

Exercises the boundary between source-aware extraction and DataFusion:

```
Source DB → DB-aware extractor → Arrow builders → RecordBatch → SendableRecordBatchStream → DataFusion
```

Validates:
- Multiple `RecordBatch` creation with consistent schema
- Streaming semantics (batches in sequence, not all at once)
- Schema consistency across batches
- Ready for DataFusion consumption (filter, projection, aggregation, etc.)

#### 4. Pushdown (`pushdown.rs`)

Exercises source-aware pushdown decisions with capability declarations.

Validates:
- Connector capability declarations: `Exact` / `Inexact` / `Unsupported`
- SQL expression translation for a given dialect
- Collation-sensitive comparisons (e.g., case-insensitive defaults)
- Cost-based pushdown policy: when to push an operator vs. keep it in Arrow
- Selectivity estimates as input to cost decisions

#### 5. Statistics (`statistics.rs`)

Exercises source statistics collection and its use in planning.

Validates:
- Row-count and table-size estimates (`pg_class`)
- Column statistics: distinct values, NULL fractions, average widths (`pg_stats`)
- Statistics availability and caching behavior
- Selectivity estimation for common predicate types
- Fallback behavior when statistics are unavailable

#### 6. Parallel Scan (`parallel_scan.rs`)

Exercises partition-aware extraction across multiple scans.

Validates:
- Partition boundary generation (keyset and ctid strategies)
- Independent source scans per partition
- Non-overlapping, exhaustive partition coverage
- Merging results from multiple partitions
- Consistency guarantees (no duplicates, all rows accounted for)

#### 7. DataFusion Transformation (`datafusion_transform.rs`)

Exercises local analytical execution on extracted Arrow data.

Validates:
- Filter operations (predicates on columns)
- Projection (column selection and expression evaluation)
- Aggregations (SUM, AVG, COUNT, MIN, MAX)
- GROUP BY operations
- Complex expressions and type coercion
- Selectivity and row reduction

#### 8. End-to-End (`end_to_end.rs`)

Combines major capabilities into a single integration test:

```
Source DB → Incremental extraction → Arrow RecordBatch → DataFusion → Filter / Transform / Aggregate → Result
```

Validates:
- Complete pipeline from extraction through transformation
- Checkpoint storage and recovery
- Statistics collection for planning
- Multiple batches and streaming
- Result correctness and completeness

---

## Scope: Completed and Current

**Phase 1 (Completed):** PostgreSQL connector with schema resolution, binary `COPY` and portal-based scans, type mapping, streaming decode into Arrow builders. Incremental extraction with timestamp watermark mode, checkpoint store with atomic rename semantics, projection/filter/limit pushdown with fidelity rules, Arrow output, CLI commands, structured logging.

**Phase 2 (Completed):** DataFrame API over DataFusion's LogicalPlan. Cost-based pushdown optimizer rule with statistics collection, `EXPLAIN`-based cost estimation, and policy engine (always/cost_based/never modes). Collation-aware fidelity rules. Parallel scan strategies (keyset and ctid) under exported snapshots. Backfill orchestration with separate checkpoint namespacing. Full test coverage and integration with PostgreSQL statistics (`pg_stats`, `pg_class`, `information_schema`).

**Phase 3 (Just Completed):** True streaming execution with bounded memory. Replaced `fetch_all()` with `sqlx::query().fetch()` streaming rows. Implemented `RowBatchBuilder` for incremental Arrow batch construction. Configurable `batch_size` (default 8192 rows). Multiple `RecordBatch` emission via `async-stream` macro. Memory now O(batch_size) instead of O(total_rows). Early first-batch latency before query completion.

**Phase 4 (Planned):** Distributed execution via Ballista scheduler and workers. Serializable physical plans, connection-pool coordination across N workers.

**Phase 5 (Planned):** MySQL connector to prove SPI abstraction. Collation fidelity, zero-date handling, replica lag bounding.

**Explicitly deferred:** Python wrapper (placeholder design in [`docs/python-bindings.md`](docs/python-bindings.md)), log-based CDC, and connectors beyond Postgres/MySQL.

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
| [`docs/phase-two-implementation-plan.md`](docs/phase-two-implementation-plan.md) | ✓ Phase 2 complete: cost model, statistics, optimizer rule, parallel strategies |
| [`docs/phase-three-implementation-plan.md`](docs/phase-three-implementation-plan.md) | ✓ Phase 3 complete: true streaming, RowBatchBuilder, configurable batch_size |
| [`docs/architecture.md`](docs/architecture.md) | Crate layout, plan lifecycle, Arrow data model, execution and memory management, config, observability |
| [`docs/connectors/README.md`](docs/connectors/README.md) | The connector SPI: capability declaration, scan planning, partitioning, type mapping rules |
| [`docs/connectors/postgres.md`](docs/connectors/postgres.md) | Detailed PostgreSQL implementation: binary `COPY`, exported snapshots, type mapping, statistics, CDC path |
| [`docs/connectors/mysql.md`](docs/connectors/mysql.md) | Detailed MySQL implementation: streaming binary protocol, collation hazards, unsigned/zero-date handling, GTID anchoring |
| [`docs/pushdown.md`](docs/pushdown.md) | Expression translation, `Exact`/`Inexact` rules, cost model, policy engine |
| [`docs/incremental-extraction.md`](docs/incremental-extraction.md) | Watermark modes, checkpoint schema and protocol, correctness hazards, backfills |
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
