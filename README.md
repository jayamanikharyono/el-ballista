# Rust Extract Layer

A Rust-native, Arrow-based **incremental extraction and ETL engine** that intelligently pushes
operations into source databases when beneficial, executes analytical transformations locally
(and later, distributed) through DataFusion/Ballista, and writes columnar results to object
storage and warehouses.

> **Status:** design phase. This repository currently contains the architecture and
> implementation specification. Code lands per the [roadmap](docs/roadmap.md).

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

## Scope right now

**In scope for the first milestones:** PostgreSQL and MySQL connectors, incremental extraction with
a durable checkpoint store, projection/filter/limit pushdown with a cost policy, Parquet output to
local disk and GCS, and single-node DataFusion execution.

**Explicitly deferred:** the Python wrapper (placeholder design kept in
[`docs/python-bindings.md`](docs/python-bindings.md) so the Rust API does not accidentally become
un-bindable), distributed execution via Ballista, and connectors beyond Postgres/MySQL.

---

## Documentation

| Document | What it covers |
| --- | --- |
| [`docs/architecture.md`](docs/architecture.md) | Crate layout, plan lifecycle, Arrow data model, execution and memory management, config, observability |
| [`docs/connectors/README.md`](docs/connectors/README.md) | The connector SPI: capability declaration, scan planning, partitioning, type mapping rules |
| [`docs/connectors/postgres.md`](docs/connectors/postgres.md) | Detailed PostgreSQL implementation: binary `COPY`, exported snapshots, type mapping, statistics, CDC path |
| [`docs/connectors/mysql.md`](docs/connectors/mysql.md) | Detailed MySQL implementation: streaming binary protocol, collation hazards, unsigned/zero-date handling, GTID anchoring |
| [`docs/pushdown.md`](docs/pushdown.md) | Expression translation, `Exact`/`Inexact` rules, cost model, policy engine |
| [`docs/incremental-extraction.md`](docs/incremental-extraction.md) | Watermark modes, checkpoint schema and protocol, correctness hazards, backfills |
| [`docs/sinks.md`](docs/sinks.md) | Parquet layout and tuning, GCS writes, BigQuery load and `MERGE` patterns |
| [`docs/roadmap.md`](docs/roadmap.md) | Phased delivery plan with exit criteria |
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

## License

TBD.
