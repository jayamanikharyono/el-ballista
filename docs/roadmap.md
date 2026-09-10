# Roadmap

Phases are ordered so that each one is independently useful. Phase 1 alone replaces a real PySpark
extraction job; nothing after it is required for the project to earn its keep.

Each phase has **exit criteria** rather than dates, since this is a project that gets worked on in
evenings.

---

## Phase 1 — Single-node PostgreSQL Extraction

```
Postgres ──► Rust ──► Arrow ──► DataFusion/Ballista
```

The whole vertical slice, narrow. One connector, no sink implementation — a job spec goes in,
Arrow RecordBatches come out, a checkpoint advances. Sink functionality is handled by DataFusion
or Ballista (ParquetWriter, CSVWriter, etc.) or by the orchestrator.

- PostgreSQL connector: schema resolution, binary `COPY` and portal-based scans, type mapping,
  streaming decode into Arrow builders
- Incremental extraction in `timestamp` mode with the exact
  [safe high watermark](connectors/postgres.md#52-the-safe-high-watermark)
- Checkpoint store (local filesystem, atomic rename semantics) with leases and run history
- Projection and filter pushdown with `Exact`/`Inexact` fidelity rules — cost model not yet, policy
  is `always` for safe predicates
- Arrow output (RecordBatch streams) to be consumed by DataFusion, Ballista, or orchestrator
- `rel run`, `rel plan`, `rel checkpoint` CLI commands
- Structured logging

**Exit criteria.** A real table extracts incrementally on a schedule for two weeks without
intervention. The differential correctness suite from
[pushdown §6](pushdown.md#6-verification-strategy) passes against the hostile-value fixture. A
killed process mid-run resumes without duplicating or losing a row. Arrow output is verified
correct via DataFusion's query engine. Throughput is measured and recorded — against the
PySpark job it replaces, on the same table and hardware, so the comparison is a number rather
than an adjective.

---

## Phase 2 — DataFrame API and Cost-Based Pushdown

The engine gets a usable front end, and pushdown becomes a decision rather than a reflex.

```rust
let df = ctx.source("orders_pg", "public.orders")
    .incremental(Watermark::timestamp("updated_at"))
    .filter(col("status").eq(lit("PAID")))
    .select(vec![col("order_id"), col("amount")])
    .with_column("amount_usd", col("amount") * lit(rate));

// Output is Arrow RecordBatches, ready for DataFusion operations
let batches = df.collect().await?;
```

- DataFrame builder over DataFusion's `LogicalPlan`, plus SQL entry via `ctx.sql()`
- `SourceAwarePushdown` optimizer rule with statistics collection, `EXPLAIN`-based estimation, and
  the `always` / `never` / `cost_based` / `hinted` policy modes
- `rel plan --explain` printing per-operator push/keep decisions and their reasoning
- Parallel scan: `keyset` and `ctid` strategies under exported snapshots
- Backfill orchestration with chunking and a separate checkpoint namespace
- Arrow RecordBatch output for consumption by DataFusion writers or orchestrator

**Exit criteria.** For at least one real table, `cost_based` demonstrably chooses differently from
`always` and produces a measurably better outcome — this is the phase where the project's central
claim either holds up or does not. Plan snapshot tests cover the decision surface. Output is
verifiable as correct Arrow data via DataFusion's query engine.

---

## Phase 3 — True Streaming Execution

```
PostgreSQL
    ↓
streaming portal (fetch, not fetch_all)
    ↓
RecordBatch stream (bounded memory, multiple batches)
    ↓
SendableRecordBatchStream
    ↓
DataFusion
```

Replace `fetch_all()` with true incremental batching. Current Phase 2 implementation materializes
the entire query result before yielding any batches. This phase makes `execute()` produce multiple
`RecordBatch` objects as data arrives from PostgreSQL, with bounded memory that scales to `O(batch_size)`
rather than `O(total_rows)`.

- Replace `sqlx::query(...).fetch_all()` with `sqlx::query(...).fetch()` (streaming rows)
- Implement batch accumulation loop: collect rows into Arrow builders until `batch_size` reached
- Yield `RecordBatch` incrementally as stream items
- Memory bounded to `O(batch_size + driver buffering)`, not `O(total_rows)`
- Preserve existing query building, type mappings, error handling
- Support all existing pushed predicates: watermark, filters, projection, limit
- Configurable `batch_size` (default 8192 rows)
- Test with multiple batches, empty results, partial final batches

**Exit criteria.** `fetch_all()` is removed from the normal scan path. Multiple `RecordBatch`
objects are produced for results exceeding `batch_size`. Memory no longer scales with total row
count. First-batch latency is significantly earlier than full-result materialization. All existing
Phase 2 features (pushdown, type mappings, schema handling) remain intact. Tests verify batching
correctness and memory efficiency.

---

## Phase 4 — Distributed Execution

Only when single-node throughput is genuinely the bottleneck, which for the target workloads it may
never be.

```
                  Scheduler
                      │
       ┌──────────────┼──────────────┐
       ▼              ▼              ▼
    Worker 1       Worker 2       Worker 3
   DataFusion     DataFusion     DataFusion
       │              │              │
       └──────────────┼──────────────┘
                      ▼
                   Parquet
```

Ballista adds a scheduler and workers over DataFusion, using Arrow IPC for shuffle. It tracks
DataFusion's version numbering and is actively developed, with current work focused on closing the
gap with single-node DataFusion, adaptive query execution, and operational predictability — which
is a fair description of "promising, still maturing", and a good reason to keep this phase last.

- Ballista deployment: scheduler, workers, and the distribution of source partitions across them
- Serializable physical plans, including our `SourceScanExec`
- Connection-pool coordination so N workers do not collectively open N × pool_max connections to a
  production database — the constraint that matters most and the one Ballista knows nothing about

**Exit criteria.** A workload that saturates one machine scales across three with better wall-clock
time and without increasing source load. Before writing any of this, confirm the bottleneck is
compute and not the source or the network, because if it is the source, distribution makes things
worse rather than better.

---

## Phase 5 — MySQL Connector

```
Postgres ──┐
MySQL   ───┼──► Arrow / DataFusion
```

The second connector proves the SPI is a real abstraction rather than a Postgres wrapper. MySQL
is the right second connector because it is *worse* at everything Postgres does well — no bulk
export, no exportable snapshot, opt-in histograms, unrepresentable values. If the SPI survives
MySQL, it will survive anything.

- MySQL connector per [its detailed plan](connectors/mysql.md), including:
  - Collation fidelity rules (binary vs. `utf8_unicode_ci` vs. `utf8mb4_general_ci`)
  - Zero-date handling (`0000-00-00` → NULL / error / custom mapping)
  - Replica lag bounding (GTIDs, `Seconds_Behind_Master` monitoring)
  - Safe high watermark from `SHOW PROCESSLIST` instead of `pg_stat_activity`
  - Streaming LIMIT-OFFSET pagination instead of exported snapshots
  - Type mapping for MySQL-specific types (ENUM, SET, JSON, GEOMETRY)

**Exit criteria.** The MySQL differential correctness suite passes, including collation fidelity
(`_ci`, `_cs`, `_bin`). Zero-date handling is correctly configurable. Replica lag is bounded and
monitored. The SPI required no breaking change to accommodate MySQL — or if it did, the change is
documented as a lesson.

---

## Out of Scope

This project is an extraction layer: source database → Arrow. Sink functionality is explicitly
out of scope and delegated to DataFusion, Ballista, or the orchestrator. The following items are
not part of this project's scope:

### Sink Operations (Out of Scope)
| Item | Delegated to |
| --- | --- |
| Parquet writing | DataFusion ParquetWriter or Ballista sink |
| CSV/JSON writing | DataFusion writers |
| GCS/S3 upload | Ballista or orchestrator |
| BigQuery load | BigQuery Rust client or orchestrator |
| Checkpoint finalization | Orchestrator or external checkpoint service |

### Other Out of Scope Items
| Item | Why it is out of scope |
| --- | --- |
| [Python wrapper](python-bindings.md) | Not part of core extraction engine. Placeholder design exists so the Rust API stays bindable |
| Cross-source joins | Better handled by orchestrator or DataFusion, not part of extraction scope |
| Log-based CDC ([Postgres](connectors/postgres.md#8-future-logical-replication-cdc), [MySQL](connectors/mysql.md#9-future-binlog-cdc)) | Replication slots and binlog retention are operational footguns |
| Aggregate and join pushdown | High translation risk, low value for extraction workloads |
| Additional connectors (ScyllaDB, MongoDB, SQL Server) | Wait for second connector proof of concept (MySQL) first |
| Web UI | Orchestrators provide their own. CLI and metrics are the interface |
| Object store source (Parquet/CSV) | DataFusion handles this natively |

---

## What would make this project fail

Worth writing down, since these are the ways a hobby project like this quietly dies:

- **Scope creep into building Spark.** The moment work starts on a scheduler, resource manager, or
  cluster UI, the project is doomed. Everything in that category is either DataFusion's job,
  Ballista's job, or the orchestrator's job.
- **Pushdown that is fast and wrong.** A silent correctness bug destroys trust permanently, and
  it is trivially easy to introduce here. This is why fidelity rules and the differential test
  suite are Phase 1 work rather than a later hardening pass.
- **Becoming a sink framework.** The moment work starts on Parquet writers, BigQuery integration, or
  S3 upload, this project loses focus. Sinks are DataFusion's job, Ballista's job, or the
  orchestrator's job — not ours. We extract to Arrow and stop.
