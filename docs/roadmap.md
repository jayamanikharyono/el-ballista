# Roadmap

Phases are ordered so that each one is independently useful. Phase 1 alone replaces a real PySpark
extraction job; nothing after it is required for the project to earn its keep.

Each phase has **exit criteria** rather than dates, since this is a project that gets worked on in
evenings.

---

## Phase 1 — Single-node PostgreSQL extractor

```
Postgres ──► Rust ──► Arrow ──► Parquet ──► GCS
```

The whole vertical slice, narrow. One connector, one sink, no DataFrame API — a job spec goes in,
Parquet comes out, a checkpoint advances.

- PostgreSQL connector: schema resolution, binary `COPY` and portal-based scans, type mapping,
  streaming decode into Arrow builders
- Incremental extraction in `timestamp` mode with the exact
  [safe high watermark](connectors/postgres.md#52-the-safe-high-watermark)
- Checkpoint store (Postgres backend) with leases and run history
- Projection and filter pushdown with `Exact`/`Inexact` fidelity rules — cost model not yet, policy
  is `always` for safe predicates
- Parquet sink to local filesystem and GCS, deterministic window paths, `_SUCCESS` markers
- `rel run`, `rel plan`, `rel checkpoint` CLI commands
- Metrics and structured tracing

**Exit criteria.** A real table extracts incrementally on a schedule for two weeks without
intervention. The differential correctness suite from
[pushdown §6](pushdown.md#6-verification-strategy) passes against the hostile-value fixture. A
killed process mid-run resumes without duplicating or losing a row. Throughput is measured and
recorded — against the PySpark job it replaces, on the same table and hardware, so the comparison
is a number rather than an adjective.

---

## Phase 2 — DataFrame API and the cost model

The engine gets a usable front end, and pushdown becomes a decision rather than a reflex.

```rust
let df = ctx.source("orders_pg", "public.orders")
    .incremental(Watermark::timestamp("updated_at"))
    .filter(col("status").eq(lit("PAID")))
    .select(vec![col("order_id"), col("amount")])
    .with_column("amount_usd", col("amount") * lit(rate));

df.write_parquet("gs://warehouse/raw/orders/", opts).await?;
```

- DataFrame builder over DataFusion's `LogicalPlan`, plus SQL entry via `ctx.sql()`
- `SourceAwarePushdown` optimizer rule with statistics collection, `EXPLAIN`-based estimation, and
  the `always` / `never` / `cost_based` / `hinted` policy modes
- `rel plan --explain` printing per-operator push/keep decisions and their reasoning
- Parallel scan: `keyset` and `ctid` strategies under exported snapshots
- Backfill orchestration with chunking and a separate checkpoint namespace

**Exit criteria.** For at least one real table, `cost_based` demonstrably chooses differently from
`always` and produces a measurably better outcome — this is the phase where the project's central
claim either holds up or does not. Plan snapshot tests cover the decision surface.

---

## Phase 3 — MySQL and multi-source

```
Postgres ──┐
MySQL   ───┼──► Arrow / DataFusion ──► GCS / BigQuery
GCS     ───┘
```

The second connector is what proves the SPI is an abstraction rather than a Postgres wrapper, and
MySQL is the right second connector precisely because it is *worse* at everything Postgres does
well — no bulk export, no exportable snapshot, opt-in histograms, unrepresentable values. If the
SPI survives MySQL, it will survive anything.

- MySQL connector per [its detailed plan](connectors/mysql.md), including the collation fidelity
  rules, zero-date handling, and replica lag bounding
- Cross-source joins: a Postgres fact table joined against a GCS Parquet dimension, executed in
  Arrow because no single source can do it
- BigQuery sink: load-job orchestration and the `MERGE` upsert pattern
- Object store source (Parquet/CSV on GCS) as a read side, which mostly falls out of DataFusion

**Exit criteria.** A join across Postgres and MySQL produces correct results with each side's
predicates pushed independently. The MySQL differential correctness suite passes, including the
`_ci` collation cases. The SPI required no breaking change to accommodate MySQL — or if it did, the
change is documented as a lesson.

---

## Phase 4 — Distributed execution

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
                     GCS
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

## Deferred, tracked, not scheduled

| Item | Why it is deferred |
| --- | --- |
| [Python wrapper](python-bindings.md) | Explicitly out of scope. Placeholder design exists so the Rust API stays bindable |
| Log-based CDC ([Postgres](connectors/postgres.md#8-future-logical-replication-cdc), [MySQL](connectors/mysql.md#9-future-binlog-cdc)) | The correct fix for deletes and commit skew, but replication slots and binlog retention are footguns that need real operational maturity first |
| Aggregate and join pushdown | Highest translation risk, lowest value for extraction workloads |
| Additional connectors (ScyllaDB, MongoDB, SQL Server) | Not until the SPI has been proven by two dissimilar implementations |
| Web UI | An orchestrator already has one. Metrics and CLI output are the interface |

---

## What would make this project fail

Worth writing down, since these are the ways a hobby project like this quietly dies:

- **Scope creep into building Spark.** The moment work starts on a scheduler, resource manager, or
  cluster UI, the project is doomed. Everything in that category is either DataFusion's job,
  Ballista's job, or the orchestrator's job.
- **Pushdown that is fast and wrong.** A silent correctness bug destroys trust permanently, and
  it is trivially easy to introduce here. This is why fidelity rules and the differential test
  suite are Phase 1 work rather than a later hardening pass.
- **Chasing a benchmark number.** "Faster than PySpark" is a side effect. Source-aware optimization
  and incremental extraction are the actual product, and a Rust DataFrame library that is 3× faster
  at reading a table is not interesting to anyone.
- **Taking down a production database.** One incident caused by an extraction job forcing a
  sequential scan on a primary during business hours ends adoption. The cost model, statement
  timeouts, small connection pools, and conservative defaults all exist for this reason.
