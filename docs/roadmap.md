# Roadmap

El Ballista is built in phases, ordered so that each phase is usable on its own. Phases have
exit criteria instead of dates. For every phase this page lists what exists in the code today
and, for each exit criterion, whether it is met (with the evidence) or not measured yet.

Scope: full and filtered extraction from PostgreSQL into Arrow, source-aware pushdown, keyset /
`ctid` partitioning with split checkpoints, and standalone or distributed execution. Writing the
data (sinks), watermark / incremental state and CDC are not part of the layer: the orchestrator
owns them and expresses a range as filters in the job spec.

The original phase plans, written before the work, are kept in [`history/`](history/README.md).
(`el-ballista` below is the built binary; from a checkout, use
`cargo run --release --bin el-ballista --`.)

| Phase | Status | Exit criteria |
| --- | --- | --- |
| 1 — Single-node PostgreSQL extraction | Implemented | Met, except the two-week scheduled run (not measured) |
| 2 — DataFrame API and cost-based pushdown | Implemented | Decisions differ from `always`; the "measurably better" part is not measured |
| 3 — Streaming execution | Implemented for the scan paths | Met, except first-batch latency (not measured) |
| 4 — Distributed execution | Implemented | Correctness and source budget met; multi-machine scaling not measured |
| 5 — MySQL connector | Prototype | Not met (schema read and full-table extract only) |

---

## Phase 1 — Single-node PostgreSQL extraction

```
Postgres ──► Rust ──► Arrow ──► DataFusion
```

The whole vertical slice, narrow: a job spec goes in, Arrow `RecordBatch`es come out.

What exists today:

- PostgreSQL connector: catalog-based schema resolution, cursor scans (`DECLARE … CURSOR` /
  `FETCH FORWARD`) and binary `COPY`, type mapping, decoding straight into Arrow builders.
- Full and filtered extraction with caller-provided filters.
- Split checkpoints in a local JSON store (atomic rename): per-split `Pending` / `Running` /
  `Completed` / `Failed`; a retry skips completed splits. The checkpoint is bound to a plan
  fingerprint and the stored split bounds (`PlanMismatch` on change), and a lock file with a
  heartbeat (`checkpoint.lock_ttl_secs`) allows one run per job.
- A run report per run (`<checkpoint.dir>/runs/<job>/<run_id>.json`, `el-ballista runs list|show`):
  plan fingerprint, pushdown decisions, per-split outcome / rows / time / error and totals,
  kept after later runs; its `run_id` matches the SQL comment tag of every source query.
- `run_with(consumer)` as the checkpointed API: a split is recorded as completed only after the
  consumer returns `Ok`. Delivery is at-least-once per split.
- CLI: `el-ballista run` (diagnostic: counts rows, writes no checkpoint), `el-ballista plan`,
  `el-ballista checkpoint show|reset`, `el-ballista demo`.
- Logging through the `log` crate with key=value fields per batch and split.

Exit criteria:

| Criterion | Evidence |
| --- | --- |
| A real table extracts on a schedule for two weeks without intervention | Not measured |
| The differential correctness suite passes against the hostile-value fixture | Met: `always` vs `never` differentials and a seeded property-based test (`tests/pg_pushdown.rs`, `tests/pg_pushdown_prop.rs`); cursor vs `COPY` differentials (`tests/pg_copy.rs`, `tests/pg_decode.rs`) |
| A killed process resumes without duplicating or losing a row | Met for committed rows: a consumer failure and a run cancelled mid-split both resume and the union of committed rows equals the source (`tests/e2e.rs`). The crash is simulated by dropping the run, not by killing the process |
| Arrow output is verified through DataFusion | Met: integration tests compare extracted rows with direct SQL |
| Throughput is measured against PySpark on the same table and hardware | Met: [`benchmark/`](../benchmark/README.md), 50M rows, equal-spec containers |

---

## Phase 2 — DataFrame API and cost-based pushdown

The engine gets a usable front end, and pushdown becomes a decision rather than a reflex.

```rust
let ctx = ExtractContext::from_config(config).await?;
let batches = ctx
    .source("postgres", "public.payment").await?
    .filter(col("customer_id").gt_eq(lit(300)))?
    .select(vec![col("payment_id"), col("amount")])?
    .collect().await?;
```

What exists today:

- `ExtractContext` (`connector::postgres::engine`): DataFrame and SQL over the source table.
- The push/keep decision runs in `PostgresTableProvider::supports_filters_pushdown`, which
  decides the whole filter set at once; `explain_decisions` gives the same decisions for
  previews. There is no custom optimizer rule.
- Policies `always` / `never` / `cost_based` / `strict` / `hinted` (`hinted` = per-column `push`
  and `deny` lists in the job spec), statistics from `pg_class` / `pg_stats`, and `EXPLAIN`-based
  estimates.
- Date and timestamp pushdown: date literals push `Exact` on date columns. Range selectivity on
  integer, date and timestamp columns comes from the `pg_stats` histogram and most-common
  values, and range filters on one column are estimated together as a window. See
  [`pushdown.md`](pushdown.md) (range selectivity and windows).
- `el-ballista plan` prints each filter's decision and its reason.
- Parallel scans: `keyset` and `ctid` partitioning. Snapshot-consistent parallel reads through
  exported snapshots are not implemented.

Exit criteria:

| Criterion | Evidence |
| --- | --- |
| For a real table, `cost_based` chooses differently from `always` | Met: on the demo table, `staff_id != 1` is kept on cost (50% selectivity), where `always` pushes it ([`pushdown_showcase.json`](../examples/configs/pushdown_showcase.json)) |
| … and produces a measurably better outcome | Not measured. The [benchmark](../benchmark/README.md) compares against PySpark, not `cost_based` against `always` on the same engine |
| Plan snapshot tests cover the decision surface | Partly: unit tests cover policy, cost model and window estimates, and `tests/pg_pushdown.rs` covers live decisions; there are no plan snapshot tests |
| Output is verified as correct Arrow through DataFusion | Met: pushed and non-pushed runs return identical rows (`tests/pg_pushdown.rs`) |

---

## Phase 3 — Streaming execution

```
PostgreSQL ─► cursor FETCH / binary COPY ─► bounded RecordBatch stream ─► DataFusion
```

What exists today: `PostgresExecutionPlan::execute()` runs a `DECLARE … CURSOR` /
`FETCH FORWARD <batch_size>` loop (or one binary `COPY`) and yields `RecordBatch`es through a
bounded channel as they fill (by rows or `max_batch_bytes`). Dropping the stream closes the
cursor or cancels the `COPY`. The builders' `stream()` and `run_with()` are bounded-memory.

Materializing helpers remain on purpose and hold the whole result: `collect()` on the builders,
`PostgresExtractor::extract_full_table` / `extract_keyset_partition` (tests and small tables),
and the MySQL prototype's `extract_full_table`. No DataFusion `MemoryPool` limit is configured.

Exit criteria:

| Criterion | Evidence |
| --- | --- |
| `fetch_all()` is gone from the normal scan path | Met: it remains only in catalog and statistics queries |
| Results larger than `batch_size` produce multiple batches | Met: tests run with small batch sizes (e.g. `batch_size = 1` in `tests/e2e.rs`) |
| Memory no longer scales with total row count | Met in the benchmark: 190 MiB peak for a 50M-row full load, and times move less than ~5% between 2g and 8g budgets |
| First-batch latency is well below full materialization | Not measured |
| Phase 2 features stay intact | Met: the full test suite runs on the streaming path |

---

## Phase 4 — Distributed execution

```
        client process (plans)
                  │
        el-ballista scheduler
     ┌────────────┼────────────┐
     ▼            ▼            ▼
el-ballista  el-ballista  el-ballista
  worker       worker       worker
     └────────────┼────────────┘
                  ▼
     Arrow stream to the consumer
```

What exists today (`connector::postgres::distributed`):

- Real `el-ballista scheduler` and `el-ballista worker` processes; stock Ballista executors cannot decode the
  Postgres scan plans, so the crate ships its own binaries with plan codecs. Standalone runs are
  plain DataFusion and never touch Ballista.
- Connection budget: each worker gets `pool_max / workers`, so the total stays `pool_max`.
- A watchdog cancels and re-submits a job whose worker died, because Ballista 54 alone would
  leave it "Running" forever; with no worker left the job aborts. See
  [`running.md`](running.md).
- The whole distributed scan is one checkpoint split.

Exit criteria:

| Criterion | Evidence |
| --- | --- |
| A workload that saturates one machine scales across three with better wall-clock time | Not measured. On one host, distributed is slower by design (66.9 s vs 33.7 s standalone for the 50M-row full load) because every row crosses a shuffle and Arrow Flight |
| … without increasing source load | Met by construction: the per-worker pool budget is unit-tested |
| Distributed results are correct | Met: 1 vs 3 workers return identical rows (`tests/pg_matrix.rs`); a worker killed mid-job does not hang it (`tests/pg_distributed.rs`) |

Before adding machines, confirm the bottleneck is compute and not the source or the network: if
it is the source, distribution makes things worse.

---

## Phase 5 — MySQL connector

```
Postgres ──┐
MySQL   ───┼──► Arrow / DataFusion
```

The second connector tests whether the connector interface is a real abstraction rather than a
Postgres wrapper. MySQL is a useful test because it is weaker at most of what Postgres does
well: no bulk export, no exportable snapshot, opt-in histograms, values Arrow cannot represent.

What exists today: a prototype in `src/connector/mysql/` (connect, `information_schema` schema
reading, full-table extraction to typed Arrow, streamed per `batch_size` or materialized). No
pushdown, no `TableProvider`, no parallel or distributed execution, no checkpointed jobs. See
[§0 of the MySQL doc](connectors/mysql.md) for the exact list.

Not implemented, designed in [`connectors/mysql.md`](connectors/mysql.md):

- Collation-aware pushdown (`_ci` / `_ai` string comparisons push as `Inexact`).
- Opt-in handling of zero dates (`0000-00-00`); today they fail the extraction with a typed error.
- Replica lag bounding (GTID anchoring, `Seconds_Behind_Source` from `SHOW REPLICA STATUS`).
- Keyset partitioning (there is no `ctid` analogue), with parallel scans trading snapshot
  consistency, since MySQL has no exportable snapshot.
- `Dictionary` encoding for `ENUM` and `Decimal256` for `DECIMAL` wider than 38 digits.

Exit criteria (not met): the MySQL differential correctness suite passes, including collation
fidelity (`_ci`, `_cs`, `_bin`); zero-date handling is configurable; replica lag is bounded and
monitored; the connector interface needed no breaking change for MySQL, or the change is
documented as a lesson.

---

## What changed along the way

Each phase shipped roughly what its [plan](history/README.md) described; several parts were
later removed to keep the contract to "Arrow batches out, at-least-once per split":

- **Phase 1** shipped a Parquet sink and watermark-based incremental extraction. The sink was
  removed: writing belongs to the caller, and two examples show writing Parquet from the stream.
- **Watermark / incremental state and `el-ballista backfill`** were removed. Incremental and backfill
  runs are filtered extractions over a caller-supplied range; the design is kept in
  [`deferred/incremental-extraction.md`](deferred/incremental-extraction.md).
- **Phase 2** planned a DataFusion optimizer rule, `SourceAwarePushdownRule`. It was deleted:
  DataFusion's own filter pushdown calls `supports_filters_pushdown`, and the provider decides
  there.
- **Phase 4** first ran an in-process Ballista cluster for single-machine runs. It was removed:
  standalone is plain DataFusion (the fastest mode in the benchmark), and distributed always
  means real scheduler and worker processes.
- **Tests** first ran against an embedded database. They now run against the Docker Compose
  stack in `tests/docker/` (Postgres 17 and MySQL 8), the same locally and in CI.

---

## Not in scope

The layer extracts from a source database into Arrow and stops there.

| Item | Owned by |
| --- | --- |
| Writing Parquet / CSV / JSON | The caller's consumer (DataFusion writers or `parquet::arrow::ArrowWriter`) |
| Object store upload (GCS, S3), warehouse loads (BigQuery) | The caller or the orchestrator |
| Publishing written output atomically per split | The caller's consumer; the layer records a split as completed only after the consumer returns `Ok` |
| Watermark management, incremental state, backfill scheduling | The orchestrator, through filters in the job spec |
| Scheduler, resource manager, cluster UI | Ballista and the orchestrator |

## Deferred

| Item | Status | Notes |
| --- | --- | --- |
| [Python bindings](python-bindings.md) | Deferred | Design only, kept so the Rust API stays bindable |
| Log-based CDC ([Postgres](connectors/postgres.md#8-out-of-scope-cdc), [MySQL](connectors/mysql.md#9-future-binlog-cdc-design-not-implemented)) | Not implemented | Replication slots and binlog retention are operational hazards |
| Metrics export | Not implemented | `src/telemetry.rs` records counters and a histogram through the `metrics` crate (rows, batches, batch bytes, split outcomes, pushdown decisions), but no exporter is installed; a host process has to install a recorder to see them |
| Snapshot-consistent parallel scans (exported snapshots) | Not implemented | Partitions are not mutually consistent today |
| Aggregate and join pushdown | Not implemented | High translation risk, low value for extraction |
| Cross-source joins | Not implemented | Better handled by DataFusion or the orchestrator |
| More connectors (SQL Server, MongoDB, ScyllaDB) | Not implemented | The MySQL connector comes first |
| Native / modulo partitioning | Not implemented | Keyset and `ctid` only |
| Object store sources (Parquet / CSV) | Not planned | DataFusion reads these natively |
| Web UI | Not planned | Orchestrators have their own; the CLI, logs and run reports are the interface |

---

## What would make this project fail

- **Scope creep into building Spark.** Scheduler, resource manager and cluster UI are
  DataFusion's, Ballista's or the orchestrator's responsibility.
- **Incorrect pushdown.** A correctness bug in pushdown produces wrong results. Fidelity rules
  and the differential test suite address this.
- **Becoming a sink framework.** Parquet writers, BigQuery integration and S3 upload are
  DataFusion's, Ballista's or the orchestrator's responsibility. This project extracts to Arrow
  and stops.
