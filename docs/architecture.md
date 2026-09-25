# Architecture

Technical reference for the Rust Extract Layer. This document covers the crate layout, the
lifecycle of a job from API call to split-checkpoint commit, the data model, execution and memory
management, configuration, and observability. The core is a source-aware extraction layer on
DataFusion/Ballista that outputs native Arrow.

For the reasoning behind the design, start with the [README](../README.md). For the parts that get
their own documents, see [pushdown](pushdown.md)
and [connectors](connectors/README.md). Watermark/incremental design is deferred under
[deferred](deferred/incremental-extraction.md). Writing the data somewhere (a "sink") is the
caller's job: this layer hands out Arrow batches and stops.

---

## 1. Dependency stance

We are a *host application* for DataFusion, not a fork of it. Everything we add plugs in through
public extension points:

| Extension point | What we register | Status |
| --- | --- | --- |
| `TableProvider` | One per source table, wrapping a connector (`PostgresTableProvider`) | Implemented |
| `supports_filters_pushdown` | Per-filter `Exact` / `Inexact` / `Unsupported` capability decisions | Implemented |
| `ExecutionPlan` | `PostgresExecutionPlan` — a streaming scan node with per-partition source queries | Implemented |
| `OptimizerRule` | None. The cost-based push/keep decision runs inside `supports_filters_pushdown`, driven by DataFusion's own `PushDownFilter` (the earlier `SourceAwarePushdownRule` was deleted) | Not used |
| `TableProviderFactory` / `SchemaProvider` | Catalog binding for `ctx.source("ref", "table")` | Deferred — the engine registers providers directly |
| `ScalarUDF` / `AggregateUDF` | Extraction-specific functions | Deferred |
| `ObjectStore` registry | GCS, S3, local filesystem for writers | Not used — writing output is out of scope (see roadmap) |

Consequences worth stating explicitly: we inherit DataFusion's optimizer rules for projection,
filter, and limit pushdown for free, and we inherit its release cadence (roughly one major every
8–10 weeks, with breaking changes each time).

**Version policy.** Pin exact versions in the crate root `Cargo.toml`; upgrade DataFusion
deliberately as a single dedicated change, never incidentally. Arrow's version must be the one
DataFusion depends on — two Arrow versions in the graph produce type errors that look like
nonsense because `arrow::datatypes::Schema` from one version is not the same type as from
another. (There is no workspace; the project is a single crate with a `lib` + `bin` target.
`main.rs` is a thin wrapper: it declares only the bin-only `cli` and `demo` modules and uses
the library for everything else.)

```toml
[dependencies]
datafusion  = "54.1.0"
ballista    = "54.1.0"      # scheduler + executor + client, same version as datafusion
arrow       = "58.4"        # must match datafusion's arrow
parquet     = "58.4"
tokio       = { version = "1", features = ["full"] }
sqlx        = { version = "0.9.0", features = ["runtime-tokio", "tls-rustls", "postgres", ...] }
```

---

## 2. Crate layout

A single crate (`lib` + `bin`), organized by layer. There is no workspace; the split below
is by module, chosen so a future extraction into crates (or a Python wrapper binding against
one obvious surface) stays mechanical. The `rel-*` crate names from early planning are kept
in comments where they map 1:1, but nothing is split yet.

```
rust-ballista-extraction-layer/
├── Cargo.toml                  # single crate; exact pins, see §1 version policy
├── src/
│   ├── lib.rs                  # pub mod checkpoint, config, connector, errors, logging, pushdown, types
│   ├── main.rs                 # `rel` binary: bin-only `cli/` + `demo.rs`, everything else from the lib
│   ├── config/                 # strict JSON job spec (deny_unknown_fields), JobConfig + blocks
│   ├── types/                  # TableMetadata / ColumnMetadata, JobId newtype
│   ├── checkpoint/             # CheckpointStore trait, JsonCheckpointStore, plan fingerprint,
│   │                           # per-job lock (heartbeat + TTL), progress file
│   ├── pushdown/               # connector-agnostic: Predicate IR (ir), Expr→IR translation with
│   │                           # fidelity (translate), policy, cost_model, SqlDialect trait,
│   │                           # backend-neutral stats / explain types
│   ├── connector/
│   │   ├── mod.rs              # the Source SPI contract (see below)
│   │   ├── errors.rs           # ExtractorError
│   │   ├── query_tag.rs        # SQL comment tags for pg_stat_activity
│   │   ├── postgres/           # everything Postgres-specific:
│   │   │   ├── api.rs          #   PostgresConnector builder API (collect/stream/run/run_with)
│   │   │   ├── pipeline/       #   job pipeline: filters, splits, run (checkpointed run_with)
│   │   │   ├── engine/         #   ExtractContext: DataFusion session + DataFrame builder
│   │   │   ├── distributed/    #   Ballista codecs, connection descriptor, pool registry, context
│   │   │   ├── table_provider.rs, execution_plan.rs   # DataFusion TableProvider / ExecutionPlan
│   │   │   ├── extractor.rs, copy.rs, row_adapter.rs  # cursor + binary COPY scans, decoders
│   │   │   ├── schema_reader.rs, arrow_type_mapper.rs, query_builder.rs, parallel.rs
│   │   │   └── dialect.rs, param_sink.rs, inline_sql.rs, stats.rs, explain.rs
│   │   └── mysql/              # prototype: dialect, schema_reader, type_mapper, row_adapter,
│   │                           # extractor, query_builder
│   ├── cli/                    # (bin) run, distribute, plan, checkpoint show|reset, scheduler,
│   │                           # worker, demo
│   ├── logging.rs              # `log` + fern setup (stderr + optional file)
│   └── errors.rs               # AppError (typed variants, #[source] kept)
├── docs/
├── examples/                   # runnable pipelines (see examples/configs/*.json)
└── tests/                      # integration suites + fixtures (docs/testing-plan.md)
```

There are no crate-root re-exports of the Postgres modules: import
`rust_ballista_extraction_layer::connector::postgres::{PostgresConnector, pipeline, engine,
distributed, …}` directly.

The Source SPI contract lives in `src/connector/mod.rs`: every backend answers four questions
without the rest of the system knowing which database it is — *what SQL?* (`Predicate::render_to`
into a `SqlSink`, `SqlDialect` conventions),
*what does it cost?* (`TableStatsSource`), *which pool?* (`SourceDescriptor`).
Caller-provided filter predicates are the only range mechanism; the layer manages no watermarks. The planning SPI
itself is DataFusion's (`TableProvider` / `ExecutionPlan`). Deliberately backend-concrete:
`sqlx` pools and `QueryBuilder` binding — another backend would own an analogous registry over its
own pool type following the same pattern. Pushed filters are decided per filter in
`PostgresTableProvider::supports_filters_pushdown` and rendered with `COLLATE "C"` for text,
bound parameters for every literal (see [pushdown](pushdown.md)).

---

## 3. Job lifecycle

```
  (1) API call              PostgresConnector::from_config(cfg)?  (validates the strict job spec)
        │                   .extract().standalone() | .distributed()
        ▼
  (2) Plan                  table metadata → Arrow schema (build_arrow_schema, once);
        │                   config filters → typed DataFusion Exprs (schema-coerced literals);
        │                   splits = keyset/ctid partitions (or one whole-table split)
        ▼
  (3) Pushdown decision     supports_filters_pushdown per filter: Exact / Inexact / keep,
        │                   by fidelity (translate) + policy (cost_based, always, …)
        ▼
  (4) Physical plan         PostgresExecutionPlan: one source query per partition
        │                   (DECLARE … CURSOR + FETCH, or binary COPY when no bound params)
        ▼
  (5) Terminal              collect()  — materializes every batch; no checkpoint
        │                   stream()   — one bounded stream over all splits; no checkpoint
        │                   run()      — DIAGNOSTIC: counts rows, discards them; no checkpoint
        │                   run_with(consumer) — the operational, checkpointed job:
        ▼
  (6) run_with              take the per-job lock (O_EXCL file + heartbeat, lock_ttl_secs);
        │                   begin(plan): new → all splits Pending with their bounds;
        │                              same plan fingerprint → Completed kept, rest Pending;
        │                              different plan → CheckpointError::PlanMismatch
        ▼
  (7) Per pending split     mark Running → consumer(split, stream) → consumer returns Ok
        │                   having drained the stream → mark Completed (rows delivered);
        │                   consumer Err / undrained stream / source error → mark Failed
        ▼
  (8) Finish                all splits Completed → Ok(RunOutcome); any Failed →
                            AppError::SplitsFailed listing them; lock released (RAII)
```

The checkpoint contract: **a split is Completed only after the consumer acknowledged it**, so
extraction failure never advances the checkpoint and a retry re-runs exactly the unfinished
splits with their **stored** bounds (not bounds recomputed from a table that has since changed).
Delivery is at-least-once per split — a split whose consumer failed or whose process died
mid-stream is re-delivered in full — so consumers should write per `split_id` idempotently.
`rel checkpoint reset` deletes a job's checkpoint (needed after changing its plan). The
distributed path treats the whole cluster query as one split. Nothing here is exactly-once.

### Isolation semantics

Each partition's scan is one snapshot: a `DECLARE … CURSOR WITHOUT HOLD` inside a transaction
at the server's default isolation (READ COMMITTED) — every `FETCH` of that cursor reads the
snapshot taken at `DECLARE` — or one `COPY` statement. Different partitions are different
statements on different connections, so they are **not mutually consistent**: a row updated
between two partitions' snapshots can be missed or seen twice if the update moves its partition
key across a bound. No cross-partition snapshot (exported snapshot) is used or claimed. Each
open partition holds back vacuum's `xmin` horizon while it runs.

### Worked example

```rust
use rust_ballista_extraction_layer::connector::postgres::engine::ExtractContext;

let ctx = ExtractContext::from_config(config).await?;
let batches = ctx.source("postgres", "public.orders").await?
    .filter(col("status").eq(lit("PAID")))?
    .select(vec![col("order_id"), col("user_id"), col("amount")])?
    .collect().await?;
```

The planner considers each caller-provided predicate on its merits. `status = 'PAID'` on a
text column translates to `("status" COLLATE "C") = $1`, which is `Exact` (byte-wise, like
Arrow), so the only question is policy: under `cost_based` a low-cardinality column with no
usable index may be **kept** in Arrow; under `always` it is pushed and DataFusion drops its
own filter. When pushed, the per-partition source query is:

```sql
SELECT "order_id", "user_id", "amount"
FROM   "public"."orders"
WHERE  (("status" COLLATE "C") = $1)
  AND  ("order_id" >= 25001 AND "order_id" < 50001)  -- keyset bounds (a middle partition)
```

(The first partition has no lower bound — `"order_id" < b1 OR "order_id" IS NULL` — and the last
is open-ended, so a resumed run that reuses stored bounds still covers keys inserted below the
planned MIN or above the planned MAX.)

Change one input — say the table is on a hot production primary — and set
`policy = "strict"`: only indexed, selective, primitive-typed predicates push. `rel plan` prints
each decision with its reasoning (exact reasons depend on live statistics — illustrative
output):

```
policy: cost_based
  status='PAID'  -> Keep (KEEP (selectivity above keep threshold; stays in Arrow))
  amount>=100    -> Keep (KEEP (selectivity above keep threshold; stays in Arrow))
```

---

## 4. Data model

Arrow `RecordBatch` is the only currency that crosses a component boundary. Decoders append
wire bytes straight into Arrow builders; there is no `Vec<Struct>` of rows in between (on the
cursor path sqlx still hands each row over as a `PgRow`).

```
Row-oriented (what we avoid)      Columnar (what we use)
────────────────────────────      ──────────────────────
Row 1 → Row 2 → Row 3 → …         order_id: [1, 2, 3, 4, 5]
per-row dispatch                  user_id:  [9, 4, 7, 7, 2]
pointer chasing                   amount:   [10.2, 11.4, 9.2, 7.1, 8.3]
no SIMD                           validity: [1,1,1,0,1] bitmaps
```

This buys SIMD-friendly kernels, cache efficiency, cheap column pruning and better compression.
Getting there is not free: every value is decoded from the Postgres wire format into Arrow
buffers once, and `ArrayRef`s are then shared (not copied) between DataFusion operators.

**Schema is resolved once, at plan time.** Every connector must produce a stable Arrow `Schema`
before execution begins, because DataFusion plans against it. Runtime schema surprises (a column
whose type differs from the catalog, a `NUMERIC` that overflows the declared decimal precision)
are errors, not silent coercions. That includes values representable in the source but not in
Arrow: Postgres `timestamp`/`date` `±infinity`, numeric `NaN`/`±Infinity`, and numeric digits
beyond the column's `Decimal128` precision/scale fail the scan with
`ExtractorError::UnsupportedValue` naming the column — never a silent null or truncation.

**Batch sizing.** The connector produces batches of at most `execution.batch_size` rows
(default 8192) and flushes early at `execution.max_batch_bytes` (default 16 MiB) so wide rows
cannot blow memory. `batch_size` is also the cursor `FETCH FORWARD` window. Too small and
per-batch overhead dominates; too large and memory spikes.

**Metadata columns.** Deferred: batches carry exactly the source columns, no
`_extracted_at` / `_extracted_date` / `_source` appended.

---

## 5. Execution and memory

Single-node execution runs on a Tokio multi-threaded runtime. The concurrency story has three
distinct knobs that are easy to conflate:

- **Source partitions** — how many concurrent queries hit the database. Bounded by connection pool
  size (`pool_max`, divided per worker in distributed mode) and, more importantly, by what the
  source can absorb without hurting production.
- **DataFusion target partitions** — CPU parallelism for Arrow operators. Currently left at
  DataFusion defaults (never explicitly configured).
- **Consumer concurrency** — `run_with` calls the consumer for up to
  `min(execution.concurrent_partitions, pool size)` splits at a time. What the consumer does
  with the batches (write Parquet, upload, …) is the caller's code; examples write one local
  Parquet file.

Backpressure flows on the streaming scan path: `PostgresExecutionPlan` yields a
`SendableRecordBatchStream` fed through a bounded channel (capacity 2 batches) by the partition's
scan task; if the consumer is slow the channel fills, the scan task stops issuing `FETCH`es (or
stops reading the `COPY` stream), which applies TCP backpressure to the database. Dropping the
stream stops the source: the cursor is closed and its transaction rolled back, and an unfinished
`COPY` is cancelled.

Not every entry point streams. Bounded-memory: the builders' `stream()` and `run_with()`,
DataFusion's `execute_stream()` over the provider, and the extractor's `*_for_each_batch`
methods. **Materializing (whole result in memory):** the builders' `collect()`, the
`ExtractContext` DataFrame `collect()`, `PostgresExtractor::extract_full_table` /
`extract_keyset_partition` (one concatenated batch — meant for tests and small tables), and
the MySQL prototype's `extract_full_table`. Use them only when the result fits in memory.

DataFusion's `MemoryPool` is not currently configured (no `FairSpillPool`, no `RuntimeEnv`
tuning) — spills are unbounded by default. Likewise there is no per-connector cap on in-flight
batches beyond `batch_size` accumulation. Both are known gaps, not design decisions.

### Distributed execution (Phase 4, implemented)

Ballista distributes DataFusion across a scheduler and long-running workers (`rel scheduler` /
`rel worker`), using Arrow IPC for shuffle exchange. The scan plan travels as JSON behind a
magic prefix, decoded by per-process extension codecs; each process resolves a `SourceDescriptor`
to its `pool_max / workers` share of source connections through a process-wide pool registry,
so a three-worker deployment shows the source the same connection count as one machine. Within
a process, a **scan limiter** (one permit per budgeted connection, `SourcePoolRegistry::scan_slots`)
makes partition scans beyond the budget *wait* — cancellably, without the pool's acquire timeout —
instead of failing. With a remote scheduler, `.distributed().scheduler(url)` checks the
registered executors through the scheduler's REST API (`GET /api/executors`): more executors
than `workers` is refused (the source would exceed `pool_max`); fewer executors, or more task
slots than connections, only warns; an unreachable/disabled REST API means "not verified" (warning).
The planning client uses the same per-process share for metadata queries. The
constraint this imposes on new code is modest but real: keep physical plan nodes serializable
and never smuggle non-serializable state (pools, passwords) into `ExecutionPlan`
implementations. See [roadmap](roadmap.md#phase-4--distributed-execution) and
`docs/roadmap/phase-four-implementation-plan.md`. For checkpointing, the whole distributed scan
is a single split (`split-0`).

---

## 6. Configuration

One JSON job spec per job (see `examples/configs/extract.example.json`) — no separate
connection catalog, no TOML. Credentials are managed once, by name, never inline:

```json
{
  "job_id": "orders_extract",
  "table": "orders",
  "columns": ["order_id", "user_id", "status", "amount", "created_at", "updated_at"],
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
  "filters": [{ "column": "status", "op": "=", "value": "PAID" }],
  "checkpoint": { "dir": "./.checkpoints", "lock_ttl_secs": 1800 },
  "pushdown": {
    "policy": "cost_based",
    "deny": [],
    "push": [],
    "max_source_cost": 50000,
    "keep_threshold": 0.30,
    "statistics_ttl_secs": 900
  },
  "parallel_scan": { "strategy": "none", "partitions": 1, "partition_column": "order_id" },
  "execution": { "batch_size": 8192, "max_batch_bytes": 16777216, "concurrent_partitions": 4, "use_copy": false },
  "distributed": { "scheduler_url": "", "workers": 2 }
}
```

Notes against the original TOML sketch this replaces:

- `password_env` names the environment variable holding the password (resolved in whichever
  process opens the pool — scheduler, worker, and client each resolve it independently).
  There is no `dsn_env` and no `[sources.*]` catalog.
- Checkpoints are local-directory JSON split-state only (`JsonCheckpointStore` — one file
  per job recording per-split `Pending`/`Running`/`Completed`/`Failed`, the plan fingerprint and
  each split's bounds, plus a lock file); there is no postgres/gcs checkpoint backend and no
  watermark state. `checkpoint.lock_ttl_secs` (default 1800) bounds how long a crashed run's
  lock blocks the next one.
- There is no `sink` block: the spec is strict (`deny_unknown_fields` in every block), so a
  leftover `"sink"` is a load error. Enumerations are lowercase strings
  (`parallel_scan.strategy`: `none`/`keyset`/`ctid`; `pushdown.policy`: `always`/`never`/
  `cost_based`/`strict`/`hinted`), and `job_id` is validated into a `JobId`.

What survived from the sketch: every source connects with a distinct, identifiable
`application_name` so a DBA can see exactly what this tool is doing in `pg_stat_activity`,
and a statement timeout is effectively mandatory — an extraction job must never be the reason
a production database holds a long-running query (enforced per-connection, plus lock and
idle-in-transaction timeouts).

---

## 7. Observability

Logging today is the `log` crate (a `fern` backend in the binary writing to stderr plus an
optional file, level from `--log-level`/`RUST_LOG`, file from `--log-file`/`REL_LOG_FILE`) —
one line per job, split, partition scan, and checkpoint commit, with every generated SQL query
and one line per Arrow batch (`split=`, `rows=`, `batch_bytes=`) at `debug` level. There is no
`tracing` and no spans.

**Metrics (implemented)** go through the [`metrics`](https://docs.rs/metrics) facade
(`src/telemetry.rs`): a no-op until the host process installs a recorder/exporter. Recorded per
batch, per split and per pushdown decision — never per row:

| Metric | Type | Labels |
| --- | --- | --- |
| `rel_extracted_rows` | counter | `job` |
| `rel_extracted_batches` | counter | `job` |
| `rel_batch_bytes` | histogram | `job` |
| `rel_splits` | counter | `job`, `outcome` = `completed` / `failed` / `skipped` |
| `rel_pushdown_decisions` | counter | `outcome` = `exact` / `inexact` / `kept` |

The table below is further instrumentation we want, not what exists; each row is deferred work,
with the code hook it would attach to in parentheses:

| Metric | Type | Why it matters |
| --- | --- | --- |
| `rel_bytes_from_source_total{source,table}` | counter | Directly measures pushdown effectiveness |
| `rel_source_query_duration_seconds` | histogram | Detects a pushdown that made the DB slow (hook: `build_query` execution) |
| `rel_pushdown_decision_total{operator,decision}` | counter | Per-operator breakdown of the implemented `rel_pushdown_decisions` (hook: `decide_cost`) |
| `rel_checkpoint_commit_total{status}` | counter | Failed commits mean duplicate work next run |

`rel_checkpoint_commit_total{status=failed}` deserves a standing alert: failed split commits mean
duplicate work on the next run.
