# Architecture

Technical reference for the Rust Extract Layer. This document covers the crate layout, the
lifecycle of a job from API call to committed checkpoint, the data model, execution and memory
management, configuration, and observability. The core is a source-aware extraction layer on
DataFusion/Ballista that outputs native Arrow.

For the reasoning behind the design, start with the [README](../README.md). For the parts that get
their own documents, see [pushdown](pushdown.md), [incremental extraction](incremental-extraction.md),
and [connectors](connectors/README.md). Sinks are out of scope.

---

## 1. Dependency stance

We are a *host application* for DataFusion, not a fork of it. Everything we add plugs in through
public extension points:

| Extension point | What we register | Status |
| --- | --- | --- |
| `TableProvider` | One per source table, wrapping a connector (`PostgresTableProvider`) | Implemented |
| `supports_filters_pushdown` | Per-filter `Exact` / `Inexact` / `Unsupported` capability decisions | Implemented |
| `ExecutionPlan` | `PostgresExecutionPlan` — a streaming scan node with per-partition source queries | Implemented |
| `OptimizerRule` | `SourceAwarePushdownRule` — the cost-based push/keep decision | Implemented |
| `TableProviderFactory` / `SchemaProvider` | Catalog binding for `ctx.source("ref", "table")` | Deferred — the engine registers providers directly |
| `ScalarUDF` / `AggregateUDF` | Extraction-specific functions (e.g. watermark helpers) | Deferred |
| `ObjectStore` registry | GCS, S3, local filesystem for sinks | Deferred — sinks are out of scope (see roadmap) |

Consequences worth stating explicitly: we inherit DataFusion's optimizer rules for projection,
filter, and limit pushdown for free, and we inherit its release cadence (roughly one major every
8–10 weeks, with breaking changes each time).

**Version policy.** Pin exact versions in the crate root `Cargo.toml`; upgrade DataFusion
deliberately as a single dedicated change, never incidentally. Arrow's version must be the one
DataFusion depends on — two Arrow versions in the graph produce type errors that look like
nonsense because `arrow::datatypes::Schema` from one version is not the same type as from
another. (There is no workspace; the project is a single crate with a `lib` + `bin` target.
A `main.rs` note: the binary re-declares the same modules rather than depending on the lib —
see `docs/testing-plan.md` §6 for why that duplication is on the cleanup list.)

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
│   ├── lib.rs / main.rs        # main.rs uses `use rust_ballista_extraction_layer::...`
│   ├── config/                 # job-spec parsing (JSON), per-layer config structs
│   ├── types/                  # TableMetadata / ColumnMetadata
│   ├── connector/
│   │   ├── mod.rs              # the Source SPI contract (see below)
│   │   ├── errors.rs           # ExtractorError
│   │   ├── postgres/           # schema_reader, arrow_type_mapper, row_adapter,
│   │                           # query_builder, parallel, table_provider, execution_plan, extractor
│   ├── pushdown/               # Predicate IR, translate/decide, dialect, cost_model,
│   │                           # stats, explain, optimizer_rule
│   ├── incremental/            # watermarks, windows, clamp_to_observed
│   ├── checkpoint/             # JsonCheckpointStore: leases + run history on local fs
│   ├── distributed/            # Ballista codecs, pool registry, DistributedContext
│   ├── engine/                 # ExtractContext: DataFusion session + builder API
│   ├── cli/                    # binary commands: run, plan, checkpoint, backfill,
│   │                           # distribute, scheduler, worker, demo
│   ├── demo.rs                 # no-arg demo pipeline
│   └── errors.rs               # AppError
├── docs/
└── examples/                   # runnable pipelines (see examples/configs/*.json)
```

The Source SPI contract lives in `src/connector/mod.rs`: every backend answers four questions
without the rest of the system knowing which database it is — *what SQL?* (`Predicate::render_to`
into a `SqlSink`, `SqlDialect` conventions), *what window is safe?* (`WatermarkSource`),
*what does it cost?* (`TableStatsSource`), *which pool?* (`SourceDescriptor`). The planning SPI
itself is DataFusion's (`TableProvider` / `ExecutionPlan`). Deliberately backend-concrete:
`sqlx` pools and `QueryBuilder` binding — another backend would own an analogous registry over its
own pool type following the same pattern.

---

## 3. Job lifecycle

```
  (1) API call / job YAML
        │
        ▼
  (2) Logical plan          DataFusion LogicalPlan, sources bound to TableProviders
        │
        ▼
  (3) Watermark resolution  checkpoint store → concrete predicate injected into the plan
        │
        ▼
  (4) Optimization          DataFusion rules + SourceAwarePushdown
        │                   ├── which filters go to the source (Exact/Inexact/keep)
        │                   ├── which columns the source must return
        │                   └── how the scan is partitioned
        ▼
  (5) Physical plan         PostgresExecutionPlan(partition queries) ──► Arrow operators
  │                       ──► sink (DataFusion writers in examples; sinks are out of scope)
        │
        ▼
  (6) Execution             N concurrent source streams → RecordBatch → DataFusion → writer
        │
        ▼
  (7) Sink durability       Parquet file finalized locally (object-store sinks deferred;
  │                         see roadmap — out of scope for this project)
        ▼
  (8) Checkpoint commit     new watermark persisted — only after (7) succeeds
```

Steps 7 and 8 are ordered and non-atomic on purpose. See
[incremental extraction](incremental-extraction.md#5-the-commit-protocol) for why this yields
at-least-once delivery with idempotent object naming rather than a distributed transaction.

### Worked example

```rust
let ctx = ExtractContext::from_config(config).await?;
let batches = ctx.source("postgres", "public.orders").await?
    .incremental(Watermark::timestamp("updated_at")).await?
    .filter(col("status").eq(lit("PAID")))?
    .select(vec![col("order_id"), col("user_id"), col("amount")])?
    .collect().await?;
```

After watermark resolution and optimization, the planner has decided that `updated_at` is indexed
and highly selective (push it); `status` is a low-cardinality text column with no usable index
(keep it under `cost_based`, or push it as `Inexact` under `always` — either way DataFusion
re-checks it, because string comparisons are never `Exact`). The resulting source query,
per partition:

```sql
SELECT order_id, user_id, amount, updated_at
FROM   public.orders
WHERE  updated_at >  $1 AND updated_at <= $2
  AND  status = $3
  AND  order_id >= 1 AND order_id < 25001   -- keyset partition predicate, one range per scan task
```

Change one input — say the table is on a hot production primary — and set
`policy = "strict"`: only indexed, selective, primitive-typed predicates push, `LIMIT` never
pushes. `rel plan --explain` prints each decision with its reasoning:

```
policy: cost_based
  status='PAID'  -> Inexact (PUSH (Inexact; low selectivity (20.00%) and cost (4882) within budget (50000); ("status" = $1)))
  amount>=100    -> Unsupported (KEEP (unsupported expression; stays in Arrow))
```

---

## 4. Data model

Arrow `RecordBatch` is the only currency that crosses a component boundary. There is no
`Vec<Row>` anywhere in a hot path.

```
Row-oriented (what we avoid)      Columnar (what we use)
────────────────────────────      ──────────────────────
Row 1 → Row 2 → Row 3 → …         order_id: [1, 2, 3, 4, 5]
per-row dispatch                  user_id:  [9, 4, 7, 7, 2]
pointer chasing                   amount:   [10.2, 11.4, 9.2, 7.1, 8.3]
no SIMD                           validity: [1,1,1,0,1] bitmaps
```

This buys SIMD-friendly kernels, cache efficiency, cheap column pruning, better compression, and
near-zero-copy handoff to Parquet and to any Arrow consumer.

**Schema is resolved once, at plan time.** Every connector must produce a stable Arrow `Schema`
before execution begins, because DataFusion plans against it. Runtime schema surprises (a column
whose type differs from the catalog, a `NUMERIC` that overflows the declared decimal precision)
are errors, not silent coercions — with one deliberate exception: values that are representable in
the source but not in Arrow (Postgres `timestamp 'infinity'`) map to null and
increment a counter. Silent nulls without a metric are how data quality bugs hide for months.

**Batch sizing.** The connector produces batches of `batch_size` rows (default 8192, configurable).
Too small and per-batch overhead dominates; too large and memory spikes, particularly with wide
string columns. The streaming builders accumulate directly into Arrow builders (one batch at a
time, bounded memory) — with one caveat: rows arrive through `sqlx` as `PgRow` values first,
so "no intermediate row struct" holds for the Arrow side, not the driver side.

**Metadata columns.** Deferred: batches currently carry exactly the source columns, no
`_extracted_at` / `_extracted_date` / `_source` / `_watermark_hi` appended. Likewise, values
representable in the source but not in Arrow are errors today, not nulls-with-a-counter —
the `rel_null_coerced_total` metric in §7 presupposes instrumentation that doesn't exist yet
(see §7).

---

## 5. Execution and memory

Single-node execution runs on a Tokio multi-threaded runtime. The concurrency story has three
distinct knobs that are easy to conflate:

- **Source partitions** — how many concurrent queries hit the database. Bounded by connection pool
  size (`pool_max`, divided per worker in distributed mode) and, more importantly, by what the
  source can absorb without hurting production.
- **DataFusion target partitions** — CPU parallelism for Arrow operators. Currently left at
  DataFusion defaults (never explicitly configured).
- **Sink writers** — concurrent object-store uploads. Deferred with the rest of the sink layer;
  examples write one local Parquet file.

Backpressure flows naturally on the streaming scan path: `PostgresExecutionPlan` yields a
`SendableRecordBatchStream`, and if the sink is slow the stream stops being polled, which stops
reading from the socket, which applies TCP backpressure to the database. The `PostgresExtractor`
cursor-based methods also stream via `FETCH` batches, so backpressure propagates through the
portal. All scan paths stream; no path buffers the full result set.

DataFusion's `MemoryPool` is not currently configured (no `FairSpillPool`, no `RuntimeEnv`
tuning) — spills are unbounded by default. Likewise there is no per-connector cap on in-flight
batches beyond `batch_size` accumulation. Both are known gaps, not design decisions.

### Distributed execution (Phase 4, implemented)

Ballista distributes DataFusion across a scheduler and long-running workers (`rel scheduler` /
`rel worker`), using Arrow IPC for shuffle exchange. The scan plan travels as JSON behind a
magic prefix, decoded by per-process extension codecs; each process resolves a `SourceDescriptor`
to its `pool_max / workers` share of source connections through a process-wide pool registry,
so a three-worker deployment shows the source the same connection count as one machine. The
constraint this imposes on new code is modest but real: keep physical plan nodes serializable
and never smuggle non-serializable state (pools, passwords) into `ExecutionPlan`
implementations. See [roadmap](roadmap.md#phase-4--distributed-execution) and
`docs/phase-four-implementation-plan.md`.

---

## 6. Configuration

One JSON job spec per job (see `examples/configs/extract.example.json`) — no separate
connection catalog, no TOML. Credentials are managed once, by name, never inline:

```json
{
  "job_id": "orders_incremental",
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
  "incremental": { "column": "updated_at", "safety_lag_secs": 300, "max_window_secs": 21600 },
  "checkpoint": { "dir": "./.checkpoints" },
  "pushdown": {
    "policy": "cost_based",
    "deny": [],
    "push": [],
    "max_source_cost": 50000,
    "keep_threshold": 0.30,
    "statistics_ttl_secs": 900
  },
  "parallel_scan": { "strategy": "none", "partitions": 1, "partition_column": "order_id" },
  "execution": { "batch_size": 8192 },
  "distributed": { "scheduler_url": "", "workers": 2 }
}
```

Notes against the original TOML sketch this replaces:

- `password_env` names the environment variable holding the password (resolved in whichever
  process opens the pool — scheduler, worker, and client each resolve it independently).
  There is no `dsn_env` and no `[sources.*]` catalog.
- Checkpoints are local-directory JSON only (`JsonCheckpointStore`); there is no postgres/gcs
  checkpoint backend.
- There is no `[sinks.*]` section — sinks are out of scope; examples write Parquet via
  DataFusion writers.

What survived from the sketch: every source connects with a distinct, identifiable
`application_name` so a DBA can see exactly what this tool is doing in `pg_stat_activity`,
and a statement timeout is effectively mandatory — an extraction job must never be the reason
a production database holds a long-running query (enforced per-connection, plus lock and
idle-in-transaction timeouts).

---

## 7. Observability

Logging today is the `log` crate (a `SimpleLogger` in the binary, `RUST_LOG`-gated) — one line
per job, window, partition scan, and checkpoint commit. There is no `tracing`, no spans, and
no metrics endpoint. The table below is the instrumentation we want, not what exists; each
row is deferred work, with the code hook it would attach to in parentheses:

| Metric | Type | Why it matters |
| --- | --- | --- |
| `rel_rows_extracted_total{source,table}` | counter | Throughput and reconciliation (hook: `run_job`/`run_distributed` commit) |
| `rel_bytes_from_source_total{source,table}` | counter | Directly measures pushdown effectiveness |
| `rel_source_query_duration_seconds` | histogram | Detects a pushdown that made the DB slow (hook: `build_query` execution) |
| `rel_pushdown_decision_total{operator,decision}` | counter | Is the cost model actually deciding, or always saying yes? (hook: `decide_cost`) |
| `rel_watermark_lag_seconds{source,table}` | gauge | The single most important freshness signal |
| `rel_null_coerced_total{source,table,column,reason}` | counter | Unrepresentable values silently becoming null (needs the §4 null-mapping first) |
| `rel_checkpoint_commit_total{status}` | counter | Failed commits mean duplicate work next run |

`rel_watermark_lag_seconds` deserves a standing alert. It is the metric that tells you a pipeline
has been quietly extracting nothing for six hours.
