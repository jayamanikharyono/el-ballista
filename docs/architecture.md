# Architecture

Technical reference for the Rust Extract Layer. This document covers the crate layout, the
lifecycle of a job from API call to committed checkpoint, the data model, execution and memory
management, configuration, and observability.

For the reasoning behind the design, start with the [README](../README.md). For the parts that get
their own documents, see [pushdown](pushdown.md), [incremental extraction](incremental-extraction.md),
[connectors](connectors/README.md), and [sinks](sinks.md).

---

## 1. Dependency stance

We are a *host application* for DataFusion, not a fork of it. Everything we add plugs in through
public extension points:

| Extension point | What we register |
| --- | --- |
| `TableProvider` | One per source table, wrapping a connector (`PostgresTable`, `MySqlTable`) |
| `supports_filters_pushdown` | Per-filter `Exact` / `Inexact` / `Unsupported` capability decisions |
| `ExecutionPlan` | `SourceScanExec` — a streaming scan node with per-partition source queries |
| `OptimizerRule` | `SourceAwarePushdown` — the cost-based push/keep decision |
| `TableProviderFactory` / `SchemaProvider` | Catalog binding for `ctx.source("ref", "table")` |
| `ScalarUDF` / `AggregateUDF` | Extraction-specific functions (e.g. watermark helpers) |
| `ObjectStore` registry | GCS, S3, local filesystem for sinks |

Consequences worth stating explicitly: we inherit DataFusion's optimizer rules for projection,
filter, and limit pushdown for free, and we inherit its release cadence (roughly one major every
8–10 weeks, with breaking changes each time).

**Version policy.** Pin exact minor versions in the workspace root `Cargo.toml`; upgrade DataFusion
deliberately as a single dedicated change, never incidentally. Arrow's version must be the one
DataFusion depends on — two Arrow versions in the graph produce type errors that look like
nonsense because `arrow::datatypes::Schema` from 55 is not the same type as from 56.

```toml
[workspace.dependencies]
datafusion  = "55.0"
arrow       = "56.0"      # must match datafusion's arrow
parquet     = "56.0"
tokio       = { version = "1", features = ["full"] }
tokio-postgres = "0.7"
mysql_async = "0.36"
object_store = { version = "0.13", features = ["gcp"] }
```

---

## 2. Crate layout

A Cargo workspace. The split is chosen so the connector crates can be developed and tested without
pulling in the sink or orchestration layers, and so a future Python wrapper has one obvious crate
to bind against.

```
rust-extract-layer/
├── Cargo.toml                  # workspace root, shared dependency versions
├── crates/
│   ├── rel-core/               # plan types, expression IR, error types, Arrow helpers
│   ├── rel-connector/          # the Source SPI: traits, capabilities, ScanPlan, type mapping
│   ├── rel-connector-postgres/ # PostgreSQL implementation
│   ├── rel-connector-mysql/    # MySQL implementation
│   ├── rel-planner/            # source-aware optimizer rule + cost model + policy engine
│   ├── rel-incremental/        # watermarks, checkpoint store, boundary logic
│   ├── rel-sink/               # Parquet writer, object store, BigQuery loader
│   ├── rel-engine/             # ExtractContext: ties DataFusion + connectors + sinks together
│   ├── rel-cli/                # `rel` binary: run, plan, explain, backfill, checkpoint
│   └── rel-python/             # PLACEHOLDER — deferred, see docs/python-bindings.md
├── docs/
└── examples/
```

Dependency direction is strictly downward; `rel-core` and `rel-connector` know nothing about
DataFusion internals beyond Arrow types and the expression IR, which keeps the door open to
swapping the execution engine if that ever becomes necessary.

```
rel-cli ──► rel-engine ──┬──► rel-planner ──┐
                         ├──► rel-incremental│
                         ├──► rel-sink       ├──► rel-connector ──► rel-core
                         └──► rel-connector-{postgres,mysql} ───────┘
```

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
  (5) Physical plan         SourceScanExec(partition queries) ──► Arrow operators ──► sink
        │
        ▼
  (6) Execution             N concurrent source streams → RecordBatch → DataFusion → writer
        │
        ▼
  (7) Sink durability       Parquet objects finalized in object storage
        │
        ▼
  (8) Checkpoint commit     new watermark persisted — only after (7) succeeds
```

Steps 7 and 8 are ordered and non-atomic on purpose. See
[incremental extraction](incremental-extraction.md#5-the-commit-protocol) for why this yields
at-least-once delivery with idempotent object naming rather than a distributed transaction.

### Worked example

```rust
ctx.source("orders_pg", "public.orders")
   .incremental(Watermark::timestamp("updated_at"))
   .filter(col("status").eq(lit("PAID")))
   .select(vec![col("order_id"), col("user_id"), col("amount")])
```

After watermark resolution and optimization, the planner has decided that `updated_at` is indexed
and highly selective (push it), `status` is a low-cardinality unindexed column (evaluating it in
Postgres costs a filter over the same rows the index already returned, so it is nearly free —
push it too), and the projection is trivially pushable. The resulting source query, per partition:

```sql
SELECT order_id, user_id, amount, updated_at
FROM   public.orders
WHERE  updated_at >  $1 AND updated_at <= $2
  AND  status = $3
  AND  (order_id % 4) = 0          -- partition predicate, only when parallel scan is enabled
```

Change one input — say `status` has a functional index and the filter is 99% selective, or the
table is on a hot production primary with a constrained CPU budget — and the planner produces a
different boundary. `rel plan --explain` prints the decision with its reasoning:

```
SourceScanExec: postgres(orders_pg) public.orders
  partitions: 4 (keyset on order_id)
  pushed:  updated_at > $1 AND updated_at <= $2   [Exact]   idx_orders_updated_at, sel≈0.002
  pushed:  status = $3                            [Exact]   no index, sel≈0.61, cost≈120 (below budget)
  kept:    json_extract(payload, '$.channel')     [Arrow]   est. 41× cheaper vectorized
  projection: 4 of 27 columns
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
the source but not in Arrow (MySQL's `0000-00-00`, Postgres `timestamp 'infinity'`) map to null and
increment a counter. Silent nulls without a metric are how data quality bugs hide for months.

**Batch sizing.** The connector produces batches of `batch_size` rows (default 8192, configurable).
Too small and per-batch overhead dominates; too large and memory spikes, particularly with wide
string columns. The connector decodes into Arrow builders directly from the wire format — it never
materializes an intermediate row struct.

**Metadata columns.** Every extracted batch gets these appended, because downstream loads and
debugging need them:

| Column | Type | Meaning |
| --- | --- | --- |
| `_extracted_at` | `Timestamp(Microsecond, "UTC")` | Wall-clock time the batch was read |
| `_extracted_date` | `Date32` | Partition key for the sink layout |
| `_source` | `Dictionary(Int8, Utf8)` | Connector reference name |
| `_watermark_hi` | source-dependent | The upper bound of the window this row came from |

---

## 5. Execution and memory

Single-node execution runs on a Tokio multi-threaded runtime. The concurrency story has three
distinct knobs that are easy to conflate:

- **Source partitions** — how many concurrent queries hit the database. Bounded by connection pool
  size and, more importantly, by what the source can absorb without hurting production.
- **DataFusion target partitions** — CPU parallelism for Arrow operators. Defaults to core count.
- **Sink writers** — concurrent object-store uploads.

Backpressure flows naturally: `SourceScanExec` yields a `SendableRecordBatchStream`, and if the
sink is slow the stream stops being polled, which stops reading from the socket, which applies TCP
backpressure to the database. This is the main reason connectors must *stream* rather than buffer
a full result set — a connector that buffers converts backpressure into an OOM.

DataFusion's `MemoryPool` bounds memory for spillable operators (sorts, hash joins, grouped
aggregates). We configure it explicitly rather than accepting unbounded growth:

```rust
let rt = RuntimeEnvBuilder::new()
    .with_memory_pool(Arc::new(FairSpillPool::new(4 * 1024 * 1024 * 1024)))
    .with_disk_manager_builder(DiskManagerBuilder::default())
    .build_arc()?;
```

Note that connector-side buffers are *not* accounted for by the pool unless we register them, so a
per-connector cap on in-flight batches is enforced separately by the scan node.

### Distributed execution (deferred)

Ballista distributes DataFusion across a scheduler and workers, using Arrow IPC for shuffle
exchange. It tracks DataFusion's version numbering (Ballista 54.x builds on DataFusion 54.x) and is
under active development, with current work focused on closing the gap with single-node DataFusion,
adaptive query execution, and operational predictability.

We do not adopt it until single-node throughput is demonstrably the bottleneck, and the design
constraint it imposes today is modest: keep physical plan nodes serializable and avoid smuggling
non-serializable state into `ExecutionPlan` implementations. See [roadmap](roadmap.md#phase-4--distributed-execution).

---

## 6. Configuration

Connection definitions live separately from job definitions so credentials are managed once.

```toml
# extract.toml
[sources.orders_pg]
connector = "postgres"
dsn_env   = "ORDERS_PG_DSN"        # never inline credentials
pool_max  = 8
statement_timeout = "5m"
application_name  = "rust-extract-layer"

[sources.orders_pg.pushdown]
policy = "cost_based"
max_source_cost = 50000
deny = ["regexp_match"]            # hard blocklist regardless of cost

[sources.billing_mysql]
connector = "mysql"
dsn_env   = "BILLING_MYSQL_DSN"
pool_max  = 4

[checkpoints]
backend = "postgres"               # or "gcs" / "local"
dsn_env = "REL_META_DSN"

[sinks.warehouse]
type = "parquet"
uri  = "gs://warehouse/raw/"
compression = "zstd(3)"
target_file_size = "256MiB"
```

Every source connects with a distinct, identifiable `application_name` (Postgres) or connection
attribute (MySQL) so a DBA can see exactly what this tool is doing in `pg_stat_activity` /
`performance_schema`. A statement timeout is mandatory: an extraction job must never be the reason
a production database holds a long-running query.

---

## 7. Observability

Structured tracing via `tracing`, with one span per job, per partition scan, and per sink flush.
Metrics exported in Prometheus format:

| Metric | Type | Why it matters |
| --- | --- | --- |
| `rel_rows_extracted_total{source,table}` | counter | Throughput and reconciliation |
| `rel_bytes_from_source_total{source,table}` | counter | Directly measures pushdown effectiveness |
| `rel_source_query_duration_seconds` | histogram | Detects a pushdown that made the DB slow |
| `rel_pushdown_decision_total{operator,decision}` | counter | Is the cost model actually deciding, or always saying yes? |
| `rel_watermark_lag_seconds{source,table}` | gauge | The single most important freshness signal |
| `rel_null_coerced_total{source,table,column,reason}` | counter | Unrepresentable values silently becoming null |
| `rel_checkpoint_commit_total{status}` | counter | Failed commits mean duplicate work next run |

`rel_watermark_lag_seconds` deserves a standing alert. It is the metric that tells you a pipeline
has been quietly extracting nothing for six hours.
