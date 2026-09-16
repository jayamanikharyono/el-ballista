# Connector SPI

A connector is not "run SQL, return rows". It is the component that tells the planner **what the
source can do well**, produces Arrow batches, and knows the specific ways its dialect will lie to you.

Detailed implementation: **[PostgreSQL](postgres.md)**.

---

## 1. The actual SPI (what exists)

The implementation does not have a separate `Source` trait hierarchy. Instead, each connector
implements DataFusion's `TableProvider` and `ExecutionPlan` directly, plus these supporting
abstractions in `src/connector/`:

```rust
/// Capability declaration surfaced to the planner
pub struct SourceCapabilities {
    pub filter_pushdown: bool,
    pub projection_pushdown: bool,
    pub limit_pushdown: bool,
    pub parallel_scan: ParallelScan,      // None | KeysetRange | CtidRange
    pub incremental: IncrementalConfig,   // timestamp mode only
}
```

```rust
/// Per-connector dialect and rendering
pub trait SqlDialect {
    fn try_render(&self, expr: &Expr, params: &mut ParamBuffer) -> Option<String>;
    fn fidelity(&self, expr: &Expr, col_meta: &ColumnMeta) -> Fidelity; // Exact | Inexact
    fn quote_ident(&self, ident: &str) -> String;
    fn placeholder(&self, index: usize) -> String;   // "$1" for pg
}
```

```rust
/// Checkpoint store abstraction
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn acquire(&self, key: &JobKey, run_id: Uuid, lease: Duration, watermark_column: &str)
        -> Result<Checkpoint, AppError>;
    async fn commit(&self, key: &JobKey, run_id: Uuid, next: DateTime<Utc>, stats: RunStats)
        -> Result<(), AppError>;
    async fn read(&self, key: &JobKey) -> Result<Option<Checkpoint>, AppError>;
}
```

The PostgreSQL connector implements `TableProvider` (for `scan`/`supports_filters`) and
`ExecutionPlan` (streaming `RecordBatch` via `sqlx::query().fetch()`). There is no separate
`Source` / `SourceTable` trait hierarchy — the SPI is DataFusion's native traits plus the
abstractions above.

---

## 2. Capability comparison

| Capability | PostgreSQL |
| --- | --- |
| Parallel scan splitting | keyset, `ctid` physical ranges |
| In-flight transaction visibility | `pg_stat_activity.xact_start` |
| Column histograms | `pg_stats` (always maintained) |
| Log-based CDC (future) | logical replication / `pgoutput` (planned) |

---

## 2. Decoding to Arrow

The PostgreSQL connector uses `sqlx` text protocol. Rows arrive as `sqlx::Row` values (an
intermediate representation) which are then decoded into Arrow array builders:

```
text wire ──► sqlx::Row ──► per-column ArrayBuilder ──► RecordBatch (batch_size rows) ──► stream
```

Builders are pre-sized from `batch_size` (default 8192) and, for variable-length columns,
from an observed average width that adapts across batches. Batches are emitted as soon as
`batch_size` rows are filled — never at the end of the result set — which is what makes
backpressure work end to end (see [architecture](../architecture.md#5-execution-and-memory)).

---

## 3. Type mapping rules

Three rules apply to both dialects:

1. **Resolve types from catalog metadata, not from result-set metadata.** The wire protocol tells
   you "this is text"; `information_schema` tells you it is a `TEXT` in `utf8` with a
   case-insensitive collation. The planner needs the second kind of information (see
   [pushdown §3](../pushdown.md#3-where-pushdown-silently-changes-results)).

2. **Never widen silently to `Utf8`.** Falling back to a string for anything unrecognized produces
   a pipeline that "works" and a warehouse full of strings that nobody can aggregate. An unmapped
   type is an error at plan time, with an explicit per-column `cast_to` escape hatch in the job
   spec for the cases where a string genuinely is the right answer. **NOT YET IMPLEMENTED** for the
   `cast_to` hatch.

3. **Unrepresentable values become null, loudly.** Postgres `timestamp 'infinity'`, and a `NUMERIC`
   exceeding the declared decimal precision have no Arrow representation. Each maps to null *and*
   increments `rel_null_coerced_total{column, reason}`. A configurable
   `on_unrepresentable = "error" | "null"` lets strict pipelines fail instead. **NOT YET
   IMPLEMENTED** for the configurable hatch.

Per-dialect mapping tables live in each connector document.

---

## 4. Parallel scan

Splitting a scan across connections multiplies throughput and multiplies the load you place on a
production database. It is off by default and opt-in per job.

| Strategy | Mechanism | Notes |
| --- | --- | --- |
| `keyset` | `pk >= :a AND pk < :b`, bounds from min/max or histogram percentiles | Works everywhere; skewed if the key is not uniform. Histogram-derived bounds fix most skew. |
| `ctid` | Postgres `ctid` ranges over `relpages` | Fastest and evenly sized; only valid within one snapshot. Postgres only. **Exported snapshots not implemented.** |
| `modulo` | `hash(pk) % n = i` | **NOT IMPLEMENTED** |
| `native` | one partition per declarative table partition | **NOT IMPLEMENTED** |

The connection pool caps concurrency regardless of the requested partition count, and the pool is
sized deliberately small for production sources. The failure mode we are avoiding is an extraction
job that consumes every available connection slot on a primary during business hours.

---

## 5. Connection hygiene

Non-negotiable for every connector, because an extraction tool must never be the cause of a
production incident:

- **`statement_timeout` is always set.** A query that has run for twenty minutes is a bug, not a
  slow query.
- **Idle-in-transaction timeout is always set.** A forgotten open transaction blocks Postgres
  autovacuum and pins undo log on other engines.
- **Connections are identifiable.** `application_name` / connection attributes carry the job id, so
  a DBA looking at `pg_stat_activity` or `performance_schema` can attribute every query.
- **Session settings are explicit.** UTC time zone, UTF-8 client encoding, read-only where supported.
  Never inherit a server default that could change underneath the pipeline.
- **Retries are bounded and classified.** Transient errors (connection reset, deadlock, replica
  recovery conflict) retry with jittered backoff; semantic errors (undefined column, permission
  denied) fail immediately. Retrying a permission error for ten minutes helps no one.

---

## 6. Adding a connector later

Beyond Postgres, the SPI is deliberately shaped to accommodate sources that are *not*
SQL databases, because that is where cross-source joins get interesting. A key-value or wide-column
store declares `filter_pushdown: partition-key-only`, `ParallelScan::CtidRange` over token ranges,
and `SnapshotSupport::None`; an object store declares full projection pushdown, statistics-based
`Inexact` filtering, and effortless parallelism. Neither has been designed in detail, and neither
should be started before the PostgreSQL connector is complete — the SPI is only proven once two
genuinely different implementations sit behind it.