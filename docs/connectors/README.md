# Connector SPI

A connector is not "run SQL, return rows". It is the component that tells the planner **what the
source can do well**, produces Arrow batches without an intermediate row representation, and knows
the specific ways its dialect will lie to you.

Detailed implementations: **[PostgreSQL](postgres.md)** and **[MySQL](mysql.md)**. Those two are the
entire connector scope for the current roadmap.

---

## 1. The traits

```rust
/// A connected, configured data source (one per entry in `[sources.*]`).
#[async_trait]
pub trait Source: Send + Sync {
    fn dialect(&self) -> &dyn SqlDialect;
    fn capabilities(&self) -> &SourceCapabilities;

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableRef>>;
    async fn describe(&self, table: &TableRef) -> Result<SourceTable>;

    /// Cheap, cached statistics used by the cost model.
    async fn statistics(&self, table: &TableRef, cols: &[String]) -> Result<SourceStatistics>;

    /// Plan-time cost estimate for a candidate query, without executing it.
    async fn estimate(&self, plan: &ScanPlan) -> Result<CostEstimate>;

    /// The source's own clock and transaction state, for watermark computation.
    async fn safe_high_watermark(&self, col: &WatermarkSpec) -> Result<WatermarkValue>;
}

/// One table, with a resolved Arrow schema.
#[async_trait]
pub trait SourceTable: Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn column_meta(&self, name: &str) -> Option<&ColumnMeta>;  // collation, unsigned, precision…

    /// Per-filter capability decision surfaced to DataFusion.
    fn supports_filters(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>>;

    /// Split one logical scan into N independently executable partitions.
    async fn partitions(&self, plan: &ScanPlan, target: usize) -> Result<Vec<ScanPartition>>;

    /// Execute one partition, streaming Arrow batches.
    async fn scan(&self, part: ScanPartition) -> Result<SendableRecordBatchStream>;
}
```

```rust
pub struct ScanPlan {
    pub table: TableRef,
    pub projection: Vec<usize>,
    pub pushed_filters: Vec<Expr>,        // planner already decided these go to the source
    pub watermark: Option<WatermarkBounds>,
    pub limit: Option<usize>,
    pub snapshot: Option<SnapshotToken>,  // cross-partition consistency, where supported
}

pub struct SourceCapabilities {
    pub filter_pushdown: bool,
    pub projection_pushdown: bool,
    pub limit_pushdown: bool,
    pub parallel_scan: ParallelScan,      // None | KeysetRange | PhysicalRange | Modulo
    pub consistent_snapshot: SnapshotSupport, // None | SingleConnection | Exportable
    pub incremental: &'static [WatermarkMode],
    pub server_side_cursor: bool,
    pub bulk_export: Option<BulkExport>,  // e.g. Postgres binary COPY
}
```

`SourceCapabilities` is the machine-readable version of the design argument: the planner does not
guess what a source can do, it asks. The two connectors differ meaningfully here, and those
differences drive real behavioral differences:

| Capability | PostgreSQL | MySQL |
| --- | --- | --- |
| Bulk export path | `COPY … TO STDOUT (FORMAT binary)` | none — streaming binary protocol |
| Parallel scan splitting | keyset, `ctid` physical ranges, native partitions | keyset only |
| Cross-connection consistent snapshot | **yes** (`pg_export_snapshot`) | **no** |
| In-flight transaction visibility | `pg_stat_activity.xact_start` | `INNODB_TRX.trx_started` (weaker) |
| Replica-safe watermark bound | `pg_last_xact_replay_timestamp()` | `Seconds_Behind_Source` (heuristic) |
| Column histograms | always maintained (`pg_stats`) | opt-in (`ANALYZE … UPDATE HISTOGRAM`) |
| Log-based CDC (future) | logical replication / `pgoutput` | binlog (row format) |

The "no exportable snapshot" row is not a footnote. It means a parallel MySQL scan of four
partitions reads four *different* points in time, so the connector must either serialize the scan
or declare the result non-atomic. That decision is documented in the
[MySQL connector](mysql.md#5-consistency-and-watermark-anchoring), not hidden in the code.

---

## 2. Decoding to Arrow

Every connector follows the same shape, and the rule is absolute: **no intermediate row struct.**
The wire buffer is decoded straight into Arrow array builders.

```
socket ──► wire frame ──► per-column ArrayBuilder ──► RecordBatch (8192 rows) ──► stream
                          ▲
                          └── typed, pre-sized from the resolved schema
```

Builders are pre-sized from `batch_size` and, for variable-length columns, from an observed average
width that adapts across batches. The measurable goal is one allocation per column per batch.

Batches are emitted as soon as `batch_size` rows are filled — never at the end of the result set —
which is what makes backpressure work end to end (see
[architecture](../architecture.md#5-execution-and-memory)).

---

## 3. Type mapping rules

Type mapping is where connectors quietly corrupt data. Three rules apply to both dialects:

1. **Resolve types from catalog metadata, not from result-set metadata.** The wire protocol tells
   you "this is a blob"; `information_schema` tells you it is a `TEXT` in `utf8mb4` with a
   case-insensitive collation, that a `TINYINT(1)` is being used as a boolean, and that a `BIGINT`
   is `UNSIGNED`. The planner needs the second kind of information (see
   [pushdown §3](../pushdown.md#3-where-pushdown-silently-changes-results)).

2. **Never widen silently to `Utf8`.** Falling back to a string for anything unrecognized produces
   a pipeline that "works" and a warehouse full of strings that nobody can aggregate. An unmapped
   type is an error at plan time, with an explicit per-column `cast_to` escape hatch in the job
   spec for the cases where a string genuinely is the right answer.

3. **Unrepresentable values become null, loudly.** MySQL's `0000-00-00`, Postgres'
   `timestamp 'infinity'`, and a `NUMERIC` exceeding the declared decimal precision have no Arrow
   representation. Each maps to null *and* increments
   `rel_null_coerced_total{column, reason}`. A configurable `on_unrepresentable = "error" | "null"`
   lets strict pipelines fail instead.

Per-dialect mapping tables live in each connector document.

---

## 4. Parallel scan

Splitting a scan across connections multiplies throughput and multiplies the load you place on a
production database. It is off by default and opt-in per job.

| Strategy | Mechanism | Notes |
| --- | --- | --- |
| `keyset` | `pk >= :a AND pk < :b`, bounds from min/max or histogram percentiles | Works everywhere; skewed if the key is not uniform. Histogram-derived bounds fix most skew. |
| `physical` | Postgres `ctid` ranges over `relpages` | Fastest and evenly sized; only valid within one snapshot. Postgres only. |
| `modulo` | `hash(pk) % n = i` | Always balanced, but forces a full scan per partition — usually a mistake. |
| `native` | one partition per declarative table partition | Best when it applies; aligns with the source's own pruning. |

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
  autovacuum and pins MySQL's undo log.
- **Connections are identifiable.** `application_name` / connection attributes carry the job id, so
  a DBA looking at `pg_stat_activity` or `performance_schema` can attribute every query.
- **Session settings are explicit.** UTC time zone, UTF-8 client encoding, read-only where supported.
  Never inherit a server default that could change underneath the pipeline.
- **Retries are bounded and classified.** Transient errors (connection reset, deadlock, replica
  recovery conflict) retry with jittered backoff; semantic errors (undefined column, permission
  denied) fail immediately. Retrying a permission error for ten minutes helps no one.

---

## 6. Adding a connector later

Beyond Postgres and MySQL, the SPI is deliberately shaped to accommodate sources that are *not*
SQL databases, because that is where cross-source joins get interesting. A key-value or wide-column
store declares `filter_pushdown: partition-key-only`, `ParallelScan::Modulo` over token ranges, and
`SnapshotSupport::None`; an object store declares full projection pushdown, statistics-based
`Inexact` filtering, and effortless parallelism. Neither has been designed in detail, and neither
should be started before the two SQL connectors are complete — the SPI is only proven once two
genuinely different implementations sit behind it.
