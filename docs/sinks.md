# Sinks

Where extracted Arrow batches land. The default and only Phase 1 sink is Parquet on an object
store; BigQuery is loaded from those files on a separate cadence rather than written to directly.

---

## 1. Why Parquet-then-load beats streaming into the warehouse

```
   Postgres / MySQL                        Postgres / MySQL
         │                                       │
         │  streaming insert                     │  incremental extract
         ▼                                       ▼
     BigQuery                                GCS Parquet
   (per-row cost,                                │  hourly
    quota pressure,                              ▼
    no cheap replay)                    BigQuery load / MERGE
                                        (free loads, replayable,
                                         Parquet is its own archive)
```

Batching into columnar files gives three things a streaming path does not: BigQuery load jobs from
GCS are not billed per row, the Parquet files remain a queryable, engine-independent archive of
exactly what was extracted, and a bad transformation can be replayed from the files without
touching the source database again. The cost is freshness — the pipeline is as fresh as its load
cadence, which for the workloads this project targets is an easy trade.

---

## 2. Layout

```
gs://warehouse/raw/orders/
  _extracted_date=2026-09-04/
    w=1757030400-1757034000/
      part-00000.parquet
      part-00001.parquet
      _SUCCESS                 ← written last; names the expected object count
    w=1757034000-1757037600/
      …
```

Two levels, each doing a specific job. `_extracted_date` is a Hive-style partition key that
BigQuery external tables and every other reader understand, and that keeps the object count per
prefix manageable. The `w=<lo>-<hi>` directory is derived deterministically from the watermark
window, which is what makes a retried run overwrite its predecessor's output byte-for-byte instead
of duplicating it — see
[the commit protocol](incremental-extraction.md#5-the-commit-protocol).

Readers ignore any window directory without a `_SUCCESS` marker, so a crashed run leaves debris
that is invisible rather than half-loaded.

Note that `_extracted_date` is the *extraction* date, not a business date. Partitioning by a
business column would mean a single incremental run writing into hundreds of old partitions, which
turns every run into a wide scattered write. Business-level partitioning belongs in the warehouse
table, applied during the load.

---

## 3. Parquet writing

`ArrowWriter` from the `parquet` crate, fed the same `RecordBatch` stream that comes out of
DataFusion — no re-encoding step in between.

| Setting | Default | Reasoning |
| --- | --- | --- |
| Compression | `zstd(3)` | Better ratio than snappy at comparable speed; both are BigQuery-readable |
| Row group size | 128 MiB | Large enough for effective statistics pruning, small enough that a reader need not buffer the world |
| Data page size | 1 MiB | |
| Target file size | 256 MiB | Fewer, larger files: object stores and BigQuery loads both dislike many small files |
| Dictionary encoding | on | Very large win on the low-cardinality string columns typical of operational tables (`status`, `country`, `type`) |
| Statistics | page-level | Enables predicate pushdown for downstream readers |
| Writer version | 2.0 | |

A writer rolls to a new file when the target size is reached, so a single partition stream produces
`part-00000`, `part-00001`, and so on. Rolling is by *written bytes*, not row count, because row
width varies by orders of magnitude between tables.

**Timestamp precision.** Parquet stores timestamps as `TIMESTAMP(isAdjustedToUTC, unit)`. Arrow
microsecond timestamps map cleanly; BigQuery's `TIMESTAMP` is also microsecond, so keeping
everything at microseconds throughout avoids a lossy conversion at the boundary. Nanosecond
timestamps are truncated with a warning rather than silently rounded.

**Small-file avoidance.** A frequently scheduled job on a low-traffic table produces a 40 KB file
every five minutes, which is a real operational problem at scale. `min_file_size` lets a run buffer
into the *next* run's output instead of flushing, and a `rel compact` job merges historical window
directories into daily files once they are past the point of being rewritten.

---

## 4. Object store

Via the `object_store` crate, which gives GCS, S3, Azure, and local filesystem behind one interface
— useful mostly because local-filesystem sinks make integration tests fast and hermetic.

Uploads use multipart for anything above the part threshold, with bounded concurrency so a wide
parallel extraction does not saturate the egress link. Retries are on the object store's own
classification of transient errors, and — critically — an object is only visible once complete,
which is the property the `_SUCCESS` protocol depends on.

Authentication follows Application Default Credentials on GCP: Workload Identity in GKE, the
attached service account on GCE/Cloud Run, `GOOGLE_APPLICATION_CREDENTIALS` elsewhere. The sink
needs `roles/storage.objectAdmin` scoped to the target prefix, and nothing else.

---

## 5. BigQuery

Loading is a separate step from extraction, on its own cadence, driven by `rel load` or by the
orchestrator.

### 5.1 Append-only tables

```
GCS window directories  ──►  load job (PARQUET, WRITE_APPEND)  ──►  raw.orders
```

Schema comes from the Parquet files. Column additions are permitted via `ALLOW_FIELD_ADDITION` to
match the [schema drift policy](incremental-extraction.md#35-schema-drift). Load jobs from GCS are
not charged for compute, which is the main reason this design exists.

### 5.2 Upsert tables

The common case: the source table is mutable, extraction produces multiple versions of the same
primary key over time, and the warehouse should hold the latest.

```sql
MERGE `proj.raw.orders` AS target
USING (
  SELECT * EXCEPT(_rn) FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (
             PARTITION BY order_id
             ORDER BY updated_at DESC, _extracted_at DESC
           ) AS _rn
    FROM `proj.staging.orders_incoming` AS s
  )
  WHERE _rn = 1
) AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.updated_at >= target.updated_at THEN UPDATE SET …
WHEN NOT MATCHED THEN INSERT …
```

The `ROW_NUMBER` deduplication is not optional. Because extraction is at-least-once and the
`overlap` setting deliberately re-reads window boundaries, the staging table *will* contain
duplicate primary keys, and `MERGE` raises an error if the source side has duplicates on the join
key. Deduplicating by `(updated_at DESC, _extracted_at DESC)` also makes the merge idempotent under
replay and correct when a row changed twice inside one window.

The `source.updated_at >= target.updated_at` guard makes out-of-order arrival safe: a replayed old
window cannot overwrite newer data.

Cluster the target on the merge key and partition it on the business date column; an unclustered
`MERGE` against a large table scans the whole thing every run and is usually the most expensive
line item in this entire pipeline.

### 5.3 External tables as a shortcut

For exploration or for tables that do not need warehouse-native performance, a BigLake or external
table over the GCS prefix skips loading entirely:

```sql
CREATE EXTERNAL TABLE `proj.raw.orders_ext`
WITH PARTITION COLUMNS (_extracted_date DATE)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://warehouse/raw/orders/*'],
  hive_partition_uri_prefix = 'gs://warehouse/raw/orders'
);
```

Query cost is higher per scan and there is no clustering, so this is a convenience for the raw
layer rather than a serving path.

### 5.4 Deletes

The warehouse can only reflect deletes that extraction observed. With `deleted_at` soft deletes,
they arrive as ordinary updates and the `MERGE` handles them. Without, they arrive only from a
`rel reconcile` run or from future log-based capture, as covered in
[incremental extraction §3.4](incremental-extraction.md#34-hard-deletes-are-invisible).

---

## 6. Sink interface

```rust
#[async_trait]
pub trait Sink: Send + Sync {
    /// Consume a stream of batches for one window; returns only when durable.
    async fn write(&self, window: &WindowId, stream: SendableRecordBatchStream)
        -> Result<SinkOutput>;

    /// Mark the window complete (the _SUCCESS marker). Called after write() succeeds.
    async fn finalize(&self, window: &WindowId, output: &SinkOutput) -> Result<()>;

    /// Remove partial output from a failed run, best-effort.
    async fn abort(&self, window: &WindowId) -> Result<()>;
}
```

`write` returning means the bytes are durable, not that they are buffered. The
[checkpoint commit](incremental-extraction.md#5-the-commit-protocol) depends on that distinction
entirely: a sink that returns early converts a crash into silent data loss, since the watermark
advances past data that was never written.

Planned implementations: `ParquetSink` (object store or local, Phase 1), `BigQuerySink` (load-job
orchestration, Phase 3), and `IpcSink` (Arrow IPC, for tests and for chaining jobs without a
serialization round trip).
