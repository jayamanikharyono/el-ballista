# MySQL Connector

Module: `el_ballista::connector::mysql` (in this crate — there is no separate
connector crate). Built on `sqlx` (`MySqlPool`, `?` placeholders, backtick quoting).

> **Status: experimental prototype (walking skeleton).** §0 below is the complete list of what
> the code does today. Everything else in this document is **design reference for promoting the
> prototype**; sections and passages that describe unbuilt behaviour are marked
> ***(design, not implemented)***. Ranges are caller-provided; watermark anchoring is deferred —
> see [deferred/incremental-extraction.md](../deferred/incremental-extraction.md).

Covers MySQL 8.0+ (the version the test suite runs against). MariaDB, Cloud SQL and MySQL 5.7 are
untested.

MySQL is the harder of the two connectors, and it is worth being explicit about why: it has no bulk
export path, no exportable snapshot, weaker statistics, and a type system with several values that
have no Arrow representation. None of this is fatal, but every one of them needs a deliberate
decision rather than a default.

---

## 0. What ships today

Everything here is implemented in `src/connector/mysql/` and exercised by `tests/mysql.rs`,
`tests/extraction_matrix.rs` and `tests/dvdrental_cross_engine.rs` against the compose MySQL 8.0.

- **Connect** — `MysqlExtractor::connect(host, port, user, password, database, pool_max)`.
  Credentials are passed to `MySqlConnectOptions` field by field (no URL is formatted, so
  passwords containing `/ # ? @ :` work). Each session is pinned to `time_zone = '+00:00'`. No
  other session settings are applied (no `net_write_timeout`, `max_execution_time`,
  `transaction_read_only`, `sql_mode` or connection attributes — see §7).
- **Schema** — `MysqlSchemaReader::get_table_metadata("table" | "schema.table")` reads
  `COLUMN_NAME`, `DATA_TYPE`, `IS_NULLABLE`, `COLUMN_TYPE` and `COLLATION_NAME` from
  `information_schema.COLUMNS` in ordinal order. A table with no visible columns is
  `MysqlError::TableNotFound` (never an empty `SELECT  FROM …`). `COLLATION_NAME` is read but not
  used (marked `TODO(mysql-pushdown)` in the code).
- **Extraction** — full table only, optionally projected; an unknown projection column is
  `MysqlError::UnknownColumns`. One plain `` SELECT `c1`, … FROM `db`.`t` `` (no casts, no
  `ORDER BY` — row order is unspecified) over a sqlx prepared statement (binary protocol).
  - `extract_full_table_for_each_batch(table, columns, batch_size, on_batch)` streams rows with
    sqlx `fetch` and calls `on_batch` with one `RecordBatch` per `batch_size` rows (the tail
    batch may be smaller): at most one batch of rows is resident. Returns the Arrow schema (also
    for an empty table, where `on_batch` is never called). `batch_size == 0` is
    `MysqlError::InvalidBatchSize`. The first error (source, decode, or from `on_batch`) stops
    the stream and is returned.
  - `extract_full_table(table, columns)` is built on the above and **materializes** the whole
    table into one `RecordBatch` — for small tables and tests.
- **Errors** — typed `MysqlError` (`Source`, `Arrow`, `Extractor`, `TableNotFound`,
  `UnknownColumns`, `InvalidBatchSize`, `Decode { column }`, `OutOfRange { column, target }`,
  `NotBoolean { column }`), each keeping its cause as `#[source]`; messages name tables and
  columns, never row values or credentials. A decode failure fails the extraction — it is never
  turned into NULL or zero rows.
- **Consistency** — a single statement on one connection: whatever that statement sees under the
  server's isolation level (InnoDB: a consistent read for the statement). No multi-statement
  snapshot, no resumability, no checkpointing.
- **Not implemented** — filters/pushdown (`MysqlDialect` exists and is unit-tested, but nothing
  calls it for predicates), a DataFusion `TableProvider`, parallel/keyset partitioning,
  distributed execution, checkpointed jobs (`run_with`) and the `run()` diagnostic,
  statistics/cost estimation, retries.
  Filtering happens in DataFusion after extraction (`tests/mysql.rs`).

---

## 1. Extraction path

There is **no `COPY` equivalent**. `SELECT … INTO OUTFILE` writes a file on the *server*, which is
useless to a remote extractor (and usually blocked by `secure_file_priv`). So the only path is the
normal result-set protocol, and the job is to use it well.

What the prototype does (see §0):

```rust
let mut stream = sqlx::query(sql).fetch(&pool);       // prepared statement → binary protocol
while let Some(row) = stream.try_next().await? {       // rows arrive as the server sends them
    buf.push(row);                                     // ≤ batch_size rows buffered
    if buf.len() == batch_size { on_batch(decode(&buf)?)?; buf.clear(); }
}
```

Two choices matter here:

**Binary protocol over text protocol.** A plain text-protocol query returns every value as a
string: integers, decimals, and timestamps all arrive formatted and must be parsed. Prepared
statements use the binary protocol, where an `INT` is 4 bytes and a `DATETIME` is a packed
structure. For extraction workloads this is a large decode-CPU difference, and it removes a class
of locale- and format-dependent parsing bugs. (Implemented: sqlx prepares every `query()`.)

**Streaming over buffering.** `fetch` yields rows as they arrive rather than materializing the
result set, so memory is bounded by `batch_size`. (Implemented for
`extract_full_table_for_each_batch`; `extract_full_table` deliberately materializes.)

### The `net_write_timeout` trap *(design, not implemented)*

While the client is streaming a large result, the server is blocked writing to the socket. If the
client consumes slowly — because a downstream consumer applied backpressure, which is *by design* —
the server can hit `net_write_timeout` (default 60s) and **kill the connection mid-result**. The
symptom is a "lost connection during query" partway through a long extraction, which looks like a
network problem and is not.

The design raises it per session:

```sql
SET SESSION net_write_timeout = 600;
```

and treats the resulting error class as retryable at partition granularity. **Today** the
prototype does not set it and has no retry: a slow `on_batch` callback can hit the server
default and the extraction fails with `MysqlError::Source`.

---

## 2. Type mapping

MySQL's wire protocol under-describes its own types, so the connector resolves every column from
`information_schema.COLUMNS` — `DATA_TYPE` plus `COLUMN_TYPE`, which carries `unsigned`, the
display width that marks `tinyint(1)` as BOOLEAN, `bit(n)` and `decimal(p,s)`. (`NUMERIC_PRECISION`,
`NUMERIC_SCALE`, `DATETIME_PRECISION` and `CHARACTER_SET_NAME` are not read; precision/scale are
parsed from `COLUMN_TYPE`.)

The table below is **what ships** (`type_mapper::arrow_type_for`, decoded by `row_adapter`);
*(design)* marks the only rows that describe unbuilt behaviour.

| MySQL | Arrow | Notes |
| --- | --- | --- |
| `TINYINT` / `SMALLINT` / `MEDIUMINT` / `INT` / `BIGINT` | `Int8` / `Int16` / `Int32` / `Int32` / `Int64` | Decoded as `i64`, narrowed with `TryFrom` (no wrapping) |
| `TINYINT` / `SMALLINT` / `MEDIUMINT` / `INT` `UNSIGNED` | `Int16` / `Int32` / `Int32` / `Int64` | Next wider signed type, so every value fits. Decoded as `u64` and converted with `TryFrom`; a value that does not fit is `MysqlError::OutOfRange`, never wrapped |
| `BIGINT UNSIGNED` | `UInt64` | Does not fit `i64` |
| `TINYINT(1)` (= `BOOL` / `BOOLEAN`) | `Boolean` | Detected by `COLUMN_TYPE = 'tinyint(1)'` exactly (`tinyint(1) unsigned` stays an integer). Only 0/1 are accepted; any other stored value is `MysqlError::NotBoolean` rather than a lossy "non-zero is true" |
| `DECIMAL(p,s)`, p ≤ 38 | `Decimal128(p, s)` | Exact |
| `DECIMAL(p,s)`, 38 < p ≤ 65 | — | **Typed error** (`UnsupportedType`) at schema build. `Decimal256(p, s)` is *(design, not implemented)* |
| `FLOAT` / `DOUBLE` | `Float32` / `Float64` | |
| `BIT(1)` | `Boolean` | |
| `BIT(n)`, 1 < n ≤ 64 | `UInt64` | Big-endian on the wire |
| `DATE` | `Date32` | `0000-00-00` → typed error `ZeroDate`; partial zero dates (`2026-00-15`) → typed `Decode` error — see §3 |
| `DATETIME(p)` | `Timestamp(µs, None)` | No time zone attached; zero datetime → typed error `ZeroDate` |
| `TIMESTAMP(p)` | `Timestamp(µs, None)` | Rendered by the server in the session `time_zone`, which is pinned to `+00:00`; no zone is recorded on the Arrow type |
| `TIME(p)` | `Duration(µs)` | Range is **−838:59:59 to 838:59:59** — an interval, not a time of day, so `Time64` would be wrong. Sign preserved |
| `YEAR` | `Int16` | Decoded as `u16` |
| `CHAR` / `VARCHAR` / `TEXT` family | `Utf8` | Collation is read but not used yet |
| `BINARY` / `VARBINARY` / `BLOB` family | `Binary` | Chosen from `DATA_TYPE`, not the protocol type |
| `ENUM` | `Utf8` | `Dictionary(Int32, Utf8)` is *(design, not implemented)* |
| `SET` | `Utf8` | Comma-joined as stored |
| `JSON` | `Utf8` | Parsed and re-serialized by `serde_json` (key order / whitespace normalized) |
| spatial and any other type | `Utf8` | Decoded as a string; a type sqlx cannot decode as a string fails with `MysqlError::Decode` |

---

## 3. Values MySQL permits that Arrow cannot represent

This is MySQL's distinguishing hazard. **Today** every such value fails the extraction with a
typed error. The `on_unrepresentable` policy and the `el_ballista_null_coerced_total` counter described
below are *(design, not implemented)*.

**Zero dates.** Unless `sql_mode` includes `NO_ZERO_DATE` and `NO_ZERO_IN_DATE`, MySQL accepts
`0000-00-00` and `2026-00-15`. Neither is a real date and neither has an Arrow encoding.
**Today** both fail the extraction: sqlx decodes a full zero date as NULL, so the projection
carries one `(col = 0)` marker per `DATE`/`DATETIME`/`TIMESTAMP` column and a zero date raises
`MysqlError::ZeroDate { column }`; a partial zero date fails decoding (`MysqlError::Decode`).
A real NULL stays NULL. *(Design, not implemented:)* an opt-in policy that maps them to null,
incrementing `el_ballista_null_coerced_total{reason="zero_date"}`. Legacy
schemas frequently use `0000-00-00` as a "no value" sentinel, so this counter is often non-zero
on the first run against an old database — which is exactly the moment to learn about
it, rather than discovering it in a warehouse query six weeks later.

**Out-of-range `TIME`.** Values beyond ±24h are legal and are why the mapping is `Duration`
(implemented).

**Truncated data in non-strict mode.** With a permissive `sql_mode`, MySQL silently truncates
oversized values on write. Nothing the extractor can do about data already stored, but the
planned `el-ballista doctor` command would report the source's `sql_mode` so the behavior is at least
visible.

**Case-insensitive collation.** MySQL 8.0's default `utf8mb4_0900_ai_ci` is accent- and
case-insensitive, so `WHERE status = 'PAID'` matches `'paid'` in MySQL and not in Arrow. In the
design, every string predicate on a `_ci` or `_ai` column is pushed as **`Inexact`**, never
`Exact`. *(Pushdown is not implemented for MySQL; `MysqlDialect` currently rates every text or
float comparison `Inexact` without looking at the collation.)*
This is covered in full in [pushdown §3.1](../pushdown.md#31-string-collation) and it
is the single most likely way to get silently wrong results from this connector.

---

## 4. Time zones

`DATETIME` stores what was written. `TIMESTAMP` is stored as UTC and converted to and from the
session `time_zone` on every read and write. A pipeline that does not pin the session time zone
produces different data when the server's default changes, when it reads from a replica configured
differently, or when DST shifts.

```sql
SET SESSION time_zone = '+00:00';
```

Applied on every connection (implemented: `MySqlConnectOptions::timezone("+00:00")`). `DATETIME` columns still carry no zone information — the Arrow type is
deliberately `Timestamp(µs, None)` rather than a lie about UTC, and interpreting them is a
modeling decision for the warehouse, not something the extractor should guess.

---

## 5. Consistency notes

**Today:** one `SELECT` per extraction on one connection — no explicit transaction, no snapshot
spanning statements, no parallelism (§0). The `START TRANSACTION WITH CONSISTENT SNAPSHOT` usage
and the `parallel_consistency` modes in §§5.1–5.4 are *(design, not implemented)*; §5.1 explains
the MySQL constraint they are designed around.

### 5.1 No exportable snapshot

A single connection gets a consistent read view:

```sql
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION WITH CONSISTENT SNAPSHOT;
```

InnoDB establishes the read view immediately, and every statement in that transaction sees the same
point in time. But **there is no `pg_export_snapshot()` equivalent** — a second connection cannot
join that snapshot. This is the sharpest asymmetry between the two connectors.

The consequence for parallel scans is unavoidable, so the connector makes it a declared choice
rather than a hidden behavior:

| `parallel_consistency` | Behavior |
| --- | --- |
| `strict` (default for `snapshot` mode) | Parallelism disabled; one connection, one snapshot, atomic result |
| `per_partition` | Parallel scan allowed; each partition is internally consistent, the union is not. Requires explicit opt-in in the job spec |

For caller-provided time ranges, `per_partition` bounds what each partition sees; for a
full `snapshot` extraction intended to be a point-in-time copy, per-partition consistency
is not enough, and the default reflects that. *(Design: neither mode is implemented —
the prototype scans serially.)*

### 5.2 The safe high watermark *(design, not implemented)*

The closest available analogue to the Postgres query:

```sql
SELECT LEAST(
         UTC_TIMESTAMP(6) - INTERVAL 1 SECOND,
         COALESCE(MIN(trx_started), UTC_TIMESTAMP(6))
       )
FROM information_schema.INNODB_TRX;
```

This is weaker than the Postgres version: `INNODB_TRX` lists transactions that have acquired a
transaction id, so a read-mostly transaction that has not yet written may be absent, and non-InnoDB
engines are invisible entirely. It is a genuine improvement over a fixed lag but not a guarantee,
so the design keeps a mandatory fixed `safety_lag` for MySQL sources with the `INNODB_TRX`
bound applied on top of it rather than instead of it.

### 5.3 GTID anchoring *(design, not implemented)*

When GTIDs are enabled, every run would record the position it read at:

```sql
SELECT @@GLOBAL.gtid_executed;
```

Stored alongside the range in the checkpoint, it gives an exact, orderable position for
auditing, for reconciling a suspected gap, and as the handover point when a table is later
migrated to binlog-based capture.

### 5.4 Reading from a replica *(design, not implemented)*

The same hazard as a Postgres standby, with worse instrumentation. The replica's clock is current
but its data is behind, so an orchestrator-supplied `hi = now()` skips rows that have not
replicated yet — and they are never re-read.

MySQL offers no exact equivalent of `pg_last_xact_replay_timestamp()`. The available signal is
`Seconds_Behind_Source` from `SHOW REPLICA STATUS`, which is measured in whole seconds, is derived
from event timestamps rather than commit times, and reports `NULL` when replication is broken.

**Rule (design):** for a MySQL source with `role = "replica"`, subtract
`max(Seconds_Behind_Source, replica_lag_floor)` from the range bound, treat `NULL` as
a hard failure rather than zero, and require `safety_lag ≥ 30s`. Reading a MySQL replica with the
default settings is one of the easiest ways to lose rows quietly. None of `role`,
`replica_lag_floor`, or `safety_lag` exist as job-spec fields today.

---

## 6. Statistics and cost estimation *(design, not implemented)*

Nothing in this section exists for MySQL yet. Weaker than Postgres, and the cost model has to account for that.

```sql
-- Row count: approximate for InnoDB, routinely off by tens of percent
SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH
FROM   information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;

-- Index cardinality: also approximate, sampled
SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, CARDINALITY
FROM   information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;

-- Column histograms: MySQL 8.0+, and OPT-IN
SELECT HISTOGRAM FROM information_schema.COLUMN_STATISTICS
WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?;

-- Access method for a candidate predicate, without executing it
EXPLAIN FORMAT=JSON SELECT …;
```

The critical difference from Postgres: **histograms do not exist unless someone created them.**

```sql
ANALYZE TABLE orders UPDATE HISTOGRAM ON status, updated_at WITH 64 BUCKETS;
```

Without histograms, selectivity estimates for non-indexed predicates are guesses. The design's
policy is to be conservative in that case — an unknown selectivity is treated as *low* (few rows
removed), which biases toward keeping the filter in Arrow rather than pushing a predicate that
might trigger a full scan on a production database. The planned `el-ballista doctor` command would report
which columns referenced by a job lack histograms and print the `ANALYZE` statement that would
fix it.

Never use `EXPLAIN ANALYZE` (8.0.18+) for estimation — it executes the query.

---

## 7. Session configuration *(design, not implemented — except `time_zone`)*

Today only `time_zone = '+00:00'` is set (plus sqlx's own connect defaults). The rest is the
design target:

```sql
SET SESSION time_zone = '+00:00';
SET SESSION net_write_timeout = 600;
SET SESSION max_execution_time = 300000;      -- milliseconds; SELECT statements only
SET SESSION transaction_read_only = ON;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE';
SET SESSION group_concat_max_len = 1048576;
```

Setting `sql_mode` on the session affects how the *server evaluates our queries*; it does not
change data already stored, and it does not cause existing zero dates to error on read. It is set
so that any expression we push down evaluates under predictable rules rather than the server's
inherited default.

Connection attributes carry the job identity for DBA attribution (visible in
`performance_schema.session_connect_attrs`), which is MySQL's equivalent of Postgres'
`application_name`.

### Required privileges

```sql
CREATE USER 'el_ballista'@'%' IDENTIFIED BY '…';
GRANT SELECT                ON app.*  TO 'el_ballista'@'%';
-- Only needed for the deferred §§5.2/5.4 features, not for prototype extraction:
-- GRANT PROCESS               ON *.*    TO 'el_ballista'@'%';  -- INNODB_TRX for §5.2
-- GRANT REPLICATION CLIENT    ON *.*    TO 'el_ballista'@'%';  -- SHOW REPLICA STATUS for §5.4
```

`PROCESS` and `REPLICATION CLIENT` are broad grants. Where a DBA declines them, the design
falls back to `role = "replica"` plus a manually specified `replica_lag_floor` and a generous
`safety_lag` — the fallback is fine, but it has to be a decision someone made on purpose.

---

## 8. Partitioning *(design, not implemented)*

The prototype has no partitioned or parallel scan. In the design, only `keyset` is available. There is no `ctid` analogue, so there is no cheap physical split:

```sql
WHERE order_id >= ? AND order_id < ?
```

Boundaries come from histogram percentiles where histograms exist, and otherwise from
`MIN(pk)`/`MAX(pk)` with uniform division — which is fast (index-only) but produces badly skewed
partitions on non-uniform keys, a common outcome with UUID or snowflake-style primary keys. When a
job requests parallelism on a table with no usable histogram and a non-integer primary key, the
connector reduces the partition count to 1 and logs the reason rather than issuing four wildly
unbalanced queries.

Combine with §5.1: parallel scanning also costs snapshot consistency on MySQL, so the default is
serial and parallelism is something to turn on knowingly.

---

## 9. Future: binlog CDC *(design, not implemented)*

The same shape as the Postgres logical replication path, and the same payoff — hard deletes become
visible and commit-order skew disappears, since binlog events are ordered by commit.

Requirements: `binlog_format = ROW`, `binlog_row_image = FULL` (otherwise updates carry only changed
columns, and reconstructing full rows requires warehouse-side state), and `REPLICATION SLAVE`
privilege. The checkpoint becomes a GTID set, which §5.3 describes, so the migration path
from timestamp-based to log-based extraction is a checkpoint conversion rather than a re-backfill.

The operational hazard mirrors Postgres' replication slot problem in a different form: binlog
retention (`binlog_expire_logs_seconds`) is finite, so a consumer that falls behind past retention
cannot resume and must re-snapshot. Lag monitoring against retention is a prerequisite, not a
follow-up.
