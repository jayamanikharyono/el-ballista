# MySQL Connector

Crate: `rel-connector-mysql`. Built on `sqlx` (`MySqlPool`, `?` placeholders, backtick
quoting).

> **Status: experimental prototype (walking skeleton).** What actually exists today is
> connect, schema reading from `information_schema`, and full-table extraction to typed
> Arrow — no pushdown, no parallel/distributed execution, no watermark machinery. Much
> of this document (§§3, 5.2–5.4, parts of §§2, 7, 8) is **design reference for promoting
> the prototype**, not a description of shipped behavior; such passages are marked
> *(design)*. Ranges are caller-provided; watermark anchoring is deferred — see
> [deferred/incremental-extraction.md](../deferred/incremental-extraction.md).

Covers MySQL 8.0+, Cloud SQL for MySQL, and (with caveats noted inline) MariaDB. MySQL 5.7 is
supported but loses histogram-based cost estimation.

MySQL is the harder of the two connectors, and it is worth being explicit about why: it has no bulk
export path, no exportable snapshot, weaker statistics, and a type system with several values that
have no Arrow representation. None of this is fatal, but every one of them needs a deliberate
decision rather than a default.

---

## 1. Extraction path

There is **no `COPY` equivalent**. `SELECT … INTO OUTFILE` writes a file on the *server*, which is
useless to a remote extractor (and usually blocked by `secure_file_priv`). So the only path is the
normal result-set protocol, and the job is to use it well.

```rust
let stmt = conn.prep(&sql).await?;
let mut stream = conn.exec_stream::<Row, _, _>(&stmt, params).await?;
while let Some(row) = stream.next().await { /* decode into Arrow builders */ }
```

Two choices matter here:

**Binary protocol over text protocol.** A plain text-protocol query returns every value as a
string: integers, decimals, and timestamps all arrive formatted and must be parsed. Prepared
statements use the binary protocol, where an `INT` is 4 bytes and a `DATETIME` is a packed
structure. For extraction workloads this is a large decode-CPU difference, and it removes a class
of locale- and format-dependent parsing bugs.

**Streaming over buffering.** `exec_stream` yields rows as they arrive rather than materializing
the result set. This is what makes backpressure work; a buffering API turns a large table into an
OOM. (Rust's ergonomics are better than JDBC's here — there is no equivalent of the
`setFetchSize(Integer.MIN_VALUE)` incantation — but the underlying requirement is the same.)

### The `net_write_timeout` trap

While the client is streaming a large result, the server is blocked writing to the socket. If the
client consumes slowly — because a downstream sink applied backpressure, which is *by design* —
the server can hit `net_write_timeout` (default 60s) and **kill the connection mid-result**. The
symptom is a "lost connection during query" partway through a long extraction, which looks like a
network problem and is not.

The connector raises it per session:

```sql
SET SESSION net_write_timeout = 600;
```

and treats the resulting error class as retryable at partition granularity. *(Design:
the prototype has a single serial scan path; per-partition retry arrives with
parallel execution.)*

---

## 2. Type mapping

MySQL's wire protocol under-describes its own types, so the connector resolves every column from
`information_schema.COLUMNS` at plan time — `COLUMN_TYPE` (which carries `unsigned`, display width,
and `ENUM`/`SET` members), `CHARACTER_SET_NAME`, `COLLATION_NAME`, `NUMERIC_PRECISION`,
`NUMERIC_SCALE`, and `DATETIME_PRECISION`.

| MySQL | Arrow | Notes |
| --- | --- | --- |
| `TINYINT` / `SMALLINT` / `MEDIUMINT` / `INT` / `BIGINT` | `Int8` / `Int16` / `Int32` / `Int32` / `Int64` | |
| the same, `UNSIGNED` | `UInt8` / `UInt16` / `UInt32` / `UInt32` / `UInt64` | **`BIGINT UNSIGNED` does not fit `i64`.** Must map to `UInt64`, and literals above `i64::MAX` must be bound as unsigned |
| `TINYINT(1)` | `Int8` | Ambiguous by convention (most ORMs use it as a boolean but some genuinely store −128..127). The prototype maps it to `Int8`; a `tinyint1_as_bool` option is *(design, not implemented)* |
| `BOOL` / `BOOLEAN` | `Boolean` | Aliases for `TINYINT(1)` |
| `DECIMAL(p,s)`, p ≤ 38 | `Decimal128(p, s)` | |
| `DECIMAL(p,s)`, 38 < p ≤ 65 | `Decimal256(p, s)` | MySQL allows up to 65 digits, exceeding `Decimal128` |
| `FLOAT` / `DOUBLE` | `Float32` / `Float64` | Comparisons always `Inexact` |
| `BIT(n)` | `UInt64` (n ≤ 64) | Big-endian on the wire |
| `DATE` | `Date32` | `0000-00-00` is **not representable** — see §3 |
| `DATETIME(p)` | `Timestamp(µs, None)` | No time zone attached, ever |
| `TIMESTAMP(p)` | `Timestamp(µs, None)` in the prototype | Converted by the server using the session `time_zone` (see §4); no zone is recorded on the Arrow type |
| `TIME(p)` | `Duration(Microsecond)` | Range is **−838:59:59 to 838:59:59**, an interval rather than a time of day. `Time64` would be wrong |
| `YEAR` | `Int16` | |
| `CHAR` / `VARCHAR` / `TEXT` family | `Utf8` | Collation recorded; drives `Exact` vs `Inexact` — see §3 |
| `BINARY` / `VARBINARY` / `BLOB` family | `Binary` | Distinguished from `TEXT` only by the `binary` character set (id 63), not by the protocol type |
| `ENUM` | `Utf8` in the prototype | Members parsed from `COLUMN_TYPE`; `Dictionary(Int32, Utf8)` is *(design, not implemented)* |
| `SET` | `Utf8` | Comma-joined as stored |
| `JSON` | `Utf8` | Stored as binary internally, returned as text |
| spatial types | `Utf8` in the prototype | Unrecognized types fall back to `Utf8` (no `cast_to` hatch yet) |

---

## 3. Values MySQL permits that Arrow cannot represent

This is MySQL's distinguishing hazard. *(Design: the `on_unrepresentable` policy and the
`rel_null_coerced_total` counter described below do not exist yet — the prototype surfaces
decode failures as errors.)*

**Zero dates.** Unless `sql_mode` includes `NO_ZERO_DATE` and `NO_ZERO_IN_DATE`, MySQL accepts
`0000-00-00` and `2026-00-15`. Neither is a real date and neither has an Arrow encoding. The
design maps them to null, incrementing `rel_null_coerced_total{reason="zero_date"}`. Legacy
schemas frequently use `0000-00-00` as a "no value" sentinel, so this counter is often non-zero
on the first run against an old database — which is exactly the moment you want to know about
it rather than discover it in a warehouse query six weeks later.

**Out-of-range `TIME`.** Values beyond ±24h are legal and are why the mapping is `Duration`.

**Truncated data in non-strict mode.** With a permissive `sql_mode`, MySQL silently truncates
oversized values on write. Nothing the extractor can do about data already stored, but the
planned `rel doctor` command would report the source's `sql_mode` so the behavior is at least
visible.

**Case-insensitive collation.** MySQL 8.0's default `utf8mb4_0900_ai_ci` is accent- and
case-insensitive, so `WHERE status = 'PAID'` matches `'paid'` in MySQL and not in Arrow. Every
string predicate on a `_ci` or `_ai` column is therefore pushed as **`Inexact`**, never `Exact`.
This is covered in full in [pushdown §3.1](../pushdown.md#31-string-collation--the-big-one) and it
is the single most likely way to get silently wrong results from this connector.

---

## 4. Time zones

`DATETIME` stores what you wrote. `TIMESTAMP` is stored as UTC and converted to and from the
session `time_zone` on every read and write. A pipeline that does not pin the session time zone
produces different data when the server's default changes, when it reads from a replica configured
differently, or when DST shifts.

```sql
SET SESSION time_zone = '+00:00';
```

Applied on every connection. `DATETIME` columns still carry no zone information — the Arrow type is
deliberately `Timestamp(µs, None)` rather than a lie about UTC, and interpreting them is a
modeling decision for the warehouse, not something the extractor should guess.

---

> **Scope note:** watermark anchoring is deferred — see [deferred/incremental-extraction.md](../deferred/incremental-extraction.md). Ranges are caller-provided.

## 5. Consistency notes

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

### 5.4 Reading from a replica

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

## 6. Statistics and cost estimation

Weaker than Postgres, and the cost model has to account for that.

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
might trigger a full scan on a production database. The planned `rel doctor` command would report
which columns referenced by a job lack histograms and print the `ANALYZE` statement that would
fix it.

Never use `EXPLAIN ANALYZE` (8.0.18+) for estimation — it executes the query.

---

## 7. Session configuration

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
CREATE USER 'rel_extract'@'%' IDENTIFIED BY '…';
GRANT SELECT                ON app.*  TO 'rel_extract'@'%';
-- Only needed for the deferred §§5.2/5.4 features, not for prototype extraction:
-- GRANT PROCESS               ON *.*    TO 'rel_extract'@'%';  -- INNODB_TRX for §5.2
-- GRANT REPLICATION CLIENT    ON *.*    TO 'rel_extract'@'%';  -- SHOW REPLICA STATUS for §5.4
```

`PROCESS` and `REPLICATION CLIENT` are broad grants. Where a DBA declines them, the design
falls back to `role = "replica"` plus a manually specified `replica_lag_floor` and a generous
`safety_lag` — the fallback is fine, but it has to be a decision someone made on purpose.

---

## 8. Partitioning

Only `keyset` is available. There is no `ctid` analogue, so there is no cheap physical split:

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
serial and parallelism is something you turn on knowingly.

---

## 9. Future: binlog CDC

The same shape as the Postgres logical replication path, and the same payoff — hard deletes become
visible and commit-order skew disappears, since binlog events are ordered by commit.

Requirements: `binlog_format = ROW`, `binlog_row_image = FULL` (otherwise updates carry only changed
columns, and reconstructing full rows requires warehouse-side state), and `REPLICATION SLAVE`
privilege. The checkpoint becomes a GTID set, which §5.3 describes, so the migration path
from timestamp-based to log-based extraction is a checkpoint conversion rather than a re-backfill.

The operational hazard mirrors Postgres' replication slot problem in a different form: binlog
retention (`binlog_expire_logs_seconds`) is finite, so a consumer that falls behind past retention
cannot resume and must re-snapshot. Lag monitoring against retention is a prerequisite, not a
follow-up. Deferred past Phase 3.
