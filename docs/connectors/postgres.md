# PostgreSQL Connector

Crate: `rel-connector-postgres`. Built on `tokio-postgres` (raw protocol access, streaming, binary
`COPY`) rather than an ORM or a query builder — we generate SQL from the planner and need the wire
format, not row-mapping convenience.

Covers self-managed PostgreSQL, Cloud SQL for PostgreSQL, and AlloyDB. Read replicas and hot
standbys are supported with an important watermark caveat in §5.3.

---

## 1. Why Postgres is the better-behaved of the two connectors

Three capabilities that MySQL lacks make Postgres the reference implementation:

- **Binary `COPY`** gives a bulk export path with far less per-row protocol overhead than the
  extended query protocol.
- **`pg_export_snapshot()`** lets multiple connections read the same MVCC snapshot, so a parallel
  scan is genuinely atomic.
- **`pg_stat_activity.xact_start`** lets us compute an exact — not heuristic — safe high watermark.

---

## 2. Extraction path

### 2.1 Binary COPY (removed September 2026 — spec retained)

> The implementation below was removed to slim the dependency tree (`tokio-postgres` and
> friends): it had no callers — every scan path used §2.2 — and no tests. Restorable from
> git history. What follows is kept as the spec for reintroducing it, since the format
> knowledge is the expensive part.

```sql
COPY (
  SELECT order_id, user_id, amount, updated_at
  FROM   public.orders
  WHERE  updated_at > $1 AND updated_at <= $2
) TO STDOUT (FORMAT binary)
```

`tokio-postgres` exposes this as `BinaryCopyOutStream`. `COPY` skips per-row `DataRow` message
framing overhead and result-set metadata, and the binary format needs no text parsing — a
`timestamp` arrives as an 8-byte integer, not as `2026-09-04 15:30:00.123456+07`, which for
timestamp- and numeric-heavy tables is the difference between decoding being free and decoding
being the bottleneck.

The stream format is a fixed header (`PGCOPY\n\377\r\n\0`, a flags word, and a header-extension
length), then per tuple a big-endian `int16` field count followed by, per field, a big-endian
`int32` byte length (`-1` meaning null) and that many bytes. We decode field bytes directly into
Arrow builders.

`COPY` has one real limitation: **no bound parameters**. The window bounds above must be literals in
the `COPY` text. Since [pushdown](../pushdown.md#2-expression-translation) mandates bound
parameters, the connector resolves this by preparing the inner `SELECT` as a server-side statement
with parameters and having `COPY` reference it, or — where that is not possible — by rendering only
*typed, connector-formatted* literals (never user strings) into the `COPY` body. Arbitrary
user-supplied string literals force a fallback to §2.2.

### 2.2 Extended query protocol with a portal (the only path)

```rust
let stmt = client.prepare_typed(&sql, &param_types).await?;
let stream = client.query_raw(&stmt, params).await?;   // streams, does not buffer
```

`query_raw` returns a `RowStream` backed by a portal, so rows arrive incrementally rather than the
whole result set materializing client-side. Slightly more per-row overhead than `COPY`, but it
supports bound parameters and, importantly, it composes with the transaction and snapshot handling
below. Incremental runs — which are dominated by a small, selective window — default to this path;
`COPY` is used for backfills, snapshots, and any run whose estimated row count exceeds
`copy_threshold` (default 1,000,000).

Result format is requested as **binary** in both paths. Text format would mean parsing every
numeric and timestamp from a string.

---

## 3. Type mapping

| PostgreSQL | Arrow | Notes |
| --- | --- | --- |
| `bool` | `Boolean` | 1 byte on the wire |
| `int2` / `int4` / `int8` | `Int16` / `Int32` / `Int64` | Big-endian |
| `float4` / `float8` | `Float32` / `Float64` | IEEE-754; `NaN` ordering differs from Arrow — see [pushdown §3.3](../pushdown.md#33-numeric-type-width-and-precision) |
| `numeric(p,s)`, p ≤ 38 | `Decimal128(p, s)` | Wire format is base-10000 digit groups; must be reassembled |
| `numeric`, unconstrained | `Decimal128(38, 9)` by default | Overflow → error or null per `on_unrepresentable`. `NaN`/`±Infinity` sign words map to null |
| `text`, `varchar`, `char`, `name` | `Utf8` | Collation recorded in `ColumnMeta`; drives `Exact` vs `Inexact` |
| `citext` | `Utf8` | Comparisons are always `Inexact` |
| `bytea` | `Binary` | |
| `uuid` | `FixedSizeBinary(16)` | `Utf8` optionally, via `uuid_as_string` |
| `date` | `Date32` | Wire value is **days since 2000-01-01**; add 10,957 for the Unix epoch |
| `timestamp` | `Timestamp(Microsecond, None)` | **Microseconds since 2000-01-01**; add 946,684,800,000,000 |
| `timestamptz` | `Timestamp(Microsecond, "UTC")` | Same encoding; already UTC internally |
| `time` | `Time64(Microsecond)` | Microseconds since midnight |
| `timetz` | `Utf8` | An offset-carrying time has no clean Arrow type; discouraged upstream |
| `interval` | `Interval(MonthDayNano)` | Wire is (int64 µs, int32 days, int32 months) |
| `json` | `Utf8` | Plain UTF-8 |
| `jsonb` | `Utf8` | **Leading version byte (`0x01`) precedes the text in binary format — strip it** |
| enum types | `Dictionary(Int32, Utf8)` | Labels read from `pg_enum` at plan time |
| `T[]` | `List(map(T))` | Binary array header: ndim, flags, element OID, dims, lower bounds. Only 1-D arrays with lower bound 1 are supported; anything else errors |
| `money` | error by default | Scale depends on the server's `lc_monetary`. Explicit `cast_to` required |
| range, `hstore`, `tsvector`, geometry | error | No mapping; use `cast_to = "utf8"` to take the text form deliberately |
| `oid`, `xid`, `cid` | `UInt32` | |

Two encodings deserve extra care in review because they are the ones most likely to be subtly wrong:

**`numeric`** arrives as `int16 ndigits`, `int16 weight`, `uint16 sign`, `int16 dscale`, then
`ndigits` base-10000 groups. `weight` is the base-10000 exponent of the first group, `dscale` is the
display scale, and the sign word also encodes `NaN` and (on newer servers) `±Infinity`.
Reconstructing a `Decimal128` requires scaling by `dscale`, not by `ndigits`. Round-trip tests over
generated values with mixed weights and scales are mandatory here.

**Timestamps** use a **2000-01-01 epoch**, not 1970. Getting this wrong shifts every timestamp by
30 years, which is obvious in a test and embarrassing in production.

---

## 4. Statistics and cost estimation

```sql
-- Row count and physical size, effectively free
SELECT reltuples::bigint, relpages
FROM   pg_class WHERE oid = $1::regclass;

-- Per-column distribution for selectivity estimation
SELECT attname, n_distinct, null_frac, most_common_vals, most_common_freqs, histogram_bounds
FROM   pg_stats
WHERE  schemaname = $1 AND tablename = $2 AND attname = ANY($3);

-- Access method and cost for a candidate predicate, WITHOUT executing it
EXPLAIN (FORMAT JSON, VERBOSE false, COSTS true) SELECT …;
```

Postgres maintains `pg_stats` automatically via autovacuum, so the cost model has real distribution
data without asking anyone to run maintenance commands. `most_common_vals` / `most_common_freqs`
give accurate selectivity for exactly the low-cardinality equality predicates that dominate
extraction jobs (`status = 'PAID'`), and `histogram_bounds` handles range predicates on the
watermark column.

`EXPLAIN` is the decisive input: its plan node type tells us whether the predicate produces an
`Index Scan`, `Bitmap Heap Scan`, or `Seq Scan`, and its cost estimate feeds directly into the
`max_source_cost` budget from [pushdown §4](../pushdown.md#4-the-cost-model). Never
`EXPLAIN ANALYZE` — that runs the query.

`histogram_bounds` is reused for a second purpose: deriving evenly sized keyset partition
boundaries without a `min`/`max` scan.

---

## 5. Consistency and watermark computation

### 5.1 Exported snapshots for atomic parallel scans

Postgres is the only one of the two connectors that can make a parallel scan atomic:

```sql
-- Coordinator connection
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
SELECT pg_export_snapshot();          -- e.g. '00000003-0000001B-1'

-- Each worker connection
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
SET TRANSACTION SNAPSHOT '00000003-0000001B-1';
COPY (SELECT … WHERE ctid >= '(0,0)' AND ctid < '(12500,0)') TO STDOUT (FORMAT binary);
```

Every partition then sees exactly the same MVCC snapshot, so the union of partitions is a
consistent point-in-time read even though it arrived over four connections.

The constraint: the exporting transaction **must stay open** until every worker has executed
`SET TRANSACTION SNAPSHOT`. That is a long-lived read-only transaction, which holds back the xmin
horizon and therefore delays vacuum. The connector caps the whole snapshot scan with
`snapshot_max_duration` (default 30 minutes) and aborts rather than exceeding it, because a stuck
extraction job that prevents vacuum on a busy table is a genuine production incident.

### 5.2 The safe high watermark

The exact form of the [commit-skew mitigation](../incremental-extraction.md#31-commit-time-vs-updated_at-skew--the-primary-hazard):

```sql
SELECT LEAST(
         now() - INTERVAL '1 second',
         COALESCE(MIN(xact_start), now())
       ) AS safe_hi
FROM pg_stat_activity
WHERE backend_type = 'client backend'
  AND state <> 'idle'
  AND datname = current_database();
```

Requires `pg_read_all_stats` membership (or superuser) to see other roles' `xact_start`; without it
the column is NULL for other sessions and the query silently returns `now()`. The connector
**verifies the privilege at startup** and falls back to a fixed `safety_lag` with a loud warning if
it is missing — a silent downgrade here means silent data loss later.

### 5.3 Reading from a hot standby

Two things change on a replica, and both are traps.

**The watermark must be bounded by replay progress, not by the clock.** A standby's `now()` is
current wall-clock time, but its *data* is as of the last replayed transaction. Setting
`hi = now()` on a replica advances the watermark past rows that have not arrived yet — they are
then never re-read. The correct bound:

```sql
SELECT LEAST(pg_last_xact_replay_timestamp(), now() - INTERVAL '1 second');
```

This is exact and it makes replica reads safe. It is also the single most valuable line in this
document.

**Queries get cancelled by recovery conflicts.** A long read on a standby can be killed with
`canceling statement due to conflict with recovery` when replay needs to remove rows the query can
still see. Mitigations: enable `hot_standby_feedback` on the standby (at the cost of bloat on the
primary), raise `max_standby_streaming_delay`, or keep scans short. The connector classifies error
code `40001` on a standby as retryable and retries the whole partition with backoff, because a
partial partition is discardable by design.

---

## 6. Session configuration

Applied on every connection, before any query:

```sql
SET application_name = 'rel:<job_id>:<table>';
SET TIME ZONE 'UTC';
SET statement_timeout = '300s';
SET idle_in_transaction_session_timeout = '60s';
SET lock_timeout = '5s';
SET default_transaction_read_only = on;
SET jit = off;
```

`jit = off` is deliberate: JIT compilation costs more than it saves on the short, simple,
high-row-count queries an extractor issues, and it introduces plan-time variance that confuses the
cost model. `lock_timeout` guarantees we never queue behind a DDL lock.

### Required privileges

```sql
CREATE ROLE rel_extract LOGIN PASSWORD '…';
GRANT CONNECT ON DATABASE app TO rel_extract;
GRANT USAGE  ON SCHEMA public TO rel_extract;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO rel_extract;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO rel_extract;
GRANT pg_read_all_stats TO rel_extract;   -- for the exact safe high watermark (§5.2)
```

---

## 7. Partitioning strategies

| Strategy | Predicate | When |
| --- | --- | --- |
| `physical` | `ctid >= '(a,0)' AND ctid < '(b,0)'` | Best for full scans inside an exported snapshot. Bounds derived from `relpages`; evenly sized regardless of key distribution. Only valid within one snapshot, since `ctid` moves on update/vacuum |
| `keyset` | `pk >= $1 AND pk < $2` | Default. Bounds from `histogram_bounds`, which handles non-uniform keys |
| `native` | one partition per child table of a declarative partitioned table | Best when it applies; aligns with the source's own pruning |
| `modulo` | `hashint8(pk) % n = i` | Last resort; forces a full scan per partition |

`ctid` ranging is genuinely fast and is the reason full snapshots on Postgres parallelize well — but
it is only correct under the exported snapshot of §5.1. Outside one, concurrent updates move rows
between ranges and a row can be read twice or not at all.

---

## 8. Future: logical replication (CDC)

The path that closes the two gaps timestamp watermarks cannot close — **hard deletes** and
**commit-order skew** — because WAL records are, by construction, in commit order.

```
CREATE_REPLICATION_SLOT rel_orders LOGICAL pgoutput
START_REPLICATION SLOT rel_orders LOGICAL 0/0 (proto_version '4', publication_names 'rel_pub')
        │
        ▼
  Begin / Relation / Insert / Update / Delete / Commit messages
        │
        ▼
  Arrow batches with an op column, checkpointed by LSN
```

The checkpoint becomes an LSN, which is monotonic in commit order, so §3.1 of the incremental doc
simply stops applying.

The operational hazard is severe enough to state up front: **an unconsumed replication slot retains
WAL indefinitely and will fill the primary's disk.** Any CDC implementation must ship with slot lag
monitoring (`pg_replication_slots.confirmed_flush_lsn` vs `pg_current_wal_lsn()`) and automatic slot
drop on prolonged job failure, before it is used against anything that matters. This is deferred
past Phase 3 for exactly that reason.
