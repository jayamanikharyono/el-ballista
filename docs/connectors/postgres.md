# PostgreSQL Connector

Module: `connector::postgres` (in this crate). Built on `sqlx` (extended query protocol,
**binary** result format) with two streaming scan paths: a `DECLARE … CURSOR` + `FETCH`
loop (default) and binary `COPY … TO STDOUT` (`execution.use_copy`).

Tested against PostgreSQL 17 (CI); managed variants (Cloud SQL, AlloyDB) and standbys should
work but are not tested.

The connector exposes full and filtered scans, partitioning, and pushdown; range selection is
caller-provided. Watermark computation is not part of it (design kept in
[deferred/incremental-extraction.md](../deferred/incremental-extraction.md)).

---

## 1. Why Postgres is the reference implementation

Postgres provides the capabilities that make it the reference:

- **Server-side cursors** (`DECLARE … CURSOR` + `FETCH FORWARD n`) give a streaming path with
  bound parameters, bounded client memory, and one statement per window.
- **Binary `COPY … TO STDOUT`** gives a bulk path with the same binary value encodings.
- Deterministic collations (`C` / `POSIX`) make `Exact` fidelity comparisons possible.

---

## 2. Extraction path

Both paths decode the **same binary value representations** through one per-column decoder
(`connector::postgres::row_adapter`), chosen once per scan from the mapped Arrow type — so the
cursor and COPY paths agree by construction (differential-tested in `tests/pg_copy.rs` by
comparing whole rows). Both flush Arrow batches on a row cap (`batch_size`, must be > 0) **or**
a byte cap (`max_batch_bytes`, counting fixed-width buffers and variable-width payload).

### 2.1 Cursor path (default)

```sql
BEGIN;
DECLARE extract_cur_<uuid> CURSOR WITHOUT HOLD FOR SELECT … WHERE … ;  -- binds allowed
FETCH FORWARD <batch_size> FROM extract_cur_<uuid>;                   -- repeated until empty
CLOSE extract_cur_<uuid>;
COMMIT;                                                                -- ROLLBACK on error
```

Rows of each `FETCH` stream into Arrow builders (no intermediate `Vec<PgRow>`); sqlx requests
binary results, and each value is read as a borrowed byte slice. Every `FETCH` is its own
statement, so `statement_timeout` applies per window, not to the whole partition. The cursor is
`WITHOUT HOLD` (a `WITH HOLD` cursor would make the server materialize the result). A stall
longer than `idle_in_transaction_session_timeout` between `FETCH`es aborts the scan. The
DataFusion/Ballista `PostgresExecutionPlan` uses this same path per partition; dropping its
output stream stops fetching (the cursor is closed and the transaction rolled back after at
most one in-flight window).

### 2.2 Binary COPY (`execution.use_copy`)

Full/keyset scans can instead run `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` when
`execution.use_copy` is true (default false): one statement streams the whole result in binary
framing — no per-`FETCH` round trips — decoded incrementally by `connector::postgres::copy`.

Limits, by construction of `COPY` and of the decoder set:
- COPY is a **single statement**: `statement_timeout` bounds the whole partition scan
  (including time the consumer spends applying backpressure). `execution.copy_statement_timeout_ms`
  overrides it for COPY scans only (`SET LOCAL` inside a transaction around the COPY, so the
  override never leaks into a pooled session; `0` disables it); cursor scans are unaffected;
- only scans **without pushed filters** use COPY (it accepts no bind parameters); anything
  with bound literals falls back to the cursor path with a loud warn log (never silent);
- keyset bounds inline as `i64` literals (no quoting surface);
- types mirror the cursor decoder exactly (`ArrowTypeMapper` gates both); anything else is
  `UnsupportedType`, never silently wrong data;
- a COPY that does not finish (decode error, consumer error, dropped stream) closes its
  connection instead of returning it to the pool, and a background task sends
  `pg_cancel_backend(pid)` over another connection **of the same pool** (so the cancel stays
  inside `pool_max`; if none frees up within 5 s the cancel is skipped and the server aborts the
  COPY on its next write to the closed socket).

### 2.3 Isolation semantics

A cursor reads from the snapshot taken at `DECLARE` (a `FETCH` never takes a new snapshot,
under READ COMMITTED as well), and a `COPY` reads one snapshot. So **each partition is
snapshot-consistent**, but **partitions are not mutually consistent**: they are separate
statements on separate connections with separate snapshots, so a row whose partition key is
updated between two partitions' snapshots can be missed or read twice. Each partition pins its
snapshot (`xmin`) for its whole duration, holding back vacuum while it runs. No cross-partition
snapshot guarantee is claimed (exported snapshots are not implemented).

---

## 3. Type mapping

| PostgreSQL | Arrow | Status | Notes |
| --- | --- | --- | --- |
| `bool` | `Boolean` | Implemented | 1 byte on the wire |
| `int2` / `int4` / `int8` | `Int16` / `Int32` / `Int64` | Implemented | Big-endian |
| `float4` / `float8` | `Float32` / `Float64` | Implemented | IEEE-754; `NaN` ordering differs from Arrow — see [pushdown §3.3](../pushdown.md#33-numeric-type-width-and-precision) |
| `numeric(p,s)`, p ≤ 38 | `Decimal128(p, s)` | Implemented | Binary base-10000 digits, exact integer arithmetic. `NaN` / `±Infinity` → typed error (`UnsupportedValue`, names the column). Comparisons never push |
| `numeric`, unconstrained | `Decimal128(38, 10)` | Implemented | A value with non-zero digits beyond 10 fractional places, or more than 28 integer digits, → typed error naming the column (never truncated). `NaN` / `±Infinity` → typed error |
| `text`, `varchar` | `Utf8` | Implemented | Borrowed UTF-8 decode. Comparisons push `Exact` under `COLLATE "C"`, whatever the column collation, when `server_encoding = UTF8`; on other encodings only `=` / `<>` push ([pushdown §2](../pushdown.md#2-expression-translation)) |
| `char(n)` | `Utf8` | Implemented | No comparison pushes (`bpchar` ignores trailing blanks, Arrow does not) |
| `bytea` | `Binary` | Implemented | |
| `uuid` | `Utf8` | Implemented | Selected as `::text` (canonical 36-char form); not `FixedSizeBinary(16)`. `=` / `<>` push `Exact` |
| `date` | `Date32` | Implemented | Binary `i32` days since 2000-01-01, shifted to 1970. `±infinity` / overflow → typed error. Comparisons against date literals push `Exact` |
| `timestamp` | `Timestamp(Microsecond, None)` | Implemented | Binary `i64` µs since 2000-01-01, shifted with checked arithmetic. `±infinity` / overflow → typed error |
| `timestamptz` | `Timestamp(Microsecond, "UTC")` | Implemented | As `timestamp` (session `TIME ZONE 'UTC'`). `±infinity` / overflow → typed error |
| `time` | — | Not implemented | |
| `timetz` | — | Not implemented | |
| `interval` | — | Not implemented | |
| `json` | `Utf8` | Implemented | Selected as `::text`: Postgres' own rendering, byte for byte, on both paths |
| `jsonb` | `Utf8` | Implemented | Selected as `::text` (Postgres' normalized jsonb rendering; big numbers preserved exactly) |
| enum types | `Utf8` | Implemented | `::text` label cast; not `Dictionary(Int32, Utf8)`. All six comparisons push `Exact` on the label text |
| other `USER-DEFINED` types (`citext`, `hstore`, PostGIS `geometry`, …) | `Utf8` | Implemented | Selected as `::text`; `=` / `<>` push `Exact` against the text form |
| `text[]` | `List(Utf8)` | Implemented | 1-D (or empty) arrays; NULL elements kept |
| other `T[]` | — | Not implemented | |
| `money` | — | Not implemented | No `cast_to` config |
| built-in range types, `tsvector` | — | Not implemented | No `cast_to` config |
| `oid`, `xid`, `cid` | — | Not implemented | |

A type is `USER-DEFINED` when `information_schema.columns` reports it so — types defined
outside `pg_catalog`, which includes extension types. Built-in types not listed above fail
with `UnsupportedType` before the scan starts.

Two encodings deserve extra care in review because they are the ones most likely to be subtly wrong:

**`numeric`** arrives as `int16 ndigits`, `int16 weight`, `uint16 sign`, `int16 dscale`, then
`ndigits` base-10000 groups. `weight` is the base-10000 exponent of the first group, `dscale` is the
display scale, and the sign word also encodes `NaN` and (on newer servers) `±Infinity`.
The decoder ignores `dscale` (display only) and shifts each digit group by `10^(scale + 4·(weight−i))`
into the column's `Decimal128` scale; a group whose low digits would fall below that scale must be
zero, or the value is rejected instead of truncated. Round-trip tests over mixed weights and scales
live in `arrow_type_mapper` and `tests/pg_decode.rs`.

**Timestamps** use a **2000-01-01 epoch**, not 1970. Getting this wrong shifts every timestamp by
30 years, which is obvious in a test and embarrassing in production. `infinity` / `-infinity` are
`i64::MAX` / `i64::MIN` (dates: `i32::MAX` / `i32::MIN`) on the wire and are rejected on both paths.

---

## 4. Statistics and cost estimation

```sql
-- Row count and size, effectively free
SELECT c.reltuples, pg_total_relation_size(c.oid)
FROM   pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE  n.nspname = $1 AND c.relname = $2;

-- Per-column statistics
SELECT attname, n_distinct, null_frac, avg_width
FROM   pg_stats
WHERE  schemaname = $1 AND tablename = $2;

-- Distribution of integer, date and timestamp columns, converted to numbers in SQL
-- (dates as days, timestamps as epoch seconds; infinities dropped)
SELECT attname, histogram_bounds, most_common_vals, most_common_freqs FROM pg_stats …;

-- Access method and cost for a candidate predicate, WITHOUT executing it
EXPLAIN (FORMAT JSON) SELECT * FROM "schema"."table" WHERE <predicate>;
```

Postgres maintains `pg_stats` automatically via autovacuum, so the cost model has real
distribution data without asking anyone to run maintenance commands. How it is used:

- **Equality** selectivity is `1 / n_distinct` (`most_common_vals` are not consulted for
  equality).
- **Range filters** (`<`, `<=`, `>`, `>=`) on integer, date and timestamp columns are estimated
  from `histogram_bounds` and `most_common_vals` / `most_common_freqs`. The distribution query is
  best-effort; without it a range filter uses the default estimate.
- **Windows.** Range filters on the same column are estimated together, so the two sides of a
  window share one selectivity. A window's cost ignores a cached `EXPLAIN` of one side, because
  that `EXPLAIN` prices an open-ended half-range, not the window (details in
  [pushdown](../pushdown.md#range-selectivity-and-windows)).

`EXPLAIN` is otherwise the decisive input: its plan node type tells us whether the predicate
produces an `Index Scan`, `Bitmap Heap Scan`, or `Seq Scan`, and its cost estimate feeds
directly into the `max_source_cost` budget from
[pushdown §4](../pushdown.md#4-the-cost-model). Never `EXPLAIN ANALYZE` — that runs the query.

Keyset partition bounds come from `MIN`/`MAX` on the raw partition column (index probes), not
from `histogram_bounds`.

---

## 5. Consistency notes

Isolation is described in §2.3. For hot tables prefer `keyset` partitioning over `ctid`
(concurrent updates move rows between physical pages, see §7), and never rely on row order
without an explicit `ORDER BY` — the source returns rows in unspecified order.

The orchestrator owns range selection; this connector only executes the scan it is given.
Timestamp-based incremental protocols (commit-skew mitigation via
`pg_stat_activity.xact_start`, safety lag, bounded windows) are not part of this layer; see
[deferred/incremental-extraction.md](../deferred/incremental-extraction.md).

---

## 6. Session configuration

Applied on every connection the pool opens, before any query (`PostgresExtractor::connect`
and the distributed pool registry):

```sql
-- application_name is set as a connection option (source.application_name)
SET TIME ZONE 'UTC';
SET statement_timeout = '<source.statement_timeout_ms>ms';   -- per statement: per FETCH, or the whole COPY
SET idle_in_transaction_session_timeout = '60s';
SET lock_timeout = '5s';
```

`lock_timeout` guarantees we never queue behind a DDL lock. Not applied by the connector (recommended for
production roles, e.g. via `ALTER ROLE … SET`): `default_transaction_read_only = on` and
`jit = off` (JIT costs more than it saves on the short, high-row-count queries an extractor
issues, and adds plan-time variance that confuses the cost model).

### Required privileges

```sql
CREATE ROLE el_ballista LOGIN PASSWORD '…';
GRANT CONNECT ON DATABASE app TO el_ballista;
GRANT USAGE  ON SCHEMA public TO el_ballista;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO el_ballista;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO el_ballista;
```

---

## 7. Partitioning strategies

| Strategy | Predicate | Status | When |
| --- | --- | --- | --- |
| `ctid` | `ctid >= '(a,0)' AND ctid < '(b,0)'` | Implemented | Evenly sized physical page ranges; bounds from `relpages`. See the note below |
| `keyset` | first: `k < b1 OR k IS NULL`; middle: `k >= bᵢ AND k < bᵢ₊₁`; last: `k >= bₙ₋₁` | Implemented | Even split of `[MIN(k), MAX(k)]` (one index probe each on the raw column), bound math in `i128`. NULL keys go to the first partition exactly once; both ends are open (the first partition has no lower bound, the last no upper bound), so the partitions cover every key — including `i64::MIN`/`i64::MAX` and rows inserted outside `[MIN, MAX]` before a resumed run reuses its stored bounds. Not `histogram_bounds`-based: skewed keys give uneven partitions |
| `native` | one partition per child table of a declarative partitioned table | Not implemented | Aligns with source's own pruning |
| `modulo` | `hashint8(pk) % n = i` | Not implemented | Last resort; forces a full scan per partition |

`ctid` ranges are only exact within a single snapshot: each partition takes its own, so a row
that a concurrent update moves to another page can be missed or read twice. Closing that gap
needs an exported snapshot shared by all partitions, which is not implemented.

---

## 8. Out of scope: CDC

Log-based change data capture (logical replication, `pgoutput`) is out of scope for El Ballista;
the orchestrator or a dedicated CDC tool owns it. It is the path that closes the two gaps
filter-based incremental extraction cannot — **hard deletes** and **commit-order skew** —
because WAL records are, by construction, in commit order, and an LSN checkpoint is monotonic
in commit order.

The operational hazard is worth stating for anyone adding it: **an unconsumed replication slot
retains WAL indefinitely and will fill the primary's disk.** Any CDC implementation must ship
with slot lag monitoring (`pg_replication_slots.confirmed_flush_lsn` vs `pg_current_wal_lsn()`)
and automatic slot drop on prolonged job failure, before it is used against anything that
matters.
