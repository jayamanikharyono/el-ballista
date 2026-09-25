# PostgreSQL Connector

Module: `connector::postgres` (in this crate). Built on `sqlx` (extended query protocol,
**binary** result format) with two streaming scan paths: a `DECLARE … CURSOR` + `FETCH`
loop (default) and binary `COPY … TO STDOUT` (`execution.use_copy`).

Covers self-managed PostgreSQL, Cloud SQL for PostgreSQL, and AlloyDB. Read replicas and hot
standbys are supported.

> **Scope note:** watermark computation (§5 in earlier revisions) is deferred — see
> [deferred/incremental-extraction.md](../deferred/incremental-extraction.md). The connector
> exposes full/filtered scans, partitioning, and pushdown; range selection is caller-provided.

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
| `bool` | `Boolean` | **Implemented** | 1 byte on the wire |
| `int2` / `int4` / `int8` | `Int16` / `Int32` / `Int64` | **Implemented** | Big-endian |
| `float4` / `float8` | `Float32` / `Float64` | **Implemented** | IEEE-754; `NaN` ordering differs from Arrow — see [pushdown §3.3](../pushdown.md#33-numeric-type-width-and-precision) |
| `numeric(p,s)`, p ≤ 38 | `Decimal128(p, s)` | **Implemented** | Binary base-10000 digits, exact integer arithmetic. `NaN` / `±Infinity` → typed error (`UnsupportedValue`, names the column) |
| `numeric`, unconstrained | `Decimal128(38, 10)` | **Implemented** | A value with non-zero digits beyond 10 fractional places, or more than 28 integer digits, → typed error naming the column (never truncated). `NaN` / `±Infinity` → typed error |
| `text`, `varchar`, `char` | `Utf8` | **Implemented** | Borrowed UTF-8 decode. Collation recorded in `ColumnMetadata`; drives `Exact` vs `Inexact` |
| `citext` | `Utf8` | **NOT IMPLEMENTED** | Comparisons would always be `Inexact` |
| `bytea` | `Binary` | **Implemented** | |
| `uuid` | `Utf8` | **Implemented** | Selected as `::text` (canonical 36-char form); not `FixedSizeBinary(16)` |
| `date` | `Date32` | **Implemented** | Binary `i32` days since 2000-01-01, shifted to 1970. `±infinity` / overflow → typed error |
| `timestamp` | `Timestamp(Microsecond, None)` | **Implemented** | Binary `i64` µs since 2000-01-01, shifted with checked arithmetic. `±infinity` / overflow → typed error |
| `timestamptz` | `Timestamp(Microsecond, "UTC")` | **Implemented** | As `timestamp` (session `TIME ZONE 'UTC'`). `±infinity` / overflow → typed error |
| `time` | — | **NOT IMPLEMENTED** | |
| `timetz` | — | **NOT IMPLEMENTED** | |
| `interval` | — | **NOT IMPLEMENTED** | |
| `json` | `Utf8` | **Implemented** | Selected as `::text`: Postgres' own rendering, byte for byte, on both paths |
| `jsonb` | `Utf8` | **Implemented** | Selected as `::text` (Postgres' normalized jsonb rendering; big numbers preserved exactly) |
| enum types | `Utf8` | **Implemented** | `::text` label cast; not `Dictionary(Int32, Utf8)` |
| `text[]` | `List(Utf8)` | **Implemented** | 1-D (or empty) arrays; NULL elements kept |
| other `T[]` | — | **NOT IMPLEMENTED** | |
| `money` | — | **NOT IMPLEMENTED** | No `cast_to` config |
| range, `hstore`, `tsvector`, geometry | — | **NOT IMPLEMENTED** | No `cast_to` config |
| `oid`, `xid`, `cid` | — | **NOT IMPLEMENTED** | |

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
extraction jobs (`status = 'PAID'`), and `histogram_bounds` handles range predicates on
caller-provided filter columns.

`EXPLAIN` is the decisive input: its plan node type tells us whether the predicate produces an
`Index Scan`, `Bitmap Heap Scan`, or `Seq Scan`, and its cost estimate feeds directly into the
`max_source_cost` budget from [pushdown §4](../pushdown.md#4-the-cost-model). Never
`EXPLAIN ANALYZE` — that runs the query.

Keyset partition bounds currently come from `MIN`/`MAX` on the raw partition column (index
probes), not from `histogram_bounds`.

---

## 5. Consistency notes

See §2.3: each partition reads one snapshot (cursor from `DECLARE`, or the single `COPY`);
partitions are not mutually consistent and no cross-partition snapshot guarantee is made. For
hot tables prefer `keyset` partitioning over `ctid` (concurrent updates move rows between
physical pages), and never rely on row order without an explicit `ORDER BY` — the source
returns rows in unspecified order. Timestamp-based incremental protocols (commit-skew
mitigation via `pg_stat_activity.xact_start`, safety lag, bounded windows) are deferred — see
[deferred/incremental-extraction.md](../deferred/incremental-extraction.md). The orchestrator
owns range selection; this connector only executes the scan it is given.

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

`lock_timeout` guarantees we never queue behind a DDL lock. Not applied today (recommended for
production roles, e.g. via `ALTER ROLE … SET`): `default_transaction_read_only = on` and
`jit = off` (JIT costs more than it saves on the short, high-row-count queries an extractor
issues, and adds plan-time variance that confuses the cost model).

### Required privileges

```sql
CREATE ROLE rel_extract LOGIN PASSWORD '…';
GRANT CONNECT ON DATABASE app TO rel_extract;
GRANT USAGE  ON SCHEMA public TO rel_extract;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO rel_extract;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO rel_extract;
```

---

## 7. Partitioning strategies

| Strategy | Predicate | Status | When |
| --- | --- | --- | --- |
| `ctid` | `ctid >= '(a,0)' AND ctid < '(b,0)'` | **Implemented** | Evenly sized physical page ranges; bounds from `relpages`. **Only valid within a single snapshot** (concurrent updates move rows between ranges). Exported snapshots not yet implemented. |
| `keyset` | first: `k < b1 OR k IS NULL`; middle: `k >= bᵢ AND k < bᵢ₊₁`; last: `k >= bₙ₋₁` | **Implemented** | Even split of `[MIN(k), MAX(k)]` (one index probe each on the raw column), bound math in `i128`. NULL keys go to the first partition exactly once; both ends are open (the first partition has no lower bound, the last no upper bound), so the partitions cover every key — including `i64::MIN`/`i64::MAX` and rows inserted outside `[MIN, MAX]` before a resumed run reuses its stored bounds. Not `histogram_bounds`-based: skewed keys give uneven partitions |
| `native` | one partition per child table of a declarative partitioned table | **NOT IMPLEMENTED** | Aligns with source's own pruning |
| `modulo` | `hashint8(pk) % n = i` | **NOT IMPLEMENTED** | Last resort; forces a full scan per partition |

`ctid` ranging requires an exported snapshot for correctness — outside one, concurrent updates move rows between ranges. Exported snapshots are not yet implemented.

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

The checkpoint becomes an LSN, which is monotonic in commit order, so the timestamp
commit-skew hazard (§3.1 of the deferred [incremental design](../deferred/incremental-extraction.md))
simply stops applying.

The operational hazard is severe enough to state up front: **an unconsumed replication slot retains
WAL indefinitely and will fill the primary's disk.** Any CDC implementation must ship with slot lag
monitoring (`pg_replication_slots.confirmed_flush_lsn` vs `pg_current_wal_lsn()`) and automatic slot
drop on prolonged job failure, before it is used against anything that matters. This is deferred
past Phase 3 for exactly that reason.
