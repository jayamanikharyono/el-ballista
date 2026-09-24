# PostgreSQL Connector

Crate: `rel-connector-postgres`. Built on `sqlx` (text protocol, streaming cursors) — we generate
SQL from the planner and need parameterized queries, not binary `COPY`.

Covers self-managed PostgreSQL, Cloud SQL for PostgreSQL, and AlloyDB. Read replicas and hot
standbys are supported.

> **Scope note:** watermark computation (§5 in earlier revisions) is deferred — see
> [deferred/incremental-extraction.md](../deferred/incremental-extraction.md). The connector
> exposes full/filtered scans, partitioning, and pushdown; range selection is caller-provided.

---

## 1. Why Postgres is the reference implementation

Postgres provides two capabilities that make it the reference:

- **Cursor-based portal scans** give a streaming path with bound parameters and no full
  result-set buffering.
- Deterministic collations (`C` / `POSIX`) make `Exact` fidelity comparisons possible.

---

## 2. Extraction path

### 2.1 Extended query protocol with a portal (the only path)

```rust
let stmt = client.prepare_typed(&sql, &param_types).await?;
let stream = client.query_raw(&stmt, params).await?;   // streams, does not buffer
```

`query_raw` returns a `RowStream` backed by a portal, so rows arrive incrementally rather than the
whole result set materializing client-side. The cursor-based path supports bound parameters and
composes with transaction and snapshot handling. All scan paths use this portal-based approach.

Result format is requested as **text** via the extended query protocol; sqlx handles decoding of
text-encoded values into Rust types. Binary format is not used (the `tokio-postgres` binary
decoder was removed when the COPY path was removed).

### 2.2 Binary COPY for bulk loads (`execution.use_copy`)

Full/keyset scans can instead run `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` when
`execution.use_copy` is true (default false). One server round-trip streams the whole
result in binary framing — no per-row parse/bind/portal overhead — decoded by
`connector::postgres::copy` into the same Arrow batches the cursor path produces
(differential-tested in `tests/pg_copy.rs`).

Limits, by construction of `COPY` (no bind parameters) and of the decoder set:
- only scans **without pushed filters** use COPY; anything with bound literals
  falls back to the cursor/`SELECT` path with a loud warn log (never silent);
- keyset bounds inline as `i64` literals (no quoting surface);
- types mirror the cursor decoder exactly (`ArrowTypeMapper` gates both); anything
  else is `UnsupportedType`, never silently wrong data.

---

## 3. Type mapping

| PostgreSQL | Arrow | Status | Notes |
| --- | --- | --- | --- |
| `bool` | `Boolean` | **Implemented** | 1 byte on the wire |
| `int2` / `int4` / `int8` | `Int16` / `Int32` / `Int64` | **Implemented** | Big-endian |
| `float4` / `float8` | `Float32` / `Float64` | **Implemented** | IEEE-754; `NaN` ordering differs from Arrow — see [pushdown §3.3](../pushdown.md#33-numeric-type-width-and-precision) |
| `numeric(p,s)`, p ≤ 38 | `Decimal128(p, s)` | **Implemented** | Text protocol; sqlx decodes to BigDecimal |
| `numeric`, unconstrained | `Decimal128(38, 10)` by default | **Implemented** | Overflow → error or null. `NaN`/`±Infinity` sign words map to null |
| `text`, `varchar`, `char`, `name` | `Utf8` | **Implemented** | Collation recorded in `ColumnMetadata`; drives `Exact` vs `Inexact` |
| `citext` | `Utf8` | **NOT IMPLEMENTED** | Comparisons would always be `Inexact` |
| `bytea` | `Binary` | **Implemented** | |
| `uuid` | `Utf8` | **Implemented** | Text protocol (not `FixedSizeBinary(16)`) |
| `date` | `Date32` | **Implemented** | Unix epoch days; text protocol handles epoch |
| `timestamp` | `Timestamp(Microsecond, None)` | **Implemented** | Text protocol; epoch handled by sqlx |
| `timestamptz` | `Timestamp(Microsecond, "UTC")` | **Implemented** | Text protocol; epoch handled by sqlx |
| `time` | — | **NOT IMPLEMENTED** | |
| `timetz` | — | **NOT IMPLEMENTED** | |
| `interval` | — | **NOT IMPLEMENTED** | |
| `json` | `Utf8` | **Implemented** | Plain UTF-8 |
| `jsonb` | `Utf8` | **Implemented** | Text protocol; no binary version byte |
| enum types | `Utf8` | **Implemented** | `::text` label cast; not `Dictionary(Int32, Utf8)` |
| `T[]` | — | **NOT IMPLEMENTED** | Only `text[]` supported via text protocol |
| `money` | — | **NOT IMPLEMENTED** | No `cast_to` config |
| range, `hstore`, `tsvector`, geometry | — | **NOT IMPLEMENTED** | No `cast_to` config |
| `oid`, `xid`, `cid` | — | **NOT IMPLEMENTED** | |

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
extraction jobs (`status = 'PAID'`), and `histogram_bounds` handles range predicates on
caller-provided filter columns.

`EXPLAIN` is the decisive input: its plan node type tells us whether the predicate produces an
`Index Scan`, `Bitmap Heap Scan`, or `Seq Scan`, and its cost estimate feeds directly into the
`max_source_cost` budget from [pushdown §4](../pushdown.md#4-the-cost-model). Never
`EXPLAIN ANALYZE` — that runs the query.

`histogram_bounds` is reused for a second purpose: deriving evenly sized keyset partition
boundaries without a `min`/`max` scan.

---

## 5. Consistency notes

The extraction layer makes **no snapshot guarantee**: concurrent writes during a scan may
appear or not depending on timing and isolation level. For hot tables prefer `keyset`
partitioning over `ctid` (concurrent updates move rows between physical pages), and never
rely on row order without an explicit `ORDER BY` — the source returns rows in unspecified
order. Timestamp-based incremental protocols (commit-skew mitigation via
`pg_stat_activity.xact_start`, safety lag, bounded windows) are deferred — see
[deferred/incremental-extraction.md](../deferred/incremental-extraction.md). The orchestrator
owns range selection; this connector only executes the scan it is given.

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
```

---

## 7. Partitioning strategies

| Strategy | Predicate | Status | When |
| --- | --- | --- | --- |
| `ctid` | `ctid >= '(a,0)' AND ctid < '(b,0)'` | **Implemented** | Evenly sized physical page ranges; bounds from `relpages`. **Only valid within a single snapshot** (concurrent updates move rows between ranges). Exported snapshots not yet implemented. |
| `keyset` | `pk >= $1 AND pk < $2` | **Implemented (default)** | Bounds from `histogram_bounds`, handles non-uniform key distribution |
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
