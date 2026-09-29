# Connector SPI

A connector is not "run SQL, return rows". It is the component that tells the planner **what the
source can do well**, produces Arrow batches, and knows the specific ways its dialect can return
a different answer than Arrow would.

Detailed implementation: **[PostgreSQL](postgres.md)**.

---

## 1. The actual SPI (what exists)

The implementation does not have a separate `Source` trait hierarchy. Instead, each connector
implements DataFusion's `TableProvider` and `ExecutionPlan` directly, plus these supporting
abstractions (see `src/connector/mod.rs` for the contract):

```rust
/// Rendering conventions for the pushdown IR (`src/pushdown/dialect.rs`; implemented per
/// backend: `connector::postgres::dialect::PostgresDialect`, `connector::mysql::MysqlDialect`).
pub trait SqlDialect: Send + Sync {
    fn quote_ident(&self, name: &str) -> String;
    fn placeholder(&self, param_index: usize) -> String;   // "$1" for pg, "?" for MySQL
    fn cast_type_name(&self, to: CastType) -> &'static str;
    fn collation_name(&self, collation: Collation) -> &'static str; // Binary -> "\"C\"" on pg
}
```

Fidelity (`Exact` / `Inexact` / not pushable) is **not** a dialect method: it is decided once,
during translation (`pushdown::translate`), from the engine-neutral `pushdown::ColumnKind` each
connector assigns to its columns ([pushdown](../pushdown.md)). Literals never reach SQL text:
`Predicate::render_to` hands them to a backend `SqlSink` (Postgres: `PgParamSink`) that binds them.

- `pushdown::stats::TableStatsSource` — table/column statistics and index metadata for the cost
  model (Postgres implementation: `connector::postgres::stats`, over `pg_class`/`pg_stats`/`pg_index`).
- `SourceDescriptor` — identifies one budgeted source pool per process without carrying a
  password (only the env-var name).
- `checkpoint::CheckpointStore` — split-execution state, keyed by `JobId`:

```rust
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn begin(&self, key: &JobId, plan: &SplitPlan) -> Result<JobCheckpoint, CheckpointError>;
    async fn mark_running(&self, key: &JobId, split_id: &str) -> Result<(), CheckpointError>;
    async fn mark_completed(&self, key: &JobId, split_id: &str, rows_extracted: u64) -> Result<(), CheckpointError>;
    async fn mark_failed(&self, key: &JobId, split_id: &str, err: &str) -> Result<(), CheckpointError>;
    async fn read(&self, key: &JobId) -> Result<Option<JobCheckpoint>, CheckpointError>;
    async fn reset(&self, key: &JobId) -> Result<(), CheckpointError>;
}
```

`begin` binds the checkpoint to the plan (a fingerprint of table, projection, resolved filters,
strategy, partitions and partition column, plus the stored split bounds): resuming with a
different plan is a `CheckpointError::PlanMismatch`.

The PostgreSQL connector implements `TableProvider` (`supports_filters_pushdown` + `scan`) and
`ExecutionPlan` (`PostgresExecutionPlan`, streaming `RecordBatch`es from a `DECLARE … CURSOR` /
`FETCH` loop or a binary `COPY`). A `SourceConnector` trait sketch exists in
`src/connector/mod.rs` but nothing uses it yet ([connector-abstraction](../connector-abstraction.md)).

---

## 2. Capability comparison

| Capability | PostgreSQL | MySQL (prototype) |
| --- | --- | --- |
| Parallel scan splitting | keyset, `ctid` physical ranges | serial only (keyset planned) |
| Filter pushdown | cost-based (`always`/`never`/`cost_based`/`strict`/`hinted`) | not implemented |
| Column histograms | `pg_stats` (always maintained) | opt-in `COLUMN_STATISTICS` |
| Log-based CDC | out of scope | out of scope |

---

## 3. Decoding to Arrow

The PostgreSQL connector reads the **binary** wire format on both scan paths: sqlx requests
binary results for every extended-protocol statement (the cursor `FETCH`es included), and
`COPY (SELECT …) TO STDOUT (FORMAT BINARY)` frames the same per-type binary encodings. One
decoder per column, chosen once per scan from the mapped Arrow type, appends each value's bytes
straight into that column's Arrow builder (`connector::postgres::row_adapter`):

```
cursor: FETCH (binary) ──► PgRow raw value bytes ─┐
                                                    ├─► per-column decoder ─► ArrayBuilder ─► RecordBatch ─► stream
COPY:   binary COPY frames ──► field bytes ────────┘
```

On the cursor path each row still passes through sqlx's `PgRow` (a driver-side row buffer), so
"no intermediate row struct" holds for the Arrow side, not the driver side. `json`, `jsonb`,
`uuid` and enum columns are selected as `::text` and arrive as Postgres' own text rendering.
A batch is flushed when it reaches `batch_size` rows **or** `max_batch_bytes` bytes — never
only at the end of the result set — which is what makes backpressure work end to end (see
[architecture](../architecture.md#5-execution-and-memory)).

The MySQL prototype selects raw columns and decodes them into typed builders
(`connector::mysql::row_adapter`); see [mysql.md](mysql.md).

---

## 4. Type mapping rules

Three rules apply to both dialects:

1. **Resolve types from catalog metadata, not from result-set metadata.** The wire protocol tells
   you "this is text"; `information_schema` tells you it is a `TEXT` in `utf8` with a
   case-insensitive collation. The planner needs the second kind of information (see
   [pushdown §3](../pushdown.md#3-where-pushdown-silently-changes-results)).

2. **Never widen silently to `Utf8`.** Falling back to a string for anything unrecognized produces
   a pipeline that "works" and a warehouse full of strings that nobody can aggregate. On Postgres
   an unmapped type is an `UnsupportedType` error before the scan starts. A per-column `cast_to`
   escape hatch in the job spec is not implemented — and the MySQL prototype currently
   violates this rule (types outside its mapping table fall back to `Utf8`; see
   [mysql.md](mysql.md)), which must be fixed when the prototype is promoted.

3. **Unrepresentable values are errors, never silent nulls or truncation.** Postgres
   `timestamp`/`date` `±infinity`, numeric `NaN`/`±Infinity`, and numeric digits beyond the
   column's `Decimal128` precision or scale (unconstrained `numeric` maps to
   `Decimal128(38, 10)`) fail the scan with `ExtractorError::UnsupportedValue` naming the
   column, on both the cursor and the COPY path. A configurable
   `on_unrepresentable = "error" | "null"` (with a `el_ballista_null_coerced_total` counter) is not
   implemented.

Per-dialect mapping tables live in each connector document.

---

## 5. Parallel scan

Splitting a scan across connections multiplies throughput and multiplies the load you place on a
production database. It is off by default and opt-in per job.

| Strategy | Mechanism | Notes |
| --- | --- | --- |
| `keyset` | `pk >= :a AND pk < :b`, bounds split evenly over `[MIN(pk), MAX(pk)]` (first partition also takes `pk IS NULL`, last is open-ended) | Works everywhere; skewed if the key is not uniform. Histogram-derived bounds are not implemented. Postgres only (the MySQL prototype is serial). |
| `ctid` | Postgres `ctid` ranges over `relpages` | Fastest and evenly sized; only exact within one snapshot, and exported snapshots are not implemented. Postgres only. |
| `modulo` | `hash(pk) % n = i` | Not implemented |
| `native` | one partition per declarative table partition | Not implemented |

The connection pool caps concurrency regardless of the requested partition count, and the pool is
sized deliberately small for production sources. The failure mode we are avoiding is an extraction
job that consumes every available connection slot on a primary during business hours.

---

## 6. Connection hygiene

Non-negotiable for every connector, because an extraction tool must never be the cause of a
production incident:

- **`statement_timeout` is always set.** A query that has run for twenty minutes is a bug, not a
  slow query.
- **Idle-in-transaction timeout is always set** (Postgres). A forgotten open transaction blocks
  Postgres autovacuum and pins undo log on other engines.
- **Connections and queries are identifiable.** Postgres connections set `application_name`
  (`source.application_name`, default `el-ballista`). Every scan query starts with a SQL
  comment tag (`connector::query_tag`), e.g.
  `/* el-ballista query_id=q_… pipeline=el-ballista run_id=r_… strategy=full+pushdown partition=3/8 */`:
  `pipeline` is the `application_name`, `run_id` is shared by every query of one run, and
  `query_id` is fresh per query. A DBA looking at `pg_stat_activity` or the server log can
  attribute every scan. The job id itself is not in either; give each job its
  own `application_name` to tell jobs apart.
- **Session settings are explicit.** Postgres pools set `TIME ZONE 'UTC'`, `statement_timeout`,
  `idle_in_transaction_session_timeout = '60s'` and `lock_timeout = '5s'` on every connection.
  Read-only transactions are not set (the scans only `SELECT`/`COPY TO`).
- **Failures are recorded per split, retry is orchestrator-driven.** A failed split is marked
  `Failed` with its error without touching completed splits; re-running the job skips
  completed splits and retries the rest. There is no in-layer retry-with-backoff and no
  pipeline-level retry policy — that lives in the orchestrator.

---

## 7. Adding a connector later

A non-SQL source (a key-value store, an object store) would declare different capabilities:
pushdown limited to what the source can evaluate exactly, its own split mechanism, and possibly
no snapshot at all. None has been designed, and the SPI is only proven once two genuinely
different implementations sit behind it.
