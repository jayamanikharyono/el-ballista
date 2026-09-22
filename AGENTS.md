# AGENTS.md — For AI Coding Agents

> **How to use this file:** You are an AI agent working on this repo. Every change must simultaneously pass three lenses. Do not reason from one perspective alone. If you satisfy Rust ownership but break Arrow semantics or DB correctness, the change is rejected.

## Project Overview

Experimental Rust-native **extraction and processing layer** — streaming path `Source DB → Arrow RecordBatch stream → DataFusion → (optional) Ballista`.

Focus: database extraction, Arrow as data contract, bounded-memory streaming, source-aware pushdown, full/filtered extraction, DataFusion/Ballista.

**Not** a data sink, warehouse, or storage system. Do not add destination/storage unless explicitly requested.

```
             ┌──────────────────┐
             │   Source DB      │  PostgreSQL, etc.
             └────────┬─────────┘
                      │ Extraction + Pushdown
                      ▼
             ┌──────────────────┐  Arrow Stream
             │   RecordBatch    │  (bounded memory)
             └────────┬─────────┘
                      ▼
             ┌──────────────────┐
             │    DataFusion    │  Query / Process
             └────────┬─────────┘
                Optional
                      ▼
             ┌──────────────────┐
             │     Ballista     │  Distributed Exec
             └──────────────────┘

Boundaries: Source connectors ⟷ Extraction ⟷ Arrow conversion ⟷ DataFusion ⟷ Ballista. No circular deps.
```

## Core Principle: Unified Three-Lens Evaluation

A change is **not done** unless it is correct from **all three** at once:

| Concern | Rust asks | DataFusion/Ballista asks | Database asks |
|---------|-----------|--------------------------|---------------|
| **Data moves** | Who owns it? Is cloning minimal? Is `Send/Sync` and drop correct? | Is schema/nullability/type preserved? Is streaming kept? (batch boundaries may differ between stages) | Is ordering deterministic? Can rows duplicate/skip under concurrent writes? |
| **Query is pushed** | Is error propagation explicit and credential-safe? | Is pushdown semantically `Exact`? Would `Inexact` require re-check? | Is index used? Does `NULL`/timestamp/collation match source semantics? |
| **Job tracks splits** | Is split completion recorded after *successful* processing? | Does schema stay consistent across batches? | Are completed splits skipped on retry? Does failure record the split without blocking others? |
| **Work is distributed** | Is there a global source-resource budget that workers share? Are resources `Send` and cleaned up on cancel? | Is partitioning deterministic? Is plan serializable (`POSTGRES_SCAN_MAGIC`)? Can task retry? | Does N workers keep source load flat (not `N×pool_max`)? |
| **Memory** | Avoid `collect()` of whole dataset; reuse buffers | Avoid materializing `Vec<RecordBatch>` | Avoid `LIMIT/OFFSET` on huge mutating tables; prefer keyset `WHERE (ts,id) > (...) ORDER BY` |

**Mental model before any edit:**

> Is this abstraction required by current architecture, or am I designing for a hypothetical future? — Prefer the former.
> Do I have evidence this is a bottleneck? — If not, prioritize correctness/simplicity.

## 1. Rust — Idiomatic, Safe, Maintainable

**Prefer:** explicit ownership/borrowing, small focused types, strong types, `RAII`, composition over inheritance-like abstractions, explicit lifetimes only when needed.

**Avoid in production/library paths:**
- `unwrap()` / `expect()` (except impossible invariant), `clone()` to please borrow checker, global mutable state, hidden side effects, large generics without use case.
- If ownership gets complex, **reconsider API/dataflow first** before adding `Arc/Mutex` or cloning.

**Rust Best Practices (enforce in this repo):**
- **Types:** newtype for IDs (`JobId(String)` > raw `String`), `#[non_exhaustive]` for public enums that may grow, `From`/`TryFrom` for conversions not `as`.
- **Ownership:** take `impl Into<String>` / `&str` for constructors, `&self` vs `&mut self` vs `self` intentionally, return `impl Iterator` / `impl Stream` where zero-cost; prefer `Arc` for shared read-only config, `Arc<Mutex>` only for shared mutable state with documented `Send/Sync` (tune per workload — `Arc` vs `Arc<Mutex>` is a recommendation, not an invariant).
- **Error handling — part of public behavior:** use `thiserror` for library crates (typed `enum AppError` already in repo), `anyhow` only at binary edges. Always `#[source]` the cause, add `context("failed to …: {table}")`, match on error kind to distinguish `source/database` vs `transformation/execution`. **Never** leak credentials. **Never** turn failure into empty dataset — `extraction failure ≠ zero rows`.
- **Async/Concurrency:** `tokio` runtime only; every `async fn` that touches I/O must be cancel-safe, every `spawn` must have `JoinHandle` handled, every `select!` must have cancellation branch. Explicitly check: connection limits, backpressure (`channel bound`), task cancellation, drop/cleanup (`Drop` impl for pools), ordering, memory growth, error propagation. Do not add `rayon`/`spawn_blocking` because it looks faster.
- **Resource lifecycle:** create late, reuse when safe, release deterministically (`drop` order matters for pools). Verify init/reuse/error/drop/concurrent access for pools/clients/streams. No `static mut`.
- **API surface:** keep small, intent-revealing (`extract`/`collect`/`run`), `#[must_use]` on builders, `Rustdoc` with `# Examples` on every `pub fn`. On API change: check all callers + examples + tests + docs.
- **Quality:** `cargo fmt` + `clippy --all-targets --all-features -- -D warnings` must pass. No `#[allow(clippy::…)]` without justification. Tests use `tokio::test`, `proptest` for fuzzing parsers. Unsafe only if `// SAFETY:` comment proves invariant.
- **Performance:** correctness first. For hot paths: avoid materialization/copies, prefer streaming, reuse `RecordBatch` buffers (`MutableArrayData`/`ArrayBuilder`), be mindful of `RecordBatch` allocation (`row_count` vs `allocated`). Optimize only with evidence (bench/profile/`EXPLAIN`/`perf`).

**Rust × Data Pipeline — Engineering Best Practices:**
- **Direct Arrow construction:** `sqlx Row → ArrayBuilder → RecordBatch → DataFusion` with no intermediate `Vec<Struct>` (not literally zero-copy). Use `ArrayRef` + `Arc` sharing; slice (`batch.slice(offset,len)`) not `clone`. Verify `batch.get_array_memory_size()` stays bounded per `batch_size`.
- **Bounded channels:** every pipeline stage is `bounded(m)` (e.g. `tokio::sync::mpsc::channel(8)` as a starting recommendation, tune per workload) with explicit backpressure. Never `unbounded()`. Source `FETCH` size and DataFusion `batch_size` are separate controls — prefer alignment as a starting point but don't require an invariant (e.g. `batch_size` is a recommendation, `Arc` vs `Arc<Mutex>` depends on sharing).
- **Schema as code:** never hardcode `Field` list twice. Single `build_schema(&TableMetadata) -> Schema` owned by connector, used by provider *and* plan. Test: `empty result still has correct schema` + `schema evolution` (add nullable column must not break old batches).
- **Row vs batch errors:** row-level decode error (`type mismatch`) → `Err` that fails the stream by default. Never silently skip failed batches — `Stream<Item=Result<RecordBatch>>` must surface `Err`; `SkipFailedBatch` only as an explicit, opt-in recovery policy. Use `Stream<Item=Result<RecordBatch>>` not `Stream<RecordBatch>` + panic.
- **Checkpoint = commit point:** pipeline `Stream` is fused with checkpoint `advance` only after downstream processing acknowledgement (this project is not a sink — not `writer.flush()`). Use explicit checkpoint state/commit semantics; `Drop` handles resource cleanup, not checkpoint rollback. Test crash-recovery: kill mid-batch, restart, assert no gap/duplicate with `(ts,id)` cursor.
- **Metrics per batch, not per row:** `tracing::info!(rows=, batch_bytes=, split=, partition=)` per `RecordBatch`. Never per-row. Use `metrics` crate (`counter!`, `histogram!`) for `extracted_rows`, `pushdown_hit`, `checkpoint_lag_seconds`.
- **Config vs code:** batch size, `target_partitions`, `pool_max` are `ExecutionConfig`/`SourceConfig`, not constants (recommendations, validate `batch_size > 0`). Builder takes `impl Into<ExecutionConfig>` and validates `batch_size > 0`.
- **Testing pipelines:** unit: `TestTableProvider` with in-memory `MemTable` + `assert_batches_eq!` (lightweight, no hostile fixture needed); integration: `testcontainers-postgres` + `pg_dump` snapshot; property: `proptest` generates `Predicate` → assert `Exact` pushdown still `collect() == scan_without_pushdown.collect()`.
- **Dependencies for pipelines:** `arrow` + `datafusion` + `tokio` cover 90%. Do not add `polars`/`rayon`/`serde_json` for pipeline path unless it replaces a handwritten `RecordBatch` loop.
- **Capability boundaries:** DB-specific semantics (collation, `NULL` ordering, timestamp precision) belong in connectors. Generic extraction/DataFusion code must not contain scattered `if postgres {}` branches.
- **Isolation semantics:** document transaction/isolation assumptions for extraction under concurrent writes (e.g. read-committed vs repeatable-read, snapshot `SET TRANSACTION` if used, or explicit "no snapshot guarantee" if not).

## 2. Arrow / Streaming / Schema — Data Contract

Arrow `RecordBatch` is the contract. Preserve: schema, types, nullability, column order, counts, batch boundaries, streaming behavior. Avoid `DB → structs → Vec → JSON → Arrow`.

**Streaming first — bounded memory:**
`Source → batch → process → batch …`  Ask: *What is max source data resident here?* If "entire dataset", find streaming alternative. `collect()` needs justification.

**Schema discipline:** verify DB type→Arrow mapping, nullable, timestamp precision/timezone, numeric precision, string/binary, empty result still exposes correct schema, consistency across batches.

## 3. DataFusion / Pushdown / Ballista

**DataFusion — use it, don't reimplement it:** `SessionContext` + `LogicalPlan` + `DataFrame` + `TableProvider`/`ExecutionPlan`. Do not duplicate: `filter`/`project`/`aggregate`/`join`/`window` is DataFusion's job. Check `datafusion::prelude::*` solves it first. Keep extraction useful without tight coupling to engine internals — wrap `TableProvider` not `SessionContext`.

**DataFusion Best Practices (54.1.0 in this repo):**
- **Construction:** `SessionContext::new()` + `ctx.register_table("orders", Arc::new(PostgresTableProvider::new(...)))` → `ctx.sql()` / `ctx.read_table()`. Do not build `LogicalPlan` by hand unless optimizer rule. Reuse `SessionContext` (holds optimizer, catalog); do not create per-query.
- **Schema & types:** DataFusion `DataType` must match Arrow `RecordBatch` exactly (including `nullable` + `Timestamp(nanos, Some("UTC"))` vs `None`). Use `Schema::new` + `Field::new` with explicit nullability; test `empty RecordBatch still has correct schema`. Prefer `arrow::datatypes` helpers, never `as`.
- **Memory & streaming:** `DataFrame::collect().await` materializes — **avoid** on large scans. Prefer `DataFrame::execute_stream().await` → `SendableRecordBatchStream` → `while let Some(batch)` . `RecordBatch` reuse via `RecordBatch::new` + `ArrayRef` sharing, not `clone`. Watch `batch.get_array_memory_size()` vs `num_rows`. Set `SessionConfig::with_target_partitions` / `with_batch_size` explicitly if changing.
- **Pushdown — correctness over performance:** only push `projection / filter` (caller-provided predicates) when source semantics == DataFusion semantics. `DataFusion filter updated_at > $lo` → `WHERE updated_at > $lo` only if null/type/order/duplicate semantics identical. Use DataFusion's `TableProviderFilterPushDown::Exact / Inexact / Unsupported` correctly: `Exact` if provider guarantees correctness (DF drops filter), `Inexact` if provider filters but DF must re-check. Never push `ORDER BY`/`LIMIT` unless source guarantees stable sort + deterministic limit.
- **Logical/Physical:** `TableProvider::scan` returns `Arc<dyn ExecutionPlan>` (your `PostgresScanExec`). `ExecutionPlan::execute` must be `Send + Sync` and produce `SendableRecordBatchStream` with correct `Partitioning` + `EquivalenceProperties`. Test with `datafusion::assert_batches_eq!` + `datafusion::physical_plan::test::TestContext`.
- **SQL & Expr:** prefer `col("updated_at").gt(lit(checkpoint))` builder over raw `Expr::Column` + `Operator`. If parsing SQL, go through `SessionContext::sql` — do not hand-roll parser. Handle `ScalarValue::Utf8` null vs `""` distinctly.
- **UDFs/Aggregates:** register via `ctx.register_udf` / `register_udaf` only if extraction needs custom logic; otherwise keep in source.
- **Quality:** enable `datafusion` feature `backtrace` in dev to surface plan errors; log `LogicalPlan::display_indent()` for debugging, never log `RecordBatch` data.

**Ballista — distributed, optional:**
`Source extraction → Arrow/DataFusion → Ballista`. `BallistaContext` wraps `SessionContext`. Consider partitioning, network/serde, scheduling, source load, locality. Keep Ballista coupling out of engine-agnostic code. Assume tasks can retry/duplicate — design for idempotence or document actual guarantees. Never claim `exactly-once` unless proven. `Ballista` and `DataFusion` versions must be compatible and tested together (often matching numbers like 54.1.0 here) — bump together and verify Arrow compat, don't assume numeric equality is the invariant.

## 4. Database — Correctness First

Assume table mutates during extraction (concurrent writes, tx isolation, deletes, clock skew, non-monotonic `updated_at`).

**Filtered:** caller-provided predicates or ranges (a time range for incremental-style jobs, a historical range for backfills). Watermark management, incremental state, and backfill orchestration live in the orchestrator, not this layer. For range determinism, prefer keyset predicates with deterministic `ORDER BY`.

**Checkpoint safety:**
```
Read boundary → Extract → Successfully process → Advance checkpoint
```
Failed extraction **must not** advance checkpoint (no data loss on retry). Never advance before success.

**Ordering:** without `ORDER BY`, order is unspecified — encode explicitly if needed for pagination/cursor/compound checkpoint/batch boundaries.

**Pagination:** avoid `LIMIT/OFFSET` at scale; prefer keyset/cursor `WHERE (ts,id) > (:ts,:id) ORDER BY … LIMIT`. Choice depends on index, size, mutation rate, planner.

**Performance & load:** evaluate every source query for index/selectivity/planner/full scan/locking/IO/network/connection pressure. Be a good citizen: no excessive connections/queries/scans/large batches. Parallelism first asks: can source absorb it?

**Transactions/Consistency:** be explicit — consistent snapshot? repeatable-read? eventual? Do not claim `consistent/snapshot/exactly-once/lossless` unless implemented.

## 5. Unified Checklists — Use Before Every Change

### Before any code change
- [ ] Inspected relevant module + existing similar impl
- [ ] Understood ownership/lifecycle and Arrow/DF/Ballista integration
- [ ] Identified which of 3 perspectives are affected (often all 3)

### Before changing SQL (merge DB + Rust + DF)
- [ ] Ordering deterministic (`ORDER BY` if required)?
- [ ] Index-friendly / selectivity / planner not full-scanning?
- [ ] Handles concurrent writes, `NULL`, duplicate/skip, failed extraction → failed split recorded, completed splits untouched?
- [ ] Pushdown `Exact`? Null/type/timestamp/ordering/duplicate semantics identical to DF?
- [ ] Checkpoint advancement safe? Pagination is keyset, not `OFFSET` at scale?
- [ ] Rust: error preserves cause, no credential leak, no `unwrap` hiding failure as empty set?

### Before changing Arrow / DataFusion (merge Rust + DF + DB)
- [ ] Schema preserved (types, nullability, timezone, precision)?
- [ ] Streaming preserved (no `collect()` of whole dataset, memory bounded, reuse buffers)?
- [ ] Pushdown semantically equivalent and budgeted (source not overloaded)?
- [ ] Schema/types/values/streaming correct? (batch sizes may differ between stages) Column ordering / record counts correct?
- [ ] Empty result still yields correct schema?
- [ ] Source semantics (null/timestamp) unchanged?
- [ ] No scattered `if postgres {}` in generic code? Is capability boundary respected?

### Before changing Ballista / distributed
- [ ] Partitioning deterministic and covers all rows without overlap/duplicate?
- [ ] What crosses network (plan vs data) and its serialized size?
- [ ] Task is retriable/idempotent? Handles duplicate execution/partial progress?
- [ ] Source load flat (shared global budget, not `N×pool_max`)? e.g. `pool_max / workers` is one way, but require a shared budget in general.
- [ ] Benefit actually measured vs single-node overhead?
- [ ] Rust: pool/client ownership clear, deterministic cleanup, `Send/Sync` correct?
- [ ] Isolation semantics documented (read-committed vs snapshot, or explicit "no snapshot guarantee")?

### Before introducing abstraction / optimizing
- Abstraction: *Is it required now, not hypothetical?* → former only.
- Optimization: *Do I have evidence of bottleneck (bench/profile)?* → otherwise keep it correct/simple.

## 6. Cross-Cutting Rules

**Memory & data movement:** `Source → batch → process → …` Keep max resident bounded. Distinguish movement vs representation vs processing vs storage — this project provides extraction/processing, **not** persistent sink.

**Dependencies:** check existing crate solves it, DF/Ballista/Arrow/sqlx version coupling, compile time/binary size, maintenance. Do not bump major DF/Ballista/Arrow casually — verify compilation, tests, examples, Arrow compat, DF/Ballista APIs, runtime.

**Observability:** log source type, operation, query context, batch size, rows, duration, partition/task — **never** passwords, connection strings, tokens, row data. Prefer structured context.

**Documentation:** describe what code *actually* does. Do not claim `exactly-once / zero-copy / snapshot / unlimited scale / production-ready` unless proven. Mark experimental as experimental.

**Change discipline:** smallest fix that solves problem → preserve behavior unless intentional → add/update tests → update docs if observable → no unrelated refactors.

**Benchmark comparability (equal-spec rule):** a benchmark number is only quotable when every engine ran the same spec — same data (`SCALE_ROWS`, same seed), same container budget (default: `--cpuset-cpus 0-3` + `--memory 4g`, shared across the distributed deployment), same scan fan-out derivation (default `ceil(rows/64000)` both sides), same batch knob (explicit `--batch-size` both sides, or label the run "tool defaults"), sequential runs, best-of-`REPEAT`. Equality is enforced ONLY at the container boundary: inside, runtimes run unrestricted (Spark `local[*]`, DataFusion defaults, Ballista visible-CPU slots) — never cap one side from the inside (`--spark-cores`, `BENCH_CONCURRENT_TASKS` are diagnostics only). `run.sh` prints this spec card on every run and bakes it into `results/summary.md`; a missing item means the numbers are smoke, not headlines. Never compare `output_bytes` across engines (different Parquet writers); the gate compares row *sets*. Never claim kernel-equality for `elapsed_ms` (Spark's timed section includes a read-back count, Rust's includes schema discovery) — report system-vs-system with the spec attached.

**Postgres connector modularization (MANDATORY):** all PostgreSQL-specific code lives under `src/connector/postgres/` — nothing Postgres-specific outside it. This keeps the connector self-contained and the rest of the crate connector-agnostic.
- New Postgres code (SQL building, cursors, type mapping, pushdown dialect/EXPLAIN, Ballista distributed execution, the extraction pipeline) goes under `src/connector/postgres/` — never at the crate root or in a sibling top-level module.
- Callers use the connector entry point, not its internals: `connector::postgres::PostgresConnector::from_config(cfg).extract()`, then `.standalone()` or `.distributed()`, finishing with `.collect()` (Arrow batches) or `.run()` (operational job). Do not reach into `pipeline`, `distributed`, `engine`, or `extractor` from outside the connector.
- Standalone vs distributed is one builder-selected path, not two parallel APIs. `.distributed()` defaults to the config's `distributed.scheduler_url`, or `DEFAULT_SCHEDULER_URL` (`http://localhost:50050`) when empty; `.scheduler(url)` / `.in_process()` / `.workers(n)` override it.
- `src/lib.rs` re-exports `distributed`, `engine`, `pipeline`, `pushdown` from `connector::postgres` as TRANSITIONAL shims so older paths still resolve. Do not write new code against these crate-root aliases; prefer `connector::postgres::*`. Remove the shims once all callers use `PostgresConnector`.
- The generic infrastructure now under `connector/postgres/pushdown` (the `SqlDialect` trait, `Predicate`, cost model) is connector-agnostic by design; when a second backend is added, lift it back out to a shared module and keep only the Postgres dialect + EXPLAIN under the connector.

## 7. Testing & Live DB

Run when applicable:
```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

**Rule: Every integration test must declare its oracle.** An oracle is the independent source of `expected` you compare `actual` against. No oracle = no correctness proof.

| Oracle type | `expected` comes from | Use for |
|---|---|---|
| **Trivial** | Hand-built `RecordBatch` via `StringBuilder`/`TimestampMicrosecondBuilder` | Schema, nullability, type mapping, empty-batch |
| **Reference** | Direct `sqlx::query("SELECT COUNT(*) …")` against Postgres | Row count / checksum independent of Arrow decode |
| **Differential** | Same logical query *with* pushdown vs *without* (filter in Arrow) | Pushdown `Exact`/`Inexact` correctness — the gold standard for this repo |
| **Metamorphic** | Same data via `collect()` vs `execute_stream()` vs 3 Ballista workers | Batch boundary / partitioning / distributed determinism |

Example differential oracle for pushdown:
```rust
let actual = extract_with_pushdown("status='PAID'").await;
let expected = extract_without_pushdown().await.filter_in_arrow(|r| r.status=="PAID");
assert_batches_eq!(actual, expected);
```

**Test matrix (each row must name its oracle):**

- **Extraction:** empty, single row, multiple batches, large, NULL, type conversion, DB/connection/query failure, stream termination, batch boundaries — *oracle: trivial + reference count*
- **Filtered:** full vs filtered agreement, empty filter keeps schema, duplicate timestamps all returned — *oracle: differential (pushed filter vs direct SQL) + reference count*
- **Splits:** per-partition completion, failed split retry, second run skips completed with identical row count — *oracle: split-checkpoint read + reference query*
- **DataFusion:** schema preservation, projection/predicate pushdown, empty/multiple batches, counts — *oracle: differential (pushed vs local)*
- **Distributed:** multiple partitions, task failure/retry/serialization/network/duplicate execution — *oracle: metamorphic (1 worker vs 3 workers checksum)*

**Hostile fixture (for DB/integration tests, not every unit test):** DB/integration tests use `tests/data/hostile.sql` — null in every nullable col, `""` vs `NULL`, `MIN/MAX` ints, `1.0` vs `1.00` decimals, `1970-01-01`/`2038-01-19`/`9999-12-31` timestamps, `bytea 0x00/0xFF`, `text[]` with null elements, 5-row `ts` tie. Loaded via `testcontainers-postgres` into isolated `CREATE SCHEMA test_{pid}_{thread} + DROP CASCADE`. Never `TRUNCATE` shared DB. Keep pure unit DataFusion tests lightweight — use `MemTable` + `assert_batches_eq!` without the hostile fixture unless they specifically test DB semantics.

- **Live DB (preferred for SQL/type/tx/index behavior):** Postgres is primary. Do not put Postgres-specific logic in generic abstraction unless that abstraction *is* Postgres.

## 8. Definition of Done

A change is **done** when: implementation correct **and** API understandable **and** memory bounded **and** DB semantics (ordering/pagination/tx) understood **and** Arrow/DF semantics preserved **and** distributed implications considered **and** relevant tests pass **and** `fmt`/`clippy` pass **and** docs accurately reflect reality **and** no unsupported guarantees added.

Preferred implementation = clearest correctness + ownership + data semantics + operational characteristics, not most abstractions.
