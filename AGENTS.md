# AGENTS.md — For AI Coding Agents

> **How to use this file:** every change must pass three lenses at once: Rust, Arrow/DataFusion/Ballista, and the database. Do not reason from one perspective alone. A change that satisfies Rust ownership but breaks Arrow semantics or DB correctness is rejected.

## Project Overview

El Ballista is an experimental Rust **extraction layer**: `Source DB → Arrow RecordBatch stream → DataFusion`, run in one process (plain DataFusion) or on a Ballista cluster. Commands below run the `el-ballista` binary (`cargo run --release --bin el-ballista -- …`).

Focus: database extraction, Arrow as the data contract, bounded-memory streaming, source-aware pushdown, full/filtered extraction, split checkpoints.

Out of scope: sinks (the layer hands out Arrow batches; there is no destination or storage), watermark / incremental state, CDC. Do not add a sink or watermark state unless explicitly requested; the incremental design is parked in `docs/deferred/incremental-extraction.md`.

```
Source DB (PostgreSQL; MySQL prototype)
   │  extraction + pushdown
   ▼
Arrow RecordBatch stream (bounded memory)
   ▼
DataFusion (standalone)   or   Ballista scheduler + workers (distributed)

Boundaries: source connectors ⟷ extraction ⟷ Arrow conversion ⟷ DataFusion ⟷ Ballista. No circular deps.
```

## Core Principle: Unified Three-Lens Evaluation

A change is **not done** unless it is correct from **all three** at once:

| Concern | Rust asks | DataFusion/Ballista asks | Database asks |
|---------|-----------|--------------------------|---------------|
| **Data moves** | Who owns it? Is cloning minimal? Is `Send/Sync` and drop correct? | Is schema/nullability/type preserved? Is streaming kept? (batch boundaries may differ between stages) | Is ordering deterministic? Can rows duplicate/skip under concurrent writes? |
| **Query is pushed** | Is error propagation explicit and credential-safe? | Is pushdown semantically `Exact`? Would `Inexact` require re-check? | Is an index used? Does `NULL`/timestamp/collation match source semantics? |
| **Job tracks splits** | Is split completion recorded after *successful* processing? | Does schema stay consistent across batches? | Are completed splits skipped on retry? Does failure record the split without blocking others? |
| **Work is distributed** | Is there a global source-resource budget that workers share? Are resources `Send` and cleaned up on cancel? | Is partitioning deterministic? Is the plan serializable (`POSTGRES_SCAN_MAGIC`)? Can the task retry? | Do N workers keep source load flat (not `N×pool_max`)? |
| **Memory** | Avoid `collect()` of the whole dataset; reuse buffers | Avoid materializing `Vec<RecordBatch>` | Avoid `LIMIT/OFFSET` on huge mutating tables; prefer keyset `WHERE (ts,id) > (...) ORDER BY` |

**Before any edit ask:** is this abstraction required by the current architecture, or designed for a hypothetical future? Prefer the former. Is there evidence this is a bottleneck? If not, prioritize correctness and simplicity.

## 1. Rust

**Avoid in library paths:** `unwrap()` / `expect()` (except an impossible invariant), `clone()` to please the borrow checker, global mutable state / `static mut`, hidden side effects, large generics without a use case. If ownership gets complex, reconsider the API/dataflow before adding `Arc<Mutex>` or cloning. `unsafe` only with a `// SAFETY:` comment proving the invariant.

**Project conventions:**
- **Types:** newtypes for IDs (`JobId`), `#[non_exhaustive]` on public enums that may grow, `From`/`TryFrom` instead of `as`.
- **Errors are public behavior:** typed `thiserror` enums (`AppError`, `ExtractorError`); keep the cause with `#[source]` and name the table/column in the message. **Never** leak credentials. **Never** turn a failure into an empty dataset: **extraction failure ≠ zero rows**.
- **Async:** `tokio` only. I/O paths must be cancel-safe, every `spawn`'s `JoinHandle` handled, dropping a stream must stop the source query. Check connection limits, backpressure, cancellation, cleanup, memory growth. Do not add `rayon` / `spawn_blocking` because it looks faster.
- **Resource lifecycle:** create late, reuse when safe, release deterministically (drop order matters for pools; `Drop` guards like `CopyCancelGuard` and `JobLock` release resources).
- **API surface:** small and intent-revealing (`stream` / `collect` / `run` / `run_with`), rustdoc with `# Examples` on every `pub fn` (compiled by `cargo test --doc`). On API change: update all callers, examples, tests and docs.
- **Quality:** `cargo fmt` + `clippy --all-targets --all-features -- -D warnings` must pass; no `#[allow(clippy::…)]` without a justification.

**Rust × data pipeline:**
- **Direct Arrow construction:** binary rows → `ArrayBuilder` → `RecordBatch` with no intermediate `Vec<Struct>` (not literally zero-copy). Share `ArrayRef`s, slice rather than clone. Batch memory stays bounded by `batch_size` and the builder byte cap.
- **Bounded channels only** (e.g. the scan task's `mpsc::channel(2)`), never `unbounded()`. Source `FETCH` size and batch size are config, not constants.
- **Schema as code:** one `build_arrow_schema` per connector, used by the provider, the execution plan and the extractor. Never write a `Field` list twice. Empty results still carry the full schema.
- **Row vs batch errors:** a decode error fails the stream (`Stream<Item = Result<RecordBatch>>`). There is no skip-failed-batch policy; never skip silently.
- **Checkpoint = commit point:** a split is marked complete only after the consumer acknowledges it (`run_with`); `Drop` cleans up resources, it does not roll back checkpoints.
- **Metrics per batch, not per row:** `src/telemetry.rs` records `el_ballista_extracted_rows`, `el_ballista_extracted_batches`, `el_ballista_batch_bytes`, `el_ballista_splits`, `el_ballista_pushdown_decisions` through the `metrics` facade. Never per row.
- **Config vs code:** `batch_size`, partitions, `pool_max` live in `ExecutionConfig` / `SourceConfig` / `ParallelScanConfig` and are validated (`batch_size > 0`).
- **Dependencies:** `arrow` + `datafusion` + `tokio` cover the pipeline. Do not add `polars` / `rayon` unless it replaces a hand-written `RecordBatch` loop.
- **Capability boundaries:** DB-specific semantics (collation, `NULL` ordering, timestamp precision) belong in the connector. No scattered `if postgres {}` in generic code.
- **Isolation semantics:** state the transaction/isolation assumption for extraction under concurrent writes (see the `connector::postgres::extractor` module docs); never imply a snapshot that is not taken.

## 2. Arrow / Streaming / Schema — Data Contract

Arrow `RecordBatch` is the contract. Preserve schema, types, nullability, column order, row counts and streaming behavior. Avoid `DB → structs → Vec → JSON → Arrow`.

**Streaming first:** ask *what is the max source data resident here?* If the answer is "the entire dataset", find a streaming alternative. `collect()` needs a justification.

**Schema discipline:** verify DB type → Arrow mapping, nullability, timestamp unit and timezone (Postgres timestamps map to `Timestamp(Microsecond, None)`, `timestamptz` to `Timestamp(Microsecond, Some("UTC"))`), numeric precision, string/binary, empty-result schema, consistency across batches.

## 3. DataFusion / Pushdown / Ballista

**Use DataFusion, don't reimplement it:** `SessionContext` + `TableProvider` / `ExecutionPlan` + `DataFrame`. Filter/project/aggregate/join/window are DataFusion's job. Wrap a `TableProvider`, not `SessionContext` internals.

**DataFusion (54.1.0):**
- **Construction:** register `PostgresTableProvider` (or `connector::postgres::register_table(&ctx, &config)`), then `ctx.sql()` / `ctx.table()`. Do not build `LogicalPlan`s by hand. Reuse the `SessionContext`.
- **Types:** the provider's schema must match the emitted batches exactly, nullability and timezone included.
- **Streaming:** `DataFrame::collect()` materializes; prefer `execute_stream()`. Set `target_partitions` / `batch_size` explicitly when changing them.
- **Pushdown — correctness over performance:** push a filter only when the source returns exactly what Arrow would (null / type / collation / ordering semantics). `Exact` = the provider guarantees the result and DataFusion drops the filter; `Inexact` = the source pre-filters and DataFusion re-checks. The provider decides the whole filter set (`supports_filters_pushdown` → `decide_all`). Never push `ORDER BY` / `LIMIT` without a stable sort.
- **Physical plan:** `TableProvider::scan` returns a `PostgresExecutionPlan`; its streams are `Send` and report correct partitioning.
- **Expr / SQL:** prefer `col(..).gt(lit(..))` builders; parse SQL only through `SessionContext::sql`. Keep `ScalarValue::Utf8` NULL and `""` distinct. Log `LogicalPlan::display_indent()` for debugging, never row data.

**Ballista:** `DistributedContext` (`connector::postgres::distributed`) holds a `SessionContext` connected to a remote scheduler. Keep Ballista out of engine-agnostic code. Assume tasks can retry or duplicate: design for idempotence or document the real guarantee. **Never claim exactly-once unless proven.** Bump DataFusion, Ballista and Arrow together and verify compatibility.

## 4. Database — Correctness First

Assume the table mutates during extraction (concurrent writes, deletes, non-monotonic `updated_at`).

- **Filtered extraction:** the caller passes predicates or ranges (e.g. a time window). Watermarks, incremental state and backfill orchestration belong to the orchestrator, not this layer.
- **Checkpoint safety:** `read boundary → extract → consumer succeeds → advance checkpoint`. A failed extraction never advances a checkpoint.
- **Ordering:** without `ORDER BY` the order is unspecified; encode it where pagination, cursors or checkpoints depend on it.
- **Pagination:** no `LIMIT/OFFSET` at scale; use keyset ranges with deterministic ordering.
- **Load:** evaluate every source query for index use, selectivity, full scans, locking and connection pressure. Parallelism first asks whether the source can absorb it.
- **Consistency:** do not claim `consistent / snapshot / exactly-once / lossless` unless implemented.

## 5. Checklists

### Before changing SQL
- [ ] Ordering deterministic where required? Index-friendly, not an accidental full scan?
- [ ] Concurrent writes, `NULL`, duplicates/skips handled? A failed extraction records the failed split and leaves completed splits untouched?
- [ ] Pushdown `Exact`? Null/type/timestamp/collation semantics identical to DataFusion?
- [ ] Checkpoint advancement safe? Pagination keyset, not `OFFSET`?
- [ ] Errors keep the cause, leak no credentials, never hide a failure as an empty set?

### Before changing Arrow / DataFusion
- [ ] Schema preserved (types, nullability, timezone, precision), empty result included?
- [ ] Streaming preserved (no whole-dataset `collect()`, memory bounded)?
- [ ] Column order and row counts correct? Source semantics unchanged?
- [ ] Capability boundary respected (no `if postgres {}` in generic code)?

### Before changing Ballista / distributed
- [ ] Partitioning deterministic, covering all rows without overlap?
- [ ] What crosses the network (plan vs data) and how big is it?
- [ ] Task retriable? Duplicate execution / partial progress handled?
- [ ] Source load flat (one shared budget, not `N×pool_max`)?
- [ ] Mode separation kept (§6 "Execution modes"): no in-process Ballista, no Ballista outside `connector/postgres/distributed/` + CLI, no fallback between modes, mode table updated?
- [ ] Benefit measured against single-node overhead?

### Before introducing abstraction / optimizing
- Abstraction: *is it required now, not hypothetical?* Only the former.
- Optimization: *is there evidence of a bottleneck (bench/profile/`EXPLAIN`)?* Otherwise keep it correct and simple.

## 6. Cross-Cutting Rules

**Dependencies:** check whether an existing crate solves it; mind DataFusion/Ballista/Arrow/sqlx version coupling. Do not bump DataFusion/Ballista/Arrow casually: verify build, tests, examples and APIs.

**Observability:** log source, operation, query context, batch size, rows, duration, partition/task. **Never** passwords, connection strings, tokens or row data.

**Documentation:** describe what the code actually does. Do not claim `exactly-once / zero-copy / snapshot / unlimited scale / production-ready` unless proven. Mark experimental as experimental.

**Change discipline:** smallest fix that solves the problem, behavior preserved unless intentional, tests added/updated, docs updated if observable, no unrelated refactors.

**Benchmarks:** a number is quotable only under the equal-spec rule. Read the method and "Equal-spec rule" sections of [`benchmark/README.md`](benchmark/README.md) before running, changing or quoting a benchmark.

**Execution modes — standalone vs distributed (MANDATORY):** exactly two modes, strictly separated. There is no third, hybrid or "in-process cluster" mode. The source database is the input to both modes, not a component of either; the table lists what each mode needs besides it.

| | **Standalone** | **Distributed** |
|---|---|---|
| Engine | DataFusion only — a plain `SessionContext`, no Ballista | DataFusion planned by the client, executed by Ballista |
| Entry point | `PostgresConnector::…extract().standalone()`, or `connector::postgres::register_table(&ctx, &config)` into your own `SessionContext` | `…extract().distributed()` (`.scheduler(url)`, `.workers(n)`), or `DistributedContext::remote(&config, url, workers)` + `register_source` |
| Processes | 1 (the caller's) | client + 1 `el-ballista scheduler` + `workers` × `el-ballista worker` |
| Required components | none beyond the calling process | a running `el-ballista scheduler` (push-based scheduling, the default); exactly `distributed.workers` running `el-ballista worker` processes — **this crate's binary**, since stock Ballista executors cannot decode the Postgres scan plans; network paths client → scheduler, scheduler ↔ workers, client and workers → source database; the `source.password_env` variable set in **every worker's** environment |
| Optional | — | the scheduler REST API (on in `el-ballista scheduler`): verifies the executor count against the budget (unreachable = warning, more executors than `workers` = error) and feeds the job watchdog (without it only `distributed.job_timeout_secs` can catch a hang) |
| Failure handling | a failed split is recorded; `run_with` re-runs it on the next call | job watchdog: a worker that stops heartbeating (`distributed.executor_timeout_secs`, default 30) or is dropped, or `job_timeout_secs`, marks the job hung; it is cancelled and re-run up to `distributed.max_retries` (default 2), then `DistributedJobAborted`. Workers heartbeat every `--heartbeat-secs` (5); the scheduler drops them after `--executor-timeout-secs` (30) |
| Source connections | the whole `pool_max` for the one process (planning included) | `pool_max / workers` per executor process, plus the client's planning pool |
| Parallelism | DataFusion runs the keyset partitions concurrently on one Tokio runtime (one thread per visible CPU); at most `execution.concurrent_partitions` (default `pool_max`) query the source at once | executor task slots (`el-ballista worker --concurrent-tasks`, default: visible CPUs); at most `pool_max / workers` scans per worker query the source |
| Checkpoint splits (`run_with`) | one per keyset partition | the whole distributed scan is one split |
| Benchmark label | `el-ballista-standalone` | `el-ballista-distributed` |

Rules:
- **Never reintroduce in-process Ballista** (`SessionContext::standalone*`, `new_standalone_scheduler*` / `new_standalone_executor*`, an `in_process()` builder) — not in the library, examples, benchmark or tests. It hard-codes pull scheduling (a 50 ms poll per empty task request) and a second Tokio runtime that splits the connection pool.
- **Ballista stays behind the boundary:** `ballista*` crates are used only in `src/connector/postgres/distributed/` and the CLI's `el-ballista scheduler` / `el-ballista worker`. Standalone code must not construct or import anything from Ballista.
- **No silent fallback:** `.distributed()` with no reachable cluster is an error. Never degrade to standalone, and never let standalone start a cluster.
- **A distributed job never hangs:** every distributed query runs under `distributed/watchdog.rs`. Ballista 54 never re-offers a lost executor's tasks, so do not rely on Ballista to recover a job, and never add a distributed terminal that bypasses the watchdog. Re-run only before the first delivered batch (a later re-run would duplicate rows); after that, a hang is an error. Keep `el-ballista worker --heartbeat-secs` well below both timeouts. The benchmark turns the client retry off (`max_retries: 0`) and retries whole attempts on a fresh cluster instead, so no number comes from a degraded cluster.
- **Tests match the mode:** distributed-path tests run on a real cluster (`tests/common/cluster.rs` starts `el-ballista scheduler` + `el-ballista worker` child processes); standalone tests use plain DataFusion. Do not test one mode through the other.
- **Changing either mode** (entry point, budget, parallelism, required components) means updating this table, the README's "Standalone or distributed" section and the benchmark README in the same change.

**Postgres connector modularization (MANDATORY):** all PostgreSQL-specific code lives under `src/connector/postgres/`, so the rest of the crate stays connector-agnostic.
- New Postgres code (SQL building, cursors, type mapping, pushdown dialect/EXPLAIN, distributed execution, the extraction pipeline) goes under `src/connector/postgres/`, never at the crate root or in a sibling top-level module.
- **What is extracted is data, never hand-written SQL:** table, filters and projection come from the job config (`table`, structured `filters`, `columns`) or the same fields on `JobConfig` — never from a caller-built SQL string (no `SELECT … WHERE` in examples, benchmark harnesses or extraction tools). SQL over *already extracted* data (`ExtractContext::sql`, DataFusion queries on the result) is processing and stays allowed.
- Callers use the connector entry point, not its internals: `connector::postgres::PostgresConnector::from_config(cfg)?.extract()`, then `.standalone()` or `.distributed()`, finishing with `.run_with(consumer)` (the checkpointed job: a split is marked completed only after the consumer returns `Ok`), `.stream()` (bounded-memory batches, no checkpoints), `.collect()` (materializes every batch) or `.run()` (count-only diagnostic, never touches checkpoints). Do not reach into `pipeline`, `distributed`, `engine` or `extractor` from outside the connector; there are no crate-root re-exports of them.
- Standalone vs distributed is one builder-selected path, not two parallel APIs. `.distributed()` defaults to the config's `distributed.scheduler_url`, or `DEFAULT_SCHEDULER_URL` (`http://localhost:50050`) when empty; `.scheduler(url)` / `.workers(n)` override it.
- `src/pushdown` holds only engine-agnostic pushdown infrastructure (the `SqlDialect` trait, `Predicate` IR, translation, policy, cost model). Postgres specifics (dialect/column kinds, parameter sink, EXPLAIN, statistics) live under `src/connector/postgres/`; a new backend adds its own dialect under its connector.

## 7. Testing & Live DB

See [`docs/testing-plan.md`](docs/testing-plan.md) for the suite, counts and CI steps. Run when applicable:
```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib --bins && cargo test --doc
docker compose -f tests/docker/compose.yaml up -d --wait   # Postgres 17 + MySQL 8, dvdrental
cargo test --tests -- --test-threads=1
```

Unit tests are pure logic next to the code, with hand-written expected values and no database. Integration tests never skip: an unreachable database is a failure.

**Rule: every integration test declares its oracle** — the independent source of `expected` that `actual` is compared against. No oracle, no correctness proof.

| Oracle type | `expected` comes from | Use for |
|---|---|---|
| **Trivial** | Hand-written values or schema | Schema, nullability, type mapping, empty batch |
| **Reference** | Direct `sqlx` SQL against the source (`COUNT(*)`, `::text`, `extract(epoch …)`) | Row counts / values independent of Arrow decoding |
| **Differential** | The same query with pushdown policy `always` vs `never` (filter evaluated in Arrow) | Pushdown `Exact`/`Inexact` correctness — the gold standard for this repo |
| **Metamorphic** | The same data via `collect()` vs `stream()`, or 1 vs 3 Ballista workers | Batch boundaries, partitioning, distributed determinism |

A differential test compares row sets, e.g. the sorted ids from a context registered with `PushdownPolicy::Always` against one with `PushdownPolicy::Never` (`tests/pg_pushdown.rs`, randomized in `tests/pg_pushdown_prop.rs`), and checks that the plan actually pushed something so the comparison is not vacuous.

**Test matrix (each row names its oracle):**
- **Extraction:** empty, single row, multiple batches, NULL, type conversion, DB/query failure, stream termination, batch boundaries — *trivial + reference*
- **Filtered:** full vs filtered agreement, empty filter keeps schema, duplicate timestamps all returned — *differential + reference*
- **Splits:** per-partition completion, failed split retry, second run skips completed splits with identical rows — *split checkpoint + reference*
- **DataFusion:** schema preservation, projection/predicate pushdown, counts — *differential*
- **Distributed:** multiple partitions, worker loss, retry, serialization — *metamorphic (1 vs 3 workers) + reference*

**Hostile fixture:** DB tests use `tests/data/hostile.sql` (NULL in every nullable column, `''` vs NULL, INT MIN/MAX, `1.0` vs `1.00` decimals, 1970/2038/9999 timestamps, `bytea` `0x00`/`0xFF`, `text[]` with NULL elements, a 5-row timestamp tie). `TestDb` (`tests/common/postgres.rs`) loads it into a private schema `test_<pid>_<n>` on the compose Postgres and drops the schema afterwards. Never `TRUNCATE` a shared table.

Postgres is the primary live database for SQL/type/transaction/index behavior. Do not put Postgres-specific logic in a generic abstraction unless that abstraction *is* Postgres.

## 8. Definition of Done

A change is **done** when the implementation is correct, the API understandable, memory bounded, DB semantics (ordering/pagination/transactions) understood, Arrow/DataFusion semantics preserved, distributed implications considered, relevant tests pass, `fmt`/`clippy` pass, docs reflect reality, and no unsupported guarantee was added.

Preferred implementation = clearest correctness, ownership, data semantics and operational behavior — not the most abstractions.
