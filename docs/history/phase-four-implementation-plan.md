# Phase 4 Implementation Plan — Distributed Execution

> Archived planning document (September 2026). Not a description of the current code — see [../roadmap.md](../roadmap.md) and [../architecture.md](../architecture.md).

**Status**: ✓ IMPLEMENTED (compile-clean, all unit tests pass; runtime validation against a real
cluster pending — see [Verification](#verification))

This document records what was built for Phase 4 of [roadmap.md](../roadmap.md): running an
extraction job through a Ballista scheduler + workers, shipping our scan plans between processes,
and coordinating source connection pools so N workers never open N × `pool_max` connections to a
production database.

**Scope**: the project stays an extraction layer (source database → Arrow RecordBatches). Ballista
supplies the scheduler, the workers, and the job-execution machinery; this phase adds what Ballista
knows nothing about — how a `PgPool` that cannot be serialized crosses a process boundary, and how
the cluster as a whole bounds its load on the source.

---

## Phase 4 Exit Criteria (from roadmap.md)

> A workload that saturates one machine scales across three with better wall-clock time and
> without increasing source load.

**Status**: IMPLEMENTED UP TO MACHINE VALIDATION

- ✓ Distribution of one table scan across N tasks (one keyset partition per task) exists,
  exercised end-to-end through the scheduler→executor plan-shipping path.
- ✓ The connection-pool coordination that makes "without increasing source load" true exists and
  is unit-tested (`pool_max / workers` per process, one shared pool per source per process).
- ⏳ The three-machine wall-clock measurement must be done against a real Postgres once the
  bottleneck is confirmed to be compute (docs/roadmap.md Phase 4 warns: if the source is the
  bottleneck, distribution makes things worse).

---

## Implementation Summary

### ✓ Task 1: Serialization surface for plans and providers

A scan is a live `PgPool` plus code; neither can cross a process boundary. Every type that travels
therefore gained serde (de)serialization, and the pool never travels at all.

| Type | Location | Change |
|------|----------|--------|
| `SourceConfig` | `../../src/config/mod.rs` | `Serialize` derive |
| `TableMetadata` | `../../src/types/table_metadata.rs` | `Serialize, Deserialize` derives |
| `ColumnMetadata` | `../../src/types/column_metadata.rs` | `Serialize, Deserialize` derives |
| `Predicate`, `Literal`, `PushdownPolicy` | `../../src/pushdown/mod.rs` | serde derives; `Predicate::Cmp.op` changed `&'static str` → `String` so it survives JSON |
| `ScanPartition` | `../../src/connector/postgres/parallel.rs` | `Serialize, Deserialize` derives |
| `PostgresConnectionDescriptor` | `../../src/distributed/connection.rs` | new — the process-independent source description (below) |
| `PostgresExecutionPlanModel` | `../../src/connector/postgres/execution_plan.rs` | new — serializable slice of a scan (below) |
| `PostgresTableProviderModel` | `../../src/connector/postgres/table_provider.rs` | new — serializable provider (below) |
| `DistributedConfig` | `../../src/config/mod.rs` | new — `scheduler_url` + `workers`, plus `JobConfig.distributed` |

### ✓ Task 2: `PostgresConnectionDescriptor` — the source, minus the password

`../../src/distributed/connection.rs`

Only a live `PgPool` knows how to reach the source, and it cannot be serialized. The codec
carries a `PostgresConnectionDescriptor` instead — everything required to rebuild a pool in the
*executing* process:

```rust
pub struct PostgresConnectionDescriptor {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password_env: String,   // name of the env var holding the password — never the password
    pub database: String,
    pub pool_max: u32,          // cluster-wide connection budget
    pub expected_workers: usize, // how many processes share the budget
    pub statement_timeout_ms: u64,
    pub application_name: String,
    pub schema: String,
}
```

Key methods:
- `from_config(&SourceConfig, expected_workers)` — every process builds the *same* descriptor
  from the same job config, so they all budget identically.
- `budgeted_max_connections() = max(1, pool_max / max(1, expected_workers))` — each process opens
  only its share; never zero, because a worker with no pool can't scan anything.
- `pool_key()` — `(host, port, user, database, budgeted_max_connections)`. The budget is part of
  the key so two jobs that disagree about worker count can't silently share one wrongly-sized pool.
- `resolved_password()` — reads the env-var name from the descriptor at pool-creation time, in the
  process that will actually use the pool. Plaintext never enters a serialized plan.

### ✓ Task 3: `SourcePoolRegistry` — one budgeted pool per source, per process

`../../src/distributed/pool_registry.rs`

The client process and every in-proc Ballista executor must not each open `pool_max` connections —
that is exactly the `N × pool_max` failure mode Phase 4 names. A process-wide registry keys one
`PgPool` per source:

```rust
pub fn registry() -> &'static SourcePoolRegistry {
    static REGISTRY: OnceLock<SourcePoolRegistry> = OnceLock::new();
    REGISTRY.get_or_init(SourcePoolRegistry::new)
}
```

- `pool(&descriptor)` creates the pool lazily via `PgPoolOptions::connect_lazy_with` (synchronous,
  no network touch until a connection is actually borrowed), sized to
  `budgeted_max_connections()`, with the session-hygiene `after_connect` from
  [connectors/postgres.md §6](../connectors/postgres.md): `TIME ZONE 'UTC'`, `statement_timeout`,
  `idle_in_transaction_session_timeout`, `lock_timeout`.
- `SourcePool` has two forms: `Connected(PgPool)` — what the client process holds after
  registration — and `Deferred { descriptor, pool: OnceLock<...> }` — what an executor holds after
  decoding a plan. `get()` resolves the *process-shared* registry pool on first use, so the
  scheduler that only plans never opens a single connection, and any executor that merely decodes
  a plan still shares one pool with every other task in its process.
- A planner must call `registry()` — not open its own pool — so the whole process tree converges on
  one pool per source per process.

Tests: `test_budget_division` (8 / 4 → 2, 8 / 16 → 1 floor), `test_pool_key_includes_budget`,
`test_deferred_get_fails_cleanly_without_password_env`.

### ✓ Task 4: `PostgresPhysicalCodec` — shipping the scan plan

`../../src/distributed/plan_codec.rs`

Ballista ships physical plans from scheduler to executor through `PhysicalExtensionCodec` hooks.
Overriding the codec **replaces** Ballista's default, so every non-Postgres node (shuffle nodes,
`UnknownExec`, ...) is delegated back to `BallistaPhysicalExtensionCodec::default()`.

```rust
pub const POSTGRES_SCAN_MAGIC: &[u8] = b"PGSC01\0";
```

- `try_encode`: if the node is a `PostgresExecutionPlan`, serialize its model to JSON, prefix with
  `POSTGRES_SCAN_MAGIC`, and append to the buffer.
- `try_decode`: if the buffer starts with the magic, strip it, deserialize to
  `PostgresExecutionPlanModel`, and rebuild the plan via `PostgresExecutionPlan::from_model`.
  Otherwise delegate to the default codec.

The magic prefix disambiguates the JSON payload from Ballista's own protobuf framing. Round-trip
unit test (`test_round_trip`) confirms encode → decode → downcast back to `PostgresExecutionPlan`
and that the encoded buffer carries the magic.

### ✓ Task 5: `PostgresLogicalCodec` — shipping the provider

`../../src/distributed/table_codec.rs`

The `el-ballista distribute` client and scheduler exchange a *logical* plan too. Ballista's default logical
codec rejects provider nodes it doesn't recognize, so `PostgresLogicalCodec` serializes our table
provider as JSON (same magic prefix) and rebuilds it on the scheduler **without opening a single
connection** — the descriptor is carried, the pool is deferred. `try_encode_table_provider` /
`try_decode_table_provider` handle `PostgresTableProvider`; logical `try_decode`/`try_encode` and
all other providers delegate to `BallistaLogicalExtensionCodec::default()`.

### ✓ Task 6: `PostgresExecutionPlanModel` — the serde slice of a scan

`../../src/connector/postgres/execution_plan.rs`

```rust
pub struct PostgresExecutionPlanModel {
    pub descriptor: PostgresConnectionDescriptor,
    pub table_metadata: TableMetadata,
    pub pushed_filters: Vec<Predicate>,
    pub pushed_limit: Option<usize>,
    pub watermark_column: Option<String>,
    pub window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    pub batch_size: usize,
    pub partitions: Vec<ScanPartition>,
}
```

`PostgresExecutionPlan` itself grew:
- an `Option<PostgresConnectionDescriptor>` field — `None` only in the legacy in-process test
  constructor;
- a `partitions: Vec<ScanPartition>` field (from [parallel.rs] keyset partitioning);
- partitioning reported as `UnknownPartitioning(partitions.len())` (or `1`) — Ballista schedules
  one leaf task per partition, which is the distribution mechanism;
- `to_model()` / `from_model()` — the latter rebuilds the Arrow schema from table metadata;
- `execute()` resolves the pool through `registry().pool(&descriptor)` on first use (pool-less
  plans return `Err(NotImplemented)` rather than silently executing locally);
- `build_query(partition_idx)` reuses the Phase 2/3 query builder and renders the watermark window
  `(lo, hi]`, pushed predicates through `crate::pushdown` (bound parameters, never interpolated),
  the inline partition bounds for that one partition, and the pushed `LIMIT`.

### ✓ Task 7: `PostgresTableProviderModel` — scan-side partitioning on the scheduler

`../../src/connector/postgres/table_provider.rs`

```rust
pub struct PostgresTableProviderModel {
    pub descriptor: PostgresConnectionDescriptor,
    pub table_metadata: TableMetadata,
    pub policy: PushdownPolicy,
    pub deny: Vec<String>,
    pub watermark_column: Option<String>,
    pub window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    pub batch_size: usize,
    pub parallel_workers: usize,
    pub partition_column: Option<String>,
}
```

- `new()` still discovers the schema through the process-shared budgeted pool and holds a
  `SourcePool::connected`.
- `from_model()` / `to_model()` convert to/from the serializable form; `from_model()` rebuilds with
  `SourcePool::deferred` so decoding never opens a connection.
- `with_parallel_workers(workers, partition_column)` sets how many Ballista scan tasks (keyset
  partitions) `scan()` produces — and therefore how executor processes share the budget.
- `scan()` computes keyset partition bounds via
  `compute_keyset_partitions` (`MIN`/`MAX` on the partition column, histogram-bounded) *at planning
  time on the scheduler*, embeds them in the plan, and each executor scans only its own range. A
  single boundless partition (empty table, or `min >= max`) collapses to an empty partition list so
  rows are never doubled.

### ✓ Task 8: `DistributedContext` — two deployment modes

`../../src/distributed/context.rs`

```rust
pub struct DistributedContext {
    pub session: SessionContext,       // planner that executes on the Ballista cluster
    pub workers: usize,                // budget divider + keyset partition count
    pub partition_column: Option<String>,
}
```

Both paths build the Client session with our codecs registered via
`SessionConfigExt::with_ballista_logical_extension_codec` / `with_ballista_physical_extension_codec`
(done through `SessionContext::new_with_config(config).state()` because DataFusion 54 removed
`SessionState::new_with_config`):

- **`standalone(config, workers)`** — `SessionContext::standalone_with_state` runs the scheduler
  plus an in-proc executor in this one process. It exercises the entire scheduler→executor
  plan-shipping path (codecs, partition distribution, budgeted pools), which is what the exit
  criterion needs validated before adding machines.
- **`remote(config, url, workers)`** — `SessionContext::remote_with_state(url, ...)` connects to an
  already-running `el-ballista scheduler`; workers are separate `el-ballista worker` processes.
- **`register_source(&config)`** — opens the budgeted pool (via the registry), discovers the
  schema, registers a `PostgresTableProvider` split into `workers` keyset partitions when a
  partition column is configured.

`workers` decides both the budget division (`pool_max / workers`, applied independently in each
process) and the keyset partition count — a three-machine deployment adds machines without adding
source connections.

### ✓ Task 9: CLI — `el-ballista distribute` / `el-ballista scheduler` / `el-ballista worker`

`../../src/cli/mod.rs`

```
el-ballista distribute --config <path> [--workers N] [--scheduler-url http://host:port]
el-ballista scheduler [--scheduler-url http://host:port]
el-ballista worker --scheduler-url http://host:port
```

- `el-ballista scheduler` — long-running Ballista scheduler on its own process (so it stays up across
  worker restarts). Builds `SchedulerConfig` with `override_logical_codec` /
  `override_physical_codec` set to our codecs, a `BallistaCluster::new_memory` cluster, and calls
  `start_server(cluster, addr, config)`. The overridden codecs let the scheduler decode the
  provider and rebuild the scan plan when it plans tasks — without opening any source connection.
- `el-ballista worker` — one long-running Ballista executor via `start_executor_process`, connected to the
  scheduler URL, with our codecs in `ExecutorProcessConfig.override_*`. Each worker resolves source
  descriptors in encoded tasks and opens only its `pool_max / workers` share (through
  `SourcePoolRegistry`).
- `el-ballista distribute` — the job driver, mirroring `el-ballista run` semantics (§5 commit protocol):
  1. `DistributedContext` in the chosen mode (`remote` if `--scheduler-url`, else `standalone`);
  2. `register_source`;
  3. acquire the checkpoint lease;
  4. `safe_high_watermark` → `build_window` (express the window as a DataFrame filter on the
     incremental column — folded into `ScalarValue::TimestampMicrosecond` to match the source rows —
     which the provider pushes into the scan, ANDed with the keyset partition bounds);
  5. `df.collect()` on the cluster;
  6. commit `clamp_to_observed(window.hi, max_observed)` so the watermark never advances past what
     was actually observed (docs/incremental-extraction.md §3.1 Mitigation 3).

`el-ballista plan` was also switched from the raw extractor to the descriptor-based
`PostgresTableProvider::new(...)` path, so planning and executing share one code path.

### ✓ Task 10: Example + wiring

- `../../examples/distributed_extraction.rs` — `DistributedContext::standalone(config, workers)` against
  the same job-spec JSON as `el-ballista run`, `register_source`, then `df.limit(0, Some(100)).show()`.
- `../../src/lib.rs` and `../../src/main.rs` — new `pub mod distributed`.
- `../../Cargo.toml` — added `ballista-core`, `ballista-scheduler`, `ballista-executor`,
  `ballista-executor`'s `arrow-ipc-optimizations` feature, and `datafusion-proto`, all `54.1.0`,
  plus the `distributed_extraction` example target.

---

## Architecture

### Before (Phase 3 — extracted in-process, all load on one process)
```
Postgres ──► PostgresExecutionPlan ──► streaming RecordBatch ──► DataFusion (this process)
                 └── one unbounded pool, one process
```

### After (Phase 4 — distributed)
```
                          el-ballista distribute (client)
                     DistributedContext: client session
                     with codecs; registers provider; reads
                     checkpoint; plans window as a pushed filter
                                   │ http
                                   ▼
        ┌──────────────── Scheduler ────────────────┐
        │  SchedulerConfig{override codecs}; plans   │
        │  scan() → keyset partitions (MIN/MAX);     │
        │  provider rebuilt from model, pool deferred │
        ▼             (opens zero connections)        ▼
     Worker 1 ─┐                          ┌─ Worker 3
     Worker 2 ─┤  each executor decodes the plan via  ┤
        │      │  PostgresPhysicalCodec                │
        │      └── resolves registry().pool(descriptor)┘
        ▼                (pool_max / workers each)
     Postgres (source sees same connection count on 1 or 3 machines)
```

### Plan-shipping flow
```
el-ballista distribute                                   scheduler / workers
  Provider ──PostgresLogicalCodec──► JSON {descriptor, metadata, ...} + PGSC01 magic
  Scan plan ──PostgresPhysicalCodec──► JSON {descriptor, partitions, pushed_*, window}
                                         │
  execute(partition i) ◄── Ballista ships one task per partition ◄── UnknownPartitioning(n)
  pool = registry().pool(&descriptor)     (resolved lazily, budgeted, never serialized)
```

---

## Connection-pool coordination (the constraint that matters most)

- `pool_max` means "cluster-wide budget for this source", not "per process".
- Every process derives the same descriptor from the same config, so every process computes
  `budgeted = max(1, pool_max / max(1, workers))`.
- Each process keeps exactly one pool per source (`SourcePoolRegistry`), shared across all its
  executors and tasks — the key includes the budget, so a wrong `workers` guess can't silently
  over-open.
- Pools are created lazily (`PgPoolOptions::connect_lazy_with`, `SourcePool::deferred`): a
  scheduler that only plans, and an executor that only decodes, open nothing until a scan
  actually borrows a connection.
- Passwords are never serialized: only the env-var *name* travels; resolution happens in the
  process that opens the pool.
- Session hygiene (UTC, timeouts) is reapplied via `after_connect` on every pool regardless of
  which process created it.

---

## DataFusion 54 notes (things that bit during implementation)

- `Partitioning::PartialEq` only ever matches `RoundRobinBatch` and `Hash`; identical
  `UnknownPartitioning(n)` values compare unequal. Partitioning assertions in tests match the
  variant instead of using `assert_eq!`.
- `ExecutionPlan` / `TableProvider` no longer expose `as_any()` on the trait objects; use the
  inherent `dyn ExecutionPlan::downcast_ref::<T>()` / `dyn TableProvider::downcast_ref::<T>()`
  helpers.
- `SessionState::new_with_config` is gone; `SessionContext::new_with_config(config).state()`
  produces the state before handing it to `standalone_with_state` / `remote_with_state`.
- `ScalarValue::TimestampMicrosecond` takes `Option<i64>`; fold timestamps into it (not `lit` of a
  `DateTime<Utc>`, which doesn't satisfy `Literal`).

---

## Tests

| Test | Location | Covers |
|------|----------|--------|
| `test_round_trip` | `../../src/distributed/plan_codec.rs` | encode → decode → downcast of a `PostgresExecutionPlan`, magic prefix present |
| `test_budget_division` | `../../src/distributed/pool_registry.rs` | `pool_max / workers` arithmetic and the ≥1 floor |
| `test_pool_key_includes_budget` | `../../src/distributed/pool_registry.rs` | budget is part of the registry key |
| `test_deferred_get_fails_cleanly_without_password_env` | `../../src/distributed/pool_registry.rs` | lazy resolution fails cleanly (twice) without `password_env` set |
| `test_display_as_does_not_panic` | `../../src/connector/postgres/execution_plan.rs` | `DisplayAs` renders both formats with a descriptor-less plan |
| `test_partition_count` | `../../src/connector/postgres/execution_plan.rs` | `UnknownPartitioning(1)` for no/one partition, `UnknownPartitioning(4)` for four |

The config round-trip tests (`../../src/config/mod.rs`) cover the new `distributed` block with its
serde defaults.

## Verification

✓ `cargo check --all-targets` — 0 errors, no warnings from the new `distributed` module
✓ `cargo build --all-targets` — succeeds (bin, lib, examples)
✓ `cargo test` — 52/52 pass
✓ Pre-existing warnings in untouched files (`row_adapter`, `cost_model`, `explain`, `engine`)
remain from earlier phases; none originate in Phase 4 code

⏳ Not verifiable in this environment (needs a live cluster + Postgres):
- three-machine wall-clock scaling vs. single machine;
- confirming the source connection count stays flat at `pool_max` across 1 → 3 workers;
- an end-to-end `el-ballista scheduler` + two `el-ballista worker` + `el-ballista distribute` run against a real table.

---

## Files

### New
| File | Purpose |
|------|---------|
| `../../src/distributed/mod.rs` | module wiring + re-exports |
| `../../src/distributed/connection.rs` | `PostgresConnectionDescriptor` |
| `../../src/distributed/pool_registry.rs` | `registry()`, `SourcePoolRegistry`, `SourcePool` |
| `../../src/distributed/plan_codec.rs` | `PostgresPhysicalCodec` + `POSTGRES_SCAN_MAGIC` |
| `../../src/distributed/table_codec.rs` | `PostgresLogicalCodec` |
| `../../src/distributed/context.rs` | `DistributedContext` (standalone/remote) |
| `../../examples/distributed_extraction.rs` | Phase 4 example |

### Modified
| File | Changes |
|------|---------|
| `../../src/connector/postgres/execution_plan.rs` | `PostgresExecutionPlanModel`, descriptor, partitions, `UnknownPartitioning(n)`, `to_model`/`from_model`, registry-resolved pool; `execution_plan` made `pub` in `mod.rs` |
| `../../src/connector/postgres/table_provider.rs` | `PostgresTableProviderModel`, `from_model`/`to_model`, `with_parallel_workers`, keyset partitions in `scan()` |
| `../../src/connector/postgres/parallel.rs` | `ScanPartition` serde derives |
| `../../src/pushdown/mod.rs` | serde derives; `Predicate::Cmp.op` → `String` |
| `../../src/pushdown/cost_model.rs` | test fixtures adapted to `op: String` |
| `../../src/types/table_metadata.rs`, `../../src/types/column_metadata.rs` | serde derives |
| `../../src/config/mod.rs` | `SourceConfig` Serialize; `DistributedConfig`; `JobConfig.distributed` |
| `../../src/cli/mod.rs` | `el-ballista distribute` / `el-ballista scheduler` / `el-ballista worker`; descriptor-based `el-ballista plan` |
| `../../src/lib.rs`, `../../src/main.rs` | `pub mod distributed` |
| `../../src/demo.rs` | `JobConfig` initializer gains `distributed` |
| `examples/*` | config instantiation updated where needed |
| `../../Cargo.toml` | new `54.1.0` deps + `distributed_extraction` example |

---

## Acceptance Criteria Verification

- [x] `PostgresExecutionPlan` fully serializable and reconstructible across a Ballista
      scheduler→executor boundary.
- [x] One source scan distributes into N keyset partitions ↔ N Ballista scan tasks.
- [x] `N × pool_max` prevented: one budgeted pool per source per process;
      `budgeted_max_connections = max(1, pool_max / max(1, workers))`.
- [x] Scheduler plans without opening any source connection (lazy/deferred pools).
- [x] Passwords never cross a process boundary (env-var name only).
- [x] Both deployment modes implemented (`standalone`, `el-ballista scheduler` + `el-ballista worker` + `remote`).
- [x] Existing Phase 1–3 behavior intact: `el-ballista run` / `el-ballista plan` / `el-ballista backfill` unchanged except
      `el-ballista plan` now goes through the same descriptor-based provider path.
- [x] Unit tests for the new surface pass; `cargo test` 52/52; build 0 errors.

---

**Phase 4 Status**: ✓ IMPLEMENTED
**Completion Date**: September 11, 2026
**Remaining**: runtime validation of the exit criterion (three-machine wall-clock + flat source
connection count) against a live deployment
**Next Phase**: Phase 5 (MySQL Connector)