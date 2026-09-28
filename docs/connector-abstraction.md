# Connector Abstraction Plan

## Goal

Support multiple source connectors (PostgreSQL today, MySQL next) where the *generic* machinery —
SQL predicate rendering, cost-based pushdown, the DataFrame engine, and Ballista distributed
execution — is shared, and only genuinely backend-specific pieces live under each connector.

Abstractions are added only when a second connector needs them, so the plan below lands in
steps, each driven by the MySQL connector growing a capability.

## Current state

- **Shared:** `src/pushdown/` is a connector-agnostic crate-root module (`ir`, `translate`,
  `policy`, `cost_model`, `dialect`, and the backend-neutral `stats` / `explain` types). Both
  `PostgresDialect` and `MysqlDialect` implement `crate::pushdown::dialect::SqlDialect`.
- **Postgres:** everything Postgres-specific lives under `src/connector/postgres/`, including
  `dialect`, `param_sink`, `inline_sql`, the `stats` collector, the `explain` executor,
  `distributed`, `engine`, `pipeline` and the `PostgresConnector` builder API (`api.rs`). There
  are no crate-root re-exports: callers use
  `rust_ballista_extraction_layer::connector::postgres::*`. The push/keep decision is made in
  `PostgresTableProvider::supports_filters_pushdown`; there is no custom optimizer rule.
- **MySQL:** the prototype (`src/connector/mysql/`) is a walking skeleton: `MysqlDialect`, a
  type mapper, an `information_schema` reader, and a full-table extractor that selects raw
  columns (no `CAST`) and decodes them into typed Arrow arrays — lossless unsigned integers
  (`bigint unsigned` → `UInt64`), `tinyint(1)` → `Boolean`, `YEAR` → `Int16`, `TIME` →
  `Duration(µs)`, `decimal` → `Decimal128`, binary types → `Binary`, `Utf8` otherwise — either
  streamed in `batch_size` batches (`extract_full_table_for_each_batch`) or materialized into one
  batch (`extract_full_table`). It has no filters/pushdown, no `TableProvider`, no
  parallel/distributed execution and no checkpointed jobs.
- **Not yet shared:** `engine::ExtractContext` hard-codes `PostgresTableProvider`, and
  `distributed::DistributedContext` hard-codes the Postgres codecs. A `SourceConnector` trait
  sketch (two methods, `dialect` and `table_metadata`) is in `connector/mod.rs`, not wired into
  anything.

## What the MySQL prototype revealed

Generic (already reused unchanged by MySQL):
- `types::{TableMetadata, ColumnMetadata}` — the catalog contract holds across backends.
- `pushdown::dialect::SqlDialect` — a second dialect (`MysqlDialect`) implements it cleanly.

Two leaks it exposed shaped the current layout:
- `SqlDialect` and fidelity belong to the shared layer, not to a Postgres module: the dialect
  trait is `crate::pushdown::dialect::SqlDialect`, and fidelity is decided once at translation
  from the engine-neutral `pushdown::ColumnKind`.
- `connector::errors::ExtractorError` messages are backend-neutral ("Source error" /
  "Unsupported source type").

Stays connector-specific (never shared):
- The SQL dialect impl (`PostgresDialect` / `MysqlDialect`), placeholders (`$1` vs `?`),
  identifier quoting (`"..."` vs `` `...` ``).
- Catalog + statistics + EXPLAIN queries (`pg_stats`/`pg_read_all_stats`/EXPLAIN vs MySQL).
- Type mapping to Arrow and row decoding.
- Cursor/streaming mechanics (`DECLARE ... CURSOR WITHOUT HOLD` + `FETCH` is Postgres-only).
- Parallel strategy: `ctid` is Postgres-physical; MySQL is keyset-only.
- Ballista logical/physical codecs (each serializes its own `TableProvider`/`ExecutionPlan`).

## Target module layout

The layout differs from the current one in [architecture](architecture.md#2-crate-layout) in
these places only:

- `src/engine/` (new, step 2): the generic DataFrame builder over a `SourceConnector`, moved out
  of `connector/postgres/engine/`.
- `src/distributed/` (new, step 3): generic Ballista orchestration. The codecs, connection
  descriptor and pool registry stay under `connector/postgres/distributed/`, since each
  connector serializes its own `TableProvider` / `ExecutionPlan`.
- `connector/mod.rs`: `SourceConnector` grows the methods below.
- `connector/mysql/`: gains a `TableProvider`, pushdown and a pool registry as the prototype is
  promoted.

## Proposed `SourceConnector` trait

The engine and distributed layers depend on this instead of `PostgresTableProvider` directly.
Only `dialect` and `table_metadata` exist today. The rest is added as part of the lift, when the
second connector actually needs it, not before.

```rust
/// A source backend the generic engine/distributed layers can drive.
#[async_trait::async_trait]
pub trait SourceConnector: Send + Sync {
    /// Backend SQL dialect (rendering, placeholders, quoting, fidelity).
    fn dialect(&self) -> &dyn pushdown::dialect::SqlDialect;

    /// Read a table's schema into the shared catalog contract.
    async fn table_metadata(&self, table: &str) -> Result<TableMetadata, AppError>;

    /// Build a DataFusion TableProvider for the table (cost-aware, pushdown-capable).
    async fn table_provider(
        &self,
        table: &str,
        pushdown: PushdownPolicy,
    ) -> Result<Arc<dyn TableProvider>, AppError>;

    /// Ballista codecs for distributed execution (serialize this backend's plans), or `None`
    /// if the connector does not support distributed execution.
    fn ballista_codecs(&self) -> Option<ConnectorCodecs> { None }
}
```

`ExtractContext`/`Pipeline`/`DistributedContext` become generic over `&dyn SourceConnector`
(or a `Box<dyn SourceConnector>`), constructed from config by a small registry that maps a
`source.kind` field (`"postgres"` | `"mysql"`) to the right connector.

## Compile-safe migration order

The shared `pushdown` module and the Postgres-only placement of dialect, statistics, `EXPLAIN`,
parameter binding, engine, pipeline and distributed code are in place. The remaining steps each
build and pass `cargo clippy -- -D warnings` before the next starts:

1. Add `SourceConnector` + `PostgresConnector`'s impl of it; leave the fast-path CLI code as-is.
2. Make `engine::ExtractContext` generic over `SourceConnector` (drop the hard-coded
   `PostgresTableProvider` in `source()`); move `engine` to the crate root.
3. Make `distributed::DistributedContext` take codecs via `SourceConnector::ballista_codecs`;
   move the generic parts to the crate root, leave codecs/descriptor/pool under postgres.
4. Add `source.kind` to `JobConfig` + a connector registry; wire `PostgresConnector`/(later)
   `MysqlConnector` selection.
