# Connector Abstraction Plan

Status: **in progress.** Steps 1 and 6 are done: `pushdown` is a shared, connector-agnostic
crate-root module (`src/pushdown/`: `ir`, `translate`, `policy`, `cost_model`, `dialect`, and the
backend-neutral `stats` / `explain` types); every Postgres-specific piece lives under
`src/connector/postgres/` (`dialect`, `param_sink`, `inline_sql`, the `stats` collector, the
`explain` executor), and the crate-root `distributed` / `engine` / `pipeline` shims are gone.
`MysqlDialect` is under `connector/mysql/`; both dialects `impl crate::pushdown::dialect::SqlDialect`.
A `SourceConnector` trait **sketch** is in `connector/mod.rs` (not wired into anything). Steps 2–5
below are not done.

## Goal

Support multiple source connectors (PostgreSQL today, MySQL next) where the *generic* machinery —
SQL predicate rendering, cost-based pushdown, the DataFrame engine, and Ballista distributed
execution — is shared, and only genuinely backend-specific pieces live under each connector.

## Current state

All Postgres code lives under `src/connector/postgres/`, including `distributed`, `engine`,
`pipeline` and the `PostgresConnector` builder API (`api.rs`). There are **no crate-root
re-exports**: callers use `rust_ballista_extraction_layer::connector::postgres::*` (see AGENTS.md —
Postgres connector modularization). The MySQL prototype (`src/connector/mysql/`) is a walking
skeleton: `MysqlDialect`, a type mapper, an `information_schema` reader, and a full-table
extractor that selects raw columns (no `CAST`) and decodes them into typed Arrow arrays —
lossless unsigned integers (`bigint unsigned` → `UInt64`), `tinyint(1)` → `Boolean`, `YEAR` →
`Int16`, `TIME` → `Duration(µs)`, `decimal` → `Decimal128`, binary types → `Binary`, `Utf8`
otherwise — either streamed in `batch_size` batches (`extract_full_table_for_each_batch`) or
materialized into one batch (`extract_full_table`). It has no filters/pushdown, no
`TableProvider`, no parallel/distributed execution and no checkpointed jobs.

## What the MySQL prototype revealed

Generic (already reused unchanged by MySQL):
- `types::{TableMetadata, ColumnMetadata}` — the catalog contract holds across backends.
- `pushdown::dialect::SqlDialect` — a second dialect (`MysqlDialect`) implements it cleanly.

Leaks found (both fixed):
- `SqlDialect` and `Fidelity` used to live under a Postgres module, so MySQL imported Postgres
  code. (DONE — `crate::pushdown::dialect::SqlDialect`; fidelity is decided once at translation
  from the engine-neutral `pushdown::ColumnKind`.)
- `connector::errors::ExtractorError` messages said "PostgreSQL error". (DONE — now "Source error" / "Unsupported source type".)

Stays connector-specific (never shared):
- The SQL dialect impl (`PostgresDialect` / `MysqlDialect`), placeholders (`$1` vs `?`),
  identifier quoting (`"..."` vs `` `...` ``).
- Catalog + statistics + EXPLAIN queries (`pg_stats`/`pg_read_all_stats`/EXPLAIN vs MySQL).
- Type mapping to Arrow and row decoding.
- Cursor/streaming mechanics (`DECLARE ... CURSOR WITHOUT HOLD` + `FETCH` is Postgres-only).
- Parallel strategy: `ctid` is Postgres-physical; MySQL is keyset-only.
- Ballista logical/physical codecs (each serializes its own `TableProvider`/`ExecutionPlan`).

## Target module layout

```
src/
  pushdown/                 # shared, connector-agnostic (DONE)
    mod.rs                  # Fidelity, ColumnKind, decide/translate entry points
    ir.rs                   # Predicate IR, SqlSink, SqlParam, render_to
    translate.rs            # DataFusion Expr -> Predicate with per-node fidelity
    policy.rs               # push/keep policy modes
    dialect.rs              # SqlDialect trait
    cost_model.rs           # CostParams, cost math
    stats.rs, explain.rs    # backend-neutral statistics / plan-estimate types + TableStatsSource
  engine/                   # NEW (step 3) generic DataFrame builder over a SourceConnector
  distributed/              # NEW (step 4) generic Ballista orchestration; codecs per connector
  connector/
    mod.rs                  # SourceDescriptor (exists) + SourceConnector (sketch exists, unwired)
    postgres/               # (DONE) everything Postgres-specific
      api.rs                # PostgresConnector builder API (collect/stream/run/run_with)
      dialect.rs, param_sink.rs, inline_sql.rs   # rendering + binding
      explain.rs, stats.rs  # Postgres EXPLAIN / pg_stats implementations
      table_provider.rs, execution_plan.rs, extractor.rs, parallel.rs, row_adapter.rs, copy.rs
      pipeline/, engine/    # job pipeline (filters, splits, run) and ExtractContext
      distributed/          # Postgres codecs + connection descriptor + pool registry
    mysql/
      dialect.rs            # MysqlDialect: pushdown::dialect::SqlDialect (DONE)
      schema_reader.rs, type_mapper.rs, row_adapter.rs, extractor.rs, query_builder.rs
```

The crate-root shims are already deleted (step 6); callers use `connector::postgres::*`.

## Proposed `SourceConnector` trait (sketch — only the first two methods exist, unwired)

The engine and distributed layers depend on this instead of `PostgresTableProvider` directly.
Only add it when the second connector actually needs it (AGENTS.md: abstraction must be required,
not hypothetical) — i.e. as part of the lift, not before.

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

Each step should build and pass `cargo clippy -- -D warnings` before the next.

1. DONE — `pushdown` moved to `src/pushdown/` (crate root, shared). `PostgresDialect` lives in
   `connector/postgres/dialect.rs`, `MysqlDialect` in `connector/mysql`. The formerly
   Postgres-coupled items were relocated into `connector/postgres` (`explain.rs`, the `stats.rs`
   collector + `impl TableStatsSource for PgPool`, `param_sink.rs` / `PgParamSink`,
   `inline_sql.rs`); the old DataFusion optimizer rule (`SourceAwarePushdownRule`) was deleted —
   the push/keep decision is made in `PostgresTableProvider::supports_filters_pushdown`.
2. Add `SourceConnector` + `PostgresConnector`'s impl of it; leave the fast-path CLI code as-is.
3. Make `engine::ExtractContext` generic over `SourceConnector` (drop the hard-coded
   `PostgresTableProvider` in `source()`); move `engine` to the crate root.
4. Make `distributed::DistributedContext` take codecs via `SourceConnector::ballista_codecs`;
   move the generic parts to the crate root, leave codecs/descriptor/pool under postgres.
5. Add `source.kind` to `JobConfig` + a connector registry; wire `PostgresConnector`/(later)
   `MysqlConnector` selection.
6. DONE — the transitional crate-root shims (`distributed`, `engine`, `pipeline`) are deleted.

Do not start step N+1 until step N compiles green.
