# Connector Abstraction Plan

Status: **in progress.** Step 1 done: `pushdown` is now a shared crate-root module
(`src/pushdown/`, no longer under `connector::postgres`); `SqlDialect`/`Fidelity`/`Predicate`/cost
model live there; `PostgresDialect` moved to `connector/postgres/dialect.rs` and `MysqlDialect` is
under `connector/mysql/` (both `impl crate::pushdown::dialect::SqlDialect`). A `SourceConnector`
trait sketch is in `connector/mod.rs`. Remaining steps below are not done yet.

## Goal

Support multiple source connectors (PostgreSQL today, MySQL next) where the *generic* machinery —
SQL predicate rendering, cost-based pushdown, the DataFrame engine, and Ballista distributed
execution — is shared, and only genuinely backend-specific pieces live under each connector.

## Current state

After the modularization, all Postgres code lives under `src/connector/postgres/`, with
`distributed`, `engine`, and `pipeline` physically there and re-exported at the
crate root as transitional shims (see AGENTS.md — Postgres connector modularization). The MySQL
prototype (`src/connector/mysql/`) is a walking skeleton: `MysqlDialect`, a type mapper, a
`information_schema` reader, and a full-table extractor (materializes `Utf8` via `CAST`).

## What the MySQL prototype revealed

Generic (already reused unchanged by MySQL):
- `types::{TableMetadata, ColumnMetadata}` — the catalog contract holds across backends.
- `pushdown::dialect::SqlDialect` — a second dialect (`MysqlDialect`) implements it cleanly.

Leaks to fix during the lift:
- `SqlDialect` and `Fidelity` live under `connector::postgres::pushdown`, so MySQL imports a
  Postgres module. They must move to a shared location.
- `connector::errors::ExtractorError` messages say "PostgreSQL error" — make them backend-neutral. (DONE — now "Source error" / "Unsupported source type".)

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
  pushdown/                 # shared, connector-agnostic (DONE — now at crate root)
    dialect.rs              # SqlDialect trait (PostgresDialect re-exported for compat)
    mod.rs                  # Fidelity, Predicate, SqlSink, translate/decide, policy
    cost_model.rs           # CostParams, cost math
    # RESIDUAL Postgres-coupled (relocate next): explain.rs, stats.rs collector,
    # PgParamSink (in mod.rs), optimizer_rule.rs (imports PostgresTableProvider)
  engine/                   # NEW generic DataFrame builder over a SourceConnector
  distributed/              # NEW generic Ballista orchestration; codecs supplied per connector
  connector/
    mod.rs                  # SourceDescriptor (exists) + SourceConnector (new, see below)
    postgres/
      dialect.rs            # PostgresDialect: pushdown::dialect::SqlDialect (DONE)
      explain.rs, stats.rs  # Postgres catalog/EXPLAIN
      table_provider.rs, execution_plan.rs, extractor.rs, parallel.rs, row_adapter.rs
      distributed/          # Postgres codecs + connection descriptor + pool
    mysql/
      dialect.rs            # MysqlDialect: pushdown::dialect::SqlDialect (DONE)
      schema_reader.rs, type_mapper.rs, extractor.rs, query_builder.rs
```

The crate-root shims are deleted once callers use `connector::postgres::*` / `sql::*` directly.

## Proposed `SourceConnector` trait (sketch — not yet added)

The engine and distributed layers depend on this instead of `PostgresTableProvider` directly.
Only add it when the second connector actually needs it (AGENTS.md: abstraction must be required,
not hypothetical) — i.e. as part of the lift, not before.

```rust
/// A source backend the generic engine/distributed layers can drive.
#[async_trait::async_trait]
pub trait SourceConnector: Send + Sync {
    /// Backend SQL dialect (rendering, placeholders, quoting, fidelity).
    fn dialect(&self) -> &dyn sql::SqlDialect;

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

1. DONE — `pushdown` moved to `src/pushdown/` (crate root, shared). `PostgresDialect` moved to
   `connector/postgres/dialect.rs` (re-exported from `pushdown::dialect` for compat);
   `MysqlDialect` under `connector/mysql`. NEXT within this step: relocate the residual
   Postgres-coupled items still living in `pushdown` (`explain.rs`, the `stats.rs` collector +
   `impl TableStatsSource for PgPool`, `PgParamSink`, `optimizer_rule.rs`) into
   `connector/postgres`, leaving only connector-agnostic code in `pushdown`.
2. Add `SourceConnector` + `PostgresConnector`'s impl of it; leave the fast-path CLI code as-is.
3. Make `engine::ExtractContext` generic over `SourceConnector` (drop the hard-coded
   `PostgresTableProvider` in `source()`); move `engine` to the crate root.
4. Make `distributed::DistributedContext` take codecs via `SourceConnector::ballista_codecs`;
   move the generic parts to the crate root, leave codecs/descriptor/pool under postgres.
5. Add `source.kind` to `JobConfig` + a connector registry; wire `PostgresConnector`/(later)
   `MysqlConnector` selection.
6. Delete the transitional crate-root shims once no caller depends on them.

Do not start step N+1 until step N compiles green.
