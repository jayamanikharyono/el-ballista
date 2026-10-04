//! Source connector SPI (Phase 5 entry ticket).
//!
//! A connector is a backend that can answer four questions without the rest of the
//! system knowing which database it is:
//!
//! 1. **What SQL?** — [`pushdown::Predicate::render_to`] renders pushed filters into a
//!    [`pushdown::SqlSink`]: identifiers, casts and collations from a
//!    [`pushdown::dialect::SqlDialect`], literals bound at render position by the backend's
//!    own sink (Postgres: `connector::postgres::param_sink::PgParamSink`). Fidelity is decided
//!    at translation from the [`pushdown::ColumnKind`] each connector assigns its columns.
//! 2. **What filter?** — caller-provided predicates or ranges. The orchestrator decides
//!    WHAT range to extract; the extraction layer decides HOW to extract it efficiently
//!    (full / filtered scan, partitioning, DataFusion/Ballista execution).
//! 3. **What does it cost?** — `pushdown::stats::TableStatsSource`: table/column
//!    statistics and index metadata for the cost model.
//! 4. **Which pool?** — `SourceDescriptor`: everything needed to open (or find) a
//!    budgeted source pool in the executing process, without ever serializing a password.
//!
//! The planning SPI itself is DataFusion's: connectors implement `TableProvider` /
//! `ExecutionPlan`, and `supports_filters_pushdown` plus DataFusion's own `PushDownFilter`
//! rule work through those traits, not through backend types.
//!
//! Deliberately backend-concrete (not SPI): `sqlx` pools and `QueryBuilder` binding.
//! `PgPool` and `MySqlPool` are distinct types with distinct encode impls; a generic pool
//! enum would add indirection with no current consumer. Each backend therefore owns its own
//! process-wide registry following the `distributed::pool_registry` pattern, and its own
//! thin binder over the shared [`pushdown::SqlParam`](crate::pushdown::SqlParam) list.
//!
//! [`pushdown::Predicate::render_to`]: crate::pushdown::Predicate::render_to
//! [`pushdown::SqlSink`]: crate::pushdown::SqlSink
//! [`pushdown::dialect::SqlDialect`]: crate::pushdown::dialect::SqlDialect
//! [`pushdown::ColumnKind`]: crate::pushdown::ColumnKind

pub mod errors;
pub mod mysql;
pub mod postgres;
pub(crate) mod query_tag;

/// Identifies one budgeted source pool within a process, without carrying anything that can
/// open a connection by itself (no pool handle) or leak a secret (no password — only the
/// environment variable name, resolved in the executing process).
///
/// Contract beyond the method below (enforced by convention, documented here because sqlx
/// keeps pools backend-concrete): the descriptor carries a cluster-wide `pool_max` budget
/// and an `expected_workers` divisor; each process opens at most
/// `max(1, pool_max / max(1, workers))` connections, and the registry key includes that
/// budget so jobs disagreeing about worker count cannot share one wrongly-sized pool.
pub(crate) trait SourceDescriptor: Clone + Send + Sync {
    /// Opaque registry key: two descriptors map to the same pool if and only if their keys
    /// match. Must include the connection budget.
    fn registry_key(&self) -> String;
}
