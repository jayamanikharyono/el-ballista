//! SQL dialect trait for rendering the pushdown IR.
//! pushdown/dialect.rs
//! Abstracts dialect-specific rendering: identifier quoting, placeholder generation, cast
//! target names, and the name of the dialect's bytewise collation. Connector-agnostic: each
//! backend implements [`SqlDialect`] under its own connector (`PostgresDialect`,
//! `MysqlDialect`). Fidelity is *not* a dialect question here: it is decided once, during
//! translation, from the engine-neutral [`ColumnKind`](crate::pushdown::ColumnKind) each
//! connector assigns to its columns.

use crate::pushdown::{CastType, Collation};

/// A SQL dialect's rendering conventions for the pushdown IR.
pub trait SqlDialect: Send + Sync {
    /// Render an identifier (table/column name) with dialect-specific quoting.
    fn quote_ident(&self, name: &str) -> String;

    /// Generate a placeholder string for the Nth parameter (1-indexed in most dialects).
    fn placeholder(&self, param_index: usize) -> String;

    /// SQL type name for a `CAST(... AS <name>)` target.
    fn cast_type_name(&self, to: CastType) -> &'static str;

    /// SQL text that follows `COLLATE` for `collation` (already quoted if the dialect needs
    /// it). [`Collation::Binary`] must compare strings byte-by-byte on UTF-8, which is the
    /// ordering Arrow/Rust use — translation relies on this to mark text comparisons `Exact`.
    fn collation_name(&self, collation: Collation) -> &'static str;
}
