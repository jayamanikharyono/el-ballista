//! SQL dialect trait and fidelity rules.
//! pushdown/dialect.rs
//! Abstracts dialect-specific rendering, placeholder generation, identifier quoting, and fidelity
//! judgment over column metadata. Connector-agnostic: each backend implements [`SqlDialect`] under
//! its own connector (`PostgresDialect`, `MysqlDialect`).

use crate::pushdown::Fidelity;
use crate::types::ColumnMetadata;

/// A SQL dialect's rendering conventions and fidelity rules.
/// Implemented per connector (PostgresDialect, MysqlDialect, etc.) to handle
/// dialect-specific SQL generation, collation semantics, and type coercion hazards.
pub trait SqlDialect: Send + Sync {
    /// Render an identifier (table/column name) with dialect-specific quoting.
    fn quote_ident(&self, name: &str) -> String;

    /// Generate a placeholder string for the Nth parameter (1-indexed in most dialects).
    fn placeholder(&self, param_index: usize) -> String;

    /// Judge the fidelity of a comparison between a literal value and a column type,
    /// consulting the column's metadata (collation, precision, unsigned, etc.) to apply
    /// dialect-specific rules beyond what the literal type alone can determine.
    fn column_literal_fidelity(
        &self,
        column: &ColumnMetadata,
        literal_is_text: bool,
        literal_is_float: bool,
    ) -> Fidelity;

    /// Judge the fidelity of a column-to-column comparison, considering both columns'
    /// metadata (collation, type, precision).
    fn column_column_fidelity(
        &self,
        left_column: &ColumnMetadata,
        right_column: &ColumnMetadata,
    ) -> Fidelity;
}

// PostgreSQL's dialect lives with its connector; re-exported here so existing
// `pushdown::dialect::PostgresDialect` paths keep resolving during the connector migration.
pub use crate::connector::postgres::dialect::PostgresDialect;
