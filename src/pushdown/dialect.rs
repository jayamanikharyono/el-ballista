//! SQL dialect trait and fidelity rules.
//! pushdown/dialect.rs
//! Abstracts dialect-specific rendering, placeholder generation, identifier quoting,
//! and fidelity judgment over column metadata. The prerequisite for multi-connector support
//! and accurate cost-based decisions.

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

/// PostgreSQL SQL dialect.
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn quote_ident(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn placeholder(&self, param_index: usize) -> String {
        format!("${}", param_index)
    }

    fn column_literal_fidelity(
        &self,
        column: &ColumnMetadata,
        literal_is_text: bool,
        literal_is_float: bool,
    ) -> Fidelity {
        if literal_is_float {
            // NaN and infinity ordering differs between Postgres and IEEE-754.
            return Fidelity::Inexact;
        }

        if literal_is_text {
            // Text comparisons depend on collation. Check if column has a known-safe collation.
            if let Some(collation) = &column.collation {
                // "C" and "POSIX" collations are deterministic and Unicode-safe.
                // Binary-safe equality is OK, but ordering might diverge on non-ASCII.
                // Conservative: only equality on deterministic ASCII-safe collations is Exact.
                if collation == "C" || collation == "POSIX" {
                    return Fidelity::Exact;
                }

                // Non-deterministic collations (citext, others) are always Inexact.
                if !is_deterministic_collation(collation) {
                    return Fidelity::Inexact;
                }
            }

            // No collation metadata: conservative assumption is Inexact.
            Fidelity::Inexact
        } else {
            // Integer and boolean comparisons are Exact.
            Fidelity::Exact
        }
    }

    fn column_column_fidelity(
        &self,
        left_column: &ColumnMetadata,
        right_column: &ColumnMetadata,
    ) -> Fidelity {
        // If types differ significantly, be conservative.
        if left_column.data_type != right_column.data_type {
            return Fidelity::Inexact;
        }

        // String types: check collation consistency.
        if is_text_type(&left_column.data_type) {
            let left_collation = left_column.collation.as_deref();
            let right_collation = right_column.collation.as_deref();

            match (left_collation, right_collation) {
                (Some(l), Some(r)) if l == r && (l == "C" || l == "POSIX") => Fidelity::Exact,
                (Some(l), Some(r)) if l == r && is_deterministic_collation(l) => Fidelity::Exact,
                _ => Fidelity::Inexact,
            }
        } else if is_float_type(&left_column.data_type) {
            // Float comparisons always have NaN/infinity hazards.
            Fidelity::Inexact
        } else {
            // Numeric and boolean columns: Exact.
            Fidelity::Exact
        }
    }
}

fn is_text_type(data_type: &str) -> bool {
    matches!(
        data_type,
        "text" | "character varying" | "character" | "citext"
    )
}

fn is_float_type(data_type: &str) -> bool {
    matches!(data_type, "real" | "double precision")
}

fn is_deterministic_collation(collation: &str) -> bool {
    !collation.contains("_") || collation == "C" || collation == "POSIX"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(
        data_type: &str,
        collation: Option<&str>,
        _numeric_precision: Option<i32>,
        _numeric_scale: Option<i32>,
    ) -> ColumnMetadata {
        ColumnMetadata {
            column_name: "test_col".to_string(),
            data_type: data_type.to_string(),
            is_nullable: true,
            numeric_precision: None,
            numeric_scale: None,
            udt_name: None,
            collation: collation.map(String::from),
        }
    }

    #[test]
    fn test_quote_ident() {
        let dialect = PostgresDialect;
        assert_eq!(dialect.quote_ident("simple"), r#""simple""#);
        assert_eq!(dialect.quote_ident(r#"with"quote"#), r#""with""quote""#);
    }

    #[test]
    fn test_placeholder() {
        let dialect = PostgresDialect;
        assert_eq!(dialect.placeholder(1), "$1");
        assert_eq!(dialect.placeholder(42), "$42");
    }

    #[test]
    fn test_column_literal_fidelity() {
        let dialect = PostgresDialect;

        // Float literals are always Inexact due to NaN/infinity.
        let int_col = col("integer", None, None, None);
        assert_eq!(
            dialect.column_literal_fidelity(&int_col, false, true),
            Fidelity::Inexact
        );

        // Integer literals against integer columns: Exact.
        assert_eq!(
            dialect.column_literal_fidelity(&int_col, false, false),
            Fidelity::Exact
        );

        // Text literals against C-collation text columns: Exact.
        let c_text_col = col("text", Some("C"), None, None);
        assert_eq!(
            dialect.column_literal_fidelity(&c_text_col, true, false),
            Fidelity::Exact
        );

        // Text literals against unknown-collation text columns: Inexact (conservative).
        let unknown_text_col = col("text", None, None, None);
        assert_eq!(
            dialect.column_literal_fidelity(&unknown_text_col, true, false),
            Fidelity::Inexact
        );
    }

    #[test]
    fn test_column_column_fidelity() {
        let dialect = PostgresDialect;

        // Same types, same deterministic collation: Exact.
        let col1 = col("text", Some("C"), None, None);
        let col2 = col("text", Some("C"), None, None);
        assert_eq!(
            dialect.column_column_fidelity(&col1, &col2),
            Fidelity::Exact
        );

        // Same types, different collations: Inexact.
        let col3 = col("text", Some("de_DE"), None, None);
        assert_eq!(
            dialect.column_column_fidelity(&col1, &col3),
            Fidelity::Inexact
        );

        // Different types: Inexact.
        let int_col = col("integer", None, None, None);
        assert_eq!(
            dialect.column_column_fidelity(&col1, &int_col),
            Fidelity::Inexact
        );

        // Float columns: always Inexact.
        let float_col1 = col("real", None, None, None);
        let float_col2 = col("real", None, None, None);
        assert_eq!(
            dialect.column_column_fidelity(&float_col1, &float_col2),
            Fidelity::Inexact
        );
    }
}
