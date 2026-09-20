//! MySQL SQL dialect (prototype) — a second implementation of [`SqlDialect`], added to validate
//! the trait against a non-Postgres backend.
//!
//! NOTE: `SqlDialect`/`Fidelity` are imported from `crate::pushdown` (currently physically under
//! `connector::postgres::pushdown`). That cross-connector dependency is exactly the signal to lift
//! the trait into a shared module — see `connector/mysql/mod.rs`.

use crate::pushdown::Fidelity;
use crate::pushdown::dialect::SqlDialect;
use crate::types::ColumnMetadata;

/// MySQL dialect: backtick identifier quoting and positional `?` placeholders.
pub struct MysqlDialect;

impl SqlDialect for MysqlDialect {
    fn quote_ident(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    fn placeholder(&self, _param_index: usize) -> String {
        // MySQL uses positional `?` placeholders (unlike Postgres `$1`).
        "?".to_string()
    }

    fn column_literal_fidelity(
        &self,
        _column: &ColumnMetadata,
        literal_is_text: bool,
        literal_is_float: bool,
    ) -> Fidelity {
        // Conservative for a prototype: float ordering (NaN/inf) and text collation semantics
        // (MySQL defaults are commonly case-insensitive, e.g. utf8mb4_0900_ai_ci) can diverge
        // from Rust's, so treat those as Inexact and let the engine keep the predicate.
        if literal_is_float || literal_is_text {
            Fidelity::Inexact
        } else {
            Fidelity::Exact
        }
    }

    fn column_column_fidelity(
        &self,
        _left_column: &ColumnMetadata,
        _right_column: &ColumnMetadata,
    ) -> Fidelity {
        // Collation/charset interplay between two columns is not modeled yet: be conservative.
        Fidelity::Inexact
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_with_backticks_and_escapes() {
        let d = MysqlDialect;
        assert_eq!(d.quote_ident("orders"), "`orders`");
        assert_eq!(d.quote_ident("we`ird"), "`we``ird`");
    }

    #[test]
    fn placeholder_is_question_mark() {
        let d = MysqlDialect;
        assert_eq!(d.placeholder(1), "?");
        assert_eq!(d.placeholder(9), "?");
    }
}
