//! MySQL SQL dialect (prototype) — a second implementation of [`SqlDialect`], added to validate
//! the trait against a non-Postgres backend.
//!
//! NOTE: `SqlDialect` is imported from the shared, engine-agnostic `crate::pushdown` module.

use crate::pushdown::dialect::SqlDialect;
use crate::pushdown::{CastType, Collation};

/// MySQL dialect: backtick identifier quoting and positional `?` placeholders.
pub struct MysqlDialect;

impl SqlDialect for MysqlDialect {
    fn quote_ident(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    // TODO(mysql-pushdown): MySQL has no pushdown path yet. Column collations (e.g.
    // `utf8mb4_0900_ai_ci` vs `_bin`) must be compared with DataFusion's byte-wise semantics
    // before any text predicate is ever pushed as `Exact`.
    fn cast_type_name(&self, to: CastType) -> &'static str {
        match to {
            CastType::Text => "CHAR",
        }
    }

    fn collation_name(&self, collation: Collation) -> &'static str {
        match collation {
            Collation::Binary => "utf8mb4_bin",
        }
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
}
