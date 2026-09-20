//! MySQL query building (prototype). Identifier quoting goes through the [`SqlDialect`] so the SQL
//! shape is dialect-driven, not hard-coded — the same seam the Postgres builder uses.
//!
//! [`SqlDialect`]: crate::pushdown::dialect::SqlDialect

use crate::pushdown::dialect::SqlDialect;
use crate::types::TableMetadata;

use super::dialect::MysqlDialect;

/// `SELECT <cols> FROM <schema>.<table>` with MySQL backtick quoting. Full-table read, no filter.
pub fn build_full_table(table: &TableMetadata) -> String {
    let dialect = MysqlDialect;
    let cols = table
        .columns
        .iter()
        .map(|c| dialect.quote_ident(&c.column_name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {cols} FROM {}.{}",
        dialect.quote_ident(&table.schema_name),
        dialect.quote_ident(&table.table_name)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnMetadata;

    fn col(name: &str, ty: &str) -> ColumnMetadata {
        ColumnMetadata {
            column_name: name.to_string(),
            data_type: ty.to_string(),
            is_nullable: true,
            numeric_precision: None,
            numeric_scale: None,
            udt_name: None,
            collation_name: None,
        }
    }

    #[test]
    fn builds_backtick_quoted_select() {
        let table = TableMetadata {
            schema_name: "app".to_string(),
            table_name: "orders".to_string(),
            columns: vec![col("order_id", "bigint"), col("status", "varchar")],
        };
        assert_eq!(
            build_full_table(&table),
            "SELECT `order_id`, `status` FROM `app`.`orders`"
        );
    }
}
