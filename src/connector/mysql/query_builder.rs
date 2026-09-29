//! MySQL query building (prototype). Identifier quoting goes through the [`SqlDialect`] so the SQL
//! shape is dialect-driven, not hard-coded — the same seam the Postgres builder uses.
//!
//! [`SqlDialect`]: crate::pushdown::dialect::SqlDialect

use crate::pushdown::dialect::SqlDialect;
use crate::types::TableMetadata;

use super::dialect::MysqlDialect;

/// `SELECT <cols> FROM <schema>.<table>` with MySQL backtick quoting. Full-table read, no filter,
/// no `ORDER BY` (row order is unspecified). The single SQL builder the extractor uses.
///
/// For every `DATE`/`DATETIME`/`TIMESTAMP` column (see `zero_date_columns`) one marker
/// column `(`col` = 0)` is appended after the projection, in the same order: sqlx decodes a
/// MySQL zero date (`0000-00-00`) as NULL, so the marker is what lets the extractor tell a
/// zero date (an error: no Arrow date represents it) from a real NULL.
///
/// # Examples
///
/// ```
/// use el_ballista::connector::mysql::query_builder::build_full_table;
/// use el_ballista::types::{ColumnMetadata, TableMetadata};
///
/// let table = TableMetadata {
///     schema_name: "app".into(),
///     table_name: "orders".into(),
///     columns: vec![ColumnMetadata {
///         column_name: "id".into(),
///         data_type: "bigint".into(),
///         is_nullable: false,
///         numeric_precision: None,
///         numeric_scale: None,
///         udt_name: None,
///         collation_name: None,
///     }],
/// };
/// assert_eq!(build_full_table(&table), "SELECT `id` FROM `app`.`orders`");
/// ```
pub fn build_full_table(table: &TableMetadata) -> String {
    let dialect = MysqlDialect;
    let markers = zero_date_columns(table).into_iter().map(|i| {
        format!(
            "({} = 0)",
            dialect.quote_ident(&table.columns[i].column_name)
        )
    });
    let cols = table
        .columns
        .iter()
        .map(|c| dialect.quote_ident(&c.column_name))
        .chain(markers)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {cols} FROM {}.{}",
        dialect.quote_ident(&table.schema_name),
        dialect.quote_ident(&table.table_name)
    )
}

/// Indices (into `table.columns`) of the temporal columns that get a zero-date marker, in
/// marker order.
pub(crate) fn zero_date_columns(table: &TableMetadata) -> Vec<usize> {
    table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| matches!(c.data_type.as_str(), "date" | "datetime" | "timestamp"))
        .map(|(i, _)| i)
        .collect()
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

    #[test]
    fn appends_zero_date_markers_for_temporal_columns() {
        let table = TableMetadata {
            schema_name: "app".to_string(),
            table_name: "t".to_string(),
            columns: vec![col("id", "int"), col("d", "date"), col("at", "datetime")],
        };
        assert_eq!(zero_date_columns(&table), vec![1, 2]);
        assert_eq!(
            build_full_table(&table),
            "SELECT `id`, `d`, `at`, (`d` = 0), (`at` = 0) FROM `app`.`t`"
        );
    }
}
