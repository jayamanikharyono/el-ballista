use sqlx::{Postgres, QueryBuilder};

use crate::types::table_metadata::TableMetadata;

pub struct PostgresQueryBuilder;

impl PostgresQueryBuilder {
    pub(crate) fn push_columns(query: &mut QueryBuilder<Postgres>, table: &TableMetadata) {
        // Empty projection (DataFusion prunes the scan to zero columns for `COUNT(*)`,
        // which only needs row counts): select a constant so the SQL stays valid.
        // Values are never read — `RowBatchBuilder` only counts rows when there are
        // no columns — so this changes no semantics, just minimal source I/O.
        if table.columns.is_empty() {
            query.push("1");
            return;
        }
        for (index, column) in table.columns.iter().enumerate() {
            if index > 0 {
                query.push(", ");
            }

            Self::push_identifier(query, &column.column_name);

            if column.data_type == "USER-DEFINED" {
                query.push("::text AS ");

                Self::push_identifier(query, &column.column_name);
            }
        }
    }

    /// Builds `SELECT <cols> FROM <table>` — extracts all rows without date filtering.
    /// Used for full table extracts where no time window is needed.
    pub fn build_full_table(query: &mut QueryBuilder<Postgres>, table: &TableMetadata) {
        query.push("SELECT ");

        Self::push_columns(query, table);

        query.push(" FROM ");

        Self::push_identifier(query, &table.schema_name);

        query.push(".");

        Self::push_identifier(query, &table.table_name);
    }

    /// Builds `SELECT <cols> FROM <table> WHERE <partition_col> >= :lo AND <partition_col> < :hi`
    /// Used for keyset-based parallel extraction with non-overlapping key ranges.
    pub fn build_keyset_partition(
        query: &mut QueryBuilder<Postgres>,
        table: &TableMetadata,
        partition_column: &str,
        lo: i64,
        hi: i64,
    ) {
        query.push("SELECT ");

        Self::push_columns(query, table);

        query.push(" FROM ");

        Self::push_identifier(query, &table.schema_name);

        query.push(".");

        Self::push_identifier(query, &table.table_name);

        query.push(" WHERE ");

        Self::push_identifier(query, partition_column);

        query.push(" >= ");

        query.push_bind(lo);

        query.push(" AND ");

        Self::push_identifier(query, partition_column);

        query.push(" < ");

        query.push_bind(hi);
    }

    pub(crate) fn push_identifier(query: &mut QueryBuilder<Postgres>, identifier: &str) {
        query.push("\"");
        query.push(identifier.replace('"', "\"\""));
        query.push("\"");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnMetadata;

    fn orders_metadata() -> TableMetadata {
        TableMetadata {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![
                ColumnMetadata {
                    column_name: "order_id".to_string(),
                    data_type: "bigint".to_string(),
                    is_nullable: false,
                    numeric_precision: None,
                    numeric_scale: None,
                    udt_name: None,
                    collation_name: None,
                },
                ColumnMetadata {
                    column_name: "status".to_string(),
                    data_type: "USER-DEFINED".to_string(),
                    is_nullable: false,
                    numeric_precision: None,
                    numeric_scale: None,
                    udt_name: Some("order_status".to_string()),
                    collation_name: None,
                },
                ColumnMetadata {
                    column_name: "updated_at".to_string(),
                    data_type: "timestamp with time zone".to_string(),
                    is_nullable: false,
                    numeric_precision: None,
                    numeric_scale: None,
                    udt_name: None,
                    collation_name: None,
                },
            ],
        }
    }

    #[test]
    fn test_build_full_table_shape() {
        let table = orders_metadata();
        let mut qb = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_full_table(&mut qb, &table);

        let sql_str = qb.sql();
        let sql = sql_str.as_str();
        assert!(
            sql.contains(r#"SELECT "order_id", "status"::text AS "status", "updated_at" FROM "public"."orders""#),
            "unexpected full-table SQL: {sql}"
        );
        assert!(!sql.contains("WHERE"), "full scan must not filter: {sql}");
    }

    #[test]
    fn test_build_keyset_partition_shape() {
        let table = orders_metadata();
        let mut qb = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_keyset_partition(&mut qb, &table, "order_id", 1, 25001);

        let sql_str = qb.sql();
        let sql = sql_str.as_str();
        // Non-overlapping, gapless keyset range with bound params.
        assert!(
            sql.contains(r#"WHERE "order_id" >= $1 AND "order_id" < $2"#),
            "unexpected keyset SQL: {sql}"
        );
    }

    #[test]
    fn test_empty_projection_selects_constant() {
        // DataFusion prunes `COUNT(*)` scans to zero columns; emitting nothing
        // would produce `SELECT  FROM`, a Postgres syntax error.
        let table = TableMetadata {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![],
        };
        let mut qb = QueryBuilder::<Postgres>::new("SELECT ");
        PostgresQueryBuilder::push_columns(&mut qb, &table);
        qb.push(" FROM ");
        assert_eq!(
            qb.sql().as_str(),
            r#"SELECT 1 FROM "#,
            "empty projection must select a constant"
        );
    }

    #[test]
    fn test_user_defined_columns_cast_to_text() {
        // Enum (and any USER-DEFINED) columns select as `"col"::text AS "col"` so sqlx can
        // decode them as String; the alias preserves the row-lookup name.
        let mut table = orders_metadata();
        table.columns.retain(|c| c.data_type == "USER-DEFINED");
        let mut qb = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_full_table(&mut qb, &table);

        assert!(
            qb.sql()
                .as_str()
                .contains(r#"SELECT "status"::text AS "status""#),
            "enum column must cast: {}",
            qb.sql().as_str()
        );
    }
}
