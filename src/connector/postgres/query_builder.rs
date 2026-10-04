use sqlx::{Postgres, QueryBuilder};

use crate::connector::postgres::arrow_type_mapper::selects_as_text;
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

            if selects_as_text(column) {
                query.push("::text AS ");

                Self::push_identifier(query, &column.column_name);
            }
        }
    }

    /// Builds `SELECT <cols> FROM <table>`: the start of every scan query (the execution
    /// plan appends its pushed filters, partition predicate and limit).
    pub(crate) fn build_full_table(query: &mut QueryBuilder<Postgres>, table: &TableMetadata) {
        query.push("SELECT ");

        Self::push_columns(query, table);

        query.push(" FROM ");

        Self::push_identifier(query, &table.schema_name);

        query.push(".");

        Self::push_identifier(query, &table.table_name);
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

    #[test]
    fn test_json_jsonb_uuid_cast_to_text() {
        let mut table = orders_metadata();
        for (name, ty) in [("meta", "jsonb"), ("raw", "json"), ("uid", "uuid")] {
            let mut c = table.columns[0].clone();
            c.column_name = name.to_string();
            c.data_type = ty.to_string();
            table.columns.push(c);
        }
        let mut qb = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_full_table(&mut qb, &table);
        let sql = qb.sql();
        let sql = sql.as_str();
        for name in ["meta", "raw", "uid"] {
            assert!(
                sql.contains(&format!(r#""{name}"::text AS "{name}""#)),
                "{name} must select as text: {sql}"
            );
        }
        assert!(sql.contains(r#""order_id", "#), "{sql}");
        assert!(!sql.contains(r#""order_id"::text"#), "{sql}");
    }
}
