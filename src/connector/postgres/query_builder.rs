use chrono::{DateTime, Utc};
use sqlx::{Postgres, QueryBuilder};

use crate::types::table_metadata::TableMetadata;

pub struct PostgresQueryBuilder;

impl PostgresQueryBuilder {
    /// Builds `SELECT <cols> FROM <table> WHERE <ts> > :lo AND <ts> <= :hi` — the half-open
    /// `[lo, hi]` window from docs/incremental-extraction.md §2. The low bound is strict and the
    /// high bound is inclusive so consecutive windows neither overlap nor gap.
    pub fn build_incremental(
        query: &mut QueryBuilder<Postgres>,
        table: &TableMetadata,
        timestamp_column: &str,
        lo: DateTime<Utc>,
        hi: DateTime<Utc>,
    ) {
        query.push("SELECT ");

        Self::push_columns(query, table);

        query.push(" FROM ");

        Self::push_identifier(query, &table.schema_name);

        query.push(".");

        Self::push_identifier(query, &table.table_name);

        query.push(" WHERE ");

        Self::push_identifier(query, timestamp_column);

        query.push(" > ");

        query.push_bind(lo);

        query.push(" AND ");

        Self::push_identifier(query, timestamp_column);

        query.push(" <= ");

        query.push_bind(hi);
    }

    pub(crate) fn push_columns(query: &mut QueryBuilder<Postgres>, table: &TableMetadata) {
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
    use chrono::{TimeZone, Utc};

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
    fn test_build_incremental_shape() {
        let table = orders_metadata();
        let mut qb = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_incremental(
            &mut qb,
            &table,
            "updated_at",
            Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap(),
        );

        let sql_str = qb.sql();
        let sql = sql_str.as_str();
        // Half-open window with bound params, in order.
        assert!(
            sql.contains(r#"SELECT "order_id", "status"::text AS "status", "updated_at" FROM "public"."orders" WHERE "updated_at" > $1 AND "updated_at" <= $2"#),
            "unexpected incremental SQL: {sql}"
        );
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
