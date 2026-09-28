use sqlx::{Postgres, QueryBuilder};

use crate::types::ColumnMetadata;
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

            if Self::selects_as_text(column) {
                query.push("::text AS ");

                Self::push_identifier(query, &column.column_name);
            }
        }
    }

    /// Columns selected as `"col"::text AS "col"` and decoded as UTF-8 on both the cursor
    /// and the COPY path:
    /// - enums / other `USER-DEFINED` types: no fixed binary layout to decode;
    /// - `json` / `jsonb`: Postgres' own text rendering, byte for byte (no
    ///   `serde_json::Value` round trip that loses big-number precision and key order;
    ///   no jsonb version byte);
    /// - `uuid`: the canonical 36-char form without a per-cell `Uuid::to_string`.
    ///
    /// The alias keeps the column name; ordinal decoding does not depend on it.
    pub(crate) fn selects_as_text(column: &ColumnMetadata) -> bool {
        matches!(
            column.data_type.as_str(),
            "USER-DEFINED" | "json" | "jsonb" | "uuid"
        )
    }

    /// Builds `SELECT <cols> FROM <table> [WHERE (<partition predicate>)]`. The predicate
    /// comes from `parallel::compute_keyset_partitions` / `compute_ctid_partitions`
    /// (integer or tid literals and quoted identifiers composed by this crate, never user
    /// text), which is why it can be inlined — the same trust as the execution plan's
    /// partition bounds. It is parenthesized because keyset predicates contain `OR`
    /// (`… OR "col" IS NULL` on the first partition).
    pub(crate) fn build_partition(
        query: &mut QueryBuilder<Postgres>,
        table: &TableMetadata,
        predicate: Option<&str>,
    ) {
        Self::build_full_table(query, table);
        if let Some(predicate) = predicate {
            query.push(" WHERE (");
            query.push(predicate);
            query.push(")");
        }
    }

    /// Builds `SELECT <cols> FROM <table>` — extracts all rows without date filtering.
    /// Used for full table extracts where no time window is needed.
    pub(crate) fn build_full_table(query: &mut QueryBuilder<Postgres>, table: &TableMetadata) {
        query.push("SELECT ");

        Self::push_columns(query, table);

        query.push(" FROM ");

        Self::push_identifier(query, &table.schema_name);

        query.push(".");

        Self::push_identifier(query, &table.table_name);
    }

    /// Builds `SELECT <cols> FROM <table> WHERE <partition_col> >= :lo AND <partition_col> < :hi`
    /// for an explicit caller-chosen half-open range. Rows whose key is NULL match no such
    /// range; computed partitions (which cover NULL keys and an open-ended tail) go through
    /// [`Self::build_partition`] instead.
    pub(crate) fn build_keyset_partition(
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

    /// Builds `SELECT <cols> FROM <table> WHERE <partition_col> >= lo AND <partition_col> < hi`
    /// with the bounds **inlined as integer literals** instead of `$1/$2` binds.
    /// `COPY (SELECT …)` accepts no bind parameters, so the COPY path renders bounds
    /// this way; `lo`/`hi` are `i64` values this crate computed itself (MIN/MAX math),
    /// never user text — the same trust level as the inlined partition predicates in
    /// `PostgresExecutionPlan::build_query`.
    pub(crate) fn build_keyset_partition_inline(
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

        query.push(format!(" >= {lo} AND "));

        Self::push_identifier(query, partition_column);

        query.push(format!(" < {hi}"));
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
    fn test_build_keyset_partition_inline_inlines_integer_bounds() {
        let table = orders_metadata();
        let mut qb = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_keyset_partition_inline(&mut qb, &table, "order_id", 1, 25001);

        let sql = qb.sql();
        let sql = sql.as_str();
        // Integer bounds inlined: COPY accepts no bind parameters, and i64
        // rendering is total (no quoting/escaping surface at all).
        assert!(
            sql.contains(r#"WHERE "order_id" >= 1 AND "order_id" < 25001"#),
            "unexpected inline keyset SQL: {sql}"
        );
        assert!(!sql.contains('$'), "inline form must bind nothing: {sql}");
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

    #[test]
    fn test_build_partition_parenthesizes_predicate() {
        let table = orders_metadata();
        let mut qb = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_partition(
            &mut qb,
            &table,
            Some(r#"("order_id" >= 1 AND "order_id" < 5) OR "order_id" IS NULL"#),
        );
        assert!(
            qb.sql().as_str().ends_with(
                r#"FROM "public"."orders" WHERE (("order_id" >= 1 AND "order_id" < 5) OR "order_id" IS NULL)"#
            ),
            "{}",
            qb.sql().as_str()
        );
        let mut qb = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_partition(&mut qb, &table, None);
        assert!(!qb.sql().as_str().contains("WHERE"));
    }
}
