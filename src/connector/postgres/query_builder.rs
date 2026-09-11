use chrono::{DateTime,Utc};
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

        Self::push_identifier(
            query,
            &table.schema_name,
        );

        query.push(".");

        Self::push_identifier(
            query,
            &table.table_name,
        );

        query.push(" WHERE ");

        Self::push_identifier(
            query,
            timestamp_column,
        );

        query.push(" > ");

        query.push_bind(lo);

        query.push(" AND ");

        Self::push_identifier(
            query,
            timestamp_column,
        );

        query.push(" <= ");

        query.push_bind(hi);
    }

    pub(crate) fn push_columns(
        query: &mut QueryBuilder<Postgres>,
        table: &TableMetadata,
    ) {
        for (index, column) in table.columns.iter().enumerate() {
            if index > 0 {
                query.push(", ");
            }

            Self::push_identifier(
                query,
                &column.column_name,
            );

            if column.data_type == "USER-DEFINED" {
                query.push("::text AS ");

                Self::push_identifier(
                    query,
                    &column.column_name,
                );
            }
        }
    }

    /// Builds `SELECT <cols> FROM <table>` — extracts all rows without date filtering.
    /// Used for full table extracts where no time window is needed.
    pub fn build_full_table(
        query: &mut QueryBuilder<Postgres>,
        table: &TableMetadata,
    ) {
        query.push("SELECT ");

        Self::push_columns(query, table);

        query.push(" FROM ");

        Self::push_identifier(
            query,
            &table.schema_name,
        );

        query.push(".");

        Self::push_identifier(
            query,
            &table.table_name,
        );
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

        Self::push_identifier(
            query,
            &table.schema_name,
        );

        query.push(".");

        Self::push_identifier(
            query,
            &table.table_name,
        );

        query.push(" WHERE ");

        Self::push_identifier(
            query,
            partition_column,
        );

        query.push(" >= ");

        query.push_bind(lo);

        query.push(" AND ");

        Self::push_identifier(
            query,
            partition_column,
        );

        query.push(" < ");

        query.push_bind(hi);
    }

    pub(crate) fn push_identifier(
        query: &mut QueryBuilder<Postgres>,
        identifier: &str,
    ) {
        query.push("\"");
        query.push(identifier.replace('"', "\"\""));
        query.push("\"");
    }
}