//! Postgres Schema Reader
//! extractor/postgres/schema_reader.rs
//! This module is used to read the PostgreSQL table schema.
//! It is used to read the PostgreSQL table schema.

use sqlx::PgPool;

use crate::types::{ColumnMetadata, TableMetadata};

pub struct PostgresSchemaReader<'a> {
    pool: &'a PgPool,
    schema_name: String,
}

impl<'a> PostgresSchemaReader<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self {
            pool,
            schema_name: "public".to_string(),
        }
    }

    #[allow(dead_code)]
    pub fn with_schema(pool: &'a PgPool, schema_name: impl Into<String>) -> Self {
        Self {
            pool,
            schema_name: schema_name.into(),
        }
    }

    pub async fn get_table_schema(
        &self,
        table_name: &str,
    ) -> Result<Vec<ColumnMetadata>, sqlx::Error> {
        let (schema, table) = match table_name.split_once('.') {
            Some((s, t)) => (s, t),
            None => (self.schema_name.as_str(), table_name),
        };

        let columns = sqlx::query_as::<_, ColumnMetadata>(
            r#"
            SELECT
                column_name,
                data_type,
                is_nullable = 'YES' AS is_nullable,
                numeric_precision,
                numeric_scale,
                udt_name,
                collation_name AS collation
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = $2
            ORDER BY ordinal_position
            "#,
        )
        .bind(schema)
        .bind(table)
        .fetch_all(self.pool)
        .await?;

        Ok(columns)
    }

    pub async fn get_table_metadata(&self, table_name: &str) -> Result<TableMetadata, sqlx::Error> {
        let (schema, table) = match table_name.split_once('.') {
            Some((s, t)) => (s, t),
            None => (self.schema_name.as_str(), table_name),
        };

        let columns = self.get_table_schema(table_name).await?;

        let table_metadata = TableMetadata {
            schema_name: schema.to_string(),
            table_name: table.to_string(),
            columns,
        };

        Ok(table_metadata)
    }
}
