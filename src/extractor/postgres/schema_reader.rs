//! Postgres Schema Reader
//! extractor/postgres/schema_reader.rs
//! This module is used to read the PostgreSQL table schema.
//! It is used to read the PostgreSQL table schema.

use sqlx::PgPool;

use crate::types::{ColumnMetadata, TableMetadata};

pub struct PostgresSchemaReader<'a> {
    pool: &'a PgPool,
}

impl<'a> PostgresSchemaReader<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_table_schema(
        &self,
        table_name: &str,
    ) -> Result<Vec<ColumnMetadata>, sqlx::Error> {
        let columns = sqlx::query_as::<_, ColumnMetadata>(
            r#"
            SELECT
                column_name,
                data_type,
                is_nullable = 'YES' AS is_nullable,
                numeric_precision,
                numeric_scale,
                udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = $1
            ORDER BY ordinal_position
            "#,
        )
        .bind(table_name)
        .fetch_all(self.pool)
        .await?;

        Ok(columns)
    }

    pub async fn get_table_metadata(
        &self,
        table_name: &str,
    ) -> Result<TableMetadata, sqlx::Error> {
        let columns = self.get_table_schema(table_name).await?;

        let table_metadata : TableMetadata = TableMetadata{
            schema_name: "public".to_string(),
            table_name: table_name.to_string(),
            columns: columns
        };

        Ok(table_metadata)
    }
}