//! Postgres Schema Reader
//! extractor/postgres/schema_reader.rs
//! This module is used to read the PostgreSQL table schema.
//! It is used to read the PostgreSQL table schema.

use sqlx::PgPool;

use crate::connector::errors::ExtractorError;
use crate::types::{ColumnMetadata, TableMetadata};

pub struct PostgresSchemaReader<'a> {
    pool: &'a PgPool,
    schema_name: String,
}

impl<'a> PostgresSchemaReader<'a> {
    /// A reader over `pool`; unqualified table names resolve in the `public` schema.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use el_ballista::connector::errors::ExtractorError;
    /// # use el_ballista::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// use el_ballista::connector::postgres::schema_reader::PostgresSchemaReader;
    ///
    /// let meta = PostgresSchemaReader::new(ex.pool()).get_table_metadata("orders").await?;
    /// for c in &meta.columns {
    ///     println!("{} {} nullable={}", c.column_name, c.data_type, c.is_nullable);
    /// }
    /// # Ok(()) }
    /// ```
    pub fn new(pool: &'a PgPool) -> Self {
        Self {
            pool,
            schema_name: "public".to_string(),
        }
    }

    /// A reader over `pool`; unqualified table names resolve in `schema_name`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use el_ballista::connector::errors::ExtractorError;
    /// # use el_ballista::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// use el_ballista::connector::postgres::schema_reader::PostgresSchemaReader;
    ///
    /// let reader = PostgresSchemaReader::with_schema(ex.pool(), "sales");
    /// let meta = reader.get_table_metadata("orders").await?; // sales.orders
    /// for c in &meta.columns {
    ///     println!("{} {} nullable={}", c.column_name, c.data_type, c.is_nullable);
    /// }
    /// # Ok(()) }
    /// ```
    pub fn with_schema(pool: &'a PgPool, schema_name: impl Into<String>) -> Self {
        Self {
            pool,
            schema_name: schema_name.into(),
        }
    }

    pub(crate) async fn get_table_schema(
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
                collation_name
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

    /// Read a table's column metadata (ordinal order).
    ///
    /// Errors with [`ExtractorError::TableNotFound`] when `information_schema.columns` has
    /// no rows for it — a missing (or invisible) table must never look like a zero-column
    /// table, or a provider would register an empty schema and every scan would return
    /// zero rows.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use el_ballista::connector::errors::ExtractorError;
    /// # use el_ballista::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// use el_ballista::connector::postgres::schema_reader::PostgresSchemaReader;
    ///
    /// let meta = PostgresSchemaReader::new(ex.pool()).get_table_metadata("public.orders").await?;
    /// for c in &meta.columns {
    ///     println!("{} {} nullable={}", c.column_name, c.data_type, c.is_nullable);
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn get_table_metadata(
        &self,
        table_name: &str,
    ) -> Result<TableMetadata, ExtractorError> {
        let (schema, table) = match table_name.split_once('.') {
            Some((s, t)) => (s, t),
            None => (self.schema_name.as_str(), table_name),
        };

        let columns = self.get_table_schema(table_name).await?;
        if columns.is_empty() {
            return Err(ExtractorError::TableNotFound(format!("{schema}.{table}")));
        }

        let table_metadata = TableMetadata {
            schema_name: schema.to_string(),
            table_name: table.to_string(),
            columns,
        };

        Ok(table_metadata)
    }
}
