//! MySQL extractor (prototype). Connect + full-table extract to Arrow.
//!
//! Columns decode to typed Arrow arrays via [`super::row_adapter`] (width-preserving integers,
//! `Decimal128`, `Boolean`, dates/timestamps, `Binary` for binary/blob, `Utf8` otherwise), from a
//! plain `SELECT` of the raw columns — no `CAST(... AS CHAR)`, which would lossily null out binary
//! values that are invalid in the connection charset. Uses `fetch_all` (not a bounded-memory
//! cursor) — prototype only.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use sqlx::MySqlPool;
use sqlx::mysql::MySqlPoolOptions;

use super::dialect::MysqlDialect;
use super::row_adapter::MysqlRowAdapter;
use super::schema_reader::MysqlSchemaReader;
use crate::connector::errors::ExtractorError;
use crate::pushdown::dialect::SqlDialect;
use crate::types::TableMetadata;

/// A minimal MySQL source connector (prototype).
pub struct MysqlExtractor {
    pool: MySqlPool,
    default_schema: String,
}

impl MysqlExtractor {
    /// Connect to MySQL. `database` is also the default schema for unqualified table names.
    pub async fn connect(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
        pool_max: u32,
    ) -> Result<Self, ExtractorError> {
        let url = format!("mysql://{user}:{password}@{host}:{port}/{database}");
        let pool = MySqlPoolOptions::new()
            .max_connections(pool_max)
            .connect(&url)
            .await?;
        Ok(Self {
            pool,
            default_schema: database.to_string(),
        })
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Full-table extract to a single Arrow `RecordBatch` with typed columns (see module docs).
    pub async fn extract_full_table(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
    ) -> Result<RecordBatch, ExtractorError> {
        let reader = MysqlSchemaReader::new(&self.pool, self.default_schema.clone());
        let metadata = reader
            .get_table_metadata(table_name)
            .await?
            .select_columns(columns.as_deref());

        let sql = Self::build_projection(&metadata);
        // `build_projection` composes only quoted identifiers via `MysqlDialect::quote_ident`
        // (no user-supplied literals), so the assembled SELECT is safe to execute.
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(&self.pool)
            .await?;

        let schema = MysqlRowAdapter::build_arrow_schema(&metadata)?;
        let arrays = metadata
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| MysqlRowAdapter::decode_column(&rows, i, col))
            .collect::<Result<Vec<_>, _>>()?;

        let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;
        Ok(batch)
    }

    /// `SELECT `col`, ... FROM `schema`.`table`` over the raw columns — typed decode happens in
    /// [`MysqlRowAdapter`], so the SQL carries no casts.
    fn build_projection(table: &TableMetadata) -> String {
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
}
