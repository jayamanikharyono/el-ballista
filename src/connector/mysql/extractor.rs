//! MySQL extractor (prototype). Connect + full-table extract to Arrow.
//!
//! PROTOTYPE SIMPLIFICATION: every column is materialized as Arrow `Utf8` via `CAST(col AS CHAR)`,
//! keeping decoding to `Option<String>` (robust across MySQL types) so the walking skeleton runs
//! end-to-end without a full per-type decode path. The intended typed mapping lives in
//! [`super::type_mapper`]; wiring typed Arrow builders is the next step. Uses `fetch_all` (not a
//! bounded-memory cursor) — prototype only.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlPool, Row};

use super::dialect::MysqlDialect;
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

    /// Full-table extract to a single Arrow `RecordBatch` (all columns `Utf8` — see module docs).
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

        let sql = Self::build_string_projection(&metadata);
        // `build_string_projection` composes only quoted identifiers via `MysqlDialect::quote_ident`
        // (no user-supplied literals), so the assembled SELECT is safe to execute.
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(&self.pool)
            .await?;

        let mut builders: Vec<StringBuilder> = metadata
            .columns
            .iter()
            .map(|_| StringBuilder::new())
            .collect();

        for row in &rows {
            for (i, builder) in builders.iter_mut().enumerate() {
                let value: Option<String> = row.try_get(i)?;
                match value {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
        }

        let fields: Vec<Field> = metadata
            .columns
            .iter()
            .map(|c| Field::new(c.column_name.clone(), DataType::Utf8, c.is_nullable))
            .collect();
        let arrays: Vec<ArrayRef> = builders
            .into_iter()
            .map(|mut b| Arc::new(b.finish()) as ArrayRef)
            .collect();

        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
        Ok(batch)
    }

    /// `SELECT CAST(`col` AS CHAR) AS `col`, ... FROM `schema`.`table`` — prototype string
    /// materialization so every value decodes as `Option<String>`.
    fn build_string_projection(table: &TableMetadata) -> String {
        let dialect = MysqlDialect;
        let cols = table
            .columns
            .iter()
            .map(|c| {
                let ident = dialect.quote_ident(&c.column_name);
                format!("CAST({ident} AS CHAR) AS {ident}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT {cols} FROM {}.{}",
            dialect.quote_ident(&table.schema_name),
            dialect.quote_ident(&table.table_name)
        )
    }
}
