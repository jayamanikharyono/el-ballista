//! MySQL extractor (prototype). Connect + full-table extract to Arrow.
//!
//! Columns decode to typed Arrow arrays via [`super::row_adapter`] from a plain `SELECT` of the
//! raw columns ([`super::query_builder::build_full_table`]) — no `CAST(... AS CHAR)`, which would
//! lossily null out binary values that are invalid in the connection charset.
//!
//! Memory: [`MysqlExtractor::extract_full_table_for_each_batch`] streams rows with sqlx `fetch`
//! and hands the caller one `RecordBatch` per `batch_size` rows, so at most one batch of rows is
//! resident. [`MysqlExtractor::extract_full_table`] is built on it but **materializes** the whole
//! table into one batch — convenient for small tables and tests only.
//!
//! Consistency: a single `SELECT` on one connection, so the result is whatever that statement
//! sees under the server's isolation level (InnoDB: a consistent read for the statement). No
//! multi-statement snapshot, ordering, or resumability is provided. While a stream is open the
//! server holds the connection; a slow `on_batch` can hit the server's `net_write_timeout`.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use sqlx::MySqlPool;
use sqlx::Row as _;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlRow};

use super::error::MysqlError;
use super::query_builder::{build_full_table, zero_date_columns};
use super::row_adapter::MysqlRowAdapter;
use super::schema_reader::MysqlSchemaReader;
use crate::connector::errors::ExtractorError;
use crate::types::TableMetadata;

/// A minimal MySQL source connector (prototype).
pub struct MysqlExtractor {
    pool: MySqlPool,
    default_schema: String,
}

impl MysqlExtractor {
    /// Connect to MySQL. `database` is also the default schema for unqualified table names.
    ///
    /// Credentials go through [`MySqlConnectOptions`] field by field (never a formatted URL), so
    /// passwords containing `/`, `#`, `?` or `@` work, and nothing credential-bearing is logged.
    /// Every session runs with `time_zone = '+00:00'`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use rust_ballista_extraction_layer::connector::mysql::MysqlExtractor;
    ///
    /// let ex = MysqlExtractor::connect("127.0.0.1", 3306, "root", "p@ss/w#rd", "test", 4).await?;
    /// let batch = ex.extract_full_table("actor", Some(vec!["actor_id"])).await?;
    /// println!("{} rows", batch.num_rows());
    /// # Ok(()) }
    /// ```
    pub async fn connect(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
        pool_max: u32,
    ) -> Result<Self, MysqlError> {
        let mut options = MySqlConnectOptions::new()
            .host(host)
            .port(port)
            .username(user)
            .database(database)
            // Pin the session to UTC (sqlx's default, stated explicitly): `TIMESTAMP` values are
            // converted through the session `time_zone`, so an unpinned session would make the
            // extracted instants depend on server configuration.
            .timezone(Some(String::from("+00:00")));
        if !password.is_empty() {
            options = options.password(password);
        }
        let pool = MySqlPoolOptions::new()
            .max_connections(pool_max)
            .connect_with(options)
            .await?;
        Ok(Self {
            pool,
            default_schema: database.to_string(),
        })
    }

    /// The underlying connection pool.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use rust_ballista_extraction_layer::connector::mysql::MysqlExtractor;
    ///
    /// let password = std::env::var("MYSQL_PASSWORD")?;
    /// let ex = MysqlExtractor::connect("127.0.0.1", 3306, "root", &password, "sakila", 4).await?;
    /// let (version,): (String,) = sqlx::query_as("SELECT VERSION()").fetch_one(ex.pool()).await?;
    /// println!("MySQL {version}");
    /// # Ok(()) }
    /// ```
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Resolve and validate the table + projection: unknown table or unknown requested column is
    /// a typed error (never a silently narrower projection or an empty `SELECT  FROM`).
    async fn resolve(
        &self,
        table_name: &str,
        columns: Option<&[&str]>,
    ) -> Result<TableMetadata, MysqlError> {
        let reader = MysqlSchemaReader::new(&self.pool, self.default_schema.clone());
        let metadata = reader.get_table_metadata(table_name).await?;
        if let Some(names) = columns {
            let missing: Vec<String> = names
                .iter()
                .filter(|n| !metadata.columns.iter().any(|c| c.column_name == **n))
                .map(|n| (*n).to_string())
                .collect();
            if !missing.is_empty() {
                return Err(MysqlError::UnknownColumns {
                    schema: metadata.schema_name,
                    table: metadata.table_name,
                    missing,
                });
            }
        }
        Ok(metadata
            .select_columns(columns)
            .map_err(ExtractorError::from)?)
    }

    /// Stream a full-table extract as `RecordBatch`es of at most `batch_size` rows, calling
    /// `on_batch` for each (bounded memory: one batch of rows resident at a time). Returns the
    /// Arrow schema, which is also correct when the table is empty (then `on_batch` is never
    /// called). The first `Err` — from the source, decode, or `on_batch` — stops the stream and
    /// is returned; a failure is never reported as zero rows.
    ///
    /// # Errors
    ///
    /// [`MysqlError::InvalidBatchSize`] for `batch_size == 0`; [`MysqlError::TableNotFound`] /
    /// [`MysqlError::UnknownColumns`] for a bad table or projection; decode errors per
    /// `MysqlRowAdapter::decode_column`; whatever `on_batch` returns.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use rust_ballista_extraction_layer::connector::mysql::MysqlExtractor;
    ///
    /// let password = std::env::var("MYSQL_PASSWORD")?;
    /// let ex = MysqlExtractor::connect("127.0.0.1", 3306, "root", &password, "test", 4).await?;
    /// let mut rows = 0;
    /// ex.extract_full_table_for_each_batch("rental", None, 8192, |batch| {
    ///     rows += batch.num_rows();
    ///     Ok(())
    /// })
    /// .await?;
    /// # Ok(()) }
    /// ```
    pub async fn extract_full_table_for_each_batch<F>(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        batch_size: usize,
        mut on_batch: F,
    ) -> Result<SchemaRef, MysqlError>
    where
        F: FnMut(RecordBatch) -> Result<(), MysqlError>,
    {
        if batch_size == 0 {
            return Err(MysqlError::InvalidBatchSize);
        }
        let metadata = self.resolve(table_name, columns.as_deref()).await?;
        let schema: SchemaRef = Arc::new(MysqlRowAdapter::build_arrow_schema(&metadata)?);

        let sql = build_full_table(&metadata);
        // `build_full_table` composes only quoted identifiers via `MysqlDialect::quote_ident`
        // (no user-supplied literals), so the assembled SELECT is safe to execute.
        let mut stream = sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).fetch(&self.pool);

        let mut buf: Vec<MySqlRow> = Vec::with_capacity(batch_size);
        while let Some(row) = stream.try_next().await? {
            buf.push(row);
            if buf.len() == batch_size {
                on_batch(Self::to_batch(&schema, &metadata, &buf)?)?;
                buf.clear();
            }
        }
        if !buf.is_empty() {
            on_batch(Self::to_batch(&schema, &metadata, &buf)?)?;
        }
        Ok(schema)
    }

    /// Full-table extract **materialized** into a single Arrow `RecordBatch` (see module docs).
    /// Built on [`Self::extract_full_table_for_each_batch`]; prefer that for large tables.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use rust_ballista_extraction_layer::connector::mysql::MysqlExtractor;
    ///
    /// let password = std::env::var("MYSQL_PASSWORD")?;
    /// let ex = MysqlExtractor::connect("127.0.0.1", 3306, "root", &password, "test", 4).await?;
    /// let batch = ex.extract_full_table("test.actor", None).await?;
    /// # Ok(()) }
    /// ```
    pub async fn extract_full_table(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
    ) -> Result<RecordBatch, MysqlError> {
        let mut batches = Vec::new();
        let schema = self
            .extract_full_table_for_each_batch(table_name, columns, MATERIALIZE_BATCH_ROWS, |b| {
                batches.push(b);
                Ok(())
            })
            .await?;
        Ok(arrow::compute::concat_batches(&schema, &batches)?)
    }

    fn to_batch(
        schema: &SchemaRef,
        metadata: &TableMetadata,
        rows: &[MySqlRow],
    ) -> Result<RecordBatch, MysqlError> {
        // Zero-date markers follow the projection (see `build_full_table`).
        let first_marker = metadata.columns.len();
        for (k, &i) in zero_date_columns(metadata).iter().enumerate() {
            for r in rows {
                let is_zero: Option<i64> =
                    r.try_get(first_marker + k)
                        .map_err(|e| MysqlError::Decode {
                            column: metadata.columns[i].column_name.clone(),
                            source: e,
                        })?;
                if is_zero == Some(1) {
                    return Err(MysqlError::ZeroDate {
                        column: metadata.columns[i].column_name.clone(),
                    });
                }
            }
        }
        let arrays = metadata
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| MysqlRowAdapter::decode_column(rows, i, col))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecordBatch::try_new(Arc::clone(schema), arrays)?)
    }
}

/// Chunk size used internally by [`MysqlExtractor::extract_full_table`] before concatenation.
const MATERIALIZE_BATCH_ROWS: usize = 8192;
