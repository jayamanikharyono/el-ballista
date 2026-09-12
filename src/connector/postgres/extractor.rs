//! PostgreSQL extractor.
//! extractor/postgres/extractor.rs
//! Orchestrates PostgreSQL schema discovery, query execution,
//! and conversion of PostgreSQL rows into Arrow `RecordBatch` values.
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgConnection, PgRow};
use sqlx::{PgPool, Postgres, QueryBuilder, Connection, Executor, Row, Transaction};
use tokio_postgres::{Config, CopyOutStream, Row as TokioPgRow};
use tokio_postgres::binary_copy::BinaryCopyOutRow;
use uuid::Uuid;
use bigdecimal::{BigDecimal, ToPrimitive};
use bytes::Bytes;

use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, ArrayBuilder, Int16Builder, Int32Builder, Int64Builder, Float32Builder, Float64Builder, BooleanBuilder, StringBuilder, BinaryBuilder, Date32Builder, TimestampMicrosecondBuilder, Decimal128Builder, ListBuilder};
use arrow::datatypes::{DataType, TimeUnit, Schema, Field};
use std::sync::Arc;
use futures::StreamExt;

use crate::connector::postgres::{
    query_builder::PostgresQueryBuilder,
    row_adapter::PostgresRowAdapter,
    schema_reader::PostgresSchemaReader,
    arrow_type_mapper::ArrowTypeMapper,
};

use crate::{
    connector::errors::ExtractorError,
    types::table_metadata::TableMetadata
};


pub struct PostgresExtractor {
    pool: PgPool,
}

impl PostgresExtractor {
    /// Connect with the session-hygiene settings docs/connectors/postgres.md §6 treats as
    /// mandatory, applied on *every* connection the pool opens (not just the first one): an
    /// identifiable `application_name`, UTC session time zone, a statement timeout, an
    /// idle-in-transaction timeout, and a lock timeout so we never queue behind a DDL lock.
    pub async fn connect(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
        pool_max: u32,
        statement_timeout_ms: u64,
        application_name: &str,
    ) -> Result<Self, sqlx::Error> {
        let connect_options = PgConnectOptions::new()
            .host(host)
            .port(port)
            .username(user)
            .password(password)
            .database(database)
            .application_name(application_name);

        let statement_timeout_setting = format!("{statement_timeout_ms}ms");

        let pool = PgPoolOptions::new()
            .max_connections(pool_max)
            .after_connect(move |conn, _meta| {
                let statement_timeout_setting = statement_timeout_setting.clone();
                Box::pin(async move {
                    sqlx::query("SET TIME ZONE 'UTC'")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "SET statement_timeout = '{statement_timeout_setting}'"
                    )))
                    .execute(&mut *conn)
                    .await?;
                    sqlx::query("SET idle_in_transaction_session_timeout = '60s'")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("SET lock_timeout = '5s'")
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(connect_options)
            .await?;

        Ok(Self { pool })
    }

    /// The underlying pool, for callers (e.g. the safe-high-watermark query in
    /// `crate::incremental`) that need to run something other than a table scan.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Extract via binary COPY — highest throughput path for full table scans.
    /// Uses `COPY (SELECT ...) TO STDOUT (FORMAT BINARY)` and decodes PostgreSQL's
    /// binary wire format directly into Arrow arrays. Memory bounded to one batch.
    pub async fn extract_full_table_via_copy(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>, ExtractorError> {
        let schema_reader = PostgresSchemaReader::new(&self.pool);
        let table_metadata = schema_reader.get_table_metadata(table_name).await?;
        let table_metadata = table_metadata.select_columns(columns.as_deref());
        let arrow_schema = PostgresRowAdapter::build_arrow_schema(&table_metadata)?;

        // Build the COPY query
        let mut query_builder = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_full_table(&mut query_builder, &table_metadata);
        let sql_str = query_builder.sql();
        let copy_sql = format!("COPY ({}) TO STDOUT (FORMAT BINARY)", sql_str.as_str());

        // Create a direct tokio-postgres connection for COPY
        let pg_config = self.build_pg_config().await?;
        let (client, connection) = pg_config.connect(tokio_postgres::NoTls).await
            .map_err(|e| ExtractorError::Internal(format!("pg connect failed: {e}")))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("postgres connection error: {e}");
            }
        });

        let copy_stream = client.copy_out(&copy_sql).await
            .map_err(|e| ExtractorError::Internal(format!("COPY failed: {e}")))?;

        let batches = Self::decode_binary_copy_stream(copy_stream, &arrow_schema, &table_metadata, batch_size).await?;
        Ok(batches)
    }

    /// Build tokio-postgres Config from connection parameters.
    async fn build_pg_config(&self) -> Result<Config, ExtractorError> {
        let mut conn = self.pool.acquire().await?;
        let row = sqlx::query(
            "SELECT current_user, inet_server_addr()::text as host, inet_server_port() as port, current_database()"
        )
        .fetch_one(&mut *conn)
        .await?;

        let user: String = row.try_get("current_user")?;
        let host: String = row.try_get("host")?;
        let port: i32 = row.try_get("port")?;
        let database: String = row.try_get("current_database")?;

        // Note: password is not retrievable from the pool, so we need it from the env
        // The caller should ensure the same password_env is available
        let password = std::env::var("PGPASSWORD")
            .or_else(|_| std::env::var("POSTGRES_PASSWORD"))
            .unwrap_or_default();

        let mut config = Config::new();
        config.host(&host);
        config.port(port as u16);
        config.user(&user);
        config.dbname(&database);
        config.password(&password);
        config.application_name("rust-extract-layer-copy");

        Ok(config)
    }

    /// Extract via server-side cursor (portal) — true streaming with bounded memory.
    /// Uses `DECLARE CURSOR ... WITH HOLD` + `FETCH FORWARD n` + `CLOSE`.
    /// Each FETCH returns up to `batch_size` rows, processed incrementally.
    pub async fn extract_incremental_via_cursor(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        timestamp_column: &str,
        lo: DateTime<Utc>,
        hi: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>, ExtractorError> {
        let schema_reader = PostgresSchemaReader::new(&self.pool);
        let table_metadata = schema_reader.get_table_metadata(table_name).await?;
        let table_metadata = table_metadata.select_columns(columns.as_deref());

        // Build the base query
        let mut query_builder = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_incremental(&mut query_builder, &table_metadata, timestamp_column, lo, hi);
        let sql_str = query_builder.sql();
        let select_sql = sql_str.as_str();

        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;

        // Use a unique cursor name to avoid conflicts
        let cursor_name = format!("extract_cur_{}", Uuid::new_v4().simple());
        
        // Declare cursor
        let declare_sql = format!("DECLARE {} CURSOR WITH HOLD FOR {}", cursor_name, select_sql);
        sqlx::query(sqlx::AssertSqlSafe(declare_sql.as_str())).execute(&mut *tx).await?;

        let mut batches = Vec::new();
        let mut batch_builder = CursorBatchBuilder::new(&table_metadata)?;

        loop {
            let fetch_sql = format!("FETCH FORWARD {} FROM {}", batch_size, cursor_name);
            let rows = sqlx::query(sqlx::AssertSqlSafe(fetch_sql.as_str())).fetch_all(&mut *tx).await?;
            
            if rows.is_empty() {
                break;
            }

            for row in rows {
                batch_builder.append_row(&row, &table_metadata)?;
                
                if batch_builder.row_count() >= batch_size {
                    let batch = batch_builder.finish()?;
                    batches.push(batch);
                    batch_builder = CursorBatchBuilder::new(&table_metadata)?;
                }
            }
        }

        // Close cursor
        let close_sql = format!("CLOSE {}", cursor_name);
        sqlx::query(sqlx::AssertSqlSafe(close_sql.as_str())).execute(&mut *tx).await?;
        tx.commit().await?;

        // Final partial batch
        if !batch_builder.is_empty() {
            batches.push(batch_builder.finish()?);
        }

        Ok(batches)
    }

    /// Extract keyset partition via cursor.
    pub async fn extract_keyset_partition_via_cursor(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        partition_column: &str,
        lo: i64,
        hi: i64,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>, ExtractorError> {
        let schema_reader = PostgresSchemaReader::new(&self.pool);
        let table_metadata = schema_reader.get_table_metadata(table_name).await?;
        let table_metadata = table_metadata.select_columns(columns.as_deref());

        let mut query_builder = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_keyset_partition(&mut query_builder, &table_metadata, partition_column, lo, hi);
        let sql_str = query_builder.sql();
        let select_sql = sql_str.as_str();

        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;

        let cursor_name = format!("extract_cur_{}", Uuid::new_v4().simple());
        let declare_sql = format!("DECLARE {} CURSOR WITH HOLD FOR {}", cursor_name, select_sql);
        sqlx::query(sqlx::AssertSqlSafe(declare_sql.as_str())).execute(&mut *tx).await?;

        let mut batches = Vec::new();
        let mut batch_builder = CursorBatchBuilder::new(&table_metadata)?;

        loop {
            let fetch_sql = format!("FETCH FORWARD {} FROM {}", batch_size, cursor_name);
            let rows = sqlx::query(sqlx::AssertSqlSafe(fetch_sql.as_str())).fetch_all(&mut *tx).await?;
            
            if rows.is_empty() {
                break;
            }

            for row in rows {
                batch_builder.append_row(&row, &table_metadata)?;
                
                if batch_builder.row_count() >= batch_size {
                    let batch = batch_builder.finish()?;
                    batches.push(batch);
                    batch_builder = CursorBatchBuilder::new(&table_metadata)?;
                }
            }
        }

        let close_sql = format!("CLOSE {}", cursor_name);
        sqlx::query(sqlx::AssertSqlSafe(close_sql.as_str())).execute(&mut *tx).await?;
        tx.commit().await?;

        if !batch_builder.is_empty() {
            batches.push(batch_builder.finish()?);
        }

        Ok(batches)
    }

    /// Extract the half-open-on-the-low-side window `(lo, hi]` — docs/incremental-extraction.md
    /// §2. Callers resolve `lo` from the checkpoint store and compute a safe `hi` (see
    /// `crate::incremental::safe_high_watermark`) before calling this; this method just runs the
    /// window it is given.
    pub async fn extract_incremental_window(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        timestamp_column: &str,
        lo: DateTime<Utc>,
        hi: DateTime<Utc>,
    ) -> Result<RecordBatch, ExtractorError> {
        // 1. Read PostgreSQL schema.
        let schema_reader = PostgresSchemaReader::new(&self.pool);

        let table_metadata: TableMetadata = schema_reader
            .get_table_metadata(table_name)
            .await?;

        // 2. Select requested columns.
        let table_metadata = table_metadata.select_columns(columns.as_deref());

        log::debug!("{}", table_metadata);

        // 3. Build Arrow schema.
        let arrow_schema = PostgresRowAdapter::build_arrow_schema(&table_metadata)?;

        // 4. Build SELECT column list.
        let mut query_builder = QueryBuilder::<Postgres>::new("");

        PostgresQueryBuilder::build_incremental(
            &mut query_builder,
            &table_metadata,
            timestamp_column,
            lo,
            hi,
        );

        log::info!("Executing query: {:#?}", query_builder.sql());

        // 5. Execute query using cursor-based streaming for bounded memory.
        let batches = self.extract_incremental_via_cursor(
            table_name,
            columns,
            timestamp_column,
            lo,
            hi,
            8192, // default batch size
        ).await?;

        // Combine all batches into one (for backward compatibility)
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(arrow_schema));
        }
        
        // Use arrow::compute::concat_batches to combine batches
        let combined = arrow::compute::concat_batches(&arrow_schema, &batches)?;
        Ok(combined)
    }

    /// Extract all rows from a table without date filtering.
    /// This is used for full table loads where no incremental window is needed.
    pub async fn extract_full_table(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
    ) -> Result<RecordBatch, ExtractorError> {
        // 1. Read PostgreSQL schema.
        let schema_reader = PostgresSchemaReader::new(&self.pool);

        let table_metadata: TableMetadata = schema_reader
            .get_table_metadata(table_name)
            .await?;

        // 2. Select requested columns.
        let table_metadata = table_metadata.select_columns(columns.as_deref());

        log::debug!("{}", table_metadata);

        // 3. Build Arrow schema.
        let arrow_schema = PostgresRowAdapter::build_arrow_schema(&table_metadata)?;

        // 4. Build SELECT query (no WHERE clause).
        let mut query_builder = QueryBuilder::<Postgres>::new("");

        PostgresQueryBuilder::build_full_table(
            &mut query_builder,
            &table_metadata,
        );

        log::info!("Executing query: {:#?}", query_builder.sql());

        // 5. Execute query using cursor-based streaming for bounded memory.
        let schema_reader = PostgresSchemaReader::new(&self.pool);
        let table_metadata_full = schema_reader.get_table_metadata(table_name).await?;
        let table_metadata_full = table_metadata_full.select_columns(columns.as_deref());
        
        let mut query_builder = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_full_table(&mut query_builder, &table_metadata_full);
        let sql_str = query_builder.sql();
        let select_sql = sql_str.as_str();

        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;

        let cursor_name = format!("extract_cur_{}", Uuid::new_v4().simple());
        let declare_sql = format!("DECLARE {} CURSOR WITH HOLD FOR {}", cursor_name, select_sql);
        sqlx::query(sqlx::AssertSqlSafe(declare_sql.as_str())).execute(&mut *tx).await?;

        let mut batches = Vec::new();
        let mut batch_builder = CursorBatchBuilder::new(&table_metadata_full)?;

        loop {
            let fetch_sql = format!("FETCH FORWARD 8192 FROM {}", cursor_name);
            let rows = sqlx::query(sqlx::AssertSqlSafe(fetch_sql.as_str())).fetch_all(&mut *tx).await?;
            
            if rows.is_empty() {
                break;
            }

            for row in rows {
                batch_builder.append_row(&row, &table_metadata_full)?;
                
                if batch_builder.row_count() >= 8192 {
                    let batch = batch_builder.finish()?;
                    batches.push(batch);
                    batch_builder = CursorBatchBuilder::new(&table_metadata_full)?;
                }
            }
        }

        let close_sql = format!("CLOSE {}", cursor_name);
        sqlx::query(sqlx::AssertSqlSafe(close_sql.as_str())).execute(&mut *tx).await?;
        tx.commit().await?;

        if !batch_builder.is_empty() {
            batches.push(batch_builder.finish()?);
        }

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(arrow_schema));
        }
        
        let combined = arrow::compute::concat_batches(&arrow_schema, &batches)?;
        Ok(combined)
    }

    /// Extract a keyset partition: rows where `partition_column` is in range [lo, hi).
    /// Used for parallel extraction by primary key or indexed column ranges.
    pub async fn extract_keyset_partition(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        partition_column: &str,
        lo: i64,
        hi: i64,
    ) -> Result<RecordBatch, ExtractorError> {
        // 1. Read PostgreSQL schema.
        let schema_reader = PostgresSchemaReader::new(&self.pool);

        let table_metadata: TableMetadata = schema_reader
            .get_table_metadata(table_name)
            .await?;

        // 2. Select requested columns.
        let table_metadata = table_metadata.select_columns(columns.as_deref());

        log::debug!("{}", table_metadata);

        // 3. Build Arrow schema.
        let arrow_schema = PostgresRowAdapter::build_arrow_schema(&table_metadata)?;

        // 4. Build SELECT query with keyset predicate.
        let mut query_builder = QueryBuilder::<Postgres>::new("");

        PostgresQueryBuilder::build_keyset_partition(
            &mut query_builder,
            &table_metadata,
            partition_column,
            lo,
            hi,
        );

        log::info!("Executing query: {:#?}", query_builder.sql());

        // 5. Execute query using cursor-based streaming for bounded memory.
        let batches = self.extract_keyset_partition_via_cursor(
            table_name,
            columns,
            partition_column,
            lo,
            hi,
            8192,
        ).await?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(arrow_schema));
        }
        
        let combined = arrow::compute::concat_batches(&arrow_schema, &batches)?;
        Ok(combined)
    }

    /// Decode PostgreSQL binary COPY stream into Arrow RecordBatches.
    async fn decode_binary_copy_stream(
        mut stream: CopyOutStream,
        schema: &Arc<Schema>,
        table_metadata: &TableMetadata,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>, ExtractorError> {
        use futures::StreamExt;
        let mut batches = Vec::new();
        let mut builder = CursorBatchBuilder::new(table_metadata)?;
        let mut stream = std::pin::pin!(stream);

        while let Some(row_result) = stream.next().await {
            let row_bytes = row_result.map_err(|e| ExtractorError::Internal(format!("COPY stream error: {e}")))?;
            Self::decode_binary_copy_row(&mut builder, &row_bytes, table_metadata)?;
            
            if builder.row_count() >= batch_size {
                batches.push(builder.finish()?);
                builder = CursorBatchBuilder::new(table_metadata)?;
            }
        }

        if !builder.is_empty() {
            batches.push(builder.finish()?);
        }

        Ok(batches)
    }

    /// Decode a single binary COPY row (raw bytes) into the builder.
    /// PostgreSQL binary COPY format per row:
    ///   Int16: number of columns
    ///   For each column: Int32 length (-1 for NULL), then that many bytes of data
    fn decode_binary_copy_row(
        builder: &mut CursorBatchBuilder,
        row_bytes: &Bytes,
        table_metadata: &TableMetadata,
    ) -> Result<(), ExtractorError> {
use std::io::{Cursor, Read};
use byteorder::{BigEndian, ReadBytesExt};
        
        let mut cursor = Cursor::new(row_bytes.as_ref());
        
        // Read number of columns
        let num_columns = cursor.read_i16::<BigEndian>()
            .map_err(|e| ExtractorError::Internal(format!("failed to read column count: {e}")))?;
        
        if num_columns as usize != table_metadata.columns.len() {
            return Err(ExtractorError::Internal(format!(
                "column count mismatch: expected {}, got {}",
                table_metadata.columns.len(), num_columns
            )));
        }

        for (idx, column) in table_metadata.columns.iter().enumerate() {
            // Read column length
            let len = cursor.read_i32::<BigEndian>()
                .map_err(|e| ExtractorError::Internal(format!("failed to read column {} length: {e}", idx)))?;
            
            if len == -1 {
                // NULL value
                builder.append_null(idx)?;
                continue;
            }
            
            if len < 0 {
                return Err(ExtractorError::Internal(format!("invalid column length: {}", len)));
            }
            
            // Read column data
            let len = len as usize;
            let mut data = vec![0u8; len];
            cursor.read_exact(&mut data)
                .map_err(|e| ExtractorError::Internal(format!("failed to read column {} data: {e}", idx)))?;
            
            let value_bytes = Bytes::from(data);
            Self::decode_column_value(builder, idx, column, &value_bytes)?;
        }
        builder.row_count += 1;
        Ok(())
    }

    fn decode_column_value(
        builder: &mut CursorBatchBuilder,
        idx: usize,
        column: &crate::types::ColumnMetadata,
        bytes: &[u8],
    ) -> Result<(), ExtractorError> {
        use crate::connector::postgres::arrow_type_mapper::ArrowTypeMapper;
        let data_type = ArrowTypeMapper::map(column)?;
        
        match data_type {
            DataType::Int16 => {
                let val = i16::from_be_bytes(bytes.try_into().map_err(|_| ExtractorError::Internal("bad int2 bytes".into()))?);
                builder.append_int16(idx, val)?;
            }
            DataType::Int32 => {
                let val = i32::from_be_bytes(bytes.try_into().map_err(|_| ExtractorError::Internal("bad int4 bytes".into()))?);
                builder.append_int32(idx, val)?;
            }
            DataType::Int64 => {
                let val = i64::from_be_bytes(bytes.try_into().map_err(|_| ExtractorError::Internal("bad int8 bytes".into()))?);
                builder.append_int64(idx, val)?;
            }
            DataType::Float32 => {
                let val = f32::from_be_bytes(bytes.try_into().map_err(|_| ExtractorError::Internal("bad float4 bytes".into()))?);
                builder.append_float32(idx, val)?;
            }
            DataType::Float64 => {
                let val = f64::from_be_bytes(bytes.try_into().map_err(|_| ExtractorError::Internal("bad float8 bytes".into()))?);
                builder.append_float64(idx, val)?;
            }
            DataType::Boolean => {
                let val = bytes[0] != 0;
                builder.append_bool(idx, val)?;
            }
            DataType::Utf8 => {
                let val = std::str::from_utf8(bytes).map_err(|_| ExtractorError::Internal("bad utf8".into()))?;
                builder.append_string(idx, val)?;
            }
            DataType::Binary => {
                builder.append_binary(idx, bytes)?;
            }
            DataType::Date32 => {
                let pg_days = i32::from_be_bytes(bytes.try_into().map_err(|_| ExtractorError::Internal("bad date bytes".into()))?);
                let arrow_days = pg_days + 10957; // days between 1970-01-01 and 2000-01-01
                builder.append_date32(idx, arrow_days)?;
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let pg_micros = i64::from_be_bytes(bytes.try_into().map_err(|_| ExtractorError::Internal("bad timestamp bytes".into()))?);
                let arrow_micros = pg_micros + 946684800_000_000; // micros between 1970 and 2000
                builder.append_timestamp_micros(idx, arrow_micros)?;
            }
            DataType::Decimal128(precision, scale) => {
                // Postgres numeric binary format is complex - for now fall back to text
                builder.append_null(idx)?; // placeholder
            }
            DataType::List(_) => {
                // COPY BINARY array payloads need an element-wise parser (dims + per-
                // element lengths); refuse loudly rather than nulling user data.
                return Err(ExtractorError::UnsupportedType(format!(
                    "array column '{}' over COPY BINARY (use cursor extract instead)",
                    column.column_name
                )));
            }
            _ => {
                builder.append_null(idx)?; // unsupported type -> null
            }
        }
        Ok(())
    }
}

/// Batch builder for cursor-based streaming extraction.
struct CursorBatchBuilder {
    schema: Arc<Schema>,
    table_metadata: TableMetadata,
    builders: Vec<Box<dyn ArrayBuilder>>,
    row_count: usize,
}

impl CursorBatchBuilder {
    fn new(table_metadata: &TableMetadata) -> Result<Self, ExtractorError> {
        let schema = PostgresRowAdapter::build_arrow_schema(table_metadata)?;
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
        for column in &table_metadata.columns {
            let data_type = ArrowTypeMapper::map(column)?;
            builders.push(PostgresRowAdapter::new_builder(&data_type));
        }
        Ok(Self { schema, table_metadata: table_metadata.clone(), builders, row_count: 0 })
    }

    fn append_row(&mut self, row: &PgRow, table_metadata: &TableMetadata) -> Result<(), ExtractorError> {
        for (idx, column) in table_metadata.columns.iter().enumerate() {
            self.append_value_to_builder(idx, row, column)?;
        }
        self.row_count += 1;
        Ok(())
    }

    fn append_value_to_builder(&mut self, builder_idx: usize, row: &PgRow, column: &crate::types::ColumnMetadata) -> Result<(), ExtractorError> {
        let builder = &mut self.builders[builder_idx];
        match column.data_type.as_str() {
            "smallint" => {
                let value: Option<i16> = row.try_get(column.column_name.as_str())?;
                let b = builder.as_any_mut().downcast_mut::<Int16Builder>().ok_or_else(|| ExtractorError::Internal("downcast Int16Builder".into()))?;
                b.append_option(value);
            }
            "integer" => {
                let value: Option<i32> = row.try_get(column.column_name.as_str())?;
                let b = builder.as_any_mut().downcast_mut::<Int32Builder>().ok_or_else(|| ExtractorError::Internal("downcast Int32Builder".into()))?;
                b.append_option(value);
            }
            "bigint" => {
                let value: Option<i64> = row.try_get(column.column_name.as_str())?;
                let b = builder.as_any_mut().downcast_mut::<Int64Builder>().ok_or_else(|| ExtractorError::Internal("downcast Int64Builder".into()))?;
                b.append_option(value);
            }
            "real" => {
                let value: Option<f32> = row.try_get(column.column_name.as_str())?;
                let b = builder.as_any_mut().downcast_mut::<Float32Builder>().ok_or_else(|| ExtractorError::Internal("downcast Float32Builder".into()))?;
                b.append_option(value);
            }
            "double precision" => {
                let value: Option<f64> = row.try_get(column.column_name.as_str())?;
                let b = builder.as_any_mut().downcast_mut::<Float64Builder>().ok_or_else(|| ExtractorError::Internal("downcast Float64Builder".into()))?;
                b.append_option(value);
            }
            "numeric" => {
                let value: Option<bigdecimal::BigDecimal> = row.try_get(column.column_name.as_str())?;
                let scale = column.numeric_scale.unwrap_or(10) as i8;
                let i128_value = value.map(|decimal| {
                    decimal.with_scale(scale as i64).to_i128().ok_or_else(|| ExtractorError::Internal(format!("numeric overflow: {}", decimal)))
                }).transpose()?;
                let b = builder.as_any_mut().downcast_mut::<Decimal128Builder>().ok_or_else(|| ExtractorError::Internal("downcast Decimal128Builder".into()))?;
                b.append_option(i128_value);
            }
            "boolean" => {
                let value: Option<bool> = row.try_get(column.column_name.as_str())?;
                let b = builder.as_any_mut().downcast_mut::<BooleanBuilder>().ok_or_else(|| ExtractorError::Internal("downcast BooleanBuilder".into()))?;
                b.append_option(value);
            }
            "text" | "character varying" | "character" => {
                let value: Option<String> = row.try_get(column.column_name.as_str())?;
                let b = builder.as_any_mut().downcast_mut::<StringBuilder>().ok_or_else(|| ExtractorError::Internal("downcast StringBuilder".into()))?;
                b.append_option(value);
            }
            "USER-DEFINED" => {
                let value: Option<String> = row.try_get(column.column_name.as_str())?;
                let b = builder.as_any_mut().downcast_mut::<StringBuilder>().ok_or_else(|| ExtractorError::Internal("downcast StringBuilder".into()))?;
                b.append_option(value);
            }
            "timestamp with time zone" => {
                let value: Option<DateTime<Utc>> = row.try_get(column.column_name.as_str())?;
                let micros = value.map(|dt| dt.timestamp_micros());
                let b = builder.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>().ok_or_else(|| ExtractorError::Internal("downcast TimestampMicrosecondBuilder".into()))?;
                b.append_option(micros);
            }
            "timestamp without time zone" => {
                let value: Option<NaiveDateTime> = row.try_get(column.column_name.as_str())?;
                let micros = value.map(|dt| dt.and_utc().timestamp_micros());
                let b = builder.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>().ok_or_else(|| ExtractorError::Internal("downcast TimestampMicrosecondBuilder".into()))?;
                b.append_option(micros);
            }
            "date" => {
                let value: Option<NaiveDate> = row.try_get(column.column_name.as_str())?;
                let days = value.map(|d| (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32);
                let b = builder.as_any_mut().downcast_mut::<Date32Builder>().ok_or_else(|| ExtractorError::Internal("downcast Date32Builder".into()))?;
                b.append_option(days);
            }
            "uuid" => {
                let value: Option<uuid::Uuid> = row.try_get(column.column_name.as_str())?;
                let s = value.map(|u| u.to_string());
                let b = builder.as_any_mut().downcast_mut::<StringBuilder>().ok_or_else(|| ExtractorError::Internal("downcast StringBuilder".into()))?;
                b.append_option(s);
            }
            "bytea" => {
                let value: Option<Vec<u8>> = row.try_get(column.column_name.as_str())?;
                let b = builder.as_any_mut().downcast_mut::<BinaryBuilder>().ok_or_else(|| ExtractorError::Internal("downcast BinaryBuilder".into()))?;
                b.append_option(value);
            }
            "jsonb" | "json" => {
                let value: Option<serde_json::Value> = row.try_get(column.column_name.as_str())?;
                let s = value.map(|v| v.to_string());
                let b = builder.as_any_mut().downcast_mut::<StringBuilder>().ok_or_else(|| ExtractorError::Internal("downcast StringBuilder".into()))?;
                b.append_option(s);
            }
            "ARRAY" => match column.udt_name.as_deref() {
                Some("_text") => {
                    let value: Option<Vec<Option<String>>> = row.try_get(column.column_name.as_str())?;
                    let b = builder.as_any_mut().downcast_mut::<ListBuilder<StringBuilder>>().ok_or_else(|| ExtractorError::Internal("downcast ListBuilder".into()))?;
                    PostgresRowAdapter::append_text_array_option(b, value);
                }
                other => {
                    return Err(ExtractorError::UnsupportedType(format!(
                        "array element type {:?} for column '{}' (only text[] arrays are supported)",
                        other, column.column_name
                    )));
                }
            }
            _ => {
                return Err(ExtractorError::UnsupportedType(column.data_type.clone()));
            }
        }
        Ok(())
    }

    fn append_null(&mut self, idx: usize) -> Result<(), ExtractorError> {
        let builder = &mut self.builders[idx];
        if let Some(b) = builder.as_any_mut().downcast_mut::<Int16Builder>() { b.append_null(); }
        else if let Some(b) = builder.as_any_mut().downcast_mut::<Int32Builder>() { b.append_null(); }
        else if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() { b.append_null(); }
        else if let Some(b) = builder.as_any_mut().downcast_mut::<Float32Builder>() { b.append_null(); }
        else if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() { b.append_null(); }
        else if let Some(b) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() { b.append_null(); }
        else if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() { b.append_null(); }
        else if let Some(b) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() { b.append_null(); }
        else if let Some(b) = builder.as_any_mut().downcast_mut::<Date32Builder>() { b.append_null(); }
        else if let Some(b) = builder.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>() { b.append_null(); }
        else if let Some(b) = builder.as_any_mut().downcast_mut::<Decimal128Builder>() { b.append_null(); }
        else if let Some(b) = builder.as_any_mut().downcast_mut::<ListBuilder<StringBuilder>>() { b.append_null(); }
        else { return Err(ExtractorError::Internal("unknown builder type for null".into())); }
        Ok(())
    }

    // Direct value appenders for COPY binary decoding
    fn append_int16(&mut self, idx: usize, val: i16) -> Result<(), ExtractorError> {
        self.builders[idx].as_any_mut().downcast_mut::<Int16Builder>().ok_or_else(|| ExtractorError::Internal("downcast".into()))?.append_value(val);
        Ok(())
    }
    fn append_int32(&mut self, idx: usize, val: i32) -> Result<(), ExtractorError> {
        self.builders[idx].as_any_mut().downcast_mut::<Int32Builder>().ok_or_else(|| ExtractorError::Internal("downcast".into()))?.append_value(val);
        Ok(())
    }
    fn append_int64(&mut self, idx: usize, val: i64) -> Result<(), ExtractorError> {
        self.builders[idx].as_any_mut().downcast_mut::<Int64Builder>().ok_or_else(|| ExtractorError::Internal("downcast".into()))?.append_value(val);
        Ok(())
    }
    fn append_float32(&mut self, idx: usize, val: f32) -> Result<(), ExtractorError> {
        self.builders[idx].as_any_mut().downcast_mut::<Float32Builder>().ok_or_else(|| ExtractorError::Internal("downcast".into()))?.append_value(val);
        Ok(())
    }
    fn append_float64(&mut self, idx: usize, val: f64) -> Result<(), ExtractorError> {
        self.builders[idx].as_any_mut().downcast_mut::<Float64Builder>().ok_or_else(|| ExtractorError::Internal("downcast".into()))?.append_value(val);
        Ok(())
    }
    fn append_bool(&mut self, idx: usize, val: bool) -> Result<(), ExtractorError> {
        self.builders[idx].as_any_mut().downcast_mut::<BooleanBuilder>().ok_or_else(|| ExtractorError::Internal("downcast".into()))?.append_value(val);
        Ok(())
    }
    fn append_string(&mut self, idx: usize, val: &str) -> Result<(), ExtractorError> {
        self.builders[idx].as_any_mut().downcast_mut::<StringBuilder>().ok_or_else(|| ExtractorError::Internal("downcast".into()))?.append_value(val);
        Ok(())
    }
    fn append_binary(&mut self, idx: usize, val: &[u8]) -> Result<(), ExtractorError> {
        self.builders[idx].as_any_mut().downcast_mut::<BinaryBuilder>().ok_or_else(|| ExtractorError::Internal("downcast".into()))?.append_value(val);
        Ok(())
    }
    fn append_date32(&mut self, idx: usize, val: i32) -> Result<(), ExtractorError> {
        self.builders[idx].as_any_mut().downcast_mut::<Date32Builder>().ok_or_else(|| ExtractorError::Internal("downcast".into()))?.append_value(val);
        Ok(())
    }
    fn append_timestamp_micros(&mut self, idx: usize, val: i64) -> Result<(), ExtractorError> {
        self.builders[idx].as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>().ok_or_else(|| ExtractorError::Internal("downcast".into()))?.append_value(val);
        Ok(())
    }

    fn is_empty(&self) -> bool { self.row_count == 0 }
    fn row_count(&self) -> usize { self.row_count }

    fn finish(&mut self) -> Result<RecordBatch, ExtractorError> {
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for (idx, builder) in self.builders.iter_mut().enumerate() {
            let column = &self.table_metadata.columns[idx];
            let data_type = ArrowTypeMapper::map(column)?;
            let array: ArrayRef = if matches!(data_type, DataType::Decimal128(_, _)) {
                let precision = column.numeric_precision.unwrap_or(38) as u8;
                let scale = column.numeric_scale.unwrap_or(10) as i8;
                let arr = builder.as_any_mut().downcast_mut::<Decimal128Builder>().ok_or_else(|| ExtractorError::Internal("downcast Decimal128".into()))?.finish();
                Arc::new(arr.with_precision_and_scale(precision, scale).map_err(|e| ExtractorError::Internal(e.to_string()))?)
            } else {
                Arc::new(builder.finish())
            };
            arrays.push(array);
        }
        self.row_count = 0;
        // Rebuild builders for reuse
        for (idx, builder) in self.builders.iter_mut().enumerate() {
            let column = &self.table_metadata.columns[idx];
            let data_type = ArrowTypeMapper::map(column)?;
            *builder = PostgresRowAdapter::new_builder(&data_type);
        }
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}