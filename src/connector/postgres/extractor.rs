//! PostgreSQL extractor.
//! extractor/postgres/extractor.rs
//! Orchestrates PostgreSQL schema discovery, query execution,
//! and conversion of PostgreSQL rows into Arrow `RecordBatch` values.
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgRow};
use sqlx::{Connection, PgPool, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, ListBuilder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::connector::postgres::{
    arrow_type_mapper::ArrowTypeMapper, query_builder::PostgresQueryBuilder,
    row_adapter::PostgresRowAdapter, schema_reader::PostgresSchemaReader,
};
use crate::connector::query_tag::QuerySession;

use crate::{connector::errors::ExtractorError, types::table_metadata::TableMetadata};

pub struct PostgresExtractor {
    pool: PgPool,
    /// Debug SQL comment identity (see `connector::query_tag`): `pipeline` reuses
    /// `application_name` (already passed to `connect`, already identifying this
    /// connection); `run_id` is generated once per extractor instance, i.e. once per
    /// process/session, and shared by every query this extractor issues.
    query_session: QuerySession,
}

impl PostgresExtractor {
    /// Connect with the session-hygiene settings docs/connectors/postgres.md §6 treats as
    /// mandatory, applied on *every* connection the pool opens (not just the first one): an
    /// identifiable `application_name`, UTC session time zone, a statement timeout, an
    /// idle-in-transaction timeout, and a lock timeout so we never queue behind a DDL lock.
    #[allow(clippy::too_many_arguments)]
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

        Ok(Self {
            pool,
            query_session: QuerySession::new(application_name),
        })
    }

    /// The underlying pool, for callers (e.g. the safe-high-watermark query in
    /// `crate::incremental`) that need to run something other than a table scan.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Render a fresh debug tag for one query issued through this extractor. See
    /// `connector::query_tag`; prepended to DECLARE/FETCH statements below so the running
    /// statement is identifiable directly in `pg_stat_activity`.
    fn tag(&self, strategy: &str) -> String {
        self.query_session.tag(strategy).render()
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
        PostgresQueryBuilder::build_incremental(
            &mut query_builder,
            &table_metadata,
            timestamp_column,
            lo,
            hi,
        );
        let sql_str = query_builder.sql();
        let select_sql = sql_str.as_str();

        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;

        // Use a unique cursor name to avoid conflicts
        let cursor_name = format!("extract_cur_{}", Uuid::new_v4().simple());

        // Declare cursor. The SELECT text carries $1/$2 placeholders from QueryBuilder,
        // so the lo/hi values must be bound here — executing without binds fails with
        // "there is no parameter $1". The debug tag is prepended to both DECLARE and
        // every FETCH: DECLARE embeds the real SELECT, but FETCH is what's actually
        // running (and visible in pg_stat_activity) for the bulk of a long extraction.
        let tag = self.tag("incremental_cursor");
        let declare_sql = format!(
            "{tag}DECLARE {} CURSOR WITH HOLD FOR {}",
            cursor_name, select_sql
        );
        sqlx::query(sqlx::AssertSqlSafe(declare_sql.as_str()))
            .bind(lo)
            .bind(hi)
            .execute(&mut *tx)
            .await?;

        // Guarded fetch loop so CLOSE runs even if `append_row` or `finish` fails.
        // `WITH HOLD` would otherwise keep the portal on the pooled connection until session close.
        let mut batches = Vec::new();
        let mut batch_builder = CursorBatchBuilder::new(&table_metadata)?;

        let fetch_result: Result<(), ExtractorError> = async {
            loop {
                let fetch_sql = format!("{tag}FETCH FORWARD {} FROM {}", batch_size, cursor_name);
                let rows = sqlx::query(sqlx::AssertSqlSafe(fetch_sql.as_str()))
                    .fetch_all(&mut *tx)
                    .await?;

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
            Ok(())
        }
        .await;

        // Always try to close the cursor; ignore errors if fetch already failed.
        let close_sql = format!("{tag}CLOSE {}", cursor_name);
        let _ = sqlx::query(sqlx::AssertSqlSafe(close_sql.as_str()))
            .execute(&mut *tx)
            .await;

        match fetch_result {
            Ok(()) => {
                tx.commit().await?;
                if !batch_builder.is_empty() {
                    batches.push(batch_builder.finish()?);
                }
                Ok(batches)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                // Preserve any partial batch error as the primary failure.
                Err(e)
            }
        }
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
        PostgresQueryBuilder::build_keyset_partition(
            &mut query_builder,
            &table_metadata,
            partition_column,
            lo,
            hi,
        );
        let sql_str = query_builder.sql();
        let select_sql = sql_str.as_str();

        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;

        let cursor_name = format!("extract_cur_{}", Uuid::new_v4().simple());
        let tag = self.tag("keyset_cursor");
        let declare_sql = format!(
            "{tag}DECLARE {} CURSOR WITH HOLD FOR {}",
            cursor_name, select_sql
        );
        // build_keyset_partition emits $1/$2 placeholders — bind lo/hi here.
        sqlx::query(sqlx::AssertSqlSafe(declare_sql.as_str()))
            .bind(lo)
            .bind(hi)
            .execute(&mut *tx)
            .await?;

        let mut batches = Vec::new();
        let mut batch_builder = CursorBatchBuilder::new(&table_metadata)?;

        let fetch_result: Result<(), ExtractorError> = async {
            loop {
                let fetch_sql = format!("{tag}FETCH FORWARD {} FROM {}", batch_size, cursor_name);
                let rows = sqlx::query(sqlx::AssertSqlSafe(fetch_sql.as_str()))
                    .fetch_all(&mut *tx)
                    .await?;

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
            Ok(())
        }
        .await;

        let close_sql = format!("{tag}CLOSE {}", cursor_name);
        let _ = sqlx::query(sqlx::AssertSqlSafe(close_sql.as_str()))
            .execute(&mut *tx)
            .await;
        match fetch_result {
            Ok(()) => {
                tx.commit().await?;
                if !batch_builder.is_empty() {
                    batches.push(batch_builder.finish()?);
                }
                Ok(batches)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }

    /// Extract the half-open-on-the-low-side window `(lo, hi]` — docs/incremental-extraction.md
    /// §2. Callers resolve `lo` from the checkpoint store and compute a safe `hi` (see
    /// `crate::incremental::safe_high_watermark`) before calling this; this method just runs the
    /// window it is given.
    /// `batch_size` controls cursor `FETCH` size; callers should pass
    /// `config.execution.batch_size` rather than relying on the default.
    pub async fn extract_incremental_window(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        timestamp_column: &str,
        lo: DateTime<Utc>,
        hi: DateTime<Utc>,
    ) -> Result<RecordBatch, ExtractorError> {
        self.extract_incremental_window_with_batch_size(
            table_name,
            columns,
            timestamp_column,
            lo,
            hi,
            8192,
        )
        .await
    }

    pub async fn extract_incremental_window_with_batch_size(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        timestamp_column: &str,
        lo: DateTime<Utc>,
        hi: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<RecordBatch, ExtractorError> {
        // Schema + arrow_schema needed only to materialize an empty batch when cursor returns 0 rows.
        // The cursor path re-reads schema internally, so don't build the SELECT here.
        let schema_reader = PostgresSchemaReader::new(&self.pool);
        let table_metadata = schema_reader
            .get_table_metadata(table_name)
            .await?
            .select_columns(columns.as_deref());
        let arrow_schema = PostgresRowAdapter::build_arrow_schema(&table_metadata)?;

        let batches = self
            .extract_incremental_via_cursor(
                table_name,
                columns,
                timestamp_column,
                lo,
                hi,
                batch_size,
            )
            .await?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(arrow_schema));
        }
        // Legacy API concatenates; for > ~1M rows prefer the `*_via_cursor` streaming variants
        // that return `Vec<RecordBatch>` and avoid a single contiguous allocation.
        let combined = arrow::compute::concat_batches(&arrow_schema, &batches)?;
        Ok(combined)
    }

    /// Extract all rows from a table without date filtering.
    /// This is used for full table loads where no incremental window is needed.
    /// Prefer `extract_full_table_via_cursor_batches` for large tables to avoid
    /// concatenating all batches into one allocation.
    pub async fn extract_full_table(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
    ) -> Result<RecordBatch, ExtractorError> {
        self.extract_full_table_with_batch_size(table_name, columns, 8192)
            .await
    }

    pub async fn extract_full_table_with_batch_size(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        batch_size: usize,
    ) -> Result<RecordBatch, ExtractorError> {
        let schema_reader = PostgresSchemaReader::new(&self.pool);
        let table_metadata = schema_reader
            .get_table_metadata(table_name)
            .await?
            .select_columns(columns.as_deref());
        let arrow_schema = PostgresRowAdapter::build_arrow_schema(&table_metadata)?;

        let batches = self
            .extract_full_table_via_cursor_batches(table_name, columns, batch_size)
            .await?;
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(arrow_schema));
        }
        let combined = arrow::compute::concat_batches(&arrow_schema, &batches)?;
        Ok(combined)
    }

    async fn extract_full_table_via_cursor_batches(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>, ExtractorError> {
        let schema_reader = PostgresSchemaReader::new(&self.pool);
        let table_metadata = schema_reader
            .get_table_metadata(table_name)
            .await?
            .select_columns(columns.as_deref());

        let mut query_builder = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_full_table(&mut query_builder, &table_metadata);
        let sql_str = query_builder.sql();
        let select_sql = sql_str.as_str();

        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;

        let cursor_name = format!("extract_cur_{}", Uuid::new_v4().simple());
        let tag = self.tag("full");
        let declare_sql = format!(
            "{tag}DECLARE {} CURSOR WITH HOLD FOR {}",
            cursor_name, select_sql
        );
        sqlx::query(sqlx::AssertSqlSafe(declare_sql.as_str()))
            .execute(&mut *tx)
            .await?;

        let mut batches = Vec::new();
        let mut batch_builder = CursorBatchBuilder::new(&table_metadata)?;

        let fetch_result: Result<(), ExtractorError> = async {
            loop {
                let fetch_sql = format!("{tag}FETCH FORWARD {} FROM {}", batch_size, cursor_name);
                let rows = sqlx::query(sqlx::AssertSqlSafe(fetch_sql.as_str()))
                    .fetch_all(&mut *tx)
                    .await?;

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
            Ok(())
        }
        .await;

        let close_sql = format!("{tag}CLOSE {}", cursor_name);
        let _ = sqlx::query(sqlx::AssertSqlSafe(close_sql.as_str()))
            .execute(&mut *tx)
            .await;
        match fetch_result {
            Ok(()) => {
                tx.commit().await?;
                if !batch_builder.is_empty() {
                    batches.push(batch_builder.finish()?);
                }
                Ok(batches)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
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
        self.extract_keyset_partition_with_batch_size(
            table_name,
            columns,
            partition_column,
            lo,
            hi,
            8192,
        )
        .await
    }

    pub async fn extract_keyset_partition_with_batch_size(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        partition_column: &str,
        lo: i64,
        hi: i64,
        batch_size: usize,
    ) -> Result<RecordBatch, ExtractorError> {
        let schema_reader = PostgresSchemaReader::new(&self.pool);
        let table_metadata = schema_reader
            .get_table_metadata(table_name)
            .await?
            .select_columns(columns.as_deref());
        let arrow_schema = PostgresRowAdapter::build_arrow_schema(&table_metadata)?;
        let batches = self
            .extract_keyset_partition_via_cursor(
                table_name,
                columns,
                partition_column,
                lo,
                hi,
                batch_size,
            )
            .await?;
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(arrow_schema));
        }
        let combined = arrow::compute::concat_batches(&arrow_schema, &batches)?;
        Ok(combined)
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
        Ok(Self {
            schema,
            table_metadata: table_metadata.clone(),
            builders,
            row_count: 0,
        })
    }

    fn append_row(
        &mut self,
        row: &PgRow,
        table_metadata: &TableMetadata,
    ) -> Result<(), ExtractorError> {
        for (idx, column) in table_metadata.columns.iter().enumerate() {
            self.append_value_to_builder(idx, row, column)?;
        }
        self.row_count += 1;
        Ok(())
    }

    fn append_value_to_builder(
        &mut self,
        builder_idx: usize,
        row: &PgRow,
        column: &crate::types::ColumnMetadata,
    ) -> Result<(), ExtractorError> {
        let builder = &mut self.builders[builder_idx];
        match column.data_type.as_str() {
            "smallint" => {
                let value: Option<i16> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Int16Builder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast Int16Builder".into()))?;
                b.append_option(value);
            }
            "integer" => {
                let value: Option<i32> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast Int32Builder".into()))?;
                b.append_option(value);
            }
            "bigint" => {
                let value: Option<i64> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Int64Builder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast Int64Builder".into()))?;
                b.append_option(value);
            }
            "real" => {
                let value: Option<f32> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Float32Builder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast Float32Builder".into()))?;
                b.append_option(value);
            }
            "double precision" => {
                let value: Option<f64> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Float64Builder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast Float64Builder".into()))?;
                b.append_option(value);
            }
            "numeric" => {
                let value: Option<bigdecimal::BigDecimal> =
                    row.try_get(column.column_name.as_str())?;
                let scale = column.numeric_scale.unwrap_or(10) as i64;
                let i128_value = value
                    .map(|d| {
                        crate::connector::postgres::arrow_type_mapper::decimal_to_unscaled(d, scale)
                    })
                    .transpose()?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Decimal128Builder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast Decimal128Builder".into()))?;
                b.append_option(i128_value);
            }
            "boolean" => {
                let value: Option<bool> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast BooleanBuilder".into()))?;
                b.append_option(value);
            }
            "text" | "character varying" | "character" => {
                let value: Option<String> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast StringBuilder".into()))?;
                b.append_option(value);
            }
            "USER-DEFINED" => {
                let value: Option<String> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast StringBuilder".into()))?;
                b.append_option(value);
            }
            "timestamp with time zone" => {
                let value: Option<DateTime<Utc>> = row.try_get(column.column_name.as_str())?;
                let micros = value.map(|dt| dt.timestamp_micros());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast TimestampMicrosecondBuilder".into())
                    })?;
                b.append_option(micros);
            }
            "timestamp without time zone" => {
                let value: Option<NaiveDateTime> = row.try_get(column.column_name.as_str())?;
                let micros = value.map(|dt| dt.and_utc().timestamp_micros());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast TimestampMicrosecondBuilder".into())
                    })?;
                b.append_option(micros);
            }
            "date" => {
                let value: Option<NaiveDate> = row.try_get(column.column_name.as_str())?;
                let days = value
                    .map(|d| (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32);
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Date32Builder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast Date32Builder".into()))?;
                b.append_option(days);
            }
            "uuid" => {
                let value: Option<uuid::Uuid> = row.try_get(column.column_name.as_str())?;
                let s = value.map(|u| u.to_string());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast StringBuilder".into()))?;
                b.append_option(s);
            }
            "bytea" => {
                let value: Option<Vec<u8>> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<BinaryBuilder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast BinaryBuilder".into()))?;
                b.append_option(value);
            }
            "jsonb" | "json" => {
                let value: Option<serde_json::Value> = row.try_get(column.column_name.as_str())?;
                let s = value.map(|v| v.to_string());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast StringBuilder".into()))?;
                b.append_option(s);
            }
            "ARRAY" => match column.udt_name.as_deref() {
                Some("_text") => {
                    let value: Option<Vec<Option<String>>> =
                        row.try_get(column.column_name.as_str())?;
                    let b = builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<StringBuilder>>()
                        .ok_or_else(|| ExtractorError::Internal("downcast ListBuilder".into()))?;
                    PostgresRowAdapter::append_text_array_option(b, value);
                }
                other => {
                    return Err(ExtractorError::UnsupportedType(format!(
                        "array element type {:?} for column '{}' (only text[] arrays are supported)",
                        other, column.column_name
                    )));
                }
            },
            _ => {
                return Err(ExtractorError::UnsupportedType(column.data_type.clone()));
            }
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.row_count == 0
    }
    fn row_count(&self) -> usize {
        self.row_count
    }

    fn finish(&mut self) -> Result<RecordBatch, ExtractorError> {
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for (idx, builder) in self.builders.iter_mut().enumerate() {
            let column = &self.table_metadata.columns[idx];
            let data_type = ArrowTypeMapper::map(column)?;
            let array: ArrayRef = if matches!(data_type, DataType::Decimal128(_, _)) {
                let precision = column.numeric_precision.unwrap_or(38) as u8;
                let scale = column.numeric_scale.unwrap_or(10) as i8;
                let arr = builder
                    .as_any_mut()
                    .downcast_mut::<Decimal128Builder>()
                    .ok_or_else(|| ExtractorError::Internal("downcast Decimal128".into()))?
                    .finish();
                Arc::new(
                    arr.with_precision_and_scale(precision, scale)
                        .map_err(|e| ExtractorError::Internal(e.to_string()))?,
                )
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
