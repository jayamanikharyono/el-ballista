//! PostgreSQL extractor.
//! extractor/postgres/extractor.rs
//! Orchestrates PostgreSQL schema discovery, query execution,
//! and conversion of PostgreSQL rows into Arrow `RecordBatch` values.
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Postgres, QueryBuilder};

use arrow::record_batch::RecordBatch;
use crate::extractor::postgres::{
    query_builder::PostgresQueryBuilder,
    row_adapter::PostgresRowAdapter,
    schema_reader::PostgresSchemaReader,
};

use crate::{
    extractor::errors::ExtractorError,
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
                    // `statement_timeout_setting` is a formatted integer (milliseconds) we
                    // built ourselves, never user input — safe to assert despite being a
                    // dynamic string. sqlx 0.9 requires this audit annotation for any SQL
                    // string that isn't a `&'static str` literal.
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

        let table_metadata : TableMetadata = schema_reader
            .get_table_metadata(table_name)
            .await?;

        // 2. Select requested columns.
        let table_metadata  =
            table_metadata.select_columns(columns.as_deref());

        log::debug!("{}", table_metadata);

        // 3. Build Arrow schema.
        let arrow_schema =
            PostgresRowAdapter::build_arrow_schema(&table_metadata);

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

        // 5. Execute query.
        //
        // NOTE: this still buffers the full window into memory (`fetch_all`) rather than
        // streaming batches of `batch_size` rows as docs/connectors/README.md §2 specifies.
        // Streaming decode and the binary `COPY` bulk path (docs/connectors/postgres.md §2) are
        // deferred — see docs/phase-one-implementation-plan.md §6.
        let rows = query_builder
            .build()
            .fetch_all(&self.pool)
            .await?;

        // 6. Convert PostgreSQL rows into Arrow RecordBatch.
        let record_batch =
            PostgresRowAdapter::rows_to_record_batch(
                &rows,
                &table_metadata,
                arrow_schema,
            )?;

        Ok(record_batch)
    }
}