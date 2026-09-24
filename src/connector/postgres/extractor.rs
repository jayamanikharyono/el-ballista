//! PostgreSQL extractor.
//! extractor/postgres/extractor.rs
//! Orchestrates PostgreSQL schema discovery, query execution,
//! and conversion of PostgreSQL rows into Arrow `RecordBatch` values.
use futures::TryStreamExt;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use arrow::record_batch::RecordBatch;

use crate::connector::postgres::{
    query_builder::PostgresQueryBuilder, row_adapter::PostgresRowAdapter,
    row_adapter::RowBatchBuilder, schema_reader::PostgresSchemaReader,
};
use crate::connector::query_tag::QuerySession;

use crate::{connector::errors::ExtractorError, types::table_metadata::TableMetadata};

/// Default byte cap per Arrow batch when callers only pass a row count (16 MiB).
pub(crate) const DEFAULT_MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
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

    /// The underlying pool, for callers (e.g. partition-bound computation) that
    /// need to run something other than a table scan.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Render a fresh debug tag for one query issued through this extractor. See
    /// `connector::query_tag`; prepended to DECLARE/FETCH statements below so the running
    /// statement is identifiable directly in `pg_stat_activity`.
    fn tag(&self, strategy: &str) -> String {
        self.query_session.tag(strategy).render()
    }

    /// Drive an open cursor to completion, invoking `on_batch` per flushed Arrow batch.
    ///
    /// Each `FETCH FORWARD` window streams rows via `fetch()` + `try_next()` (no intermediate
    /// `Vec<PgRow>`), appends them into a capacity-preallocated [`RowBatchBuilder`], and
    /// flushes on **rows OR bytes** so wide rows cannot blow memory before `batch_size`.
    /// Callers must `DECLARE` before and `CLOSE` after; this helper never commits/rolls back.
    async fn drive_cursor(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tag: &str,
        cursor_name: &str,
        table_metadata: &TableMetadata,
        batch_size: usize,
        max_batch_bytes: usize,
        on_batch: &mut impl FnMut(RecordBatch) -> Result<(), ExtractorError>,
    ) -> Result<u64, ExtractorError> {
        let mut builder = RowBatchBuilder::with_capacity(table_metadata, batch_size)?;
        let mut total_rows: u64 = 0;
        loop {
            let fetch_sql = format!("{tag}FETCH FORWARD {batch_size} FROM {cursor_name}");
            let mut stream = sqlx::query(sqlx::AssertSqlSafe(fetch_sql.as_str())).fetch(&mut **tx);
            let mut fetched = 0usize;
            while let Some(row) = stream.try_next().await? {
                fetched += 1;
                builder.append_row(&row)?;
                total_rows += 1;
                if builder.should_flush(batch_size, max_batch_bytes) {
                    on_batch(builder.finish()?)?;
                }
            }
            if fetched == 0 {
                break;
            }
        }
        if !builder.is_empty() {
            on_batch(builder.finish()?)?;
        }
        Ok(total_rows)
    }

    /// Declare a cursor for `select_sql` (already rendered, may carry `$1/$2` binds),
    /// drive it to completion via [`Self::drive_cursor`], then `CLOSE` + commit/rollback.
    /// `bind` applies the statement parameters at `DECLARE` time.
    ///
    /// The cursor is deliberately declared `WITHOUT HOLD`: the extraction layer is a
    /// second-class citizen on the source — a `WITH HOLD` cursor forces the server to
    /// materialize the result (temp space + I/O on a production instance), while a
    /// plain cursor streams from the live snapshot with no server-side copy. If the DB
    /// owner kills a scan that impacts production, only that partition's work is lost
    /// and the split checkpoint lets the next run resume after it.
    ///
    /// Honest isolation semantics: the transaction stays open for the whole scan
    /// (`WITHOUT HOLD` portals die with it), but under the default READ COMMITTED
    /// isolation each `FETCH` takes a fresh snapshot — concurrent writes can duplicate
    /// or skip rows across `FETCH` windows. No snapshot/consistency guarantee is
    /// claimed; keyset ranges bound the damage to one partition, and the caller
    /// decides whether the source is quiet enough for the job at hand. (A stall
    /// longer than `idle_in_transaction_session_timeout` between `FETCH`es aborts the
    /// scan — the checkpoint records the split as failed, not completed.)
    #[allow(clippy::too_many_arguments)]
    async fn extract_via_cursor_impl<F>(
        &self,
        table_metadata: &TableMetadata,
        select_sql: &str,
        strategy_tag: &str,
        batch_size: usize,
        max_batch_bytes: usize,
        bind: F,
        on_batch: &mut impl FnMut(RecordBatch) -> Result<(), ExtractorError>,
    ) -> Result<u64, ExtractorError>
    where
        F: FnOnce(
            sqlx::query::Query<'_, Postgres, sqlx::postgres::PgArguments>,
        ) -> sqlx::query::Query<'_, Postgres, sqlx::postgres::PgArguments>,
    {
        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;
        let cursor_name = format!("extract_cur_{}", Uuid::new_v4().simple());
        let tag = self.tag(strategy_tag);
        let declare_sql =
            format!("{tag}DECLARE {cursor_name} CURSOR WITHOUT HOLD FOR {select_sql}");
        bind(sqlx::query(sqlx::AssertSqlSafe(declare_sql.as_str())))
            .execute(&mut *tx)
            .await?;

        // Guarded drive so CLOSE runs even if decode/flush fails — otherwise the
        // portal (and its open transaction) stays pinned on the pooled connection
        // until session close.
        let drive_result = Self::drive_cursor(
            &mut tx,
            &tag,
            &cursor_name,
            table_metadata,
            batch_size,
            max_batch_bytes,
            on_batch,
        )
        .await;

        let close_sql = format!("{tag}CLOSE {cursor_name}");
        let _ = sqlx::query(sqlx::AssertSqlSafe(close_sql.as_str()))
            .execute(&mut *tx)
            .await;
        match drive_result {
            Ok(total) => {
                tx.commit().await?;
                Ok(total)
            }
            Err(e) => {
                let _ = tx.rollback().await;
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
        self.extract_keyset_partition_via_cursor_with_limits(
            table_name,
            columns,
            partition_column,
            lo,
            hi,
            batch_size,
            DEFAULT_MAX_BATCH_BYTES,
        )
        .await
    }

    /// Same as [`Self::extract_keyset_partition_via_cursor`] with an explicit byte cap.
    #[allow(clippy::too_many_arguments)]
    pub async fn extract_keyset_partition_via_cursor_with_limits(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        partition_column: &str,
        lo: i64,
        hi: i64,
        batch_size: usize,
        max_batch_bytes: usize,
    ) -> Result<Vec<RecordBatch>, ExtractorError> {
        let mut batches = Vec::new();
        self.extract_keyset_partition_for_each_batch(
            table_name,
            columns,
            partition_column,
            lo,
            hi,
            batch_size,
            max_batch_bytes,
            &mut |batch| {
                batches.push(batch);
                Ok(())
            },
        )
        .await?;
        Ok(batches)
    }

    /// Bounded-memory keyset scan: invokes `on_batch` per flushed batch instead of
    /// accumulating a `Vec`. Used by concurrent partition extraction.
    #[allow(clippy::too_many_arguments)]
    pub async fn extract_keyset_partition_for_each_batch(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        partition_column: &str,
        lo: i64,
        hi: i64,
        batch_size: usize,
        max_batch_bytes: usize,
        on_batch: &mut impl FnMut(RecordBatch) -> Result<(), ExtractorError>,
    ) -> Result<u64, ExtractorError> {
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
        let sql = query_builder.sql().as_str().to_string();
        log::debug!("generated query [keyset_cursor]: {sql} (bind $1={lo}, $2={hi})");

        self.extract_via_cursor_impl(
            &table_metadata,
            &sql,
            "keyset_cursor",
            batch_size,
            max_batch_bytes,
            |q| q.bind(lo).bind(hi),
            on_batch,
        )
        .await
    }

    /// Extract all rows from a table without filtering.
    /// This is used for full table loads.
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

    pub(crate) async fn extract_full_table_via_cursor_batches(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>, ExtractorError> {
        self.extract_full_table_via_cursor_with_limits(
            table_name,
            columns,
            batch_size,
            DEFAULT_MAX_BATCH_BYTES,
        )
        .await
    }

    /// Same as [`Self::extract_full_table_via_cursor_batches`] with an explicit byte cap.
    pub(crate) async fn extract_full_table_via_cursor_with_limits(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        batch_size: usize,
        max_batch_bytes: usize,
    ) -> Result<Vec<RecordBatch>, ExtractorError> {
        let mut batches = Vec::new();
        self.extract_full_table_for_each_batch(
            table_name,
            columns,
            batch_size,
            max_batch_bytes,
            &mut |batch| {
                batches.push(batch);
                Ok(())
            },
        )
        .await?;
        Ok(batches)
    }

    /// Bounded-memory full scan: invokes `on_batch` per flushed batch instead of
    /// accumulating a `Vec`. Operational (`run`) paths should prefer this.
    pub async fn extract_full_table_for_each_batch(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        batch_size: usize,
        max_batch_bytes: usize,
        on_batch: &mut impl FnMut(RecordBatch) -> Result<(), ExtractorError>,
    ) -> Result<u64, ExtractorError> {
        let schema_reader = PostgresSchemaReader::new(&self.pool);
        let table_metadata = schema_reader
            .get_table_metadata(table_name)
            .await?
            .select_columns(columns.as_deref());

        let mut query_builder = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_full_table(&mut query_builder, &table_metadata);
        let sql = query_builder.sql().as_str().to_string();
        log::debug!("generated query [full]: {sql}");

        self.extract_via_cursor_impl(
            &table_metadata,
            &sql,
            "full",
            batch_size,
            max_batch_bytes,
            |q| q,
            on_batch,
        )
        .await
    }

    /// Bounded-memory full scan over `COPY (SELECT …) TO STDOUT (FORMAT BINARY)`.
    /// Same rows as [`Self::extract_full_table_for_each_batch`] with less per-row
    /// protocol overhead. Errors [`ExtractorError::UnsupportedType`] when the
    /// projection contains a type with no binary decoder — the support set mirrors
    /// the cursor decoder exactly, so opt out with `use_copy = false` for such tables.
    pub async fn extract_full_table_via_copy_for_each_batch(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        batch_size: usize,
        max_batch_bytes: usize,
        on_batch: &mut impl FnMut(RecordBatch) -> Result<(), ExtractorError>,
    ) -> Result<u64, ExtractorError> {
        use sqlx::postgres::PgPoolCopyExt as _;

        let schema_reader = PostgresSchemaReader::new(&self.pool);
        let table_metadata = schema_reader
            .get_table_metadata(table_name)
            .await?
            .select_columns(columns.as_deref());

        let mut query_builder = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_full_table(&mut query_builder, &table_metadata);
        let select = query_builder.sql().as_str().to_string();

        if !crate::connector::postgres::copy::supports_binary_copy(&table_metadata.columns) {
            return Err(ExtractorError::UnsupportedType(
                "binary COPY has no decoder for a column in this projection".into(),
            ));
        }

        let tag = self.tag("full_copy");
        log::debug!("generated query [full_copy]: COPY ({select}) TO STDOUT (FORMAT BINARY)");
        let copy_sql = format!("{tag}COPY ({select}) TO STDOUT (FORMAT BINARY)");

        // One pooled connection for the whole COPY (the pool checks it out
        // exclusively); no transaction needed — a single statement, own snapshot.
        let mut byte_stream = self.pool.copy_out_raw(copy_sql.as_str()).await?;
        crate::connector::postgres::copy::drive_copy_stream(
            &mut byte_stream,
            &table_metadata,
            batch_size,
            max_batch_bytes,
            on_batch,
        )
        .await
    }

    /// Same as [`Self::extract_full_table_via_copy_for_each_batch`] collecting to a
    /// `Vec` (for `extract()`-style callers; operational paths prefer the callback).
    pub(crate) async fn extract_full_table_via_copy_with_limits(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        batch_size: usize,
        max_batch_bytes: usize,
    ) -> Result<Vec<RecordBatch>, ExtractorError> {
        let mut batches = Vec::new();
        self.extract_full_table_via_copy_for_each_batch(
            table_name,
            columns,
            batch_size,
            max_batch_bytes,
            &mut |batch| {
                batches.push(batch);
                Ok(())
            },
        )
        .await?;
        Ok(batches)
    }

    /// Bounded-memory keyset scan over `COPY (SELECT … WHERE col >= lo AND col < hi)`.
    /// Bounds inline as integer literals — `COPY` accepts no bind parameters, and
    /// `i64` rendering has no quoting surface. Same strict-support contract as the
    /// full-table variant above.
    #[allow(clippy::too_many_arguments)]
    pub async fn extract_keyset_partition_via_copy_for_each_batch(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        partition_column: &str,
        lo: i64,
        hi: i64,
        batch_size: usize,
        max_batch_bytes: usize,
        on_batch: &mut impl FnMut(RecordBatch) -> Result<(), ExtractorError>,
    ) -> Result<u64, ExtractorError> {
        use sqlx::postgres::PgPoolCopyExt as _;

        let schema_reader = PostgresSchemaReader::new(&self.pool);
        let table_metadata = schema_reader.get_table_metadata(table_name).await?;
        let table_metadata = table_metadata.select_columns(columns.as_deref());

        let mut query_builder = QueryBuilder::<Postgres>::new("");
        PostgresQueryBuilder::build_keyset_partition_inline(
            &mut query_builder,
            &table_metadata,
            partition_column,
            lo,
            hi,
        );
        let select = query_builder.sql().as_str().to_string();

        if !crate::connector::postgres::copy::supports_binary_copy(&table_metadata.columns) {
            return Err(ExtractorError::UnsupportedType(
                "binary COPY has no decoder for a column in this projection".into(),
            ));
        }

        let tag = self.tag("keyset_copy");
        log::debug!("generated query [keyset_copy]: COPY ({select}) TO STDOUT (FORMAT BINARY)");

        let copy_sql = format!("{tag}COPY ({select}) TO STDOUT (FORMAT BINARY)");
        let mut byte_stream = self.pool.copy_out_raw(copy_sql.as_str()).await?;
        crate::connector::postgres::copy::drive_copy_stream(
            &mut byte_stream,
            &table_metadata,
            batch_size,
            max_batch_bytes,
            on_batch,
        )
        .await
    }

    /// Same as [`Self::extract_keyset_partition_via_copy_for_each_batch`] collecting
    /// to a `Vec` (for `extract()`-style callers; operational paths prefer the callback).
    #[allow(clippy::too_many_arguments)]
    pub async fn extract_keyset_partition_via_copy_with_limits(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        partition_column: &str,
        lo: i64,
        hi: i64,
        batch_size: usize,
        max_batch_bytes: usize,
    ) -> Result<Vec<RecordBatch>, ExtractorError> {
        let mut batches = Vec::new();
        self.extract_keyset_partition_via_copy_for_each_batch(
            table_name,
            columns,
            partition_column,
            lo,
            hi,
            batch_size,
            max_batch_bytes,
            &mut |batch| {
                batches.push(batch);
                Ok(())
            },
        )
        .await?;
        Ok(batches)
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
