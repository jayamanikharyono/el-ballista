//! PostgreSQL extractor.
//! extractor/postgres/extractor.rs
//! Orchestrates PostgreSQL schema discovery, query execution,
//! and conversion of PostgreSQL rows into Arrow `RecordBatch` values.
//!
//! Two scan mechanisms, both streaming bounded batches:
//! - **cursor** ([`cursor_scan`]): `DECLARE … CURSOR WITHOUT HOLD` inside a transaction,
//!   then `FETCH FORWARD <batch_size>` windows until empty;
//! - **binary COPY** ([`copy_scan`](crate::connector::postgres::copy::copy_scan)): one
//!   `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` statement.
//!
//! Both are also what the DataFusion/Ballista `PostgresExecutionPlan` runs.
//!
//! # Isolation semantics
//!
//! A cursor reads from the snapshot taken when it is `DECLARE`d: every `FETCH` of one
//! cursor sees the same snapshot (under READ COMMITTED as well — `FETCH` does not take a
//! new one), so **each partition is snapshot-consistent** on its own. A `COPY` is one
//! statement and likewise reads one snapshot. Separate partitions (and separate scans) are
//! separate statements on separate connections, so they are **not mutually consistent**:
//! a row updated between two partitions' snapshots can be missed or seen twice if the
//! update moves its partition key across partition bounds. Each partition pins its
//! snapshot (`xmin` horizon) for its whole duration, which holds back vacuum on the
//! source while it runs. No cross-partition snapshot guarantee is claimed.
use std::future::Future;

use futures::TryStreamExt;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use arrow::record_batch::RecordBatch;

use crate::connector::postgres::copy::{copy_scan, validate_batch_size};
use crate::connector::postgres::parallel::ScanPartition;
use crate::connector::postgres::{
    query_builder::PostgresQueryBuilder, row_adapter::PostgresRowAdapter,
    row_adapter::RowBatchBuilder, schema_reader::PostgresSchemaReader,
};
use crate::connector::query_tag::QuerySession;

use crate::{connector::errors::ExtractorError, types::table_metadata::TableMetadata};

/// Default byte cap per Arrow batch when callers only pass a row count (16 MiB).
pub(crate) const DEFAULT_MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;

/// A fresh, unique cursor name (`extract_cur_<uuid>`), safe to inline as an identifier.
pub(crate) fn new_cursor_name() -> String {
    format!("extract_cur_{}", Uuid::new_v4().simple())
}

/// The `"{tag}DECLARE {cursor} CURSOR WITHOUT HOLD FOR "` prefix; callers push the
/// `SELECT` (with any binds) after it.
pub(crate) fn declare_prefix(tag: &str, cursor_name: &str) -> QueryBuilder<Postgres> {
    QueryBuilder::new(format!(
        "{tag}DECLARE {cursor_name} CURSOR WITHOUT HOLD FOR "
    ))
}

/// Run one cursor scan on a pooled connection: `BEGIN`, execute `declare` (a
/// `DECLARE <cursor_name> CURSOR WITHOUT HOLD FOR SELECT …` built with
/// [`declare_prefix`], binds included), `FETCH FORWARD <batch_size>` until empty — each
/// `FETCH` its own statement, so `statement_timeout` applies per window — then `CLOSE` and
/// `COMMIT` (or `ROLLBACK` on any error). Shared by the extractor and the
/// DataFusion/Ballista execution plan.
///
/// `on_batch` is awaited per flushed batch (rows **or** bytes cap). An `Err` from it — a
/// consumer that went away — stops fetching; the cursor is closed and the transaction
/// rolled back, so the source stops scanning after at most one in-flight window.
///
/// The cursor is deliberately `WITHOUT HOLD`: a `WITH HOLD` cursor forces the server to
/// materialize the result at commit (temp space + I/O on a production instance), while a
/// plain cursor streams. See the module docs for isolation semantics. A stall longer than
/// `idle_in_transaction_session_timeout` between `FETCH`es aborts the scan.
// Justification (AGENTS §1): the parameters are the independent scan knobs (source,
// projection, bounds, batch/byte caps, callback); a params struct would only rename them.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn cursor_scan<F, Fut>(
    pool: &PgPool,
    tag: &str,
    cursor_name: &str,
    mut declare: QueryBuilder<Postgres>,
    table_metadata: &TableMetadata,
    batch_size: usize,
    max_batch_bytes: usize,
    on_batch: F,
) -> Result<u64, ExtractorError>
where
    F: FnMut(RecordBatch) -> Fut,
    Fut: Future<Output = Result<(), ExtractorError>>,
{
    validate_batch_size(batch_size)?;
    // Build decoders before touching the source: unsupported types fail up front.
    let builder = RowBatchBuilder::with_capacity(table_metadata, batch_size)?;

    let mut conn = pool.acquire().await?;
    let mut tx = conn.begin().await?;
    declare.build().execute(&mut *tx).await?;

    // Guarded drive so CLOSE runs even if decode/flush fails — otherwise the portal (and
    // its open transaction) stays pinned on the pooled connection until session close.
    let drive_result = drive_cursor(
        &mut tx,
        tag,
        cursor_name,
        builder,
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

/// Drive an open cursor to completion, awaiting `on_batch` per flushed Arrow batch.
///
/// Each `FETCH FORWARD` window streams rows via `fetch()` + `try_next()` (no intermediate
/// `Vec<PgRow>`) into the [`RowBatchBuilder`], flushing on **rows OR bytes** so wide rows
/// cannot blow memory before `batch_size`. Never commits/rolls back.
async fn drive_cursor<F, Fut>(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tag: &str,
    cursor_name: &str,
    mut builder: RowBatchBuilder,
    batch_size: usize,
    max_batch_bytes: usize,
    mut on_batch: F,
) -> Result<u64, ExtractorError>
where
    F: FnMut(RecordBatch) -> Fut,
    Fut: Future<Output = Result<(), ExtractorError>>,
{
    let fetch_sql = format!("{tag}FETCH FORWARD {batch_size} FROM {cursor_name}");
    let mut total_rows: u64 = 0;
    loop {
        let mut fetched = 0usize;
        {
            let mut stream = sqlx::query(sqlx::AssertSqlSafe(fetch_sql.as_str())).fetch(&mut **tx);
            while let Some(row) = stream.try_next().await? {
                fetched += 1;
                builder.append_row(&row)?;
                total_rows += 1;
                if builder.should_flush(batch_size, max_batch_bytes) {
                    // Awaiting the consumer mid-window is fine: the FETCH result is bounded
                    // by `batch_size` rows and the socket applies backpressure.
                    on_batch(builder.finish()?).await?;
                }
            }
        }
        if fetched == 0 {
            break;
        }
    }
    if !builder.is_empty() {
        on_batch(builder.finish()?).await?;
    }
    Ok(total_rows)
}

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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
    ///
    /// let password = std::env::var("PGPASSWORD")?;
    /// let ex = PostgresExtractor::connect(
    ///     "localhost", 5432, "postgres", &password, "shop", 4, 30_000, "orders-extract",
    /// ).await?;
    /// let batch = ex.extract_full_table("orders", Some(vec!["id", "status"])).await?;
    /// println!("{} rows", batch.num_rows());
    /// # Ok(()) }
    /// ```
    // Justification (AGENTS §1): one parameter per connection setting (host, port, user,
    // password, database, pool size, timeout, application name); long-standing public API.
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use rust_ballista_extraction_layer::connector::errors::ExtractorError;
    /// # use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// let (n,): (i64,) = sqlx::query_as("SELECT count(*) FROM orders").fetch_one(ex.pool()).await?;
    /// println!("{n} rows");
    /// # Ok(()) }
    /// ```
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Render a fresh debug tag for one query issued through this extractor. See
    /// `connector::query_tag`; prepended to DECLARE/FETCH statements below so the running
    /// statement is identifiable directly in `pg_stat_activity`.
    fn tag(&self, strategy: &str) -> String {
        self.query_session.tag(strategy).render()
    }

    /// Resolve the projection: catalog columns of `table_name`, narrowed to `columns`.
    /// Unknown tables and unknown column names are typed errors.
    async fn projection(
        &self,
        table_name: &str,
        columns: Option<&[&str]>,
    ) -> Result<TableMetadata, ExtractorError> {
        let schema_reader = PostgresSchemaReader::new(&self.pool);
        Ok(schema_reader
            .get_table_metadata(table_name)
            .await?
            .select_columns(columns)?)
    }

    /// Cursor-scan `table_metadata` with the `SELECT` that `push_select` appends after the
    /// `DECLARE` prefix.
    async fn cursor_select(
        &self,
        table_metadata: &TableMetadata,
        strategy_tag: &str,
        batch_size: usize,
        max_batch_bytes: usize,
        push_select: impl FnOnce(&mut QueryBuilder<Postgres>),
        on_batch: &mut impl FnMut(RecordBatch) -> Result<(), ExtractorError>,
    ) -> Result<u64, ExtractorError> {
        let tag = self.tag(strategy_tag);
        let cursor_name = new_cursor_name();
        let mut declare = declare_prefix(&tag, &cursor_name);
        push_select(&mut declare);
        log::debug!(
            "generated query [{strategy_tag}]: {}",
            declare.sql().as_str()
        );
        cursor_scan(
            &self.pool,
            &tag,
            &cursor_name,
            declare,
            table_metadata,
            batch_size,
            max_batch_bytes,
            |b| std::future::ready(on_batch(b)),
        )
        .await
    }

    /// Binary-COPY `table_metadata` with the `SELECT` that `push_select` renders (no binds:
    /// `COPY` accepts none).
    async fn copy_select(
        &self,
        table_metadata: &TableMetadata,
        strategy_tag: &str,
        batch_size: usize,
        max_batch_bytes: usize,
        push_select: impl FnOnce(&mut QueryBuilder<Postgres>),
        on_batch: &mut impl FnMut(RecordBatch) -> Result<(), ExtractorError>,
    ) -> Result<u64, ExtractorError> {
        if !crate::connector::postgres::copy::supports_binary_copy(&table_metadata.columns) {
            return Err(ExtractorError::UnsupportedType(
                "binary COPY has no decoder for a column in this projection".into(),
            ));
        }
        let mut select = QueryBuilder::<Postgres>::new("");
        push_select(&mut select);
        let tag = self.tag(strategy_tag);
        let copy_sql = format!(
            "{tag}COPY ({}) TO STDOUT (FORMAT BINARY)",
            select.sql().as_str()
        );
        log::debug!("generated query [{strategy_tag}]: {copy_sql}");
        copy_scan(
            &self.pool,
            &copy_sql,
            &tag,
            table_metadata,
            batch_size,
            max_batch_bytes,
            None,
            |b| std::future::ready(on_batch(b)),
        )
        .await
    }

    // ---------------------------------------------------------------------------------
    // Streaming entry points (bounded memory): `*_for_each_batch`.
    // ---------------------------------------------------------------------------------

    /// Bounded-memory full scan over a cursor: invokes `on_batch` per flushed batch.
    /// Operational (`run`) paths use this (or its COPY twin).
    ///
    /// Errors: [`ExtractorError::InvalidConfig`] for `batch_size = 0`,
    /// [`ExtractorError::TableNotFound`], [`ExtractorError::Projection`] for an unknown
    /// column, [`ExtractorError::UnsupportedValue`] for a value with no faithful Arrow form.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use rust_ballista_extraction_layer::connector::errors::ExtractorError;
    /// # use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// let total = ex.extract_full_table_for_each_batch("orders", None, 8192, 16 << 20,
    ///     &mut |batch| {
    ///         println!("{} rows", batch.num_rows()); // hand each batch on; memory stays bounded
    ///         Ok(())
    ///     },
    /// ).await?;
    /// println!("{total} rows extracted");
    /// # Ok(()) }
    /// ```
    pub async fn extract_full_table_for_each_batch(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        batch_size: usize,
        max_batch_bytes: usize,
        on_batch: &mut impl FnMut(RecordBatch) -> Result<(), ExtractorError>,
    ) -> Result<u64, ExtractorError> {
        validate_batch_size(batch_size)?;
        let table_metadata = self.projection(table_name, columns.as_deref()).await?;
        self.cursor_select(
            &table_metadata,
            "full",
            batch_size,
            max_batch_bytes,
            |qb| PostgresQueryBuilder::build_full_table(qb, &table_metadata),
            on_batch,
        )
        .await
    }

    /// Bounded-memory full scan over `COPY (SELECT …) TO STDOUT (FORMAT BINARY)`.
    /// Same rows as [`Self::extract_full_table_for_each_batch`] with less per-row
    /// protocol overhead. Errors [`ExtractorError::UnsupportedType`] when the
    /// projection contains a type with no binary decoder — the support set mirrors
    /// the cursor decoder exactly, so opt out with `use_copy = false` for such tables.
    /// COPY is one statement: `statement_timeout` bounds the whole scan.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use rust_ballista_extraction_layer::connector::errors::ExtractorError;
    /// # use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// let total = ex.extract_full_table_via_copy_for_each_batch("orders", None, 8192, 16 << 20,
    ///     &mut |batch| {
    ///         println!("{} rows", batch.num_rows()); // hand each batch on; memory stays bounded
    ///         Ok(())
    ///     },
    /// ).await?;
    /// println!("{total} rows extracted");
    /// # Ok(()) }
    /// ```
    pub async fn extract_full_table_via_copy_for_each_batch(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        batch_size: usize,
        max_batch_bytes: usize,
        on_batch: &mut impl FnMut(RecordBatch) -> Result<(), ExtractorError>,
    ) -> Result<u64, ExtractorError> {
        validate_batch_size(batch_size)?;
        let table_metadata = self.projection(table_name, columns.as_deref()).await?;
        self.copy_select(
            &table_metadata,
            "full_copy",
            batch_size,
            max_batch_bytes,
            |qb| PostgresQueryBuilder::build_full_table(qb, &table_metadata),
            on_batch,
        )
        .await
    }

    /// Bounded-memory scan of an explicit half-open key range `[lo, hi)` over a cursor
    /// (bounds bound as `$1/$2`). Rows whose key is NULL are in no explicit range; to cover
    /// a whole table use partitions from
    /// [`compute_keyset_partitions`](crate::connector::postgres::parallel::compute_keyset_partitions)
    /// with [`Self::extract_partition_for_each_batch`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use rust_ballista_extraction_layer::connector::errors::ExtractorError;
    /// # use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// // Rows with 0 <= id < 100_000.
    /// let total = ex.extract_keyset_partition_for_each_batch("orders", None, "id", 0, 100_000,
    ///     8192, 16 << 20,
    ///     &mut |batch| {
    ///         println!("{} rows", batch.num_rows()); // hand each batch on; memory stays bounded
    ///         Ok(())
    ///     },
    /// ).await?;
    /// println!("{total} rows extracted");
    /// # Ok(()) }
    /// ```
    // Justification (AGENTS §1): the parameters are the independent scan knobs (source,
    // projection, bounds, batch/byte caps, callback); a params struct would only rename them.
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
        validate_batch_size(batch_size)?;
        let table_metadata = self.projection(table_name, columns.as_deref()).await?;
        self.cursor_select(
            &table_metadata,
            "keyset_cursor",
            batch_size,
            max_batch_bytes,
            |qb| {
                PostgresQueryBuilder::build_keyset_partition(
                    qb,
                    &table_metadata,
                    partition_column,
                    lo,
                    hi,
                )
            },
            on_batch,
        )
        .await
    }

    /// COPY twin of [`Self::extract_keyset_partition_for_each_batch`]: bounds inline as
    /// integer literals (`COPY` accepts no bind parameters, and `i64` rendering has no
    /// quoting surface).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use rust_ballista_extraction_layer::connector::errors::ExtractorError;
    /// # use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// // Rows with 0 <= id < 100_000, over binary COPY.
    /// let total = ex.extract_keyset_partition_via_copy_for_each_batch("orders", None, "id",
    ///     0, 100_000, 8192, 16 << 20,
    ///     &mut |batch| {
    ///         println!("{} rows", batch.num_rows()); // hand each batch on; memory stays bounded
    ///         Ok(())
    ///     },
    /// ).await?;
    /// println!("{total} rows extracted");
    /// # Ok(()) }
    /// ```
    // Justification (AGENTS §1): the parameters are the independent scan knobs (source,
    // projection, bounds, batch/byte caps, callback); a params struct would only rename them.
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
        validate_batch_size(batch_size)?;
        let table_metadata = self.projection(table_name, columns.as_deref()).await?;
        self.copy_select(
            &table_metadata,
            "keyset_copy",
            batch_size,
            max_batch_bytes,
            |qb| {
                PostgresQueryBuilder::build_keyset_partition_inline(
                    qb,
                    &table_metadata,
                    partition_column,
                    lo,
                    hi,
                )
            },
            on_batch,
        )
        .await
    }

    /// Bounded-memory scan of one computed partition (`None` = the whole table), over a
    /// cursor or, with `use_copy`, binary COPY.
    ///
    /// `partition` must come from
    /// [`compute_keyset_partitions`](crate::connector::postgres::parallel::compute_keyset_partitions)
    /// or `compute_ctid_partitions`:
    /// its predicate (literals this crate rendered) is inlined into the SQL. Keyset
    /// partitions from one computation cover every row exactly once, NULL keys included.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use rust_ballista_extraction_layer::connector::errors::ExtractorError;
    /// # use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// use rust_ballista_extraction_layer::connector::postgres::parallel::compute_keyset_partitions;
    ///
    /// for part in compute_keyset_partitions(ex.pool(), "public", "orders", "id", 4).await? {
    ///     let rows = ex.extract_partition_for_each_batch("orders", None, Some(&part), true,
    ///         8192, 16 << 20, &mut |_batch| Ok(()),
    ///     ).await?;
    ///     println!("partition {}: {rows} rows", part.partition_id);
    /// }
    /// # Ok(()) }
    /// ```
    // Justification (AGENTS §1): the parameters are the independent scan knobs (source,
    // projection, bounds, batch/byte caps, callback); a params struct would only rename them.
    #[allow(clippy::too_many_arguments)]
    pub async fn extract_partition_for_each_batch(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        partition: Option<&ScanPartition>,
        use_copy: bool,
        batch_size: usize,
        max_batch_bytes: usize,
        on_batch: &mut impl FnMut(RecordBatch) -> Result<(), ExtractorError>,
    ) -> Result<u64, ExtractorError> {
        validate_batch_size(batch_size)?;
        let table_metadata = self.projection(table_name, columns.as_deref()).await?;
        let predicate = partition.and_then(|p| p.predicate.as_deref());
        let push = |qb: &mut QueryBuilder<Postgres>| {
            PostgresQueryBuilder::build_partition(qb, &table_metadata, predicate)
        };
        if use_copy {
            self.copy_select(
                &table_metadata,
                "partition_copy",
                batch_size,
                max_batch_bytes,
                push,
                on_batch,
            )
            .await
        } else {
            self.cursor_select(
                &table_metadata,
                "partition_cursor",
                batch_size,
                max_batch_bytes,
                push,
                on_batch,
            )
            .await
        }
    }

    // ---------------------------------------------------------------------------------
    // Materializing convenience wrappers. These hold the WHOLE result in memory
    // (`Vec<RecordBatch>`, or one concatenated `RecordBatch`): for tests, examples and
    // small tables only. No internal streaming path calls the concatenating ones.
    // ---------------------------------------------------------------------------------

    /// **Materializing**: [`Self::extract_partition_for_each_batch`] collected to a `Vec`.
    pub(crate) async fn extract_partition_with_limits(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        partition: Option<&ScanPartition>,
        use_copy: bool,
        batch_size: usize,
        max_batch_bytes: usize,
    ) -> Result<Vec<RecordBatch>, ExtractorError> {
        let mut batches = Vec::new();
        self.extract_partition_for_each_batch(
            table_name,
            columns,
            partition,
            use_copy,
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

    /// **Materializing**: one computed partition (cursor path) concatenated into a single
    /// `RecordBatch` (empty result keeps the schema).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use rust_ballista_extraction_layer::connector::errors::ExtractorError;
    /// # use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// use rust_ballista_extraction_layer::connector::postgres::parallel::compute_keyset_partitions;
    ///
    /// let parts = compute_keyset_partitions(ex.pool(), "public", "orders", "id", 4).await?;
    /// let batch = ex.extract_partition("orders", None, &parts[0]).await?; // small tables only
    /// println!("{} rows", batch.num_rows());
    /// # Ok(()) }
    /// ```
    pub async fn extract_partition(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        partition: &ScanPartition,
    ) -> Result<RecordBatch, ExtractorError> {
        let table_metadata = self.projection(table_name, columns.as_deref()).await?;
        let batches = self
            .extract_partition_with_limits(
                table_name,
                columns,
                Some(partition),
                false,
                8192,
                DEFAULT_MAX_BATCH_BYTES,
            )
            .await?;
        concat_or_empty(&table_metadata, &batches)
    }

    /// **Materializing**: explicit key range `[lo, hi)` over a cursor, collected to a `Vec`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use rust_ballista_extraction_layer::connector::errors::ExtractorError;
    /// # use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// let batches = ex.extract_keyset_partition_via_cursor("orders", None, "id", 0, 1_000, 256).await?;
    /// let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    /// println!("{rows} rows in {} batches", batches.len());
    /// # Ok(()) }
    /// ```
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

    /// **Materializing**: same as [`Self::extract_keyset_partition_via_cursor`] with an
    /// explicit byte cap.
    // Justification (AGENTS §1): the parameters are the independent scan knobs (source,
    // projection, bounds, batch/byte caps, callback); a params struct would only rename them.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn extract_keyset_partition_via_cursor_with_limits(
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

    /// **Materializing**: COPY twin of
    /// `Self::extract_keyset_partition_via_cursor_with_limits`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use rust_ballista_extraction_layer::connector::errors::ExtractorError;
    /// # use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// let batches = ex
    ///     .extract_keyset_partition_via_copy_with_limits("orders", None, "id", 0, 1_000, 256, 1 << 20)
    ///     .await?;
    /// assert!(batches.iter().all(|b| b.num_rows() <= 256));
    /// # Ok(()) }
    /// ```
    // Justification (AGENTS §1): the parameters are the independent scan knobs (source,
    // projection, bounds, batch/byte caps, callback); a params struct would only rename them.
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

    /// **Materializing**: the whole table (cursor path) concatenated into ONE
    /// `RecordBatch`. For large tables use [`Self::extract_full_table_for_each_batch`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use rust_ballista_extraction_layer::connector::errors::ExtractorError;
    /// # use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// let batch = ex.extract_full_table("orders", Some(vec!["id", "status"])).await?;
    /// assert_eq!(batch.num_columns(), 2);
    /// # Ok(()) }
    /// ```
    pub async fn extract_full_table(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
    ) -> Result<RecordBatch, ExtractorError> {
        self.extract_full_table_with_batch_size(table_name, columns, 8192)
            .await
    }

    /// **Materializing**: [`Self::extract_full_table`] with an explicit fetch size.
    pub(crate) async fn extract_full_table_with_batch_size(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        batch_size: usize,
    ) -> Result<RecordBatch, ExtractorError> {
        let table_metadata = self.projection(table_name, columns.as_deref()).await?;
        let batches = self
            .extract_full_table_via_cursor_batches(table_name, columns, batch_size)
            .await?;
        concat_or_empty(&table_metadata, &batches)
    }

    /// **Materializing**: full cursor scan collected to a `Vec`.
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

    /// **Materializing**: full cursor scan with an explicit byte cap, collected to a `Vec`.
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

    /// **Materializing**: explicit key range `[lo, hi)` concatenated into ONE
    /// `RecordBatch`. Rows whose key is NULL are in no explicit range.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use rust_ballista_extraction_layer::connector::errors::ExtractorError;
    /// # use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
    /// # async fn demo(ex: &PostgresExtractor) -> Result<(), ExtractorError> {
    /// // Rows with 1_000 <= id < 2_000, as one batch.
    /// let batch = ex.extract_keyset_partition("orders", None, "id", 1_000, 2_000).await?;
    /// println!("{} rows", batch.num_rows());
    /// # Ok(()) }
    /// ```
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

    /// **Materializing**: [`Self::extract_keyset_partition`] with an explicit fetch size.
    pub(crate) async fn extract_keyset_partition_with_batch_size(
        &self,
        table_name: &str,
        columns: Option<Vec<&str>>,
        partition_column: &str,
        lo: i64,
        hi: i64,
        batch_size: usize,
    ) -> Result<RecordBatch, ExtractorError> {
        let table_metadata = self.projection(table_name, columns.as_deref()).await?;
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
        concat_or_empty(&table_metadata, &batches)
    }
}

/// Concatenate a materialized result; an empty result still carries the schema.
fn concat_or_empty(
    table_metadata: &TableMetadata,
    batches: &[RecordBatch],
) -> Result<RecordBatch, ExtractorError> {
    let arrow_schema = PostgresRowAdapter::build_arrow_schema(table_metadata)?;
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(arrow_schema));
    }
    Ok(arrow::compute::concat_batches(&arrow_schema, batches)?)
}
