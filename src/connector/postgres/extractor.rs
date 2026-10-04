//! PostgreSQL scan mechanics: how one partition's rows are read into Arrow `RecordBatch`es.
//! connector/postgres/extractor.rs
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
//! update moves it into another partition's range — under keyset partitioning by changing
//! its partition key; under ctid partitioning by any update that writes the new row version
//! to another page (one that is not HOT), whatever columns it changes. Each partition pins its
//! snapshot (`xmin` horizon) for its whole duration, which holds back vacuum on the
//! source while it runs. No cross-partition snapshot guarantee is claimed.
use std::future::Future;

use futures::TryStreamExt;
use sqlx::{Connection, PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use arrow::record_batch::RecordBatch;

use crate::connector::postgres::copy::validate_batch_size;
use crate::connector::postgres::row_adapter::RowBatchBuilder;

use crate::{connector::errors::ExtractorError, types::table_metadata::TableMetadata};

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
    let builder = RowBatchBuilder::for_batches(table_metadata, batch_size, max_batch_bytes)?;

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
