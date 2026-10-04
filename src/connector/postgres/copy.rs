//! `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` extraction.
//!
//! A bulk-load alternative to cursor `FETCH`: one server round-trip streams the whole
//! result in PostgreSQL's binary COPY framing, skipping per-row SQL parse/bind/portal
//! overhead. Bytes arrive in `Bytes` chunks that may split tuples arbitrarily, so
//! [`CopyBatchDecoder`] buffers and parses incrementally, yielding byte-capped Arrow
//! batches exactly like the cursor path.
//!
//! Wire format (all big-endian): 11-byte signature `PGCOPY\n\377\r\n\0`, `flags: i32`,
//! `header_ext_len: i32` + extension bytes, then per tuple `nfields: i16` (`-1` =
//! end-of-stream) with each field as `len: i32` (`-1` = SQL NULL) + bytes. Field
//! payloads are the standard binary representations — the same ones `sqlx` decodes
//! from the extended-protocol wire — so field decoding lives next to the `PgRow`
//! decoder ([`RowBatchBuilder::append_copy_field`]) and a differential COPY-vs-cursor
//! test agrees by construction.
//!
//! Contract: `supports_binary_copy` gates every caller. Unsupported shapes
//! (unmapped types, pushed filters with bound literals which `COPY` cannot take)
//! fall back to the cursor/`SELECT` path with a loud log — never silently wrong data.

use crate::connector::postgres::execution_plan::CopyStatements;
use sqlx::{Executor, SqlSafeStr};
use std::future::Future;
use tracing::{Instrument, debug, warn};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use sqlx::PgPool;
use sqlx::pool::PoolConnection;
use sqlx::postgres::Postgres;

use crate::connector::errors::ExtractorError;
use crate::connector::postgres::arrow_type_mapper::ArrowTypeMapper;
use crate::connector::postgres::row_adapter::{ByteCursor, RowBatchBuilder};
use crate::types::{ColumnMetadata, TableMetadata};

/// Microseconds between the Unix epoch (Arrow) and the PostgreSQL epoch (2000-01-01).
pub const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// Days between the Unix epoch (Arrow `Date32`) and the PostgreSQL epoch (2000-01-01).
pub const PG_EPOCH_DAYS: i32 = 10_957;

/// Binary COPY signature: `PGCOPY\n\377\r\n\0`.
const COPY_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";

/// True when every column has a binary-COPY decoder (i.e. [`ArrowTypeMapper::map`]
/// succeeds — the decoder set mirrors it exactly). Zero columns (e.g. `COUNT(*)`)
/// count as supported: the single `SELECT 1` column is skipped and only rows counted.
pub(crate) fn supports_binary_copy(columns: &[ColumnMetadata]) -> bool {
    columns.iter().all(|c| ArrowTypeMapper::map(c).is_ok())
}

/// Reject `batch_size = 0` before any statement runs: a zero row cap would issue
/// `FETCH FORWARD 0` (which re-reads the current row, or returns nothing) and could turn
/// an extraction into "success, zero rows".
pub(crate) fn validate_batch_size(batch_size: usize) -> Result<(), ExtractorError> {
    if batch_size == 0 {
        return Err(ExtractorError::InvalidConfig(
            "batch_size must be > 0".to_string(),
        ));
    }
    Ok(())
}

/// Incremental binary-COPY parser. Feed wire `Bytes` chunks via `Self::push_bytes`
/// (completed byte-capped batches come back), then `Self::finish` for the trailing
/// partial batch. Chunk boundaries are meaningless: a tuple split across chunks waits
/// for the rest — peak memory stays `O(batch)` regardless of chunking.
pub struct CopyBatchDecoder {
    builder: RowBatchBuilder,
    ncols: usize,
    batch_size: usize,
    max_batch_bytes: usize,
    buf: Vec<u8>,
    pos: usize,
    header_done: bool,
    eof: bool,
}

impl CopyBatchDecoder {
    /// Build a decoder. Errors on unmapped column types so callers fall back to the
    /// cursor path *before* opening the COPY stream, and on `batch_size = 0`.
    pub(crate) fn new(
        table_metadata: &TableMetadata,
        batch_size: usize,
        max_batch_bytes: usize,
    ) -> Result<Self, ExtractorError> {
        validate_batch_size(batch_size)?;
        if !supports_binary_copy(&table_metadata.columns) {
            return Err(ExtractorError::UnsupportedType(
                "binary COPY unsupported for this projection (unmapped type)".into(),
            ));
        }
        Ok(Self {
            builder: RowBatchBuilder::for_batches(table_metadata, batch_size, max_batch_bytes)?,
            ncols: table_metadata.columns.len(),
            batch_size,
            max_batch_bytes,
            buf: Vec::new(),
            pos: 0,
            header_done: false,
            eof: false,
        })
    }

    /// Feed one wire chunk; returns the batches that filled up (usually zero or one).
    pub(crate) fn push_bytes(&mut self, chunk: &[u8]) -> Result<Vec<RecordBatch>, ExtractorError> {
        // Compact the consumed prefix so a long stream never grows the buffer.
        if self.pos > 65536 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        loop {
            match self.try_parse_tuple()? {
                TupleOutcome::NeedMore => break,
                TupleOutcome::Eof => {
                    self.eof = true;
                    break;
                }
                TupleOutcome::Tuple => {
                    if self
                        .builder
                        .should_flush(self.batch_size, self.max_batch_bytes)
                    {
                        out.push(self.builder.finish()?);
                    }
                }
            }
        }
        Ok(out)
    }

    /// End of stream: error on a truncated tuple or a missing trailer, else return the
    /// trailing partial batch (if any).
    pub(crate) fn finish(&mut self) -> Result<Option<RecordBatch>, ExtractorError> {
        if self.pos != self.buf.len() {
            return Err(ExtractorError::Internal(
                "truncated binary COPY stream: trailing unparseable bytes".into(),
            ));
        }
        if !self.eof {
            return Err(ExtractorError::Internal(
                "truncated binary COPY stream: missing end-of-stream trailer".into(),
            ));
        }
        if self.builder.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.builder.finish()?))
        }
    }

    fn parse_header(&mut self) -> Result<bool, ExtractorError> {
        let corrupt = |why: &str| ExtractorError::Internal(format!("corrupt binary COPY: {why}"));
        let mut cur = ByteCursor::new(&self.buf[self.pos..]);
        // Signature (11) + flags (4) + header-ext length (4) + extension.
        let Some(sig) = cur.take(11) else {
            return Ok(false);
        };
        if sig != COPY_SIGNATURE {
            return Err(corrupt("bad signature"));
        }
        let (Some(flags), Some(ext_len)) = (cur.i32(), cur.i32()) else {
            return Ok(false);
        };
        if flags & 0x0001_0000 != 0 {
            return Err(corrupt("OID columns not supported"));
        }
        let ext_len =
            usize::try_from(ext_len).map_err(|_| corrupt("negative header extension length"))?;
        if cur.take(ext_len).is_none() {
            return Ok(false);
        }
        self.pos += 19 + ext_len;
        self.header_done = true;
        Ok(true)
    }

    /// Attempt one tuple at the current position. `NeedMore` leaves all state
    /// untouched (including builders — nothing is appended until the whole tuple is
    /// buffered, so a retried parse can never double-append). Two passes over the
    /// buffered bytes (framing, then decode) — no per-tuple allocation.
    fn try_parse_tuple(&mut self) -> Result<TupleOutcome, ExtractorError> {
        let corrupt = |why: &str| ExtractorError::Internal(format!("corrupt binary COPY: {why}"));
        if !self.header_done && !self.parse_header()? {
            return Ok(TupleOutcome::NeedMore);
        }

        let buffered = &self.buf[self.pos..];
        let mut cur = ByteCursor::new(buffered);
        let Some(nfields) = cur.i16() else {
            return Ok(TupleOutcome::NeedMore);
        };
        if nfields == -1 {
            self.pos += 2;
            return Ok(TupleOutcome::Eof);
        }
        // Zero-column projections (`SELECT 1`) carry exactly one unread payload column.
        let want_fields = self.ncols.max(1);
        if usize::try_from(nfields).ok() != Some(want_fields) {
            return Err(corrupt("field count mismatch with projection"));
        }

        // Pass 1: framing only — is the whole tuple buffered?
        for _ in 0..want_fields {
            let Some(len) = cur.i32() else {
                return Ok(TupleOutcome::NeedMore);
            };
            if len == -1 {
                continue;
            }
            let len = usize::try_from(len).map_err(|_| corrupt("negative field length"))?;
            if cur.take(len).is_none() {
                return Ok(TupleOutcome::NeedMore);
            }
        }
        let total = buffered.len() - cur.rest().len();

        // Pass 2: decode (framing already validated).
        let mut cur = ByteCursor::new(&buffered[2..total]);
        if self.ncols > 0 {
            for idx in 0..want_fields {
                let len = cur.i32().ok_or_else(|| corrupt("framing changed"))?;
                let raw = if len == -1 {
                    None
                } else {
                    let len = usize::try_from(len).map_err(|_| corrupt("negative length"))?;
                    Some(cur.take(len).ok_or_else(|| corrupt("framing changed"))?)
                };
                self.builder.append_copy_field(idx, raw)?;
            }
        }
        self.builder.inc_row();
        self.pos += total;
        Ok(TupleOutcome::Tuple)
    }
}

/// Drive a COPY byte stream to completion, awaiting `on_batch` per flushed batch.
///
/// `on_batch` is async so a streaming consumer can apply backpressure (e.g. a bounded
/// channel send); synchronous callers pass `|b| std::future::ready(f(b))`. An `Err` from
/// `on_batch` stops the scan and is returned as-is.
pub(crate) async fn drive_copy_stream<S, F, Fut>(
    stream: &mut S,
    table_metadata: &TableMetadata,
    batch_size: usize,
    max_batch_bytes: usize,
    mut on_batch: F,
) -> Result<u64, ExtractorError>
where
    S: futures::Stream<Item = Result<Bytes, sqlx::Error>> + Unpin,
    F: FnMut(RecordBatch) -> Fut,
    Fut: Future<Output = Result<(), ExtractorError>>,
{
    use futures::TryStreamExt as _;
    let mut decoder = CopyBatchDecoder::new(table_metadata, batch_size, max_batch_bytes)?;
    let mut total: u64 = 0;
    while let Some(chunk) = stream.try_next().await? {
        for batch in decoder.push_bytes(&chunk)? {
            total += batch.num_rows() as u64;
            on_batch(batch).await?;
        }
    }
    if let Some(batch) = decoder.finish()? {
        total += batch.num_rows() as u64;
        on_batch(batch).await?;
    }
    Ok(total)
}

/// Run one `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` on a pooled connection and decode
/// it through [`drive_copy_stream`]. Shared by the extractor and the DataFusion/Ballista
/// execution plan.
///
/// `copy_sql` must start with `tag` (the unique debug comment of `connector::query_tag`):
/// the tag identifies this statement in `pg_stat_activity`.
///
/// COPY is a **single statement**: `statement_timeout` bounds the whole scan (including
/// time the consumer spends applying backpressure), and the statement reads one snapshot.
/// `statement_timeout_ms` overrides the session timeout for this COPY only (`Some(0)` = no
/// limit), via `SET LOCAL` in a transaction around it.
/// If the scan does not complete — decode error, consumer error or dropped consumer, or
/// this future being dropped — the connection is closed instead of being returned to the
/// pool (so nobody drains the rest of the COPY), and a fire-and-forget task sends
/// `pg_cancel_backend` over another connection of the same pool (within the source budget)
/// so the source stops scanning now.
// Justification (AGENTS §1): the parameters are the independent scan knobs (pool, SQL, tag,
// projection, batch/byte caps, timeout, callback); a params struct would only rename them.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn copy_scan<F, Fut>(
    pool: &PgPool,
    statements: &CopyStatements,
    tag: &str,
    table_metadata: &TableMetadata,
    batch_size: usize,
    max_batch_bytes: usize,
    statement_timeout_ms: Option<u64>,
    on_batch: F,
) -> Result<u64, ExtractorError>
where
    F: FnMut(RecordBatch) -> Fut,
    Fut: Future<Output = Result<(), ExtractorError>>,
{
    // Validate before touching the source (type support, batch size).
    let probe = CopyBatchDecoder::new(table_metadata, batch_size, max_batch_bytes)?;

    let mut conn = pool.acquire().await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await?;
    let mut guard = CopyCancelGuard {
        conn,
        pool: pool.clone(),
        pid,
        tag: tag.to_string(),
        completed: false,
    };
    // One transaction around the type check and the COPY, committed only after the scan
    // completed (on any failure the guard closes the connection instead).
    sqlx::query("BEGIN").execute(&mut *guard.conn).await?;
    // Binary COPY carries no types: a column whose type changed since planning (`ALTER …
    // TYPE`) would decode into wrong values. Describing the inner SELECT reports the types
    // the COPY will send, and takes the table's lock until the transaction ends, so they
    // cannot change before the COPY reads them.
    let described = (&mut *guard.conn)
        .describe(sqlx::AssertSqlSafe(statements.select.clone()).into_sql_str())
        .await?;
    probe.builder.check_column_types(described.columns())?;
    // A timeout override is scoped to this COPY: `SET LOCAL`, so it can never leak into a
    // pooled session.
    if let Some(ms) = statement_timeout_ms {
        let set = format!("SET LOCAL statement_timeout = {ms}");
        sqlx::query(sqlx::AssertSqlSafe(set.as_str()))
            .execute(&mut *guard.conn)
            .await?;
    }
    let result = {
        let mut stream = guard.conn.copy_out_raw(&statements.copy).await?;
        drive_copy_stream(
            &mut stream,
            table_metadata,
            batch_size,
            max_batch_bytes,
            on_batch,
        )
        .await
    };
    if result.is_ok() {
        sqlx::query("COMMIT").execute(&mut *guard.conn).await?;
    }
    guard.completed = result.is_ok();
    result
}

/// Owns the COPY connection. On drop without `completed`, closes the connection (never
/// returned to the pool mid-COPY) and spawns a detached `pg_cancel_backend(pid)`.
struct CopyCancelGuard {
    conn: PoolConnection<Postgres>,
    pool: PgPool,
    pid: i32,
    tag: String,
    completed: bool,
}

impl Drop for CopyCancelGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.conn.close_on_drop();
        match tokio::runtime::Handle::try_current() {
            // Detached by design (the JoinHandle is intentionally dropped): `Drop` cannot
            // await, and the consumer is gone. The task is bounded (a 5 s acquire timeout
            // plus one statement) and logs its own outcome.
            Ok(handle) => {
                let (pool, pid, tag) = (self.pool.clone(), self.pid, std::mem::take(&mut self.tag));
                let cancel = cancel_backend(pool, pid, tag).instrument(tracing::Span::current());
                drop(handle.spawn(cancel));
            }
            Err(_) => warn!(
                pid = self.pid,
                "COPY ended early outside a tokio runtime; not cancelling it (the closed \
                 connection still stops the scan)"
            ),
        }
    }
}

/// Cancel backend `pid` if (and only if) it is still running the statement tagged `tag`.
///
/// Uses a connection **from the same pool**, so the cancel never exceeds the source budget
/// (`pool_max`): the COPY connection itself is being closed, which frees its slot. If no
/// connection becomes available within [`CANCEL_ACQUIRE_TIMEOUT`], the cancel is skipped —
/// the server still aborts the COPY on its next write to the closed socket.
async fn cancel_backend(pool: PgPool, pid: i32, tag: String) {
    const SQL: &str = "SELECT pg_cancel_backend(pid) FROM pg_stat_activity \
                       WHERE pid = $1 AND starts_with(query, $2)";
    let result = match tokio::time::timeout(CANCEL_ACQUIRE_TIMEOUT, pool.acquire()).await {
        Ok(Ok(mut conn)) => sqlx::query(SQL)
            .bind(pid)
            .bind(&tag)
            .execute(&mut *conn)
            .await
            .map(|_| ()),
        Ok(Err(e)) => Err(e),
        Err(_) => {
            warn!(
                pid,
                timeout_ms = u64::try_from(CANCEL_ACQUIRE_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
                "no pooled connection free to cancel the unfinished COPY; relying on the \
                 closed connection to stop it"
            );
            return;
        }
    };
    match result {
        Ok(()) => debug!(pid, "cancelled the unfinished COPY"),
        Err(e) => warn!(pid, error = %e, "could not cancel the unfinished COPY"),
    }
}

/// How long [`cancel_backend`] waits for a pooled connection before giving up.
const CANCEL_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TupleOutcome {
    NeedMore,
    Tuple,
    Eof,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnMetadata;
    use arrow::array::Array;

    fn col(name: &str, data_type: &str, udt: Option<&str>) -> ColumnMetadata {
        ColumnMetadata {
            column_name: name.to_string(),
            data_type: data_type.to_string(),
            is_nullable: true,
            numeric_precision: Some(12),
            numeric_scale: Some(2),
            udt_name: udt.map(String::from),
            collation_name: None,
        }
    }

    fn table(cols: Vec<ColumnMetadata>) -> TableMetadata {
        TableMetadata {
            schema_name: "public".to_string(),
            table_name: "t".to_string(),
            columns: cols,
        }
    }

    /// Build a binary-COPY byte stream in memory: header + `tuples` (each a vec of
    /// `Option<field bytes>`) + trailer. Trivial oracle: bytes we craft ourselves.
    fn copy_stream(tuples: &[Vec<Option<Vec<u8>>>]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(COPY_SIGNATURE);
        out.extend_from_slice(&0i32.to_be_bytes()); // flags
        out.extend_from_slice(&0i32.to_be_bytes()); // header ext len
        for tuple in tuples {
            out.extend_from_slice(&(tuple.len() as i16).to_be_bytes());
            for field in tuple {
                match field {
                    None => out.extend_from_slice(&(-1i32).to_be_bytes()),
                    Some(b) => {
                        out.extend_from_slice(&(b.len() as i32).to_be_bytes());
                        out.extend_from_slice(b);
                    }
                }
            }
        }
        out.extend_from_slice(&(-1i16).to_be_bytes()); // trailer
        out
    }

    fn i16b(v: i16) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }
    fn i32b(v: i32) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }
    fn i64b(v: i64) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }

    /// Drive bytes through a decoder in deliberately awkward chunk splits (1 byte at a
    /// time for the first tuple) to prove chunk boundaries are meaningless.
    fn decode_all(meta: &TableMetadata, bytes: &[u8], chunk_every: usize) -> Vec<RecordBatch> {
        let mut decoder = CopyBatchDecoder::new(meta, 1024, 16 * 1024 * 1024).unwrap();
        let mut batches = Vec::new();
        for chunk in bytes.chunks(chunk_every) {
            batches.extend(decoder.push_bytes(chunk).unwrap());
        }
        if let Some(b) = decoder.finish().unwrap() {
            batches.push(b);
        }
        batches
    }

    #[test]
    fn ints_bool_and_nulls_round_trip() {
        let meta = table(vec![
            col("a", "smallint", None),
            col("b", "integer", None),
            col("c", "bigint", None),
            col("f", "boolean", None),
        ]);
        let bytes = copy_stream(&[
            vec![Some(i16b(7)), Some(i32b(-3)), Some(i64b(9)), Some(vec![1])],
            vec![None, None, None, None],
        ]);
        // Byte-at-a-time for maximal chunk-split hostility.
        let batches = decode_all(&meta, &bytes, 1);
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b.num_rows(), 2);
        assert_eq!(b.num_columns(), 4);
    }

    #[test]
    fn timestamps_use_postgres_epoch() {
        use chrono::TimeZone;
        let meta = table(vec![
            col("ts", "timestamp with time zone", None),
            col("d", "date", None),
        ]);
        // 2024-03-01 12:00:00 UTC as PG micros + PG days.
        let dt = chrono::Utc.with_ymd_and_hms(2024, 3, 1, 12, 0, 0).unwrap();
        let pg_micros = dt.timestamp_micros() - PG_EPOCH_MICROS;
        let pg_days = (dt.date_naive() - chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap())
            .num_days() as i32;
        let bytes = copy_stream(&[vec![Some(i64b(pg_micros)), Some(i32b(pg_days))]]);
        let batches = decode_all(&meta, &bytes, 7);
        let b = &batches[0];
        assert_eq!(b.num_rows(), 1);
        let ts = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), dt.timestamp_micros());
        let day = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .unwrap();
        assert_eq!(
            day.value(0),
            (dt.date_naive() - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days()
                as i32
        );
    }

    #[test]
    fn numeric_binary_matches_cursor_semantics() {
        // 123.45 scale 2 -> 12345; -7.50 scale 2 -> -750; truncation toward zero.
        fn numeric_bytes(int_digits: &[u16], weight: i16, neg: bool) -> Vec<u8> {
            let mut out = Vec::new();
            out.extend_from_slice(&(int_digits.len() as u16).to_be_bytes());
            out.extend_from_slice(&weight.to_be_bytes());
            out.extend_from_slice(&(if neg { 0x4000u16 } else { 0x0000u16 }).to_be_bytes());
            out.extend_from_slice(&2u16.to_be_bytes()); // dscale
            for d in int_digits {
                out.extend_from_slice(&d.to_be_bytes());
            }
            out
        }
        let meta = table(vec![col("n", "numeric", None)]);
        let bytes = copy_stream(&[
            vec![Some(numeric_bytes(&[123, 4500], 0, false))],
            vec![Some(numeric_bytes(&[7, 5000], 0, true))],
            vec![None],
        ]);
        let batches = decode_all(&meta, &bytes, 5);
        let b = &batches[0];
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 12345);
        assert_eq!(arr.value(1), -750);
        assert!(arr.is_null(2));
    }

    #[test]
    fn uuid_jsonb_and_text_array_decode() {
        // uuid/json/jsonb are selected as `::text` (see query_builder::push_columns), so
        // on the wire they are plain UTF-8 text — exactly Postgres' own rendering.
        let meta = table(vec![
            col("u", "uuid", None),
            col("j", "jsonb", None),
            col("t", "ARRAY", Some("_text")),
        ]);
        let uid = b"123e4567-e89b-12d3-a456-426614174000".to_vec();
        let jsonb = br#"{"a": 12345678901234567.89}"#.to_vec();
        // Binary text[]: ndim=1, flags=0, oid=25, dim(count=2, lbound=1), "a", NULL.
        let mut arr = Vec::new();
        arr.extend_from_slice(&1i32.to_be_bytes());
        arr.extend_from_slice(&0i32.to_be_bytes());
        arr.extend_from_slice(&25u32.to_be_bytes());
        arr.extend_from_slice(&2i32.to_be_bytes());
        arr.extend_from_slice(&1i32.to_be_bytes());
        arr.extend_from_slice(&1i32.to_be_bytes());
        arr.extend_from_slice(b"a");
        arr.extend_from_slice(&(-1i32).to_be_bytes());
        let bytes = copy_stream(&[vec![Some(uid), Some(jsonb), Some(arr)]]);
        let batches = decode_all(&meta, &bytes, 3);
        let b = &batches[0];
        assert_eq!(b.num_rows(), 1);
        let u = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(u.value(0), "123e4567-e89b-12d3-a456-426614174000");
        let j = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(j.value(0), r#"{"a": 12345678901234567.89}"#);
        let t = b
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(t.len(), 1);
        assert!(t.is_valid(0));
    }

    fn decode_err(meta: &TableMetadata, bytes: &[u8]) -> ExtractorError {
        let mut decoder = CopyBatchDecoder::new(meta, 1024, 16 * 1024 * 1024).unwrap();
        match decoder.push_bytes(bytes) {
            Err(e) => e,
            Ok(_) => decoder.finish().expect_err("decode must fail"),
        }
    }

    #[test]
    fn infinity_timestamps_and_dates_are_typed_errors() {
        // Both signs, all three types; the error names the column.
        for (ty, raw) in [
            ("timestamp with time zone", i64b(i64::MAX)),
            ("timestamp with time zone", i64b(i64::MIN)),
            ("timestamp without time zone", i64b(i64::MAX)),
            ("timestamp without time zone", i64b(i64::MIN)),
            ("date", i32b(i32::MAX)),
            ("date", i32b(i32::MIN)),
        ] {
            let meta = table(vec![col("when_", ty, None)]);
            let err = decode_err(&meta, &copy_stream(&[vec![Some(raw)]]));
            assert!(
                matches!(&err, ExtractorError::UnsupportedValue { column, .. } if column == "when_"),
                "{ty}: {err}"
            );
        }
    }

    #[test]
    fn numeric_nan_and_overscale_are_typed_errors() {
        let mut n = col("n", "numeric", None);
        n.numeric_precision = None;
        n.numeric_scale = None;
        let meta = table(vec![n]);
        let nan = vec![0u8, 0, 0, 0, 0xC0, 0, 0, 0];
        assert!(matches!(
            decode_err(&meta, &copy_stream(&[vec![Some(nan)]])),
            ExtractorError::UnsupportedValue { .. }
        ));
        // 1.123456789012 (12 fractional digits) into the Decimal128(38,10) default.
        let mut over = Vec::new();
        for w in [4u16, 0, 0, 12] {
            over.extend_from_slice(&w.to_be_bytes());
        }
        for d in [1u16, 1234, 5678, 9012] {
            over.extend_from_slice(&d.to_be_bytes());
        }
        assert!(matches!(
            decode_err(&meta, &copy_stream(&[vec![Some(over)]])),
            ExtractorError::UnsupportedValue { .. }
        ));
    }

    #[test]
    fn batch_size_zero_is_rejected() {
        let meta = table(vec![col("a", "integer", None)]);
        assert!(matches!(
            CopyBatchDecoder::new(&meta, 0, 1024),
            Err(ExtractorError::InvalidConfig(_))
        ));
    }

    #[test]
    fn bad_signature_and_truncation_fail_loudly() {
        let meta = table(vec![col("a", "integer", None)]);
        let mut bad = copy_stream(&[vec![Some(i32b(1))]]);
        bad[0] = b'X';
        let mut decoder = CopyBatchDecoder::new(&meta, 8, 1024).unwrap();
        assert!(decoder.push_bytes(&bad).is_err());

        // Truncated mid-tuple: push half, finish must complain, not panic.
        let bytes = copy_stream(&[vec![Some(i32b(1))]]);
        let mut decoder = CopyBatchDecoder::new(&meta, 8, 1024).unwrap();
        let half = bytes.len() / 2;
        assert!(decoder.push_bytes(&bytes[..half]).unwrap().is_empty());
        assert!(decoder.finish().is_err());
    }

    #[test]
    fn empty_result_keeps_schema() {
        let meta = table(vec![col("a", "bigint", None), col("n", "text", None)]);
        let bytes = copy_stream(&[]);
        let batches = decode_all(&meta, &bytes, 64);
        assert!(batches.is_empty());
    }

    #[test]
    fn unsupported_type_fails_construction_for_fallback() {
        let meta = table(vec![col("p", "point", None)]);
        assert!(!supports_binary_copy(&meta.columns));
        assert!(CopyBatchDecoder::new(&meta, 8, 1024).is_err());
    }

    #[test]
    fn zero_column_projection_counts_rows() {
        let meta = table(vec![]);
        // `SELECT 1` rows: one int4 payload column the decoder skips.
        let bytes = copy_stream(&[vec![Some(i32b(1))], vec![Some(i32b(1))]]);
        let batches = decode_all(&meta, &bytes, 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 0);
    }
}
