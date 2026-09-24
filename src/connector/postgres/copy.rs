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
//! Contract: [`supports_binary_copy`] gates every caller. Unsupported shapes
//! (unmapped types, pushed filters with bound literals which `COPY` cannot take)
//! fall back to the cursor/`SELECT` path with a loud log — never silently wrong data.

use arrow::record_batch::RecordBatch;
use bytes::Bytes;

use crate::connector::errors::ExtractorError;
use crate::connector::postgres::arrow_type_mapper::ArrowTypeMapper;
use crate::connector::postgres::row_adapter::RowBatchBuilder;
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
pub fn supports_binary_copy(columns: &[ColumnMetadata]) -> bool {
    columns.iter().all(|c| ArrowTypeMapper::map(c).is_ok())
}

/// Parse one binary-`text[]` array value into nullable elements. Layout: `ndim: i32`,
/// `has_nulls: i32` (informational), `elem_oid: u32` (must be 25 = `text`), then per
/// dimension `count: i32` + `lower_bound: i32`, then `count` elements of
/// `len: i32` (`-1` = NULL) + UTF-8 bytes. Only 1-D arrays are supported (everything
/// this crate produces); anything else is an error, not a silent flatten.
pub fn parse_copy_text_array(raw: &[u8]) -> Result<Vec<Option<String>>, ExtractorError> {
    let corrupt = |why: &str| ExtractorError::Internal(format!("corrupt binary text[]: {why}"));
    let mut cur = Cursor::new(raw);
    let ndim = cur.i32().ok_or_else(|| corrupt("short ndim"))?;
    let _has_nulls = cur.i32().ok_or_else(|| corrupt("short flags"))?;
    let elem_oid = cur.u32().ok_or_else(|| corrupt("short elem oid"))?;
    if elem_oid != 25 {
        return Err(corrupt("non-text element OID"));
    }
    if ndim == 0 {
        if !cur.rest().is_empty() {
            return Err(corrupt("trailing bytes in empty array"));
        }
        return Ok(Vec::new());
    }
    if ndim != 1 {
        return Err(corrupt("only 1-D text[] arrays are supported"));
    }
    let count = cur.i32().ok_or_else(|| corrupt("short dim"))?;
    let _lower = cur.i32().ok_or_else(|| corrupt("short lower bound"))?;
    if count < 0 {
        return Err(corrupt("negative element count"));
    }
    let mut items = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let len = cur.i32().ok_or_else(|| corrupt("short element len"))?;
        if len == -1 {
            items.push(None);
        } else if len < 0 {
            return Err(corrupt("negative element length"));
        } else {
            let bytes = cur
                .bytes(len as usize)
                .ok_or_else(|| corrupt("truncated element"))?;
            items.push(Some(String::from_utf8(bytes.to_vec()).map_err(|e| {
                ExtractorError::Internal(format!("invalid UTF-8 in text[] element: {e}"))
            })?));
        }
    }
    if !cur.rest().is_empty() {
        return Err(corrupt("trailing bytes"));
    }
    Ok(items)
}

/// Minimal big-endian cursor over a slice. `None` = truncated input (the caller
/// buffers more and retries — never a panic on hostile lengths).
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn rest(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        if end > self.buf.len() {
            return None;
        }
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Some(out)
    }

    fn i16(&mut self) -> Option<i16> {
        self.take(2).map(|b| i16::from_be_bytes([b[0], b[1]]))
    }

    fn i32(&mut self) -> Option<i32> {
        self.take(4)
            .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u32(&mut self) -> Option<u32> {
        self.take(4)
            .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        self.take(n)
    }
}

/// Incremental binary-COPY parser. Feed wire `Bytes` chunks via [`Self::push_bytes`]
/// (completed byte-capped batches come back), then [`Self::finish`] for the trailing
/// partial batch. Chunk boundaries are meaningless: a tuple split across chunks waits
/// for the rest — peak memory stays `O(batch)` regardless of chunking.
pub struct CopyBatchDecoder {
    builder: RowBatchBuilder,
    columns: Vec<ColumnMetadata>,
    batch_size: usize,
    max_batch_bytes: usize,
    buf: Vec<u8>,
    pos: usize,
    header_done: bool,
    eof: bool,
}

impl CopyBatchDecoder {
    /// Build a decoder. Errors on unmapped column types so callers fall back to the
    /// cursor path *before* opening the COPY stream.
    pub fn new(
        table_metadata: &TableMetadata,
        batch_size: usize,
        max_batch_bytes: usize,
    ) -> Result<Self, ExtractorError> {
        if !supports_binary_copy(&table_metadata.columns) {
            return Err(ExtractorError::UnsupportedType(
                "binary COPY unsupported for this projection (unmapped type)".into(),
            ));
        }
        Ok(Self {
            builder: RowBatchBuilder::with_capacity(table_metadata, batch_size)?,
            columns: table_metadata.columns.clone(),
            batch_size,
            max_batch_bytes,
            buf: Vec::new(),
            pos: 0,
            header_done: false,
            eof: false,
        })
    }

    /// Feed one wire chunk; returns the batches that filled up (usually zero or one).
    pub fn push_bytes(&mut self, chunk: &[u8]) -> Result<Vec<RecordBatch>, ExtractorError> {
        self.buf.extend_from_slice(chunk);
        // Compact the consumed prefix so a long stream never grows the buffer.
        if self.pos > 65536 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
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
    pub fn finish(&mut self) -> Result<Option<RecordBatch>, ExtractorError> {
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

    /// Attempt one tuple at the current position. `NeedMore` leaves all state
    /// untouched (including builders — nothing is appended until the whole tuple is
    /// buffered, so a retried parse can never double-append).
    fn try_parse_tuple(&mut self) -> Result<TupleOutcome, ExtractorError> {
        let corrupt = |why: &str| ExtractorError::Internal(format!("corrupt binary COPY: {why}"));
        if !self.header_done {
            // Signature (11) + flags (4) + header-ext length (4) + extension.
            if self.buf.len() - self.pos < 19 {
                return Ok(TupleOutcome::NeedMore);
            }
            if &self.buf[self.pos..self.pos + 11] != COPY_SIGNATURE {
                return Err(corrupt("bad signature"));
            }
            let flags = i32::from_be_bytes(
                self.buf[self.pos + 11..self.pos + 15]
                    .try_into()
                    .map_err(|_| corrupt("short flags"))?,
            );
            if flags & 0x0001_0000 != 0 {
                return Err(corrupt("OID columns not supported"));
            }
            let ext_len = i32::from_be_bytes(
                self.buf[self.pos + 15..self.pos + 19]
                    .try_into()
                    .map_err(|_| corrupt("short header ext"))?,
            );
            if ext_len < 0 {
                return Err(corrupt("negative header extension length"));
            }
            let total = 19usize
                .checked_add(ext_len as usize)
                .ok_or_else(|| corrupt("header too large"))?;
            if self.buf.len() - self.pos < total {
                return Ok(TupleOutcome::NeedMore);
            }
            self.pos += total;
            self.header_done = true;
        }

        let ncols = self.columns.len();
        // Walk the tuple first (lengths only): only decode once the whole tuple is
        // buffered, so a split tuple never partially appends.
        let mut cur = Cursor::new(&self.buf[self.pos..]);
        let nfields = match cur.i16() {
            Some(v) => v,
            None => return Ok(TupleOutcome::NeedMore),
        };
        if nfields == -1 {
            self.pos += 2;
            return Ok(TupleOutcome::Eof);
        }
        if nfields < 0 {
            return Err(corrupt("negative field count"));
        }
        // Zero-column projections (`SELECT 1`) carry exactly one unread payload column.
        let want_fields = if ncols == 0 { 1 } else { ncols };
        if nfields as usize != want_fields {
            return Err(corrupt("field count mismatch with projection"));
        }
        let mut total = 2usize;
        let mut lens: Vec<i32> = Vec::with_capacity(want_fields);
        for _ in 0..want_fields {
            let len = match cur.i32() {
                Some(v) => v,
                None => return Ok(TupleOutcome::NeedMore),
            };
            if len < -1 {
                return Err(corrupt("negative field length"));
            }
            if len == -1 {
                total = total
                    .checked_add(4)
                    .ok_or_else(|| corrupt("tuple too large"))?;
                lens.push(-1);
            } else {
                // Skip the value bytes: the next length starts after them. A
                // truncated value means the tuple is split across chunks — wait.
                if cur.bytes(len as usize).is_none() {
                    return Ok(TupleOutcome::NeedMore);
                }
                total = total
                    .checked_add(4)
                    .and_then(|t| t.checked_add(len as usize))
                    .ok_or_else(|| corrupt("tuple too large"))?;
                lens.push(len);
            }
        }
        if self.buf.len() - self.pos < total {
            return Ok(TupleOutcome::NeedMore);
        }

        // Whole tuple buffered: consume and decode (infallible w.r.t. buffering).
        self.pos += 2;
        for (idx, len) in lens.into_iter().enumerate() {
            let raw = if len == -1 {
                self.pos += 4;
                None
            } else {
                self.pos += 4;
                let end = self.pos + len as usize;
                let bytes = &self.buf[self.pos..end];
                self.pos = end;
                Some(bytes)
            };
            if ncols > 0 {
                // `raw` borrows `self.buf` while `self.builder` is borrowed mutably:
                // disjoint fields, but the borrow checker needs the split spelled out.
                let columns = &self.columns;
                self.builder.append_copy_field(idx, &columns[idx], raw)?;
            }
        }
        if ncols > 0 {
            self.builder.inc_row();
        } else {
            // Zero-column projection: count the row, skip the payload.
            self.builder.inc_row();
        }
        Ok(TupleOutcome::Tuple)
    }
}

/// Drive a COPY byte stream to completion, invoking `on_batch` per flushed batch.
/// Thin async wrapper over [`CopyBatchDecoder`] for the extractor's `for_each_batch`
/// shape; the `ExecutionPlan` executor drives the decoder directly so it can yield.
pub async fn drive_copy_stream<S>(
    stream: &mut S,
    table_metadata: &TableMetadata,
    batch_size: usize,
    max_batch_bytes: usize,
    on_batch: &mut impl FnMut(RecordBatch) -> Result<(), ExtractorError>,
) -> Result<u64, ExtractorError>
where
    S: futures::Stream<Item = Result<Bytes, sqlx::Error>> + Unpin,
{
    use futures::TryStreamExt as _;
    let mut decoder = CopyBatchDecoder::new(table_metadata, batch_size, max_batch_bytes)?;
    let mut total: u64 = 0;
    while let Some(chunk) = stream.try_next().await? {
        for batch in decoder.push_bytes(&chunk)? {
            total += batch.num_rows() as u64;
            on_batch(batch)?;
        }
    }
    if let Some(batch) = decoder.finish()? {
        total += batch.num_rows() as u64;
        on_batch(batch)?;
    }
    Ok(total)
}

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
        let meta = table(vec![
            col("u", "uuid", None),
            col("j", "jsonb", None),
            col("t", "ARRAY", Some("_text")),
        ]);
        let uid = uuid::Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let mut jsonb = vec![1u8];
        jsonb.extend_from_slice(br#"{"a":1}"#);
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
        let bytes = copy_stream(&[vec![Some(uid.as_bytes().to_vec()), Some(jsonb), Some(arr)]]);
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
        assert_eq!(j.value(0), r#"{"a":1}"#);
        let t = b
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(t.len(), 1);
        assert!(t.is_valid(0));
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
