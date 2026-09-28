//! PostgreSQL value → Arrow decoding, shared by the cursor and `COPY` paths.
//!
//! Both extraction paths deliver the **binary** wire representation of every value:
//! sqlx requests binary result format for every extended-protocol statement (the cursor
//! `FETCH`es included), and `COPY … TO STDOUT (FORMAT BINARY)` frames the same per-type
//! binary encodings. So one decoder per column, chosen once per scan from the mapped
//! Arrow type, handles both: `RowBatchBuilder::append_row` feeds it
//! `PgRow::try_get_raw(i)` bytes, [`RowBatchBuilder::append_copy_field`] feeds it COPY
//! field bytes. The two paths therefore agree by construction.
//!
//! Types that have no faithful binary decode here (`json`, `jsonb`, `uuid`, enums) are
//! selected as `::text` by `PostgresQueryBuilder::push_columns`, so they arrive as
//! Postgres' own text rendering on both paths (big JSON numbers are preserved byte for
//! byte).
//!
//! Values with no faithful Arrow representation — `±infinity` timestamps/dates,
//! numeric `NaN`/`±Infinity`, numeric digits beyond the column's `Decimal128` scale or
//! precision — fail with [`ExtractorError::UnsupportedValue`] naming the column. Nothing
//! is coerced to NULL or truncated, and nothing panics.
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use sqlx::postgres::{PgRow, PgValueFormat};
use sqlx::{Column as _, Row as _, TypeInfo as _, ValueRef as _};
use std::sync::Arc;

use crate::connector::errors::ExtractorError;
use crate::connector::postgres::arrow_type_mapper::{ArrowTypeMapper, numeric_bytes_to_unscaled};
use crate::connector::postgres::copy::{PG_EPOCH_DAYS, PG_EPOCH_MICROS};
use crate::types::TableMetadata;

pub struct PostgresRowAdapter;

impl PostgresRowAdapter {
    /// The Arrow schema for a projection: one field per column, in metadata order, with the
    /// mapped type and the catalog nullability. The single schema source for the provider,
    /// the execution plan and both decoders.
    ///
    /// # Examples
    ///
    /// ```
    /// use arrow::datatypes::DataType;
    /// use rust_ballista_extraction_layer::connector::postgres::row_adapter::PostgresRowAdapter;
    /// use rust_ballista_extraction_layer::types::{ColumnMetadata, TableMetadata};
    ///
    /// let col = |name: &str, data_type: &str, is_nullable| ColumnMetadata {
    ///     column_name: name.into(), data_type: data_type.into(), is_nullable,
    ///     numeric_precision: None, numeric_scale: None, udt_name: None, collation_name: None,
    /// };
    /// let meta = TableMetadata {
    ///     schema_name: "public".into(),
    ///     table_name: "orders".into(),
    ///     columns: vec![col("id", "bigint", false), col("status", "text", true)],
    /// };
    /// let schema = PostgresRowAdapter::build_arrow_schema(&meta)?;
    /// assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    /// assert!(!schema.field(0).is_nullable() && schema.field(1).is_nullable());
    /// # Ok::<(), rust_ballista_extraction_layer::connector::errors::ExtractorError>(())
    /// ```
    pub fn build_arrow_schema(
        table_metadata: &TableMetadata,
    ) -> Result<Arc<Schema>, ExtractorError> {
        let fields: Vec<Field> = table_metadata
            .columns
            .iter()
            .map(|column| {
                let data_type = ArrowTypeMapper::map(column)?;
                Ok(Field::new(
                    &column.column_name,
                    data_type,
                    column.is_nullable,
                ))
            })
            .collect::<Result<_, ExtractorError>>()?;

        Ok(Arc::new(Schema::new(fields)))
    }
}

/// PostgreSQL built-in type OIDs (`pg_type.oid`) accepted by each decoder on the cursor
/// path, where the row description says what the server actually sent.
mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const TEXT_ARRAY: u32 = 1009;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
}

/// One column's Arrow builder, typed once per scan. Matching this enum per cell replaces
/// the old per-cell type-name string match and `dyn ArrayBuilder` downcast.
enum ColumnBuilder {
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Text(StringBuilder),
    Binary(BinaryBuilder),
    Date32(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
    Decimal128 {
        builder: Decimal128Builder,
        precision: u8,
        scale: i8,
    },
    TextList(ListBuilder<StringBuilder>),
}

impl ColumnBuilder {
    /// A fresh builder for `data_type`, sized for `rows` rows and `data_bytes` bytes of
    /// variable-width payload.
    fn new(data_type: &DataType, rows: usize, data_bytes: usize) -> Result<Self, ExtractorError> {
        Ok(match data_type {
            DataType::Int16 => Self::Int16(Int16Builder::with_capacity(rows)),
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(rows)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(rows)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(rows)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(rows)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(rows)),
            DataType::Utf8 => Self::Text(StringBuilder::with_capacity(rows, data_bytes)),
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(rows, data_bytes)),
            DataType::Date32 => Self::Date32(Date32Builder::with_capacity(rows)),
            // Timestamp builders carry the schema's timezone: `RecordBatch::try_new`
            // rejects a naive array under a `Timestamp(µs, "UTC")` field.
            DataType::Timestamp(TimeUnit::Microsecond, tz) => Self::Timestamp(
                TimestampMicrosecondBuilder::with_capacity(rows).with_timezone_opt(tz.clone()),
            ),
            DataType::Decimal128(precision, scale) => Self::Decimal128 {
                builder: Decimal128Builder::with_capacity(rows)
                    .with_precision_and_scale(*precision, *scale)?,
                precision: *precision,
                scale: *scale,
            },
            DataType::List(item) if item.data_type() == &DataType::Utf8 => Self::TextList(
                ListBuilder::with_capacity(StringBuilder::with_capacity(rows, data_bytes), rows),
            ),
            other => {
                return Err(ExtractorError::UnsupportedType(format!(
                    "no Postgres binary decoder for Arrow type {other}"
                )));
            }
        })
    }

    /// Fixed bytes one row adds to this column's buffers (values or offsets), for the
    /// byte cap. Variable-width payload is counted separately by [`Self::append`].
    fn fixed_width(&self) -> usize {
        match self {
            Self::Int16(_) => 2,
            Self::Int32(_) | Self::Float32(_) | Self::Date32(_) => 4,
            Self::Int64(_) | Self::Float64(_) | Self::Timestamp(_) => 8,
            Self::Boolean(_) => 1,
            Self::Decimal128 { .. } => 16,
            // i32 offset per row.
            Self::Text(_) | Self::Binary(_) | Self::TextList(_) => 4,
        }
    }

    /// Whether the cursor path may feed this decoder a value of server type `oid`.
    fn accepts_oid(&self, oid: u32) -> bool {
        match self {
            Self::Int16(_) => oid == oid::INT2,
            Self::Int32(_) => oid == oid::INT4,
            Self::Int64(_) => oid == oid::INT8,
            Self::Float32(_) => oid == oid::FLOAT4,
            Self::Float64(_) => oid == oid::FLOAT8,
            Self::Boolean(_) => oid == oid::BOOL,
            Self::Text(_) => matches!(oid, oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME),
            Self::Binary(_) => oid == oid::BYTEA,
            Self::Date32(_) => oid == oid::DATE,
            Self::Timestamp(_) => matches!(oid, oid::TIMESTAMP | oid::TIMESTAMPTZ),
            Self::Decimal128 { .. } => oid == oid::NUMERIC,
            Self::TextList(_) => oid == oid::TEXT_ARRAY,
        }
    }

    /// Decode one binary value (`None` = SQL NULL) and append it. Returns the
    /// variable-width payload bytes appended (0 for fixed-width types).
    fn append(&mut self, raw: Option<&[u8]>, column: &str) -> Result<usize, ExtractorError> {
        match self {
            Self::Int16(b) => b.append_option(fixed::<2>(raw, column)?.map(i16::from_be_bytes)),
            Self::Int32(b) => b.append_option(fixed::<4>(raw, column)?.map(i32::from_be_bytes)),
            Self::Int64(b) => b.append_option(fixed::<8>(raw, column)?.map(i64::from_be_bytes)),
            Self::Float32(b) => b.append_option(fixed::<4>(raw, column)?.map(f32::from_be_bytes)),
            Self::Float64(b) => b.append_option(fixed::<8>(raw, column)?.map(f64::from_be_bytes)),
            Self::Boolean(b) => b.append_option(fixed::<1>(raw, column)?.map(|v| v[0] != 0)),
            Self::Text(b) => {
                return Ok(match raw {
                    None => {
                        b.append_null();
                        0
                    }
                    Some(bytes) => {
                        b.append_value(utf8(bytes, column)?);
                        bytes.len()
                    }
                });
            }
            Self::Binary(b) => {
                b.append_option(raw);
                return Ok(raw.map_or(0, <[u8]>::len));
            }
            Self::Date32(b) => {
                let days = fixed::<4>(raw, column)?
                    .map(|v| pg_date_to_arrow(i32::from_be_bytes(v), column))
                    .transpose()?;
                b.append_option(days);
            }
            Self::Timestamp(b) => {
                let micros = fixed::<8>(raw, column)?
                    .map(|v| pg_timestamp_to_arrow(i64::from_be_bytes(v), column))
                    .transpose()?;
                b.append_option(micros);
            }
            Self::Decimal128 {
                builder,
                precision,
                scale,
            } => {
                let value = raw
                    .map(|bytes| {
                        numeric_bytes_to_unscaled(bytes, *precision, *scale).map_err(|e| {
                            ExtractorError::UnsupportedValue {
                                column: column.to_string(),
                                arrow_type: format!("Decimal128({precision}, {scale})"),
                                reason: e.to_string(),
                            }
                        })
                    })
                    .transpose()?;
                builder.append_option(value);
            }
            Self::TextList(b) => {
                return match raw {
                    None => {
                        b.append_null();
                        Ok(0)
                    }
                    Some(bytes) => {
                        let n = append_text_array(bytes, b.values(), column)?;
                        b.append(true);
                        Ok(n)
                    }
                };
            }
        }
        Ok(0)
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Text(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
            Self::Date32(b) => Arc::new(b.finish()),
            Self::Timestamp(b) => Arc::new(b.finish()),
            Self::Decimal128 { builder, .. } => Arc::new(builder.finish()),
            Self::TextList(b) => Arc::new(b.finish()),
        }
    }
}

/// Exactly `N` bytes, or NULL. Any other length is a corrupt/mismatched field.
#[inline]
fn fixed<const N: usize>(
    raw: Option<&[u8]>,
    column: &str,
) -> Result<Option<[u8; N]>, ExtractorError> {
    match raw {
        None => Ok(None),
        Some(b) => b.try_into().map(Some).map_err(|_| {
            ExtractorError::Internal(format!(
                "column '{column}': binary value has {} bytes, expected {N}",
                b.len()
            ))
        }),
    }
}

#[inline]
fn utf8<'a>(bytes: &'a [u8], column: &str) -> Result<&'a str, ExtractorError> {
    std::str::from_utf8(bytes).map_err(|e| ExtractorError::UnsupportedValue {
        column: column.to_string(),
        arrow_type: "Utf8".to_string(),
        reason: format!("invalid UTF-8: {e}"),
    })
}

fn unsupported(column: &str, arrow_type: &str, reason: &str) -> ExtractorError {
    ExtractorError::UnsupportedValue {
        column: column.to_string(),
        arrow_type: arrow_type.to_string(),
        reason: reason.to_string(),
    }
}

/// Postgres binary `timestamp[tz]` (µs since 2000-01-01, `i64::MAX`/`i64::MIN` =
/// `±infinity`) → Arrow µs since 1970-01-01. Infinity and out-of-range values are typed
/// errors, never a fabricated timestamp.
///
/// # Examples
///
/// ```
/// use rust_ballista_extraction_layer::connector::postgres::row_adapter::pg_timestamp_to_arrow;
///
/// assert_eq!(pg_timestamp_to_arrow(0, "ts").unwrap(), 946_684_800_000_000);
/// assert!(pg_timestamp_to_arrow(i64::MAX, "ts").is_err());
/// assert!(pg_timestamp_to_arrow(i64::MIN, "ts").is_err());
/// ```
pub fn pg_timestamp_to_arrow(pg_micros: i64, column: &str) -> Result<i64, ExtractorError> {
    const TS: &str = "Timestamp(Microsecond)";
    match pg_micros {
        i64::MAX => Err(unsupported(column, TS, "timestamp 'infinity'")),
        i64::MIN => Err(unsupported(column, TS, "timestamp '-infinity'")),
        v => v
            .checked_add(PG_EPOCH_MICROS)
            .ok_or_else(|| unsupported(column, TS, "timestamp out of i64 µs range")),
    }
}

/// Postgres binary `date` (days since 2000-01-01, `i32::MAX`/`i32::MIN` = `±infinity`)
/// → Arrow `Date32` (days since 1970-01-01). Infinity and overflow are typed errors.
///
/// # Examples
///
/// ```
/// use rust_ballista_extraction_layer::connector::postgres::row_adapter::pg_date_to_arrow;
///
/// assert_eq!(pg_date_to_arrow(0, "d").unwrap(), 10_957);
/// assert!(pg_date_to_arrow(i32::MAX, "d").is_err());
/// ```
pub fn pg_date_to_arrow(pg_days: i32, column: &str) -> Result<i32, ExtractorError> {
    match pg_days {
        i32::MAX => Err(unsupported(column, "Date32", "date 'infinity'")),
        i32::MIN => Err(unsupported(column, "Date32", "date '-infinity'")),
        v => v
            .checked_add(PG_EPOCH_DAYS)
            .ok_or_else(|| unsupported(column, "Date32", "date out of Date32 range")),
    }
}

/// Append one binary `text[]` value's elements to `values` (borrowed `&str`, no per-element
/// allocation) and return the payload bytes. Layout: `ndim: i32`, `has_nulls: i32`,
/// `elem_oid: u32` (must be 25 = `text`), then per dimension `count: i32` +
/// `lower_bound: i32`, then elements as `len: i32` (`-1` = NULL) + UTF-8 bytes. Only 0-D
/// (empty) and 1-D arrays are supported; anything else is an error, not a silent flatten.
///
/// The whole value is validated before anything is appended, so an error never leaves a
/// partially appended list element.
fn append_text_array(
    raw: &[u8],
    values: &mut StringBuilder,
    column: &str,
) -> Result<usize, ExtractorError> {
    let corrupt =
        |why: &str| ExtractorError::Internal(format!("column '{column}': corrupt text[]: {why}"));
    let mut cur = ByteCursor::new(raw);
    let ndim = cur.i32().ok_or_else(|| corrupt("short ndim"))?;
    let _has_nulls = cur.i32().ok_or_else(|| corrupt("short flags"))?;
    let elem_oid = cur.i32().ok_or_else(|| corrupt("short elem oid"))?;
    if elem_oid != 25 {
        return Err(corrupt("non-text element OID"));
    }
    if ndim == 0 {
        return if cur.is_empty() {
            Ok(0)
        } else {
            Err(corrupt("trailing bytes in empty array"))
        };
    }
    if ndim != 1 {
        return Err(unsupported(
            column,
            "List(Utf8)",
            "only 1-D text[] arrays are supported",
        ));
    }
    let count = cur.i32().ok_or_else(|| corrupt("short dim"))?;
    let _lower = cur.i32().ok_or_else(|| corrupt("short lower bound"))?;
    let count = usize::try_from(count).map_err(|_| corrupt("negative element count"))?;
    let elements = cur.rest();

    // Pass 1: validate framing and UTF-8 without appending.
    let mut probe = ByteCursor::new(elements);
    let mut payload = 0usize;
    for _ in 0..count {
        if let Some(bytes) = next_element(&mut probe).ok_or_else(|| corrupt("truncated"))?? {
            utf8(bytes, column)?;
            payload += bytes.len();
        }
    }
    if !probe.is_empty() {
        return Err(corrupt("trailing bytes"));
    }
    // Pass 2: append (cannot fail: everything was validated above).
    let mut cur = ByteCursor::new(elements);
    for _ in 0..count {
        match next_element(&mut cur).ok_or_else(|| corrupt("truncated"))?? {
            None => values.append_null(),
            Some(bytes) => values.append_value(utf8(bytes, column)?),
        }
    }
    Ok(payload)
}

/// One `len: i32` + bytes array element. Outer `None` = truncated input.
fn next_element<'a>(cur: &mut ByteCursor<'a>) -> Option<Result<Option<&'a [u8]>, ExtractorError>> {
    let len = cur.i32()?;
    if len == -1 {
        return Some(Ok(None));
    }
    let Ok(len) = usize::try_from(len) else {
        return Some(Err(ExtractorError::Internal(
            "corrupt text[]: negative element length".into(),
        )));
    };
    cur.take(len).map(|b| Ok(Some(b)))
}

/// Minimal big-endian reader over a slice. `None` = truncated (never a panic).
pub(crate) struct ByteCursor<'a> {
    buf: &'a [u8],
}

impl<'a> ByteCursor<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub(crate) fn rest(&self) -> &'a [u8] {
        self.buf
    }

    pub(crate) fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, tail) = self.buf.split_at_checked(n)?;
        self.buf = tail;
        Some(head)
    }

    pub(crate) fn i16(&mut self) -> Option<i16> {
        let (head, tail) = self.buf.split_first_chunk::<2>()?;
        self.buf = tail;
        Some(i16::from_be_bytes(*head))
    }

    pub(crate) fn i32(&mut self) -> Option<i32> {
        let (head, tail) = self.buf.split_first_chunk::<4>()?;
        self.buf = tail;
        Some(i32::from_be_bytes(*head))
    }
}

/// One column's decoder state: its builder, and what is needed to re-create it.
struct ColumnDecoder {
    name: String,
    data_type: DataType,
    builder: ColumnBuilder,
    /// Payload bytes of the batch in progress; the next builder pre-reserves as much.
    var_bytes: usize,
}

/// Accumulates rows into Arrow builders and flushes bounded batches (rows **or** bytes).
///
/// Decoders are chosen once at construction from the mapped Arrow types; after each
/// `Self::finish` every builder is re-created `with_capacity(capacity)` (Arrow's
/// `finish` hands its buffers to the array, so a finished builder has no capacity left).
pub struct RowBatchBuilder {
    schema: Arc<Schema>,
    columns: Vec<ColumnDecoder>,
    /// Rows reserved in each fresh set of builders: the rows a batch is expected to hold
    /// before a flush cap is hit (see [`Self::next_capacity`]).
    capacity: usize,
    /// Row cap of a batch (`batch_size`); `capacity` never exceeds it.
    max_rows: usize,
    /// Byte cap of a batch (`max_batch_bytes`), used to size `capacity`.
    max_batch_bytes: usize,
    row_count: usize,
    /// Sum of [`ColumnBuilder::fixed_width`] over columns: bytes every row adds.
    fixed_row_bytes: usize,
    /// Variable-width payload bytes appended since the last `finish()`.
    var_bytes: usize,
    /// Cursor path: the row description was checked against the decoders.
    row_types_checked: bool,
}

impl RowBatchBuilder {
    /// Create a builder pre-sized for 1024 rows.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::connector::postgres::row_adapter::RowBatchBuilder;
    /// use rust_ballista_extraction_layer::types::{ColumnMetadata, TableMetadata};
    ///
    /// let col = |name: &str, data_type: &str, is_nullable| ColumnMetadata {
    ///     column_name: name.into(), data_type: data_type.into(), is_nullable,
    ///     numeric_precision: None, numeric_scale: None, udt_name: None, collation_name: None,
    /// };
    /// let meta = TableMetadata {
    ///     schema_name: "public".into(),
    ///     table_name: "orders".into(),
    ///     columns: vec![col("id", "bigint", false), col("status", "text", true)],
    /// };
    /// let builder = RowBatchBuilder::new(&meta)?;
    /// assert_eq!(builder.row_count(), 0);
    /// # Ok::<(), rust_ballista_extraction_layer::connector::errors::ExtractorError>(())
    /// ```
    pub fn new(table_metadata: &TableMetadata) -> Result<Self, ExtractorError> {
        Self::with_capacity(table_metadata, 1024)
    }

    /// Create a builder pre-sized for `capacity` rows, with no byte cap.
    pub(crate) fn with_capacity(
        table_metadata: &TableMetadata,
        capacity: usize,
    ) -> Result<Self, ExtractorError> {
        Self::for_batches(table_metadata, capacity, usize::MAX)
    }

    /// Create a builder for batches capped at `batch_size` rows **and** `max_batch_bytes`
    /// bytes (extraction loops pass both). Builders reserve room only for the rows a batch
    /// is expected to hold before either cap flushes it — not blindly `batch_size` rows:
    /// on a wide table the byte cap flushes long before `batch_size`, and capacity reserved
    /// beyond that stays allocated inside every finished batch (Arrow buffers keep their
    /// capacity), multiplying memory by `batch_size / rows actually held`.
    pub(crate) fn for_batches(
        table_metadata: &TableMetadata,
        batch_size: usize,
        max_batch_bytes: usize,
    ) -> Result<Self, ExtractorError> {
        let schema = PostgresRowAdapter::build_arrow_schema(table_metadata)?;
        let mut columns = schema
            .fields()
            .iter()
            .map(|field| {
                Ok(ColumnDecoder {
                    name: field.name().clone(),
                    data_type: field.data_type().clone(),
                    // Placeholder; replaced below once the row width is known.
                    builder: ColumnBuilder::new(field.data_type(), 0, 0)?,
                    var_bytes: 0,
                })
            })
            .collect::<Result<Vec<_>, ExtractorError>>()?;
        let fixed_row_bytes: usize = columns.iter().map(|c| c.builder.fixed_width()).sum();
        let max_rows = batch_size.max(1);
        let max_batch_bytes = max_batch_bytes.max(1);
        // Before the first batch only the fixed width is known: an upper bound on the rows
        // that fit (variable-width payload only lowers it); `finish()` refines it.
        let capacity = Self::rows_within(max_rows, max_batch_bytes, fixed_row_bytes);
        for col in &mut columns {
            col.builder = ColumnBuilder::new(&col.data_type, capacity, 1024)?;
        }
        Ok(Self {
            schema,
            columns,
            capacity,
            max_rows,
            max_batch_bytes,
            row_count: 0,
            fixed_row_bytes,
            var_bytes: 0,
            row_types_checked: false,
        })
    }

    /// Append one cursor-path row. Columns are decoded by **ordinal position**: every query
    /// builder in this crate emits `SELECT` columns in `table_metadata.columns` order.
    /// Values are read as borrowed binary slices (`try_get_raw`), never owned
    /// `String`/`Vec`.
    pub(crate) fn append_row(&mut self, row: &PgRow) -> Result<(), ExtractorError> {
        if !self.row_types_checked {
            self.check_row_types(row)?;
            self.row_types_checked = true;
        }
        let mut bytes = 0usize;
        for (idx, col) in self.columns.iter_mut().enumerate() {
            let value = row.try_get_raw(idx)?;
            let raw = if value.is_null() {
                None
            } else {
                if value.format() != PgValueFormat::Binary {
                    return Err(ExtractorError::Internal(format!(
                        "column '{}': expected binary result format",
                        col.name
                    )));
                }
                Some(value.as_bytes().map_err(sqlx::Error::Decode)?)
            };
            let n = col.builder.append(raw, &col.name)?;
            col.var_bytes += n;
            bytes += n;
        }
        self.row_count += 1;
        self.var_bytes += bytes;
        Ok(())
    }

    /// Verify, once per scan, that the server sends what each decoder expects: a
    /// mismatch (e.g. a concurrent `ALTER TABLE … TYPE`) is an error, never a
    /// misinterpreted byte pattern.
    fn check_row_types(&self, row: &PgRow) -> Result<(), ExtractorError> {
        let described = row.columns();
        if self.columns.is_empty() {
            return Ok(()); // `SELECT 1` for a zero-column projection: nothing decoded.
        }
        if described.len() != self.columns.len() {
            return Err(ExtractorError::Internal(format!(
                "row has {} columns, projection has {}",
                described.len(),
                self.columns.len()
            )));
        }
        for (col, desc) in self.columns.iter().zip(described) {
            let type_info = desc.type_info();
            let Some(type_oid) = type_info.oid() else {
                return Err(ExtractorError::UnsupportedType(format!(
                    "column '{}': server type {} has no OID",
                    col.name,
                    type_info.name()
                )));
            };
            if !col.builder.accepts_oid(type_oid.0) {
                return Err(ExtractorError::UnsupportedType(format!(
                    "column '{}': server sent {} (oid {}) but the column maps to {}",
                    col.name,
                    type_info.name(),
                    type_oid.0,
                    col.data_type
                )));
            }
        }
        Ok(())
    }

    /// Flush when either the row cap or the byte cap is reached. Counts fixed-width
    /// buffers (`rows × width`) as well as variable-width payload, so a wide numeric
    /// table is capped too.
    pub(crate) fn should_flush(&self, batch_size: usize, max_batch_bytes: usize) -> bool {
        self.row_count >= batch_size || self.estimated_bytes() >= max_batch_bytes
    }

    /// Approximate buffered bytes since the last `finish()`: fixed-width values/offsets
    /// plus variable-width payload.
    pub(crate) fn estimated_bytes(&self) -> usize {
        self.row_count
            .saturating_mul(self.fixed_row_bytes)
            .saturating_add(self.var_bytes)
    }

    /// Check if the builder is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Get the current row count.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::connector::postgres::row_adapter::RowBatchBuilder;
    /// use rust_ballista_extraction_layer::types::{ColumnMetadata, TableMetadata};
    ///
    /// let col = |name: &str, data_type: &str, is_nullable| ColumnMetadata {
    ///     column_name: name.into(), data_type: data_type.into(), is_nullable,
    ///     numeric_precision: None, numeric_scale: None, udt_name: None, collation_name: None,
    /// };
    /// let meta = TableMetadata {
    ///     schema_name: "public".into(),
    ///     table_name: "orders".into(),
    ///     columns: vec![col("id", "bigint", false), col("status", "text", true)],
    /// };
    /// let builder = RowBatchBuilder::new(&meta)?;
    /// assert_eq!(builder.row_count(), 0);
    /// # Ok::<(), rust_ballista_extraction_layer::connector::errors::ExtractorError>(())
    /// ```
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Count one row appended through [`Self::append_copy_field`]. Split out because the
    /// COPY driver appends N fields and then counts the row once.
    pub(crate) fn inc_row(&mut self) {
        self.row_count += 1;
    }

    /// Append one `COPY … TO STDOUT (FORMAT BINARY)` field (`None` = SQL NULL) through
    /// the same decoder the cursor path uses.
    pub(crate) fn append_copy_field(
        &mut self,
        idx: usize,
        raw: Option<&[u8]>,
    ) -> Result<(), ExtractorError> {
        let col = self.columns.get_mut(idx).ok_or_else(|| {
            ExtractorError::Internal(format!("COPY field {idx} beyond projection"))
        })?;
        let n = col.builder.append(raw, &col.name)?;
        col.var_bytes += n;
        self.var_bytes += n;
        Ok(())
    }

    /// Rows that fit under both caps at `bytes_per_row`, plus 1/8 headroom so a batch of
    /// slightly narrower rows does not trigger a doubling reallocation; never above
    /// `max_rows`, never 0.
    fn rows_within(max_rows: usize, max_batch_bytes: usize, bytes_per_row: usize) -> usize {
        let fit = max_batch_bytes / bytes_per_row.max(1);
        fit.saturating_add(fit / 8)
            .saturating_add(1)
            .min(max_rows)
            .max(1)
    }

    /// Capacity for the next batch, from the measured width of the batch just built.
    /// Width per row does not depend on how many rows the batch held, so a short final or
    /// window-end batch still gives a good estimate.
    fn next_capacity(&self) -> usize {
        if self.row_count == 0 {
            return self.capacity;
        }
        let bytes_per_row = self.estimated_bytes().div_ceil(self.row_count);
        Self::rows_within(self.max_rows, self.max_batch_bytes, bytes_per_row)
    }

    /// Rows reserved in each fresh set of column builders.
    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Finish the batch in progress and reset for the next one. The finished arrays are
    /// shrunk to their contents; builders are re-created sized for the rows the next batch
    /// is expected to hold (see [`Self::for_batches`]), with each column's payload scaled
    /// to that row count.
    pub(crate) fn finish(&mut self) -> Result<RecordBatch, ExtractorError> {
        let next = self.next_capacity();
        let rows = self.row_count.max(1);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        for col in &mut self.columns {
            let mut array = col.builder.finish();
            // Release reserved-but-unused buffer capacity (an in-place realloc, no copy).
            // Without it a batch that flushed on the byte cap below its reservation — e.g.
            // the first batch of a scan, sized before any row width was measured — would
            // carry that unused capacity downstream for as long as it is buffered.
            if let Some(exclusive) = Arc::get_mut(&mut array) {
                exclusive.shrink_to_fit();
            }
            arrays.push(array);
            let payload = col.var_bytes.saturating_mul(next) / rows;
            col.builder = ColumnBuilder::new(&col.data_type, next, payload)?;
            col.var_bytes = 0;
        }
        self.capacity = next;

        // A zero-column projection (e.g. `COUNT(*)`, which only needs row counts)
        // carries its row count explicitly: `try_new` rejects empty column lists.
        let batch = if arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(self.row_count));
            RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options)?
        } else {
            RecordBatch::try_new(self.schema.clone(), arrays)?
        };

        self.row_count = 0;
        self.var_bytes = 0;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnMetadata;
    use arrow::array::{Array, ListArray, StringArray};
    use arrow::datatypes::DataType;

    fn col(name: &str, data_type: &str) -> ColumnMetadata {
        ColumnMetadata {
            column_name: name.to_string(),
            data_type: data_type.to_string(),
            is_nullable: true,
            numeric_precision: None,
            numeric_scale: None,
            udt_name: None,
            collation_name: None,
        }
    }

    fn table(columns: Vec<ColumnMetadata>) -> TableMetadata {
        TableMetadata {
            schema_name: "public".to_string(),
            table_name: "test_table".to_string(),
            columns,
        }
    }

    fn test_table_metadata() -> TableMetadata {
        let mut id = col("id", "bigint");
        id.is_nullable = false;
        table(vec![id, col("name", "text")])
    }

    #[test]
    fn test_row_batch_builder_empty_finish_keeps_schema() {
        let mut builder = RowBatchBuilder::new(&test_table_metadata()).unwrap();
        assert!(builder.is_empty());
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert!(!batch.schema().field(0).is_nullable());
        assert!(batch.schema().field(1).is_nullable());
    }

    #[test]
    fn test_finish_recreates_builders_with_capacity() {
        // P2: Arrow's `finish` moves the buffers out; the builder must be re-created with
        // the configured capacity so the next batch does not regrow from zero.
        let meta = table(vec![col("a", "bigint"), col("s", "text")]);
        let mut b = RowBatchBuilder::with_capacity(&meta, 4096).unwrap();
        for i in 0..10i64 {
            b.append_copy_field(0, Some(&i.to_be_bytes())).unwrap();
            b.append_copy_field(1, Some(b"xyz")).unwrap();
            b.inc_row();
        }
        let first = b.finish().unwrap();
        assert_eq!(first.num_rows(), 10);
        match &b.columns[0].builder {
            ColumnBuilder::Int64(ib) => assert!(ib.capacity() >= 4096),
            _ => panic!("expected Int64 builder"),
        }
        // Second batch still decodes into a valid, typed array.
        b.append_copy_field(0, Some(&7i64.to_be_bytes())).unwrap();
        b.append_copy_field(1, None).unwrap();
        b.inc_row();
        let second = b.finish().unwrap();
        assert_eq!(second.num_rows(), 1);
        assert!(second.column(1).is_null(0));
    }

    /// Fill one batch the way the extraction loops do: append rows until `should_flush`.
    fn fill_until_flush(b: &mut RowBatchBuilder, batch_size: usize, cap: usize, payload: &[u8]) {
        let mut i = 0i64;
        while !b.should_flush(batch_size, cap) {
            b.append_copy_field(0, Some(&i.to_be_bytes())).unwrap();
            b.append_copy_field(1, Some(payload)).unwrap();
            b.inc_row();
            i += 1;
        }
    }

    #[test]
    fn test_capacity_follows_the_byte_cap_not_batch_size() {
        // A wide row (8-byte id + 4-byte offset + 188-byte text = 200 B) under a 1 MiB cap
        // flushes at ~5,243 rows, far below batch_size = 256,000. Builders must not reserve
        // 256,000 rows: that capacity would ride along in every finished batch.
        let meta = table(vec![col("a", "bigint"), col("s", "text")]);
        let (batch_size, cap) = (256_000, 1 << 20);
        let payload = [b'x'; 188];
        let mut b = RowBatchBuilder::for_batches(&meta, batch_size, cap).unwrap();
        // Before any data only the fixed width (12 B) is known: an upper bound, < batch_size.
        assert!(b.capacity() < batch_size);

        fill_until_flush(&mut b, batch_size, cap, &payload);
        let first = b.finish().unwrap();
        let rows = first.num_rows();
        assert_eq!(rows, cap.div_ceil(200));
        // Next batch: sized for what fits (+1/8 headroom), not batch_size.
        assert!(
            b.capacity() >= rows && b.capacity() <= rows + rows / 8 + 2,
            "{}",
            b.capacity()
        );

        fill_until_flush(&mut b, batch_size, cap, &payload);
        let second = b.finish().unwrap();
        assert_eq!(second.num_rows(), rows);
        // Allocated memory stays close to the cap (payload + headroom), where reserving
        // batch_size rows used to add 256,000 x 12 B of fixed-width capacity (~3 MiB).
        let allocated = second.get_array_memory_size();
        assert!(
            allocated < 2 * cap,
            "second batch holds {allocated} bytes for a {cap}-byte cap"
        );
    }

    #[test]
    fn test_capacity_is_batch_size_when_the_row_cap_binds() {
        // Narrow rows under a generous byte cap: the row cap flushes first, so the builder
        // keeps reserving exactly batch_size rows.
        let meta = table(vec![col("a", "bigint"), col("s", "text")]);
        let (batch_size, cap) = (4_096, 16 << 20);
        let mut b = RowBatchBuilder::for_batches(&meta, batch_size, cap).unwrap();
        assert_eq!(b.capacity(), batch_size);
        fill_until_flush(&mut b, batch_size, cap, b"xyz");
        assert_eq!(b.finish().unwrap().num_rows(), batch_size);
        assert_eq!(b.capacity(), batch_size);
        // A short final batch does not shrink it: width per row, not row count, decides.
        for i in 0..10i64 {
            b.append_copy_field(0, Some(&i.to_be_bytes())).unwrap();
            b.append_copy_field(1, Some(b"xyz")).unwrap();
            b.inc_row();
        }
        assert_eq!(b.finish().unwrap().num_rows(), 10);
        assert_eq!(b.capacity(), batch_size);
    }

    #[test]
    fn test_byte_cap_counts_fixed_width_columns() {
        // P4: 100 numeric columns × 16 bytes = 1600 bytes/row with no variable payload.
        let mut n = col("n", "numeric");
        n.numeric_precision = Some(12);
        n.numeric_scale = Some(2);
        let meta = table((0..100).map(|_| n.clone()).collect());
        let mut b = RowBatchBuilder::with_capacity(&meta, 64).unwrap();
        for i in 0..100 {
            b.append_copy_field(i, None).unwrap();
        }
        b.inc_row();
        assert_eq!(b.estimated_bytes(), 1600);
        assert!(b.should_flush(1_000_000, 1600));
        assert!(!b.should_flush(1_000_000, 1601));
    }

    #[test]
    fn test_infinity_and_overflow_are_typed_errors() {
        for v in [i64::MAX, i64::MIN] {
            let err = pg_timestamp_to_arrow(v, "ts").unwrap_err();
            assert!(
                matches!(&err, ExtractorError::UnsupportedValue { column, .. } if column == "ts"),
                "{err}"
            );
        }
        // Largest finite PG timestamp (294276-12-31) overflows once shifted to 1970.
        assert!(pg_timestamp_to_arrow(i64::MAX - 1, "ts").is_err());
        assert_eq!(pg_timestamp_to_arrow(-PG_EPOCH_MICROS, "ts").unwrap(), 0);
        for v in [i32::MAX, i32::MIN] {
            assert!(matches!(
                pg_date_to_arrow(v, "d"),
                Err(ExtractorError::UnsupportedValue { .. })
            ));
        }
        assert!(pg_date_to_arrow(i32::MAX - 1, "d").is_err());
        assert_eq!(pg_date_to_arrow(-PG_EPOCH_DAYS, "d").unwrap(), 0);
    }

    #[test]
    fn test_numeric_error_names_column() {
        let mut n = col("price", "numeric");
        n.numeric_precision = None;
        n.numeric_scale = None;
        let mut b = RowBatchBuilder::new(&table(vec![n])).unwrap();
        // NaN sign word.
        let nan = [0u8, 0, 0, 0, 0xC0, 0, 0, 0];
        let err = b.append_copy_field(0, Some(&nan)).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("price") && msg.contains("NaN"), "{msg}");
    }

    fn text_array(items: &[Option<&str>]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&1i32.to_be_bytes());
        out.extend_from_slice(&0i32.to_be_bytes());
        out.extend_from_slice(&25u32.to_be_bytes());
        out.extend_from_slice(&(items.len() as i32).to_be_bytes());
        out.extend_from_slice(&1i32.to_be_bytes());
        for item in items {
            match item {
                None => out.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(s) => {
                    out.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    out.extend_from_slice(s.as_bytes());
                }
            }
        }
        out
    }

    #[test]
    fn test_text_array_decode_and_rejection() {
        let mut tags = col("tags", "ARRAY");
        tags.udt_name = Some("_text".to_string());
        let mut b = RowBatchBuilder::new(&table(vec![tags])).unwrap();
        b.append_copy_field(0, Some(&text_array(&[Some("a"), None, Some("c")])))
            .unwrap();
        b.inc_row();
        b.append_copy_field(0, None).unwrap();
        b.inc_row();
        // Empty (0-D) array.
        let mut empty = Vec::new();
        empty.extend_from_slice(&0i32.to_be_bytes());
        empty.extend_from_slice(&0i32.to_be_bytes());
        empty.extend_from_slice(&25u32.to_be_bytes());
        b.append_copy_field(0, Some(&empty)).unwrap();
        b.inc_row();
        // Truncated array fails without appending a partial element.
        let mut bad = text_array(&[Some("abc")]);
        bad.pop();
        assert!(b.append_copy_field(0, Some(&bad)).is_err());

        let batch = b.finish().unwrap();
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.len(), 3);
        assert!(list.is_valid(0) && !list.is_valid(1) && list.is_valid(2));
        let values = list
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), "a");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "c");
    }

    #[test]
    fn test_streaming_builders_match_schema_types() {
        // A timestamptz column must be built by a timezone-aware builder, or
        // RecordBatch::try_new rejects the batch. Same for text[] list columns.
        let mut tags = col("tags", "ARRAY");
        tags.udt_name = Some("_text".to_string());
        let meta = table(vec![col("created_at", "timestamp with time zone"), tags]);
        let mut builder = RowBatchBuilder::new(&meta).unwrap();
        let ts = 0i64.to_be_bytes();
        builder.append_copy_field(0, Some(&ts)).unwrap();
        builder.append_copy_field(1, None).unwrap();
        builder.inc_row();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_zero_column_batch_carries_row_count() {
        let mut builder = RowBatchBuilder::new(&table(vec![])).unwrap();
        builder.inc_row();
        builder.inc_row();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 0);
    }

    #[test]
    fn test_bad_fixed_length_is_an_error_not_a_panic() {
        let mut b = RowBatchBuilder::new(&table(vec![col("a", "integer")])).unwrap();
        assert!(b.append_copy_field(0, Some(&[1, 2, 3])).is_err());
        assert!(b.append_copy_field(5, None).is_err());
    }
}
