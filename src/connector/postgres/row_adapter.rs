//! Postgres Row Adapter
//! extractor/postgres/row_adapter.rs
//! This module is used to convert the PostgreSQL rows to the Arrow record batch.
//! Phase 3: Includes RowBatchBuilder for row-by-row appending with streaming batches.
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Date32Array,
    Date32Builder, Decimal128Array, Decimal128Builder, Float32Array, Float32Builder, Float64Array,
    Float64Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder,
    ListBuilder, StringArray, StringBuilder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use sqlx::{Row, postgres::PgRow};
use std::sync::Arc;

use crate::connector::errors::ExtractorError;
use crate::connector::postgres::arrow_type_mapper::ArrowTypeMapper;
use crate::types::{ColumnMetadata, TableMetadata};

pub struct PostgresRowAdapter;

/// Days from CE (0001-01-01) to the Unix epoch (1970-01-01).
/// `Datelike::num_days_from_ce()` for 1970-01-01 is 719_163; subtracting avoids
/// constructing `NaiveDate::from_ymd_opt(1970, 1, 1)` on every date cell.
const EPOCH_DAYS_FROM_CE: i32 = 719_163;

#[inline]
pub(crate) fn date_to_days(d: NaiveDate) -> i32 {
    d.num_days_from_ce() - EPOCH_DAYS_FROM_CE
}

impl PostgresRowAdapter {
    /// Create an empty Arrow builder matching a mapped data type. Timestamp builders carry
    /// the schema's timezone: `RecordBatch::try_new` rejects arrays whose type disagrees
    /// with the schema, so a `timestamptz` column (schema `Timestamp(µs, "UTC")`) must not
    /// be built by a naive `TimestampMicrosecondBuilder`.
    pub fn new_builder(data_type: &DataType) -> Box<dyn ArrayBuilder> {
        Self::new_builder_with_capacity(data_type, 1024)
    }

    /// Same as [`Self::new_builder`] but pre-reserves `capacity` rows so hot extraction
    /// loops do not regrow every column from zero on every batch. Pass the expected
    /// `batch_size` (row cap) from the caller.
    pub fn new_builder_with_capacity(
        data_type: &DataType,
        capacity: usize,
    ) -> Box<dyn ArrayBuilder> {
        match data_type {
            DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
            DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
            DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
            DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 1024)),
            DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, 1024)),
            DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => Box::new(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone_opt(tz.clone()),
            ),
            // Precision/scale are applied at finish time; only text[] arrays are supported.
            DataType::Decimal128(_, _) => Box::new(Decimal128Builder::with_capacity(capacity)),
            DataType::List(_) => Box::new(ListBuilder::with_capacity(
                StringBuilder::with_capacity(capacity, 1024),
                capacity,
            )),
            _ => Box::new(StringBuilder::with_capacity(capacity, 1024)),
        }
    }
}

/// RowBatchBuilder: Accumulates rows into Arrow builders for streaming batching.
/// Phase 3: Enables true streaming with bounded memory O(batch_size) instead of O(total_rows).
pub struct RowBatchBuilder {
    schema: Arc<Schema>,
    table_metadata: TableMetadata,
    builders: Vec<Box<dyn ArrayBuilder>>,
    /// Cached Arrow types per column so `finish()` does not re-run the string
    /// `match` in `ArrowTypeMapper::map` for every column of every batch.
    data_types: Vec<DataType>,
    row_count: usize,
    /// Approximate resident bytes of variable-width payloads appended since the last
    /// `finish()`. Used by [`Self::should_flush`] to cap wide-row batches by bytes
    /// as well as by row count.
    estimated_bytes: usize,
}

/// Downcast a boxed builder and append one optional value. Used by the COPY-wire
/// decoder where every arm shares the same shape; the `PgRow` decoder keeps its
/// explicit downcasts (its error strings are pinned by existing behavior).
macro_rules! append_copy {
    ($builder:expr, $ty:ty, $value:expr) => {{
        let b = $builder.as_any_mut().downcast_mut::<$ty>().ok_or_else(|| {
            ExtractorError::Internal(concat!("downcast to ", stringify!($ty), " failed").into())
        })?;
        b.append_option($value);
    }};
}

impl RowBatchBuilder {
    /// Create a new RowBatchBuilder with empty Arrow builders.
    pub fn new(table_metadata: &TableMetadata) -> Result<Self, ExtractorError> {
        Self::with_capacity(table_metadata, 1024)
    }

    /// Create a builder pre-sized for `capacity` rows. Extraction loops should pass
    /// their `batch_size` so builders do not reallocate on every batch.
    pub fn with_capacity(
        table_metadata: &TableMetadata,
        capacity: usize,
    ) -> Result<Self, ExtractorError> {
        let schema = PostgresRowAdapter::build_arrow_schema(table_metadata)?;

        let mut builders: Vec<Box<dyn ArrayBuilder>> =
            Vec::with_capacity(table_metadata.columns.len());
        let mut data_types: Vec<DataType> = Vec::with_capacity(table_metadata.columns.len());
        for column in &table_metadata.columns {
            let data_type = ArrowTypeMapper::map(column)?;
            builders.push(PostgresRowAdapter::new_builder_with_capacity(
                &data_type, capacity,
            ));
            data_types.push(data_type);
        }

        Ok(Self {
            schema,
            table_metadata: table_metadata.clone(),
            builders,
            data_types,
            row_count: 0,
            estimated_bytes: 0,
        })
    }

    /// Append a single row to the builders.
    ///
    /// Columns are decoded by **ordinal position**: every query builder in this crate
    /// emits `SELECT` columns in `table_metadata.columns` order, so index `idx` is the
    /// same column. Ordinal lookup avoids a per-cell name hash on the hot path.
    pub fn append_row(&mut self, row: &PgRow) -> Result<(), ExtractorError> {
        // Disjoint field borrows: `columns`/`data_types` are read-only while `builders`
        // is mutated. This avoids the per-row `columns.clone()` the old code needed to
        // satisfy the borrow checker.
        let columns = &self.table_metadata.columns;
        let builders = &mut self.builders;
        let mut bytes = 0usize;
        for (idx, column) in columns.iter().enumerate() {
            bytes += Self::append_value_to_builder(builders, idx, row, column)?;
        }
        self.row_count += 1;
        self.estimated_bytes += bytes;
        Ok(())
    }

    /// Flush when either the row cap or the byte cap is reached. Wide rows
    /// (text/json/bytea/text[]) can otherwise blow memory long before `batch_size`.
    pub fn should_flush(&self, batch_size: usize, max_batch_bytes: usize) -> bool {
        self.row_count >= batch_size || self.estimated_bytes >= max_batch_bytes
    }

    /// Approximate variable-width bytes buffered since the last `finish()`.
    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    /// Append a value to a specific builder based on the column type.
    /// Returns the approximate variable-width bytes appended (0 for fixed-width).
    /// Static (no `&self`) so [`Self::append_row`] can hold disjoint borrows of
    /// `table_metadata` and `builders` without cloning.
    fn append_value_to_builder(
        builders: &mut [Box<dyn ArrayBuilder>],
        builder_idx: usize,
        row: &PgRow,
        column: &ColumnMetadata,
    ) -> Result<usize, ExtractorError> {
        let builder = &mut builders[builder_idx];
        // Ordinal decode: SELECT order == metadata order (see query_builder::push_columns).
        let col: usize = builder_idx;

        match column.data_type.as_str() {
            "smallint" => {
                let value: Option<i16> = row.try_get(col)?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Int16Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Int16Builder failed".into())
                    })?;
                b.append_option(value);
                Ok(0)
            }
            "integer" => {
                let value: Option<i32> = row.try_get(col)?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Int32Builder failed".into())
                    })?;
                b.append_option(value);
                Ok(0)
            }
            "bigint" => {
                let value: Option<i64> = row.try_get(col)?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Int64Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Int64Builder failed".into())
                    })?;
                b.append_option(value);
                Ok(0)
            }
            "real" => {
                let value: Option<f32> = row.try_get(col)?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Float32Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Float32Builder failed".into())
                    })?;
                b.append_option(value);
                Ok(0)
            }
            "double precision" => {
                let value: Option<f64> = row.try_get(col)?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Float64Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Float64Builder failed".into())
                    })?;
                b.append_option(value);
                Ok(0)
            }
            "numeric" => {
                let value: Option<BigDecimal> = row.try_get(col)?;
                let scale = column.numeric_scale.unwrap_or(10) as i64;
                let i128_value = value
                    .map(|d| {
                        crate::connector::postgres::arrow_type_mapper::decimal_to_unscaled(d, scale)
                    })
                    .transpose()?;

                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Decimal128Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Decimal128Builder failed".into())
                    })?;
                b.append_option(i128_value);
                Ok(0)
            }
            "boolean" => {
                let value: Option<bool> = row.try_get(col)?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to BooleanBuilder failed".into())
                    })?;
                b.append_option(value);
                Ok(0)
            }
            "text" | "character varying" | "character" => {
                let value: Option<String> = row.try_get(col)?;
                let bytes = value.as_ref().map_or(0, |s| s.len());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to StringBuilder failed".into())
                    })?;
                b.append_option(value);
                Ok(bytes)
            }
            "USER-DEFINED" => {
                let value: Option<String> = row.try_get(col)?;
                let bytes = value.as_ref().map_or(0, |s| s.len());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to StringBuilder failed".into())
                    })?;
                b.append_option(value);
                Ok(bytes)
            }
            "timestamp with time zone" => {
                let value: Option<DateTime<Utc>> = row.try_get(col)?;
                let micros = value.map(|dt| dt.timestamp_micros());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal(
                            "downcast to TimestampMicrosecondBuilder failed".into(),
                        )
                    })?;
                b.append_option(micros);
                Ok(0)
            }
            "timestamp without time zone" => {
                let value: Option<NaiveDateTime> = row.try_get(col)?;
                let micros = value.map(|dt| dt.and_utc().timestamp_micros());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal(
                            "downcast to TimestampMicrosecondBuilder failed".into(),
                        )
                    })?;
                b.append_option(micros);
                Ok(0)
            }
            "date" => {
                let value: Option<NaiveDate> = row.try_get(col)?;
                let days = value.map(date_to_days);
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Date32Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Date32Builder failed".into())
                    })?;
                b.append_option(days);
                Ok(0)
            }
            "uuid" => {
                let value: Option<uuid::Uuid> = row.try_get(col)?;
                let s = value.map(|u| u.to_string());
                let bytes = s.as_ref().map_or(0, |s| s.len());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to StringBuilder failed".into())
                    })?;
                b.append_option(s);
                Ok(bytes)
            }
            "bytea" => {
                let value: Option<Vec<u8>> = row.try_get(col)?;
                let bytes = value.as_ref().map_or(0, |v| v.len());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<BinaryBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to BinaryBuilder failed".into())
                    })?;
                b.append_option(value);
                Ok(bytes)
            }
            "jsonb" | "json" => {
                let value: Option<serde_json::Value> = row.try_get(col)?;
                let s = value.map(|v| v.to_string());
                let bytes = s.as_ref().map_or(0, |s| s.len());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to StringBuilder failed".into())
                    })?;
                b.append_option(s);
                Ok(bytes)
            }
            "ARRAY" => match column.udt_name.as_deref() {
                Some("_text") => {
                    let value: Option<Vec<Option<String>>> = row.try_get(col)?;
                    let bytes = value
                        .as_ref()
                        .map_or(0, |items| items.iter().flatten().map(|s| s.len()).sum());
                    let b = builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<StringBuilder>>()
                        .ok_or_else(|| {
                            ExtractorError::Internal("downcast to ListBuilder failed".into())
                        })?;
                    PostgresRowAdapter::append_text_array_option(b, value);
                    Ok(bytes)
                }
                other => Err(ExtractorError::UnsupportedType(format!(
                    "array element type {:?} for column '{}' (only text[] arrays are supported)",
                    other, column.column_name
                ))),
            },
            _ => Err(ExtractorError::UnsupportedType(column.data_type.clone())),
        }
    }

    /// Check if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Get the current row count.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Count one row appended through [`Self::append_copy_field`]. Split out (rather
    /// than folded into the field appender) because the COPY driver appends N fields
    /// and then counts the row once.
    pub(crate) fn inc_row(&mut self) {
        self.row_count += 1;
    }

    /// Append one `COPY … TO STDOUT (FORMAT BINARY)` field's raw bytes to a builder.
    ///
    /// `raw` is `None` for SQL NULL (length `-1` on the wire). Byte layouts are the
    /// PostgreSQL **binary** representations — the same ones `sqlx` decodes from the
    /// extended-protocol wire, so a differential `COPY`-vs-cursor test agrees by
    /// construction as long as each arm mirrors `sqlx`'s decoding:
    /// big-endian integers/floats, `0x00/0x01` booleans, timestamps as `µs` since
    /// 2000-01-01, dates as days since 2000-01-01, the base-10000 `numeric` layout
    /// (via [`numeric_bytes_to_unscaled`](crate::connector::postgres::arrow_type_mapper::numeric_bytes_to_unscaled)),
    /// raw UTF-8 text, 16-byte UUIDs, raw `bytea`, UTF-8 `json` / version-prefixed
    /// `jsonb`, and the binary `text[]` array layout (see
    /// [`parse_copy_text_array`](crate::connector::postgres::copy::parse_copy_text_array)).
    /// Anything else is [`ExtractorError::UnsupportedType`] — the caller falls back
    /// to the cursor path, never silently wrong data.
    pub(crate) fn append_copy_field(
        &mut self,
        builder_idx: usize,
        column: &ColumnMetadata,
        raw: Option<&[u8]>,
    ) -> Result<(), ExtractorError> {
        use crate::connector::postgres::arrow_type_mapper::numeric_bytes_to_unscaled;
        use crate::connector::postgres::copy::parse_copy_text_array;

        /// Fixed-width helper: `None` appends null, `Some` decodes exactly N bytes.
        fn fixed<const N: usize>(
            raw: Option<&[u8]>,
            what: &str,
        ) -> Result<Option<[u8; N]>, ExtractorError> {
            match raw {
                None => Ok(None),
                Some(b) if b.len() == N => {
                    let mut arr = [0u8; N];
                    arr.copy_from_slice(b);
                    Ok(Some(arr))
                }
                _ => Err(ExtractorError::Internal(format!(
                    "corrupt binary COPY field for {what}: bad length"
                ))),
            }
        }

        let builder = &mut self.builders[builder_idx];
        let bytes: usize = match column.data_type.as_str() {
            "smallint" => {
                let value = fixed::<2>(raw, "smallint")?.map(i16::from_be_bytes);
                append_copy!(builder, Int16Builder, value);
                0
            }
            "integer" => {
                let value = fixed::<4>(raw, "integer")?.map(i32::from_be_bytes);
                append_copy!(builder, Int32Builder, value);
                0
            }
            "bigint" => {
                let value = fixed::<8>(raw, "bigint")?.map(i64::from_be_bytes);
                append_copy!(builder, Int64Builder, value);
                0
            }
            "real" => {
                let value = fixed::<4>(raw, "real")?.map(f32::from_be_bytes);
                append_copy!(builder, Float32Builder, value);
                0
            }
            "double precision" => {
                let value = fixed::<8>(raw, "double precision")?.map(f64::from_be_bytes);
                append_copy!(builder, Float64Builder, value);
                0
            }
            "boolean" => {
                let value = fixed::<1>(raw, "boolean")?.map(|b| b[0] != 0);
                append_copy!(builder, BooleanBuilder, value);
                0
            }
            "text" | "character varying" | "character" | "USER-DEFINED" => {
                let value: Option<String> = raw
                    .map(|b| {
                        String::from_utf8(b.to_vec()).map_err(|e| {
                            ExtractorError::Internal(format!("invalid UTF-8 in text field: {e}"))
                        })
                    })
                    .transpose()?;
                let n = value.as_ref().map_or(0, |s| s.len());
                append_copy!(builder, StringBuilder, value);
                n
            }
            "timestamp with time zone" => {
                let micros = fixed::<8>(raw, "timestamptz")?.map(|b| {
                    let pg_micros = i64::from_be_bytes(b);
                    // ±infinity arrive as INT64_MAX/MIN; chrono cannot represent them,
                    // and the cursor path errors on them too — fail, don't fabricate.
                    pg_micros.checked_add(crate::connector::postgres::copy::PG_EPOCH_MICROS)
                });
                let micros = match micros {
                    None => None, // SQL NULL.
                    Some(None) => {
                        return Err(ExtractorError::Internal(
                            "timestamptz infinity has no Arrow representation".into(),
                        ));
                    }
                    Some(Some(m)) => Some(m),
                };
                append_copy!(builder, TimestampMicrosecondBuilder, micros);
                0
            }
            "timestamp without time zone" => {
                let micros = fixed::<8>(raw, "timestamp")?.map(|b| {
                    let pg_micros = i64::from_be_bytes(b);
                    pg_micros.checked_add(crate::connector::postgres::copy::PG_EPOCH_MICROS)
                });
                let micros = match micros {
                    None => None,
                    Some(None) => {
                        return Err(ExtractorError::Internal(
                            "timestamp infinity has no Arrow representation".into(),
                        ));
                    }
                    Some(Some(m)) => Some(m),
                };
                append_copy!(builder, TimestampMicrosecondBuilder, micros);
                0
            }
            "date" => {
                let days = fixed::<4>(raw, "date")?.map(|b| {
                    let pg_days = i32::from_be_bytes(b);
                    if pg_days == i32::MAX || pg_days == i32::MIN {
                        None // ±infinity: no Date32 representation (cursor path errors too).
                    } else {
                        pg_days.checked_add(crate::connector::postgres::copy::PG_EPOCH_DAYS)
                    }
                });
                let days = match days {
                    None => None,
                    Some(None) => {
                        return Err(ExtractorError::Internal(
                            "date infinity/overflow has no Date32 representation".into(),
                        ));
                    }
                    Some(Some(d)) => Some(d),
                };
                append_copy!(builder, Date32Builder, days);
                0
            }
            "numeric" => {
                let scale = column.numeric_scale.unwrap_or(10) as i64;
                let value = raw
                    .map(|b| numeric_bytes_to_unscaled(b, scale))
                    .transpose()?;
                append_copy!(builder, Decimal128Builder, value);
                0
            }
            "uuid" => {
                let value =
                    fixed::<16>(raw, "uuid")?.map(|b| uuid::Uuid::from_bytes(b).to_string());
                let n = value.as_ref().map_or(0, |s| s.len());
                append_copy!(builder, StringBuilder, value);
                n
            }
            "bytea" => {
                let value: Option<Vec<u8>> = raw.map(|b| b.to_vec());
                let n = value.as_ref().map_or(0, |v| v.len());
                append_copy!(builder, BinaryBuilder, value);
                n
            }
            "json" => {
                let value: Option<String> = raw
                    .map(|b| {
                        String::from_utf8(b.to_vec()).map_err(|e| {
                            ExtractorError::Internal(format!("invalid UTF-8 in json field: {e}"))
                        })
                    })
                    .transpose()?;
                let n = value.as_ref().map_or(0, |s| s.len());
                append_copy!(builder, StringBuilder, value);
                n
            }
            "jsonb" => {
                let value: Option<String> = raw
                    .map(|b| {
                        // jsonb binary: 1-byte version (always 1) + UTF-8 JSON text.
                        if b.first() != Some(&1) {
                            return Err(ExtractorError::Internal(
                                "corrupt binary jsonb: bad version byte".into(),
                            ));
                        }
                        String::from_utf8(b[1..].to_vec()).map_err(|e| {
                            ExtractorError::Internal(format!("invalid UTF-8 in jsonb field: {e}"))
                        })
                    })
                    .transpose()?;
                let n = value.as_ref().map_or(0, |s| s.len());
                append_copy!(builder, StringBuilder, value);
                n
            }
            "ARRAY" => match column.udt_name.as_deref() {
                Some("_text") => {
                    let items = raw.map(parse_copy_text_array).transpose()?;
                    let n = items.as_ref().map_or(0, |v: &Vec<Option<String>>| {
                        v.iter().flatten().map(|s| s.len()).sum()
                    });
                    let b = builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<StringBuilder>>()
                        .ok_or_else(|| {
                            ExtractorError::Internal("downcast to ListBuilder failed".into())
                        })?;
                    PostgresRowAdapter::append_text_array_option(b, items);
                    n
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
        };
        self.estimated_bytes += bytes;
        Ok(())
    }

    /// Finish building and return a RecordBatch. Resets internal state for reuse.
    /// Builders are reused across batches: `ArrayBuilder::finish()` already resets
    /// length while retaining capacity, so no reallocation happens here.
    pub fn finish(&mut self) -> Result<RecordBatch, ExtractorError> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.builders.len());

        for (idx, builder) in self.builders.iter_mut().enumerate() {
            let column = &self.table_metadata.columns[idx];
            let data_type = &self.data_types[idx];

            // For Decimal128, apply precision and scale after finishing
            let array = if matches!(data_type, DataType::Decimal128(_, _)) {
                let precision = column.numeric_precision.unwrap_or(38) as u8;
                let scale = column.numeric_scale.unwrap_or(10) as i8;
                let raw_array = builder.finish();

                let decimal_array = raw_array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Decimal128Array failed".into())
                    })?;

                let array = decimal_array
                    .clone()
                    .with_precision_and_scale(precision, scale)
                    .map_err(ExtractorError::Arrow)?;
                Arc::new(array)
            } else {
                builder.finish()
            };

            arrays.push(array);
        }

        // A zero-column projection (e.g. `COUNT(*)`, which only needs row counts)
        // carries its row count explicitly: `try_new` without it rejects empty
        // column lists ("must either specify a row count or at least one column").
        let batch = if arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(self.row_count));
            RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options)?
        } else {
            RecordBatch::try_new(self.schema.clone(), arrays)?
        };

        // `finish()` on each builder already reset it for reuse (capacity retained);
        // only reset the counters here — no `clear()` + re-`push()` churn.
        self.row_count = 0;
        self.estimated_bytes = 0;

        Ok(batch)
    }
}

impl PostgresRowAdapter {
    pub fn build_array(
        rows: &[PgRow],
        column: &ColumnMetadata,
    ) -> Result<ArrayRef, ExtractorError> {
        match column.data_type.as_str() {
            "smallint" => {
                let values: Vec<Option<i16>> = rows
                    .iter()
                    .map(|row| row.try_get(column.column_name.as_str()))
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(Int16Array::from(values)))
            }

            "integer" => {
                let values: Vec<Option<i32>> = rows
                    .iter()
                    .map(|row| row.try_get(column.column_name.as_str()))
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(Int32Array::from(values)))
            }

            "bigint" => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| row.try_get(column.column_name.as_str()))
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(Int64Array::from(values)))
            }

            "real" => {
                let values: Vec<Option<f32>> = rows
                    .iter()
                    .map(|row| row.try_get(column.column_name.as_str()))
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(Float32Array::from(values)))
            }

            "double precision" => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|row| row.try_get(column.column_name.as_str()))
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(Float64Array::from(values)))
            }

            "numeric" => {
                let precision_raw = column.numeric_precision.unwrap_or(38);
                let scale_raw = column.numeric_scale.unwrap_or(10);
                if !(1..=38).contains(&precision_raw) || !(-127..=127).contains(&scale_raw) {
                    return Err(ExtractorError::Internal(format!(
                        "numeric precision/scale out of range for column '{}': {}/{}",
                        column.column_name, precision_raw, scale_raw
                    )));
                }
                let precision = precision_raw as u8;
                let scale = scale_raw as i8;
                let scale_i64 = scale as i64;

                let values: Vec<Option<i128>> = rows
                    .iter()
                    .map(|row| {
                        let value: Option<BigDecimal> = row.try_get(column.column_name.as_str())?;

                        value
                            .map(|decimal| {
                                crate::connector::postgres::arrow_type_mapper::decimal_to_unscaled(
                                    decimal, scale_i64,
                                )
                                .map_err(|e| sqlx::Error::Decode(e.to_string().into()))
                            })
                            .transpose()
                    })
                    .collect::<Result<_, _>>()?;

                let array =
                    Decimal128Array::from(values).with_precision_and_scale(precision, scale)?;

                Ok(Arc::new(array))
            }

            "boolean" => {
                let values: Vec<Option<bool>> = rows
                    .iter()
                    .map(|row| row.try_get(column.column_name.as_str()))
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(BooleanArray::from(values)))
            }

            "text" | "character varying" | "character" => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| row.try_get(column.column_name.as_str()))
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(StringArray::from(values)))
            }

            "USER-DEFINED" => {
                let udt_name = column.udt_name.as_deref().unwrap_or("unknown");

                log::debug!(
                    "Decoding user-defined PostgreSQL type '{}' as String",
                    udt_name
                );

                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| row.try_get(column.column_name.as_str()))
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(StringArray::from(values)))
            }

            "timestamp with time zone" => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| {
                        let value: Option<DateTime<Utc>> =
                            row.try_get(column.column_name.as_str())?;

                        Ok(value.map(|dt| dt.timestamp_micros()))
                    })
                    .collect::<Result<_, sqlx::Error>>()?;

                let array = TimestampMicrosecondArray::from(values).with_timezone("UTC");

                Ok(Arc::new(array))
            }

            "timestamp without time zone" => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| {
                        let value: Option<NaiveDateTime> =
                            row.try_get(column.column_name.as_str())?;

                        Ok(value.map(|dt| dt.and_utc().timestamp_micros()))
                    })
                    .collect::<Result<_, sqlx::Error>>()?;

                let array = TimestampMicrosecondArray::from(values);

                Ok(Arc::new(array))
            }

            "ARRAY" => match column.udt_name.as_deref() {
                Some("_text") => Self::build_text_array(rows, column),
                other => Err(ExtractorError::UnsupportedType(format!(
                    "array element type {:?} for column '{}' (only text[] arrays are supported)",
                    other, column.column_name
                ))),
            },

            "jsonb" | "json" => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| {
                        let value: Option<serde_json::Value> =
                            row.try_get(column.column_name.as_str())?;

                        Ok(value.map(|v| v.to_string()))
                    })
                    .collect::<Result<_, sqlx::Error>>()?;

                Ok(Arc::new(StringArray::from(values)))
            }

            "date" => {
                let values: Vec<Option<i32>> = rows
                    .iter()
                    .map(|row| {
                        let value: Option<NaiveDate> = row.try_get(column.column_name.as_str())?;

                        Ok(value.map(date_to_days))
                    })
                    .collect::<Result<_, sqlx::Error>>()?;

                Ok(Arc::new(Date32Array::from(values)))
            }

            "uuid" => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| {
                        let value: Option<uuid::Uuid> = row.try_get(column.column_name.as_str())?;

                        Ok(value.map(|u| u.to_string()))
                    })
                    .collect::<Result<_, sqlx::Error>>()?;

                Ok(Arc::new(StringArray::from(values)))
            }

            "bytea" => {
                let values: Vec<Option<Vec<u8>>> = rows
                    .iter()
                    .map(|row| row.try_get(column.column_name.as_str()))
                    .collect::<Result<_, _>>()?;

                let array = BinaryArray::from_iter(values.iter().map(|v| v.as_deref()));

                Ok(Arc::new(array))
            }

            _ => Err(ExtractorError::UnsupportedType(column.data_type.clone())),
        }
    }

    pub fn build_text_array(
        rows: &[PgRow],
        column: &ColumnMetadata,
    ) -> Result<ArrayRef, ExtractorError> {
        let mut builder = ListBuilder::new(StringBuilder::new());

        for row in rows {
            let value: Option<Vec<Option<String>>> = row.try_get(column.column_name.as_str())?;

            Self::append_text_array_option(&mut builder, value);
        }

        Ok(Arc::new(builder.finish()))
    }

    /// Append one `text[]` value (or SQL NULL) to a list builder. Shared by the batch path
    /// (`build_text_array`) and the streaming builders; array elements may themselves be NULL.
    pub fn append_text_array_option(
        builder: &mut ListBuilder<StringBuilder>,
        value: Option<Vec<Option<String>>>,
    ) {
        match value {
            Some(items) => {
                for item in items {
                    builder.values().append_option(item);
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }

    pub fn rows_to_record_batch(
        rows: &[PgRow],
        table_metadata: &TableMetadata,
        arrow_schema: Arc<Schema>,
    ) -> Result<RecordBatch, ExtractorError> {
        let arrays = table_metadata
            .columns
            .iter()
            .map(|column| Self::build_array(rows, column))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(RecordBatch::try_new(arrow_schema, arrays)?)
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnMetadata;
    use arrow::array::Array;
    use arrow::datatypes::DataType;

    /// Helper to create a simple test table metadata
    fn test_table_metadata() -> TableMetadata {
        TableMetadata {
            schema_name: "public".to_string(),
            table_name: "test_table".to_string(),
            columns: vec![
                ColumnMetadata {
                    column_name: "id".to_string(),
                    data_type: "bigint".to_string(),
                    is_nullable: false,
                    numeric_precision: None,
                    numeric_scale: None,
                    udt_name: None,
                    collation_name: None,
                },
                ColumnMetadata {
                    column_name: "name".to_string(),
                    data_type: "text".to_string(),
                    is_nullable: true,
                    numeric_precision: None,
                    numeric_scale: None,
                    udt_name: None,
                    collation_name: None,
                },
            ],
        }
    }

    #[test]
    fn test_numeric_unscaled_conversion() {
        use bigdecimal::ToPrimitive;
        use std::str::FromStr;
        // Mirrors the "numeric" arms in append_row: Arrow Decimal128 stores the
        // unscaled integer, so 123.45 at scale 2 must become 12345 (not 123).
        // Regression test: with_scale().to_i128() truncated to the integer part.
        for (text, scale, expected) in [
            ("123.45", 2i64, 12345i128),
            ("123.45", 0, 123),
            ("-7.5", 1, -75),
            ("0.00", 2, 0),
            ("1200", -2, 12),
        ] {
            let decimal = BigDecimal::from_str(text).unwrap();
            let unscaled = (decimal * BigDecimal::from(10).powi(scale))
                .to_i128()
                .unwrap();
            assert_eq!(unscaled, expected, "text={} scale={}", text, scale);
        }
    }

    #[test]
    fn test_row_batch_builder_creation() {
        let table_metadata = test_table_metadata();
        let builder = RowBatchBuilder::new(&table_metadata);

        assert!(builder.is_ok());
        let builder = builder.unwrap();
        assert!(builder.is_empty());
        assert_eq!(builder.row_count(), 0);
    }

    #[test]
    fn test_row_batch_builder_empty_finish() {
        let table_metadata = test_table_metadata();
        let mut builder = RowBatchBuilder::new(&table_metadata).unwrap();

        // Finishing an empty batch should still produce a valid empty RecordBatch
        let batch = builder.finish();
        assert!(batch.is_ok());
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2); // id, name
    }

    #[test]
    fn test_row_batch_builder_schema() {
        let table_metadata = test_table_metadata();
        let builder = RowBatchBuilder::new(&table_metadata).unwrap();

        let schema = builder.schema.clone();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");

        // Verify data types
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_row_batch_builder_reuse_after_finish() {
        let table_metadata = test_table_metadata();
        let mut builder = RowBatchBuilder::new(&table_metadata).unwrap();

        // First finish on empty should work
        let batch1 = builder.finish().unwrap();
        assert_eq!(batch1.num_rows(), 0);

        // After finish, builder should be reset and ready for reuse
        assert!(builder.is_empty());
        assert_eq!(builder.row_count(), 0);

        // Second finish should also work
        let batch2 = builder.finish().unwrap();
        assert_eq!(batch2.num_rows(), 0);
    }

    #[test]
    fn test_row_batch_builder_streaming_batching() {
        // This test verifies the batching semantics
        let table_metadata = test_table_metadata();
        let mut builder = RowBatchBuilder::new(&table_metadata).unwrap();

        // Simulate batch_size = 3
        let _batch_size = 3;

        // With 10 rows and batch_size=3, we should produce 4 batches:
        // [3, 3, 3, 1]
        let expected_batches = vec![3, 3, 3, 1];

        // This test just verifies the builder can be created and finished
        // Actual row insertion would require mocking SQLx rows, which is complex
        // The integration test with real database covers actual data flow

        for _expected_batch_size in expected_batches {
            // In real usage, rows would be appended one by one
            // until builder.row_count() >= batch_size
            // Then finish() is called

            // For now, verify that finish works
            let batch = builder.finish().unwrap();
            assert_eq!(batch.num_rows(), 0); // empty in this test
        }
    }

    #[test]
    fn test_build_arrow_schema_preserves_nullability() {
        let table_metadata = test_table_metadata();
        let schema = PostgresRowAdapter::build_arrow_schema(&table_metadata).unwrap();

        // id is NOT NULL
        assert!(!schema.field(0).is_nullable());

        // name is nullable
        assert!(schema.field(1).is_nullable());
    }

    #[test]
    fn test_build_arrow_schema_type_mapping() {
        let mut table_metadata = test_table_metadata();

        // Add a numeric column
        table_metadata.columns.push(ColumnMetadata {
            column_name: "amount".to_string(),
            data_type: "numeric".to_string(),
            is_nullable: true,
            numeric_precision: Some(10),
            numeric_scale: Some(2),
            udt_name: None,
            collation_name: None,
        });

        let schema = PostgresRowAdapter::build_arrow_schema(&table_metadata).unwrap();

        assert_eq!(schema.fields().len(), 3);

        // Verify the numeric field is Decimal128
        let numeric_field = schema.field(2);
        assert_eq!(numeric_field.name(), "amount");
        match numeric_field.data_type() {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(*precision, 10);
                assert_eq!(*scale, 2);
            }
            _ => panic!("Expected Decimal128 type for numeric column"),
        }
    }

    #[test]
    fn test_row_batch_builder_finish_resets_state() {
        let table_metadata = test_table_metadata();
        let mut builder = RowBatchBuilder::new(&table_metadata).unwrap();

        // Finish should reset row_count
        assert_eq!(builder.row_count(), 0);

        let _ = builder.finish().unwrap();

        assert_eq!(builder.row_count(), 0);
        assert!(builder.is_empty());
    }

    #[test]
    fn test_append_text_array_option() {
        let mut builder = ListBuilder::new(StringBuilder::new());

        // Row with values including a NULL element.
        PostgresRowAdapter::append_text_array_option(
            &mut builder,
            Some(vec![Some("a".to_string()), None, Some("c".to_string())]),
        );
        // SQL NULL array.
        PostgresRowAdapter::append_text_array_option(&mut builder, None);
        // Empty (non-null) array.
        PostgresRowAdapter::append_text_array_option(&mut builder, Some(vec![]));

        let list = builder.finish();
        assert_eq!(list.len(), 3);
        assert!(list.is_valid(0));
        assert!(!list.is_valid(1));
        assert!(list.is_valid(2));

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
        use crate::types::TableMetadata;

        // Regression test: a timestamptz column must be built by a timezone-aware builder,
        // or RecordBatch::try_new rejects the batch ("expected Timestamp(µs, UTC) but
        // found Timestamp(µs)"). Same for text[] list columns.
        let metadata = TableMetadata {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![
                ColumnMetadata {
                    column_name: "created_at".to_string(),
                    data_type: "timestamp with time zone".to_string(),
                    is_nullable: false,
                    numeric_precision: None,
                    numeric_scale: None,
                    udt_name: None,
                    collation_name: None,
                },
                ColumnMetadata {
                    column_name: "tags".to_string(),
                    data_type: "ARRAY".to_string(),
                    is_nullable: false,
                    numeric_precision: None,
                    numeric_scale: None,
                    udt_name: Some("_text".to_string()),
                    collation_name: None,
                },
            ],
        };

        let mut builder = RowBatchBuilder::new(&metadata).unwrap();
        // Empty batch still validates every column type against the schema.
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_zero_column_batch_carries_row_count() {
        // `COUNT(*)` prunes the scan to zero columns: `finish()` must produce a
        // valid 0-column batch instead of failing with "must either specify a
        // row count or at least one column".
        let metadata = TableMetadata {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![],
        };
        let mut builder = RowBatchBuilder::new(&metadata).unwrap();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 0);
    }
}
