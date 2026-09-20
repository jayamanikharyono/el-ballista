//! MySQL → Arrow typed decode.
//!
//! Builds the Arrow schema from [`TableMetadata`] via [`super::type_mapper::arrow_type_for`] and
//! decodes each column into the matching Arrow builder. Column-oriented: one type decision per
//! column, then a decode loop over the rows.
//!
//! Widths are preserved to match the Postgres connector (see [`super::type_mapper`]): integers
//! decode into `Int8`/`Int16`/`Int32`/`Int64` and floats into `Float32`/`Float64`. Integer values
//! are read at the widest compatible sqlx type (`i64`, or `u64` for unsigned columns) and cast down
//! to the target width — lossless because the mapping picks a width that holds the source range.
//! `decimal`/`numeric` → `Decimal128`; `bit` → `Boolean`; `date`/`datetime`/`timestamp` → Arrow
//! date/timestamp; binary/blob → `Binary`; `json`/text → `Utf8`.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{NaiveDate, NaiveDateTime};
use sqlx::Row;
use sqlx::mysql::MySqlRow;

use super::type_mapper::arrow_type_for;
use crate::connector::errors::ExtractorError;
use crate::types::{ColumnMetadata, TableMetadata};

/// Days from the Unix epoch, for `Date32`.
fn days_from_epoch(d: NaiveDate) -> i32 {
    (d - NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch")).num_days() as i32
}

/// `BigDecimal` → the unscaled `i128` Arrow `Decimal128` stores (`123.45` at scale 2 → `12345`).
fn decimal_to_unscaled(d: BigDecimal, scale: i64) -> Result<i128, ExtractorError> {
    let text = d.to_string();
    let unscaled = d * BigDecimal::from(10).powi(scale);
    unscaled.to_i128().ok_or_else(|| {
        ExtractorError::Internal(format!("decimal value cannot fit into i128: {text}"))
    })
}

/// Read integer cell `idx` as `i128` (widest, sign-aware): `u64` for unsigned columns, else `i64`.
/// The caller casts to the target width, which the type contract guarantees is lossless.
fn int_cell(r: &MySqlRow, idx: usize, unsigned: bool) -> Result<Option<i128>, ExtractorError> {
    if unsigned {
        let v: Option<u64> = r.try_get(idx)?;
        Ok(v.map(|x| x as i128))
    } else {
        let v: Option<i64> = r.try_get(idx)?;
        Ok(v.map(|x| x as i128))
    }
}

pub struct MysqlRowAdapter;

impl MysqlRowAdapter {
    /// Arrow schema mirroring the table's columns and their mapped Arrow types.
    pub fn build_arrow_schema(table: &TableMetadata) -> Result<Schema, ExtractorError> {
        let mut fields: Vec<Field> = Vec::with_capacity(table.columns.len());
        for c in &table.columns {
            fields.push(Field::new(&c.column_name, arrow_type_for(c)?, c.is_nullable));
        }
        Ok(Schema::new(fields))
    }

    /// Decode column `idx` from every row into a typed Arrow array.
    pub fn decode_column(
        rows: &[MySqlRow],
        idx: usize,
        col: &ColumnMetadata,
    ) -> Result<ArrayRef, ExtractorError> {
        let data_type = col.data_type.to_ascii_lowercase();
        let unsigned = col
            .udt_name
            .as_deref()
            .map(|t| t.to_ascii_lowercase().contains("unsigned"))
            .unwrap_or(false);

        match arrow_type_for(col)? {
            DataType::Int8 => {
                let mut b = Int8Builder::with_capacity(rows.len());
                for r in rows {
                    match int_cell(r, idx, unsigned)? {
                        Some(x) => b.append_value(x as i8),
                        None => b.append_null(),
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Int16 => {
                let mut b = Int16Builder::with_capacity(rows.len());
                for r in rows {
                    match int_cell(r, idx, unsigned)? {
                        Some(x) => b.append_value(x as i16),
                        None => b.append_null(),
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Int32 => {
                let mut b = Int32Builder::with_capacity(rows.len());
                for r in rows {
                    match int_cell(r, idx, unsigned)? {
                        Some(x) => b.append_value(x as i32),
                        None => b.append_null(),
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(rows.len());
                for r in rows {
                    match int_cell(r, idx, unsigned)? {
                        Some(x) => b.append_value(x as i64),
                        None => b.append_null(),
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Float32 => {
                let mut b = Float32Builder::with_capacity(rows.len());
                for r in rows {
                    let v: Option<f32> = r.try_get(idx)?;
                    match v {
                        Some(x) => b.append_value(x),
                        None => b.append_null(),
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(rows.len());
                for r in rows {
                    let v: Option<f64> = r.try_get(idx)?;
                    match v {
                        Some(x) => b.append_value(x),
                        None => b.append_null(),
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Decimal128(precision, scale) => {
                let mut b = Decimal128Builder::with_capacity(rows.len());
                for r in rows {
                    let v: Option<BigDecimal> = r.try_get(idx)?;
                    match v {
                        Some(d) => b.append_value(decimal_to_unscaled(d, scale as i64)?),
                        None => b.append_null(),
                    }
                }
                let arr = b
                    .finish()
                    .with_precision_and_scale(precision, scale)
                    .map_err(ExtractorError::Arrow)?;
                Ok(Arc::new(arr))
            }
            DataType::Boolean => {
                // `bit`: decode the raw bytes; any non-zero byte is `true`.
                let mut b = BooleanBuilder::with_capacity(rows.len());
                for r in rows {
                    let v: Option<Vec<u8>> = r.try_get(idx)?;
                    match v {
                        Some(bytes) => b.append_value(bytes.iter().any(|&x| x != 0)),
                        None => b.append_null(),
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Date32 => {
                let mut b = Date32Builder::with_capacity(rows.len());
                for r in rows {
                    let v: Option<NaiveDate> = r.try_get(idx)?;
                    match v {
                        Some(d) => b.append_value(days_from_epoch(d)),
                        None => b.append_null(),
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
                for r in rows {
                    let v: Option<NaiveDateTime> = r.try_get(idx)?;
                    match v {
                        Some(ts) => b.append_value(ts.and_utc().timestamp_micros()),
                        None => b.append_null(),
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Binary => {
                let mut b = BinaryBuilder::new();
                for r in rows {
                    let v: Option<Vec<u8>> = r.try_get(idx)?;
                    match v {
                        Some(bytes) => b.append_value(&bytes),
                        None => b.append_null(),
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            // Utf8 and any unmapped type: exact text.
            _ => {
                let mut b = StringBuilder::new();
                match data_type.as_str() {
                    "json" => {
                        for r in rows {
                            let v: Option<serde_json::Value> = r.try_get(idx)?;
                            match v {
                                Some(j) => b.append_value(j.to_string()),
                                None => b.append_null(),
                            }
                        }
                    }
                    _ => {
                        for r in rows {
                            let v: Option<String> = r.try_get(idx)?;
                            match v {
                                Some(s) => b.append_value(s),
                                None => b.append_null(),
                            }
                        }
                    }
                }
                Ok(Arc::new(b.finish()))
            }
        }
    }
}
