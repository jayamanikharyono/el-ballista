//! MySQL → Arrow typed decode.
//!
//! Builds the Arrow schema from [`TableMetadata`] via [`super::type_mapper::arrow_type_for`] and
//! decodes a chunk of rows (one `RecordBatch` worth — the extractor bounds it by `batch_size`)
//! column by column into the matching Arrow builder: one type decision per column, then a decode
//! loop over the rows.
//!
//! Integers are lossless: signed columns decode as `i64`, unsigned as `u64`, and are converted
//! into the mapped width with `TryFrom` — a value that does not fit is a typed
//! [`MysqlError::OutOfRange`], never a wrapping `as` cast. `bigint unsigned` → `UInt64`;
//! `BOOLEAN` (`tinyint(1)`) → `Boolean` (0/1 only, else [`MysqlError::NotBoolean`]); `year` →
//! `Int16`; `time` → `Duration(µs)` (signed, up to ±838:59:59); `bit(1)` → `Boolean`, `bit(n)` →
//! `UInt64`; `decimal` → `Decimal128`; `date`/`datetime`/`timestamp` → Arrow date/timestamp;
//! binary/blob → `Binary`; `json`/text → `Utf8`.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    DurationMicrosecondBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use sqlx::mysql::MySqlRow;
use sqlx::mysql::types::MySqlTime;
use sqlx::{MySql, Row};

use super::error::MysqlError;
use super::type_mapper::{arrow_type_for, is_boolean_tinyint, is_unsigned};
use crate::types::{ColumnMetadata, TableMetadata};

/// Days from the Unix epoch, for `Date32`.
fn days_from_epoch(d: NaiveDate, col: &ColumnMetadata) -> Result<i32, MysqlError> {
    let days = d
        .signed_duration_since(DateTime::UNIX_EPOCH.date_naive())
        .num_days();
    i32::try_from(days).map_err(|_| out_of_range(col, "Date32"))
}

/// `BigDecimal` → the unscaled `i128` Arrow `Decimal128` stores (`123.45` at scale 2 → `12345`).
fn decimal_to_unscaled(d: BigDecimal, scale: i8, col: &ColumnMetadata) -> Result<i128, MysqlError> {
    let unscaled = d * BigDecimal::from(10).powi(i64::from(scale));
    unscaled
        .to_i128()
        .ok_or_else(|| out_of_range(col, "Decimal128"))
}

fn out_of_range(col: &ColumnMetadata, target: &'static str) -> MysqlError {
    MysqlError::OutOfRange {
        column: col.column_name.clone(),
        target,
    }
}

fn decode_err(col: &ColumnMetadata, source: sqlx::Error) -> MysqlError {
    MysqlError::Decode {
        column: col.column_name.clone(),
        source,
    }
}

/// Read cell `idx` as `Option<T>` with the column name attached to any decode failure.
fn cell<'r, T>(r: &'r MySqlRow, idx: usize, col: &ColumnMetadata) -> Result<Option<T>, MysqlError>
where
    T: sqlx::Decode<'r, MySql> + sqlx::Type<MySql>,
{
    r.try_get::<Option<T>, _>(idx)
        .map_err(|e| decode_err(col, e))
}

/// Read integer cell `idx` at the widest sign-matching sqlx type (`u64` for unsigned columns,
/// else `i64`) and convert into `T` without wrapping.
fn int_cell<T>(
    r: &MySqlRow,
    idx: usize,
    col: &ColumnMetadata,
    unsigned: bool,
    target: &'static str,
) -> Result<Option<T>, MysqlError>
where
    T: TryFrom<i64> + TryFrom<u64>,
{
    if unsigned {
        cell::<u64>(r, idx, col)?
            .map(|v| T::try_from(v).map_err(|_| out_of_range(col, target)))
            .transpose()
    } else {
        cell::<i64>(r, idx, col)?
            .map(|v| T::try_from(v).map_err(|_| out_of_range(col, target)))
            .transpose()
    }
}

/// Signed microseconds of a MySQL `TIME` (a duration: `-838:59:59` ..= `838:59:59`).
fn time_to_micros(t: MySqlTime) -> i64 {
    // hours ≤ 838 (sqlx validates on decode) → every term is far inside i64.
    let secs = i64::from(t.hours()) * 3600 + i64::from(t.minutes()) * 60 + i64::from(t.seconds());
    let micros = secs * 1_000_000 + i64::from(t.microseconds());
    // `t.sign()`, not `t.is_negative()`: sqlx-mysql 0.9.0's `MySqlTime::is_negative` returns
    // `sign.is_positive()` (upstream bug), which would flip every value's sign.
    if t.sign().is_negative() {
        -micros
    } else {
        micros
    }
}

macro_rules! decode_ints {
    ($builder:ty, $native:ty, $target:literal, $rows:expr, $idx:expr, $col:expr, $unsigned:expr) => {{
        let mut b = <$builder>::with_capacity($rows.len());
        for r in $rows {
            b.append_option(int_cell::<$native>(r, $idx, $col, $unsigned, $target)?);
        }
        Ok(Arc::new(b.finish()) as ArrayRef)
    }};
}

/// Stateless MySQL row → Arrow decoder (see module docs for the type contract).
pub struct MysqlRowAdapter;

impl MysqlRowAdapter {
    /// Arrow schema mirroring the table's columns and their mapped Arrow types.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::connector::mysql::row_adapter::MysqlRowAdapter;
    /// use rust_ballista_extraction_layer::types::{ColumnMetadata, TableMetadata};
    ///
    /// let table = TableMetadata {
    ///     schema_name: "app".into(),
    ///     table_name: "t".into(),
    ///     columns: vec![ColumnMetadata {
    ///         column_name: "ok".into(),
    ///         data_type: "tinyint".into(),
    ///         is_nullable: false,
    ///         numeric_precision: None,
    ///         numeric_scale: None,
    ///         udt_name: Some("tinyint(1)".into()),
    ///         collation_name: None,
    ///     }],
    /// };
    /// let schema = MysqlRowAdapter::build_arrow_schema(&table).unwrap();
    /// assert_eq!(schema.field(0).data_type(), &arrow::datatypes::DataType::Boolean);
    /// ```
    pub fn build_arrow_schema(table: &TableMetadata) -> Result<Schema, MysqlError> {
        let mut fields: Vec<Field> = Vec::with_capacity(table.columns.len());
        for c in &table.columns {
            fields.push(Field::new(
                &c.column_name,
                arrow_type_for(c)?,
                c.is_nullable,
            ));
        }
        Ok(Schema::new(fields))
    }

    /// Decode column `idx` from every row of `rows` into a typed Arrow array.
    ///
    /// `rows` is one batch worth of rows (the extractor bounds it by `batch_size`); the result
    /// has exactly `rows.len()` elements.
    pub(crate) fn decode_column(
        rows: &[MySqlRow],
        idx: usize,
        col: &ColumnMetadata,
    ) -> Result<ArrayRef, MysqlError> {
        let data_type = col.data_type.to_ascii_lowercase();
        let unsigned = is_unsigned(col);

        match arrow_type_for(col)? {
            DataType::Int8 => decode_ints!(Int8Builder, i8, "Int8", rows, idx, col, unsigned),
            DataType::Int16 if data_type == "year" => {
                // YEAR (1901..=2155, or 0) is sent as an unsigned 2-byte value.
                let mut b = Int16Builder::with_capacity(rows.len());
                for r in rows {
                    let v = cell::<u16>(r, idx, col)?
                        .map(|y| i16::try_from(y).map_err(|_| out_of_range(col, "Int16")))
                        .transpose()?;
                    b.append_option(v);
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Int16 => decode_ints!(Int16Builder, i16, "Int16", rows, idx, col, unsigned),
            DataType::Int32 => decode_ints!(Int32Builder, i32, "Int32", rows, idx, col, unsigned),
            DataType::Int64 => decode_ints!(Int64Builder, i64, "Int64", rows, idx, col, unsigned),
            DataType::UInt64 if data_type == "bit" => {
                // BIT(n) arrives as big-endian raw bytes; sqlx's unsigned decoder folds them into
                // a u64 (n ≤ 64). The catalog already proved the column is BIT, so the
                // wire-flag compatibility check is skipped rather than relying on UNSIGNED.
                let mut b = UInt64Builder::with_capacity(rows.len());
                for r in rows {
                    let v: Option<u64> =
                        r.try_get_unchecked(idx).map_err(|e| decode_err(col, e))?;
                    b.append_option(v);
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::UInt64 => {
                let mut b = UInt64Builder::with_capacity(rows.len());
                for r in rows {
                    b.append_option(cell::<u64>(r, idx, col)?);
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Float32 => {
                let mut b = Float32Builder::with_capacity(rows.len());
                for r in rows {
                    b.append_option(cell::<f32>(r, idx, col)?);
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(rows.len());
                for r in rows {
                    b.append_option(cell::<f64>(r, idx, col)?);
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Decimal128(precision, scale) => {
                let mut b = Decimal128Builder::with_capacity(rows.len());
                for r in rows {
                    let v = cell::<BigDecimal>(r, idx, col)?
                        .map(|d| decimal_to_unscaled(d, scale, col))
                        .transpose()?;
                    b.append_option(v);
                }
                let arr = b.finish().with_precision_and_scale(precision, scale)?;
                Ok(Arc::new(arr))
            }
            DataType::Boolean if is_boolean_tinyint(col) => {
                // BOOLEAN is `tinyint(1)`: accept exactly 0/1 so no value is lost.
                let mut b = BooleanBuilder::with_capacity(rows.len());
                for r in rows {
                    let v = match cell::<i64>(r, idx, col)? {
                        None => None,
                        Some(0) => Some(false),
                        Some(1) => Some(true),
                        Some(_) => {
                            return Err(MysqlError::NotBoolean {
                                column: col.column_name.clone(),
                            });
                        }
                    };
                    b.append_option(v);
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Boolean => {
                // `BIT(1)` (and a bare `bool` DATA_TYPE, should a server report one).
                let mut b = BooleanBuilder::with_capacity(rows.len());
                for r in rows {
                    b.append_option(cell::<bool>(r, idx, col)?);
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Date32 => {
                let mut b = Date32Builder::with_capacity(rows.len());
                for r in rows {
                    let v = cell::<NaiveDate>(r, idx, col)?
                        .map(|d| days_from_epoch(d, col))
                        .transpose()?;
                    b.append_option(v);
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                // `DATETIME` decodes naive; `TIMESTAMP` (TZ-converting) decodes TZ-aware.
                let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
                for r in rows {
                    let v: Option<NaiveDateTime> = r.try_get(idx).or_else(|_| {
                        r.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(idx)
                            .map(|o| o.map(|ts| ts.naive_utc()))
                            .map_err(|e| decode_err(col, e))
                    })?;
                    b.append_option(v.map(|ts| ts.and_utc().timestamp_micros()));
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                let mut b = DurationMicrosecondBuilder::with_capacity(rows.len());
                for r in rows {
                    b.append_option(cell::<MySqlTime>(r, idx, col)?.map(time_to_micros));
                }
                Ok(Arc::new(b.finish()))
            }
            DataType::Binary => {
                let mut b = BinaryBuilder::new();
                for r in rows {
                    b.append_option(cell::<Vec<u8>>(r, idx, col)?);
                }
                Ok(Arc::new(b.finish()))
            }
            // Utf8 and any unmapped type: exact text.
            _ => {
                let mut b = StringBuilder::new();
                if data_type == "json" {
                    for r in rows {
                        let v = cell::<serde_json::Value>(r, idx, col)?;
                        b.append_option(v.map(|j| j.to_string()));
                    }
                } else {
                    for r in rows {
                        b.append_option(cell::<String>(r, idx, col)?);
                    }
                }
                Ok(Arc::new(b.finish()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::mysql::types::MySqlTimeSign;

    fn col(name: &str) -> ColumnMetadata {
        ColumnMetadata {
            column_name: name.to_string(),
            data_type: "int".to_string(),
            is_nullable: true,
            numeric_precision: None,
            numeric_scale: None,
            udt_name: None,
            collation_name: None,
        }
    }

    #[test]
    fn time_is_a_signed_duration() {
        let t = MySqlTime::new(MySqlTimeSign::Positive, 838, 59, 59, 0).unwrap();
        assert_eq!(time_to_micros(t), 3_020_399_000_000);
        let t = MySqlTime::new(MySqlTimeSign::Negative, 12, 34, 56, 500_000).unwrap();
        assert_eq!(time_to_micros(t), -45_296_500_000);
        assert_eq!(time_to_micros(MySqlTime::ZERO), 0);
    }

    #[test]
    fn decimal_rescales_exactly() {
        let d: BigDecimal = "123.45".parse().unwrap();
        assert_eq!(decimal_to_unscaled(d, 2, &col("a")).unwrap(), 12345);
        let d: BigDecimal = "-7.5".parse().unwrap();
        assert_eq!(decimal_to_unscaled(d, 2, &col("a")).unwrap(), -750);
    }

    #[test]
    fn dates_count_days_from_epoch() {
        let d = NaiveDate::from_ymd_opt(1970, 1, 2).unwrap();
        assert_eq!(days_from_epoch(d, &col("d")).unwrap(), 1);
        let d = NaiveDate::from_ymd_opt(1969, 12, 31).unwrap();
        assert_eq!(days_from_epoch(d, &col("d")).unwrap(), -1);
    }

    /// The conversions `int_cell` relies on: in-range values survive, out-of-range values are
    /// rejected (the old `as` casts turned `u32::MAX` into `-1`).
    #[test]
    fn integer_narrowing_never_wraps() {
        assert_eq!(i64::try_from(u64::from(u32::MAX)).ok(), Some(4_294_967_295));
        assert_eq!(i16::try_from(u64::from(u8::MAX)).ok(), Some(255));
        assert_eq!(i32::try_from(u64::from(u16::MAX)).ok(), Some(65_535));
        assert_eq!(i32::try_from(16_777_215u64).ok(), Some(16_777_215)); // mediumint unsigned max
        assert!(i8::try_from(200u64).is_err());
        assert!(i64::try_from(u64::MAX).is_err());
        let e = out_of_range(&col("u"), "Int32");
        assert_eq!(
            e.to_string(),
            "value in MySQL column `u` does not fit Arrow Int32"
        );
    }
}
