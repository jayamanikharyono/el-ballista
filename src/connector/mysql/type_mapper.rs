//! MySQL type -> Arrow type mapping.
//!
//! Two seams: [`mysql_type_to_arrow`] is the coarse catalog-name mapping (integers widen to
//! `Int64`, decimals stay `Utf8`); [`arrow_type_for`] is the width-preserving mapping the
//! extractor decodes through ([`super::row_adapter`]) — integer widths, `Float32` vs `Float64`,
//! `Decimal128(p, s)`, `Boolean`, `Date32`/timestamps, `Binary`, and `Utf8` otherwise.

use arrow::datatypes::{DataType, TimeUnit};

use crate::connector::errors::ExtractorError;
use crate::types::ColumnMetadata;

/// Map a MySQL `DATA_TYPE` (catalog name, e.g. `bigint`, `varchar`, `datetime`) to the Arrow
/// [`DataType`] we intend to materialize it as. Unknown types fall back to `Utf8`.
pub fn mysql_type_to_arrow(data_type: &str) -> DataType {
    match data_type.to_ascii_lowercase().as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => {
            DataType::Int64
        }
        "float" | "double" | "real" => DataType::Float64,
        // Keep exact decimals as text until a decimal decode path exists.
        "decimal" | "numeric" => DataType::Utf8,
        "bit" | "bool" | "boolean" => DataType::Boolean,
        "date" => DataType::Date32,
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set"
        | "json" => DataType::Utf8,
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => {
            DataType::Binary
        }
        _ => DataType::Utf8,
    }
}

/// Typed Arrow mapping for a cataloged column — the seam `row_adapter` decodes through.
///
/// Widths are preserved (tinyint → `Int8`, …, bigint → `Int64`; float → `Float32`,
/// double → `Float64`) so values round-trip without widening. `decimal`/`numeric` map to
/// `Decimal128` with precision/scale parsed from the column type (`decimal(12,2)`); the
/// schema reader leaves `numeric_precision`/`numeric_scale` unset, so `COLUMN_TYPE`
/// (carried in `udt_name`) is the source. Unknown or unparseable shapes fall back to `Utf8`.
pub fn arrow_type_for(col: &ColumnMetadata) -> Result<DataType, ExtractorError> {
    Ok(match col.data_type.to_ascii_lowercase().as_str() {
        "tinyint" => DataType::Int8,
        "smallint" | "year" => DataType::Int16,
        "mediumint" | "int" | "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" | "real" => DataType::Float64,
        "decimal" | "numeric" => {
            let (precision, scale) = decimal_precision_scale(col);
            if !(1..=38).contains(&precision) || !(0..=precision).contains(&scale) {
                return Err(ExtractorError::UnsupportedType(format!(
                    "decimal({}, {}) out of Decimal128 range for {}",
                    precision, scale, col.column_name
                )));
            }
            // Range-checked above (1..=38 / 0..=precision), so these conversions are
            // infallible; `try_from` keeps it explicit instead of `as` casts.
            let precision = u8::try_from(precision).map_err(|e| {
                ExtractorError::Internal(format!("decimal precision out of range: {e}"))
            })?;
            let scale = i8::try_from(scale).map_err(|e| {
                ExtractorError::Internal(format!("decimal scale out of range: {e}"))
            })?;
            DataType::Decimal128(precision, scale)
        }
        "bit" | "bool" | "boolean" => DataType::Boolean,
        "date" => DataType::Date32,
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set"
        | "json" => DataType::Utf8,
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => {
            DataType::Binary
        }
        _ => DataType::Utf8,
    })
}

/// `(precision, scale)` from `COLUMN_TYPE` (`decimal(12,2)` → `(12, 2)`); bare `decimal` →
/// `(10, 0)` (MySQL default). Unparseable → `(10, 0)` so decode still proceeds as text-scale
/// decimal rather than failing schema build.
fn decimal_precision_scale(col: &ColumnMetadata) -> (i64, i64) {
    if let Some(p) = col.numeric_precision {
        return (p as i64, col.numeric_scale.unwrap_or(0) as i64);
    }
    let type_text = col.udt_name.as_deref().unwrap_or("");
    let inner = type_text
        .split_once('(')
        .and_then(|(_, rest)| rest.strip_suffix(')'))
        .unwrap_or("");
    let mut parts = inner.split(',');
    let precision = parts
        .next()
        .and_then(|p| p.trim().parse().ok())
        .unwrap_or(10);
    let scale = parts
        .next()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    (precision, scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_common_types() {
        assert_eq!(mysql_type_to_arrow("bigint"), DataType::Int64);
        assert_eq!(mysql_type_to_arrow("INT"), DataType::Int64);
        assert_eq!(mysql_type_to_arrow("varchar"), DataType::Utf8);
        assert_eq!(mysql_type_to_arrow("double"), DataType::Float64);
        assert_eq!(
            mysql_type_to_arrow("datetime"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(mysql_type_to_arrow("date"), DataType::Date32);
    }

    #[test]
    fn unknown_falls_back_to_utf8() {
        assert_eq!(mysql_type_to_arrow("geometry"), DataType::Utf8);
    }
}
