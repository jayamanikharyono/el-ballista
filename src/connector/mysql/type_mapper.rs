//! MySQL type -> Arrow type mapping — the single seam [`super::row_adapter`] decodes through.
//!
//! The decision is made from the catalog `DATA_TYPE` plus `COLUMN_TYPE` (carried in
//! [`ColumnMetadata::udt_name`] by [`super::schema_reader`]), because `DATA_TYPE` alone cannot
//! tell `tinyint(1)` (BOOLEAN) from `tinyint`, `bit(1)` from `bit(8)`, or signed from unsigned.
//!
//! | MySQL                                   | Arrow                     |
//! |-----------------------------------------|---------------------------|
//! | `tinyint(1)` (BOOL/BOOLEAN)             | `Boolean` (0/1 only)      |
//! | `tinyint` / `smallint` / `mediumint`/`int` / `bigint` | `Int8`/`Int16`/`Int32`/`Int32`/`Int64` |
//! | `tinyint`/`smallint`/`mediumint`/`int` `unsigned` | `Int16`/`Int32`/`Int32`/`Int64` (next wider signed) |
//! | `bigint unsigned`                       | `UInt64`                  |
//! | `year`                                  | `Int16`                   |
//! | `float` / `double`                      | `Float32` / `Float64`     |
//! | `decimal(p,s)` (p ≤ 38)                 | `Decimal128(p, s)`        |
//! | `bit(1)` / `bit(n>1)`                   | `Boolean` / `UInt64`      |
//! | `date`                                  | `Date32`                  |
//! | `datetime` / `timestamp`                | `Timestamp(µs, None)`     |
//! | `time` (±838:59:59, a duration)         | `Duration(µs)`            |
//! | binary / blob family                    | `Binary`                  |
//! | text family, `enum`, `set`, `json`, other | `Utf8`                  |

use arrow::datatypes::{DataType, TimeUnit};

use crate::connector::errors::ExtractorError;
use crate::types::ColumnMetadata;

/// `COLUMN_TYPE` lower-cased (e.g. `int(10) unsigned`, `tinyint(1)`), or `""` when unknown.
fn column_type(col: &ColumnMetadata) -> String {
    col.udt_name
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

/// Whether the column is declared `UNSIGNED` (from `COLUMN_TYPE`).
pub(crate) fn is_unsigned(col: &ColumnMetadata) -> bool {
    column_type(col)
        .split_whitespace()
        .any(|word| word == "unsigned")
}

/// Whether the column is MySQL's `BOOL`/`BOOLEAN` alias, i.e. `COLUMN_TYPE` is exactly
/// `tinyint(1)` (signed; `tinyint(1) unsigned` stays an integer).
pub(crate) fn is_boolean_tinyint(col: &ColumnMetadata) -> bool {
    column_type(col) == "tinyint(1)"
}

/// Typed Arrow mapping for a cataloged column (see the module table).
///
/// # Examples
///
/// ```
/// use arrow::datatypes::DataType;
/// use el_ballista::connector::mysql::type_mapper::arrow_type_for;
/// use el_ballista::types::ColumnMetadata;
///
/// let col = ColumnMetadata {
///     column_name: "n".into(),
///     data_type: "bigint".into(),
///     is_nullable: true,
///     numeric_precision: None,
///     numeric_scale: None,
///     udt_name: Some("bigint unsigned".into()),
///     collation_name: None,
/// };
/// assert_eq!(arrow_type_for(&col).unwrap(), DataType::UInt64);
/// ```
pub fn arrow_type_for(col: &ColumnMetadata) -> Result<DataType, ExtractorError> {
    let unsigned = is_unsigned(col);
    Ok(match col.data_type.to_ascii_lowercase().as_str() {
        "tinyint" if is_boolean_tinyint(col) => DataType::Boolean,
        "tinyint" if unsigned => DataType::Int16,
        "tinyint" => DataType::Int8,
        "smallint" if unsigned => DataType::Int32,
        "smallint" => DataType::Int16,
        "mediumint" => DataType::Int32,
        "int" | "integer" if unsigned => DataType::Int64,
        "int" | "integer" => DataType::Int32,
        "bigint" if unsigned => DataType::UInt64,
        "bigint" => DataType::Int64,
        "year" => DataType::Int16,
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
        "bit" if bit_width(col) <= 1 => DataType::Boolean,
        "bit" => DataType::UInt64,
        "bool" | "boolean" => DataType::Boolean,
        "date" => DataType::Date32,
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "time" => DataType::Duration(TimeUnit::Microsecond),
        "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set"
        | "json" => DataType::Utf8,
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => {
            DataType::Binary
        }
        _ => DataType::Utf8,
    })
}

/// `n` from `bit(n)`; bare `bit` is `bit(1)` in MySQL.
fn bit_width(col: &ColumnMetadata) -> u32 {
    let type_text = column_type(col);
    type_text
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')'))
        .and_then(|(n, _)| n.trim().parse().ok())
        .unwrap_or(1)
}

/// `(precision, scale)` from `COLUMN_TYPE` (`decimal(12,2)` → `(12, 2)`); bare `decimal` →
/// `(10, 0)` (MySQL default). Unparseable → `(10, 0)` so decode still proceeds as text-scale
/// decimal rather than failing schema build.
fn decimal_precision_scale(col: &ColumnMetadata) -> (i64, i64) {
    if let Some(p) = col.numeric_precision {
        return (i64::from(p), i64::from(col.numeric_scale.unwrap_or(0)));
    }
    let type_text = column_type(col);
    let inner = type_text
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')'))
        .map(|(inner, _)| inner)
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

    fn col(data_type: &str, column_type: &str) -> ColumnMetadata {
        ColumnMetadata {
            column_name: "c".to_string(),
            data_type: data_type.to_string(),
            is_nullable: true,
            numeric_precision: None,
            numeric_scale: None,
            udt_name: Some(column_type.to_string()),
            collation_name: None,
        }
    }

    fn ty(data_type: &str, column_type: &str) -> DataType {
        arrow_type_for(&col(data_type, column_type)).unwrap()
    }

    #[test]
    fn signed_integers_keep_width() {
        assert_eq!(ty("tinyint", "tinyint"), DataType::Int8);
        assert_eq!(ty("tinyint", "tinyint(4)"), DataType::Int8);
        assert_eq!(ty("smallint", "smallint"), DataType::Int16);
        assert_eq!(ty("mediumint", "mediumint"), DataType::Int32);
        assert_eq!(ty("int", "int"), DataType::Int32);
        assert_eq!(ty("bigint", "bigint"), DataType::Int64);
    }

    #[test]
    fn unsigned_integers_widen_losslessly() {
        assert_eq!(ty("tinyint", "tinyint unsigned"), DataType::Int16);
        assert_eq!(ty("smallint", "smallint unsigned"), DataType::Int32);
        assert_eq!(ty("mediumint", "mediumint(8) unsigned"), DataType::Int32);
        assert_eq!(ty("int", "int(10) unsigned zerofill"), DataType::Int64);
        assert_eq!(ty("bigint", "bigint unsigned"), DataType::UInt64);
    }

    #[test]
    fn boolean_is_tinyint_1_only() {
        assert_eq!(ty("tinyint", "tinyint(1)"), DataType::Boolean);
        assert_eq!(ty("tinyint", "TINYINT(1)"), DataType::Boolean);
        // Unsigned tinyint(1) is not the BOOLEAN alias.
        assert_eq!(ty("tinyint", "tinyint(1) unsigned"), DataType::Int16);
        assert_eq!(ty("tinyint", "tinyint(2)"), DataType::Int8);
    }

    #[test]
    fn year_time_bit() {
        assert_eq!(ty("year", "year"), DataType::Int16);
        assert_eq!(
            ty("time", "time(6)"),
            DataType::Duration(TimeUnit::Microsecond)
        );
        assert_eq!(ty("bit", "bit(1)"), DataType::Boolean);
        assert_eq!(ty("bit", "bit"), DataType::Boolean);
        assert_eq!(ty("bit", "bit(8)"), DataType::UInt64);
    }

    #[test]
    fn decimals_floats_temporal_text() {
        assert_eq!(ty("decimal", "decimal(12,2)"), DataType::Decimal128(12, 2));
        assert_eq!(
            ty("decimal", "decimal(10,2) unsigned"),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(ty("decimal", "decimal"), DataType::Decimal128(10, 0));
        assert!(arrow_type_for(&col("decimal", "decimal(65,2)")).is_err());
        assert_eq!(ty("float", "float"), DataType::Float32);
        assert_eq!(ty("double", "double"), DataType::Float64);
        assert_eq!(ty("date", "date"), DataType::Date32);
        assert_eq!(
            ty("datetime", "datetime(6)"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(ty("json", "json"), DataType::Utf8);
        assert_eq!(ty("blob", "blob"), DataType::Binary);
        assert_eq!(ty("geometry", "geometry"), DataType::Utf8);
    }
}
