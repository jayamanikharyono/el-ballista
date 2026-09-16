//! PostgreSQL → Arrow type mapping.
//! extractor/postgres/arrow_type_mapper.rs
//! Maps PostgreSQL column metadata to Arrow [`DataType`] values.

use arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

use bigdecimal::{BigDecimal, ToPrimitive};

use crate::connector::errors::ExtractorError;
use crate::types::ColumnMetadata;

/// Convert a `BigDecimal` to the unscaled `i128` Arrow `Decimal128` expects.
/// `123.45` at scale 2 → `12345`, `1200` at scale -2 → `12`.
/// Text is captured before the move so error messages still show the original value.
pub fn decimal_to_unscaled(decimal: BigDecimal, scale: i64) -> Result<i128, ExtractorError> {
    let text = decimal.to_string();
    let unscaled = decimal * BigDecimal::from(10).powi(scale);
    unscaled.to_i128().ok_or_else(|| {
        ExtractorError::Internal(format!("numeric value cannot fit into i128: {}", text))
    })
}

pub struct ArrowTypeMapper;

impl ArrowTypeMapper {
    pub fn map(column: &ColumnMetadata) -> Result<DataType, ExtractorError> {
        match column.data_type.as_str() {
            "smallint" => Ok(DataType::Int16),
            "integer" => Ok(DataType::Int32),
            "bigint" => Ok(DataType::Int64),

            "real" => Ok(DataType::Float32),
            "double precision" => Ok(DataType::Float64),

            "boolean" => Ok(DataType::Boolean),

            "text" | "character varying" | "character" => Ok(DataType::Utf8),

            "date" => Ok(DataType::Date32),

            "timestamp without time zone" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),

            "timestamp with time zone" => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )),

            "bytea" => Ok(DataType::Binary),

            "numeric" => {
                let precision_raw = column.numeric_precision.unwrap_or(38);
                let scale_raw = column.numeric_scale.unwrap_or(10);
                if !(1..=38).contains(&precision_raw) {
                    return Err(ExtractorError::Internal(format!(
                        "numeric precision {} out of Decimal128 range 1..38 for column '{}'",
                        precision_raw, column.column_name
                    )));
                }
                if !(-127..=127).contains(&scale_raw) {
                    return Err(ExtractorError::Internal(format!(
                        "numeric scale {} out of i8 range for column '{}'",
                        scale_raw, column.column_name
                    )));
                }
                // Decimal128 precision is u8 but Postgres allows >38; clamp report above.
                let precision = precision_raw as u8;
                let scale = scale_raw as i8;
                Ok(DataType::Decimal128(precision, scale))
            }

            "json" | "jsonb" => Ok(DataType::Utf8),
            "uuid" => Ok(DataType::Utf8),

            // PostgreSQL ENUM / custom types.
            // The extractor casts these values to TEXT before decoding.
            "USER-DEFINED" => Ok(DataType::Utf8),

            "ARRAY" => Self::map_array(column),

            other => Err(ExtractorError::UnsupportedType(other.to_string())),
        }
    }

    fn map_array(column_metadata: &ColumnMetadata) -> Result<DataType, ExtractorError> {
        match column_metadata.udt_name.as_deref() {
            Some("_text") => Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))),

            other => Err(ExtractorError::UnsupportedType(format!(
                "array element type {:?} for column '{}' (only text[] arrays are supported)",
                other, column_metadata.column_name
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(data_type: &str, udt_name: Option<&str>) -> ColumnMetadata {
        ColumnMetadata {
            column_name: "test_col".to_string(),
            data_type: data_type.to_string(),
            is_nullable: true,
            numeric_precision: Some(20),
            numeric_scale: Some(4),
            udt_name: udt_name.map(String::from),
            collation_name: None,
        }
    }

    #[test]
    fn test_mappings() {
        assert_eq!(
            ArrowTypeMapper::map(&col("smallint", None)).unwrap(),
            DataType::Int16
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("integer", None)).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("bigint", None)).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("real", None)).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("double precision", None)).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("boolean", None)).unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("text", None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("date", None)).unwrap(),
            DataType::Date32
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("bytea", None)).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("json", None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("jsonb", None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("uuid", None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("numeric", None)).unwrap(),
            DataType::Decimal128(20, 4)
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("ARRAY", Some("_text"))).unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
        assert!(ArrowTypeMapper::map(&col("unsupported_type", None)).is_err());
        assert!(ArrowTypeMapper::map(&col("ARRAY", Some("_int4"))).is_err());
    }
}
