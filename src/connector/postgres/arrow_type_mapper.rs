//! PostgreSQL → Arrow type mapping.
//! extractor/postgres/arrow_type_mapper.rs
//! Maps PostgreSQL column metadata to Arrow [`DataType`] values.

use std::sync::Arc;
use arrow::datatypes::{DataType, Field, TimeUnit};

use crate::connector::errors::ExtractorError;
use crate::types::ColumnMetadata;

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

            "text" | "character varying" | "character" => {
                Ok(DataType::Utf8)
            }

            "date" => Ok(DataType::Date32),

            "timestamp without time zone" => {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }

            "timestamp with time zone" => {
                Ok(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some("UTC".into()),
                ))
            }

            "bytea" => Ok(DataType::Binary),

            "numeric" => {
                let precision =
                    column.numeric_precision.unwrap_or(38) as u8;
                let scale =
                    column.numeric_scale.unwrap_or(10) as i8;

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
            Some("_text") => Ok(DataType::List(Arc::new(
                Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ),
            ))),

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
        assert_eq!(ArrowTypeMapper::map(&col("smallint", None)).unwrap(), DataType::Int16);
        assert_eq!(ArrowTypeMapper::map(&col("integer", None)).unwrap(), DataType::Int32);
        assert_eq!(ArrowTypeMapper::map(&col("bigint", None)).unwrap(), DataType::Int64);
        assert_eq!(ArrowTypeMapper::map(&col("real", None)).unwrap(), DataType::Float32);
        assert_eq!(ArrowTypeMapper::map(&col("double precision", None)).unwrap(), DataType::Float64);
        assert_eq!(ArrowTypeMapper::map(&col("boolean", None)).unwrap(), DataType::Boolean);
        assert_eq!(ArrowTypeMapper::map(&col("text", None)).unwrap(), DataType::Utf8);
        assert_eq!(ArrowTypeMapper::map(&col("date", None)).unwrap(), DataType::Date32);
        assert_eq!(ArrowTypeMapper::map(&col("bytea", None)).unwrap(), DataType::Binary);
        assert_eq!(ArrowTypeMapper::map(&col("json", None)).unwrap(), DataType::Utf8);
        assert_eq!(ArrowTypeMapper::map(&col("jsonb", None)).unwrap(), DataType::Utf8);
        assert_eq!(ArrowTypeMapper::map(&col("uuid", None)).unwrap(), DataType::Utf8);
        assert_eq!(ArrowTypeMapper::map(&col("numeric", None)).unwrap(), DataType::Decimal128(20, 4));
        assert_eq!(
            ArrowTypeMapper::map(&col("ARRAY", Some("_text"))).unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
        assert!(ArrowTypeMapper::map(&col("unsupported_type", None)).is_err());
        assert!(ArrowTypeMapper::map(&col("ARRAY", Some("_int4"))).is_err());
    }
}