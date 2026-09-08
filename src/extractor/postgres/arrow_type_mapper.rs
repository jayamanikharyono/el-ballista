//! PostgreSQL → Arrow type mapping.
//! extractor/postgres/arrow_type_mapper.rs
//! Maps PostgreSQL column metadata to Arrow [`DataType`] values.

use std::sync::Arc;
use arrow::datatypes::{DataType, Field, TimeUnit};

use crate::types::ColumnMetadata;

pub struct ArrowTypeMapper;

impl ArrowTypeMapper {
    pub fn map(column: &ColumnMetadata) -> DataType {
        match column.data_type.as_str() {
            "smallint" => DataType::Int16,
            "integer" => DataType::Int32,
            "bigint" => DataType::Int64,

            "real" => DataType::Float32,
            "double precision" => DataType::Float64,

            "boolean" => DataType::Boolean,

            "text" | "character varying" | "character" => {
                DataType::Utf8
            }

            "date" => DataType::Date32,

            "timestamp without time zone" => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }

            "timestamp with time zone" => {
                DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some("UTC".into()),
                )
            }

            "bytea" => DataType::Binary,

            "numeric" => {
                let precision =
                    column.numeric_precision.unwrap_or(38) as u8;
                let scale =
                    column.numeric_scale.unwrap_or(10) as i8;

                DataType::Decimal128(precision, scale)
            }

            // PostgreSQL ENUM / custom types.
            // The extractor casts these values to TEXT before decoding.
            "USER-DEFINED" => DataType::Utf8,

            "ARRAY" => Self::map_array(column),

            _ => DataType::Utf8,
        }
    }

    fn map_array(column_metadata: &ColumnMetadata) -> DataType {
        match column_metadata.udt_name.as_deref() {
            Some ("_text") => DataType::List(Arc::new(
                Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ),
            )),

            _ => DataType::Utf8,
        }
    }
}