//! Postgres Row Adapter
//! extractor/postgres/row_adapter.rs
//! This module is used to convert the PostgreSQL rows to the Arrow record batch.

use arrow::array::{ArrayRef, Int16Array, Int32Array, Int64Array, Float32Array, Float64Array, BooleanArray, StringArray, Decimal128Array, TimestampMicrosecondArray, Date32Array};
use arrow::array::{ListBuilder, StringBuilder};
use arrow::datatypes::{Schema, Field};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use std::sync::Arc;
use sqlx::{
    postgres::PgRow,
    Row,
};
use bigdecimal::{BigDecimal,ToPrimitive};

use crate::extractor::errors::ExtractorError;
use crate::types::{ColumnMetadata, TableMetadata};
use crate::extractor::postgres::arrow_type_mapper::ArrowTypeMapper;
pub struct PostgresRowAdapter;

impl PostgresRowAdapter {
    pub fn build_array(
        rows: &[PgRow],
        column: &ColumnMetadata,
    ) -> Result<ArrayRef, ExtractorError> {
        match column.data_type.as_str() {
            "smallint" => {
                let values: Vec<Option<i16>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get(column.column_name.as_str())
                    })
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(
                    Int16Array::from(values)
                ))
            }

            "integer" => {
                let values: Vec<Option<i32>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get(column.column_name.as_str())
                    })
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(
                    Int32Array::from(values)
                ))
            }


            "bigint" => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get(column.column_name.as_str())
                    })
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(
                    Int64Array::from(values)
                ))
            }


            "real" => {
                let values: Vec<Option<f32>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get(column.column_name.as_str())
                    })
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(
                    Float32Array::from(values)
                ))
            }


            "double precision" => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get(column.column_name.as_str())
                    })
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(
                    Float64Array::from(values)
                ))
            }

            "numeric" => {
                let precision = column
                    .numeric_precision
                    .unwrap_or(38) as u8;

                let scale = column
                    .numeric_scale
                    .unwrap_or(10) as i8;

                let values: Vec<Option<i128>> = rows
                    .iter()
                    .map(|row| {
                        let value: Option<BigDecimal> =
                            row.try_get(column.column_name.as_str())?;

                        value
                            .map(|decimal| {
                                decimal
                                    .with_scale(scale as i64)
                                    .to_i128()
                                    .ok_or_else(|| {
                                        sqlx::Error::Decode(
                                            format!(
                                                "numeric value cannot fit into i128: {}",
                                                decimal
                                            ).into()
                                        )
                                    })
                            })
                            .transpose()
                    })
                    .collect::<Result<_, _>>()?;

                let array = Decimal128Array::from(values)
                    .with_precision_and_scale(precision, scale)?;

                Ok(Arc::new(array))
            }


            "boolean" => {
                let values: Vec<Option<bool>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get(column.column_name.as_str())
                    })
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(
                    BooleanArray::from(values)
                ))
            }

            "text" |
            "character varying" |
            "character"  => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get(column.column_name.as_str())
                    })
                    .collect::<Result<_, _>>()?;

                Ok(Arc::new(
                    StringArray::from(values)
                ))
            }

            "USER-DEFINED" => {
                let udt_name = column
                    .udt_name
                    .as_deref()
                    .unwrap_or("unknown");

                log::debug!(
                    "Decoding user-defined PostgreSQL type '{}' as String",
                    udt_name
                );

                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get(column.column_name.as_str())
                    })
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

                let array = TimestampMicrosecondArray::from(values)
                    .with_timezone("UTC");

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
                        let value : Option<NaiveDate> = row.try_get(column.column_name.as_str())?;

                        Ok(value.map(|d| {
                            (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                                .num_days() as i32
                        }))
                    })
                    .collect::<Result<_, sqlx::Error>>()?;

                Ok(Arc::new(Date32Array::from(values)))
            }

            "uuid" => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| {
                        let value: Option<uuid::Uuid> =
                            row.try_get(column.column_name.as_str())?;

                        Ok(value.map(|u| u.to_string()))
                    })
                    .collect::<Result<_, sqlx::Error>>()?;

                Ok(Arc::new(StringArray::from(values)))
            }

            _ => {
                Err(
                    ExtractorError::UnsupportedType(
                        column.data_type.clone()
                    )
                )
            }
        }

    }

    pub fn build_text_array(
        rows: &[PgRow],
        column: &ColumnMetadata,
    ) -> Result<ArrayRef, ExtractorError> {
        let mut builder = ListBuilder::new(StringBuilder::new());

        for row in rows {
            let value: Option<Vec<Option<String>>> =
                row.try_get(column.column_name.as_str())?;

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

        Ok(Arc::new(builder.finish()))
    }

    pub fn rows_to_record_batch(
        rows: &[PgRow],
        table_metadata: &TableMetadata,
        arrow_schema: Arc<Schema>,
    ) -> Result<RecordBatch, ExtractorError> {

        let arrays = table_metadata.columns
            .iter()
            .map(|column| {
                Self::build_array(rows, column)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(
            RecordBatch::try_new(
                arrow_schema,
                arrays,
            )?
        )
    }


    pub fn build_arrow_schema(
        table_metadata: &TableMetadata
    ) -> Arc<Schema> {
            let fields : Vec<Field> = table_metadata.columns
                .iter()
                .map(|column| {
                    Field::new(
                        &column.column_name,
                        ArrowTypeMapper::map(column),
                        column.is_nullable
                    )
                })
                .collect();
    
            Arc::new(Schema::new(fields))
    
    }
}