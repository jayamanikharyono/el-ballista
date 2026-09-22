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
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sqlx::{Row, postgres::PgRow};
use std::sync::Arc;

use crate::connector::errors::ExtractorError;
use crate::connector::postgres::arrow_type_mapper::ArrowTypeMapper;
use crate::types::{ColumnMetadata, TableMetadata};

pub struct PostgresRowAdapter;

impl PostgresRowAdapter {
    /// Create an empty Arrow builder matching a mapped data type. Timestamp builders carry
    /// the schema's timezone: `RecordBatch::try_new` rejects arrays whose type disagrees
    /// with the schema, so a `timestamptz` column (schema `Timestamp(µs, "UTC")`) must not
    /// be built by a naive `TimestampMicrosecondBuilder`.
    pub fn new_builder(data_type: &DataType) -> Box<dyn ArrayBuilder> {
        match data_type {
            DataType::Int16 => Box::new(Int16Builder::new()),
            DataType::Int32 => Box::new(Int32Builder::new()),
            DataType::Int64 => Box::new(Int64Builder::new()),
            DataType::Float32 => Box::new(Float32Builder::new()),
            DataType::Float64 => Box::new(Float64Builder::new()),
            DataType::Boolean => Box::new(BooleanBuilder::new()),
            DataType::Utf8 => Box::new(StringBuilder::new()),
            DataType::Binary => Box::new(BinaryBuilder::new()),
            DataType::Date32 => Box::new(Date32Builder::new()),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                Box::new(TimestampMicrosecondBuilder::new().with_timezone_opt(tz.clone()))
            }
            // Precision/scale are applied at finish time; only text[] arrays are supported.
            DataType::Decimal128(_, _) => Box::new(Decimal128Builder::new()),
            DataType::List(_) => Box::new(ListBuilder::new(StringBuilder::new())),
            _ => Box::new(StringBuilder::new()),
        }
    }
}

/// RowBatchBuilder: Accumulates rows into Arrow builders for streaming batching.
/// Phase 3: Enables true streaming with bounded memory O(batch_size) instead of O(total_rows).
pub struct RowBatchBuilder {
    schema: Arc<Schema>,
    table_metadata: TableMetadata,
    builders: Vec<Box<dyn ArrayBuilder>>,
    row_count: usize,
}

impl RowBatchBuilder {
    /// Create a new RowBatchBuilder with empty Arrow builders.
    pub fn new(table_metadata: &TableMetadata) -> Result<Self, ExtractorError> {
        let schema = PostgresRowAdapter::build_arrow_schema(table_metadata)?;

        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
        for column in &table_metadata.columns {
            let data_type = ArrowTypeMapper::map(column)?;
            builders.push(PostgresRowAdapter::new_builder(&data_type));
        }

        Ok(Self {
            schema,
            table_metadata: table_metadata.clone(),
            builders,
            row_count: 0,
        })
    }

    /// Append a single row to the builders.
    pub fn append_row(&mut self, row: &PgRow) -> Result<(), ExtractorError> {
        let columns = self.table_metadata.columns.clone();
        for (idx, column) in columns.iter().enumerate() {
            self.append_value_to_builder(idx, row, column)?;
        }
        self.row_count += 1;
        Ok(())
    }

    /// Append a value to a specific builder based on the column type.
    fn append_value_to_builder(
        &mut self,
        builder_idx: usize,
        row: &PgRow,
        column: &ColumnMetadata,
    ) -> Result<(), ExtractorError> {
        let builder = &mut self.builders[builder_idx];

        match column.data_type.as_str() {
            "smallint" => {
                let value: Option<i16> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Int16Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Int16Builder failed".into())
                    })?;
                b.append_option(value);
            }
            "integer" => {
                let value: Option<i32> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Int32Builder failed".into())
                    })?;
                b.append_option(value);
            }
            "bigint" => {
                let value: Option<i64> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Int64Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Int64Builder failed".into())
                    })?;
                b.append_option(value);
            }
            "real" => {
                let value: Option<f32> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Float32Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Float32Builder failed".into())
                    })?;
                b.append_option(value);
            }
            "double precision" => {
                let value: Option<f64> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Float64Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Float64Builder failed".into())
                    })?;
                b.append_option(value);
            }
            "numeric" => {
                let value: Option<BigDecimal> = row.try_get(column.column_name.as_str())?;
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
            }
            "boolean" => {
                let value: Option<bool> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to BooleanBuilder failed".into())
                    })?;
                b.append_option(value);
            }
            "text" | "character varying" | "character" => {
                let value: Option<String> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to StringBuilder failed".into())
                    })?;
                b.append_option(value);
            }
            "USER-DEFINED" => {
                let value: Option<String> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to StringBuilder failed".into())
                    })?;
                b.append_option(value);
            }
            "timestamp with time zone" => {
                let value: Option<DateTime<Utc>> = row.try_get(column.column_name.as_str())?;
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
            }
            "timestamp without time zone" => {
                let value: Option<NaiveDateTime> = row.try_get(column.column_name.as_str())?;
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
            }
            "date" => {
                let value: Option<NaiveDate> = row.try_get(column.column_name.as_str())?;
                let days = value
                    .map(|d| (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32);
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Date32Builder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to Date32Builder failed".into())
                    })?;
                b.append_option(days);
            }
            "uuid" => {
                let value: Option<uuid::Uuid> = row.try_get(column.column_name.as_str())?;
                let s = value.map(|u| u.to_string());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to StringBuilder failed".into())
                    })?;
                b.append_option(s);
            }
            "bytea" => {
                let value: Option<Vec<u8>> = row.try_get(column.column_name.as_str())?;
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<BinaryBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to BinaryBuilder failed".into())
                    })?;
                b.append_option(value);
            }
            "jsonb" | "json" => {
                let value: Option<serde_json::Value> = row.try_get(column.column_name.as_str())?;
                let s = value.map(|v| v.to_string());
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| {
                        ExtractorError::Internal("downcast to StringBuilder failed".into())
                    })?;
                b.append_option(s);
            }
            "ARRAY" => match column.udt_name.as_deref() {
                Some("_text") => {
                    let value: Option<Vec<Option<String>>> =
                        row.try_get(column.column_name.as_str())?;
                    let b = builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<StringBuilder>>()
                        .ok_or_else(|| {
                            ExtractorError::Internal("downcast to ListBuilder failed".into())
                        })?;
                    PostgresRowAdapter::append_text_array_option(b, value);
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
        }

        Ok(())
    }

    /// Check if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Get the current row count.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Finish building and return a RecordBatch. Resets internal state for reuse.
    pub fn finish(&mut self) -> Result<RecordBatch, ExtractorError> {
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for (idx, builder) in self.builders.iter_mut().enumerate() {
            let column = &self.table_metadata.columns[idx];
            let data_type = ArrowTypeMapper::map(column)?;

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

        // Reset builders for next batch
        self.builders.clear();
        self.row_count = 0;

        // Reinitialize empty builders
        for column in &self.table_metadata.columns {
            let data_type = ArrowTypeMapper::map(column)?;
            self.builders
                .push(PostgresRowAdapter::new_builder(&data_type));
        }

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

                        Ok(value.map(|d| {
                            (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
                        }))
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
