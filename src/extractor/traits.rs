#![allow(dead_code)]

use arrow::record_batch::RecordBatch;
use sqlx::postgres::PgRow;
use crate::extractor::errors::ExtractorError;
use crate::types::ColumnMetadata;

pub trait Extractor<T> {
    fn extract(&self, table_name: &str) -> Result<RecordBatch, ExtractorError>;
}

pub trait RowAdapter<T> {
    fn adapt(&self, row: &PgRow) -> Result<RecordBatch, ExtractorError>;
}

pub trait SchemaDiscovery<T> {
    fn discover(&self, table_name: &str) -> Result<Vec<ColumnMetadata>, ExtractorError>;
}

pub trait ArrowTypeMapper<T> {
    fn map(&self, row: &PgRow) -> Result<T, ExtractorError>;
}