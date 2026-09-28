//! Typed errors for the MySQL connector (prototype).
//!
//! Backend-specific on purpose: the shared [`ExtractorError`] is Postgres-worded and has no
//! variants for the MySQL decode contract (unsigned range, `BOOLEAN` domain, unknown table). Every
//! variant that wraps a lower-level failure keeps it as `#[source]`. Messages name the table or
//! column, never row values or credentials.

use thiserror::Error;

use crate::connector::errors::ExtractorError;

/// Errors from [`super::MysqlExtractor`] and the MySQL schema/type/decode helpers.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MysqlError {
    /// Connecting to, or querying, the MySQL source failed.
    #[error("MySQL source error: {0}")]
    Source(#[from] sqlx::Error),

    /// Building an Arrow array or `RecordBatch` failed.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// A shared-extractor error (e.g. an unsupported type from the type mapper).
    #[error(transparent)]
    Extractor(#[from] ExtractorError),

    /// `information_schema.COLUMNS` has no rows for the table: it does not exist, or the
    /// connecting user cannot see any of its columns.
    #[error("MySQL table not found (or no visible columns): {schema}.{table}")]
    TableNotFound { schema: String, table: String },

    /// A requested projection column does not exist in the table.
    #[error("unknown column(s) {missing:?} in MySQL table {schema}.{table}")]
    UnknownColumns {
        schema: String,
        table: String,
        missing: Vec<String>,
    },

    /// `batch_size` must be at least 1 (0 would silently yield zero rows).
    #[error("batch_size must be > 0")]
    InvalidBatchSize,

    /// Decoding one cell of `column` failed.
    #[error("failed to decode MySQL column `{column}`")]
    Decode {
        column: String,
        #[source]
        source: sqlx::Error,
    },

    /// A decoded value does not fit the Arrow type the column maps to. The type mapping picks a
    /// width that holds the declared source range, so this signals a metadata/decode mismatch —
    /// never silently wrapped or truncated.
    #[error("value in MySQL column `{column}` does not fit Arrow {target}")]
    OutOfRange {
        column: String,
        target: &'static str,
    },

    /// A `BOOLEAN` (`tinyint(1)`) cell held something other than 0/1. Mapping it to Arrow
    /// `Boolean` would lose the value, so it is an error rather than "non-zero is true".
    #[error("MySQL BOOLEAN column `{column}` holds a value other than 0/1")]
    NotBoolean { column: String },

    /// A `DATE`/`DATETIME`/`TIMESTAMP` cell holds the MySQL zero date (`0000-00-00`). No
    /// Arrow date represents it, and turning it into NULL would make it indistinguishable
    /// from a real NULL, so it fails the extraction.
    #[error("MySQL column `{column}` holds a zero date (0000-00-00), which has no Arrow value")]
    ZeroDate { column: String },
}
