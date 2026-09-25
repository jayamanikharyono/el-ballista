use thiserror::Error;

use crate::types::ProjectionError;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ExtractorError {
    #[error("Source error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Unsupported source type: {0}")]
    UnsupportedType(String),

    /// A source value with no faithful Arrow representation in the column's mapped type
    /// (`±infinity` timestamps/dates, numeric `NaN`/`±Infinity`, numeric digits beyond the
    /// `Decimal128` scale or precision). Never silently coerced to NULL or truncated.
    #[error("column '{column}': value not representable as {arrow_type}: {reason}")]
    UnsupportedValue {
        column: String,
        arrow_type: String,
        reason: String,
    },

    /// Schema discovery found no columns: the table does not exist (or is not visible to
    /// this role). Distinct from an empty table, which still has a schema.
    #[error("table not found (or not visible): {0}")]
    TableNotFound(String),

    /// A requested projection does not match the table (unknown name, bad index).
    #[error("invalid projection: {0}")]
    Projection(#[from] ProjectionError),

    /// Caller-supplied extraction parameters that cannot produce a correct scan
    /// (e.g. `batch_size = 0`, unknown parallel strategy).
    #[error("invalid extraction configuration: {0}")]
    InvalidConfig(String),

    #[error("Statistics collection error: {0}")]
    Statistics(String),

    /// A source query failed; `context` says which one (statistics, partition bounds,
    /// EXPLAIN …) and the sqlx error is kept as the `source()` cause.
    #[error("{context}")]
    SourceQuery {
        context: String,
        #[source]
        source: sqlx::Error,
    },

    /// A source response could not be parsed (e.g. EXPLAIN JSON); the parser error is kept.
    #[error("{context}")]
    Parse {
        context: String,
        #[source]
        source: serde_json::Error,
    },

    /// An error cached and shared by several callers (e.g. a lazily opened pool that failed);
    /// the original error stays reachable via `source()`.
    #[error("{context}")]
    Shared {
        context: String,
        #[source]
        source: std::sync::Arc<ExtractorError>,
    },

    #[error("Internal error: {0}")]
    Internal(String),
}
