use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExtractorError {
    #[error("PostgreSQL error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Unsupported PostgreSQL type: {0}")]
    UnsupportedType(String),

    #[error("Statistics collection error: {0}")]
    Statistics(String),

    #[error("Internal error: {0}")]
    Internal(String),
}