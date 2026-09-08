use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExtractorError {
    #[error("PostgreSQL error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Unsupported PostgreSQL type: {0}")]
    UnsupportedType(String),

    // #[error("Invalid column: {0}")]
    // InvalidColumn(String),
    //
    // #[error("Invalid table: {0}")]
    // InvalidTable(String)
}