use crate::extractor::errors::ExtractorError;
use datafusion::error::DataFusionError;
use sqlx::Error as SqlxError;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("extraction failed: {0}")]
    Extractor(#[from] ExtractorError),

    #[error("DataFusion failed: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error("SQLX error: {0}")]
    SqlxError(#[from] SqlxError),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("checkpoint error: {0}")]
    Checkpoint(String),

    #[error("incremental extraction error: {0}")]
    Incremental(String),

    #[error("sink error: {0}")]
    Sink(String),
}