//! Crate-level error type.
//!
//! Every variant that wraps a lower-level failure keeps it as its `#[source]` (never a
//! flattened string), so callers can walk [`std::error::Error::source`] and match on the
//! underlying kind — source/database (`SourceConnect`, `SqlxError`, `Extractor`) versus
//! transformation/execution (`DataFusion`) versus job bookkeeping (`Checkpoint`, split
//! failures). `main` prints the chain once (`error: … / caused by: …`).

use std::path::PathBuf;

use crate::checkpoint::CheckpointError;
use crate::connector::errors::ExtractorError;
use crate::types::InvalidJobId;
use datafusion::error::DataFusionError;
use sqlx::Error as SqlxError;

/// The error a `run_with` split consumer returns: any boxed error, so consumers can use `?`
/// on their own error types. Kept as the `#[source]` of [`AppError::Consumer`].
pub type ConsumerError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AppError {
    #[error("extraction failed: {0}")]
    Extractor(#[from] ExtractorError),

    #[error("DataFusion failed: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error("source database error: {0}")]
    SqlxError(#[from] SqlxError),

    /// Opening a connection to the source failed (network, authentication, missing
    /// database). A source-side failure, not a configuration error.
    #[error("cannot connect to source {target}")]
    SourceConnect {
        /// `host:port/database` (never credentials).
        target: String,
        #[source]
        source: SqlxError,
    },

    /// A job spec value that cannot produce a correct extraction.
    #[error("configuration error: {0}")]
    Config(String),

    /// The job spec file could not be read.
    #[error("cannot read config file {}", path.display())]
    ConfigRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The job spec file is not a valid job spec (bad JSON, unknown field, wrong type).
    #[error("invalid config file {}", path.display())]
    ConfigParse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error(transparent)]
    InvalidJobId(#[from] InvalidJobId),

    #[error("checkpoint error: {0}")]
    Checkpoint(#[from] CheckpointError),

    /// A `run_with` consumer returned an error for a split; the split is recorded Failed.
    #[error("consumer failed on split '{split_id}'")]
    Consumer {
        split_id: String,
        #[source]
        source: ConsumerError,
    },

    /// A `run_with` consumer returned `Ok` without reading its split stream to the end, or
    /// after the stream yielded an error it did not propagate. The split is NOT marked
    /// completed: completion means every row of the split was delivered.
    #[error(
        "split '{split_id}': consumer returned Ok but {reason}; the split is not marked completed"
    )]
    SplitIncomplete {
        split_id: String,
        reason: &'static str,
    },

    /// One or more splits of a run failed. Every other split was still attempted; the
    /// successful ones are recorded Completed and are skipped on retry.
    #[error(
        "job '{job_id}': {} of {total} split(s) failed: {}",
        .failures.len(),
        describe_failures(.failures)
    )]
    SplitsFailed {
        job_id: String,
        total: usize,
        failures: Vec<SplitFailure>,
    },
}

/// One failed split inside [`AppError::SplitsFailed`].
#[derive(Debug)]
pub struct SplitFailure {
    pub split_id: String,
    pub error: Box<AppError>,
}

impl SplitFailure {
    /// The failure as one line: the error and its whole source chain.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::errors::{AppError, SplitFailure};
    ///
    /// let failure = SplitFailure {
    ///     split_id: "split-1".into(),
    ///     error: Box::new(AppError::Config("bad value".into())),
    /// };
    /// assert_eq!(failure.message(), "configuration error: bad value");
    /// ```
    pub fn message(&self) -> String {
        error_chain(self.error.as_ref()).join(": ")
    }
}

fn describe_failures(failures: &[SplitFailure]) -> String {
    failures
        .iter()
        .map(|f| format!("[{}: {}]", f.split_id, f.message()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The error followed by each distinct message in its `source()` chain. A source whose text
/// is already contained in the previous message (legacy variants embed their cause in their
/// own `Display`) is skipped, so every piece of information appears exactly once.
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
/// use rust_ballista_extraction_layer::errors::{AppError, error_chain};
///
/// let err = AppError::ConfigRead {
///     path: PathBuf::from("/etc/job.json"),
///     source: std::io::Error::new(std::io::ErrorKind::NotFound, "no such file"),
/// };
/// assert_eq!(error_chain(&err), ["cannot read config file /etc/job.json", "no such file"]);
/// ```
pub fn error_chain(err: &(dyn std::error::Error + 'static)) -> Vec<String> {
    let mut out = vec![err.to_string()];
    let mut current = err.source();
    while let Some(source) = current {
        let text = source.to_string();
        let seen = out.last().is_some_and(|prev| prev.contains(&text));
        if !seen && !text.is_empty() {
            out.push(text);
        }
        current = source.source();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_keeps_sources_and_skips_embedded_duplicates() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "no such file");
        let err = AppError::ConfigRead {
            path: PathBuf::from("/x.json"),
            source: io,
        };
        assert_eq!(
            error_chain(&err),
            vec![
                "cannot read config file /x.json".to_string(),
                "no such file".to_string()
            ]
        );

        // A legacy variant that embeds its cause prints it once, not twice.
        let err = AppError::Extractor(ExtractorError::Internal("boom".into()));
        assert_eq!(error_chain(&err).len(), 1);
    }

    #[test]
    fn splits_failed_lists_every_failed_split() {
        let err = AppError::SplitsFailed {
            job_id: "j".into(),
            total: 3,
            failures: vec![SplitFailure {
                split_id: "split-1".into(),
                error: Box::new(AppError::Config("bad".into())),
            }],
        };
        let text = err.to_string();
        assert!(text.contains("1 of 3"), "{text}");
        assert!(text.contains("split-1: configuration error: bad"), "{text}");
    }
}
