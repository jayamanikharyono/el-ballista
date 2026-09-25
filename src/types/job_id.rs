//! [`JobId`]: the validated identity of one extraction job.
//!
//! Used by the job config (`job_id`) and by the checkpoint store, which derives the
//! checkpoint / lock / progress file names from it (collision-free, see
//! `checkpoint::file_stem`). Validation happens once, at construction (including serde
//! deserialization), so every holder of a `JobId` can rely on it.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Maximum length of a job id, in bytes. Keeps derived file names well under the usual
/// 255-byte file-name limit.
pub const MAX_JOB_ID_LEN: usize = 128;

/// A job id that is non-empty, at most [`MAX_JOB_ID_LEN`] bytes, has no leading or trailing
/// whitespace and contains no control characters.
///
/// # Examples
///
/// ```
/// use rust_ballista_extraction_layer::types::JobId;
///
/// let id = JobId::new("orders.v1").unwrap();
/// assert_eq!(id.as_str(), "orders.v1");
/// assert!(JobId::new("").is_err());
/// assert!(JobId::new(" padded ").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct JobId(String);

/// Why a string is not a valid [`JobId`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid job_id {id:?}: {reason}")]
pub struct InvalidJobId {
    /// The rejected input.
    pub id: String,
    /// Which rule it broke.
    pub reason: &'static str,
}

impl JobId {
    /// Validate and wrap a job id.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::types::JobId;
    /// assert!(JobId::new("nightly/orders").is_ok());
    /// assert!(JobId::new("bad\nid").is_err());
    /// ```
    pub fn new(id: impl Into<String>) -> Result<Self, InvalidJobId> {
        let id = id.into();
        let reason = if id.is_empty() {
            Some("must not be empty")
        } else if id.len() > MAX_JOB_ID_LEN {
            Some("must be at most 128 bytes")
        } else if id.trim() != id {
            Some("must not have leading or trailing whitespace")
        } else if id.chars().any(char::is_control) {
            Some("must not contain control characters")
        } else {
            None
        };
        match reason {
            Some(reason) => Err(InvalidJobId { id, reason }),
            None => Ok(Self(id)),
        }
    }

    /// The id as a string slice.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::types::JobId;
    ///
    /// let id = JobId::new("orders.v1")?;
    /// assert_eq!(id.as_str(), "orders.v1");
    /// # Ok::<(), rust_ballista_extraction_layer::types::InvalidJobId>(())
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for JobId {
    type Error = InvalidJobId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for JobId {
    type Error = InvalidJobId;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl FromStr for JobId {
    type Err = InvalidJobId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<JobId> for String {
    fn from(value: JobId) -> Self {
        value.0
    }
}

impl AsRef<str> for JobId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl PartialEq<str> for JobId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for JobId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_on_construction_and_deserialization() {
        assert!(JobId::new("orders_v1").is_ok());
        assert!(JobId::new("orders.v1").is_ok());
        for bad in ["", " x", "x ", "a\tb", &"x".repeat(MAX_JOB_ID_LEN + 1)] {
            assert!(JobId::new(bad).is_err(), "{bad:?}");
        }
        let ok: JobId = serde_json::from_str("\"j1\"").unwrap();
        assert_eq!(ok, "j1");
        let err = serde_json::from_str::<JobId>("\"\"").unwrap_err();
        assert!(err.to_string().contains("must not be empty"), "{err}");
        assert_eq!(serde_json::to_string(&ok).unwrap(), "\"j1\"");
    }
}
