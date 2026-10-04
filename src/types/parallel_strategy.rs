//! Parallel scan strategy of a job (`parallel_scan.strategy`).
//!
//! Lives in the shared `types` module (not under a connector) so `config` can name it
//! without depending on connector code; connectors re-export it.

use serde::{Deserialize, Serialize};

/// A `parallel_scan.strategy` value that is not `none`, `keyset` or `ctid`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown parallel_scan.strategy {0:?} (expected none, keyset or ctid)")]
pub struct UnknownParallelStrategy(pub String);

/// Parallel scan strategy: how to partition the table across connections.
/// Serialized into distributed plans and job specs by its lowercase name (`none`, `keyset`,
/// `ctid`); `None` preserves single-scan behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ParallelStrategy {
    /// No parallelism; scan with a single connection.
    #[default]
    None,
    /// Partition by primary key ranges using keyset predicates.
    Keyset,
    /// Partition by physical page ranges (`ctid`); no partition column needed. Each partition
    /// reads its own snapshot, and an `UPDATE` that writes the new row version to another page
    /// (any update that is not HOT) moves the row into another partition's range, so under
    /// concurrent updates a row can be read twice or missed (`VACUUM FULL` / `CLUSTER`, which
    /// rewrite every page, between partitions do the same). Prefer keyset for tables that
    /// change during the scan. Exported snapshots for cross-connection consistency are not
    /// implemented (see `connector::postgres::parallel`).
    Ctid,
}

impl ParallelStrategy {
    /// Parse a configured strategy name, case-insensitively (surrounding whitespace
    /// ignored): `none` (or empty), `keyset`, `ctid`. Anything else is an
    /// [`UnknownParallelStrategy`] error — a typo must not silently fall back to a single
    /// unpartitioned scan.
    ///
    /// # Examples
    ///
    /// ```
    /// use el_ballista::types::ParallelStrategy;
    ///
    /// assert_eq!(ParallelStrategy::parse("Keyset").unwrap(), ParallelStrategy::Keyset);
    /// assert!(ParallelStrategy::parse("keyst").is_err());
    /// ```
    pub fn parse(s: &str) -> Result<Self, UnknownParallelStrategy> {
        let normalized = s.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "" | "none" => Ok(ParallelStrategy::None),
            "keyset" => Ok(ParallelStrategy::Keyset),
            "ctid" => Ok(ParallelStrategy::Ctid),
            _ => Err(UnknownParallelStrategy(s.to_string())),
        }
    }
}

impl std::str::FromStr for ParallelStrategy {
    type Err = UnknownParallelStrategy;

    /// Same as [`ParallelStrategy::parse`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}
