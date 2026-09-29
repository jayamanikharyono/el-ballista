//! Split-execution checkpoint store.
//! checkpoint/mod.rs
//!
//! Tracks per-split execution state so a failed or interrupted extraction can
//! resume without re-running splits that already completed. A "split" is one
//! non-overlapping source scan: a keyset partition on the single-node path, or
//! the whole distributed scan on the Ballista path. An unsplit table is a single
//! split (`split-0`).
//!
//! # What a completed split means
//!
//! A split is marked `Completed` only after the caller's consumer acknowledged it
//! (`run_with` returned `Ok` for that split after reading its stream to the end). The
//! checkpoint is bound to the extraction plan that produced it:
//!
//! - **Plan fingerprint.** A stable hash ([`fingerprint::PlanIdentity`]) of the table,
//!   projection/schema, the resolved filter predicates, the parallel strategy, partition count
//!   and partition column. Resuming with a different plan is a typed
//!   [`CheckpointError::PlanMismatch`] — never a silent skip of work that meant something else.
//! - **Stored split bounds.** Each split records the bounds it was planned with
//!   ([`SplitBounds`]); a resumed run re-creates exactly those scans instead of recomputing
//!   them from a table that has changed since.
//! - **Exclusive lock.** One run per job at a time ([`lock::JobLock`]), with a heartbeat and a
//!   takeover TTL for locks left behind by a crashed run.
//!
//! This store intentionally knows nothing about watermarks, incremental windows,
//! backfills, or CDC. The orchestrator decides WHAT range to extract (full table
//! or a caller-provided filter); this store only records HOW the extraction went
//! (which splits finished).
//!
//! Backed by [`json_store::JsonCheckpointStore`]: one JSON file per job under a
//! local directory, written atomically (temp file + fsync + rename + directory fsync). Good
//! for one machine or a shared filesystem with working `O_EXCL`; not a distributed
//! compare-and-swap store.

pub mod fingerprint;
pub mod json_store;
pub mod lock;
pub mod progress;

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::JobId;

pub use fingerprint::PlanIdentity;

/// Typed checkpoint failures. I/O and serde causes are kept as `#[source]`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CheckpointError {
    #[error("cannot {op} {}", path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("corrupt checkpoint file {}", path.display())]
    Corrupt {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error("cannot serialize checkpoint state for job '{job_id}'")]
    Serialize {
        job_id: String,
        #[source]
        source: serde_json::Error,
    },

    /// The file at this job's path records another job (e.g. a hash-suffixed name collision
    /// or a copied file). Never reused.
    #[error("checkpoint file {} belongs to job '{found}', not '{expected}'", path.display())]
    JobMismatch {
        path: PathBuf,
        expected: String,
        found: String,
    },

    /// The stored checkpoint was produced by a different extraction plan (other filters,
    /// table, projection or partitioning). Its completed splits mean different rows, so they
    /// cannot be skipped.
    #[error(
        "job '{job_id}': the stored checkpoint belongs to a different extraction plan \
         (stored {stored}, current {current}; differs in: {differs}). Its completed splits \
         cannot be reused: use a new job_id, or run `el-ballista checkpoint reset --config <file>` \
         to discard it"
    )]
    PlanMismatch {
        job_id: String,
        stored: String,
        current: String,
        differs: String,
    },

    /// Same plan fingerprint, but the split list handed to `begin` differs from the stored
    /// one (a caller bug: resumed runs must reuse the stored bounds).
    #[error("job '{job_id}': split plan does not match the stored checkpoint: {detail}")]
    SplitPlanMismatch { job_id: String, detail: String },

    #[error("no checkpoint begun for job '{job_id}'")]
    NotBegun { job_id: String },

    #[error("unknown split '{split_id}' for job '{job_id}'")]
    UnknownSplit { job_id: String, split_id: String },

    /// Another run holds this job's lock and its heartbeat is fresh.
    #[error(
        "job '{job_id}' is already running (lock {} held by owner {}, host {}, pid {}, \
         last heartbeat {}); wait for it to finish, or for its lock to go stale after \
         checkpoint.lock_ttl_secs",
        path.display(),
        .holder.owner,
        .holder.host,
        .holder.pid,
        .holder.heartbeat_at
    )]
    LockHeld {
        job_id: String,
        path: PathBuf,
        holder: Box<LockHolder>,
    },

    /// This run's lock was taken over by another process (its heartbeat went stale); the
    /// run must not commit further splits.
    #[error("job '{job_id}': lost the job lock to another run; no further splits are committed")]
    LockLost { job_id: String },
}

/// Who holds a job lock (see [`CheckpointError::LockHeld`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockHolder {
    pub owner: String,
    pub host: String,
    pub pid: u32,
    /// RFC 3339 time of the holder's last heartbeat.
    pub heartbeat_at: String,
}

/// Execution state of one split.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SplitState {
    Pending,
    Running,
    Completed,
    Failed,
}

/// The bounds a split was planned with, stored so a resumed run re-creates the same scan.
/// `lo`/`hi` are the keyset key range (`lo` inclusive, `hi` exclusive, `hi == None` for the
/// open-ended last split; the first split also holds NULL keys). `predicate` is a
/// human-readable copy of the rendered range; it is informational — scans are always
/// re-rendered from `lo`/`hi`, never from this text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitBounds {
    pub lo: Option<i64>,
    pub hi: Option<i64>,
    #[serde(default)]
    pub predicate: Option<String>,
}

/// One split of a plan handed to [`CheckpointStore::begin`]. `bounds == None` is a whole-table
/// (or whole distributed scan) split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedSplit {
    pub split_id: String,
    pub bounds: Option<SplitBounds>,
}

/// The plan a run registers: its identity (fingerprint + readable components) and splits.
#[derive(Debug, Clone)]
pub struct SplitPlan {
    pub identity: PlanIdentity,
    pub splits: Vec<PlannedSplit>,
}

/// Status of a single split within a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitStatus {
    pub split_id: String,
    pub state: SplitState,
    /// Rows delivered by this split on its last completed attempt.
    #[serde(default)]
    pub rows_extracted: u64,
    pub updated_at: DateTime<Utc>,
    /// Failure reason from the last failed attempt, if any.
    #[serde(default)]
    pub error: Option<String>,
    /// Bounds the split was planned with (`None` = whole table).
    #[serde(default)]
    pub bounds: Option<SplitBounds>,
}

/// The persisted per-job record: plan identity, every split and its state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCheckpoint {
    pub job_id: JobId,
    /// Fingerprint of the plan that created this checkpoint. `None` in files written before
    /// fingerprints existed — such a checkpoint can never be resumed (it is a mismatch).
    #[serde(default)]
    pub fingerprint: Option<String>,
    /// The readable components behind `fingerprint`, for mismatch diagnostics.
    #[serde(default)]
    pub plan: BTreeMap<String, String>,
    pub splits: Vec<SplitStatus>,
    pub updated_at: DateTime<Utc>,
}

impl JobCheckpoint {
    /// Split ids that still need work (everything not `Completed`).
    ///
    /// # Examples
    ///
    /// ```
    /// use chrono::Utc;
    /// use el_ballista::checkpoint::{JobCheckpoint, SplitState, SplitStatus};
    /// use el_ballista::types::JobId;
    ///
    /// let split = |id: &str, state| SplitStatus {
    ///     split_id: id.into(), state, rows_extracted: 0,
    ///     updated_at: Utc::now(), error: None, bounds: None,
    /// };
    /// let checkpoint = JobCheckpoint {
    ///     job_id: JobId::new("orders")?,
    ///     fingerprint: None,
    ///     plan: Default::default(),
    ///     splits: vec![split("split-0", SplitState::Completed), split("split-1", SplitState::Failed)],
    ///     updated_at: Utc::now(),
    /// };
    /// let pending: Vec<&str> = checkpoint.pending_splits().iter().map(|s| s.split_id.as_str()).collect();
    /// assert_eq!(pending, ["split-1"]);
    /// # Ok::<(), el_ballista::types::InvalidJobId>(())
    /// ```
    pub fn pending_splits(&self) -> Vec<&SplitStatus> {
        self.splits
            .iter()
            .filter(|s| s.state != SplitState::Completed)
            .collect()
    }

    /// True when every recorded split completed.
    ///
    /// # Examples
    ///
    /// ```
    /// use chrono::Utc;
    /// use el_ballista::checkpoint::{JobCheckpoint, SplitState, SplitStatus};
    /// use el_ballista::types::JobId;
    ///
    /// let split = |id: &str, state| SplitStatus {
    ///     split_id: id.into(), state, rows_extracted: 0,
    ///     updated_at: Utc::now(), error: None, bounds: None,
    /// };
    /// let checkpoint = JobCheckpoint {
    ///     job_id: JobId::new("orders")?,
    ///     fingerprint: None,
    ///     plan: Default::default(),
    ///     splits: vec![split("split-0", SplitState::Completed), split("split-1", SplitState::Failed)],
    ///     updated_at: Utc::now(),
    /// };
    /// assert!(!checkpoint.all_completed()); // split-1 failed
    /// # Ok::<(), el_ballista::types::InvalidJobId>(())
    /// ```
    pub fn all_completed(&self) -> bool {
        !self.splits.is_empty() && self.splits.iter().all(|s| s.state == SplitState::Completed)
    }

    /// The split with this id, if recorded.
    ///
    /// # Examples
    ///
    /// ```
    /// use chrono::Utc;
    /// use el_ballista::checkpoint::{JobCheckpoint, SplitState, SplitStatus};
    /// use el_ballista::types::JobId;
    ///
    /// let split = |id: &str, state| SplitStatus {
    ///     split_id: id.into(), state, rows_extracted: 0,
    ///     updated_at: Utc::now(), error: None, bounds: None,
    /// };
    /// let checkpoint = JobCheckpoint {
    ///     job_id: JobId::new("orders")?,
    ///     fingerprint: None,
    ///     plan: Default::default(),
    ///     splits: vec![split("split-0", SplitState::Completed), split("split-1", SplitState::Failed)],
    ///     updated_at: Utc::now(),
    /// };
    /// assert_eq!(checkpoint.split("split-1").map(|s| s.state), Some(SplitState::Failed));
    /// assert!(checkpoint.split("split-9").is_none());
    /// # Ok::<(), el_ballista::types::InvalidJobId>(())
    /// ```
    pub fn split(&self, split_id: &str) -> Option<&SplitStatus> {
        self.splits.iter().find(|s| s.split_id == split_id)
    }

    /// `Ok` when this checkpoint was produced by `identity`'s plan; otherwise a
    /// [`CheckpointError::PlanMismatch`] naming the differing components.
    pub(crate) fn check_plan(&self, identity: &PlanIdentity) -> Result<(), CheckpointError> {
        let current = identity.fingerprint();
        if self.fingerprint.as_deref() == Some(current.as_str()) {
            return Ok(());
        }
        let differs = identity.diff(&self.plan);
        Err(CheckpointError::PlanMismatch {
            job_id: self.job_id.to_string(),
            stored: self
                .fingerprint
                .clone()
                .unwrap_or_else(|| "none (checkpoint predates plan fingerprints)".into()),
            current,
            differs: if differs.is_empty() {
                "unknown".to_string()
            } else {
                differs.join(", ")
            },
        })
    }

    /// The split plan stored in this checkpoint (ids and bounds, in order): what a resumed
    /// run must execute.
    pub(crate) fn stored_splits(&self) -> Vec<PlannedSplit> {
        self.splits
            .iter()
            .map(|s| PlannedSplit {
                split_id: s.split_id.clone(),
                bounds: s.bounds.clone(),
            })
            .collect()
    }
}

/// The on-disk file stem for a job: collision-free and filesystem-safe. ASCII alphanumerics,
/// `-` and `_` are kept; every other byte (including `.` and `%`) is percent-encoded, so
/// `orders.v1` and `orders_v1` never share a file. Stems longer than 150 bytes are
/// truncated and suffixed with a hash of the full id (readers still verify the stored
/// `job_id`, so even a hash collision is detected rather than reused).
pub(crate) fn file_stem(job_id: &JobId) -> String {
    const MAX_STEM: usize = 150;
    let mut encoded = String::with_capacity(job_id.as_str().len());
    for b in job_id.as_str().bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' {
            encoded.push(char::from(b));
        } else {
            encoded.push_str(&format!("%{b:02X}"));
        }
    }
    if encoded.len() <= MAX_STEM {
        return encoded;
    }
    // `encoded` is ASCII; cut where no `%XX` escape is split (an escape starting at
    // `cut - 1` or `cut - 2` would be).
    let bytes = encoded.as_bytes();
    let mut cut = 100;
    while cut >= 1 && (bytes[cut - 1] == b'%' || (cut >= 2 && bytes[cut - 2] == b'%')) {
        cut -= 1;
    }
    format!(
        "{}~{:016x}",
        &encoded[..cut],
        fingerprint::fnv1a64(job_id.as_str().as_bytes())
    )
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Register the split plan for a run.
    ///
    /// - No checkpoint yet: every split starts `Pending` with its planned bounds.
    /// - A checkpoint from the same plan (fingerprint): `Completed` splits are preserved so a
    ///   retry skips them; `Running`/`Failed` ones return to `Pending`. The splits passed in
    ///   must equal the stored ones (resumed runs reuse stored bounds).
    /// - A checkpoint from a different plan: [`CheckpointError::PlanMismatch`].
    async fn begin(&self, key: &JobId, plan: &SplitPlan) -> Result<JobCheckpoint, CheckpointError>;

    /// Mark a split as running (a retry attempt moves `Failed`/`Pending` back to `Running`).
    async fn mark_running(&self, key: &JobId, split_id: &str) -> Result<(), CheckpointError>;

    /// Mark a split completed with its delivered row count. Call only after the consumer
    /// acknowledged the split.
    async fn mark_completed(
        &self,
        key: &JobId,
        split_id: &str,
        rows_extracted: u64,
    ) -> Result<(), CheckpointError>;

    /// Mark a split failed without touching other splits.
    async fn mark_failed(
        &self,
        key: &JobId,
        split_id: &str,
        err: &str,
    ) -> Result<(), CheckpointError>;

    /// Read the current checkpoint without changing anything.
    async fn read(&self, key: &JobId) -> Result<Option<JobCheckpoint>, CheckpointError>;

    /// Delete the checkpoint so the next run starts fresh.
    async fn reset(&self, key: &JobId) -> Result<(), CheckpointError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_stems_are_collision_free_and_safe() {
        let a = file_stem(&JobId::new("orders.v1").unwrap());
        let b = file_stem(&JobId::new("orders_v1").unwrap());
        assert_ne!(a, b);
        assert_eq!(a, "orders%2Ev1");
        assert_eq!(b, "orders_v1");
        let c = file_stem(&JobId::new("a/b%c").unwrap());
        assert_eq!(c, "a%2Fb%25c");
        assert!(!c.contains('/'));

        let long = JobId::new("é".repeat(60)).unwrap(); // 120 bytes -> 360 encoded
        let stem = file_stem(&long);
        assert!(stem.len() <= 150, "{}", stem.len());
        assert!(stem.contains('~'));
        let other = file_stem(&JobId::new(format!("{}x", "é".repeat(59))).unwrap());
        assert_ne!(stem, other);
    }
}
