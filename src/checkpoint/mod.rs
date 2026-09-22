//! Split-execution checkpoint store.
//! checkpoint/mod.rs
//!
//! Tracks per-split execution state so a failed or interrupted extraction can
//! resume without re-running splits that already completed. A "split" is one
//! non-overlapping source scan: a keyset partition on the single-node path, or
//! one Ballista scan task on the distributed path. An unsplit table is a single
//! split (`split-0`).
//!
//! This store intentionally knows nothing about watermarks, incremental windows,
//! backfills, or CDC. The orchestrator decides WHAT range to extract (full table
//! or a caller-provided filter); this store only records HOW the extraction went
//! (which splits finished). See the extraction-scope note in the crate docs.
//!
//! Backed by [`json_store::JsonCheckpointStore`]: one JSON file per job under a
//! local directory, written atomically (temp file + rename). Good enough for a
//! single scheduler triggering one job at a time on one machine; not a
//! cross-machine compare-and-swap lease.

pub mod json_store;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::errors::AppError;

/// Execution state of one split.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitState {
    Pending,
    Running,
    Completed,
    Failed,
}

/// Identifies one extraction job's checkpoint file.
#[derive(Debug, Clone)]
pub struct JobKey {
    pub job_id: String,
}

impl JobKey {
    pub fn new(job_id: impl Into<String>) -> Self {
        Self {
            job_id: job_id.into(),
        }
    }
}

/// Status of a single split within a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitStatus {
    pub split_id: String,
    pub state: SplitState,
    /// Rows extracted by this split on its last completed attempt.
    #[serde(default)]
    pub rows_extracted: u64,
    pub updated_at: DateTime<Utc>,
    /// Failure reason from the last failed attempt, if any.
    #[serde(default)]
    pub error: Option<String>,
}

/// The persisted per-job record: every split and its state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCheckpoint {
    pub job_id: String,
    pub splits: Vec<SplitStatus>,
    pub updated_at: DateTime<Utc>,
}

impl JobCheckpoint {
    /// Split ids that still need work (everything not `Completed`).
    pub fn pending_splits(&self) -> Vec<&SplitStatus> {
        self.splits
            .iter()
            .filter(|s| s.state != SplitState::Completed)
            .collect()
    }

    /// True when every recorded split completed.
    pub fn all_completed(&self) -> bool {
        !self.splits.is_empty() && self.splits.iter().all(|s| s.state == SplitState::Completed)
    }
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Register the split plan for a run. Splits already recorded as `Completed`
    /// are preserved so a retry skips them; new split ids start `Pending`.
    async fn begin(&self, key: &JobKey, split_ids: &[String]) -> Result<JobCheckpoint, AppError>;

    /// Mark a split as running (a retry attempt moves `Failed`/`Pending` back to `Running`).
    async fn mark_running(&self, key: &JobKey, split_id: &str) -> Result<(), AppError>;

    /// Mark a split completed with its extracted row count.
    async fn mark_completed(
        &self,
        key: &JobKey,
        split_id: &str,
        rows_extracted: u64,
    ) -> Result<(), AppError>;

    /// Mark a split failed without touching other splits.
    async fn mark_failed(&self, key: &JobKey, split_id: &str, err: &str) -> Result<(), AppError>;

    /// Read the current checkpoint without changing anything.
    async fn read(&self, key: &JobKey) -> Result<Option<JobCheckpoint>, AppError>;

    /// Delete the checkpoint file so the next run starts fresh.
    async fn reset(&self, key: &JobKey) -> Result<(), AppError>;
}
