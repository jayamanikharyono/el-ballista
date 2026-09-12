//! Checkpoint store SPI.
//! checkpoint/mod.rs
//! A trimmed version of `CheckpointStore` from docs/incremental-extraction.md §6: one committed
//! watermark per job, a best-effort lease so two invocations of the same job don't race each
//! other, and enough state to answer "what did the last run do."
//!
//! The doc specifies a Postgres-backed implementation as the default, with a local-file
//! implementation for development. We're deliberately starting with only the file-backed
//! implementation (`json_store`) — standing up a metadata database before the extraction path
//! itself is proven is the wrong order of operations. Swapping in a Postgres-backed
//! `CheckpointStore` later is additive: this trait doesn't need to change for that.
//!
//! Known limitation: `JsonCheckpointStore`'s lease is advisory (checked in-process, not a real
//! cross-process file lock), so it protects against a second `rel run` invocation on the same
//! machine racing this one — not against two machines. That's an acceptable gap for a single
//! scheduler triggering one job at a time, which is what Phase 1 targets; it is not what the
//! doc's compare-and-swap lease guarantees, and should not be assumed to be until the Postgres
//! store lands.

pub mod json_store;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::AppError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunState {
    Committed,
    Running,
    Failed,
}

#[derive(Debug, Clone)]
pub struct JobKey {
    pub job_id: String,
    pub namespace: String,
}

impl JobKey {
    /// A job key in the default namespace. Backfills construct their own namespace directly
    /// so they don't clobber the live incremental job's watermark (see
    /// docs/incremental-extraction.md §6).
    pub fn new(job_id: impl Into<String>) -> Self {
        Self {
            job_id: job_id.into(),
            namespace: "default".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub job_id: String,
    pub namespace: String,
    pub watermark_column: String,
    /// The last committed high watermark. `None` means "no successful run yet" — the caller
    /// treats this as the beginning of time.
    pub watermark_value: Option<DateTime<Utc>>,
    pub state: RunState,
    pub run_id: Option<Uuid>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct RunStats {
    pub rows_extracted: u64,
    pub window_lo: Option<DateTime<Utc>>,
    pub window_hi: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Acquire the run lease for this job, returning the checkpoint state to resume from. Fails
    /// if another run currently holds an unexpired lease.
    async fn acquire(
        &self,
        key: &JobKey,
        run_id: Uuid,
        lease: Duration,
        watermark_column: &str,
    ) -> Result<Checkpoint, AppError>;

    /// Advance the committed watermark. Only succeeds if `run_id` still holds the lease.
    async fn commit(
        &self,
        key: &JobKey,
        run_id: Uuid,
        next: DateTime<Utc>,
        stats: RunStats,
    ) -> Result<(), AppError>;

    /// Release the lease without advancing the watermark — a failed run.
    async fn abandon(&self, key: &JobKey, run_id: Uuid, err: &str) -> Result<(), AppError>;

    /// Read the current checkpoint without acquiring anything.
    async fn read(&self, key: &JobKey) -> Result<Option<Checkpoint>, AppError>;
}
