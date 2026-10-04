//! Driver-owned, non-blocking extraction progress.
//!
//! Ownership rule: **only the driver writes anything checkpoint-related** (split states
//! via [`CheckpointStore`](super::CheckpointStore) and the advisory progress file below). Ballista executor
//! tasks / worker processes never touch the checkpoint directory — they stream
//! `RecordBatch`es back; the driver derives per-partition status from those batches (or
//! from its own single-node partition loops) and reports it here.
//!
//! Design: the extraction loop holds a [`ProgressReporter`] and calls `ProgressReporter::try_report`
//! with a split's **cumulative** row count as its batches arrive (`PartitionStatus::running`)
//! and once more when the split finishes (`completed` / `failed`). Each report replaces the
//! split's previous state, so a split's rows are counted once however many reports it sends,
//! and the snapshot's `rows_extracted` is the sum over splits. `try_report` is
//! lock-free and never awaits — a full channel degrades to a dropped counter, never to
//! backpressure on the scan. A background [`ProgressFlusher`] task aggregates reports and
//! persists a debounced JSON snapshot (`<job>.progress.json`) at most every
//! `flush_interval` or `flush_rows`. The progress file is *advisory* (crash-recovery hint,
//! observability); the commit point remains the split states in the checkpoint file,
//! which the driver advances per finished split.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use tracing::Instrument;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::types::JobId;

/// Per-partition status reported by driver-side scan loops (or derived from streamed batches).
#[derive(Debug, Clone)]
pub struct PartitionStatus {
    /// 0-based partition index within this run.
    pub partition_id: usize,
    /// Stable split id (`split-{partition_id}` for keyset scans, `split-0` otherwise).
    pub split_id: String,
    /// Rows observed for this partition so far (cumulative per partition).
    pub rows: u64,
    /// True when the partition scan finished without error.
    pub done: bool,
    /// Set when the partition scan failed (recorded in the advisory snapshot).
    pub error: Option<String>,
}

impl PartitionStatus {
    /// A partition still scanning, with the rows it has delivered so far (cumulative).
    pub(crate) fn running(partition_id: usize, split_id: impl Into<String>, rows: u64) -> Self {
        Self {
            partition_id,
            split_id: split_id.into(),
            rows,
            done: false,
            error: None,
        }
    }

    /// Convenience for a finished partition.
    pub(crate) fn completed(partition_id: usize, split_id: impl Into<String>, rows: u64) -> Self {
        Self {
            partition_id,
            split_id: split_id.into(),
            rows,
            done: true,
            error: None,
        }
    }

    /// Convenience for a failed partition.
    pub(crate) fn failed(
        partition_id: usize,
        split_id: impl Into<String>,
        rows: u64,
        err: impl Into<String>,
    ) -> Self {
        Self {
            partition_id,
            split_id: split_id.into(),
            rows,
            done: false,
            error: Some(err.into()),
        }
    }
}

/// Per-partition advisory state in a [`ProgressSnapshot`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionState {
    /// Cumulative rows observed for the partition.
    pub rows: u64,
    /// True when the partition scan finished without error.
    pub done: bool,
    /// Partition error, if any.
    pub error: Option<String>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// Advisory progress snapshot persisted by the background flusher. Not a commit point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressSnapshot {
    pub job_id: String,
    pub updated_at: DateTime<Utc>,
    pub rows_extracted: u64,
    pub partitions_total: usize,
    pub partitions_done: usize,
    pub partitions: HashMap<String, PartitionState>,
    /// Reports dropped because the reporter channel was full (scan was never slowed).
    pub dropped_reports: u64,
}

enum ProgressMsg {
    Partition(PartitionStatus),
}

/// Non-blocking reporter handle. Clone it into every driver-side scan task; workers never
/// see it.
#[derive(Debug, Clone)]
pub struct ProgressReporter {
    tx: mpsc::Sender<ProgressMsg>,
    dropped: Arc<AtomicU64>,
}

impl ProgressReporter {
    /// Report one partition status. Never blocks or awaits: uses `try_send` and counts
    /// a drop if the background task is behind. Progress is advisory, so a drop only
    /// delays the snapshot — correctness (split commits) is unaffected.
    pub(crate) fn try_report(&self, status: PartitionStatus) {
        if self.tx.try_send(ProgressMsg::Partition(status)).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// How many reports were dropped due to a full channel.
    pub(crate) fn dropped_reports(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Fold one report into the per-split states. A report *replaces* its split's state (its
/// `rows` are cumulative for that split), so a split is counted once however many reports it
/// sends; `rows_since_flush` grows only by rows not seen before. A late "still running" report
/// never downgrades a split that already finished. Returns whether anything changed.
fn apply_status(
    states: &mut HashMap<String, PartitionState>,
    status: PartitionStatus,
    rows_since_flush: &mut u64,
) -> bool {
    let previous = states.get(&status.split_id);
    if let Some(prev) = previous
        && (prev.done || prev.error.is_some())
        && !status.done
        && status.error.is_none()
    {
        return false;
    }
    let previous_rows = previous.map_or(0, |p| p.rows);
    *rows_since_flush += status.rows.saturating_sub(previous_rows);
    states.insert(
        status.split_id,
        PartitionState {
            rows: status.rows,
            done: status.done,
            error: status.error,
            updated_at: Utc::now(),
        },
    );
    true
}

/// Background progress writer owned by the driver. Exactly one per run.
pub struct ProgressFlusher {
    reporter: ProgressReporter,
    handle: Option<tokio::task::JoinHandle<ProgressSnapshot>>,
    shutdown_tx: Option<oneshot::Sender<oneshot::Sender<ProgressSnapshot>>>,
}

impl ProgressFlusher {
    /// Start the background task. `dir` is the checkpoint dir; the snapshot lands at
    /// `<dir>/<stem>.progress.json`, named like the checkpoint file
    /// ([`file_stem`](super::file_stem)).
    pub(crate) fn start(
        dir: impl Into<PathBuf>,
        job_id: &JobId,
        partitions_total: usize,
        flush_interval: Duration,
        flush_rows: u64,
    ) -> Self {
        let dir: PathBuf = dir.into();
        let file_name = format!("{}.progress.json", super::file_stem(job_id));
        let job_id: String = job_id.to_string();
        let (tx, rx) = mpsc::channel::<ProgressMsg>(1024);
        let dropped = Arc::new(AtomicU64::new(0));
        let reporter = ProgressReporter {
            tx,
            dropped: dropped.clone(),
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<oneshot::Sender<ProgressSnapshot>>();

        let handle = tokio::spawn(
            async move {
                let mut rx = rx;
                let mut states: HashMap<String, PartitionState> = HashMap::new();
                // Rows newly observed since the last write (the `flush_rows` trigger), and whether
                // anything changed since then (the periodic trigger).
                let mut rows_since_flush: u64 = 0;
                let mut dirty = false;
                let mut ticker = tokio::time::interval(flush_interval);
                // First tick fires immediately; skip it so we don't write an empty snapshot.
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                ticker.tick().await;

                let snapshot = |states: &HashMap<String, PartitionState>| {
                    let partitions_done = states.values().filter(|s| s.done).count();
                    ProgressSnapshot {
                        job_id: job_id.clone(),
                        updated_at: Utc::now(),
                        rows_extracted: states.values().map(|s| s.rows).sum(),
                        partitions_total,
                        partitions_done,
                        partitions: states.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                        dropped_reports: dropped.load(Ordering::Relaxed),
                    }
                };

                let write_snapshot = |snap: &ProgressSnapshot| {
                    let dir = dir.clone();
                    let snap = snap.clone();
                    let path = dir.join(&file_name);
                    async move {
                        // Best-effort: progress is advisory. Never fail the run on it.
                        if tokio::fs::create_dir_all(&dir).await.is_err() {
                            return;
                        }
                        let Ok(text) = serde_json::to_string_pretty(&snap) else {
                            return;
                        };
                        let tmp = path.with_extension("progress.json.tmp");
                        if tokio::fs::write(&tmp, text.as_bytes()).await.is_err() {
                            return;
                        }
                        let _ = tokio::fs::rename(&tmp, &path).await;
                    }
                };

                let mut shutdown_rx = shutdown_rx;
                loop {
                    tokio::select! {
                        biased;
                        msg = rx.recv() => {
                            match msg {
                                Some(ProgressMsg::Partition(status)) => {
                                    if apply_status(&mut states, status, &mut rows_since_flush) {
                                        dirty = true;
                                    }
                                    if rows_since_flush >= flush_rows {
                                        rows_since_flush = 0;
                                        dirty = false;
                                        let snap = snapshot(&states);
                                        write_snapshot(&snap).await;
                                    }
                                }
                                None => break,
                            }
                        }
                        _ = ticker.tick() => {
                            // Periodic flush only if something changed since the last write.
                            if dirty {
                                rows_since_flush = 0;
                                dirty = false;
                                let snap = snapshot(&states);
                                write_snapshot(&snap).await;
                            }
                        }
                        reply = &mut shutdown_rx => {
                            let snap = snapshot(&states);
                            write_snapshot(&snap).await;
                            if let Ok(reply) = reply {
                                let _ = reply.send(snap.clone());
                            }
                            return snap;
                        }
                    }
                }
                snapshot(&states)
            }
            .instrument(tracing::Span::current()),
        );

        Self {
            reporter,
            handle: Some(handle),
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Non-blocking reporter to hand to driver-side scan loops.
    pub(crate) fn reporter(&self) -> ProgressReporter {
        self.reporter.clone()
    }

    /// Stop the background task and return the final snapshot. Performs one last
    /// (awaited) progress write; the caller then proceeds to commit split states.
    pub(crate) async fn shutdown(mut self) -> ProgressSnapshot {
        let (reply_tx, reply_rx) = oneshot::channel();
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(reply_tx);
        }
        if let Some(handle) = self.handle.take() {
            // Prefer the snapshot from the shutdown handshake; fall back to joining.
            if let Ok(snap) = reply_rx.await {
                let _ = handle.await;
                return snap;
            }
            return handle.await.unwrap_or_else(|_| ProgressSnapshot {
                job_id: String::new(),
                updated_at: Utc::now(),
                rows_extracted: 0,
                partitions_total: 0,
                partitions_done: 0,
                partitions: HashMap::new(),
                dropped_reports: self.reporter.dropped_reports(),
            });
        }
        ProgressSnapshot {
            job_id: String::new(),
            updated_at: Utc::now(),
            rows_extracted: 0,
            partitions_total: 0,
            partitions_done: 0,
            partitions: HashMap::new(),
            dropped_reports: self.reporter.dropped_reports(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reporter_never_blocks_and_shutdown_returns_snapshot() {
        let dir = std::env::temp_dir().join(format!(
            "el_ballista_progress_test_{}_{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let flusher = ProgressFlusher::start(
            &dir,
            &JobId::new("job1").unwrap(),
            2,
            Duration::from_secs(60),
            1_000_000,
        );
        let reporter = flusher.reporter();
        reporter.try_report(PartitionStatus::completed(0, "split-0", 10));
        reporter.try_report(PartitionStatus::running(1, "split-1", 5));
        let snap = flusher.shutdown().await;
        assert_eq!(snap.rows_extracted, 15);
        assert_eq!(snap.partitions_done, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Regression: a split reports its running total per batch and then its final count;
    /// the snapshot must count its rows once, not once per report.
    #[tokio::test]
    async fn running_then_completed_counts_each_split_once() {
        let dir = std::env::temp_dir().join(format!(
            "el_ballista_progress_once_{}_{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let flusher = ProgressFlusher::start(
            &dir,
            &JobId::new("job-once").unwrap(),
            2,
            Duration::from_secs(60),
            u64::MAX,
        );
        let reporter = flusher.reporter();
        // split-0: two batches (8192 + 6404), then completed with the total.
        reporter.try_report(PartitionStatus::running(0, "split-0", 8192));
        reporter.try_report(PartitionStatus::running(0, "split-0", 14_596));
        reporter.try_report(PartitionStatus::completed(0, "split-0", 14_596));
        // split-1: one batch, then failed (a failed split delivered nothing).
        reporter.try_report(PartitionStatus::running(1, "split-1", 100));
        reporter.try_report(PartitionStatus::failed(1, "split-1", 0, "boom"));
        let snap = flusher.shutdown().await;
        assert_eq!(snap.rows_extracted, 14_596);
        assert_eq!(snap.partitions_done, 1);
        assert_eq!(snap.partitions["split-0"].rows, 14_596);
        assert!(snap.partitions["split-0"].done);
        assert_eq!(snap.partitions["split-1"].error.as_deref(), Some("boom"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn late_running_report_never_downgrades_a_finished_split() {
        let mut states = HashMap::new();
        let mut since = 0u64;
        assert!(apply_status(
            &mut states,
            PartitionStatus::completed(0, "split-0", 10),
            &mut since
        ));
        assert!(!apply_status(
            &mut states,
            PartitionStatus::running(0, "split-0", 4),
            &mut since
        ));
        assert_eq!(states["split-0"].rows, 10);
        assert!(states["split-0"].done);
        assert_eq!(since, 10);
    }

    #[test]
    fn rows_since_flush_grows_by_new_rows_only() {
        let mut states = HashMap::new();
        let mut since = 0u64;
        apply_status(
            &mut states,
            PartitionStatus::running(0, "split-0", 100),
            &mut since,
        );
        apply_status(
            &mut states,
            PartitionStatus::running(0, "split-0", 250),
            &mut since,
        );
        apply_status(
            &mut states,
            PartitionStatus::completed(0, "split-0", 250),
            &mut since,
        );
        assert_eq!(since, 250);
    }

    #[tokio::test]
    async fn try_report_under_load_does_not_panic() {
        let dir = std::env::temp_dir().join(format!(
            "el_ballista_progress_load_{}_{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let flusher = ProgressFlusher::start(
            &dir,
            &JobId::new("job-load").unwrap(),
            1,
            Duration::from_secs(60),
            u64::MAX,
        );
        let reporter = flusher.reporter();
        for i in 0..2000 {
            reporter.try_report(PartitionStatus::completed(0, "split-0", i));
        }
        let snap = flusher.shutdown().await;
        assert!(snap.rows_extracted <= 1999);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
