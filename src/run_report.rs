//! Run reports: one immutable JSON record per run of a job.
//!
//! A checkpoint answers "what is left to do?" — one mutable file per job, rewritten as splits
//! finish, so a retry can resume. A run report answers "what happened in run X?" — one file
//! per run, never rewritten once the run has ended, so every attempt stays visible:
//!
//! ```text
//! <checkpoint.dir>/<job>.json                      checkpoint (mutable, one per job)
//! <checkpoint.dir>/runs/<job>/<run_id>.json        run report (one per run)
//! ```
//!
//! The `run_id` is the same one every source query of the run carries in its SQL comment tag
//! (`/* el-ballista … run_id=r_… */`), so a report lines up with Postgres logs and
//! `pg_stat_activity`.
//!
//! Lifecycle: a `running` stub is written when the run starts and atomically replaced
//! (tmp file + fsync + rename) with the final `succeeded` / `failed` record when it ends. A
//! stub left as `running` means the process died mid-run.
//!
//! Reports are observability, not state: nothing reads them to decide what to extract, and a
//! report that cannot be written is logged, never a reason to fail the extraction. They hold
//! metadata only — never row data or credentials. Retention is left to the operator or
//! orchestrator; nothing is deleted automatically.
//!
//! Row counts across reports: delivery is at-least-once per split, so a split that failed
//! mid-stream and was re-delivered by a later run appears in both reports. Sum
//! `totals.rows_delivered` across runs only with that in mind.

use std::path::{Path, PathBuf};
use tracing::warn;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::checkpoint::{SplitBounds, file_stem};
use crate::types::JobId;

/// Version of the report layout, bumped on incompatible changes.
pub const FORMAT_VERSION: u32 = 1;

/// Where a run stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunStatus {
    /// Started and not finished (or the process died before finishing).
    Running,
    /// Every split completed.
    Succeeded,
    /// At least one split failed, or the run failed before scanning.
    Failed,
}

/// Which terminal produced the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunKind {
    /// `run_with(consumer)`: rows delivered to a consumer, splits checkpointed.
    Checkpointed,
    /// `run()` / `el-ballista run` / `el-ballista distribute`: rows counted and discarded, no checkpoint.
    Diagnostic,
}

/// Where the run executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunMode {
    /// Plain DataFusion in this process.
    Standalone,
    /// A Ballista cluster (`el-ballista scheduler` + `el-ballista worker`s).
    Distributed,
}

/// The extraction plan the run executed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanSummary {
    /// The checkpoint's plan fingerprint (`None` if the run failed before planning).
    pub fingerprint: Option<String>,
    /// Schema-qualified source table.
    pub table: String,
    /// Projected columns (`None` = every column).
    pub columns: Option<Vec<String>>,
    /// The job's filters, one entry per AND-conjunct, in display form.
    pub filters: Vec<String>,
    /// `none` / `keyset` / `ctid`.
    pub strategy: String,
    /// Configured partition count.
    pub partitions: usize,
    /// Configured partition column.
    pub partition_column: String,
}

/// How one filter was handled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushdownEntry {
    /// The filter in display form (an OR-group shows as one `(a OR b)` entry).
    pub filter: String,
    /// Whether the source evaluates it (`Exact` or `Inexact`).
    pub pushed: bool,
    /// The decision with its reason, as `el-ballista plan` prints it.
    pub decision: String,
}

/// What happened to one split in this run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SplitOutcome {
    /// Scanned and delivered in this run.
    Completed,
    /// Failed in this run (see `error`).
    Failed,
    /// Completed by an earlier run; not scanned again.
    Skipped,
}

/// One split of the run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitReport {
    pub split_id: String,
    /// Key range (`None` = whole table / whole distributed scan).
    pub bounds: Option<SplitBounds>,
    pub outcome: SplitOutcome,
    /// Rows delivered in this run; for a skipped split, the rows its earlier run recorded.
    pub rows: u64,
    /// Arrow batches delivered in this run (0 for skipped splits).
    pub batches: u64,
    /// Arrow bytes delivered in this run (0 for skipped splits).
    pub bytes: u64,
    /// Wall time of the split in this run (0 for skipped splits).
    pub elapsed_ms: u64,
    /// Error chain of a failed split.
    pub error: Option<String>,
}

/// Run totals.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunTotals {
    /// Rows streamed to the consumer in this run.
    pub rows_delivered: u64,
    /// Rows of every completed split: delivered now plus the recorded rows of skipped splits.
    pub rows_extracted: u64,
    pub splits_completed: usize,
    pub splits_skipped: usize,
    pub splits_failed: usize,
    pub splits_total: usize,
}

/// One run of a job. See the module docs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunReport {
    pub format_version: u32,
    pub job_id: JobId,
    pub run_id: String,
    pub status: RunStatus,
    pub kind: RunKind,
    pub mode: RunMode,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
    /// Version of this crate that ran the job.
    pub crate_version: String,
    pub plan: PlanSummary,
    pub pushdown: Vec<PushdownEntry>,
    pub splits: Vec<SplitReport>,
    pub totals: RunTotals,
    /// Ballista workers (distributed runs only).
    pub workers: Option<usize>,
    /// Error chain when the run failed.
    pub error: Option<String>,
}

impl RunReport {
    /// A `running` report for a run that starts now.
    ///
    /// # Examples
    /// ```
    /// use el_ballista::run_report::*;
    /// use el_ballista::types::JobId;
    ///
    /// let plan = PlanSummary {
    ///     fingerprint: None,
    ///     table: "public.payment".into(),
    ///     columns: None,
    ///     filters: vec!["customer_id >= 300".into()],
    ///     strategy: "none".into(),
    ///     partitions: 1,
    ///     partition_column: "payment_id".into(),
    /// };
    /// let report = RunReport::start(
    ///     JobId::new("payment_extract").unwrap(),
    ///     "r_1a2b3c4d".into(),
    ///     RunKind::Checkpointed,
    ///     RunMode::Standalone,
    ///     plan,
    /// );
    /// assert_eq!(report.status, RunStatus::Running);
    /// assert!(report.finished_at.is_none());
    /// ```
    pub fn start(
        job_id: JobId,
        run_id: String,
        kind: RunKind,
        mode: RunMode,
        plan: PlanSummary,
    ) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            job_id,
            run_id,
            status: RunStatus::Running,
            kind,
            mode,
            started_at: Utc::now(),
            finished_at: None,
            duration_ms: None,
            crate_version: env!("CARGO_PKG_VERSION").to_string(),
            plan,
            pushdown: Vec::new(),
            splits: Vec::new(),
            totals: RunTotals::default(),
            workers: None,
            error: None,
        }
    }

    /// Mark the report finished now: `error` `None` = succeeded, `Some(chain)` = failed.
    /// Totals are recomputed from `splits`.
    pub fn finish(&mut self, error: Option<String>) {
        let now = Utc::now();
        self.finished_at = Some(now);
        self.duration_ms = u64::try_from((now - self.started_at).num_milliseconds()).ok();
        let count = |o: SplitOutcome| self.splits.iter().filter(|s| s.outcome == o).count();
        let rows = |o: SplitOutcome| {
            self.splits
                .iter()
                .filter(|s| s.outcome == o)
                .map(|s| s.rows)
                .sum::<u64>()
        };
        let delivered = rows(SplitOutcome::Completed);
        self.totals = RunTotals {
            rows_delivered: delivered,
            rows_extracted: delivered + rows(SplitOutcome::Skipped),
            splits_completed: count(SplitOutcome::Completed) + count(SplitOutcome::Skipped),
            splits_skipped: count(SplitOutcome::Skipped),
            splits_failed: count(SplitOutcome::Failed),
            splits_total: self.totals.splits_total.max(self.splits.len()),
        };
        self.status = if error.is_none() && self.totals.splits_failed == 0 {
            RunStatus::Succeeded
        } else {
            RunStatus::Failed
        };
        self.error = error;
    }
}

/// The directory holding a job's run reports: `<checkpoint_dir>/runs/<job file stem>`.
///
/// # Examples
/// ```
/// use std::path::Path;
/// use el_ballista::run_report::runs_dir;
/// use el_ballista::types::JobId;
///
/// let dir = runs_dir(Path::new(".checkpoints"), &JobId::new("payment_extract").unwrap());
/// assert_eq!(dir, Path::new(".checkpoints/runs/payment_extract"));
/// ```
pub fn runs_dir(checkpoint_dir: &Path, job_id: &JobId) -> PathBuf {
    checkpoint_dir.join("runs").join(file_stem(job_id))
}

/// A run id usable as a file name: ASCII alphanumerics, `-` and `_` only.
fn valid_run_id(run_id: &str) -> bool {
    !run_id.is_empty()
        && run_id.len() <= 128
        && run_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn invalid_run_id(run_id: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("invalid run id {run_id:?} (expected letters, digits, '-' or '_')"),
    )
}

/// The file of one run: `<runs_dir>/<run_id>.json`.
pub fn report_path(
    checkpoint_dir: &Path,
    job_id: &JobId,
    run_id: &str,
) -> std::io::Result<PathBuf> {
    if !valid_run_id(run_id) {
        return Err(invalid_run_id(run_id));
    }
    Ok(runs_dir(checkpoint_dir, job_id).join(format!("{run_id}.json")))
}

/// Write `report` atomically (tmp file + fsync + rename + directory fsync) and return its path.
/// Creates the runs directory if needed.
pub async fn write_report(checkpoint_dir: &Path, report: &RunReport) -> std::io::Result<PathBuf> {
    let path = report_path(checkpoint_dir, &report.job_id, &report.run_id)?;
    let dir = runs_dir(checkpoint_dir, &report.job_id);
    tokio::fs::create_dir_all(&dir).await?;
    let text = serde_json::to_string_pretty(report).map_err(std::io::Error::other)?;
    let tmp = dir.join(format!(
        "{}.json.tmp.{}.{}",
        report.run_id,
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    let result = async {
        tokio::fs::write(&tmp, text.as_bytes()).await?;
        tokio::fs::File::open(&tmp).await?.sync_all().await?;
        tokio::fs::rename(&tmp, &path).await
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    result?;
    sync_dir(&dir).await;
    Ok(path)
}

/// Best-effort directory fsync so the rename survives a crash (unix; a no-op elsewhere).
async fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(handle) = tokio::fs::File::open(dir).await {
        let _ = handle.sync_all().await;
    }
    #[cfg(not(unix))]
    let _ = dir;
}

/// Read one run's report.
pub async fn read_report(
    checkpoint_dir: &Path,
    job_id: &JobId,
    run_id: &str,
) -> std::io::Result<RunReport> {
    let path = report_path(checkpoint_dir, job_id, run_id)?;
    let text = tokio::fs::read_to_string(&path).await?;
    serde_json::from_str(&text).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{}: {e}", path.display()),
        )
    })
}

/// Every report of a job, oldest first (by `started_at`, then `run_id`). A missing runs
/// directory is an empty list; a file that does not parse as a report is skipped with a
/// warning (a run report never blocks reading the others).
pub async fn list_reports(
    checkpoint_dir: &Path,
    job_id: &JobId,
) -> std::io::Result<Vec<RunReport>> {
    let dir = runs_dir(checkpoint_dir, job_id);
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut reports = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let parsed = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| e.to_string())
            .and_then(|text| serde_json::from_str::<RunReport>(&text).map_err(|e| e.to_string()));
        match parsed {
            Ok(report) if &report.job_id == job_id => reports.push(report),
            Ok(report) => warn!(
                path = %path.display(),
                job_id = %report.job_id,
                "skipping a run report of another job"
            ),
            Err(e) => {
                warn!(path = %path.display(), error = %e, "skipping an unreadable run report")
            }
        }
    }
    reports.sort_by(|a, b| {
        a.started_at
            .cmp(&b.started_at)
            .then_with(|| a.run_id.cmp(&b.run_id))
    });
    Ok(reports)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> PlanSummary {
        PlanSummary {
            fingerprint: Some("fp".into()),
            table: "public.payment".into(),
            columns: Some(vec!["payment_id".into()]),
            filters: vec!["customer_id >= 300".into()],
            strategy: "keyset".into(),
            partitions: 3,
            partition_column: "payment_id".into(),
        }
    }

    fn split(id: &str, outcome: SplitOutcome, rows: u64) -> SplitReport {
        SplitReport {
            split_id: id.into(),
            bounds: None,
            outcome,
            rows,
            batches: u64::from(outcome != SplitOutcome::Skipped),
            bytes: 0,
            elapsed_ms: 0,
            error: (outcome == SplitOutcome::Failed).then(|| "boom".to_string()),
        }
    }

    fn report(run_id: &str) -> RunReport {
        RunReport::start(
            JobId::new("payment.v1").unwrap(),
            run_id.into(),
            RunKind::Checkpointed,
            RunMode::Standalone,
            plan(),
        )
    }

    #[test]
    fn finish_computes_totals_and_status() {
        let mut r = report("r_1");
        r.totals.splits_total = 3;
        r.splits = vec![
            split("split-0", SplitOutcome::Skipped, 10),
            split("split-1", SplitOutcome::Completed, 5),
            split("split-2", SplitOutcome::Completed, 7),
        ];
        r.finish(None);
        assert_eq!(r.status, RunStatus::Succeeded);
        assert_eq!(
            r.totals,
            RunTotals {
                rows_delivered: 12,
                rows_extracted: 22,
                splits_completed: 3,
                splits_skipped: 1,
                splits_failed: 0,
                splits_total: 3,
            }
        );
        assert!(r.finished_at.is_some() && r.duration_ms.is_some());

        // A failed split fails the run even without a run-level error.
        let mut r = report("r_2");
        r.splits = vec![
            split("split-0", SplitOutcome::Completed, 5),
            split("split-1", SplitOutcome::Failed, 0),
        ];
        r.finish(None);
        assert_eq!(r.status, RunStatus::Failed);
        assert_eq!(r.totals.splits_failed, 1);
        assert_eq!(r.totals.splits_total, 2);

        // A run-level error (e.g. a plan mismatch before any split ran) fails it too.
        let mut r = report("r_3");
        r.finish(Some("plan mismatch".into()));
        assert_eq!(r.status, RunStatus::Failed);
        assert_eq!(r.error.as_deref(), Some("plan mismatch"));
    }

    #[test]
    fn json_layout_is_snake_case_and_round_trips() {
        let mut r = report("r_1");
        r.splits = vec![split("split-0", SplitOutcome::Skipped, 3)];
        r.finish(None);
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["status"], "succeeded");
        assert_eq!(json["kind"], "checkpointed");
        assert_eq!(json["mode"], "standalone");
        assert_eq!(json["splits"][0]["outcome"], "skipped");
        assert_eq!(json["format_version"], FORMAT_VERSION);
        let back: RunReport = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn run_ids_are_validated_for_file_names() {
        let job = JobId::new("j").unwrap();
        let dir = Path::new("cp");
        assert!(report_path(dir, &job, "r_1a2b3c4d").is_ok());
        for bad in ["", "../x", "a/b", "a.b", "r 1"] {
            assert!(
                report_path(dir, &job, bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn write_read_and_list_reports() {
        let dir = std::env::temp_dir().join(format!("el_run_reports_{}", uuid::Uuid::new_v4()));
        let job = JobId::new("payment.v1").unwrap();

        // Nothing yet: an empty list, not an error.
        assert!(list_reports(&dir, &job).await.unwrap().is_empty());

        let mut first = report("r_first");
        let path = write_report(&dir, &first).await.unwrap();
        // The job id is file-stem encoded like checkpoints (`.` -> `%2E`).
        assert!(path.ends_with("runs/payment%2Ev1/r_first.json"), "{path:?}");
        // The running stub is replaced in place by the final record.
        first.finish(None);
        write_report(&dir, &first).await.unwrap();
        assert_eq!(read_report(&dir, &job, "r_first").await.unwrap(), first);

        let mut second = report("r_second");
        second.started_at = first.started_at + chrono::Duration::seconds(1);
        write_report(&dir, &second).await.unwrap();
        // Junk in the directory is skipped, not fatal.
        tokio::fs::write(runs_dir(&dir, &job).join("junk.json"), "{")
            .await
            .unwrap();

        let all = list_reports(&dir, &job).await.unwrap();
        let ids: Vec<_> = all.iter().map(|r| r.run_id.as_str()).collect();
        assert_eq!(ids, ["r_first", "r_second"]);
        assert_eq!(all[1].status, RunStatus::Running);
        // No temp files are left behind.
        let mut names = Vec::new();
        let mut entries = tokio::fs::read_dir(runs_dir(&dir, &job)).await.unwrap();
        while let Some(e) = entries.next_entry().await.unwrap() {
            names.push(e.file_name().to_string_lossy().to_string());
        }
        assert!(names.iter().all(|n| !n.contains(".tmp.")), "{names:?}");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
