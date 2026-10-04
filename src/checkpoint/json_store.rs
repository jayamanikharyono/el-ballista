//! File-backed [`CheckpointStore`].
//! checkpoint/json_store.rs
//!
//! One JSON file per job (`<stem>.json`, see `file_stem`: collision-free
//! percent-encoding of the job id), written atomically: write a unique `.tmp` file, `fsync`
//! it, rename it over the real path, then `fsync` the directory (unix) so the rename survives
//! a crash. Every durability step's error propagates — a commit is only reported done once
//! it is on disk.
//!
//! Within one process, read-modify-write cycles on a job file are serialized by an async
//! mutex (a run marks splits concurrently); across processes, the run holds the job's
//! [`JobLock`] (see [`JsonCheckpointStore::lock`]).

use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::warn;

use async_trait::async_trait;
use chrono::Utc;

use super::lock::JobLock;
use super::{
    CheckpointError, CheckpointStore, JobCheckpoint, SplitPlan, SplitState, SplitStatus, file_stem,
};
use crate::types::JobId;

pub struct JsonCheckpointStore {
    dir: PathBuf,
    /// Serializes read-modify-write cycles within this process.
    write_lock: tokio::sync::Mutex<()>,
}

fn io_err(op: &'static str, path: &Path, source: std::io::Error) -> CheckpointError {
    CheckpointError::Io {
        op,
        path: path.to_path_buf(),
        source,
    }
}

impl JsonCheckpointStore {
    /// Open (creating if needed) a checkpoint directory.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use el_ballista::checkpoint::json_store::JsonCheckpointStore;
    /// let store = JsonCheckpointStore::new(".checkpoints")?;
    /// # Ok::<(), el_ballista::checkpoint::CheckpointError>(())
    /// ```
    pub fn new(dir: impl AsRef<Path>) -> Result<Self, CheckpointError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(|e| io_err("create checkpoint dir", &dir, e))?;
        Ok(Self {
            dir,
            write_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// The checkpoint directory.
    ///
    /// # Examples
    ///
    /// ```
    /// use el_ballista::checkpoint::json_store::JsonCheckpointStore;
    ///
    /// let dir = std::env::temp_dir().join("el-ballista-doc-json-store-dir");
    /// let store = JsonCheckpointStore::new(&dir)?;
    /// assert_eq!(store.dir(), dir.as_path());
    /// # Ok::<(), el_ballista::checkpoint::CheckpointError>(())
    /// ```
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Acquire this job's exclusive run lock (see [`JobLock`]).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::time::Duration;
    /// use el_ballista::checkpoint::json_store::JsonCheckpointStore;
    /// use el_ballista::types::JobId;
    ///
    /// let dir = std::env::temp_dir().join(format!("el-ballista-doc-store-lock-{}", std::process::id()));
    /// let store = JsonCheckpointStore::new(&dir)?;
    /// let lock = store.lock(&JobId::new("orders")?, Duration::from_secs(60)).await?;
    /// // ... extract and commit splits while holding the lock ...
    /// lock.release().await?;
    /// # Ok(()) }
    /// ```
    pub async fn lock(&self, key: &JobId, ttl: Duration) -> Result<JobLock, CheckpointError> {
        JobLock::acquire(&self.dir, key, ttl).await
    }

    fn path_for(&self, key: &JobId) -> PathBuf {
        self.dir.join(format!("{}.json", file_stem(key)))
    }

    /// Async read. A missing file is `None`; any other I/O error propagates (it is never
    /// mistaken for "no checkpoint"). The stored `job_id` must match `key`.
    async fn read_file(&self, key: &JobId) -> Result<Option<JobCheckpoint>, CheckpointError> {
        let path = self.path_for(key);
        let text = match tokio::fs::read_to_string(&path).await {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_err("read checkpoint", &path, e)),
        };
        let checkpoint: JobCheckpoint =
            serde_json::from_str(&text).map_err(|source| CheckpointError::Corrupt {
                path: path.clone(),
                source,
            })?;
        if &checkpoint.job_id != key {
            return Err(CheckpointError::JobMismatch {
                path,
                expected: key.to_string(),
                found: checkpoint.job_id.to_string(),
            });
        }
        Ok(Some(checkpoint))
    }

    /// Atomic, durable write: tmp file + fsync + rename + directory fsync. Every step's
    /// error propagates.
    async fn write_file(
        &self,
        key: &JobId,
        checkpoint: &JobCheckpoint,
    ) -> Result<(), CheckpointError> {
        let path = self.path_for(key);
        let tmp_path = self.dir.join(format!(
            "{}.json.tmp.{}.{}",
            file_stem(key),
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let text = serde_json::to_string_pretty(checkpoint).map_err(|source| {
            CheckpointError::Serialize {
                job_id: key.to_string(),
                source,
            }
        })?;

        let result = async {
            tokio::fs::write(&tmp_path, text.as_bytes())
                .await
                .map_err(|e| io_err("write checkpoint", &tmp_path, e))?;
            let file = tokio::fs::File::open(&tmp_path)
                .await
                .map_err(|e| io_err("open checkpoint for fsync", &tmp_path, e))?;
            file.sync_all()
                .await
                .map_err(|e| io_err("fsync checkpoint", &tmp_path, e))?;
            tokio::fs::rename(&tmp_path, &path)
                .await
                .map_err(|e| io_err("finalize checkpoint", &path, e))
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&tmp_path).await;
        }
        result?;
        sync_dir(&self.dir).await
    }

    /// Remove `.tmp.*` files left by writers of this job that crashed between write and
    /// rename. Call only while holding the job lock (no live writer exists then).
    async fn remove_orphan_tmp_files(&self, key: &JobId) -> Result<(), CheckpointError> {
        let prefix = format!("{}.json.tmp.", file_stem(key));
        let mut entries = tokio::fs::read_dir(&self.dir)
            .await
            .map_err(|e| io_err("list checkpoint dir", &self.dir, e))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| io_err("list checkpoint dir", &self.dir, e))?
        {
            let name = entry.file_name();
            if name.to_str().is_some_and(|n| n.starts_with(&prefix)) {
                warn!(
                    job_id = %key,
                    path = %entry.path().display(),
                    "removing an orphaned checkpoint temp file (an earlier run stopped mid-write)"
                );
                match tokio::fs::remove_file(entry.path()).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(io_err("remove temp file", &entry.path(), e)),
                }
            }
        }
        Ok(())
    }

    async fn update_split(
        &self,
        key: &JobId,
        split_id: &str,
        apply: impl FnOnce(&mut SplitStatus),
    ) -> Result<(), CheckpointError> {
        let _guard = self.write_lock.lock().await;
        let mut checkpoint =
            self.read_file(key)
                .await?
                .ok_or_else(|| CheckpointError::NotBegun {
                    job_id: key.to_string(),
                })?;
        let now = Utc::now();
        let split = checkpoint
            .splits
            .iter_mut()
            .find(|s| s.split_id == split_id)
            .ok_or_else(|| CheckpointError::UnknownSplit {
                job_id: key.to_string(),
                split_id: split_id.to_string(),
            })?;
        apply(split);
        split.updated_at = now;
        checkpoint.updated_at = now;
        self.write_file(key, &checkpoint).await
    }
}

/// `fsync` the directory so a rename in it is durable (unix; a no-op elsewhere).
async fn sync_dir(dir: &Path) -> Result<(), CheckpointError> {
    #[cfg(unix)]
    {
        let owned = dir.to_path_buf();
        let joined = tokio::task::spawn_blocking(move || {
            std::fs::File::open(&owned).and_then(|d| d.sync_all())
        })
        .await;
        match joined {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(io_err("fsync checkpoint dir", dir, e)),
            Err(join) => Err(io_err(
                "fsync checkpoint dir",
                dir,
                std::io::Error::other(join.to_string()),
            )),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for JsonCheckpointStore {
    async fn begin(&self, key: &JobId, plan: &SplitPlan) -> Result<JobCheckpoint, CheckpointError> {
        let _guard = self.write_lock.lock().await;
        self.remove_orphan_tmp_files(key).await?;
        let now = Utc::now();
        let existing = self.read_file(key).await?;

        let splits = match &existing {
            None => plan
                .splits
                .iter()
                .map(|p| SplitStatus {
                    split_id: p.split_id.clone(),
                    state: SplitState::Pending,
                    rows_extracted: 0,
                    updated_at: now,
                    error: None,
                    bounds: p.bounds.clone(),
                })
                .collect(),
            Some(stored) => {
                stored.check_plan(&plan.identity)?;
                if stored.stored_splits() != plan.splits {
                    return Err(CheckpointError::SplitPlanMismatch {
                        job_id: key.to_string(),
                        detail: format!(
                            "stored {} split(s), got {}; resumed runs must reuse the stored bounds",
                            stored.splits.len(),
                            plan.splits.len()
                        ),
                    });
                }
                stored
                    .splits
                    .iter()
                    .map(|prev| match prev.state {
                        // A retry must not re-run completed splits.
                        SplitState::Completed => prev.clone(),
                        // Running here means a crashed run (we hold the lock now).
                        _ => SplitStatus {
                            state: SplitState::Pending,
                            rows_extracted: 0,
                            updated_at: now,
                            ..prev.clone()
                        },
                    })
                    .collect()
            }
        };

        let checkpoint = JobCheckpoint {
            job_id: key.clone(),
            fingerprint: Some(plan.identity.fingerprint()),
            plan: plan.identity.components().clone(),
            splits,
            updated_at: now,
        };
        self.write_file(key, &checkpoint).await?;
        Ok(checkpoint)
    }

    async fn mark_running(&self, key: &JobId, split_id: &str) -> Result<(), CheckpointError> {
        self.update_split(key, split_id, |split| split.state = SplitState::Running)
            .await
    }

    async fn mark_completed(
        &self,
        key: &JobId,
        split_id: &str,
        rows_extracted: u64,
    ) -> Result<(), CheckpointError> {
        self.update_split(key, split_id, |split| {
            split.state = SplitState::Completed;
            split.rows_extracted = rows_extracted;
            split.error = None;
        })
        .await
    }

    async fn mark_failed(
        &self,
        key: &JobId,
        split_id: &str,
        err: &str,
    ) -> Result<(), CheckpointError> {
        self.update_split(key, split_id, |split| {
            split.state = SplitState::Failed;
            split.error = Some(err.to_string());
        })
        .await
    }

    async fn read(&self, key: &JobId) -> Result<Option<JobCheckpoint>, CheckpointError> {
        self.read_file(key).await
    }

    async fn reset(&self, key: &JobId) -> Result<(), CheckpointError> {
        let _guard = self.write_lock.lock().await;
        let stem = file_stem(key);
        for path in [
            self.path_for(key),
            self.dir.join(format!("{stem}.progress.json")),
        ] {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_err("reset checkpoint", &path, e)),
            }
        }
        self.remove_orphan_tmp_files(key).await?;
        sync_dir(&self.dir).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{PlanIdentity, PlannedSplit, SplitBounds};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Unique temp dir per test: the store is file-backed, so parallel tests must not share.
    fn test_store() -> (JsonCheckpointStore, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "el_ballista_checkpoint_test_{}_{}",
            std::process::id(),
            TEST_DIR_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = JsonCheckpointStore::new(&dir).unwrap();
        (store, dir)
    }

    fn job(id: &str) -> JobId {
        JobId::new(id).unwrap()
    }

    /// `(split_id, Some((lo, hi)))` for a keyset split, `None` bounds for a whole-table one.
    type TestSplit<'a> = (&'a str, Option<(i64, Option<i64>)>);

    fn plan(filters: &str, splits: &[TestSplit<'_>]) -> SplitPlan {
        SplitPlan {
            identity: PlanIdentity::new()
                .with("table", "public.orders")
                .with("filters", filters),
            splits: splits
                .iter()
                .map(|(id, b)| PlannedSplit {
                    split_id: id.to_string(),
                    bounds: b.map(|(lo, hi)| SplitBounds {
                        lo: Some(lo),
                        hi,
                        predicate: None,
                    }),
                })
                .collect(),
        }
    }

    fn two_splits() -> SplitPlan {
        plan(
            "none",
            &[
                ("split-0", Some((1, Some(5)))),
                ("split-1", Some((5, None))),
            ],
        )
    }

    #[tokio::test]
    async fn test_begin_run_complete_cycle() {
        let (store, dir) = test_store();
        let key = job("job_orders");

        assert!(store.read(&key).await.unwrap().is_none());
        let checkpoint = store.begin(&key, &two_splits()).await.unwrap();
        assert_eq!(checkpoint.splits.len(), 2);
        assert!(
            checkpoint
                .splits
                .iter()
                .all(|s| s.state == SplitState::Pending)
        );
        assert_eq!(
            checkpoint.fingerprint.as_deref(),
            Some(two_splits().identity.fingerprint().as_str())
        );

        store.mark_running(&key, "split-0").await.unwrap();
        store.mark_completed(&key, "split-0", 10).await.unwrap();

        let checkpoint = store.read(&key).await.unwrap().unwrap();
        let s0 = checkpoint.split("split-0").unwrap();
        assert_eq!(s0.state, SplitState::Completed);
        assert_eq!(s0.rows_extracted, 10);
        assert!(!checkpoint.all_completed());

        // A retry with the same plan preserves the completed split and its stored bounds.
        let checkpoint = store.begin(&key, &two_splits()).await.unwrap();
        let s0 = checkpoint.split("split-0").unwrap();
        assert_eq!(s0.state, SplitState::Completed);
        assert_eq!(s0.rows_extracted, 10);
        assert_eq!(s0.bounds.as_ref().unwrap().hi, Some(5));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_different_filter_is_a_plan_mismatch_not_a_skip() {
        // Scenario A: same job, new filter -> typed error, never "already completed".
        let (store, dir) = test_store();
        let key = job("orders");
        let first = plan("id > 8", &[("split-0", None)]);
        store.begin(&key, &first).await.unwrap();
        store.mark_completed(&key, "split-0", 2).await.unwrap();

        let second = plan("id > 2", &[("split-0", None)]);
        let err = store.begin(&key, &second).await.unwrap_err();
        match &err {
            CheckpointError::PlanMismatch { differs, .. } => assert_eq!(differs, "filters"),
            other => panic!("expected PlanMismatch, got {other}"),
        }
        assert!(
            err.to_string().contains("el-ballista checkpoint reset"),
            "{err}"
        );

        // The stored checkpoint is untouched by the refused begin.
        let stored = store.read(&key).await.unwrap().unwrap();
        assert!(stored.all_completed());

        // After reset the new plan starts fresh.
        store.reset(&key).await.unwrap();
        let fresh = store.begin(&key, &second).await.unwrap();
        assert!(fresh.pending_splits().len() == 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_same_fingerprint_requires_the_stored_bounds() {
        // Scenario B: bounds recomputed after the table grew must not be accepted in
        // place of the stored ones.
        let (store, dir) = test_store();
        let key = job("orders");
        store.begin(&key, &two_splits()).await.unwrap();
        let drifted = plan(
            "none",
            &[
                ("split-0", Some((1, Some(1000)))),
                ("split-1", Some((1000, None))),
            ],
        );
        let err = store.begin(&key, &drifted).await.unwrap_err();
        assert!(
            matches!(err, CheckpointError::SplitPlanMismatch { .. }),
            "{err}"
        );
        let stored = store.read(&key).await.unwrap().unwrap();
        assert_eq!(stored.stored_splits(), two_splits().splits);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_legacy_checkpoint_without_fingerprint_is_a_mismatch() {
        let (store, dir) = test_store();
        let key = job("legacy");
        let legacy = r#"{"job_id": "legacy", "splits": [{"split_id": "split-0",
            "state": "Completed", "rows_extracted": 5, "updated_at": "2026-01-01T00:00:00Z"}],
            "updated_at": "2026-01-01T00:00:00Z"}"#;
        std::fs::write(dir.join("legacy.json"), legacy).unwrap();
        let err = store
            .begin(&key, &plan("none", &[("split-0", None)]))
            .await
            .unwrap_err();
        assert!(matches!(err, CheckpointError::PlanMismatch { .. }), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_failed_split_can_retry() {
        let (store, dir) = test_store();
        let key = job("job_orders");
        let p = plan("none", &[("split-0", None)]);

        store.begin(&key, &p).await.unwrap();
        store.mark_running(&key, "split-0").await.unwrap();
        store.mark_failed(&key, "split-0", "boom").await.unwrap();

        let checkpoint = store.read(&key).await.unwrap().unwrap();
        assert_eq!(checkpoint.splits[0].state, SplitState::Failed);

        // Retry moves it back to Pending via begin, then Running again.
        store.begin(&key, &p).await.unwrap();
        store.mark_running(&key, "split-0").await.unwrap();
        store.mark_completed(&key, "split-0", 5).await.unwrap();
        let checkpoint = store.read(&key).await.unwrap().unwrap();
        assert!(checkpoint.all_completed());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_unknown_split_is_an_error() {
        let (store, dir) = test_store();
        let key = job("job_orders");

        store
            .begin(&key, &plan("none", &[("split-0", None)]))
            .await
            .unwrap();
        let err = store.mark_completed(&key, "nope", 1).await.unwrap_err();
        assert!(err.to_string().contains("unknown split"));
        let err = store.mark_failed(&job("never-begun"), "split-0", "x").await;
        assert!(matches!(err, Err(CheckpointError::NotBegun { .. })));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_reset_clears() {
        let (store, dir) = test_store();
        let key = job("job_orders");

        store
            .begin(&key, &plan("none", &[("split-0", None)]))
            .await
            .unwrap();
        store.reset(&key).await.unwrap();
        assert!(store.read(&key).await.unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_similar_job_ids_do_not_share_a_file_and_job_id_is_verified() {
        let (store, dir) = test_store();
        let dotted = job("orders.v1");
        let underscored = job("orders_v1");
        store
            .begin(&dotted, &plan("a", &[("split-0", None)]))
            .await
            .unwrap();
        store.mark_completed(&dotted, "split-0", 1).await.unwrap();
        // Formerly both sanitized to `orders_v1.json` and the second job would have
        // skipped the first one's completed split.
        assert!(store.read(&underscored).await.unwrap().is_none());

        // A file whose recorded job_id differs is refused, never reused.
        std::fs::copy(dir.join("orders%2Ev1.json"), dir.join("orders_v1.json")).unwrap();
        let err = store.read(&underscored).await.unwrap_err();
        assert!(matches!(err, CheckpointError::JobMismatch { .. }), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_io_errors_propagate_and_orphan_tmp_files_are_removed() {
        let (store, dir) = test_store();
        let key = job("j");
        // A directory where the checkpoint file should be: reading it is an I/O error,
        // which must not read as "no checkpoint".
        std::fs::create_dir_all(dir.join("j.json")).unwrap();
        let err = store.read(&key).await.unwrap_err();
        assert!(matches!(err, CheckpointError::Io { .. }), "{err}");
        std::fs::remove_dir_all(dir.join("j.json")).unwrap();

        let orphan = dir.join("j.json.tmp.1.deadbeef");
        std::fs::write(&orphan, "{").unwrap();
        let other_job_tmp = dir.join("jj.json.tmp.1.deadbeef");
        std::fs::write(&other_job_tmp, "{").unwrap();
        store
            .begin(&key, &plan("none", &[("split-0", None)]))
            .await
            .unwrap();
        assert!(!orphan.exists());
        assert!(other_job_tmp.exists(), "other jobs' files are left alone");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_concurrent_marks_do_not_lose_updates() {
        let (store, dir) = test_store();
        let key = job("j");
        let ids: Vec<String> = (0..8).map(|i| format!("split-{i}")).collect();
        let p = SplitPlan {
            identity: PlanIdentity::new(),
            splits: ids
                .iter()
                .map(|id| PlannedSplit {
                    split_id: id.clone(),
                    bounds: None,
                })
                .collect(),
        };
        store.begin(&key, &p).await.unwrap();
        futures::future::try_join_all(ids.iter().map(|id| store.mark_completed(&key, id, 1)))
            .await
            .unwrap();
        assert!(store.read(&key).await.unwrap().unwrap().all_completed());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
