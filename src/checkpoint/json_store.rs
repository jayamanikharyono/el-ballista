//! File-backed [`CheckpointStore`].
//! checkpoint/json_store.rs
//! One JSON file per job (`{job_id}.json`, sanitized), written atomically (write
//! to a unique `.tmp` file, then rename over the real path) with fsync of file
//! and directory so a commit survives a crash on ext4.

use std::fs;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::Utc;

use super::{CheckpointStore, JobCheckpoint, JobKey, SplitState, SplitStatus};
use crate::errors::AppError;

pub struct JsonCheckpointStore {
    dir: PathBuf,
}

impl JsonCheckpointStore {
    pub fn new(dir: impl AsRef<Path>) -> Result<Self, AppError> {
        let dir = dir.as_ref().to_path_buf();

        fs::create_dir_all(&dir).map_err(|e| {
            AppError::Checkpoint(format!(
                "cannot create checkpoint dir {}: {e}",
                dir.display()
            ))
        })?;

        Ok(Self { dir })
    }

    fn path_for(&self, key: &JobKey) -> PathBuf {
        self.dir.join(format!("{}.json", sanitize(&key.job_id)))
    }

    fn read_file(&self, key: &JobKey) -> Result<Option<JobCheckpoint>, AppError> {
        let path = self.path_for(key);

        if !path.exists() {
            return Ok(None);
        }

        let text = fs::read_to_string(&path)
            .map_err(|e| AppError::Checkpoint(format!("cannot read {}: {e}", path.display())))?;

        let checkpoint: JobCheckpoint = serde_json::from_str(&text).map_err(|e| {
            AppError::Checkpoint(format!("corrupt checkpoint file {}: {e}", path.display()))
        })?;

        Ok(Some(checkpoint))
    }

    fn write_file(&self, key: &JobKey, checkpoint: &JobCheckpoint) -> Result<(), AppError> {
        let path = self.path_for(key);
        // Unique tmp avoids last-writer-wins collision when concurrent `write_file` calls
        // race on the same job (last `rename` wins, but no torn JSON). `fsync` + dir `fsync`
        // makes the rename durable before the caller considers the commit done.
        let tmp_name = format!(
            "{}.tmp.{}.{}",
            path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("checkpoint.json"),
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        );
        let tmp_path = self.dir.join(tmp_name);

        let text = serde_json::to_string_pretty(checkpoint)
            .map_err(|e| AppError::Checkpoint(format!("cannot serialize checkpoint: {e}")))?;

        fs::write(&tmp_path, &text).map_err(|e| {
            AppError::Checkpoint(format!("cannot write {}: {e}", tmp_path.display()))
        })?;

        // Durability: sync file contents before rename.
        if let Ok(f) = std::fs::OpenOptions::new().read(true).open(&tmp_path) {
            let _ = f.sync_all();
        }

        fs::rename(&tmp_path, &path).map_err(|e| {
            // Best-effort cleanup of orphaned tmp on cross-device rename failure.
            let _ = fs::remove_file(&tmp_path);
            AppError::Checkpoint(format!("cannot finalize {}: {e}", path.display()))
        })?;

        // Durability: sync directory entry so rename survives crash on ext4.
        if let Ok(dir) = std::fs::OpenOptions::new().read(true).open(&self.dir) {
            let _ = dir.sync_all();
        }

        Ok(())
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[async_trait]
impl CheckpointStore for JsonCheckpointStore {
    async fn begin(&self, key: &JobKey, split_ids: &[String]) -> Result<JobCheckpoint, AppError> {
        let now = Utc::now();
        let existing = self.read_file(key)?;

        let mut splits: Vec<SplitStatus> = Vec::with_capacity(split_ids.len());
        for split_id in split_ids {
            let preserved = existing
                .as_ref()
                .and_then(|c| c.splits.iter().find(|s| &s.split_id == split_id));
            match preserved {
                // A retry must not re-run completed splits.
                Some(prev) if prev.state == SplitState::Completed => splits.push(prev.clone()),
                Some(prev) => splits.push(SplitStatus {
                    split_id: split_id.clone(),
                    state: SplitState::Pending,
                    rows_extracted: prev.rows_extracted,
                    updated_at: now,
                    error: prev.error.clone(),
                }),
                None => splits.push(SplitStatus {
                    split_id: split_id.clone(),
                    state: SplitState::Pending,
                    rows_extracted: 0,
                    updated_at: now,
                    error: None,
                }),
            }
        }

        let checkpoint = JobCheckpoint {
            job_id: key.job_id.clone(),
            splits,
            updated_at: now,
        };
        self.write_file(key, &checkpoint)?;
        Ok(checkpoint)
    }

    async fn mark_running(&self, key: &JobKey, split_id: &str) -> Result<(), AppError> {
        let mut checkpoint = self.read_file(key)?.ok_or_else(|| {
            AppError::Checkpoint(format!("no checkpoint begun for job '{}'", key.job_id))
        })?;
        let now = Utc::now();
        let split = checkpoint
            .splits
            .iter_mut()
            .find(|s| s.split_id == split_id)
            .ok_or_else(|| {
                AppError::Checkpoint(format!(
                    "unknown split '{split_id}' for job '{}'",
                    key.job_id
                ))
            })?;
        split.state = SplitState::Running;
        split.updated_at = now;
        checkpoint.updated_at = now;
        self.write_file(key, &checkpoint)
    }

    async fn mark_completed(
        &self,
        key: &JobKey,
        split_id: &str,
        rows_extracted: u64,
    ) -> Result<(), AppError> {
        let mut checkpoint = self.read_file(key)?.ok_or_else(|| {
            AppError::Checkpoint(format!("no checkpoint begun for job '{}'", key.job_id))
        })?;
        let now = Utc::now();
        let split = checkpoint
            .splits
            .iter_mut()
            .find(|s| s.split_id == split_id)
            .ok_or_else(|| {
                AppError::Checkpoint(format!(
                    "unknown split '{split_id}' for job '{}'",
                    key.job_id
                ))
            })?;
        split.state = SplitState::Completed;
        split.rows_extracted = rows_extracted;
        split.error = None;
        split.updated_at = now;
        checkpoint.updated_at = now;
        log::info!(
            "job '{}' split '{split_id}' completed (rows_extracted={rows_extracted})",
            key.job_id,
        );
        self.write_file(key, &checkpoint)
    }

    async fn mark_failed(&self, key: &JobKey, split_id: &str, err: &str) -> Result<(), AppError> {
        let mut checkpoint = match self.read_file(key)? {
            Some(current) => current,
            None => return Ok(()),
        };
        let now = Utc::now();
        if let Some(split) = checkpoint
            .splits
            .iter_mut()
            .find(|s| s.split_id == split_id)
        {
            log::warn!("job '{}' split '{split_id}' failed: {err}", key.job_id,);
            split.state = SplitState::Failed;
            split.error = Some(err.to_string());
            split.updated_at = now;
            checkpoint.updated_at = now;
            self.write_file(key, &checkpoint)?;
        }
        Ok(())
    }

    async fn read(&self, key: &JobKey) -> Result<Option<JobCheckpoint>, AppError> {
        self.read_file(key)
    }

    async fn reset(&self, key: &JobKey) -> Result<(), AppError> {
        let path = self.path_for(key);
        if path.exists() {
            fs::remove_file(&path).map_err(|e| {
                AppError::Checkpoint(format!("cannot reset {}: {e}", path.display()))
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Unique temp dir per test: the store is file-backed, so parallel tests must not share.
    /// Best-effort cleanup; a leftover dir is harmless.
    fn test_store() -> (JsonCheckpointStore, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "rel_checkpoint_test_{}_{}",
            std::process::id(),
            TEST_DIR_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = JsonCheckpointStore::new(&dir).unwrap();
        (store, dir)
    }

    #[tokio::test]
    async fn test_begin_run_complete_cycle() {
        let (store, dir) = test_store();
        let key = JobKey::new("job_orders");

        assert!(store.read(&key).await.unwrap().is_none());
        let checkpoint = store
            .begin(&key, &["split-0".to_string(), "split-1".to_string()])
            .await
            .unwrap();
        assert_eq!(checkpoint.splits.len(), 2);
        assert!(
            checkpoint
                .splits
                .iter()
                .all(|s| s.state == SplitState::Pending)
        );

        store.mark_running(&key, "split-0").await.unwrap();
        store.mark_completed(&key, "split-0", 10).await.unwrap();

        let checkpoint = store.read(&key).await.unwrap().unwrap();
        let s0 = checkpoint
            .splits
            .iter()
            .find(|s| s.split_id == "split-0")
            .unwrap();
        assert_eq!(s0.state, SplitState::Completed);
        assert_eq!(s0.rows_extracted, 10);
        assert!(!checkpoint.all_completed());

        // A retry preserves the completed split and re-queues the rest.
        let checkpoint = store
            .begin(&key, &["split-0".to_string(), "split-1".to_string()])
            .await
            .unwrap();
        let s0 = checkpoint
            .splits
            .iter()
            .find(|s| s.split_id == "split-0")
            .unwrap();
        assert_eq!(s0.state, SplitState::Completed);
        assert_eq!(s0.rows_extracted, 10);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_failed_split_can_retry() {
        let (store, dir) = test_store();
        let key = JobKey::new("job_orders");

        store.begin(&key, &["split-0".to_string()]).await.unwrap();
        store.mark_running(&key, "split-0").await.unwrap();
        store.mark_failed(&key, "split-0", "boom").await.unwrap();

        let checkpoint = store.read(&key).await.unwrap().unwrap();
        assert_eq!(checkpoint.splits[0].state, SplitState::Failed);

        // Retry moves it back to Pending via begin, then Running again.
        store.begin(&key, &["split-0".to_string()]).await.unwrap();
        store.mark_running(&key, "split-0").await.unwrap();
        store.mark_completed(&key, "split-0", 5).await.unwrap();
        let checkpoint = store.read(&key).await.unwrap().unwrap();
        assert!(checkpoint.all_completed());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_unknown_split_is_an_error() {
        let (store, dir) = test_store();
        let key = JobKey::new("job_orders");

        store.begin(&key, &["split-0".to_string()]).await.unwrap();
        let err = store.mark_completed(&key, "nope", 1).await.unwrap_err();
        assert!(err.to_string().contains("unknown split"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_reset_clears() {
        let (store, dir) = test_store();
        let key = JobKey::new("job_orders");

        store.begin(&key, &["split-0".to_string()]).await.unwrap();
        store.reset(&key).await.unwrap();
        assert!(store.read(&key).await.unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
