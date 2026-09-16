//! File-backed `CheckpointStore`.
//! checkpoint/json_store.rs
//! One JSON file per (job_id, namespace), written atomically (write to a `.tmp` file, then
//! rename over the real path). Good enough for a single scheduler triggering one job at a time
//! on one machine — the deployment Phase 1 targets — but see the module-level note in
//! `checkpoint/mod.rs` on what this does *not* guarantee.

use std::fs;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use super::{Checkpoint, CheckpointStore, JobKey, RunState, RunStats};
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
        let file_name = format!(
            "{}__{}.json",
            sanitize(&key.job_id),
            sanitize(&key.namespace)
        );
        self.dir.join(file_name)
    }

    fn read_file(&self, key: &JobKey) -> Result<Option<Checkpoint>, AppError> {
        let path = self.path_for(key);

        if !path.exists() {
            return Ok(None);
        }

        let text = fs::read_to_string(&path)
            .map_err(|e| AppError::Checkpoint(format!("cannot read {}: {e}", path.display())))?;

        let checkpoint: Checkpoint = serde_json::from_str(&text).map_err(|e| {
            AppError::Checkpoint(format!("corrupt checkpoint file {}: {e}", path.display()))
        })?;

        Ok(Some(checkpoint))
    }

    fn write_file(&self, key: &JobKey, checkpoint: &Checkpoint) -> Result<(), AppError> {
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
    async fn acquire(
        &self,
        key: &JobKey,
        run_id: Uuid,
        lease: Duration,
        watermark_column: &str,
    ) -> Result<Checkpoint, AppError> {
        let now = Utc::now();
        let existing = self.read_file(key)?;

        if let Some(current) = &existing
            && current.state == RunState::Running
            && let Some(expires) = current.lease_expires_at
        {
            if expires > now {
                return Err(AppError::Checkpoint(format!(
                    "job '{}' namespace '{}' is already running (run_id={:?}, lease expires {})",
                    key.job_id, key.namespace, current.run_id, expires
                )));
            }

            log::warn!(
                "reclaiming expired lease for job '{}' namespace '{}' (previous run_id={:?})",
                key.job_id,
                key.namespace,
                current.run_id
            );
        }

        let checkpoint = Checkpoint {
            job_id: key.job_id.clone(),
            namespace: key.namespace.clone(),
            watermark_column: watermark_column.to_string(),
            watermark_value: existing.as_ref().and_then(|c| c.watermark_value),
            state: RunState::Running,
            run_id: Some(run_id),
            lease_expires_at: Some(now + lease),
            updated_at: now,
        };

        self.write_file(key, &checkpoint)?;

        Ok(checkpoint)
    }

    async fn commit(
        &self,
        key: &JobKey,
        run_id: Uuid,
        next: DateTime<Utc>,
        stats: RunStats,
    ) -> Result<(), AppError> {
        let current = self.read_file(key)?.ok_or_else(|| {
            AppError::Checkpoint(format!("no checkpoint to commit for job '{}'", key.job_id))
        })?;

        if current.run_id != Some(run_id) {
            return Err(AppError::Checkpoint(format!(
                "lost the lease for job '{}' (held by {:?}, tried to commit as {})",
                key.job_id, current.run_id, run_id
            )));
        }

        // Monotonicity: a stale or clamped `next` must not rewind the watermark.
        if let Some(prev) = current.watermark_value
            && next < prev
        {
            return Err(AppError::Checkpoint(format!(
                "watermark regression for job '{}': current {} -> next {}",
                key.job_id, prev, next
            )));
        }

        // Lease expiry: committing with an expired lease is a split-brain signal.
        if let Some(expires) = current.lease_expires_at
            && Utc::now() > expires
        {
            log::warn!(
                "committing watermark for job '{}' with expired lease (expired at {})",
                key.job_id, expires
            );
        }

        log::info!(
            "job '{}' committing watermark -> {} (rows_extracted={}, window=({:?}, {:?}])",
            key.job_id,
            next,
            stats.rows_extracted,
            stats.window_lo,
            stats.window_hi
        );

        let checkpoint = Checkpoint {
            watermark_value: Some(next),
            state: RunState::Committed,
            run_id: None,
            lease_expires_at: None,
            updated_at: Utc::now(),
            ..current
        };

        self.write_file(key, &checkpoint)
    }

    async fn abandon(&self, key: &JobKey, run_id: Uuid, err: &str) -> Result<(), AppError> {
        let current = match self.read_file(key)? {
            Some(current) => current,
            None => return Ok(()),
        };

        if current.run_id != Some(run_id) {
            // Someone else already reclaimed or committed the lease; nothing to abandon.
            return Ok(());
        }

        log::warn!("job '{}' abandoning run {}: {}", key.job_id, run_id, err);

        let checkpoint = Checkpoint {
            state: RunState::Failed,
            run_id: None,
            lease_expires_at: None,
            updated_at: Utc::now(),
            ..current
        };

        self.write_file(key, &checkpoint)
    }

    async fn read(&self, key: &JobKey) -> Result<Option<Checkpoint>, AppError> {
        self.read_file(key)
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

    fn stats() -> RunStats {
        RunStats {
            rows_extracted: 10,
            window_lo: None,
            window_hi: None,
        }
    }

    #[tokio::test]
    async fn test_acquire_commit_read_cycle() {
        let (store, dir) = test_store();
        let key = JobKey::new("job_orders");
        let run_id = Uuid::new_v4();

        // Fresh job: no checkpoint yet, acquire starts clean with no watermark.
        assert!(store.read(&key).await.unwrap().is_none());
        let checkpoint = store
            .acquire(&key, run_id, Duration::minutes(30), "updated_at")
            .await
            .unwrap();
        assert_eq!(checkpoint.state, RunState::Running);
        assert_eq!(checkpoint.watermark_value, None);

        // Commit advances the watermark and clears the lease.
        let hi = DateTime::<Utc>::from_timestamp(1000, 0).unwrap();
        store.commit(&key, run_id, hi, stats()).await.unwrap();

        let checkpoint = store.read(&key).await.unwrap().unwrap();
        assert_eq!(checkpoint.state, RunState::Committed);
        assert_eq!(checkpoint.watermark_value, Some(hi));
        assert_eq!(checkpoint.run_id, None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_double_acquire_live_lease_fails() {
        let (store, dir) = test_store();
        let key = JobKey::new("job_orders");

        store
            .acquire(&key, Uuid::new_v4(), Duration::minutes(30), "updated_at")
            .await
            .unwrap();

        // A second run while the lease is live must fail, not steal.
        let err = store
            .acquire(&key, Uuid::new_v4(), Duration::minutes(30), "updated_at")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already running"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_expired_lease_reclaimed() {
        let (store, dir) = test_store();
        let key = JobKey::new("job_orders");

        // Zero-second lease is already expired: the next acquire reclaims it.
        store
            .acquire(&key, Uuid::new_v4(), Duration::seconds(0), "updated_at")
            .await
            .unwrap();
        let checkpoint = store
            .acquire(&key, Uuid::new_v4(), Duration::minutes(30), "updated_at")
            .await
            .unwrap();
        assert_eq!(checkpoint.state, RunState::Running);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_commit_wrong_run_id_fails() {
        let (store, dir) = test_store();
        let key = JobKey::new("job_orders");
        let run_id = Uuid::new_v4();

        store
            .acquire(&key, run_id, Duration::minutes(30), "updated_at")
            .await
            .unwrap();

        // A stale run committing under its own id must not clobber the lease holder.
        let err = store
            .commit(&key, Uuid::new_v4(), Utc::now(), stats())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("lost the lease"));

        // The real holder still commits fine.
        store
            .commit(&key, run_id, Utc::now(), stats())
            .await
            .unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_abandon_is_idempotent() {
        let (store, dir) = test_store();
        let key = JobKey::new("job_orders");
        let run_id = Uuid::new_v4();

        // Abandoning a job that was never acquired is a no-op, not an error.
        store
            .abandon(&key, run_id, "nothing to abandon")
            .await
            .unwrap();

        store
            .acquire(&key, run_id, Duration::minutes(30), "updated_at")
            .await
            .unwrap();
        store.abandon(&key, run_id, "boom").await.unwrap();

        let checkpoint = store.read(&key).await.unwrap().unwrap();
        assert_eq!(checkpoint.state, RunState::Failed);

        // Second abandon: lease already gone, still fine.
        store.abandon(&key, run_id, "boom again").await.unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }
}
