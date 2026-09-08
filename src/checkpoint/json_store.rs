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
            AppError::Checkpoint(format!("cannot create checkpoint dir {}: {e}", dir.display()))
        })?;

        Ok(Self { dir })
    }

    fn path_for(&self, key: &JobKey) -> PathBuf {
        let file_name = format!("{}__{}.json", sanitize(&key.job_id), sanitize(&key.namespace));
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
        let tmp_path = path.with_extension("json.tmp");

        let text = serde_json::to_string_pretty(checkpoint)
            .map_err(|e| AppError::Checkpoint(format!("cannot serialize checkpoint: {e}")))?;

        fs::write(&tmp_path, text)
            .map_err(|e| AppError::Checkpoint(format!("cannot write {}: {e}", tmp_path.display())))?;

        fs::rename(&tmp_path, &path).map_err(|e| {
            AppError::Checkpoint(format!("cannot finalize {}: {e}", path.display()))
        })?;

        Ok(())
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
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

        if let Some(current) = &existing {
            if current.state == RunState::Running {
                if let Some(expires) = current.lease_expires_at {
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
            }
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
        let current = self
            .read_file(key)?
            .ok_or_else(|| AppError::Checkpoint(format!("no checkpoint to commit for job '{}'", key.job_id)))?;

        if current.run_id != Some(run_id) {
            return Err(AppError::Checkpoint(format!(
                "lost the lease for job '{}' (held by {:?}, tried to commit as {})",
                key.job_id, current.run_id, run_id
            )));
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
