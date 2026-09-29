//! Exclusive per-job run lock.
//!
//! A run that commits split states holds `<stem>.lock` in the checkpoint directory for its
//! whole duration. The file is created with `O_CREAT | O_EXCL` (`create_new`), so of two
//! concurrent runs of the same job exactly one gets it; the other fails with
//! [`CheckpointError::LockHeld`] before touching the source.
//!
//! The lock records its owner (`owner_id`, `host`, `pid`, `acquired_at`) and a
//! `heartbeat_at` that a background task refreshes every `ttl / 4`. A lock whose heartbeat is
//! older than `ttl` — or whose owner is a dead process on this same host (Linux) — was left by
//! a crashed run and may be taken over (logged as a warning). A holder that finds its lock
//! taken over marks itself lost (`JobLock::ensure_held` then fails), so it stops committing.
//!
//! [`JobLock`] is an RAII guard: [`JobLock::release`] removes the file; dropping the guard
//! without releasing (panic, cancellation) stops the heartbeat and removes the file
//! best-effort. A process that dies outright leaves the file behind until it goes stale.
//!
//! Takeover is best-effort on filesystems without atomic rename (and assumes clocks agree
//! across hosts within a fraction of the TTL).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::{CheckpointError, LockHolder};
use crate::types::JobId;

/// Contents of a lock file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockInfo {
    pub owner_id: String,
    pub host: String,
    pub pid: u32,
    pub acquired_at: DateTime<Utc>,
    pub heartbeat_at: DateTime<Utc>,
}

/// RAII guard for a job's run lock. See the module docs.
#[derive(Debug)]
pub struct JobLock {
    job_id: JobId,
    path: PathBuf,
    owner_id: String,
    lost: Arc<AtomicBool>,
    stop: Option<oneshot::Sender<()>>,
    heartbeat: Option<JoinHandle<()>>,
}

fn io_err(op: &'static str, path: &Path, source: std::io::Error) -> CheckpointError {
    CheckpointError::Io {
        op,
        path: path.to_path_buf(),
        source,
    }
}

fn hostname() -> String {
    #[cfg(target_os = "linux")]
    if let Ok(h) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
        let h = h.trim();
        if !h.is_empty() {
            return h.to_string();
        }
    }
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

/// `Some(false)` when `pid` is known dead on this host, `Some(true)` when alive, `None` when
/// this platform cannot tell.
fn pid_alive(pid: u32) -> Option<bool> {
    #[cfg(target_os = "linux")]
    {
        Some(Path::new("/proc").join(pid.to_string()).exists())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        None
    }
}

/// Why an existing lock may be taken over, or `None` when it is live.
fn stale_reason(info: &LockInfo, ttl: Duration, now: DateTime<Utc>, host: &str) -> Option<String> {
    let age = now.signed_duration_since(info.heartbeat_at);
    if age.to_std().is_ok_and(|age| age > ttl) {
        return Some(format!(
            "heartbeat is {}s old (ttl {}s)",
            age.num_seconds(),
            ttl.as_secs()
        ));
    }
    if info.host == host && info.pid != std::process::id() && pid_alive(info.pid) == Some(false) {
        return Some(format!(
            "owner pid {} on this host is not running",
            info.pid
        ));
    }
    None
}

async fn read_info(path: &Path) -> Result<Option<LockInfo>, CheckpointError> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_err("read lock", path, e)),
    }
}

/// Modification age of a file whose content could not be parsed (e.g. a creator that has not
/// finished writing it yet).
async fn mtime_age(path: &Path) -> Option<Duration> {
    let meta = tokio::fs::metadata(path).await.ok()?;
    meta.modified().ok()?.elapsed().ok()
}

async fn write_new(path: &Path, info: &LockInfo) -> Result<bool, CheckpointError> {
    let open = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await;
    let mut file = match open {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
        Err(e) => return Err(io_err("create lock", path, e)),
    };
    let bytes = serde_json::to_vec_pretty(info).map_err(|source| CheckpointError::Serialize {
        job_id: info.owner_id.clone(),
        source,
    })?;
    file.write_all(&bytes)
        .await
        .map_err(|e| io_err("write lock", path, e))?;
    file.sync_all()
        .await
        .map_err(|e| io_err("fsync lock", path, e))?;
    Ok(true)
}

/// Atomically replace the lock content (tmp + rename), used by the heartbeat.
async fn rewrite(path: &Path, info: &LockInfo) -> Result<(), CheckpointError> {
    let tmp = path.with_extension(format!("lock.hb.{}", uuid::Uuid::new_v4().simple()));
    let bytes = serde_json::to_vec_pretty(info).map_err(|source| CheckpointError::Serialize {
        job_id: info.owner_id.clone(),
        source,
    })?;
    tokio::fs::write(&tmp, &bytes)
        .await
        .map_err(|e| io_err("write lock heartbeat", &tmp, e))?;
    if let Err(e) = tokio::fs::rename(&tmp, path).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(io_err("replace lock", path, e));
    }
    Ok(())
}

impl JobLock {
    /// Acquire the lock for `job_id` in `dir` (the checkpoint directory), taking over a stale
    /// lock if necessary, and start the heartbeat.
    pub(crate) async fn acquire(
        dir: &Path,
        job_id: &JobId,
        ttl: Duration,
    ) -> Result<Self, CheckpointError> {
        let path = dir.join(format!("{}.lock", super::file_stem(job_id)));
        let host = hostname();
        let owner_id = uuid::Uuid::new_v4().simple().to_string();

        for _attempt in 0..3 {
            let now = Utc::now();
            let info = LockInfo {
                owner_id: owner_id.clone(),
                host: host.clone(),
                pid: std::process::id(),
                acquired_at: now,
                heartbeat_at: now,
            };
            if write_new(&path, &info).await? {
                log::info!(
                    "job '{job_id}': acquired run lock {} (owner {owner_id})",
                    path.display()
                );
                return Ok(Self::start(job_id.clone(), path, owner_id, ttl));
            }

            // Held by someone: live, stale, or mid-creation.
            let existing = read_info(&path).await?;
            let Some(existing) = existing else {
                // Vanished (released) or not fully written yet.
                if mtime_age(&path).await.is_some_and(|age| age > ttl) {
                    log::warn!(
                        "job '{job_id}': removing unreadable stale lock {}",
                        path.display()
                    );
                    let _ = tokio::fs::remove_file(&path).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                continue;
            };
            let Some(reason) = stale_reason(&existing, ttl, now, &host) else {
                return Err(CheckpointError::LockHeld {
                    job_id: job_id.to_string(),
                    path,
                    holder: Box::new(LockHolder {
                        owner: existing.owner_id,
                        host: existing.host,
                        pid: existing.pid,
                        heartbeat_at: existing.heartbeat_at.to_rfc3339(),
                    }),
                });
            };
            log::warn!(
                "job '{job_id}': taking over stale run lock {} from owner {} (host {}, pid {}): {reason}",
                path.display(),
                existing.owner_id,
                existing.host,
                existing.pid
            );
            // Move the stale file aside (atomic), then verify it is the one we judged stale:
            // if another process took it over in between, we moved its fresh lock and must
            // put it back.
            let aside =
                path.with_extension(format!("lock.stale.{}", uuid::Uuid::new_v4().simple()));
            match tokio::fs::rename(&path, &aside).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(io_err("move stale lock", &path, e)),
            }
            let moved = read_info(&aside).await?;
            if moved.as_ref().map(|m| &m.owner_id) != Some(&existing.owner_id) {
                // Restore (hard_link fails if a lock already exists again: then that one wins).
                let _ = tokio::fs::hard_link(&aside, &path).await;
                let _ = tokio::fs::remove_file(&aside).await;
                continue;
            }
            let _ = tokio::fs::remove_file(&aside).await;
        }

        let existing = read_info(&path).await?;
        Err(CheckpointError::LockHeld {
            job_id: job_id.to_string(),
            path,
            holder: Box::new(match existing {
                Some(i) => LockHolder {
                    owner: i.owner_id,
                    host: i.host,
                    pid: i.pid,
                    heartbeat_at: i.heartbeat_at.to_rfc3339(),
                },
                None => LockHolder {
                    owner: "unknown".into(),
                    host: "unknown".into(),
                    pid: 0,
                    heartbeat_at: "unknown".into(),
                },
            }),
        })
    }

    fn start(job_id: JobId, path: PathBuf, owner_id: String, ttl: Duration) -> Self {
        let lost = Arc::new(AtomicBool::new(false));
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let every = (ttl / 4).max(Duration::from_secs(1));
        let hb_path = path.clone();
        let hb_owner = owner_id.clone();
        let hb_lost = lost.clone();
        let hb_job = job_id.clone();
        let heartbeat = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(every);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await; // the first tick fires immediately
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return,
                    _ = ticker.tick() => {}
                }
                match read_info(&hb_path).await {
                    Ok(Some(mut info)) if info.owner_id == hb_owner => {
                        info.heartbeat_at = Utc::now();
                        if let Err(e) = rewrite(&hb_path, &info).await {
                            // Retried next tick; if it keeps failing the lock goes stale and
                            // a takeover is detected below.
                            log::warn!("job '{hb_job}': lock heartbeat failed: {e}");
                        }
                    }
                    Ok(_) => {
                        log::error!(
                            "job '{hb_job}': run lock {} was taken over by another run",
                            hb_path.display()
                        );
                        hb_lost.store(true, Ordering::SeqCst);
                        return;
                    }
                    Err(e) => log::warn!("job '{hb_job}': cannot read run lock: {e}"),
                }
            }
        });
        Self {
            job_id,
            path,
            owner_id,
            lost,
            stop: Some(stop_tx),
            heartbeat: Some(heartbeat),
        }
    }

    /// The lock file path.
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
    /// let dir = std::env::temp_dir().join(format!("el-ballista-doc-lock-path-{}", std::process::id()));
    /// let store = JsonCheckpointStore::new(&dir)?;
    /// let lock = store.lock(&JobId::new("orders")?, Duration::from_secs(60)).await?;
    /// assert!(lock.path().starts_with(&dir) && lock.path().extension() == Some("lock".as_ref()));
    /// lock.release().await?;
    /// # Ok(()) }
    /// ```
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// `Err(LockLost)` once another run has taken this lock over.
    pub(crate) fn ensure_held(&self) -> Result<(), CheckpointError> {
        if self.lost.load(Ordering::SeqCst) {
            Err(CheckpointError::LockLost {
                job_id: self.job_id.to_string(),
            })
        } else {
            Ok(())
        }
    }

    /// Stop the heartbeat and remove the lock file (only if this run still owns it).
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
    /// let dir = std::env::temp_dir().join(format!("el-ballista-doc-lock-release-{}", std::process::id()));
    /// let store = JsonCheckpointStore::new(&dir)?;
    /// let lock = store.lock(&JobId::new("orders")?, Duration::from_secs(60)).await?;
    /// let path = lock.path().to_path_buf();
    /// lock.release().await?;
    /// assert!(!path.exists());
    /// # Ok(()) }
    /// ```
    pub async fn release(mut self) -> Result<(), CheckpointError> {
        self.stop_heartbeat().await;
        let owned =
            matches!(read_info(&self.path).await?, Some(info) if info.owner_id == self.owner_id);
        if owned {
            match tokio::fs::remove_file(&self.path).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_err("remove lock", &self.path, e)),
            }
            log::info!("job '{}': released run lock", self.job_id);
        }
        Ok(())
    }

    async fn stop_heartbeat(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(handle) = self.heartbeat.take() {
            // The task only exits on stop/loss; a join error means it panicked, which only
            // affects liveness of the heartbeat, never lock ownership.
            if let Err(e) = handle.await {
                log::warn!("job '{}': lock heartbeat task failed: {e}", self.job_id);
            }
        }
    }
}

impl Drop for JobLock {
    fn drop(&mut self) {
        // Not released explicitly (panic, cancelled future): stop the heartbeat and remove
        // the file if we still own it. Synchronous and best-effort by necessity.
        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
            if let Ok(bytes) = std::fs::read(&self.path)
                && let Ok(info) = serde_json::from_slice::<LockInfo>(&bytes)
                && info.owner_id == self.owner_id
            {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    static N: AtomicU64 = AtomicU64::new(0);

    fn dir() -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "el_ballista_lock_test_{}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn second_acquire_fails_while_held_and_succeeds_after_release() {
        let d = dir();
        let job = JobId::new("orders.v1").unwrap();
        let ttl = Duration::from_secs(60);
        let first = JobLock::acquire(&d, &job, ttl).await.unwrap();
        assert!(first.path().exists());
        let err = JobLock::acquire(&d, &job, ttl).await.unwrap_err();
        assert!(matches!(err, CheckpointError::LockHeld { .. }), "{err}");
        // A different job is independent (and its file name does not collide).
        let other = JobLock::acquire(&d, &JobId::new("orders_v1").unwrap(), ttl)
            .await
            .unwrap();
        first.release().await.unwrap();
        let again = JobLock::acquire(&d, &job, ttl).await.unwrap();
        again.release().await.unwrap();
        other.release().await.unwrap();
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn stale_heartbeat_is_taken_over_and_the_old_holder_notices() {
        let d = dir();
        let job = JobId::new("j").unwrap();
        let path = d.join("j.lock");
        let old = Utc::now() - chrono::Duration::seconds(3600);
        let stale = LockInfo {
            owner_id: "crashed".into(),
            host: "elsewhere".into(),
            pid: 1,
            acquired_at: old,
            heartbeat_at: old,
        };
        std::fs::write(&path, serde_json::to_vec(&stale).unwrap()).unwrap();
        let lock = JobLock::acquire(&d, &job, Duration::from_secs(60))
            .await
            .unwrap();
        let info: LockInfo = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_ne!(info.owner_id, "crashed");
        lock.ensure_held().unwrap();
        lock.release().await.unwrap();
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn fresh_lock_from_a_live_process_is_not_taken_over() {
        let d = dir();
        let job = JobId::new("j").unwrap();
        let now = Utc::now();
        let live = LockInfo {
            owner_id: "other".into(),
            host: "another-host".into(),
            pid: 1,
            acquired_at: now,
            heartbeat_at: now,
        };
        std::fs::write(d.join("j.lock"), serde_json::to_vec(&live).unwrap()).unwrap();
        let err = JobLock::acquire(&d, &job, Duration::from_secs(60))
            .await
            .unwrap_err();
        match err {
            CheckpointError::LockHeld { holder, .. } => assert_eq!(holder.owner, "other"),
            other => panic!("expected LockHeld, got {other}"),
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn heartbeat_detects_takeover_and_drop_removes_owned_file() {
        let d = dir();
        let job = JobId::new("j").unwrap();
        let lock = JobLock::acquire(&d, &job, Duration::from_secs(4))
            .await
            .unwrap();
        // Another run overwrites the lock (as a takeover would).
        let now = Utc::now();
        let thief = LockInfo {
            owner_id: "thief".into(),
            host: "h".into(),
            pid: 1,
            acquired_at: now,
            heartbeat_at: now,
        };
        std::fs::write(lock.path(), serde_json::to_vec(&thief).unwrap()).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(matches!(
            lock.ensure_held(),
            Err(CheckpointError::LockLost { .. })
        ));
        let path = lock.path().to_path_buf();
        drop(lock);
        // Not ours any more: dropping must not delete the thief's lock.
        assert!(path.exists());
        let _ = std::fs::remove_dir_all(&d);

        let d = dir();
        let lock = JobLock::acquire(&d, &job, Duration::from_secs(60))
            .await
            .unwrap();
        let path = lock.path().to_path_buf();
        drop(lock);
        assert!(!path.exists(), "dropped guard removes its own lock");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn dead_owner_on_same_host_is_stale() {
        let now = Utc::now();
        let info = LockInfo {
            owner_id: "x".into(),
            host: "me".into(),
            pid: u32::MAX - 7, // never a live pid
            acquired_at: now,
            heartbeat_at: now,
        };
        if cfg!(target_os = "linux") {
            assert!(stale_reason(&info, Duration::from_secs(60), now, "me").is_some());
        }
        assert!(stale_reason(&info, Duration::from_secs(60), now, "other-host").is_none());
    }
}
