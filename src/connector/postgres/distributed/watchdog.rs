//! Distributed job watchdog: a hung Ballista job is cancelled and re-run, and after
//! `distributed.max_retries` re-runs the extraction aborts with a typed error.
//! distributed/watchdog.rs
//!
//! Why it exists: Ballista 54 notices a dead executor only through missed heartbeats, and
//! then puts that executor's tasks back in the queue but never hands them out again (its
//! executor-lost handler does not trigger a new round of task offers). The job stays
//! "Running" forever, and the client's status poll has no timeout, so the extraction hangs.
//!
//! How it works, per attempt:
//! - The executors the scheduler lists when the attempt starts are its baseline
//!   (`GET /api/executors`, polled every second).
//! - The attempt counts as hung when a baseline executor disappears from the listing, when
//!   its heartbeat timestamp has not advanced for `distributed.executor_timeout_secs`, or
//!   when the attempt passes `distributed.job_timeout_secs`. A job error that names a lost
//!   executor or a failed partition fetch counts too.
//! - A hung attempt is cancelled on the scheduler (`PATCH /api/job/{id}`; the job is found
//!   by the session's unique `ballista.job.name`). The watchdog then waits until the
//!   scheduler has dropped the dead executor, so the re-run is not placed on it, and submits
//!   the query again. After `max_retries` re-runs the stream yields
//!   [`ExtractorError::DistributedJobAborted`].
//! - Only an attempt that has not yielded a batch yet is re-run. Ballista yields results
//!   only after the job has finished, so this covers the whole execution; a hang while the
//!   results are being fetched is an error, because a re-run would deliver rows twice.
//! - Without the REST API the executors are invisible: only `job_timeout_secs` applies, and
//!   a warning says so once.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;

use crate::config::DistributedConfig;
use crate::connector::errors::ExtractorError;

use super::executors::{rest_authority, rest_call};

/// How often the scheduler is polled while an attempt runs.
const POLL: Duration = Duration::from_secs(1);
/// Upper bound for waiting until the scheduler drops a dead executor before re-running. It
/// covers Ballista's own default executor timeout (180 s) plus its 15 s expiry check, for
/// schedulers not started by `el-ballista scheduler` (whose default timeout is 30 s).
const REMOVAL_WAIT: Duration = Duration::from_secs(240);

/// Watchdog settings of one distributed extraction.
#[derive(Debug, Clone)]
pub(crate) struct WatchSettings {
    pub(crate) scheduler_url: String,
    /// The session's unique `ballista.job.name`: finds this extraction's jobs to cancel.
    pub(crate) job_name: String,
    pub(crate) max_retries: u32,
    pub(crate) executor_timeout: Duration,
    pub(crate) job_timeout: Option<Duration>,
    pub(crate) poll: Duration,
    pub(crate) removal_wait: Duration,
}

impl WatchSettings {
    pub(crate) fn new(config: &DistributedConfig, scheduler_url: &str, job_name: &str) -> Self {
        Self {
            scheduler_url: scheduler_url.to_string(),
            job_name: job_name.to_string(),
            max_retries: config.max_retries,
            executor_timeout: Duration::from_secs(config.executor_timeout_secs.max(1)),
            job_timeout: config.job_timeout_secs.map(Duration::from_secs),
            poll: POLL,
            removal_wait: REMOVAL_WAIT,
        }
    }
}

/// One executor as the scheduler lists it: its id and last heartbeat timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutorBeat {
    pub(crate) id: String,
    pub(crate) last_seen: Option<u64>,
}

/// Why an attempt counts as hung.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Hang {
    /// A baseline executor the scheduler no longer lists.
    ExecutorRemoved(String),
    /// A baseline executor whose heartbeat has not advanced for this long.
    ExecutorSilent(String, Duration),
    /// The attempt passed `job_timeout_secs`.
    Timeout(Duration),
    /// The job failed with an executor-loss or partition-fetch error.
    ExecutorFailure(String),
}

impl Hang {
    fn dead_executor(&self) -> Option<&str> {
        match self {
            Hang::ExecutorRemoved(id) | Hang::ExecutorSilent(id, _) => Some(id),
            Hang::Timeout(_) | Hang::ExecutorFailure(_) => None,
        }
    }

    fn describe(&self) -> String {
        match self {
            Hang::ExecutorRemoved(id) => {
                format!("worker {id} was removed by the scheduler (it died or stopped responding)")
            }
            Hang::ExecutorSilent(id, quiet) => {
                format!("worker {id} sent no heartbeat for {}s", quiet.as_secs())
            }
            Hang::Timeout(limit) => format!(
                "the attempt passed distributed.job_timeout_secs ({}s)",
                limit.as_secs()
            ),
            Hang::ExecutorFailure(msg) => format!("the job failed on a worker failure: {msg}"),
        }
    }
}

/// Liveness of one attempt, from successive executor listings. Pure: the clock is passed in.
#[derive(Debug)]
pub(crate) struct Liveness {
    executor_timeout: Duration,
    job_timeout: Option<Duration>,
    started: Instant,
    /// Baseline executor id -> (last heartbeat value seen, when it last changed).
    beats: HashMap<String, (Option<u64>, Instant)>,
}

impl Liveness {
    pub(crate) fn new(
        now: Instant,
        executor_timeout: Duration,
        job_timeout: Option<Duration>,
    ) -> Self {
        Self {
            executor_timeout,
            job_timeout,
            started: now,
            beats: HashMap::new(),
        }
    }

    /// Feed one listing (`None` = the REST API did not answer) and report a hang, if any. The
    /// first non-empty listing becomes the baseline.
    pub(crate) fn observe(
        &mut self,
        now: Instant,
        listing: Option<&[ExecutorBeat]>,
    ) -> Option<Hang> {
        if let Some(limit) = self.job_timeout
            && now.duration_since(self.started) >= limit
        {
            return Some(Hang::Timeout(limit));
        }
        let listing = listing?;
        if self.beats.is_empty() {
            self.beats = listing
                .iter()
                .map(|e| (e.id.clone(), (e.last_seen, now)))
                .collect();
            return None;
        }
        let present: HashMap<&str, Option<u64>> = listing
            .iter()
            .map(|e| (e.id.as_str(), e.last_seen))
            .collect();
        let mut ids: Vec<String> = self.beats.keys().cloned().collect();
        ids.sort();
        for id in ids {
            let Some(seen) = present.get(id.as_str()) else {
                return Some(Hang::ExecutorRemoved(id));
            };
            let entry = self.beats.get_mut(&id).expect("id comes from beats");
            if *seen != entry.0 {
                *entry = (*seen, now);
            } else {
                let quiet = now.duration_since(entry.1);
                if quiet >= self.executor_timeout {
                    return Some(Hang::ExecutorSilent(id, quiet));
                }
            }
        }
        None
    }
}

/// Parse `GET /api/executors`: an array of `{ "id": ..., "last_seen": <ms or null> }`.
pub(crate) fn parse_beats(body: &[u8]) -> Result<Vec<ExecutorBeat>, String> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("invalid JSON: {e}"))?;
    value
        .as_array()
        .ok_or("expected a JSON array")?
        .iter()
        .map(|e| {
            Ok(ExecutorBeat {
                id: e
                    .get("id")
                    .and_then(|v| v.as_str())
                    .ok_or("executor without an id")?
                    .to_string(),
                last_seen: e.get("last_seen").and_then(|v| v.as_u64()),
            })
        })
        .collect()
}

/// Parse `GET /api/jobs` and return the ids of this extraction's jobs that are still active.
pub(crate) fn parse_active_jobs(body: &[u8], job_name: &str) -> Result<Vec<String>, String> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("invalid JSON: {e}"))?;
    Ok(value
        .as_array()
        .ok_or("expected a JSON array")?
        .iter()
        .filter(|j| j.get("job_name").and_then(|v| v.as_str()) == Some(job_name))
        .filter(|j| {
            matches!(
                j.get("status").and_then(|v| v.as_str()),
                Some("Running" | "Queued")
            )
        })
        .filter_map(|j| j.get("job_id").and_then(|v| v.as_str()).map(str::to_string))
        .collect())
}

/// The scheduler's REST endpoint of one extraction.
#[derive(Debug, Clone)]
struct Rest {
    authority: Option<String>,
}

impl Rest {
    fn new(scheduler_url: &str) -> Self {
        Self {
            authority: rest_authority(scheduler_url).map(str::to_string),
        }
    }

    async fn executors(&self) -> Option<Vec<ExecutorBeat>> {
        let authority = self.authority.as_deref()?;
        let body = rest_call(authority, "GET", "/api/executors").await.ok()?;
        parse_beats(&body).ok()
    }

    /// Cancel every still-active job named `job_name`; returns how many were cancelled.
    async fn cancel(&self, job_name: &str) -> usize {
        let Some(authority) = self.authority.as_deref() else {
            return 0;
        };
        let listing = match rest_call(authority, "GET", "/api/jobs").await {
            Ok(body) => parse_active_jobs(&body, job_name),
            Err(e) => Err(e.to_string()),
        };
        let jobs = match listing {
            Ok(jobs) => jobs,
            Err(e) => {
                warn!(job = %job_name, error = %e, "could not list the scheduler's jobs to cancel");
                return 0;
            }
        };
        let mut cancelled = 0;
        for id in jobs {
            match rest_call(authority, "PATCH", &format!("/api/job/{id}")).await {
                Ok(_) => cancelled += 1,
                Err(e) => {
                    warn!(job = %job_name, ballista_job = %id, error = %e, "cancelling a job failed")
                }
            }
        }
        cancelled
    }
}

/// Watches one attempt.
struct Monitor {
    rest: Rest,
    liveness: Liveness,
    poll: Duration,
    job_name: String,
    warned: bool,
}

impl Monitor {
    async fn begin(settings: &WatchSettings) -> Self {
        let rest = Rest::new(&settings.scheduler_url);
        let mut monitor = Self {
            rest,
            liveness: Liveness::new(
                Instant::now(),
                settings.executor_timeout,
                settings.job_timeout,
            ),
            poll: settings.poll,
            job_name: settings.job_name.clone(),
            warned: false,
        };
        // The baseline: the executors registered as the attempt starts.
        let _ = monitor.check().await;
        monitor
    }

    async fn check(&mut self) -> Option<Hang> {
        let listing = self.rest.executors().await;
        if listing.is_none() && !self.warned {
            self.warned = true;
            warn!(
                job = %self.job_name,
                "the scheduler REST API does not answer, so dead workers cannot be detected; \
                 only distributed.job_timeout_secs applies"
            );
        }
        self.liveness.observe(Instant::now(), listing.as_deref())
    }

    /// Resolves once the attempt is hung. Cancel-safe: all state lives in `self`.
    async fn wait_for_hang(&mut self) -> Hang {
        loop {
            tokio::time::sleep(self.poll).await;
            if let Some(hang) = self.check().await {
                return hang;
            }
        }
    }
}

/// A job error caused by a worker failure rather than by the query itself.
fn is_executor_failure(e: &DataFusionError) -> bool {
    let msg = e.to_string();
    ["ExecutorLost", "FetchPartitionError", "executor lost"]
        .iter()
        .any(|m| msg.contains(m))
}

/// Whether a job error came from a worker failure (then the attempt can be re-run): the
/// error names one, or a worker the attempt started with is gone. A query error is `None`.
async fn worker_failure(e: &DataFusionError, monitor: &mut Monitor) -> Option<Hang> {
    let hang = monitor.check().await;
    if is_executor_failure(e) {
        Some(hang.unwrap_or_else(|| Hang::ExecutorFailure(e.to_string())))
    } else {
        hang.filter(|h| h.dead_executor().is_some())
    }
}

fn aborted(attempts: u32, reason: String) -> DataFusionError {
    DataFusionError::External(Box::new(ExtractorError::DistributedJobAborted {
        attempts,
        reason,
    }))
}

enum State {
    /// Submit attempt `attempt` (1-based); `last` is why the previous one was abandoned.
    Start {
        attempt: u32,
        last: Option<String>,
    },
    Running {
        attempt: u32,
        stream: SendableRecordBatchStream,
        monitor: Box<Monitor>,
        delivered: bool,
    },
    /// Yield this error, then end.
    Fail(DataFusionError),
    Done,
}

type StartFn = dyn Fn() -> futures::future::BoxFuture<'static, Result<SendableRecordBatchStream, DataFusionError>>
    + Send
    + Sync;

struct Seed {
    state: State,
    settings: Arc<WatchSettings>,
    start: Arc<StartFn>,
}

/// Cancel the hung attempt, wait for the scheduler to drop its dead worker, and decide what
/// comes next: another attempt, or an abort when no worker is left.
async fn recover(settings: &WatchSettings, attempt: u32, hang: &Hang) -> State {
    let rest = Rest::new(&settings.scheduler_url);
    let reason = hang.describe();
    let cancelled = rest.cancel(&settings.job_name).await;
    warn!(
        job = %settings.job_name,
        attempt,
        reason = %reason,
        cancelled,
        "distributed attempt hung; cancelled on the scheduler"
    );
    let total = settings.max_retries + 1;
    if attempt >= total {
        return State::Fail(aborted(attempt, reason));
    }
    if let Some(dead) = hang.dead_executor() {
        let deadline = Instant::now() + settings.removal_wait;
        let mut logged = false;
        loop {
            match rest.executors().await {
                Some(listing) if !listing.iter().any(|e| e.id == dead) => break,
                _ if Instant::now() >= deadline => {
                    warn!(
                        job = %settings.job_name,
                        worker = %dead,
                        waited_secs = settings.removal_wait.as_secs(),
                        "the scheduler still lists the dead worker; re-running anyway"
                    );
                    break;
                }
                _ => {
                    if !logged {
                        logged = true;
                        info!(
                            job = %settings.job_name,
                            worker = %dead,
                            "waiting for the scheduler to drop the dead worker before re-running"
                        );
                    }
                    tokio::time::sleep(settings.poll).await;
                }
            }
        }
    }
    if rest.executors().await.is_some_and(|l| l.is_empty()) {
        return State::Fail(aborted(
            attempt,
            format!("{reason}; no worker is left registered with the scheduler"),
        ));
    }
    State::Start {
        attempt: attempt + 1,
        last: Some(reason),
    }
}

/// One step of the watched stream: the next item and the state after it.
async fn step(mut seed: Seed) -> Option<(Result<RecordBatch, DataFusionError>, Seed)> {
    loop {
        match std::mem::replace(&mut seed.state, State::Done) {
            State::Done => return None,
            State::Fail(e) => return Some((Err(e), seed)),
            State::Start { attempt, last } => {
                let total = seed.settings.max_retries + 1;
                if attempt > total {
                    let reason = last.unwrap_or_else(|| "hung".to_string());
                    seed.state = State::Fail(aborted(total, reason));
                    continue;
                }
                if attempt > 1 {
                    warn!(job = %seed.settings.job_name, attempt, total, "re-running the distributed job");
                }
                let monitor = Box::new(Monitor::begin(&seed.settings).await);
                match (seed.start)().await {
                    Ok(stream) => {
                        seed.state = State::Running {
                            attempt,
                            stream,
                            monitor,
                            delivered: false,
                        }
                    }
                    Err(e) => return Some((Err(e), seed)),
                }
            }
            State::Running {
                attempt,
                mut stream,
                mut monitor,
                delivered,
            } => {
                let hang = tokio::select! {
                    item = stream.next() => match item {
                        Some(Ok(batch)) => {
                            seed.state = State::Running { attempt, stream, monitor, delivered: true };
                            return Some((Ok(batch), seed));
                        }
                        None => return None,
                        Some(Err(e)) => match worker_failure(&e, &mut monitor).await {
                            Some(hang) if !delivered => hang,
                            _ => return Some((Err(e), seed)),
                        },
                    },
                    hang = monitor.wait_for_hang() => hang,
                };
                // Drop the client side of the hung attempt before cancelling it.
                drop(stream);
                if delivered {
                    let rest = Rest::new(&seed.settings.scheduler_url);
                    let cancelled = rest.cancel(&seed.settings.job_name).await;
                    warn!(
                        job = %seed.settings.job_name,
                        reason = %hang.describe(),
                        cancelled,
                        "distributed job hung after rows were delivered; cancelled, not re-run"
                    );
                    seed.state = State::Fail(aborted(
                        attempt,
                        format!(
                            "{} after rows were already delivered (not re-run, it would \
                             duplicate them)",
                            hang.describe()
                        ),
                    ));
                    continue;
                }
                seed.state = recover(&seed.settings, attempt, &hang).await;
            }
        }
    }
}

/// Run a distributed query under the watchdog. `start` submits one attempt (it is called
/// again for each re-run); `schema` is the query's output schema. Lazy: nothing is
/// submitted until the stream is polled.
pub(crate) fn watched_stream<F, Fut>(
    settings: WatchSettings,
    schema: SchemaRef,
    start: F,
) -> SendableRecordBatchStream
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<SendableRecordBatchStream, DataFusionError>> + Send + 'static,
{
    let start: Arc<StartFn> = Arc::new(move || Box::pin(start()));
    let seed = Seed {
        state: State::Start {
            attempt: 1,
            last: None,
        },
        settings: Arc::new(settings),
        start,
    };
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::unfold(seed, step),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn beat(id: &str, seen: u64) -> ExecutorBeat {
        ExecutorBeat {
            id: id.to_string(),
            last_seen: Some(seen),
        }
    }

    #[test]
    fn first_listing_is_the_baseline_and_healthy_beats_never_hang() {
        let t0 = Instant::now();
        let mut l = Liveness::new(t0, Duration::from_secs(30), None);
        assert_eq!(l.observe(t0, Some(&[beat("a", 1), beat("b", 1)])), None);
        for s in 1..20u64 {
            let now = t0 + Duration::from_secs(s * 5);
            assert_eq!(
                l.observe(now, Some(&[beat("a", s + 1), beat("b", s + 1)])),
                None
            );
        }
    }

    #[test]
    fn removed_executor_is_a_hang() {
        let t0 = Instant::now();
        let mut l = Liveness::new(t0, Duration::from_secs(30), None);
        l.observe(t0, Some(&[beat("a", 1), beat("b", 1)]));
        assert_eq!(
            l.observe(t0 + Duration::from_secs(1), Some(&[beat("b", 2)])),
            Some(Hang::ExecutorRemoved("a".into()))
        );
    }

    #[test]
    fn silent_executor_hangs_after_the_timeout_only() {
        let t0 = Instant::now();
        let mut l = Liveness::new(t0, Duration::from_secs(30), None);
        l.observe(t0, Some(&[beat("a", 1), beat("b", 1)]));
        // b keeps beating, a goes quiet.
        assert_eq!(
            l.observe(
                t0 + Duration::from_secs(29),
                Some(&[beat("a", 1), beat("b", 7)])
            ),
            None
        );
        assert_eq!(
            l.observe(
                t0 + Duration::from_secs(31),
                Some(&[beat("a", 1), beat("b", 8)])
            ),
            Some(Hang::ExecutorSilent("a".into(), Duration::from_secs(31)))
        );
    }

    #[test]
    fn rest_outage_is_no_information_but_the_job_timeout_still_applies() {
        let t0 = Instant::now();
        let mut l = Liveness::new(t0, Duration::from_secs(5), Some(Duration::from_secs(60)));
        l.observe(t0, Some(&[beat("a", 1)]));
        assert_eq!(l.observe(t0 + Duration::from_secs(40), None), None);
        assert_eq!(
            l.observe(t0 + Duration::from_secs(60), None),
            Some(Hang::Timeout(Duration::from_secs(60)))
        );
    }

    #[test]
    fn parses_executor_and_job_listings() {
        let executors = br#"[{"id":"a","host":"h","port":1,"last_seen":1790426736000,"specification":{"task_slots":4}},{"id":"b","last_seen":null}]"#;
        assert_eq!(
            parse_beats(executors).unwrap(),
            vec![
                beat("a", 1_790_426_736_000),
                ExecutorBeat {
                    id: "b".into(),
                    last_seen: None
                }
            ]
        );
        let jobs = br#"[
            {"job_id":"j1","job_name":"el-ballista-x","status":"Running"},
            {"job_id":"j2","job_name":"el-ballista-x","status":"Successful"},
            {"job_id":"j3","job_name":"other","status":"Running"},
            {"job_id":"j4","job_name":"el-ballista-x","status":"Queued"}]"#;
        assert_eq!(
            parse_active_jobs(jobs, "el-ballista-x").unwrap(),
            vec!["j1", "j4"]
        );
        assert!(parse_beats(b"{}").is_err());
    }

    #[test]
    fn executor_failures_are_recognised_in_job_errors() {
        assert!(is_executor_failure(&DataFusionError::Execution(
            "Job x failed: ExecutorLost(\"e1\")".into()
        )));
        assert!(!is_executor_failure(&DataFusionError::Execution(
            "column \"nope\" does not exist".into()
        )));
    }

    fn settings(url: &str, max_retries: u32) -> WatchSettings {
        WatchSettings {
            scheduler_url: url.to_string(),
            job_name: "el-ballista-test".into(),
            max_retries,
            executor_timeout: Duration::from_secs(30),
            job_timeout: Some(Duration::from_millis(150)),
            poll: Duration::from_millis(20),
            removal_wait: Duration::from_millis(50),
        }
    }

    fn empty_schema() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::empty())
    }

    /// A query that never finishes (like a job whose tasks are never handed out again).
    fn never_finishes() -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            empty_schema(),
            futures::stream::pending(),
        ))
    }

    #[tokio::test]
    async fn a_hung_job_is_retried_then_aborted() {
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let c = Arc::clone(&calls);
        // No REST API at this URL: only the job timeout can see the hang.
        let mut stream = watched_stream(
            settings("http://127.0.0.1:1", 2),
            empty_schema(),
            move || {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { Ok(never_finishes()) }
            },
        );
        let err = stream.next().await.expect("an item").expect_err("aborted");
        assert!(
            err.to_string()
                .contains("distributed job aborted after 3 attempt(s)"),
            "{err}"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn a_retry_that_succeeds_delivers_its_rows_once() {
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let c = Arc::clone(&calls);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Int32, false),
        ]));
        let s2 = Arc::clone(&schema);
        let mut stream = watched_stream(
            settings("http://127.0.0.1:1", 2),
            Arc::clone(&schema),
            move || {
                let n = c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let schema = Arc::clone(&s2);
                async move {
                    if n == 0 {
                        return Ok(never_finishes());
                    }
                    let batch = RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
                    )?;
                    Ok(Box::pin(RecordBatchStreamAdapter::new(
                        schema,
                        futures::stream::iter(vec![Ok(batch)]),
                    )) as SendableRecordBatchStream)
                }
            },
        );
        let mut rows = 0;
        while let Some(item) = stream.next().await {
            rows += item.unwrap().num_rows();
        }
        assert_eq!(rows, 3);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn query_errors_are_not_retried() {
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let c = Arc::clone(&calls);
        let mut stream = watched_stream(
            settings("http://127.0.0.1:1", 2),
            empty_schema(),
            move || {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async {
                    Ok(Box::pin(RecordBatchStreamAdapter::new(
                        empty_schema(),
                        futures::stream::iter(vec![Err(DataFusionError::Plan(
                            "bad column".into(),
                        ))]),
                    )) as SendableRecordBatchStream)
                }
            },
        );
        let err = stream.next().await.unwrap().unwrap_err();
        assert!(err.to_string().contains("bad column"));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
