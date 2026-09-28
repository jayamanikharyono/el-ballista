//! A real Ballista cluster for distributed-path tests: one `rel scheduler` and N `rel worker`
//! **child processes** of this crate's own binary, on free localhost ports. There is no
//! in-process Ballista any more, so this is the only way tests exercise plan shipping
//! (codecs, keyset distribution, per-process budgets) — the same deployment shape as
//! `benchmark/run.sh`, minus the containers.
//!
//! Children inherit the environment (the source password variable included), so start the
//! cluster *after* `TestDb::connect()`. They are killed and reaped on `Drop` (also on test
//! panic). Each child's output goes to `$TMPDIR/rel-test-cluster-<pid>-<n>-<role>.log`, kept
//! only when the test fails, so a failure can be diagnosed.

use std::fs::File;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

const BIN: &str = env!("CARGO_BIN_EXE_rust-ballista-extraction-layer");
static CLUSTERS: AtomicUsize = AtomicUsize::new(0);

pub struct TestCluster {
    /// `http://127.0.0.1:<port>` — pass to `DistributedContext::remote` / `.scheduler(url)`.
    pub url: String,
    pub workers: usize,
    children: Vec<Child>,
    /// Scheduler/worker logs; removed on drop unless the test is failing.
    logs: Vec<std::path::PathBuf>,
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("no free localhost port")
}

fn log_path(n: usize, role: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rel-test-cluster-{}-{n}-{role}.log",
        std::process::id()
    ))
}

fn spawn(n: usize, role: &str, args: &[String]) -> Child {
    let log = log_path(n, role);
    let out = File::create(&log).expect("create cluster log");
    let err = out.try_clone().expect("clone cluster log");
    Command::new(BIN)
        .args(args)
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {BIN} {role}: {e}"))
}

/// Failure-detection timings of a test cluster (`rel scheduler --executor-timeout-secs`,
/// `rel worker --heartbeat-secs`). `None` keeps the CLI defaults (30 s / 5 s).
#[derive(Debug, Clone, Copy, Default)]
pub struct Failover {
    pub executor_timeout_secs: Option<u64>,
    pub heartbeat_secs: Option<u64>,
}

impl TestCluster {
    /// Start a scheduler and `workers` executors with `concurrent_tasks` slots each, and wait
    /// (up to 60 s) until every executor is registered with the scheduler.
    pub async fn start(workers: usize, concurrent_tasks: usize) -> Self {
        Self::start_with(workers, concurrent_tasks, Failover::default()).await
    }

    /// As [`TestCluster::start`], with explicit failure-detection timings (short ones let a
    /// test kill a worker and see the job recover in seconds).
    #[allow(dead_code)]
    pub async fn start_with(workers: usize, concurrent_tasks: usize, failover: Failover) -> Self {
        let n = CLUSTERS.fetch_add(1, Ordering::SeqCst);
        let port = free_port();
        let url = format!("http://127.0.0.1:{port}");
        let mut scheduler_args: Vec<String> = vec![
            "scheduler".into(),
            "--scheduler-url".into(),
            url.clone(),
            "--bind-host".into(),
            "127.0.0.1".into(),
        ];
        if let Some(t) = failover.executor_timeout_secs {
            scheduler_args.extend(["--executor-timeout-secs".into(), t.to_string()]);
        }
        let mut children = vec![spawn(n, "scheduler", &scheduler_args)];
        // A worker that starts before the scheduler listens fails to connect and exits.
        wait_until(&url, 0, || registered_executors(port)).await;
        for w in 0..workers {
            let mut args: Vec<String> = vec![
                "worker".into(),
                "--scheduler-url".into(),
                url.clone(),
                "--bind-host".into(),
                "127.0.0.1".into(),
                "--port".into(),
                free_port().to_string(),
                "--grpc-port".into(),
                free_port().to_string(),
                "--concurrent-tasks".into(),
                concurrent_tasks.to_string(),
            ];
            if let Some(h) = failover.heartbeat_secs {
                args.extend(["--heartbeat-secs".into(), h.to_string()]);
            }
            children.push(spawn(n, &format!("worker{w}"), &args));
        }
        let logs = std::iter::once("scheduler".to_string())
            .chain((0..workers).map(|w| format!("worker{w}")))
            .map(|role| log_path(n, &role))
            .collect();
        let cluster = Self {
            url,
            workers,
            children,
            logs,
        };
        wait_until(&cluster.url, workers, || registered_executors(port)).await;
        cluster
    }
}

impl TestCluster {
    /// Kill worker `w` (0-based) with SIGKILL, like an OOM kill: no deregistration, the
    /// scheduler keeps listing it until its heartbeat times out.
    #[allow(dead_code)]
    pub fn kill_worker(&mut self, w: usize) {
        let child = &mut self.children[1 + w];
        let _ = child.kill();
        let _ = child.wait();
    }
}

impl TestCluster {
    /// `(job_name, status)` of every job the scheduler knows (`GET /api/jobs`).
    #[allow(dead_code)]
    pub async fn jobs(&self) -> Vec<(String, String)> {
        let port: u16 = self.url.rsplit(':').next().unwrap().parse().unwrap();
        let Some(body) = rest_get(port, "/api/jobs").await else {
            return Vec::new();
        };
        let value: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
        value
            .as_array()
            .map(|jobs| {
                jobs.iter()
                    .map(|j| {
                        let field =
                            |k: &str| j.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
                        (field("job_name"), field("status"))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Body of a `200` response to `GET path` (identity encoding only, as axum sends JSON).
async fn rest_get(port: u16, path: &str) -> Option<String> {
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .ok()?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.ok()?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.ok()?;
    let text = String::from_utf8_lossy(&buf);
    let (head, body) = text.split_once("\r\n\r\n")?;
    head.starts_with("HTTP/1.1 200").then(|| body.to_string())
}

/// Poll `probe` until it reports at least `want` executors (0 = the scheduler just answers).
async fn wait_until<F, Fut>(url: &str, want: usize, probe: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Option<usize>>,
{
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if probe().await.is_some_and(|n| n >= want) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cluster at {url} did not reach {want} registered executor(s) within 60 s (see \
             $TMPDIR/rel-test-cluster-{}-*.log)",
            std::process::id()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Count of executors listed by the scheduler REST API (`GET /api/executors`); `None` while
/// the scheduler is not answering yet.
async fn registered_executors(port: u16) -> Option<usize> {
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .ok()?;
    let req = format!(
        "GET /api/executors HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.ok()?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.ok()?;
    let text = String::from_utf8_lossy(&buf);
    let (head, body) = text.split_once("\r\n\r\n")?;
    if !head.starts_with("HTTP/1.1 200") {
        return None;
    }
    // Identity or chunked: count executor objects by their spec key, which appears once each.
    Some(body.matches("\"task_slots\"").count())
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        // Workers first, then the scheduler.
        for child in self.children.iter_mut().rev() {
            let _ = child.kill();
            let _ = child.wait();
        }
        // Keep the logs only when the test is failing (they explain why).
        if !std::thread::panicking() {
            for log in &self.logs {
                let _ = std::fs::remove_file(log);
            }
        }
    }
}
