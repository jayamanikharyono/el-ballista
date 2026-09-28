//! Remote-deployment check for the source connection budget.
//!
//! `pool_max` is split as `pool_max / workers` per executing process, so the budget only
//! holds if exactly `workers` executor processes are registered with the scheduler. The
//! Ballista client API cannot list executors, but the scheduler's REST API (enabled in this
//! crate's scheduler build, served on the same port as gRPC) can: `GET /api/executors`.
//!
//! `verify_remote_executors` asks it and:
//! - **errors** when more executors are registered than `workers` (each opens its own share,
//!   so the source would see more than `pool_max` connections);
//! - warns when fewer are registered (budget under-used, not unsafe), or when an executor
//!   has more task slots than its connection share (extra tasks wait for the shared scan
//!   limiter; they never exceed the budget);
//! - warns and continues when the REST API is unreachable or disabled — the deployment is
//!   then simply not verified, as before.
//!
//! The request is a single plain-HTTP `GET` over a `TcpStream` with a short timeout, so no
//! HTTP-client dependency is needed. `https://` scheduler URLs are not probed. The same
//! minimal client ([`rest_call`]) serves the job watchdog ([`super::watchdog`]).

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::connector::errors::ExtractorError;

/// Upper bound for the whole probe (connect + request + response).
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
/// Refuse to buffer more than this from the scheduler.
const MAX_RESPONSE_BYTES: usize = 1 << 20;

/// What the scheduler reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredExecutors {
    /// Task slots of each registered executor.
    pub task_slots: Vec<u32>,
}

/// Outcome of comparing the registered executors with the configured budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetCheck {
    /// Exactly `workers` executors, none with more task slots than its connection share.
    Ok,
    /// Budget-safe but worth a warning (fewer executors, or more task slots than connections).
    Warn(String),
    /// More executors than `workers`: the source would exceed `pool_max`.
    Exceeded(String),
}

/// Compare a scheduler's executor listing with `workers` and the per-process `budget`.
pub(crate) fn check_budget(
    registered: &RegisteredExecutors,
    workers: usize,
    budget: u32,
) -> BudgetCheck {
    let n = registered.task_slots.len();
    if n > workers {
        return BudgetCheck::Exceeded(format!(
            "{n} executors are registered but the job budgets pool_max for {workers}: each \
             executor opens up to {budget} source connection(s), so the source would see up to \
             {} (set distributed.workers / --workers to {n}, or stop the extra executors)",
            n as u64 * u64::from(budget)
        ));
    }
    let mut notes = Vec::new();
    if n < workers {
        notes.push(format!(
            "only {n} of {workers} expected executor(s) are registered; the source budget is \
             under-used"
        ));
    }
    if let Some(max) = registered.task_slots.iter().copied().max()
        && max > budget
    {
        notes.push(format!(
            "an executor has {max} task slots but only {budget} source connection(s); extra \
             scan tasks will wait for a connection (consider --concurrent-tasks {budget})"
        ));
    }
    if notes.is_empty() {
        BudgetCheck::Ok
    } else {
        BudgetCheck::Warn(notes.join("; "))
    }
}

/// Query `GET {scheduler_url}/api/executors`. `Ok(None)` means "could not verify" (https,
/// unreachable, REST disabled, unexpected body); only a well-formed listing is `Some`.
pub(crate) async fn fetch_registered_executors(scheduler_url: &str) -> Option<RegisteredExecutors> {
    let Some(authority) = rest_authority(scheduler_url) else {
        log::warn!("scheduler URL {scheduler_url:?} is not plain http://; executors not verified");
        return None;
    };
    match rest_call(authority, "GET", "/api/executors").await {
        Ok(body) => match parse_executors(&body) {
            Ok(executors) => Some(executors),
            Err(e) => {
                log::warn!("scheduler REST /api/executors: unexpected response ({e})");
                None
            }
        },
        Err(e) => {
            log::warn!("scheduler REST /api/executors unavailable ({e}); executors not verified");
            None
        }
    }
}

/// `http://host:port[/…]` → `host:port`; `None` for anything but a plain-http URL.
pub(crate) fn rest_authority(scheduler_url: &str) -> Option<&str> {
    scheduler_url
        .strip_prefix("http://")
        .map(|rest| rest.split('/').next().unwrap_or(rest))
        .filter(|a| !a.is_empty())
}

/// One scheduler REST request (`GET`, or `PATCH` to cancel a job) bounded by
/// [`PROBE_TIMEOUT`]; returns the body of a `2xx` response.
pub(crate) async fn rest_call(
    authority: &str,
    method: &str,
    path: &str,
) -> std::io::Result<Vec<u8>> {
    match tokio::time::timeout(PROBE_TIMEOUT, http_request(authority, method, path)).await {
        Ok(result) => result,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("{method} {path} timed out after {PROBE_TIMEOUT:?}"),
        )),
    }
}

/// Verify a remote deployment against the budget: `Err` only when the budget is exceeded.
pub(crate) async fn verify_remote_executors(
    scheduler_url: &str,
    workers: usize,
    budget: u32,
) -> Result<(), ExtractorError> {
    let Some(registered) = fetch_registered_executors(scheduler_url).await else {
        log::warn!(
            "remote Ballista: could not list executors; the source budget (pool_max over \
             {workers} worker(s) = {budget} connection(s) per process) assumes exactly {workers} \
             executor process(es)"
        );
        return Ok(());
    };
    match check_budget(&registered, workers, budget) {
        BudgetCheck::Ok => {
            log::info!(
                "remote Ballista: {} executor(s) registered, matching the source budget",
                registered.task_slots.len()
            );
            Ok(())
        }
        BudgetCheck::Warn(msg) => {
            log::warn!("remote Ballista: {msg}");
            Ok(())
        }
        BudgetCheck::Exceeded(msg) => Err(ExtractorError::InvalidConfig(msg)),
    }
}

async fn http_request(authority: &str, method: &str, path: &str) -> std::io::Result<Vec<u8>> {
    let mut stream = tokio::net::TcpStream::connect(authority).await?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nAccept: application/json\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        response.extend_from_slice(&chunk[..n]);
        if response.len() > MAX_RESPONSE_BYTES {
            return Err(std::io::Error::other("response too large"));
        }
    }
    http_body(&response).map_err(std::io::Error::other)
}

/// Extract the body of a `2xx` HTTP/1.1 response (identity or chunked encoding).
fn http_body(response: &[u8]) -> Result<Vec<u8>, String> {
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("no HTTP header terminator")?;
    let head = std::str::from_utf8(&response[..split]).map_err(|_| "non-UTF-8 headers")?;
    let body = &response[split + 4..];
    let status_line = head.lines().next().unwrap_or_default();
    if !status_line
        .split_whitespace()
        .nth(1)
        .is_some_and(|code| code.len() == 3 && code.starts_with('2'))
    {
        return Err(format!("status {status_line:?}"));
    }
    let chunked = head.lines().any(|l| {
        let l = l.to_ascii_lowercase();
        l.starts_with("transfer-encoding:") && l.contains("chunked")
    });
    if !chunked {
        return Ok(body.to_vec());
    }
    let mut out = Vec::new();
    let mut rest = body;
    loop {
        let line_end = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("truncated chunk size")?;
        let size_str = std::str::from_utf8(&rest[..line_end]).map_err(|_| "bad chunk size")?;
        let size = usize::from_str_radix(size_str.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|_| format!("bad chunk size {size_str:?}"))?;
        rest = &rest[line_end + 2..];
        if size == 0 {
            return Ok(out);
        }
        if rest.len() < size + 2 {
            return Err("truncated chunk".into());
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
}

/// Parse the scheduler's `/api/executors` JSON: an array of objects with
/// `specification.task_slots`.
fn parse_executors(body: &[u8]) -> Result<RegisteredExecutors, String> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("invalid JSON: {e}"))?;
    let list = value.as_array().ok_or("expected a JSON array")?;
    let task_slots = list
        .iter()
        .map(|e| {
            e.get("specification")
                .and_then(|s| s.get("task_slots"))
                .and_then(|t| t.as_u64())
                .and_then(|t| u32::try_from(t).ok())
                .ok_or_else(|| "executor without specification.task_slots".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RegisteredExecutors { task_slots })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slots(v: &[u32]) -> RegisteredExecutors {
        RegisteredExecutors {
            task_slots: v.to_vec(),
        }
    }

    #[test]
    fn budget_check_outcomes() {
        assert_eq!(check_budget(&slots(&[2, 2]), 2, 2), BudgetCheck::Ok);
        assert!(matches!(
            check_budget(&slots(&[2, 2, 2]), 2, 2),
            BudgetCheck::Exceeded(m) if m.contains("3 executors")
        ));
        assert!(matches!(
            check_budget(&slots(&[2]), 2, 2),
            BudgetCheck::Warn(m) if m.contains("only 1 of 2")
        ));
        assert!(matches!(
            check_budget(&slots(&[8, 8]), 2, 2),
            BudgetCheck::Warn(m) if m.contains("8 task slots")
        ));
    }

    #[test]
    fn parses_identity_and_chunked_responses() {
        let json = br#"[{"id":"a","specification":{"task_slots":4}},{"id":"b","specification":{"task_slots":2}}]"#;
        let mut identity = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n".to_vec();
        identity.extend_from_slice(json);
        assert_eq!(
            parse_executors(&http_body(&identity).unwrap()).unwrap(),
            slots(&[4, 2])
        );

        let mut chunked = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n".to_vec();
        let (a, b) = json.split_at(10);
        for part in [a, b] {
            chunked.extend_from_slice(format!("{:x}\r\n", part.len()).as_bytes());
            chunked.extend_from_slice(part);
            chunked.extend_from_slice(b"\r\n");
        }
        chunked.extend_from_slice(b"0\r\n\r\n");
        assert_eq!(
            parse_executors(&http_body(&chunked).unwrap()).unwrap(),
            slots(&[4, 2])
        );
    }

    #[test]
    fn non_200_and_malformed_bodies_are_errors() {
        assert!(http_body(b"HTTP/1.1 404 Not Found\r\n\r\n{}").is_err());
        assert!(http_body(b"garbage").is_err());
        assert!(parse_executors(b"{}").is_err());
        assert!(parse_executors(br#"[{"id":"a"}]"#).is_err());
    }

    #[tokio::test]
    async fn probes_a_live_http_endpoint_and_skips_https() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                let body = r#"[{"specification":{"task_slots":1}}]"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });
        let got = fetch_registered_executors(&format!("http://{addr}")).await;
        assert_eq!(got, Some(slots(&[1])));
        assert_eq!(
            fetch_registered_executors("https://example.invalid").await,
            None
        );
        // Budget exceeded -> typed error; unreachable -> not verified, Ok.
        assert!(
            verify_remote_executors("http://127.0.0.1:1", 1, 1)
                .await
                .is_ok()
        );
    }
}
