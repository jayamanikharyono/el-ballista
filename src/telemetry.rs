//! Metric names and recording helpers (AGENTS.md §1 "metrics per batch, not per row").
//!
//! Recorded through the [`metrics`] facade: without an installed recorder (the default) every
//! call is a cheap no-op; a host process installs one (Prometheus, StatsD, …) to export them.
//! Recording is per batch / per split / per pushdown decision — never per row.
//!
//! | name | kind | labels | meaning |
//! |---|---|---|---|
//! | `rel_extracted_rows` | counter | `job` | rows delivered in run batches |
//! | `rel_extracted_batches` | counter | `job` | batches delivered |
//! | `rel_batch_bytes` | histogram | `job` | Arrow memory size of each delivered batch |
//! | `rel_splits` | counter | `job`, `outcome` = `completed`/`failed`/`skipped` | split outcomes of `run_with` |
//! | `rel_pushdown_decisions` | counter | `outcome` = `exact`/`inexact`/`kept` | planner decisions per filter |
//!
//! There is no `checkpoint_lag_seconds`: this layer has no watermark (incremental state lives
//! in the orchestrator).

/// Rows delivered in run batches.
pub const EXTRACTED_ROWS: &str = "rel_extracted_rows";
/// Batches delivered.
pub const EXTRACTED_BATCHES: &str = "rel_extracted_batches";
/// Arrow memory size of each delivered batch.
pub const BATCH_BYTES: &str = "rel_batch_bytes";
/// Split outcomes of a checkpointed run.
pub const SPLITS: &str = "rel_splits";
/// Pushdown planner decisions, one per filter.
pub const PUSHDOWN_DECISIONS: &str = "rel_pushdown_decisions";

/// Record one delivered batch of `job`.
pub(crate) fn record_batch(job: &str, rows: u64, bytes: u64) {
    let job = job.to_string();
    metrics::counter!(EXTRACTED_ROWS, "job" => job.clone()).increment(rows);
    metrics::counter!(EXTRACTED_BATCHES, "job" => job.clone()).increment(1);
    metrics::histogram!(BATCH_BYTES, "job" => job).record(bytes as f64);
}

/// Record `n` splits of `job` that ended with `outcome` (`completed`, `failed`, `skipped`).
pub(crate) fn record_splits(job: &str, outcome: &'static str, n: u64) {
    if n > 0 {
        metrics::counter!(SPLITS, "job" => job.to_string(), "outcome" => outcome).increment(n);
    }
}

/// Record one pushdown decision (`exact`, `inexact` or `kept`).
pub(crate) fn record_pushdown(outcome: &'static str) {
    metrics::counter!(PUSHDOWN_DECISIONS, "outcome" => outcome).increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_without_a_recorder_is_a_no_op() {
        // No recorder installed: must not panic or allocate a global.
        record_batch("job", 10, 1024);
        record_splits("job", "completed", 2);
        record_splits("job", "failed", 0);
        record_pushdown("exact");
    }
}
