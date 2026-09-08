//! Incremental extraction: watermark computation and window construction.
//! incremental/mod.rs
//! Implements the mechanisms from docs/incremental-extraction.md that guard against the
//! commit-time vs. `updated_at` skew hazard (§3.1) and window sizing on catch-up (§4).
//!
//! Scope: `timestamp` mode only, which is the documented default and what Phase 1 targets.
//! Not implemented yet: `append_id` / `snapshot` / `log` modes, the composite
//! `(timestamp, primary_key)` keyset tiebreaker for boundary ties (§3.2 Rule 2), and parallel
//! scan partitioning. See docs/phase-one-implementation-plan.md §5.

use arrow::array::{Array, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Duration, Utc};
use sqlx::{PgPool, Row};

use crate::errors::AppError;

/// A resolved `(lo, hi]` window for one run.
#[derive(Debug, Clone, Copy)]
pub struct Window {
    pub lo: DateTime<Utc>,
    pub hi: DateTime<Utc>,
}

/// docs/connectors/postgres.md §5.2 — the exact (not heuristic) safe high watermark: never
/// advance past the oldest in-flight transaction, so a row written by a still-open transaction
/// isn't skipped once it commits (docs/incremental-extraction.md §3.1).
///
/// Requires `pg_read_all_stats` membership to see other sessions' `xact_start`. When the query
/// fails — most likely because that privilege is missing — this falls back to `now() -
/// safety_lag` with a loud warning, per Mitigation 1 in the same section. Silently downgrading a
/// correctness mechanism is the thing to avoid, so the fallback is logged, not swallowed.
pub async fn safe_high_watermark(pool: &PgPool, safety_lag: Duration) -> Result<DateTime<Utc>, AppError> {
    let result = sqlx::query(
        r#"
        SELECT LEAST(
                 now() - INTERVAL '1 second',
                 COALESCE(MIN(xact_start), now())
               ) AS safe_hi
        FROM pg_stat_activity
        WHERE backend_type = 'client backend'
          AND state <> 'idle'
          AND datname = current_database()
        "#,
    )
    .fetch_one(pool)
    .await;

    match result {
        Ok(row) => {
            let safe_hi: DateTime<Utc> = row.try_get("safe_hi").map_err(|e| {
                AppError::Incremental(format!(
                    "safe watermark query returned an unexpected shape: {e}"
                ))
            })?;
            Ok(safe_hi)
        }
        Err(e) => {
            log::warn!(
                "safe high watermark query failed ({e}); this role likely lacks \
                 pg_read_all_stats. Falling back to now() - safety_lag, which is a WEAKER \
                 guarantee against commit-skew data loss — see docs/incremental-extraction.md §3.1."
            );
            Ok(Utc::now() - safety_lag)
        }
    }
}

/// docs/incremental-extraction.md §4 — window sizing and catch-up. Resolves `lo` from the
/// checkpoint (or the beginning of time on a job's first run) and caps the window width at
/// `max_window`, so a job that has been down for days processes bounded chunks instead of one
/// giant scan that times out.
pub fn build_window(
    lo: Option<DateTime<Utc>>,
    hi_candidate: DateTime<Utc>,
    max_window: Duration,
) -> Window {
    let lo = lo.unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());

    let hi = if hi_candidate - lo > max_window {
        lo + max_window
    } else {
        hi_candidate
    };

    Window { lo, hi }
}

/// docs/incremental-extraction.md §3.1 Mitigation 3 — never let the committed watermark race
/// ahead of the data actually observed in this run. Pass the max watermark-column value seen in
/// the extracted batch, if any rows were returned.
pub fn clamp_to_observed(hi: DateTime<Utc>, max_observed: Option<DateTime<Utc>>) -> DateTime<Utc> {
    match max_observed {
        Some(observed) if observed < hi => observed,
        _ => hi,
    }
}

/// The maximum value of a `Timestamp(Microsecond, ...)` column in a batch, or `None` if the
/// batch is empty, the column is missing, or every value in it is null. Used to feed
/// `clamp_to_observed`.
pub fn max_timestamp_column(batch: &RecordBatch, column_name: &str) -> Option<DateTime<Utc>> {
    let idx = batch.schema().index_of(column_name).ok()?;
    let array = batch.column(idx);
    let ts_array = array.as_any().downcast_ref::<TimestampMicrosecondArray>()?;

    (0..ts_array.len())
        .filter(|i| ts_array.is_valid(*i))
        .map(|i| ts_array.value(i))
        .max()
        .and_then(DateTime::<Utc>::from_timestamp_micros)
}
