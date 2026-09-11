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
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::errors::AppError;

/// Cache for privilege check result to avoid repeated queries
static PRIVILEGE_CHECK: once_cell::sync::Lazy<Arc<RwLock<Option<bool>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(RwLock::new(None)));

/// A resolved `(lo, hi]` window for one run.
#[derive(Debug, Clone, Copy)]
pub struct Window {
    pub lo: DateTime<Utc>,
    pub hi: DateTime<Utc>,
}

/// Check if the current database role has `pg_read_all_stats` privilege.
/// This privilege is required to see `xact_start` from other sessions in `pg_stat_activity`.
/// Results are cached to avoid repeated privilege checks.
///
/// Returns:
/// - `Ok(true)` if the role has the privilege
/// - `Ok(false)` if the role lacks the privilege
/// - `Err` if the check query itself fails (rare)
pub async fn check_pg_read_all_stats_privilege(pool: &PgPool) -> Result<bool, AppError> {
    // Check cache first
    {
        let cached = PRIVILEGE_CHECK.read().await;
        if let Some(has_privilege) = *cached {
            return Ok(has_privilege);
        }
    }

    // Not cached, perform the check
    let result = sqlx::query(
        r#"
        SELECT pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER') AS has_privilege
        "#,
    )
    .fetch_one(pool)
    .await;

    match result {
        Ok(row) => {
            let has_privilege: bool = row.try_get("has_privilege").map_err(|e| {
                AppError::Incremental(format!(
                    "privilege check query returned unexpected shape: {e}"
                ))
            })?;

            // Cache the result
            {
                let mut cache = PRIVILEGE_CHECK.write().await;
                *cache = Some(has_privilege);
            }

            if !has_privilege {
                log::warn!(
                    "Current database role lacks pg_read_all_stats privilege. \
                     Safe high watermark will fall back to now() - safety_lag, which provides \
                     WEAKER protection against commit-skew data loss. \
                     Consider granting: GRANT pg_read_all_stats TO <role>"
                );
            } else {
                log::info!("Database role has pg_read_all_stats privilege - safe high watermark protection enabled");
            }

            Ok(has_privilege)
        }
        Err(e) => {
            log::warn!("Failed to check pg_read_all_stats privilege: {e}. Assuming no privilege.");
            Ok(false)
        }
    }
}

/// docs/connectors/postgres.md §5.2 — the exact (not heuristic) safe high watermark: never
/// advance past the oldest in-flight transaction, so a row written by a still-open transaction
/// isn't skipped once it commits (docs/incremental-extraction.md §3.1).
///
/// This function implements **Mitigation 2** from docs/incremental-extraction.md §3.1:
/// Query `pg_stat_activity` to find the oldest open transaction and never advance the watermark
/// past it. This prevents skipping rows that:
/// - Were written by a transaction that started before our watermark query
/// - But committed after we read the data
///
/// The query:
/// ```sql
/// SELECT LEAST(now() - INTERVAL '1 second', COALESCE(MIN(xact_start), now())) AS safe_hi
/// FROM pg_stat_activity
/// WHERE backend_type = 'client backend'
///   AND state <> 'idle'
///   AND datname = current_database()
/// ```
///
/// Breakdown:
/// - `MIN(xact_start)`: Oldest transaction start time across all active connections
/// - `COALESCE(..., now())`: If no active transactions, use current time
/// - `LEAST(..., now() - 1s)`: Never return future time, subtract 1s for clock skew
/// - Filter: Only application connections to current database, exclude idle
///
/// Requires `pg_read_all_stats` membership to see other sessions' `xact_start`. When the query
/// fails — most likely because that privilege is missing — this falls back to `now() -
/// safety_lag` with a loud warning, per Mitigation 1 in the same section. Silently downgrading a
/// correctness mechanism is the thing to avoid, so the fallback is logged, not swallowed.
///
/// **Important:** Check privilege status at startup using `check_pg_read_all_stats_privilege()`
/// to log the warning once, rather than on every watermark query.
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
            
            log::debug!(
                "Safe high watermark from pg_stat_activity: {} (protects against commit skew)",
                safe_hi.to_rfc3339()
            );
            
            Ok(safe_hi)
        }
        Err(e) => {
            log::warn!(
                "Safe high watermark query failed ({e}); this role likely lacks \
                 pg_read_all_stats privilege. Falling back to now() - safety_lag ({} seconds), \
                 which provides WEAKER protection against commit-skew data loss. \
                 See docs/incremental-extraction.md §3.1 Mitigation 1 vs 2.",
                safety_lag.num_seconds()
            );
            
            let fallback_hi = Utc::now() - safety_lag;
            log::debug!(
                "Fallback safe high watermark: {} (now - {} seconds)",
                fallback_hi.to_rfc3339(),
                safety_lag.num_seconds()
            );
            
            Ok(fallback_hi)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_window_first_run() {
        let hi_candidate = DateTime::<Utc>::from_timestamp(1000, 0).unwrap();
        let max_window = Duration::seconds(500);

        let window = build_window(None, hi_candidate, max_window);
        assert_eq!(window.lo, DateTime::<Utc>::from_timestamp(0, 0).unwrap());
        // Clamped by max_window from 0
        assert_eq!(window.hi, DateTime::<Utc>::from_timestamp(500, 0).unwrap());
    }

    #[test]
    fn test_build_window_within_max() {
        let lo = DateTime::<Utc>::from_timestamp(100, 0).unwrap();
        let hi_candidate = DateTime::<Utc>::from_timestamp(300, 0).unwrap();
        let max_window = Duration::seconds(500);

        let window = build_window(Some(lo), hi_candidate, max_window);
        assert_eq!(window.lo, lo);
        assert_eq!(window.hi, hi_candidate);
    }

    #[test]
    fn test_clamp_to_observed() {
        let hi = DateTime::<Utc>::from_timestamp(1000, 0).unwrap();
        let observed = DateTime::<Utc>::from_timestamp(800, 0).unwrap();

        assert_eq!(clamp_to_observed(hi, Some(observed)), observed);
        assert_eq!(clamp_to_observed(hi, None), hi);

        let later_observed = DateTime::<Utc>::from_timestamp(1200, 0).unwrap();
        assert_eq!(clamp_to_observed(hi, Some(later_observed)), hi);
    }
}
