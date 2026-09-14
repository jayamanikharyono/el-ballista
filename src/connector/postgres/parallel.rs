//! Parallel scan strategies.
//! extractor/postgres/parallel.rs
//! Splits a table scan across multiple connections using keyset or ctid partitioning.
//! `export_snapshot`/`use_snapshot` exist for future cross-connection consistency work but are
//! NOT wired into any scan path: pooled connections cannot hold `SET TRANSACTION SNAPSHOT`
//! across checkouts, so snapshot-consistent parallel reads stay deferred. Keyset bounds come
//! from a single MIN/MAX read; ctid ranges from relpages.

use sqlx::PgPool;

use serde::{Deserialize, Serialize};

use crate::connector::errors::ExtractorError;

/// Parallel scan strategy: how to partition the table across connections.
/// Serialized into distributed plans; `None` preserves single-scan behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ParallelStrategy {
    /// No parallelism; scan with a single connection.
    #[default]
    None,
    /// Partition by primary key ranges using keyset predicates.
    Keyset,
    /// Partition by physical tuple ID ranges. No partition column needed; concurrent
    /// VACUUM can move tuples between pages, so prefer keyset for hot tables. Exported
    /// snapshots for cross-connection consistency remain deferred (see module docs).
    Ctid,
}

impl ParallelStrategy {
    pub fn parse(s: &str) -> Self {
        match s {
            "keyset" => ParallelStrategy::Keyset,
            "ctid" => ParallelStrategy::Ctid,
            _ => ParallelStrategy::None,
        }
    }
}

/// Configuration for parallel scans.
#[derive(Debug, Clone)]
pub struct ParallelScanConfig {
    pub strategy: ParallelStrategy,
    pub partitions: usize,
}

impl Default for ParallelScanConfig {
    fn default() -> Self {
        Self {
            strategy: ParallelStrategy::None,
            partitions: 1,
        }
    }
}

/// Represents a single partition of a parallel scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanPartition {
    pub partition_id: usize,
    pub lo: Option<i64>,
    pub hi: Option<i64>,
    pub predicate: Option<String>,
}

/// Computes partition bounds for a keyset-based scan.
/// Uses min/max from the partition column or histogram bounds from pg_stats.
pub async fn compute_keyset_partitions(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
    partition_column: &str,
    num_partitions: usize,
) -> Result<Vec<ScanPartition>, ExtractorError> {
    if num_partitions <= 1 {
        return Ok(single_partition());
    }

    // Fetch min and max values from the partition column
    let query = format!(
        "SELECT MIN(\"{}\"::bigint), MAX(\"{}\"::bigint) FROM {}.\"{}\"",
        partition_column, partition_column, schema_name, table_name
    );

    let (min_val, max_val): (Option<i64>, Option<i64>) =
        sqlx::query_as(sqlx::AssertSqlSafe(query.as_str()))
            .fetch_one(pool)
            .await
            .map_err(|e| {
                ExtractorError::Statistics(format!(
                    "cannot compute partition bounds for {}.{}: {}",
                    schema_name, table_name, e
                ))
            })?;

    let partitions = keyset_partitions_from_bounds(
        partition_column,
        min_val.unwrap_or(0),
        max_val.unwrap_or(0),
        num_partitions,
    );

    log::info!(
        "computed {} keyset partitions for {}.{} by column {}",
        partitions.len(),
        schema_name,
        table_name,
        partition_column
    );

    Ok(partitions)
}

/// Pure partitioning math for keyset scans, given already-known column bounds: no
/// database access, so this is unit-testable without a live Postgres — the DB round trip
/// in `compute_keyset_partitions` above is only responsible for producing `min_val`/`max_val`.
fn keyset_partitions_from_bounds(
    partition_column: &str,
    min_val: i64,
    max_val: i64,
    num_partitions: usize,
) -> Vec<ScanPartition> {
    debug_assert!(num_partitions > 1, "callers must special-case <=1 partitions before this");

    if min_val >= max_val {
        log::warn!(
            "keyset partitioning: min >= max ({} >= {}), falling back to single partition",
            min_val,
            max_val
        );
        return single_partition();
    }

    let range = max_val - min_val;
    let partition_size = (range / num_partitions as i64).max(1);

    let mut partitions = Vec::new();
    for i in 0..num_partitions {
        let lo = min_val + (i as i64 * partition_size);
        let hi = if i == num_partitions - 1 {
            max_val + 1 // Include the max value in the last partition
        } else {
            lo + partition_size
        };

        let predicate = format!(
            "\"{}\" >= {} AND \"{}\" < {}",
            partition_column, lo, partition_column, hi
        );

        partitions.push(ScanPartition {
            partition_id: i,
            lo: Some(lo),
            hi: Some(hi),
            predicate: Some(predicate),
        });
    }

    partitions
}

/// The universal "don't partition" fallback: one partition covering everything.
fn single_partition() -> Vec<ScanPartition> {
    vec![ScanPartition {
        partition_id: 0,
        lo: None,
        hi: None,
        predicate: None,
    }]
}

/// Computes partition bounds for a ctid-based scan.
/// Divides the table's physical pages into equal ranges.
pub async fn compute_ctid_partitions(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
    num_partitions: usize,
) -> Result<Vec<ScanPartition>, ExtractorError> {
    if num_partitions <= 1 {
        return Ok(single_partition());
    }

    // Fetch relpages from pg_class
    let query = "SELECT relpages FROM pg_class WHERE relname = $1 AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $2)";

    let relpages: i32 = sqlx::query_scalar(query)
        .bind(table_name)
        .bind(schema_name)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            ExtractorError::Statistics(format!(
                "cannot fetch relpages for {}.{}: {}",
                schema_name, table_name, e
            ))
        })?
        .flatten()
        .unwrap_or(1);

    let partitions = ctid_partitions_from_relpages(relpages, num_partitions);

    log::info!(
        "computed {} ctid partitions for {}.{} ({} pages)",
        partitions.len(),
        schema_name,
        table_name,
        relpages
    );

    Ok(partitions)
}

/// Pure partitioning math for ctid scans, given an already-known page count: no database
/// access, so this is unit-testable without a live Postgres — the DB round trip in
/// `compute_ctid_partitions` above is only responsible for producing `relpages`.
fn ctid_partitions_from_relpages(relpages: i32, num_partitions: usize) -> Vec<ScanPartition> {
    debug_assert!(num_partitions > 1, "callers must special-case <=1 partitions before this");

    if relpages <= 0 {
        log::warn!("ctid partitioning: no pages, falling back to single partition");
        return single_partition();
    }

    let pages_per_partition = (relpages as usize / num_partitions).max(1);

    let mut partitions = Vec::new();
    for i in 0..num_partitions {
        let page_lo = i * pages_per_partition;

        // ctid format: (page, tuple_offset). The last partition is open-ended
        // (no upper bound): relpages is a planner estimate that goes stale, and any
        // tuples on pages >= relpages (growth after ANALYZE, or relpages smaller
        // than the partition count) would otherwise never be scanned. An open tail
        // also avoids emitting an inverted (>= (94,1) AND < (3,1)) always-empty range
        // when relpages < num_partitions.
        let (page_hi, predicate) = if i == num_partitions - 1 {
            // The predicate itself is open-ended (no upper tid bound), so it's correct
            // regardless of `page_hi`'s value. But `page_hi` is still a field callers can
            // read directly (e.g. for logging/display), and `relpages` alone can be
            // *less* than `page_lo` when relpages < num_partitions (more partitions than
            // pages) — reporting that as `hi` would look like an inverted range. Clamp it
            // to `page_lo` so the field always reads as "empty/unbounded", never inverted.
            (
                (relpages as usize).max(page_lo),
                format!("ctid >= '({},1)'::tid", page_lo),
            )
        } else {
            let hi = (i + 1) * pages_per_partition;
            (
                hi,
                format!(
                    "ctid >= '({},1)'::tid AND ctid < '({},1)'::tid",
                    page_lo, hi
                ),
            )
        };

        partitions.push(ScanPartition {
            partition_id: i,
            lo: Some(page_lo as i64),
            hi: Some(page_hi as i64),
            predicate: Some(predicate),
        });
    }

    partitions
}

/// Establish an exported snapshot for consistent cross-connection reads.
pub async fn export_snapshot(pool: &PgPool) -> Result<String, ExtractorError> {
    let snapshot_id: String = sqlx::query_scalar("SELECT pg_export_snapshot()")
        .fetch_one(pool)
        .await
        .map_err(|e| ExtractorError::Statistics(format!("cannot export snapshot: {e}")))?;

    Ok(snapshot_id)
}

/// Set a connection to use an exported snapshot.
pub async fn use_snapshot(pool: &PgPool, snapshot_id: &str) -> Result<(), ExtractorError> {
    // Validate snapshot_id format to prevent injection (snapshots are hex-only).
    if !snapshot_id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return Err(ExtractorError::Statistics(
            "invalid snapshot_id format".to_string(),
        ));
    }

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SET TRANSACTION SNAPSHOT '{}';",
        snapshot_id
    ).as_str()))
    .execute(pool)
    .await
    .map_err(|e| ExtractorError::Statistics(format!("cannot set snapshot: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_strategy_parse() {
        assert_eq!(ParallelStrategy::parse("keyset"), ParallelStrategy::Keyset);
        assert_eq!(ParallelStrategy::parse("ctid"), ParallelStrategy::Ctid);
        assert_eq!(ParallelStrategy::parse("none"), ParallelStrategy::None);
        assert_eq!(ParallelStrategy::parse("unknown"), ParallelStrategy::None);
    }

    #[test]
    fn test_parallel_scan_config_default() {
        let config = ParallelScanConfig::default();
        assert_eq!(config.strategy, ParallelStrategy::None);
        assert_eq!(config.partitions, 1);
    }

    // --- keyset_partitions_from_bounds: real math, no DB needed ---

    #[test]
    fn test_keyset_partitions_even_split_covers_full_range_contiguously() {
        // 1000-wide range (0..1000), 4 partitions -> each 250 wide, contiguous, and the
        // last partition's hi is max_val + 1 (inclusive of the max value itself).
        let partitions = keyset_partitions_from_bounds("id", 0, 1000, 4);
        assert_eq!(partitions.len(), 4);
        assert_eq!(partitions[0].lo, Some(0));
        assert_eq!(partitions[0].hi, Some(250));
        assert_eq!(partitions[1].lo, Some(250));
        assert_eq!(partitions[1].hi, Some(500));
        assert_eq!(partitions[2].lo, Some(500));
        assert_eq!(partitions[2].hi, Some(750));
        assert_eq!(partitions[3].lo, Some(750));
        assert_eq!(partitions[3].hi, Some(1001), "last partition must include max_val");
        for (i, p) in partitions.iter().enumerate() {
            assert_eq!(p.partition_id, i);
            assert!(p.predicate.as_ref().unwrap().contains("\"id\""));
        }
        // No gaps or overlaps between consecutive partitions.
        for w in partitions.windows(2) {
            assert_eq!(w[0].hi, w[1].lo, "partitions must be contiguous with no gap/overlap");
        }
    }

    #[test]
    fn test_keyset_partitions_range_smaller_than_partition_count_still_min_size_one() {
        // Range of 2 (0..2) split into 4 partitions: partition_size = (2/4).max(1) = 1, so
        // partition_size * num_partitions (4) overshoots the actual range (2). The last
        // partition or two legitimately end up empty ([3, 3)) — that's harmless (an empty
        // scan), not a bug. What must never happen is an *inverted* range (hi < lo), which
        // would silently turn into a nonsense predicate.
        let partitions = keyset_partitions_from_bounds("id", 0, 2, 4);
        assert_eq!(partitions.len(), 4);
        for p in &partitions {
            let (lo, hi) = (p.lo.unwrap(), p.hi.unwrap());
            assert!(hi >= lo, "partition range must never invert, got [{lo}, {hi})");
        }
        // Pin the actual (harmless) overshoot shape so a change here is a deliberate one.
        assert_eq!((partitions[3].lo, partitions[3].hi), (Some(3), Some(3)));
    }

    #[test]
    fn test_keyset_partitions_min_equals_max_falls_back_to_single_partition() {
        // A degenerate (or entirely-NULL) column collapses min == max: partitioning would
        // divide by a zero-width range, so this must fall back to one unbounded partition
        // rather than emit a bogus/empty predicate.
        let partitions = keyset_partitions_from_bounds("id", 42, 42, 4);
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].lo, None);
        assert_eq!(partitions[0].hi, None);
        assert_eq!(partitions[0].predicate, None);
    }

    #[test]
    fn test_keyset_partitions_min_greater_than_max_falls_back_to_single_partition() {
        // Should never happen from a real MIN/MAX query, but a caller could pass swapped
        // bounds by mistake: must degrade safely, not underflow/panic on `max - min`.
        let partitions = keyset_partitions_from_bounds("id", 100, 50, 4);
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].lo, None);
    }

    // --- ctid_partitions_from_relpages: real math, no DB needed ---

    #[test]
    fn test_ctid_partitions_even_split_covers_all_pages_contiguously() {
        let partitions = ctid_partitions_from_relpages(100, 4);
        assert_eq!(partitions.len(), 4);
        assert_eq!(partitions[0].lo, Some(0));
        assert_eq!(partitions[0].hi, Some(25));
        assert_eq!(partitions[0].predicate.as_deref(), Some("ctid >= '(0,1)'::tid AND ctid < '(25,1)'::tid"));
        assert_eq!(partitions[3].lo, Some(75));
        assert_eq!(partitions[3].hi, Some(100));
        // Last partition is open-ended (no upper tid bound) so late-arriving pages beyond
        // the relpages estimate are still scanned.
        assert_eq!(partitions[3].predicate.as_deref(), Some("ctid >= '(75,1)'::tid"));
        for w in partitions.windows(2) {
            assert_eq!(w[0].hi, w[1].lo, "page ranges must be contiguous");
        }
    }

    #[test]
    fn test_ctid_partitions_fewer_pages_than_partitions_never_inverts_range() {
        // 2 pages, 4 partitions: pages_per_partition = (2/4).max(1) = 1, so the last
        // partition's page_lo (3) overshoots relpages (2). The predicate stays correct
        // regardless (open-ended: only a lower bound), but the *stored* `hi` field must
        // still never read as less than `lo` -- that's what `.max(page_lo)` guards.
        let partitions = ctid_partitions_from_relpages(2, 4);
        assert_eq!(partitions.len(), 4);
        for p in &partitions {
            if let Some(hi) = p.hi {
                assert!(hi >= p.lo.unwrap(), "page range must not invert");
            }
        }
        // Pin the actual clamped shape of the last (open-ended, overshooting) partition.
        assert_eq!(partitions[3].lo, Some(3));
        assert_eq!(partitions[3].hi, Some(3), "clamped to lo, not the smaller relpages value");
        assert_eq!(partitions[3].predicate.as_deref(), Some("ctid >= '(3,1)'::tid"));
    }

    #[test]
    fn test_ctid_partitions_zero_pages_falls_back_to_single_partition() {
        // relpages <= 0 (empty or never-analyzed table) must degrade to one partition
        // rather than divide by a meaningless page count.
        let partitions = ctid_partitions_from_relpages(0, 4);
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].predicate, None);
    }

    #[test]
    fn test_ctid_partitions_negative_relpages_falls_back_to_single_partition() {
        // Defensive: relpages is a planner statistic and could in principle be stale/odd;
        // must not panic on `as usize` conversion of a negative value.
        let partitions = ctid_partitions_from_relpages(-1, 4);
        assert_eq!(partitions.len(), 1);
    }
}
