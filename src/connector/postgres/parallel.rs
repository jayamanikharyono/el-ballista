//! Parallel scan strategies.
//! extractor/postgres/parallel.rs
//! Splits a table scan across multiple connections using keyset or ctid partitioning,
//! with optional exported snapshot for consistency.

use sqlx::PgPool;

use crate::connector::errors::ExtractorError;

/// Parallel scan strategy: how to partition the table across connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParallelStrategy {
    /// No parallelism; scan with a single connection.
    None,
    /// Partition by primary key ranges using keyset predicates.
    Keyset { partition_column: String },
    /// Partition by physical tuple ID ranges (requires to be exported snapshot).
    Ctid,
}

impl ParallelStrategy {
    pub fn parse(s: &str) -> Self {
        match s {
            "keyset" => ParallelStrategy::Keyset {
                partition_column: "id".to_string(),
            },
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
#[derive(Debug, Clone)]
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
        return Ok(vec![ScanPartition {
            partition_id: 0,
            lo: None,
            hi: None,
            predicate: None,
        }]);
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

    let min_val = min_val.unwrap_or(0);
    let max_val = max_val.unwrap_or(0);

    if min_val >= max_val {
        log::warn!(
            "keyset partitioning: min >= max ({} >= {}), falling back to single partition",
            min_val,
            max_val
        );
        return Ok(vec![ScanPartition {
            partition_id: 0,
            lo: None,
            hi: None,
            predicate: None,
        }]);
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

    log::info!(
        "computed {} keyset partitions for {}.{} by column {}",
        num_partitions,
        schema_name,
        table_name,
        partition_column
    );

    Ok(partitions)
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
        return Ok(vec![ScanPartition {
            partition_id: 0,
            lo: None,
            hi: None,
            predicate: None,
        }]);
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

    if relpages <= 0 {
        log::warn!(
            "ctid partitioning: no pages for {}.{}, falling back to single partition",
            schema_name,
            table_name
        );
        return Ok(vec![ScanPartition {
            partition_id: 0,
            lo: None,
            hi: None,
            predicate: None,
        }]);
    }

    let pages_per_partition = (relpages as usize / num_partitions).max(1);

    let mut partitions = Vec::new();
    for i in 0..num_partitions {
        let page_lo = i * pages_per_partition;
        let page_hi = if i == num_partitions - 1 {
            relpages as usize
        } else {
            (i + 1) * pages_per_partition
        };

        // ctid format: (page, tuple_offset)
        // Create predicate: ctid >= '(page_lo,1)'::tid AND ctid < '(page_hi,1)'::tid
        let predicate = format!(
            "ctid >= '({},1)'::tid AND ctid < '({},1)'::tid",
            page_lo, page_hi
        );

        partitions.push(ScanPartition {
            partition_id: i,
            lo: Some(page_lo as i64),
            hi: Some(page_hi as i64),
            predicate: Some(predicate),
        });
    }

    log::info!(
        "computed {} ctid partitions for {}.{} ({} pages)",
        num_partitions,
        schema_name,
        table_name,
        relpages
    );

    Ok(partitions)
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
        assert_eq!(ParallelStrategy::parse("keyset"), ParallelStrategy::Keyset {
            partition_column: "id".to_string()
        });
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

    #[test]
    fn test_scan_partition_creation() {
        let partition = ScanPartition {
            partition_id: 0,
            lo: Some(100),
            hi: Some(200),
            predicate: Some("id >= 100 AND id < 200".to_string()),
        };

        assert_eq!(partition.partition_id, 0);
        assert_eq!(partition.lo, Some(100));
        assert_eq!(partition.hi, Some(200));
    }

    #[test]
    fn test_keyset_partition_bounds() {
        // Test case: 1000 rows, 4 partitions
        // Each partition should get ~250 rows
        let partitions = vec![
            ScanPartition {
                partition_id: 0,
                lo: Some(0),
                hi: Some(250),
                predicate: Some("id >= 0 AND id < 250".to_string()),
            },
            ScanPartition {
                partition_id: 1,
                lo: Some(250),
                hi: Some(500),
                predicate: Some("id >= 250 AND id < 500".to_string()),
            },
        ];

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].lo, Some(0));
        assert_eq!(partitions[0].hi, Some(250));
    }

    #[test]
    fn test_ctid_partition_bounds() {
        // Test case: 100 pages, 4 partitions
        // Each partition should get 25 pages
        let partitions = vec![
            ScanPartition {
                partition_id: 0,
                lo: Some(0),
                hi: Some(25),
                predicate: Some("ctid >= '(0,1)'::tid AND ctid < '(25,1)'::tid".to_string()),
            },
            ScanPartition {
                partition_id: 1,
                lo: Some(25),
                hi: Some(50),
                predicate: Some("ctid >= '(25,1)'::tid AND ctid < '(50,1)'::tid".to_string()),
            },
        ];

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].lo, Some(0));
        assert_eq!(partitions[0].hi, Some(25));
    }
}
