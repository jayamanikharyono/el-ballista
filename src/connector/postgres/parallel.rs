//! Parallel scan strategies.
//! extractor/postgres/parallel.rs
//! Splits a table scan across multiple connections using keyset or ctid partitioning.
//! Keyset bounds come from a single MIN/MAX read; ctid ranges from relpages.
//!
//! Isolation: each partition is scanned by its own statement/cursor and so reads its own
//! snapshot; partitions are **not** mutually consistent (snapshot-consistent parallel reads
//! via exported snapshots are not implemented).

use sqlx::PgPool;
use tracing::{debug, warn};

use serde::{Deserialize, Serialize};

use crate::connector::errors::ExtractorError;

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

pub use crate::types::ParallelStrategy;

/// What a partition's `lo`/`hi` bound. A serialized plan carries this, never SQL text: a
/// decoded [`ScanPartition`] re-renders its predicate from these typed fields with the same
/// constructors that rendered it, so plan bytes cannot inject SQL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum PartitionKind {
    /// No bounds: the whole table.
    Whole,
    /// A keyset range on `column` (see [`keyset_partition`]).
    Keyset { column: String },
    /// A physical page range `[lo, hi)`; `open_tail` drops the upper bound (the last
    /// partition, so pages past the `relpages` estimate are scanned too).
    Ctid { open_tail: bool },
}

/// Represents a single partition of a parallel scan.
///
/// `predicate` is the authoritative filter (rendered SQL; `None` = the whole table), rendered
/// from `kind`, `lo` and `hi` by this module. It is not serialized: a deserialized partition
/// renders it again from those typed fields and rejects bounds this module would never
/// produce. For keyset partitions `lo`/`hi` are informational: `lo` is the inclusive lower key
/// bound, `hi` the exclusive upper bound, and `hi == None` on the open-ended last partition.
/// The first keyset partition also holds every row whose key is NULL.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "ScanPartitionWire")]
pub struct ScanPartition {
    pub partition_id: usize,
    pub lo: Option<i64>,
    pub hi: Option<i64>,
    pub kind: PartitionKind,
    #[serde(skip_serializing)]
    pub predicate: Option<String>,
}

/// The serialized form of a [`ScanPartition`]: its typed bounds only.
#[derive(Deserialize)]
struct ScanPartitionWire {
    partition_id: usize,
    lo: Option<i64>,
    hi: Option<i64>,
    kind: PartitionKind,
}

impl TryFrom<ScanPartitionWire> for ScanPartition {
    type Error = ExtractorError;

    fn try_from(wire: ScanPartitionWire) -> Result<Self, Self::Error> {
        let ScanPartitionWire {
            partition_id,
            lo,
            hi,
            kind,
        } = wire;
        match kind {
            PartitionKind::Whole if lo.is_none() && hi.is_none() => Ok(ScanPartition {
                partition_id,
                lo,
                hi,
                kind: PartitionKind::Whole,
                predicate: None,
            }),
            PartitionKind::Whole => Err(ExtractorError::InvalidConfig(format!(
                "whole-table partition {partition_id} with bounds lo={lo:?} hi={hi:?}"
            ))),
            PartitionKind::Keyset { column } => keyset_partition(&column, partition_id, lo, hi),
            PartitionKind::Ctid { open_tail } => ctid_partition(partition_id, lo, hi, open_tail),
        }
    }
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

    // MIN/MAX on the raw column (then cast), so a btree index on an int2/int4/int8 key
    // answers each with one index probe. `MIN(col::bigint)` would force a sequential scan
    // on anything but an int8 column.
    let query = format!(
        "SELECT MIN({col})::bigint, MAX({col})::bigint FROM {}.{}",
        quote_ident(schema_name),
        quote_ident(table_name),
        col = quote_ident(partition_column),
    );

    let (min_val, max_val): (Option<i64>, Option<i64>) =
        sqlx::query_as(sqlx::AssertSqlSafe(query.as_str()))
            .fetch_one(pool)
            .await
            .map_err(|e| ExtractorError::SourceQuery {
                context: format!(
                    "cannot compute partition bounds for {}.{}",
                    schema_name, table_name
                ),
                source: e,
            })?;

    let partitions = keyset_partitions_from_bounds(
        partition_column,
        min_val.unwrap_or(0),
        max_val.unwrap_or(0),
        num_partitions,
    );

    debug!(
        table = %format!("{schema_name}.{table_name}"),
        column = %partition_column,
        partitions = partitions.len(),
        "keyset partitions planned"
    );

    Ok(partitions)
}

/// Pure partitioning math for keyset scans, given already-known column bounds: no
/// database access, so this is unit-testable without a live Postgres — the DB round trip
/// in `compute_keyset_partitions` above is only responsible for producing `min_val`/`max_val`.
///
/// Coverage guarantees (every row matches exactly one predicate):
/// - partition 0: `("col" >= min AND "col" < b1) OR "col" IS NULL` — NULL keys match no
///   range comparison, so they are placed explicitly and exactly once;
/// - middle partitions: `"col" >= bᵢ AND "col" < bᵢ₊₁`;
/// - last partition: `"col" >= bₙ₋₁`, open-ended, so `max` itself (even `i64::MAX`) and keys
///   inserted above it during the scan are included.
///
/// Bound arithmetic runs in `i128`, so any span within `i64::MIN..=i64::MAX` is exact.
/// When the span has fewer keys than `num_partitions`, fewer (never empty) partitions are
/// produced.
fn keyset_partitions_from_bounds(
    partition_column: &str,
    min_val: i64,
    max_val: i64,
    num_partitions: usize,
) -> Vec<ScanPartition> {
    if num_partitions <= 1 || min_val >= max_val {
        if min_val > max_val {
            warn!(
                min = min_val,
                max = max_val,
                "keyset bounds are inverted; scanning unsplit"
            );
        }
        return single_partition();
    }

    let col = quote_ident(partition_column);
    let (min, max) = (i128::from(min_val), i128::from(max_val));
    // Number of distinct keys in [min, max]; at most 2^64, exact in i128.
    let span = max - min + 1;
    let n = i128::try_from(num_partitions)
        .unwrap_or(i128::MAX)
        .min(span);
    let size = (span + n - 1) / n; // ceil: every boundary below is <= max.

    // Interior boundaries b1..b(k-1), all in (min, max] and therefore valid i64 values.
    let boundaries: Vec<i64> = (1..n)
        .map(|i| min + i * size)
        .take_while(|b| *b <= max)
        .filter_map(|b| i64::try_from(b).ok())
        .collect();

    let mut partitions = Vec::with_capacity(boundaries.len() + 1);
    let mut lo = min_val;
    for (i, &hi) in boundaries.iter().enumerate() {
        partitions.push(ScanPartition {
            partition_id: i,
            lo: Some(lo),
            hi: Some(hi),
            kind: PartitionKind::Keyset {
                column: partition_column.to_string(),
            },
            predicate: Some(keyset_predicate(&col, i, lo, Some(hi))),
        });
        lo = hi;
    }
    // For min < max there is always at least one boundary, so this tail is never partition
    // 0 in practice; `keyset_predicate` stays total either way (partition 0 holds NULLs).
    let tail_id = partitions.len();
    partitions.push(ScanPartition {
        partition_id: tail_id,
        lo: Some(lo),
        hi: None,
        kind: PartitionKind::Keyset {
            column: partition_column.to_string(),
        },
        predicate: Some(keyset_predicate(&col, tail_id, lo, None)),
    });
    partitions
}

/// The one rendering of a keyset partition predicate (`col` already quoted).
///
/// Both ends of the key space are open so the partitions cover every row even after the
/// table changed since the bounds were computed (a resumed run reuses stored bounds):
/// partition 0 has no lower bound (`col < hi`, plus NULL keys), and `hi == None` is the
/// open-ended tail. `lo` of partition 0 is therefore informational only.
fn keyset_predicate(col: &str, partition_id: usize, lo: i64, hi: Option<i64>) -> String {
    match (partition_id == 0, hi) {
        (true, Some(hi)) => format!("{col} < {hi} OR {col} IS NULL"),
        (false, Some(hi)) => format!("{col} >= {lo} AND {col} < {hi}"),
        (true, None) => "TRUE".to_string(),
        (false, None) => format!("{col} >= {lo}"),
    }
}

/// Re-create one keyset partition from stored bounds — exactly the partition
/// [`compute_keyset_partitions`] produced when it computed them — so a resumed job scans the
/// same key ranges it planned, instead of recomputing bounds from a table that has since
/// changed. `lo == hi == None` is the whole-table partition (valid only as partition 0).
///
/// Only integer bounds are accepted and the predicate is re-rendered from them (never read
/// back as SQL), so a stored checkpoint cannot inject SQL.
pub fn keyset_partition(
    partition_column: &str,
    partition_id: usize,
    lo: Option<i64>,
    hi: Option<i64>,
) -> Result<ScanPartition, ExtractorError> {
    let predicate = match (lo, hi) {
        (None, None) if partition_id == 0 => None,
        (Some(lo), hi) if hi.is_none_or(|hi| hi > lo) => Some(keyset_predicate(
            &quote_ident(partition_column),
            partition_id,
            lo,
            hi,
        )),
        _ => {
            return Err(ExtractorError::InvalidConfig(format!(
                "invalid stored keyset bounds for partition {partition_id}: lo={lo:?} hi={hi:?}"
            )));
        }
    };
    let kind = match predicate {
        Some(_) => PartitionKind::Keyset {
            column: partition_column.to_string(),
        },
        None => PartitionKind::Whole,
    };
    Ok(ScanPartition {
        partition_id,
        lo,
        hi,
        kind,
        predicate,
    })
}

/// The universal "don't partition" fallback: one partition covering everything.
fn single_partition() -> Vec<ScanPartition> {
    vec![ScanPartition {
        partition_id: 0,
        lo: None,
        hi: None,
        kind: PartitionKind::Whole,
        predicate: None,
    }]
}

/// Computes partition bounds for a ctid-based scan.
/// Divides the table's physical pages into equal ranges.
pub(crate) async fn compute_ctid_partitions(
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
        .map_err(|e| ExtractorError::SourceQuery {
            context: format!("cannot fetch relpages for {}.{}", schema_name, table_name),
            source: e,
        })?
        .flatten()
        .unwrap_or(1);

    let partitions = ctid_partitions_from_relpages(relpages, num_partitions);

    debug!(
        table = %format!("{schema_name}.{table_name}"),
        pages = relpages,
        partitions = partitions.len(),
        "ctid partitions planned"
    );

    Ok(partitions)
}

/// Pure partitioning math for ctid scans, given an already-known page count: no database
/// access, so this is unit-testable without a live Postgres — the DB round trip in
/// `compute_ctid_partitions` above is only responsible for producing `relpages`.
fn ctid_partitions_from_relpages(relpages: i32, num_partitions: usize) -> Vec<ScanPartition> {
    debug_assert!(
        num_partitions > 1,
        "callers must special-case <=1 partitions before this"
    );

    if relpages <= 0 {
        warn!("ctid partitioning found no pages (never analyzed?); scanning unsplit");
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
        let open_tail = i == num_partitions - 1;
        let page_hi = if open_tail {
            // The predicate itself is open-ended (no upper tid bound), so it's correct
            // regardless of `page_hi`'s value. But `page_hi` is still a field callers can
            // read directly (e.g. for logging/display), and `relpages` alone can be
            // *less* than `page_lo` when relpages < num_partitions (more partitions than
            // pages) — reporting that as `hi` would look like an inverted range. Clamp it
            // to `page_lo` so the field always reads as "empty/unbounded", never inverted.
            (relpages as usize).max(page_lo)
        } else {
            (i + 1) * pages_per_partition
        };
        let (lo, hi) = (page_lo as i64, page_hi as i64);

        partitions.push(ScanPartition {
            partition_id: i,
            lo: Some(lo),
            hi: Some(hi),
            kind: PartitionKind::Ctid { open_tail },
            predicate: Some(ctid_predicate(lo, hi, open_tail)),
        });
    }

    partitions
}

/// The one rendering of a ctid page-range predicate: pages `[lo, hi)`, or from `lo` on when
/// `open_tail`.
fn ctid_predicate(lo: i64, hi: i64, open_tail: bool) -> String {
    if open_tail {
        format!("ctid >= '({lo},1)'::tid")
    } else {
        format!("ctid >= '({lo},1)'::tid AND ctid < '({hi},1)'::tid")
    }
}

/// Re-create one ctid partition from its page bounds, as [`compute_ctid_partitions`]
/// produced it. Only page numbers a Postgres block number can hold are accepted (`hi > lo`
/// unless it is the open tail, whose `hi` is display-only), and the predicate is rendered
/// from them, never read back as SQL.
fn ctid_partition(
    partition_id: usize,
    lo: Option<i64>,
    hi: Option<i64>,
    open_tail: bool,
) -> Result<ScanPartition, ExtractorError> {
    let page = |p: i64| u32::try_from(p).is_ok();
    match (lo, hi) {
        (Some(lo), Some(hi)) if page(lo) && page(hi) && (hi > lo || (open_tail && hi >= lo)) => {
            Ok(ScanPartition {
                partition_id,
                lo: Some(lo),
                hi: Some(hi),
                kind: PartitionKind::Ctid { open_tail },
                predicate: Some(ctid_predicate(lo, hi, open_tail)),
            })
        }
        _ => Err(ExtractorError::InvalidConfig(format!(
            "invalid ctid page bounds for partition {partition_id}: lo={lo:?} hi={hi:?} \
             open_tail={open_tail}"
        ))),
    }
}

/// Test-only access to the pure keyset math for other modules' unit tests.
#[cfg(test)]
pub(crate) mod tests_support {
    use super::ScanPartition;

    pub(crate) fn keyset(column: &str, min: i64, max: i64, n: usize) -> Vec<ScanPartition> {
        super::keyset_partitions_from_bounds(column, min, max, n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_strategy_parse() {
        assert_eq!(
            ParallelStrategy::parse("keyset").unwrap(),
            ParallelStrategy::Keyset
        );
        assert_eq!(
            ParallelStrategy::parse("Keyset").unwrap(),
            ParallelStrategy::Keyset
        );
        assert_eq!(
            ParallelStrategy::parse(" CTID ").unwrap(),
            ParallelStrategy::Ctid
        );
        assert_eq!(
            ParallelStrategy::parse("none").unwrap(),
            ParallelStrategy::None
        );
        assert_eq!(
            ParallelStrategy::parse("NONE").unwrap(),
            ParallelStrategy::None
        );
        assert_eq!(ParallelStrategy::parse("").unwrap(), ParallelStrategy::None);
        let err = ParallelStrategy::parse("keyst").unwrap_err();
        assert!(err.to_string().contains("keyst"), "{err}");
    }

    #[test]
    fn test_strategy_serde_uses_lowercase_names() {
        assert_eq!(
            serde_json::to_string(&ParallelStrategy::Keyset).unwrap(),
            "\"keyset\""
        );
        let s: ParallelStrategy = serde_json::from_str("\"ctid\"").unwrap();
        assert_eq!(s, ParallelStrategy::Ctid);
        assert!(serde_json::from_str::<ParallelStrategy>("\"Keyset\"").is_err());
    }

    #[test]
    fn test_keyset_partition_rebuilds_exactly_what_was_computed() {
        // A resumed job re-creates its stored splits from lo/hi alone; the predicate
        // must be byte-identical to the one the original plan scanned.
        for (min, max, n) in [
            (1, 10, 3),
            (0, 1000, 4),
            (i64::MIN, i64::MAX, 4),
            (1, i64::MAX, 2),
            (0, 2, 4),
            (5, 5, 4),
        ] {
            for p in keyset_partitions_from_bounds("id", min, max, n) {
                let rebuilt = keyset_partition("id", p.partition_id, p.lo, p.hi).unwrap();
                assert_eq!(rebuilt.predicate, p.predicate, "{min}..{max}/{n}");
                assert_eq!((rebuilt.lo, rebuilt.hi), (p.lo, p.hi));
            }
        }
        // Inverted or half-missing bounds are rejected, never scanned.
        assert!(keyset_partition("id", 1, Some(5), Some(5)).is_err());
        assert!(keyset_partition("id", 1, None, None).is_err());
    }

    #[test]
    fn test_serialized_partitions_carry_bounds_not_sql() {
        let mut all = keyset_partitions_from_bounds("id", 1, 10, 3);
        all.extend(ctid_partitions_from_relpages(100, 4));
        all.extend(ctid_partitions_from_relpages(2, 4)); // clamped open tail
        all.extend(single_partition());
        for p in &all {
            let json = serde_json::to_string(p).unwrap();
            assert!(!json.contains("predicate"), "SQL text on the wire: {json}");
            let decoded: ScanPartition = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.predicate, p.predicate, "{json}");
            assert_eq!((decoded.lo, decoded.hi), (p.lo, p.hi));
            assert_eq!(decoded.kind, p.kind);
        }
    }

    #[test]
    fn test_decoded_partitions_never_trust_sql_or_impossible_bounds() {
        let decode = |json: &str| serde_json::from_str::<ScanPartition>(json);
        // A smuggled predicate is ignored: the SQL comes from the typed bounds alone.
        let p = decode(
            r#"{"partition_id":1,"lo":5,"hi":9,"kind":{"Keyset":{"column":"id"}},
                "predicate":"TRUE; DROP TABLE t"}"#,
        )
        .unwrap();
        assert_eq!(p.predicate.as_deref(), Some(r#""id" >= 5 AND "id" < 9"#));
        // A hostile column name is a quoted identifier, never SQL.
        let p = decode(
            r#"{"partition_id":1,"lo":5,"hi":null,"kind":{"Keyset":{"column":"x\" OR TRUE --"}}}"#,
        )
        .unwrap();
        assert_eq!(p.predicate.as_deref(), Some(r#""x"" OR TRUE --" >= 5"#));
        // Bounds this module never produces are rejected, not scanned.
        for bad in [
            r#"{"partition_id":1,"lo":9,"hi":5,"kind":{"Keyset":{"column":"id"}}}"#,
            r#"{"partition_id":0,"lo":1,"hi":2,"kind":"Whole"}"#,
            r#"{"partition_id":0,"lo":-1,"hi":2,"kind":{"Ctid":{"open_tail":false}}}"#,
            r#"{"partition_id":0,"lo":3,"hi":3,"kind":{"Ctid":{"open_tail":false}}}"#,
            r#"{"partition_id":0,"lo":0,"hi":4294967296,"kind":{"Ctid":{"open_tail":false}}}"#,
            r#"{"partition_id":0,"lo":null,"hi":2,"kind":{"Ctid":{"open_tail":true}}}"#,
        ] {
            assert!(decode(bad).is_err(), "accepted {bad}");
        }
    }

    // --- keyset_partitions_from_bounds: real math, no DB needed ---

    /// Evaluate a generated predicate the way Postgres would, for a key (None = NULL).
    /// Only understands the shapes this module emits.
    fn matches(p: &ScanPartition, key: Option<i64>) -> bool {
        let lo_ok = |k: i64| p.lo.is_none_or(|lo| k >= lo);
        let hi_ok = |k: i64| p.hi.is_none_or(|hi| k < hi);
        let nulls = p
            .predicate
            .as_deref()
            .is_some_and(|s| s.contains("IS NULL"));
        match key {
            None => p.predicate.is_none() || nulls,
            Some(k) => p.predicate.is_none() || (lo_ok(k) && hi_ok(k)),
        }
    }

    /// Every key (and NULL) must fall in exactly one partition.
    fn assert_exact_cover(partitions: &[ScanPartition], keys: &[Option<i64>]) {
        for &k in keys {
            let hits = partitions.iter().filter(|p| matches(p, k)).count();
            assert_eq!(
                hits, 1,
                "key {k:?} matched {hits} partitions: {partitions:?}"
            );
        }
        for w in partitions.windows(2) {
            assert_eq!(w[0].hi, w[1].lo, "partitions must be contiguous");
        }
        for (i, p) in partitions.iter().enumerate() {
            assert_eq!(p.partition_id, i);
        }
    }

    #[test]
    fn test_keyset_partitions_even_split_covers_full_range_contiguously() {
        // 0..=1000 is 1001 keys -> ceil(1001/4) = 251 per partition.
        let partitions = keyset_partitions_from_bounds("id", 0, 1000, 4);
        assert_eq!(partitions.len(), 4);
        let bounds: Vec<_> = partitions.iter().map(|p| (p.lo, p.hi)).collect();
        assert_eq!(
            bounds,
            vec![
                (Some(0), Some(251)),
                (Some(251), Some(502)),
                (Some(502), Some(753)),
                (Some(753), None),
            ]
        );
        assert_exact_cover(
            &partitions,
            &[
                None,
                Some(0),
                Some(250),
                Some(251),
                Some(999),
                Some(1000),
                Some(5000),
            ],
        );
    }

    #[test]
    fn test_keyset_first_partition_has_no_lower_bound() {
        // NEW-3: a resumed run reuses stored bounds, so a key inserted below the planned MIN
        // must still fall into partition 0.
        let p = keyset_partition("id", 0, Some(100), Some(200)).unwrap();
        assert_eq!(
            p.predicate.as_deref(),
            Some(r#""id" < 200 OR "id" IS NULL"#)
        );
        let whole = keyset_partitions_from_bounds("id", 7, 7, 4);
        assert!(whole.iter().all(|p| p.predicate.as_deref() != Some("")));
    }

    #[test]
    fn test_keyset_predicates_include_nulls_once_and_open_tail() {
        // Exact generated SQL.
        let partitions = keyset_partitions_from_bounds("k", 1, 10, 3);
        let preds: Vec<_> = partitions
            .iter()
            .map(|p| p.predicate.clone().unwrap())
            .collect();
        assert_eq!(
            preds,
            vec![
                r#""k" < 5 OR "k" IS NULL"#.to_string(),
                r#""k" >= 5 AND "k" < 9"#.to_string(),
                r#""k" >= 9"#.to_string(),
            ]
        );
        assert_eq!(
            preds.iter().filter(|p| p.contains("IS NULL")).count(),
            1,
            "NULL keys must belong to exactly one partition"
        );
    }

    #[test]
    fn test_keyset_partitions_i64_max_key_is_included() {
        // `max = i64::MAX` used to compute `max + 1` (overflow) and drop the max row.
        let partitions = keyset_partitions_from_bounds("id", 1, i64::MAX, 2);
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[1].hi, None);
        assert_exact_cover(
            &partitions,
            &[None, Some(1), Some(2), Some(i64::MAX - 1), Some(i64::MAX)],
        );
    }

    #[test]
    fn test_keyset_partitions_full_i64_span_does_not_overflow() {
        // `max - min` overflowed i64 for spans wider than i64::MAX (random/hashed keys).
        let partitions = keyset_partitions_from_bounds("id", i64::MIN, i64::MAX, 4);
        assert_eq!(partitions.len(), 4);
        assert_eq!(partitions[0].lo, Some(i64::MIN));
        assert_eq!(partitions[1].hi, Some(0));
        assert_exact_cover(
            &partitions,
            &[
                None,
                Some(i64::MIN),
                Some(-1),
                Some(0),
                Some(1),
                Some(i64::MAX),
            ],
        );
        let partitions = keyset_partitions_from_bounds("id", i64::MIN, i64::MIN + 1, 8);
        assert_eq!(partitions.len(), 2);
        assert_exact_cover(&partitions, &[None, Some(i64::MIN), Some(i64::MIN + 1)]);
    }

    #[test]
    fn test_keyset_partitions_fewer_keys_than_partitions_never_empty() {
        // 0..=2 is 3 keys: at most 3 partitions, none inverted or empty.
        let partitions = keyset_partitions_from_bounds("id", 0, 2, 4);
        assert_eq!(partitions.len(), 3);
        for p in &partitions {
            if let (Some(lo), Some(hi)) = (p.lo, p.hi) {
                assert!(hi > lo, "partition range must never be empty/inverted");
            }
        }
        assert_exact_cover(&partitions, &[None, Some(0), Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn test_keyset_partitions_min_equals_max_falls_back_to_single_partition() {
        // A degenerate (or entirely-NULL) column collapses min == max: one unbounded
        // partition (predicate None = whole table, NULLs included).
        let partitions = keyset_partitions_from_bounds("id", 42, 42, 4);
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].lo, None);
        assert_eq!(partitions[0].hi, None);
        assert_eq!(partitions[0].predicate, None);
    }

    #[test]
    fn test_keyset_partitions_min_greater_than_max_falls_back_to_single_partition() {
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
        assert_eq!(
            partitions[0].predicate.as_deref(),
            Some("ctid >= '(0,1)'::tid AND ctid < '(25,1)'::tid")
        );
        assert_eq!(partitions[3].lo, Some(75));
        assert_eq!(partitions[3].hi, Some(100));
        // Last partition is open-ended (no upper tid bound) so late-arriving pages beyond
        // the relpages estimate are still scanned.
        assert_eq!(
            partitions[3].predicate.as_deref(),
            Some("ctid >= '(75,1)'::tid")
        );
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
        assert_eq!(
            partitions[3].hi,
            Some(3),
            "clamped to lo, not the smaller relpages value"
        );
        assert_eq!(
            partitions[3].predicate.as_deref(),
            Some("ctid >= '(3,1)'::tid")
        );
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
