//! Postgres statistics collection. The backend-agnostic types and the [`TableStatsSource`] trait
//! live in [`crate::pushdown::stats`]; this is the Postgres implementation (`pg_class`,
//! `pg_stats`, `pg_index`, `pg_enum`) plus the TTL-caching [`StatisticsCollector`].
//!
//! [`TableStatsSource`]: crate::pushdown::stats::TableStatsSource

use async_trait::async_trait;
use chrono::{Duration, Utc};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::connector::errors::ExtractorError;
use crate::pushdown::stats::{ColumnStats, IndexInfo, SourceStatistics, TableStatsSource};

/// TTL cache over a [`TableStatsSource`] so the catalog is not hit on every plan.
pub struct StatisticsCollector {
    pool: Arc<PgPool>,
    cache: Arc<RwLock<HashMap<String, SourceStatistics>>>,
    ttl_secs: u64,
}

impl StatisticsCollector {
    /// A collector over `pool` whose cached statistics expire after `ttl_secs`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo(ex: &rust_ballista_extraction_layer::connector::postgres::PostgresExtractor)
    /// # -> Result<(), rust_ballista_extraction_layer::connector::errors::ExtractorError> {
    /// use std::sync::Arc;
    /// use rust_ballista_extraction_layer::connector::postgres::stats::StatisticsCollector;
    ///
    /// let collector = StatisticsCollector::new(Arc::new(ex.pool().clone()), 900);
    /// let stats = collector.get_statistics("public", "orders").await?; // cached for 15 min
    /// println!("~{} rows, {} bytes", stats.row_count_estimate, stats.table_size_bytes);
    /// # Ok(()) }
    /// ```
    pub fn new(pool: Arc<PgPool>, ttl_secs: u64) -> Self {
        Self {
            pool,
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl_secs,
        }
    }

    /// Fetch or cached table statistics. If cached and fresh, returns the cached version;
    /// otherwise fetches from pg_stats and pg_class and updates the cache.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo(ex: &rust_ballista_extraction_layer::connector::postgres::PostgresExtractor)
    /// # -> Result<(), rust_ballista_extraction_layer::connector::errors::ExtractorError> {
    /// use std::sync::Arc;
    /// use rust_ballista_extraction_layer::connector::postgres::stats::StatisticsCollector;
    ///
    /// let collector = StatisticsCollector::new(Arc::new(ex.pool().clone()), 900);
    /// let stats = collector.get_statistics("public", "orders").await?; // cached for 15 min
    /// println!("~{} rows, {} bytes", stats.row_count_estimate, stats.table_size_bytes);
    /// # Ok(()) }
    /// ```
    pub async fn get_statistics(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<SourceStatistics, ExtractorError> {
        let cache_key = format!("{}.{}", schema_name, table_name);
        let ttl = Duration::seconds(i64::try_from(self.ttl_secs).unwrap_or(i64::MAX));

        {
            let cache = self.cache.read().await;
            if let Some(stats) = cache.get(&cache_key) {
                let age = Utc::now().signed_duration_since(stats.fetched_at);
                if age < ttl {
                    return Ok(stats.clone());
                }
            }
        }

        let stats = self.pool.table_statistics(schema_name, table_name).await?;

        {
            let mut cache = self.cache.write().await;
            cache.insert(cache_key, stats.clone());
        }

        Ok(stats)
    }
}

/// Postgres encodes `pg_stats.n_distinct < 0` as a negated *fraction* of the row count
/// (`-1` = every row distinct). Convert to the absolute count [`ColumnStats`] expects; with no
/// usable row estimate the result is `0.0` ("unknown").
fn absolute_n_distinct(raw: f64, row_count: f64) -> f64 {
    if raw >= 0.0 {
        raw
    } else {
        (-raw) * row_count.max(0.0)
    }
}

/// One row per ordered column: `(column, histogram_bounds, most_common_vals,
/// most_common_freqs)`, the value arrays already on the numeric axis of
/// [`crate::pushdown::stats::ordinal`].
type DistributionRow = (
    String,
    Option<Vec<f64>>,
    Option<Vec<Option<f64>>>,
    Option<Vec<f64>>,
);

/// Histogram bounds and most-common values of the integer, date and timestamp columns, converted
/// in SQL to `float8` positions: integers as-is, dates as days since 1970-01-01, timestamps as
/// epoch seconds (`timestamp` without zone read as UTC, like the extractor). `pg_stats` exposes
/// both as `anyarray`, so they go through their text form into the column's real array type.
/// Infinite dates/timestamps are dropped. With inheritance or partitions, the `inherited = true`
/// row (what a scan of the parent reads) wins by sorting last.
async fn fetch_ordered_distributions(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<DistributionRow>, sqlx::Error> {
    sqlx::query_as::<_, DistributionRow>(
        r#"
        WITH cols AS (
            SELECT s.attname::text AS attname,
                   s.inherited,
                   s.histogram_bounds::text AS hist,
                   s.most_common_vals::text AS mcv,
                   s.most_common_freqs::float8[] AS freqs,
                   t.typname::text AS typname
            FROM pg_stats s
            JOIN pg_namespace n ON n.nspname = s.schemaname
            JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = s.tablename
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = s.attname
            JOIN pg_type t ON t.oid = a.atttypid
            WHERE s.schemaname = $1
              AND s.tablename = $2
              AND t.typname IN ('int2', 'int4', 'int8', 'date', 'timestamp', 'timestamptz')
        )
        SELECT attname,
               CASE typname
                   WHEN 'date' THEN ARRAY(
                       SELECT (v - DATE '1970-01-01')::float8
                       FROM unnest(hist::date[]) v WHERE isfinite(v))
                   WHEN 'timestamp' THEN ARRAY(
                       SELECT extract(epoch FROM v)::float8
                       FROM unnest(hist::timestamp[]) v WHERE isfinite(v))
                   WHEN 'timestamptz' THEN ARRAY(
                       SELECT extract(epoch FROM v)::float8
                       FROM unnest(hist::timestamptz[]) v WHERE isfinite(v))
                   ELSE hist::float8[]
               END AS histogram_bounds,
               CASE typname
                   WHEN 'date' THEN ARRAY(
                       SELECT CASE WHEN isfinite(v) THEN (v - DATE '1970-01-01')::float8 END
                       FROM unnest(mcv::date[]) WITH ORDINALITY u(v, i) ORDER BY i)
                   WHEN 'timestamp' THEN ARRAY(
                       SELECT CASE WHEN isfinite(v) THEN extract(epoch FROM v)::float8 END
                       FROM unnest(mcv::timestamp[]) WITH ORDINALITY u(v, i) ORDER BY i)
                   WHEN 'timestamptz' THEN ARRAY(
                       SELECT CASE WHEN isfinite(v) THEN extract(epoch FROM v)::float8 END
                       FROM unnest(mcv::timestamptz[]) WITH ORDINALITY u(v, i) ORDER BY i)
                   ELSE mcv::float8[]
               END AS most_common_vals,
               freqs AS most_common_freqs
        FROM cols
        ORDER BY attname, inherited
        "#,
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(pool)
    .await
}

/// Histogram bounds as the cost model needs them: finite and ascending. Anything else (fewer
/// than two bounds, NaN) is treated as "no histogram".
fn finite_sorted(bounds: Vec<f64>) -> Vec<f64> {
    let ok = bounds.len() >= 2
        && bounds.iter().all(|b| b.is_finite())
        && bounds.windows(2).all(|w| w[0] <= w[1]);
    if ok { bounds } else { Vec::new() }
}

/// Most-common values paired with their frequencies. A length mismatch (should not happen)
/// drops both. An infinite most-common value (NULL from the SQL above) is omitted, so its
/// share is folded into the histogram part of the estimate — a small approximation for a
/// rare case.
fn paired_mcv(vals: Option<Vec<Option<f64>>>, freqs: Option<Vec<f64>>) -> (Vec<f64>, Vec<f64>) {
    match (vals, freqs) {
        (Some(vals), Some(freqs)) if vals.len() == freqs.len() => vals
            .into_iter()
            .zip(freqs)
            .filter_map(|(v, f)| v.filter(|v| v.is_finite()).map(|v| (v, f)))
            .unzip(),
        _ => (Vec::new(), Vec::new()),
    }
}

#[async_trait]
impl TableStatsSource for PgPool {
    async fn table_statistics(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<SourceStatistics, ExtractorError> {
        // Table-level stats from pg_class/pg_namespace (schema-qualified; heap/partitioned only).
        let (row_count, table_size_bytes) = sqlx::query_as::<_, (f64, i64)>(
            r#"
            SELECT
                COALESCE(c.reltuples, 0)::float8 AS row_count,
                COALESCE(pg_total_relation_size(c.oid), 0)::int8 AS table_size
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1
              AND c.relname = $2
              AND c.relkind IN ('r', 'p')
            "#,
        )
        .bind(schema_name)
        .bind(table_name)
        .fetch_optional(self)
        .await
        .map_err(|e| ExtractorError::SourceQuery {
            context: format!(
                "cannot fetch table statistics for {}.{}",
                schema_name, table_name
            ),
            source: e,
        })?
        .unwrap_or((0.0, 0));
        // PG 14+ reports reltuples = -1 for a never-vacuumed/analyzed table: unknown, not
        // negative.
        let row_count = row_count.max(0.0);

        let stats_rows: Vec<(String, f64, f32, i32)> = sqlx::query_as(
            r#"
            SELECT
                attname AS column_name,
                COALESCE(n_distinct, 0)::float8 AS n_distinct,
                COALESCE(null_frac, 0)::float4 AS null_frac,
                COALESCE(avg_width, 4)::int4 AS avg_width
            FROM pg_stats
            WHERE schemaname = $1
              AND tablename = $2
            -- With inheritance/partitions a column has an `inherited = true` row too (what a
            -- scan of the parent reads); sorting it last makes it the one kept, matching the
            -- histogram query below.
            ORDER BY attname, inherited
            "#,
        )
        .bind(schema_name)
        .bind(table_name)
        .fetch_all(self)
        .await
        .map_err(|e| ExtractorError::SourceQuery {
            context: format!(
                "cannot fetch column statistics for {}.{}",
                schema_name, table_name
            ),
            source: e,
        })?;

        let mut columns: HashMap<String, ColumnStats> = stats_rows
            .into_iter()
            .map(|(column_name, n_distinct, null_frac, avg_width)| {
                let stat = ColumnStats {
                    column_name: column_name.clone(),
                    n_distinct: absolute_n_distinct(n_distinct, row_count),
                    null_frac,
                    avg_width,
                    ..Default::default()
                };
                (column_name, stat)
            })
            .collect();

        // Histograms and most-common values are an estimate refinement: without them the
        // cost model falls back to its defaults, so a failure here only logs.
        match fetch_ordered_distributions(self, schema_name, table_name).await {
            Ok(rows) => {
                for (column_name, bounds, mcv, freqs) in rows {
                    if let Some(stat) = columns.get_mut(&column_name) {
                        stat.histogram_bounds = finite_sorted(bounds.unwrap_or_default());
                        let (vals, freqs) = paired_mcv(mcv, freqs);
                        stat.most_common_vals = vals;
                        stat.most_common_freqs = freqs;
                    }
                }
            }
            Err(e) => log::warn!(
                "histogram statistics unavailable for {schema_name}.{table_name}: {e}; range \
                 estimates use defaults"
            ),
        }

        Ok(SourceStatistics {
            table_name: table_name.to_string(),
            row_count_estimate: row_count,
            table_size_bytes: u64::try_from(table_size_bytes).unwrap_or(0),
            columns,
            fetched_at: Utc::now(),
        })
    }

    /// Index metadata from pg_index, so the cost model recognizes near-free indexed
    /// predicates without needing EXPLAIN. Expression keys are omitted from `columns` (and
    /// flagged via `has_expressions`), partial indexes are flagged via `is_partial`.
    async fn table_indexes(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Vec<IndexInfo>, ExtractorError> {
        let rows: Vec<(String, Vec<String>, bool, bool, String, bool, bool)> = sqlx::query_as(
            r#"
            SELECT
                i.relname AS index_name,
                COALESCE(
                    array_agg(a.attname ORDER BY array_position(ix.indkey::int2[], a.attnum))
                        FILTER (WHERE a.attname IS NOT NULL),
                    '{}'
                ) AS columns,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary,
                am.amname AS index_type,
                ix.indpred IS NOT NULL AS is_partial,
                ix.indexprs IS NOT NULL AS has_expressions
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (ix.indkey)
            WHERE n.nspname = $1
              AND t.relname = $2
            GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname,
                     ix.indpred IS NOT NULL, ix.indexprs IS NOT NULL
            ORDER BY i.relname
            "#,
        )
        .bind(schema_name)
        .bind(table_name)
        .fetch_all(self)
        .await
        .map_err(|e| ExtractorError::SourceQuery {
            context: format!(
                "cannot fetch index metadata for {}.{}",
                schema_name, table_name
            ),
            source: e,
        })?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    name,
                    columns,
                    is_unique,
                    is_primary,
                    index_type,
                    is_partial,
                    has_expressions,
                )| {
                    IndexInfo {
                        name,
                        columns,
                        is_unique,
                        is_primary,
                        index_type,
                        is_partial,
                        has_expressions,
                    }
                },
            )
            .collect())
    }

    async fn table_enum_columns(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<HashSet<String>, ExtractorError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT DISTINCT a.attname AS column_name
            FROM pg_attribute a
            JOIN pg_class t ON t.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_type ty ON ty.oid = a.atttypid
            JOIN pg_enum e ON e.enumtypid = ty.oid
            WHERE n.nspname = $1
              AND t.relname = $2
            "#,
        )
        .bind(schema_name)
        .bind(table_name)
        .fetch_all(self)
        .await
        .map_err(|e| ExtractorError::SourceQuery {
            context: format!(
                "cannot fetch enum metadata for {}.{}",
                schema_name, table_name
            ),
            source: e,
        })?;

        Ok(rows.into_iter().map(|(column_name,)| column_name).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_negative_n_distinct_is_a_fraction_of_rows() {
        assert_eq!(absolute_n_distinct(250.0, 1000.0), 250.0);
        assert_eq!(absolute_n_distinct(-1.0, 1000.0), 1000.0);
        assert_eq!(absolute_n_distinct(-0.5, 1000.0), 500.0);
        // Unknown row count: unknown distinctness, never negative.
        assert_eq!(absolute_n_distinct(-1.0, -1.0), 0.0);
    }

    #[test]
    fn test_histogram_bounds_must_be_finite_and_ascending() {
        assert_eq!(
            finite_sorted(vec![1.0, 2.0, 2.0, 5.0]),
            vec![1.0, 2.0, 2.0, 5.0]
        );
        assert!(finite_sorted(vec![1.0]).is_empty());
        assert!(finite_sorted(vec![2.0, 1.0]).is_empty());
        assert!(finite_sorted(vec![1.0, f64::NAN, 3.0]).is_empty());
        assert!(finite_sorted(vec![1.0, f64::INFINITY]).is_empty());
    }

    #[test]
    fn test_most_common_values_pair_with_frequencies() {
        let (vals, freqs) = paired_mcv(
            Some(vec![Some(3.0), None, Some(1.0)]),
            Some(vec![0.5, 0.2, 0.1]),
        );
        // The infinite (NULL) value is dropped together with its frequency.
        assert_eq!((vals, freqs), (vec![3.0, 1.0], vec![0.5, 0.1]));
        // Mismatched lengths or a missing side: nothing.
        assert_eq!(
            paired_mcv(Some(vec![Some(1.0)]), Some(vec![])),
            (vec![], vec![])
        );
        assert_eq!(paired_mcv(None, Some(vec![0.5])), (vec![], vec![]));
    }
}
