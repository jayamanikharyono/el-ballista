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
            ORDER BY attname
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

        let columns = stats_rows
            .into_iter()
            .map(|(column_name, n_distinct, null_frac, avg_width)| {
                let stat = ColumnStats {
                    column_name: column_name.clone(),
                    n_distinct: absolute_n_distinct(n_distinct, row_count),
                    null_frac,
                    avg_width,
                };
                (column_name, stat)
            })
            .collect();

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
}
