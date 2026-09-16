//! Source statistics collection and caching.
//! pushdown/stats.rs
//! Gathers table and column statistics from the source for use by the cost model.
//! Cached with TTL to avoid hammering the catalog on every plan.

use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use async_trait::async_trait;

use crate::connector::errors::ExtractorError;

/// Per-column statistics from pg_stats.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub column_name: String,
    pub n_distinct: f64,
    pub null_frac: f32,
    pub avg_width: i32,
}

/// Per-table and per-column statistics used by the cost model.
#[derive(Debug, Clone)]
pub struct SourceStatistics {
    pub table_name: String,
    pub row_count_estimate: f64,
    pub table_size_bytes: u64,
    pub columns: HashMap<String, ColumnStats>,
    pub fetched_at: DateTime<Utc>,
}

/// Index information from pg_index. Lives here (rather than cost_model) so both the
/// statistics collector and the cost model share one definition without a module cycle.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
    pub index_type: String, // btree, hash, gist, gin, etc.
}

impl SourceStatistics {
    /// Empty statistics for contexts that cannot reach the source (e.g. a provider rebuilt
    /// from a serialized plan on a scheduler). Selectivity falls back to conservative
    /// defaults, so cost-based decisions degrade to keeping — never to pushing blindly.
    pub fn empty(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            row_count_estimate: 0.0,
            table_size_bytes: 0,
            columns: HashMap::new(),
            fetched_at: Utc::now(),
        }
    }
}

pub struct StatisticsCollector {
    pool: Arc<PgPool>,
    cache: Arc<RwLock<HashMap<String, SourceStatistics>>>,
    ttl_secs: u64,
}

impl StatisticsCollector {
    pub fn new(pool: Arc<PgPool>, ttl_secs: u64) -> Self {
        Self {
            pool,
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl_secs,
        }
    }

    /// Fetch or cached table statistics. If cached and fresh, returns the cached version;
    /// otherwise fetches from pg_stats and pg_class and updates the cache.
    pub async fn get_statistics(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<SourceStatistics, ExtractorError> {
        let cache_key = format!("{}.{}", schema_name, table_name);

        // Check cache.
        {
            let cache = self.cache.read().await;
            if let Some(stats) = cache.get(&cache_key) {
                let age = Utc::now().signed_duration_since(stats.fetched_at);
                if age < Duration::seconds(self.ttl_secs as i64) {
                    return Ok(stats.clone());
                }
            }
        }

        // Fetch fresh statistics through the source trait (per-backend catalog queries).
        let stats = self.pool.table_statistics(schema_name, table_name).await?;

        // Update cache.
        {
            let mut cache = self.cache.write().await;
            cache.insert(cache_key, stats.clone());
        }

        Ok(stats)
    }
}

/// What the cost model needs from a source, independent of backend: table/column statistics
/// (`pg_stats` on Postgres; the MySQL equivalent per docs/connectors/mysql.md §6) and index
/// metadata (`pg_index`; MySQL `SHOW INDEX`). [`StatisticsCollector`] adds TTL caching on top.
#[async_trait]
pub trait TableStatsSource {
    /// Row-count estimate, table size, and per-column distinctness/null-fraction/width.
    async fn table_statistics(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<SourceStatistics, ExtractorError>;

    /// Index metadata for recognizing near-free indexed predicates.
    async fn table_indexes(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Vec<IndexInfo>, ExtractorError>;

    /// Columns whose type is a true Postgres enum (present in `pg_enum`). The pushdown
    /// layer recasts enum-vs-text comparisons to label comparisons for exactly these
    /// columns; everything else keeps native operator resolution. Defaults to empty
    /// (no normalization) for backends without enum catalogs.
    async fn table_enum_columns(
        &self,
        _schema_name: &str,
        _table_name: &str,
    ) -> Result<std::collections::HashSet<String>, ExtractorError> {
        Ok(std::collections::HashSet::new())
    }
}

#[async_trait]
impl TableStatsSource for PgPool {
    async fn table_statistics(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<SourceStatistics, ExtractorError> {
        // Fetch table-level stats from pg_class/nspname — the old
        // `pg_class JOIN information_schema.tables ON relname=table_name` cross-matched
        // same-named tables in other schemas and included toast/index rels.
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
        .map_err(|e| {
            ExtractorError::Statistics(format!(
                "cannot fetch table statistics for {}.{}: {}",
                schema_name, table_name, e
            ))
        })?
        .unwrap_or((0.0, 0));

        // Fetch column-level stats from pg_stats.
        let stats_rows: Vec<(String, f64, f32, i32)> = sqlx::query_as(
            r#"
            SELECT
                attname AS column_name,
                COALESCE(n_distinct, 1)::float8 AS n_distinct,
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
        .map_err(|e| {
            ExtractorError::Statistics(format!(
                "cannot fetch column statistics for {}.{}: {}",
                schema_name, table_name, e
            ))
        })?;

        let mut columns = HashMap::new();
        for (column_name, n_distinct, null_frac, avg_width) in stats_rows {
            let stat = ColumnStats {
                column_name: column_name.clone(),
                n_distinct,
                null_frac,
                avg_width,
            };
            columns.insert(column_name, stat);
        }

        Ok(SourceStatistics {
            table_name: table_name.to_string(),
            row_count_estimate: row_count,
            table_size_bytes: table_size_bytes as u64,
            columns,
            fetched_at: Utc::now(),
        })
    }

    /// Index metadata from pg_index, so the cost model recognizes near-free indexed
    /// predicates without needing EXPLAIN.
    async fn table_indexes(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Vec<IndexInfo>, ExtractorError> {
        let rows: Vec<(String, Vec<String>, bool, bool, String)> = sqlx::query_as(
            r#"
            SELECT
                i.relname AS index_name,
                COALESCE(array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)), '{}') AS columns,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary,
                am.amname AS index_type
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (ix.indkey)
            WHERE n.nspname = $1
              AND t.relname = $2
            GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname
            ORDER BY i.relname
            "#,
        )
        .bind(schema_name)
        .bind(table_name)
        .fetch_all(self)
        .await
        .map_err(|e| {
            ExtractorError::Statistics(format!(
                "cannot fetch index metadata for {}.{}: {}",
                schema_name, table_name, e
            ))
        })?;

        Ok(rows
            .into_iter()
            .map(
                |(name, columns, is_unique, is_primary, index_type)| IndexInfo {
                    name,
                    columns,
                    is_unique,
                    is_primary,
                    index_type,
                },
            )
            .collect())
    }

    async fn table_enum_columns(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<std::collections::HashSet<String>, ExtractorError> {
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
        .map_err(|e| {
            ExtractorError::Statistics(format!(
                "cannot fetch enum metadata for {}.{}: {}",
                schema_name, table_name, e
            ))
        })?;

        Ok(rows.into_iter().map(|(column_name,)| column_name).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_statistics_has_no_column_entries() {
        // `SourceStatistics::empty` is used whenever the source is unreachable (e.g. a
        // provider rebuilt from a serialized plan on a scheduler, per its doc comment).
        // Its whole contract is "the cost model must fall back to conservative defaults,
        // never push blindly" — which only holds if there really is no column data to
        // look up. A regression that started populating `columns` here would silently
        // make the cost model behave as if it had real statistics.
        let stats = SourceStatistics::empty("orders");
        assert_eq!(stats.table_name, "orders");
        assert_eq!(stats.row_count_estimate, 0.0);
        assert_eq!(stats.table_size_bytes, 0);
        assert!(stats.columns.is_empty());
        assert!(!stats.columns.contains_key("any_column"));
    }

    #[test]
    fn test_empty_statistics_distinct_tables_are_independent() {
        // Two `empty()` calls for different tables must not alias any shared state
        // (e.g. a `HashMap::new()` refactored into a shared static by mistake).
        let a = SourceStatistics::empty("orders");
        let b = SourceStatistics::empty("customers");
        assert_ne!(a.table_name, b.table_name);
        assert!(a.columns.is_empty() && b.columns.is_empty());
    }
}
