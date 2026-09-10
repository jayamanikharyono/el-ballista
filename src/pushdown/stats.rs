//! Source statistics collection and caching.
//! pushdown/stats.rs
//! Gathers table and column statistics from the source for use by the cost model.
//! Cached with TTL to avoid hammering the catalog on every plan.

use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::extractor::errors::ExtractorError;

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

        // Fetch fresh statistics.
        let stats = self.fetch_statistics(schema_name, table_name).await?;

        // Update cache.
        {
            let mut cache = self.cache.write().await;
            cache.insert(cache_key, stats.clone());
        }

        Ok(stats)
    }

    async fn fetch_statistics(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<SourceStatistics, ExtractorError> {
        // Fetch table-level stats from pg_class.
        let (row_count, table_size_bytes) = sqlx::query_as::<_, (f64, i64)>(
            r#"
            SELECT
                COALESCE(reltuples, 0)::float8 AS row_count,
                COALESCE(pg_total_relation_size(relid), 0)::int8 AS table_size
            FROM pg_class
            JOIN information_schema.tables ON
                pg_class.relname = information_schema.tables.table_name
            WHERE information_schema.tables.table_schema = $1
              AND information_schema.tables.table_name = $2
            "#,
        )
        .bind(schema_name)
        .bind(table_name)
        .fetch_optional(self.pool.as_ref())
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
        .fetch_all(self.pool.as_ref())
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_stats_creation() {
        let stats = ColumnStats {
            column_name: "id".to_string(),
            n_distinct: 1000.0,
            null_frac: 0.0,
            avg_width: 8,
        };

        assert_eq!(stats.column_name, "id");
        assert_eq!(stats.n_distinct, 1000.0);
    }

    #[test]
    fn test_source_statistics_creation() {
        let stats = SourceStatistics {
            table_name: "orders".to_string(),
            row_count_estimate: 10000.0,
            table_size_bytes: 1024 * 1024,
            columns: HashMap::new(),
            fetched_at: Utc::now(),
        };

        assert_eq!(stats.table_name, "orders");
        assert_eq!(stats.row_count_estimate, 10000.0);
    }
}
