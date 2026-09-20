//! Postgres statistics collection. The backend-agnostic types and the [`TableStatsSource`] trait
//! live in [`crate::pushdown::stats`]; this is the Postgres implementation (`pg_class`, `pg_stats`,
//! `pg_index`, `pg_enum`) plus the TTL-caching [`StatisticsCollector`].
//!
//! [`TableStatsSource`]: crate::pushdown::stats::TableStatsSource

use chrono::{Duration, Utc};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use async_trait::async_trait;

use crate::connector::errors::ExtractorError;
use crate::pushdown::stats::{ColumnStats, IndexInfo, SourceStatistics, TableStatsSource};

/// TTL cache over a [`TableStatsSource`] so the catalog is not hit on every plan.
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

        {
            let mut cache = self.cache.write().await;
            cache.insert(cache_key, stats.clone());
        }

        Ok(stats)
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
