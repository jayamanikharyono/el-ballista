//! Backend-agnostic source statistics consumed by the cost model.
//! pushdown/stats.rs
//! The types the cost model reads, plus the [`TableStatsSource`] trait a connector implements to
//! fill them (the Postgres implementation over `pg_class`/`pg_stats`/`pg_index` lives in
//! `connector::postgres::stats`).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};

use crate::connector::errors::ExtractorError;

/// Per-column statistics.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub column_name: String,
    /// Estimated number of distinct values, as an absolute count. Connectors convert any
    /// source-specific encoding (Postgres stores negative fractions of the row count) before
    /// filling this in; a value `<= 0` means "unknown".
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

/// Index metadata. Lives here (rather than cost_model) so both the statistics collector and the
/// cost model share one definition without a module cycle.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    /// Plain key columns in index order. Expression keys are not listed (see
    /// `has_expressions`), so `columns[0]` is the leading key only when `has_expressions` is
    /// false.
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
    pub index_type: String, // btree, hash, gist, gin, etc.
    /// Partial index (`WHERE ...`): only serves predicates implying its condition.
    pub is_partial: bool,
    /// At least one key is an expression rather than a plain column.
    pub has_expressions: bool,
}

impl SourceStatistics {
    /// Empty statistics for contexts that cannot reach the source (e.g. a provider rebuilt
    /// from a serialized plan on a scheduler). Selectivity falls back to conservative
    /// defaults, so cost-based decisions degrade to keeping — never to pushing blindly.
    ///
    /// # Examples
    /// ```
    /// use rust_ballista_extraction_layer::pushdown::stats::SourceStatistics;
    /// let stats = SourceStatistics::empty("orders");
    /// assert!(stats.columns.is_empty());
    /// ```
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

/// What the cost model needs from a source, independent of backend: table/column statistics
/// (`pg_stats` on Postgres; the MySQL equivalent per docs/connectors/mysql.md §6) and index
/// metadata (`pg_index`; MySQL `SHOW INDEX`).
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

    /// Columns whose type is an enumerated type. Connectors classify these as
    /// [`ColumnKind::Label`](crate::pushdown::ColumnKind::Label) so comparisons go through
    /// the label text. Defaults to empty for backends without enum catalogs.
    async fn table_enum_columns(
        &self,
        _schema_name: &str,
        _table_name: &str,
    ) -> Result<HashSet<String>, ExtractorError> {
        Ok(HashSet::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_statistics_has_no_column_entries() {
        // `SourceStatistics::empty` is used whenever the source is unreachable. Its whole
        // contract is "the cost model must fall back to conservative defaults, never push
        // blindly" — which only holds if there really is no column data to look up.
        let stats = SourceStatistics::empty("orders");
        assert_eq!(stats.table_name, "orders");
        assert_eq!(stats.row_count_estimate, 0.0);
        assert_eq!(stats.table_size_bytes, 0);
        assert!(stats.columns.is_empty());
    }

    #[test]
    fn test_empty_statistics_distinct_tables_are_independent() {
        let a = SourceStatistics::empty("orders");
        let b = SourceStatistics::empty("customers");
        assert_ne!(a.table_name, b.table_name);
        assert!(a.columns.is_empty() && b.columns.is_empty());
    }
}
