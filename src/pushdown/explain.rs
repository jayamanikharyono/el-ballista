//! EXPLAIN-based query cost estimation.
//! pushdown/explain.rs
//! Runs `EXPLAIN (FORMAT JSON)` without executing queries to gather cost and access method
//! information. Cached to avoid repeated estimation for the same predicate patterns.

use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::connector::errors::ExtractorError;

/// The plan node type that Postgres used to execute the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessMethod {
    SequentialScan,
    IndexScan,
    IndexOnlyScan,
    BitmapHeapScan,
    Unknown,
}

impl AccessMethod {
    /// Convert from Postgres EXPLAIN output node type string.
    pub fn from_node_type(node_type: &str) -> Self {
        match node_type {
            "Seq Scan" => AccessMethod::SequentialScan,
            "Index Scan" => AccessMethod::IndexScan,
            "Index Only Scan" => AccessMethod::IndexOnlyScan,
            "Bitmap Heap Scan" => AccessMethod::BitmapHeapScan,
            _ => AccessMethod::Unknown,
        }
    }

    /// Whether this access method uses an index.
    pub fn is_indexed(&self) -> bool {
        matches!(
            self,
            AccessMethod::IndexScan | AccessMethod::IndexOnlyScan | AccessMethod::BitmapHeapScan
        )
    }
}

/// Cost estimate from EXPLAIN output.
#[derive(Debug, Clone)]
pub struct ExplainEstimate {
    pub access_method: AccessMethod,
    pub total_cost: f64,
    pub plan_rows: f64,
    pub index_name: Option<String>,
    pub estimated_at: DateTime<Utc>,
}

/// Caches EXPLAIN estimates for query patterns to avoid repeated estimation.
#[derive(Debug)]
pub struct ExplainEstimator {
    pool: Arc<PgPool>,
    cache: Arc<RwLock<HashMap<String, ExplainEstimate>>>,
    ttl_secs: u64,
}

impl ExplainEstimator {
    pub fn new(pool: Arc<PgPool>, ttl_secs: u64) -> Self {
        Self {
            pool,
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl_secs,
        }
    }

    /// Estimate the cost of a query without executing it.
    /// Returns cached estimate if available and fresh, otherwise runs EXPLAIN and caches.
    /// The cache is keyed by table *and* predicate: different predicates plan differently.
    pub async fn estimate_cost(
        &self,
        table_name: &str,
        schema_name: &str,
        predicate: &str,
    ) -> Result<ExplainEstimate, ExtractorError> {
        let cache_key = format!("{schema_name}.{table_name}::{predicate}");

        // Check cache.
        {
            let cache = self.cache.read().await;
            if let Some(estimate) = cache.get(&cache_key) {
                let age = Utc::now().signed_duration_since(estimate.estimated_at);
                if age < Duration::seconds(self.ttl_secs as i64) {
                    return Ok(estimate.clone());
                }
            }
        }

        // Run EXPLAIN to get fresh estimate.
        let estimate = self.run_explain(schema_name, table_name, predicate).await?;

        // Cache the result.
        {
            let mut cache = self.cache.write().await;
            cache.insert(cache_key, estimate.clone());
        }

        Ok(estimate)
    }

    /// Best-effort synchronous read of a cached estimate. Used on the sync planning path
    /// (`supports_filters_pushdown`), which cannot run EXPLAIN itself. Returns `None` on a
    /// cold cache — the cost model then falls back to statistics alone. This keeps the sync
    /// and async decision paths consistent: both decide over the same cached inputs, so
    /// `scan()` can never contradict what `supports_filters_pushdown` promised.
    pub fn cached_estimate(
        &self,
        table_name: &str,
        schema_name: &str,
        predicate: &str,
    ) -> Option<ExplainEstimate> {
        // Synchronous read of an async lock: try_read never blocks; a contended lock simply
        // looks like a cold cache, which is always a safe answer.
        let cache = self.cache.try_read().ok()?;
        let estimate = cache
            .get(&format!("{schema_name}.{table_name}::{predicate}"))?
            .clone();
        let age = Utc::now().signed_duration_since(estimate.estimated_at);
        if age < Duration::seconds(self.ttl_secs as i64) {
            Some(estimate)
        } else {
            None
        }
    }

    async fn run_explain(
        &self,
        schema_name: &str,
        table_name: &str,
        predicate: &str,
    ) -> Result<ExplainEstimate, ExtractorError> {
        // `predicate` is rendered by `Predicate::render_inline` (literals inlined, text
        // quoted) — never user SQL. Identifiers are quoted here. EXPLAIN plans without
        // executing, so even a surprising predicate costs a plan, not a scan.
        let sql = format!(
            "EXPLAIN (FORMAT JSON) SELECT * FROM \"{}\" . \"{}\" WHERE {}",
            schema_name.replace('"', "\"\""),
            table_name.replace('"', "\"\""),
            predicate,
        );
        log::debug!("EXPLAIN estimation: {sql}");

        // `EXPLAIN (FORMAT JSON)` returns its single output column as SQL type `json`
        // (not `text`), so sqlx must be told to decode it as JSON -- a bare `String`
        // target fails with "SQL type JSON is not compatible with Rust type String".
        // `sqlx::types::Json<Value>` decodes the `json` column directly; re-stringify
        // once here so `parse_explain_json` (already unit-tested against raw strings)
        // stays the single parsing implementation for both this path and those tests.
        let row: (sqlx::types::Json<serde_json::Value>,) =
            sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
                .fetch_one(self.pool.as_ref())
                .await
                .map_err(|e| {
                    ExtractorError::Statistics(format!(
                        "EXPLAIN failed for {schema_name}.{table_name}: {e}"
                    ))
                })?;

        parse_explain_json(&row.0.0.to_string())
    }
}

/// Parse EXPLAIN (FORMAT JSON) output to extract cost and access method.
fn parse_explain_json(json_str: &str) -> Result<ExplainEstimate, ExtractorError> {
    use serde_json::Value;

    let parsed: Value = serde_json::from_str(json_str)
        .map_err(|e| ExtractorError::Statistics(format!("cannot parse EXPLAIN JSON: {}", e)))?;

    // EXPLAIN output is an array of plans; we care about the first.
    let plan = parsed
        .get(0)
        .and_then(|p| p.get("Plan"))
        .ok_or_else(|| ExtractorError::Statistics("malformed EXPLAIN output".to_string()))?;

    let node_type = plan
        .get("Node Type")
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown");

    let access_method = AccessMethod::from_node_type(node_type);

    let total_cost = plan
        .get("Total Cost")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let plan_rows = plan
        .get("Plans Rows")
        .or_else(|| plan.get("Plan Rows"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let index_name = plan
        .get("Index Name")
        .and_then(|v| v.as_str())
        .map(String::from);

    Ok(ExplainEstimate {
        access_method,
        total_cost,
        plan_rows,
        index_name,
        estimated_at: Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_method_from_node_type() {
        assert_eq!(
            AccessMethod::from_node_type("Seq Scan"),
            AccessMethod::SequentialScan
        );
        assert_eq!(
            AccessMethod::from_node_type("Index Scan"),
            AccessMethod::IndexScan
        );
        assert_eq!(
            AccessMethod::from_node_type("Index Only Scan"),
            AccessMethod::IndexOnlyScan
        );
        assert_eq!(
            AccessMethod::from_node_type("Bitmap Heap Scan"),
            AccessMethod::BitmapHeapScan
        );
        assert_eq!(
            AccessMethod::from_node_type("Unknown"),
            AccessMethod::Unknown
        );
    }

    #[test]
    fn test_access_method_is_indexed() {
        assert!(!AccessMethod::SequentialScan.is_indexed());
        assert!(AccessMethod::IndexScan.is_indexed());
        assert!(AccessMethod::IndexOnlyScan.is_indexed());
        assert!(AccessMethod::BitmapHeapScan.is_indexed());
        assert!(!AccessMethod::Unknown.is_indexed());
    }

    #[test]
    fn test_parse_explain_json_rejects_invalid_json() {
        // Devil's advocate: a truncated/corrupt EXPLAIN response (connection hiccup,
        // Postgres version producing an unexpected format) must surface as an error, not
        // panic or silently return a zeroed/default estimate that looks like real data.
        let err = parse_explain_json("not json at all").unwrap_err();
        assert!(matches!(err, ExtractorError::Statistics(_)));
    }

    #[test]
    fn test_parse_explain_json_rejects_empty_plan_array() {
        // Well-formed JSON, but no plan at index 0: must error rather than fabricate a
        // default estimate that downstream cost logic would treat as "cheap, no index".
        let err = parse_explain_json("[]").unwrap_err();
        assert!(matches!(err, ExtractorError::Statistics(_)));
    }

    #[test]
    fn test_parse_explain_json_rejects_missing_plan_key() {
        let err = parse_explain_json(r#"[{"NotPlan": {}}]"#).unwrap_err();
        assert!(matches!(err, ExtractorError::Statistics(_)));
    }

    #[test]
    fn test_parse_explain_json_missing_fields_default_conservatively() {
        // A Plan node with none of the fields we look for (rather than a hard parse
        // failure) must default to Unknown/0.0/no-index — never crash on a missing key,
        // and never silently invent a positive cost or an index name.
        let estimate = parse_explain_json(r#"[{"Plan": {}}]"#).unwrap();
        assert_eq!(estimate.access_method, AccessMethod::Unknown);
        assert_eq!(estimate.total_cost, 0.0);
        assert_eq!(estimate.plan_rows, 0.0);
        assert!(estimate.index_name.is_none());
    }

    #[test]
    fn test_parse_explain_json_accepts_plans_rows_key_variant() {
        // Some EXPLAIN shapes use "Plans Rows" instead of "Plan Rows" (checked first);
        // both must be honored, or a whole class of plans silently reports plan_rows: 0.
        let json = r#"[{"Plan": {"Node Type": "Seq Scan", "Plans Rows": 42}}]"#;
        let estimate = parse_explain_json(json).unwrap();
        assert_eq!(estimate.plan_rows, 42.0);
    }

    #[test]
    fn test_parse_explain_json_with_index() {
        let json = r#"[
            {
                "Plan": {
                    "Node Type": "Index Scan",
                    "Index Name": "idx_orders_id",
                    "Total Cost": 50.5,
                    "Plan Rows": 100
                }
            }
        ]"#;

        let estimate = parse_explain_json(json).unwrap();
        assert_eq!(estimate.access_method, AccessMethod::IndexScan);
        assert_eq!(estimate.total_cost, 50.5);
        assert_eq!(estimate.plan_rows, 100.0);
        assert_eq!(estimate.index_name, Some("idx_orders_id".to_string()));
    }

    #[test]
    fn test_parse_explain_json_seq_scan() {
        let json = r#"[
            {
                "Plan": {
                    "Node Type": "Seq Scan",
                    "Total Cost": 1000.0,
                    "Plan Rows": 5000
                }
            }
        ]"#;

        let estimate = parse_explain_json(json).unwrap();
        assert_eq!(estimate.access_method, AccessMethod::SequentialScan);
        assert_eq!(estimate.total_cost, 1000.0);
        assert_eq!(estimate.plan_rows, 5000.0);
        assert!(estimate.index_name.is_none());
    }
}
