//! Postgres EXPLAIN-based cost estimation. Runs `EXPLAIN (FORMAT JSON)` without executing
//! queries and caches the result. The backend-agnostic estimate types ([`AccessMethod`],
//! [`ExplainEstimate`]) live in [`crate::pushdown::explain`]; this is the Postgres executor for
//! them, plus the Postgres-specific plan parsing.

use chrono::{Duration, Utc};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::connector::errors::ExtractorError;
use crate::pushdown::explain::{AccessMethod, ExplainEstimate};

/// Map a Postgres EXPLAIN `Node Type` to an [`AccessMethod`].
///
/// # Examples
/// ```
/// use el_ballista::connector::postgres::explain::access_method_from_node_type;
/// use el_ballista::pushdown::explain::AccessMethod;
/// assert_eq!(access_method_from_node_type("Index Scan"), AccessMethod::IndexScan);
/// ```
pub fn access_method_from_node_type(node_type: &str) -> AccessMethod {
    match node_type {
        "Seq Scan" => AccessMethod::SequentialScan,
        "Index Scan" => AccessMethod::IndexScan,
        "Index Only Scan" => AccessMethod::IndexOnlyScan,
        "Bitmap Heap Scan" => AccessMethod::BitmapHeapScan,
        _ => AccessMethod::Unknown,
    }
}

/// Caches EXPLAIN estimates for query patterns to avoid repeated estimation.
#[derive(Debug)]
pub struct ExplainEstimator {
    pool: Arc<PgPool>,
    cache: Arc<RwLock<HashMap<String, ExplainEstimate>>>,
    ttl_secs: u64,
}

impl ExplainEstimator {
    /// An estimator over `pool` whose cached estimates expire after `ttl_secs`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo(ex: &el_ballista::connector::postgres::PostgresExtractor)
    /// # -> Result<(), el_ballista::connector::errors::ExtractorError> {
    /// use std::sync::Arc;
    /// use el_ballista::connector::postgres::explain::ExplainEstimator;
    ///
    /// let estimator = ExplainEstimator::new(Arc::new(ex.pool().clone()), 300);
    /// let estimate = estimator.estimate_cost("orders", "public", "status = 'PAID'").await?;
    /// println!("{:?}, cost {:?}", estimate.access_method, estimate.total_cost);
    /// # Ok(()) }
    /// ```
    pub fn new(pool: Arc<PgPool>, ttl_secs: u64) -> Self {
        Self {
            pool,
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl_secs,
        }
    }

    fn ttl(&self) -> Duration {
        Duration::seconds(i64::try_from(self.ttl_secs).unwrap_or(i64::MAX))
    }

    /// Estimate the cost of a query without executing it.
    /// Returns cached estimate if available and fresh, otherwise runs EXPLAIN and caches.
    /// The cache is keyed by table *and* predicate: different predicates plan differently.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo(ex: &el_ballista::connector::postgres::PostgresExtractor)
    /// # -> Result<(), el_ballista::connector::errors::ExtractorError> {
    /// use std::sync::Arc;
    /// use el_ballista::connector::postgres::explain::ExplainEstimator;
    ///
    /// let estimator = ExplainEstimator::new(Arc::new(ex.pool().clone()), 300);
    /// let predicate = "created_at >= '2026-01-01'";
    /// let estimate = estimator.estimate_cost("orders", "public", predicate).await?;
    /// println!("{:?} rows, {:?}", estimate.plan_rows, estimate.access_method);
    /// # Ok(()) }
    /// ```
    pub async fn estimate_cost(
        &self,
        table_name: &str,
        schema_name: &str,
        predicate: &str,
    ) -> Result<ExplainEstimate, ExtractorError> {
        let cache_key = format!("{schema_name}.{table_name}::{predicate}");

        {
            let cache = self.cache.read().await;
            if let Some(estimate) = cache.get(&cache_key) {
                let age = Utc::now().signed_duration_since(estimate.estimated_at);
                if age < self.ttl() {
                    return Ok(estimate.clone());
                }
            }
        }

        let estimate = self.run_explain(schema_name, table_name, predicate).await?;

        {
            let mut cache = self.cache.write().await;
            cache.insert(cache_key, estimate.clone());
        }

        Ok(estimate)
    }

    /// Best-effort synchronous read of a cached estimate. Used on the sync planning path
    /// (`supports_filters_pushdown`), which cannot run EXPLAIN itself. Returns `None` on a
    /// cold cache (or a contended lock) — the cost model then falls back to statistics alone.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo(ex: &el_ballista::connector::postgres::PostgresExtractor)
    /// # -> Result<(), el_ballista::connector::errors::ExtractorError> {
    /// use std::sync::Arc;
    /// use el_ballista::connector::postgres::explain::ExplainEstimator;
    ///
    /// let estimator = ExplainEstimator::new(Arc::new(ex.pool().clone()), 300);
    /// let pred = "id = 7";
    /// assert!(estimator.cached_estimate("orders", "public", pred).is_none()); // cold cache
    /// estimator.estimate_cost("orders", "public", pred).await?;
    /// assert!(estimator.cached_estimate("orders", "public", pred).is_some());
    /// # Ok(()) }
    /// ```
    pub fn cached_estimate(
        &self,
        table_name: &str,
        schema_name: &str,
        predicate: &str,
    ) -> Option<ExplainEstimate> {
        let cache = self.cache.try_read().ok()?;
        let estimate = cache
            .get(&format!("{schema_name}.{table_name}::{predicate}"))?
            .clone();
        let age = Utc::now().signed_duration_since(estimate.estimated_at);
        (age < self.ttl()).then_some(estimate)
    }

    async fn run_explain(
        &self,
        schema_name: &str,
        table_name: &str,
        predicate: &str,
    ) -> Result<ExplainEstimate, ExtractorError> {
        // `predicate` is rendered by `PredicateInlineSql::render_inline` (literals inlined,
        // text quoted, backslashes escaped) — never user SQL. Identifiers are quoted here.
        // EXPLAIN plans without executing, so even a surprising predicate costs a plan, not a
        // scan.
        let sql = format!(
            "EXPLAIN (FORMAT JSON) SELECT * FROM \"{}\" . \"{}\" WHERE {}",
            schema_name.replace('"', "\"\""),
            table_name.replace('"', "\"\""),
            predicate,
        );
        log::debug!("EXPLAIN estimation: {sql}");

        // `EXPLAIN (FORMAT JSON)` returns its single output column as SQL type `json`, so
        // decode as JSON and re-stringify once for the (unit-tested) parser.
        let row: (sqlx::types::Json<serde_json::Value>,) =
            sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
                .fetch_one(self.pool.as_ref())
                .await
                .map_err(|e| ExtractorError::SourceQuery {
                    context: format!("EXPLAIN failed for {schema_name}.{table_name}"),
                    source: e,
                })?;

        parse_explain_json(&row.0.0.to_string())
    }
}

/// Parse EXPLAIN (FORMAT JSON) output to extract cost and access method. Fields Postgres did
/// not report stay `None` — a missing `Total Cost` is unknown, never "free".
fn parse_explain_json(json_str: &str) -> Result<ExplainEstimate, ExtractorError> {
    use serde_json::Value;

    let parsed: Value = serde_json::from_str(json_str).map_err(|e| ExtractorError::Parse {
        context: "cannot parse EXPLAIN JSON".to_string(),
        source: e,
    })?;

    // EXPLAIN output is an array of plans; we care about the first.
    let plan = parsed
        .get(0)
        .and_then(|p| p.get("Plan"))
        .ok_or_else(|| ExtractorError::Statistics("malformed EXPLAIN output".to_string()))?;

    let access_method = access_method_from_node_type(
        plan.get("Node Type")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown"),
    );

    Ok(ExplainEstimate {
        access_method,
        total_cost: plan.get("Total Cost").and_then(Value::as_f64),
        plan_rows: plan.get("Plan Rows").and_then(Value::as_f64),
        index_name: plan
            .get("Index Name")
            .and_then(|v| v.as_str())
            .map(String::from),
        estimated_at: Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_method_from_node_type() {
        assert_eq!(
            access_method_from_node_type("Seq Scan"),
            AccessMethod::SequentialScan
        );
        assert_eq!(
            access_method_from_node_type("Index Scan"),
            AccessMethod::IndexScan
        );
        assert_eq!(
            access_method_from_node_type("Index Only Scan"),
            AccessMethod::IndexOnlyScan
        );
        assert_eq!(
            access_method_from_node_type("Bitmap Heap Scan"),
            AccessMethod::BitmapHeapScan
        );
        assert_eq!(
            access_method_from_node_type("Unknown"),
            AccessMethod::Unknown
        );
    }

    #[test]
    fn test_parse_explain_json_rejects_invalid_json() {
        // A truncated/corrupt EXPLAIN response must surface as an error, not a zeroed estimate.
        let err = parse_explain_json("not json at all").unwrap_err();
        assert!(matches!(err, ExtractorError::Parse { .. }));
        assert!(
            std::error::Error::source(&err).is_some(),
            "parser cause kept"
        );
    }

    #[test]
    fn test_parse_explain_json_rejects_empty_plan_array() {
        let err = parse_explain_json("[]").unwrap_err();
        assert!(matches!(err, ExtractorError::Statistics(_)));
    }

    #[test]
    fn test_parse_explain_json_rejects_missing_plan_key() {
        let err = parse_explain_json(r#"[{"NotPlan": {}}]"#).unwrap_err();
        assert!(matches!(err, ExtractorError::Statistics(_)));
    }

    #[test]
    fn test_parse_explain_json_missing_fields_are_unknown_not_zero() {
        // A Plan node without the fields we look for: Unknown access, and cost/rows `None`
        // (never 0.0, which the cost model would read as "free").
        let estimate = parse_explain_json(r#"[{"Plan": {}}]"#).unwrap();
        assert_eq!(estimate.access_method, AccessMethod::Unknown);
        assert_eq!(estimate.total_cost, None);
        assert_eq!(estimate.plan_rows, None);
        assert!(estimate.index_name.is_none());
    }

    #[test]
    fn test_parse_explain_json_reads_plan_rows_key() {
        // Postgres spells it "Plan Rows"; the old "Plans Rows" variant never existed.
        let json = r#"[{"Plan": {"Node Type": "Seq Scan", "Plan Rows": 42, "Plans Rows": 7}}]"#;
        let estimate = parse_explain_json(json).unwrap();
        assert_eq!(estimate.plan_rows, Some(42.0));
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
        assert_eq!(estimate.total_cost, Some(50.5));
        assert_eq!(estimate.plan_rows, Some(100.0));
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
        assert_eq!(estimate.total_cost, Some(1000.0));
        assert_eq!(estimate.plan_rows, Some(5000.0));
        assert!(estimate.index_name.is_none());
    }
}
