//! Backend-agnostic plan-estimate types consumed by the cost model.
//! pushdown/explain.rs
//! A connector that can ask its source for a plan estimate (Postgres: `EXPLAIN (FORMAT JSON)`,
//! see `connector::postgres::explain`) reports it in this shape. Every field the source may omit
//! is an `Option`: a missing cost is *unknown*, never zero, so the cost model falls back to its
//! own estimate instead of treating the predicate as free.

use chrono::{DateTime, Utc};

/// How the source planned to reach the rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessMethod {
    SequentialScan,
    IndexScan,
    IndexOnlyScan,
    BitmapHeapScan,
    Unknown,
}

impl AccessMethod {
    /// Whether this access method uses an index.
    ///
    /// # Examples
    /// ```
    /// use el_ballista::pushdown::explain::AccessMethod;
    /// assert!(AccessMethod::IndexScan.is_indexed());
    /// assert!(!AccessMethod::SequentialScan.is_indexed());
    /// ```
    pub fn is_indexed(&self) -> bool {
        matches!(
            self,
            AccessMethod::IndexScan | AccessMethod::IndexOnlyScan | AccessMethod::BitmapHeapScan
        )
    }
}

/// A source plan estimate for one predicate.
#[derive(Debug, Clone)]
pub struct ExplainEstimate {
    pub access_method: AccessMethod,
    /// Planner total cost; `None` when the source did not report one.
    pub total_cost: Option<f64>,
    /// Planner row estimate; `None` when the source did not report one.
    pub plan_rows: Option<f64>,
    pub index_name: Option<String>,
    pub estimated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_method_is_indexed() {
        assert!(!AccessMethod::SequentialScan.is_indexed());
        assert!(AccessMethod::IndexScan.is_indexed());
        assert!(AccessMethod::IndexOnlyScan.is_indexed());
        assert!(AccessMethod::BitmapHeapScan.is_indexed());
        assert!(!AccessMethod::Unknown.is_indexed());
    }
}
