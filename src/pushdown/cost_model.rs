//! Cost-based pushdown decision logic.
//! pushdown/cost_model.rs
//! Implements the decision tree from docs/pushdown.md §4.2:
//! 1. Index available → push (near-free, huge win)
//! 2. No index but selectivity < keep_threshold and cost < budget → push
//! 3. Otherwise, keep (execute in Arrow)

use super::{Fidelity, Predicate};
use crate::pushdown::stats::{ColumnStats, SourceStatistics};

pub use crate::pushdown::stats::IndexInfo;

/// The outcome of a cost-based pushdown decision.
#[derive(Debug, Clone, PartialEq)]
pub enum CostDecision {
    /// Push the predicate to the source.
    Push {
        reason: String,
        has_index: bool,
        selectivity: f64,
        cost_estimate: u64,
    },
    /// Keep the predicate in Arrow.
    Keep {
        reason: String,
        selectivity: f64,
    },
}

/// Parameters controlling cost-based decisions.
#[derive(Debug, Clone)]
pub struct CostParams {
    /// Maximum planner-estimated source cost units before falling back to Arrow.
    pub max_source_cost: u64,
    /// Selectivity threshold: below this, a predicate is a candidate for pushing.
    /// Ranges from 0.0 (no rows) to 1.0 (all rows).
    pub keep_threshold: f64,
}

impl Default for CostParams {
    fn default() -> Self {
        Self {
            max_source_cost: 50_000,
            keep_threshold: 0.30,
        }
    }
}

/// Make a cost-based pushdown decision for a predicate.
///
/// Decision logic (from docs/pushdown.md §4.2):
/// 1. If an index is available on the predicate's column(s), push (near-free, huge win).
/// 2. If no index but selectivity < keep_threshold and source_cost < max_source_cost, push.
/// 3. Otherwise, keep (execute in Arrow).
pub fn decide_push(
    predicate: &Predicate,
    fidelity: Fidelity,
    stats: &SourceStatistics,
    params: &CostParams,
    estimated_source_cost: Option<u64>,
    available_indexes: &[IndexInfo],
) -> CostDecision {
    // Step 1: Check if predicate references any indexed columns.
    let indexed_columns = extract_predicate_columns(predicate);
    let has_applicable_index = available_indexes.iter().any(|idx| {
        indexed_columns.iter().any(|col| idx.columns.contains(col))
    });

    if has_applicable_index {
        // Index is available: push (near-free, huge win).
        let index_name = available_indexes
            .iter()
            .find(|idx| indexed_columns.iter().any(|col| idx.columns.contains(col)))
            .map(|idx| idx.name.clone())
            .unwrap_or_else(|| "index".to_string());

        return CostDecision::Push {
            reason: format!("index available: {}", index_name),
            has_index: true,
            selectivity: 0.0, // Unknown but high confidence in index
            cost_estimate: 1, // Negligible
        };
    }

    // Step 2: Estimate selectivity from column statistics.
    let selectivity = estimate_selectivity_from_stats(predicate, stats);

    // Step 3: Check if selectivity meets threshold.
    if selectivity >= params.keep_threshold {
        return CostDecision::Keep {
            reason: format!(
                "selectivity too high: {:.2}% >= {:.2}%",
                selectivity * 100.0,
                params.keep_threshold * 100.0
            ),
            selectivity,
        };
    }

    // Step 4: Check cost budget.
    let cost = estimated_source_cost.unwrap_or_else(|| estimate_source_cost(predicate, stats));

    if cost > params.max_source_cost {
        return CostDecision::Keep {
            reason: format!(
                "cost exceeds budget: {} > {}",
                cost, params.max_source_cost
            ),
            selectivity,
        };
    }

    // All checks passed: push.
    CostDecision::Push {
        reason: format!(
            "low selectivity ({:.2}%) and cost ({}) within budget ({})",
            selectivity * 100.0,
            cost,
            params.max_source_cost
        ),
        has_index: false,
        selectivity,
        cost_estimate: cost,
    }
}

/// Extract column names referenced in a predicate.
fn extract_predicate_columns(predicate: &Predicate) -> Vec<String> {
    let mut columns = Vec::new();
    extract_columns_recursive(predicate, &mut columns);
    columns.sort();
    columns.dedup();
    columns
}

fn extract_columns_recursive(predicate: &Predicate, columns: &mut Vec<String>) {
    match predicate {
        Predicate::Column(name) => {
            if !columns.contains(name) {
                columns.push(name.clone());
            }
        }
        Predicate::Literal(_) => {} // Literals don't reference columns
        Predicate::Cmp { left, right, .. } => {
            extract_columns_recursive(left, columns);
            extract_columns_recursive(right, columns);
        }
        Predicate::And(l, r) | Predicate::Or(l, r) => {
            extract_columns_recursive(l, columns);
            extract_columns_recursive(r, columns);
        }
        Predicate::Not(p) | Predicate::IsNull(p) | Predicate::IsNotNull(p) => {
            extract_columns_recursive(p, columns);
        }
        Predicate::Cast { expr, .. } => extract_columns_recursive(expr, columns),
    }
}

/// Estimate selectivity (0.0 to 1.0) from column statistics.
/// Uses n_distinct, null_frac from pg_stats where available.
pub fn estimate_selectivity_from_stats(
    predicate: &Predicate,
    stats: &SourceStatistics,
) -> f64 {
    match predicate {
        Predicate::Column(_) => {
            // A column reference alone is not a predicate; conservative assumption.
            0.5
        }
        Predicate::Literal(_) => {
            // A literal alone is not a predicate.
            0.5
        }
        Predicate::Cmp { left, right, op } => {
            estimate_comparison_selectivity(left, right, op, stats)
        }
        Predicate::And(l, r) => {
            // AND: multiply selectivity (assumes independence).
            let l_sel = estimate_selectivity_from_stats(l, stats);
            let r_sel = estimate_selectivity_from_stats(r, stats);
            l_sel * r_sel
        }
        Predicate::Or(l, r) => {
            // OR: s_or = s1 + s2 - (s1 * s2)
            let l_sel = estimate_selectivity_from_stats(l, stats);
            let r_sel = estimate_selectivity_from_stats(r, stats);
            l_sel + r_sel - (l_sel * r_sel)
        }
        Predicate::Not(p) => {
            // NOT: 1 - selectivity of inner predicate.
            1.0 - estimate_selectivity_from_stats(p, stats)
        }
        Predicate::IsNull(p) => {
            // IS NULL: use null_frac from column stats.
            if let Predicate::Column(col_name) = p.as_ref() {
                stats
                    .columns
                    .get(col_name)
                    .map(|col: &ColumnStats| col.null_frac as f64)
                    .unwrap_or(0.01) // Conservative: assume 1% nulls if unknown
            } else {
                0.01
            }
        }
        Predicate::IsNotNull(p) => {
            // IS NOT NULL: 1 - null_frac.
            if let Predicate::Column(col_name) = p.as_ref() {
                1.0 - stats
                    .columns
                    .get(col_name)
                    .map(|col: &ColumnStats| col.null_frac as f64)
                    .unwrap_or(0.01)
            } else {
                0.99
            }
        }
        Predicate::Cast { expr, .. } => {
            // A cast preserves row counts; selectivity is the inner predicate's.
            estimate_selectivity_from_stats(expr, stats)
        }
    }
}

fn estimate_comparison_selectivity(
    left: &Predicate,
    right: &Predicate,
    op: &str,
    stats: &SourceStatistics,
) -> f64 {
    // Extract column name and literal value if this is a column-vs-literal comparison.
    let (col_name, _is_literal) = match (left, right) {
        (Predicate::Column(col), Predicate::Literal(_)) => (Some(col.as_str()), true),
        (Predicate::Literal(_), Predicate::Column(col)) => (Some(col.as_str()), true),
        _ => (None, false),
    };

    if let Some(col_name) = col_name {
        if let Some(col_stats) = stats.columns.get(col_name) {
            // Selectivity heuristics based on operator and column statistics.
            match op {
                "=" => {
                    // Equality: 1 / n_distinct
                    if col_stats.n_distinct > 0.0 {
                        1.0 / col_stats.n_distinct
                    } else {
                        0.01 // Conservative default
                    }
                }
                "<>" => {
                    // Not equal: 1 - (1 / n_distinct)
                    if col_stats.n_distinct > 0.0 {
                        1.0 - (1.0 / col_stats.n_distinct)
                    } else {
                        0.99
                    }
                }
                "<" | ">" | "<=" | ">=" => {
                    // Range queries: assume uniform distribution, 1/3 of rows on average.
                    0.33
                }
                _ => 0.5, // Unknown operator, conservative middle ground
            }
        } else {
            // No stats for this column; conservative assumption.
            0.5
        }
    } else {
        // Column-to-column comparison or other non-standard form.
        0.5
    }
}

/// Estimate source cost for a predicate.
/// Simple heuristic: full table scan cost if no index, reduced if indexed.
pub fn estimate_source_cost(predicate: &Predicate, stats: &SourceStatistics) -> u64 {
    // Cost heuristic: proportional to table size and selectivity.
    let selectivity = estimate_selectivity_from_stats(predicate, stats);
    let base_cost = stats.table_size_bytes / 1024; // Convert to KB
    let cost = (base_cost as f64 * selectivity) as u64;
    cost.max(1) // Minimum cost of 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_stats() -> SourceStatistics {
        let mut columns = HashMap::new();
        columns.insert(
            "id".to_string(),
            ColumnStats {
                column_name: "id".to_string(),
                n_distinct: 1000.0,
                null_frac: 0.0,
                avg_width: 8,
            },
        );
        columns.insert(
            "status".to_string(),
            ColumnStats {
                column_name: "status".to_string(),
                n_distinct: 5.0, // Few distinct values
                null_frac: 0.0,
                avg_width: 10,
            },
        );

        SourceStatistics {
            table_name: "test_table".to_string(),
            row_count_estimate: 1000.0,
            table_size_bytes: 10_000_000, // 10MB
            columns,
            fetched_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_cost_params_defaults() {
        let params = CostParams::default();
        assert_eq!(params.max_source_cost, 50_000);
        assert_eq!(params.keep_threshold, 0.30);
    }

    #[test]
    fn test_extract_predicate_columns() {
        use super::super::Predicate;

        let pred = Predicate::Column("id".to_string());
        let columns = extract_predicate_columns(&pred);
        assert_eq!(columns, vec!["id"]);
    }

    #[test]
    fn test_cost_decision_push() {
        let decision = CostDecision::Push {
            reason: "index available".to_string(),
            has_index: true,
            selectivity: 0.001,
            cost_estimate: 10,
        };
        assert!(matches!(decision, CostDecision::Push { .. }));
    }

    #[test]
    fn test_cost_decision_keep() {
        let decision = CostDecision::Keep {
            reason: "selectivity too high".to_string(),
            selectivity: 0.9,
        };
        assert!(matches!(decision, CostDecision::Keep { .. }));
    }

    #[test]
    fn test_estimate_selectivity_equality() {
        let stats = create_test_stats();
        let pred = Predicate::Cmp {
            left: Box::new(Predicate::Column("id".to_string())),
            op: "=".to_string(),
            right: Box::new(Predicate::Literal(super::super::Literal::Int(42))),
        };

        let sel = estimate_selectivity_from_stats(&pred, &stats);
        assert!(sel > 0.0 && sel < 0.1); // 1/1000 ≈ 0.001
    }

    #[test]
    fn test_estimate_selectivity_and() {
        let stats = create_test_stats();
        let pred = Predicate::And(
            Box::new(Predicate::Cmp {
                left: Box::new(Predicate::Column("id".to_string())),
                op: "=".to_string(),
                right: Box::new(Predicate::Literal(super::super::Literal::Int(42))),
            }),
            Box::new(Predicate::Cmp {
                left: Box::new(Predicate::Column("status".to_string())),
                op: "=".to_string(),
                right: Box::new(Predicate::Literal(super::super::Literal::Text("PAID".to_string()))),
            }),
        );

        let sel = estimate_selectivity_from_stats(&pred, &stats);
        // (1/1000) * (1/5) = 0.0002
        assert!(sel < 0.001);
    }
}
