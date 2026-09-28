//! Cost-based pushdown decision logic.
//! pushdown/cost_model.rs
//! Implements the decision tree from docs/pushdown.md §4.2:
//! 1. An index serves the predicate (or EXPLAIN says it would) → push (near-free, huge win)
//! 2. No index but selectivity < keep_threshold and cost < budget → push
//! 3. Otherwise, keep (execute in Arrow)
//!
//! Only reached for predicates that already translated as `Exact` or `Inexact`: the cost model
//! decides *whether it is worth it*, never *whether it is correct*.

use crate::pushdown::explain::ExplainEstimate;
use crate::pushdown::stats::{ColumnStats, SourceStatistics, ordinal};
use crate::pushdown::{CmpOp, Collation, ColumnKind, ColumnKinds, Literal, Predicate};

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
    Keep { reason: String, selectivity: f64 },
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

/// Everything the cost model (and the `strict` policy) reads for one decision.
pub struct CostInputs<'a> {
    pub stats: &'a SourceStatistics,
    pub params: &'a CostParams,
    pub indexes: &'a [IndexInfo],
    /// Best-effort EXPLAIN estimate for this exact predicate, when the estimator cache has one.
    /// Sharpens the cost and index answers; never required.
    pub explain: Option<ExplainEstimate>,
    /// Column kinds, for index usability (a binary-collated text comparison can only use an
    /// index whose column collation is already byte-wise) and the `strict` primitive gate.
    pub column_kinds: &'a ColumnKinds,
    /// The other translated filters of the same scan (they are ANDed with this one). A range
    /// comparison is estimated together with the sibling ranges on the same column, so the
    /// two halves of a window like `updated_at >= a` / `updated_at < b` are each judged by
    /// the window's selectivity, not by one open-ended side. Empty = judge alone.
    pub siblings: &'a [Predicate],
}

impl CostInputs<'_> {
    /// The EXPLAIN total cost as integer cost units, when EXPLAIN reported one.
    pub(crate) fn explain_cost(&self) -> Option<u64> {
        self.explain
            .as_ref()
            .and_then(|est| est.total_cost)
            .map(|cost| cost.max(0.0) as u64)
    }
}

/// Make a cost-based pushdown decision for a predicate.
///
/// Decision logic (from docs/pushdown.md §4.2):
/// 1. If EXPLAIN chose an index access path, or a catalog index can serve the predicate
///    (see [`usable_index`]), push.
/// 2. If no index but selectivity < keep_threshold and source_cost < max_source_cost, push.
/// 3. Otherwise, keep (execute in Arrow).
pub(crate) fn decide_push(predicate: &Predicate, inputs: &CostInputs<'_>) -> CostDecision {
    let explain_index = inputs
        .explain
        .as_ref()
        .filter(|est| est.access_method.is_indexed());
    let index_name = match explain_index {
        Some(est) => Some(format!(
            "EXPLAIN index path{}",
            est.index_name
                .as_deref()
                .map(|n| format!(" ({n})"))
                .unwrap_or_default()
        )),
        None => usable_index(predicate, inputs.indexes, inputs.column_kinds)
            .map(|idx| format!("index available: {}", idx.name)),
    };

    if let Some(reason) = index_name {
        return CostDecision::Push {
            reason,
            has_index: true,
            selectivity: 0.0, // Unknown but high confidence in index
            cost_estimate: 1, // Negligible
        };
    }

    let estimate = estimate_selectivity(predicate, inputs.stats, inputs.siblings);
    let selectivity = estimate.value;
    let basis = estimate.describe_basis();

    if selectivity >= inputs.params.keep_threshold {
        return CostDecision::Keep {
            reason: format!(
                "selectivity too high: {:.2}%{basis} >= {:.2}%",
                selectivity * 100.0,
                inputs.params.keep_threshold * 100.0
            ),
            selectivity,
        };
    }

    // A missing EXPLAIN cost is unknown, not free: fall back to the statistics heuristic.
    let cost = decision_cost(&estimate, inputs);

    if cost > inputs.params.max_source_cost {
        return CostDecision::Keep {
            reason: format!(
                "cost exceeds budget: {} > {}",
                cost, inputs.params.max_source_cost
            ),
            selectivity,
        };
    }

    CostDecision::Push {
        reason: format!(
            "low selectivity ({:.2}%{basis}) and cost ({}) within budget ({})",
            selectivity * 100.0,
            cost,
            inputs.params.max_source_cost
        ),
        has_index: false,
        selectivity,
        cost_estimate: cost,
    }
}

/// An index that can serve `predicate` as a whole, or `None`.
///
/// - A comparison against a literal (not `<>`) is served by a non-partial, non-expression index
///   whose **leading** key is the compared column (btree for any operator, hash for `=`). A
///   binary-collated text operand is only served when the column's own collation is byte-wise
///   ([`ColumnKind::Text`] `bytewise_collation`), because a `COLLATE` mismatch disables the
///   index; a cast operand (enum label, uuid text) never is.
/// - `IS NULL` on a column: a btree on it as leading key.
/// - `AND`: either side. `OR`: every branch (otherwise the source must scan anyway).
/// - Everything else (`NOT`, `IS NOT NULL`, column-to-column): no.
pub(crate) fn usable_index<'i>(
    predicate: &Predicate,
    indexes: &'i [IndexInfo],
    kinds: &ColumnKinds,
) -> Option<&'i IndexInfo> {
    match predicate {
        Predicate::Cmp { left, op, right } => {
            if *op == CmpOp::NotEq {
                return None;
            }
            let column = match (left.as_ref(), right.as_ref()) {
                (side, Predicate::Literal(_)) | (Predicate::Literal(_), side) => {
                    indexable_operand(side, kinds)?
                }
                _ => return None,
            };
            leading_index(column, *op == CmpOp::Eq, indexes)
        }
        Predicate::IsNull(inner) => match inner.as_ref() {
            Predicate::Column(column) => leading_index(column, false, indexes),
            _ => None,
        },
        Predicate::And(l, r) => {
            usable_index(l, indexes, kinds).or_else(|| usable_index(r, indexes, kinds))
        }
        Predicate::Or(l, r) => {
            let left = usable_index(l, indexes, kinds)?;
            usable_index(r, indexes, kinds)?;
            Some(left)
        }
        _ => None,
    }
}

/// The column a comparison operand can be looked up by in a plain index, if any.
fn indexable_operand<'p>(operand: &'p Predicate, kinds: &ColumnKinds) -> Option<&'p str> {
    match operand {
        Predicate::Column(name) => Some(name.as_str()),
        Predicate::Collate {
            expr,
            collation: Collation::Binary,
        } => match expr.as_ref() {
            Predicate::Column(name)
                if kinds.get(name)
                    == Some(&ColumnKind::Text {
                        bytewise_collation: true,
                    }) =>
            {
                Some(name.as_str())
            }
            _ => None,
        },
        _ => None,
    }
}

fn leading_index<'i>(
    column: &str,
    equality: bool,
    indexes: &'i [IndexInfo],
) -> Option<&'i IndexInfo> {
    indexes.iter().find(|idx| {
        !idx.is_partial
            && !idx.has_expressions
            && idx.columns.first().map(String::as_str) == Some(column)
            && (idx.index_type == "btree" || (equality && idx.index_type == "hash"))
    })
}

/// Whether `column` is the leading key of some plain (non-partial, non-expression) btree/hash
/// index. Used by the `strict` policy's "every column indexed" gate.
pub(crate) fn column_has_plain_index(column: &str, indexes: &[IndexInfo]) -> bool {
    leading_index(column, true, indexes).is_some()
}

/// Estimate selectivity (0.0 to 1.0) from column statistics.
/// Uses n_distinct, null_frac where available.
pub(crate) fn estimate_selectivity_from_stats(
    predicate: &Predicate,
    stats: &SourceStatistics,
) -> f64 {
    let selectivity = match predicate {
        // A bare column/literal/cast is not a predicate on its own: conservative middle.
        Predicate::Column(_)
        | Predicate::Literal(_)
        | Predicate::Cast { .. }
        | Predicate::Collate { .. } => 0.5,
        Predicate::Cmp { left, right, op } => {
            estimate_comparison_selectivity(left, right, *op, stats)
        }
        Predicate::And(l, r) => {
            // AND: multiply selectivity (assumes independence).
            estimate_selectivity_from_stats(l, stats) * estimate_selectivity_from_stats(r, stats)
        }
        Predicate::Or(l, r) => {
            let l_sel = estimate_selectivity_from_stats(l, stats);
            let r_sel = estimate_selectivity_from_stats(r, stats);
            l_sel + r_sel - (l_sel * r_sel)
        }
        Predicate::Not(p) => 1.0 - estimate_selectivity_from_stats(p, stats),
        Predicate::IsNull(p) => match p.as_ref() {
            Predicate::Column(col_name) => stats
                .columns
                .get(col_name)
                .map(|col: &ColumnStats| f64::from(col.null_frac))
                .unwrap_or(0.01), // Conservative: assume 1% nulls if unknown
            _ => 0.01,
        },
        Predicate::IsNotNull(p) => match p.as_ref() {
            Predicate::Column(col_name) => {
                1.0 - stats
                    .columns
                    .get(col_name)
                    .map(|col: &ColumnStats| f64::from(col.null_frac))
                    .unwrap_or(0.01)
            }
            _ => 0.99,
        },
    };
    selectivity.clamp(0.0, 1.0)
}

/// The column behind a comparison operand, looking through cast/collate wrappers.
fn operand_column(operand: &Predicate) -> Option<&str> {
    match operand {
        Predicate::Column(name) => Some(name.as_str()),
        Predicate::Cast { expr, .. } | Predicate::Collate { expr, .. } => operand_column(expr),
        _ => None,
    }
}

/// `1 / n_distinct`, clamped into `[0, 1]`; `None` when `n_distinct` is unknown (`<= 0`).
fn equality_selectivity(col_stats: &ColumnStats) -> Option<f64> {
    (col_stats.n_distinct > 0.0).then(|| (1.0 / col_stats.n_distinct).clamp(0.0, 1.0))
}

fn estimate_comparison_selectivity(
    left: &Predicate,
    right: &Predicate,
    op: CmpOp,
    stats: &SourceStatistics,
) -> f64 {
    let col_name = match (left, right) {
        (side, Predicate::Literal(_)) | (Predicate::Literal(_), side) => operand_column(side),
        _ => None,
    };
    let Some(col_stats) = col_name.and_then(|name| stats.columns.get(name)) else {
        // Column-to-column, or no stats for this column: conservative middle ground.
        return 0.5;
    };
    match op {
        CmpOp::Eq => equality_selectivity(col_stats).unwrap_or(0.01),
        CmpOp::NotEq => equality_selectivity(col_stats).map_or(0.99, |s| 1.0 - s),
        // Range queries: assume uniform distribution, 1/3 of rows on average.
        CmpOp::Lt | CmpOp::LtEq | CmpOp::Gt | CmpOp::GtEq => 0.33,
    }
}

/// Estimate source cost from a selectivity. Simple heuristic: table size in KB times the
/// fraction of rows returned, at least 1.
pub(crate) fn source_cost_for(selectivity: f64, stats: &SourceStatistics) -> u64 {
    let base_cost = stats.table_size_bytes / 1024; // Convert to KB
    let cost = (base_cost as f64 * selectivity) as u64;
    cost.max(1) // Minimum cost of 1
}

/// Default selectivity per bounded side for the share of a column that its most-common values
/// do not cover, when the source has no histogram (the long-standing range default).
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.33;

/// A selectivity estimate and what it was based on (for `el-ballista plan` reasons).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SelectivityEstimate {
    pub value: f64,
    pub basis: EstimateBasis,
}

/// Where a [`SelectivityEstimate`] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EstimateBasis {
    /// `n_distinct` / `null_frac` / fixed defaults — the per-predicate estimate.
    Statistics,
    /// The column's histogram and most-common values, over a range formed by `comparisons`
    /// range comparisons (more than one when sibling filters on the same column joined in).
    /// `most_common_only`: the source had no histogram, only most-common values (typical of a
    /// low-cardinality date column).
    Histogram {
        comparisons: usize,
        most_common_only: bool,
    },
}

impl SelectivityEstimate {
    /// A short suffix for decision reasons: empty for the plain statistics estimate.
    pub(crate) fn describe_basis(&self) -> String {
        let window = |n: usize| {
            if n > 1 {
                format!(", window of {n} range filters")
            } else {
                String::new()
            }
        };
        match self.basis {
            EstimateBasis::Statistics => String::new(),
            EstimateBasis::Histogram {
                comparisons,
                most_common_only,
            } => {
                let source = if most_common_only {
                    "most-common values"
                } else {
                    "histogram"
                };
                format!(" from {source}{}", window(comparisons))
            }
        }
    }

    /// Whether sibling filters joined this estimate into a window. The cost of such a filter
    /// is the window's, so an `EXPLAIN` of the filter on its own (an open-ended half-range)
    /// does not describe it.
    pub(crate) fn is_window(&self) -> bool {
        matches!(self.basis, EstimateBasis::Histogram { comparisons, .. } if comparisons > 1)
    }
}

/// Source cost for a decision: the `EXPLAIN` estimate of the predicate when one is cached,
/// except for a window estimate (see [`SelectivityEstimate::is_window`]), which — like a
/// predicate without `EXPLAIN` — uses the table-size heuristic over its selectivity. That keeps
/// both sides of a window on one cost, and `el-ballista plan` (which warms `EXPLAIN`) in agreement with
/// a first run (which has not).
pub(crate) fn decision_cost(estimate: &SelectivityEstimate, inputs: &CostInputs<'_>) -> u64 {
    let heuristic = || source_cost_for(estimate.value, inputs.stats);
    if estimate.is_window() {
        heuristic()
    } else {
        inputs.explain_cost().unwrap_or_else(heuristic)
    }
}

/// Selectivity of `predicate`, refined for range comparisons on ordered columns (integer,
/// timestamp, date):
///
/// - The predicate's own range comparisons on one column (a single `col < x`, or an `AND` of
///   them) are joined with every range comparison on the same column among `siblings`, into
///   one window `[lo, hi]` — the rows the source returns when the window is pushed.
/// - With a histogram and/or most-common values, the window is estimated the way Postgres does
///   it: the most-common values inside it, plus the histogram's share of the remaining rows
///   (linear inside a bucket).
/// - Without them (no statistics for the column, or none of its distribution), nothing changes:
///   the plain per-predicate estimate. A window gets no discount from fixed factors, so a
///   provider without the distribution keeps rather than pushes on a guess.
///
/// Everything else uses [`estimate_selectivity_from_stats`].
pub(crate) fn estimate_selectivity(
    predicate: &Predicate,
    stats: &SourceStatistics,
    siblings: &[Predicate],
) -> SelectivityEstimate {
    estimate_range(predicate, stats, siblings).unwrap_or_else(|| SelectivityEstimate {
        value: estimate_selectivity_from_stats(predicate, stats),
        basis: EstimateBasis::Statistics,
    })
}

/// `column op literal` for a range comparison on a plain column, with the operator turned so
/// the column is on the left (`5 < id` becomes `id > 5`). Equality and `<>` are not ranges.
fn range_comparison(predicate: &Predicate) -> Option<(&str, CmpOp, &Literal)> {
    let Predicate::Cmp { left, op, right } = predicate else {
        return None;
    };
    let (column, op, literal) = match (left.as_ref(), right.as_ref()) {
        (Predicate::Column(c), Predicate::Literal(l)) => (c, *op, l),
        (Predicate::Literal(l), Predicate::Column(c)) => {
            let flipped = match op {
                CmpOp::Lt => CmpOp::Gt,
                CmpOp::LtEq => CmpOp::GtEq,
                CmpOp::Gt => CmpOp::Lt,
                CmpOp::GtEq => CmpOp::LtEq,
                CmpOp::Eq | CmpOp::NotEq => return None,
            };
            (c, flipped, l)
        }
        _ => return None,
    };
    matches!(op, CmpOp::Lt | CmpOp::LtEq | CmpOp::Gt | CmpOp::GtEq).then_some((
        column.as_str(),
        op,
        literal,
    ))
}

/// The range comparisons making up `predicate` when it is one range comparison or an `AND`
/// tree of range comparisons on a single column; `None` for any other shape.
fn range_leaves<'p>(
    predicate: &'p Predicate,
    out: &mut Vec<(&'p str, CmpOp, &'p Literal)>,
) -> bool {
    match predicate {
        Predicate::And(l, r) => range_leaves(l, out) && range_leaves(r, out),
        other => match range_comparison(other) {
            Some(leaf) if out.first().is_none_or(|first| first.0 == leaf.0) => {
                out.push(leaf);
                true
            }
            _ => false,
        },
    }
}

/// One end of a range on a column's numeric axis.
#[derive(Debug, Clone, Copy)]
struct Bound {
    at: f64,
    inclusive: bool,
}

/// The intersection of range comparisons on one column.
#[derive(Debug, Clone, Copy, Default)]
struct Window {
    lo: Option<Bound>,
    hi: Option<Bound>,
}

impl Window {
    /// Narrow the window by `column op at`.
    fn tighten(&mut self, op: CmpOp, at: f64) {
        let inclusive = matches!(op, CmpOp::LtEq | CmpOp::GtEq);
        let bound = Bound { at, inclusive };
        match op {
            CmpOp::Gt | CmpOp::GtEq => {
                // Keep the higher lower bound; at a tie the exclusive one is tighter.
                let tighter = match self.lo {
                    None => true,
                    Some(lo) => at > lo.at || (at == lo.at && !inclusive),
                };
                if tighter {
                    self.lo = Some(bound);
                }
            }
            CmpOp::Lt | CmpOp::LtEq => {
                let tighter = match self.hi {
                    None => true,
                    Some(hi) => at < hi.at || (at == hi.at && !inclusive),
                };
                if tighter {
                    self.hi = Some(bound);
                }
            }
            CmpOp::Eq | CmpOp::NotEq => {}
        }
    }

    fn contains(&self, v: f64) -> bool {
        let above_lo = self
            .lo
            .is_none_or(|lo| v > lo.at || (lo.inclusive && v == lo.at));
        let below_hi = self
            .hi
            .is_none_or(|hi| v < hi.at || (hi.inclusive && v == hi.at));
        above_lo && below_hi
    }

    fn bounded_sides(&self) -> i32 {
        i32::from(self.lo.is_some()) + i32::from(self.hi.is_some())
    }
}

fn estimate_range(
    predicate: &Predicate,
    stats: &SourceStatistics,
    siblings: &[Predicate],
) -> Option<SelectivityEstimate> {
    let mut own = Vec::new();
    if !range_leaves(predicate, &mut own) || own.is_empty() {
        return None;
    }
    let column = own[0].0;
    let col_stats = stats.columns.get(column)?;
    let has_distribution =
        col_stats.histogram_bounds.len() >= 2 || !col_stats.most_common_vals.is_empty();
    if !has_distribution {
        return None;
    }

    let mut window = Window::default();
    let mut comparisons = 0;
    for (_, op, literal) in &own {
        window.tighten(*op, ordinal(literal)?);
        comparisons += 1;
    }
    for sibling in siblings {
        let mut leaves = Vec::new();
        if range_leaves(sibling, &mut leaves) && leaves.first().is_some_and(|l| l.0 == column) {
            for (_, op, literal) in leaves {
                if let Some(at) = ordinal(literal) {
                    window.tighten(op, at);
                    comparisons += 1;
                }
            }
        }
    }

    let value = window_selectivity(col_stats, &window)?;
    Some(SelectivityEstimate {
        value,
        basis: EstimateBasis::Histogram {
            comparisons,
            most_common_only: col_stats.histogram_bounds.len() < 2,
        },
    })
}

/// Share of the histogram population below `v`: `0` below the first bound, `1` above the
/// last, linear inside the bucket that holds `v` (every bucket holds the same share).
fn histogram_fraction_below(bounds: &[f64], v: f64) -> f64 {
    let (first, last) = match (bounds.first(), bounds.last()) {
        (Some(f), Some(l)) if bounds.len() >= 2 => (*f, *l),
        _ => return 0.5,
    };
    if v <= first {
        return 0.0;
    }
    if v >= last {
        return 1.0;
    }
    // bounds[i] <= v < bounds[i + 1]
    let i = bounds.partition_point(|b| *b <= v) - 1;
    let (lo, hi) = (bounds[i], bounds[i + 1]);
    let within = if hi > lo { (v - lo) / (hi - lo) } else { 0.5 };
    (i as f64 + within) / (bounds.len() - 1) as f64
}

/// Selectivity of `window` from the column's most-common values and histogram, or `None`
/// when the source has neither for the column. Postgres builds the histogram from the rows
/// that are neither NULL nor a most-common value, so the two parts add up.
fn window_selectivity(stats: &ColumnStats, window: &Window) -> Option<f64> {
    let has_mcv = !stats.most_common_vals.is_empty()
        && stats.most_common_vals.len() == stats.most_common_freqs.len();
    let has_hist = stats.histogram_bounds.len() >= 2;
    if !has_mcv && !has_hist {
        return None;
    }

    let (mcv_total, mcv_inside) = if has_mcv {
        stats
            .most_common_vals
            .iter()
            .zip(&stats.most_common_freqs)
            .fold((0.0, 0.0), |(total, inside), (v, f)| {
                (
                    total + f,
                    if window.contains(*v) {
                        inside + f
                    } else {
                        inside
                    },
                )
            })
    } else {
        (0.0, 0.0)
    };
    let rest = (1.0 - f64::from(stats.null_frac) - mcv_total).max(0.0);

    let hist_share = if has_hist {
        let bounds = &stats.histogram_bounds;
        let below_hi = window
            .hi
            .map_or(1.0, |hi| histogram_fraction_below(bounds, hi.at));
        let below_lo = window
            .lo
            .map_or(0.0, |lo| histogram_fraction_below(bounds, lo.at));
        (below_hi - below_lo).max(0.0)
    } else {
        // Only most-common values are known (typical for a low-cardinality date column): the
        // rest, usually tiny, gets the default factor per bounded side.
        DEFAULT_RANGE_SELECTIVITY.powi(window.bounded_sides())
    };

    Some((mcv_inside + rest * hist_share).clamp(0.0, 1.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pushdown::explain::AccessMethod;
    use crate::pushdown::{Literal, Predicate};
    use std::collections::HashMap;

    fn create_test_stats() -> SourceStatistics {
        let mut columns = HashMap::new();
        for (name, n_distinct) in [("id", 1000.0), ("status", 5.0), ("name", 1000.0)] {
            columns.insert(
                name.to_string(),
                ColumnStats {
                    column_name: name.to_string(),
                    n_distinct,
                    null_frac: 0.0,
                    avg_width: 8,
                    ..Default::default()
                },
            );
        }
        SourceStatistics {
            table_name: "test_table".to_string(),
            row_count_estimate: 1000.0,
            table_size_bytes: 10_000_000, // 10MB
            columns,
            fetched_at: chrono::Utc::now(),
        }
    }

    fn btree(name: &str, columns: &[&str]) -> IndexInfo {
        IndexInfo {
            name: name.to_string(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            is_unique: false,
            is_primary: false,
            index_type: "btree".to_string(),
            is_partial: false,
            has_expressions: false,
        }
    }

    fn cmp(col: &str, op: CmpOp, v: i64) -> Predicate {
        Predicate::Cmp {
            left: Box::new(Predicate::Column(col.to_string())),
            op,
            right: Box::new(Predicate::Literal(Literal::Int(v))),
        }
    }

    fn inputs<'a>(
        stats: &'a SourceStatistics,
        params: &'a CostParams,
        indexes: &'a [IndexInfo],
        kinds: &'a ColumnKinds,
    ) -> CostInputs<'a> {
        CostInputs {
            stats,
            params,
            indexes,
            explain: None,
            column_kinds: kinds,
            siblings: &[],
        }
    }

    #[test]
    fn test_cost_params_defaults() {
        let params = CostParams::default();
        assert_eq!(params.max_source_cost, 50_000);
        assert_eq!(params.keep_threshold, 0.30);
    }

    #[test]
    fn test_or_requires_index_on_every_branch() {
        let stats = create_test_stats();
        let params = CostParams::default();
        let kinds = ColumnKinds::new();
        let indexes = vec![btree("orders_pkey", &["id"])];
        let inputs = inputs(&stats, &params, &indexes, &kinds);

        let both = Predicate::Or(
            Box::new(cmp("id", CmpOp::Eq, 1)),
            Box::new(cmp("id", CmpOp::Eq, 2)),
        );
        assert!(matches!(
            decide_push(&both, &inputs),
            CostDecision::Push {
                has_index: true,
                ..
            }
        ));

        // One branch unindexed -> must NOT take the near-free index shortcut.
        let mixed = Predicate::Or(
            Box::new(cmp("id", CmpOp::Eq, 1)),
            Box::new(cmp("status", CmpOp::Eq, 1)),
        );
        assert!(!matches!(
            decide_push(&mixed, &inputs),
            CostDecision::Push {
                has_index: true,
                ..
            }
        ));
    }

    #[test]
    fn test_index_shortcut_requires_leading_plain_key() {
        let kinds = ColumnKinds::new();
        let p = cmp("id", CmpOp::Eq, 1);
        // Leading key: usable.
        assert!(usable_index(&p, &[btree("a", &["id", "x"])], &kinds).is_some());
        // Second key only: not usable.
        assert!(usable_index(&p, &[btree("a", &["x", "id"])], &kinds).is_none());
        // Partial or expression index: not usable.
        let mut partial = btree("p", &["id"]);
        partial.is_partial = true;
        assert!(usable_index(&p, &[partial], &kinds).is_none());
        let mut expr = btree("e", &["id"]);
        expr.has_expressions = true;
        assert!(usable_index(&p, &[expr], &kinds).is_none());
        // Hash only serves equality; gin never serves scalar comparisons.
        let mut hash = btree("h", &["id"]);
        hash.index_type = "hash".to_string();
        assert!(usable_index(&p, std::slice::from_ref(&hash), &kinds).is_some());
        assert!(usable_index(&cmp("id", CmpOp::Lt, 1), &[hash], &kinds).is_none());
        let mut gin = btree("g", &["id"]);
        gin.index_type = "gin".to_string();
        assert!(usable_index(&p, &[gin], &kinds).is_none());
        // `<>` never uses the shortcut.
        assert!(
            usable_index(&cmp("id", CmpOp::NotEq, 1), &[btree("a", &["id"])], &kinds).is_none()
        );
    }

    #[test]
    fn test_index_shortcut_for_binary_collated_text_needs_bytewise_column() {
        let text_cmp = Predicate::Cmp {
            left: Box::new(Predicate::Collate {
                expr: Box::new(Predicate::Column("name".to_string())),
                collation: Collation::Binary,
            }),
            op: CmpOp::Eq,
            right: Box::new(Predicate::Literal(Literal::Text("a".to_string()))),
        };
        let indexes = [btree("name_idx", &["name"])];
        let icu = ColumnKinds::from([(
            "name".to_string(),
            ColumnKind::Text {
                bytewise_collation: false,
            },
        )]);
        assert!(usable_index(&text_cmp, &indexes, &icu).is_none());
        let c = ColumnKinds::from([(
            "name".to_string(),
            ColumnKind::Text {
                bytewise_collation: true,
            },
        )]);
        assert!(usable_index(&text_cmp, &indexes, &c).is_some());
        // Unknown collation: never assumed.
        assert!(usable_index(&text_cmp, &indexes, &ColumnKinds::new()).is_none());
    }

    #[test]
    fn test_explain_index_path_takes_shortcut_and_missing_cost_falls_back() {
        let stats = create_test_stats();
        let params = CostParams::default();
        let kinds = ColumnKinds::new();
        let mut inputs = inputs(&stats, &params, &[], &kinds);
        inputs.explain = Some(ExplainEstimate {
            access_method: AccessMethod::IndexScan,
            total_cost: None,
            plan_rows: None,
            index_name: Some("idx".to_string()),
            estimated_at: chrono::Utc::now(),
        });
        let p = cmp("status", CmpOp::Eq, 1);
        assert!(matches!(
            decide_push(&p, &inputs),
            CostDecision::Push {
                has_index: true,
                ..
            }
        ));

        // A seq-scan estimate with no Total Cost: the heuristic cost is used, not 0.
        inputs.explain = Some(ExplainEstimate {
            access_method: AccessMethod::SequentialScan,
            total_cost: None,
            plan_rows: None,
            index_name: None,
            estimated_at: chrono::Utc::now(),
        });
        assert_eq!(inputs.explain_cost(), None);
        let tight = CostParams {
            max_source_cost: 5,
            keep_threshold: 0.30,
        };
        inputs.params = &tight;
        let p = cmp("id", CmpOp::Eq, 1);
        assert!(matches!(
            decide_push(&p, &inputs),
            CostDecision::Keep { .. }
        ));
    }

    #[test]
    fn test_decide_push_no_index_low_selectivity_within_budget_pushes() {
        let stats = create_test_stats();
        let params = CostParams::default();
        let kinds = ColumnKinds::new();
        let decision = decide_push(
            &cmp("id", CmpOp::Eq, 42),
            &inputs(&stats, &params, &[], &kinds),
        );
        match decision {
            CostDecision::Push {
                has_index,
                selectivity,
                ..
            } => {
                assert!(!has_index, "no index was supplied, must not claim one");
                assert!(selectivity < params.keep_threshold);
            }
            other => panic!("expected Push, got {other:?}"),
        }
    }

    #[test]
    fn test_decide_push_range_predicate_keeps_by_default() {
        // Range comparisons estimate a flat 0.33 selectivity, which is >= the default
        // 0.30 keep_threshold: every unindexed range predicate should Keep out of the box.
        let stats = create_test_stats();
        let params = CostParams::default();
        let kinds = ColumnKinds::new();
        let decision = decide_push(
            &cmp("id", CmpOp::Gt, 0),
            &inputs(&stats, &params, &[], &kinds),
        );
        assert!(
            matches!(decision, CostDecision::Keep { .. }),
            "expected Keep for a range predicate under the default threshold, got {decision:?}"
        );
    }

    #[test]
    fn test_decide_push_exceeds_cost_budget_keeps() {
        let stats = create_test_stats();
        let params = CostParams {
            max_source_cost: 5,
            keep_threshold: 0.30,
        };
        let kinds = ColumnKinds::new();
        let mut inputs = inputs(&stats, &params, &[], &kinds);
        inputs.explain = Some(ExplainEstimate {
            access_method: AccessMethod::SequentialScan,
            total_cost: Some(1_000_000.0),
            plan_rows: Some(1.0),
            index_name: None,
            estimated_at: chrono::Utc::now(),
        });
        match decide_push(&cmp("id", CmpOp::Eq, 42), &inputs) {
            CostDecision::Keep { selectivity, .. } => {
                assert!(
                    selectivity < params.keep_threshold,
                    "must have passed the selectivity gate before failing on cost"
                );
            }
            other => panic!("expected Keep (cost over budget), got {other:?}"),
        }
    }

    #[test]
    fn test_decide_push_index_shortcut_ignores_cost_and_selectivity() {
        let stats = create_test_stats();
        let params = CostParams::default();
        let kinds = ColumnKinds::new();
        let indexes = vec![btree("id_idx", &["id"])];
        match decide_push(
            &cmp("id", CmpOp::Eq, 42),
            &inputs(&stats, &params, &indexes, &kinds),
        ) {
            CostDecision::Push {
                has_index,
                cost_estimate,
                ..
            } => {
                assert!(has_index);
                assert_eq!(cost_estimate, 1);
            }
            other => panic!("expected index-shortcut Push, got {other:?}"),
        }
    }

    #[test]
    fn test_estimate_selectivity_equality_and_clamp() {
        let mut stats = create_test_stats();
        let sel = estimate_selectivity_from_stats(&cmp("id", CmpOp::Eq, 42), &stats);
        assert!(sel > 0.0 && sel < 0.1); // 1/1000

        // A fractional n_distinct (tiny table) must not produce selectivity > 1.
        if let Some(c) = stats.columns.get_mut("id") {
            c.n_distinct = 0.25;
        }
        assert_eq!(
            estimate_selectivity_from_stats(&cmp("id", CmpOp::Eq, 42), &stats),
            1.0
        );
        assert_eq!(
            estimate_selectivity_from_stats(&cmp("id", CmpOp::NotEq, 42), &stats),
            0.0
        );
    }

    #[test]
    fn test_estimate_selectivity_looks_through_collate() {
        let stats = create_test_stats();
        let p = Predicate::Cmp {
            left: Box::new(Predicate::Collate {
                expr: Box::new(Predicate::Column("status".to_string())),
                collation: Collation::Binary,
            }),
            op: CmpOp::Eq,
            right: Box::new(Predicate::Literal(Literal::Text("PAID".to_string()))),
        };
        assert!((estimate_selectivity_from_stats(&p, &stats) - 0.2).abs() < 1e-9);
    }

    /// A timestamp comparison `column op <seconds since epoch>`.
    fn ts_cmp(column: &str, op: CmpOp, secs: i64) -> Predicate {
        Predicate::Cmp {
            left: Box::new(Predicate::Column(column.to_string())),
            op,
            right: Box::new(Predicate::Literal(Literal::Timestamp(
                chrono::DateTime::from_timestamp(secs, 0).unwrap(),
            ))),
        }
    }

    fn date_cmp(column: &str, op: CmpOp, days: i64) -> Predicate {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        Predicate::Cmp {
            left: Box::new(Predicate::Column(column.to_string())),
            op,
            right: Box::new(Predicate::Literal(Literal::Date(
                epoch + chrono::Duration::days(days),
            ))),
        }
    }

    /// Stats for a `ts` column spread uniformly over 100 days (histogram of 101 bounds, one
    /// per day, no most-common values), a `d` date column whose 10 values are all
    /// most-common (no histogram, 10% each), and an `n` column with statistics but no
    /// distribution. 100 MB table.
    fn distribution_stats() -> SourceStatistics {
        const DAY: f64 = 86_400.0;
        let mut columns = HashMap::new();
        columns.insert(
            "ts".to_string(),
            ColumnStats {
                column_name: "ts".to_string(),
                n_distinct: 1_000_000.0,
                histogram_bounds: (0..=100).map(|d| f64::from(d) * DAY).collect(),
                ..Default::default()
            },
        );
        columns.insert(
            "d".to_string(),
            ColumnStats {
                column_name: "d".to_string(),
                n_distinct: 10.0,
                most_common_vals: (0..10).map(f64::from).collect(),
                most_common_freqs: vec![0.1; 10],
                ..Default::default()
            },
        );
        columns.insert(
            "n".to_string(),
            ColumnStats {
                column_name: "n".to_string(),
                n_distinct: 1_000.0,
                ..Default::default()
            },
        );
        SourceStatistics {
            table_name: "events".to_string(),
            row_count_estimate: 1_000_000.0,
            table_size_bytes: 100_000_000,
            columns,
            fetched_at: chrono::Utc::now(),
        }
    }

    const DAY: i64 = 86_400;

    #[test]
    fn test_histogram_fraction_below() {
        let bounds = [0.0, 10.0, 20.0, 40.0];
        assert_eq!(histogram_fraction_below(&bounds, -5.0), 0.0);
        assert_eq!(histogram_fraction_below(&bounds, 0.0), 0.0);
        assert!((histogram_fraction_below(&bounds, 5.0) - 1.0 / 6.0).abs() < 1e-12);
        assert!((histogram_fraction_below(&bounds, 10.0) - 1.0 / 3.0).abs() < 1e-12);
        // Third bucket is wider: halfway through it is 2.5 of 3 buckets.
        assert!((histogram_fraction_below(&bounds, 30.0) - 2.5 / 3.0).abs() < 1e-12);
        assert_eq!(histogram_fraction_below(&bounds, 40.0), 1.0);
        assert_eq!(histogram_fraction_below(&bounds, 99.0), 1.0);
        // Repeated bounds (a heavy value) do not divide by zero.
        let flat = [1.0, 1.0, 1.0, 2.0];
        assert!(histogram_fraction_below(&flat, 1.5).is_finite());
    }

    #[test]
    fn test_histogram_range_selectivity_one_side() {
        let stats = distribution_stats();
        // Last 25 of 100 days.
        let est = estimate_selectivity(&ts_cmp("ts", CmpOp::GtEq, 75 * DAY), &stats, &[]);
        assert!((est.value - 0.25).abs() < 1e-9, "{est:?}");
        assert_eq!(
            est.basis,
            EstimateBasis::Histogram {
                comparisons: 1,
                most_common_only: false
            }
        );
        // Literal on the left is the same range.
        let flipped = Predicate::Cmp {
            left: Box::new(Predicate::Literal(Literal::Timestamp(
                chrono::DateTime::from_timestamp(75 * DAY, 0).unwrap(),
            ))),
            op: CmpOp::LtEq,
            right: Box::new(Predicate::Column("ts".to_string())),
        };
        assert!((estimate_selectivity(&flipped, &stats, &[]).value - 0.25).abs() < 1e-9);
        // Out of the histogram's range: nothing / everything.
        assert_eq!(
            estimate_selectivity(&ts_cmp("ts", CmpOp::Gt, 500 * DAY), &stats, &[]).value,
            0.0
        );
        assert_eq!(
            estimate_selectivity(&ts_cmp("ts", CmpOp::Lt, 500 * DAY), &stats, &[]).value,
            1.0
        );
    }

    #[test]
    fn test_window_with_sibling_filters() {
        let stats = distribution_stats();
        let lo = ts_cmp("ts", CmpOp::GtEq, 50 * DAY);
        let hi = ts_cmp("ts", CmpOp::Lt, 51 * DAY);
        // Alone, the lower bound keeps half the table...
        assert!((estimate_selectivity(&lo, &stats, &[]).value - 0.5).abs() < 1e-9);
        // ...judged with its sibling it is a one-day window: 1%.
        let est = estimate_selectivity(&lo, &stats, std::slice::from_ref(&hi));
        assert!((est.value - 0.01).abs() < 1e-9, "{est:?}");
        assert_eq!(
            est.basis,
            EstimateBasis::Histogram {
                comparisons: 2,
                most_common_only: false
            }
        );
        assert_eq!(
            est.describe_basis(),
            " from histogram, window of 2 range filters"
        );
        // Symmetric for the upper bound.
        let est = estimate_selectivity(&hi, &stats, std::slice::from_ref(&lo));
        assert!((est.value - 0.01).abs() < 1e-9);
        // The same window as one AND predicate.
        let and = Predicate::And(Box::new(lo.clone()), Box::new(hi.clone()));
        assert!((estimate_selectivity(&and, &stats, &[]).value - 0.01).abs() < 1e-9);
        // Siblings on other columns, equality siblings, and OR siblings are ignored.
        let others = [
            ts_cmp("other", CmpOp::Lt, 51 * DAY),
            ts_cmp("ts", CmpOp::Eq, 51 * DAY),
            Predicate::Or(Box::new(hi.clone()), Box::new(hi.clone())),
        ];
        assert!((estimate_selectivity(&lo, &stats, &others).value - 0.5).abs() < 1e-9);
        // The tightest bound wins when several apply; an empty window is 0.
        let tighter = ts_cmp("ts", CmpOp::GtEq, 50 * DAY + DAY / 2);
        let est = estimate_selectivity(&lo, &stats, &[hi.clone(), tighter]);
        assert!((est.value - 0.005).abs() < 1e-9);
        let past = ts_cmp("ts", CmpOp::Lt, 10 * DAY);
        assert_eq!(estimate_selectivity(&lo, &stats, &[past]).value, 0.0);
    }

    #[test]
    fn test_most_common_values_window() {
        let stats = distribution_stats();
        // d in [3, 5): values 3 and 4 -> 20%.
        let lo = date_cmp("d", CmpOp::GtEq, 3);
        let hi = date_cmp("d", CmpOp::Lt, 5);
        let est = estimate_selectivity(&lo, &stats, std::slice::from_ref(&hi));
        assert!((est.value - 0.2).abs() < 1e-9, "{est:?}");
        assert_eq!(
            est.describe_basis(),
            " from most-common values, window of 2 range filters"
        );
        // Inclusive vs exclusive bound on an exact most-common value.
        let est = estimate_selectivity(&date_cmp("d", CmpOp::Gt, 8), &stats, &[]);
        assert!((est.value - 0.1).abs() < 1e-9);
        let est = estimate_selectivity(&date_cmp("d", CmpOp::GtEq, 8), &stats, &[]);
        assert!((est.value - 0.2).abs() < 1e-9);
        // A single day on a date column.
        let est = estimate_selectivity(
            &date_cmp("d", CmpOp::GtEq, 9),
            &stats,
            &[date_cmp("d", CmpOp::LtEq, 9)],
        );
        assert!((est.value - 0.1).abs() < 1e-9);
    }

    #[test]
    fn test_window_without_distribution_gets_no_discount() {
        // Statistics but no histogram / most-common values (distribution query failed, or a
        // type it does not cover): no push on fixed factors — the plain estimate, alone or not.
        let stats = distribution_stats();
        let lo = cmp("n", CmpOp::GtEq, 10);
        let hi = cmp("n", CmpOp::Lt, 20);
        let alone = estimate_selectivity(&lo, &stats, &[]);
        let joint = estimate_selectivity(&lo, &stats, std::slice::from_ref(&hi));
        assert_eq!(alone, joint);
        assert_eq!(joint.basis, EstimateBasis::Statistics);
        assert!((joint.value - DEFAULT_RANGE_SELECTIVITY).abs() < 1e-12);
    }

    #[test]
    fn test_window_cost_ignores_explain_of_one_side() {
        // A cached EXPLAIN of `ts >= a` alone costs the whole open-ended scan; the window's
        // cost is the heuristic over the window, so plan preview and a cold run agree.
        let stats = distribution_stats();
        let params = CostParams::default();
        let kinds = ColumnKinds::new();
        let lo = ts_cmp("ts", CmpOp::GtEq, 50 * DAY);
        let siblings = [ts_cmp("ts", CmpOp::Lt, 51 * DAY)];
        let explain = ExplainEstimate {
            access_method: AccessMethod::SequentialScan,
            total_cost: Some(5_000_000.0),
            plan_rows: Some(500_000.0),
            index_name: None,
            estimated_at: chrono::Utc::now(),
        };
        let mut warm = inputs(&stats, &params, &[], &kinds);
        warm.explain = Some(explain.clone());
        warm.siblings = &siblings;
        let mut cold = inputs(&stats, &params, &[], &kinds);
        cold.siblings = &siblings;
        let (warm, cold) = (decide_push(&lo, &warm), decide_push(&lo, &cold));
        assert_eq!(warm, cold);
        assert!(matches!(warm, CostDecision::Push { .. }), "{warm:?}");
        // Without a window, a cached EXPLAIN cost still decides.
        let mut single = inputs(&stats, &params, &[], &kinds);
        single.explain = Some(explain);
        let narrow = ts_cmp("ts", CmpOp::GtEq, 99 * DAY); // 1% alone
        assert!(matches!(
            decide_push(&narrow, &single),
            CostDecision::Keep { .. }
        ));
    }

    #[test]
    fn test_no_column_statistics_never_gets_window_credit() {
        // A provider without statistics (e.g. rebuilt on a scheduler) must keep its
        // conservative default: no sibling makes an unknown column look selective.
        let stats = SourceStatistics::empty("events");
        let lo = ts_cmp("ts", CmpOp::GtEq, 50 * DAY);
        let hi = ts_cmp("ts", CmpOp::Lt, 51 * DAY);
        let est = estimate_selectivity(&lo, &stats, std::slice::from_ref(&hi));
        assert_eq!(est.basis, EstimateBasis::Statistics);
        assert_eq!(est.value, estimate_selectivity_from_stats(&lo, &stats));
    }

    #[test]
    fn test_decide_push_daily_window_on_unindexed_column() {
        // The incremental-load case: no index on `ts`, 100 MB table.
        let stats = distribution_stats();
        let params = CostParams::default();
        let kinds = ColumnKinds::new();
        let lo = ts_cmp("ts", CmpOp::GtEq, 50 * DAY);
        let hi = ts_cmp("ts", CmpOp::Lt, 51 * DAY);
        // Judged alone, the lower bound keeps half the rows: keep.
        let alone = decide_push(&lo, &inputs(&stats, &params, &[], &kinds));
        assert!(matches!(alone, CostDecision::Keep { .. }), "{alone:?}");
        // Judged as a window: 1% of ~97,656 KB = 976 cost units -> push, with the basis named.
        let mut with_sibling = inputs(&stats, &params, &[], &kinds);
        let siblings = [hi];
        with_sibling.siblings = &siblings;
        match decide_push(&lo, &with_sibling) {
            CostDecision::Push {
                reason,
                cost_estimate,
                ..
            } => {
                assert!(reason.contains("window of 2 range filters"), "{reason}");
                assert!(cost_estimate < params.max_source_cost);
            }
            other => panic!("expected Push for a one-day window, got {other:?}"),
        }
    }

    #[test]
    fn test_estimate_selectivity_and() {
        let stats = create_test_stats();
        let pred = Predicate::And(
            Box::new(cmp("id", CmpOp::Eq, 42)),
            Box::new(cmp("status", CmpOp::Eq, 1)),
        );
        // (1/1000) * (1/5) = 0.0002
        assert!(estimate_selectivity_from_stats(&pred, &stats) < 0.001);
    }
}
