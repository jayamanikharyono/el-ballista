//! Push/keep policy over translated predicates — docs/pushdown.md §4.3.
//! pushdown/policy.rs
//! Translation ([`crate::pushdown::translate_with`]) has already decided *whether a predicate
//! can be pushed correctly* and with which fidelity; this module decides *whether it should be*.
//! No policy ever changes the translated fidelity.

use crate::pushdown::cost_model::{
    CostDecision, CostInputs, column_has_plain_index, decide_push, decision_cost,
    estimate_selectivity,
};
use crate::pushdown::{ColumnKind, Fidelity, Literal, Predicate};

/// The push/keep policy — docs/pushdown.md §4.3.
/// - `Always`: push everything translatable (minus denylist). Dedicated replica.
/// - `Never`: keep everything in Arrow. Emergency: source under pressure.
/// - `CostBased`: the §4.2 model over real statistics (default). Normal operation.
/// - `Strict`: paranoid mode — push a filter only if every referenced column is the leading
///   key of a plain index, selectivity says it removes at least `(1 - keep_threshold)` of
///   rows, and every literal and column involved is primitive (bool/int/timestamp/date; floats,
///   text, and casts never push). `LIMIT` is never pushed under `strict`, and `push` hints are
///   ignored (`deny` still applies).
/// - `Hinted`: per-column overrides from the job spec — `push` forces, `deny` refuses, the
///   rest falls back to the cost model. When you know something the stats do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum PushdownPolicy {
    Always,
    Never,
    CostBased,
    Strict,
    Hinted,
}

/// A pushdown policy name that is not one of the known modes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "unknown pushdown policy {0:?} (expected one of: always, never, cost_based, strict, hinted)"
)]
pub struct UnknownPushdownPolicy(pub String);

impl PushdownPolicy {
    /// Parse a policy name, case-insensitively. Unknown names are an error rather than a
    /// silent fallback, so a mistyped emergency `never` cannot fail open.
    ///
    /// # Examples
    /// ```
    /// use el_ballista::pushdown::PushdownPolicy;
    /// assert_eq!(PushdownPolicy::parse("NEVER"), Ok(PushdownPolicy::Never));
    /// assert!(PushdownPolicy::parse("nevr").is_err());
    /// ```
    pub fn parse(raw: &str) -> Result<PushdownPolicy, UnknownPushdownPolicy> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "always" => Ok(PushdownPolicy::Always),
            "never" => Ok(PushdownPolicy::Never),
            "cost_based" => Ok(PushdownPolicy::CostBased),
            "strict" => Ok(PushdownPolicy::Strict),
            "hinted" => Ok(PushdownPolicy::Hinted),
            _ => Err(UnknownPushdownPolicy(raw.to_string())),
        }
    }
}

impl std::str::FromStr for PushdownPolicy {
    type Err = UnknownPushdownPolicy;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::parse(raw)
    }
}

/// The outcome of deciding whether one filter should be pushed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Decision {
    Push {
        fidelity: Fidelity,
        predicate: Predicate,
    },
    Keep,
}

/// Decision over an already-translated predicate: policy × denylist × push hints × cost model.
/// The pushed fidelity is always the translated one.
///
/// # Examples
///
/// ```
/// use el_ballista::pushdown::decide_translated;
/// use std::collections::HashMap;
/// use datafusion::prelude::{col, lit};
/// use el_ballista::pushdown::cost_model::CostParams;
/// use el_ballista::pushdown::stats::SourceStatistics;
/// use el_ballista::pushdown::{ColumnKinds, CostInputs, Decision, PushdownPolicy, translate};
///
/// let stats = SourceStatistics {
///     table_name: "orders".into(), row_count_estimate: 1e6, table_size_bytes: 0,
///     columns: HashMap::new(), fetched_at: chrono::Utc::now(),
/// };
/// let (params, kinds) = (CostParams::default(), ColumnKinds::new());
/// let inputs = CostInputs {
///     stats: &stats, params: &params, indexes: &[], explain: None, column_kinds: &kinds,
///     siblings: &[],
/// };
/// let (fidelity, predicate) = translate(&col("id").eq(lit(7i64))).unwrap();
/// let deny = ["id".to_string()];
/// let pushed = decide_translated(fidelity, predicate.clone(), PushdownPolicy::Always, &[], &[], &inputs);
/// assert!(matches!(pushed, Decision::Push { .. }));
/// let kept = decide_translated(fidelity, predicate, PushdownPolicy::Always, &deny, &[], &inputs);
/// assert!(matches!(kept, Decision::Keep));
/// ```
pub fn decide_translated(
    fidelity: Fidelity,
    predicate: Predicate,
    policy: PushdownPolicy,
    deny: &[String],
    push: &[String],
    inputs: &CostInputs<'_>,
) -> Decision {
    decide_explained(fidelity, predicate, policy, deny, push, inputs).0
}

/// [`decide_translated`] plus a human-readable reason for the verdict (for `el-ballista plan
/// --explain`). One function computes both, so the explanation can never describe a different
/// decision than the one taken.
///
/// # Examples
///
/// ```
/// use el_ballista::pushdown::decide_explained;
/// use std::collections::HashMap;
/// use datafusion::prelude::{col, lit};
/// use el_ballista::pushdown::cost_model::CostParams;
/// use el_ballista::pushdown::stats::SourceStatistics;
/// use el_ballista::pushdown::{ColumnKinds, CostInputs, Decision, PushdownPolicy, translate};
///
/// let stats = SourceStatistics {
///     table_name: "orders".into(), row_count_estimate: 1e6, table_size_bytes: 0,
///     columns: HashMap::new(), fetched_at: chrono::Utc::now(),
/// };
/// let (params, kinds) = (CostParams::default(), ColumnKinds::new());
/// let inputs = CostInputs {
///     stats: &stats, params: &params, indexes: &[], explain: None, column_kinds: &kinds,
///     siblings: &[],
/// };
/// let (fidelity, predicate) = translate(&col("id").eq(lit(7i64))).unwrap();
/// let (decision, reason) =
///     decide_explained(fidelity, predicate, PushdownPolicy::Never, &[], &[], &inputs);
/// assert!(matches!(decision, Decision::Keep));
/// assert_eq!(reason, "policy=never");
/// ```
pub fn decide_explained(
    fidelity: Fidelity,
    predicate: Predicate,
    policy: PushdownPolicy,
    deny: &[String],
    push: &[String],
    inputs: &CostInputs<'_>,
) -> (Decision, String) {
    if policy == PushdownPolicy::Never {
        return (Decision::Keep, "policy=never".to_string());
    }
    if references_denied_column(&predicate, deny) {
        return (Decision::Keep, "denylisted column".to_string());
    }
    let push_it = |reason: String, predicate: Predicate| {
        (
            Decision::Push {
                fidelity,
                predicate,
            },
            reason,
        )
    };
    match policy {
        PushdownPolicy::Never => (Decision::Keep, "policy=never".to_string()),
        PushdownPolicy::Always => push_it("policy=always".to_string(), predicate),
        PushdownPolicy::Strict => {
            let gate = strict_gate(&predicate, inputs);
            let reason = gate.describe();
            match gate {
                StrictGate::Push => push_it(reason, predicate),
                _ => (Decision::Keep, reason),
            }
        }
        PushdownPolicy::Hinted if references_push_column(&predicate, push) => {
            push_it("hinted column".to_string(), predicate)
        }
        PushdownPolicy::Hinted | PushdownPolicy::CostBased => {
            match decide_push(&predicate, inputs) {
                CostDecision::Push { reason, .. } => push_it(reason, predicate),
                CostDecision::Keep { reason, .. } => (Decision::Keep, reason),
            }
        }
    }
}

/// One gate of the `strict` policy, in evaluation order. The first failing gate decides.
enum StrictGate {
    Push,
    NoStats,
    NoColumns,
    NotIndexed,
    NonPrimitive,
    Unselective { selectivity: f64, threshold: f64 },
    OverBudget { cost: u64, budget: u64 },
}

impl StrictGate {
    fn describe(&self) -> String {
        match self {
            StrictGate::Push => "strict: indexed + selective + primitive".to_string(),
            StrictGate::NoStats => "strict: no statistics reachable".to_string(),
            StrictGate::NoColumns => "strict: predicate references no columns".to_string(),
            StrictGate::NotIndexed => "strict: not all columns indexed".to_string(),
            StrictGate::NonPrimitive => {
                "strict: non-primitive type (float/text/cast/other)".to_string()
            }
            StrictGate::Unselective {
                selectivity,
                threshold,
            } => format!(
                "strict: selectivity too high ({:.2}% >= {:.2}%)",
                selectivity * 100.0,
                threshold * 100.0
            ),
            StrictGate::OverBudget { cost, budget } => {
                format!("strict: cost exceeds budget ({cost} > {budget})")
            }
        }
    }
}

/// `strict` policy: every gate must pass, otherwise keep. Without statistics strict keeps
/// everything — paranoid means paranoid.
fn strict_gate(predicate: &Predicate, inputs: &CostInputs<'_>) -> StrictGate {
    if inputs.stats.columns.is_empty() {
        return StrictGate::NoStats;
    }

    let columns = predicate_columns(predicate);
    if columns.is_empty() {
        return StrictGate::NoColumns;
    }
    if !columns
        .iter()
        .all(|col| column_has_plain_index(col, inputs.indexes))
    {
        return StrictGate::NotIndexed;
    }

    if !is_primitive_predicate(predicate, inputs) {
        return StrictGate::NonPrimitive;
    }

    let estimate = estimate_selectivity(predicate, inputs.stats, inputs.siblings);
    let selectivity = estimate.value;
    if selectivity >= inputs.params.keep_threshold {
        return StrictGate::Unselective {
            selectivity,
            threshold: inputs.params.keep_threshold,
        };
    }

    let estimated = decision_cost(&estimate, inputs);
    if estimated > inputs.params.max_source_cost {
        return StrictGate::OverBudget {
            cost: estimated,
            budget: inputs.params.max_source_cost,
        };
    }

    StrictGate::Push
}

/// Primitive shapes only: bool/int/timestamp/date literals over boolean/integer/timestamp/date
/// columns. Anything else (float, text, casts, collations, unknown columns) fails closed.
fn is_primitive_predicate(predicate: &Predicate, inputs: &CostInputs<'_>) -> bool {
    match predicate {
        Predicate::Column(name) => matches!(
            inputs.column_kinds.get(name),
            Some(
                ColumnKind::Boolean
                    | ColumnKind::Integer
                    | ColumnKind::Timestamp
                    | ColumnKind::Date
            )
        ),
        Predicate::Literal(lit) => matches!(
            lit,
            Literal::Bool(_) | Literal::Int(_) | Literal::Timestamp(_) | Literal::Date(_)
        ),
        Predicate::Cmp { left, right, .. }
        | Predicate::And(left, right)
        | Predicate::Or(left, right) => {
            is_primitive_predicate(left, inputs) && is_primitive_predicate(right, inputs)
        }
        Predicate::Not(p) | Predicate::IsNull(p) | Predicate::IsNotNull(p) => {
            is_primitive_predicate(p, inputs)
        }
        Predicate::Cast { .. } | Predicate::Collate { .. } => false,
    }
}

/// Whether a predicate touches any denylisted column. Matching is case-insensitive: a denylist
/// is a safety control, so `deny: ["Email"]` must also block `email`.
fn references_denied_column(predicate: &Predicate, deny: &[String]) -> bool {
    !deny.is_empty()
        && predicate_columns(predicate)
            .iter()
            .any(|col| deny.iter().any(|d| d.eq_ignore_ascii_case(col)))
}

/// `hinted` policy: a predicate touching any column in the job spec's `push` list is forced to
/// the source (provided it translated — hints never override correctness). Deny wins over push.
fn references_push_column(predicate: &Predicate, push: &[String]) -> bool {
    !push.is_empty()
        && predicate_columns(predicate)
            .iter()
            .any(|col| push.iter().any(|p| p == col))
}

/// Sorted, deduplicated column names referenced by a predicate.
pub(crate) fn predicate_columns(predicate: &Predicate) -> Vec<String> {
    fn walk(predicate: &Predicate, out: &mut Vec<String>) {
        match predicate {
            Predicate::Column(name) => {
                if !out.contains(name) {
                    out.push(name.clone());
                }
            }
            Predicate::Literal(_) => {}
            Predicate::Cmp { left, right, .. }
            | Predicate::And(left, right)
            | Predicate::Or(left, right) => {
                walk(left, out);
                walk(right, out);
            }
            Predicate::Not(p) | Predicate::IsNull(p) | Predicate::IsNotNull(p) => walk(p, out),
            Predicate::Cast { expr, .. } | Predicate::Collate { expr, .. } => walk(expr, out),
        }
    }

    let mut columns = Vec::new();
    walk(predicate, &mut columns);
    columns.sort();
    columns
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pushdown::cost_model::CostParams;
    use crate::pushdown::stats::{ColumnStats, IndexInfo, SourceStatistics};
    use crate::pushdown::{ColumnKinds, translate_with};
    use datafusion::logical_expr::Expr;
    use datafusion::prelude::{col, lit};
    use std::collections::HashMap;

    #[test]
    fn test_pushdown_policy_parse() {
        assert_eq!(PushdownPolicy::parse("always"), Ok(PushdownPolicy::Always));
        assert_eq!(PushdownPolicy::parse("never"), Ok(PushdownPolicy::Never));
        assert_eq!(PushdownPolicy::parse("NEVER"), Ok(PushdownPolicy::Never));
        assert_eq!(
            PushdownPolicy::parse(" Cost_Based "),
            Ok(PushdownPolicy::CostBased)
        );
        assert_eq!(PushdownPolicy::parse("hinted"), Ok(PushdownPolicy::Hinted));
        assert_eq!(PushdownPolicy::parse("strict"), Ok(PushdownPolicy::Strict));
        assert_eq!(
            PushdownPolicy::parse("unknown"),
            Err(UnknownPushdownPolicy("unknown".to_string()))
        );
        assert!(PushdownPolicy::parse("").is_err());
    }

    /// Skewed fixture: `id` is highly selective *and* indexed, `status` has two values,
    /// `amount` is a float. The snapshots below pin the exact decision vector per policy, so
    /// any stats or model change that quietly disables pushdown shows up as a diff.
    fn skewed_stats() -> SourceStatistics {
        let mut columns = HashMap::new();
        for (name, n_distinct, width) in [
            ("id", 100_000.0, 8),
            ("status", 2.0, 10),
            ("amount", 50_000.0, 8),
            ("active", 2.0, 1),
            ("price", 50_000.0, 8),
        ] {
            columns.insert(
                name.to_string(),
                ColumnStats {
                    column_name: name.to_string(),
                    n_distinct,
                    null_frac: 0.0,
                    avg_width: width,
                    ..Default::default()
                },
            );
        }
        SourceStatistics {
            table_name: "orders".to_string(),
            row_count_estimate: 100_000.0,
            table_size_bytes: 10_000_000,
            columns,
            fetched_at: chrono::Utc::now(),
        }
    }

    fn skewed_kinds() -> ColumnKinds {
        ColumnKinds::from([
            ("id".to_string(), ColumnKind::Integer),
            (
                "status".to_string(),
                ColumnKind::Text {
                    bytewise_collation: false,
                },
            ),
            ("amount".to_string(), ColumnKind::Float),
            ("active".to_string(), ColumnKind::Boolean),
            ("price".to_string(), ColumnKind::Opaque),
            ("secret".to_string(), ColumnKind::Integer),
        ])
    }

    fn index(name: &str, column: &str) -> IndexInfo {
        IndexInfo {
            name: name.to_string(),
            columns: vec![column.to_string()],
            is_unique: false,
            is_primary: false,
            index_type: "btree".to_string(),
            is_partial: false,
            has_expressions: false,
        }
    }

    /// Compact snapshot form: "push:exact" / "push:inexact" / "keep" / "untranslatable".
    fn summarize(
        expr: &Expr,
        policy: PushdownPolicy,
        deny: &[String],
        push: &[String],
        inputs: &CostInputs<'_>,
    ) -> &'static str {
        let Some((fidelity, predicate)) = translate_with(expr, inputs.column_kinds) else {
            return "untranslatable";
        };
        match decide_translated(fidelity, predicate, policy, deny, push, inputs) {
            Decision::Push {
                fidelity: Fidelity::Exact,
                ..
            } => "push:exact",
            Decision::Push {
                fidelity: Fidelity::Inexact,
                ..
            } => "push:inexact",
            Decision::Keep => "keep",
        }
    }

    fn filters() -> Vec<Expr> {
        vec![
            col("id").eq(lit(42i64)),
            col("status").eq(lit("PAID")),
            col("amount").eq(lit(10.5f64)),
            col("amount").gt(lit(10.5f64)),
        ]
    }

    fn snapshot(
        policy: PushdownPolicy,
        deny: &[String],
        push: &[String],
        inputs: &CostInputs<'_>,
    ) -> Vec<&'static str> {
        filters()
            .iter()
            .map(|f| summarize(f, policy, deny, push, inputs))
            .collect()
    }

    #[test]
    fn test_policy_matrix_without_statistics_and_deny() {
        // Without statistics (a provider rebuilt from a serialized plan), cost_based and the
        // unhinted remainder of hinted keep — they never push blindly. Strict keeps too.
        let stats = SourceStatistics::empty("orders");
        let params = CostParams::default();
        let kinds = skewed_kinds();
        let inputs = CostInputs {
            stats: &stats,
            params: &params,
            indexes: &[],
            explain: None,
            column_kinds: &kinds,
            siblings: &[],
        };
        let expr = col("secret").eq(lit(100i64));
        let deny = vec!["SECRET".to_string()];
        let cases: Vec<(PushdownPolicy, &[String], &str)> = vec![
            (PushdownPolicy::Always, &[], "push:exact"),
            (PushdownPolicy::Always, &deny, "keep"),
            (PushdownPolicy::Never, &[], "keep"),
            (PushdownPolicy::CostBased, &[], "keep"),
            (PushdownPolicy::Hinted, &[], "keep"),
            (PushdownPolicy::Strict, &[], "keep"),
        ];
        for (policy, deny, expected) in cases {
            assert_eq!(
                summarize(&expr, policy, deny, &[], &inputs),
                expected,
                "policy={policy:?} deny={deny:?}"
            );
        }
        // Deny is case-insensitive in both directions.
        let deny = vec!["Email".to_string()];
        let kinds = ColumnKinds::from([("email".to_string(), ColumnKind::Integer)]);
        let inputs = CostInputs {
            column_kinds: &kinds,
            ..inputs
        };
        assert_eq!(
            summarize(
                &col("email").eq(lit(1i64)),
                PushdownPolicy::Always,
                &deny,
                &[],
                &inputs
            ),
            "keep"
        );
    }

    #[test]
    fn test_cost_based_differs_from_always() {
        let stats = skewed_stats();
        let indexes = vec![index("orders_pkey", "id")];
        let params = CostParams::default();
        let kinds = skewed_kinds();
        let inputs = CostInputs {
            stats: &stats,
            params: &params,
            indexes: &indexes,
            explain: None,
            column_kinds: &kinds,
            siblings: &[],
        };
        // `always` pushes everything translatable (text = is exact under binary collation,
        // float = is a superset, float > is not translatable); `cost_based` keeps the
        // low-value status predicate (selectivity 1/2) while still pushing the id lookup.
        assert_eq!(
            snapshot(PushdownPolicy::Always, &[], &[], &inputs),
            vec!["push:exact", "push:exact", "push:inexact", "untranslatable"],
        );
        assert_eq!(
            snapshot(PushdownPolicy::CostBased, &[], &[], &inputs),
            vec!["push:exact", "keep", "push:inexact", "untranslatable"],
        );
    }

    #[test]
    fn test_hinted_overrides_cost_model_but_not_deny() {
        let stats = skewed_stats();
        let indexes = vec![index("orders_pkey", "id")];
        let params = CostParams::default();
        let kinds = skewed_kinds();
        let inputs = CostInputs {
            stats: &stats,
            params: &params,
            indexes: &indexes,
            explain: None,
            column_kinds: &kinds,
            siblings: &[],
        };
        let push = vec!["status".to_string()];
        assert_eq!(
            snapshot(PushdownPolicy::Hinted, &[], &push, &inputs),
            vec!["push:exact", "push:exact", "push:inexact", "untranslatable"],
        );
        // Deny wins over push.
        let deny = vec!["status".to_string()];
        assert_eq!(
            snapshot(PushdownPolicy::Hinted, &deny, &push, &inputs),
            vec!["push:exact", "keep", "push:inexact", "untranslatable"],
        );
    }

    #[test]
    fn test_strict_pushes_only_indexed_selective_primitive() {
        let stats = skewed_stats();
        let indexes = vec![
            index("orders_pkey", "id"),
            index("orders_active_idx", "active"),
            index("orders_price_idx", "price"),
            index("orders_amount_idx", "amount"),
        ];
        let params = CostParams::default();
        let kinds = skewed_kinds();
        let inputs = CostInputs {
            stats: &stats,
            params: &params,
            indexes: &indexes,
            explain: None,
            column_kinds: &kinds,
            siblings: &[],
        };
        let filters = [
            col("id").eq(lit(42i64)),       // indexed + bigint + selective → push
            col("active").eq(lit(true)),    // indexed + boolean but 1/2 selectivity → keep
            col("status").eq(lit("PAID")),  // text and unindexed → keep
            col("amount").eq(lit(10.5f64)), // indexed float → keep (non-primitive)
            col("price").is_null(),         // indexed but opaque type → keep
        ];
        let got: Vec<&str> = filters
            .iter()
            .map(|f| summarize(f, PushdownPolicy::Strict, &[], &[], &inputs))
            .collect();
        assert_eq!(got, vec!["push:exact", "keep", "keep", "keep", "keep"]);

        // Strict keeps the translated fidelity: an Inexact translation stays Inexact.
        let (fidelity, predicate) = translate_with(&col("amount").eq(lit(1.0f64)), &kinds).unwrap();
        assert_eq!(fidelity, Fidelity::Inexact);
        let (decision, reason) = decide_explained(
            fidelity,
            predicate,
            PushdownPolicy::Strict,
            &[],
            &[],
            &inputs,
        );
        assert!(matches!(decision, Decision::Keep));
        assert!(reason.contains("non-primitive"), "{reason}");

        // Gate reasons name the failing gate.
        let (f, p) = translate_with(&col("status").eq(lit("PAID")), &kinds).unwrap();
        let (_, reason) = decide_explained(f, p, PushdownPolicy::Strict, &[], &[], &inputs);
        assert!(reason.contains("indexed"), "{reason}");
    }

    #[test]
    fn test_strict_fidelity_is_the_translated_one() {
        // A strict push never upgrades or downgrades fidelity: `id = 42 AND active` is Exact.
        let stats = skewed_stats();
        let indexes = vec![index("orders_pkey", "id"), index("a", "active")];
        let params = CostParams::default();
        let kinds = skewed_kinds();
        let inputs = CostInputs {
            stats: &stats,
            params: &params,
            indexes: &indexes,
            explain: None,
            column_kinds: &kinds,
            siblings: &[],
        };
        let expr = col("id").eq(lit(42i64)).and(col("active").eq(lit(true)));
        let (fidelity, predicate) = translate_with(&expr, &kinds).unwrap();
        match decide_translated(
            fidelity,
            predicate,
            PushdownPolicy::Strict,
            &[],
            &[],
            &inputs,
        ) {
            Decision::Push { fidelity: f, .. } => assert_eq!(f, fidelity),
            Decision::Keep => panic!("expected strict to push the selective indexed AND"),
        }
    }
}
