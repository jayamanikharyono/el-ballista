//! Source-aware pushdown: expression translation, fidelity rules, and policy.
//! pushdown/mod.rs
//! A bounded implementation of docs/pushdown.md: the allowlist-based expression translator
//! (§2) and the `Exact`/`Inexact` fidelity rules for the hazards in §3 that can be judged from
//! the expression and literal alone, plus the policy modes from §4.3 (`always` / `never` /
//! `cost_based`).
//!
//! **Not implemented**, and important to be honest about: the real cost model (§4). This module
//! has no access to `pg_stats`, `EXPLAIN`, or table statistics — `PushdownPolicy::CostBased`
//! here just means "push everything not on the denylist," which is `always` with a denylist,
//! not a genuine bytes-saved-vs-source-cost calculation. `hinted` policy, per-column collation
//! lookups (so `Inexact` is used for *every* string comparison rather than only non-`C`
//! collations, per §3.1), `IN`/`BETWEEN`/`LIKE`/arithmetic translation, and aggregate/join
//! pushdown are all deferred too, matching docs/roadmap.md's Phase 2/3 split.
//!
//! The guiding rule for every judgment call in this file: docs/roadmap.md's "What would make
//! this project fail" names "pushdown that is fast and wrong" as the fatal failure mode. So
//! wherever this module is unsure whether a translation preserves Arrow semantics, it chooses
//! `Inexact` or refuses to translate at all (`None`) — never `Exact`. Getting the conservative
//! case wrong costs performance; getting the `Exact` case wrong costs correctness.

pub mod dialect;
pub mod stats;
pub mod cost_model;
pub mod explain;
pub mod optimizer_rule;

use chrono::{DateTime, Utc};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use sqlx::{Postgres, QueryBuilder};

/// How faithfully a pushed predicate reproduces Arrow semantics — docs/pushdown.md §1.
/// `Unsupported` isn't a variant here: an expression this module can't translate simply isn't
/// represented (`translate` returns `None`), and the caller reports `Unsupported` to DataFusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fidelity {
    Exact,
    Inexact,
}

impl Fidelity {
    /// "Fidelity is the minimum of the children's" — docs/pushdown.md §2, `AND`/`OR` row.
    fn combine(self, other: Fidelity) -> Fidelity {
        if self == Fidelity::Inexact || other == Fidelity::Inexact {
            Fidelity::Inexact
        } else {
            Fidelity::Exact
        }
    }
}

/// The push/keep policy — docs/pushdown.md §4.3. `Hinted` (per-column overrides) isn't
/// implemented; a config naming it falls back to `CostBased`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushdownPolicy {
    Always,
    Never,
    CostBased,
}

impl PushdownPolicy {
    pub fn parse(raw: &str) -> PushdownPolicy {
        match raw {
            "always" => PushdownPolicy::Always,
            "never" => PushdownPolicy::Never,
            _ => PushdownPolicy::CostBased,
        }
    }
}

/// A translated predicate, in a form that can be rendered into a `QueryBuilder` with bound
/// parameters — never as an interpolated string. Deliberately not raw SQL text: `QueryBuilder`
/// generates its own `$N` placeholders as `push_bind` is called, so the tree has to be walked at
/// render time rather than assembled into a string upfront.
#[derive(Debug, Clone)]
pub enum Predicate {
    Column(String),
    Literal(Literal),
    Cmp {
        left: Box<Predicate>,
        op: &'static str,
        right: Box<Predicate>,
    },
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    IsNull(Box<Predicate>),
    IsNotNull(Box<Predicate>),
}

#[derive(Debug, Clone)]
pub enum Literal {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Timestamp(DateTime<Utc>),
}

impl Predicate {
    pub fn render(&self, query: &mut QueryBuilder<Postgres>) {
        match self {
            Predicate::Column(name) => {
                query.push("\"");
                query.push(name.replace('"', "\"\""));
                query.push("\"");
            }
            Predicate::Literal(lit) => lit.bind(query),
            Predicate::Cmp { left, op, right } => {
                query.push("(");
                left.render(query);
                query.push(format!(" {op} "));
                right.render(query);
                query.push(")");
            }
            Predicate::And(l, r) => {
                query.push("(");
                l.render(query);
                query.push(" AND ");
                r.render(query);
                query.push(")");
            }
            Predicate::Or(l, r) => {
                query.push("(");
                l.render(query);
                query.push(" OR ");
                r.render(query);
                query.push(")");
            }
            Predicate::Not(p) => {
                query.push("NOT (");
                p.render(query);
                query.push(")");
            }
            Predicate::IsNull(p) => {
                p.render(query);
                query.push(" IS NULL");
            }
            Predicate::IsNotNull(p) => {
                p.render(query);
                query.push(" IS NOT NULL");
            }
        }
    }
}

impl Literal {
    fn bind(&self, query: &mut QueryBuilder<Postgres>) {
        match self {
            Literal::Bool(v) => {
                query.push_bind(*v);
            }
            Literal::Int(v) => {
                query.push_bind(*v);
            }
            Literal::Float(v) => {
                query.push_bind(*v);
            }
            Literal::Text(v) => {
                query.push_bind(v.clone());
            }
            Literal::Timestamp(v) => {
                query.push_bind(*v);
            }
        }
    }

    /// docs/pushdown.md §3.1 / §3.3 — a string comparison defaults to `Inexact` because this
    /// module has no collation metadata to prove otherwise (the real rule only downgrades
    /// non-`C`/non-deterministic collations; treating every string as if it might be one is the
    /// conservative stand-in). A float comparison is `Inexact` because Postgres orders `NaN` as
    /// greater than everything and IEEE-754 does not. Everything else translated here
    /// (bool/int/timestamp) is `Exact`.
    fn fidelity(&self) -> Fidelity {
        match self {
            Literal::Text(_) | Literal::Float(_) => Fidelity::Inexact,
            Literal::Bool(_) | Literal::Int(_) | Literal::Timestamp(_) => Fidelity::Exact,
        }
    }
}

/// Translates a DataFusion `Expr` into a `Predicate` with its fidelity, or `None` if the
/// expression isn't in the allowlist (§2) — arithmetic, `LIKE`, `IN`, `BETWEEN`, `CAST`,
/// UDFs, and anything else not matched below stays in Arrow, which is always safe, just
/// possibly less efficient.
pub fn translate(expr: &Expr) -> Option<(Fidelity, Predicate)> {
    match expr {
        Expr::Column(col) => Some((Fidelity::Exact, Predicate::Column(col.name.clone()))),

        Expr::Literal(value, ..) => {
            let literal = translate_literal(value)?;
            let fidelity = literal.fidelity();
            Some((fidelity, Predicate::Literal(literal)))
        }

        Expr::Not(inner) => {
            let (fidelity, predicate) = translate(inner)?;
            Some((fidelity, Predicate::Not(Box::new(predicate))))
        }

        Expr::IsNull(inner) => {
            let (fidelity, predicate) = translate(inner)?;
            Some((fidelity, Predicate::IsNull(Box::new(predicate))))
        }

        Expr::IsNotNull(inner) => {
            let (fidelity, predicate) = translate(inner)?;
            Some((fidelity, Predicate::IsNotNull(Box::new(predicate))))
        }

        Expr::BinaryExpr(binary) => translate_binary(binary),

        _ => None,
    }
}

fn translate_binary(binary: &BinaryExpr) -> Option<(Fidelity, Predicate)> {
    let left = &binary.left;
    let right = &binary.right;

    match binary.op {
        Operator::And => {
            let (fl, pl) = translate(left)?;
            let (fr, pr) = translate(right)?;
            Some((fl.combine(fr), Predicate::And(Box::new(pl), Box::new(pr))))
        }

        Operator::Or => {
            let (fl, pl) = translate(left)?;
            let (fr, pr) = translate(right)?;
            Some((fl.combine(fr), Predicate::Or(Box::new(pl), Box::new(pr))))
        }

        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq => {
            let (_, pl) = translate(left)?;
            let (_, pr) = translate(right)?;

            let sql_op = match binary.op {
                Operator::Eq => "=",
                Operator::NotEq => "<>",
                Operator::Lt => "<",
                Operator::LtEq => "<=",
                Operator::Gt => ">",
                Operator::GtEq => ">=",
                _ => unreachable!(),
            };

            let fidelity = comparison_fidelity(&pl, &pr);

            Some((
                fidelity,
                Predicate::Cmp {
                    left: Box::new(pl),
                    op: sql_op,
                    right: Box::new(pr),
                },
            ))
        }

        // Arithmetic, string concatenation, bitwise ops, etc. — deliberately not translated.
        // docs/pushdown.md §3.4: arithmetic overflow/division semantics diverge between Arrow
        // and the source, so this stays `None` (kept in Arrow) rather than guessing `Inexact`.
        _ => None,
    }
}

/// Comparison fidelity from the resolved operands, since this module has no catalog access to
/// check column types/collation directly. A column-to-column comparison (or anything else this
/// doesn't recognize) is conservatively `Inexact` — there's nothing here to prove it safe.
fn comparison_fidelity(left: &Predicate, right: &Predicate) -> Fidelity {
    match (left, right) {
        (Predicate::Literal(lit), Predicate::Column(_))
        | (Predicate::Column(_), Predicate::Literal(lit)) => lit.fidelity(),
        (Predicate::Literal(a), Predicate::Literal(b)) => a.fidelity().combine(b.fidelity()),
        _ => Fidelity::Inexact,
    }
}

fn translate_literal(value: &ScalarValue) -> Option<Literal> {
    match value {
        ScalarValue::Boolean(Some(v)) => Some(Literal::Bool(*v)),
        ScalarValue::Int8(Some(v)) => Some(Literal::Int(*v as i64)),
        ScalarValue::Int16(Some(v)) => Some(Literal::Int(*v as i64)),
        ScalarValue::Int32(Some(v)) => Some(Literal::Int(*v as i64)),
        ScalarValue::Int64(Some(v)) => Some(Literal::Int(*v)),
        ScalarValue::UInt8(Some(v)) => Some(Literal::Int(*v as i64)),
        ScalarValue::UInt16(Some(v)) => Some(Literal::Int(*v as i64)),
        ScalarValue::UInt32(Some(v)) => Some(Literal::Int(*v as i64)),
        ScalarValue::Float32(Some(v)) => Some(Literal::Float(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(Literal::Float(*v)),
        ScalarValue::Utf8(Some(v)) => Some(Literal::Text(v.clone())),
        ScalarValue::LargeUtf8(Some(v)) => Some(Literal::Text(v.clone())),
        ScalarValue::TimestampMicrosecond(Some(v), ..) => {
            DateTime::<Utc>::from_timestamp_micros(*v).map(Literal::Timestamp)
        }
        // NULL literals and every other ScalarValue variant (Decimal128, Binary, Date32, lists,
        // structs, ...) aren't translated — comparisons against them stay in Arrow.
        _ => None,
    }
}

/// The outcome of deciding whether one filter should be pushed, combining `translate`'s
/// correctness judgment with the configured policy and denylist. This is the function
/// `supports_filters_pushdown` and `scan` both call, so the two can never disagree about which
/// filters are pushable.
pub enum Decision {
    Push { fidelity: Fidelity, predicate: Predicate },
    Keep,
}

pub fn decide(expr: &Expr, policy: PushdownPolicy, deny: &[String]) -> Decision {
    if policy == PushdownPolicy::Never {
        return Decision::Keep;
    }

    let Some((fidelity, predicate)) = translate(expr) else {
        return Decision::Keep;
    };

    if references_denied_column(&predicate, deny) {
        return Decision::Keep;
    }

    Decision::Push { fidelity, predicate }
}

fn references_denied_column(predicate: &Predicate, deny: &[String]) -> bool {
    match predicate {
        Predicate::Column(name) => deny.iter().any(|d| d == name),
        Predicate::Literal(_) => false,
        Predicate::Cmp { left, right, .. } => {
            references_denied_column(left, deny) || references_denied_column(right, deny)
        }
        Predicate::And(l, r) | Predicate::Or(l, r) => {
            references_denied_column(l, deny) || references_denied_column(r, deny)
        }
        Predicate::Not(p) | Predicate::IsNull(p) | Predicate::IsNotNull(p) => {
            references_denied_column(p, deny)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    #[test]
    fn test_pushdown_policy_parse() {
        assert_eq!(PushdownPolicy::parse("always"), PushdownPolicy::Always);
        assert_eq!(PushdownPolicy::parse("never"), PushdownPolicy::Never);
        assert_eq!(PushdownPolicy::parse("cost_based"), PushdownPolicy::CostBased);
        assert_eq!(PushdownPolicy::parse("unknown"), PushdownPolicy::CostBased);
    }

    #[test]
    fn test_fidelity_combine() {
        assert_eq!(Fidelity::Exact.combine(Fidelity::Exact), Fidelity::Exact);
        assert_eq!(Fidelity::Exact.combine(Fidelity::Inexact), Fidelity::Inexact);
        assert_eq!(Fidelity::Inexact.combine(Fidelity::Exact), Fidelity::Inexact);
        assert_eq!(Fidelity::Inexact.combine(Fidelity::Inexact), Fidelity::Inexact);
    }

    #[test]
    fn test_translate_comparisons_and_fidelities() {
        // Int comparison -> Exact
        let int_expr = col("id").eq(lit(42i64));
        let (f, _) = translate(&int_expr).unwrap();
        assert_eq!(f, Fidelity::Exact);

        // String comparison -> Inexact (due to potential collation differences)
        let str_expr = col("status").eq(lit("PAID"));
        let (f, _) = translate(&str_expr).unwrap();
        assert_eq!(f, Fidelity::Inexact);

        // Float comparison -> Inexact (due to NaN differences)
        let float_expr = col("amount").gt(lit(10.5f64));
        let (f, _) = translate(&float_expr).unwrap();
        assert_eq!(f, Fidelity::Inexact);

        // AND combines fidelities
        let and_expr = col("id").eq(lit(1i64)).and(col("status").eq(lit("PAID")));
        let (f, _) = translate(&and_expr).unwrap();
        assert_eq!(f, Fidelity::Inexact);

        let both_exact = col("id").eq(lit(1i64)).and(col("active").eq(lit(true)));
        let (f, _) = translate(&both_exact).unwrap();
        assert_eq!(f, Fidelity::Exact);
    }

    #[test]
    fn test_decide_policy_and_denylist() {
        let expr = col("secret").eq(lit(100i64));

        // Policy::Never -> Keep
        assert!(matches!(decide(&expr, PushdownPolicy::Never, &[]), Decision::Keep));

        // Policy::Always -> Push
        assert!(matches!(decide(&expr, PushdownPolicy::Always, &[]), Decision::Push { .. }));

        // Denylist blocks
        let deny = vec!["secret".to_string()];
        assert!(matches!(decide(&expr, PushdownPolicy::Always, &deny), Decision::Keep));
    }
}


#[cfg(test)]
mod tests_phase25;
