//! DataFusion `Expr` → [`Predicate`] translation with per-node fidelity.
//! pushdown/translate.rs
//!
//! The contract with DataFusion (docs/pushdown.md §1):
//! - `Exact`: the pushed SQL returns *exactly* the rows Arrow would keep. DataFusion drops its
//!   own filter.
//! - `Inexact`: the pushed SQL returns a **superset** of those rows; DataFusion re-checks and
//!   removes the extras. A re-check can remove rows, never add them back — so a translation
//!   whose source semantics could return *fewer* rows than Arrow is not `Inexact`, it is not
//!   pushable at all (`None`).
//!
//! Composition rules that follow from that:
//! - `AND`/`OR`: fidelity is the minimum of the children (superset ∘ superset = superset).
//! - `NOT`, `IS [NOT] NULL` over a composite child: the child must be `Exact` (the negation of
//!   a superset is a subset).
//!
//! Comparisons are judged from the *column's* [`ColumnKind`], which the connector derives from
//! its catalog (the Postgres mapping is in `connector::postgres::dialect`). Anything this module
//! does not recognize stays in Arrow, which is always correct.

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

use crate::pushdown::{CastType, CmpOp, Collation, Fidelity, Literal, Predicate};

/// How a source column compares, in engine-neutral terms. Assigned by the connector from its
/// catalog; translation decides fidelity from this alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColumnKind {
    /// Boolean: comparisons against bool literals are `Exact`.
    Boolean,
    /// Fixed-width integer: comparisons against integer literals are `Exact`.
    Integer,
    /// Timestamp (with or without zone; sessions run in UTC): `Exact` against timestamp literals.
    Timestamp,
    /// Calendar date: `Exact` against date literals (and against another date column). A
    /// comparison that DataFusion had to wrap in a cast (a date column against a timestamp,
    /// say) is not a plain column comparison, so it stays in Arrow.
    Date,
    /// Binary floating point. `=` is `Inexact` (the source treats `-0 = 0`, a superset of
    /// Arrow's total-order equality); every other comparison is not pushable.
    Float,
    /// Collatable text. Compared under [`Collation::Binary`], which equals Arrow's byte order,
    /// so every comparison is `Exact`. `bytewise_collation` is true when the column's own
    /// collation already is byte-wise (Postgres `C`/`POSIX`): only then can a plain index on it
    /// serve the binary-collated comparison.
    Text { bytewise_collation: bool },
    /// Enumerated type whose Arrow value is its label: compared as
    /// `CAST(col AS text) COLLATE <binary>`, so every comparison is `Exact`.
    Label,
    /// A non-string type the extractor emits as its canonical text form (uuid, json, a
    /// composite row, ...): only `=` / `<>` push, as `CAST(col AS text) COLLATE <binary>`,
    /// `Exact`, and `IS [NOT] NULL` tests `CAST(col AS text)`. Ordering comparisons are not
    /// pushable.
    TextCast,
    /// Anything else: only `IS [NOT] NULL` pushes.
    Opaque,
}

/// Column name → kind, built by the connector.
pub type ColumnKinds = HashMap<String, ColumnKind>;

/// Where column kinds come from.
#[derive(Clone, Copy)]
enum Schema<'a> {
    /// No catalog: infer a column's kind from the literal it is compared with. Only for
    /// callers without a schema (tests, diagnostics); providers use [`translate_with`].
    Inferred,
    Known(&'a ColumnKinds),
}

/// Translate with column kinds inferred from the literals they are compared against (a text
/// literal implies a collatable text column, and so on). Providers with a catalog must use
/// [`translate_with`], which judges comparisons by the real column type.
///
/// # Examples
/// ```
/// use datafusion::prelude::{col, lit};
/// use el_ballista::pushdown::{Fidelity, translate};
/// let (fidelity, _) = translate(&col("id").eq(lit(1i64))).unwrap();
/// assert_eq!(fidelity, Fidelity::Exact);
/// assert!(translate(&(col("a") + col("b"))).is_none());
/// ```
pub fn translate(expr: &Expr) -> Option<(Fidelity, Predicate)> {
    translate_predicate(expr, Schema::Inferred)
}

/// Translate a filter using the connector's column kinds. Returns `None` when the filter has
/// no source form that is `Exact` or a superset (`Inexact`) of Arrow's answer; unknown columns
/// are never pushed.
///
/// # Examples
/// ```
/// use datafusion::prelude::{col, lit};
/// use el_ballista::pushdown::{ColumnKind, ColumnKinds, translate_with};
/// let kinds = ColumnKinds::from([("x".to_string(), ColumnKind::Float)]);
/// assert!(translate_with(&col("x").lt(lit(0.0f64)), &kinds).is_none());
/// ```
pub fn translate_with(expr: &Expr, kinds: &ColumnKinds) -> Option<(Fidelity, Predicate)> {
    translate_predicate(expr, Schema::Known(kinds))
}

/// Translate an expression in boolean (filter) position.
fn translate_predicate(expr: &Expr, schema: Schema<'_>) -> Option<(Fidelity, Predicate)> {
    match expr {
        // A bare boolean column used as a filter.
        Expr::Column(col) => {
            let accepted = match schema {
                Schema::Inferred => true,
                Schema::Known(kinds) => kinds.get(&col.name) == Some(&ColumnKind::Boolean),
            };
            accepted.then(|| (Fidelity::Exact, Predicate::Column(col.name.clone())))
        }

        Expr::Literal(ScalarValue::Boolean(Some(v)), ..) => {
            Some((Fidelity::Exact, Predicate::Literal(Literal::Bool(*v))))
        }

        Expr::Not(inner) => {
            let predicate = exact_predicate(inner, schema)?;
            Some((Fidelity::Exact, Predicate::Not(Box::new(predicate))))
        }

        Expr::IsNull(inner) => {
            let operand = null_test_operand(inner, schema)?;
            Some((Fidelity::Exact, Predicate::IsNull(Box::new(operand))))
        }

        Expr::IsNotNull(inner) => {
            let operand = null_test_operand(inner, schema)?;
            Some((Fidelity::Exact, Predicate::IsNotNull(Box::new(operand))))
        }

        Expr::BinaryExpr(binary) => translate_binary(binary, schema),

        _ => None,
    }
}

/// A child that must be `Exact` (under `NOT` / `IS [NOT] NULL`): negating a superset gives a
/// subset, which DataFusion's re-check cannot repair.
fn exact_predicate(expr: &Expr, schema: Schema<'_>) -> Option<Predicate> {
    match translate_predicate(expr, schema)? {
        (Fidelity::Exact, predicate) => Some(predicate),
        (Fidelity::Inexact, _) => None,
    }
}

/// Operand of `IS [NOT] NULL`: any known column, or an `Exact` boolean predicate.
///
/// A [`ColumnKind::TextCast`] column is tested through its text form, the value Arrow holds:
/// the column itself can be row-valued (a Postgres composite type), and SQL's row `IS NULL` is
/// true when every *field* is NULL, while the text form `(,)` of such a row is not NULL. Every
/// other kind is a scalar, whose nullness does not depend on its type.
fn null_test_operand(expr: &Expr, schema: Schema<'_>) -> Option<Predicate> {
    if let Expr::Column(col) = expr {
        let column = || Predicate::Column(col.name.clone());
        return match schema {
            Schema::Inferred => Some(column()),
            Schema::Known(kinds) => match kinds.get(&col.name)? {
                ColumnKind::TextCast => Some(as_text(column())),
                _ => Some(column()),
            },
        };
    }
    exact_predicate(expr, schema)
}

fn translate_binary(binary: &BinaryExpr, schema: Schema<'_>) -> Option<(Fidelity, Predicate)> {
    let op = match binary.op {
        Operator::And | Operator::Or => {
            let (fl, pl) = translate_predicate(&binary.left, schema)?;
            let (fr, pr) = translate_predicate(&binary.right, schema)?;
            let (l, r) = (Box::new(pl), Box::new(pr));
            let predicate = if binary.op == Operator::And {
                Predicate::And(l, r)
            } else {
                Predicate::Or(l, r)
            };
            return Some((fl.combine(fr), predicate));
        }
        Operator::Eq => CmpOp::Eq,
        Operator::NotEq => CmpOp::NotEq,
        Operator::Lt => CmpOp::Lt,
        Operator::LtEq => CmpOp::LtEq,
        Operator::Gt => CmpOp::Gt,
        Operator::GtEq => CmpOp::GtEq,
        // Arithmetic, string concatenation, bitwise ops, etc. — deliberately not translated.
        // docs/pushdown.md §3.4: arithmetic overflow/division semantics diverge between Arrow
        // and the source.
        _ => return None,
    };
    translate_comparison(&binary.left, op, &binary.right, schema)
}

enum Operand<'e> {
    Column(&'e str),
    Literal(Literal),
}

fn operand(expr: &Expr) -> Option<Operand<'_>> {
    match expr {
        Expr::Column(col) => Some(Operand::Column(col.name.as_str())),
        Expr::Literal(value, ..) => translate_literal(value).map(Operand::Literal),
        _ => None,
    }
}

fn translate_comparison(
    left: &Expr,
    op: CmpOp,
    right: &Expr,
    schema: Schema<'_>,
) -> Option<(Fidelity, Predicate)> {
    let cmp = |l: Predicate, r: Predicate| Predicate::Cmp {
        left: Box::new(l),
        op,
        right: Box::new(r),
    };
    match (operand(left)?, operand(right)?) {
        (Operand::Column(c), Operand::Literal(lit)) => {
            let (fidelity, col_side) = column_vs_literal(c, op, &lit, schema)?;
            Some((fidelity, cmp(col_side, Predicate::Literal(lit))))
        }
        (Operand::Literal(lit), Operand::Column(c)) => {
            let (fidelity, col_side) = column_vs_literal(c, op, &lit, schema)?;
            Some((fidelity, cmp(Predicate::Literal(lit), col_side)))
        }
        (Operand::Column(a), Operand::Column(b)) => {
            let Schema::Known(kinds) = schema else {
                // No catalog: nothing proves the two columns compare alike.
                return None;
            };
            let (ka, kb) = (kinds.get(a)?, kinds.get(b)?);
            let (la, lb) = match (ka, kb) {
                (ColumnKind::Integer, ColumnKind::Integer)
                | (ColumnKind::Boolean, ColumnKind::Boolean)
                | (ColumnKind::Timestamp, ColumnKind::Timestamp)
                | (ColumnKind::Date, ColumnKind::Date) => (column(a), column(b)),
                (ColumnKind::Text { .. }, ColumnKind::Text { .. }) => {
                    (binary_collated(column(a)), binary_collated(column(b)))
                }
                _ => return None,
            };
            Some((Fidelity::Exact, cmp(la, lb)))
        }
        // Literal-vs-literal is constant-folded by DataFusion before it gets here.
        (Operand::Literal(_), Operand::Literal(_)) => None,
    }
}

fn column(name: &str) -> Predicate {
    Predicate::Column(name.to_string())
}

fn binary_collated(expr: Predicate) -> Predicate {
    Predicate::Collate {
        expr: Box::new(expr),
        collation: Collation::Binary,
    }
}

fn as_text(expr: Predicate) -> Predicate {
    Predicate::Cast {
        expr: Box::new(expr),
        to_type: CastType::Text,
    }
}

/// The column side of a column-vs-literal comparison and its fidelity, or `None` when the
/// source cannot evaluate it as `Exact` or as a superset.
fn column_vs_literal(
    name: &str,
    op: CmpOp,
    literal: &Literal,
    schema: Schema<'_>,
) -> Option<(Fidelity, Predicate)> {
    let kind = match schema {
        Schema::Known(kinds) => *kinds.get(name)?,
        Schema::Inferred => inferred_kind(literal),
    };
    match (kind, literal) {
        (ColumnKind::Boolean, Literal::Bool(_))
        | (ColumnKind::Integer, Literal::Int(_))
        | (ColumnKind::Timestamp, Literal::Timestamp(_))
        | (ColumnKind::Date, Literal::Date(_)) => Some((Fidelity::Exact, column(name))),
        // The source says `-0 = 0` (and NaN = NaN, like Arrow): a superset for `=`. For every
        // other operator it can return fewer rows (`x < 0.0` drops -0.0), so keep in Arrow.
        (ColumnKind::Float, Literal::Float(_)) if op == CmpOp::Eq => {
            Some((Fidelity::Inexact, column(name)))
        }
        (ColumnKind::Text { .. }, Literal::Text(_)) => {
            Some((Fidelity::Exact, binary_collated(column(name))))
        }
        (ColumnKind::Label, Literal::Text(_)) => {
            Some((Fidelity::Exact, binary_collated(as_text(column(name)))))
        }
        (ColumnKind::TextCast, Literal::Text(_)) if op.is_equality() => {
            Some((Fidelity::Exact, binary_collated(as_text(column(name)))))
        }
        _ => None,
    }
}

fn inferred_kind(literal: &Literal) -> ColumnKind {
    match literal {
        Literal::Bool(_) => ColumnKind::Boolean,
        Literal::Int(_) => ColumnKind::Integer,
        Literal::Float(_) => ColumnKind::Float,
        Literal::Text(_) => ColumnKind::Text {
            bytewise_collation: false,
        },
        Literal::Timestamp(_) => ColumnKind::Timestamp,
        Literal::Date(_) => ColumnKind::Date,
    }
}

fn translate_literal(value: &ScalarValue) -> Option<Literal> {
    match value {
        ScalarValue::Boolean(Some(v)) => Some(Literal::Bool(*v)),
        ScalarValue::Int8(Some(v)) => Some(Literal::Int(i64::from(*v))),
        ScalarValue::Int16(Some(v)) => Some(Literal::Int(i64::from(*v))),
        ScalarValue::Int32(Some(v)) => Some(Literal::Int(i64::from(*v))),
        ScalarValue::Int64(Some(v)) => Some(Literal::Int(*v)),
        ScalarValue::UInt8(Some(v)) => Some(Literal::Int(i64::from(*v))),
        ScalarValue::UInt16(Some(v)) => Some(Literal::Int(i64::from(*v))),
        ScalarValue::UInt32(Some(v)) => Some(Literal::Int(i64::from(*v))),
        ScalarValue::Float32(Some(v)) => Some(Literal::Float(f64::from(*v))),
        ScalarValue::Float64(Some(v)) => Some(Literal::Float(*v)),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Some(Literal::Text(v.clone())),
        ScalarValue::TimestampMicrosecond(Some(v), ..) => {
            DateTime::<Utc>::from_timestamp_micros(*v).map(Literal::Timestamp)
        }
        ScalarValue::Date32(Some(days)) => date_literal(*days),
        // NULL literals and every other ScalarValue variant (Decimal128, Binary, Date64, lists,
        // structs, ...) aren't translated — comparisons against them stay in Arrow.
        _ => None,
    }
}

/// A `Date32` value (days since 1970-01-01) as a date literal, limited to years 1..=9999.
/// Every such date exists in Postgres (whose `date` spans 4713 BC to 5874897 AD) and renders
/// as a plain `YYYY-MM-DD`; anything outside that range stays in Arrow rather than risk a
/// source-side range error or a mis-rendered BC date.
fn date_literal(days: i32) -> Option<Literal> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let date = epoch.checked_add_signed(chrono::Duration::days(i64::from(days)))?;
    (1..=9999)
        .contains(&chrono::Datelike::year(&date))
        .then_some(Literal::Date(date))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pushdown::ir::test_support::render;
    use datafusion::prelude::{col, lit};

    fn kinds() -> ColumnKinds {
        ColumnKinds::from([
            ("id".to_string(), ColumnKind::Integer),
            ("id2".to_string(), ColumnKind::Integer),
            ("flag".to_string(), ColumnKind::Boolean),
            ("ts".to_string(), ColumnKind::Timestamp),
            ("d".to_string(), ColumnKind::Date),
            ("x".to_string(), ColumnKind::Float),
            (
                "name".to_string(),
                ColumnKind::Text {
                    bytewise_collation: false,
                },
            ),
            ("m".to_string(), ColumnKind::Label),
            ("u".to_string(), ColumnKind::TextCast),
            ("blob".to_string(), ColumnKind::Opaque),
        ])
    }

    /// `(fidelity, sql)` or `None`, translated with the fixture kinds.
    fn t(expr: Expr) -> Option<(Fidelity, String)> {
        translate_with(&expr, &kinds()).map(|(f, p)| (f, render(&p).0))
    }

    fn exact(sql: &str) -> Option<(Fidelity, String)> {
        Some((Fidelity::Exact, sql.to_string()))
    }

    #[test]
    fn test_primitive_comparisons_are_exact() {
        assert_eq!(t(col("id").eq(lit(42i64))), exact(r#"("id" = $1)"#));
        assert_eq!(t(col("id").not_eq(lit(42i64))), exact(r#"("id" <> $1)"#));
        assert_eq!(t(lit(42i64).lt(col("id"))), exact(r#"($1 < "id")"#));
        assert_eq!(t(col("flag").eq(lit(true))), exact(r#"("flag" = $1)"#));
        let ts = ScalarValue::TimestampMicrosecond(Some(1_700_000_000_000_000), None);
        assert_eq!(t(col("ts").gt(lit(ts))), exact(r#"("ts" > $1)"#));
        // Type mismatch between column kind and literal: never guessed.
        assert_eq!(t(col("id").eq(lit("1"))), None);
        assert_eq!(t(col("d").eq(lit(1i64))), None);
        // A timestamp literal against a date column is not a date comparison.
        assert_eq!(t(col("d").gt(lit(ts_literal()))), None);
        assert_eq!(t(col("blob").eq(lit("x"))), None);
        // Unknown columns never push.
        assert_eq!(t(col("nope").eq(lit(1i64))), None);
    }

    #[test]
    fn test_text_comparisons_are_binary_collated_and_exact() {
        // Every text operator compares byte-wise, like Arrow.
        for (expr, op) in [
            (col("name").eq(lit("a")), "="),
            (col("name").not_eq(lit("a")), "<>"),
            (col("name").lt(lit("a")), "<"),
            (col("name").lt_eq(lit("a")), "<="),
            (col("name").gt(lit("a")), ">"),
            (col("name").gt_eq(lit("a")), ">="),
        ] {
            assert_eq!(
                t(expr),
                exact(&format!(r#"(("name" COLLATE "C") {op} $1)"#))
            );
        }
        // Column vs column: both sides collated.
        assert_eq!(
            t(col("name").lt(col("name"))),
            exact(r#"(("name" COLLATE "C") < ("name" COLLATE "C"))"#)
        );
    }

    #[test]
    fn test_float_rules() {
        // `=` is a superset (-0 = 0 at the source): Inexact.
        assert_eq!(
            t(col("x").eq(lit(0.0f64))),
            Some((Fidelity::Inexact, r#"("x" = $1)"#.to_string()))
        );
        // Every other float comparison could lose -0.0 rows: not pushable.
        for expr in [
            col("x").lt(lit(0.0f64)),
            col("x").lt_eq(lit(0.0f64)),
            col("x").gt(lit(0.0f64)),
            col("x").gt_eq(lit(0.0f64)),
            col("x").not_eq(lit(0.0f64)),
            col("x").eq(col("x")),
        ] {
            assert_eq!(t(expr.clone()), None, "{expr}");
        }
    }

    #[test]
    fn test_not_requires_exact_child() {
        // NOT over Inexact (float =) would be a subset: not pushable.
        assert_eq!(t(Expr::Not(Box::new(col("x").eq(lit(0.0f64))))), None);
        assert_eq!(t(col("x").eq(lit(0.0f64)).is_null()), None);
        // NOT over an exact text comparison is fine (binary collation is exact).
        assert_eq!(
            t(Expr::Not(Box::new(col("name").eq(lit("foo"))))),
            exact(r#"(NOT (("name" COLLATE "C") = $1))"#)
        );
        // (NOT flag) IS NULL keeps its grouping.
        assert_eq!(
            t(Expr::IsNull(Box::new(Expr::Not(Box::new(col("flag")))))),
            exact(r#"((NOT "flag") IS NULL)"#)
        );
        // A bare non-boolean column is not a predicate.
        assert_eq!(t(Expr::Not(Box::new(col("id")))), None);
    }

    #[test]
    fn test_and_or_take_min_fidelity() {
        let (f, _) = translate_with(
            &col("id").eq(lit(1i64)).or(col("x").eq(lit(1.0f64))),
            &kinds(),
        )
        .unwrap();
        assert_eq!(f, Fidelity::Inexact);
        let (f, _) = translate_with(
            &col("id").eq(lit(1i64)).and(col("name").eq(lit("a"))),
            &kinds(),
        )
        .unwrap();
        assert_eq!(f, Fidelity::Exact);
        // One untranslatable side sinks the whole OR (it cannot be split).
        assert_eq!(
            t(col("id").eq(lit(1i64)).or(col("x").lt(lit(1.0f64)))),
            None
        );
    }

    #[test]
    fn test_null_tests_on_any_known_column() {
        assert_eq!(t(col("blob").is_null()), exact(r#"("blob" IS NULL)"#));
        assert_eq!(t(col("m").is_null()), exact(r#"("m" IS NULL)"#));
        assert_eq!(t(col("nope").is_null()), None);
    }

    #[test]
    fn test_null_tests_on_text_cast_columns_use_the_text_form() {
        // A text-cast column may be row-valued (a composite type), where `IS NULL` is true for
        // a row of NULL fields and `IS NOT NULL` false for a row with any NULL field. Arrow
        // holds the text form (`(,)`, `(1,)`): test that, in both directions and under NOT.
        assert_eq!(
            t(col("u").is_null()),
            exact(r#"(CAST("u" AS text) IS NULL)"#)
        );
        assert_eq!(
            t(col("u").is_not_null()),
            exact(r#"(CAST("u" AS text) IS NOT NULL)"#)
        );
        assert_eq!(
            t(Expr::Not(Box::new(col("u").is_null()))),
            exact(r#"(NOT (CAST("u" AS text) IS NULL))"#)
        );
    }

    fn ts_literal() -> ScalarValue {
        ScalarValue::TimestampMicrosecond(Some(1_700_000_000_000_000), None)
    }

    fn date32(y: i32, m: u32, d: u32) -> ScalarValue {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days = (NaiveDate::from_ymd_opt(y, m, d).unwrap() - epoch).num_days();
        ScalarValue::Date32(Some(i32::try_from(days).unwrap()))
    }

    #[test]
    fn test_date_comparisons_are_exact() {
        // Every operator, either operand order, as a plain column comparison (index-usable).
        assert_eq!(
            t(col("d").gt_eq(lit(date32(2026, 9, 27)))),
            exact(r#"("d" >= $1)"#)
        );
        assert_eq!(
            t(col("d").lt(lit(date32(2026, 9, 28)))),
            exact(r#"("d" < $1)"#)
        );
        assert_eq!(
            t(col("d").eq(lit(date32(2026, 1, 1)))),
            exact(r#"("d" = $1)"#)
        );
        assert_eq!(
            t(col("d").not_eq(lit(date32(2026, 1, 1)))),
            exact(r#"("d" <> $1)"#)
        );
        assert_eq!(
            t(lit(date32(2026, 1, 1)).lt_eq(col("d"))),
            exact(r#"($1 <= "d")"#)
        );
        // A daily window, NOT over it, and date-to-date column comparison.
        assert_eq!(
            t(col("d")
                .gt_eq(lit(date32(2026, 9, 27)))
                .and(col("d").lt(lit(date32(2026, 9, 28))))),
            exact(r#"(("d" >= $1) AND ("d" < $2))"#)
        );
        assert_eq!(
            t(Expr::Not(Box::new(col("d").eq(lit(date32(2026, 1, 1)))))),
            exact(r#"(NOT ("d" = $1))"#)
        );
        let kinds = ColumnKinds::from([
            ("d".to_string(), ColumnKind::Date),
            ("d2".to_string(), ColumnKind::Date),
        ]);
        let (f, p) = translate_with(&col("d").lt(col("d2")), &kinds).unwrap();
        assert_eq!(
            (f, render(&p).0),
            (Fidelity::Exact, r#"("d" < "d2")"#.into())
        );
        // The bound literal is the calendar date itself.
        let (_, p) = translate_with(&col("d").eq(lit(date32(2026, 9, 27))), &kinds).unwrap();
        let expected = NaiveDate::from_ymd_opt(2026, 9, 27).unwrap();
        assert_eq!(
            render(&p).1,
            vec![crate::pushdown::SqlParam::Date(expected)]
        );
        // Date literal against a non-date column, a cast column, NULL, out-of-range years:
        // all stay in Arrow.
        assert_eq!(t(col("ts").gt(lit(date32(2026, 1, 1)))), None);
        assert_eq!(t(col("id").gt(lit(date32(2026, 1, 1)))), None);
        assert_eq!(
            t(Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(col("d")),
                datafusion::arrow::datatypes::DataType::Utf8,
            ))
            .eq(lit("2026-01-01"))),
            None
        );
        assert_eq!(t(col("d").eq(lit(ScalarValue::Date32(None)))), None);
        assert_eq!(
            t(col("d").eq(lit(ScalarValue::Date32(Some(i32::MAX))))),
            None
        );
        assert_eq!(
            t(col("d").eq(lit(ScalarValue::Date32(Some(-800_000))))),
            None
        );
        assert!(matches!(
            date_literal(0),
            Some(Literal::Date(d)) if d == NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
        ));
    }

    #[test]
    fn test_label_and_text_cast_columns() {
        // Enum comparisons anywhere in the tree become label comparisons.
        assert_eq!(
            t(col("m").eq(lit("sad")).or(col("m").eq(lit("happy")))),
            exact(
                r#"(((CAST("m" AS text) COLLATE "C") = $1) OR ((CAST("m" AS text) COLLATE "C") = $2))"#
            )
        );
        assert_eq!(
            t(Expr::Not(Box::new(col("m").lt(lit("ok"))))),
            exact(r#"(NOT ((CAST("m" AS text) COLLATE "C") < $1))"#)
        );
        // Enum against a non-text literal has no source form.
        assert_eq!(t(col("m").eq(lit(1i64))), None);
        // uuid/json-like columns push only = / <> through their text form.
        assert_eq!(
            t(col("u").eq(lit("123e4567-e89b-12d3-a456-426614174000"))),
            exact(r#"((CAST("u" AS text) COLLATE "C") = $1)"#)
        );
        assert_eq!(
            t(col("u").not_eq(lit("x"))),
            exact(r#"((CAST("u" AS text) COLLATE "C") <> $1)"#)
        );
        assert_eq!(t(col("u").lt(lit("x"))), None);
    }

    #[test]
    fn test_outside_allowlist_stays_in_arrow() {
        assert!(translate(&(col("a") + col("b"))).is_none());
        assert!(translate(&col("x").like(lit("a%"))).is_none());
        assert!(translate(&col("x").eq(lit(ScalarValue::Int64(None)))).is_none());
        assert!(translate(&col("a").eq(col("b"))).is_none());
    }

    #[test]
    fn test_inferred_schema_matches_literal_kind() {
        let (f, p) = translate(&col("status").eq(lit("PAID"))).unwrap();
        assert_eq!(f, Fidelity::Exact);
        assert_eq!(render(&p).0, r#"(("status" COLLATE "C") = $1)"#);
        assert!(translate(&col("amount").gt(lit(10.5f64))).is_none());
    }
}
