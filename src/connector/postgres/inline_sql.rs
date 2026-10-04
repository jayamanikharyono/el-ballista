//! Postgres inline SQL rendering for `EXPLAIN`.
//!
//! [`PredicateInlineSql::render_inline`] renders a [`Predicate`] with literals inlined as
//! **Postgres** SQL text. It goes through the same [`Predicate::render_to`] as execution (so
//! parenthesization and casts are identical) with a sink that writes literals instead of binding
//! them. Used only for `EXPLAIN (FORMAT JSON)` cost estimation, cache keys, and `el-ballista plan`
//! output — never for a query that returns rows.

use crate::connector::postgres::dialect::PostgresDialect;
use crate::pushdown::{Predicate, SqlParam, SqlSink};

/// Render a predicate as inline Postgres SQL text (EXPLAIN / diagnostics only).
pub trait PredicateInlineSql {
    /// Inline Postgres SQL for this predicate.
    ///
    /// # Examples
    /// ```
    /// use datafusion::prelude::{col, lit};
    /// use el_ballista::connector::postgres::internals::PredicateInlineSql;
    /// use el_ballista::pushdown::translate;
    /// let (_, p) = translate(&col("id").eq(lit(7i64))).unwrap();
    /// assert_eq!(p.render_inline(), r#"("id" = 7)"#);
    /// ```
    fn render_inline(&self) -> String;
}

impl PredicateInlineSql for Predicate {
    fn render_inline(&self) -> String {
        let mut sink = InlineSink(String::new());
        self.render_to(&PostgresDialect, &mut sink);
        sink.0
    }
}

struct InlineSink(String);

impl SqlSink for InlineSink {
    fn push_sql(&mut self, sql: &str) {
        self.0.push_str(sql);
    }

    fn push_param(&mut self, param: SqlParam) {
        self.0.push_str(&render_param_inline(&param));
    }
}

fn render_param_inline(param: &SqlParam) -> String {
    match param {
        SqlParam::Bool(v) => v.to_string().to_uppercase(),
        SqlParam::Int(v) => v.to_string(),
        SqlParam::Float(v) => format!("{}::float8", quote_text(&v.to_string())),
        SqlParam::Text(v) => quote_text(v),
        SqlParam::Timestamp(v) => format!("{}::timestamptz", quote_text(&v.to_rfc3339())),
        SqlParam::Date(v) => format!("{}::date", quote_text(&v.format("%Y-%m-%d").to_string())),
    }
}

/// A Postgres string literal that means `value` regardless of `standard_conforming_strings`:
/// with a backslash present, an escape string `E'...'` (backslashes doubled — `E''` always
/// treats `\` as an escape); otherwise a plain `'...'`, which never interprets backslashes when
/// there are none. Quotes are doubled in both forms.
fn quote_text(value: &str) -> String {
    let quoted = value.replace('\'', "''");
    if value.contains('\\') {
        format!("E'{}'", quoted.replace('\\', "\\\\"))
    } else {
        format!("'{quoted}'")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pushdown::{CmpOp, Literal};

    fn text_eq(value: &str) -> Predicate {
        Predicate::Cmp {
            left: Box::new(Predicate::Column("status".to_string())),
            op: CmpOp::Eq,
            right: Box::new(Predicate::Literal(Literal::Text(value.to_string()))),
        }
    }

    #[test]
    fn test_render_inline_quotes_text() {
        assert_eq!(text_eq("PA'D").render_inline(), r#"("status" = 'PA''D')"#);
    }

    #[test]
    fn test_render_inline_escapes_backslash_with_e_string() {
        // `a\b` must reach Postgres as the 3-character string under either setting of
        // standard_conforming_strings.
        assert_eq!(text_eq(r"a\b").render_inline(), r#"("status" = E'a\\b')"#);
        assert_eq!(
            text_eq(r"it's \n").render_inline(),
            r#"("status" = E'it''s \\n')"#
        );
        // Trailing backslash cannot swallow the closing quote.
        assert_eq!(text_eq(r"x\").render_inline(), r#"("status" = E'x\\')"#);
    }

    #[test]
    fn test_render_inline_other_literals() {
        let ts = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let p = Predicate::Cmp {
            left: Box::new(Predicate::Column("updated_at".to_string())),
            op: CmpOp::Gt,
            right: Box::new(Predicate::Literal(Literal::Timestamp(ts))),
        };
        assert_eq!(
            p.render_inline(),
            r#"("updated_at" > '2023-11-14T22:13:20+00:00'::timestamptz)"#
        );
        let p = Predicate::Cmp {
            left: Box::new(Predicate::Column("x".to_string())),
            op: CmpOp::Eq,
            right: Box::new(Predicate::Literal(Literal::Float(f64::NAN))),
        };
        assert_eq!(p.render_inline(), r#"("x" = 'NaN'::float8)"#);
        let d = chrono::NaiveDate::from_ymd_opt(2026, 9, 27).unwrap();
        let p = Predicate::Cmp {
            left: Box::new(Predicate::Column("shipped_on".to_string())),
            op: CmpOp::GtEq,
            right: Box::new(Predicate::Literal(Literal::Date(d))),
        };
        assert_eq!(p.render_inline(), r#"("shipped_on" >= '2026-09-27'::date)"#);
    }

    #[test]
    fn test_render_inline_parenthesizes_like_execution() {
        let p = Predicate::IsNull(Box::new(Predicate::Not(Box::new(Predicate::Column(
            "flag".to_string(),
        )))));
        assert_eq!(p.render_inline(), r#"((NOT "flag") IS NULL)"#);
    }
}
