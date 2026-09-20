//! Postgres inline SQL rendering for `EXPLAIN`.
//!
//! `render_inline_pg` renders a [`Predicate`](crate::pushdown::Predicate) with literals inlined as
//! **Postgres** SQL text (double-quoted identifiers, `'`-doubled strings, `::float8` /
//! `::timestamptz` casts). It is Postgres-specific, so it lives with the connector rather than in
//! the backend-agnostic `pushdown` module (AGENTS.md §1/§6). Used only for `EXPLAIN (FORMAT JSON)`
//! cost estimation — never a real executed query, which binds parameters via
//! `Predicate::render_to` instead.

use crate::pushdown::{Literal, Predicate};

/// Render a predicate as inline Postgres SQL text (EXPLAIN only).
pub(crate) trait PredicateInlineSql {
    fn render_inline_pg(&self) -> String;
}

impl PredicateInlineSql for Predicate {
    fn render_inline_pg(&self) -> String {
        match self {
            Predicate::Column(name) => format!("\"{}\"", name.replace('"', "\"\"")),
            Predicate::Literal(lit) => render_literal_inline(lit),
            Predicate::Cmp { left, op, right } => {
                format!(
                    "({} {op} {})",
                    left.render_inline_pg(),
                    right.render_inline_pg()
                )
            }
            Predicate::And(l, r) => {
                format!("({} AND {})", l.render_inline_pg(), r.render_inline_pg())
            }
            Predicate::Or(l, r) => {
                format!("({} OR {})", l.render_inline_pg(), r.render_inline_pg())
            }
            Predicate::Not(p) => format!("NOT ({})", p.render_inline_pg()),
            Predicate::IsNull(p) => format!("{} IS NULL", p.render_inline_pg()),
            Predicate::IsNotNull(p) => format!("{} IS NOT NULL", p.render_inline_pg()),
            Predicate::Cast { expr, to_type } => {
                format!("{}::{to_type}", expr.render_inline_pg())
            }
        }
    }
}

fn render_literal_inline(literal: &Literal) -> String {
    match literal {
        Literal::Bool(v) => v.to_string().to_uppercase(),
        Literal::Int(v) => v.to_string(),
        Literal::Float(v) => format!("'{v}'::float8"),
        Literal::Text(v) => format!("'{}'", v.replace('\'', "''")),
        Literal::Timestamp(v) => format!("'{}'::timestamptz", v.to_rfc3339()),
    }
}
