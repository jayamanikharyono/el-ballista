//! Engine-agnostic predicate IR and its dialect-driven renderer.
//! pushdown/ir.rs
//! [`Predicate`] is what crosses the planner/executor boundary (it is serialized into Ballista
//! plans), so every field that ends up in SQL text is a closed enum — never a free-form string
//! decoded from plan bytes. Literals are never interpolated: [`Predicate::render_to`] hands them
//! to a [`SqlSink`], which binds them at render position.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::pushdown::dialect::SqlDialect;

/// A comparison operator with identical three-valued semantics in SQL and DataFusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl CmpOp {
    /// The SQL spelling of the operator.
    ///
    /// # Examples
    /// ```
    /// use rust_ballista_extraction_layer::pushdown::CmpOp;
    /// assert_eq!(CmpOp::NotEq.as_sql(), "<>");
    /// ```
    pub fn as_sql(self) -> &'static str {
        match self {
            CmpOp::Eq => "=",
            CmpOp::NotEq => "<>",
            CmpOp::Lt => "<",
            CmpOp::LtEq => "<=",
            CmpOp::Gt => ">",
            CmpOp::GtEq => ">=",
        }
    }

    /// `=` or `<>` (as opposed to an ordering comparison).
    ///
    /// # Examples
    /// ```
    /// use rust_ballista_extraction_layer::pushdown::CmpOp;
    /// assert!(CmpOp::Eq.is_equality() && !CmpOp::Lt.is_equality());
    /// ```
    pub fn is_equality(self) -> bool {
        matches!(self, CmpOp::Eq | CmpOp::NotEq)
    }
}

/// Target type of an IR cast. Rendered through [`SqlDialect::cast_type_name`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CastType {
    /// The source's canonical text form of the value.
    Text,
}

/// Collation of an IR `COLLATE` node. Rendered through [`SqlDialect::collation_name`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Collation {
    /// Byte-wise comparison of UTF-8 text — the ordering Arrow and Rust `str` use.
    Binary,
}

/// A translated predicate, rendered into SQL with bound parameters — never as an interpolated
/// string. Deliberately not raw SQL text: sqlx's `QueryBuilder` generates its own `$N`
/// placeholders as `push_bind` is called, so the tree is walked at render time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Predicate {
    Column(String),
    Literal(Literal),
    Cmp {
        left: Box<Predicate>,
        op: CmpOp,
        right: Box<Predicate>,
    },
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    IsNull(Box<Predicate>),
    IsNotNull(Box<Predicate>),
    /// `CAST(expr AS <type>)`. Produced by translation only for columns the connector
    /// classifies as compared through their text form (enum labels, uuid, json, ...).
    Cast {
        expr: Box<Predicate>,
        to_type: CastType,
    },
    /// `expr COLLATE <collation>`. Produced by translation around every text operand so the
    /// source compares byte-wise, exactly like Arrow.
    Collate {
        expr: Box<Predicate>,
        collation: Collation,
    },
}

/// A literal operand. Plan-serializable; mirrored by [`SqlParam`] for binding.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Literal {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Timestamp(DateTime<Utc>),
    /// A calendar date (no time, no zone). Compared against `date` columns only.
    Date(NaiveDate),
}

impl Predicate {
    /// Render into a [`SqlSink`]. Identifiers, cast types and collations come from the
    /// `dialect`; literals are handed to the sink, which binds them at render position (sqlx's
    /// `push_bind` appends its placeholder inline, so a collect-then-bind pass would misnumber
    /// them).
    ///
    /// Every composite node is wrapped in its own parentheses (`("flag" IS NULL)`,
    /// `(NOT ...)`, `CAST(... AS text)`), so the rendered SQL never depends on the dialect's
    /// operator precedence — e.g. Postgres binds `IS NULL` tighter than `NOT`, so an
    /// unparenthesized `NOT "flag" IS NULL` would mean `NOT ("flag" IS NULL)`.
    ///
    /// # Examples
    /// ```ignore
    /// // (needs a dialect + sink; see connector::postgres::param_sink::PgParamSink)
    /// predicate.render_to(&PostgresDialect, &mut PgParamSink::new(&mut query_builder));
    /// ```
    pub fn render_to(&self, dialect: &dyn SqlDialect, sink: &mut dyn SqlSink) {
        match self {
            Predicate::Column(name) => sink.push_sql(&dialect.quote_ident(name)),
            Predicate::Literal(lit) => sink.push_param(SqlParam::from(lit)),
            Predicate::Cmp { left, op, right } => {
                sink.push_sql("(");
                left.render_to(dialect, sink);
                sink.push_sql(" ");
                sink.push_sql(op.as_sql());
                sink.push_sql(" ");
                right.render_to(dialect, sink);
                sink.push_sql(")");
            }
            Predicate::And(l, r) => {
                sink.push_sql("(");
                l.render_to(dialect, sink);
                sink.push_sql(" AND ");
                r.render_to(dialect, sink);
                sink.push_sql(")");
            }
            Predicate::Or(l, r) => {
                sink.push_sql("(");
                l.render_to(dialect, sink);
                sink.push_sql(" OR ");
                r.render_to(dialect, sink);
                sink.push_sql(")");
            }
            Predicate::Not(p) => {
                sink.push_sql("(NOT ");
                p.render_to(dialect, sink);
                sink.push_sql(")");
            }
            Predicate::IsNull(p) => {
                sink.push_sql("(");
                p.render_to(dialect, sink);
                sink.push_sql(" IS NULL)");
            }
            Predicate::IsNotNull(p) => {
                sink.push_sql("(");
                p.render_to(dialect, sink);
                sink.push_sql(" IS NOT NULL)");
            }
            Predicate::Cast { expr, to_type } => {
                sink.push_sql("CAST(");
                expr.render_to(dialect, sink);
                sink.push_sql(" AS ");
                sink.push_sql(dialect.cast_type_name(*to_type));
                sink.push_sql(")");
            }
            Predicate::Collate { expr, collation } => {
                sink.push_sql("(");
                expr.render_to(dialect, sink);
                sink.push_sql(" COLLATE ");
                sink.push_sql(dialect.collation_name(*collation));
                sink.push_sql(")");
            }
        }
    }
}

/// A bound query parameter in backend-neutral form. Mirrors [`Literal`] (the plan-serializable
/// side); each connector's [`SqlSink`] translates these into driver binds.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlParam {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Timestamp(DateTime<Utc>),
    Date(NaiveDate),
}

impl From<&Literal> for SqlParam {
    fn from(literal: &Literal) -> Self {
        match literal {
            Literal::Bool(v) => SqlParam::Bool(*v),
            Literal::Int(v) => SqlParam::Int(*v),
            Literal::Float(v) => SqlParam::Float(*v),
            Literal::Text(v) => SqlParam::Text(v.clone()),
            Literal::Timestamp(v) => SqlParam::Timestamp(*v),
            Literal::Date(v) => SqlParam::Date(*v),
        }
    }
}

/// A destination for rendered SQL: text and bound parameters flow through one interface so
/// each backend binds at render position.
pub trait SqlSink {
    /// Append literal SQL text (keywords, identifiers, placeholders).
    fn push_sql(&mut self, sql: &str);
    /// Bind one parameter at the current position.
    fn push_param(&mut self, param: SqlParam);
}

#[cfg(test)]
pub(crate) mod test_support {
    //! A collecting sink + neutral dialect for unit tests of the renderer.
    use super::*;

    pub struct AnsiDialect;

    impl SqlDialect for AnsiDialect {
        fn quote_ident(&self, name: &str) -> String {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
        fn placeholder(&self, param_index: usize) -> String {
            format!("${param_index}")
        }
        fn cast_type_name(&self, to: CastType) -> &'static str {
            match to {
                CastType::Text => "text",
            }
        }
        fn collation_name(&self, collation: Collation) -> &'static str {
            match collation {
                Collation::Binary => "\"C\"",
            }
        }
    }

    /// Render with `$n` placeholders and return `(sql, params)`.
    pub fn render(predicate: &Predicate) -> (String, Vec<SqlParam>) {
        struct Collect {
            sql: String,
            params: Vec<SqlParam>,
        }
        impl SqlSink for Collect {
            fn push_sql(&mut self, sql: &str) {
                self.sql.push_str(sql);
            }
            fn push_param(&mut self, param: SqlParam) {
                self.params.push(param);
                self.sql
                    .push_str(&AnsiDialect.placeholder(self.params.len()));
            }
        }
        let mut sink = Collect {
            sql: String::new(),
            params: Vec::new(),
        };
        predicate.render_to(&AnsiDialect, &mut sink);
        (sink.sql, sink.params)
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::render;
    use super::*;

    fn col(name: &str) -> Box<Predicate> {
        Box::new(Predicate::Column(name.to_string()))
    }

    #[test]
    fn test_every_node_is_parenthesized() {
        // `(NOT flag) IS NULL` must keep its grouping.
        let p = Predicate::IsNull(Box::new(Predicate::Not(col("flag"))));
        assert_eq!(render(&p).0, r#"((NOT "flag") IS NULL)"#);

        // NOT (flag IS NULL) renders differently from the above.
        let p = Predicate::Not(Box::new(Predicate::IsNull(col("flag"))));
        assert_eq!(render(&p).0, r#"(NOT ("flag" IS NULL))"#);

        // IS NOT NULL over NOT, and NOT over IS NOT NULL.
        let p = Predicate::IsNotNull(Box::new(Predicate::Not(col("flag"))));
        assert_eq!(render(&p).0, r#"((NOT "flag") IS NOT NULL)"#);
        let p = Predicate::Not(Box::new(Predicate::IsNotNull(col("flag"))));
        assert_eq!(render(&p).0, r#"(NOT ("flag" IS NOT NULL))"#);

        // Double negation and IS NULL over IS NULL.
        let p = Predicate::Not(Box::new(Predicate::Not(col("flag"))));
        assert_eq!(render(&p).0, r#"(NOT (NOT "flag"))"#);
        let p = Predicate::IsNull(Box::new(Predicate::IsNull(col("x"))));
        assert_eq!(render(&p).0, r#"(("x" IS NULL) IS NULL)"#);

        // Cast inside IS NULL / NOT / comparison.
        let cast = Predicate::Cast {
            expr: col("m"),
            to_type: CastType::Text,
        };
        let p = Predicate::IsNull(Box::new(cast.clone()));
        assert_eq!(render(&p).0, r#"(CAST("m" AS text) IS NULL)"#);
        let p = Predicate::Cmp {
            left: Box::new(Predicate::Collate {
                expr: Box::new(cast),
                collation: Collation::Binary,
            }),
            op: CmpOp::Eq,
            right: Box::new(Predicate::Literal(Literal::Text("x".into()))),
        };
        assert_eq!(render(&p).0, r#"((CAST("m" AS text) COLLATE "C") = $1)"#);
        let p = Predicate::Not(Box::new(p));
        assert_eq!(
            render(&p).0,
            r#"(NOT ((CAST("m" AS text) COLLATE "C") = $1))"#
        );

        // IS NULL over a comparison, inside OR.
        let cmp = Predicate::Cmp {
            left: col("id"),
            op: CmpOp::Gt,
            right: Box::new(Predicate::Literal(Literal::Int(1))),
        };
        let p = Predicate::Or(
            Box::new(Predicate::IsNull(Box::new(cmp))),
            Box::new(Predicate::IsNotNull(col("id"))),
        );
        assert_eq!(
            render(&p).0,
            r#"((("id" > $1) IS NULL) OR ("id" IS NOT NULL))"#
        );
    }

    #[test]
    fn test_params_bind_left_to_right() {
        let p = Predicate::And(
            Box::new(Predicate::Cmp {
                left: col("id"),
                op: CmpOp::Eq,
                right: Box::new(Predicate::Literal(Literal::Int(1))),
            }),
            Box::new(Predicate::Cmp {
                left: col("active"),
                op: CmpOp::Eq,
                right: Box::new(Predicate::Literal(Literal::Bool(true))),
            }),
        );
        let (sql, params) = render(&p);
        assert_eq!(sql, r#"(("id" = $1) AND ("active" = $2))"#);
        assert_eq!(params, vec![SqlParam::Int(1), SqlParam::Bool(true)]);
    }

    #[test]
    fn test_ops_and_casts_are_closed_enums_in_serialized_form() {
        // Plan bytes carry enum tags, not SQL text: a tampered/garbage op cannot decode.
        let p = Predicate::Cmp {
            left: col("id"),
            op: CmpOp::LtEq,
            right: Box::new(Predicate::Literal(Literal::Int(1))),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("LtEq"), "{json}");
        let tampered = json.replace("LtEq", "; DROP TABLE x; --");
        assert!(serde_json::from_str::<Predicate>(&tampered).is_err());
    }
}
