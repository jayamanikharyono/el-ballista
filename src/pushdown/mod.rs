//! Source-aware pushdown: expression translation, fidelity rules, and policy.
//! pushdown/mod.rs
//! A bounded implementation of docs/pushdown.md: the allowlist-based expression translator
//! (§2) and the `Exact`/`Inexact` fidelity rules for the hazards in §3 that can be judged from
//! the expression and literal alone, plus the policy modes from §4.3 (`always` / `never` /
//! `cost_based` / `hinted`).
//!
//! `cost_based` consults real source statistics (`pg_stats` via [`stats`]), index metadata
//! (`pg_index`), and optional EXPLAIN estimates ([`explain`]) through [`decide_with`]. The
//! legacy [`decide`] (no statistics) preserves the old optimistic behavior for contexts that
//! cannot reach the source, such as a provider rebuilt from a serialized plan.
//!
//! Still deferred, matching docs/roadmap.md's phase split: per-predicate (expression-level)
//! hints, `IN`/`BETWEEN`/`LIKE`/arithmetic translation, per-column collation lookups (so
//! `Inexact` is used for *every* string comparison rather than only non-`C` collations, per
//! §3.1), and aggregate/join pushdown.
//!
//! The guiding rule for every judgment call in this file: docs/roadmap.md's "What would make
//! this project fail" names "pushdown that is fast and wrong" as the fatal failure mode. So
//! wherever this module is unsure whether a translation preserves Arrow semantics, it chooses
//! `Inexact` or refuses to translate at all (`None`) — never `Exact`. Getting the conservative
//! case wrong costs performance; getting the `Exact` case wrong costs correctness.

pub mod cost_model;
pub mod dialect;
pub mod explain;
pub mod optimizer_rule;
pub mod stats;

use chrono::{DateTime, Utc};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, QueryBuilder};
use std::collections::HashMap;

use crate::pushdown::cost_model::{CostDecision, CostParams, decide_push};
use crate::pushdown::dialect::SqlDialect;
use crate::pushdown::explain::ExplainEstimate;
use crate::pushdown::stats::{IndexInfo, SourceStatistics};

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

/// The push/keep policy — docs/pushdown.md §4.3.
/// - `Always`: push everything translatable (minus denylist). Dedicated replica.
/// - `Never`: keep everything in Arrow. Emergency: source under pressure.
/// - `CostBased`: the §4.2 model over real statistics (default). Normal operation.
/// - `Strict`: paranoid mode — push a filter only if every referenced column is indexed,
///   selectivity says it removes at least `(1 - keep_threshold)` of rows, and every literal
///   and column involved is primitive (bool/int/timestamp; numerics, floats, text, and casts
///   never push). `LIMIT` is never pushed under `strict`, and `push` hints are ignored
///   (`deny` still applies). Emergency: source under pressure.
/// - `Hinted`: per-column overrides from the job spec — `push` forces, `deny` refuses, the
///   rest falls back to the cost model. When you know something the stats do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PushdownPolicy {
    Always,
    Never,
    CostBased,
    Strict,
    Hinted,
}

impl PushdownPolicy {
    pub fn parse(raw: &str) -> PushdownPolicy {
        match raw {
            "always" => PushdownPolicy::Always,
            "never" => PushdownPolicy::Never,
            "strict" => PushdownPolicy::Strict,
            "hinted" => PushdownPolicy::Hinted,
            _ => PushdownPolicy::CostBased,
        }
    }
}

/// A translated predicate, in a form that can be rendered into a `QueryBuilder` with bound
/// parameters — never as an interpolated string. Deliberately not raw SQL text: `QueryBuilder`
/// generates its own `$N` placeholders as `push_bind` is called, so the tree has to be walked at
/// render time rather than assembled into a string upfront.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Predicate {
    Column(String),
    Literal(Literal),
    Cmp {
        left: Box<Predicate>,
        op: String,
        right: Box<Predicate>,
    },
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    IsNull(Box<Predicate>),
    IsNotNull(Box<Predicate>),
    /// Explicit cast, e.g. `"status"::text`. Produced only by enum normalization (never by
    /// `translate`): Postgres has no `enum = text` operator, so a text literal against an
    /// enum column compares against the label instead. `to_type` always comes from our own
    /// code (today: `"text"`), never from user input.
    Cast {
        expr: Box<Predicate>,
        to_type: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Literal {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Timestamp(DateTime<Utc>),
}

impl Predicate {
    /// Render to backend-neutral SQL plus an ordered parameter list — the SPI every connector
    /// shares. Identifiers and placeholders come from the `dialect`; literals are never
    /// interpolated, only collected into `params` in left-to-right order.
    ///
    /// Production Postgres rendering goes through [`render_to`] with a [`PgParamSink`]
    /// (bind-at-position); this collecting form is the observable IR pinned by tests and the
    /// starting point for the next backend's binder.
    ///
    /// [`render_to`]: Predicate::render_to
    /// [`PgParamSink`]: crate::pushdown::PgParamSink
    #[allow(dead_code)] // SPI surface for the next backend; pinned by unit tests
    pub fn render_sql(&self, dialect: &dyn SqlDialect, params: &mut Vec<SqlParam>) -> String {
        let mut sink = CollectingSink::new(dialect, params);
        self.render_to(dialect, &mut sink);
        sink.finish()
    }

    /// Render into a [`SqlSink`]. This is the primitive [`render_sql`] is built on; backends
    /// bind at render position through their own sink (sqlx's `push_bind` appends its
    /// placeholder inline, so collecting params for a later bind pass would misnumber them).
    ///
    /// [`render_sql`]: Predicate::render_sql
    pub fn render_to(&self, dialect: &dyn SqlDialect, sink: &mut dyn SqlSink) {
        match self {
            Predicate::Column(name) => sink.push_sql(&dialect.quote_ident(name)),
            Predicate::Literal(lit) => sink.push_param(SqlParam::from(lit)),
            Predicate::Cmp { left, op, right } => {
                sink.push_sql("(");
                left.render_to(dialect, sink);
                sink.push_sql(&format!(" {op} "));
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
                sink.push_sql("NOT (");
                p.render_to(dialect, sink);
                sink.push_sql(")");
            }
            Predicate::IsNull(p) => {
                p.render_to(dialect, sink);
                sink.push_sql(" IS NULL");
            }
            Predicate::IsNotNull(p) => {
                p.render_to(dialect, sink);
                sink.push_sql(" IS NOT NULL");
            }
            Predicate::Cast { expr, to_type } => {
                expr.render_to(dialect, sink);
                sink.push_sql(&format!("::{to_type}"));
            }
        }
    }

    /// Render with literals inlined as SQL text. **EXPLAIN only** — the result is passed to
    /// `EXPLAIN (FORMAT JSON)`, which plans without executing, never to a real query. Bound
    /// parameters cannot be used there because EXPLAIN has nothing to bind. Text is quoted
    /// with `'`-doubling; floats go through an explicit `::float8` cast so `NaN`/`Infinity`
    /// parse; timestamps are RFC-3339 quoted.
    pub fn render_inline(&self) -> String {
        match self {
            Predicate::Column(name) => format!("\"{}\"", name.replace('"', "\"\"")),
            Predicate::Literal(lit) => lit.render_inline(),
            Predicate::Cmp { left, op, right } => {
                format!("({} {op} {})", left.render_inline(), right.render_inline())
            }
            Predicate::And(l, r) => {
                format!("({} AND {})", l.render_inline(), r.render_inline())
            }
            Predicate::Or(l, r) => {
                format!("({} OR {})", l.render_inline(), r.render_inline())
            }
            Predicate::Not(p) => format!("NOT ({})", p.render_inline()),
            Predicate::IsNull(p) => format!("{} IS NULL", p.render_inline()),
            Predicate::IsNotNull(p) => format!("{} IS NOT NULL", p.render_inline()),
            Predicate::Cast { expr, to_type } => {
                format!("{}::{to_type}", expr.render_inline())
            }
        }
    }
}

/// A bound query parameter in backend-neutral form. Mirrors [`Literal`] (which is the
/// plan-serializable side); each connector translates these into driver binds — a MySQL
/// connector would walk the same list into `QueryBuilder<MySql>`.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlParam {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Timestamp(DateTime<Utc>),
}

impl From<&Literal> for SqlParam {
    fn from(literal: &Literal) -> Self {
        match literal {
            Literal::Bool(v) => SqlParam::Bool(*v),
            Literal::Int(v) => SqlParam::Int(*v),
            Literal::Float(v) => SqlParam::Float(*v),
            Literal::Text(v) => SqlParam::Text(v.clone()),
            Literal::Timestamp(v) => SqlParam::Timestamp(*v),
        }
    }
}

/// A destination for rendered SQL: text and bound parameters flow through one interface so
/// each backend binds at render position (sqlx `push_bind` appends its placeholder inline —
/// a collect-then-bind pass would misnumber them).
pub trait SqlSink {
    /// Append literal SQL text (keywords, identifiers, placeholders).
    fn push_sql(&mut self, sql: &str);
    /// Bind one parameter at the current position.
    fn push_param(&mut self, param: SqlParam);
}

/// [`SqlSink`] that accumulates text plus an ordered parameter list — the observable form of
/// the backend-neutral IR. A MySQL connector would walk the same output into its own driver.
/// Currently exercised by unit tests; see [`Predicate::render_sql`].
#[allow(dead_code)] // SPI surface for the next backend; pinned by unit tests
pub struct CollectingSink<'a> {
    dialect: &'a dyn SqlDialect,
    sql: String,
    params: &'a mut Vec<SqlParam>,
}

impl<'a> CollectingSink<'a> {
    pub fn new(dialect: &'a dyn SqlDialect, params: &'a mut Vec<SqlParam>) -> Self {
        Self {
            dialect,
            sql: String::new(),
            params,
        }
    }

    pub fn finish(self) -> String {
        self.sql
    }
}

impl SqlSink for CollectingSink<'_> {
    fn push_sql(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }

    fn push_param(&mut self, param: SqlParam) {
        self.params.push(param);
        let placeholder = self.dialect.placeholder(self.params.len());
        self.sql.push_str(&placeholder);
    }
}

/// [`SqlSink`] that binds straight into a Postgres query: `push_bind` appends `$n` at the
/// current position, so text and numbering stay aligned by construction.
pub struct PgParamSink<'q> {
    query: &'q mut QueryBuilder<Postgres>,
}

impl<'q> PgParamSink<'q> {
    pub fn new(query: &'q mut QueryBuilder<Postgres>) -> Self {
        Self { query }
    }
}

impl SqlSink for PgParamSink<'_> {
    fn push_sql(&mut self, sql: &str) {
        self.query.push(sql);
    }

    fn push_param(&mut self, param: SqlParam) {
        match param {
            SqlParam::Bool(v) => self.query.push_bind(v),
            SqlParam::Int(v) => self.query.push_bind(v),
            SqlParam::Float(v) => self.query.push_bind(v),
            SqlParam::Text(v) => self.query.push_bind(v),
            SqlParam::Timestamp(v) => self.query.push_bind(v),
        };
    }
}

impl Literal {
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

    /// Inline SQL text for [`Predicate::render_inline`].
    fn render_inline(&self) -> String {
        match self {
            Literal::Bool(v) => v.to_string().to_uppercase(),
            Literal::Int(v) => v.to_string(),
            Literal::Float(v) => format!("'{v}'::float8"),
            Literal::Text(v) => format!("'{}'", v.replace('\'', "''")),
            Literal::Timestamp(v) => format!("'{}'::timestamptz", v.to_rfc3339()),
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
                    op: sql_op.to_string(),
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
    Push {
        fidelity: Fidelity,
        predicate: Predicate,
    },
    Keep,
}

/// Legacy entry point preserved for callers without source access: `CostBased` and the
/// unhinted remainder of `Hinted` optimistically push everything translatable. Prefer
/// [`decide_with`] with real [`CostInputs`] wherever a pool is available.
#[allow(dead_code)] // public compat shim; exercised by unit tests, unused by the CLI
pub fn decide(expr: &Expr, policy: PushdownPolicy, deny: &[String]) -> Decision {
    decide_with(expr, policy, deny, &[], None)
}

/// Live inputs for cost-based decisions. `None` (the [`decide`] legacy path) means "no source
/// reachable": `CostBased` and the unhinted remainder of `Hinted` degrade to pushing everything
/// translatable, exactly the old behavior. Providers rebuilt from serialized plans (scheduler
/// side) therefore keep Phase 4 behavior; providers built from config carry real statistics.
pub struct CostInputs<'a> {
    pub stats: &'a SourceStatistics,
    pub params: &'a CostParams,
    pub indexes: &'a [IndexInfo],
    /// Best-effort EXPLAIN estimate for this exact predicate, when the estimator cache has one.
    /// Sharpens the cost and index answers; never required.
    pub explain: Option<ExplainEstimate>,
    /// Column name → `information_schema` data type, for the `strict` primitive-type gate.
    pub column_types: &'a HashMap<String, String>,
}

/// Normalize enum comparisons into a pushable form.
///
/// Postgres has no `enum = text` operator, so a text literal against an enum column would
/// fail at execution with `42883`. The fix compares against the label instead:
/// `"status" = 'PAID'` becomes `"status"::text = 'PAID'`. Enum-label equality *is* enum
/// equality, so the cast only narrows; fidelity is forced `Inexact` so Arrow still
/// re-checks, exactly like any other string comparison.
///
/// A non-text literal against an enum column has no safe push form (`text = integer` has no
/// operator either) — that returns `None` (keep in Arrow). Without this, such a predicate
/// would report `Exact` (an integer literal looks exact) while failing at runtime.
///
/// Only columns in `enum_columns` (true enums from `pg_enum`, cached per provider) are
/// rewritten. In particular `citext` is *not*: its native `citext = text` operator is
/// case-insensitive, and recasting to text would narrow case-sensitively — dropping rows
/// Arrow would keep. When in doubt we keep, never push.
pub fn normalize_enum_comparison(
    fidelity: Fidelity,
    predicate: Predicate,
    enum_columns: &std::collections::HashSet<String>,
) -> Option<(Fidelity, Predicate)> {
    let Predicate::Cmp { left, op, right } = predicate else {
        return Some((fidelity, predicate));
    };
    match (*left, *right) {
        (Predicate::Column(col), Predicate::Literal(lit)) => {
            normalize_enum_side(fidelity, col, lit, op, true, enum_columns)
        }
        (Predicate::Literal(lit), Predicate::Column(col)) => {
            normalize_enum_side(fidelity, col, lit, op, false, enum_columns)
        }
        (left, right) => Some((
            fidelity,
            Predicate::Cmp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
        )),
    }
}

fn normalize_enum_side(
    fidelity: Fidelity,
    col: String,
    lit: Literal,
    op: String,
    col_on_left: bool,
    enum_columns: &std::collections::HashSet<String>,
) -> Option<(Fidelity, Predicate)> {
    if !enum_columns.contains(&col) {
        // Not an enum column: rebuild unchanged, prior behavior stands.
        let (left, right) = if col_on_left {
            (Predicate::Column(col), Predicate::Literal(lit))
        } else {
            (Predicate::Literal(lit), Predicate::Column(col))
        };
        return Some((
            fidelity,
            Predicate::Cmp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
        ));
    }
    if !matches!(lit, Literal::Text(_)) {
        // Enum against a non-text literal has no pushable form (`text = integer` has no
        // operator either). Keep in Arrow — otherwise an `Exact`-looking predicate would
        // fail at execution with 42883.
        return None;
    }
    let cast = Predicate::Cast {
        expr: Box::new(Predicate::Column(col)),
        to_type: "text".to_string(),
    };
    let (left, right) = if col_on_left {
        (cast, Predicate::Literal(lit))
    } else {
        (Predicate::Literal(lit), cast)
    };
    Some((
        Fidelity::Inexact,
        Predicate::Cmp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
    ))
}
/// Full decision function: translation correctness × policy × denylist × per-column push hints ×
/// cost model. `supports_filters_pushdown` and `scan` must call this with the *same* inputs for
/// a given provider, because DataFusion trusts an `Exact` answer by dropping its own filter —
/// the two can never disagree.
///
/// Providers with catalog access should prefer [`PostgresTableProvider::decide_cost`], which
/// additionally normalizes enum comparisons (see [`normalize_enum_comparison`]) before
/// deciding.
///
/// [`PostgresTableProvider::decide_cost`]: crate::connector::postgres::PostgresTableProvider::decide_cost
pub fn decide_with(
    expr: &Expr,
    policy: PushdownPolicy,
    deny: &[String],
    push: &[String],
    cost: Option<&CostInputs>,
) -> Decision {
    let Some((fidelity, predicate)) = translate(expr) else {
        return Decision::Keep;
    };
    decide_translated(fidelity, predicate, policy, deny, push, cost)
}

/// Decision over an already-translated predicate. Same contract as [`decide_with`]; the
/// split exists so providers can normalize (enum casts) between translation and decision.
pub fn decide_translated(
    fidelity: Fidelity,
    predicate: Predicate,
    policy: PushdownPolicy,
    deny: &[String],
    push: &[String],
    cost: Option<&CostInputs>,
) -> Decision {
    if policy == PushdownPolicy::Never {
        return Decision::Keep;
    }

    if references_denied_column(&predicate, deny) {
        return Decision::Keep;
    }

    match policy {
        PushdownPolicy::Never => Decision::Keep, // handled above; kept for exhaustiveness
        PushdownPolicy::Always => Decision::Push {
            fidelity,
            predicate,
        },
        PushdownPolicy::Strict => decide_strict(&predicate, cost),
        PushdownPolicy::Hinted if references_push_column(&predicate, push) => Decision::Push {
            fidelity,
            predicate,
        },
        PushdownPolicy::Hinted | PushdownPolicy::CostBased => match cost {
            Some(inputs) => {
                let (effective_indexes, owned_index);
                let indexes: &[IndexInfo] = match &inputs.explain {
                    Some(est) if est.access_method.is_indexed() => {
                        // EXPLAIN saw an index the catalog query missed (expression index,
                        // newly created): synthesize an entry so the model treats it as free.
                        owned_index = IndexInfo {
                            name: est
                                .index_name
                                .clone()
                                .unwrap_or_else(|| "explain".to_string()),
                            columns: predicate_columns(&predicate),
                            is_unique: false,
                            is_primary: false,
                            index_type: "explain".to_string(),
                        };
                        effective_indexes = inputs
                            .indexes
                            .iter()
                            .cloned()
                            .chain(std::iter::once(owned_index))
                            .collect::<Vec<_>>();
                        &effective_indexes
                    }
                    _ => inputs.indexes,
                };
                let estimated_cost = inputs
                    .explain
                    .as_ref()
                    .map(|est| est.total_cost.max(0.0) as u64);
                match decide_push(
                    &predicate,
                    fidelity,
                    inputs.stats,
                    inputs.params,
                    estimated_cost,
                    indexes,
                ) {
                    CostDecision::Push { .. } => Decision::Push {
                        fidelity,
                        predicate,
                    },
                    CostDecision::Keep { .. } => Decision::Keep,
                }
            }
            // No source reachable: optimistically push (legacy behavior).
            None => Decision::Push {
                fidelity,
                predicate,
            },
        },
    }
}

fn references_denied_column(predicate: &Predicate, deny: &[String]) -> bool {
    references_denied_column_public(predicate, deny)
}

/// `strict` policy: every gate must pass, otherwise keep.
/// 1. Every referenced column is indexed (a predicate over nothing pushable gains nothing).
/// 2. Every literal and column involved is primitive — bool/int/timestamp literals over
///    smallint/integer/bigint/boolean/date/timestamp columns. Numerics, floats, text, and
///    casts (enum labels) never push: their cross-type semantics are exactly where
///    "fast and wrong" lives.
/// 3. Selectivity below `keep_threshold` (it must filter the majority) and estimated cost
///    within budget, same estimators as the cost model.
///
///    Without statistics (`None`) strict keeps everything — paranoid means paranoid, unlike
///    the legacy optimistic fallback. `push` hints are ignored under strict; `deny` (checked
///    by the caller) still applies.
fn decide_strict(predicate: &Predicate, cost: Option<&CostInputs>) -> Decision {
    match strict_gate(predicate, cost) {
        StrictGate::Push => {
            // Fidelity comes from the translated shape; strict already excluded every
            // non-exact form above except plain primitive comparisons.
            let fidelity = match predicate {
                Predicate::Cmp { left, right, .. } => comparison_fidelity_for_strict(left, right),
                _ => Fidelity::Exact,
            };
            Decision::Push {
                fidelity,
                predicate: predicate.clone(),
            }
        }
        _ => Decision::Keep,
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

fn strict_gate(predicate: &Predicate, cost: Option<&CostInputs>) -> StrictGate {
    use crate::pushdown::cost_model::{estimate_selectivity_from_stats, estimate_source_cost};

    let Some(inputs) = cost else {
        return StrictGate::NoStats;
    };

    let columns = predicate_columns(predicate);
    if columns.is_empty() {
        return StrictGate::NoColumns;
    }
    let all_indexed = columns
        .iter()
        .all(|col| inputs.indexes.iter().any(|idx| idx.columns.contains(col)));
    if !all_indexed {
        return StrictGate::NotIndexed;
    }

    if !is_primitive_predicate(predicate, inputs.column_types) {
        return StrictGate::NonPrimitive;
    }

    let selectivity = estimate_selectivity_from_stats(predicate, inputs.stats);
    if selectivity >= inputs.params.keep_threshold {
        return StrictGate::Unselective {
            selectivity,
            threshold: inputs.params.keep_threshold,
        };
    }

    let estimated = inputs
        .explain
        .as_ref()
        .map(|est| est.total_cost.max(0.0) as u64)
        .unwrap_or_else(|| estimate_source_cost(predicate, inputs.stats));
    if estimated > inputs.params.max_source_cost {
        return StrictGate::OverBudget {
            cost: estimated,
            budget: inputs.params.max_source_cost,
        };
    }

    StrictGate::Push
}

/// Human-readable strict verdict, for `rel plan --explain`.
pub fn describe_strict(predicate: &Predicate, cost: Option<&CostInputs>) -> String {
    match strict_gate(predicate, cost) {
        StrictGate::Push => "strict: indexed + selective + primitive".to_string(),
        StrictGate::NoStats => "strict: no statistics reachable".to_string(),
        StrictGate::NoColumns => "strict: predicate references no columns".to_string(),
        StrictGate::NotIndexed => "strict: not all columns indexed".to_string(),
        StrictGate::NonPrimitive => {
            "strict: non-primitive type (numeric/float/text/cast)".to_string()
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

/// Primitive shapes only: bool/int/timestamp literals over smallint/integer/bigint/boolean/
/// date/timestamp columns. Anything else (numeric, float, text, casts, unknown columns)
/// fails closed.
fn is_primitive_predicate(predicate: &Predicate, column_types: &HashMap<String, String>) -> bool {
    match predicate {
        Predicate::Column(name) => is_primitive_column(column_types.get(name)),
        Predicate::Literal(lit) => {
            matches!(
                lit,
                Literal::Bool(_) | Literal::Int(_) | Literal::Timestamp(_)
            )
        }
        Predicate::Cmp { left, right, .. } => {
            is_primitive_predicate(left, column_types)
                && is_primitive_predicate(right, column_types)
        }
        Predicate::And(l, r) | Predicate::Or(l, r) => {
            is_primitive_predicate(l, column_types) && is_primitive_predicate(r, column_types)
        }
        Predicate::Not(p) | Predicate::IsNull(p) | Predicate::IsNotNull(p) => {
            is_primitive_predicate(p, column_types)
        }
        // Casts (enum labels) are never primitive.
        Predicate::Cast { .. } => false,
    }
}

fn is_primitive_column(data_type: Option<&String>) -> bool {
    matches!(
        data_type.map(String::as_str),
        Some(
            "smallint"
                | "integer"
                | "bigint"
                | "boolean"
                | "date"
                | "timestamp with time zone"
                | "timestamp without time zone"
        )
    )
}

/// Fidelity for strict-pushed comparisons. Strict already excluded text/float literals and
/// non-primitive columns, so only exact shapes remain; column-to-column stays conservative.
fn comparison_fidelity_for_strict(left: &Predicate, right: &Predicate) -> Fidelity {
    match (left, right) {
        (Predicate::Literal(_), Predicate::Column(_))
        | (Predicate::Column(_), Predicate::Literal(_)) => Fidelity::Exact,
        (Predicate::Literal(a), Predicate::Literal(b)) => a.fidelity().combine(b.fidelity()),
        _ => Fidelity::Inexact,
    }
}

/// Whether a predicate touches any denylisted column. Public for `rel plan --explain`.
pub fn references_denied_column_public(predicate: &Predicate, deny: &[String]) -> bool {
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
        Predicate::Cast { expr, .. } => references_denied_column(expr, deny),
    }
}

/// `hinted` policy: a predicate touching any column in the job spec's `push` list is forced to
/// the source (provided it translated — hints never override correctness). Deny wins over push:
/// that check runs first in [`decide_with`]. Public for `rel plan --explain`.
pub fn references_push_column_public(predicate: &Predicate, push: &[String]) -> bool {
    references_push_column(predicate, push)
}

fn references_push_column(predicate: &Predicate, push: &[String]) -> bool {
    if push.is_empty() {
        return false;
    }
    predicate_columns(predicate)
        .iter()
        .any(|col| push.iter().any(|p| p == col))
}

/// Sorted, deduplicated column names referenced by a predicate.
fn predicate_columns(predicate: &Predicate) -> Vec<String> {
    fn walk(predicate: &Predicate, out: &mut Vec<String>) {
        match predicate {
            Predicate::Column(name) => {
                if !out.contains(name) {
                    out.push(name.clone());
                }
            }
            Predicate::Literal(_) => {}
            Predicate::Cmp { left, right, .. } => {
                walk(left, out);
                walk(right, out);
            }
            Predicate::And(l, r) | Predicate::Or(l, r) => {
                walk(l, out);
                walk(r, out);
            }
            Predicate::Not(p) | Predicate::IsNull(p) | Predicate::IsNotNull(p) => walk(p, out),
            Predicate::Cast { expr, .. } => walk(expr, out),
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
    use datafusion::prelude::{col, lit};

    #[test]
    fn test_pushdown_policy_parse() {
        assert_eq!(PushdownPolicy::parse("always"), PushdownPolicy::Always);
        assert_eq!(PushdownPolicy::parse("never"), PushdownPolicy::Never);
        assert_eq!(
            PushdownPolicy::parse("cost_based"),
            PushdownPolicy::CostBased
        );
        assert_eq!(PushdownPolicy::parse("hinted"), PushdownPolicy::Hinted);
        assert_eq!(PushdownPolicy::parse("strict"), PushdownPolicy::Strict);
        assert_eq!(PushdownPolicy::parse("unknown"), PushdownPolicy::CostBased);
    }

    #[test]
    fn test_fidelity_combine() {
        assert_eq!(Fidelity::Exact.combine(Fidelity::Exact), Fidelity::Exact);
        assert_eq!(
            Fidelity::Exact.combine(Fidelity::Inexact),
            Fidelity::Inexact
        );
        assert_eq!(
            Fidelity::Inexact.combine(Fidelity::Exact),
            Fidelity::Inexact
        );
        assert_eq!(
            Fidelity::Inexact.combine(Fidelity::Inexact),
            Fidelity::Inexact
        );
    }

    #[test]
    fn test_translate_render_matrix() {
        use crate::pushdown::dialect::PostgresDialect;
        use datafusion::common::ScalarValue;

        let pg = PostgresDialect;
        let ts = ScalarValue::TimestampMicrosecond(Some(1_700_000_000_000_000), None);

        // (expr, fidelity, postgres SQL). Params asserted separately below.
        let cases: Vec<(Expr, Fidelity, &str)> = vec![
            (col("id").eq(lit(42i64)), Fidelity::Exact, "(\"id\" = $1)"),
            (
                col("id").not_eq(lit(42i64)),
                Fidelity::Exact,
                "(\"id\" <> $1)",
            ),
            (col("id").lt(lit(42i64)), Fidelity::Exact, "(\"id\" < $1)"),
            (
                col("id").lt_eq(lit(42i64)),
                Fidelity::Exact,
                "(\"id\" <= $1)",
            ),
            (col("id").gt(lit(42i64)), Fidelity::Exact, "(\"id\" > $1)"),
            (
                col("id").gt_eq(lit(42i64)),
                Fidelity::Exact,
                "(\"id\" >= $1)",
            ),
            (
                col("active").eq(lit(true)),
                Fidelity::Exact,
                "(\"active\" = $1)",
            ),
            (
                col("updated_at").gt(lit(ts.clone())),
                Fidelity::Exact,
                "(\"updated_at\" > $1)",
            ),
            (
                col("status").eq(lit("PAID")),
                Fidelity::Inexact,
                "(\"status\" = $1)",
            ),
            (
                col("amount").gt(lit(10.5f64)),
                Fidelity::Inexact,
                "(\"amount\" > $1)",
            ),
            (
                col("id").eq(lit(1i64)).and(col("active").eq(lit(true))),
                Fidelity::Exact,
                "((\"id\" = $1) AND (\"active\" = $2))",
            ),
            (
                col("id").eq(lit(1i64)).or(col("status").eq(lit("x"))),
                Fidelity::Inexact,
                "((\"id\" = $1) OR (\"status\" = $2))",
            ),
            (
                Expr::Not(Box::new(col("id").eq(lit(1i64)))),
                Fidelity::Exact,
                "NOT ((\"id\" = $1))",
            ),
            (col("id").is_null(), Fidelity::Exact, "\"id\" IS NULL"),
            (
                col("id").is_not_null(),
                Fidelity::Exact,
                "\"id\" IS NOT NULL",
            ),
        ];

        for (expr, fidelity, sql) in &cases {
            let (f, pred) =
                translate(expr).unwrap_or_else(|| panic!("expected translatable expr: {expr:?}"));
            assert_eq!(*fidelity, f, "fidelity for {expr:?}");
            let mut params = Vec::new();
            assert_eq!(*sql, pred.render_sql(&pg, &mut params), "SQL for {expr:?}");
        }

        // Params arrive left-to-right: compound case yields [Int(1), Bool(true)].
        let (_, pred) =
            translate(&col("id").eq(lit(1i64)).and(col("active").eq(lit(true)))).unwrap();
        let mut params = Vec::new();
        pred.render_sql(&pg, &mut params);
        assert_eq!(params, vec![SqlParam::Int(1), SqlParam::Bool(true)]);

        // Outside the allowlist: arithmetic, LIKE, and casts stay in Arrow.
        assert!(translate(&(col("a") + col("b"))).is_none());
        assert!(translate(&col("x").like(lit("a%"))).is_none());
        assert!(
            translate(&Expr::Not(Box::new(Expr::Not(Box::new(
                col("x").is_null()
            )))))
            .is_some()
        );
    }

    #[test]
    fn test_decide_policy_and_denylist() {
        let expr = col("secret").eq(lit(100i64));

        // Policy::Never -> Keep
        assert!(matches!(
            decide(&expr, PushdownPolicy::Never, &[]),
            Decision::Keep
        ));

        // Policy::Always -> Push
        assert!(matches!(
            decide(&expr, PushdownPolicy::Always, &[]),
            Decision::Push { .. }
        ));

        // Denylist blocks
        let deny = vec!["secret".to_string()];
        assert!(matches!(
            decide(&expr, PushdownPolicy::Always, &deny),
            Decision::Keep
        ));
    }

    #[test]
    fn test_policy_matrix_legacy_fallbacks_and_deny() {
        // Without statistics, cost_based and the unhinted remainder of hinted degrade to
        // optimistic push (legacy behavior for contexts that cannot reach the source).
        // Strict is the exception: no stats means keep everything.
        // Deny blocks under every policy except the never that already keeps.
        let expr = col("secret").eq(lit(100i64));
        let deny = vec!["secret".to_string()];
        let cases: Vec<(PushdownPolicy, &[String], Option<&CostInputs>, &str)> = vec![
            (PushdownPolicy::Always, &[], None, "push:exact"),
            (PushdownPolicy::Never, &[], None, "keep"),
            (PushdownPolicy::Never, &deny, None, "keep"),
            (PushdownPolicy::CostBased, &[], None, "push:exact"),
            (PushdownPolicy::CostBased, &deny, None, "keep"),
            (PushdownPolicy::Hinted, &[], None, "push:exact"),
            (PushdownPolicy::Hinted, &deny, None, "keep"),
            (PushdownPolicy::Strict, &[], None, "keep"),
            (PushdownPolicy::Strict, &deny, None, "keep"),
        ];
        for (policy, deny, cost, expected) in cases {
            let got = summarize(&decide_with(&expr, policy, deny, &[], cost));
            assert_eq!(got, expected, "policy={policy:?}");
        }

        // Untranslatable expressions keep under every policy, even always.
        let untranslatable = col("a") + col("b");
        for policy in [
            PushdownPolicy::Always,
            PushdownPolicy::Never,
            PushdownPolicy::CostBased,
            PushdownPolicy::Strict,
            PushdownPolicy::Hinted,
        ] {
            assert!(
                matches!(
                    decide_with(&untranslatable, policy, &[], &[], None),
                    Decision::Keep
                ),
                "untranslatable must keep under {policy:?}"
            );
        }
    }

    /// Skewed fixture: `id` is highly selective *and* indexed, `status` has two values,
    /// `amount` is a float (Inexact). This is the Phase 2 exit-criterion shape — the snapshot
    /// below pins the exact decision vector per policy, so any stats or model change that
    /// quietly disables pushdown shows up as a diff.
    fn skewed_stats() -> crate::pushdown::stats::SourceStatistics {
        use crate::pushdown::stats::ColumnStats;
        use std::collections::HashMap;

        let mut columns = HashMap::new();
        columns.insert(
            "id".to_string(),
            ColumnStats {
                column_name: "id".to_string(),
                n_distinct: 100_000.0,
                null_frac: 0.0,
                avg_width: 8,
            },
        );
        columns.insert(
            "status".to_string(),
            ColumnStats {
                column_name: "status".to_string(),
                n_distinct: 2.0,
                null_frac: 0.0,
                avg_width: 10,
            },
        );
        columns.insert(
            "amount".to_string(),
            ColumnStats {
                column_name: "amount".to_string(),
                n_distinct: 50_000.0,
                null_frac: 0.0,
                avg_width: 8,
            },
        );
        columns.insert(
            "active".to_string(),
            ColumnStats {
                column_name: "active".to_string(),
                n_distinct: 2.0,
                null_frac: 0.0,
                avg_width: 1,
            },
        );
        columns.insert(
            "price".to_string(),
            ColumnStats {
                column_name: "price".to_string(),
                n_distinct: 50_000.0,
                null_frac: 0.0,
                avg_width: 8,
            },
        );
        crate::pushdown::stats::SourceStatistics {
            table_name: "orders".to_string(),
            row_count_estimate: 100_000.0,
            table_size_bytes: 10_000_000,
            columns,
            fetched_at: chrono::Utc::now(),
        }
    }

    fn skewed_column_types() -> HashMap<String, String> {
        HashMap::from([
            ("id".to_string(), "bigint".to_string()),
            ("status".to_string(), "text".to_string()),
            ("amount".to_string(), "double precision".to_string()),
            ("active".to_string(), "boolean".to_string()),
            ("price".to_string(), "numeric".to_string()),
        ])
    }

    fn indexed_id() -> Vec<crate::pushdown::stats::IndexInfo> {
        vec![crate::pushdown::stats::IndexInfo {
            name: "orders_pkey".to_string(),
            columns: vec!["id".to_string()],
            is_unique: true,
            is_primary: true,
            index_type: "btree".to_string(),
        }]
    }

    fn indexed_id_active_price() -> Vec<crate::pushdown::stats::IndexInfo> {
        let mut indexes = indexed_id();
        for (name, column) in [
            ("orders_active_idx", "active"),
            ("orders_price_idx", "price"),
        ] {
            indexes.push(crate::pushdown::stats::IndexInfo {
                name: name.to_string(),
                columns: vec![column.to_string()],
                is_unique: false,
                is_primary: false,
                index_type: "btree".to_string(),
            });
        }
        indexes
    }

    /// Compact snapshot form: "push:exact" / "push:inexact" / "keep".
    fn summarize(decision: &Decision) -> &'static str {
        match decision {
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

    fn decide_snapshot(
        policy: PushdownPolicy,
        deny: &[String],
        push: &[String],
        cost: Option<&CostInputs>,
    ) -> Vec<&'static str> {
        let filters = [
            col("id").eq(lit(42i64)),
            col("status").eq(lit("PAID")),
            col("amount").gt(lit(10.5f64)),
        ];
        filters
            .iter()
            .map(|f| summarize(&decide_with(f, policy, deny, push, cost)))
            .collect()
    }

    #[test]
    fn test_cost_based_differs_from_always() {
        use crate::pushdown::cost_model::CostParams;

        let stats = skewed_stats();
        let indexes = indexed_id();
        let params = CostParams::default();
        let column_types = skewed_column_types();
        let inputs = CostInputs {
            stats: &stats,
            params: &params,
            indexes: &indexes,
            explain: None,
            column_types: &column_types,
        };

        // `always` pushes everything translatable; `cost_based` keeps the two low-value
        // predicates (status: selectivity 1/2; amount: range estimate 0.33, both >= 0.30)
        // while still pushing the indexed id lookup.
        assert_eq!(
            decide_snapshot(PushdownPolicy::Always, &[], &[], None),
            vec!["push:exact", "push:inexact", "push:inexact"],
        );
        assert_eq!(
            decide_snapshot(PushdownPolicy::CostBased, &[], &[], Some(&inputs)),
            vec!["push:exact", "keep", "keep"],
        );
    }

    #[test]
    fn test_hinted_overrides_cost_model_but_not_deny() {
        use crate::pushdown::cost_model::CostParams;

        let stats = skewed_stats();
        let indexes = indexed_id();
        let params = CostParams::default();
        let column_types = skewed_column_types();
        let inputs = CostInputs {
            stats: &stats,
            params: &params,
            indexes: &indexes,
            explain: None,
            column_types: &column_types,
        };
        let push = vec!["status".to_string()];

        // A hint forces the 50%-selective status predicate to the source even though the
        // cost model keeps it; the unhinted amount predicate still follows the model.
        assert_eq!(
            decide_snapshot(PushdownPolicy::Hinted, &[], &push, Some(&inputs)),
            vec!["push:exact", "push:inexact", "keep"],
        );

        // Deny wins over push.
        let deny = vec!["status".to_string()];
        assert_eq!(
            decide_snapshot(PushdownPolicy::Hinted, &deny, &push, Some(&inputs)),
            vec!["push:exact", "keep", "keep"],
        );
    }

    #[test]
    fn test_strict_pushes_only_indexed_selective_primitive() {
        use crate::pushdown::cost_model::CostParams;

        let stats = skewed_stats();
        // id, active, and price are indexed; status and amount are not.
        let indexes = indexed_id_active_price();
        let params = CostParams::default();
        let column_types = skewed_column_types();
        let inputs = CostInputs {
            stats: &stats,
            params: &params,
            indexes: &indexes,
            explain: None,
            column_types: &column_types,
        };

        let filters = [
            col("id").eq(lit(42i64)),       // indexed + bigint + selective → push
            col("active").eq(lit(true)),    // indexed + boolean but 1/2 selectivity → keep
            col("status").eq(lit("PAID")),  // text and unindexed → keep
            col("amount").gt(lit(10.5f64)), // float → keep
            col("price").eq(lit(10i64)),    // indexed + selective but numeric → keep
        ];
        let got: Vec<&str> = filters
            .iter()
            .map(|f| {
                summarize(&decide_with(
                    f,
                    PushdownPolicy::Strict,
                    &[],
                    &[],
                    Some(&inputs),
                ))
            })
            .collect();
        assert_eq!(got, vec!["push:exact", "keep", "keep", "keep", "keep"]);

        // Without statistics strict keeps everything — paranoid, unlike the optimistic
        // legacy fallback.
        let got: Vec<&str> = filters
            .iter()
            .map(|f| summarize(&decide_with(f, PushdownPolicy::Strict, &[], &[], None)))
            .collect();
        assert_eq!(got, vec!["keep", "keep", "keep", "keep", "keep"]);

        // Gate reasons name the failing gate.
        let status = col("status").eq(lit("PAID"));
        let (_, pred) = translate(&status).unwrap();
        assert!(describe_strict(&pred, Some(&inputs)).contains("indexed"));
        let price = col("price").eq(lit(10i64));
        let (_, pred) = translate(&price).unwrap();
        assert!(describe_strict(&pred, Some(&inputs)).contains("primitive"));
    }

    fn enum_set() -> std::collections::HashSet<String> {
        std::collections::HashSet::from(["status".to_string()])
    }

    #[test]
    fn test_normalize_enum_text_comparison_casts_to_label() {
        // `status = 'PAID'` on an enum column becomes `"status"::text = 'PAID'`, Inexact so
        // Arrow still re-checks. Without this Postgres fails the pushed query with 42883
        // (no `order_status = text` operator).
        let (fidelity, pred) = translate(&col("status").eq(lit("PAID"))).unwrap();
        let (fidelity, pred) = normalize_enum_comparison(fidelity, pred, &enum_set()).unwrap();
        assert_eq!(fidelity, Fidelity::Inexact);
        assert_eq!(pred.render_inline(), "(\"status\"::text = 'PAID')");
    }

    #[test]
    fn test_normalize_enum_int_comparison_has_no_pushable_form() {
        // An integer literal against an enum column can be neither pushed natively nor via
        // the label cast (`text = integer` has no operator either) — keep in Arrow. This
        // also fixes an `Exact` hazard: the integer literal alone looks exact.
        let (fidelity, pred) = translate(&col("status").eq(lit(42i64))).unwrap();
        assert_eq!(fidelity, Fidelity::Exact);
        assert!(normalize_enum_comparison(fidelity, pred, &enum_set()).is_none());
    }

    #[test]
    fn test_normalize_leaves_non_enum_columns_alone() {
        // Same shapes, no enum membership: predicates pass through byte-identical.
        let (fidelity, pred) = translate(&col("status").eq(lit("PAID"))).unwrap();
        let (fidelity2, pred2) =
            normalize_enum_comparison(fidelity, pred, &std::collections::HashSet::new()).unwrap();
        assert_eq!(fidelity2, Fidelity::Inexact);
        assert_eq!(pred2.render_inline(), "(\"status\" = 'PAID')");

        // Non-comparisons pass through untouched too.
        let (fidelity, pred) = translate(&col("id").is_null()).unwrap();
        assert!(normalize_enum_comparison(fidelity, pred, &enum_set()).is_some());
    }

    #[test]
    fn test_render_inline_is_quoted_and_never_executed() {
        let pred = Predicate::Cmp {
            left: Box::new(Predicate::Column("status".to_string())),
            op: "=".to_string(),
            right: Box::new(Predicate::Literal(Literal::Text("PA'D".to_string()))),
        };
        assert_eq!(pred.render_inline(), "(\"status\" = 'PA''D')");

        let ts = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let pred = Predicate::Cmp {
            left: Box::new(Predicate::Column("updated_at".to_string())),
            op: ">".to_string(),
            right: Box::new(Predicate::Literal(Literal::Timestamp(ts))),
        };
        assert!(pred.render_inline().contains("::timestamptz"));
    }

    /// Test-only second dialect: backtick identifiers, `?` placeholders. Proves `render_sql`
    /// is backend-neutral — the same predicate tree renders for any connector, with params
    /// collected left-to-right for the backend's binder. (Not a MySQL implementation; Phase 5
    /// will bring the real one with its own collation and type rules.)
    struct QmarkDialect;

    impl SqlDialect for QmarkDialect {
        fn quote_ident(&self, name: &str) -> String {
            format!("`{}`", name.replace('`', "``"))
        }

        fn placeholder(&self, _param_index: usize) -> String {
            "?".to_string()
        }

        fn column_literal_fidelity(
            &self,
            _column: &crate::types::ColumnMetadata,
            _literal_is_text: bool,
            _literal_is_float: bool,
        ) -> Fidelity {
            Fidelity::Inexact
        }

        fn column_column_fidelity(
            &self,
            _left_column: &crate::types::ColumnMetadata,
            _right_column: &crate::types::ColumnMetadata,
        ) -> Fidelity {
            Fidelity::Inexact
        }
    }

    #[test]
    fn test_render_sql_is_dialect_neutral() {
        use crate::pushdown::dialect::PostgresDialect;

        let pred = Predicate::And(
            Box::new(Predicate::Cmp {
                left: Box::new(Predicate::Column("status".to_string())),
                op: "=".to_string(),
                right: Box::new(Predicate::Literal(Literal::Text("PAID".to_string()))),
            }),
            Box::new(Predicate::Cmp {
                left: Box::new(Predicate::Column("id".to_string())),
                op: ">".to_string(),
                right: Box::new(Predicate::Literal(Literal::Int(42))),
            }),
        );

        let pg = PostgresDialect;
        let mut pg_params = Vec::new();
        let pg_sql = pred.render_sql(&pg, &mut pg_params);
        assert_eq!(pg_sql, "((\"status\" = $1) AND (\"id\" > $2))");
        assert_eq!(
            pg_params,
            vec![SqlParam::Text("PAID".to_string()), SqlParam::Int(42),],
        );

        let qmark = QmarkDialect;
        let mut qmark_params = Vec::new();
        let qmark_sql = pred.render_sql(&qmark, &mut qmark_params);
        assert_eq!(qmark_sql, "((`status` = ?) AND (`id` > ?))");
        // Same params, same order, regardless of dialect.
        assert_eq!(qmark_params, pg_params);
    }
}
