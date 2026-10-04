//! Caller-provided filters: parsing (shorthand + structured), lowering to DataFusion
//! predicates, schema-aware coercion, and the pushdown preview.
//!
//! Every path (`collect` / `stream` / `run` / `run_with`, standalone or distributed,
//! `el-ballista plan`) funnels through [`Pipeline::filter_exprs_with_schema`], so the config, the
//! CLI, the preview and the provider can never disagree about what a filter means.

use arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::prelude::{ident, lit};

use super::Pipeline;
use crate::config::{FilterEntry, FilterInput, FilterOp, FilterSpec};
use crate::errors::AppError;

/// What the extraction layer will do with one caller-provided filter: the parsed
/// predicate plus the decision whether it executes in the source database.
/// Returned by [`PostgresConnector::explain_filters`](crate::connector::postgres::PostgresConnector::explain_filters)
/// — the programmatic form of what
/// `el-ballista plan` prints.
#[derive(Debug, Clone)]
pub struct FilterDecision {
    /// Canonical display of the filter (`status = 'PAID'`), regardless of whether
    /// the job spec used the shorthand or structured form.
    pub filter: String,
    /// The parsed DataFusion predicate (what actually executes).
    pub expr: Expr,
    /// DataFusion's pushdown verdict for this filter.
    pub pushdown: TableProviderFilterPushDown,
    /// True when the filter executes in the source database (`Exact` or `Inexact`).
    /// `Inexact` still re-checks in Arrow; `Unsupported` stays in Arrow entirely.
    pub pushed_to_source: bool,
    /// Human-readable reason (same text `el-ballista plan` prints).
    pub reason: String,
}

impl Pipeline {
    /// The job's filters grouped by AND-conjunct: one inner vec per outer
    /// `filters` entry. A `Single` yields one spec; an `OrGroup` yields its
    /// branches in order. Pure: no I/O, safe to call for validation alone.
    pub(crate) fn filter_groups(&self) -> Result<Vec<Vec<FilterSpec>>, AppError> {
        self.config
            .filters
            .iter()
            .map(|entry| match entry {
                FilterEntry::Single(input) => resolve_input(input).map(|spec| vec![spec]),
                FilterEntry::OrGroup(inputs) => {
                    if inputs.is_empty() {
                        return Err(AppError::Config(
                            "filters contains an empty OR-group (inner array must hold >= 1 predicate)".to_string(),
                        ));
                    }
                    inputs.iter().map(resolve_input).collect()
                }
            })
            .collect()
    }

    /// The job's filters as parsed DataFusion predicates — the single choke point
    /// every extraction path funnels through, so config entries, the CLI, and the
    /// provider can never disagree about what a filter means. Pure: no I/O, safe
    /// to call for validation alone. One `Expr` per AND-conjunct: an OR-group
    /// lowers to a single `a OR b` expression. Literals follow the JSON value
    /// types; string values stay text (for timestamp/date coercion use
    /// `Pipeline::filter_exprs_with_schema`).
    pub fn filter_exprs(&self) -> Result<Vec<Expr>, AppError> {
        self.filter_groups()?
            .iter()
            .map(|group| {
                group
                    .iter()
                    .map(FilterSpec::to_expr)
                    .collect::<Result<Vec<_>, _>>()
                    .and_then(or_group)
            })
            .collect()
    }

    /// Like [`Pipeline::filter_exprs`], but string values are coerced against the
    /// extraction schema: an RFC3339 (or `%Y-%m-%d`) string on a timestamp column
    /// becomes a real timestamp literal that pushes to the source — so
    /// orchestrator-supplied time ranges push instead of degrading to Arrow-side
    /// filtering after a full scan. A `%Y-%m-%d` string on a date column becomes
    /// a date literal, which pushes the same way.
    pub(crate) fn filter_exprs_with_schema(&self, schema: &Schema) -> Result<Vec<Expr>, AppError> {
        self.filter_groups()?
            .iter()
            .map(|group| {
                group
                    .iter()
                    .map(|spec| {
                        let dtype = schema
                            .index_of(spec.column.trim())
                            .ok()
                            .map(|i| schema.field(i).data_type());
                        spec.to_expr_with_type(dtype)
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .and_then(or_group)
            })
            .collect()
    }

    /// Preview what the extraction layer will do with each caller-provided filter:
    /// the parsed predicate plus whether it pushes to the source database under the
    /// job's pushdown policy. One entry per AND-conjunct — an OR-group previews
    /// as a single `(a OR b)` decision, because that is the unit `scan()` decides
    /// on. Same decision point `scan()` uses, so the preview can never disagree
    /// with execution. Use it to decide — before extracting — which
    /// filters need a source index and which will run in Arrow.
    pub async fn explain_filters(&self) -> Result<Vec<FilterDecision>, AppError> {
        let provider = self.provider().await?;
        let groups = self.filter_groups()?;
        let exprs = self.filter_exprs_with_schema(&provider.schema())?;
        let refs: Vec<&Expr> = exprs.iter().collect();
        // Warm EXPLAIN estimates first so cost-based decisions use them, exactly as
        // a warmed production provider would.
        provider.warm_explain(&exprs).await;
        let verdicts = provider.supports_filters_pushdown(&refs)?;
        // Reasons for the whole set at once: a range filter is judged together with the
        // other ranges on its column, exactly as `supports_filters_pushdown` just did.
        let reasons = provider.explain_decisions(&refs);

        Ok(groups
            .iter()
            .zip(exprs)
            .zip(verdicts)
            .zip(reasons)
            .map(|(((group, expr), pushdown), reason)| {
                let pushed_to_source = pushdown != TableProviderFilterPushDown::Unsupported;
                FilterDecision {
                    filter: describe_group(group),
                    expr,
                    pushdown,
                    pushed_to_source,
                    reason,
                }
            })
            .collect())
    }
}

/// Resolve one [`FilterInput`] to its [`FilterSpec`] — shorthand strings parsed,
/// structured entries passed through. Shared by the single and OR-group paths so
/// both JSON forms lower identically.
fn resolve_input(input: &FilterInput) -> Result<FilterSpec, AppError> {
    match input {
        FilterInput::Shorthand(raw) => parse_filter_shorthand(raw),
        FilterInput::Structured(spec) => Ok(spec.clone()),
    }
}

/// Fold one AND-conjunct's branch predicates into a single `Expr`: a singleton
/// stays as-is, an OR-group becomes `a OR b OR ...` left-associatively.
fn or_group(exprs: Vec<Expr>) -> Result<Expr, AppError> {
    let mut iter = exprs.into_iter();
    let Some(first) = iter.next() else {
        return Err(AppError::Config(
            "filters contains an empty OR-group (inner array must hold >= 1 predicate)".to_string(),
        ));
    };
    Ok(iter.fold(first, |acc, e| acc.or(e)))
}

/// Canonical display for one AND-conjunct: a singleton renders as its spec,
/// an OR-group as `(a OR b)`.
pub(super) fn describe_group(group: &[FilterSpec]) -> String {
    match group {
        [] => "(empty OR-group)".to_string(),
        [single] => single.describe(),
        _ => format!(
            "({})",
            group
                .iter()
                .map(FilterSpec::describe)
                .collect::<Vec<_>>()
                .join(" OR ")
        ),
    }
}

/// Parse a shorthand filter `column<op>value` where `<op>` is one of
/// `= != > >= < <=` into a [`FilterSpec`]. Longer operators are checked before
/// their single-character prefixes (`!=`/`>=`/`<=` before `=`/`>`/`<`) so e.g.
/// `amount>=100` doesn't get mis-split on the `=`. Shared by the pipeline
/// (config `filters`) and the CLI (`--filter` flags); both funnel through here
/// so they can never disagree.
pub fn parse_filter_shorthand(raw: &str) -> Result<FilterSpec, AppError> {
    const OPS: [(&str, usize, FilterOp); 6] = [
        ("!=", 2, FilterOp::NotEq),
        (">=", 2, FilterOp::GtEq),
        ("<=", 2, FilterOp::LtEq),
        ("=", 1, FilterOp::Eq),
        (">", 1, FilterOp::Gt),
        ("<", 1, FilterOp::Lt),
    ];

    for (op_str, op_len, op) in OPS {
        if let Some(idx) = raw.find(op_str) {
            let column = raw[..idx].trim();
            if column.is_empty() {
                break;
            }
            let value_str = raw[idx + op_len..].trim();
            return Ok(FilterSpec {
                column: column.to_string(),
                op,
                value: shorthand_value(value_str),
            });
        }
    }

    Err(AppError::Config(format!(
        "cannot parse filter '{raw}' — expected 'column<op>value' with op one of = != > >= < <="
    )))
}

/// Parse a shorthand filter all the way to a DataFusion predicate (no schema
/// coercion — string values stay text). A job's own filters come coerced to the table
/// schema from `PostgresConnector::filter_exprs` and every run.
///
/// # Examples
///
/// ```
/// use datafusion::prelude::{ident, lit};
/// use el_ballista::connector::postgres::parse_filter_expr;
///
/// let expr = parse_filter_expr("status='PAID'")?;
/// assert_eq!(expr, ident("status").eq(lit("PAID")));
/// # Ok::<(), el_ballista::errors::AppError>(())
/// ```
pub fn parse_filter_expr(raw: &str) -> Result<Expr, AppError> {
    parse_filter_shorthand(raw)?.to_expr()
}

/// Infer a JSON value from a shorthand value string, mirroring JSON typing. A value
/// wrapped in one pair of matching quotes (`'007'`, `"PAID"`) is always a string, with the
/// quotes removed — quoting is how a caller keeps `zip='007'` from becoming the integer 7.
/// Unquoted integers, floats and booleans become their native types; everything else stays
/// text (so `'PAID'` and `PAID` agree).
fn shorthand_value(raw: &str) -> serde_json::Value {
    for quote in ['\'', '"'] {
        if let Some(inner) = raw
            .strip_prefix(quote)
            .and_then(|rest| rest.strip_suffix(quote))
        {
            return serde_json::Value::String(inner.to_string());
        }
    }
    let unquoted = raw;

    if let Ok(v) = unquoted.parse::<i64>() {
        v.into()
    } else if let Ok(v) = unquoted.parse::<f64>() {
        serde_json::Number::from_f64(v).map_or_else(
            || serde_json::Value::String(unquoted.to_string()),
            serde_json::Value::Number,
        )
    } else if unquoted.eq_ignore_ascii_case("true") {
        true.into()
    } else if unquoted.eq_ignore_ascii_case("false") {
        false.into()
    } else {
        serde_json::Value::String(unquoted.to_string())
    }
}

impl FilterSpec {
    /// Canonical display (`status = 'PAID'`) for logs, `el-ballista plan`, and previews.
    pub(crate) fn describe(&self) -> String {
        match self.op {
            FilterOp::IsNull => format!("{} is null", self.column.trim()),
            FilterOp::IsNotNull => format!("{} is not null", self.column.trim()),
            _ => format!(
                "{} {} {}",
                self.column.trim(),
                self.op.as_str(),
                render_filter_json_value(&self.value)
            ),
        }
    }

    /// Lower to a DataFusion predicate from the JSON value types alone: numbers
    /// become int/float literals, booleans boolean literals, strings text
    /// literals, null an `IS NULL` / `IS NOT NULL` check.
    pub(crate) fn to_expr(&self) -> Result<Expr, AppError> {
        self.to_expr_with_type(None)
    }

    /// Like [`FilterSpec::to_expr`], but a string value is coerced when the
    /// column's Arrow type says more: an RFC3339 (or `%Y-%m-%d`) string on a
    /// timestamp column becomes a real timestamp literal that pushes to the
    /// source; a `%Y-%m-%d` string on a date column becomes a date literal that
    /// pushes too (as a `date` comparison, so a plain index on the column can
    /// serve it). Anything else falls back to
    /// [`FilterSpec::to_expr`].
    pub(crate) fn to_expr_with_type(&self, dtype: Option<&DataType>) -> Result<Expr, AppError> {
        let column = self.column.trim();
        if column.is_empty() {
            return Err(AppError::Config(
                "filter column must not be empty".to_string(),
            ));
        }

        match self.op {
            FilterOp::IsNull => return Ok(ident(column).is_null()),
            FilterOp::IsNotNull => return Ok(ident(column).is_not_null()),
            _ => {}
        }

        let value = match &self.value {
            serde_json::Value::Null => match self.op {
                FilterOp::Eq => return Ok(ident(column).is_null()),
                FilterOp::NotEq => return Ok(ident(column).is_not_null()),
                _ => {
                    return Err(AppError::Config(format!(
                        "filter '{column}' uses null with '{}': only '=' / '!=' apply to null (or use is_null)",
                        self.op.as_str()
                    )));
                }
            },
            serde_json::Value::Bool(b) => lit(*b),
            serde_json::Value::Number(n) => {
                if let Some(v) = n.as_i64() {
                    lit(v)
                } else if let Some(v) = n.as_f64() {
                    lit(v)
                } else {
                    return Err(AppError::Config(format!(
                        "filter '{column}' has an out-of-range numeric value: {n}"
                    )));
                }
            }
            serde_json::Value::String(s) => {
                if let Some(expr) = coerce_string_to_type(s, dtype) {
                    expr
                } else {
                    lit(s.as_str())
                }
            }
            _ => {
                return Err(AppError::Config(format!(
                    "filter '{column}' value must be a number, boolean, string, or null"
                )));
            }
        };

        Ok(match self.op {
            FilterOp::Eq => ident(column).eq(value),
            FilterOp::NotEq => ident(column).not_eq(value),
            FilterOp::Gt => ident(column).gt(value),
            FilterOp::GtEq => ident(column).gt_eq(value),
            FilterOp::Lt => ident(column).lt(value),
            FilterOp::LtEq => ident(column).lt_eq(value),
            FilterOp::IsNull | FilterOp::IsNotNull => unreachable!("handled above"),
        })
    }
}

fn render_filter_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{s}'"),
        _ => "(complex value)".to_string(),
    }
}

/// Try to read a string as a timestamp literal for the given column type.
/// Accepts RFC3339 (`2026-01-01T00:00:00Z`) and, for convenience, plain
/// `%Y-%m-%d` dates (midnight UTC). Returns `None` when the string is not a
/// timestamp or the column type is not temporal — the caller keeps the text
/// literal instead.
fn coerce_string_to_type(s: &str, dtype: Option<&DataType>) -> Option<Expr> {
    let dtype = dtype?;
    let s = s.trim();
    match dtype {
        DataType::Timestamp(unit, tz) => {
            let micros = parse_timestamp_micros(s)?;
            let value = match unit {
                TimeUnit::Second => {
                    ScalarValue::TimestampSecond(Some(micros / 1_000_000), tz.clone())
                }
                TimeUnit::Millisecond => {
                    ScalarValue::TimestampMillisecond(Some(micros / 1_000), tz.clone())
                }
                TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(micros), tz.clone())
                }
                TimeUnit::Nanosecond => {
                    ScalarValue::TimestampNanosecond(Some(micros.checked_mul(1_000)?), tz.clone())
                }
            };
            Some(lit(value))
        }
        DataType::Date32 => {
            let days = parse_date_days(s)?;
            Some(lit(ScalarValue::Date32(Some(days))))
        }
        DataType::Date64 => {
            let days = parse_date_days(s)?;
            Some(lit(ScalarValue::Date64(Some(days as i64 * 86_400_000))))
        }
        _ => None,
    }
}

fn parse_timestamp_micros(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_micros());
    }
    // Plain dates read as midnight UTC — the common daily-range shorthand.
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(d.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros());
    }
    None
}

fn parse_date_days(s: &str) -> Option<i32> {
    let d = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()?;
    let days = d
        .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?)
        .num_days();
    i32::try_from(days).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::JobConfig;
    use datafusion::prelude::col;

    #[test]
    fn from_config_file_loads_extract_job() {
        let pipeline =
            Pipeline::from_config_file("examples/configs/full_extract.example.json").unwrap();
        assert_eq!(pipeline.config().job_id, "payment_full");
    }

    #[test]
    fn example_config_filters_parse_to_pushable_predicates() {
        // extract.example.json ships four ANDed structured filters (the pure-AND
        // case). OR-group lowering is pinned separately by
        // `or_group_lowers_to_single_or_expr`, which builds its config
        // synthetically instead of coupling to this fixture.
        let config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        let pipeline = Pipeline::from_config(config).unwrap();
        assert_eq!(pipeline.config().job_id, "payment_extract");
        // Flat specs: 4 predicates across 4 AND-conjuncts.
        let specs: Vec<FilterSpec> = pipeline
            .filter_groups()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(specs.len(), 4);
        assert_eq!(
            specs[0].describe(),
            "payment_date >= '2007-04-06T00:00:00Z'"
        );
        assert_eq!(specs[1].describe(), "payment_date < '2007-04-13T00:00:00Z'");
        assert_eq!(specs[2].describe(), "customer_id >= 300");
        assert_eq!(specs[3].describe(), "amount > 5");
        // Grouped: 4 singleton conjuncts, each lowering to its own expression.
        let groups = pipeline.filter_groups().unwrap();
        assert_eq!(groups.len(), 4);
        assert!(groups.iter().all(|g| g.len() == 1));
        let exprs = pipeline.filter_exprs().unwrap();
        assert_eq!(exprs.len(), 4);
        assert_eq!(exprs[2], col("customer_id").gt_eq(lit(300i64)));
        assert_eq!(exprs[3], col("amount").gt(lit(5i64)));
    }

    #[test]
    fn or_group_lowers_to_single_or_expr() {
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.filters = vec![FilterEntry::OrGroup(vec![
            FilterInput::Shorthand("status=PAID".to_string()),
            FilterInput::Shorthand("amount>100".to_string()),
        ])];
        let pipeline = Pipeline::from_config(config).unwrap();
        let exprs = pipeline.filter_exprs().unwrap();
        assert_eq!(exprs.len(), 1);
        assert_eq!(
            exprs[0],
            col("status")
                .eq(lit("PAID"))
                .or(col("amount").gt(lit(100i64)))
        );
        let decisions = pipeline.filter_groups().unwrap();
        assert_eq!(
            describe_group(&decisions[0]),
            "(status = 'PAID' OR amount > 100)"
        );
    }

    #[test]
    fn empty_or_group_is_config_error_not_empty_scan() {
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.filters = vec![FilterEntry::OrGroup(vec![])];
        // Rejected when the pipeline is built (validation) ...
        assert!(Pipeline::from_config(config.clone()).is_err());
        // ... and by the lowering itself, should a caller bypass validation.
        let pipeline = Pipeline { config };
        assert!(pipeline.filter_exprs().is_err());
        assert!(pipeline.filter_groups().is_err());
    }

    #[test]
    fn parse_filter_expr_shapes() {
        let expr = parse_filter_expr("status=PAID").unwrap();
        assert_eq!(expr, col("status").eq(lit("PAID")));
        let expr = parse_filter_expr("amount>=100").unwrap();
        assert_eq!(expr, col("amount").gt_eq(lit(100i64)));
        assert!(parse_filter_expr("not-a-filter").is_err());
    }

    #[test]
    fn filter_exprs_parses_every_config_filter() {
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.filters = vec![
            FilterEntry::Single(FilterInput::Shorthand("status=PAID".to_string())),
            FilterEntry::Single(FilterInput::Shorthand("amount>100".to_string())),
        ];
        let pipeline = Pipeline::from_config(config).unwrap();
        let exprs = pipeline.filter_exprs().unwrap();
        assert_eq!(exprs.len(), 2);
        assert_eq!(exprs[0], col("status").eq(lit("PAID")));
        assert_eq!(exprs[1], col("amount").gt(lit(100i64)));
    }

    #[test]
    fn filter_exprs_surfaces_the_bad_filter() {
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.filters = vec![
            FilterEntry::Single(FilterInput::Shorthand("status=PAID".to_string())),
            FilterEntry::Single(FilterInput::Shorthand("not-a-filter".to_string())),
        ];
        let pipeline = Pipeline::from_config(config).unwrap();
        let err = pipeline.filter_exprs().unwrap_err();
        assert!(err.to_string().contains("not-a-filter"));
    }

    #[test]
    fn filter_exprs_empty_means_full_extraction() {
        let config = JobConfig::from_file("examples/configs/full_extract.example.json").unwrap();
        let pipeline = Pipeline::from_config(config).unwrap();
        assert!(pipeline.filter_exprs().unwrap().is_empty());
    }

    fn mk_spec(column: &str, op: FilterOp, value: serde_json::Value) -> FilterSpec {
        FilterSpec {
            column: column.to_string(),
            op,
            value,
        }
    }

    #[test]
    fn filter_spec_lowering_follows_json_types() {
        assert_eq!(
            mk_spec("a", FilterOp::Eq, serde_json::json!(100))
                .to_expr()
                .unwrap(),
            col("a").eq(lit(100i64))
        );
        assert_eq!(
            mk_spec("a", FilterOp::Gt, serde_json::json!(1.5))
                .to_expr()
                .unwrap(),
            col("a").gt(lit(1.5f64))
        );
        assert_eq!(
            mk_spec("a", FilterOp::Eq, serde_json::json!(true))
                .to_expr()
                .unwrap(),
            col("a").eq(lit(true))
        );
        assert_eq!(
            mk_spec("a", FilterOp::Eq, serde_json::json!("PAID"))
                .to_expr()
                .unwrap(),
            col("a").eq(lit("PAID"))
        );
        assert_eq!(
            mk_spec("a", FilterOp::Eq, serde_json::json!(null))
                .to_expr()
                .unwrap(),
            col("a").is_null()
        );
        assert_eq!(
            mk_spec("a", FilterOp::NotEq, serde_json::json!(null))
                .to_expr()
                .unwrap(),
            col("a").is_not_null()
        );
        assert_eq!(
            mk_spec("a", FilterOp::IsNull, serde_json::json!(null))
                .to_expr()
                .unwrap(),
            col("a").is_null()
        );
    }

    #[test]
    fn filter_spec_rejects_misused_null_and_nested_values() {
        assert!(
            mk_spec("a", FilterOp::Gt, serde_json::json!(null))
                .to_expr()
                .is_err()
        );
        assert!(
            mk_spec("a", FilterOp::Eq, serde_json::json!({"n": 1}))
                .to_expr()
                .is_err()
        );
        assert!(
            mk_spec("", FilterOp::Eq, serde_json::json!(1))
                .to_expr()
                .is_err()
        );
    }

    #[test]
    fn shorthand_and_structured_lower_identically() {
        // The two JSON forms must agree predicate-for-predicate.
        for raw in ["status=PAID", "amount>=100", "flag=true", "ratio<1.5"] {
            let from_shorthand = parse_filter_expr(raw).unwrap();
            let spec = parse_filter_shorthand(raw).unwrap();
            assert_eq!(from_shorthand, spec.to_expr().unwrap(), "{raw}");
            assert_eq!(spec.describe(), spec.describe());
        }
    }

    #[test]
    fn timestamp_string_coerces_with_schema_but_not_without() {
        use arrow::datatypes::{DataType, TimeUnit};

        let spec = parse_filter_shorthand("updated_at>=2026-01-01T00:00:00Z").unwrap();
        // Without schema knowledge the bound stays text (documented limitation).
        assert_eq!(
            spec.to_expr().unwrap(),
            col("updated_at").gt_eq(lit("2026-01-01T00:00:00Z"))
        );

        // With a timestamp column type it becomes a real timestamp literal that pushes.
        let dtype = DataType::Timestamp(TimeUnit::Microsecond, None);
        let expected_micros = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .timestamp_micros();
        assert_eq!(
            spec.to_expr_with_type(Some(&dtype)).unwrap(),
            col("updated_at").gt_eq(lit(ScalarValue::TimestampMicrosecond(
                Some(expected_micros),
                None
            )))
        );

        // Plain dates read as midnight UTC.
        let day = parse_filter_shorthand("updated_at>=2026-01-02").unwrap();
        let expected_day = chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(
            day.to_expr_with_type(Some(&dtype)).unwrap(),
            col("updated_at").gt_eq(lit(ScalarValue::TimestampMicrosecond(
                Some(expected_day),
                None
            )))
        );

        // A text column keeps the string — coercion never fires blindly.
        assert_eq!(
            spec.to_expr_with_type(Some(&DataType::Utf8)).unwrap(),
            col("updated_at").gt_eq(lit("2026-01-01T00:00:00Z"))
        );

        // Date columns coerce date strings.
        let holiday = mk_spec("day", FilterOp::Eq, serde_json::json!("2024-02-29"));
        let expected_days = chrono::NaiveDate::from_ymd_opt(2024, 2, 29)
            .unwrap()
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        assert_eq!(
            holiday.to_expr_with_type(Some(&DataType::Date32)).unwrap(),
            col("day").eq(lit(ScalarValue::Date32(Some(expected_days))))
        );
    }

    #[test]
    fn quoted_shorthand_values_stay_strings() {
        // `zip='007'` used to become the integer 7 (quotes stripped, then parsed).
        let spec = parse_filter_shorthand("zip='007'").unwrap();
        assert_eq!(spec.value, serde_json::json!("007"));
        assert_eq!(spec.to_expr().unwrap(), col("zip").eq(lit("007")));
        let spec = parse_filter_shorthand("flag=\"true\"").unwrap();
        assert_eq!(spec.value, serde_json::json!("true"));
        // Unquoted values keep JSON-like typing.
        assert_eq!(
            parse_filter_shorthand("zip=007").unwrap().value,
            serde_json::json!(7)
        );
        assert_eq!(
            parse_filter_shorthand("status='PAID'").unwrap().value,
            parse_filter_shorthand("status=PAID").unwrap().value
        );
        // A lone or mismatched quote is not a quoted value.
        assert_eq!(
            parse_filter_shorthand("name='x").unwrap().value,
            serde_json::json!("'x")
        );
    }

    #[test]
    fn filter_specs_mixes_both_json_forms() {
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.filters = vec![
            FilterEntry::Single(FilterInput::Shorthand("status=PAID".to_string())),
            FilterEntry::Single(FilterInput::Structured(mk_spec(
                "amount",
                FilterOp::Gt,
                serde_json::json!(100),
            ))),
        ];
        let pipeline = Pipeline::from_config(config).unwrap();
        let specs: Vec<FilterSpec> = pipeline
            .filter_groups()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].column, "status");
        assert_eq!(specs[0].describe(), "status = 'PAID'");
        assert_eq!(specs[1].describe(), "amount > 100");
        let exprs = pipeline.filter_exprs().unwrap();
        assert_eq!(exprs[1], col("amount").gt(lit(100i64)));
    }
}
