//! Source-aware pushdown: engine-agnostic predicate IR, translation, fidelity, and policy.
//! pushdown/mod.rs
//! A bounded implementation of docs/pushdown.md:
//! - [`translate`] / [`translate_with`]: the allowlist-based `Expr` → [`Predicate`] translator
//!   (§2), deciding fidelity per node from engine-neutral [`ColumnKind`]s (§3);
//! - [`Predicate::render_to`]: rendering through a [`dialect::SqlDialect`] into a [`SqlSink`];
//! - [`cost_model`] and the policy modes (§4.3) in [`decide_translated`].
//!
//! Nothing in this module is backend-specific. Each connector supplies its dialect, its
//! column-kind classification, its statistics ([`stats::TableStatsSource`]) and, optionally,
//! plan estimates ([`explain::ExplainEstimate`]); the Postgres pieces live under
//! `connector::postgres` (`dialect`, `param_sink`, `inline_sql`, `stats`, `explain`).
//!
//! The guiding rule: docs/roadmap.md names "pushdown that is fast and wrong" as the fatal
//! failure mode. `Inexact` means the source returns a *superset* that DataFusion re-checks, so a
//! translation that could return *fewer* rows than Arrow is never `Inexact` — it is not pushed.
//!
//! Still deferred: per-predicate hints, `IN`/`BETWEEN`/`LIKE`/arithmetic translation, and
//! aggregate/join pushdown.

pub mod cost_model;
pub mod dialect;
pub mod explain;
mod ir;
mod policy;
pub mod stats;
mod translate;

pub use cost_model::CostInputs;
pub use ir::{CastType, CmpOp, Collation, Literal, Predicate, SqlParam, SqlSink};
pub use policy::{
    Decision, PushdownPolicy, UnknownPushdownPolicy, decide_explained, decide_translated,
};
pub use translate::{ColumnKind, ColumnKinds, translate, translate_with};

/// How faithfully a pushed predicate reproduces Arrow semantics — docs/pushdown.md §1.
/// `Unsupported` isn't a variant here: an expression that can't be pushed correctly simply
/// isn't represented (`translate` returns `None`), and the caller reports `Unsupported`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Fidelity {
    /// The source returns exactly the rows Arrow keeps; DataFusion drops its own filter.
    Exact,
    /// The source returns a superset; DataFusion re-checks.
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
