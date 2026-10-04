//! The PostgreSQL connector.
//!
//! Public API (AGENTS.md §6, "Callers use the connector entry point"):
//! - [`PostgresConnector`]: `from_config(cfg)?.extract()`, then `.standalone()` or
//!   `.distributed()`, finishing with `run_with` / `stream` / `collect` / `run`;
//! - [`register_table`] / [`PostgresTableProvider::from_config`]: the job's table in your own
//!   DataFusion `SessionContext` (standalone);
//! - [`ExtractContext`]: a DataFrame builder over the job's source;
//! - [`distributed::DistributedContext`]: a lower-level distributed session.
//!
//! Everything else (scans, decoding, pushdown dialect, the pipeline, the Ballista codecs'
//! internals) is crate-private.

pub(crate) mod arrow_type_mapper;
pub(crate) mod copy;
pub(crate) mod dialect;
pub(crate) mod execution_plan;
pub(crate) mod explain;
pub(crate) mod extractor;
pub(crate) mod inline_sql;
pub(crate) mod parallel;
pub(crate) mod param_sink;
mod query_builder;
pub(crate) mod row_adapter;
pub(crate) mod schema_reader;
pub(crate) mod stats;
pub(crate) mod table_provider;

pub mod distributed;
pub(crate) mod engine;
pub(crate) mod pipeline;

mod api;

pub use api::{
    DEFAULT_SCHEDULER_URL, DistributedExtraction, ExtractBuilder, PostgresConnector,
    StandaloneExtraction,
};
pub use engine::{ExtractContext, POSTGRES_CONNECTOR_REF, SourceDataFrame};
pub use pipeline::{FilterDecision, RunOutcome, SplitInfo, parse_filter_expr};
pub use table_provider::{PostgresTableProvider, register_table};

/// Close every source connection pool this process opened, gracefully. Pools are shared
/// process-wide and otherwise live until exit; call this once at shutdown, after the last
/// extraction.
///
/// # Examples
///
/// ```no_run
/// # async fn demo() {
/// el_ballista::connector::postgres::close_pools().await;
/// # }
/// ```
pub async fn close_pools() {
    distributed::pool_registry::registry().close_all().await;
}

/// Not public API: hooks for this crate's own live-database tests (catalog statistics,
/// EXPLAIN estimates, inline SQL rendering). May change or disappear in any release.
#[doc(hidden)]
pub mod internals {
    pub use super::explain::ExplainEstimator;
    pub use super::inline_sql::PredicateInlineSql;
    pub use crate::pushdown::stats::TableStatsSource;
}
