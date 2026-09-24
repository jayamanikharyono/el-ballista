pub mod arrow_type_mapper;
pub mod copy;
pub mod dialect;
pub mod execution_plan;
pub mod extractor;
pub mod parallel;
mod query_builder;
pub mod row_adapter;
pub mod schema_reader;
pub mod table_provider;

// Relocated under the Postgres connector (AGENTS.md: all Postgres code lives here).
pub mod distributed;
pub mod engine;
pub mod pipeline;

// Fluent extraction entry point: `PostgresConnector::from_config(cfg).extract().standalone()/.distributed()`.
pub mod api;
pub use api::{DEFAULT_SCHEDULER_URL, PostgresConnector};

pub use extractor::PostgresExtractor;
pub use table_provider::PostgresTableProvider;
