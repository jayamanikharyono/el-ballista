// Library exports for extraction layer
pub mod checkpoint;
pub mod config;
pub mod connector;
pub mod errors;
pub mod logging;
pub mod types;

// These modules physically live under `connector::postgres` (see AGENTS.md — Postgres
// connector modularization). They are re-exported at the crate root as transitional
// compatibility shims; new code should prefer the `connector::postgres::*` paths and the
// `connector::postgres::PostgresConnector` entry point.
pub mod pushdown; // shared, connector-agnostic (dialects live under each connector)
pub use connector::postgres::{distributed, engine, incremental, pipeline};
