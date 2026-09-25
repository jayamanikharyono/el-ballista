// Library exports for extraction layer
pub mod checkpoint;
pub mod config;
pub mod connector;
pub mod errors;
pub mod logging;
pub mod telemetry;
pub mod types;

// Postgres extraction (pipeline, engine, distributed execution) lives under
// `connector::postgres` (AGENTS.md — Postgres connector modularization); the entry point is
// `connector::postgres::PostgresConnector`.
pub mod pushdown; // shared, connector-agnostic (dialects live under each connector)
