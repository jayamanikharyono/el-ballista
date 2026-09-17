//! MySQL connector — EXPERIMENTAL prototype (walking skeleton).
//!
//! Purpose: a deliberately minimal second connector to drive out the connector abstraction
//! (AGENTS.md: "Before introducing abstraction — required now, not hypothetical"). It exercises
//! the shared seams — [`SqlDialect`], the shared [`TableMetadata`]/[`ColumnMetadata`], catalog
//! reading — through a non-Postgres backend so we can see which abstractions hold and which leak.
//!
//! Scope (prototype): connect, read schema from `information_schema`, and full-table extract to
//! Arrow. NOT included yet: pushdown, incremental watermarks, parallel/distributed execution,
//! bounded-memory cursor streaming, or rich per-type Arrow decoding (the prototype materializes
//! every column as `Utf8` via `CAST(... AS CHAR)` — see [`extractor`]). [`type_mapper`] documents
//! the intended Arrow types for the next step.
//!
//! Abstraction findings this prototype surfaces (act on these when promoting MySQL past prototype):
//! - [`SqlDialect`] and `Fidelity` live under `connector::postgres::pushdown`; a second dialect
//!   needs them, so they should be lifted to a shared, connector-agnostic module.
//! - [`ExtractorError`](crate::connector::errors::ExtractorError) messages are Postgres-worded;
//!   make them backend-neutral.
//! - MySQL uses `?` placeholders and backtick quoting and has no `ctid` — parallel partitioning
//!   must be keyset-only for MySQL.
//!
//! [`SqlDialect`]: crate::pushdown::dialect::SqlDialect
//! [`TableMetadata`]: crate::types::TableMetadata
//! [`ColumnMetadata`]: crate::types::ColumnMetadata

pub mod dialect;
pub mod extractor;
pub mod query_builder;
pub mod schema_reader;
pub mod type_mapper;

pub use dialect::MysqlDialect;
pub use extractor::MysqlExtractor;
