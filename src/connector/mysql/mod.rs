//! MySQL connector — EXPERIMENTAL prototype (walking skeleton).
//!
//! Purpose: a deliberately minimal second connector to drive out the connector abstraction
//! (AGENTS.md: "Before introducing abstraction — required now, not hypothetical"). It exercises
//! the shared seams — [`SqlDialect`], the shared [`TableMetadata`]/[`ColumnMetadata`], catalog
//! reading — through a non-Postgres backend so we can see which abstractions hold and which leak.
//!
//! Scope (prototype): connect, read schema from `information_schema`, and full-table extract to
//! Arrow. NOT included yet: pushdown, parallel/distributed execution,
//! or bounded-memory cursor streaming. Columns decode to typed Arrow arrays via [`row_adapter`]
//! (width-preserving integers, `Decimal128`, `Boolean`, dates/timestamps, `Binary`, `Utf8`
//! otherwise); `fetch_all` still materializes the whole result in memory — prototype only.
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
pub mod row_adapter;
pub mod schema_reader;
pub mod type_mapper;

pub use dialect::MysqlDialect;
pub use extractor::MysqlExtractor;
