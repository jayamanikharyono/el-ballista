//! MySQL connector — EXPERIMENTAL prototype (walking skeleton).
//!
//! Purpose: a deliberately minimal second connector to drive out the connector abstraction
//! (AGENTS.md: "Before introducing abstraction — required now, not hypothetical"). It exercises
//! the shared seams — [`SqlDialect`], the shared [`TableMetadata`]/[`ColumnMetadata`], catalog
//! reading — through a non-Postgres backend so we can see which abstractions hold and which leak.
//!
//! Scope (prototype): connect, read schema from `information_schema`, and full-table extract to
//! Arrow — streamed in `batch_size` batches ([`MysqlExtractor::extract_full_table_for_each_batch`])
//! or materialized into one batch ([`MysqlExtractor::extract_full_table`]). NOT included yet:
//! filters/pushdown, parallel/distributed execution, a `TableProvider`, or checkpointed jobs.
//! Columns decode to typed Arrow arrays via [`row_adapter`] per the table in [`type_mapper`]
//! (lossless unsigned integers, `BOOLEAN` → `Boolean`, `Decimal128`, dates/timestamps, `TIME` →
//! `Duration`, `Binary`, `Utf8` otherwise). Errors are the typed [`MysqlError`].
//!
//! Abstraction findings this prototype surfaced (see `docs/connector-abstraction.md`):
//! - [`SqlDialect`] had to be shared: it now lives in the connector-agnostic
//!   `crate::pushdown::dialect` (done).
//! - [`ExtractorError`](crate::connector::errors::ExtractorError) messages were Postgres-worded;
//!   they are backend-neutral now (done).
//! - MySQL uses `?` placeholders and backtick quoting and has no `ctid` — parallel partitioning
//!   must be keyset-only for MySQL (still open: no parallel MySQL scans yet).
//!
//! [`SqlDialect`]: crate::pushdown::dialect::SqlDialect
//! [`TableMetadata`]: crate::types::TableMetadata
//! [`ColumnMetadata`]: crate::types::ColumnMetadata

pub mod dialect;
pub mod error;
pub mod extractor;
pub mod query_builder;
pub mod row_adapter;
pub mod schema_reader;
pub mod type_mapper;

pub use dialect::MysqlDialect;
pub use error::MysqlError;
pub use extractor::MysqlExtractor;
