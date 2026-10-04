//! Phase 4 (docs/roadmap.md): distributed execution over Ballista.
//! distributed/mod.rs
//! The scheduler and each worker are separate processes and can't share a live `PgPool`, so
//! scans travel as serialized plans (see `plan_codec` / `table_codec`) and each executing
//! process resolves its *budgeted* share of `pool_max` from the process-wide
//! `SourcePoolRegistry`. `DistributedContext::remote` connects to a running `el-ballista scheduler`
//! with `el-ballista worker` processes; there is no in-process Ballista (single-process extraction is
//! plain DataFusion: `connector::postgres::register_table` / `.standalone()`).

pub(crate) mod connection;
pub(crate) mod context;
pub(crate) mod executors;
pub(crate) mod plan_codec;
pub(crate) mod pool_registry;
pub(crate) mod table_codec;
pub(crate) mod watchdog;

pub use context::DistributedContext;
/// The Ballista extension codecs for Postgres scan plans. Public only so this crate's
/// `el-ballista scheduler` / `el-ballista worker` binaries can install them.
#[doc(hidden)]
pub use plan_codec::PostgresPhysicalCodec;
#[doc(hidden)]
pub use table_codec::PostgresLogicalCodec;
