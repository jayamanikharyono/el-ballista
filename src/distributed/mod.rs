//! Phase 4 (docs/roadmap.md): distributed execution over Ballista.
//! distributed/mod.rs
//! The scheduler and each worker are separate processes and can't share a live `PgPool`, so
//! scans travel as serialized plans (see `plan_codec` / `table_codec`) and each executing
//! process resolves its *budgeted* share of `pool_max` from the process-wide
//! `SourcePoolRegistry`. `DistributedContext` wires both modes — `standalone` (scheduler +
//! in-proc executor) and `remote` (a `rel scheduler` with `rel worker` processes).

pub mod connection;
pub mod context;
pub mod plan_codec;
pub mod pool_registry;
pub mod table_codec;

pub use connection::PostgresConnectionDescriptor;
pub use context::DistributedContext;
pub use plan_codec::PostgresPhysicalCodec;
pub use table_codec::PostgresLogicalCodec;