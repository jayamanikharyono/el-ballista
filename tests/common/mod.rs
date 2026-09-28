#![allow(dead_code, clippy::all)]
//! Shared Postgres test harness: the Docker compose stack, no embedded server.
//!
//! `TestDb::connect()` reads `DATABASE_URL` and, when unset, defaults to the compose
//! endpoint `postgres://postgres:postgres@127.0.0.1:5432/test`. It never self-provisions
//! and never skips: a failure to connect is a hard `panic!`, not a silent `None` —
//! masking a broken harness as "skipped" is exactly what produced false-green CI in the
//! past.
//!
//! ```bash
//! docker compose -f tests/docker/compose.yaml up -d --wait
//! cargo test --test pg_paths
//! docker compose -f tests/docker/compose.yaml down -v
//! ```
//!
//! Design: schema isolation (not database isolation) inside the one server — each `TestDb`
//! gets `test_<pid>_<n>`, loads the hostile fixture (`tests/data/hostile.sql`) inside it, and
//! drops the schema on `Drop` (fresh connection, bounded timeout, failures logged) or via
//! `TestDb::cleanup().await`.
//! Tests can run in parallel.
//!
//! Single implementation: [`postgres.rs`] holds the harness; this module re-exports it so
//! every suite shares one fixture, one provisioning rule, and one cleanup path.
//! (`scripts/e2e.sh` sets `DATABASE_URL` to the compose endpoint it brings up.)

#[path = "postgres.rs"]
mod postgres;

// Glob re-export (not an itemized list): each suite uses a different subset of the
// harness API, and an itemized `pub use` would trip `unused_imports` in the binaries
// that don't need every name.
pub use postgres::*;

// Real-cluster harness (`el-ballista scheduler` + `el-ballista worker` child processes). Only the
// distributed suites start one, so the re-export is unused in the others.
#[path = "cluster.rs"]
mod cluster;
#[allow(unused_imports)]
pub use cluster::*;
