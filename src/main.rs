// `cli` and `demo` are bin-only entry points (not part of the library crate); everything
// else is `rust_ballista_extraction_layer::...` — this binary is a thin wrapper around the
// lib crate, not a second copy of it. See docs/testing-plan.md §7 for why that used to
// matter: main.rs re-declaring every lib module meant the lib and bin targets each
// compiled (and `cargo test --bins` ran) their own copy of every unit test.
mod cli;
mod demo;

use rust_ballista_extraction_layer::errors::AppError;
use rust_ballista_extraction_layer::logging;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    // Wire the `log` facade to a real backend: always stderr, plus a file when
    // `--log-file <path>` (or the REL_LOG_FILE env var) is set. Level comes from
    // `--log-level` / RUST_LOG (default info). At debug level every generated SQL
    // query is logged, so pointing `--log-file` at a path with debug captures each
    // query as it is produced. See src/logging.rs.
    logging::init_from_env_and_args();

    // No args: run the demo pipeline (extract -> filter -> transform -> drop/rename ->
    // aggregate -> write). Any args: hand off to the checkpoint-driven CLI (`rel run`,
    // `rel checkpoint show|reset`, or `rel demo` to run the same pipeline explicitly).
    let has_args = std::env::args().nth(1).is_some();

    let result = if has_args {
        cli::dispatch().await
    } else {
        demo::run().await
    };

    if let Err(e) = &result {
        eprintln!("error: {e}");
    }

    result
}
