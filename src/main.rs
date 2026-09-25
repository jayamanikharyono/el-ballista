// `cli` and `demo` are bin-only entry points (not part of the library crate); everything
// else is `rust_ballista_extraction_layer::...` — this binary is a thin wrapper around the
// lib crate, not a second copy of it. See docs/testing-plan.md §7 for why that used to
// matter: main.rs re-declaring every lib module meant the lib and bin targets each
// compiled (and `cargo test --bins` ran) their own copy of every unit test.
mod cli;
mod demo;

use std::process::ExitCode;

use rust_ballista_extraction_layer::connector::postgres::distributed::pool_registry::registry;
use rust_ballista_extraction_layer::errors::error_chain;
use rust_ballista_extraction_layer::logging;

#[tokio::main]
async fn main() -> ExitCode {
    // Wire the `log` facade to a real backend: always stderr, plus a file when
    // `--log-file <path>` (or the REL_LOG_FILE env var) is set. Level comes from
    // `--log-level` / RUST_LOG (default info). At debug level every generated SQL
    // query is logged, so pointing `--log-file` at a path with debug captures each
    // query as it is produced. See src/logging.rs.
    logging::init_from_env_and_args();

    // No args: print usage (the demo only runs when asked for: `rel demo`).
    if std::env::args().nth(1).is_none() {
        eprintln!("{}", cli::USAGE);
        return ExitCode::from(2);
    }

    let result = cli::dispatch().await;
    // Close pooled source connections gracefully before exit.
    registry().close_all().await;

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // The chain exactly once: the error, then each distinct cause.
            let mut chain = error_chain(&e).into_iter();
            if let Some(top) = chain.next() {
                eprintln!("error: {top}");
            }
            for cause in chain {
                eprintln!("  caused by: {cause}");
            }
            ExitCode::FAILURE
        }
    }
}
