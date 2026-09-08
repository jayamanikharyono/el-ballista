mod extractor;
mod types;
mod errors;
mod config;
mod checkpoint;
mod incremental;
mod sink;
mod cli;
mod demo;

use crate::errors::AppError;

#[tokio::main]
async fn main() -> Result<(), AppError> {
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
