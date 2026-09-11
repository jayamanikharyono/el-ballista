mod connector;
mod types;
mod errors;
mod config;
mod checkpoint;
mod incremental;
mod pushdown;
mod cli;
mod demo;
mod engine;

use crate::errors::AppError;

struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("[{}] {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

static LOGGER: SimpleLogger = SimpleLogger;

fn init_logger() {
    let _ = log::set_logger(&LOGGER);
    let level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(log::LevelFilter::Info);
    log::set_max_level(level);
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    init_logger();

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
