//! Connector API example — extract via the fluent `PostgresConnector` builder.
//!
//! `connector.extract().standalone()` / `.distributed()` is the single entry point the CLI
//! (`rel run` / `rel distribute`) also uses. `collect()` returns the Arrow batches with no
//! checkpoint side effects; `run()` performs the operational job (split-execution
//! checkpointing).
//!
//! Usage: cargo run --example pipeline_extraction -- [config.json]

use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set RUST_LOG=debug (or --log-level debug) to log every generated SQL query.
    rust_ballista_extraction_layer::logging::init_from_env_and_args();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/configs/full_extract.example.json".to_string());

    let connector = PostgresConnector::from_config_file(&config_path)?;
    println!(
        "connector for job '{}' (filters: {:?})",
        connector.config().job_id,
        connector.config().filters
    );

    // Single-node: get the Arrow batches back (no checkpoint side effects).
    let batches = connector.extract().standalone().collect().await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "standalone collect(): {} batch(es), {} row(s)",
        batches.len(),
        rows
    );

    // Single-node operational run (split-execution checkpointing).
    let outcome = connector.extract().standalone().run().await?;
    println!(
        "standalone run(): {} row(s), splits {}/{}",
        outcome.rows_extracted, outcome.splits_completed, outcome.splits_total
    );

    // Distributed over Ballista. `.distributed()` defaults to the standard scheduler URL;
    // `.in_process()` here keeps the example self-contained (spins up a local cluster).
    let outcome = connector.extract().distributed().in_process().run().await?;
    println!(
        "distributed run(): {} row(s), workers {:?}",
        outcome.rows_extracted, outcome.workers
    );

    Ok(())
}
