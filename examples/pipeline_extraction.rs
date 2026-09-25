//! Connector API example — extract via the fluent `PostgresConnector` builder.
//!
//! `connector.extract().standalone()` / `.distributed()` is the single entry point the CLI
//! (`rel run` / `rel distribute`) also uses. `collect()` / `stream()` return the Arrow data
//! with no checkpoint side effects; `run_with(consumer)` is the operational job (a split is
//! checkpointed only after the consumer acknowledged it); `run()` is a diagnostic row count.
//!
//! Usage: cargo run --example pipeline_extraction -- [config.json]

use futures::TryStreamExt;
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

    // Single-node operational run: each split's stream goes to the consumer; the split is
    // checkpointed only after the consumer returns Ok. A second invocation skips them.
    let outcome = connector
        .extract()
        .standalone()
        .run_with(|split, mut stream| async move {
            let mut rows = 0usize;
            while let Some(batch) = stream.try_next().await? {
                rows += batch.num_rows(); // write `batch` somewhere durable here
            }
            println!("  {} delivered {rows} row(s)", split.split_id);
            Ok(())
        })
        .await?;
    println!(
        "standalone run_with(): {} row(s) delivered, splits {}/{} ({} skipped)",
        outcome.rows_delivered,
        outcome.splits_completed,
        outcome.splits_total,
        outcome.splits_skipped
    );

    // Diagnostic count only (no checkpoint, nothing delivered) — distributed over Ballista.
    // `.distributed()` defaults to the standard scheduler URL; `.in_process()` here keeps the
    // example self-contained (spins up a local cluster).
    let counted = connector.extract().distributed().in_process().run().await?;
    println!(
        "distributed run() [diagnostic]: {} row(s), workers {:?}",
        counted.rows_extracted, counted.workers
    );

    Ok(())
}
