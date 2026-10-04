//! Full extraction: every row of the job's table, streamed in bounded-memory batches.
//!
//! The job config names the table and columns (no filters = every row); the connector
//! discovers the schema, maps it to Arrow and streams `RecordBatch`es of at most
//! `execution.batch_size` rows, so memory stays bounded whatever the table size.
//!
//! ```bash
//! PGPASSWORD=... cargo run --example full_extraction -- [config.json]
//! # default: examples/configs/full_extract.dvd_rental.json (the dvdrental demo database)
//! ```

use el_ballista::connector::postgres::{PostgresConnector, close_pools};
use futures::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Route the `log` facade to stderr (+ optional --log-file / EL_BALLISTA_LOG_FILE).
    el_ballista::logging::init_from_env_and_args();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/configs/full_extract.dvd_rental.json".to_string());
    // The password comes from the environment variable the config names (`source.password_env`).
    let connector = PostgresConnector::from_config_file(&config_path)?;
    let config = connector.config();
    println!(
        "► Full extraction of {}.{} (config {config_path})",
        config.source.schema, config.table
    );

    let mut stream = connector.extract().standalone().stream().await?;
    let schema = stream.schema();
    println!("  Arrow schema:");
    for field in schema.fields() {
        println!(
            "    {}: {}{}",
            field.name(),
            field.data_type(),
            if field.is_nullable() {
                " (nullable)"
            } else {
                ""
            }
        );
    }

    let (mut rows, mut batches, mut largest) = (0usize, 0usize, 0usize);
    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows();
        batches += 1;
        largest = largest.max(batch.num_rows());
    }
    println!(
        "  ✓ Total {rows} rows, {} columns, in {batches} batch(es) (largest {largest} rows, cap {})",
        schema.fields().len(),
        config.execution.batch_size
    );

    close_pools().await;
    Ok(())
}
