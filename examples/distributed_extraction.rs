//! Phase 4 (docs/roadmap.md): run the extraction job through a distributed Ballista deployment.
//! A workload that saturates one machine should scale across three without increasing the load
//! on the source database — each process opens only `pool_max / workers` connections.
//!
//!   cargo run --example distributed_extraction -- <config.json> [workers] [output.parquet]
//!
//! The JSON config is the same job spec the library and CLI use
//! (`examples/configs/extract.example.json`). The connector applies the config's filters
//! (schema-coerced, pushed to the source when possible) and column projection; this example
//! streams the cluster's result straight into one local Parquet file (memory stays O(batch)).
//! Materializing output is the caller's job — this project is not a sink.

use std::fs::{self, File};

use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Route the `log` facade to stderr (+ optional --log-file / REL_LOG_FILE).
    // Set RUST_LOG=debug (or --log-level debug) to log every generated SQL query.
    rust_ballista_extraction_layer::logging::init_from_env_and_args();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/configs/extract.example.json".to_string());
    let workers: usize = match std::env::args().nth(2) {
        Some(raw) => raw.parse()?,
        None => 2,
    };
    let output_path = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "output/distributed_extraction.parquet".to_string());

    let connector = PostgresConnector::from_config_file(&config_path)?;
    println!("  Table: {}", connector.config().resolved_table());
    println!("  Filters: {:?}", connector.config().filters);

    // In-process scheduler + executors: the same plan-shipping path as a remote cluster,
    // minus the network.
    let mut stream = connector
        .extract()
        .distributed()
        .in_process()
        .workers(workers)
        .stream()
        .await?;
    println!("distributed extraction (workers={workers})");

    if let Some(parent) = std::path::Path::new(&output_path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(&output_path)?;
    let mut writer = ArrowWriter::try_new(file, stream.schema(), None)?;
    let mut rows = 0usize;
    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows();
        writer.write(&batch)?;
    }
    writer.close()?;

    let file_size = fs::metadata(&output_path)?.len();
    println!("  ✓ {rows} row(s) written to {output_path} ({file_size} bytes)");
    Ok(())
}
