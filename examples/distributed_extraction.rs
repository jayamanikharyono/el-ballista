//! Phase 4 (docs/roadmap.md): run the extraction job through a distributed Ballista deployment.
//! A workload that saturates one machine should scale across three without increasing the load
//! on the source database — each process opens only `pool_max / workers` connections.
//!
//!   cargo run --example distributed_extraction -- <config.json> [workers]
//!
//! The JSON config is the same job spec `rel run` uses (`extract.example.json`); the example
//! prints the first rows of the job's window (or the whole table) exactly as the cluster
//! returns them.

use rust_ballista_extraction_layer::config::JobConfig;
use rust_ballista_extraction_layer::distributed::DistributedContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "extract.example.json".to_string());
    let workers: usize = std::env::args().nth(2).map(|s| s.parse().unwrap()).unwrap_or(2);

    let config = JobConfig::from_file(&config_path)?;

    let ctx = DistributedContext::standalone(&config, workers).await?;
    ctx.register_source(&config).await?;

    let df = ctx
        .session
        .table(&config.table)
        .await?
        .limit(0, Some(100))?;

    println!("distributed extraction (workers={})", ctx.workers);
    df.show().await?;

    Ok(())
}