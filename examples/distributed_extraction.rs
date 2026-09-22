//! Phase 4 (docs/roadmap.md): run the extraction job through a distributed Ballista deployment.
//! A workload that saturates one machine should scale across three without increasing the load
//! on the source database — each process opens only `pool_max / workers` connections.
//!
//!   cargo run --example distributed_extraction -- <config.json> [workers]
//!
//! The JSON config is the same job spec `rel run` uses
//! (`examples/configs/extract.example.json`); the example
//! prints the first rows of the table (or the filtered range) exactly as the cluster
//! returns them.

use arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Expr;
use datafusion::prelude::col;
use parquet::arrow::ArrowWriter;
use rust_ballista_extraction_layer::config::JobConfig;
use rust_ballista_extraction_layer::connector::postgres::pipeline::Pipeline;
use rust_ballista_extraction_layer::distributed::DistributedContext;
use std::fs;
use std::fs::File;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Route the `log` facade to stderr (+ optional --log-file / REL_LOG_FILE).
    // Set RUST_LOG=debug (or --log-level debug) to log every generated SQL query.
    rust_ballista_extraction_layer::logging::init_from_env_and_args();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/configs/extract.example.json".to_string());
    let workers: usize = std::env::args()
        .nth(2)
        .map(|s| s.parse().unwrap())
        .unwrap_or(2);

    let config = JobConfig::from_file(&config_path)?;
    // The pipeline is the single choke point for what the config's `filters`
    // mean — every path (standalone, distributed, CLI) funnels through
    // `filter_exprs_with_schema`, so this example reuses it instead of
    // re-parsing filters. Skipping this loop silently extracts the FULL table.
    let pipeline = Pipeline::from_config(config);
    println!("  Table: {}", pipeline.config().resolved_table());
    println!("  Filters: {:?}", pipeline.config().filters);

    let ctx = DistributedContext::standalone(pipeline.config(), workers).await?;
    ctx.register_source(pipeline.config()).await?;

    let mut df = ctx.session.table(&pipeline.config().table).await?;
    // Same application as `Pipeline::extract_distributed`: one `filter()` per
    // AND-conjunct (an OR-group lowers to a single `a OR b` expression), then
    // the config's column projection. Each `filter()` is AND semantics.
    {
        let schema = df.schema().inner().clone();
        for expr in pipeline.filter_exprs_with_schema(&schema)? {
            df = df.filter(expr)?;
        }
        if let Some(columns) = &pipeline.config().columns {
            let proj: Vec<Expr> = columns.iter().map(|c| col(c.as_str())).collect();
            df = df.select(proj)?;
        }
    }

    println!("distributed extraction (workers={})", ctx.workers);
    println!("\n► Step 6: Write results to Parquet (local sink)");

    let schema: SchemaRef = df.schema().inner().clone();
    let results = df.collect().await?;
    // Output location comes from the job spec's `sink.path`, not a hardcoded
    // string: `<sink.path>.parquet` next to the configured sink directory.
    let output_path = format!("{}.parquet", pipeline.config().sink.path);
    if let Some(parent) = std::path::Path::new(&output_path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }

    let file = File::create(&output_path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    for batch in &results {
        writer.write(batch)?;
    }
    writer.close()?;

    let file_size = fs::metadata(&output_path)?.len();
    println!("  ✓ Parquet file written: {}", output_path);
    println!("  ✓ File size: {} bytes", file_size);

    Ok(())
}
