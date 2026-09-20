//! Phase 2 DataFrame front end: the same job spec `rel run` uses, driven through
//! `ExtractContext` instead of the CLI.
//!
//!   cargo run --example dataframe_extraction -- <config.json>
//!
//! Registers the table with a cost-aware provider (statistics + EXPLAIN cache feed the
//! pushdown decisions), resolves the incremental window from the checkpoint store, and
//! collects the filtered projection as Arrow RecordBatches.

use datafusion::prelude::{col, lit};
use rust_ballista_extraction_layer::config::JobConfig;
use rust_ballista_extraction_layer::engine::{ExtractContext, Watermark};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Route the `log` facade to stderr (+ optional --log-file / REL_LOG_FILE).
    // Set RUST_LOG=debug (or --log-level debug) to log every generated SQL query.
    rust_ballista_extraction_layer::logging::init_from_env_and_args();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/configs/extract.example.json".to_string());
    let config = JobConfig::from_file(&config_path)?;
    let watermark_column = config.incremental.column.clone();

    let ctx = ExtractContext::from_config(config).await?;

    // Same builder shape as docs/roadmap.md Phase 2.
    let batches = ctx
        .source("postgres", "public.orders")
        .await?
        .incremental(Watermark::timestamp(&watermark_column))
        .await?
        .filter(col("status").eq(lit("PAID")))?
        .select(vec![col("order_id"), col("amount")])?
        .with_column("amount_x2", col("amount") * lit(2.0))?
        .limit(0, Some(1000))?
        .collect()
        .await?;

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "dataframe extraction: {rows} row(s) in {} batch(es)",
        batches.len()
    );

    // SQL entry point against the same registered sources.
    let sql_rows = ctx
        .sql("SELECT COUNT(*) AS n FROM public.orders")
        .await?
        .collect()
        .await?;
    println!("sql entry point: {} batch(es)", sql_rows.len());

    Ok(())
}
